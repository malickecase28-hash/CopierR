use crate::config::Durability;
use anyhow::{Context, Result};
use copier_core::{ExecutionAck, ExecutionCommand, MirrorBinding, TradeEvent};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredCommandState {
    Queued,
    Dispatched,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCommand {
    pub command: ExecutionCommand,
    pub state: StoredCommandState,
    #[serde(default)]
    pub dispatched_at: Option<i64>,
    #[serde(default)]
    pub unknown_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeferredEvent {
    pub route_id: String,
    pub event: TradeEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MirrorMutation {
    Upsert { binding: MirrorBinding },
    Remove { binding_key: String },
    UpsertPending { binding: MirrorBinding },
    RemovePending { binding_key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JournalRecord {
    EventPlanned {
        event_id: String,
        commands: Vec<ExecutionCommand>,
        #[serde(default)]
        deferred: Vec<DeferredEvent>,
        #[serde(default)]
        source_timestamp: Option<i64>,
    },
    DeferredPlanned {
        route_id: String,
        event_id: String,
        command: Box<Option<ExecutionCommand>>,
    },
    CommandDispatched {
        command_id: String,
        timestamp_unix_ns: i64,
    },
    CommandUnknown {
        command_id: String,
        timestamp_unix_ns: i64,
        reason: String,
    },
    Checkpoint {
        state: Box<ReplayState>,
    },
    PauseChanged {
        paused: bool,
    },
    AccountHold {
        account_id: String,
        reason: Option<String>,
    },
    MirrorReviewed {
        mutation: MirrorMutation,
        reason: String,
        timestamp_unix_ns: i64,
    },
    AckApplied {
        ack: ExecutionAck,
        mirror: Box<Option<MirrorMutation>>,
        #[serde(default)]
        operator: bool,
    },
    /// A follower position copied without the stop loss / take profit it was meant to carry (`missing` set), or given it since
    /// (`missing` = null). The copy stays bound, so the source's closes still reach it.
    ProtectionState {
        binding_key: String,
        account_id: String,
        target_order_id: String,
        missing: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UnprotectedCopy {
    pub account_id: String,
    pub target_order_id: String,
    pub reason: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ReplayState {
    pub seen_events: HashSet<String>,
    #[serde(default)]
    pub event_times: HashMap<String, i64>,
    #[serde(default)]
    pub pruned_event_cutoff: Option<i64>,
    pub commands: HashMap<String, StoredCommand>,
    pub mirrors: HashMap<String, MirrorBinding>,
    #[serde(default)]
    pub pending_mirrors: HashMap<String, MirrorBinding>,
    pub paused: bool,
    pub holds: HashMap<String, String>,
    pub recent_failures: VecDeque<ExecutionAck>,
    #[serde(default)]
    pub recent_reviews: VecDeque<JournalRecord>,
    #[serde(default)]
    pub deferred: Vec<DeferredEvent>,
    #[serde(default)]
    pub unprotected: HashMap<String, UnprotectedCopy>,
}

impl ReplayState {
    pub fn apply(&mut self, record: &JournalRecord) {
        match record {
            JournalRecord::Checkpoint { state } => *self = (**state).clone(),
            JournalRecord::MirrorReviewed { mutation, .. } => {
                self.mutate_mirror(mutation);
                self.remember_review(record);
            }
            JournalRecord::PauseChanged { paused } => self.paused = *paused,
            JournalRecord::ProtectionState { binding_key, account_id, target_order_id, missing } => match missing {
                Some(reason) => {
                    self.unprotected.insert(binding_key.clone(), UnprotectedCopy {
                        account_id: account_id.clone(),
                        target_order_id: target_order_id.clone(),
                        reason: reason.clone(),
                    });
                }
                None => {
                    self.unprotected.remove(binding_key);
                }
            },
            JournalRecord::AccountHold { account_id, reason } => {
                if let Some(reason) = reason {
                    self.holds.insert(account_id.clone(), reason.clone());
                } else {
                    self.holds.remove(account_id);
                }
            }
            JournalRecord::EventPlanned {
                event_id,
                commands,
                deferred,
                source_timestamp,
            } => {
                self.seen_events.insert(event_id.clone());
                if let Some(timestamp) = source_timestamp {
                    self.event_times.insert(event_id.clone(), *timestamp);
                }
                self.deferred.extend(deferred.iter().cloned());
                for command in commands {
                    self.queue(command);
                }
            }
            JournalRecord::DeferredPlanned {
                route_id,
                event_id,
                command,
            } => {
                self.deferred.retain(|pending| {
                    pending.route_id != *route_id || pending.event.event_id != *event_id
                });
                if let Some(command) = command.as_ref() {
                    self.queue(command);
                }
            }
            JournalRecord::CommandDispatched {
                command_id,
                timestamp_unix_ns,
            } => {
                if let Some(stored) = self.commands.get_mut(command_id) {
                    stored.state = StoredCommandState::Dispatched;
                    stored.dispatched_at = Some(*timestamp_unix_ns);
                }
            }
            JournalRecord::CommandUnknown {
                command_id, reason, ..
            } => {
                if let Some(stored) = self.commands.get_mut(command_id) {
                    stored.state = StoredCommandState::Unknown;
                    stored.unknown_reason = Some(reason.clone());
                }
            }
            JournalRecord::AckApplied {
                ack,
                mirror,
                operator,
            } => {
                if matches!(
                    ack.status,
                    copier_core::AckStatus::Rejected | copier_core::AckStatus::Unknown
                ) {
                    if self.recent_failures.len() >= 100 {
                        self.recent_failures.pop_front();
                    }
                    self.recent_failures.push_back(ack.clone());
                }
                if ack.status == copier_core::AckStatus::Unknown {
                    if let Some(stored) = self.commands.get_mut(&ack.command_id) {
                        stored.state = StoredCommandState::Unknown;
                        stored.unknown_reason = ack.message.clone();
                    }
                } else {
                    self.commands.remove(&ack.command_id);
                }
                if let Some(mutation) = mirror.as_ref() {
                    self.mutate_mirror(mutation);
                }
                if *operator {
                    self.remember_review(record);
                }
            }
        }
    }

    fn remember_review(&mut self, record: &JournalRecord) {
        if self.recent_reviews.len() >= 100 {
            self.recent_reviews.pop_front();
        }
        self.recent_reviews.push_back(record.clone());
    }

    fn mutate_mirror(&mut self, mutation: &MirrorMutation) {
        match mutation {
            MirrorMutation::Upsert { binding } => {
                self.mirrors.insert(binding.binding_key(), binding.clone());
            }
            MirrorMutation::Remove { binding_key } => {
                self.mirrors.remove(binding_key);
                self.unprotected.remove(binding_key);
            }
            MirrorMutation::UpsertPending { binding } => {
                self.pending_mirrors
                    .insert(binding.binding_key(), binding.clone());
            }
            MirrorMutation::RemovePending { binding_key } => {
                self.pending_mirrors.remove(binding_key);
            }
        }
    }

    pub fn prune_events_before(&mut self, cutoff: i64) {
        self.pruned_event_cutoff = Some(
            self.pruned_event_cutoff
                .map_or(cutoff, |previous| previous.max(cutoff)),
        );
        self.event_times.retain(|event_id, timestamp| {
            if *timestamp < cutoff {
                self.seen_events.remove(event_id);
                false
            } else {
                true
            }
        });
    }

    fn queue(&mut self, command: &ExecutionCommand) {
        self.commands
            .entry(command.command_id.clone())
            .or_insert_with(|| StoredCommand {
                command: command.clone(),
                state: StoredCommandState::Queued,
                dispatched_at: None,
                unknown_reason: None,
            });
    }

    pub fn finalize_after_replay(&mut self) {
        for stored in self.commands.values_mut() {
            if stored.state == StoredCommandState::Dispatched {
                stored.state = StoredCommandState::Unknown;
                stored.unknown_reason =
                    Some("daemon restarted before terminal acknowledgement".to_owned());
            }
        }
    }
}

pub struct Journal {
    path: PathBuf,
    durability: Durability,
    file: Mutex<Option<tokio::fs::File>>,
    _lock: fs::File,
}

impl Journal {
    pub async fn open(path: PathBuf, durability: Durability) -> Result<(Self, ReplayState)> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create journal directory {}", parent.display())
            })?;
        }
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock_exclusive()
            .with_context(|| format!("journal is locked by another process: {}", path.display()))?;
        let mut replay = replay_file(&path)?;
        replay.finalize_after_replay();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("failed to open journal {}", path.display()))?;
        Ok((
            Self {
                path,
                durability,
                file: Mutex::new(Some(file)),
                _lock: lock,
            },
            replay,
        ))
    }

    pub async fn append(&self, record: &JournalRecord) -> Result<()> {
        if self.durability == Durability::None {
            return Ok(());
        }
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        let mut slot = self.file.lock().await;
        let mut file = slot
            .take()
            .context("journal is unavailable after a write failure")?;
        file.write_all(&line)
            .await
            .with_context(|| format!("failed to append journal {}", self.path.display()))?;
        file.flush().await?;
        if self.durability == Durability::Fsync {
            file.sync_data().await?;
        }
        *slot = Some(file);
        Ok(())
    }

    pub async fn compact(&self, state: &ReplayState) -> Result<()> {
        if self.durability == Durability::None {
            return Ok(());
        }
        let mut slot = self.file.lock().await;
        drop(
            slot.take()
                .context("journal is unavailable after a write failure")?,
        );
        let path = self.path.clone();
        let state = state.clone();
        let result =
            tokio::task::spawn_blocking(move || replace_with_checkpoint(&path, state)).await;
        *slot = Some(OpenOptions::new().append(true).open(&self.path).await?);
        result??;
        Ok(())
    }
}

fn replace_with_checkpoint(path: &Path, state: ReplayState) -> Result<()> {
    let mut name = path.as_os_str().to_owned();
    name.push(".checkpoint.tmp");
    let temporary = PathBuf::from(name);
    let mut file = BufWriter::new(fs::File::create(&temporary)?);
    serde_json::to_writer(
        &mut file,
        &JournalRecord::Checkpoint {
            state: Box::new(state),
        },
    )?;
    file.write_all(b"\n")?;
    file.flush()?;
    file.get_ref().sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    #[cfg(unix)]
    fs::File::open(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}

fn replay_file(path: &Path) -> Result<ReplayState> {
    let mut replay = ReplayState::default();
    if !path.exists() {
        return Ok(replay);
    }
    let file = fs::File::open(path)
        .with_context(|| format!("failed to replay journal {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut index = 0;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        index += 1;
        anyhow::ensure!(
            line.ends_with('\n'),
            "incomplete journal record at line {index}"
        );
        if line.trim().is_empty() {
            continue;
        }
        let record: JournalRecord = serde_json::from_str(&line)
            .with_context(|| format!("invalid journal record at line {index}"))?;
        replay.apply(&record);
    }
    Ok(replay)
}
