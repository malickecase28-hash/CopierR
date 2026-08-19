use crate::config::Durability;
use anyhow::{Context, Result};
use copier_core::{ExecutionAck, ExecutionCommand, MirrorBinding};
use serde::{Deserialize, Serialize};
use std::{collections::{HashMap, HashSet}, fs, path::{Path, PathBuf}, sync::Arc};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredCommandState {
    Queued,
    Dispatched,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct StoredCommand {
    pub command: ExecutionCommand,
    pub state: StoredCommandState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MirrorMutation {
    Upsert { binding: MirrorBinding },
    Remove { binding_key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum JournalRecord {
    EventPlanned {
        event_id: String,
        commands: Vec<ExecutionCommand>,
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
    AckApplied {
        ack: ExecutionAck,
        mirror: Option<MirrorMutation>,
    },
}

#[derive(Debug, Default)]
pub struct ReplayState {
    pub seen_events: HashSet<String>,
    pub commands: HashMap<String, StoredCommand>,
    pub mirrors: HashMap<String, MirrorBinding>,
}

impl ReplayState {
    pub fn apply(&mut self, record: &JournalRecord) {
        match record {
            JournalRecord::EventPlanned { event_id, commands } => {
                self.seen_events.insert(event_id.clone());
                for command in commands {
                    self.commands.entry(command.command_id.clone()).or_insert_with(|| StoredCommand {
                        command: command.clone(),
                        state: StoredCommandState::Queued,
                    });
                }
            }
            JournalRecord::CommandDispatched { command_id, .. } => {
                if let Some(stored) = self.commands.get_mut(command_id) {
                    stored.state = StoredCommandState::Dispatched;
                }
            }
            JournalRecord::CommandUnknown { command_id, .. } => {
                if let Some(stored) = self.commands.get_mut(command_id) {
                    stored.state = StoredCommandState::Unknown;
                }
            }
            JournalRecord::AckApplied { ack, mirror } => {
                if ack.status == copier_core::AckStatus::Unknown {
                    if let Some(stored) = self.commands.get_mut(&ack.command_id) {
                        stored.state = StoredCommandState::Unknown;
                    }
                } else {
                    self.commands.remove(&ack.command_id);
                }
                if let Some(mutation) = mirror {
                    match mutation {
                        MirrorMutation::Upsert { binding } => {
                            self.mirrors.insert(binding.binding_key(), binding.clone());
                        }
                        MirrorMutation::Remove { binding_key } => {
                            self.mirrors.remove(binding_key);
                        }
                    }
                }
            }
        }
    }

    pub fn finalize_after_replay(&mut self) {
        for stored in self.commands.values_mut() {
            if stored.state == StoredCommandState::Dispatched {
                stored.state = StoredCommandState::Unknown;
            }
        }
    }
}

pub struct Journal {
    path: PathBuf,
    durability: Durability,
    file: Arc<Mutex<tokio::fs::File>>,
}

impl Journal {
    pub async fn open(path: PathBuf, durability: Durability) -> Result<(Self, ReplayState)> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create journal directory {}", parent.display()))?;
        }
        let mut replay = replay_file(&path)?;
        replay.finalize_after_replay();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("failed to open journal {}", path.display()))?;
        Ok((Self { path, durability, file: Arc::new(Mutex::new(file)) }, replay))
    }

    pub async fn append(&self, record: &JournalRecord) -> Result<()> {
        if self.durability == Durability::None {
            return Ok(());
        }
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        let mut file = self.file.lock().await;
        file.write_all(&line).await
            .with_context(|| format!("failed to append journal {}", self.path.display()))?;
        file.flush().await?;
        if self.durability == Durability::Fsync {
            file.sync_data().await?;
        }
        Ok(())
    }
}

fn replay_file(path: &Path) -> Result<ReplayState> {
    let mut replay = ReplayState::default();
    if !path.exists() {
        return Ok(replay);
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to replay journal {}", path.display()))?;
    for (index, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: JournalRecord = serde_json::from_str(line)
            .with_context(|| format!("invalid journal record at line {}", index + 1))?;
        replay.apply(&record);
    }
    Ok(replay)
}
