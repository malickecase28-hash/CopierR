use crate::{config::DaemonConfig, journal::{Journal, JournalRecord, MirrorMutation, ReplayState, StoredCommandState}};
use anyhow::{bail, Result};
use copier_core::{AckStatus, AgentFrame, CopyEngine, ExecutionAck, ExecutionCommand, MirrorBinding, ServerFrame, TradeAction, TradeEvent};
use std::{collections::HashMap, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::{SystemTime, UNIX_EPOCH}};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, info, warn};

#[derive(Clone)]
struct Session {
    id: u64,
    tx: mpsc::Sender<ServerFrame>,
}

pub struct AppState {
    pub config: Arc<DaemonConfig>,
    engine: CopyEngine,
    journal: Arc<Journal>,
    runtime: Mutex<ReplayState>,
    sessions: RwLock<HashMap<String, Session>>,
    next_session_id: AtomicU64,
}

impl AppState {
    pub fn new(config: Arc<DaemonConfig>, journal: Arc<Journal>, replay: ReplayState) -> Result<Self> {
        let engine = CopyEngine::new(config.routes.clone())?;
        Ok(Self {
            config,
            engine,
            journal,
            runtime: Mutex::new(replay),
            sessions: RwLock::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
        })
    }

    pub fn authenticate(&self, account_id: &str, platform: copier_core::Platform, token: &str) -> Result<()> {
        let account = self.config.account(account_id).ok_or_else(|| anyhow::anyhow!("unknown account {account_id}"))?;
        if account.platform != platform {
            bail!("platform mismatch for account {account_id}");
        }
        if account.token != token {
            bail!("invalid token for account {account_id}");
        }
        Ok(())
    }

    pub async fn register_session(&self, account_id: String, tx: mpsc::Sender<ServerFrame>) -> u64 {
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        self.sessions.write().await.insert(account_id.clone(), Session { id, tx });
        info!(account = %account_id, session_id = id, "account connected");
        id
    }

    pub async fn dispatch_queued_for(&self, account_id: &str) -> Result<()> {
        let queued: Vec<ExecutionCommand> = {
            let runtime = self.runtime.lock().await;
            runtime.commands.values()
                .filter(|stored| stored.state == StoredCommandState::Queued && stored.command.target_account_id == account_id)
                .map(|stored| stored.command.clone())
                .collect()
        };
        for command in queued {
            self.dispatch_if_connected(command).await?;
        }
        Ok(())
    }

    pub async fn unregister_session(&self, account_id: &str, session_id: u64) -> Result<()> {
        let removed = {
            let mut sessions = self.sessions.write().await;
            match sessions.get(account_id) {
                Some(session) if session.id == session_id => {
                    sessions.remove(account_id);
                    true
                }
                _ => false,
            }
        };
        if !removed {
            return Ok(());
        }
        info!(account = %account_id, session_id, "account disconnected");
        let command_ids: Vec<String> = {
            let runtime = self.runtime.lock().await;
            runtime.commands.values()
                .filter(|stored| stored.state == StoredCommandState::Dispatched && stored.command.target_account_id == account_id)
                .map(|stored| stored.command.command_id.clone())
                .collect()
        };
        for command_id in command_ids {
            self.mark_unknown(&command_id, "connection lost before terminal acknowledgement").await?;
        }
        Ok(())
    }

    pub async fn handle_frame(&self, connected_account_id: &str, frame: AgentFrame) -> Result<()> {
        match frame {
            AgentFrame::Hello(_) => bail!("HELLO is only valid as the first frame"),
            AgentFrame::Event(event) => {
                if event.source_account_id != connected_account_id {
                    bail!("event source account does not match authenticated session");
                }
                self.handle_event(event).await
            }
            AgentFrame::Ack(ack) => {
                if ack.account_id != connected_account_id {
                    bail!("ack account does not match authenticated session");
                }
                self.handle_ack(ack).await
            }
            AgentFrame::Ping(_) => Ok(()),
        }
    }

    async fn handle_event(&self, event: TradeEvent) -> Result<()> {
        let account = self.config.account(&event.source_account_id)
            .ok_or_else(|| anyhow::anyhow!("unknown source account {}", event.source_account_id))?;
        if !account.role.can_publish() {
            bail!("account {} is not permitted to publish", event.source_account_id);
        }
        if event.origin_command_id.is_some() && !account.allow_rebroadcast {
            debug!(event_id = %event.event_id, "suppressed copied-trade feedback event");
            return Ok(());
        }
        {
            let runtime = self.runtime.lock().await;
            if runtime.seen_events.contains(&event.event_id) {
                return Ok(());
            }
        }

        let now = unix_time_ns();
        let mut planned = Vec::new();
        for route in self.engine.routes_for(&event.source_account_id) {
            let binding_key = MirrorBinding::key(&event.source_account_id, &event.source_order_id, &route.target_account_id);
            let mirror = {
                let runtime = self.runtime.lock().await;
                runtime.mirrors.get(&binding_key).cloned()
            };
            match self.engine.build_command(&event, route, mirror.as_ref(), now) {
                Ok(Some(command)) => planned.push(command),
                Ok(None) => {}
                Err(copier_core::RouteError::MissingMirror(_)) => {
                    warn!(event_id = %event.event_id, route = %route.id, "mirror binding not ready; event skipped for route");
                }
                Err(error) => return Err(error.into()),
            }
        }

        let record = JournalRecord::EventPlanned {
            event_id: event.event_id.clone(),
            commands: planned.clone(),
        };
        self.journal.append(&record).await?;
        {
            let mut runtime = self.runtime.lock().await;
            runtime.apply(&record);
        }
        for command in planned {
            self.dispatch_if_connected(command).await?;
        }
        Ok(())
    }

    async fn dispatch_if_connected(&self, command: ExecutionCommand) -> Result<()> {
        let session = self.sessions.read().await.get(&command.target_account_id).cloned();
        let Some(session) = session else {
            return Ok(());
        };
        let dispatched = JournalRecord::CommandDispatched {
            command_id: command.command_id.clone(),
            timestamp_unix_ns: unix_time_ns(),
        };
        self.journal.append(&dispatched).await?;
        {
            let mut runtime = self.runtime.lock().await;
            runtime.apply(&dispatched);
        }
        if session.tx.send(ServerFrame::Command(command.clone())).await.is_err() {
            self.mark_unknown(&command.command_id, "session queue closed before write").await?;
        }
        Ok(())
    }

    async fn mark_unknown(&self, command_id: &str, reason: &str) -> Result<()> {
        let record = JournalRecord::CommandUnknown {
            command_id: command_id.to_owned(),
            timestamp_unix_ns: unix_time_ns(),
            reason: reason.to_owned(),
        };
        self.journal.append(&record).await?;
        let mut runtime = self.runtime.lock().await;
        runtime.apply(&record);
        warn!(command_id, reason, "command outcome is UNKNOWN; automatic retry disabled");
        Ok(())
    }

    async fn handle_ack(&self, ack: ExecutionAck) -> Result<()> {
        let command = {
            let runtime = self.runtime.lock().await;
            runtime.commands.get(&ack.command_id).map(|stored| stored.command.clone())
        };
        let Some(command) = command else {
            debug!(command_id = %ack.command_id, "duplicate or late acknowledgement ignored");
            return Ok(());
        };
        if command.target_account_id != ack.account_id {
            bail!("ack target does not match command target");
        }
        if ack.status == AckStatus::Unknown {
            let record = JournalRecord::AckApplied { ack, mirror: None };
            self.journal.append(&record).await?;
            self.runtime.lock().await.apply(&record);
            return Ok(());
        }
        if matches!(ack.status, AckStatus::Accepted | AckStatus::Filled)
            && command.action == TradeAction::Open
            && ack.external_id.is_none()
        {
            self.mark_unknown(&command.command_id, "open acknowledgement omitted external order id").await?;
            return Ok(());
        }

        let mirror = if matches!(ack.status, AckStatus::Accepted | AckStatus::Filled) {
            self.mirror_mutation(&command, &ack).await?
        } else {
            None
        };
        let record = JournalRecord::AckApplied { ack: ack.clone(), mirror };
        self.journal.append(&record).await?;
        self.runtime.lock().await.apply(&record);
        info!(command_id = %ack.command_id, account = %ack.account_id, status = %ack.status, "terminal acknowledgement applied");
        Ok(())
    }

    async fn mirror_mutation(&self, command: &ExecutionCommand, ack: &ExecutionAck) -> Result<Option<MirrorMutation>> {
        let key = MirrorBinding::key(&command.source_account_id, &command.source_order_id, &command.target_account_id);
        match command.action {
            TradeAction::Open => {
                let target_order_id = ack.external_id.clone().expect("validated external id");
                Ok(Some(MirrorMutation::Upsert {
                    binding: MirrorBinding {
                        route_id: command.route_id.clone(),
                        source_account_id: command.source_account_id.clone(),
                        source_order_id: command.source_order_id.clone(),
                        target_account_id: command.target_account_id.clone(),
                        target_order_id,
                        source_open_volume: command.source_volume,
                        source_remaining_volume: command.source_remaining_volume.unwrap_or(command.source_volume),
                        target_open_volume: command.volume,
                        target_remaining_volume: command.volume,
                    },
                }))
            }
            TradeAction::Modify => Ok(None),
            TradeAction::Reduce => {
                let current = self.runtime.lock().await.mirrors.get(&key).cloned();
                let Some(mut binding) = current else { return Ok(None); };
                binding.source_remaining_volume = command.source_remaining_volume
                    .unwrap_or_else(|| (binding.source_remaining_volume - command.source_volume).max(0.0));
                binding.target_remaining_volume = (binding.target_remaining_volume - command.volume).max(0.0);
                Ok(Some(MirrorMutation::Upsert { binding }))
            }
            TradeAction::Close => Ok(Some(MirrorMutation::Remove { binding_key: key })),
        }
    }
}

pub fn unix_time_ns() -> i64 {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    duration.as_nanos().min(i64::MAX as u128) as i64
}
