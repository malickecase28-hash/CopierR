use crate::{
    config::{read_secret, AccountBackend, DaemonConfig},
    journal::{
        DeferredEvent, Journal, JournalRecord, MirrorMutation, ReplayState, StoredCommandState,
    },
};
use anyhow::{bail, ensure, Result};
use copier_core::{
    AckStatus, AgentFrame, BrokerPosition, CopyEngine, ExecutionAck, ExecutionCommand,
    MirrorBinding, RouteRule, ServerFrame, TradeAction, TradeEvent,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::{debug, error, info, warn};

#[cfg(test)]
mod tests;

struct Session {
    id: u64,
    tx: mpsc::Sender<ServerFrame>,
    positions: Option<Vec<BrokerPosition>>,
    discrepancies: Vec<String>,
    snapshot_at: Option<Instant>,
}

#[derive(Default, Serialize)]
struct Latency {
    count: u64,
    sum_ms: u64,
    max_ms: u64,
    buckets: [u64; 8],
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Alert {
    kind: &'static str,
    account_id: String,
    command_id: Option<String>,
}

impl Latency {
    const BOUNDS_MS: [u64; 7] = [1, 5, 10, 25, 100, 500, 5_000];

    fn observe(&mut self, start: i64, end: i64) {
        let ms = elapsed_ms(start, end);
        self.count += 1;
        self.sum_ms = self.sum_ms.saturating_add(ms);
        self.max_ms = self.max_ms.max(ms);
        let index = Self::BOUNDS_MS
            .iter()
            .position(|bound| ms <= *bound)
            .unwrap_or(7);
        self.buckets[index] += 1;
    }
}

struct Runtime {
    ledger: ReplayState,
    sessions: HashMap<String, Session>,
    next_session_id: u64,
    dispatch_latency: Latency,
    acknowledgement_latency: Latency,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AckOrigin {
    Venue,
    Local,
    Operator,
}

#[derive(Deserialize)]
pub struct MirrorReview {
    pub route_id: String,
    pub source_id: String,
    pub target_id: Option<String>,
    pub source_lots: Option<f64>,
    #[serde(default)]
    pub pending: bool,
    pub reason: String,
}

pub struct AppState {
    pub config: Arc<DaemonConfig>,
    engine: CopyEngine,
    journal: Arc<Journal>,
    runtime: Mutex<Runtime>,
    wake: Notify,
}

impl AppState {
    pub fn new(
        config: Arc<DaemonConfig>,
        journal: Arc<Journal>,
        replay: ReplayState,
    ) -> Result<Self> {
        let engine = CopyEngine::new(config.routes.clone())?;
        // A copy keeps following its route's closes, so the route must stay in the configuration while it has copies (with
        // enabled = false to stop new opens). Removing it would leave those positions with nothing to close them.
        if let Some(binding) = replay
            .mirrors
            .values()
            .chain(replay.pending_mirrors.values())
            .find(|binding| !config.routes.iter().any(|route| route.id == binding.route_id))
        {
            bail!(
                "route {} was removed while it still has copied exposure ({} -> {}); restore it with enabled = false and                  reconcile or close its copies before removing it",
                binding.route_id,
                binding.source_order_id,
                binding.target_order_id
            );
        }
        Ok(Self {
            config,
            engine,
            journal,
            wake: Notify::new(),
            runtime: Mutex::new(Runtime {
                ledger: replay,
                sessions: HashMap::new(),
                next_session_id: 1,
                dispatch_latency: Latency::default(),
                acknowledgement_latency: Latency::default(),
            }),
        })
    }

    pub fn authenticate(
        &self,
        account_id: &str,
        platform: copier_core::Platform,
        token: &str,
    ) -> Result<()> {
        let account = self
            .config
            .account(account_id)
            .ok_or_else(|| anyhow::anyhow!("unknown account {account_id}"))?;
        ensure!(
            account.platform == platform,
            "platform mismatch for account {account_id}"
        );
        let expected = match account.token_env.as_deref() {
            Some(name) if !name.trim().is_empty() => read_secret(name)?,
            _ => account.token.clone(),
        };
        ensure!(
            token_matches(&expected, token),
            "invalid token for account {account_id}"
        );
        Ok(())
    }

    pub async fn register_session(
        &self,
        account_id: String,
        tx: mpsc::Sender<ServerFrame>,
    ) -> Result<u64> {
        let mut runtime = self.runtime.lock().await;
        ensure!(
            !runtime.sessions.contains_key(&account_id),
            "account {account_id} already has an active session"
        );
        let id = runtime.next_session_id;
        runtime.next_session_id += 1;
        info!(account = %account_id, session_id = id, "account connected");
        runtime.sessions.insert(
            account_id,
            Session {
                id,
                tx,
                positions: None,
                discrepancies: Vec::new(),
                snapshot_at: None,
            },
        );
        self.wake.notify_one();
        Ok(id)
    }

    pub async fn unregister_session(&self, account_id: &str, session_id: u64) -> Result<()> {
        let mut runtime = self.runtime.lock().await;
        if !runtime
            .sessions
            .get(account_id)
            .is_some_and(|session| session.id == session_id)
        {
            return Ok(());
        }
        runtime.sessions.remove(account_id);
        warn!(account = %account_id, session_id, "account disconnected");
        let dispatched: Vec<_> = runtime
            .ledger
            .commands
            .values()
            .filter(|stored| {
                stored.state == StoredCommandState::Dispatched
                    && stored.command.target_account_id == account_id
            })
            .map(|stored| stored.command.command_id.clone())
            .collect();
        for command_id in dispatched {
            self.mark_unknown(
                &mut runtime.ledger,
                command_id,
                "connection lost before terminal acknowledgement",
            )
            .await?;
        }
        Ok(())
    }

    pub async fn handle_frame(&self, connected_account_id: &str, frame: AgentFrame) -> Result<()> {
        match frame {
            AgentFrame::Hello(_) => bail!("HELLO is only valid as the first frame"),
            AgentFrame::Event(event) => {
                ensure!(
                    event.source_account_id == connected_account_id,
                    "event source account does not match authenticated session"
                );
                self.handle_event(event).await
            }
            AgentFrame::Ack(ack) => {
                ensure!(
                    ack.account_id == connected_account_id,
                    "ack account does not match authenticated session"
                );
                let mut runtime = self.runtime.lock().await;
                self.handle_ack(&mut runtime, ack, AckOrigin::Venue).await
            }
            AgentFrame::Ping(_) => {
                if let Some(session) = self.runtime.lock().await.sessions.get(connected_account_id)
                {
                    let _ = session.tx.try_send(ServerFrame::Pong {
                        server_time_unix_ns: unix_time_ns(),
                    });
                }
                Ok(())
            }
            AgentFrame::Snapshot(positions) => {
                self.observe_positions(connected_account_id, positions)
                    .await
            }
        }
    }

    async fn handle_event(&self, event: TradeEvent) -> Result<()> {
        event.validate()?;
        let account = self
            .config
            .account(&event.source_account_id)
            .ok_or_else(|| anyhow::anyhow!("unknown source account {}", event.source_account_id))?;
        ensure!(
            account.platform == event.platform && account.role.can_publish(),
            "account {} cannot publish this event",
            account.id
        );
        if event.origin_command_id.is_some() && !account.allow_rebroadcast {
            return Ok(());
        }
        let mut runtime = self.runtime.lock().await;
        let cutoff = self
            .event_cutoff()
            .into_iter()
            .chain(runtime.ledger.pruned_event_cutoff)
            .max();
        let source_timestamp = cutoff.map(|_| event.timestamp_unix_ns);
        if let Some(cutoff) = cutoff {
            ensure!(
                event.timestamp_unix_ns >= cutoff
                    && event.timestamp_unix_ns <= unix_time_ns().saturating_add(60_000_000_000),
                "event timestamp is outside the configured deduplication window"
            );
        }

        // Include the account in new deduplication keys while still honoring old journals.
        let event_id = serde_json::to_string(&(&event.source_account_id, &event.event_id))?;
        if runtime.ledger.seen_events.contains(&event_id)
            || runtime.ledger.seen_events.contains(&event.event_id)
        {
            return Ok(());
        }
        let mut planned = Vec::new();
        let mut deferred = Vec::new();
        for route in self.engine.routes_for(&event.source_account_id) {
            if !self.takes(route, event.action) {
                continue;
            }
            if matches!(
                event.action,
                TradeAction::Close | TradeAction::CancelPending
            ) {
                let queued_open = runtime.ledger.commands.values().find(|stored| {
                    stored.state == StoredCommandState::Queued
                        && stored.command.action.adds_exposure()
                        && Self::same_trade(&stored.command, &event, route)
                });
                if let Some(stored) = queued_open {
                    let ack = local_rejection(
                        &stored.command,
                        "source closed before follower order was dispatched",
                    );
                    self.handle_ack(&mut runtime, ack, AckOrigin::Local).await?;
                }
            }
            let ledger = &mut runtime.ledger;
            let has_earlier_event = Self::has_pending(ledger, &event, route)
                || ledger.deferred.iter().any(|pending| {
                    pending.route_id == route.id
                        && pending.event.source_order_id == event.source_order_id
                        && pending.event.action.is_pending() == event.action.is_pending()
                });
            if has_earlier_event && event.action.adds_exposure() {
                continue;
            }
            if ledger.commands.len() + ledger.deferred.len() + planned.len() + deferred.len()
                >= self.config.backlog_capacity
            {
                self.hold(
                    ledger,
                    &route.target_account_id,
                    Some("durable backlog capacity exceeded; inspect the source and follower"),
                )
                .await?;
                // A new open is dropped (the follower is held), but a change to an existing copy is kept: the event is about to be
                // marked as seen, and a close lost here would never be replayed. It waits in the deferred list until the hold clears.
                if !event.action.adds_exposure() {
                    deferred.push(DeferredEvent {
                        route_id: route.id.clone(),
                        event: event.clone(),
                    });
                }
            } else if has_earlier_event {
                deferred.push(DeferredEvent {
                    route_id: route.id.clone(),
                    event: event.clone(),
                });
            } else if let Some(command) = self.plan(ledger, &event, route)? {
                planned.push(command);
            }
        }
        let record = JournalRecord::EventPlanned {
            event_id,
            commands: planned,
            deferred,
            source_timestamp,
        };
        self.record(&mut runtime.ledger, record).await?;
        self.wake.notify_one();
        Ok(())
    }

    fn event_cutoff(&self) -> Option<i64> {
        let retention = self.config.event_retention_secs;
        (retention > 0).then(|| {
            unix_time_ns()
                .saturating_sub(retention.saturating_mul(1_000_000_000).min(i64::MAX as u64) as i64)
        })
    }

    /// Whether a route acts on an event of this kind: new exposure needs the route switched on (and listed in pending_routes for
    /// a pending placement); a change to exposure it already copied always goes through.
    fn takes(&self, route: &RouteRule, action: TradeAction) -> bool {
        !action.adds_exposure()
            || (route.enabled && (!action.is_pending() || self.config.pending_routes.contains(&route.id)))
    }

    fn same_trade(command: &ExecutionCommand, event: &TradeEvent, route: &RouteRule) -> bool {
        command.route_id == route.id
            && command.source_order_id == event.source_order_id
            && command.action.is_pending() == event.action.is_pending()
    }

    fn has_pending(ledger: &ReplayState, event: &TradeEvent, route: &RouteRule) -> bool {
        ledger
            .commands
            .values()
            .any(|stored| Self::same_trade(&stored.command, event, route))
    }

    fn plan(
        &self,
        ledger: &ReplayState,
        event: &TradeEvent,
        route: &RouteRule,
    ) -> Result<Option<ExecutionCommand>> {
        let key = MirrorBinding::key(
            &event.source_account_id,
            &event.source_order_id,
            &route.target_account_id,
        );
        let bindings = if event.action.is_pending() {
            &ledger.pending_mirrors
        } else {
            &ledger.mirrors
        };
        let mirror = bindings.get(&key);
        if event.action.adds_exposure() && mirror.is_some() {
            return Ok(None);
        }
        // Only the route that made a copy changes it (a disabled route and its replacement can share a source and follower).
        if mirror.is_some_and(|binding| binding.route_id != route.id) {
            return Ok(None);
        }
        match self
            .engine
            .build_command(event, route, mirror, unix_time_ns())
        {
            Ok(command) => Ok(command),
            Err(copier_core::RouteError::MissingMirror(_)) => {
                debug!(event_id = %event.event_id, route = %route.id, "source action has no copied position");
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn pump(&self) -> Result<()> {
        let mut runtime = self.runtime.lock().await;
        let now = unix_time_ns();
        let overdue: Vec<_> = runtime
            .ledger
            .commands
            .values()
            .filter(|stored| {
                stored.state == StoredCommandState::Dispatched
                    && stored
                        .dispatched_at
                        .is_some_and(|start| elapsed_ms(start, now) >= self.config.ack_timeout_ms)
            })
            .map(|stored| stored.command.command_id.clone())
            .collect();
        for command_id in overdue {
            self.mark_unknown(
                &mut runtime.ledger,
                command_id,
                "broker acknowledgement deadline exceeded",
            )
            .await?;
        }
        if runtime.ledger.paused {
            return Ok(());
        }

        let mut index = 0;
        while index < runtime.ledger.deferred.len() {
            let pending = &runtime.ledger.deferred[index];
            let route = self
                .engine
                .routes_for(&pending.event.source_account_id)
                .iter()
                .find(|route| route.id == pending.route_id && self.takes(route, pending.event.action));
            let command = match route {
                Some(route) if Self::has_pending(&runtime.ledger, &pending.event, route) => {
                    index += 1;
                    continue;
                }
                Some(route) => self.plan(&runtime.ledger, &pending.event, route)?,
                None => None,
            };
            let record = JournalRecord::DeferredPlanned {
                route_id: pending.route_id.clone(),
                event_id: pending.event.event_id.clone(),
                command: Box::new(command),
            };
            self.record(&mut runtime.ledger, record).await?;
        }

        let busy: HashSet<_> = runtime
            .ledger
            .commands
            .values()
            .filter(|stored| stored.state != StoredCommandState::Queued)
            .map(|stored| stored.command.target_account_id.as_str())
            .collect();
        let mut next = HashMap::<&str, &ExecutionCommand>::new();
        for stored in runtime.ledger.commands.values() {
            let command = &stored.command;
            let target = command.target_account_id.as_str();
            if stored.state == StoredCommandState::Queued
                && !busy.contains(target)
                && !runtime.ledger.holds.contains_key(target)
            {
                let oldest = next.entry(target).or_insert(command);
                if (command.created_unix_ns, &command.command_id)
                    < (oldest.created_unix_ns, &oldest.command_id)
                {
                    *oldest = command;
                }
            }
        }
        let dispatchable: Vec<_> = next.into_values().cloned().collect();
        for command in dispatchable {
            self.dispatch(&mut runtime, command, now).await?;
        }
        Ok(())
    }

    async fn dispatch(
        &self,
        runtime: &mut Runtime,
        command: ExecutionCommand,
        now: i64,
    ) -> Result<()> {
        let target = &command.target_account_id;
        if command.action.adds_exposure()
            && elapsed_ms(command.created_unix_ns, now) >= self.config.max_dispatch_age_ms
        {
            let ack = local_rejection(&command, "expired before dispatch");
            return self.handle_ack(runtime, ack, AckOrigin::Local).await;
        }
        if command.action.adds_exposure()
            && self
                .config
                .account(&command.source_account_id)
                .is_some_and(|account| account.backend != AccountBackend::Agent)
        {
            let positions = runtime
                .sessions
                .get(&command.source_account_id)
                .and_then(fresh_positions);
            if !positions.is_some_and(|positions| {
                positions.iter().any(|position| {
                    position.id == command.source_order_id
                        && position.pending == command.action.is_pending()
                        && (position.lots - command.source_volume).abs() <= 1e-8
                })
            }) {
                return Ok(());
            }
        }
        let configured_route = self
            .engine
            .routes_for(&command.source_account_id)
            .iter()
            .any(|route| {
                route.id == command.route_id
                    && route.enabled
                    && route.target_account_id == command.target_account_id
                    && (!command.action.is_pending()
                        || self.config.pending_routes.contains(&route.id))
            });
        // Only new exposure needs a route that is switched on; a change to an existing copy goes out whatever the route's state.
        if command.action.adds_exposure() && !configured_route {
            let ack = local_rejection(
                &command,
                "route is disabled or missing after a configuration change",
            );
            return self.handle_ack(runtime, ack, AckOrigin::Local).await;
        }
        let Some(session) = runtime.sessions.get(target) else {
            return Ok(());
        };
        let requires_snapshot = command.action.is_pending()
            || self.config.follower_risk.contains_key(target)
            || self
                .config
                .account(target)
                .is_some_and(|a| a.backend != AccountBackend::Agent)
            || session.snapshot_at.is_some();
        if !session.discrepancies.is_empty()
            || (requires_snapshot && fresh_positions(session).is_none())
        {
            return Ok(());
        }
        if command.action.adds_exposure() {
            if let Some(reason) = self.risk_rejection(session, &command) {
                return self
                    .handle_ack(runtime, local_rejection(&command, reason), AckOrigin::Local)
                    .await;
            }
        }
        let Ok(permit) = session.tx.clone().try_reserve_owned() else {
            return Ok(());
        };
        let dispatched_at = unix_time_ns();
        let record = JournalRecord::CommandDispatched {
            command_id: command.command_id.clone(),
            timestamp_unix_ns: dispatched_at,
        };
        self.record(&mut runtime.ledger, record).await?;
        runtime
            .dispatch_latency
            .observe(command.created_unix_ns, dispatched_at);
        permit.send(ServerFrame::Command(Box::new(command)));
        Ok(())
    }

    fn risk_rejection(
        &self,
        session: &Session,
        command: &ExecutionCommand,
    ) -> Option<&'static str> {
        let risk = self.config.follower_risk.get(&command.target_account_id)?;
        let Some(positions) = fresh_positions(session) else {
            return Some("follower risk requires an authoritative broker snapshot");
        };
        if risk
            .max_open_positions
            .is_some_and(|limit| positions.len() >= limit)
        {
            return Some("follower open-position limit reached");
        }
        if risk.max_total_lots.is_some_and(|limit| {
            command.volume + positions.iter().map(|position| position.lots).sum::<f64>() > limit
        }) {
            return Some("follower gross lots limit reached");
        }
        None
    }

    pub async fn observe_positions(
        &self,
        account_id: &str,
        mut positions: Vec<BrokerPosition>,
    ) -> Result<()> {
        positions.sort_unstable_by(|a, b| (a.pending, &a.id).cmp(&(b.pending, &b.id)));
        ensure!(
            positions
                .iter()
                .all(|p| !p.id.is_empty() && p.lots.is_finite() && p.lots > 0.0)
                && positions
                    .windows(2)
                    .all(|pair| (pair[0].pending, &pair[0].id) != (pair[1].pending, &pair[1].id)),
            "invalid broker position snapshot for {account_id}"
        );
        let mut runtime = self.runtime.lock().await;
        if runtime.ledger.commands.values().any(|stored| {
            stored.command.target_account_id == account_id
                && stored.state == StoredCommandState::Dispatched
        }) {
            return Ok(());
        }
        let discrepancies = compare_positions(&runtime.ledger, account_id, &positions);
        let session = runtime
            .sessions
            .get_mut(account_id)
            .ok_or_else(|| anyhow::anyhow!("account {account_id} has no active session"))?;
        if !discrepancies.is_empty() && session.discrepancies != discrepancies {
            warn!(
                account = account_id,
                ?discrepancies,
                "follower reconciliation required"
            );
        }
        let changed = session.positions.as_ref() != Some(&positions)
            || session.discrepancies != discrepancies;
        session.discrepancies = discrepancies;
        session.positions = Some(positions);
        session.snapshot_at = Some(Instant::now());
        if changed {
            self.wake.notify_one();
        }
        Ok(())
    }

    pub async fn observe_initial_positions(
        &self,
        account_id: &str,
        positions: Vec<BrokerPosition>,
    ) -> Result<()> {
        if !self
            .config
            .account(account_id)
            .is_some_and(|account| account.role.can_publish())
        {
            return self.observe_positions(account_id, positions).await;
        }
        let actual: HashMap<_, _> = positions
            .iter()
            .filter(|position| !position.pending)
            .map(|position| (position.id.as_str(), position.lots))
            .collect();
        let mut runtime = self.runtime.lock().await;
        let drift: Vec<_> = runtime
            .ledger
            .mirrors
            .values()
            .filter(|binding| binding.source_account_id == account_id)
            .filter(|binding| {
                !actual
                    .get(binding.source_order_id.as_str())
                    .is_some_and(|lots| (*lots - binding.source_remaining_volume).abs() <= 1e-8)
                    && !runtime.ledger.commands.values().any(|stored| {
                        stored.command.route_id == binding.route_id
                            && stored.command.source_order_id == binding.source_order_id
                            && !stored.command.action.is_pending()
                    })
            })
            .map(|binding| {
                (
                    binding.target_account_id.clone(),
                    binding.source_order_id.clone(),
                )
            })
            .collect();
        for (target, order) in drift {
            self.hold(
                &mut runtime.ledger,
                &target,
                Some(&format!(
                    "source {account_id} position {order} changed while disconnected"
                )),
            )
            .await?;
        }
        drop(runtime);
        self.observe_positions(account_id, positions).await
    }

    pub async fn source_drift(&self, account_id: &str, order_id: &str) -> Result<()> {
        let mut runtime = self.runtime.lock().await;
        let targets: Vec<_> = runtime
            .ledger
            .mirrors
            .values()
            .filter(|binding| {
                binding.source_account_id == account_id && binding.source_order_id == order_id
            })
            .map(|binding| binding.target_account_id.clone())
            .collect();
        for target in targets {
            self.hold(&mut runtime.ledger, &target, Some(&format!("source {account_id} position {order_id} changed in place and requires reconciliation"))).await?;
        }
        Ok(())
    }

    pub async fn set_paused(&self, paused: bool) -> Result<()> {
        self.record(
            &mut self.runtime.lock().await.ledger,
            JournalRecord::PauseChanged { paused },
        )
        .await?;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn set_hold(&self, account_id: &str, reason: Option<&str>) -> Result<()> {
        ensure!(self.config.account(account_id).is_some(), "unknown account");
        self.hold(&mut self.runtime.lock().await.ledger, account_id, reason)
            .await
    }

    /// A venue filled `command` as `target_order_id` but could not attach its stop loss / take profit. The copy is bound as filled
    /// (so the source's later changes and close still reach it) and stays flagged with a `protection_missing` alert until a stop
    /// update for it succeeds or it closes.
    pub async fn protection_missing(&self, command: &ExecutionCommand, target_order_id: &str, reason: &str) -> Result<()> {
        let mut runtime = self.runtime.lock().await;
        warn!(command_id = %command.command_id, account = %command.target_account_id, position = target_order_id, reason, "copy opened without its protection");
        let record = JournalRecord::ProtectionState {
            binding_key: MirrorBinding::key(&command.source_account_id, &command.source_order_id, &command.target_account_id),
            account_id: command.target_account_id.clone(),
            target_order_id: target_order_id.to_owned(),
            missing: Some(reason.chars().take(512).collect()),
        };
        self.record(&mut runtime.ledger, record).await
    }

    pub async fn resolve(&self, ack: ExecutionAck) -> Result<()> {
        ensure!(
            matches!(ack.status, AckStatus::Filled | AckStatus::Rejected),
            "resolution must be filled or rejected"
        );
        ensure!(
            ack.message
                .as_deref()
                .is_some_and(|reason| !reason.trim().is_empty()),
            "resolution requires an audit reason"
        );
        let mut runtime = self.runtime.lock().await;
        ensure!(
            runtime.ledger.paused,
            "pause dispatch before resolving a command"
        );
        let stored = runtime
            .ledger
            .commands
            .get(&ack.command_id)
            .ok_or_else(|| anyhow::anyhow!("command is not in the ledger"))?;
        ensure!(
            stored.state == StoredCommandState::Unknown
                || (stored.state == StoredCommandState::Queued
                    && ack.status == AckStatus::Rejected),
            "resolve an unknown outcome or reject a queued command"
        );
        if ack.status == AckStatus::Filled && stored.command.action.adds_exposure() {
            let id = ack
                .external_id
                .as_deref()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| anyhow::anyhow!("filled exposure requires an external broker ID"))?;
            if self
                .config
                .account(&ack.account_id)
                .is_some_and(|account| account.backend != AccountBackend::Agent)
            {
                let positions = runtime
                    .sessions
                    .get(&ack.account_id)
                    .and_then(fresh_positions)
                    .unwrap_or_default();
                ensure!(
                    positions.iter().any(|position| position.id == id
                        && position.pending == stored.command.action.is_pending()),
                    "filled exposure needs a matching ID in a fresh broker snapshot"
                );
            }
        }
        self.handle_ack(&mut runtime, ack, AckOrigin::Operator)
            .await
    }

    pub async fn review_mirror(&self, review: MirrorReview) -> Result<()> {
        ensure!(
            !review.reason.trim().is_empty() && !review.source_id.is_empty(),
            "review requires a reason and source ID"
        );
        let route = self
            .config
            .routes
            .iter()
            .find(|route| route.id == review.route_id)
            .ok_or_else(|| anyhow::anyhow!("unknown route"))?;
        let mut runtime = self.runtime.lock().await;
        ensure!(
            runtime.ledger.paused,
            "pause dispatch before reviewing a mirror"
        );
        ensure!(
            !runtime
                .ledger
                .commands
                .values()
                .any(|stored| stored.command.route_id == route.id
                    && stored.command.source_order_id == review.source_id
                    && stored.command.action.is_pending() == review.pending),
            "resolve or discard commands for this mirror first"
        );
        let positions = runtime
            .sessions
            .get(&route.target_account_id)
            .and_then(fresh_positions)
            .ok_or_else(|| anyhow::anyhow!("review requires a fresh follower snapshot"))?;
        let key = MirrorBinding::key(
            &route.source_account_id,
            &review.source_id,
            &route.target_account_id,
        );
        let bindings = if review.pending {
            &runtime.ledger.pending_mirrors
        } else {
            &runtime.ledger.mirrors
        };
        let previous = bindings.get(&key);
        let mutation = if let Some(target_id) = review.target_id {
            let broker = positions
                .iter()
                .find(|position| position.id == target_id && position.pending == review.pending)
                .ok_or_else(|| {
                    anyhow::anyhow!("reviewed target is absent from the follower snapshot")
                })?;
            ensure!(
                !bindings
                    .values()
                    .any(|other| other.target_account_id == route.target_account_id
                        && other.target_order_id == target_id
                        && other.binding_key() != key),
                "target already belongs to another mirror"
            );
            let source_lots = review
                .source_lots
                .filter(|lots| lots.is_finite() && *lots > 0.0)
                .ok_or_else(|| anyhow::anyhow!("review requires positive source lots"))?;
            let binding = MirrorBinding {
                route_id: route.id.clone(),
                source_account_id: route.source_account_id.clone(),
                source_order_id: review.source_id,
                target_account_id: route.target_account_id.clone(),
                target_order_id: target_id,
                source_open_volume: source_lots,
                source_remaining_volume: source_lots,
                target_open_volume: broker.lots,
                target_remaining_volume: broker.lots,
            };
            if review.pending {
                MirrorMutation::UpsertPending { binding }
            } else {
                MirrorMutation::Upsert { binding }
            }
        } else {
            let previous =
                previous.ok_or_else(|| anyhow::anyhow!("mirror is absent from the ledger"))?;
            ensure!(
                !positions
                    .iter()
                    .any(|position| position.pending == review.pending
                        && position.id == previous.target_order_id),
                "cannot remove a mirror whose target exposure still exists"
            );
            if review.pending {
                MirrorMutation::RemovePending { binding_key: key }
            } else {
                MirrorMutation::Remove { binding_key: key }
            }
        };
        self.record(
            &mut runtime.ledger,
            JournalRecord::MirrorReviewed {
                mutation,
                reason: review.reason.chars().take(512).collect(),
                timestamp_unix_ns: unix_time_ns(),
            },
        )
        .await?;
        let positions = runtime
            .sessions
            .get(&route.target_account_id)
            .and_then(fresh_positions)
            .unwrap_or_default();
        let discrepancies = compare_positions(&runtime.ledger, &route.target_account_id, positions);
        if let Some(session) = runtime.sessions.get_mut(&route.target_account_id) {
            session.discrepancies = discrepancies;
        }
        Ok(())
    }

    pub async fn status(&self) -> serde_json::Value {
        let runtime = self.runtime.lock().await;
        let accounts: Vec<_> = self.config.accounts.iter().map(|account| {
            let session = runtime.sessions.get(&account.id);
            serde_json::json!({
                "account_id": account.id,
                "connected": session.is_some(),
                "snapshot_ready": session.and_then(fresh_positions).is_some(),
                "exposure": session.and_then(fresh_positions),
                "discrepancies": session.map(|session| session.discrepancies.as_slice()).unwrap_or_default(),
                "hold": runtime.ledger.holds.get(&account.id),
            })
        }).collect();
        let unresolved: Vec<_> = runtime.ledger.commands.values().filter(|stored| stored.state == StoredCommandState::Unknown)
            .map(|stored| serde_json::json!({"command": stored.command, "reason": stored.unknown_reason})).collect();
        let queued: Vec<_> = runtime
            .ledger
            .commands
            .values()
            .filter(|stored| stored.state == StoredCommandState::Queued)
            .map(|stored| &stored.command)
            .collect();
        let in_flight: Vec<_> = runtime.ledger.commands.values().filter(|stored| stored.state == StoredCommandState::Dispatched)
            .map(|stored| serde_json::json!({"command": stored.command, "dispatched_at": stored.dispatched_at})).collect();
        serde_json::json!({
            "paused": runtime.ledger.paused,
            "accounts": accounts,
            "queued": queued.len(),
            "queued_commands": queued,
            "in_flight": in_flight,
            "deferred": runtime.ledger.deferred.len(),
            "mirrors": runtime.ledger.mirrors.len(),
            "pending_mirrors": runtime.ledger.pending_mirrors.len(),
            "unknown": unresolved,
            "recent_failures": runtime.ledger.recent_failures,
            "recent_reviews": runtime.ledger.recent_reviews,
            "unprotected": runtime.ledger.unprotected.values().collect::<Vec<_>>(),
            "alerts": self.collect_alerts(&runtime),
            "latency_bounds_ms": Latency::BOUNDS_MS,
            "queue_to_dispatch": runtime.dispatch_latency,
            "dispatch_to_ack": runtime.acknowledgement_latency,
        })
    }

    pub async fn alerts(&self) -> Vec<Alert> {
        self.collect_alerts(&*self.runtime.lock().await)
    }

    fn collect_alerts(&self, runtime: &Runtime) -> Vec<Alert> {
        let mut alerts = Vec::new();
        for account in &self.config.accounts {
            let kind = match runtime.sessions.get(&account.id) {
                None => Some("account_disconnected"),
                Some(session) if !session.discrepancies.is_empty() => {
                    Some("reconciliation_required")
                }
                Some(session)
                    if (account.backend != AccountBackend::Agent
                        || self.config.follower_risk.contains_key(&account.id))
                        && session
                            .snapshot_at
                            .map_or(true, |at| at.elapsed() > Duration::from_secs(15)) =>
                {
                    Some("snapshot_unavailable")
                }
                _ => None,
            };
            if let Some(kind) = kind {
                alerts.push(Alert {
                    kind,
                    account_id: account.id.clone(),
                    command_id: None,
                });
            }
            if runtime.ledger.holds.contains_key(&account.id) {
                alerts.push(Alert {
                    kind: "dispatch_held",
                    account_id: account.id.clone(),
                    command_id: None,
                });
            }
        }
        alerts.extend(
            runtime
                .ledger
                .commands
                .values()
                .filter(|stored| stored.state == StoredCommandState::Unknown)
                .map(|stored| Alert {
                    kind: "execution_unknown",
                    account_id: stored.command.target_account_id.clone(),
                    command_id: Some(stored.command.command_id.clone()),
                }),
        );
        alerts.extend(runtime.ledger.unprotected.values().map(|copy| Alert {
            kind: "protection_missing",
            account_id: copy.account_id.clone(),
            command_id: None,
        }));
        let now = unix_time_ns();
        alerts.extend(
            runtime
                .ledger
                .recent_failures
                .iter()
                .filter(|ack| {
                    ack.status == AckStatus::Rejected
                        && elapsed_ms(ack.timestamp_unix_ns, now) <= 300_000
                })
                .map(|ack| Alert {
                    kind: "execution_rejected",
                    account_id: ack.account_id.clone(),
                    command_id: Some(ack.command_id.clone()),
                }),
        );
        alerts.sort_unstable();
        alerts.dedup();
        alerts
    }

    pub async fn maintain(&self) -> Result<()> {
        let checkpoint = Duration::from_secs(self.config.checkpoint_interval_secs);
        let mut last_checkpoint = Instant::now();
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = interval.tick() => {}, _ = self.wake.notified() => {} }
            self.pump().await?;
            if !checkpoint.is_zero() && last_checkpoint.elapsed() >= checkpoint {
                let mut runtime = self.runtime.lock().await;
                if let Some(cutoff) = self.event_cutoff() {
                    runtime.ledger.prune_events_before(cutoff);
                }
                self.journal.compact(&runtime.ledger).await?;
                last_checkpoint = Instant::now();
                info!("execution journal checkpointed");
            }
        }
    }

    async fn hold(
        &self,
        ledger: &mut ReplayState,
        account_id: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        if ledger.holds.get(account_id).map(String::as_str) != reason {
            if let Some(reason) = reason {
                warn!(account = account_id, reason, "follower dispatch held");
            }
            self.record(
                ledger,
                JournalRecord::AccountHold {
                    account_id: account_id.to_owned(),
                    reason: reason.map(str::to_owned),
                },
            )
            .await?;
        }
        Ok(())
    }

    async fn mark_unknown(
        &self,
        ledger: &mut ReplayState,
        command_id: String,
        reason: &str,
    ) -> Result<()> {
        warn!(%command_id, reason, "command outcome is unknown");
        self.record(
            ledger,
            JournalRecord::CommandUnknown {
                command_id,
                timestamp_unix_ns: unix_time_ns(),
                reason: reason.to_owned(),
            },
        )
        .await
    }

    async fn handle_ack(
        &self,
        runtime: &mut Runtime,
        mut ack: ExecutionAck,
        origin: AckOrigin,
    ) -> Result<()> {
        let Some(stored) = runtime.ledger.commands.get(&ack.command_id) else {
            debug!(command_id = %ack.command_id, "duplicate or late acknowledgement ignored");
            return Ok(());
        };
        let command = stored.command.clone();
        let dispatched_at = stored.dispatched_at;
        ensure!(
            command.target_account_id == ack.account_id,
            "ack target does not match command target"
        );
        ensure!(
            origin != AckOrigin::Venue || stored.state != StoredCommandState::Queued,
            "unsent command cannot be acknowledged"
        );
        ack.timestamp_unix_ns = unix_time_ns();
        if matches!(ack.status, AckStatus::Accepted | AckStatus::Filled)
            && command.action.adds_exposure()
            && ack.external_id.as_deref().unwrap_or("").is_empty()
        {
            ack.status = AckStatus::Unknown;
            ack.message = Some("acknowledgement omitted external broker ID".to_owned());
        }
        // Avoid persisting vendor response bodies or unbounded external messages.
        ack.message = ack.message.map(|reason| reason.chars().take(512).collect());
        let mirror = if matches!(ack.status, AckStatus::Accepted | AckStatus::Filled) {
            mirror_mutation(&runtime.ledger, &command, &ack)
        } else {
            None
        };
        let requires_refresh = mirror.is_some();
        if matches!(ack.status, AckStatus::Rejected | AckStatus::Unknown) {
            warn!(command_id = %ack.command_id, account = %ack.account_id, status = %ack.status, "execution requires inspection");
            if ack.status == AckStatus::Rejected && !command.action.adds_exposure() {
                self.hold(
                    &mut runtime.ledger,
                    &ack.account_id,
                    Some("follower rejected a position change; reconcile with the source"),
                )
                .await?;
            }
        }
        self.record(
            &mut runtime.ledger,
            JournalRecord::AckApplied {
                ack: ack.clone(),
                mirror: Box::new(mirror),
                operator: origin == AckOrigin::Operator,
            },
        )
        .await?;
        let binding_key = MirrorBinding::key(&command.source_account_id, &command.source_order_id, &command.target_account_id);
        if command.action == TradeAction::Modify
            && ack.status == AckStatus::Filled
            && runtime.ledger.unprotected.contains_key(&binding_key)
        {
            let target_order_id = command.target_order_id.clone().unwrap_or_default();
            self.record(
                &mut runtime.ledger,
                JournalRecord::ProtectionState { binding_key, account_id: ack.account_id.clone(), target_order_id, missing: None },
            )
            .await?;
        }
        if let Some(start) = dispatched_at {
            runtime
                .acknowledgement_latency
                .observe(start, unix_time_ns());
        }
        if requires_refresh {
            if let Some(session) = runtime.sessions.get_mut(&ack.account_id) {
                session.positions = None;
            }
        }
        self.wake.notify_one();
        Ok(())
    }

    async fn record(&self, ledger: &mut ReplayState, record: JournalRecord) -> Result<()> {
        if let Err(error) = self.journal.append(&record).await {
            error!(%error, "execution journal write failed; dispatch stopped");
            return Err(error);
        }
        ledger.apply(&record);
        Ok(())
    }
}

fn mirror_mutation(
    ledger: &ReplayState,
    command: &ExecutionCommand,
    ack: &ExecutionAck,
) -> Option<MirrorMutation> {
    let key = MirrorBinding::key(
        &command.source_account_id,
        &command.source_order_id,
        &command.target_account_id,
    );
    match command.action {
        TradeAction::Open | TradeAction::PlaceLimit | TradeAction::PlaceStop => {
            let binding = MirrorBinding {
                route_id: command.route_id.clone(),
                source_account_id: command.source_account_id.clone(),
                source_order_id: command.source_order_id.clone(),
                target_account_id: command.target_account_id.clone(),
                target_order_id: ack.external_id.clone().expect("validated position ID"),
                source_open_volume: command.source_volume,
                source_remaining_volume: command
                    .source_remaining_volume
                    .unwrap_or(command.source_volume),
                target_open_volume: command.volume,
                target_remaining_volume: command.volume,
            };
            Some(if command.action.is_pending() {
                MirrorMutation::UpsertPending { binding }
            } else {
                MirrorMutation::Upsert { binding }
            })
        }
        TradeAction::Modify | TradeAction::AmendPending => None,
        TradeAction::Reduce => {
            let mut binding = ledger.mirrors.get(&key)?.clone();
            binding.source_remaining_volume =
                command.source_remaining_volume.unwrap_or_else(|| {
                    (binding.source_remaining_volume - command.source_volume).max(0.0)
                });
            binding.target_remaining_volume =
                (binding.target_remaining_volume - command.volume).max(0.0);
            Some(MirrorMutation::Upsert { binding })
        }
        TradeAction::Close => Some(MirrorMutation::Remove { binding_key: key }),
        TradeAction::CancelPending => Some(MirrorMutation::RemovePending { binding_key: key }),
    }
}

fn compare_positions(
    ledger: &ReplayState,
    account_id: &str,
    actual: &[BrokerPosition],
) -> Vec<String> {
    let mut expected = HashMap::new();
    let mut problems = Vec::new();
    let bindings = ledger
        .mirrors
        .values()
        .map(|binding| (false, binding))
        .chain(
            ledger
                .pending_mirrors
                .values()
                .map(|binding| (true, binding)),
        );
    for (pending, binding) in
        bindings.filter(|(_, binding)| binding.target_account_id == account_id)
    {
        if expected
            .insert(
                (pending, binding.target_order_id.as_str()),
                binding.target_remaining_volume,
            )
            .is_some()
        {
            problems.push(format!(
                "multiple mirrors share position {}",
                binding.target_order_id
            ));
        }
    }
    for position in actual {
        match expected.remove(&(position.pending, position.id.as_str())) {
            Some(lots) if (lots - position.lots).abs() > 0.000_000_01 => {
                problems.push(format!(
                    "position {}: expected {lots} lots, broker reports {}",
                    position.id, position.lots
                ));
            }
            None if position.copier_managed => {
                problems.push(format!("untracked CopierR position {}", position.id))
            }
            _ => {}
        }
    }
    problems.extend(expected.into_keys().map(|(pending, id)| {
        let kind = if pending { "pending order" } else { "position" };
        format!("expected {kind} {id} absent at broker")
    }));
    problems.sort();
    problems
}

fn fresh_positions(session: &Session) -> Option<&[BrokerPosition]> {
    session
        .snapshot_at
        .filter(|at| at.elapsed() <= Duration::from_secs(15))?;
    session.positions.as_deref()
}

fn local_rejection(command: &ExecutionCommand, reason: &str) -> ExecutionAck {
    ExecutionAck {
        command_id: command.command_id.clone(),
        account_id: command.target_account_id.clone(),
        status: AckStatus::Rejected,
        external_id: None,
        timestamp_unix_ns: unix_time_ns(),
        message: Some(reason.to_owned()),
    }
}

pub fn token_matches(expected: &str, supplied: &str) -> bool {
    let expected = Sha256::digest(expected.as_bytes());
    let supplied = Sha256::digest(supplied.as_bytes());
    expected
        .iter()
        .zip(supplied.iter())
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

fn elapsed_ms(start: i64, end: i64) -> u64 {
    (end.saturating_sub(start).max(0) as u64) / 1_000_000
}

pub fn unix_time_ns() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_nanos().min(i64::MAX as u128) as i64
}
