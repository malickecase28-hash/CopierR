use crate::{
    config::{read_secret, AccountConfig, CTraderEnvironment},
    runtime::{unix_time_ns, AppState},
};
use anyhow::{bail, Context, Result};
use copier_core::{AckStatus, AgentFrame, BrokerPosition, ExecutionAck, ExecutionCommand, ServerFrame, Side, TradeAction, TradeEvent};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::mpsc, time::{interval, timeout, Duration, Instant, MissedTickBehavior}};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

const APPLICATION_AUTH_REQ: i64 = 2100;
const APPLICATION_AUTH_RES: i64 = 2101;
const ACCOUNT_AUTH_REQ: i64 = 2102;
const ACCOUNT_AUTH_RES: i64 = 2103;
const NEW_ORDER_REQ: i64 = 2106;
const CANCEL_ORDER_REQ: i64 = 2108;
const AMEND_ORDER_REQ: i64 = 2109;
const AMEND_POSITION_SLTP_REQ: i64 = 2110;
const CLOSE_POSITION_REQ: i64 = 2111;
const SYMBOLS_LIST_REQ: i64 = 2114;
const SYMBOLS_LIST_RES: i64 = 2115;
const SYMBOL_BY_ID_REQ: i64 = 2116;
const SYMBOL_BY_ID_RES: i64 = 2117;
const RECONCILE_REQ: i64 = 2124;
const RECONCILE_RES: i64 = 2125;
const EXECUTION_EVENT: i64 = 2126;
const ORDER_ERROR_EVENT: i64 = 2132;
const ERROR_RES: i64 = 2142;
const ACCOUNTS_TOKEN_INVALIDATED_EVENT: i64 = 2147;
const CLIENT_DISCONNECT_EVENT: i64 = 2148;
const ACCOUNT_DISCONNECT_EVENT: i64 = 2164;
const HEARTBEAT_EVENT: i64 = 51;

#[derive(Debug, Clone)]
struct HubAccount {
    local: AccountConfig,
    ctid: i64,
    access_token: String,
}

#[derive(Debug, Clone)]
struct SymbolInfo {
    id: i64,
    name: String,
    lot_size_cents: f64,
    min_units: i64,
    max_units: i64,
    step_units: i64,
}

impl SymbolInfo {
    fn units(&self, lots: f64) -> Result<i64> {
        let raw = lots * self.lot_size_cents;
        let rounded = raw.round();
        anyhow::ensure!(raw.is_finite() && rounded >= 1.0 && rounded < i64::MAX as f64 &&
            (rounded - raw).abs() <= 1e-6, "cTrader volume cannot be represented exactly in broker units");
        Ok(rounded as i64)
    }

    fn order_units(&self, lots: f64) -> Result<i64> {
        let units = self.units(lots)?;
        anyhow::ensure!((self.min_units..=self.max_units).contains(&units) && units % self.step_units == 0,
            "cTrader volume is outside the broker's minimum, maximum or step");
        Ok(units)
    }
}

#[derive(Debug, Clone)]
struct PositionSnapshot {
    id: String,
    symbol: String,
    side: Side,
    volume_lots: f64,
    price: Option<f64>,
    stop_loss: Option<f64>,
    take_profit: Option<f64>,
    origin_command_id: Option<String>,
}

struct AccountRuntime {
    account: HubAccount,
    session_id: u64,
    symbols_by_name: HashMap<String, SymbolInfo>,
    symbols_by_id: HashMap<i64, SymbolInfo>,
    positions: HashMap<String, PositionSnapshot>,
    position_revision: u64,
    initial_orders: Vec<BrokerPosition>,
    /// Pending orders as of the last reconciliation, published with the positions kept current from execution events.
    orders: Vec<BrokerPosition>,
    order_triggers: HashMap<String, &'static str>,
}

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub fn spawn_hub(
    state: Arc<AppState>,
    environment: CTraderEnvironment,
    accounts: Vec<AccountConfig>,
) -> Result<()> {
    let global = state.config.ctrader.as_ref().expect("validated cTrader global config").clone();
    let client_id = read_secret(&global.client_id_env)?;
    let client_secret = read_secret(&global.client_secret_env)?;
    let mut prepared = Vec::with_capacity(accounts.len());
    for local in accounts {
        let direct = local.ctrader.as_ref().expect("validated cTrader account config");
        let ctid = direct.ctid_trader_account_id;
        let access_token_env = direct.access_token_env.clone();
        let access_token = read_secret(&access_token_env)?;
        prepared.push(HubAccount {
            local,
            ctid,
            access_token,
        });
    }
    let url = match environment {
        CTraderEnvironment::Live => global.live_url,
        CTraderEnvironment::Demo => global.demo_url,
    };
    tokio::spawn(async move {
        let mut delay = super::ReconnectDelay::default();
        loop {
            let started = Instant::now();
            let result = run_hub(state.clone(), environment, url.clone(), client_id.clone(), client_secret.clone(), prepared.clone()).await;
            warn!(?environment, error = ?result.err(), "cTrader Open API hub disconnected; reconnecting");
            delay.wait(started.elapsed()).await;
        }
    });
    Ok(())
}

async fn run_hub(
    state: Arc<AppState>,
    environment: CTraderEnvironment,
    url: String,
    client_id: String,
    client_secret: String,
    accounts: Vec<HubAccount>,
) -> Result<()> {
    let (mut ws, _) = timeout(Duration::from_secs(15), connect_async(&url)).await?
        .with_context(|| format!("failed connecting cTrader Open API {url}"))?;
    send_json(&mut ws, APPLICATION_AUTH_REQ, "app-auth", json!({
        "clientId": client_id,
        "clientSecret": client_secret
    })).await?;
    wait_for(&mut ws, APPLICATION_AUTH_RES, None).await?;

    let mut runtimes = HashMap::<i64, AccountRuntime>::new();
    let (command_tx, mut command_rx) = mpsc::channel::<ExecutionCommand>(state.config.queue_capacity.saturating_mul(2));

    let result: Result<()> = async {
    for prepared in accounts {
        let auth_id = format!("auth:{}", prepared.ctid);
        send_json(&mut ws, ACCOUNT_AUTH_REQ, &auth_id, json!({
            "ctidTraderAccountId": prepared.ctid,
            "accessToken": prepared.access_token
        })).await?;
        wait_for(&mut ws, ACCOUNT_AUTH_RES, Some(prepared.ctid)).await?;

        let symbols_id = format!("symbols:{}", prepared.ctid);
        send_json(&mut ws, SYMBOLS_LIST_REQ, &symbols_id, json!({
            "ctidTraderAccountId": prepared.ctid,
            "includeArchivedSymbols": false
        })).await?;
        let light = wait_for(&mut ws, SYMBOLS_LIST_RES, Some(prepared.ctid)).await?;
        let light_symbols = parse_light_symbols(&light)?;
        let symbol_ids: Vec<i64> = light_symbols.iter().map(|(id, _)| *id).collect();

        send_json(&mut ws, SYMBOL_BY_ID_REQ, &format!("symbol-details:{}", prepared.ctid), json!({
            "ctidTraderAccountId": prepared.ctid,
            "symbolId": symbol_ids
        })).await?;
        let details = wait_for(&mut ws, SYMBOL_BY_ID_RES, Some(prepared.ctid)).await?;
        let (symbols_by_name, symbols_by_id) = parse_symbol_details(&light_symbols, &details)?;

        send_json(&mut ws, RECONCILE_REQ, &format!("reconcile:{}", prepared.ctid), json!({
            "ctidTraderAccountId": prepared.ctid,
            "returnProtectionOrders": false
        })).await?;
        let reconcile = wait_for(&mut ws, RECONCILE_RES, Some(prepared.ctid)).await?;
        let positions = parse_reconcile_positions(&reconcile, &symbols_by_id)?;
        let (initial_orders, order_triggers) = parse_reconcile_orders(&reconcile, &symbols_by_id)?;

        let local_id = prepared.local.id.clone();
        let (tx, mut rx) = mpsc::channel(state.config.queue_capacity);
        let session_id = state.register_session(local_id.clone(), tx).await?;
        let forward = command_tx.clone();
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                if let ServerFrame::Command(command) = frame {
                    if forward.send(*command).await.is_err() {
                        break;
                    }
                }
            }
        });
        runtimes.insert(prepared.ctid, AccountRuntime {
            account: prepared,
            session_id,
            symbols_by_name,
            symbols_by_id,
            positions,
            position_revision: 0,
            orders: initial_orders.clone(),
            initial_orders,
            order_triggers,
        });
    }

    for runtime in runtimes.values_mut() {
        let local_id = &runtime.account.local.id;
        let mut positions = broker_positions(&runtime.positions);
        positions.append(&mut runtime.initial_orders);
        state.observe_initial_positions(local_id, positions).await?;
        info!(account = %local_id, ctid = runtime.account.ctid, ?environment, "cTrader Open API account connected");
    }
    let mut pending = HashMap::<String, ExecutionCommand>::new();
    let mut protection_pending = HashMap::<String, (ExecutionCommand, String)>::new();
    let mut heartbeat = interval(Duration::from_secs(10));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let mut reconcile_tick = interval(Duration::from_secs(3));
    reconcile_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    reconcile_tick.tick().await;
    let mut reconcile_pending = HashMap::<i64, (String, Instant, u64)>::new();
    let mut reconcile_sequence = 0u64;
    let mut last_message = Instant::now();

        loop {
            tokio::select! {
                maybe_command = command_rx.recv() => {
                    let Some(command) = maybe_command else { break; };
                    let Some(runtime) = runtimes.values().find(|runtime| runtime.account.local.id == command.target_account_id) else {
                        continue;
                    };
                    match command_message(runtime, &command) {
                        Ok((payload_type, payload)) => {
                            send_json(&mut ws, payload_type, &command.command_id, payload).await?;
                            pending.insert(command.command_id.clone(), command);
                        }
                        Err(err) => {
                            let ack = ExecutionAck {
                                command_id: command.command_id,
                                account_id: command.target_account_id,
                                status: AckStatus::Rejected,
                                external_id: None,
                                timestamp_unix_ns: unix_time_ns(),
                                message: Some(err.to_string()),
                            };
                            state.handle_frame(&ack.account_id.clone(), AgentFrame::Ack(ack)).await?;
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    anyhow::ensure!(last_message.elapsed() < Duration::from_secs(35), "cTrader heartbeat timed out");
                    send_json(&mut ws, HEARTBEAT_EVENT, "heartbeat", json!({})).await?;
                }
                _ = reconcile_tick.tick() => {
                    for runtime in runtimes.values() {
                        let ctid = runtime.account.ctid;
                        if let Some((_, started, _)) = reconcile_pending.get(&ctid) {
                            anyhow::ensure!(started.elapsed() < Duration::from_secs(15), "cTrader reconciliation timed out for account {ctid}");
                            continue;
                        }
                        reconcile_sequence += 1;
                        let request_id = format!("reconcile:{ctid}:{reconcile_sequence}");
                        send_json(&mut ws, RECONCILE_REQ, &request_id, json!({
                            "ctidTraderAccountId": ctid,
                            "returnProtectionOrders": false
                        })).await?;
                        reconcile_pending.insert(ctid, (request_id, Instant::now(), runtime.position_revision));
                    }
                }
                message = ws.next() => {
                    let Some(message) = message else { break; };
                    last_message = Instant::now();
                    let value = parse_ws_message(message?)?;
                    let Some(value) = value else { continue; };
                    if value.get("payloadType").and_then(as_i64) == Some(RECONCILE_RES) {
                        let ctid = value.get("payload").and_then(|payload| payload.get("ctidTraderAccountId")).and_then(as_i64)
                            .context("cTrader reconcile response omitted its account")?;
                        if let Some((request, _, _)) = reconcile_pending.get(&ctid) {
                            if value.get("clientMsgId").and_then(Value::as_str) == Some(request) {
                                let (_, _, revision) = reconcile_pending.remove(&ctid).expect("matched reconciliation");
                                let runtime = runtimes.get_mut(&ctid).context("unknown cTrader reconciliation account")?;
                                if revision != runtime.position_revision { continue; }
                                let positions = parse_reconcile_positions(&value, &runtime.symbols_by_id)?;
                                let mut exposure = broker_positions(&positions);
                                let (orders, triggers) = parse_reconcile_orders(&value, &runtime.symbols_by_id)?;
                                runtime.order_triggers = triggers;
                                runtime.orders = orders.clone();
                                exposure.extend(orders);
                                reconcile_source_positions(&state, runtime, positions).await?;
                                state.observe_positions(&runtime.account.local.id, exposure).await?;
                            }
                        }
                        continue;
                    }
                    handle_message(&state, &mut ws, &mut runtimes, &mut pending, &mut protection_pending, value).await?;
                }
            }
        }
        Ok(())
    }.await;

    for runtime in runtimes.values() {
        state.unregister_session(&runtime.account.local.id, runtime.session_id).await?;
    }
    result
}

fn broker_positions(positions: &HashMap<String, PositionSnapshot>) -> Vec<BrokerPosition> {
    positions.values().map(|position| BrokerPosition {
        id: position.id.clone(), lots: position.volume_lots,
        copier_managed: position.origin_command_id.is_some(),
        pending: false,
    }).collect()
}

async fn handle_message(
    state: &Arc<AppState>,
    ws: &mut Ws,
    runtimes: &mut HashMap<i64, AccountRuntime>,
    pending: &mut HashMap<String, ExecutionCommand>,
    protection_pending: &mut HashMap<String, (ExecutionCommand, String)>,
    value: Value,
) -> Result<()> {
    let payload_type = value.get("payloadType").and_then(Value::as_i64).unwrap_or_default();
    let client_msg_id = value.get("clientMsgId").and_then(Value::as_str).unwrap_or("").to_owned();
    let payload = value.get("payload").cloned().unwrap_or_else(|| json!({}));
    if matches!(payload_type, CLIENT_DISCONNECT_EVENT | ACCOUNT_DISCONNECT_EVENT | ACCOUNTS_TOKEN_INVALIDATED_EVENT) {
        bail!("cTrader ended the broker session ({payload_type})");
    }

    if payload_type == EXECUTION_EVENT {
        let ctid = payload.get("ctidTraderAccountId").and_then(as_i64)
            .ok_or_else(|| anyhow::anyhow!("cTrader execution event omitted account id"))?;
        let execution_type = enum_number(payload.get("executionType"), &[
            ("ORDER_ACCEPTED", 2), ("ORDER_FILLED", 3), ("ORDER_REPLACED", 4),
            ("ORDER_CANCELLED", 5), ("ORDER_EXPIRED", 6), ("ORDER_REJECTED", 7),
            ("ORDER_CANCEL_REJECTED", 8), ("ORDER_PARTIAL_FILL", 11),
        ]).unwrap_or_default();

        // ORDER_ACCEPTED and ORDER_PARTIAL_FILL are non-final. Keep the original
        // durable command outstanding until cTrader reports a final outcome.
        let pending_order = pending.get(&client_msg_id).is_some_and(|command| command.action.is_pending());
        // An SL/TP change on a position (a copied Modify, or the protection sent after a copied open) is answered by cTrader
        // with ORDER_ACCEPTED for the protective order, and nothing more: that answer is final for it. Treating it as
        // non-final left the command outstanding forever, and every later change and the close waited behind it (25 Nov 2026).
        let sltp_request = protection_pending.contains_key(&client_msg_id)
            || pending.get(&client_msg_id).is_some_and(|command| command.action == TradeAction::Modify);
        if sltp_request {
            info!(client_msg_id, execution_type, "cTrader answered an SL/TP request");
        }
        if matches!(execution_type, 2 | 11) && !pending_order && !sltp_request {
            if let Some(runtime) = runtimes.get_mut(&ctid) {
                apply_execution(state, runtime, &payload, execution_type).await?;
            }
            return Ok(());
        }

        if let Some((original, external_id)) = protection_pending.remove(&client_msg_id) {
            // The order is filled either way; only its SL/TP may be missing (see protection_finished).
            let failure = (!matches!(execution_type, 2 | 3 | 4))
                .then(|| format!("cTrader SL/TP request ended with execution type {execution_type}"));
            protection_finished(state, &original, external_id, failure).await?;
        } else if let Some(command) = pending.remove(&client_msg_id) {
            let external_id = if pending_order { execution_order_id(&payload) } else { execution_position_id(&payload) }
                .or_else(|| command.target_order_id.clone());
            if command.action == TradeAction::CancelPending && execution_type == 2 {
                pending.insert(client_msg_id, command);
            } else if command.action == TradeAction::Open
                && matches!(execution_type, 3 | 4)
                && (command.stop_loss.is_some() || command.take_profit.is_some())
            {
                if let Some(position_id) = external_id.clone() {
                    let protection_id = format!("p:{}", command.command_id);
                    let ctid = runtimes.values().find(|runtime| runtime.account.local.id == command.target_account_id)
                        .map(|runtime| runtime.account.ctid)
                        .ok_or_else(|| anyhow::anyhow!("missing cTrader runtime for protection"))?;
                    let mut protection = json!({
                        "ctidTraderAccountId": ctid,
                        "positionId": position_id.parse::<i64>().context("invalid cTrader position id")?
                    });
                    if let Some(stop_loss) = command.stop_loss { protection["stopLoss"] = json!(stop_loss); }
                    if let Some(take_profit) = command.take_profit { protection["takeProfit"] = json!(take_profit); }
                    send_json(ws, AMEND_POSITION_SLTP_REQ, &protection_id, protection).await?;
                    protection_pending.insert(protection_id, (command, position_id));
                } else {
                    let ack = unknown_ack(&command, "cTrader fill omitted position id");
                    state.handle_frame(&command.target_account_id, AgentFrame::Ack(ack)).await?;
                }
            } else {
                let status = match (command.action, execution_type) {
                    (TradeAction::PlaceLimit | TradeAction::PlaceStop, 2) => AckStatus::Accepted,
                    (TradeAction::AmendPending, 4) | (TradeAction::CancelPending, 5) => AckStatus::Filled,
                    (TradeAction::Modify, 2 | 3 | 4) => AckStatus::Filled,
                    (_, 7 | 8) => AckStatus::Rejected,
                    (action, 3 | 4) if !action.is_pending() => AckStatus::Filled,
                    (action, 5 | 6) if !action.is_pending() => AckStatus::Unknown,
                    _ => AckStatus::Unknown,
                };
                let ack = ExecutionAck {
                    command_id: command.command_id.clone(),
                    account_id: command.target_account_id.clone(),
                    status,
                    external_id,
                    timestamp_unix_ns: unix_time_ns(),
                    message: payload.get("errorCode").and_then(Value::as_str).map(str::to_owned),
                };
                state.handle_frame(&command.target_account_id, AgentFrame::Ack(ack)).await?;
            }
        }

        if let Some(runtime) = runtimes.get_mut(&ctid) {
            apply_execution(state, runtime, &payload, execution_type).await?;
        }
        return Ok(());
    }

    if payload_type == ORDER_ERROR_EVENT || payload_type == ERROR_RES {
        if let Some(command) = pending.remove(&client_msg_id) {
            let message = payload.get("description").or_else(|| payload.get("errorCode"))
                .and_then(Value::as_str).unwrap_or("cTrader rejected request").to_owned();
            let ack = ExecutionAck {
                command_id: command.command_id.clone(),
                account_id: command.target_account_id.clone(),
                status: AckStatus::Rejected,
                external_id: None,
                timestamp_unix_ns: unix_time_ns(),
                message: Some(message),
            };
            state.handle_frame(&command.target_account_id, AgentFrame::Ack(ack)).await?;
        } else if let Some((command, external_id)) = protection_pending.remove(&client_msg_id) {
            let reason = payload.get("description").or_else(|| payload.get("errorCode"))
                .and_then(Value::as_str).unwrap_or("cTrader refused the SL/TP request");
            protection_finished(state, &command, external_id, Some(format!("cTrader SL/TP request refused: {reason}"))).await?;
        } else {
            warn!(payload_type, client_msg_id, payload = %payload, "unmatched cTrader error event");
        }
    }
    Ok(())
}

/// The end of the SL/TP request that follows a filled market open. The open is acknowledged as filled with its position ID either
/// way, so the copy is bound and the source's later changes and close still reach it. When the protection failed, the copy is
/// recorded as unprotected (alert `protection_missing`) instead of leaving the whole command unknown: an unknown open had no
/// binding, so the follower sat without a stop and the source's close waited behind it until an operator stepped in.
async fn protection_finished(state: &Arc<AppState>, command: &ExecutionCommand, external_id: String, failure: Option<String>) -> Result<()> {
    let ack = ExecutionAck {
        command_id: command.command_id.clone(),
        account_id: command.target_account_id.clone(),
        status: AckStatus::Filled,
        external_id: Some(external_id.clone()),
        timestamp_unix_ns: unix_time_ns(),
        message: Some(failure.clone().unwrap_or_else(|| "cTrader position protection finalized".to_owned())),
    };
    state.handle_frame(&command.target_account_id, AgentFrame::Ack(ack)).await?;
    if let Some(reason) = failure {
        state.protection_missing(command, &external_id, &reason).await?;
    }
    Ok(())
}

/// An execution event's effect on the account: its pending orders, then its position (which republishes the snapshot). An order
/// change with no position in the event (accepted, amended, cancelled, expired) republishes it here.
async fn apply_execution(state: &Arc<AppState>, runtime: &mut AccountRuntime, payload: &Value, execution_type: i64) -> Result<()> {
    let orders_changed = apply_order_execution(runtime, payload, execution_type)?;
    if payload.get("position").is_some() {
        update_position_from_execution(state, runtime, payload).await
    } else if orders_changed {
        publish_positions(state, runtime).await
    } else {
        Ok(())
    }
}

async fn update_position_from_execution(state: &Arc<AppState>, runtime: &mut AccountRuntime, payload: &Value) -> Result<()> {
    let Some(position) = payload.get("position") else { return Ok(()); };
    runtime.position_revision += 1;
    let position_id = position.get("positionId").and_then(value_to_string)
        .ok_or_else(|| anyhow::anyhow!("cTrader position omitted id"))?;
    let status = enum_number(position.get("positionStatus"), &[
        ("POSITION_STATUS_OPEN", 1), ("POSITION_STATUS_CLOSED", 2),
        ("POSITION_STATUS_CREATED", 3), ("POSITION_STATUS_ERROR", 4),
    ]).unwrap_or(1);

    if status == 2 {
        if let Some(old) = runtime.positions.remove(&position_id) {
            if runtime.account.local.role.can_publish() {
                emit_event(state, runtime, &old, TradeAction::Close, old.volume_lots, Some(0.0)).await?;
            }
            publish_positions(state, runtime).await?;
        }
        return Ok(());
    }
    if status != 1 { return Ok(()); }

    let current = parse_position(position, &runtime.symbols_by_id)?;
    if let Some(old) = runtime.positions.get(&position_id).cloned() {
        if runtime.account.local.role.can_publish() {
            emit_changed_position(state, runtime, &old, &current).await?;
        }
    } else if runtime.account.local.role.can_publish() {
        emit_event(state, runtime, &current, TradeAction::Open, current.volume_lots, Some(current.volume_lots)).await?;
    }
    runtime.positions.insert(position_id, current);
    publish_positions(state, runtime).await
}

/// The account's positions as kept current from execution events, published as a fresh snapshot at once. A master open waits for
/// its position in a current snapshot and a follower's next command waits for a snapshot after each fill; both waited for the
/// 3-second reconciliation, and an execution arriving while one was in flight discarded it, so opens waited 1.4-3.2 s
/// (23 Nov 2026 demo test). Reconciliation still runs every 3 s and corrects anything an execution event did not carry.
async fn publish_positions(state: &Arc<AppState>, runtime: &AccountRuntime) -> Result<()> {
    let mut exposure = broker_positions(&runtime.positions);
    exposure.extend(runtime.orders.iter().cloned());
    state.observe_positions(&runtime.account.local.id, exposure).await
}

async fn reconcile_source_positions(
    state: &Arc<AppState>, runtime: &mut AccountRuntime, positions: HashMap<String, PositionSnapshot>,
) -> Result<()> {
    if runtime.account.local.role.can_publish() {
        for (id, current) in &positions {
            if let Some(old) = runtime.positions.get(id) {
                emit_changed_position(state, runtime, old, current).await?;
            } else {
                emit_event(state, runtime, current, TradeAction::Open, current.volume_lots, Some(current.volume_lots)).await?;
            }
        }
        for (id, old) in &runtime.positions {
            if !positions.contains_key(id) {
                emit_event(state, runtime, old, TradeAction::Close, old.volume_lots, Some(0.0)).await?;
            }
        }
    }
    runtime.positions = positions;
    Ok(())
}

async fn emit_changed_position(state: &Arc<AppState>, runtime: &AccountRuntime, old: &PositionSnapshot, current: &PositionSnapshot) -> Result<()> {
    if current.volume_lots > old.volume_lots + 1e-8 || current.symbol != old.symbol || current.side != old.side {
        state.source_drift(&runtime.account.local.id, &current.id).await?;
    } else if current.volume_lots + 1e-8 < old.volume_lots {
        emit_event(state, runtime, current, TradeAction::Reduce, old.volume_lots - current.volume_lots, Some(current.volume_lots)).await?;
    } else if changed_price(old.stop_loss, current.stop_loss) || changed_price(old.take_profit, current.take_profit) {
        emit_event(state, runtime, current, TradeAction::Modify, current.volume_lots, Some(current.volume_lots)).await?;
    }
    Ok(())
}

async fn emit_event(
    state: &Arc<AppState>,
    runtime: &AccountRuntime,
    position: &PositionSnapshot,
    action: TradeAction,
    volume: f64,
    remaining_volume: Option<f64>,
) -> Result<()> {
    let now = unix_time_ns();
    let event = TradeEvent {
        event_id: format!("ctrader:{}:{}:{}:{}", runtime.account.local.id, position.id, action, now),
        source_account_id: runtime.account.local.id.clone(),
        platform: runtime.account.local.platform,
        action,
        source_order_id: position.id.clone(),
        symbol: position.symbol.clone(),
        side: Some(position.side),
        volume,
        remaining_volume,
        price: position.price,
        stop_loss: position.stop_loss,
        take_profit: position.take_profit,
        timestamp_unix_ns: now,
        origin_command_id: position.origin_command_id.clone(),
    };
    event.validate()?;
    state.handle_frame(&runtime.account.local.id, AgentFrame::Event(event)).await
}

fn command_message(runtime: &AccountRuntime, command: &ExecutionCommand) -> Result<(i64, Value)> {
    let ctid = runtime.account.ctid;
    match command.action {
        TradeAction::Open | TradeAction::PlaceLimit | TradeAction::PlaceStop => {
            let symbol = runtime.symbols_by_name.get(&command.symbol)
                .ok_or_else(|| anyhow::anyhow!("cTrader symbol {} not found", command.symbol))?;
            let side = match command.side {
                Some(Side::Buy) => 1,
                Some(Side::Sell) => 2,
                None => bail!("cTrader open command omitted side"),
            };
            let volume = symbol.order_units(command.volume)?;
            let order_type = match command.action {
                TradeAction::PlaceLimit => 2,
                TradeAction::PlaceStop => 3,
                _ => 1,
            };
            let mut payload = json!({
                "ctidTraderAccountId": ctid,
                "symbolId": symbol.id,
                "orderType": order_type,
                "tradeSide": side,
                "volume": volume,
                "clientOrderId": command.command_id,
                "label": format!("CopierR:{}", command.command_id)
            });
            if command.action.is_pending() {
                let field = if command.action == TradeAction::PlaceLimit { "limitPrice" } else { "stopPrice" };
                payload[field] = json!(command.price.context("pending order omitted its trigger")?);
                payload["timeInForce"] = json!(2);
                apply_protection(&mut payload, command);
            }
            Ok((NEW_ORDER_REQ, payload))
        }
        TradeAction::Modify => {
            let position_id = parse_target_position(command)?;
            let mut payload = json!({ "ctidTraderAccountId": ctid, "positionId": position_id });
            apply_protection(&mut payload, command);
            Ok((AMEND_POSITION_SLTP_REQ, payload))
        }
        TradeAction::Reduce | TradeAction::Close => {
            let position_id = parse_target_position(command)?;
            let symbol = runtime.symbols_by_name.get(&command.symbol)
                .ok_or_else(|| anyhow::anyhow!("cTrader symbol {} not found", command.symbol))?;
            let volume = if command.action == TradeAction::Close {
                symbol.units(command.volume)?
            } else { symbol.order_units(command.volume)? };
            Ok((CLOSE_POSITION_REQ, json!({
                "ctidTraderAccountId": ctid,
                "positionId": position_id,
                "volume": volume
            })))
        }
        TradeAction::AmendPending | TradeAction::CancelPending => {
            let order_id = parse_target_position(command)?;
            let mut payload = json!({ "ctidTraderAccountId": ctid, "orderId": order_id });
            if command.action == TradeAction::CancelPending {
                return Ok((CANCEL_ORDER_REQ, payload));
            }
            // Set the original trigger field using the authoritative order type.
            let trigger = runtime.order_triggers.get(&order_id.to_string()).context("pending order is absent from cTrader snapshot")?;
            payload[*trigger] = json!(command.price.context("pending amendment omitted its trigger")?);
            apply_protection(&mut payload, command);
            Ok((AMEND_ORDER_REQ, payload))
        }
    }
}

fn apply_protection(payload: &mut Value, command: &ExecutionCommand) {
    if let Some(stop_loss) = command.stop_loss { payload["stopLoss"] = json!(stop_loss); }
    if let Some(take_profit) = command.take_profit { payload["takeProfit"] = json!(take_profit); }
}

fn parse_target_position(command: &ExecutionCommand) -> Result<i64> {
    command.target_order_id.as_deref()
        .ok_or_else(|| anyhow::anyhow!("cTrader command requires target position id"))?
        .parse::<i64>()
        .context("invalid cTrader target position id")
}

async fn send_json(ws: &mut Ws, payload_type: i64, client_msg_id: &str, payload: Value) -> Result<()> {
    let frame = json!({
        "clientMsgId": client_msg_id,
        "payloadType": payload_type,
        "payload": payload
    });
    timeout(Duration::from_secs(10), ws.send(Message::Text(frame.to_string()))).await?
        .context("cTrader websocket send failed")
}

async fn wait_for(ws: &mut Ws, expected: i64, account_id: Option<i64>) -> Result<Value> {
    timeout(Duration::from_secs(15), wait_for_response(ws, expected, account_id)).await?
}

async fn wait_for_response(ws: &mut Ws, expected: i64, account_id: Option<i64>) -> Result<Value> {
    loop {
        let message = ws.next().await.ok_or_else(|| anyhow::anyhow!("cTrader websocket closed during startup"))??;
        let Some(value) = parse_ws_message(message)? else { continue; };
        let payload_type = value.get("payloadType").and_then(Value::as_i64).unwrap_or_default();
        if payload_type == ERROR_RES || payload_type == ORDER_ERROR_EVENT {
            bail!("cTrader startup error: {}", value);
        }
        if payload_type != expected { continue; }
        if let Some(account_id) = account_id {
            let response_id = value.get("payload").and_then(|payload| payload.get("ctidTraderAccountId")).and_then(as_i64);
            if response_id != Some(account_id) { continue; }
        }
        return Ok(value);
    }
}

fn parse_ws_message(message: Message) -> Result<Option<Value>> {
    match message {
        Message::Text(text) => Ok(Some(serde_json::from_str(&text).context("invalid cTrader JSON frame")?)),
        Message::Binary(bytes) => {
            let text = std::str::from_utf8(&bytes).context("non-UTF8 cTrader JSON frame")?;
            Ok(Some(serde_json::from_str(text).context("invalid cTrader JSON frame")?))
        }
        Message::Ping(_) | Message::Pong(_) => Ok(None),
        Message::Close(frame) => bail!("cTrader websocket closed: {frame:?}"),
        _ => Ok(None),
    }
}

fn parse_light_symbols(value: &Value) -> Result<Vec<(i64, String)>> {
    let symbols = value.get("payload").and_then(|payload| payload.get("symbol"))
        .and_then(Value::as_array).ok_or_else(|| anyhow::anyhow!("cTrader symbols response omitted symbols"))?;
    let mut out = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let id = symbol.get("symbolId").and_then(as_i64).ok_or_else(|| anyhow::anyhow!("cTrader symbol omitted id"))?;
        let name = symbol.get("symbolName").and_then(Value::as_str).unwrap_or("").to_owned();
        if !name.is_empty() { out.push((id, name)); }
    }
    Ok(out)
}

fn parse_symbol_details(
    light: &[(i64, String)],
    value: &Value,
) -> Result<(HashMap<String, SymbolInfo>, HashMap<i64, SymbolInfo>)> {
    let names: HashMap<i64, String> = light.iter().cloned().collect();
    let symbols = value.get("payload").and_then(|payload| payload.get("symbol"))
        .and_then(Value::as_array).ok_or_else(|| anyhow::anyhow!("cTrader symbol details response omitted symbols"))?;
    let mut by_name = HashMap::new();
    let mut by_id = HashMap::new();
    for symbol in symbols {
        let id = symbol.get("symbolId").and_then(as_i64).ok_or_else(|| anyhow::anyhow!("cTrader full symbol omitted id"))?;
        let Some(name) = names.get(&id).cloned() else { continue; };
        let lot_size_cents = symbol.get("lotSize").and_then(as_f64).filter(|value| value.is_finite() && *value > 0.0)
            .context("cTrader symbol omitted a valid lot size")?;
        let min_units = symbol.get("minVolume").and_then(as_i64).filter(|value| *value > 0).unwrap_or(1);
        let max_units = symbol.get("maxVolume").and_then(as_i64).filter(|value| *value >= min_units).unwrap_or(i64::MAX);
        let step_units = symbol.get("stepVolume").and_then(as_i64).filter(|value| *value > 0).unwrap_or(1);
        let info = SymbolInfo { id, name: name.clone(), lot_size_cents, min_units, max_units, step_units };
        by_name.insert(name, info.clone());
        by_id.insert(id, info);
    }
    Ok((by_name, by_id))
}

fn reconcile_items<'a>(value: &'a Value, field: &str) -> Result<&'a [Value]> {
    let payload = value.get("payload").and_then(Value::as_object).context("cTrader reconcile response omitted payload")?;
    match payload.get(field) {
        Some(value) => Ok(value.as_array().context("invalid cTrader reconciliation list")?),
        None => Ok(&[]),
    }
}

fn parse_reconcile_positions(value: &Value, symbols: &HashMap<i64, SymbolInfo>) -> Result<HashMap<String, PositionSnapshot>> {
    let mut positions = HashMap::new();
    for position in reconcile_items(value, "position")? {
        let parsed = parse_position(position, symbols)?;
        anyhow::ensure!(positions.insert(parsed.id.clone(), parsed).is_none(), "duplicate cTrader position in snapshot");
    }
    Ok(positions)
}

type OrderTriggers = HashMap<String, &'static str>;

fn parse_reconcile_orders(value: &Value, symbols: &HashMap<i64, SymbolInfo>) -> Result<(Vec<BrokerPosition>, OrderTriggers)> {
    let mut orders = Vec::new();
    let mut triggers = HashMap::new();
    for order in reconcile_items(value, "order")? {
        if let Some((entry, trigger)) = parse_pending_order(order, symbols)? {
            if let Some(trigger) = trigger { triggers.insert(entry.id.clone(), trigger); }
            orders.push(entry);
        }
    }
    Ok((orders, triggers))
}

/// A pending (limit, stop or stop-limit) order as exposure, with the field its trigger lives in; None for any other order type.
fn parse_pending_order(order: &Value, symbols: &HashMap<i64, SymbolInfo>) -> Result<Option<(BrokerPosition, Option<&'static str>)>> {
    let kind = enum_number(order.get("orderType"), &[("LIMIT", 2), ("STOP", 3), ("STOP_LIMIT", 6)]);
    if !matches!(kind, Some(2 | 3 | 6)) { return Ok(None); }
    let id = order.get("orderId").and_then(value_to_string).context("cTrader pending order omitted ID")?;
    let trade = order.get("tradeData").context("cTrader pending order omitted trade data")?;
    let symbol = trade.get("symbolId").and_then(as_i64).and_then(|id| symbols.get(&id)).context("unknown cTrader pending-order symbol")?;
    let volume = trade.get("volume").and_then(as_f64).context("cTrader pending order omitted volume")?;
    let executed = order.get("executedVolume").and_then(as_f64).unwrap_or(0.0);
    let copier_managed = trade.get("label").and_then(Value::as_str).is_some_and(|label| label.starts_with("CopierR:"));
    let trigger = (kind != Some(6)).then(|| if kind == Some(2) { "limitPrice" } else { "stopPrice" });
    Ok(Some((BrokerPosition { id, lots: (volume - executed) / symbol.lot_size_cents, copier_managed, pending: true }, trigger)))
}

/// Keeps the account's pending orders current from an execution event: accepted, amended or partly filled orders are kept with
/// their remaining volume; filled, cancelled or expired ones leave. True when the list changed (the snapshot is then republished).
fn apply_order_execution(runtime: &mut AccountRuntime, payload: &Value, execution_type: i64) -> Result<bool> {
    let Some(order) = payload.get("order") else { return Ok(false); };
    let Some((entry, trigger)) = parse_pending_order(order, &runtime.symbols_by_id)? else { return Ok(false); };
    match execution_type {
        2 | 4 | 11 => {
            runtime.orders.retain(|held| held.id != entry.id);
            if let Some(trigger) = trigger { runtime.order_triggers.insert(entry.id.clone(), trigger); }
            if entry.lots > 1e-12 { runtime.orders.push(entry); }
        }
        3 | 5 | 6 => {
            runtime.orders.retain(|held| held.id != entry.id);
            runtime.order_triggers.remove(&entry.id);
        }
        _ => return Ok(false), // a rejected request leaves the order as it was
    }
    runtime.position_revision += 1; // a reconciliation already in flight predates this change
    Ok(true)
}

fn parse_position(value: &Value, symbols: &HashMap<i64, SymbolInfo>) -> Result<PositionSnapshot> {
    let id = value.get("positionId").and_then(value_to_string)
        .ok_or_else(|| anyhow::anyhow!("cTrader position omitted id"))?;
    let trade = value.get("tradeData").ok_or_else(|| anyhow::anyhow!("cTrader position omitted tradeData"))?;
    let symbol_id = trade.get("symbolId").and_then(as_i64).ok_or_else(|| anyhow::anyhow!("cTrader position omitted symbol id"))?;
    let symbol = symbols.get(&symbol_id).ok_or_else(|| anyhow::anyhow!("unknown cTrader symbol id {symbol_id}"))?;
    let volume_cents = trade.get("volume").and_then(as_f64).filter(|volume| volume.is_finite() && *volume > 0.0)
        .context("cTrader position omitted a valid volume")?;
    let side = match enum_number(trade.get("tradeSide"), &[("BUY", 1), ("SELL", 2)]).unwrap_or_default() {
        1 => Side::Buy,
        2 => Side::Sell,
        other => bail!("unknown cTrader trade side {other}"),
    };
    let label = trade.get("label").and_then(Value::as_str).unwrap_or("");
    let origin_command_id = label.strip_prefix("CopierR:").map(str::to_owned);
    Ok(PositionSnapshot {
        id,
        symbol: symbol.name.clone(),
        side,
        volume_lots: volume_cents / symbol.lot_size_cents,
        price: value.get("price").and_then(as_f64),
        stop_loss: value.get("stopLoss").and_then(as_f64),
        take_profit: value.get("takeProfit").and_then(as_f64),
        origin_command_id,
    })
}

fn execution_position_id(payload: &Value) -> Option<String> {
    payload.get("position").and_then(|position| position.get("positionId")).and_then(value_to_string)
        .or_else(|| payload.get("order").and_then(|order| order.get("positionId")).and_then(value_to_string))
}

fn execution_order_id(payload: &Value) -> Option<String> {
    payload.get("order").and_then(|order| order.get("orderId")).and_then(value_to_string)
}

fn unknown_ack(command: &ExecutionCommand, message: &str) -> ExecutionAck {
    ExecutionAck {
        command_id: command.command_id.clone(),
        account_id: command.target_account_id.clone(),
        status: AckStatus::Unknown,
        external_id: None,
        timestamp_unix_ns: unix_time_ns(),
        message: Some(message.to_owned()),
    }
}

fn enum_number(value: Option<&Value>, names: &[(&str, i64)]) -> Option<i64> {
    let value = value?;
    if let Some(number) = as_i64(value) { return Some(number); }
    let name = value.as_str()?;
    names.iter().find(|(candidate, _)| *candidate == name).map(|(_, number)| *number)
}

fn as_i64(value: &Value) -> Option<i64> {
    value.as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn as_f64(value: &Value) -> Option<f64> {
    value.as_f64()
        .or_else(|| value.as_i64().map(|value| value as f64))
        .or_else(|| value.as_u64().map(|value| value as f64))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn value_to_string(value: &Value) -> Option<String> {
    value.as_str().map(str::to_owned)
        .or_else(|| value.as_i64().map(|value| value.to_string()))
        .or_else(|| value.as_u64().map(|value| value.to_string()))
}

fn changed_price(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => (left - right).abs() > 1e-10,
        (None, None) => false,
        _ => true,
    }
}
