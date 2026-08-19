use crate::{
    config::{read_secret, AccountConfig, CTraderEnvironment},
    runtime::{unix_time_ns, AppState},
};
use anyhow::{bail, Context, Result};
use copier_core::{AckStatus, AgentFrame, ExecutionAck, ExecutionCommand, ServerFrame, Side, TradeAction, TradeEvent};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::mpsc, time::{interval, Duration, MissedTickBehavior}};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{error, info, warn};

const APPLICATION_AUTH_REQ: i64 = 2100;
const APPLICATION_AUTH_RES: i64 = 2101;
const ACCOUNT_AUTH_REQ: i64 = 2102;
const ACCOUNT_AUTH_RES: i64 = 2103;
const NEW_ORDER_REQ: i64 = 2106;
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
        if let Err(err) = run_hub(state, environment, url, client_id, client_secret, prepared).await {
            error!(?environment, error = %err, "cTrader Open API hub stopped");
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
    let (mut ws, _) = connect_async(&url).await.with_context(|| format!("failed connecting cTrader Open API {url}"))?;
    send_json(&mut ws, APPLICATION_AUTH_REQ, "app-auth", json!({
        "clientId": client_id,
        "clientSecret": client_secret
    })).await?;
    wait_for(&mut ws, APPLICATION_AUTH_RES, None).await?;

    let mut runtimes = HashMap::<i64, AccountRuntime>::new();
    let (command_tx, mut command_rx) = mpsc::channel::<ExecutionCommand>(state.config.queue_capacity.saturating_mul(2));

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

        let local_id = prepared.local.id.clone();
        let (tx, mut rx) = mpsc::channel(state.config.queue_capacity);
        let session_id = state.register_session(local_id.clone(), tx).await;
        let forward = command_tx.clone();
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                if let ServerFrame::Command(command) = frame {
                    if forward.send(command).await.is_err() {
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
        });
        state.dispatch_queued_for(&local_id).await?;
        info!(account = %local_id, ctid = prepared_ctid(&runtimes, &local_id), ?environment, "cTrader Open API account connected");
    }

    let mut pending = HashMap::<String, ExecutionCommand>::new();
    let mut protection_pending = HashMap::<String, (ExecutionCommand, String)>::new();
    let mut heartbeat = interval(Duration::from_secs(10));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    heartbeat.tick().await;

    let result: Result<()> = async {
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
                    send_json(&mut ws, HEARTBEAT_EVENT, "heartbeat", json!({})).await?;
                }
                message = ws.next() => {
                    let Some(message) = message else { break; };
                    let value = parse_ws_message(message?)?;
                    let Some(value) = value else { continue; };
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

fn prepared_ctid(runtimes: &HashMap<i64, AccountRuntime>, local_id: &str) -> i64 {
    runtimes.values().find(|runtime| runtime.account.local.id == local_id)
        .map(|runtime| runtime.account.ctid).unwrap_or_default()
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
        if matches!(execution_type, 2 | 11) {
            if let Some(runtime) = runtimes.get_mut(&ctid) {
                update_position_from_execution(state, runtime, &payload).await?;
            }
            return Ok(());
        }

        if let Some((original, external_id)) = protection_pending.remove(&client_msg_id) {
            let status = match execution_type {
                3 | 4 => AckStatus::Filled,
                5 | 6 | 7 | 8 => AckStatus::Rejected,
                _ => AckStatus::Unknown,
            };
            let ack = ExecutionAck {
                command_id: original.command_id.clone(),
                account_id: original.target_account_id.clone(),
                status,
                external_id: Some(external_id),
                timestamp_unix_ns: unix_time_ns(),
                message: Some("cTrader position protection finalized".to_owned()),
            };
            state.handle_frame(&original.target_account_id, AgentFrame::Ack(ack)).await?;
        } else if let Some(command) = pending.remove(&client_msg_id) {
            let external_id = execution_external_id(&payload).or_else(|| command.target_order_id.clone());
            if command.action == TradeAction::Open
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
                let status = match execution_type {
                    3 | 4 => AckStatus::Filled,
                    5 | 6 | 7 | 8 => AckStatus::Rejected,
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
            update_position_from_execution(state, runtime, &payload).await?;
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
            let ack = ExecutionAck {
                command_id: command.command_id.clone(),
                account_id: command.target_account_id.clone(),
                status: AckStatus::Unknown,
                external_id: Some(external_id),
                timestamp_unix_ns: unix_time_ns(),
                message: Some("cTrader order filled but SL/TP protection request failed".to_owned()),
            };
            state.handle_frame(&command.target_account_id, AgentFrame::Ack(ack)).await?;
        } else {
            warn!(payload_type, client_msg_id, payload = %payload, "unmatched cTrader error event");
        }
    }
    Ok(())
}

async fn update_position_from_execution(state: &Arc<AppState>, runtime: &mut AccountRuntime, payload: &Value) -> Result<()> {
    let Some(position) = payload.get("position") else { return Ok(()); };
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
        }
        return Ok(());
    }

    let current = parse_position(position, &runtime.symbols_by_id)?;
    if let Some(old) = runtime.positions.get(&position_id).cloned() {
        if runtime.account.local.role.can_publish() {
            if current.volume_lots + f64::EPSILON < old.volume_lots {
                emit_event(state, runtime, &current, TradeAction::Reduce, old.volume_lots - current.volume_lots, Some(current.volume_lots)).await?;
            } else if current.volume_lots > old.volume_lots + f64::EPSILON {
                warn!(account = %runtime.account.local.id, position = %position_id, "cTrader scale-in detected; left for reconciliation in v0.2");
            } else if changed_price(old.stop_loss, current.stop_loss) || changed_price(old.take_profit, current.take_profit) {
                emit_event(state, runtime, &current, TradeAction::Modify, current.volume_lots, Some(current.volume_lots)).await?;
            }
        }
    } else if runtime.account.local.role.can_publish() {
        emit_event(state, runtime, &current, TradeAction::Open, current.volume_lots, Some(current.volume_lots)).await?;
    }
    runtime.positions.insert(position_id, current);
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
        TradeAction::Open => {
            let symbol = runtime.symbols_by_name.get(&command.symbol)
                .ok_or_else(|| anyhow::anyhow!("cTrader symbol {} not found", command.symbol))?;
            let side = match command.side {
                Some(Side::Buy) => 1,
                Some(Side::Sell) => 2,
                None => bail!("cTrader open command omitted side"),
            };
            let volume = (command.volume * symbol.lot_size_cents).round() as i64;
            if volume <= 0 { bail!("cTrader normalized volume is zero"); }
            Ok((NEW_ORDER_REQ, json!({
                "ctidTraderAccountId": ctid,
                "symbolId": symbol.id,
                "orderType": 1,
                "tradeSide": side,
                "volume": volume,
                "clientOrderId": command.command_id,
                "label": format!("CopierR:{}", command.command_id)
            })))
        }
        TradeAction::Modify => {
            let position_id = parse_target_position(command)?;
            let mut payload = json!({ "ctidTraderAccountId": ctid, "positionId": position_id });
            if let Some(stop_loss) = command.stop_loss { payload["stopLoss"] = json!(stop_loss); }
            if let Some(take_profit) = command.take_profit { payload["takeProfit"] = json!(take_profit); }
            Ok((AMEND_POSITION_SLTP_REQ, payload))
        }
        TradeAction::Reduce | TradeAction::Close => {
            let position_id = parse_target_position(command)?;
            let symbol = runtime.symbols_by_name.get(&command.symbol)
                .ok_or_else(|| anyhow::anyhow!("cTrader symbol {} not found", command.symbol))?;
            let volume_lots = if command.action == TradeAction::Close {
                runtime.positions.get(&position_id.to_string()).map(|position| position.volume_lots).unwrap_or(command.volume)
            } else {
                command.volume
            };
            let volume = (volume_lots * symbol.lot_size_cents).round() as i64;
            Ok((CLOSE_POSITION_REQ, json!({
                "ctidTraderAccountId": ctid,
                "positionId": position_id,
                "volume": volume
            })))
        }
    }
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
    ws.send(Message::Text(frame.to_string())).await.context("cTrader websocket send failed")
}

async fn wait_for(ws: &mut Ws, expected: i64, account_id: Option<i64>) -> Result<Value> {
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
        let lot_size_cents = symbol.get("lotSize").and_then(as_f64).unwrap_or(10_000_000.0);
        let info = SymbolInfo { id, name: name.clone(), lot_size_cents };
        by_name.insert(name, info.clone());
        by_id.insert(id, info);
    }
    Ok((by_name, by_id))
}

fn parse_reconcile_positions(value: &Value, symbols: &HashMap<i64, SymbolInfo>) -> Result<HashMap<String, PositionSnapshot>> {
    let mut positions = HashMap::new();
    let values = value.get("payload").and_then(|payload| payload.get("position"))
        .and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
    for position in values {
        let parsed = parse_position(position, symbols)?;
        positions.insert(parsed.id.clone(), parsed);
    }
    Ok(positions)
}

fn parse_position(value: &Value, symbols: &HashMap<i64, SymbolInfo>) -> Result<PositionSnapshot> {
    let id = value.get("positionId").and_then(value_to_string)
        .ok_or_else(|| anyhow::anyhow!("cTrader position omitted id"))?;
    let trade = value.get("tradeData").ok_or_else(|| anyhow::anyhow!("cTrader position omitted tradeData"))?;
    let symbol_id = trade.get("symbolId").and_then(as_i64).ok_or_else(|| anyhow::anyhow!("cTrader position omitted symbol id"))?;
    let symbol = symbols.get(&symbol_id).ok_or_else(|| anyhow::anyhow!("unknown cTrader symbol id {symbol_id}"))?;
    let volume_cents = trade.get("volume").and_then(as_f64).unwrap_or_default();
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

fn execution_external_id(payload: &Value) -> Option<String> {
    payload.get("position").and_then(|position| position.get("positionId")).and_then(value_to_string)
        .or_else(|| payload.get("order").and_then(|order| order.get("orderId")).and_then(value_to_string))
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
