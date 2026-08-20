use crate::{
    config::{read_secret, AccountConfig, MetaApiAccountConfig, MetaApiGlobalConfig},
    runtime::{unix_time_ns, AppState},
};
use anyhow::{bail, Context, Result};
use copier_core::{AckStatus, AgentFrame, ExecutionAck, ExecutionCommand, Platform, ServerFrame, Side, TradeAction, TradeEvent};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::mpsc, time::{interval, sleep, Duration, MissedTickBehavior}};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Deserialize)]
struct PositionSnapshot {
    id: String,
    #[serde(rename = "type")]
    position_type: String,
    symbol: String,
    volume: f64,
    #[serde(rename = "openPrice")]
    open_price: f64,
    #[serde(rename = "stopLoss")]
    stop_loss: Option<f64>,
    #[serde(rename = "takeProfit")]
    take_profit: Option<f64>,
    #[serde(rename = "clientId")]
    client_id: Option<String>,
}

pub async fn run_account(
    state: Arc<AppState>,
    account: AccountConfig,
    global: MetaApiGlobalConfig,
    auth_token: String,
) -> Result<()> {
    let direct = account.metaapi.clone().expect("validated MetaApi config");
    let client = Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(8)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to construct MetaApi HTTP client")?;

    let provider_account_id = ensure_account(&client, &account, &direct, &global, &auth_token).await?;
    let api_base = direct.api_base.clone().unwrap_or_else(|| {
        format!("https://mt-client-api-v1.{}.agiliumtrade.ai", direct.region)
    });

    let (tx, mut rx) = mpsc::channel(state.config.queue_capacity);
    let session_id = state.register_session(account.id.clone(), tx).await;
    state.dispatch_queued_for(&account.id).await?;

    let mut known = match fetch_positions(&client, &api_base, &provider_account_id, &auth_token).await {
        Ok(positions) => index_positions(positions),
        Err(err) => {
            warn!(account = %account.id, error = %err, "initial MetaApi snapshot failed; starting empty");
            HashMap::new()
        }
    };

    info!(account = %account.id, provider_account = %provider_account_id, region = %direct.region, "MetaApi direct venue connected");
    let mut poll = interval(Duration::from_millis(direct.poll_interval_ms));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    poll.tick().await;

    let result: Result<()> = async {
        loop {
            tokio::select! {
                maybe_frame = rx.recv() => {
                    let Some(frame) = maybe_frame else { break; };
                    if let ServerFrame::Command(command) = frame {
                        let ack = execute(&client, &api_base, &provider_account_id, &auth_token, direct.magic, *command).await;
                        state.handle_frame(&account.id, AgentFrame::Ack(ack)).await?;
                    }
                }
                _ = poll.tick(), if account.role.can_publish() => {
                    match fetch_positions(&client, &api_base, &provider_account_id, &auth_token).await {
                        Ok(current) => {
                            let current = index_positions(current);
                            emit_position_diffs(&state, &account, &known, &current).await?;
                            known = current;
                        }
                        Err(err) => warn!(account = %account.id, error = %err, "MetaApi position poll failed"),
                    }
                }
            }
        }
        Ok(())
    }.await;

    state.unregister_session(&account.id, session_id).await?;
    result
}

async fn ensure_account(
    client: &Client,
    account: &AccountConfig,
    direct: &MetaApiAccountConfig,
    global: &MetaApiGlobalConfig,
    auth_token: &str,
) -> Result<String> {
    if let Some(id) = direct.account_id.as_deref().filter(|value| !value.is_empty()) {
        return Ok(id.to_owned());
    }

    let login = direct.login.as_deref().expect("validated login");
    let server = direct.server.as_deref().expect("validated server");
    let password_env = direct.password_env.as_deref().expect("validated password env");
    let password = read_secret(password_env)?;
    let platform = match account.platform {
        Platform::Mt4 => "mt4",
        Platform::Mt5 => "mt5",
        _ => bail!("MetaApi provisioning only supports MT4/MT5"),
    };
    let tx_id = provisioning_transaction_id(&account.id, login, server);
    let url = format!("{}/users/current/accounts", global.provisioning_base.trim_end_matches('/'));
    let body = json!({
        "login": login,
        "password": password,
        "name": account.id,
        "server": server,
        "platform": platform,
        "magic": direct.magic,
        "type": "cloud-g2"
    });

    for attempt in 1..=24 {
        let response = client
            .post(&url)
            .header("auth-token", auth_token)
            .header("transaction-id", &tx_id)
            .json(&body)
            .send()
            .await
            .context("MetaApi provisioning request failed")?;
        let status = response.status();
        if status == StatusCode::ACCEPTED {
            debug!(account = %account.id, attempt, "MetaApi provisioning still in progress");
            sleep(Duration::from_secs(5)).await;
            continue;
        }
        if status.is_success() {
            let value: Value = response.json().await.context("invalid MetaApi provisioning response")?;
            if let Some(id) = value.get("id").and_then(Value::as_str) {
                info!(account = %account.id, provider_account = %id, "MetaApi account provisioned");
                return Ok(id.to_owned());
            }
            bail!("MetaApi provisioning response did not include account id");
        }
        let body = response.text().await.unwrap_or_default();
        bail!("MetaApi provisioning failed with {status}: {body}");
    }
    bail!("MetaApi provisioning did not complete after repeated attempts")
}

fn provisioning_transaction_id(account_id: &str, login: &str, server: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"copierr:metaapi:provision:v1\0");
    hasher.update(account_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(login.as_bytes());
    hasher.update(b"\0");
    hasher.update(server.as_bytes());
    let digest = hasher.finalize();
    digest[..16].iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn execute(
    client: &Client,
    api_base: &str,
    account_id: &str,
    auth_token: &str,
    magic: u64,
    command: ExecutionCommand,
) -> ExecutionAck {
    let timestamp = unix_time_ns();
    let position_id = command.target_order_id.clone();
    let mut body = match command.action {
        TradeAction::Open => {
            let action = match command.side {
                Some(Side::Buy) => "ORDER_TYPE_BUY",
                Some(Side::Sell) => "ORDER_TYPE_SELL",
                None => return rejected(&command, "open command omitted side"),
            };
            json!({
                "actionType": action,
                "symbol": command.symbol,
                "volume": command.volume,
                "magic": magic,
                "clientId": metaapi_client_id(&command.command_id)
            })
        }
        TradeAction::Modify => json!({
            "actionType": "POSITION_MODIFY",
            "positionId": position_id,
            "magic": magic
        }),
        TradeAction::Reduce => json!({
            "actionType": "POSITION_PARTIAL",
            "positionId": position_id,
            "volume": command.volume,
            "magic": magic,
            "clientId": metaapi_client_id(&command.command_id)
        }),
        TradeAction::Close => json!({
            "actionType": "POSITION_CLOSE_ID",
            "positionId": position_id,
            "magic": magic,
            "clientId": metaapi_client_id(&command.command_id)
        }),
    };
    if matches!(command.action, TradeAction::Open | TradeAction::Modify) {
        if let Some(stop_loss) = command.stop_loss {
            body["stopLoss"] = json!(stop_loss);
        }
        if let Some(take_profit) = command.take_profit {
            body["takeProfit"] = json!(take_profit);
        }
    }

    let url = format!("{}/users/current/accounts/{}/trade", api_base.trim_end_matches('/'), account_id);
    let response = match client.post(url).header("auth-token", auth_token).json(&body).send().await {
        Ok(response) => response,
        Err(err) => {
            return ExecutionAck {
                command_id: command.command_id,
                account_id: command.target_account_id,
                status: AckStatus::Unknown,
                external_id: None,
                timestamp_unix_ns: timestamp,
                message: Some(format!("MetaApi transport uncertainty: {err}")),
            };
        }
    };

    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let ack_status = if status.is_server_error() { AckStatus::Unknown } else { AckStatus::Rejected };
        return ExecutionAck {
            command_id: command.command_id,
            account_id: command.target_account_id,
            status: ack_status,
            external_id: None,
            timestamp_unix_ns: timestamp,
            message: Some(format!("MetaApi HTTP {status}: {text}")),
        };
    }

    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(err) => {
            return ExecutionAck {
                command_id: command.command_id,
                account_id: command.target_account_id,
                status: AckStatus::Unknown,
                external_id: None,
                timestamp_unix_ns: timestamp,
                message: Some(format!("invalid MetaApi trade response: {err}")),
            };
        }
    };
    let code = value.get("stringCode").and_then(Value::as_str).unwrap_or("");
    let external_id = value.get("positionId").or_else(|| value.get("orderId"))
        .and_then(value_to_string)
        .or_else(|| position_id.clone());
    let ack_status = match code {
        "TRADE_RETCODE_DONE" | "ERR_NO_ERROR" | "TRADE_RETCODE_NO_CHANGES" => AckStatus::Filled,
        "TRADE_RETCODE_PLACED"
        | "TRADE_RETCODE_DONE_PARTIAL"
        | "TRADE_RETCODE_DISCONNECTED_DURING_TRADE"
        | "TRADE_RETCODE_TIMEOUT"
        | "ERR_TRADE_TIMED_OUT"
        | "ERR_NO_RESULT"
        | "TRADE_RETCODE_UNKNOWN" => AckStatus::Unknown,
        _ => AckStatus::Rejected,
    };
    ExecutionAck {
        command_id: command.command_id,
        account_id: command.target_account_id,
        status: ack_status,
        external_id,
        timestamp_unix_ns: timestamp,
        message: value.get("message").and_then(Value::as_str).map(str::to_owned)
            .or_else(|| (!code.is_empty()).then(|| code.to_owned())),
    }
}

fn rejected(command: &ExecutionCommand, message: &str) -> ExecutionAck {
    ExecutionAck {
        command_id: command.command_id.clone(),
        account_id: command.target_account_id.clone(),
        status: AckStatus::Rejected,
        external_id: None,
        timestamp_unix_ns: unix_time_ns(),
        message: Some(message.to_owned()),
    }
}

fn metaapi_client_id(command_id: &str) -> String {
    let compact: String = command_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(20)
        .collect();
    let split_at = compact.len().min(10);
    let (left, right) = compact.split_at(split_at);
    let right = if right.is_empty() { "0" } else { right };
    format!("CR_{left}_{right}")
}

fn value_to_string(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        Some(value.to_owned())
    } else if let Some(value) = value.as_i64() {
        Some(value.to_string())
    } else {
        value.as_u64().map(|value| value.to_string())
    }
}

async fn fetch_positions(client: &Client, api_base: &str, account_id: &str, auth_token: &str) -> Result<Vec<PositionSnapshot>> {
    let url = format!("{}/users/current/accounts/{}/positions", api_base.trim_end_matches('/'), account_id);
    let response = client.get(url).header("auth-token", auth_token).send().await
        .context("MetaApi positions request failed")?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        bail!("MetaApi positions returned {status}: {text}");
    }
    response.json().await.context("invalid MetaApi positions response")
}

fn index_positions(positions: Vec<PositionSnapshot>) -> HashMap<String, PositionSnapshot> {
    positions.into_iter().map(|position| (position.id.clone(), position)).collect()
}

async fn emit_position_diffs(
    state: &Arc<AppState>,
    account: &AccountConfig,
    previous: &HashMap<String, PositionSnapshot>,
    current: &HashMap<String, PositionSnapshot>,
) -> Result<()> {
    for (id, position) in current {
        match previous.get(id) {
            None => {
                state.handle_frame(&account.id, AgentFrame::Event(to_event(account, position, TradeAction::Open, position.volume, Some(position.volume))?)).await?;
            }
            Some(old) if position.volume + f64::EPSILON < old.volume => {
                let delta = old.volume - position.volume;
                state.handle_frame(&account.id, AgentFrame::Event(to_event(account, position, TradeAction::Reduce, delta, Some(position.volume))?)).await?;
            }
            Some(old) if position.volume > old.volume + f64::EPSILON => {
                warn!(account = %account.id, position = %id, old_volume = old.volume, new_volume = position.volume, "MetaApi scale-in detected; direct copier currently leaves this position for reconciliation");
            }
            Some(old) if changed_price(old.stop_loss, position.stop_loss) || changed_price(old.take_profit, position.take_profit) => {
                state.handle_frame(&account.id, AgentFrame::Event(to_event(account, position, TradeAction::Modify, position.volume, Some(position.volume))?)).await?;
            }
            _ => {}
        }
    }
    for (id, old) in previous {
        if !current.contains_key(id) {
            let mut closed = old.clone();
            closed.volume = 0.0;
            state.handle_frame(&account.id, AgentFrame::Event(to_event(account, &closed, TradeAction::Close, old.volume, Some(0.0))?)).await?;
        }
    }
    Ok(())
}

fn to_event(
    account: &AccountConfig,
    position: &PositionSnapshot,
    action: TradeAction,
    volume: f64,
    remaining_volume: Option<f64>,
) -> Result<TradeEvent> {
    let side = match position.position_type.as_str() {
        "POSITION_TYPE_BUY" => Some(Side::Buy),
        "POSITION_TYPE_SELL" => Some(Side::Sell),
        other => bail!("unknown MetaApi position type {other}"),
    };
    let now = unix_time_ns();
    let origin_command_id = position.client_id.as_deref()
        .filter(|value| value.starts_with("CR_"))
        .map(str::to_owned);
    let event = TradeEvent {
        event_id: format!("metaapi:{}:{}:{}:{}", account.id, position.id, action, now),
        source_account_id: account.id.clone(),
        platform: account.platform,
        action,
        source_order_id: position.id.clone(),
        symbol: position.symbol.clone(),
        side,
        volume,
        remaining_volume,
        price: Some(position.open_price),
        stop_loss: position.stop_loss,
        take_profit: position.take_profit,
        timestamp_unix_ns: now,
        origin_command_id,
    };
    event.validate()?;
    Ok(event)
}

fn changed_price(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => (left - right).abs() > 1e-10,
        (None, None) => false,
        _ => true,
    }
}
