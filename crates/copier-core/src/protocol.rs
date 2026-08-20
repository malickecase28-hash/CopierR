use crate::model::{AckStatus, ExecutionAck, ExecutionCommand, Platform, Side, TradeAction, TradeEvent};
use std::str::FromStr;
use thiserror::Error;

pub const WIRE_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloFrame {
    pub account_id: String,
    pub platform: Platform,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AgentFrame {
    Hello(HelloFrame),
    Event(TradeEvent),
    Ack(ExecutionAck),
    Ping(i64),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServerFrame {
    Welcome { server_time_unix_ns: i64 },
    Command(Box<ExecutionCommand>),
    Error { code: String, message: String },
    Pong { server_time_unix_ns: i64 },
}

pub fn parse_agent_line(line: &str) -> Result<AgentFrame, WireError> {
    let fields: Vec<&str> = strip_line_end(line).split('\t').collect();
    let kind = fields.first().copied().ok_or(WireError::Empty)?;
    match kind {
        "HELLO" => {
            require_len(&fields, 5, "HELLO")?;
            require_version(fields[1])?;
            Ok(AgentFrame::Hello(HelloFrame {
                account_id: required(fields[2], "account_id")?.to_owned(),
                platform: Platform::from_str(fields[3])?,
                token: fields[4].to_owned(),
            }))
        }
        "EVENT" => {
            require_len(&fields, 16, "EVENT")?;
            require_version(fields[1])?;
            let event = TradeEvent {
                event_id: required(fields[2], "event_id")?.to_owned(),
                source_account_id: required(fields[3], "source_account_id")?.to_owned(),
                platform: Platform::from_str(fields[4])?,
                action: TradeAction::from_str(fields[5])?,
                source_order_id: required(fields[6], "source_order_id")?.to_owned(),
                symbol: required(fields[7], "symbol")?.to_owned(),
                side: parse_side(fields[8])?,
                volume: parse_f64(fields[9], "volume")?,
                remaining_volume: parse_opt_f64(fields[10], "remaining_volume")?,
                price: parse_opt_f64(fields[11], "price")?,
                stop_loss: parse_opt_f64(fields[12], "stop_loss")?,
                take_profit: parse_opt_f64(fields[13], "take_profit")?,
                timestamp_unix_ns: parse_i64(fields[14], "timestamp_unix_ns")?,
                origin_command_id: optional_string(fields[15]),
            };
            event.validate()?;
            Ok(AgentFrame::Event(event))
        }
        "ACK" => {
            require_len(&fields, 8, "ACK")?;
            require_version(fields[1])?;
            Ok(AgentFrame::Ack(ExecutionAck {
                command_id: required(fields[2], "command_id")?.to_owned(),
                account_id: required(fields[3], "account_id")?.to_owned(),
                status: AckStatus::from_str(fields[4])?,
                external_id: optional_string(fields[5]),
                timestamp_unix_ns: parse_i64(fields[6], "timestamp_unix_ns")?,
                message: optional_string(fields[7]),
            }))
        }
        "PING" => {
            require_len(&fields, 3, "PING")?;
            require_version(fields[1])?;
            Ok(AgentFrame::Ping(parse_i64(fields[2], "timestamp_unix_ns")?))
        }
        other => Err(WireError::UnknownFrame(other.to_owned())),
    }
}

pub fn parse_server_line(line: &str) -> Result<ServerFrame, WireError> {
    let fields: Vec<&str> = strip_line_end(line).split('\t').collect();
    let kind = fields.first().copied().ok_or(WireError::Empty)?;
    match kind {
        "WELCOME" => {
            require_len(&fields, 3, "WELCOME")?;
            require_version(fields[1])?;
            Ok(ServerFrame::Welcome {
                server_time_unix_ns: parse_i64(fields[2], "server_time_unix_ns")?,
            })
        }
        "COMMAND" => {
            require_len(&fields, 18, "COMMAND")?;
            require_version(fields[1])?;
            Ok(ServerFrame::Command(Box::new(ExecutionCommand {
                command_id: required(fields[2], "command_id")?.to_owned(),
                origin_event_id: required(fields[3], "origin_event_id")?.to_owned(),
                route_id: required(fields[4], "route_id")?.to_owned(),
                source_account_id: required(fields[5], "source_account_id")?.to_owned(),
                source_order_id: required(fields[6], "source_order_id")?.to_owned(),
                target_account_id: required(fields[7], "target_account_id")?.to_owned(),
                action: TradeAction::from_str(fields[8])?,
                target_order_id: optional_string(fields[9]),
                symbol: required(fields[10], "symbol")?.to_owned(),
                side: parse_side(fields[11])?,
                volume: parse_f64(fields[12], "volume")?,
                source_volume: parse_f64(fields[13], "source_volume")?,
                source_remaining_volume: parse_opt_f64(fields[14], "source_remaining_volume")?,
                price: parse_opt_f64(fields[15], "price")?,
                stop_loss: parse_opt_f64(fields[16], "stop_loss")?,
                take_profit: parse_opt_f64(fields[17], "take_profit")?,
                created_unix_ns: 0,
            })))
        }
        "ERROR" => {
            require_len(&fields, 4, "ERROR")?;
            require_version(fields[1])?;
            Ok(ServerFrame::Error {
                code: fields[2].to_owned(),
                message: fields[3].to_owned(),
            })
        }
        "PONG" => {
            require_len(&fields, 3, "PONG")?;
            require_version(fields[1])?;
            Ok(ServerFrame::Pong {
                server_time_unix_ns: parse_i64(fields[2], "server_time_unix_ns")?,
            })
        }
        other => Err(WireError::UnknownFrame(other.to_owned())),
    }
}

pub fn encode_agent_frame(frame: &AgentFrame) -> String {
    match frame {
        AgentFrame::Hello(hello) => format!(
            "HELLO\t{}\t{}\t{}\t{}\n",
            WIRE_VERSION,
            field(&hello.account_id),
            hello.platform,
            field(&hello.token)
        ),
        AgentFrame::Event(event) => format!(
            "EVENT\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            WIRE_VERSION,
            field(&event.event_id),
            field(&event.source_account_id),
            event.platform,
            event.action,
            field(&event.source_order_id),
            field(&event.symbol),
            opt_side(event.side),
            event.volume,
            opt_f64(event.remaining_volume),
            opt_f64(event.price),
            opt_f64(event.stop_loss),
            opt_f64(event.take_profit),
            event.timestamp_unix_ns,
            opt_str(event.origin_command_id.as_deref()),
        ),
        AgentFrame::Ack(ack) => format!(
            "ACK\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            WIRE_VERSION,
            field(&ack.command_id),
            field(&ack.account_id),
            ack.status,
            opt_str(ack.external_id.as_deref()),
            ack.timestamp_unix_ns,
            opt_str(ack.message.as_deref()),
        ),
        AgentFrame::Ping(ts) => format!("PING\t{}\t{}\n", WIRE_VERSION, ts),
    }
}

pub fn encode_server_frame(frame: &ServerFrame) -> String {
    match frame {
        ServerFrame::Welcome { server_time_unix_ns } => {
            format!("WELCOME\t{}\t{}\n", WIRE_VERSION, server_time_unix_ns)
        }
        ServerFrame::Command(command) => format!(
            "COMMAND\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            WIRE_VERSION,
            field(&command.command_id),
            field(&command.origin_event_id),
            field(&command.route_id),
            field(&command.source_account_id),
            field(&command.source_order_id),
            field(&command.target_account_id),
            command.action,
            opt_str(command.target_order_id.as_deref()),
            field(&command.symbol),
            opt_side(command.side),
            command.volume,
            command.source_volume,
            opt_f64(command.source_remaining_volume),
            opt_f64(command.price),
            opt_f64(command.stop_loss),
            opt_f64(command.take_profit),
        ),
        ServerFrame::Error { code, message } => format!(
            "ERROR\t{}\t{}\t{}\n",
            WIRE_VERSION,
            field(code),
            field(message)
        ),
        ServerFrame::Pong { server_time_unix_ns } => {
            format!("PONG\t{}\t{}\n", WIRE_VERSION, server_time_unix_ns)
        }
    }
}

fn strip_line_end(value: &str) -> &str {
    value.trim_end_matches(['\r', '\n'])
}

fn require_len(fields: &[&str], expected: usize, kind: &str) -> Result<(), WireError> {
    if fields.len() == expected {
        Ok(())
    } else {
        Err(WireError::FieldCount {
            kind: kind.to_owned(),
            expected,
            actual: fields.len(),
        })
    }
}

fn require_version(value: &str) -> Result<(), WireError> {
    let version = value
        .parse::<u16>()
        .map_err(|_| WireError::InvalidNumber("version", value.to_owned()))?;
    if version == WIRE_VERSION {
        Ok(())
    } else {
        Err(WireError::UnsupportedVersion(version))
    }
}

fn required<'a>(value: &'a str, name: &'static str) -> Result<&'a str, WireError> {
    if value.is_empty() {
        Err(WireError::Missing(name))
    } else {
        Ok(value)
    }
}

fn optional_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

fn parse_side(value: &str) -> Result<Option<Side>, WireError> {
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Side::from_str(value)?))
    }
}

fn parse_f64(value: &str, field: &'static str) -> Result<f64, WireError> {
    value
        .parse::<f64>()
        .map_err(|_| WireError::InvalidNumber(field, value.to_owned()))
}

fn parse_opt_f64(value: &str, field: &'static str) -> Result<Option<f64>, WireError> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_f64(value, field).map(Some)
    }
}

fn parse_i64(value: &str, field: &'static str) -> Result<i64, WireError> {
    value
        .parse::<i64>()
        .map_err(|_| WireError::InvalidNumber(field, value.to_owned()))
}

fn field(value: &str) -> String {
    value.replace(['\t', '\r', '\n'], " ")
}

fn opt_str(value: Option<&str>) -> String {
    value.map(field).unwrap_or_default()
}

fn opt_side(value: Option<Side>) -> String {
    value.map(|side| side.to_string()).unwrap_or_default()
}

fn opt_f64(value: Option<f64>) -> String {
    value.map(|number| number.to_string()).unwrap_or_default()
}

#[derive(Debug, Error)]
pub enum WireError {
    #[error("empty wire frame")]
    Empty,
    #[error("unknown wire frame: {0}")]
    UnknownFrame(String),
    #[error("unsupported wire protocol version: {0}")]
    UnsupportedVersion(u16),
    #[error("{kind} expected {expected} fields, got {actual}")]
    FieldCount {
        kind: String,
        expected: usize,
        actual: usize,
    },
    #[error("missing required wire field: {0}")]
    Missing(&'static str),
    #[error("invalid numeric field {0}: {1}")]
    InvalidNumber(&'static str, String),
    #[error(transparent)]
    Model(#[from] crate::model::ModelError),
}
