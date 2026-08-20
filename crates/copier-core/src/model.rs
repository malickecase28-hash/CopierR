use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    Trinity,
    Mt4,
    Mt5,
    #[serde(rename = "ctrader", alias = "c_trader")]
    CTrader,
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Trinity => "trinity",
            Self::Mt4 => "mt4",
            Self::Mt5 => "mt5",
            Self::CTrader => "ctrader",
        })
    }
}

impl FromStr for Platform {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "trinity" | "trinityr" => Ok(Self::Trinity),
            "mt4" => Ok(Self::Mt4),
            "mt5" => Ok(Self::Mt5),
            "ctrader" | "c_trader" => Ok(Self::CTrader),
            _ => Err(ModelError::InvalidPlatform(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn reversed(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        })
    }
}

impl FromStr for Side {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "buy" | "long" => Ok(Self::Buy),
            "sell" | "short" => Ok(Self::Sell),
            _ => Err(ModelError::InvalidSide(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeAction {
    Open,
    Modify,
    Reduce,
    Close,
}

impl fmt::Display for TradeAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Open => "open",
            Self::Modify => "modify",
            Self::Reduce => "reduce",
            Self::Close => "close",
        })
    }
}

impl FromStr for TradeAction {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "open" => Ok(Self::Open),
            "modify" => Ok(Self::Modify),
            "reduce" | "partial_close" => Ok(Self::Reduce),
            "close" => Ok(Self::Close),
            _ => Err(ModelError::InvalidAction(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeEvent {
    pub event_id: String,
    pub source_account_id: String,
    pub platform: Platform,
    pub action: TradeAction,
    pub source_order_id: String,
    pub symbol: String,
    pub side: Option<Side>,
    pub volume: f64,
    pub remaining_volume: Option<f64>,
    pub price: Option<f64>,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub timestamp_unix_ns: i64,
    pub origin_command_id: Option<String>,
}

impl TradeEvent {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.event_id.is_empty() {
            return Err(ModelError::Missing("event_id"));
        }
        if self.source_account_id.is_empty() {
            return Err(ModelError::Missing("source_account_id"));
        }
        if self.source_order_id.is_empty() {
            return Err(ModelError::Missing("source_order_id"));
        }
        if self.symbol.is_empty() {
            return Err(ModelError::Missing("symbol"));
        }
        if matches!(self.action, TradeAction::Open | TradeAction::Reduce) && self.volume <= 0.0 {
            return Err(ModelError::InvalidVolume(self.volume));
        }
        if self.action == TradeAction::Open && self.side.is_none() {
            return Err(ModelError::Missing("side"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionCommand {
    pub command_id: String,
    pub origin_event_id: String,
    pub route_id: String,
    pub source_account_id: String,
    pub source_order_id: String,
    pub target_account_id: String,
    pub action: TradeAction,
    pub target_order_id: Option<String>,
    pub symbol: String,
    pub side: Option<Side>,
    pub volume: f64,
    pub source_volume: f64,
    pub source_remaining_volume: Option<f64>,
    pub price: Option<f64>,
    pub stop_loss: Option<f64>,
    pub take_profit: Option<f64>,
    pub created_unix_ns: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckStatus {
    Accepted,
    Filled,
    Rejected,
    Unknown,
}

impl fmt::Display for AckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Accepted => "accepted",
            Self::Filled => "filled",
            Self::Rejected => "rejected",
            Self::Unknown => "unknown",
        })
    }
}

impl FromStr for AckStatus {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "accepted" => Ok(Self::Accepted),
            "filled" => Ok(Self::Filled),
            "rejected" => Ok(Self::Rejected),
            "unknown" => Ok(Self::Unknown),
            _ => Err(ModelError::InvalidAckStatus(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionAck {
    pub command_id: String,
    pub account_id: String,
    pub status: AckStatus,
    pub external_id: Option<String>,
    pub timestamp_unix_ns: i64,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MirrorBinding {
    pub route_id: String,
    pub source_account_id: String,
    pub source_order_id: String,
    pub target_account_id: String,
    pub target_order_id: String,
    pub source_open_volume: f64,
    pub source_remaining_volume: f64,
    pub target_open_volume: f64,
    pub target_remaining_volume: f64,
}

impl MirrorBinding {
    pub fn key(source_account_id: &str, source_order_id: &str, target_account_id: &str) -> String {
        format!("{source_account_id}\u{1f}{source_order_id}\u{1f}{target_account_id}")
    }

    pub fn binding_key(&self) -> String {
        Self::key(
            &self.source_account_id,
            &self.source_order_id,
            &self.target_account_id,
        )
    }
}

#[derive(Debug, Error)]
pub enum ModelError {
    #[error("missing required field: {0}")]
    Missing(&'static str),
    #[error("invalid platform: {0}")]
    InvalidPlatform(String),
    #[error("invalid side: {0}")]
    InvalidSide(String),
    #[error("invalid trade action: {0}")]
    InvalidAction(String),
    #[error("invalid acknowledgement status: {0}")]
    InvalidAckStatus(String),
    #[error("invalid volume: {0}")]
    InvalidVolume(f64),
}
