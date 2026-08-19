use crate::model::{ExecutionCommand, MirrorBinding, Side, TradeAction, TradeEvent};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SizingMode {
    Mirror,
    Fixed,
    Multiplier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteRule {
    pub id: String,
    pub source_account_id: String,
    pub target_account_id: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub sizing: SizingMode,
    #[serde(default = "default_one")]
    pub size_value: f64,
    #[serde(default)]
    pub reverse: bool,
    #[serde(default = "default_true")]
    pub copy_sl_tp: bool,
    #[serde(default)]
    pub min_volume: f64,
    #[serde(default = "default_max_volume")]
    pub max_volume: f64,
    #[serde(default = "default_volume_step")]
    pub volume_step: f64,
    #[serde(default)]
    pub allow_buy: bool,
    #[serde(default)]
    pub allow_sell: bool,
    #[serde(default)]
    pub allow_symbols: Vec<String>,
    #[serde(default)]
    pub deny_symbols: Vec<String>,
    #[serde(default)]
    pub symbol_map: HashMap<String, String>,
    #[serde(default)]
    pub symbol_prefix: String,
    #[serde(default)]
    pub symbol_suffix: String,
    #[serde(default)]
    pub max_event_age_ms: u64,
}

impl Default for SizingMode {
    fn default() -> Self {
        Self::Mirror
    }
}

impl RouteRule {
    pub fn validate(&self) -> Result<(), RouteError> {
        if self.id.is_empty() || self.source_account_id.is_empty() || self.target_account_id.is_empty() {
            return Err(RouteError::InvalidRule(self.id.clone()));
        }
        if self.source_account_id == self.target_account_id {
            return Err(RouteError::SelfRoute(self.id.clone()));
        }
        if self.max_volume <= 0.0 || self.volume_step <= 0.0 || self.min_volume < 0.0 {
            return Err(RouteError::InvalidVolumePolicy(self.id.clone()));
        }
        if self.sizing != SizingMode::Mirror && self.size_value <= 0.0 {
            return Err(RouteError::InvalidVolumePolicy(self.id.clone()));
        }
        Ok(())
    }

    fn allows(&self, event: &TradeEvent) -> bool {
        if !self.enabled {
            return false;
        }
        if !self.allow_symbols.is_empty() && !self.allow_symbols.iter().any(|s| s == &event.symbol) {
            return false;
        }
        if self.deny_symbols.iter().any(|s| s == &event.symbol) {
            return false;
        }
        match event.side {
            Some(Side::Buy) if self.allow_sell && !self.allow_buy => false,
            Some(Side::Sell) if self.allow_buy && !self.allow_sell => false,
            _ => true,
        }
    }

    fn target_symbol(&self, source: &str) -> String {
        if let Some(mapped) = self.symbol_map.get(source) {
            return mapped.clone();
        }
        format!("{}{}{}", self.symbol_prefix, source, self.symbol_suffix)
    }

    fn open_volume(&self, source_volume: f64) -> f64 {
        let raw = match self.sizing {
            SizingMode::Mirror => source_volume,
            SizingMode::Fixed => self.size_value,
            SizingMode::Multiplier => source_volume * self.size_value,
        };
        self.normalize_open_volume(raw)
    }

    fn normalize_open_volume(&self, raw: f64) -> f64 {
        let clamped = raw.clamp(self.min_volume, self.max_volume);
        normalize_step(clamped, self.volume_step)
    }

    fn normalize_reduce_volume(&self, raw: f64, remaining: f64) -> f64 {
        normalize_step(raw.min(remaining).max(0.0), self.volume_step)
    }
}

#[derive(Debug, Clone)]
pub struct CopyEngine {
    routes_by_source: HashMap<String, Vec<RouteRule>>,
}

impl CopyEngine {
    pub fn new(routes: Vec<RouteRule>) -> Result<Self, RouteError> {
        let mut routes_by_source: HashMap<String, Vec<RouteRule>> = HashMap::new();
        for route in routes {
            route.validate()?;
            routes_by_source
                .entry(route.source_account_id.clone())
                .or_default()
                .push(route);
        }
        Ok(Self { routes_by_source })
    }

    pub fn routes_for(&self, source_account_id: &str) -> &[RouteRule] {
        self.routes_by_source
            .get(source_account_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn build_command(
        &self,
        event: &TradeEvent,
        route: &RouteRule,
        mirror: Option<&MirrorBinding>,
        now_unix_ns: i64,
    ) -> Result<Option<ExecutionCommand>, RouteError> {
        if !route.allows(event) {
            return Ok(None);
        }
        if route.max_event_age_ms > 0 {
            let age_ns = now_unix_ns.saturating_sub(event.timestamp_unix_ns).max(0) as u64;
            if age_ns > route.max_event_age_ms.saturating_mul(1_000_000) {
                return Ok(None);
            }
        }

        let side = event.side.map(|side| if route.reverse { side.reversed() } else { side });
        let (target_order_id, volume) = match event.action {
            TradeAction::Open => (None, route.open_volume(event.volume)),
            TradeAction::Modify => {
                let binding = mirror.ok_or_else(|| RouteError::MissingMirror(route.id.clone()))?;
                (Some(binding.target_order_id.clone()), binding.target_remaining_volume)
            }
            TradeAction::Reduce => {
                let binding = mirror.ok_or_else(|| RouteError::MissingMirror(route.id.clone()))?;
                if binding.source_open_volume <= 0.0 {
                    return Err(RouteError::InvalidMirror(route.id.clone()));
                }
                let ratio = event.volume / binding.source_open_volume;
                let target_reduce = binding.target_open_volume * ratio;
                (
                    Some(binding.target_order_id.clone()),
                    route.normalize_reduce_volume(target_reduce, binding.target_remaining_volume),
                )
            }
            TradeAction::Close => {
                let binding = mirror.ok_or_else(|| RouteError::MissingMirror(route.id.clone()))?;
                (Some(binding.target_order_id.clone()), binding.target_remaining_volume)
            }
        };

        if matches!(event.action, TradeAction::Open | TradeAction::Reduce) && volume <= 0.0 {
            return Ok(None);
        }

        Ok(Some(ExecutionCommand {
            command_id: command_id(event, route),
            origin_event_id: event.event_id.clone(),
            route_id: route.id.clone(),
            source_account_id: event.source_account_id.clone(),
            source_order_id: event.source_order_id.clone(),
            target_account_id: route.target_account_id.clone(),
            action: event.action,
            target_order_id,
            symbol: route.target_symbol(&event.symbol),
            side,
            volume,
            source_volume: event.volume,
            source_remaining_volume: event.remaining_volume,
            price: event.price,
            stop_loss: route.copy_sl_tp.then_some(event.stop_loss).flatten(),
            take_profit: route.copy_sl_tp.then_some(event.take_profit).flatten(),
            created_unix_ns: now_unix_ns,
        }))
    }
}

fn command_id(event: &TradeEvent, route: &RouteRule) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"copierr:v1\0");
    hasher.update(event.event_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(route.id.as_bytes());
    hasher.update(b"\0");
    hasher.update(route.target_account_id.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(32);
    for byte in &digest[..16] {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn normalize_step(value: f64, step: f64) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    ((value / step).round() * step * 100_000_000.0).round() / 100_000_000.0
}

fn default_true() -> bool {
    true
}

fn default_one() -> f64 {
    1.0
}

fn default_max_volume() -> f64 {
    100_000.0
}

fn default_volume_step() -> f64 {
    0.01
}

#[derive(Debug, Error)]
pub enum RouteError {
    #[error("invalid route rule: {0}")]
    InvalidRule(String),
    #[error("route may not target its own source account: {0}")]
    SelfRoute(String),
    #[error("invalid volume policy on route: {0}")]
    InvalidVolumePolicy(String),
    #[error("mirror binding is missing for route: {0}")]
    MissingMirror(String),
    #[error("mirror binding is invalid for route: {0}")]
    InvalidMirror(String),
}
