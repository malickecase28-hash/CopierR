use anyhow::{bail, Context, Result};
use copier_core::{CopyEngine, Platform, RouteRule};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fs, path::{Path, PathBuf}};

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountRole {
    Master,
    Follower,
    #[default]
    Both,
}

impl AccountRole {
    pub fn can_publish(self) -> bool {
        matches!(self, Self::Master | Self::Both)
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    None,
    #[default]
    Flush,
    Fsync,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountBackend {
    #[default]
    Agent,
    CTraderOpenApi,
    MetaApi,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CTraderEnvironment {
    #[default]
    Live,
    Demo,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CTraderGlobalConfig {
    pub client_id_env: String,
    pub client_secret_env: String,
    #[serde(default = "default_ctrader_live_url")]
    pub live_url: String,
    #[serde(default = "default_ctrader_demo_url")]
    pub demo_url: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CTraderAccountConfig {
    pub ctid_trader_account_id: i64,
    pub access_token_env: String,
    #[serde(default)]
    pub environment: CTraderEnvironment,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetaApiGlobalConfig {
    pub auth_token_env: String,
    #[serde(default = "default_metaapi_provisioning_base")]
    pub provisioning_base: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetaApiAccountConfig {
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub login: Option<String>,
    #[serde(default)]
    pub server: Option<String>,
    #[serde(default)]
    pub password_env: Option<String>,
    #[serde(default = "default_metaapi_region")]
    pub region: String,
    #[serde(default)]
    pub api_base: Option<String>,
    #[serde(default = "default_metaapi_poll_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_magic")]
    pub magic: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountConfig {
    pub id: String,
    pub platform: Platform,
    #[serde(default)]
    pub role: AccountRole,
    #[serde(default)]
    pub backend: AccountBackend,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub token_env: Option<String>,
    #[serde(default)]
    pub allow_rebroadcast: bool,
    #[serde(default)]
    pub ctrader: Option<CTraderAccountConfig>,
    #[serde(default)]
    pub metaapi: Option<MetaApiAccountConfig>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct FollowerRisk {
    pub max_open_positions: Option<usize>,
    pub max_total_lots: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DaemonConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    /// The agent port speaks plain TCP: tokens and trade commands are not encrypted. It must stay on loopback unless this is set,
    /// which is meant for an address that is already encrypted end to end (a Tailscale/WireGuard interface or an SSH tunnel).
    #[serde(default)]
    pub allow_remote_agents: bool,
    #[serde(default = "default_journal_path")]
    pub journal_path: PathBuf,
    #[serde(default)]
    pub durability: Durability,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_backlog_capacity")]
    pub backlog_capacity: usize,
    #[serde(default = "default_ack_timeout_ms")]
    pub ack_timeout_ms: u64,
    #[serde(default = "default_dispatch_age_ms")]
    pub max_dispatch_age_ms: u64,
    #[serde(default = "default_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,
    #[serde(default)]
    pub event_retention_secs: u64,
    #[serde(default)]
    pub ops_listen: Option<std::net::SocketAddr>,
    #[serde(default)]
    pub ops_token_env: Option<String>,
    #[serde(default)]
    pub alerts_url_env: Option<String>,
    #[serde(default)]
    pub alerts_token_env: Option<String>,
    #[serde(default)]
    pub follower_risk: std::collections::HashMap<String, FollowerRisk>,
    #[serde(default)]
    pub pending_routes: HashSet<String>,
    #[serde(default)]
    pub accounts: Vec<AccountConfig>,
    #[serde(default)]
    pub routes: Vec<RouteRule>,
    #[serde(default)]
    pub ctrader: Option<CTraderGlobalConfig>,
    #[serde(default)]
    pub metaapi: Option<MetaApiGlobalConfig>,
}

impl DaemonConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let config: Self = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.queue_capacity == 0 || self.backlog_capacity == 0 || self.ack_timeout_ms == 0 || self.max_dispatch_age_ms == 0 {
            bail!("queue_capacity, backlog_capacity, ack_timeout_ms and max_dispatch_age_ms must be positive");
        }
        if !self.allow_remote_agents && !is_loopback_listen(&self.listen) {
            bail!(
                "listen {} is not a loopback address: agent tokens and trade commands travel unencrypted. Keep it on 127.0.0.1 and                  reach remote agents through an SSH or Tailscale tunnel, or bind it to the tunnel's address and set                  allow_remote_agents = true",
                self.listen
            );
        }
        if self.ops_listen.is_some_and(|address| !address.ip().is_loopback()) {
            bail!("ops_listen must be a loopback address");
        }
        if self.ops_listen.is_some() && self.ops_token_env.as_deref().unwrap_or("").trim().is_empty() {
            bail!("ops_listen requires ops_token_env");
        }
        if self.alerts_token_env.is_some() && self.alerts_url_env.is_none() {
            bail!("alerts_token_env requires alerts_url_env");
        }
        let mut ids = HashSet::new();
        let mut ctrader_ids = HashSet::new();
        let mut metaapi_ids = HashSet::new();
        for account in &self.accounts {
            if account.id.trim().is_empty() {
                bail!("account id may not be empty");
            }
            if !ids.insert(account.id.clone()) {
                bail!("duplicate account id: {}", account.id);
            }
            match account.backend {
                AccountBackend::Agent => {
                    if account.token.is_empty()
                        && account.token_env.as_deref().unwrap_or("").trim().is_empty()
                    {
                        bail!("agent account {} requires token or token_env", account.id);
                    }
                }
                AccountBackend::CTraderOpenApi => {
                    if account.platform != Platform::CTrader {
                        bail!("cTrader Open API account {} must use platform=ctrader", account.id);
                    }
                    if self.ctrader.is_none() {
                        bail!("cTrader Open API account {} requires global [ctrader] config", account.id);
                    }
                    let direct = account.ctrader.as_ref()
                        .ok_or_else(|| anyhow::anyhow!("account {} requires ctrader settings", account.id))?;
                    if direct.ctid_trader_account_id <= 0 || direct.access_token_env.trim().is_empty() {
                        bail!("account {} has invalid cTrader direct settings", account.id);
                    }
                    if !ctrader_ids.insert((direct.environment == CTraderEnvironment::Live, direct.ctid_trader_account_id)) {
                        bail!("cTrader broker account is configured more than once");
                    }
                }
                AccountBackend::MetaApi => {
                    if !matches!(account.platform, Platform::Mt4 | Platform::Mt5) {
                        bail!("MetaApi account {} must use platform=mt4 or platform=mt5", account.id);
                    }
                    if self.metaapi.is_none() {
                        bail!("MetaApi account {} requires global [metaapi] config", account.id);
                    }
                    let direct = account.metaapi.as_ref()
                        .ok_or_else(|| anyhow::anyhow!("account {} requires metaapi settings", account.id))?;
                    if direct.poll_interval_ms < 25 {
                        bail!("account {} metaapi poll_interval_ms must be at least 25", account.id);
                    }
                    if let Some(id) = direct.account_id.as_deref().filter(|id| !id.is_empty()) {
                        if !metaapi_ids.insert(id) { bail!("MetaApi broker account is configured more than once"); }
                    }
                    if direct.account_id.as_deref().unwrap_or("").is_empty()
                        && (direct.login.as_deref().unwrap_or("").is_empty()
                            || direct.server.as_deref().unwrap_or("").is_empty()
                            || direct.password_env.as_deref().unwrap_or("").is_empty())
                    {
                        bail!("account {} needs metaapi.account_id or login/server/password_env for provisioning", account.id);
                    }
                }
            }
        }
        for (id, risk) in &self.follower_risk {
            if !ids.contains(id) || risk.max_open_positions == Some(0)
                || risk.max_total_lots.is_some_and(|lots| !lots.is_finite() || lots <= 0.0)
            {
                bail!("invalid follower_risk configuration for {id}");
            }
        }
        let mut route_ids = HashSet::new();
        let mut route_pairs = HashSet::new();
        for route in &self.routes {
            if !route_ids.insert(&route.id) || (route.enabled && !route_pairs.insert((&route.source_account_id, &route.target_account_id))) {
                bail!("duplicate route id or enabled source/target pair on {}", route.id);
            }
            if !ids.contains(&route.source_account_id) {
                bail!("route {} references unknown source account {}", route.id, route.source_account_id);
            }
            if !ids.contains(&route.target_account_id) {
                bail!("route {} references unknown target account {}", route.id, route.target_account_id);
            }
            if self.pending_routes.contains(&route.id) && route.reverse {
                bail!("pending route {} cannot reverse side while preserving the trigger price", route.id);
            }
        }
        if self.pending_routes.iter().any(|id| !route_ids.contains(id)) {
            bail!("pending_routes references an unknown route");
        }
        CopyEngine::new(self.routes.clone()).context("invalid routing configuration")?;
        if let Some(ctrader) = &self.ctrader {
            if ctrader.client_id_env.trim().is_empty() || ctrader.client_secret_env.trim().is_empty() {
                bail!("ctrader client credential environment variable names may not be empty");
            }
        }
        if let Some(metaapi) = &self.metaapi {
            if metaapi.auth_token_env.trim().is_empty() {
                bail!("metaapi auth_token_env may not be empty");
            }
        }
        Ok(())
    }

    pub fn account(&self, id: &str) -> Option<&AccountConfig> {
        self.accounts.iter().find(|account| account.id == id)
    }
}

/// `127.0.0.1:48100`, `[::1]:48100` or `localhost:48100`.
pub fn is_loopback_listen(listen: &str) -> bool {
    if let Ok(address) = listen.parse::<std::net::SocketAddr>() {
        return address.ip().is_loopback();
    }
    listen.rsplit_once(':').is_some_and(|(host, _)| host.eq_ignore_ascii_case("localhost"))
}

pub fn read_secret(env_name: &str) -> Result<String> {
    let value = std::env::var(env_name)
        .with_context(|| format!("required environment variable {env_name} is not set"))?;
    if value.is_empty() {
        bail!("required environment variable {env_name} is empty");
    }
    Ok(value)
}

fn default_queue_capacity() -> usize { 2_048 }
fn default_backlog_capacity() -> usize { 10_000 }
fn default_ack_timeout_ms() -> u64 { 75_000 }
fn default_dispatch_age_ms() -> u64 { 30_000 }
fn default_checkpoint_interval_secs() -> u64 { 3_600 }
fn default_listen() -> String { "127.0.0.1:48100".to_owned() }
fn default_journal_path() -> PathBuf { PathBuf::from("var/copier.journal.jsonl") }
fn default_ctrader_live_url() -> String { "wss://live.ctraderapi.com:5036".to_owned() }
fn default_ctrader_demo_url() -> String { "wss://demo.ctraderapi.com:5036".to_owned() }
fn default_metaapi_provisioning_base() -> String { "https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai".to_owned() }
fn default_metaapi_region() -> String { "new-york".to_owned() }
fn default_metaapi_poll_ms() -> u64 { 100 }
fn default_magic() -> u64 { 8_104_811 }

#[cfg(test)]
mod tests {
    use super::*;

    fn with_listen(listen: &str, allow: bool) -> DaemonConfig {
        toml::from_str(&format!(
            "listen = \"{listen}\"\nallow_remote_agents = {allow}\n[[accounts]]\nid = \"a\"\nplatform = \"trinity\"\ntoken = \"t\"\n"
        ))
        .unwrap()
    }

    #[test]
    fn agent_port_stays_on_loopback_unless_remote_agents_are_allowed() {
        for listen in ["127.0.0.1:48100", "[::1]:48100", "localhost:48100"] {
            assert!(with_listen(listen, false).validate().is_ok(), "{listen}");
        }
        for listen in ["0.0.0.0:48100", "100.64.0.7:48100", "server.example:48100"] {
            let error = with_listen(listen, false).validate().expect_err(listen);
            assert!(error.to_string().contains("not a loopback address"), "{error}");
            assert!(with_listen(listen, true).validate().is_ok(), "{listen} with allow_remote_agents");
        }
    }
}
