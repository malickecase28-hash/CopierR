use anyhow::{bail, Context, Result};
use copier_core::{CopyEngine, Platform, RouteRule};
use serde::Deserialize;
use std::{collections::HashSet, fs, path::{Path, PathBuf}};

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    None,
    #[default]
    Flush,
    Fsync,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccountBackend {
    #[default]
    Agent,
    CTraderOpenApi,
    MetaApi,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CTraderEnvironment {
    #[default]
    Live,
    Demo,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CTraderGlobalConfig {
    pub client_id_env: String,
    pub client_secret_env: String,
    #[serde(default = "default_ctrader_live_url")]
    pub live_url: String,
    #[serde(default = "default_ctrader_demo_url")]
    pub demo_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CTraderAccountConfig {
    pub ctid_trader_account_id: i64,
    pub access_token_env: String,
    #[serde(default)]
    pub environment: CTraderEnvironment,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetaApiGlobalConfig {
    pub auth_token_env: String,
    #[serde(default = "default_metaapi_provisioning_base")]
    pub provisioning_base: String,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
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
    pub allow_rebroadcast: bool,
    #[serde(default)]
    pub ctrader: Option<CTraderAccountConfig>,
    #[serde(default)]
    pub metaapi: Option<MetaApiAccountConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DaemonConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_journal_path")]
    pub journal_path: PathBuf,
    #[serde(default)]
    pub durability: Durability,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
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
        if self.queue_capacity == 0 {
            bail!("queue_capacity must be positive");
        }
        let mut ids = HashSet::new();
        for account in &self.accounts {
            if account.id.trim().is_empty() {
                bail!("account id may not be empty");
            }
            if !ids.insert(account.id.clone()) {
                bail!("duplicate account id: {}", account.id);
            }
            match account.backend {
                AccountBackend::Agent => {
                    if account.token.is_empty() {
                        bail!("agent account {} requires token", account.id);
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
        for route in &self.routes {
            if !ids.contains(&route.source_account_id) {
                bail!("route {} references unknown source account {}", route.id, route.source_account_id);
            }
            if !ids.contains(&route.target_account_id) {
                bail!("route {} references unknown target account {}", route.id, route.target_account_id);
            }
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

pub fn read_secret(env_name: &str) -> Result<String> {
    let value = std::env::var(env_name)
        .with_context(|| format!("required environment variable {env_name} is not set"))?;
    if value.is_empty() {
        bail!("required environment variable {env_name} is empty");
    }
    Ok(value)
}

fn default_queue_capacity() -> usize { 2_048 }
fn default_listen() -> String { "127.0.0.1:48100".to_owned() }
fn default_journal_path() -> PathBuf { PathBuf::from("var/copier.journal.jsonl") }
fn default_ctrader_live_url() -> String { "wss://live.ctraderapi.com:5036".to_owned() }
fn default_ctrader_demo_url() -> String { "wss://demo.ctraderapi.com:5036".to_owned() }
fn default_metaapi_provisioning_base() -> String { "https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai".to_owned() }
fn default_metaapi_region() -> String { "new-york".to_owned() }
fn default_metaapi_poll_ms() -> u64 { 100 }
fn default_magic() -> u64 { 8_104_811 }
