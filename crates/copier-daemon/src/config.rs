use anyhow::{bail, Context, Result};
use copier_core::{CopyEngine, Platform, RouteRule};
use serde::Deserialize;
use std::{collections::{HashMap, HashSet}, fs, path::{Path, PathBuf}};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccountRole {
    Master,
    Follower,
    Both,
}

impl Default for AccountRole {
    fn default() -> Self {
        Self::Both
    }
}

impl AccountRole {
    pub fn can_publish(self) -> bool {
        matches!(self, Self::Master | Self::Both)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    None,
    Flush,
    Fsync,
}

impl Default for Durability {
    fn default() -> Self {
        Self::Flush
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EgressMode {
    Direct,
    ProxyEnv,
    NetworkNamespace,
}

impl Default for EgressMode {
    fn default() -> Self {
        Self::Direct
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct EgressProfile {
    #[serde(default)]
    pub mode: EgressMode,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub proxy_url: Option<String>,
    #[serde(default)]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TerminalConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub working_dir: Option<PathBuf>,
    #[serde(default)]
    pub egress_profile: Option<String>,
    #[serde(default)]
    pub restart_on_exit: bool,
    #[serde(default = "default_restart_delay_ms")]
    pub restart_delay_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountConfig {
    pub id: String,
    pub platform: Platform,
    #[serde(default)]
    pub role: AccountRole,
    pub token: String,
    #[serde(default)]
    pub allow_rebroadcast: bool,
    #[serde(default)]
    pub terminal: Option<TerminalConfig>,
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
    pub egress_profiles: HashMap<String, EgressProfile>,
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
            if account.id.is_empty() {
                bail!("account id may not be empty");
            }
            if account.token.is_empty() {
                bail!("account {} has an empty token", account.id);
            }
            if !ids.insert(account.id.clone()) {
                bail!("duplicate account id: {}", account.id);
            }
            if let Some(terminal) = &account.terminal {
                if terminal.enabled && terminal.command.is_empty() {
                    bail!("account {} has an empty terminal command", account.id);
                }
                if let Some(profile) = &terminal.egress_profile {
                    if !self.egress_profiles.contains_key(profile) {
                        bail!("account {} references unknown egress profile {}", account.id, profile);
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
        for (name, profile) in &self.egress_profiles {
            match profile.mode {
                EgressMode::Direct => {}
                EgressMode::ProxyEnv if profile.proxy_url.as_deref().unwrap_or("").is_empty() => {
                    bail!("egress profile {name} requires proxy_url");
                }
                EgressMode::NetworkNamespace if profile.namespace.as_deref().unwrap_or("").is_empty() => {
                    bail!("egress profile {name} requires namespace");
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn account(&self, id: &str) -> Option<&AccountConfig> {
        self.accounts.iter().find(|account| account.id == id)
    }
}

fn default_true() -> bool { true }
fn default_restart_delay_ms() -> u64 { 2_000 }
fn default_queue_capacity() -> usize { 2_048 }
fn default_listen() -> String { "127.0.0.1:48100".to_owned() }
fn default_journal_path() -> PathBuf { PathBuf::from("var/copier.journal.jsonl") }
