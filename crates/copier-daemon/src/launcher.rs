use crate::config::{AccountConfig, DaemonConfig, EgressMode, EgressProfile, TerminalConfig};
use anyhow::{bail, Context, Result};
use std::sync::Arc;
use tokio::{process::Command, time::{sleep, Duration}};
use tracing::{error, info, warn};

pub fn spawn_configured_terminals(config: Arc<DaemonConfig>) {
    for account in config.accounts.clone() {
        let Some(terminal) = account.terminal.clone() else { continue; };
        if !terminal.enabled {
            continue;
        }
        let profile = terminal
            .egress_profile
            .as_deref()
            .and_then(|name| config.egress_profiles.get(name))
            .cloned();
        tokio::spawn(supervise(account, terminal, profile));
    }
}

async fn supervise(account: AccountConfig, terminal: TerminalConfig, profile: Option<EgressProfile>) {
    loop {
        match spawn_once(&account, &terminal, profile.as_ref()).await {
            Ok(status) => info!(account = %account.id, ?status, "terminal exited"),
            Err(error) => error!(account = %account.id, %error, "terminal launch failed"),
        }
        if !terminal.restart_on_exit {
            break;
        }
        warn!(account = %account.id, delay_ms = terminal.restart_delay_ms, "restarting terminal");
        sleep(Duration::from_millis(terminal.restart_delay_ms)).await;
    }
}

async fn spawn_once(
    account: &AccountConfig,
    terminal: &TerminalConfig,
    profile: Option<&EgressProfile>,
) -> Result<std::process::ExitStatus> {
    let mut command = build_command(terminal, profile)?;
    if let Some(dir) = &terminal.working_dir {
        command.current_dir(dir);
    }
    command.env("COPIERR_ACCOUNT_ID", &account.id);
    command.env("COPIERR_PLATFORM", account.platform.to_string());
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn terminal for {}", account.id))?;
    child.wait().await.context("failed waiting for terminal")
}

fn build_command(terminal: &TerminalConfig, profile: Option<&EgressProfile>) -> Result<Command> {
    match profile.map(|profile| profile.mode).unwrap_or(EgressMode::Direct) {
        EgressMode::Direct => {
            let mut command = Command::new(&terminal.command);
            command.args(&terminal.args);
            Ok(command)
        }
        EgressMode::ProxyEnv => {
            let profile = profile.expect("profile is present");
            let proxy = profile.proxy_url.as_deref().unwrap_or_default();
            let mut command = Command::new(&terminal.command);
            command.args(&terminal.args);
            command.env("ALL_PROXY", proxy);
            command.env("HTTP_PROXY", proxy);
            command.env("HTTPS_PROXY", proxy);
            if let Some(region) = &profile.region {
                command.env("COPIERR_EGRESS_REGION", region);
            }
            Ok(command)
        }
        EgressMode::NetworkNamespace => {
            #[cfg(unix)]
            {
                let profile = profile.expect("profile is present");
                let namespace = profile.namespace.as_deref().unwrap_or_default();
                let mut command = Command::new("ip");
                command.args(["netns", "exec", namespace, &terminal.command]);
                command.args(&terminal.args);
                if let Some(region) = &profile.region {
                    command.env("COPIERR_EGRESS_REGION", region);
                }
                Ok(command)
            }
            #[cfg(not(unix))]
            {
                let _ = profile;
                bail!("network_namespace egress is only available on Unix-like hosts")
            }
        }
    }
}
