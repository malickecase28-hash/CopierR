mod config;
mod journal;
mod launcher;
mod runtime;
mod server;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::DaemonConfig;
use journal::Journal;
use runtime::AppState;
use std::{path::PathBuf, sync::Arc};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "copierr", version, about = "Low-latency multi-account trade copier daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Daemon {
        #[arg(short, long, default_value = "copierr.toml")]
        config: PathBuf,
    },
    Validate {
        #[arg(short, long, default_value = "copierr.toml")]
        config: PathBuf,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Validate { config } => {
            let config = DaemonConfig::load(config)?;
            info!(accounts = config.accounts.len(), routes = config.routes.len(), "configuration valid");
        }
        Command::Daemon { config } => {
            let config = Arc::new(DaemonConfig::load(config)?);
            let (journal, replay) = Journal::open(config.journal_path.clone(), config.durability).await?;
            let state = Arc::new(AppState::new(config.clone(), Arc::new(journal), replay)?);
            launcher::spawn_configured_terminals(config);
            tokio::select! {
                result = server::run(state) => result?,
                _ = tokio::signal::ctrl_c() => info!("shutdown signal received"),
            }
        }
    }
    Ok(())
}
