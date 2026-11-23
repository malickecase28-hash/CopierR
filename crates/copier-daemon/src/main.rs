mod config;
mod journal;
mod ops;
mod runtime;
mod server;
mod ui;
mod venues;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::DaemonConfig;
use journal::Journal;
use runtime::AppState;
use std::{path::PathBuf, sync::Arc};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "copierr", version, about = "Linux-first low-latency multi-account trade copier")]
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
    Ui {
        #[arg(short, long, default_value = "127.0.0.1:48200")]
        listen: String,
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
            let listener = server::bind(&state).await?;
            let operations = ops::bind(state.clone()).await?;
            let notifications = ops::notifications(state.clone())?;
            venues::spawn(state.clone())?;
            tokio::select! {
                result = server::run(state.clone(), listener) => result?,
                result = operations => result?,
                result = notifications => result?,
                result = state.maintain() => result?,
                result = shutdown() => { result?; info!("shutdown signal received"); }
            }
        }
        Command::Ui { listen } => ui::run(&listen).await?,
    }
    Ok(())
}

async fn shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
