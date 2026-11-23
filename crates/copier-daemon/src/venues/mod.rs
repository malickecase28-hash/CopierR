mod ctrader;
mod metaapi;

use crate::{config::{read_secret, AccountBackend, CTraderEnvironment}, runtime::AppState};
use anyhow::Result;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::warn;

pub fn spawn(state: Arc<AppState>) -> Result<()> {
    let mut ctrader_live = Vec::new();
    let mut ctrader_demo = Vec::new();

    for account in state.config.accounts.clone() {
        match account.backend {
            AccountBackend::Agent => {}
            AccountBackend::CTraderOpenApi => {
                let environment = account.ctrader.as_ref().expect("validated cTrader config").environment;
                match environment {
                    CTraderEnvironment::Live => ctrader_live.push(account),
                    CTraderEnvironment::Demo => ctrader_demo.push(account),
                }
            }
            AccountBackend::MetaApi => {
                let global = state.config.metaapi.as_ref().expect("validated MetaApi config").clone();
                let auth_token = read_secret(&global.auth_token_env)?;
                let state = state.clone();
                tokio::spawn(async move {
                    let mut delay = ReconnectDelay::default();
                    loop {
                        let started = std::time::Instant::now();
                        let result = metaapi::run_account(state.clone(), account.clone(), global.clone(), auth_token.clone()).await;
                        warn!(account = %account.id, error = ?result.err(), "MetaApi venue disconnected; reconnecting");
                        delay.wait(started.elapsed()).await;
                    }
                });
            }
        }
    }

    if !ctrader_live.is_empty() {
        ctrader::spawn_hub(state.clone(), CTraderEnvironment::Live, ctrader_live)?;
    }
    if !ctrader_demo.is_empty() {
        ctrader::spawn_hub(state, CTraderEnvironment::Demo, ctrader_demo)?;
    }
    Ok(())
}

#[derive(Default)]
struct ReconnectDelay(u64);

impl ReconnectDelay {
    async fn wait(&mut self, uptime: Duration) {
        self.0 = if uptime >= Duration::from_secs(60) { 1 } else { (self.0 * 2).clamp(1, 30) };
        sleep(Duration::from_secs(self.0)).await;
    }
}
