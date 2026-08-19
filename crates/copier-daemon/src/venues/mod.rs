mod ctrader;
mod metaapi;

use crate::{config::{read_secret, AccountBackend, CTraderEnvironment}, runtime::AppState};
use anyhow::Result;
use std::sync::Arc;
use tracing::error;

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
                    if let Err(err) = metaapi::run_account(state, account, global, auth_token).await {
                        error!(error = %err, "MetaApi venue stopped");
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
