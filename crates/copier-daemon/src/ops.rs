use crate::{
    config::read_secret,
    runtime::{token_matches, unix_time_ns, AppState, MirrorReview},
};
use anyhow::{Context, Result};
use axum::{
    extract::{DefaultBodyLimit, Path, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, put},
    Json, Router,
};
use copier_core::{AckStatus, ExecutionAck};
use serde::Deserialize;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::net::TcpListener;

#[derive(Clone)]
struct Operations {
    app: Arc<AppState>,
    token: Arc<str>,
}

impl Operations {
    fn authenticate(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        match headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        {
            Some(token) if token_matches(&self.token, token) => Ok(()),
            _ => Err(StatusCode::UNAUTHORIZED),
        }
    }
}

pub async fn bind(app: Arc<AppState>) -> Result<impl Future<Output = Result<()>>> {
    let listener = match app.config.ops_listen {
        Some(address) => Some(
            TcpListener::bind(address)
                .await
                .context("failed to bind operations API")?,
        ),
        None => None,
    };
    let router = if listener.is_some() {
        Some(router(app)?)
    } else {
        None
    };
    Ok(async move {
        match (listener, router) {
            (Some(listener), Some(router)) => axum::serve(listener, router)
                .await
                .context("operations API stopped"),
            _ => std::future::pending().await,
        }
    })
}

fn router(app: Arc<AppState>) -> Result<Router> {
    let token = read_secret(
        app.config
            .ops_token_env
            .as_deref()
            .context("missing operations token setting")?,
    )?;
    Ok(Router::new()
        .route("/v1/status", get(status))
        .route("/v1/accounts/:account_id/detach-check", get(detach_check))
        .route("/v1/pause", put(pause))
        .route("/v1/hold", put(hold))
        .route("/v1/resolve", put(resolve))
        .route("/v1/reconcile", put(reconcile))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(Operations {
            app,
            token: Arc::from(token),
        }))
}

pub fn notifications(app: Arc<AppState>) -> Result<impl Future<Output = Result<()>>> {
    let url = app
        .config
        .alerts_url_env
        .as_deref()
        .map(|name| {
            let url = reqwest::Url::parse(&read_secret(name)?)
                .map_err(|_| anyhow::anyhow!("invalid CopierR alerts URL"))?;
            anyhow::ensure!(
                url.scheme() == "https"
                    && url.host().is_some()
                    && url.username().is_empty()
                    && url.password().is_none(),
                "CopierR alerts require an HTTPS URL without embedded credentials"
            );
            Ok::<_, anyhow::Error>(url)
        })
        .transpose()?;
    let token = app
        .config
        .alerts_token_env
        .as_deref()
        .map(read_secret)
        .transpose()?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()?;
    Ok(async move {
        let Some(url) = url else {
            return std::future::pending().await;
        };
        let mut delivered = Vec::new();
        let mut delay = 2;
        loop {
            let alerts = app.alerts().await;
            if alerts != delivered {
                let mut request = client
                    .post(url.clone())
                    .json(&serde_json::json!({"service": "copierr", "alerts": alerts}));
                if let Some(token) = token.as_deref() {
                    request = request.bearer_auth(token);
                }
                match request.send().await {
                    Ok(response) if response.status().is_success() => {
                        delivered = alerts;
                        delay = 2;
                    }
                    _ => {
                        tracing::warn!(retry_in_secs = delay, "CopierR alert delivery failed");
                        delay = (delay * 2).min(60);
                    }
                }
            } else {
                delay = 2;
            }
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
    })
}

async fn reconcile(
    State(ops): State<Operations>,
    headers: HeaderMap,
    Json(body): Json<MirrorReview>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize(&ops, &headers)?;
    ops.app.review_mirror(body).await.map_err(operation_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn status(
    State(ops): State<Operations>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    ops.authenticate(&headers)?;
    Ok(Json(ops.app.status().await))
}

async fn detach_check(
    State(ops): State<Operations>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    ops.authenticate(&headers)?;
    ops.app
        .detach_check(&account_id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

#[derive(Deserialize)]
struct Pause {
    paused: bool,
}

async fn pause(
    State(ops): State<Operations>,
    headers: HeaderMap,
    Json(body): Json<Pause>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize(&ops, &headers)?;
    ops.app
        .set_paused(body.paused)
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct Hold {
    account_id: String,
    reason: Option<String>,
}

async fn hold(
    State(ops): State<Operations>,
    headers: HeaderMap,
    Json(body): Json<Hold>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize(&ops, &headers)?;
    if body
        .reason
        .as_deref()
        .is_some_and(|reason| reason.trim().is_empty())
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "hold requires a reason or null to clear it".to_owned(),
        ));
    }
    ops.app
        .set_hold(&body.account_id, body.reason.as_deref())
        .await
        .map_err(operation_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct Resolution {
    command_id: String,
    account_id: String,
    status: AckStatus,
    external_id: Option<String>,
    reason: String,
}

async fn resolve(
    State(ops): State<Operations>,
    headers: HeaderMap,
    Json(body): Json<Resolution>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize(&ops, &headers)?;
    ops.app
        .resolve(ExecutionAck {
            command_id: body.command_id,
            account_id: body.account_id,
            status: body.status,
            external_id: body.external_id,
            timestamp_unix_ns: unix_time_ns(),
            message: Some(body.reason),
        })
        .await
        .map_err(operation_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn authorize(ops: &Operations, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    ops.authenticate(headers)
        .map_err(|status| (status, String::new()))
}

fn operation_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::CONFLICT, error.to_string())
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    tracing::error!(%error, "operations request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "operations request failed".to_owned(),
    )
}
