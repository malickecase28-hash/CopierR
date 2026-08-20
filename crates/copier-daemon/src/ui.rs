use crate::config::DaemonConfig;
use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    process::{Child, Command},
    sync::Mutex,
};

const INDEX_HTML: &str = include_str!("ui.html");
const LOG_LIMIT: usize = 200;

pub async fn run(listen: &str) -> Result<()> {
    let address: SocketAddr = listen
        .parse()
        .with_context(|| format!("invalid UI listen address {listen}"))?;
    if !address.ip().is_loopback() {
        anyhow::bail!("the UI must bind to a loopback address");
    }
    let state = Arc::new(UiState {
        daemon_binary: std::env::current_exe().context("failed to resolve current executable")?,
        daemon: Mutex::new(None),
        logs: Arc::new(Mutex::new(VecDeque::new())),
    });
    let app = Router::new()
        .route("/", get(index))
        .route("/api/status", get(status))
        .route("/api/validate", post(validate))
        .route("/api/start", post(start))
        .route("/api/stop", post(stop))
        .with_state(state.clone());
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind UI at {listen}"))?;
    println!("CopierR UI available at http://{listen}/");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .context("UI server stopped")?;
    stop_child(&state).await?;
    Ok(())
}

struct UiState {
    daemon_binary: PathBuf,
    daemon: Mutex<Option<Child>>,
    logs: Arc<Mutex<VecDeque<String>>>,
}

#[derive(Debug, Deserialize)]
struct ConfigRequest {
    config: DaemonConfig,
    #[serde(default)]
    config_path: PathBuf,
    #[serde(default)]
    working_dir: Option<PathBuf>,
    #[serde(default)]
    secrets: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize)]
struct ValidationResponse {
    valid: bool,
    toml: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    running: bool,
    pid: Option<u32>,
    logs: Vec<String>,
}

#[derive(Debug, Serialize)]
struct MessageResponse {
    message: String,
}

#[derive(Debug)]
struct ApiError(String);

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self(error.to_string())
    }
}

impl From<std::io::Error> for ApiError {
    fn from(error: std::io::Error) -> Self {
        Self(error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::BAD_REQUEST,
            Json(MessageResponse { message: self.0 }),
        )
            .into_response()
    }
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn validate(Json(request): Json<ConfigRequest>) -> Result<Json<ValidationResponse>, ApiError> {
    request.config.validate().map_err(ApiError::from)?;
    let toml = toml::to_string_pretty(&request.config).context("failed to serialize TOML")?;
    Ok(Json(ValidationResponse {
        valid: true,
        toml,
        message: "Configuration is valid.".to_owned(),
    }))
}

async fn start(
    State(state): State<Arc<UiState>>,
    Json(request): Json<ConfigRequest>,
) -> Result<Json<MessageResponse>, ApiError> {
    request.config.validate().map_err(ApiError::from)?;
    let working_dir = request
        .working_dir
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(std::env::current_dir().context("failed to resolve UI working directory")?);
    let config_path = if request.config_path.as_os_str().is_empty() {
        PathBuf::from("copierr.test.toml")
    } else {
        request.config_path
    };
    let config_path = if config_path.is_absolute() {
        config_path
    } else {
        working_dir.join(config_path)
    };
    let toml = toml::to_string_pretty(&request.config).context("failed to serialize TOML")?;
    write_config(&config_path, &toml).await?;

    let mut daemon = state.daemon.lock().await;
    if let Some(child) = daemon.as_mut() {
        if child.try_wait()?.is_none() {
            return Err(ApiError("CopierR is already running.".to_owned()));
        }
        *daemon = None;
    }

    let mut command = Command::new(&state.daemon_binary);
    command
        .arg("daemon")
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.current_dir(&working_dir);
    for (name, value) in request.secrets {
        if !name.trim().is_empty() && !value.is_empty() {
            command.env(name, value);
        }
    }
    let mut child = command.spawn().context("failed to launch CopierR daemon")?;
    let pid = child.id();
    if let Some(stdout) = child.stdout.take() {
        spawn_log_reader(stdout, state.logs.clone(), "out");
    }
    if let Some(stderr) = child.stderr.take() {
        spawn_log_reader(stderr, state.logs.clone(), "err");
    }
    *daemon = Some(child);
    Ok(Json(MessageResponse {
        message: format!("CopierR daemon started{}.", pid.map(|pid| format!(" (pid {pid})")).unwrap_or_default()),
    }))
}

async fn stop(State(state): State<Arc<UiState>>) -> Result<Json<MessageResponse>, ApiError> {
    let stopped = stop_child(&state).await?;
    if !stopped {
        return Ok(Json(MessageResponse { message: "CopierR is not running.".to_owned() }));
    }
    Ok(Json(MessageResponse { message: "CopierR daemon stopped.".to_owned() }))
}

async fn stop_child(state: &UiState) -> Result<bool> {
    let mut daemon = state.daemon.lock().await;
    let Some(mut child) = daemon.take() else {
        return Ok(false);
    };
    if child.try_wait()?.is_none() {
        child.kill().await.context("failed to stop CopierR daemon")?;
        child.wait().await.context("failed waiting for CopierR daemon")?;
    }
    Ok(true)
}

async fn status(State(state): State<Arc<UiState>>) -> Result<Json<StatusResponse>, ApiError> {
    let mut daemon = state.daemon.lock().await;
    let mut pid = None;
    let mut running = false;
    if let Some(child) = daemon.as_mut() {
        if child.try_wait()?.is_none() {
            running = true;
            pid = child.id();
        } else {
            *daemon = None;
        }
    }
    let logs = state.logs.lock().await.iter().cloned().collect();
    Ok(Json(StatusResponse { running, pid, logs }))
}

async fn write_config(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, content)
        .await
        .with_context(|| format!("failed to write config {}", path.display()))?;
    Ok(())
}

fn spawn_log_reader<R>(reader: R, logs: Arc<Mutex<VecDeque<String>>>, stream: &'static str)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut logs = logs.lock().await;
            logs.push_back(format!("[{stream}] {line}"));
            while logs.len() > LOG_LIMIT {
                logs.pop_front();
            }
        }
    });
}
