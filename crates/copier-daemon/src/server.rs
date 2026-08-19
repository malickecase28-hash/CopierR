use crate::runtime::{unix_time_ns, AppState};
use anyhow::{Context, Result};
use copier_core::{encode_server_frame, parse_agent_line, AgentFrame, ServerFrame};
use std::sync::Arc;
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::{TcpListener, TcpStream}, sync::mpsc};
use tracing::{debug, info, warn};

pub async fn run(state: Arc<AppState>) -> Result<()> {
    let listener = TcpListener::bind(&state.config.listen).await
        .with_context(|| format!("failed to bind {}", state.config.listen))?;
    info!(listen = %state.config.listen, "CopierR daemon listening");
    loop {
        let (stream, peer) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(state, stream).await {
                warn!(%peer, %error, "connection terminated");
            }
        });
    }
}

async fn handle_connection(state: Arc<AppState>, stream: TcpStream) -> Result<()> {
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut first = String::new();
    if reader.read_line(&mut first).await? == 0 {
        return Ok(());
    }
    let hello = match parse_agent_line(&first)? {
        AgentFrame::Hello(hello) => hello,
        _ => anyhow::bail!("first frame must be HELLO"),
    };
    state.authenticate(&hello.account_id, hello.platform, &hello.token)?;

    writer.write_all(encode_server_frame(&ServerFrame::Welcome {
        server_time_unix_ns: unix_time_ns(),
    }).as_bytes()).await?;

    let (tx, mut rx) = mpsc::channel(state.config.queue_capacity);
    let session_id = state.register_session(hello.account_id.clone(), tx).await;
    let account_id = hello.account_id.clone();
    let writer_account = account_id.clone();
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            let encoded = encode_server_frame(&frame);
            if writer.write_all(encoded.as_bytes()).await.is_err() {
                break;
            }
        }
        debug!(account = %writer_account, "writer task stopped");
    });

    state.dispatch_queued_for(&account_id).await?;

    let mut line = String::with_capacity(512);
    loop {
        line.clear();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            break;
        }
        let frame = match parse_agent_line(&line) {
            Ok(frame) => frame,
            Err(error) => {
                warn!(account = %account_id, %error, "invalid agent frame");
                continue;
            }
        };
        if matches!(frame, AgentFrame::Ping(_)) {
            if let Some(session) = state_session_sender(&state, &account_id).await {
                let _ = session.send(ServerFrame::Pong { server_time_unix_ns: unix_time_ns() }).await;
            }
            continue;
        }
        state.handle_frame(&account_id, frame).await?;
    }

    writer_task.abort();
    state.unregister_session(&account_id, session_id).await?;
    Ok(())
}

async fn state_session_sender(state: &Arc<AppState>, account_id: &str) -> Option<mpsc::Sender<ServerFrame>> {
    state.session_sender(account_id).await
}
