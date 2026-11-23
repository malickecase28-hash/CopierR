use crate::{config::AccountBackend, runtime::{unix_time_ns, AppState}};
use anyhow::{Context, Result};
use copier_core::{encode_server_frame, parse_agent_line, AgentFrame, ServerFrame};
use std::sync::Arc;
use tokio::{io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader}, net::{TcpListener, TcpStream}, sync::mpsc, time::{timeout, Duration}};
use tracing::{debug, info, warn};

pub async fn bind(state: &AppState) -> Result<TcpListener> {
    let listener = TcpListener::bind(&state.config.listen).await
        .with_context(|| format!("failed to bind {}", state.config.listen))?;
    info!(listen = %state.config.listen, "CopierR daemon listening");
    Ok(listener)
}

pub async fn run(state: Arc<AppState>, listener: TcpListener) -> Result<()> {
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
    if timeout(Duration::from_secs(10), read_frame(&mut reader, &mut first)).await?? == 0 {
        return Ok(());
    }
    let hello = match parse_agent_line(&first)? {
        AgentFrame::Hello(hello) => hello,
        _ => anyhow::bail!("first frame must be HELLO"),
    };
    let account = state.config.account(&hello.account_id)
        .ok_or_else(|| anyhow::anyhow!("unknown account {}", hello.account_id))?;
    if account.backend != AccountBackend::Agent {
        anyhow::bail!("account {} is managed by an internal direct venue and cannot attach over TCP", hello.account_id);
    }
    state.authenticate(&hello.account_id, hello.platform, &hello.token)?;

    writer.write_all(encode_server_frame(&ServerFrame::Welcome {
        server_time_unix_ns: unix_time_ns(),
    }).as_bytes()).await?;

    let (tx, mut rx) = mpsc::channel(state.config.queue_capacity);
    let session_id = state.register_session(hello.account_id.clone(), tx).await?;
    let account_id = hello.account_id.clone();
    let writer_account = account_id.clone();
    let mut writer_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            let encoded = encode_server_frame(&frame);
            if writer.write_all(encoded.as_bytes()).await.is_err() {
                break;
            }
        }
        debug!(account = %writer_account, "writer task stopped");
    });

    let connection = async {
        let mut line = String::with_capacity(512);
        loop {
            line.clear();
            let bytes = read_frame(&mut reader, &mut line).await?;
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
            state.handle_frame(&account_id, frame).await?;
        }
        Ok(())
    };
    let connection_result: Result<()> = tokio::select! {
        result = connection => result,
        result = &mut writer_task => { result?; Ok(()) }
    };

    writer_task.abort();
    let cleanup_result = state.unregister_session(&account_id, session_id).await;
    connection_result?;
    cleanup_result?;
    Ok(())
}

async fn read_frame<R: AsyncBufRead + Unpin>(reader: &mut R, buffer: &mut String) -> Result<usize> {
    const MAX_FRAME_BYTES: usize = 1024 * 1024;
    let bytes = reader.take(MAX_FRAME_BYTES as u64).read_line(buffer).await?;
    anyhow::ensure!(bytes < MAX_FRAME_BYTES && (bytes == 0 || buffer.ends_with('\n')), "agent frame exceeded its size limit or was incomplete");
    Ok(bytes)
}
