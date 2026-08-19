use copier_core::{encode_agent_frame, parse_server_line, AgentFrame, ExecutionAck, HelloFrame, ServerFrame, TradeEvent, WireError};
use thiserror::Error;
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream}};

pub struct CopierClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl CopierClient {
    pub async fn connect(endpoint: &str, hello: HelloFrame) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(endpoint).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = stream.into_split();
        let mut client = Self { reader: BufReader::new(reader), writer };
        client.send_frame(&AgentFrame::Hello(hello)).await?;
        match client.next_frame().await? {
            Some(ServerFrame::Welcome { .. }) => Ok(client),
            Some(ServerFrame::Error { code, message }) => Err(ClientError::Rejected { code, message }),
            Some(other) => Err(ClientError::Unexpected(format!("{other:?}"))),
            None => Err(ClientError::Disconnected),
        }
    }

    pub async fn send_event(&mut self, event: TradeEvent) -> Result<(), ClientError> {
        self.send_frame(&AgentFrame::Event(event)).await
    }

    pub async fn send_ack(&mut self, ack: ExecutionAck) -> Result<(), ClientError> {
        self.send_frame(&AgentFrame::Ack(ack)).await
    }

    pub async fn ping(&mut self, timestamp_unix_ns: i64) -> Result<(), ClientError> {
        self.send_frame(&AgentFrame::Ping(timestamp_unix_ns)).await
    }

    pub async fn next_frame(&mut self) -> Result<Option<ServerFrame>, ClientError> {
        let mut line = String::with_capacity(512);
        let bytes = self.reader.read_line(&mut line).await?;
        if bytes == 0 {
            return Ok(None);
        }
        Ok(Some(parse_server_line(&line)?))
    }

    async fn send_frame(&mut self, frame: &AgentFrame) -> Result<(), ClientError> {
        let encoded = encode_agent_frame(frame);
        self.writer.write_all(encoded.as_bytes()).await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error("CopierR rejected connection: {code}: {message}")]
    Rejected { code: String, message: String },
    #[error("unexpected server frame: {0}")]
    Unexpected(String),
    #[error("CopierR disconnected during handshake")]
    Disconnected,
}
