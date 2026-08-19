//! Platform-neutral contracts, routing and wire protocol for CopierR.

pub mod model;
pub mod protocol;
pub mod routing;

pub use model::{
    AckStatus, ExecutionAck, ExecutionCommand, MirrorBinding, ModelError, Platform, Side,
    TradeAction, TradeEvent,
};
pub use protocol::{
    encode_agent_frame, encode_server_frame, parse_agent_line, parse_server_line, AgentFrame,
    HelloFrame, ServerFrame, WireError, WIRE_VERSION,
};
pub use routing::{CopyEngine, RouteError, RouteRule, SizingMode};
