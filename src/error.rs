use std::net::SocketAddr;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    #[error("Protobuf encode error: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("Peer {0} not found in registry")]
    PeerNotFound(SocketAddr),

    #[error("Connection to {0} closed by remote")]
    ConnectionClosed(SocketAddr),

    #[error("Handshake failed with {0}: {1}")]
    HandshakeFailed(SocketAddr, String),

    #[error("Heartbeat timeout for peer {0}")]
    HeartbeatTimeout(SocketAddr),

    #[error("Discovery error: {0}")]
    Discovery(String),
}

pub type Result<T> = std::result::Result<T, Error>;
