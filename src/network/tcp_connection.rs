#![allow(dead_code)]
//! Per-peer connection lifecycle: connect → handshake → read/write loops.
//!
//! Each peer is managed by two Tokio tasks:
//!   • **writer** – drains an `mpsc` channel and serialises envelopes.
//!   • **reader** – deserialises inbound envelopes and dispatches them.
//!
//! On failure the tasks exit and a supervising reconnect loop
//! (in [`spawn_outbound`]) will back off and retry.

use prost::Message;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::metric;
use crate::common::{self, NodeId};
use crate::common::error::{Error, Result};
use crate::peers::peer_registry::{PeerHandle, PeerRegistry};
use crate::proto::{self, envelope::Payload, Envelope};
use crate::network::{Network, protocol};

// ── tunables ────────────────────────────────────────────────────────────

const CHANNEL_BUF: usize = 256;
const RECONNECT_BASE: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

pub struct TcpNetwork;

impl Network for TcpNetwork {
    fn spawn_outbound(
        &self,
        address: String,
        local_node_id: NodeId,
        local_node_name: NodeId,
        gc_replica: bool,
        registry: PeerRegistry,
        app_tx: mpsc::Sender<Envelope>,
    ) -> tokio::task::JoinHandle<()> {
        spawn_outbound(
            address,
            local_node_id,
            local_node_name,
            gc_replica,
            registry,
            app_tx,
        )
    }

    fn start_network_background(&self, listen_port: String,
        node_id: String,
        node_name: String,
        gc_replica: bool,
        registry: PeerRegistry,
        app_tx: mpsc::Sender<Envelope>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let _ = bind_and_listen(listen_port, node_id, node_name, gc_replica, registry, app_tx).await;
        })
    }
}
// ── public entry points ─────────────────────────────────────────────────

/// Spawn a supervised outbound connection that will keep retrying.
///
/// Returns a `JoinHandle` and a `CancellationToken`-style `mpsc::Sender`
/// whose drop will cause the task tree to shut down.
pub fn spawn_outbound(
    address: String,
    local_node_id: NodeId,
    local_node_name: NodeId,
    gc_replica: bool,
    registry: PeerRegistry,
    app_tx: mpsc::Sender<Envelope>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let addr = common::lookup(&address).await;
        match addr {
            Err(e) => {
                error!(%address, %e, "failed to resolve address");
            },
            Ok(addr) => {
                let mut backoff = RECONNECT_BASE;
                loop {
                    info!(%addr, "connecting to peer…");
                    match TcpStream::connect(addr).await {
                        Ok(stream) => {
                            info!(%addr, "TCP connected");
                            backoff = RECONNECT_BASE; // reset on success
                            if let Err(e) = run_connection(
                                stream,
                                addr,
                                &local_node_id,
                                &local_node_name,
                                gc_replica,
                                &registry,
                                &app_tx,
                                true, // we are the initiator
                            )
                            .await
                            {
                                warn!(%addr, %e, "connection terminated");
                            }
                        }
                        Err(e) => {
                            warn!(%addr, %e, "connection failed");
                        }
                    }
                    info!(%addr, ?backoff, "will reconnect after backoff");
                    time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                }
            }
        }
    })
}

/// Handle an already-accepted inbound TCP stream.
pub async fn handle_inbound(
    stream: TcpStream,
    remote_addr: SocketAddr,
    local_node_id: &str,
    local_node_name: &str,
    gc_replica: bool,
    registry: &PeerRegistry,
    app_tx: &mpsc::Sender<Envelope>,
) {
    match run_connection(
        stream,
        remote_addr,
        local_node_id,
        local_node_name,
        gc_replica,
        registry,
        app_tx,
        false,
    )
    .await
    {
        Ok(node_id) => {
            warn!(%node_id, "inbound connection ended");
        }
        Err(e) => {
            warn!(%e, "connection failed");
        }
    }
}

// ── internal ────────────────────────────────────────────────────────────

async fn run_connection(
    stream: TcpStream,
    addr: SocketAddr,
    local_node_id: &str,
    local_node_name: &str,
    gc_replica: bool,
    registry: &PeerRegistry,
    app_tx: &mpsc::Sender<Envelope>,
    initiator: bool,
) -> Result<NodeId> {
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = tokio::io::split(stream);

    // ── handshake ───────────────────────────────────────────────────
    let (remote_node_id, remote_gc_replica) = handshake(
        &mut reader,
        &mut writer,
        local_node_id,
        local_node_name,
        gc_replica,
        addr,
        initiator,
    )
    .await?;
    info!(%addr, remote_node_id, remote_gc_replica, "handshake complete");

    // ── register in peer registry ────────────────────────────────────
    let (tx, rx) = mpsc::channel::<Envelope>(CHANNEL_BUF);
    registry.insert(
        remote_node_id.clone(),
        remote_gc_replica,
        PeerHandle { tx },
    );

    // ── spawn writer task ───────────────────────────────────────────
    let writer_handle = tokio::spawn(writer_loop(local_node_id.to_string(), writer, rx, addr));

    // ── reader loop (runs on current task) ──────────────────────────
    let reader_result = reader_loop(&mut reader, addr, app_tx).await;

    // Tear down sibling tasks when reader exits.
    writer_handle.abort();

    match reader_result {
        Ok(()) => Ok(remote_node_id),
        Err(e) => Err(e),
    }
}

// ── handshake ───────────────────────────────────────────────────────────

async fn handshake(
    reader: &mut ReadHalf<TcpStream>,
    writer: &mut WriteHalf<TcpStream>,
    local_node_id: &str,
    local_node_name: &str,
    gc_replica: bool,
    addr: SocketAddr,
    initiator: bool,
) -> Result<(String, bool)> {
    let hs = Envelope {
        payload: Some(Payload::Handshake(proto::Handshake {
            node_id: local_node_id.to_string(),
            node_name: local_node_name.to_string(),
            gc_replica,
            version: 1,
        })),
    };

    if initiator {
        // Send first, then wait for reply.
        protocol::write_envelope(writer, &hs).await?;
        receive_handshake(reader, addr).await
    } else {
        // Wait for remote handshake, then reply.
        let (remote_id, gc_replica) = receive_handshake(reader, addr).await?;
        protocol::write_envelope(writer, &hs).await?;
        Ok((remote_id, gc_replica))
    }
}

async fn receive_handshake(
    reader: &mut ReadHalf<TcpStream>,
    addr: SocketAddr,
) -> Result<(String, bool)> {
    let envelope = protocol::read_envelope(reader)
        .await?
        .ok_or_else(|| Error::HandshakeFailed(addr, "EOF before handshake".into()))?;

    match envelope.payload {
        Some(Payload::Handshake(hs)) => {
            if hs.version != 1 {
                return Err(Error::HandshakeFailed(
                    addr,
                    format!("unsupported version {}", hs.version),
                ));
            }
            Ok((hs.node_id, hs.gc_replica))
        }
        other => Err(Error::HandshakeFailed(
            addr,
            format!("expected Handshake, got {other:?}"),
        )),
    }
}

// ── read loop ───────────────────────────────────────────────────────────

async fn reader_loop(
    reader: &mut ReadHalf<TcpStream>,
    addr: SocketAddr,
    app_tx: &mpsc::Sender<Envelope>,
) -> Result<()> {
    loop {
        match protocol::read_envelope(reader).await? {
            Some(env) => dispatch(env, addr, app_tx).await?,
            Option::None => return Err(Error::ConnectionClosed(addr)),
        }
    }
}

async fn dispatch(
    envelope: Envelope,
    addr: SocketAddr,
    app_tx: &mpsc::Sender<Envelope>,
) -> Result<()> {
    match &envelope.payload {
        Some(Payload::CrdtOp(_))
        | Some(Payload::CrdtPullRequest(_))
        | Some(Payload::Handshake(_)) => {
            // Forward to the application layer for processing.
            let _ = app_tx.send(envelope).await;
        }
        Option::None => {
            warn!(%addr, "received envelope with no payload");
        }
    }
    Ok(())
}

// ── write loop ──────────────────────────────────────────────────────────

async fn writer_loop(
    local_node_id: NodeId,
    mut writer: WriteHalf<TcpStream>,
    mut rx: mpsc::Receiver<Envelope>,
    addr: SocketAddr,
) {
    while let Some(envelope) = rx.recv().await {
        let len = envelope.encoded_len();
        metric!(node_id = local_node_id, event = "send_envelope", size_bytes = len as u64);
        if let Err(e) = protocol::write_envelope(&mut writer, &envelope).await {
            error!(%addr, %e, "write failed — exiting writer");
            return;
        }
    }
    debug!(%addr, "writer channel closed");
}

pub async fn bind_and_listen(
    listen_port: String,
    node_id: String,
    node_name: String,
    gc_replica: bool,
    registry: PeerRegistry,
    app_tx: mpsc::Sender<Envelope>,
) -> anyhow::Result<()> {
    let listen_addr: SocketAddr = format!("0.0.0.0:{}", listen_port).parse()?;
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    info!(
        addr = %listen_addr,
        node_id = %node_id,
        "listening"
    );
    loop {
        let accept_result = listener.accept().await;
        handle_accepted_connection(
            node_id.clone(),
            node_name.clone(),
            gc_replica,
            registry.clone(),
            app_tx.clone(),
            accept_result,
        );
    }
}


pub fn handle_accepted_connection(
    node_id: String,
    node_name: String,
    gc_replica: bool,
    registry: PeerRegistry,
    app_tx: mpsc::Sender<Envelope>,
    accept_result: std::io::Result<(tokio::net::TcpStream, SocketAddr)>,
) {
    match accept_result {
        Ok((stream, remote_addr)) => {
            info!(%remote_addr, "accepted inbound connection");
            tokio::spawn(async move {
                handle_inbound(
                    stream,
                    remote_addr,
                    &node_id,
                    &node_name,
                    gc_replica,
                    &registry,
                    &app_tx,
                )
                .await;
            });
        }
        Err(e) => error!(%e, "accept failed"),
    }
}
