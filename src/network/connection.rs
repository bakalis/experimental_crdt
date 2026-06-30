#![allow(dead_code)]
//! Per-peer connection lifecycle: connect → handshake → read/write loops.
//!
//! Each peer is managed by two Tokio tasks:
//!   • **writer** – drains an `mpsc` channel and serialises envelopes.
//!   • **reader** – deserialises inbound envelopes and dispatches them.
//!
//! On failure the tasks exit and a supervising reconnect loop
//! (in [`spawn_outbound`]) will back off and retry.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::common::NodeId;
use crate::common::error::{Error, Result};
use crate::peers::peer_registry::{PeerHandle, PeerRegistry};
use crate::proto::{self, envelope::Payload, Envelope};
use crate::network::protocol;

// ── tunables ────────────────────────────────────────────────────────────

const CHANNEL_BUF: usize = 256;
const RECONNECT_BASE: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

// ── public entry points ─────────────────────────────────────────────────

/// Spawn a supervised outbound connection that will keep retrying.
///
/// Returns a `JoinHandle` and a `CancellationToken`-style `mpsc::Sender`
/// whose drop will cause the task tree to shut down.
pub fn spawn_outbound(
    addr: SocketAddr,
    local_node_id: NodeId,
    local_node_name: NodeId,
    gc_replica: bool,
    registry: PeerRegistry,
    app_tx: mpsc::Sender<Envelope>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
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
        addr,
        remote_gc_replica,
        PeerHandle { tx },
    );

    // ── spawn writer task ───────────────────────────────────────────
    let writer_handle = tokio::spawn(writer_loop(writer, rx, addr));

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
    mut writer: WriteHalf<TcpStream>,
    mut rx: mpsc::Receiver<Envelope>,
    addr: SocketAddr,
) {
    while let Some(envelope) = rx.recv().await {
        if let Err(e) = protocol::write_envelope(&mut writer, &envelope).await {
            error!(%addr, %e, "write failed — exiting writer");
            return;
        }
    }
    debug!(%addr, "writer channel closed");
}
