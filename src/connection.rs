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

use crate::error::{Error, Result};
use crate::peer_registry::{PeerHandle, PeerRegistry};
use crate::common::NodeId;
use crate::proto::{self, envelope::Payload, Envelope};
use crate::protocol;

// ── tunables ────────────────────────────────────────────────────────────

const CHANNEL_BUF: usize = 256;
// const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
// const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);
const RECONNECT_BASE: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

// ── public entry points ─────────────────────────────────────────────────

/// Spawn a supervised outbound connection that will keep retrying.
///
/// Returns a `JoinHandle` and a `CancellationToken`-style `mpsc::Sender`
/// whose drop will cause the task tree to shut down.
pub fn spawn_outbound(addr: SocketAddr,
    local_node_id: NodeId,
    local_node_name: NodeId,
    manager: PeerRegistry,
    app_tx: mpsc::Sender<(SocketAddr, Envelope)>,
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
                        &manager,
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
    manager: &PeerRegistry,
    app_tx: &mpsc::Sender<(SocketAddr, Envelope)>,
) {
    match run_connection(
        stream,
        remote_addr,
        local_node_id,
        local_node_name,
        manager,
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
    manager: &PeerRegistry,
    app_tx: &mpsc::Sender<(SocketAddr, Envelope)>,
    initiator: bool,
) -> Result<NodeId> {
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = tokio::io::split(stream);

    // ── handshake ───────────────────────────────────────────────────
    let remote_node_id = handshake(
        &mut reader,
        &mut writer,
        local_node_id,
        local_node_name,
        addr,
        initiator,
    )
    .await?;
    info!(%addr, remote_node_id, "handshake complete");

    // ── register in peer manager ────────────────────────────────────
    let (tx, rx) = mpsc::channel::<Envelope>(CHANNEL_BUF);
    manager.insert(
        remote_node_id.clone(),
        addr,
        PeerHandle {
            node_id: Some(remote_node_id.clone()),
            addr,
            tx: tx.clone(),
        },
    );

    // ── spawn writer task ───────────────────────────────────────────
    let writer_handle = tokio::spawn(writer_loop(writer, rx, addr));

    // ── spawn heartbeat emitter ─────────────────────────────────────
    // let hb_tx = tx.clone();heartbeat_em
    // let hb_handle = tokio::spawn(heartbeat_emitter(hb_tx, addr));

    // ── reader loop (runs on current task) ──────────────────────────
    let reader_result = reader_loop(&mut reader, addr, app_tx, &tx).await;

    // Tear down sibling tasks when reader exits.
    // hb_handle.abort();
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
    addr: SocketAddr,
    initiator: bool,
) -> Result<String> {
    let hs = Envelope {
        payload: Some(Payload::Handshake(proto::Handshake {
            node_id: local_node_id.to_string(),
            node_name: local_node_name.to_string(),
            version: 1,
        })),
    };

    if initiator {
        // Send first, then wait for reply.
        protocol::write_envelope(writer, &hs).await?;
        receive_handshake(reader, addr).await
    } else {
        // Wait for remote handshake, then reply.
        let remote_id = receive_handshake(reader, addr).await?;
        protocol::write_envelope(writer, &hs).await?;
        Ok(remote_id)
    }
}

async fn receive_handshake(
    reader: &mut ReadHalf<TcpStream>,
    addr: SocketAddr,
) -> Result<String> {
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
            Ok(hs.node_id)
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
    app_tx: &mpsc::Sender<(SocketAddr, Envelope)>,
    peer_tx: &mpsc::Sender<Envelope>,
) -> Result<()> {
    // let mut last_seen = tokio::time::Instant::now();
    loop {
        let read_fut = protocol::read_envelope(reader);
        // let timeout_fut = time::sleep(HEARTBEAT_TIMEOUT);

        tokio::select! {
            result = read_fut => {
                match result? {
                    Some(env) => {
                        // last_seen = tokio::time::Instant::now();
                        dispatch(env, addr, app_tx, peer_tx).await?;
                    }
                    None => return Err(Error::ConnectionClosed(addr)),
                }
            }
            /* _ = timeout_fut => {
                // Check if we've actually timed out (select may have
                // spuriously fired).
                if last_seen.elapsed() >= HEARTBEAT_TIMEOUT {
                    return Err(Error::HeartbeatTimeout(addr));
                }
            } */
        }
    }
}

async fn dispatch(
    envelope: Envelope,
    addr: SocketAddr,
    app_tx: &mpsc::Sender<(SocketAddr, Envelope)>,
    peer_tx: &mpsc::Sender<Envelope>,
) -> Result<()> {
    match &envelope.payload {
        Some(Payload::Heartbeat(hb)) => {
            debug!(%addr, seq = hb.seq, "heartbeat received");
            let ack = Envelope {
                payload: Some(Payload::HeartbeatAck(proto::HeartbeatAck { seq: hb.seq })),
            };
            let _ = peer_tx.try_send(ack);
        }
        Some(Payload::HeartbeatAck(ack)) => {
            debug!(%addr, seq = ack.seq, "heartbeat ack");
        }
        Some(Payload::CrdtOp(_)) | Some(Payload::Handshake(_)) => {
            // Forward to the application layer for processing.
            let _ = app_tx.send((addr, envelope)).await;
        }
        None => {
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

// ── heartbeat emitter ───────────────────────────────────────────────────

/* async fn heartbeat_emitter(tx: mpsc::Sender<Envelope>, addr: SocketAddr) {
    let mut seq: u64 = 0;
    let mut interval = time::interval(HEARTBEAT_INTERVAL);
    loop {
        interval.tick().await;
        seq += 1;
        let hb = Envelope {
            payload: Some(Payload::Heartbeat(proto::Heartbeat { seq })),
        };
        if tx.send(hb).await.is_err() {
            debug!(%addr, "heartbeat emitter stopping (channel closed)");
            return;
        }
    }
} */
