//! Top-level server with S3-backed peer discovery and delta-CRDT engine.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::connection;
use crate::crdt::or_set::{OrSet, OrSetOp};
use crate::crdt_engine::CrdtEngine;
use crate::discovery::{Discovery, DiscoveryConfig};
use crate::dissemination::{PullPeriodic, SharedDissemination};
use crate::common::NodeId;
use crate::peer_manager::PeerManager;
use crate::proto::envelope::Payload;
use crate::proto::Envelope;

// ── PeerConnector ───────────────────────────────────────────────────────

type OutboundTasks = HashMap<NodeId, (SocketAddr, JoinHandle<()>)>;

#[derive(Clone)]
pub struct PeerConnector {
    node_id: String,
    node_name: String,
    manager: PeerManager,
    app_tx: mpsc::Sender<(SocketAddr, Envelope)>,
    outbound_tasks: Arc<Mutex<OutboundTasks>>,
}

impl PeerConnector {
    pub async fn add_peer(&self, node_id: NodeId, addr: SocketAddr) {
        let mut tasks = self.outbound_tasks.lock().await;
        if tasks.contains_key(&node_id) {
            return;
        }
        let handle = connection::spawn_outbound(
            addr,
            self.node_id.clone(),
            self.node_name.clone(),
            self.manager.clone(),
            self.app_tx.clone(),
        );
        tasks.insert(node_id, (addr, handle));
        info!(%addr, "outbound peer task spawned");
    }

    pub async fn remove_peer(&self, node_id: NodeId) {
        if let Some((addr, handle)) = self.outbound_tasks.lock().await.remove(&node_id) {
            handle.abort();
            info!(%addr, "outbound task aborted");
        }
        self.manager.remove(&node_id);
    }
}

// ── Server ──────────────────────────────────────────────────────────────

pub struct Server {
    node_id: String,
    node_name: String,
    listen_addr: SocketAddr,
    advertise_addr: SocketAddr,
    /// Optional address for the client operation listener (JSON-over-TCP).
    client_addr: Option<SocketAddr>,
    manager: PeerManager,
    outbound_tasks: Arc<Mutex<OutboundTasks>>,
}

impl Server {
    pub fn new(
        listen_addr: SocketAddr,
        advertise_addr: SocketAddr,
        node_name: &str,
        client_addr: Option<SocketAddr>,
    ) -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            node_name: node_name.to_string(),
            listen_addr,
            advertise_addr,
            client_addr,
            manager: PeerManager::new(),
            outbound_tasks: Arc::new(Mutex::new(OutboundTasks::new())),
        }
    }

    pub async fn run(&self, discovery_cfg: DiscoveryConfig) -> anyhow::Result<()> {
        let (app_tx, mut app_rx) = mpsc::channel::<(SocketAddr, Envelope)>(1024);

        // ── Build dissemination strategy ─────────────────────────────
        // Pull-only: peers periodically request deltas from each other.
        let dissemination: SharedDissemination =
            Arc::new(PullPeriodic::new(self.manager.clone()));

        // ── Build CRDT engine (OR-Set<String>) ───────────────────────
        let engine = CrdtEngine::<OrSet<String>>::new(
            self.node_id.clone(),
            "default-orset".to_string(),
            OrSet::new(),
            dissemination.clone(),
            self.manager.clone(),
            None, // pull_interval: None falls back to default 10 s for PullPeriodic
        );

        // Spawn pull loop (no-op for PushBroadcast since it doesn't support pull).
        let pull_handle = engine.start_pull_loop();

        // ── Optional client operation listener ───────────────────────
        let client_handle: Option<JoinHandle<()>> = if let Some(addr) = self.client_addr {
            let eng = engine.clone();
            Some(tokio::spawn(async move {
                let listener = match TcpListener::bind(addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        error!(%addr, %e, "failed to bind client listener");
                        return;
                    }
                };
                info!(%addr, "client op listener started");
                loop {
                    match listener.accept().await {
                        Ok((stream, peer)) => {
                            info!(%peer, "client connected");
                            let eng2 = eng.clone();
                            tokio::spawn(async move {
                                let reader = BufReader::new(stream);
                                let mut lines = reader.lines();
                                while let Ok(Some(line)) = lines.next_line().await {
                                    match serde_json::from_str::<OrSetOp<String>>(&line) {
                                        Ok(op) => {
                                            eng2.client_operation(op).await;
                                        }
                                        Err(e) => {
                                            warn!(%peer, %e, "invalid client op; expected JSON OrSetOp");
                                        }
                                    }
                                }
                                info!(%peer, "client disconnected");
                            });
                        }
                        Err(e) => error!(%e, "client accept failed"),
                    }
                }
            }))
        } else {
            None
        };

        // ── Build discovery service ──────────────────────────────────
        let discovery = Discovery::new(
            discovery_cfg.clone(),
            self.node_id.clone(),
            self.node_name.clone(),
            self.advertise_addr,
        )
        .await?;

        discovery.register().await?;

        let connector = PeerConnector {
            node_id: self.node_id.clone(),
            node_name: self.node_name.clone(),
            manager: self.manager.clone(),
            app_tx: app_tx.clone(),
            outbound_tasks: Arc::clone(&self.outbound_tasks),
        };

        let shutdown_discovery = Discovery::new(
            discovery_cfg,
            self.node_id.clone(),
            self.node_name.clone(),
            self.advertise_addr,
        )
        .await?;

        // ── Spawn discovery reconciliation loop ──────────────────────
        let disc_manager = self.manager.clone();
        let disc_connector = connector.clone();
        let discovery_handle = tokio::spawn(async move {
            discovery
                .run_discovery_loop(disc_manager, disc_connector)
                .await;
        });

        // ── Bind TCP listener ────────────────────────────────────────
        let listener = TcpListener::bind(self.listen_addr).await?;
        info!(
            addr = %self.listen_addr,
            advertise = %self.advertise_addr,
            node_id = %self.node_id,
            "listening"
        );

        let mut update_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut updates = 0;

        // ── Main event loop ──────────────────────────────────────────
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, remote_addr)) => {
                            info!(%remote_addr, "accepted inbound connection");
                            let mgr = self.manager.clone();
                            let nid = self.node_id.clone();
                            let nname = self.node_name.clone();
                            let tx = app_tx.clone();
                            tokio::spawn(async move {
                                connection::handle_inbound(
                                    stream, remote_addr, &nid, &nname, &mgr, &tx,
                                )
                                .await;
                            });
                        }
                        Err(e) => error!(%e, "accept failed"),
                    }
                }

                _ = update_interval.tick() => {
                    if updates < 10 {
                        let op = OrSetOp::Add(format!("tick-{}", chrono::Utc::now().timestamp()));
                        engine.client_operation(op).await;
                    }
                    engine.print_state().await;
                    updates += 1;
                }

                Some((addr, envelope)) = app_rx.recv() => {
                    // Route CRDT operations to the engine.
                    match envelope.payload {
                        Some(Payload::CrdtOp(crdt_op)) => {
                            engine
                                .server_message(
                                    crdt_op.origin_node_id,
                                    crdt_op.crdt_id,
                                    crdt_op.payload,
                                    crdt_op.hlc_ts,
                                )
                                .await;
                        }
                        other => {
                            info!(%addr, ?other, "app received non-CRDT message");
                        }
                    }
                }

                _ = tokio::signal::ctrl_c() => {
                    info!("shutdown signal received — deregistering from S3");
                    discovery_handle.abort();
                    pull_handle.abort();
                    if let Some(h) = client_handle {
                        h.abort();
                    }

                    if let Err(e) = shutdown_discovery.deregister().await {
                        error!(%e, "failed to deregister from S3");
                    }

                    let mut tasks = self.outbound_tasks.lock().await;
                    for (_, (addr, handle)) in tasks.drain() {
                        handle.abort();
                        info!(%addr, "aborted outbound task");
                    }

                    info!(node_id = %self.node_id, "shutdown complete");
                    return Ok(());
                }
            }
        }
    }
}
