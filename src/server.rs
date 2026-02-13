//! Top-level server with S3-backed peer discovery.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::connection;
use crate::discovery::{Discovery, DiscoveryConfig};
use crate::common::NodeId;
use crate::peer_manager::PeerManager;
use crate::proto::Envelope;

// ── PeerConnector ───────────────────────────────────────────────────────

/// Thin, clonable handle the discovery loop uses to add/remove outbound
/// connections without holding a reference to the full `Server`.
#[derive(Clone)]
pub struct PeerConnector {
    node_id: String,
    node_name: String,
    manager: PeerManager,
    app_tx: mpsc::Sender<(SocketAddr, Envelope)>,
    outbound_tasks: Arc<Mutex<HashMap<NodeId, (SocketAddr, JoinHandle<()>)>>>,
}

impl PeerConnector {
    pub async fn add_peer(&self, node_id: NodeId, addr: SocketAddr) {
        let mut tasks = self.outbound_tasks.lock().await;
        if tasks.contains_key(&node_id) {
            return;
        }
        let handle = connection::spawn_outbound(
            node_id.clone(),
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
    manager: PeerManager,
    outbound_tasks: Arc<Mutex<HashMap<NodeId, (SocketAddr, JoinHandle<()>)>>>,
}

impl Server {
    pub fn new(listen_addr: SocketAddr, advertise_addr: SocketAddr, node_name: &str) -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            node_name: node_name.to_string(),
            listen_addr,
            advertise_addr,
            manager: PeerManager::new(),
            outbound_tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run(&self, discovery_cfg: DiscoveryConfig) -> anyhow::Result<()> {
        let (app_tx, mut app_rx) = mpsc::channel::<(SocketAddr, Envelope)>(1024);

        // ── Build discovery service ─────────────────────────────────
        let discovery = Discovery::new(
            discovery_cfg.clone(),
            self.node_id.clone(),
            self.node_name.clone(),
            self.advertise_addr,
        )
        .await?;

        // Initial registration — fail fast if S3 is unreachable.
        discovery.register().await?;

        let connector = PeerConnector {
            node_id: self.node_id.clone(),
            node_name: self.node_name.clone(),
            manager: self.manager.clone(),
            app_tx: app_tx.clone(),
            outbound_tasks: Arc::clone(&self.outbound_tasks),
        };

        // Keep a clone of discovery for deregistration on shutdown.
        let shutdown_discovery = Discovery::new(
            discovery_cfg,
            self.node_id.clone(),
            self.node_name.clone(),
            self.advertise_addr,
        )
        .await?;

        // ── Spawn discovery reconciliation loop ─────────────────────
        let disc_manager = self.manager.clone();
        let disc_connector = connector.clone();
        let discovery_handle = tokio::spawn(async move {
            discovery.run_discovery_loop(disc_manager, disc_connector).await;
        });

        // ── Bind TCP listener ───────────────────────────────────────
        let listener = TcpListener::bind(self.listen_addr).await?;
        info!(
            addr = %self.listen_addr,
            advertise = %self.advertise_addr,
            node_id = %self.node_id,
            "listening"
        );
        let mut message_interval = tokio::time::interval(std::time::Duration::from_secs(20));

        // ── Main event loop_ ─────────────────────────────────────────
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

                Some((addr, envelope)) = app_rx.recv() => {
                    // Future: route to CRDT engine.
                    info!(%addr, ?envelope, "app received message");
                }
                
                _ = tokio::signal::ctrl_c() => {
                    info!("shutdown signal received — deregistering from S3");
                    discovery_handle.abort();

                    if let Err(e) = shutdown_discovery.deregister().await {
                        error!(%e, "failed to deregister from S3");
                    }

                    // Abort all outbound connection tasks.
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
