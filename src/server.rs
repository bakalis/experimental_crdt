//! Top-level server: binds the TCP listener, connects to initial peers,
//! and exposes an API surface for runtime topology changes.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::connection;
use crate::peer_manager::PeerManager;
use crate::proto::Envelope;

pub struct Server {
    node_id: String,
    node_name: String,
    listen_addr: SocketAddr,
    manager: PeerManager,
    outbound_tasks: Arc<Mutex<HashMap<SocketAddr, JoinHandle<()>>>>,
}

impl Server {
    pub fn new(listen_addr: SocketAddr, node_name: &str) -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            node_name: node_name.to_string(),
            listen_addr,
            manager: PeerManager::new(),
            outbound_tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Main entry point — runs forever (or until the process is signalled).
    pub async fn run(&self, initial_peers: Vec<SocketAddr>) -> anyhow::Result<()> {
        // Channel that the application layer receives all inbound messages on.
        let (app_tx, mut app_rx) = mpsc::channel::<(SocketAddr, Envelope)>(1024);

        // ── connect to initial peers ────────────────────────────────
        for addr in initial_peers {
            self.add_peer(addr, app_tx.clone()).await;
        }

        // ── bind TCP listener ───────────────────────────────────────
        let listener = TcpListener::bind(self.listen_addr).await?;
        info!(addr = %self.listen_addr, node_id = %self.node_id, "listening");

        let mut timer = tokio::time::interval(std::time::Duration::from_secs(5));
        
        // ── accept loop + app message consumer ──────────────────────
        loop {
            tokio::select! {
                // Accept inbound connection.
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
                                    stream,
                                    remote_addr,
                                    &nid,
                                    &nname,
                                    &mgr,
                                    &tx,
                                ).await;
                            });
                        }
                        Err(e) => error!(%e, "accept failed"),
                    }
                }
                    
                _ = timer.tick() => {
                    let hs = Envelope {
                        payload: Some(crate::proto::envelope::Payload::CrdtOp(crate::proto::CrdtOp {
                            crdt_id: "timer".to_string(),
                            payload: vec![],
                            hlc_ts: 0,
                            origin_node_id: self.node_id.clone(),
                        })),
                    };

                    self.broadcast(hs).await;
                }

                // Process application-level messages (CRDT ops, etc.).
                Some((addr, envelope)) = app_rx.recv() => {
                    // Placeholder: in a real system you would feed this
                    // into your CRDT engine.
                    info!(%addr, ?envelope, "app received message");
                }
            }
        }
    }

    // ── Runtime scaling API ─────────────────────────────────────────

    /// Dynamically add a new outbound peer at runtime.
    pub async fn add_peer(
        &self,
        addr: SocketAddr,
        app_tx: mpsc::Sender<(SocketAddr, Envelope)>,
    ) {
        let handle = connection::spawn_outbound(
            addr,
            self.node_id.clone(),
            self.node_name.clone(),
            self.manager.clone(),
            app_tx,
        );
        self.outbound_tasks.lock().await.insert(addr, handle);
        info!(%addr, "outbound peer task spawned");
    }

    /// Dynamically remove a peer at runtime.
    ///
    /// This aborts the reconnection supervisor **and** removes the peer
    /// from the active registry (which closes channels and tears down
    /// the read/write tasks).
    pub async fn remove_peer(&self, addr: &SocketAddr) {
        if let Some(handle) = self.outbound_tasks.lock().await.remove(addr) {
            handle.abort();
            info!(%addr, "outbound task aborted");
        }
        self.manager.remove(addr);
    }

    /// Retrieve a snapshot of all connected peer addresses.
    pub fn connected_peers(&self) -> Vec<SocketAddr> {
        self.manager.peer_addrs()
    }

    /// Broadcast a CRDT operation to every connected peer.
    pub async fn broadcast(&self, envelope: Envelope) {
        self.manager.broadcast(envelope).await;
    }
}
