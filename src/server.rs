use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::connection;
use crate::crdt::or_set::{OrSet, OrSetOp};
use crate::crdt_engine::CrdtEngine;
use crate::discovery::{Discovery, DiscoveryConfig};
use crate::dissemination::{PullPeriodic, PullRoundEngine, SharedDissemination};
use crate::gc::GcConfig;
use crate::common::NodeId;
use crate::peer_registry::PeerRegistry;
use crate::proto;
use crate::proto::envelope::Payload;
use crate::proto::Envelope;
use crate::s3_client::S3Client;

// ── PeerConnector ───────────────────────────────────────────────────────

type OutboundTasks = HashMap<NodeId, (SocketAddr, JoinHandle<()>)>;

#[derive(Clone)]
pub struct PeerConnector {
    node_id: String,
    node_name: String,
    registry: PeerRegistry,
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
            self.registry.clone(),
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
        self.registry.remove(&node_id);
    }
}

// ── Server ──────────────────────────────────────────────────────────────

pub struct Server {
    node_id: String,
    node_name: String,
    listen_addr: SocketAddr,
    /// Optional address for the client operation listener (JSON-over-TCP).
    advertise_addr: SocketAddr,
    client_addr: Option<SocketAddr>,
    registry: PeerRegistry,
    outbound_tasks: Arc<Mutex<OutboundTasks>>,
    gc_prefix: String,
    gc_initiate_interval: Duration,
    gc_observe_interval: Duration,
    gc_cleanup_interval: Duration,
}

impl Server {
    pub fn new(
        listen_addr: SocketAddr,
        advertise_addr: SocketAddr,
        node_name: &str,
        client_addr: Option<SocketAddr>,
        gc_prefix: String,
        gc_initiate_interval: Duration,
        gc_observe_interval: Duration,
        gc_cleanup_interval: Duration,
    ) -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            node_name: node_name.to_string(),
            listen_addr,
            advertise_addr,
            client_addr,
            registry: PeerRegistry::new(),
            outbound_tasks: Arc::new(Mutex::new(OutboundTasks::new())),
            gc_prefix,
            gc_initiate_interval,
            gc_observe_interval,
            gc_cleanup_interval,
        }
    }

    pub async fn run(&self, discovery_cfg: DiscoveryConfig) -> anyhow::Result<()> {
        let (app_tx, mut app_rx) = mpsc::channel::<(SocketAddr, Envelope)>(1024);

        // ── Build dissemination strategy ─────────────────────────────
        // Pull-only: peers periodically request deltas from each other.
        let dissemination: SharedDissemination =
            Arc::new(PullPeriodic::new(self.registry.clone(), std::time::Duration::from_secs(1)));

        let gc_storage_client = S3Client::new(
            &discovery_cfg.endpoint,
            &discovery_cfg.region,
            &discovery_cfg.access_key,
            &discovery_cfg.secret_key,
        );
        let gc_config = GcConfig {
            bucket: discovery_cfg.bucket.clone(),
            prefix: self.gc_prefix.clone(),
            registration_ttl: discovery_cfg.registration_ttl,
            initiate_interval: self.gc_initiate_interval,
            observe_interval: self.gc_observe_interval,
            cleanup_interval: self.gc_cleanup_interval,
        };

        // ── Build CRDT engine (OR-Set<String>) ───────────────────────
        let engine = CrdtEngine::<OrSet<String>>::new(
            self.node_id.clone(),
            "default-orset".to_string(),
            OrSet::new(),
            dissemination.clone(),
            Some((gc_storage_client, gc_config)),
        );

        engine.new_replica_bootstrap().await?;
        let mut gc_loop_handles: Option<Vec<JoinHandle<()>>> = Some(engine.start_gc_loops().await);

        // The dissemination layer owns the pull loop.
        let pull_handle = dissemination
            .start_pull_loop(Arc::new(engine.clone()) as Arc<dyn PullRoundEngine>);

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
                                let (mut read_half, mut write_half) = stream.into_split();
                                loop {
                                    // Read 4-byte big-endian length prefix.
                                    let mut len_buf = [0u8; 4];
                                    if read_half.read_exact(&mut len_buf).await.is_err() {
                                        break; // connection closed
                                    }
                                    let msg_len = u32::from_be_bytes(len_buf) as usize;
                                    let mut msg_buf = vec![0u8; msg_len];
                                    if read_half.read_exact(&mut msg_buf).await.is_err() {
                                        break;
                                    }

                                    use prost::Message as _;
                                    let response = match proto::ProtoClientCommand::decode(msg_buf.as_slice()) {
                                        Ok(cmd) => match cmd.command {
                                            Some(proto::proto_client_command::Command::Add(value)) => {
                                                eng2.client_operation(OrSetOp::Add(value)).await;
                                                "ok".to_string()
                                            }
                                            Some(proto::proto_client_command::Command::Remove(value)) => {
                                                eng2.client_operation(OrSetOp::Remove(value)).await;
                                                "ok".to_string()
                                            }
                                            Some(proto::proto_client_command::Command::PrintState(_)) => {
                                                eng2.print_state().await
                                            }
                                            Some(proto::proto_client_command::Command::PrintInternals(_)) => {
                                                eng2.print_internals().await
                                            }
                                            None => "error: empty command".to_string(),
                                        },
                                        Err(e) => {
                                            warn!(%peer, %e, "invalid client command; failed to decode protobuf");
                                            format!("error: {e}")
                                        }
                                    };

                                    // Send response: 4-byte length prefix + UTF-8 bytes.
                                    let resp_bytes = response.into_bytes();
                                    let resp_len = resp_bytes.len() as u32;
                                    if write_half.write_all(&resp_len.to_be_bytes()).await.is_err() {
                                        break;
                                    }
                                    if write_half.write_all(&resp_bytes).await.is_err() {
                                        break;
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
            registry: self.registry.clone(),
            app_tx: app_tx.clone(),
            outbound_tasks: Arc::clone(&self.outbound_tasks),
        };

        // Main discovery object is moved to its task, 
        // but we create a separate handle for shutdown deregistration.
        let shutdown_discovery = Discovery::new(
            discovery_cfg,
            self.node_id.clone(),
            self.node_name.clone(),
            self.advertise_addr,
        )
        .await?;

        // ── Spawn discovery reconciliation loop ──────────────────────
        let disc_registry = self.registry.clone();
        let disc_connector = connector.clone();
        let discovery_handle = tokio::spawn(async move {
            discovery
                .run_discovery_loop(disc_registry, disc_connector)
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

        // TODO: just for testing: periodically perform random client ops and print state
        /* let mut update_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut updates = 0; */

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, remote_addr)) => {
                            info!(%remote_addr, "accepted inbound connection");
                            let mgr = self.registry.clone();
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

                /* _ = update_interval.tick() => {
                    if updates < 20 {
                        if rand::random::<f64>() < 0.2 {
                            if let Some(random_element) = engine.get_random_element().await {
                                let op = OrSetOp::Remove(random_element);
                                engine.client_operation(op).await;
                            }
                        } else {
                            let op = OrSetOp::Add(format!("tick-{}", chrono::Utc::now().timestamp()));
                            engine.client_operation(op).await;
                        }
                    }
                    engine.print_state().await;
                    updates += 1;
                } */

                Some((addr, envelope)) = app_rx.recv() => {
                    // Route CRDT operations to the engine.
                    match envelope.payload {
                        Some(Payload::CrdtOp(crdt_op)) => {
                            let origin_id = crdt_op.origin_node_id;
                            let cid = crdt_op.crdt_id;
                            let requester_knowledge = crdt_op.requester_knowledge;
                            engine.server_delta(
                                    origin_id.clone(),
                                    cid.clone(),
                                    crdt_op.payload,
                                )
                                .await;
                            // Handle a piggybacked VV request: the sender wants us to
                            // send a delta back for what they are missing.
                            if let Some(vc) = requester_knowledge {
                                if !vc.entries.is_empty() {
                                    engine.server_pull_request(
                                            origin_id,
                                            cid,
                                            vc.entries,
                                        )
                                        .await;
                                }
                            }
                        }
                        Some(Payload::CrdtPullRequest(req)) => {
                            let knowledge = req.knowledge
                                .map(|vc| vc.entries)
                                .unwrap_or_default();
                            engine.server_pull_request(
                                    req.origin_node_id,
                                    req.crdt_id,
                                    knowledge,
                                )
                                .await;
                        }
                        other => {
                            info!(%addr, ?other, "app received non-CRDT message");
                        }
                    }
                }

                // TODO: also handle all other shutdown signals (SIGINT, SIGTERM, etc.) 
                // and do graceful shutdown.
                _ = tokio::signal::ctrl_c() => {
                    info!("shutdown signal received — deregistering from S3");
                    discovery_handle.abort();
                    pull_handle.abort();
                    if let Some(handles) = gc_loop_handles.take() {
                        for h in handles {
                            h.abort();
                        }
                    }
                    if let Some(h) = client_handle {
                        h.abort();
                    }

                    if let Err(e) = shutdown_discovery.deregister().await {
                        error!(%e, "failed to deregister from S3");
                    }
                    if let Err(e) = engine.remove_gc_clock().await {
                        error!(%e, "failed to remove GC clock entry on shutdown");
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
