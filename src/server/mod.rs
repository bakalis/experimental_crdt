pub mod types;
pub mod client_requests_handler;
pub mod peer_message_handler;
pub mod initializer;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, error};
use tokio::signal::unix::{signal, SignalKind};

use crate::server::types::{CrdtType, EngineType, OutboundTasks};
use crate::network::connection;
use crate::engine::crdt_engine::{CrdtEngineRequest};
use crate::crdt::or_set::OrSet;
use crate::discovery::{Discovery, DiscoveryConfig};
use crate::network::dissemination::{PullPeriodic, SharedDissemination};
use crate::gc::GcConfig;
use crate::peers::peer_registry::PeerRegistry;
use crate::proto::Envelope;
use crate::storage::s3_client::S3Client;

// ── Server ──────────────────────────────────────────────────────────────
pub struct ServerConfig {
    pub listen_host: String,
    pub listen_port: String,
    pub node_name: String,
    pub gc_replica: bool,
    pub client_port: Option<String>,
}

pub struct Server {
    node_id: String,
    node_name: String,
    gc_replica: bool,
    listen_port: String,
    client_port: Option<String>,
    registry: PeerRegistry,
    discovery: Arc<Discovery>,
    outbound_tasks: Arc<Mutex<OutboundTasks>>,
}

async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    sigterm.recv().await;
}

impl Server {
    pub async fn new(
        server_config: ServerConfig,
        discovery_config: DiscoveryConfig,
    ) -> anyhow::Result<Self> {
        let s3_client = S3Client::new(
            &discovery_config.endpoint,
            &discovery_config.region,
            &discovery_config.access_key,
            &discovery_config.secret_key,
        );

         // TODO: this is done to help testing by enforcing specific network topologies (see discovery_connect_node_ids). 
        let node_id = server_config.node_name.clone();

        let discovery = Discovery::new(
            s3_client,
            discovery_config.clone(),
            node_id.clone(),
            server_config.node_name.clone(),
            server_config.listen_host.clone(),
            server_config.listen_port.clone()
        )
        .await?;

        Ok(Self {
            node_id,
            node_name: server_config.node_name.to_string(),
            gc_replica: server_config.gc_replica,
            listen_port: server_config.listen_port,
            client_port: server_config.client_port,
            registry: PeerRegistry::new(),
            discovery: Arc::new(discovery),
            outbound_tasks: Arc::new(Mutex::new(OutboundTasks::new())),
        })
    }

    pub async fn run(&self, gc_config: GcConfig) -> anyhow::Result<()> {
        let (app_tx, mut app_rx) = mpsc::channel::<(SocketAddr, Envelope)>(1024);
        let prefix = gc_config.storage_config.prefix.clone();

        // ── Build dissemination strategy ─────────────────────────────
        // Pull-only: peers periodically request deltas from each other.
        let dissemination: SharedDissemination<CrdtType> = Arc::new(PullPeriodic::new(
            self.registry.clone(),
            std::time::Duration::from_secs(1),
        ));

        let gc_storage_client = self.discovery.client.clone();

        let (engine_tx, engine_rx) = tokio::sync::mpsc::channel::<CrdtEngineRequest<CrdtType>>(1024);

        // ── Build CRDT engine (OR-Set<String>) ───────────────────────
        let engine: EngineType = EngineType::new(
            self.node_id.clone(),
            "default-orset".to_string(),
            OrSet::new(),
            self.registry.clone(),
            dissemination.clone(),
            (gc_storage_client, gc_config),
            engine_rx,
        );

        let (handles, listener) = initializer::start_background_tasks(self, engine, engine_tx.clone(), dissemination.clone(), app_tx.clone())
            .await?;

        let print_metrics_interval = std::time::Duration::from_secs(10);
        let mut metrics_interval = tokio::time::interval(print_metrics_interval);

        // Run Main Loop
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    peer_message_handler::handle_accepted_connection(self.node_id.clone(),
                    self.node_name.clone(), self.gc_replica, self.registry.clone(),
                    app_tx.clone(), accept_result);
                }
                
                Some((addr, envelope)) = app_rx.recv() => {
                    peer_message_handler::handle_received_envelope(envelope, addr, engine_tx.clone()).await;
                }

                _ = metrics_interval.tick() => {
                    engine_tx.send(CrdtEngineRequest::LogCrdtMetrics).await?;
                }

                // TODO: also handle all other shutdown signals (SIGINT, SIGTERM, etc.)
                // and do graceful shutdown.
                _ = tokio::signal::ctrl_c() => {
                    self.handle_shutdown(engine_tx.clone(), &handles, prefix).await?;
                    return Ok(());
                }
                _ = shutdown_signal() => {
                    self.handle_shutdown(engine_tx.clone(), &handles, prefix).await?;
                    return Ok(());
                }
            }
        }
    }

    pub async fn handle_shutdown(
        &self,
        engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<CrdtType>>,
        handles: &[JoinHandle<()>],
        gc_prefix: String,
    ) -> anyhow::Result<()> {
        info!("shutdown signal received — deregistering from S3");
        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel();
        engine_tx.send(CrdtEngineRequest::DeregistrationState(tx_shutdown)).await?;
        let (final_dot, final_state) = rx_shutdown.await?;

        if let Err(e) = self.discovery.deregister(gc_prefix, final_dot, final_state).await {
            error!(%e, "failed to deregister from S3");
        }

        for h in handles {
            h.abort();
        }
        
        let mut tasks = self.outbound_tasks.lock().await;
        for (_, (addr, handle)) in tasks.drain() {
            handle.abort();
            info!(%addr, "aborted outbound task");
        }

        info!(node_id = %self.node_id, "shutdown complete");
        Ok(())
    }
}
