pub mod types;
pub mod client_requests_handler;
pub mod peer_message_handler;
pub mod initializer;

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, error};

use crate::network::Network;
use crate::server::types::{CrdtType, EngineType, OutboundTasks};
use crate::engine::crdt_engine::{CrdtEngineRequest};
use crate::crdt::or_set::OrSet;
use crate::discovery::{Discovery, DiscoveryConfig};
use crate::network::dissemination::{PullPeriodic, SharedDissemination};
use crate::gc::GcConfig;
use crate::peers::peer_registry::PeerRegistry;
use crate::proto::Envelope;
use crate::storage::s3_client::S3Client;

// ── Server ──────────────────────────────────────────────────────────────
#[derive(Clone)]
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

    pub async fn run(&self, gc_config: GcConfig,
        app_tx: mpsc::Sender<Envelope>,
        mut app_rx: mpsc::Receiver<Envelope>,
        network: Arc<dyn Network>,
        shutdown: CancellationToken
    ) -> anyhow::Result<()> {
        let prefix = gc_config.storage_config.prefix.clone();

        // ── Build dissemination strategy ─────────────────────────────
        // Pull-only: peers periodically request deltas from each other.
        let dissemination: SharedDissemination<CrdtType> = Box::new(PullPeriodic::new(
            self.registry.clone(),
            std::time::Duration::from_secs(1),
        ));

        let gc_storage_client = self.discovery.client.clone();

        let (engine_tx, engine_rx) = tokio::sync::mpsc::channel::<CrdtEngineRequest<CrdtType>>(1024);

        let mut handles = vec![];

        if let Some(dissemination_handle) = dissemination.start_pull_loop(engine_tx.clone()) {
            handles.push(dissemination_handle);
        }

        // ── Build CRDT engine (OR-Set<String>) ───────────────────────
        let engine: EngineType = EngineType::new(
            self.node_id.clone(),
            "default-orset".to_string(),
            OrSet::new(),
            self.registry.clone(),
            dissemination,
            (gc_storage_client, gc_config),
            engine_rx,
        );

        let new_handles = initializer::start_background_tasks(self, engine, engine_tx.clone(), app_tx.clone(), network, self.registry.clone())
            .await?;

        handles.extend(new_handles);

        let print_metrics_interval = std::time::Duration::from_secs(10);
        let mut metrics_interval = tokio::time::interval(print_metrics_interval);

        // Run Main Loop
        loop {
            tokio::select! {
                Some(envelope) = app_rx.recv() => {
                    peer_message_handler::handle_received_envelope(self.node_id.clone(), envelope, engine_tx.clone()).await;
                }

                _ = metrics_interval.tick() => {
                    engine_tx.send(CrdtEngineRequest::LogCrdtMetrics).await?;
                }

                _ = shutdown.cancelled() => {
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
        for (node_id, handle) in tasks.drain() {
            handle.abort();
            info!(%node_id, "aborted outbound task");
        }

        info!(node_id = %self.node_id, "shutdown complete");
        Ok(())
    }
}
