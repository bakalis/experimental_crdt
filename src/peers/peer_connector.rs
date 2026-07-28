use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tracing::info;

use crate::network::Network;
use crate::peers::peer_registry::PeerRegistry;
use crate::common::NodeId;
use crate::proto::Envelope;
use crate::server::types::OutboundTasks;

#[derive(Clone)]
pub struct PeerConnector {
    node_id: String,
    node_name: String,
    gc_replica: bool,
    registry: PeerRegistry,
    app_tx: mpsc::Sender<Envelope>,
    outbound_tasks: Arc<Mutex<OutboundTasks>>,
    network: Arc<dyn Network>
}

impl PeerConnector {
    pub fn new(
        node_id: String,
        node_name: String,
        gc_replica: bool,
        registry: PeerRegistry,
        app_tx: mpsc::Sender<Envelope>,
        outbound_tasks: Arc<Mutex<OutboundTasks>>,
        network: Arc<dyn Network>
    ) -> Self {
        Self {
            node_id,
            node_name,
            gc_replica,
            registry,
            app_tx,
            outbound_tasks,
            network
        }
    }

    pub async fn add_peer(&self, node_id: NodeId, addr: String) -> anyhow::Result<()> {
        if !self.network.allow_bidirectional() && self.node_id >= node_id {
            info!(%node_id, "skipping peer with lower or equal node_id");
            return Ok(()); // skip connecting to peers with lower or equal node_id
        }

        let mut tasks = self.outbound_tasks.lock().await;
        if tasks.contains_key(&node_id) {
            return Ok(()); // already connected
        }
        let handle = self.network.spawn_outbound(
            addr.clone(),
            self.node_id.clone(),
            self.node_name.clone(),
            self.gc_replica,
            self.registry.clone(),
            self.app_tx.clone(),
        );
        tasks.insert(node_id, handle);
        info!(%addr, "outbound peer task spawned");
        Ok(())
    }

    pub async fn remove_peer(&self, node_id: NodeId) {
        if let Some(handle) = self.outbound_tasks.lock().await.remove(&node_id) {
            handle.abort();
            info!(%node_id, "outbound task aborted");
        }
        self.registry.remove(&node_id);
    }
}

