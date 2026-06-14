use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tracing::info;

use crate::network::connection;
use crate::peers::peer_registry::PeerRegistry;
use crate::common::{self, NodeId};
use crate::proto::Envelope;
use crate::server::types::OutboundTasks;

#[derive(Clone)]
pub struct PeerConnector {
    node_id: String,
    node_name: String,
    gc_replica: bool,
    registry: PeerRegistry,
    app_tx: mpsc::Sender<(SocketAddr, Envelope)>,
    outbound_tasks: Arc<Mutex<OutboundTasks>>,
}

impl PeerConnector {
    pub fn new(
        node_id: String,
        node_name: String,
        gc_replica: bool,
        registry: PeerRegistry,
        app_tx: mpsc::Sender<(SocketAddr, Envelope)>,
        outbound_tasks: Arc<Mutex<OutboundTasks>>,
    ) -> Self {
        Self {
            node_id,
            node_name,
            gc_replica,
            registry,
            app_tx,
            outbound_tasks,
        }
    }

    pub async fn add_peer(&self, node_id: NodeId, addr: String) -> anyhow::Result<()> {
        let mut tasks = self.outbound_tasks.lock().await;
        if tasks.contains_key(&node_id) {
            return Ok(()); // already connected
        }
        let address = common::lookup(&addr).await?;
        let handle = connection::spawn_outbound(
            address,
            self.node_id.clone(),
            self.node_name.clone(),
            self.gc_replica,
            self.registry.clone(),
            self.app_tx.clone(),
        );
        tasks.insert(node_id, (address, handle));
        info!(%addr, "outbound peer task spawned");
        Ok(())
    }

    pub async fn remove_peer(&self, node_id: NodeId) {
        if let Some((addr, handle)) = self.outbound_tasks.lock().await.remove(&node_id) {
            handle.abort();
            info!(%addr, "outbound task aborted");
        }
        self.registry.remove(&node_id);
    }
}

