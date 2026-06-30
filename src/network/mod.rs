use crate::common::NodeId;
use crate::peers::peer_registry::PeerRegistry;
use crate::proto::Envelope;
use tokio::sync::mpsc;

pub mod tcp_connection;
pub mod protocol;
pub mod dissemination;
pub mod simulated;

pub trait Network: Send + Sync {
    fn spawn_outbound(
        &self,
        address: String,
        local_node_id: NodeId,
        local_node_name: NodeId,
        gc_replica: bool,
        registry: PeerRegistry,
        app_tx: mpsc::Sender<Envelope>,
    ) -> tokio::task::JoinHandle<()>;

    fn start_network_background(&self, listen_port: String,
        node_id: String,
        node_name: String,
        gc_replica: bool,
        registry: PeerRegistry,
        app_tx: mpsc::Sender<Envelope>,
    ) -> tokio::task::JoinHandle<()>;

    fn allow_bidirectional(&self) -> bool {
        false
    }
}
