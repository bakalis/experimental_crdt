#![allow(dead_code)]

use core::option::Option::Some;
use prost::Message;
use std::collections::HashMap;
use tokio::sync::mpsc;
use std::sync::Arc;
use tracing::{error, debug, warn};

use crate::metric;
use crate::network::Network;
use crate::common::NodeId;
use crate::common::error::Result;
use crate::peers::peer_registry::{PeerHandle, PeerRegistry};
use crate::proto::Envelope;

pub type PeerChannels = Arc<HashMap<String, (NodeId, bool, mpsc::Sender<Envelope>)>>;

const CHANNEL_BUF: usize = 256;

#[derive(Clone)]
pub struct SimulatedNetwork {
    pub peer_channels: PeerChannels,
}

impl Network for SimulatedNetwork {
    fn spawn_outbound(
        &self,
        address: String,
        local_node_id: NodeId,
        _local_node_name: NodeId,
        _gc_replica: bool,
        registry: PeerRegistry,
        _app_tx: mpsc::Sender<Envelope>,
    ) -> tokio::task::JoinHandle<()> {
        self.spawn_outbound(address, local_node_id, registry)
    }

    fn allow_bidirectional(&self) -> bool {
       true 
    }

    fn start_network_background(&self, listen_port: String,
        node_id: String,
        node_name: String,
        gc_replica: bool,
        _registry: PeerRegistry,
        _app_tx: mpsc::Sender<Envelope>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            debug!(%listen_port, %node_id, %node_name, %gc_replica, "simulated network running");
        })
    }
}

impl SimulatedNetwork {
    pub fn new(channels: HashMap<String, (NodeId, bool, mpsc::Sender<Envelope>)>) -> Self {
        Self {
            peer_channels: Arc::new(channels),
        }
    }

    pub fn spawn_outbound(
        &self,
        address: String,
        local_node_id: NodeId,
        registry: PeerRegistry,
    ) -> tokio::task::JoinHandle<()> {
        let peer_channels = Arc::clone(&self.peer_channels);
        tokio::spawn(async move {
            if let Err(e) = Self::run_connection(
                peer_channels,
                address,
                &local_node_id,
                &registry,
            )
            .await
            {
                warn!(%e, "connection terminated");
            }
        })
    }

    async fn run_connection(
        peer_channels: PeerChannels,
        addr: String,
        local_node_id: &str,
        registry: &PeerRegistry,
    ) -> Result<NodeId> {
        // ── handshake ───────────────────────────────────────────────────
        // ── register in peer registry ────────────────────────────────────
        if let Some((remote_node_id, remote_gc_replica, remote_tx)) = peer_channels.get(&addr) {
            let (tx, rx) = mpsc::channel::<Envelope>(CHANNEL_BUF);

            registry.insert(
                remote_node_id.clone(),
                *remote_gc_replica,
                PeerHandle { tx },
            );

            // ── spawn writer task ───────────────────────────────────────────
            Self::writer_loop(local_node_id.to_string(), remote_node_id.to_string(), remote_tx.clone(), rx).await;
            return Ok(remote_node_id.to_string());
        }
        Ok(local_node_id.to_string())
    }

    async fn writer_loop(
        local_node_id: NodeId,
        remote_node_id: NodeId,
        remote_tx: mpsc::Sender<Envelope>,
        mut rx: mpsc::Receiver<Envelope>,
    ) {
        while let Some(envelope) = rx.recv().await {
            let len = envelope.encoded_len();
            metric!(node_id = local_node_id, event = "send_envelope", size_bytes = len as u64);
            if let Err(e) = remote_tx.send(envelope).await {
                error!(%remote_node_id, %e, "write failed — exiting writer");
                return;
            }
        }
        debug!(%remote_node_id, "writer channel closed");
    }
}
