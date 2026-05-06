//! Thread-safe registry of connected peers.
//!
//! Uses [`DashMap`] for lock-free concurrent reads with fine-grained
//! locking on writes — ideal for a read-heavy peer table that is
//! mutated infrequently (add/remove peers at runtime).

use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{common::NodeId, proto::Envelope};

/// Handle held per connected peer — allows sending messages into
/// that peer's write loop.
#[derive(Debug, Clone)]
pub struct PeerHandle {
    /// Send side of the channel that feeds the peer's writer task.
    pub tx: mpsc::Sender<Envelope>,
}

/// Concurrent, clonable registry of all active peers.
#[derive(Debug, Clone)]
pub struct PeerRegistry {
    peers: Arc<DashMap<NodeId, (SocketAddr, PeerHandle)>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(DashMap::new()),
        }
    }

    /// Register (or replace) a peer handle.
    pub fn insert(&self, node_id: NodeId, addr: SocketAddr, handle: PeerHandle) {
        info!(%addr, "peer registered");
        self.peers.insert(node_id, (addr, handle));
    }

    /// Remove a peer from the registry. Returns the old handle if present.
    pub fn remove(&self, node_id: &NodeId) -> Option<PeerHandle> {
        let removed = self.peers.remove(node_id).map(|(_, (_, h))| h);
        if removed.is_some() {
            info!(%node_id, "peer removed");
        } else {
            warn!(%node_id, "tried to remove unknown peer");
        }
        removed
    }

    /// Retrieve a cloned handle for a specific peer.
    pub fn get(&self, node_id: &NodeId) -> Option<(SocketAddr, PeerHandle)> {
        self.peers.get(node_id).map(|r| r.value().clone())
    }

    /// Snapshot of all current peer node_ids.
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.peers.iter().map(|r| r.key().clone()).collect()
    }
    
    pub fn send_to_peer(&self, node_id: &NodeId, envelope: Envelope) -> Result<(), String> {
        if let Some(entry) = self.peers.get(node_id) {
            let addr = entry.value().0;
            entry.value().1.tx.try_send(envelope).map_err(|e| {
                format!("failed to send to peer {}: {}", node_id, e)
            })
        } else {
            Err(format!("peer {} not found", node_id))
        }
    }

    /// Broadcast an envelope to **all** connected peers.
    /// Skips peers whose channel is full / closed and logs a warning.
    pub fn broadcast(&self, envelope: Envelope) {
        for entry in self.peers.iter() {
            let addr = entry.value().0;
            if let Err(e) = entry.value().1.tx.try_send(envelope.clone()) {
                warn!(%addr, %e, "failed to enqueue message for peer");
            }
        }
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}
