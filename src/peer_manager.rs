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

use crate::proto::Envelope;

/// Handle held per connected peer — allows sending messages into
/// that peer's write loop.
#[derive(Debug, Clone)]
pub struct PeerHandle {
    pub node_id: Option<String>,
    pub addr: SocketAddr,
    /// Send side of the channel that feeds the peer's writer task.
    pub tx: mpsc::Sender<Envelope>,
}

/// Concurrent, clonable registry of all active peers.
#[derive(Debug, Clone)]
pub struct PeerManager {
    peers: Arc<DashMap<SocketAddr, PeerHandle>>,
}

impl PeerManager {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(DashMap::new()),
        }
    }

    /// Register (or replace) a peer handle.
    pub fn insert(&self, addr: SocketAddr, handle: PeerHandle) {
        info!(%addr, "peer registered");
        self.peers.insert(addr, handle);
    }

    /// Remove a peer from the registry. Returns the old handle if present.
    pub fn remove(&self, addr: &SocketAddr) -> Option<PeerHandle> {
        let removed = self.peers.remove(addr).map(|(_, h)| h);
        if removed.is_some() {
            info!(%addr, "peer removed");
        } else {
            warn!(%addr, "tried to remove unknown peer");
        }
        removed
    }

    /// Retrieve a cloned handle for a specific peer.
    pub fn get(&self, addr: &SocketAddr) -> Option<PeerHandle> {
        self.peers.get(addr).map(|r| r.value().clone())
    }

    /// Snapshot of all current peer addresses.
    pub fn peer_addrs(&self) -> Vec<SocketAddr> {
        self.peers.iter().map(|r| *r.key()).collect()
    }

    /// Broadcast an envelope to **all** connected peers.
    /// Skips peers whose channel is full / closed and logs a warning.
    pub async fn broadcast(&self, envelope: Envelope) {
        for entry in self.peers.iter() {
            let addr = *entry.key();
            if let Err(e) = entry.value().tx.try_send(envelope.clone()) {
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
