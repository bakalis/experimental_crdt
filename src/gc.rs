//! Epoch-fenced garbage collection for CRDT tombstones.
//!
//! This module implements a safe garbage collection protocol for distributed CRDTs
//! based on epoch fencing. The protocol ensures that tombstones are only collected
//! when all active replicas have seen them, preventing causal violations.
//!
//! ## Protocol Overview
//!
//! 1. **Epochs**: Global monotonic counter that advances periodically
//! 2. **Epoch Fencing**: Each tombstone is tagged with the epoch when it was created
//! 3. **Peer Tracking**: Each node tracks the minimum epoch acknowledged by all peers
//! 4. **Safe Collection**: Tombstones from epoch E can be GC'd when all peers
//!    have acknowledged epoch E+SAFETY_MARGIN
//!
//! ## Safety Guarantees
//!
//! - A tombstone is never collected while any replica might need it
//! - Replicas that rejoin after extended downtime can catch up via full sync
//! - No coordination or consensus required for epoch advancement

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::common::NodeId;
use crate::logical_clocks::dot_version_vector::Dot;

/// A monotonically increasing epoch counter used for GC fencing.
pub type Epoch = u64;

/// Safety margin: how many epochs to wait before collecting tombstones.
/// This provides tolerance for delayed messages and temporary partitions.
const EPOCH_SAFETY_MARGIN: u64 = 2;

/// Configuration for the GC coordinator.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// How often to advance the local epoch.
    pub epoch_interval: Duration,
    /// How often to attempt garbage collection.
    pub gc_interval: Duration,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            epoch_interval: Duration::from_secs(30),
            gc_interval: Duration::from_secs(60),
        }
    }
}

/// Tracks GC-related metadata for each peer.
#[derive(Debug, Clone)]
pub struct PeerGcInfo {
    /// The latest epoch this peer has acknowledged.
    pub last_ack_epoch: Epoch,
    /// The last time we received an epoch acknowledgment from this peer.
    pub last_seen: std::time::Instant,
}

impl PeerGcInfo {
    fn new(epoch: Epoch) -> Self {
        Self {
            last_ack_epoch: epoch,
            last_seen: std::time::Instant::now(),
        }
    }
}

/// Manages garbage collection state for a CRDT replica.
///
/// This structure tracks:
/// - The current local epoch
/// - Each peer's acknowledged epoch
/// - Tombstones that are candidates for collection
#[derive(Debug)]
pub struct GcCoordinator {
    /// This node's ID.
    pub node_id: NodeId,
    /// Current epoch for this replica.
    current_epoch: Arc<Mutex<Epoch>>,
    /// Tracks the latest acknowledged epoch for each peer.
    peer_epochs: Arc<DashMap<NodeId, PeerGcInfo>>,
}

impl GcCoordinator {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            current_epoch: Arc::new(Mutex::new(0)),
            peer_epochs: Arc::new(DashMap::new()),
        }
    }

    /// Get the current epoch for this replica.
    pub async fn current_epoch(&self) -> Epoch {
        *self.current_epoch.lock().await
    }

    /// Advance to the next epoch.
    ///
    /// Called periodically by the GC coordinator task.
    pub async fn advance_epoch(&self) -> Epoch {
        let mut epoch = self.current_epoch.lock().await;
        *epoch += 1;
        let new_epoch = *epoch;
        info!(
            node_id = %self.node_id,
            epoch = new_epoch,
            "advanced to new GC epoch"
        );
        new_epoch
    }

    /// Record an epoch acknowledgment from a peer.
    ///
    /// This is called when we receive an epoch marker in a message from a peer,
    /// or when a peer explicitly acknowledges our epoch broadcast.
    pub fn record_peer_epoch(&self, peer_id: &NodeId, epoch: Epoch) {
        self.peer_epochs
            .entry(peer_id.clone())
            .and_modify(|info| {
                if epoch > info.last_ack_epoch {
                    info.last_ack_epoch = epoch;
                    info.last_seen = std::time::Instant::now();
                }
            })
            .or_insert_with(|| PeerGcInfo::new(epoch));

        debug!(
            node_id = %self.node_id,
            peer_id = %peer_id,
            epoch = epoch,
            "recorded peer epoch acknowledgment"
        );
    }

    /// Remove a peer from tracking when they disconnect.
    pub fn remove_peer(&self, peer_id: &NodeId) {
        self.peer_epochs.remove(peer_id);
        info!(
            node_id = %self.node_id,
            peer_id = %peer_id,
            "removed peer from GC tracking"
        );
    }

    /// Calculate the minimum epoch that all known peers have acknowledged.
    ///
    /// Returns None if there are no known peers (single-node cluster).
    pub fn min_peer_epoch(&self) -> Option<Epoch> {
        if self.peer_epochs.is_empty() {
            return None;
        }

        self.peer_epochs
            .iter()
            .map(|entry| entry.value().last_ack_epoch)
            .min()
    }

    /// Calculate the safe collection epoch: tombstones from epochs <= this
    /// value can be safely garbage collected.
    ///
    /// Returns None if GC is not yet safe (e.g., not enough epochs have passed,
    /// or we don't have enough peer information).
    pub async fn safe_collection_epoch(&self) -> Option<Epoch> {
        let current = *self.current_epoch.lock().await;

        // Can't collect if we haven't advanced enough epochs yet.
        if current < EPOCH_SAFETY_MARGIN {
            return None;
        }

        // If we have no peers, we can safely collect up to current - SAFETY_MARGIN.
        // This is safe because we're the only node, so we know all tombstones
        // created before current - SAFETY_MARGIN are no longer needed.
        if self.peer_epochs.is_empty() {
            return Some(current - EPOCH_SAFETY_MARGIN);
        }

        // With peers, we can only collect when all peers have acknowledged an epoch.
        let min_peer = self.min_peer_epoch().unwrap(); // Safe: we checked is_empty above

        // Safe to collect if:
        // 1. All peers have acknowledged epoch E
        // 2. We're now at epoch E + SAFETY_MARGIN or later
        if min_peer + EPOCH_SAFETY_MARGIN <= current {
            Some(min_peer)
        } else {
            None
        }
    }

    /// Get a snapshot of all peer epochs for debugging.
    pub fn peer_epoch_snapshot(&self) -> HashMap<NodeId, Epoch> {
        self.peer_epochs
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().last_ack_epoch))
            .collect()
    }
}

/// Metadata attached to each tombstone to track when it can be collected.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TombstoneMetadata {
    /// The epoch when this tombstone was created.
    pub creation_epoch: Epoch,
    /// The dot of the add operation being tombstoned.
    pub add_dot: Dot,
    /// The dot of the remove operation.
    pub remove_dot: Dot,
}

impl TombstoneMetadata {
    pub fn new(creation_epoch: Epoch, add_dot: Dot, remove_dot: Dot) -> Self {
        Self {
            creation_epoch,
            add_dot,
            remove_dot,
        }
    }

    /// Check if this tombstone can be collected given a safe collection epoch.
    pub fn can_collect(&self, safe_epoch: Option<Epoch>) -> bool {
        match safe_epoch {
            Some(safe) => self.creation_epoch <= safe,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_epoch_advancement() {
        let gc = GcCoordinator::new("node1".to_string());
        assert_eq!(gc.current_epoch().await, 0);

        gc.advance_epoch().await;
        assert_eq!(gc.current_epoch().await, 1);

        gc.advance_epoch().await;
        assert_eq!(gc.current_epoch().await, 2);
    }

    #[tokio::test]
    async fn test_peer_tracking() {
        let gc = GcCoordinator::new("node1".to_string());

        gc.record_peer_epoch(&"node2".to_string(), 5);
        gc.record_peer_epoch(&"node3".to_string(), 3);

        assert_eq!(gc.min_peer_epoch(), Some(3));

        // Update node3's epoch
        gc.record_peer_epoch(&"node3".to_string(), 6);
        assert_eq!(gc.min_peer_epoch(), Some(5));
    }

    #[tokio::test]
    async fn test_safe_collection_epoch() {
        let gc = GcCoordinator::new("node1".to_string());

        // Initially, no safe epoch (current epoch is 0)
        assert_eq!(gc.safe_collection_epoch().await, None);

        // Advance epochs
        for _ in 0..5 {
            gc.advance_epoch().await;
        }
        // Current epoch is now 5

        // With no peers, safe epoch is current - SAFETY_MARGIN = 5 - 2 = 3
        assert_eq!(gc.safe_collection_epoch().await, Some(3));

        // Add peers
        gc.record_peer_epoch(&"node2".to_string(), 2);
        gc.record_peer_epoch(&"node3".to_string(), 3);

        // Min peer epoch is 2, so safe epoch is still 2 (not 2 + 2 <= 5, but we need min)
        // Safe epoch calculation: min_peer must be at least (current - SAFETY_MARGIN)
        // For current=5, we need min_peer + SAFETY_MARGIN <= 5
        // If min_peer=2, then 2 + 2 = 4 <= 5, so safe_epoch = 2
        assert_eq!(gc.safe_collection_epoch().await, Some(2));

        // Advance current epoch to 10
        for _ in 0..5 {
            gc.advance_epoch().await;
        }

        // Min peer is still 2, so safe epoch is still 2
        assert_eq!(gc.safe_collection_epoch().await, Some(2));

        // Update peers
        gc.record_peer_epoch(&"node2".to_string(), 7);
        gc.record_peer_epoch(&"node3".to_string(), 7);

        // Now min_peer=7, current=10, so 7 + 2 = 9 <= 10, safe_epoch = 7
        assert_eq!(gc.safe_collection_epoch().await, Some(7));
    }

    #[tokio::test]
    async fn test_tombstone_can_collect() {
        let dot1 = Dot::new("node1".to_string(), 10);
        let dot2 = Dot::new("node1".to_string(), 15);

        let ts = TombstoneMetadata::new(5, dot1, dot2);

        // No safe epoch yet
        assert!(!ts.can_collect(None));

        // Safe epoch is less than creation epoch
        assert!(!ts.can_collect(Some(4)));

        // Safe epoch equals creation epoch
        assert!(ts.can_collect(Some(5)));

        // Safe epoch is greater than creation epoch
        assert!(ts.can_collect(Some(10)));
    }
}
