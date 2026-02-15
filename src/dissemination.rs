//! Dissemination layer — Push, Pull, and Push-Pull strategies.
//!
//! All strategies produce `Envelope` messages using the existing
//! protobuf `CrdtOp` message. No schema changes required.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{info, debug, warn};

use crate::common::NodeId;
use crate::peer_manager::PeerManager;
use crate::proto::{self, envelope::Payload, Envelope};

// ── Trait ───────────────────────────────────────────────────────────────

#[async_trait]
pub trait DisseminationStrategy: Send + Sync + 'static {
    /// Called after a local mutation to push a delta outward.
    /// Pull-only strategies no-op here.
    async fn push_delta(
        &self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        payload: Vec<u8>,
        dot_counter: u64,
    );

    /// Whether this strategy uses periodic pull rounds.
    fn supports_pull(&self) -> bool { false }

    /// Build a pull-request message containing our knowledge vector.
    /// Returns `None` if pull is not supported.
    fn build_pull_request(
        &self,
        _node_id: &NodeId,
        _crdt_id: &str,
        _our_knowledge: &HashMap<NodeId, u64>,
    ) -> Option<Envelope> { None }
}

pub type SharedDissemination = Arc<dyn DisseminationStrategy>;

// ── Push (Broadcast) ────────────────────────────────────────────────────

/// Eagerly pushes every delta to all connected peers.
pub struct PushBroadcast {
    peer_manager: PeerManager,
}

impl PushBroadcast {
    pub fn new(peer_manager: PeerManager) -> Self {
        Self { peer_manager }
    }
}

#[async_trait]
impl DisseminationStrategy for PushBroadcast {
    async fn push_delta(
        &self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        payload: Vec<u8>,
        dot_counter: u64,
    ) {
        let envelope = Envelope {
            payload: Some(Payload::CrdtOp(proto::CrdtOp {
                crdt_id: crdt_id.to_string(),
                payload,
                hlc_ts: dot_counter,
                origin_node_id: origin_node_id.clone(),
            })),
        };

        info!(crdt_id, peers = self.peer_manager.len(), "push-broadcast: sending delta");
        self.peer_manager.broadcast(envelope).await;
    }
}

// ── Pull (Periodic) ────────��───────────────────────────────────────────

/// Never pushes. The engine periodically sends pull requests to peers,
/// who respond with deltas computed via `dvv.delta_since()`.
///
/// Pull requests are sent as `CrdtOp` with `hlc_ts = 0` as a sentinel.
/// The `payload` contains the serialised knowledge map.
pub struct PullPeriodic {
    #[allow(dead_code)]
    peer_manager: PeerManager,
}

impl PullPeriodic {
    pub fn new(peer_manager: PeerManager) -> Self {
        Self { peer_manager }
    }
}

#[async_trait]
impl DisseminationStrategy for PullPeriodic {
    async fn push_delta(
        &self,
        _origin_node_id: &NodeId,
        _crdt_id: &str,
        _payload: Vec<u8>,
        _dot_counter: u64,
    ) {
        // Pull-only: do not push on mutation.
        debug!("pull-periodic: skipping push (pull-only mode)");
    }

    fn supports_pull(&self) -> bool { true }

    fn build_pull_request(
        &self,
        node_id: &NodeId,
        crdt_id: &str,
        our_knowledge: &HashMap<NodeId, u64>,
    ) -> Option<Envelope> {
        let knowledge_bytes = serde_json::to_vec(our_knowledge)
            .expect("knowledge map serialisation");

        Some(Envelope {
            payload: Some(Payload::CrdtOp(proto::CrdtOp {
                crdt_id: crdt_id.to_string(),
                payload: knowledge_bytes,
                hlc_ts: 0, // sentinel: this is a pull request
                origin_node_id: node_id.clone(),
            })),
        })
    }
}

// ── Push-Pull ───────────────────────────────────────────────────────────

/// Pushes eagerly on mutation AND supports periodic pull for anti-entropy.
/// Most robust: push gives low latency, pull repairs lost messages.
pub struct PushPull {
    peer_manager: PeerManager,
}

impl PushPull {
    pub fn new(peer_manager: PeerManager) -> Self {
        Self { peer_manager }
    }
}

#[async_trait]
impl DisseminationStrategy for PushPull {
    async fn push_delta(
        &self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        payload: Vec<u8>,
        dot_counter: u64,
    ) {
        let envelope = Envelope {
            payload: Some(Payload::CrdtOp(proto::CrdtOp {
                crdt_id: crdt_id.to_string(),
                payload,
                hlc_ts: dot_counter,
                origin_node_id: origin_node_id.clone(),
            })),
        };

        debug!(crdt_id, peers = self.peer_manager.len(), "push-pull: pushing delta");
        self.peer_manager.broadcast(envelope).await;
    }

    fn supports_pull(&self) -> bool { true }

    fn build_pull_request(
        &self,
        node_id: &NodeId,
        crdt_id: &str,
        our_knowledge: &HashMap<NodeId, u64>,
    ) -> Option<Envelope> {
        let knowledge_bytes = serde_json::to_vec(our_knowledge)
            .expect("knowledge map serialisation");

        Some(Envelope {
            payload: Some(Payload::CrdtOp(proto::CrdtOp {
                crdt_id: crdt_id.to_string(),
                payload: knowledge_bytes,
                hlc_ts: 0,
                origin_node_id: node_id.clone(),
            })),
        })
    }
}
