//! Dissemination layer — Push, Pull, and Push-Pull strategies.
//!
//! All strategies produce `Envelope` messages using the existing
//! protobuf `CrdtOp` message. No schema changes required.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinHandle;
use tracing::{info, debug, warn};

use crate::common::NodeId;
use crate::peer_registry::PeerRegistry;
use crate::proto::{self, envelope::Payload, Envelope};

// ── Pull-round callback trait ───────────────────────────────────────────

/// Implemented by `CrdtEngine` so dissemination strategies can trigger a
/// pull round without knowing the concrete engine type.
#[async_trait]
pub trait PullRoundEngine: Send + Sync + 'static {
    async fn do_pull_round(&self);
}

// ── Trait ───────────────────────────────────────────────────────────────

#[async_trait]
pub trait DisseminationStrategy: Send + Sync + 'static {
    /// Called after a local mutation to push the current version vector outward.
    /// Receivers respond with a delta if they are ahead or concurrent.
    /// Pull-only strategies no-op here.
    async fn push_version_vector(
        &self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        knowledge: &HashMap<NodeId, u64>,
    );

    /// Called after merging a remote delta (which is itself a local state
    /// change). Push and hybrid strategies advertise the updated VV so other
    /// peers can react; pull-only strategies no-op (the periodic round covers
    /// anti-entropy).
    async fn on_post_merge(
        &self,
        _origin_node_id: &NodeId,
        _crdt_id: &str,
        _knowledge: &HashMap<NodeId, u64>,
    ) {}

    /// Send a delta response (and optionally a piggybacked VV request) to a
    /// single peer, routing the decision through the strategy:
    ///
    /// * `delta_payload` — `Some` when we have data the remote is missing.
    /// * `our_knowledge` — `Some` when we need the remote to send us data
    ///   (we are behind or concurrent). Push/hybrid strategies piggyback this
    ///   into `CrdtOp.requester_knowledge` when both are present; pull-only
    ///   strategies ignore it (the periodic round handles anti-entropy).
    fn respond_to_pull_request(
        &self,
        _from_node: &NodeId,
        _crdt_id: &str,
        _origin_node_id: &NodeId,
        _delta_payload: Option<Vec<u8>>,
        _our_knowledge: Option<HashMap<NodeId, u64>>,
    ) {}

    /// Execute a pull round: broadcast our current knowledge vector to every
    /// connected peer so they can compute and send us any delta we are missing.
    /// Pull-only and hybrid strategies implement this; push-only strategies no-op.
    fn do_pull_round(
        &self,
        _node_id: &NodeId,
        _crdt_id: &str,
        _knowledge: &HashMap<NodeId, u64>,
    ) {}

    /// Spawn a background pull-round loop.
    ///
    /// `PushBroadcast` returns a no-op task; `PullPeriodic` and `PushPull`
    /// spawn a ticker that calls `engine.do_pull_round()` at each interval.
    fn start_pull_loop(
        &self,
        _engine: Arc<dyn PullRoundEngine>,
    ) -> JoinHandle<()> {
        tokio::spawn(async {})
    }
}

pub type SharedDissemination = Arc<dyn DisseminationStrategy>;

// ── helpers ─────────────────────────────────────────────────────────────

/// Build a `CrdtOp` envelope, optionally with a piggybacked VV request.
fn make_crdt_op_envelope(
    crdt_id: &str,
    origin_node_id: &NodeId,
    payload: Vec<u8>,
    requester_knowledge: Option<HashMap<NodeId, u64>>,
) -> Envelope {
    Envelope {
        payload: Some(Payload::CrdtOp(proto::CrdtOp {
            crdt_id: crdt_id.to_string(),
            payload,
            origin_node_id: origin_node_id.clone(),
            requester_knowledge: requester_knowledge.map(|k| proto::VectorClock { entries: k }),
        })),
    }
}

fn make_pull_request_envelope(
    crdt_id: &str,
    origin_node_id: &NodeId,
    knowledge: &HashMap<NodeId, u64>,
) -> Envelope {
    Envelope {
        payload: Some(Payload::CrdtPullRequest(proto::CrdtPullRequest {
            crdt_id: crdt_id.to_string(),
            origin_node_id: origin_node_id.clone(),
            knowledge: Some(proto::VectorClock { entries: knowledge.clone() }),
        })),
    }
}

// ── Push (Broadcast) ────────────────────────────────────────────────────

/// Eagerly pushes every delta to all connected peers.
/// Pull is not used; `start_pull_loop` returns a no-op task.
pub struct PushBroadcast {
    peer_registry: PeerRegistry,
}

impl PushBroadcast {
    pub fn new(peer_registry: PeerRegistry) -> Self {
        Self { peer_registry }
    }
}

#[async_trait]
impl DisseminationStrategy for PushBroadcast {
    async fn push_version_vector(
        &self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        let envelope = make_pull_request_envelope(crdt_id, origin_node_id, knowledge);
        info!(crdt_id, peers = self.peer_registry.len(), "push-broadcast: advertising version vector");
        self.peer_registry.broadcast(envelope);
    }

    async fn on_post_merge(
        &self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        self.push_version_vector(origin_node_id, crdt_id, knowledge).await;
    }

    fn respond_to_pull_request(
        &self,
        from_node: &NodeId,
        crdt_id: &str,
        origin_node_id: &NodeId,
        delta_payload: Option<Vec<u8>>,
        our_knowledge: Option<HashMap<NodeId, u64>>,
    ) {
        let envelope = match (delta_payload, our_knowledge) {
            (Some(payload), Some(knowledge)) => {
                // Piggyback the VV request on the delta to save a round-trip.
                debug!(%from_node, crdt_id, "push-broadcast: sending delta with piggybacked VV request");
                make_crdt_op_envelope(crdt_id, origin_node_id, payload, Some(knowledge))
            }
            (Some(payload), None) => {
                debug!(%from_node, crdt_id, "push-broadcast: sending delta");
                make_crdt_op_envelope(crdt_id, origin_node_id, payload, None)
            }
            (None, Some(knowledge)) => {
                debug!(%from_node, crdt_id, "push-broadcast: sending VV request");
                make_pull_request_envelope(crdt_id, origin_node_id, &knowledge)
            }
            (None, None) => return,
        };
        if let Err(e) = self.peer_registry.send_to_peer(from_node, envelope) {
            warn!(%from_node, %e, "push-broadcast: failed to send pull-request response");
        }
    }

    // start_pull_loop defaults to no-op.
}

// ── Pull (Periodic) ─────────────────────────────────────────────────────

/// Never pushes. Spawns a background ticker via `start_pull_loop` that
/// periodically sends pull requests to peers.
///
/// Pull requests are sent as `CrdtPullRequest` messages with the serialised
/// knowledge map as the payload.
pub struct PullPeriodic {
    peer_registry: PeerRegistry,
    interval: Duration,
}

impl PullPeriodic {
    pub fn new(peer_registry: PeerRegistry, interval: Duration) -> Self {
        Self { peer_registry, interval }
    }
}

#[async_trait]
impl DisseminationStrategy for PullPeriodic {
    async fn push_version_vector(
        &self,
        _origin_node_id: &NodeId,
        _crdt_id: &str,
        _knowledge: &HashMap<NodeId, u64>,
    ) {
        // Pull-only: do not push on mutation.
        debug!("pull-periodic: skipping push (pull-only mode)");
    }

    // on_post_merge: default no-op — the periodic pull round covers anti-entropy.

    fn respond_to_pull_request(
        &self,
        from_node: &NodeId,
        crdt_id: &str,
        origin_node_id: &NodeId,
        delta_payload: Option<Vec<u8>>,
        _our_knowledge: Option<HashMap<NodeId, u64>>,
    ) {
        // Pull-only: respond with a delta if we have one, but never send a VV
        // request back (the periodic pull round handles our anti-entropy).
        if let Some(payload) = delta_payload {
            debug!(%from_node, crdt_id, "pull-periodic: sending delta response");
            let envelope = make_crdt_op_envelope(crdt_id, origin_node_id, payload, None);
            if let Err(e) = self.peer_registry.send_to_peer(from_node, envelope) {
                warn!(%from_node, %e, "pull-periodic: failed to send delta response");
            }
        }
    }

    fn do_pull_round(
        &self,
        node_id: &NodeId,
        crdt_id: &str,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        let envelope = make_pull_request_envelope(crdt_id, node_id, knowledge);
        debug!(crdt_id, peers = self.peer_registry.len(), "pull-periodic: broadcasting pull request");
        self.peer_registry.broadcast(envelope);
    }

    fn start_pull_loop(&self, engine: Arc<dyn PullRoundEngine>) -> JoinHandle<()> {
        let interval = self.interval;
        tokio::spawn(async move {
            info!("pull-periodic: pull loop started (interval = {:?})", interval);
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                engine.do_pull_round().await;
            }
        })
    }
}

/// Pushes eagerly on mutation AND spawns a pull ticker via `start_pull_loop`
/// for anti-entropy. Most robust: push gives low latency, pull repairs lost
/// messages.
pub struct PushPull {
    peer_registry: PeerRegistry,
    interval: Duration,
}

impl PushPull {
    pub fn new(peer_registry: PeerRegistry, interval: Duration) -> Self {
        Self { peer_registry, interval }
    }
}

#[async_trait]
impl DisseminationStrategy for PushPull {
    async fn push_version_vector(
        &self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        let envelope = make_pull_request_envelope(crdt_id, origin_node_id, knowledge);
        debug!(crdt_id, peers = self.peer_registry.len(), "push-pull: advertising version vector");
        self.peer_registry.broadcast(envelope);
    }

    async fn on_post_merge(
        &self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        self.push_version_vector(origin_node_id, crdt_id, knowledge).await;
    }

    fn respond_to_pull_request(
        &self,
        from_node: &NodeId,
        crdt_id: &str,
        origin_node_id: &NodeId,
        delta_payload: Option<Vec<u8>>,
        our_knowledge: Option<HashMap<NodeId, u64>>,
    ) {
        let envelope = match (delta_payload, our_knowledge) {
            (Some(payload), Some(knowledge)) => {
                // Piggyback the VV request on the delta to save a round-trip.
                debug!(%from_node, crdt_id, "push-pull: sending delta with piggybacked VV request");
                make_crdt_op_envelope(crdt_id, origin_node_id, payload, Some(knowledge))
            }
            (Some(payload), None) => {
                debug!(%from_node, crdt_id, "push-pull: sending delta");
                make_crdt_op_envelope(crdt_id, origin_node_id, payload, None)
            }
            (None, Some(knowledge)) => {
                debug!(%from_node, crdt_id, "push-pull: sending VV request");
                make_pull_request_envelope(crdt_id, origin_node_id, &knowledge)
            }
            (None, None) => return,
        };
        if let Err(e) = self.peer_registry.send_to_peer(from_node, envelope) {
            warn!(%from_node, %e, "push-pull: failed to send pull-request response");
        }
    }

    fn do_pull_round(
        &self,
        node_id: &NodeId,
        crdt_id: &str,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        let envelope = make_pull_request_envelope(crdt_id, node_id, knowledge);
        debug!(crdt_id, peers = self.peer_registry.len(), "push-pull: broadcasting pull request");
        self.peer_registry.broadcast(envelope);
    }

    fn start_pull_loop(&self, engine: Arc<dyn PullRoundEngine>) -> JoinHandle<()> {
        let interval = self.interval;
        tokio::spawn(async move {
            info!("push-pull: pull loop started (interval = {:?})", interval);
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                engine.do_pull_round().await;
            }
        })
    }
}
