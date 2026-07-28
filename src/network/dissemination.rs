#![allow(dead_code)]
//! Dissemination layer — Push, Pull, and Push-Pull strategies.
//!
//! All strategies produce `Envelope` messages using the existing
//! protobuf `CrdtOp` message. No schema changes required.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::common::NodeId;
use crate::crdt::DeltaCrdt;
use crate::engine::crdt_engine::CrdtEngineRequest;
use crate::peers::peer_registry::PeerRegistry;
use crate::proto::crdt_op::KnowledgeMatrix;
use crate::proto::VectorClock;
use crate::proto::{self, envelope::Payload, Envelope};
use rand::prelude::SliceRandom;

// ── Trait ───────────────────────────────────────────────────────────────

#[async_trait]
pub trait DisseminationStrategy<C: DeltaCrdt>: Send + Sync + 'static {
    /// Called after a local mutation to push the current version vector outward.
    /// Receivers respond with a delta if they are ahead or concurrent.
    /// Pull-only strategies no-op here.
    async fn push_version_vector(
        &mut self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        gc_replica: bool,
        knowledge: &HashMap<NodeId, u64>,
    );

    /// Called after merging a remote delta (which is itself a local state
    /// change). Push and hybrid strategies advertise the updated VV so other
    /// peers can react; pull-only strategies no-op (the periodic round covers
    /// anti-entropy).
    async fn on_post_merge(
        &mut self,
        _origin_node_id: &NodeId,
        _crdt_id: &str,
        _gc_replica: bool,
        _knowledge: &HashMap<NodeId, u64>,
    ) {
    }

    /// Send a delta response (and optionally a piggybacked VV request) to a
    /// single peer, routing the decision through the strategy:
    ///
    /// * `delta_payload` — `Some` when we have data the remote is missing.
    /// * `our_knowledge` — `Some` when we need the remote to send us data
    ///   (we are behind or concurrent). Push/hybrid strategies piggyback this
    ///   into `CrdtOp.requester_knowledge` when both are present; pull-only
    ///   strategies ignore it (the periodic round handles anti-entropy).
    fn respond_to_pull_request(
        &mut self,
        _from_node: &NodeId,
        _crdt_id: &str,
        _origin_node_id: &NodeId,
        _delta_payload: Option<Vec<u8>>,
        _gc_replica: bool,
        _knowledge_matrix: Option<HashMap<NodeId, HashMap<NodeId, u64>>>,
        _our_knowledge: Option<HashMap<NodeId, u64>>,
    ) {
    }

    /// Execute a pull round: broadcast our current knowledge vector to every
    /// connected peer so they can compute and send us any delta we are missing.
    /// Pull-only and hybrid strategies implement this; push-only strategies no-op.
    fn do_pull_round(
        &mut self,
        _node_id: &NodeId,
        _crdt_id: &str,
        _gc_replica: bool,
        _knowledge: &HashMap<NodeId, u64>,
    ) {
    }

    /// Spawn a background pull-round loop.
    ///
    /// `PushBroadcast` returns a no-op task; `PullPeriodic` and `PushPull`
    /// spawn a ticker that calls `engine.do_pull_round()` at each interval.
    fn start_pull_loop(&self, _engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<C>>) -> Option<JoinHandle<()>> {
        None
    }

    fn get_dissemination_round(&self) -> usize;
}

pub type SharedDissemination<C> = Box<dyn DisseminationStrategy<C>>;

// ── helpers ─────────────────────────────────────────────────────────────

/// Build a `CrdtOp` envelope, optionally with a piggybacked VV request.
fn make_crdt_op_envelope(
    crdt_id: &str,
    origin_node_id: &NodeId,
    payload: Vec<u8>,
    knowledge_matrix: Option<HashMap<NodeId, HashMap<NodeId, u64>>>,
    requester_knowledge: Option<HashMap<NodeId, u64>>,
) -> Envelope {
    Envelope {
        payload: Some(Payload::CrdtOp(proto::CrdtOp {
            crdt_id: crdt_id.to_string(),
            payload,
            origin_node_id: origin_node_id.clone(),
            requester_knowledge: requester_knowledge.map(|k| proto::VectorClock { entries: k }),
            knowledge_matrix: knowledge_matrix.map(|m| KnowledgeMatrix {
                entries: m
                    .into_iter()
                    .map(|(node_id, clock_map)| {
                        (node_id.to_string(), VectorClock { entries: clock_map })
                    })
                    .collect(),
            }),
        })),
    }
}

fn make_pull_request_envelope(
    crdt_id: &str,
    origin_node_id: &NodeId,
    gc_replica: bool,
    knowledge: &HashMap<NodeId, u64>,
) -> Envelope {
    Envelope {
        payload: Some(Payload::CrdtPullRequest(proto::CrdtPullRequest {
            crdt_id: crdt_id.to_string(),
            gc_replica,
            origin_node_id: origin_node_id.clone(),
            knowledge: Some(proto::VectorClock {
                entries: knowledge.clone(),
            }),
        })),
    }
}

// ── Push (Broadcast) ────────────────────────────────────────────────────

/// Eagerly pushes every delta to all connected peers.
/// Pull is not used; `start_pull_loop` returns a no-op task.
pub struct PushDissemination {
    dissemination_round: usize,
    peer_registry: PeerRegistry,
}

impl PushDissemination {
    pub fn new(peer_registry: PeerRegistry) -> Self {
        Self { dissemination_round: 0, peer_registry }
    }
}

#[async_trait]
impl <C: DeltaCrdt> DisseminationStrategy<C> for PushDissemination {
    async fn push_version_vector(
        &mut self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        gc_replica: bool,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        let envelope = make_pull_request_envelope(crdt_id, origin_node_id, gc_replica, knowledge);
        if let Some(random_peer_id) = self
            .peer_registry
            .peer_ids()
            .choose(&mut rand::thread_rng())
        {
            self.dissemination_round += 1;
            let _ = self.peer_registry.send_to_peer(random_peer_id, envelope);
        }
    }

    async fn on_post_merge(
        &mut self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        gc_replica: bool,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        <PushDissemination as DisseminationStrategy<C>>::push_version_vector::<'_, '_, '_, '_, '_>(self, origin_node_id, crdt_id, gc_replica, knowledge).await;
    }

    fn respond_to_pull_request(
        &mut self,
        from_node: &NodeId,
        crdt_id: &str,
        origin_node_id: &NodeId,
        delta_payload: Option<Vec<u8>>,
        gc_replica: bool,
        knowledge_matrix: Option<HashMap<NodeId, HashMap<NodeId, u64>>>,
        our_knowledge: Option<HashMap<NodeId, u64>>,
    ) {
        let envelope = match (delta_payload, our_knowledge) {
            (Some(payload), Some(knowledge)) => {
                // Piggyback the VV request on the delta to save a round-trip.
                debug!(%from_node, crdt_id, "push-broadcast: sending delta with piggybacked VV request");
                self.dissemination_round += 1;
                make_crdt_op_envelope(
                    crdt_id,
                    origin_node_id,
                    payload,
                    knowledge_matrix,
                    Some(knowledge),
                )
            }
            (Some(payload), Option::None) => {
                debug!(%from_node, crdt_id, "push-broadcast: sending delta");
                make_crdt_op_envelope(crdt_id, origin_node_id, payload, knowledge_matrix, None)
            }
            (Option::None, Some(knowledge)) => {
                debug!(%from_node, crdt_id, "push-broadcast: sending VV request");
                self.dissemination_round += 1;
                make_pull_request_envelope(crdt_id, origin_node_id, gc_replica, &knowledge)
            }
            (Option::None, Option::None) => return,
        };
        if let Err(e) = self.peer_registry.send_to_peer(from_node, envelope) {
            warn!(%from_node, %e, "push-broadcast: failed to send pull-request response");
        }
    }

    fn get_dissemination_round(&self) -> usize {
        self.dissemination_round
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
    dissemination_round: usize,
    peer_registry: PeerRegistry,
    interval: Duration,
}

impl PullPeriodic {
    pub fn new(peer_registry: PeerRegistry, interval: Duration) -> Self {
        Self {
            dissemination_round: 0,
            peer_registry,
            interval,
        }
    }
}

#[async_trait]
impl <C: DeltaCrdt> DisseminationStrategy<C> for PullPeriodic {
    async fn push_version_vector(
        &mut self,
        _origin_node_id: &NodeId,
        _crdt_id: &str,
        _gc_replica: bool,
        _knowledge: &HashMap<NodeId, u64>,
    ) {
        // Pull-only: do not push on mutation.
        debug!("pull-periodic: skipping push (pull-only mode)");
    }

    // on_post_merge: default no-op — the periodic pull round covers anti-entropy.

    fn respond_to_pull_request(
        &mut self,
        from_node: &NodeId,
        crdt_id: &str,
        origin_node_id: &NodeId,
        delta_payload: Option<Vec<u8>>,
        _gc_replica: bool,
        knowledge_matrix: Option<HashMap<NodeId, HashMap<NodeId, u64>>>,
        _our_knowledge: Option<HashMap<NodeId, u64>>,
    ) {
        // Pull-only: respond with a delta if we have one, but never send a VV
        // request back (the periodic pull round handles our anti-entropy).
        if let Some(payload) = delta_payload {
            debug!(%from_node, crdt_id, "pull-periodic: sending delta response");
            let envelope =
                make_crdt_op_envelope(crdt_id, origin_node_id, payload, knowledge_matrix, None);
            if let Err(e) = self.peer_registry.send_to_peer(from_node, envelope) {
                warn!(%from_node, %e, "pull-periodic: failed to send delta response");
            }
        }
    }

    fn do_pull_round(
        &mut self,
        node_id: &NodeId,
        crdt_id: &str,
        gc_replica: bool,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        let envelope = make_pull_request_envelope(crdt_id, node_id, gc_replica, knowledge);
        if let Some(random_peer_id) = self
            .peer_registry
            .peer_ids()
            .choose(&mut rand::thread_rng())
        {
            self.dissemination_round += 1;
            let _ = self.peer_registry.send_to_peer(random_peer_id, envelope);
        }
    }
    
    fn get_dissemination_round(&self) -> usize {
        self.dissemination_round
    }

    fn start_pull_loop(&self, engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<C>>) -> Option<JoinHandle<()>> {
        let interval = self.interval;
        Some(tokio::spawn(async move {
            info!(
                "pull-periodic: pull loop started (interval = {:?})",
                interval
            );
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(e) = engine_tx.send(CrdtEngineRequest::DoPullRound).await {
                    warn!(%e, "pull-periodic: failed to send DoPullRound request to engine");
                }
            }
        }))
    }
}

/// Pushes eagerly on mutation AND spawns a pull ticker via `start_pull_loop`
/// for anti-entropy. Most robust: push gives low latency, pull repairs lost
/// messages.
pub struct PushPull {
    dissemination_round: usize,
    peer_registry: PeerRegistry,
    interval: Duration,
}

impl PushPull {
    pub fn new(peer_registry: PeerRegistry, interval: Duration) -> Self {
        Self {
            dissemination_round: 0,
            peer_registry,
            interval,
        }
    }
}

#[async_trait]
impl <C: DeltaCrdt> DisseminationStrategy<C> for PushPull {
    async fn push_version_vector(
        &mut self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        gc_replica: bool,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        let envelope = make_pull_request_envelope(crdt_id, origin_node_id, gc_replica, knowledge);
        if let Some(random_peer_id) = self
            .peer_registry
            .peer_ids()
            .choose(&mut rand::thread_rng())
        {
            self.dissemination_round += 1;
            let _ = self.peer_registry.send_to_peer(random_peer_id, envelope);
        }
    }

    async fn on_post_merge(
        &mut self,
        origin_node_id: &NodeId,
        crdt_id: &str,
        gc_replica: bool,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        <PushPull as DisseminationStrategy<C>>::push_version_vector::<'_, '_, '_, '_, '_>(self, origin_node_id, crdt_id, gc_replica, knowledge).await;
    }

    fn respond_to_pull_request(
        &mut self,
        from_node: &NodeId,
        crdt_id: &str,
        origin_node_id: &NodeId,
        delta_payload: Option<Vec<u8>>,
        gc_replica: bool,
        knowledge_matrix: Option<HashMap<NodeId, HashMap<NodeId, u64>>>,
        our_knowledge: Option<HashMap<NodeId, u64>>,
    ) {
        let envelope = match (delta_payload, our_knowledge) {
            (Some(payload), Some(knowledge)) => {
                // Piggyback the VV request on the delta to save a round-trip.
                debug!(%from_node, crdt_id, "push-pull: sending delta with piggybacked VV request");
                self.dissemination_round += 1;
                make_crdt_op_envelope(
                    crdt_id,
                    origin_node_id,
                    payload,
                    knowledge_matrix,
                    Some(knowledge),
                )
            }
            (Some(payload), None) => {
                debug!(%from_node, crdt_id, "push-pull: sending delta");
                make_crdt_op_envelope(crdt_id, origin_node_id, payload, knowledge_matrix, None)
            }
            (None, Some(knowledge)) => {
                debug!(%from_node, crdt_id, "push-pull: sending VV request");
                self.dissemination_round += 1;
                make_pull_request_envelope(crdt_id, origin_node_id, gc_replica, &knowledge)
            }
            (None, None) => return,
        };
        if let Err(e) = self.peer_registry.send_to_peer(from_node, envelope) {
            warn!(%from_node, %e, "push-pull: failed to send pull-request response");
        }
    }

    fn do_pull_round(
        &mut self,
        node_id: &NodeId,
        crdt_id: &str,
        gc_replica: bool,
        knowledge: &HashMap<NodeId, u64>,
    ) {
        let envelope = make_pull_request_envelope(crdt_id, node_id, gc_replica, knowledge);
        if let Some(random_peer_id) = self
            .peer_registry
            .peer_ids()
            .choose(&mut rand::thread_rng())
        {
            self.dissemination_round += 1;
            let _ = self.peer_registry.send_to_peer(random_peer_id, envelope);
        }
    }

    fn get_dissemination_round(&self) -> usize {
        self.dissemination_round
    }

    fn start_pull_loop(&self, engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<C>>) -> Option<JoinHandle<()>> {
        let interval = self.interval;
        Some(tokio::spawn(async move {
            info!("push-pull: pull loop started (interval = {:?})", interval);
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(e) = engine_tx.send(CrdtEngineRequest::DoPullRound).await {
                    warn!(%e, "push-pull: failed to send DoPullRound request to engine");
                }
            }
        }))
    }
}
