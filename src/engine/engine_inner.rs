use std::collections::HashMap;
use tracing::{debug, warn};

use crate::common::NodeId;
use crate::logical_clocks::dot_version_vector::{Dot, DotVersionVector};
use crate::engine::PullResponse;
use crate::gc::storage::S3GcStorage;
use crate::gc::coordinator::GcCoordinator;
use crate::crdt::{DeltaCrdt, DeltaContext};

// ── Inner state (behind the mutex) ─────────────────────────────────────
pub struct EngineInner<C: DeltaCrdt> {
    pub crdt: C,
    pub dvv: DotVersionVector,
    pub gc: GcCoordinator<S3GcStorage>,
}

impl<C: DeltaCrdt> EngineInner<C> {
    pub fn client_operation(&mut self, op: C::Op) {
        // 1. Mint a new dot.
        self.dvv.event();
        let dot = Dot::new(self.dvv.dot.node_id.clone(), self.dvv.dot.counter);

        // 2. Apply to CRDT.
        self.crdt.apply_local(dot, op);
    }

    pub fn server_pull_request(
        &mut self,
        from_node: &NodeId,
        remote_knowledge: HashMap<NodeId, u64>,
    ) -> PullResponse {
        debug!(%from_node, "received version vector");
        let mut knowledge = HashMap::new();
        knowledge.insert(from_node.clone(), remote_knowledge.clone());
        self.gc.update_matrix_clock(&knowledge);

        let our_knowledge = self.dvv.effective_map();
        let communication_between_gc_replicas = self.gc.config.gc_replica && self
            .gc
            .registry
            .get(from_node)
            .map(|(_, gc_replica, _)| gc_replica)
            .unwrap_or(false);

        // Compute delta if we are ahead of the remote in any dimension.
        let dvv_delta = self.dvv.delta_since(&remote_knowledge);
        let delta_payload = if communication_between_gc_replicas || dvv_delta.dot.counter != 0 || !dvv_delta.context.is_empty() {
            let state = self.crdt.delta_since(&remote_knowledge, &self.dvv);
            Some(C::encode_delta(&state))
        } else {
            None
        };

        // Compute our VV request if the remote is ahead of us in any dimension.
        let we_are_fully_ahead = remote_knowledge
            .iter()
            .all(|(node, &remote_ctr)| our_knowledge.get(node).copied().unwrap_or(0) >= remote_ctr);
        let our_knowledge_request = if !we_are_fully_ahead {
            Some(our_knowledge)
        } else {
            None
        };

        // Include the knowledge matrix if the requester is a GC replica, so they can make informed decisions about what to GC.
        let knowledge_matrix = if self
            .gc
            .registry
            .get(from_node)
            .map(|(_, gc_replica, _)| gc_replica)
            .unwrap_or(false)
        {
            self.gc.get_knowledge_matrix()
        } else {
            None
        };

        PullResponse { delta_payload, knowledge_matrix, our_knowledge_request }
    }

    pub fn handle_remote_delta(&mut self, payload: &[u8]) {
        let delta = match C::decode_delta(payload) {
            Ok(d) => d,
            Err(e) => {
                warn!(%e, "failed to decode remote delta");
                return;
            }
        };

        // Merge CRDT state.
        self.crdt.merge_delta(&delta);

        // Merge causal context into DVV.
        let (ctx, node, counter) = delta.causal_context();
        let remote_dvv = DotVersionVector {
            dot: Dot::new(node, counter),
            context: ctx,
        };
        self.dvv.merge(&remote_dvv);
    }
}
