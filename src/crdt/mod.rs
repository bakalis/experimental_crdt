//! Generic delta-CRDT trait.
//!
//! CRDT implementations receive a freshly-minted `Dot` and the current
//! `DotVersionVector` on local ops and state queries so they can populate
//! causal metadata directly — no post-hoc patching by the engine.

pub mod or_set;

use std::collections::HashMap;

use crate::common::{Counter, NodeId};
use crate::logical_clocks::dot_version_vector::{Dot, DotVersionVector};

/// Trait implemented by CRDT delta types that carry causal metadata.
///
/// The engine calls `causal_context` to extract DVV info after merging a
/// remote delta so it can update its local `DotVersionVector`.
pub trait DeltaContext {
    /// Extract the causal metadata: `(context_map, dot_node, dot_counter)`.
    fn causal_context(&self) -> (HashMap<NodeId, Counter>, NodeId, Counter);
}

/// A delta-CRDT whose causality tracking is fully owned by the engine.
///
/// All DVV operations (`event`, `merge`, `dominates_dot`, `delta_since`)
/// are performed exclusively by the engine. The CRDT only manages its own
/// data structure using `Dot` values supplied by the engine.
pub trait DeltaCrdt: Send + Sync + 'static {

    /// The application-level operation type.
    type Op: Send + Sync + 'static;

    /// The delta/state type — carries both CRDT data and causal metadata.
    type Delta: Send + Sync + Clone + DeltaContext + 'static;

    /// Apply a local op.
    ///
    /// The engine passes the post-event `dvv` so the CRDT can populate
    /// causal metadata (context, dot_node, dot_counter) directly.
    fn apply_local(&mut self, dot: Dot, op: Self::Op, dvv: &DotVersionVector) -> Self::Delta;

    /// Merge a remote delta into local state.
    fn merge_delta(&mut self, delta: &Self::Delta);

    /// Full-state snapshot for pull responses / initial sync.
    ///
    /// The engine passes the current `dvv` so causal metadata is filled in.
    fn full_state(&self, dvv: &DotVersionVector) -> Self::Delta;

    /// Minimal delta for a peer whose causal knowledge is `remote_knowledge`.
    ///
    /// Only adds and tombstones that the remote has not yet seen are
    /// included.  The engine passes the current `dvv` so causal metadata
    /// is filled in.
    fn delta_since(
        &self,
        remote_knowledge: &HashMap<NodeId, Counter>,
        dvv: &DotVersionVector,
    ) -> Self::Delta;

    fn print_state(&self) -> String;

    // TODO: NOT GENERIC: need to be refactored after testing is over
    fn get_random_element(&self) -> Option<String>;

    /// Garbage collect tombstones that are safe to remove.
    ///
    /// Takes the safe collection epoch (if any) and removes tombstones
    /// that were created before or at that epoch.
    fn garbage_collect(&mut self, safe_epoch: Option<u64>) -> usize;

    // ── Serialisation ───────────────────────────────────────────────────

    fn encode_delta(delta: &Self::Delta) -> Vec<u8>;
    fn decode_delta(
        bytes: &[u8],
    ) -> Result<Self::Delta, Box<dyn std::error::Error + Send + Sync>>;
    fn encode_op(op: &Self::Op) -> Vec<u8>;
    fn decode_op(
        bytes: &[u8],
    ) -> Result<Self::Op, Box<dyn std::error::Error + Send + Sync>>;
}
