//! Generic delta-CRDT trait.
//!
//! CRDT implementations receive a freshly-minted `Dot` on local ops.

pub mod or_set;

use std::collections::HashMap;

use crate::common::{Counter, NodeId};
use crate::logical_clocks::dot_version_vector::Dot;

/// Trait implemented by CRDT delta types that carry causal metadata.
///
/// The engine calls `causal_context` to extract DVV info after merging a
/// remote delta, and `set_causal_context` to annotate a local delta with
/// the sender's full DVV state before pushing it to peers.
pub trait DeltaContext {
    /// Extract the causal metadata: `(context_map, dot_node, dot_counter)`.
    fn causal_context(&self) -> (HashMap<NodeId, Counter>, NodeId, Counter);

    /// Annotate this delta with the sender's full causal context.
    fn set_causal_context(
        &mut self,
        context: HashMap<NodeId, Counter>,
        node: NodeId,
        counter: Counter,
    );
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
    /// Returns a delta describing the mutation.  The causal context fields
    /// in the returned delta are left as defaults; the engine fills them in
    /// via `set_causal_context` before sending.
    fn apply_local(&mut self, dot: Dot, op: Self::Op) -> Self::Delta;

    /// Merge a remote delta into local state. 
    fn merge_delta(&mut self, delta: &Self::Delta);

    /// Full-state snapshot for pull responses / initial sync.
    ///
    /// Causal context fields are left as defaults; the engine annotates
    /// them via `set_causal_context` before sending.
    fn full_state(&self) -> Self::Delta;

    fn print_state(&self) -> String;

    // TODO: NOT GENERIC: need to be refactored after testing is over
    fn get_random_element(&self) -> Option<String>;

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
