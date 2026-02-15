//! Generic delta-CRDT trait and module re-exports.
//!
//! The engine owns a `DotVersionVector` per CRDT instance.
//! Concrete CRDT implementations receive a `&mut DotVersionVector`
//! on mutating operations so they can call `dvv.event()` to mint
//! new dots and `dvv.dominates_dot()` to garbage-collect.

pub mod or_set;

use std::fmt::Debug;

use crate::common::NodeId;
use crate::logical_clocks::dot_version_vector::DotVersionVector;

/// A delta-CRDT that delegates causality tracking to a `DotVersionVector`.
///
/// All wire payloads are `Vec<u8>` — the CRDT owns its serialisation
/// format and the engine treats them as opaque bytes stuffed into
/// the protobuf `CrdtOp.payload` field.
pub trait DeltaCrdt: Send + Sync + Debug + 'static {
    /// Apply a **local** operation.
    ///
    /// 1. Call `dvv.event()` to mint a new dot for this mutation.
    /// 2. Mutate internal state.
    /// 3. Return the serialised delta bytes (for `CrdtOp.payload`).
    fn apply_local(
        &mut self,
        dvv: &mut DotVersionVector,
        node_id: &NodeId,
        op_bytes: &[u8],
    ) -> Vec<u8>;

    /// Apply a **remote** delta received from another node.
    ///
    /// 1. Deserialise the delta from `payload`.
    /// 2. Merge into internal state (must be idempotent).
    /// 3. Merge the remote's causal context into `dvv`.
    fn apply_remote(
        &mut self,
        dvv: &mut DotVersionVector,
        payload: &[u8],
    );

    /// Produce a full-state snapshot as bytes (for new joiners / pull responses).
    fn encode_state(&self, dvv: &DotVersionVector) -> Vec<u8>;

    /// Merge a full remote state snapshot into this replica.
    fn merge_state(
        &mut self,
        dvv: &mut DotVersionVector,
        payload: &[u8],
    );
}
