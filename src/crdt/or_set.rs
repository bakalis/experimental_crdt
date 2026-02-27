//! Observed-Remove Set (OR-Set).
//!
//! Each element is tagged with a `Dot` supplied by the engine.
//! This implementation never touches a `DotVersionVector`.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::common::{Counter, NodeId};
use crate::crdt::{DeltaCrdt, DeltaContext};
use crate::logical_clocks::dot_version_vector::Dot;

// ── Op ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrSetOp<E> {
    Add(E),
    Remove(E),
}

// ── Delta ───────────────────────────────────────────────────────────────

/// Delta produced by one operation — also used as the full-state snapshot
/// type (adds = all entries, removes = empty).
///
/// The `dvv_*` fields are left as defaults by the CRDT; the engine fills
/// them in via `DeltaContext::set_causal_context` before sending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSetDelta<E> {
    pub adds: Vec<(E, Dot)>,
    pub removes: Vec<Dot>,
    /// Sender's DVV effective map — filled in by the engine, not the CRDT.
    pub dvv_context: HashMap<NodeId, Counter>,
    pub dvv_dot_node: NodeId,
    pub dvv_dot_counter: Counter,
}

impl<E> DeltaContext for OrSetDelta<E>
where
    E: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de>,
{
    fn causal_context(&self) -> (HashMap<NodeId, Counter>, NodeId, Counter) {
        (
            self.dvv_context.clone(),
            self.dvv_dot_node.clone(),
            self.dvv_dot_counter,
        )
    }

    fn set_causal_context(
        &mut self,
        context: HashMap<NodeId, Counter>,
        node: NodeId,
        counter: Counter,
    ) {
        self.dvv_context = context;
        self.dvv_dot_node = node;
        self.dvv_dot_counter = counter;
    }
}

// ── OrSet ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OrSet<E: Clone + Eq + Hash + Debug> {
    entries: HashMap<E, HashSet<Dot>>,
}

impl<E: Clone + Eq + Hash + Debug> Default for OrSet<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Clone + Eq + Hash + Debug> OrSet<E> {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn contains(&self, elem: &E) -> bool {
        self.entries
            .get(elem)
            .map(|dots| !dots.is_empty())
            .unwrap_or(false)
    }

    pub fn elements(&self) -> impl Iterator<Item = &E> {
        self.entries
            .iter()
            .filter(|(_, dots)| !dots.is_empty())
            .map(|(elem, _)| elem)
    }

    pub fn len(&self) -> usize {
        self.entries
            .values()
            .filter(|dots| !dots.is_empty())
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<E> DeltaCrdt for OrSet<E>
where
    E: Clone + Eq + Hash + Debug + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    type Op = OrSetOp<E>;
    type Delta = OrSetDelta<E>;

    fn apply_local(&mut self, dot: Dot, op: OrSetOp<E>) -> OrSetDelta<E> {
        match op {
            OrSetOp::Add(elem) => {
                // Remove all existing dots for this element (add-wins / re-add).
                let old_dots: Vec<Dot> = self
                    .entries
                    .remove(&elem)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();

                let mut new_dots = HashSet::new();
                new_dots.insert(dot.clone());
                self.entries.insert(elem.clone(), new_dots);

                tracing::info!("elements: {:?}", self.elements().collect::<Vec<_>>());

                OrSetDelta {
                    adds: vec![(elem, dot)],
                    removes: old_dots,
                    // Engine fills these in before sending.
                    dvv_context: HashMap::new(),
                    dvv_dot_node: String::new(),
                    dvv_dot_counter: 0,
                }
            }

            OrSetOp::Remove(elem) => {
                let removed_dots: Vec<Dot> = self
                    .entries
                    .remove(&elem)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();

                OrSetDelta {
                    adds: vec![],
                    removes: removed_dots,
                    // Engine fills these in before sending.
                    dvv_context: HashMap::new(),
                    dvv_dot_node: String::new(),
                    dvv_dot_counter: 0,
                }
            }
        }
    }

    fn merge_delta(&mut self, delta: &OrSetDelta<E>) {
        // 1. Remove tombstoned dots.
        for dot in &delta.removes {
            for dots in self.entries.values_mut() {
                dots.remove(dot);
            }
        }
        self.entries.retain(|_, dots| !dots.is_empty());

        // 2. Apply adds (idempotent via HashSet).
        for (elem, dot) in &delta.adds {
            self.entries
                .entry(elem.clone())
                .or_default()
                .insert(dot.clone());
        }
    }

    fn full_state(&self) -> OrSetDelta<E> {
        let adds: Vec<(E, Dot)> = self
            .entries
            .iter()
            .flat_map(|(elem, dots)| {
                dots.iter().map(move |dot| (elem.clone(), dot.clone()))
            })
            .collect();

        OrSetDelta {
            adds,
            removes: vec![],
            // Engine fills these in before sending.
            dvv_context: HashMap::new(),
            dvv_dot_node: String::new(),
            dvv_dot_counter: 0,
        }
    }

    fn encode_delta(delta: &OrSetDelta<E>) -> Vec<u8> {
        serde_json::to_vec(delta).expect("delta serialisation")
    }

    fn decode_delta(
        bytes: &[u8],
    ) -> Result<OrSetDelta<E>, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::from_slice(bytes).map_err(|e| Box::new(e) as _)
    }

    fn encode_op(op: &OrSetOp<E>) -> Vec<u8> {
        serde_json::to_vec(op).expect("op serialisation")
    }

    fn decode_op(
        bytes: &[u8],
    ) -> Result<OrSetOp<E>, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::from_slice(bytes).map_err(|e| Box::new(e) as _)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_clocks::dot_version_vector::DotVersionVector;

    fn nid(s: &str) -> NodeId {
        s.to_string()
    }

    /// Simulate what the engine does for a local operation:
    /// mint a dot, apply to CRDT, then annotate with DVV context.
    fn engine_apply_local(
        set: &mut OrSet<String>,
        dvv: &mut DotVersionVector,
        op: OrSetOp<String>,
    ) -> OrSetDelta<String> {
        dvv.event();
        let dot = Dot::new(dvv.dot.node_id.clone(), dvv.dot.counter);
        let mut delta = set.apply_local(dot, op);
        delta.set_causal_context(
            dvv.effective_map(),
            dvv.dot.node_id.clone(),
            dvv.dot.counter,
        );
        delta
    }

    /// Simulate what the engine does when merging a remote delta:
    /// merge into CRDT then merge the DVV context.
    fn engine_merge_delta(
        set: &mut OrSet<String>,
        dvv: &mut DotVersionVector,
        delta: &OrSetDelta<String>,
    ) {
        set.merge_delta(delta);
        let (ctx, node, counter) = delta.causal_context();
        let remote_dvv = DotVersionVector {
            dot: Dot::new(node, counter),
            context: ctx,
        };
        dvv.merge(&remote_dvv);
    }

    #[test]
    fn add_uses_dot() {
        let mut set = OrSet::<String>::new();
        let mut dvv = DotVersionVector::new(nid("A"));

        let _ = engine_apply_local(&mut set, &mut dvv, OrSetOp::Add("x".to_string()));

        assert!(set.contains(&"x".to_string()));
        assert_eq!(dvv.dot.counter, 1);
    }

    #[test]
    fn remove_clears_element() {
        let mut set = OrSet::<String>::new();
        let mut dvv = DotVersionVector::new(nid("A"));

        let _ = engine_apply_local(&mut set, &mut dvv, OrSetOp::Add("x".to_string()));
        assert_eq!(dvv.dot.counter, 1);

        // Engine mints a dot for every op, including Remove.
        let _ = engine_apply_local(&mut set, &mut dvv, OrSetOp::Remove("x".to_string()));
        assert_eq!(dvv.dot.counter, 2);
        assert!(!set.contains(&"x".to_string()));
    }

    #[test]
    fn concurrent_add_wins() {
        let mut a_set = OrSet::<String>::new();
        let mut a_dvv = DotVersionVector::new(nid("A"));
        let mut b_set = OrSet::<String>::new();
        let mut b_dvv = DotVersionVector::new(nid("B"));

        let delta_a =
            engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("x".to_string()));
        engine_merge_delta(&mut b_set, &mut b_dvv, &delta_a);
        assert!(b_set.contains(&"x".to_string()));

        let remove_delta =
            engine_apply_local(&mut b_set, &mut b_dvv, OrSetOp::Remove("x".to_string()));
        let readd_delta =
            engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("x".to_string()));

        engine_merge_delta(&mut a_set, &mut a_dvv, &remove_delta);
        assert!(a_set.contains(&"x".to_string()));

        engine_merge_delta(&mut b_set, &mut b_dvv, &readd_delta);
        assert!(b_set.contains(&"x".to_string()));
    }

    #[test]
    fn delta_roundtrip_through_bytes() {
        let mut set = OrSet::<String>::new();
        let mut dvv = DotVersionVector::new(nid("A"));

        let delta = engine_apply_local(&mut set, &mut dvv, OrSetOp::Add("hello".to_string()));
        let delta_bytes = OrSet::<String>::encode_delta(&delta);

        let decoded = OrSet::<String>::decode_delta(&delta_bytes).unwrap();

        let mut remote_set = OrSet::<String>::new();
        let mut remote_dvv = DotVersionVector::new(nid("B"));
        engine_merge_delta(&mut remote_set, &mut remote_dvv, &decoded);

        assert!(remote_set.contains(&"hello".to_string()));
        assert_eq!(remote_dvv.effective_counter(&nid("A")), 1);
    }

    #[test]
    fn full_state_sync() {
        let mut a_set = OrSet::<String>::new();
        let mut a_dvv = DotVersionVector::new(nid("A"));

        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("x".to_string()));
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("y".to_string()));

        let mut state = a_set.full_state();
        state.set_causal_context(
            a_dvv.effective_map(),
            a_dvv.dot.node_id.clone(),
            a_dvv.dot.counter,
        );
        let state_bytes = OrSet::<String>::encode_delta(&state);
        let decoded = OrSet::<String>::decode_delta(&state_bytes).unwrap();

        let mut b_set = OrSet::<String>::new();
        let mut b_dvv = DotVersionVector::new(nid("B"));
        engine_merge_delta(&mut b_set, &mut b_dvv, &decoded);

        assert!(b_set.contains(&"x".to_string()));
        assert!(b_set.contains(&"y".to_string()));
        assert_eq!(b_dvv.effective_counter(&nid("A")), 2);
    }
}
