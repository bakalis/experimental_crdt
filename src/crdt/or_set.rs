//! Observed-Remove Set (OR-Set).
//!
//! Each element is tagged with a `Dot` supplied by the engine.
//! This implementation never touches a `DotVersionVector`.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use tracing::info;

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

/// Delta produced by one operation — also used as the full-state snapshot.
///
/// The `context/dot_node/dot_counter` fields are left as defaults by the
/// CRDT; the engine fills them in via `DeltaContext::set_causal_context`
/// before sending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSetDelta<E> {
    /// New (element, dot) pairs being added.
    pub adds: Vec<(E, Dot)>,
    /// New tombstones: (removed_add_dot, remove_event_dot).
    pub tombstones: Vec<(Dot, Dot)>,
    /// Sender's causal context for DVV merge — filled in by the engine.
    pub context: HashMap<NodeId, Counter>,
    pub dot_node: NodeId,
    pub dot_counter: Counter,
}

impl<E> DeltaContext for OrSetDelta<E>
where
    E: Clone + Send + Sync + 'static + Serialize + for<'de> Deserialize<'de>,
{
    fn causal_context(&self) -> (HashMap<NodeId, Counter>, NodeId, Counter) {
        (
            self.context.clone(),
            self.dot_node.clone(),
            self.dot_counter,
        )
    }

    fn set_causal_context(
        &mut self,
        context: HashMap<NodeId, Counter>,
        node: NodeId,
        counter: Counter,
    ) {
        self.context = context;
        self.dot_node = node;
        self.dot_counter = counter;
    }
}

// ── OrSet ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OrSet<E: Clone + Eq + Hash + Debug> {
    /// Active elements: element → set of dots that justify its presence.
    adds: HashMap<E, HashSet<Dot>>,
    /// Tombstones: original add-dot → remove-event dot.
    tombstones: HashMap<Dot, Dot>,
}

impl<E: Clone + Eq + Hash + Debug> Default for OrSet<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Clone + Eq + Hash + Debug> OrSet<E> {
    pub fn new() -> Self {
        Self {
            adds: HashMap::new(),
            tombstones: HashMap::new(),
        }
    }

    pub fn contains(&self, elem: &E) -> bool {
        self.adds
            .get(elem)
            .map(|dots| dots.iter().any(|d| !self.tombstones.contains_key(d)))
            .unwrap_or(false)
    }

    pub fn elements(&self) -> impl Iterator<Item = &E> {
        self.adds
            .iter()
            .filter(|(_, dots)| dots.iter().any(|d| !self.tombstones.contains_key(d)))
            .map(|(elem, _)| elem)
    }

    pub fn len(&self) -> usize {
        self.adds
            .values()
            .filter(|dots| dots.iter().any(|d| !self.tombstones.contains_key(d)))
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
                self.adds
                    .entry(elem.clone())
                    .or_default()
                    .insert(dot.clone());

                OrSetDelta {
                    adds: vec![(elem, dot)],
                    tombstones: vec![],
                    // Engine fills these in before sending.
                    context: HashMap::new(),
                    dot_node: String::new(),
                    dot_counter: 0,
                }
            }

            OrSetOp::Remove(elem) => {
                let remove_dot = dot;
                let add_dots: Vec<Dot> = self
                    .adds
                    .remove(&elem)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();

                let new_tombstones: Vec<(Dot, Dot)> = add_dots
                    .into_iter()
                    .map(|add_dot| {
                        self.tombstones.insert(add_dot.clone(), remove_dot.clone());
                        (add_dot, remove_dot.clone())
                    })
                    .collect();

                OrSetDelta {
                    adds: vec![],
                    tombstones: new_tombstones,
                    // Engine fills these in before sending.
                    context: HashMap::new(),
                    dot_node: String::new(),
                    dot_counter: 0,
                }
            }
        }
    }

    fn get_random_element(&self) -> Option<String> {
        self.elements().next().cloned().map(|e| format!("{:?}", e))
    }

    fn print_state(&self) -> String {
        let mut elems = self.elements().cloned().collect::<Vec<_>>();
        elems.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
        let elem_str = elems
            .iter()
            .map(|e| format!("{:?}", e))
            .collect::<Vec<_>>()
            .join(", ");
        info!("current state: {}", elem_str);
        elem_str
    }

    fn merge_delta(&mut self, delta: &OrSetDelta<E>) {
        // 1. Apply tombstones: record them and evict the add-dots from adds.
        for (add_dot, remove_dot) in &delta.tombstones {
            self.tombstones.insert(add_dot.clone(), remove_dot.clone());
            for dots in self.adds.values_mut() {
                dots.remove(add_dot);
            }
        }
        self.adds.retain(|_, dots| !dots.is_empty());

        // 2. Apply adds (skip any dot that is already tombstoned).
        for (elem, dot) in &delta.adds {
            if !self.tombstones.contains_key(dot) {
                self.adds
                    .entry(elem.clone())
                    .or_default()
                    .insert(dot.clone());
            }
        }
    }

    fn full_state(&self) -> OrSetDelta<E> {
        let adds: Vec<(E, Dot)> = self
            .adds
            .iter()
            .flat_map(|(elem, dots)| {
                dots.iter().map(move |dot| (elem.clone(), dot.clone()))
            })
            .collect();

        let tombstones: Vec<(Dot, Dot)> = self
            .tombstones
            .iter()
            .map(|(add_dot, remove_dot)| (add_dot.clone(), remove_dot.clone()))
            .collect();

        OrSetDelta {
            adds,
            tombstones,
            // Engine fills these in before sending.
            context: HashMap::new(),
            dot_node: String::new(),
            dot_counter: 0,
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

        // Remove mints a new dot (tombstone event) — DVV advances.
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

        // A's re-add dot is not tombstoned by B's remove (which only tombstones A:1).
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
        // Remove "x" so full state includes a tombstone.
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Remove("x".to_string()));

        let mut state = a_set.full_state();
        state.set_causal_context(
            a_dvv.effective_map(),
            a_dvv.dot.node_id.clone(),
            a_dvv.dot.counter,
        );

        // Full state must carry tombstones.
        assert!(!state.tombstones.is_empty());

        let state_bytes = OrSet::<String>::encode_delta(&state);
        let decoded = OrSet::<String>::decode_delta(&state_bytes).unwrap();

        let mut b_set = OrSet::<String>::new();
        let mut b_dvv = DotVersionVector::new(nid("B"));
        engine_merge_delta(&mut b_set, &mut b_dvv, &decoded);

        assert!(!b_set.contains(&"x".to_string()));
        assert!(b_set.contains(&"y".to_string()));
        assert_eq!(b_dvv.effective_counter(&nid("A")), 3);
    }

    #[test]
    fn incremental_delta_only_contains_new_ops() {
        let mut set = OrSet::<String>::new();
        let mut dvv = DotVersionVector::new(nid("A"));

        // Add "x" — delta carries only this add, no tombstones.
        let add_delta = engine_apply_local(&mut set, &mut dvv, OrSetOp::Add("x".to_string()));
        assert_eq!(add_delta.adds.len(), 1);
        assert_eq!(add_delta.tombstones.len(), 0);

        // Add "y" — delta carries only this add, no tombstones.
        let add_y_delta = engine_apply_local(&mut set, &mut dvv, OrSetOp::Add("y".to_string()));
        assert_eq!(add_y_delta.adds.len(), 1);
        assert_eq!(add_y_delta.tombstones.len(), 0);

        // Remove "x" — delta carries only the tombstone, no adds.
        let remove_delta =
            engine_apply_local(&mut set, &mut dvv, OrSetOp::Remove("x".to_string()));
        assert_eq!(remove_delta.adds.len(), 0);
        assert_eq!(remove_delta.tombstones.len(), 1);
    }
}
