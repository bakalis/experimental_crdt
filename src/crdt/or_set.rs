//! Observed-Remove Set (OR-Set).
//!
//! Each element is tagged with a `Dot` supplied by the engine.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::common::{Counter, NodeId};
use crate::crdt::{DeltaCrdt, DeltaContext};
use crate::logical_clocks::dot_version_vector::{Dot, DotVersionVector};

// ── Op ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrSetOp<E> {
    Add(E),
    Remove(E),
}

// ── Delta ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSetDelta<E> {
    /// Missing (element, dot) pairs being added.
    pub adds: Vec<(E, Dot)>,
    /// Missing tombstones: (removed_add_dot, remove_event_dot).
    pub tombstones: Vec<(Dot, Dot)>,
    /// Sender's causal context for DVV merge.
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

    fn apply_local(&mut self, dot: Dot, op: OrSetOp<E>, dvv: &DotVersionVector) -> OrSetDelta<E> {
        match op {
            OrSetOp::Add(elem) => {
                self.adds
                    .entry(elem.clone())
                    .or_default()
                    .insert(dot.clone());

                OrSetDelta {
                    adds: vec![(elem, dot)],
                    tombstones: vec![],
                    context: dvv.effective_map(),
                    dot_node: dvv.dot.node_id.clone(),
                    dot_counter: dvv.dot.counter,
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
                    context: dvv.effective_map(),
                    dot_node: dvv.dot.node_id.clone(),
                    dot_counter: dvv.dot.counter,
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

    fn full_state(&self, dvv: &DotVersionVector) -> OrSetDelta<E> {
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
            context: dvv.effective_map(),
            dot_node: dvv.dot.node_id.clone(),
            dot_counter: dvv.dot.counter,
        }
    }

    fn delta_since(
        &self,
        remote_knowledge: &HashMap<NodeId, Counter>,
        dvv: &DotVersionVector,
    ) -> OrSetDelta<E> {
        // Include only add-dots the remote hasn't seen yet.
        let adds: Vec<(E, Dot)> = self
            .adds
            .iter()
            .flat_map(|(elem, dots)| {
                dots.iter()
                    .filter(|dot| {
                        dot.counter > remote_knowledge.get(&dot.node_id).copied().unwrap_or(0)
                    })
                    .map(move |dot| (elem.clone(), dot.clone()))
            })
            .collect();

        // Include only tombstones whose remove-event is new to the remote.
        let tombstones: Vec<(Dot, Dot)> = self
            .tombstones
            .iter()
            .filter(|(_, remove_dot)| {
                remove_dot.counter
                    > remote_knowledge
                        .get(&remove_dot.node_id)
                        .copied()
                        .unwrap_or(0)
            })
            .map(|(add_dot, remove_dot)| (add_dot.clone(), remove_dot.clone()))
            .collect();

        OrSetDelta {
            adds,
            tombstones,
            context: dvv.effective_map(),
            dot_node: dvv.dot.node_id.clone(),
            dot_counter: dvv.dot.counter,
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
    use std::collections::HashMap;

    fn nid(s: &str) -> NodeId {
        s.to_string()
    }

    /// Simulate what the engine does for a local operation:
    /// advance DVV, mint a dot, and apply to CRDT (which fills context from DVV).
    fn engine_apply_local(
        set: &mut OrSet<String>,
        dvv: &mut DotVersionVector,
        op: OrSetOp<String>,
    ) -> OrSetDelta<String> {
        dvv.event();
        let dot = Dot::new(dvv.dot.node_id.clone(), dvv.dot.counter);
        set.apply_local(dot, op, dvv)
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

        let state = a_set.full_state(&a_dvv);

        // Full state must carry tombstones and correct causal metadata.
        assert!(!state.tombstones.is_empty());
        assert_eq!(state.dot_counter, a_dvv.dot.counter);

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

    // ── delta_since tests ───────────────────────────────────────────────

    #[test]
    fn delta_since_empty_remote_equals_full_state() {
        let mut set = OrSet::<String>::new();
        let mut dvv = DotVersionVector::new(nid("A"));

        engine_apply_local(&mut set, &mut dvv, OrSetOp::Add("x".to_string()));
        engine_apply_local(&mut set, &mut dvv, OrSetOp::Add("y".to_string()));
        engine_apply_local(&mut set, &mut dvv, OrSetOp::Remove("x".to_string()));

        let empty_knowledge = HashMap::new();
        let delta = set.delta_since(&empty_knowledge, &dvv);

        // A brand-new joiner must receive every add and every tombstone.
        let full = set.full_state(&dvv);
        assert_eq!(delta.adds.len(), full.adds.len());
        assert_eq!(delta.tombstones.len(), full.tombstones.len());
    }

    #[test]
    fn delta_since_up_to_date_remote_is_empty() {
        let mut set = OrSet::<String>::new();
        let mut dvv = DotVersionVector::new(nid("A"));

        engine_apply_local(&mut set, &mut dvv, OrSetOp::Add("x".to_string()));
        engine_apply_local(&mut set, &mut dvv, OrSetOp::Add("y".to_string()));

        // Remote already knows everything.
        let delta = set.delta_since(&dvv.effective_map(), &dvv);
        assert!(delta.adds.is_empty());
        assert!(delta.tombstones.is_empty());
    }

    #[test]
    fn delta_since_sends_only_missing_adds() {
        let mut a_set = OrSet::<String>::new();
        let mut a_dvv = DotVersionVector::new(nid("A"));

        // A adds "x" then "y".
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("x".to_string()));
        let snapshot_knowledge = a_dvv.effective_map(); // B knows up to here
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("y".to_string()));

        // B only knows about A:1 — should receive only the "y" add.
        let delta = a_set.delta_since(&snapshot_knowledge, &a_dvv);
        assert_eq!(delta.adds.len(), 1);
        assert_eq!(delta.adds[0].0, "y".to_string());
        assert!(delta.tombstones.is_empty());
    }

    #[test]
    fn delta_since_sends_only_missing_tombstones() {
        let mut a_set = OrSet::<String>::new();
        let mut a_dvv = DotVersionVector::new(nid("A"));

        // A adds "x" and "y"; B syncs at this point.
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("x".to_string()));
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("y".to_string()));
        let b_knowledge = a_dvv.effective_map();

        // A then removes "x" — B doesn't know yet.
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Remove("x".to_string()));

        let delta = a_set.delta_since(&b_knowledge, &a_dvv);
        assert!(delta.adds.is_empty(), "no new adds expected");
        assert_eq!(delta.tombstones.len(), 1, "B is missing the remove tombstone");
    }

    #[test]
    fn delta_since_pull_response_converges() {
        // Simulate the full pull protocol: A has state, B pulls with its knowledge.
        let mut a_set = OrSet::<String>::new();
        let mut a_dvv = DotVersionVector::new(nid("A"));
        let mut b_set = OrSet::<String>::new();
        let mut b_dvv = DotVersionVector::new(nid("B"));

        // A adds "x", "y", then removes "x".
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("x".to_string()));
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("y".to_string()));
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Remove("x".to_string()));

        // B has no state; it sends its empty knowledge map in a pull request.
        let b_knowledge = b_dvv.effective_map();

        // A responds with a minimal delta (equals full state here since B is empty).
        // Context is now populated directly by delta_since — no post-hoc patching.
        let response = a_set.delta_since(&b_knowledge, &a_dvv);

        // B merges the response.
        engine_merge_delta(&mut b_set, &mut b_dvv, &response);

        assert!(!b_set.contains(&"x".to_string()), "x should be removed");
        assert!(b_set.contains(&"y".to_string()), "y should be present");
        assert_eq!(b_dvv.effective_counter(&nid("A")), 3);
    }

    #[test]
    fn delta_since_partial_pull_response_converges() {
        // B already has some state from A; a partial pull should bring it up to date.
        let mut a_set = OrSet::<String>::new();
        let mut a_dvv = DotVersionVector::new(nid("A"));
        let mut b_set = OrSet::<String>::new();
        let mut b_dvv = DotVersionVector::new(nid("B"));

        // A adds "x"; B syncs.
        let delta_x = engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("x".to_string()));
        engine_merge_delta(&mut b_set, &mut b_dvv, &delta_x);
        assert!(b_set.contains(&"x".to_string()));

        // A then adds "y" and removes "x" — B hasn't seen these yet.
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Add("y".to_string()));
        engine_apply_local(&mut a_set, &mut a_dvv, OrSetOp::Remove("x".to_string()));

        // B pulls with its current knowledge.
        let b_knowledge = b_dvv.effective_map();
        let response = a_set.delta_since(&b_knowledge, &a_dvv);

        // The response must be minimal: 1 new add ("y") and 1 new tombstone.
        assert_eq!(response.adds.len(), 1);
        assert_eq!(response.tombstones.len(), 1);

        engine_merge_delta(&mut b_set, &mut b_dvv, &response);

        assert!(!b_set.contains(&"x".to_string()), "x should be removed");
        assert!(b_set.contains(&"y".to_string()), "y should be present");
        assert_eq!(b_dvv.effective_counter(&nid("A")), 3);
    }
}
