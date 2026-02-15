//! Observed-Remove Set (OR-Set) using the existing DotVersionVector.
//!
//! Each element is tagged with a `Dot` from the DVV.  The DVV mints
//! dots on add and `dominates_dot()` tells us whether a dot has been
//! causally superseded.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::common::{Counter, NodeId};
use crate::crdt::DeltaCrdt;
use crate::logical_clocks::dot_version_vector::{CausalContext, Dot, DotVersionVector};

// ── Wire types (serialised into CrdtOp.payload) ────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrSetOp<E> {
    Add(E),
    Remove(E),
}

/// Delta produced by a single operation — this is what travels on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSetDelta<E> {
    pub adds: Vec<(E, Dot)>,
    pub removes: Vec<Dot>,
    /// Sender's DVV state so the receiver can merge causal knowledge.
    pub dvv_context: CausalContext,
    pub dvv_dot_node: NodeId,
    pub dvv_dot_counter: Counter,
}

/// Full-state snapshot for initial sync / pull responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSetState<E> {
    pub entries: Vec<(E, Dot)>,
    pub dvv_context: CausalContext,
    pub dvv_dot_node: NodeId,
    pub dvv_dot_counter: Counter,
}

// ── The OR-Set ─────────────────────────────────────────────────────────

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
    fn apply_local(
        &mut self,
        dvv: &mut DotVersionVector,
        node_id: &NodeId,
        op_bytes: &[u8],
    ) -> Vec<u8> {
        // TODO: Refactor json to protobuf
        let op: OrSetOp<E> = serde_json::from_slice(op_bytes)
            .expect("apply_local: caller must provide valid OrSetOp bytes");

        match op {
            OrSetOp::Add(elem) => {
                // Mint a new dot via the DVV.
                dvv.event();
                let dot = Dot::new(node_id.clone(), dvv.dot.counter);

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

                let delta = OrSetDelta::<E> {
                    adds: vec![(elem, dot)],
                    removes: old_dots,
                    dvv_context: dvv.context.clone(),
                    dvv_dot_node: dvv.dot.node_id.clone(),
                    dvv_dot_counter: dvv.dot.counter,
                };
                tracing::info!("elements: {:?}", self.elements().collect::<Vec<_>>());
                serde_json::to_vec(&delta).expect("delta serialisation")
            }

            OrSetOp::Remove(elem) => {
                let removed_dots: Vec<Dot> = self
                    .entries
                    .remove(&elem)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();

                // Remove doesn't create a new event in the DVV.
                let delta = OrSetDelta::<E> {
                    adds: vec![],
                    removes: removed_dots,
                    dvv_context: dvv.context.clone(),
                    dvv_dot_node: dvv.dot.node_id.clone(),
                    dvv_dot_counter: dvv.dot.counter,
                };
                serde_json::to_vec(&delta).expect("delta serialisation")
            }
        }
    }

    fn apply_remote(
        &mut self,
        dvv: &mut DotVersionVector,
        payload: &[u8],
    ) {
        let delta: OrSetDelta<E> = match serde_json::from_slice(payload) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(%e, "failed to decode OrSetDelta");
                return;
            }
        };

        // 1. Remove tombstoned dots.
        for dot in &delta.removes {
            for dots in self.entries.values_mut() {
                dots.remove(dot);
            }
        }
        self.entries.retain(|_, dots| !dots.is_empty());

        // 2. Apply adds — only if we haven't already seen this dot.
        for (elem, dot) in &delta.adds {
            if !dvv.dominates_dot(dot) {
                let entry = self.entries.entry(elem.clone()).or_default();
                entry.insert(dot.clone());
            }
        }

        // 3. Merge the sender's DVV context into ours.
        let remote_dvv = DotVersionVector {
            dot: Dot::new(delta.dvv_dot_node, delta.dvv_dot_counter),
            context: delta.dvv_context,
        };
        dvv.merge(&remote_dvv);
    }

    fn encode_state(&self, dvv: &DotVersionVector) -> Vec<u8> {
        let entries: Vec<(E, Dot)> = self
            .entries
            .iter()
            .flat_map(|(elem, dots)| {
                dots.iter().map(move |dot| (elem.clone(), dot.clone()))
            })
            .collect();

        let state = OrSetState {
            entries,
            dvv_context: dvv.context.clone(),
            dvv_dot_node: dvv.dot.node_id.clone(),
            dvv_dot_counter: dvv.dot.counter,
        };
        serde_json::to_vec(&state).expect("state serialisation")
    }

    fn merge_state(
        &mut self,
        dvv: &mut DotVersionVector,
        payload: &[u8],
    ) {
        let state: OrSetState<E> = match serde_json::from_slice(payload) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(%e, "failed to decode OrSetState");
                return;
            }
        };

        for (elem, dot) in &state.entries {
            let entry = self.entries.entry(elem.clone()).or_default();
            entry.insert(dot.clone());
        }

        let remote_dvv = DotVersionVector {
            dot: Dot::new(state.dvv_dot_node, state.dvv_dot_counter),
            context: state.dvv_context,
        };
        dvv.merge(&remote_dvv);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nid(s: &str) -> NodeId { s.to_string() }

    fn op_bytes<E: Serialize>(op: &OrSetOp<E>) -> Vec<u8> {
        serde_json::to_vec(op).unwrap()
    }

    #[test]
    fn add_uses_dvv_dot() {
        let mut set = OrSet::<String>::new();
        let mut dvv = DotVersionVector::new(nid("A"));

        let _ = set.apply_local(&mut dvv, &nid("A"), &op_bytes(&OrSetOp::Add("x".to_string())));

        assert!(set.contains(&"x".to_string()));
        assert_eq!(dvv.dot.counter, 1);
    }

    #[test]
    fn remove_does_not_advance_dvv() {
        let mut set = OrSet::<String>::new();
        let mut dvv = DotVersionVector::new(nid("A"));

        let _ = set.apply_local(&mut dvv, &nid("A"), &op_bytes(&OrSetOp::Add("x".to_string())));
        assert_eq!(dvv.dot.counter, 1);

        let _ = set.apply_local(&mut dvv, &nid("A"), &op_bytes(&OrSetOp::Remove("x".to_string())));
        assert_eq!(dvv.dot.counter, 1);
        assert!(!set.contains(&"x".to_string()));
    }

    #[test]
    fn concurrent_add_wins() {
        let mut a_set = OrSet::<String>::new();
        let mut a_dvv = DotVersionVector::new(nid("A"));
        let mut b_set = OrSet::<String>::new();
        let mut b_dvv = DotVersionVector::new(nid("B"));

        let delta_a = a_set.apply_local(&mut a_dvv, &nid("A"), &op_bytes(&OrSetOp::Add("x".to_string())));
        b_set.apply_remote(&mut b_dvv, &delta_a);
        assert!(b_set.contains(&"x".to_string()));

        let remove_delta = b_set.apply_local(&mut b_dvv, &nid("B"), &op_bytes(&OrSetOp::Remove("x".to_string())));
        let readd_delta = a_set.apply_local(&mut a_dvv, &nid("A"), &op_bytes(&OrSetOp::Add("x".to_string())));

        a_set.apply_remote(&mut a_dvv, &remove_delta);
        assert!(a_set.contains(&"x".to_string()));

        b_set.apply_remote(&mut b_dvv, &readd_delta);
        assert!(b_set.contains(&"x".to_string()));
    }

    #[test]
    fn delta_roundtrip_through_bytes() {
        let mut set = OrSet::<String>::new();
        let mut dvv = DotVersionVector::new(nid("A"));

        let delta_bytes = set.apply_local(&mut dvv, &nid("A"), &op_bytes(&OrSetOp::Add("hello".to_string())));

        let mut remote_set = OrSet::<String>::new();
        let mut remote_dvv = DotVersionVector::new(nid("B"));
        remote_set.apply_remote(&mut remote_dvv, &delta_bytes);

        assert!(remote_set.contains(&"hello".to_string()));
        assert_eq!(remote_dvv.effective_counter(&nid("A")), 1);
    }

    #[test]
    fn full_state_sync() {
        let mut a_set = OrSet::<String>::new();
        let mut a_dvv = DotVersionVector::new(nid("A"));

        a_set.apply_local(&mut a_dvv, &nid("A"), &op_bytes(&OrSetOp::Add("x".to_string())));
        a_set.apply_local(&mut a_dvv, &nid("A"), &op_bytes(&OrSetOp::Add("y".to_string())));

        let state_bytes = a_set.encode_state(&a_dvv);

        let mut b_set = OrSet::<String>::new();
        let mut b_dvv = DotVersionVector::new(nid("B"));
        b_set.merge_state(&mut b_dvv, &state_bytes);

        assert!(b_set.contains(&"x".to_string()));
        assert!(b_set.contains(&"y".to_string()));
        assert_eq!(b_dvv.effective_counter(&nid("A")), 2);
    }
}
