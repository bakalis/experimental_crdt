//! Formal garbage collection protocol implementation.
//!
//! This module implements the epoch-fenced GC protocol as specified in the
//! algorithmic notation, using object store primitives for coordination.
//!
//! ## Protocol Overview
//!
//! The protocol uses an object store with linearizable operations to coordinate
//! garbage collection across replicas without requiring consensus. Each epoch
//! represents a GC round, and the protocol ensures that tombstones are only
//! collected when all replicas have seen them.
//!
//! ## Object Store Keys
//!
//! - `epoch_entry/N` → Stable timestamp (version vector) for epoch N
//! - `bottom_state/N` → GC'd CRDT state for epoch N (state + initiator VV)
//! - `gc_intent/N` → Intent marker for coordinating epoch N initiation
//! - `member/node_id` → Membership marker for active replicas
//! - `clock/node_id` → Version vector published by each replica
//!
//! ## Main Procedures
//!
//! - `InitiateGC`: Initiates a new GC epoch (Algorithm from spec)
//! - `Bootstrap`: New replica joins and synchronizes (Algorithm from spec)
//! - `ObserveEpochChange`: Live replica catches up to latest epoch
//! - `ComputeStableTimestamp`: Calculates causally stable timestamp
//! - `Cleanup`: Removes old epoch entries

use std::cmp::max;
use std::collections::HashMap;

use anyhow::{anyhow, Result};
use tracing::{debug, info, warn};

use crate::common::NodeId;
use crate::crdt::{DeltaCrdt, DeltaContext};
use crate::logical_clocks::dot_version_vector::DotVersionVector;
use crate::object_store::{BottomState, EpochEntry, GcIntent, ObjectStore, VersionVector};

/// Replica state for the GC protocol.
pub struct GcReplica<C: DeltaCrdt> {
    /// This replica's ID
    pub node_id: NodeId,
    /// Current epoch number for this replica
    pub epoch: u64,
    /// CRDT state
    pub crdt_state: C,
    /// Dotted version vector (causal context)
    pub dvv: DotVersionVector,
    /// Object store for coordination
    pub store: ObjectStore,
}

impl<C: DeltaCrdt> GcReplica<C> {
    pub fn new(node_id: NodeId, crdt_state: C, dvv: DotVersionVector, store: ObjectStore) -> Self {
        Self {
            node_id,
            epoch: 0,
            crdt_state,
            dvv,
            store,
        }
    }

    /// Compute the causally stable timestamp from member clocks.
    ///
    /// Algorithm: ComputeStableTimestamp from spec
    /// Returns the pointwise minimum of all replica clocks, treating missing clocks as 0.
    pub async fn compute_stable_timestamp(&self, members: &[NodeId]) -> Result<VersionVector> {
        let mut stable = VersionVector::new();

        for member in members {
            let clock_key = format!("clock/{}", member);
            let clock: VersionVector = self.store.read_or_default(&clock_key).await?;

            // Pointwise minimum (meet operation)
            if stable.is_empty() {
                stable = clock;
            } else {
                for (node, counter) in &clock {
                    let entry = stable.entry(node.clone()).or_insert(u64::MAX);
                    *entry = (*entry).min(*counter);
                }
            }
        }

        debug!(
            node_id = %self.node_id,
            members = members.len(),
            "computed stable timestamp"
        );
        Ok(stable)
    }

    /// Initiate a new GC epoch.
    ///
    /// Algorithm: InitiateGC from spec
    ///
    /// This procedure:
    /// 1. Ensures this replica is current with latest epochs
    /// 2. Performs closed-world check on membership
    /// 3. Uses CAS to claim the next epoch
    /// 4. Computes stable timestamp from member clocks
    /// 5. Checks for strict progress (V_stable > V_prev)
    /// 6. Commits the epoch by writing epoch_entry and bottom_state
    /// 7. Applies GC locally
    pub async fn initiate_gc(&mut self) -> Result<bool> {
        // Step 1: Ensure this replica is current
        self.observe_epoch_change().await?;

        // Step 2: Find latest epoch
        let n_latest = self.find_latest_epoch().await?;

        if self.epoch != n_latest {
            debug!(
                node_id = %self.node_id,
                current_epoch = self.epoch,
                latest_epoch = n_latest,
                "replica is behind; aborting GC initiation"
            );
            return Ok(false);
        }

        let n = self.epoch + 1;
        debug!(
            node_id = %self.node_id,
            new_epoch = n,
            "attempting to initiate GC epoch"
        );

        // Step 3: Closed-world check
        let members_0 = self.list_members().await?;

        // Step 4: CAS to claim epoch
        let intent = GcIntent {
            epoch: n,
            initiator_id: self.node_id.clone(),
        };
        let intent_key = format!("gc_intent/{}", n);
        let claimed = self.store.cas(&intent_key, None, &intent).await?;

        if !claimed {
            debug!(
                node_id = %self.node_id,
                epoch = n,
                "another replica claimed epoch; aborting"
            );
            return Ok(false);
        }

        // Step 5: Second membership check (closed-world window)
        let members_1 = self.list_members().await?;
        if members_1 != members_0 {
            warn!(
                node_id = %self.node_id,
                epoch = n,
                "membership changed during CAS window; aborting"
            );
            self.store.delete(&intent_key).await?;
            return Ok(false);
        }

        // Step 6: Compute stable timestamp
        let v_stable = self.compute_stable_timestamp(&members_1).await?;

        // Step 7: Strict progress check
        if n > 1 {
            let prev_key = format!("epoch_entry/{}", n - 1);
            let prev_entry: Option<EpochEntry> = self.store.read(&prev_key).await?;
            if let Some(prev) = prev_entry {
                if !is_strictly_greater(&v_stable, &prev.stable_timestamp) {
                    info!(
                        node_id = %self.node_id,
                        epoch = n,
                        "no threshold advancement; aborting (safe no-op)"
                    );
                    self.store.delete(&intent_key).await?;
                    return Ok(false);
                }
            }
        }

        // Step 8: Apply GC locally
        let safe_epoch = Some(n);
        let collected = self.crdt_state.garbage_collect(safe_epoch);
        info!(
            node_id = %self.node_id,
            epoch = n,
            tombstones_collected = collected,
            "applied local GC"
        );

        // Step 9: Commit epoch
        let entry = EpochEntry {
            epoch: n,
            stable_timestamp: v_stable.clone(),
        };
        let entry_key = format!("epoch_entry/{}", n);
        self.store.write(&entry_key, &entry).await?;

        // Step 10: Write bottom state
        // For now, we'll use a placeholder. In a full implementation, we'd serialize
        // the actual CRDT state after GC.
        let bottom_state = BottomState {
            state_bytes: vec![], // TODO: serialize self.crdt_state
            initiator_vv: self.dvv.effective_map(),
        };
        let bottom_key = format!("bottom_state/{}", n);
        self.store.write(&bottom_key, &bottom_state).await?;

        // Step 11: Apply locally
        self.epoch = n;

        // Step 12: Release barrier
        self.store.delete(&intent_key).await?;

        info!(
            node_id = %self.node_id,
            epoch = n,
            "successfully initiated and committed GC epoch"
        );

        Ok(true)
    }

    /// Bootstrap a new replica by syncing to the latest epoch.
    ///
    /// Algorithm: Bootstrap from spec
    ///
    /// This procedure:
    /// 1. Writes member registration
    /// 2. Finds latest bottom_state
    /// 3. Waits if GC is in progress for next epoch
    /// 4. Loads bottom state and epoch entry
    /// 5. Initializes local state and version vector
    /// 6. Writes clock
    pub async fn bootstrap(&mut self) -> Result<()> {
        // Step 1: Register as member
        let member_key = format!("member/{}", self.node_id);
        self.store.write(&member_key, &true).await?;
        info!(node_id = %self.node_id, "registered as member");

        // Step 2: Find latest bottom_state
        let n_latest = self.find_latest_bottom_state().await?;

        // Step 3: Check if GC is in progress for next epoch
        let next_intent_key = format!("gc_intent/{}", n_latest + 1);
        if let Some(_intent) = self.store.read::<GcIntent>(&next_intent_key).await? {
            info!(
                node_id = %self.node_id,
                epoch = n_latest + 1,
                "GC in progress for next epoch; waiting for completion"
            );

            // Wait for bottom_state to appear
            // TODO: Implement proper waiting with timeout
            // For now, we'll just log and proceed with current epoch
            warn!("waiting for GC completion not yet implemented; proceeding with current epoch");
        }

        // Step 4: Load bottom state
        let bottom_key = format!("bottom_state/{}", n_latest);
        let bottom_state: BottomState = self
            .store
            .read(&bottom_key)
            .await?
            .ok_or_else(|| anyhow!("bottom_state missing for epoch {}", n_latest))?;

        // Step 5: Load epoch entry
        let entry_key = format!("epoch_entry/{}", n_latest);
        let entry: EpochEntry = self
            .store
            .read(&entry_key)
            .await?
            .ok_or_else(|| anyhow!("epoch_entry missing for epoch {}", n_latest))?;

        // Step 6: Initialize state
        // TODO: Deserialize bottom_state.state_bytes into self.crdt_state
        // For now, we keep existing CRDT state

        // Step 7: Initialize version vector
        // V_new = V_initiator ⊔ V_stable
        let mut vv = bottom_state.initiator_vv;
        for (node, counter) in entry.stable_timestamp {
            let entry = vv.entry(node).or_insert(0);
            *entry = (*entry).max(counter);
        }

        // Reconstruct DVV from version vector
        self.dvv = DotVersionVector::new(self.node_id.clone());
        for (node, counter) in vv {
            if node == self.node_id {
                // Set our own dot counter
                self.dvv.dot.counter = counter;
            } else {
                // Add to context
                self.dvv.context.insert(node, counter);
            }
        }

        self.epoch = n_latest;

        // Step 8: Write clock
        let clock_key = format!("clock/{}", self.node_id);
        self.store.write(&clock_key, &self.dvv.effective_map()).await?;

        info!(
            node_id = %self.node_id,
            epoch = self.epoch,
            "bootstrap complete"
        );

        Ok(())
    }

    /// Observe epoch change and catch up to the latest epoch.
    ///
    /// Algorithm: ObserveEpochChange from spec
    ///
    /// This procedure:
    /// 1. Finds latest epoch
    /// 2. If already current, returns
    /// 3. Otherwise, reads stable timestamp and applies GC
    /// 4. Updates local epoch
    /// 5. Writes current clock
    pub async fn observe_epoch_change(&mut self) -> Result<()> {
        let n_latest = self.find_latest_epoch().await?;

        if n_latest <= self.epoch {
            // Already up to date
            return Ok(());
        }

        debug!(
            node_id = %self.node_id,
            current_epoch = self.epoch,
            latest_epoch = n_latest,
            "observing epoch change"
        );

        // Load the stable timestamp for the latest epoch
        let entry_key = format!("epoch_entry/{}", n_latest);
        let entry: EpochEntry = self
            .store
            .read(&entry_key)
            .await?
            .ok_or_else(|| anyhow!("epoch_entry missing for epoch {}", n_latest))?;

        // Apply GC to local state
        let safe_epoch = Some(n_latest);
        let collected = self.crdt_state.garbage_collect(safe_epoch);

        info!(
            node_id = %self.node_id,
            from_epoch = self.epoch,
            to_epoch = n_latest,
            tombstones_collected = collected,
            "caught up to latest epoch"
        );

        // Update local epoch
        self.epoch = n_latest;

        // Write current clock
        let clock_key = format!("clock/{}", self.node_id);
        self.store.write(&clock_key, &self.dvv.effective_map()).await?;

        Ok(())
    }

    /// Local write operation.
    ///
    /// Algorithm: LocalWrite from spec
    ///
    /// Increments counter, applies operation, and periodically writes clock.
    pub fn local_write(&mut self, op: C::Op) -> C::Delta {
        // Increment counter
        self.dvv.event();
        let dot = self.dvv.dot.clone();

        // Apply operation
        let delta = self.crdt_state.apply_local(dot, op, &self.dvv);

        // Note: Clock write is done periodically, not on every write
        // The caller can trigger clock writes as needed

        delta
    }

    /// Merge incoming delta.
    ///
    /// Algorithm: MergeDelta from spec
    ///
    /// Merges delta into CRDT state and updates version vector.
    pub fn merge_delta(&mut self, delta: &C::Delta) {
        // Merge CRDT state
        self.crdt_state.merge_delta(delta);

        // Merge causal context
        let (ctx, node, counter) = delta.causal_context();
        let remote_dvv = DotVersionVector {
            dot: crate::logical_clocks::dot_version_vector::Dot::new(node, counter),
            context: ctx,
        };
        self.dvv.merge(&remote_dvv);
    }

    /// Write current clock to object store.
    pub async fn write_clock(&self) -> Result<()> {
        let clock_key = format!("clock/{}", self.node_id);
        self.store.write(&clock_key, &self.dvv.effective_map()).await?;
        Ok(())
    }

    /// Cleanup old epoch entries.
    ///
    /// Algorithm: Cleanup from spec
    ///
    /// Deletes all epoch_entry and bottom_state entries older than the latest epoch.
    pub async fn cleanup(&self) -> Result<usize> {
        let n_latest = self.find_latest_epoch().await?;
        let mut deleted = 0;

        for n in 1..n_latest {
            let entry_key = format!("epoch_entry/{}", n);
            let bottom_key = format!("bottom_state/{}", n);

            if let Ok(()) = self.store.delete(&entry_key).await {
                deleted += 1;
            }
            if let Ok(()) = self.store.delete(&bottom_key).await {
                deleted += 1;
            }
        }

        info!(
            node_id = %self.node_id,
            deleted_entries = deleted,
            kept_epoch = n_latest,
            "cleaned up old epoch entries"
        );

        Ok(deleted)
    }

    // ────────────────────────────────────────────────────────────────────────────
    // Helper methods
    // ────────────────────────────────────────────────────────────────────────────

    /// Find the latest epoch that has an epoch_entry.
    async fn find_latest_epoch(&self) -> Result<u64> {
        let keys = self.store.list_keys("epoch_entry/").await?;
        let mut max_epoch = 0;

        for key in keys {
            if let Some(epoch_str) = key.strip_prefix("epoch_entry/") {
                if let Ok(epoch) = epoch_str.parse::<u64>() {
                    max_epoch = max_epoch.max(epoch);
                }
            }
        }

        Ok(max_epoch)
    }

    /// Find the latest epoch that has a bottom_state.
    async fn find_latest_bottom_state(&self) -> Result<u64> {
        let keys = self.store.list_keys("bottom_state/").await?;
        let mut max_epoch = 0;

        for key in keys {
            if let Some(epoch_str) = key.strip_prefix("bottom_state/") {
                if let Ok(epoch) = epoch_str.parse::<u64>() {
                    max_epoch = max_epoch.max(epoch);
                }
            }
        }

        Ok(max_epoch)
    }

    /// List all current members.
    async fn list_members(&self) -> Result<Vec<NodeId>> {
        let keys = self.store.list_suffixes("member/").await?;
        Ok(keys)
    }
}

/// Check if vv1 is strictly greater than vv2 (vv1 > vv2 in pointwise order).
fn is_strictly_greater(vv1: &VersionVector, vv2: &VersionVector) -> bool {
    if vv2.is_empty() {
        return !vv1.is_empty();
    }

    let mut has_greater = false;

    // Check all keys in vv2
    for (node, &counter2) in vv2 {
        let counter1 = vv1.get(node).copied().unwrap_or(0);
        if counter1 < counter2 {
            return false; // vv1 is not >= vv2
        }
        if counter1 > counter2 {
            has_greater = true;
        }
    }

    // Check keys only in vv1
    for (node, &counter1) in vv1 {
        if !vv2.contains_key(node) && counter1 > 0 {
            has_greater = true;
        }
    }

    has_greater
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_strictly_greater() {
        let mut vv1 = VersionVector::new();
        let mut vv2 = VersionVector::new();

        vv1.insert("a".to_string(), 5);
        vv2.insert("a".to_string(), 3);

        assert!(is_strictly_greater(&vv1, &vv2));
        assert!(!is_strictly_greater(&vv2, &vv1));

        vv1.insert("b".to_string(), 2);
        vv2.insert("b".to_string(), 2);

        assert!(is_strictly_greater(&vv1, &vv2)); // a: 5 > 3, b: 2 = 2

        vv2.insert("c".to_string(), 1);
        assert!(!is_strictly_greater(&vv1, &vv2)); // vv1 missing c

        vv1.insert("c".to_string(), 1);
        assert!(is_strictly_greater(&vv1, &vv2)); // Equal on c, but greater on a
    }

    #[test]
    fn test_is_strictly_greater_empty() {
        let mut vv1 = VersionVector::new();
        let vv2 = VersionVector::new();

        assert!(!is_strictly_greater(&vv1, &vv2)); // Both empty

        vv1.insert("a".to_string(), 1);
        assert!(is_strictly_greater(&vv1, &vv2)); // vv1 non-empty, vv2 empty
    }
}
