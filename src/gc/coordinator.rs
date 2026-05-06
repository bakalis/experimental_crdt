use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use crate::common::{Counter, NodeId};
use crate::crdt::DeltaCrdt;
use crate::logical_clocks::dot_version_vector;
use crate::discovery::list_live_registrations;
use crate::gc::storage::{BottomStateRecord, GcStorage, GcStorageConfig, S3GcStorage};
use crate::logical_clocks::dot_version_vector::{Dot, DotVersionVector};
use crate::s3_client::S3Client;
use tracing::{info, warn};

const GC_INTENT_POLL_INTERVAL_MS: u64 = 200;

#[derive(Debug, Clone)]
pub struct GcConfig {
    pub bucket: String,
    pub prefix: String,
    pub registration_ttl: Duration,
    pub initiate_interval: Duration,
    pub observe_interval: Duration,
    pub cleanup_interval: Duration,
}

impl GcConfig {
    pub fn storage_config(&self) -> GcStorageConfig {
        GcStorageConfig {
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GcCoordinator<S: GcStorage> {
    storage: S,
    membership: Arc<DiscoveryMembershipProvider>,
    config: GcConfig,
}

pub fn new_member_exists(old: &[NodeId], new: &[NodeId]) -> bool {
    let old_set: HashSet<&NodeId> = old.iter().collect();
    new.iter().any(|node| !old_set.contains(node))
}

impl GcCoordinator<S3GcStorage> {
    pub fn new(client: S3Client, config: GcConfig) -> Self {
        let storage = S3GcStorage::new(client.clone(), config.storage_config());
        let membership = Arc::new(DiscoveryMembershipProvider {
            client,
            bucket: config.bucket.clone(),
            registration_ttl: config.registration_ttl,
        });
        Self {
            storage,
            membership,
            config,
        }
    }
}

impl<S: GcStorage> GcCoordinator<S> {
    pub fn initiate_interval(&self) -> Duration {
        self.config.initiate_interval
    }

    pub fn observe_interval(&self) -> Duration {
        self.config.observe_interval
    }

    pub fn cleanup_interval(&self) -> Duration {
        self.config.cleanup_interval
    }

    pub async fn publish_clock(
        &self,
        node_id: &NodeId,
        clock: &HashMap<NodeId, Counter>,
    ) -> anyhow::Result<()> {
        self.storage.write_clock(node_id, clock).await
    }

    pub async fn observe_epoch_change<C: DeltaCrdt>(
        &self,
        node_id: &NodeId,
        crdt: &mut C,
        current_clock: &HashMap<NodeId, Counter>,
    ) -> anyhow::Result<()> {
        let n_latest = self.storage.latest_epoch_entry().await?;
        if n_latest == 0 {
            return Ok(());
        }

        let stable = self.storage.read_epoch_entry(n_latest).await?;
        let frontier = dot_version_vector::frontier_dvv(node_id, &stable);
        crdt.perform_gc(&frontier);
        self.storage.write_clock(node_id, current_clock).await
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        self.prune_disconnected_clocks().await?;
        let latest = self.storage.latest_epoch_entry().await?;
        if latest == 0 {
            return Ok(());
        }
        self.storage.cleanup_before_epoch(latest).await
    }

    pub async fn initiate_gc<C: DeltaCrdt>(
        &self,
        node_id: &NodeId,
        crdt: &mut C,
        dvv: &DotVersionVector,
    ) -> anyhow::Result<()> {
        self.observe_epoch_change(node_id, crdt, &dvv.effective_map()).await?;

        let n_latest = self.storage.latest_epoch_entry().await?;
        let n = n_latest + 1;
        let m0 = self.membership.live_members().await?;
        info!(epoch = n, "attempting to initiate GC");

        if !self.storage.claim_gc_intent(n, node_id).await? {
            info!(epoch = n, "gc_intent already claimed by another replica, aborting GC initiation");
            return Ok(());
        }

        let m1 = self.membership.live_members().await?;
        if new_member_exists(&m0, &m1) {
            if let Err(e) = self.storage.release_gc_intent(n).await {
                warn!(%e, epoch = n, "failed to release gc_intent after membership-change abort");
            }
            info!(epoch = n, "new member(s) detected during GC initiation, aborting GC");
            return Ok(());
        }

        let v_stable = self.compute_stable_timestamp(&m1).await?;
        let v_prev = self.storage.read_epoch_entry(n - 1).await?;
        if !dot_version_vector::vv_lt(&v_prev, &v_stable) {
            info!(epoch = n, "stable timestamp did not advance since last GC, aborting GC");
            if let Err(e) = self.storage.release_gc_intent(n).await {
                warn!(%e, epoch = n, "failed to release gc_intent after strict-progress abort");
            }
            return Ok(());
        }

        let frontier = dot_version_vector::frontier_dvv(node_id, &v_stable);
        crdt.perform_gc(&frontier);
        let gc_state = crdt.full_state(dvv);
        let gc_state_payload = C::encode_delta(&gc_state);

        self.storage.write_epoch_entry(n, &v_stable).await?;
        self.storage
            .write_bottom_state(
                n,
                &BottomStateRecord {
                    state_payload: gc_state_payload,
                    initiator_clock: dvv.effective_map(),
                },
            )
            .await?;
        if let Err(e) = self.storage.release_gc_intent(n).await {
            warn!(%e, epoch = n, "failed to release gc_intent after commit");
        }
        Ok(())
    }

    pub async fn new_replica_bootstrap<C: DeltaCrdt>(
        &self,
        node_id: &NodeId,
        crdt: &mut C,
        dvv: &mut DotVersionVector,
    ) -> anyhow::Result<()> {
        let _ = self
            .storage
            .write_epoch_entry_if_absent(0, &HashMap::<NodeId, Counter>::new())
            .await?;
        let initial_state = crdt.full_state(dvv);
        let _ = self
            .storage
            .write_bottom_state_if_absent(
                0,
                &BottomStateRecord {
                    state_payload: C::encode_delta(&initial_state),
                    initiator_clock: dvv.effective_map(),
                },
            )
            .await?;

        let mut n_latest = self.storage.latest_bottom_state_epoch().await?;
        loop {
            let next_intent = self.storage.read_gc_intent(n_latest + 1).await?;
            if next_intent.is_some() {
                info!(epoch = n_latest + 1, "detected gc_intent for next epoch, waiting for bottom_state to be published");
                loop {
                    tokio::time::sleep(Duration::from_millis(GC_INTENT_POLL_INTERVAL_MS)).await;
                    let present = self.storage.read_bottom_state(n_latest + 1).await?;
                    if present.is_some() {
                        info!(epoch = n_latest + 1, "detected published bottom_state for next epoch, proceeding with bootstrap");
                        n_latest += 1;
                        break;
                    }
                }
            } else {
                break;
            }
        }

        let stable = self.storage.read_epoch_entry(n_latest).await?;
        if let Some(bottom) = self.storage.read_bottom_state(n_latest).await? {
            let snapshot = C::decode_delta(&bottom.state_payload)
                .map_err(|e| anyhow::anyhow!("failed to decode bottom_state payload: {e}"))?;
            crdt.merge_delta(&snapshot);

            let merged_clock = dot_version_vector::vv_join(&bottom.initiator_clock, &stable);
            let remote_dvv = DotVersionVector {
                dot: Dot::new(node_id.clone(), 0),
                context: merged_clock,
            };
            dvv.merge(&remote_dvv);
        }

        self.storage.write_clock(node_id, &dvv.effective_map()).await?;
        Ok(())
    }

    pub async fn remove_clock(&self, node_id: &NodeId) -> anyhow::Result<()> {
        self.storage.delete_clock(node_id).await
    }

    async fn prune_disconnected_clocks(&self) -> anyhow::Result<()> {
        let live: std::collections::HashSet<NodeId> =
            self.membership.live_members().await?.into_iter().collect();
        for node_id in self.storage.list_clock_replicas().await? {
            if !live.contains(&node_id) {
                self.storage.delete_clock(&node_id).await?;
            }
        }
        Ok(())
    }

    async fn compute_stable_timestamp(
        &self,
        members: &[NodeId],
    ) -> anyhow::Result<HashMap<NodeId, Counter>> {
        let mut stable: Option<HashMap<NodeId, Counter>> = None;
        for member in members {
            let c = match self.storage.read_clock(member).await? {
                Some(v) => v,
                None => return Ok(HashMap::new()),
            };
            stable = Some(match stable {
                Some(acc) => dot_version_vector::vv_meet(&acc, &c),
                None => c,
            });
        }
        Ok(stable.unwrap_or_default())
    }

}

#[async_trait]
trait MembershipProvider: Send + Sync {
    async fn live_members(&self) -> anyhow::Result<Vec<NodeId>>;

}

struct DiscoveryMembershipProvider {
    client: S3Client,
    bucket: String,
    registration_ttl: Duration,
}

#[async_trait]
impl MembershipProvider for DiscoveryMembershipProvider {
    async fn live_members(&self) -> anyhow::Result<Vec<NodeId>> {
        let regs = list_live_registrations(&self.client, &self.bucket, self.registration_ttl)
            .await
            .context("failed to fetch live members from discovery")?;
        let mut members: Vec<NodeId> = regs.into_iter().map(|r| r.node_id).collect();
        members.sort();
        members.dedup();
        Ok(members)
    }
}

#[cfg(test)]
mod tests {
    use super::new_member_exists;

    fn s(x: &str) -> String {
        x.to_string()
    }

    #[test]
    fn no_new_members_same_lists() {
        let old = vec![s("1"), s("2"), s("3")];
        let new = vec![s("1"), s("2"), s("3")];

        assert!(!new_member_exists(&old, &new));
    }

    #[test]
    fn no_new_members_subset() {
        let old = vec![s("1"), s("2"), s("3")];
        let new = vec![s("1"), s("2")];

        assert!(!new_member_exists(&old, &new));
    }

    #[test]
    fn detects_single_new_member() {
        let old = vec![s("1"), s("2"), s("3")];
        let new = vec![s("1"), s("2"), s("3"), s("4")];

        assert!(new_member_exists(&old, &new));
    }

    #[test]
    fn detects_multiple_new_members() {
        let old = vec![s("1"), s("2")];
        let new = vec![s("1"), s("2"), s("3"), s("4")];

        assert!(new_member_exists(&old, &new));
    }

    #[test]
    fn empty_old_with_new_members() {
        let old: Vec<String> = vec![];
        let new = vec![s("1")];

        assert!(new_member_exists(&old, &new));
    }

    #[test]
    fn both_empty() {
        let old: Vec<String> = vec![];
        let new: Vec<String> = vec![];

        assert!(!new_member_exists(&old, &new));
    }

    #[test]
    fn order_does_not_matter() {
        let old = vec![s("1"), s("2"), s("3")];
        let new = vec![s("3"), s("2"), s("1")];

        assert!(!new_member_exists(&old, &new));
    }

    #[test]
    fn detects_new_member_with_reordering() {
        let old = vec![s("1"), s("2"), s("3")];
        let new = vec![s("3"), s("4"), s("2"), s("1")];

        assert!(new_member_exists(&old, &new));
    }

    #[test]
    fn handles_duplicates_in_new() {
        let old = vec![s("1"), s("2"), s("3")];
        let new = vec![s("1"), s("2"), s("2"), s("3")];

        assert!(!new_member_exists(&old, &new));
    }
}
