use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::common::{Counter, NodeId};
use crate::crdt::DeltaCrdt;
use crate::discovery::list_live_node_ids;
use crate::gc::storage::{EpochState, GcStorage, S3GcStorage};
use crate::gc::GcConfig;
use crate::logical_clocks::dot_version_vector::{self, CausalContext};
use crate::logical_clocks::dot_version_vector::DotVersionVector;
use crate::peers::peer_registry::PeerRegistry;
use crate::storage::s3_client::S3Client;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{info, warn};

const GC_INTENT_POLL_INTERVAL_MS: u64 = 200;

pub type MatrixClock = HashMap<NodeId, CausalContext>;

#[derive(Clone)]
pub struct GcCoordinator<S: GcStorage> {
    epoch: u64,
    storage: S,
    membership: Arc<DiscoveryMembershipProvider>,
    pub registry: PeerRegistry,
    pub config: GcConfig,
    matrix_clock: Option<MatrixClock>,
}

pub fn new_member_exists(old: &[NodeId], new: &[NodeId]) -> bool {
    let old_set: HashSet<&NodeId> = old.iter().collect();
    new.iter().any(|node| !old_set.contains(node))
}

impl GcCoordinator<S3GcStorage> {
    pub fn new(client: S3Client, config: GcConfig, registry: PeerRegistry) -> Self {
        let gc_replica = config.gc_replica;
        let storage_config = config.storage_config.clone();
        let bucket = storage_config.bucket.clone();
        let storage = S3GcStorage::new(client.clone(), storage_config);
        let membership = Arc::new(DiscoveryMembershipProvider { client, bucket });
        Self {
            epoch: 0,
            storage,
            membership,
            config,
            registry,
            matrix_clock: if gc_replica {
                Some(HashMap::new())
            } else {
                None
            },
        }
    }
}

impl<S: GcStorage> GcCoordinator<S> {
    pub fn initiate_interval(&self) -> Option<Duration> {
        self.config
            .gc_replica_config
            .as_ref()
            .map(|c| c.initiate_interval)
    }

    pub fn observe_interval(&self) -> Duration {
        self.config.observe_interval
    }

    pub fn get_knowledge_matrix(&self) -> Option<MatrixClock> {
        if !self.config.gc_replica {
            return None;
        }
        let mut knowledge_matrix = HashMap::new();
        let non_gc_peers = self.registry.get_all_non_gc_replicas();
        for node in &non_gc_peers {
            if self.matrix_clock.as_ref()?.contains_key(node) {
                knowledge_matrix
                    .insert(node.clone(), self.matrix_clock.as_ref()?.get(node)?.clone());
            }
        }
        Some(knowledge_matrix)
    }

    pub fn print_matrix_clock(&self) -> String {
        if !self.config.gc_replica {
            return "Matrix Clock: GC replica disabled".to_string();
        }
        format!("Matrix Clock: {:?}", self.matrix_clock.as_ref().unwrap())
    }

    pub fn update_matrix_clock(&mut self, knowledge_matrix: &MatrixClock) {
        if !self.config.gc_replica {
            return;
        }
        for (node, context) in knowledge_matrix {
            self.matrix_clock
                .as_mut()
                .unwrap()
                .insert(node.clone(), context.clone());
        }
    }

    pub fn remove_matrix_clock_row(&mut self, node_id: &NodeId) {
        if !self.config.gc_replica {
            return;
        }
        self.matrix_clock.as_mut().unwrap().remove(node_id);
    }

    pub fn remove_matrix_clock_column(&mut self, node_id: &NodeId) {
        if !self.config.gc_replica {
            return;
        }
        for context in self.matrix_clock.as_mut().unwrap().values_mut() {
            context.remove(node_id);
        }
    }

    pub async fn observe_epoch_change<C: DeltaCrdt>(
        &mut self,
        node_id: &NodeId,
        crdt: &mut C,
        current_clock: &mut DotVersionVector,
    ) -> anyhow::Result<()> {
        let epoch_state = self.storage.read_epoch_state().await?;
        let (current_epoch, v_stable, obsolete) = (epoch_state.epoch, epoch_state.v_stable, epoch_state.obsolete_dots);

        if current_epoch <= self.epoch {
            return Ok(());
        }

        let frontier = dot_version_vector::frontier_dvv(node_id, &v_stable);
        crdt.perform_gc(&frontier);
        self.epoch = current_epoch;

        for p in &obsolete {
            current_clock.remove(p);
            if self.config.gc_replica {
                self.remove_matrix_clock_row(p);
                self.remove_matrix_clock_column(p);
            }
        }

        Ok(())
    }

    pub async fn initiate_gc<C: DeltaCrdt>(
        &mut self,
        node_id: &NodeId,
        crdt: &mut C,
        dvv: &mut DotVersionVector,
    ) -> anyhow::Result<()> {
        let epoch_state = self.storage.read_epoch_state().await?;
        let (current_epoch, previous_v_stable) = (epoch_state.epoch, epoch_state.v_stable);

        if self.epoch != current_epoch {
            info!(epoch = current_epoch, "Not in latest epoch, aborting GC for previous epoch {}", self.epoch);
            return Ok(());
        }

        let n = current_epoch + 1;

        let m0 = self.membership.live_members().await?;
    
        if !self.storage.claim_gc_intent(n, node_id).await? {
            info!(
                epoch = n,
                "gc_intent already claimed by another replica, aborting GC initiation"
            );
            return Ok(());
        }
        
        let m1 = self.membership.live_members().await?;

        if new_member_exists(&m0, &m1) {
            if let Err(e) = self.storage.release_gc_intent(n).await {
                warn!(%e, epoch = n, "failed to release gc_intent after membership-change abort");
            }
            info!(
                epoch = n,
                "new member(s) detected during GC initiation, aborting GC"
            );
            return Ok(());
        }

        let final_dots = self.storage.read_final_dots().await?;
        let (mut v_stable, obsolete) = self.compute_stable_timestamp(node_id, &m1, final_dots, &dvv.effective_map()).await?;

        if dot_version_vector::vv_leq(&v_stable, &previous_v_stable) {
            info!(
                epoch = n,
                "stable timestamp did not advance since last GC, aborting GC"
            );
            if let Err(e) = self.storage.release_gc_intent(n).await {
                warn!(%e, epoch = n, "failed to release gc_intent after strict-progress abort");
            }
            return Ok(());
        }

        for p in &obsolete {
            self.remove_matrix_clock_row(p);
            self.remove_matrix_clock_column(p);
            dvv.remove(p);
            v_stable.remove(p);
        }

        let frontier = dot_version_vector::frontier_dvv(node_id, &v_stable);
        crdt.perform_gc(&frontier);
        let gc_state = crdt.full_state(dvv);
        let gc_state_payload = C::encode_delta(&gc_state);
        let epoch_state = EpochState {
            epoch: n,
            v_stable,
            obsolete_dots: obsolete,
            state_payload: gc_state_payload,
            initiator_clock: dvv.effective_map(),
        };

        self.storage.write_epoch_state(epoch_state).await?;
        self.epoch = n;

        if let Err(e) = self.storage.release_gc_intent(n).await {
            warn!(%e, epoch = n, "failed to release gc_intent after commit");
        }

        Ok(())
    } 

    pub async fn new_replica_bootstrap<C: DeltaCrdt>(
        &mut self,
        node_id: &NodeId,
        crdt: &mut C,
        dvv: &mut DotVersionVector,
    ) -> anyhow::Result<()> {
        let epoch_state = self.storage.read_epoch_state().await?;
        let (mut current_epoch, _, _, mut epoch_state_payload, mut initiator_clock) = 
                                (epoch_state.epoch, epoch_state.v_stable, epoch_state.obsolete_dots, epoch_state.state_payload, epoch_state.initiator_clock);

        let next_epoch = current_epoch + 1;

        if self.storage.read_gc_intent(next_epoch).await?.is_some() {
            loop {
                tokio::time::sleep(Duration::from_millis(GC_INTENT_POLL_INTERVAL_MS)).await;

                let new_epoch_state = self.storage.read_epoch_state().await?;
                let new_epoch = new_epoch_state.epoch;
                epoch_state_payload = new_epoch_state.state_payload;
                initiator_clock = new_epoch_state.initiator_clock;

                let intent = self.storage.read_gc_intent(next_epoch).await?;

                if new_epoch > current_epoch || intent.is_none() {
                    current_epoch = new_epoch;
                    break;
                }
            }
        }

        let frontier = dot_version_vector::frontier_dvv(node_id, &initiator_clock);
        crdt.merge_delta(&C::decode_delta(&epoch_state_payload)?);
        dvv.merge(&frontier);
        self.epoch = current_epoch;
        Ok(())
    }

    async fn compute_stable_timestamp(
        &self,
        node_id: &NodeId,
        members: &[NodeId],
        final_dots: HashMap<NodeId, Counter>,
        self_clock: &HashMap<NodeId, Counter>,
    ) -> anyhow::Result<(HashMap<NodeId, Counter>, HashSet<NodeId>)> {
        let mc = match &self.matrix_clock {
            Some(mc) => mc,
            Option::None => return Ok((HashMap::new(), HashSet::new())),
        };

        let members_set: HashSet<&NodeId> = members.iter().collect();

        let mut v_live: Option<HashMap<NodeId, Counter>> = None;
        for p in members {
            let ctx = if p == node_id {
                self_clock
            } else { 
                match mc.get(p) {
                    Some(ctx) => ctx,
                    Option::None => return Ok((HashMap::new(), HashSet::new())),
                }
            };

            v_live = Some(match v_live.take() {
                Option::None => ctx.clone(),
                Some(acc) => dot_version_vector::vv_meet(&acc, ctx),
            });
        }

        let mut v_live = v_live.unwrap_or_default();

        let non_members: HashSet<NodeId> = mc
            .keys()
            .chain(v_live.keys())
            .filter(|p| !members_set.contains(p))
            .cloned()
            .collect();

        let mut obsolete: HashSet<NodeId> = HashSet::new();
        let mut pending: HashSet<NodeId> = HashSet::new();

        for p in &non_members {
            let v_p = v_live.get(p);
            if final_dots.get(p) == v_p {
                obsolete.insert(p.clone());
            } else {
                pending.insert(p.clone());
            }
        }

        for p in &pending {
            if let Some(ctx) = mc.get(p) {
                v_live = dot_version_vector::vv_meet(&v_live, ctx);
            }
        }

        Ok((v_live, obsolete))
    }
}

#[async_trait]
trait MembershipProvider: Send + Sync {
    async fn live_members(&self) -> anyhow::Result<Vec<NodeId>>;
}

struct DiscoveryMembershipProvider {
    client: S3Client,
    bucket: String,
}

#[async_trait]
impl MembershipProvider for DiscoveryMembershipProvider {
    async fn live_members(&self) -> anyhow::Result<Vec<NodeId>> {
        let regs = list_live_node_ids(&self.client, &self.bucket)
            .await
            .context("failed to fetch live members from discovery")?;
        let mut members: Vec<NodeId> = regs;
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
