use core::mem::drop;
use core::option::Option::{self, None, Some};
use core::result::Result::Ok;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::Duration;
use std::collections::hash_map::Entry;

use crate::metric;
use crate::common::{Counter, NodeId};
use crate::crdt::DeltaCrdt;
use crate::discovery;
use crate::gc::storage::{EpochMetadata, EpochState, GcStorage, S3GcStorage};
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

#[derive(Clone, Debug)]
pub enum GcInitiationAbortReason {
    MembershipChange,
    NoProgress,
    ConcurrentInitiator,
}

pub struct GcCoordinator<S: GcStorage> {
    pub epoch: u64,
    storage: S,
    membership: Arc<DiscoveryMembershipProvider>,
    pub registry: PeerRegistry,
    pub config: GcConfig,
    matrix_clock: Option<RwLock<MatrixClock>>,
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
                Some(RwLock::new(HashMap::new()))
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

    pub async fn log_metrics(&self, dissemination_round: usize, node_id: &NodeId, dvv: &DotVersionVector) -> anyhow::Result<()> {
        if !self.config.gc_replica {
            return Ok(());
        }

        let (matrix_clock_size_bytes, v_stable) = if let Some(mc) = &self.matrix_clock {
            let mc = mc.read().await;
            let bytes = mc.iter()
                .map(|(node, ctx)| {
                    node.len() + ctx.keys().map(|n| n.len() + 8).sum::<usize>()
                })
                .sum::<usize>();

            let m1 = self.membership.live_members().await?;
            let final_dots = self.storage.read_final_dots().await?;
            let (v_s, _) = self.compute_stable_timestamp(node_id, &m1, final_dots, &dvv.effective_map()).await?;
            (bytes, v_s)
        } else {
            (0, HashMap::new())
        };

        metric!(
            node_id = node_id,
            event = "gc_coordinator_metrics",
            dissemination_round = dissemination_round,
            v_stable = format!("{:?}", v_stable),
            matrix_clock_size_bytes = matrix_clock_size_bytes
        );

        Ok(())
    }

    pub async fn get_knowledge_matrix(&self, node_id: &NodeId) -> Option<MatrixClock> {
        if !self.config.gc_replica {
            return None;
        }
        let mut knowledge_matrix = HashMap::new();
        let non_gc_peers = self.registry.get_all_non_gc_replicas();
        let matrix_clock = self.matrix_clock.as_ref()?.read().await;
        for node in &non_gc_peers {
            if matrix_clock.contains_key(node) {
                knowledge_matrix
                    .insert(node.clone(), matrix_clock.get(node)?.clone());
            }
        }
        metric!(
            node_id = node_id,
            event = "knowledge_matrix",
            knowledge_matrix = format!("{:?}", knowledge_matrix.keys().cloned().collect::<Vec<_>>().join(","))
        );
        Some(knowledge_matrix)
    }

    pub async fn print_matrix_clock(&self) -> String {
        if !self.config.gc_replica {
            return "Matrix Clock: GC replica disabled".to_string();
        }
        let matrix_clock = self.matrix_clock.as_ref().unwrap().read().await;
        format!("Matrix Clock: {:?}", matrix_clock)
    }

    pub async fn update_matrix_clock(&mut self, knowledge_matrix: &MatrixClock) {
        if !self.config.gc_replica {
            return;
        }
        let mut matrix_clock = self.matrix_clock.as_mut().unwrap().write().await;
        for (node, incoming_context) in knowledge_matrix {
            match matrix_clock.entry(node.clone()) {
                Entry::Occupied(mut entry) => {
                    let joined = dot_version_vector::vv_join(incoming_context, entry.get());
                    entry.insert(joined);
                }
                Entry::Vacant(entry) => {
                    entry.insert(incoming_context.clone());
                }
            }
        }
    }

    pub async fn remove_matrix_clock_row(&mut self, node_id: &NodeId) {
        if !self.config.gc_replica {
            return;
        }
        let mut matrix_clock = self.matrix_clock.as_mut().unwrap().write().await;
        matrix_clock.remove(node_id);
    }

    pub async fn remove_matrix_clock_column(&mut self, node_id: &NodeId) {
        if !self.config.gc_replica {
            return;
        }
        let mut matrix_clock = self.matrix_clock.as_mut().unwrap().write().await;
        for context in matrix_clock.values_mut() {
            context.remove(node_id);
        }
    }

    pub async fn observe_epoch_change<C: DeltaCrdt>(
        &mut self,
        node_id: &NodeId,
        crdt: &mut C,
        dvv: &mut DotVersionVector,
    ) -> anyhow::Result<bool> {
        let epoch_state = self.storage.read_epoch_metadata().await?;
        let (current_epoch, v_stable, obsolete) = (epoch_state.epoch, epoch_state.v_stable, epoch_state.obsolete_dots);

        if current_epoch <= self.epoch {
            return Ok(false);
        }

        let frontier = dot_version_vector::frontier_dvv(node_id, &v_stable);
        crdt.perform_gc(&frontier);
        self.epoch = current_epoch;

        for p in &obsolete {
            dvv.remove(p);
            if self.config.gc_replica {
                self.remove_matrix_clock_row(p).await;
                self.remove_matrix_clock_column(p).await;
            }
        }

        Ok(true)
    }

    pub async fn initiate_gc<C: DeltaCrdt>(
        &mut self,
        node_id: &NodeId,
        crdt: &mut C,
        dvv: &mut DotVersionVector,
    ) -> anyhow::Result<Option<GcInitiationAbortReason>> {
        let epoch_state = self.storage.read_epoch_metadata().await?;
        let (current_epoch, previous_v_stable) = (epoch_state.epoch, epoch_state.v_stable);

        let n = current_epoch + 1;

        let m0 = self.membership.live_members().await?;
    
        if !self.storage.claim_gc_intent(n, node_id).await? {
            info!(
                epoch = n,
                "gc_intent already claimed by another replica, aborting GC initiation"
            );
            return Ok(Some(GcInitiationAbortReason::ConcurrentInitiator));
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
            return Ok(Some(GcInitiationAbortReason::MembershipChange));
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
            return Ok(Some(GcInitiationAbortReason::NoProgress));
        }

        for p in &obsolete {
            self.remove_matrix_clock_row(p).await;
            self.remove_matrix_clock_column(p).await;
            dvv.remove(p);
            v_stable.remove(p);
        }

        let gc_state_payload = {
            let frontier = dot_version_vector::frontier_dvv(node_id, &v_stable);
            crdt.perform_gc(&frontier);
            let gc_state = crdt.full_state(dvv);
            C::encode_delta(&gc_state)
        };  

        let epoch_metadata = EpochMetadata {
            epoch: n,
            v_stable: v_stable.clone(),
            obsolete_dots: obsolete.clone(),
        };
        let epoch_state = EpochState {
            epoch: n,
            state_payload: gc_state_payload,
            initiator_clock: dvv.effective_map(),
        };

        self.storage.write_epoch_state(&epoch_state).await?;
        self.storage.write_epoch_metadata(&epoch_metadata).await?;
        drop(epoch_state);
        self.epoch = n;

        if let Err(e) = self.storage.release_gc_intent(n).await {
            warn!(%e, epoch = n, "failed to release gc_intent after commit");
        }

        Ok(None)
    } 

    pub async fn new_replica_bootstrap<C: DeltaCrdt>(
        &mut self,
        node_id: &NodeId,
        crdt: &mut C,
        dvv: &mut DotVersionVector,
    ) -> anyhow::Result<()> {
        let epoch_metadata = self.storage.read_epoch_metadata().await?;
        let mut current_epoch = epoch_metadata.epoch;
        let next_epoch = current_epoch + 1;

        if self.storage.read_gc_intent(next_epoch).await?.is_some() {
            loop {
                tokio::time::sleep(Duration::from_millis(GC_INTENT_POLL_INTERVAL_MS)).await;
                let new_epoch_metadata = self.storage.read_epoch_metadata().await?;
                let new_epoch = new_epoch_metadata.epoch;
                let intent = self.storage.read_gc_intent(next_epoch).await?;
                if new_epoch > current_epoch || intent.is_none() {
                    current_epoch = new_epoch;
                    break;
                }
            }
        }

        // epoch_metadata and epoch_state are written separately, so after settling
        // on `current_epoch` we need to make sure epoch_state has caught up.
        let epoch_state = loop {
            let epoch_state = self.storage.read_epoch_state().await?;
            if epoch_state.epoch == current_epoch {
                break epoch_state;
            }
            tokio::time::sleep(Duration::from_millis(GC_INTENT_POLL_INTERVAL_MS)).await;
        };

        let frontier = dot_version_vector::frontier_dvv(node_id, &epoch_state.initiator_clock);
        crdt.merge_delta(&C::decode_delta(&epoch_state.state_payload)?);
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
            Some(mc) => mc.read().await,
            Option::None => return Ok((HashMap::new(), HashSet::new())),
        };

        let members_set: HashSet<&NodeId> = members.iter().collect();

        let mut v_live: Option<HashMap<NodeId, Counter>> = None;
        let mut v_merge: Option<HashMap<NodeId, Counter>> = None;

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

            v_merge = Some(match v_merge.take() {
                Option::None => ctx.clone(),
                Some(acc) => dot_version_vector::vv_join(&acc, ctx),
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
        let regs = discovery::list_live_node_ids(&self.client, &self.bucket)
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
