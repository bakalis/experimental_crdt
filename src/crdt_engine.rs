//! Generic delta-CRDT replication engine.
//!
//! Owns a `DotVersionVector` + a `C: DeltaCrdt`.
//! All DVV operations happen here; the CRDT only sees typed `Dot` values.
//! GC/membership concerns are delegated to dedicated layers.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::common::NodeId;
use crate::crdt::{DeltaContext, DeltaCrdt};
use crate::dissemination::{PullRoundEngine, SharedDissemination};
use crate::gc::{GcConfig, GcCoordinator};
use crate::gc::storage::S3GcStorage;
use crate::logical_clocks::dot_version_vector::{Dot, DotVersionVector};
use crate::s3_client::S3Client;

// ── Inner state (behind the mutex) ─────────────────────────────────────

struct EngineInner<C: DeltaCrdt> {
    node_id: NodeId,
    crdt_id: String,
    crdt: C,
    dvv: DotVersionVector,
    dissemination: SharedDissemination,
    gc: Option<GcCoordinator<S3GcStorage>>,
}

impl<C: DeltaCrdt> EngineInner<C> {
    fn client_operation(&mut self, op: C::Op) {
        debug!(node_id = %self.node_id, crdt_id = %self.crdt_id, "applying local op");

        // 1. Mint a new dot.
        self.dvv.event();
        let dot = Dot::new(self.dvv.dot.node_id.clone(), self.dvv.dot.counter);

        // 2. Apply to CRDT.
        self.crdt.apply_local(dot, op);
    }

    fn server_delta(&mut self, from_node: &NodeId, crdt_id: &str, payload: &[u8]) {
        if crdt_id != self.crdt_id {
            warn!(
                expected = %self.crdt_id,
                received = %crdt_id,
                "ignoring delta for unknown CRDT"
            );
            return;
        }
        debug!(%from_node, %crdt_id, "merging remote delta");
        self.handle_remote_delta(payload);
    }

    fn server_pull_request(&mut self, from_node: &NodeId, crdt_id: &str, knowledge: HashMap<NodeId, u64>) {
        if crdt_id != self.crdt_id {
            warn!(
                expected = %self.crdt_id,
                received = %crdt_id,
                "ignoring pull request for unknown CRDT"
            );
            return;
        }
        self.handle_pull_request(from_node, knowledge);
    }

    fn handle_remote_delta(&mut self, payload: &[u8]) {
        let delta = match C::decode_delta(payload) {
            Ok(d) => d,
            Err(e) => {
                warn!(%e, "failed to decode remote delta");
                return;
            }
        };

        // Merge CRDT state.
        self.crdt.merge_delta(&delta);

        // Merge causal context into DVV.
        let (ctx, node, counter) = delta.causal_context();
        let remote_dvv = DotVersionVector {
            dot: Dot::new(node, counter),
            context: ctx,
        };
        self.dvv.merge(&remote_dvv);
    }

    fn handle_pull_request(&mut self, from_node: &NodeId, remote_knowledge: HashMap<NodeId, u64>) {
        debug!(%from_node, "received version vector");

        let our_knowledge = self.dvv.effective_map();

        // Compute delta if we are ahead of the remote in any dimension.
        let dvv_delta = self.dvv.delta_since(&remote_knowledge);
        let delta_payload = if dvv_delta.dot.counter != 0 || !dvv_delta.context.is_empty() {
            let state = self.crdt.delta_since(&remote_knowledge, &self.dvv);
            Some(C::encode_delta(&state))
        } else {
            None
        };

        // Compute our VV request if the remote is ahead of us in any dimension.
        let we_are_fully_ahead = remote_knowledge
            .iter()
            .all(|(node, &remote_ctr)| our_knowledge.get(node).copied().unwrap_or(0) >= remote_ctr);
        let our_knowledge_request = if !we_are_fully_ahead { Some(our_knowledge) } else { None };

        // Route both actions through the dissemination layer. The strategy
        // decides whether to piggyback the VV request on the delta message.
        self.dissemination.respond_to_pull_request(
            from_node,
            &self.crdt_id,
            &self.node_id,
            delta_payload,
            our_knowledge_request,
        );

        debug!(%from_node, "version vector exchange complete");
    }

    fn do_pull_round(&self) {
        let our_knowledge = self.dvv.effective_map();
        self.dissemination.do_pull_round(&self.node_id, &self.crdt_id, &our_knowledge);
        debug!("pull round complete");
    }
}

// ── Public engine ───────────────────────────────────────────────────────

pub struct CrdtEngine<C: DeltaCrdt> {
    inner: Arc<Mutex<EngineInner<C>>>,
}

impl<C: DeltaCrdt> Clone for CrdtEngine<C> {
    fn clone(&self) -> Self {
        CrdtEngine {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C: DeltaCrdt> CrdtEngine<C> {
    pub fn new(
        node_id: NodeId,
        crdt_id: String,
        crdt: C,
        dissemination: SharedDissemination,
        gc: Option<(S3Client, GcConfig)>,
    ) -> Self {
        let dvv = DotVersionVector::new(node_id.clone());
        let inner = EngineInner {
            node_id,
            crdt_id,
            crdt,
            dvv,
            dissemination,
            gc: gc.map(|(client, config)| GcCoordinator::new(client, config)),
        };
        CrdtEngine {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn get_random_element(&self) -> Option<String> {
        let inner = self.inner.lock().await;
        inner.crdt.get_random_element()
    }

    pub async fn print_internals(&self) -> String {
        let inner = self.inner.lock().await;
        inner.crdt.print_internals()
    }

    pub async fn print_state(&self) -> String {
        let inner = self.inner.lock().await;
        inner.crdt.print_state()
    }

    /// Called when a client wants to perform a local write.
    pub async fn client_operation(&self, op: C::Op) {
        let (dissemination, node_id, crdt_id, knowledge) = {
            let mut inner = self.inner.lock().await;
            inner.client_operation(op);
            (
                Arc::clone(&inner.dissemination),
                inner.node_id.clone(),
                inner.crdt_id.clone(),
                inner.dvv.effective_map(),
            )
        };
        // Advertise the updated VV outside the lock so peers can compute what to send us.
        dissemination.push_version_vector(&node_id, &crdt_id, &knowledge).await;
    }

    /// Called when a `CrdtOp` (delta) message arrives from the network.
    pub async fn server_delta(
        &self,
        from_node: NodeId,
        crdt_id: String,
        payload: Vec<u8>,
    ) {
        let (dissemination, node_id, crdt_id2, knowledge) = {
            let mut inner = self.inner.lock().await;
            inner.server_delta(&from_node, &crdt_id, &payload);
            (
                Arc::clone(&inner.dissemination),
                inner.node_id.clone(),
                inner.crdt_id.clone(),
                inner.dvv.effective_map(),
            )
        };
        // Merges are local state changes; push/hybrid strategies advertise
        // the updated VV so other peers can react.
        dissemination.on_post_merge(&node_id, &crdt_id2, &knowledge).await;
    }

    /// Called when a `CrdtPullRequest` message arrives from the network.
    pub async fn server_pull_request(
        &self,
        from_node: NodeId,
        crdt_id: String,
        knowledge: HashMap<NodeId, u64>,
    ) {
        self.inner
            .lock()
            .await
            .server_pull_request(&from_node, &crdt_id, knowledge);
    }

    pub async fn publish_clock(&self) -> anyhow::Result<()> {
        let inner = self.inner.lock().await;
        let Some(gc) = inner.gc.clone() else {
            return Ok(());
        };
        gc.publish_clock(&inner.node_id, &inner.dvv.effective_map()).await
    }

    pub async fn observe_epoch_change(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        let Some(gc) = inner.gc.clone() else {
            return Ok(());
        };
        let node_id = inner.node_id.clone();
        let clock = inner.dvv.effective_map();
        gc.observe_epoch_change(&node_id, &mut inner.crdt, &clock).await
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        let inner = self.inner.lock().await;
        let Some(gc) = inner.gc.clone() else {
            return Ok(());
        };
        gc.cleanup().await
    }

    pub async fn initiate_gc(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        let Some(gc) = inner.gc.clone() else {
            return Ok(());
        };
        let node_id = inner.node_id.clone();
        let dvv_snapshot = inner.dvv.clone();
        gc.initiate_gc(&node_id, &mut inner.crdt, &dvv_snapshot).await
    }

    pub async fn new_replica_bootstrap(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        let Some(gc) = inner.gc.clone() else {
            return Ok(());
        };
        let node_id = inner.node_id.clone();
        let mut dvv = std::mem::replace(&mut inner.dvv, DotVersionVector::new(node_id.clone()));
        let result = gc
            .new_replica_bootstrap(&node_id, &mut inner.crdt, &mut dvv)
            .await;
        inner.dvv = dvv;
        result
    }

    pub async fn remove_gc_clock(&self) -> anyhow::Result<()> {
        let inner = self.inner.lock().await;
        let Some(gc) = inner.gc.clone() else {
            return Ok(());
        };
        gc.remove_clock(&inner.node_id).await
    }

    pub async fn start_gc_loops(&self) -> Vec<JoinHandle<()>> {
        let inner = self.inner.lock().await;
        let Some(gc) = inner.gc.clone() else {
            return vec![];
        };
        drop(inner);

        let mut handles = Vec::new();

        {
            let eng = self.clone();
            let interval = gc.initiate_interval();
            handles.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    if let Err(e) = eng.initiate_gc().await {
                        warn!(%e, "initiate_gc failed");
                    }
                }
            }));
        }
        {
            let eng = self.clone();
            let interval = gc.observe_interval();
            handles.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    if let Err(e) = eng.observe_epoch_change().await {
                        warn!(%e, "observe_epoch_change failed");
                    }
                    if let Err(e) = eng.publish_clock().await {
                        warn!(%e, "publish_clock failed");
                    }
                }
            }));
        }
        {
            let eng = self.clone();
            let interval = gc.cleanup_interval();
            handles.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    if let Err(e) = eng.cleanup().await {
                        warn!(%e, "cleanup failed");
                    }
                }
            }));
        }

        handles
    }
}

#[async_trait]
impl<C: DeltaCrdt> PullRoundEngine for CrdtEngine<C> {
    async fn do_pull_round(&self) {
        self.inner.lock().await.do_pull_round();
    }
}
