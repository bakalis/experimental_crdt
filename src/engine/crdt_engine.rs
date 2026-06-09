#![allow(dead_code)]
//! Generic delta-CRDT replication engine.
//!
//! Owns a `DotVersionVector` + a `C: DeltaCrdt`.
//! All DVV operations happen here; the CRDT only sees typed `Dot` values.
//! GC/membership concerns are delegated to dedicated layers.

use core::result::Result::Ok;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::common::{Counter, NodeId};
use crate::crdt::{DeltaContext, DeltaCrdt};
use crate::network::dissemination::SharedDissemination;
use crate::gc::storage::S3GcStorage;
use crate::gc::{GcConfig, GcCoordinator};
use crate::logical_clocks::dot_version_vector::{Dot, DotVersionVector};
use crate::peers::peer_registry::PeerRegistry;
use crate::storage::s3_client::S3Client;

pub enum CrdtEngineRequest<C: DeltaCrdt> {
    DoPullRound,
    ClientOperation(C::Op),
    PrintState(tokio::sync::oneshot::Sender<String>),
    PrintInternals(tokio::sync::oneshot::Sender<String>),
    PrintMatrix(tokio::sync::oneshot::Sender<String>),
    DeltaRequest(NodeId, String, HashMap<NodeId, u64>),
    DeltaResponse(NodeId, String, Vec<u8>, Option<HashMap<NodeId, HashMap<NodeId, u64>>>),
    DeregistrationState(tokio::sync::oneshot::Sender<(Counter, Vec<u8>)>),
    InitiateGc,
    ObserveEpochChange,
}

// ── Inner state (behind the mutex) ─────────────────────────────────────

struct EngineInner<C: DeltaCrdt> {
    node_id: NodeId,
    crdt_id: String,
    crdt: C,
    dvv: DotVersionVector,
    dissemination: SharedDissemination<C>,
    gc: GcCoordinator<S3GcStorage>,
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

    fn server_pull_request(
        &mut self,
        from_node: &NodeId,
        crdt_id: &str,
        knowledge: HashMap<NodeId, u64>,
    ) {
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
        let mut knowledge = HashMap::new();
        knowledge.insert(from_node.clone(), remote_knowledge.clone());
        self.gc.update_matrix_clock(&knowledge);

        let our_knowledge = self.dvv.effective_map();
        let communication_between_gc_replicas = self.gc.config.gc_replica && self
            .gc
            .registry
            .get(from_node)
            .map(|(_, gc_replica, _)| gc_replica)
            .unwrap_or(false);

        // Compute delta if we are ahead of the remote in any dimension.
        let dvv_delta = self.dvv.delta_since(&remote_knowledge);
        let delta_payload = if communication_between_gc_replicas || dvv_delta.dot.counter != 0 || !dvv_delta.context.is_empty() {
            let state = self.crdt.delta_since(&remote_knowledge, &self.dvv);
            Some(C::encode_delta(&state))
        } else {
            None
        };

        // Compute our VV request if the remote is ahead of us in any dimension.
        let we_are_fully_ahead = remote_knowledge
            .iter()
            .all(|(node, &remote_ctr)| our_knowledge.get(node).copied().unwrap_or(0) >= remote_ctr);
        let our_knowledge_request = if !we_are_fully_ahead {
            Some(our_knowledge)
        } else {
            None
        };

        // Include the knowledge matrix if the requester is a GC replica, so they can make informed decisions about what to GC.
        let knowledge_matrix = if self
            .gc
            .registry
            .get(from_node)
            .map(|(_, gc_replica, _)| gc_replica)
            .unwrap_or(false)
        {
            self.gc.get_knowledge_matrix()
        } else {
            None
        };

        // Route both actions through the dissemination layer. The strategy
        // decides whether to piggyback the VV request on the delta message.
        self.dissemination.respond_to_pull_request(
            from_node,
            &self.crdt_id,
            &self.node_id,
            delta_payload,
            self.gc.config.gc_replica,
            knowledge_matrix,
            our_knowledge_request,
        );

        debug!(%from_node, "version vector exchange complete");
    }

    fn do_pull_round(&self) {
        let our_knowledge = self.dvv.effective_map();
        self.dissemination.do_pull_round(
            &self.node_id,
            &self.crdt_id,
            self.gc.config.gc_replica,
            &our_knowledge,
        );
        debug!("pull round complete");
    }
}

// ── Public engine ───────────────────────────────────────────────────────

pub struct CrdtEngine<C: DeltaCrdt> {
    inner: Arc<Mutex<EngineInner<C>>>,
    rx: tokio::sync::mpsc::Receiver<CrdtEngineRequest<C>>,
}

impl<C: DeltaCrdt> CrdtEngine<C> {
    pub fn new(
        node_id: NodeId,
        crdt_id: String,
        crdt: C,
        peer_registry: PeerRegistry,
        dissemination: SharedDissemination<C>,
        gc: (S3Client, GcConfig),
        rx: tokio::sync::mpsc::Receiver<CrdtEngineRequest<C>>,
    ) -> Self {
        let dvv = DotVersionVector::new(node_id.clone());
        let (client, config) = gc;
        let inner = EngineInner {
            node_id,
            crdt_id,
            crdt,
            dvv,
            dissemination,
            gc: GcCoordinator::new(client, config, peer_registry),
        };
        CrdtEngine {
            inner: Arc::new(Mutex::new(inner)),
            rx
        }
    }

    pub async fn run(&mut self) {
        let _ = self.new_replica_bootstrap().await;
        
        while let Some(request) = self.rx.recv().await {
            match request {
                CrdtEngineRequest::DoPullRound => {
                    let inner = self.inner.lock().await;
                    inner.do_pull_round();
                },
                CrdtEngineRequest::ClientOperation(op) => {
                    self.client_operation(op).await;
                },
                CrdtEngineRequest::PrintState(resp_tx) => {
                    let state = self.print_state().await;
                    let _ = resp_tx.send(state);
                },
                CrdtEngineRequest::PrintInternals(resp_tx) => {
                    let internals = self.print_internals().await;
                    let _ = resp_tx.send(internals);
                },
                CrdtEngineRequest::PrintMatrix(resp_tx) => {
                    let matrix = self.print_matrix_clock().await;
                    let _ = resp_tx.send(matrix);
                },
                CrdtEngineRequest::DeltaRequest(from_node, crdt_id, knowledge_matrix) => {
                    self.server_pull_request(from_node, crdt_id, knowledge_matrix).await;
                },
                CrdtEngineRequest::DeltaResponse(from_node, crdt_id, payload, knowledge_matrix) => {
                    self.server_delta(from_node, crdt_id, payload, knowledge_matrix).await;
                },
                CrdtEngineRequest::DeregistrationState(resp_tx) => {
                    let state = self.get_final_state_for_deregistration().await;
                    let _ = resp_tx.send(state);
                }
                CrdtEngineRequest::InitiateGc => {
                    if let Err(e) = self.initiate_gc().await {
                        warn!(%e, "initiate_gc failed");
                    }
                },
                CrdtEngineRequest::ObserveEpochChange => {
                    if let Err(e) = self.observe_epoch_change().await {
                        warn!(%e, "observe_epoch_change failed");
                    }
                }
            }
        }
    }

    pub async fn start_gc_loops(&self, engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<C>>) -> Vec<JoinHandle<()>> {
        let inner = self.inner.lock().await;
        let gc = inner.gc.clone();
        drop(inner);

        let mut handles = Vec::new();

        {
            let eng_tx = engine_tx.clone();
            if let Some(interval) = gc.initiate_interval() {
                handles.push(tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    loop {
                        ticker.tick().await;
                        let _ = eng_tx.send(CrdtEngineRequest::InitiateGc).await;
                    }
                }));
            }
        }

        {
            let eng_tx = engine_tx.clone();
            let interval = gc.observe_interval();
            handles.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    let _ = eng_tx.send(CrdtEngineRequest::ObserveEpochChange).await;
                }
            }));
        }
        handles 
    }

    async fn get_random_element(&self) -> Option<String> {
        let inner = self.inner.lock().await;
        inner.crdt.get_random_element()
    }

    async fn print_internals(&self) -> String {
        let inner = self.inner.lock().await;
        inner.crdt.print_internals()
    }

    async fn print_matrix_clock(&self) -> String {
        let inner = self.inner.lock().await;
        inner.gc.print_matrix_clock()
    }

    async fn print_state(&self) -> String {
        let inner = self.inner.lock().await;
        inner.crdt.print_state()
    }

    /// Called when a client wants to perform a local write.
    async fn client_operation(&self, op: C::Op) {
        let (dissemination, node_id, crdt_id, gc_replica, knowledge) = {
            let mut inner = self.inner.lock().await;
            inner.client_operation(op);
            (
                Arc::clone(&inner.dissemination),
                inner.node_id.clone(),
                inner.crdt_id.clone(),
                inner.gc.config.gc_replica,
                inner.dvv.effective_map(),
            )
        };
        // Advertise the updated VV outside the lock so peers can compute what to send us.
        dissemination
            .push_version_vector(&node_id, &crdt_id, gc_replica, &knowledge)
            .await;
    }

    /// Called when a `CrdtOp` (delta) message arrives from the network.
    async fn server_delta(
        &self,
        from_node: NodeId,
        crdt_id: String,
        payload: Vec<u8>,
        knowledge_matrix: Option<HashMap<NodeId, HashMap<NodeId, u64>>>,
    ) {
        let (dissemination, node_id, crdt_id, gc_replica, knowledge) = {
            let mut inner = self.inner.lock().await;
            inner.server_delta(&from_node, &crdt_id, &payload);
            inner
                .gc
                .update_matrix_clock(&knowledge_matrix.unwrap_or_default());
            (
                Arc::clone(&inner.dissemination),
                inner.node_id.clone(),
                inner.crdt_id.clone(),
                inner.gc.config.gc_replica,
                inner.dvv.effective_map(),
            )
        };
        // Merges are local state changes; push/hybrid strategies advertise
        // the updated VV so other peers can react.
        dissemination
            .on_post_merge(&node_id, &crdt_id, gc_replica, &knowledge)
            .await;
    }

    /// Called when a `CrdtPullRequest` message arrives from the network.
    async fn server_pull_request(
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

    async fn observe_epoch_change(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        let node_id = inner.node_id.clone();
        let EngineInner {
            gc,
            crdt,
            dvv,
            ..
        } = &mut *inner;
        gc.observe_epoch_change(&node_id, crdt, dvv)
            .await
    }

    async fn initiate_gc(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        let node_id = inner.node_id.clone();
        let EngineInner {
            gc,
            crdt,
            dvv,
            ..
        } = &mut *inner;
        gc.initiate_gc(&node_id, crdt, dvv)
            .await
    }

    async fn new_replica_bootstrap(&self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().await;
        let node_id = inner.node_id.clone();
        let EngineInner {
            gc,
            crdt,
            dvv,
            ..
        } = &mut *inner;
        let result = gc
            .new_replica_bootstrap(&node_id, crdt, dvv)
            .await;
        result 
    }

    async fn get_final_state_for_deregistration(&self) -> (Counter, Vec<u8>) {
        let inner = self.inner.lock().await;
        let final_state_delta = inner.crdt.full_state(&inner.dvv);
        let final_dot: Counter = final_state_delta.causal_context().2;
        let final_state = C::encode_delta(&final_state_delta);
        (final_dot, final_state)
    }
}
