#![allow(dead_code)]
//! Generic delta-CRDT replication engine.
//!
//! Owns a `DotVersionVector` + a `C: DeltaCrdt`.
//! All DVV operations happen here; the CRDT only sees typed `Dot` values.
//! GC/membership concerns are delegated to dedicated layers.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::metric;
use crate::common::{Counter, NodeId};
use crate::crdt::{DeltaContext, DeltaCrdt};
use crate::network::dissemination::SharedDissemination;
use crate::gc::{GcConfig, GcCoordinator, coordinator::GcInitiationAbortReason};
use crate::logical_clocks::dot_version_vector::DotVersionVector;
use crate::peers::peer_registry::PeerRegistry;
use crate::storage::s3_client::S3Client;
use crate::engine::PullResponse;
use crate::engine::engine_inner::EngineInner;

pub enum CrdtEngineRequest<C: DeltaCrdt> {
    DoPullRound,
    ClientOperation(C::Op),
    PrintState(tokio::sync::oneshot::Sender<String>),
    PrintInternals(tokio::sync::oneshot::Sender<String>),
    PrintMatrix(tokio::sync::oneshot::Sender<String>),
    GetRandomElement(tokio::sync::oneshot::Sender<String>),
    DeltaRequest(NodeId, String, HashMap<NodeId, u64>),
    DeltaResponse(NodeId, String, Vec<u8>, Option<HashMap<NodeId, HashMap<NodeId, u64>>>),
    DeregistrationState(tokio::sync::oneshot::Sender<(Counter, Vec<u8>)>),
    InitiateGc,
    ObserveEpochChange,
}


// ── Public engine ───────────────────────────────────────────────────────
pub struct CrdtEngine<C: DeltaCrdt> {
    node_id: NodeId,
    crdt_id: String,
    dissemination: SharedDissemination<C>,
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
            crdt,
            dvv,
            gc: GcCoordinator::new(client, config, peer_registry),
        };
        CrdtEngine {
            node_id,
            crdt_id,
            dissemination,
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
                    self.dissemination.do_pull_round(&self.node_id, 
                        &self.crdt_id,
                        inner.gc.config.gc_replica,
                        &inner.dvv.effective_map());
                },
                CrdtEngineRequest::ClientOperation(op) => {
                    self.client_operation(op).await;
                },
                CrdtEngineRequest::GetRandomElement(resp_tx) => {
                    let random_element = self.get_random_element().await;
                    let element = random_element.unwrap_or_else(|| "error: empty crdt".to_string());
                    let _ = resp_tx.send(element);
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
        let initiate_interval = inner.gc.initiate_interval();
        let observe_interval = inner.gc.observe_interval();

        let mut handles = Vec::new();

        {
            let eng_tx = engine_tx.clone();
            if let Some(interval) = initiate_interval {
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
            handles.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(observe_interval);
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
        let (gc_replica, knowledge) = {
            let mut inner = self.inner.lock().await;
            inner.client_operation(op);
            (
                inner.gc.config.gc_replica,
                inner.dvv.effective_map(),
            )
        };
        // Advertise the updated VV outside the lock so peers can compute what to send us.
        self.dissemination
            .push_version_vector(&self.node_id, &self.crdt_id, gc_replica, &knowledge)
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
        if crdt_id != self.crdt_id {
            warn!(
                expected = %self.crdt_id,
                received = %crdt_id,
                "ignoring delta for unknown CRDT"
            );
            return;
        }

        debug!(%from_node, %crdt_id, "merging remote delta");
        let (gc_replica, knowledge) = {
            let mut inner = self.inner.lock().await;
            inner.handle_remote_delta(&payload);
            inner
                .gc
                .update_matrix_clock(&knowledge_matrix.unwrap_or_default());
            (
                inner.gc.config.gc_replica,
                inner.dvv.effective_map(),
            )
        };
        // Merges are local state changes; push/hybrid strategies advertise
        // the updated VV so other peers can react.
        self.dissemination
            .on_post_merge(&self.node_id, &self.crdt_id, gc_replica, &knowledge)
            .await;
    }

    /// Called when a `CrdtPullRequest` message arrives from the network.
    async fn server_pull_request(
        &self,
        from_node: NodeId,
        crdt_id: String,
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

        let mut inner = self.inner.lock().await;
        let PullResponse { delta_payload, knowledge_matrix, our_knowledge_request } = inner.server_pull_request(&from_node, knowledge);

        // Route both actions through the dissemination layer. The strategy
        // decides whether to piggyback the VV request on the delta message.
        self.dissemination.respond_to_pull_request(
            &from_node,
            &self.crdt_id,
            &self.node_id,
            delta_payload,
            inner.gc.config.gc_replica,
            knowledge_matrix,
            our_knowledge_request,
        );
    }

    async fn observe_epoch_change(&self) -> anyhow::Result<bool> {
        let start_millis = std::time::Instant::now();
        let mut inner = self.inner.lock().await;
        let EngineInner {
            gc,
            crdt,
            dvv,
            ..
        } = &mut *inner;
        let result = gc.observe_epoch_change(&self.node_id, crdt, dvv)
            .await;

        let epoch_changed = match &result {
            Ok(changed) => *changed,
            Err(_) => false,
        };
        metric!(event = "gc_observe_epoch_change",
            duration_millis = start_millis.elapsed().as_millis() as u64,
            epoch_changed = epoch_changed);
        result
    }

    async fn initiate_gc(&self) -> anyhow::Result<Option<GcInitiationAbortReason>> {
        let start_millis = std::time::Instant::now();
        let mut inner = self.inner.lock().await;
        let EngineInner {
            gc,
            crdt,
            dvv,
            ..
        } = &mut *inner;
        let result = gc.initiate_gc(&self.node_id, crdt, dvv)
            .await;
        let (abort, abort_reason) = match &result {
            Ok(reason) => match reason {
                Some(r) => (true, format!("{:?}", r)),
                Option::None => (false, "None".to_string()),
            },
            Err(_) => (true, "None".to_string())
        };

        metric!(event = "gc_initiate",
            duration_millis = start_millis.elapsed().as_millis() as u64,
            abort = abort,
            abort_reason = abort_reason);
        result
    }

    async fn new_replica_bootstrap(&self) -> anyhow::Result<()> {
        let start_millis = std::time::Instant::now();
        let mut inner = self.inner.lock().await;
        let EngineInner {
            gc,
            crdt,
            dvv,
            ..
        } = &mut *inner;
        let result = gc
            .new_replica_bootstrap(&self.node_id, crdt, dvv)
            .await;
        metric!(event = "new_replica_bootstrap", duration_millis = start_millis.elapsed().as_millis() as u64);
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
