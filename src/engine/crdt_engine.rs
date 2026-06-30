#![allow(dead_code)]
//! Generic delta-CRDT replication engine.
//!
//! Owns a `DotVersionVector` + a `C: DeltaCrdt`.
//! All DVV operations happen here; the CRDT only sees typed `Dot` values.
//! GC/membership concerns are delegated to dedicated layers.

use std::collections::HashMap;
use rand::Rng;

use tokio::task::JoinHandle;
use tracing::{warn};

use crate::metric;
use crate::common::{Counter, NodeId};
use crate::crdt::{DeltaContext, DeltaCrdt};
use crate::network::dissemination::SharedDissemination;
use crate::gc::{GcConfig, GcCoordinator, coordinator::GcInitiationAbortReason, storage::S3GcStorage};
use crate::logical_clocks::dot_version_vector::{Dot, DotVersionVector};
use crate::peers::peer_registry::PeerRegistry;
use crate::storage::s3_client::S3Client;

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
    LogCrdtMetrics,
}


// ── Public engine ───────────────────────────────────────────────────────
pub struct CrdtEngine<C: DeltaCrdt> {
    node_id: NodeId,
    crdt_id: String,
    gc: GcCoordinator<S3GcStorage>,
    dissemination: SharedDissemination<C>,
    crdt: C,
    dvv: DotVersionVector,
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
        CrdtEngine {
            node_id,
            crdt_id,
            gc: GcCoordinator::new(client, config, peer_registry),
            dissemination,
            crdt,
            dvv,
            rx
        }
    }

    pub async fn run(&mut self) {
        let _ = self.new_replica_bootstrap().await;
        
        while let Some(request) = self.rx.recv().await {
            match request {
                CrdtEngineRequest::DoPullRound => {
                    let dvv = &self.dvv;
                    self.dissemination.do_pull_round(&self.node_id, 
                        &self.crdt_id,
                        self.gc.config.gc_replica,
                        &dvv.effective_map());
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
                CrdtEngineRequest::DeltaResponse(_from_node, crdt_id, payload, knowledge_matrix) => {
                    self.server_delta(crdt_id, payload, knowledge_matrix).await;
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
                CrdtEngineRequest::LogCrdtMetrics => {
                    // self.gc.log_metrics().await;
                    self.crdt.log_metrics(&self.dvv, self.gc.epoch);
                }
            }
        }
    }

    pub async fn start_gc_loops(&self, engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<C>>) -> Vec<JoinHandle<()>> {
        let initiate_interval = self.gc.initiate_interval();
        let observe_interval = self.gc.observe_interval();

        let mut handles = Vec::new();

        {
            let eng_tx = engine_tx.clone();
            if let Some(interval) = initiate_interval {
                handles.push(tokio::spawn(async move {
                    loop {
                        let jitter_range = (interval / 20).as_nanos() as i64;
                        let jitter_nanos = rand::thread_rng().gen_range(-jitter_range..=jitter_range);
                        let sleep_duration = if jitter_nanos >= 0 {
                            interval + std::time::Duration::from_nanos(jitter_nanos as u64)
                        } else {
                            interval.saturating_sub(std::time::Duration::from_nanos((-jitter_nanos) as u64))
                        };
                        tokio::time::sleep(sleep_duration).await;
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
        self.crdt.get_random_element()
    }

    async fn print_internals(&self) -> String {
        self.crdt.print_internals()
    }

    async fn print_matrix_clock(&self) -> String {
        self.gc.print_matrix_clock().await
    }

    async fn print_state(&self) -> String {
        self.crdt.print_state()
    }

    /// Called when a client wants to perform a local write.
    async fn client_operation(&mut self, op: C::Op) {
        let start_millis = std::time::Instant::now();
        // 1. Mint a new dot.
        self.dvv.event();
        let dot = Dot::new(self.dvv.dot.node_id.clone(), self.dvv.dot.counter);

        // 2. Apply to CRDT.
        self.crdt.apply_local(dot, op);
        metric!(event = "client_operation", duration_millis = start_millis.elapsed().as_millis() as u64,
            node_id = self.node_id, dot_counter = self.dvv.dot.counter);
        // Advertise the updated VV outside the lock so peers can compute what to send us.
        self.dissemination
            .push_version_vector(&self.node_id, &self.crdt_id, self.gc.config.gc_replica, &self.dvv.effective_map())
            .await;
    }

    /// Called when a `CrdtOp` (delta) message arrives from the network.
    async fn server_delta(
        &mut self,
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

        let delta = match C::decode_delta(&payload) {
            Ok(d) => d,
            Err(e) => {
                warn!(%e, "failed to decode remote delta");
                return;
            }
        };

        if let Some(matrix) = &knowledge_matrix {
            self.gc.update_matrix_clock(matrix).await;
            let _ = self.gc.log_metrics(self.dissemination.get_dissemination_round(), &self.node_id, &self.dvv).await;
        } 

        // Merge CRDT state.
        self.crdt.merge_delta(&delta);

        // Merge causal context into DVV.
        let (ctx, node, counter) = delta.causal_context();
        let remote_dvv = DotVersionVector {
            dot: Dot::new(node, counter),
            context: ctx,
        };
        self.dvv.merge(&remote_dvv);

        self.dissemination
            .on_post_merge(&self.node_id, &self.crdt_id, self.gc.config.gc_replica, &self.dvv.effective_map())
            .await;
    }

    /// Called when a `CrdtPullRequest` message arrives from the network.
    async fn server_pull_request(
        &mut self,
        from_node: NodeId,
        crdt_id: String,
        remote_knowledge: HashMap<NodeId, u64>,
    ) {
        if crdt_id != self.crdt_id {
            warn!(
                expected = %self.crdt_id,
                received = %crdt_id,
                "ignoring pull request for unknown CRDT"
            );
            return;
        }

        let mut knowledge = HashMap::new();
        knowledge.insert(from_node.clone(), remote_knowledge.clone());
        self.gc.update_matrix_clock(&knowledge).await;
        let _ = self.gc.log_metrics(self.dissemination.get_dissemination_round(), &self.node_id, &self.dvv).await;

        let our_knowledge = self.dvv.effective_map();
        let communication_between_gc_replicas = self.gc.config.gc_replica && self
            .gc
            .registry
            .get(&from_node)
            .map(|(gc_replica, _)| gc_replica)
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
            .get(&from_node)
            .map(|(gc_replica, _)| gc_replica)
            .unwrap_or(false)
        {
            self.gc.get_knowledge_matrix().await
        } else {
            None
        };

        // Route both actions through the dissemination layer. The strategy
        // decides whether to piggyback the VV request on the delta message.
        self.dissemination.respond_to_pull_request(
            &from_node,
            &self.crdt_id,
            &self.node_id,
            delta_payload,
            self.gc.config.gc_replica,
            knowledge_matrix,
            our_knowledge_request,
        );
    }

    async fn observe_epoch_change(&mut self) -> anyhow::Result<bool> {
        let start_millis = std::time::Instant::now();
        let result = self.gc.observe_epoch_change(&self.node_id, &mut self.crdt, &mut self.dvv)
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

    async fn initiate_gc(&mut self) -> anyhow::Result<Option<GcInitiationAbortReason>> {
        let start_millis = std::time::Instant::now();

        let result = self.gc.initiate_gc(&self.node_id, &mut self.crdt, &mut self.dvv)
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

    async fn new_replica_bootstrap(&mut self) -> anyhow::Result<()> {
        let start_millis = std::time::Instant::now();
        let result = self.gc
            .new_replica_bootstrap(&self.node_id, &mut self.crdt, &mut self.dvv)
            .await;

        metric!(event = "new_replica_bootstrap",
            duration_millis = start_millis.elapsed().as_millis() as u64);
        result 
    }

    async fn get_final_state_for_deregistration(&self) -> (Counter, Vec<u8>) {
        let final_state_delta = self.crdt.full_state(&self.dvv);
        let final_dot: Counter = final_state_delta.causal_context().2;
        let final_state = C::encode_delta(&final_state_delta);
        (final_dot, final_state)
    }
}
