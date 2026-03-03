//! Generic delta-CRDT replication engine.
//!
//! Owns a `DotVersionVector` + a `C: DeltaCrdt`.
//! All DVV operations happen here; the CRDT only sees typed `Dot` values.
//! The engine is `Clone`-able (backed by `Arc<Mutex<...>>`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::common::NodeId;
use crate::crdt::{DeltaCrdt, DeltaContext};
use crate::dissemination::SharedDissemination;
use crate::logical_clocks::dot_version_vector::{Dot, DotVersionVector};
use crate::peer_manager::PeerManager;
use crate::proto::{self, envelope::Payload, Envelope};

// ── Inner state (behind the mutex) ─────────────────────────────────────

struct EngineInner<C: DeltaCrdt> {
    node_id: NodeId,
    crdt_id: String,
    crdt: C,
    dvv: DotVersionVector,
    dissemination: SharedDissemination,
    peer_manager: PeerManager,
}

impl<C: DeltaCrdt> EngineInner<C> {
    async fn client_operation(&mut self, op: C::Op) {
        debug!(node_id = %self.node_id, crdt_id = %self.crdt_id, "applying local op");

        // 1. Mint a new dot.
        self.dvv.event();
        let dot = Dot::new(self.dvv.dot.node_id.clone(), self.dvv.dot.counter);

        // 2. Apply to CRDT.
        let mut delta = self.crdt.apply_local(dot, op);

        // 3. Annotate with full DVV effective map so receivers can merge.
        delta.set_causal_context(
            self.dvv.effective_map(),
            self.dvv.dot.node_id.clone(),
            self.dvv.dot.counter,
        );

        // 4. Push delta to peers.
        let payload = C::encode_delta(&delta);
        self.dissemination
            .push_delta(&self.node_id, &self.crdt_id, payload, self.dvv.dot.counter)
            .await;
    }

    fn server_message(
        &mut self,
        from_node: &NodeId,
        crdt_id: &str,
        payload: &[u8],
        hlc_ts: u64,
    ) {
        if crdt_id != self.crdt_id {
            warn!(
                expected = %self.crdt_id,
                received = %crdt_id,
                "ignoring delta for unknown CRDT"
            );
            return;
        }

        if hlc_ts == 0 {
            // Pull request — payload is a knowledge map.
            self.handle_pull_request(from_node, payload);
        } else {
            debug!(from_node, crdt_id, "merging remote delta");
            self.handle_remote_delta(payload);
        }
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

    fn handle_pull_request(&mut self, from_node: &NodeId, payload: &[u8]) {
        let remote_knowledge: HashMap<NodeId, u64> = match serde_json::from_slice(payload) {
            Ok(k) => k,
            Err(e) => {
                warn!(%from_node, %e, "failed to decode pull request knowledge map");
                return;
            }
        };

        debug!(%from_node, "received pull request");

        let dvv_delta = self.dvv.delta_since(&remote_knowledge);
        if dvv_delta.dot.counter == 0 && dvv_delta.context.is_empty() {
            debug!(%from_node, "peer is up to date");
            return;
        }

        let mut state = self.crdt.full_state();
        state.set_causal_context(
            self.dvv.effective_map(),
            self.dvv.dot.node_id.clone(),
            self.dvv.dot.counter,
        );
        let state_bytes = C::encode_delta(&state);

        let response = Envelope {
            payload: Some(Payload::CrdtOp(proto::CrdtOp {
                crdt_id: self.crdt_id.clone(),
                payload: state_bytes,
                hlc_ts: self.dvv.dot.counter,
                origin_node_id: self.node_id.clone(),
            })),
        };

        if let Some((_, handle)) = self.peer_manager.get(from_node) {
            if let Err(e) = handle.tx.try_send(response) {
                warn!(%from_node, %e, "failed to send pull response");
            }
        }
    }

    fn do_pull_round(&self) {
        let our_knowledge = self.dvv.effective_map();
        let peer_ids = self.peer_manager.peer_ids();

        for peer_id in &peer_ids {
            if let Some(envelope) = self.dissemination.build_pull_request(
                &self.node_id,
                &self.crdt_id,
                &our_knowledge,
            ) {
                if let Some((_, handle)) = self.peer_manager.get(peer_id) {
                    if let Err(e) = handle.tx.try_send(envelope.clone()) {
                        warn!(%peer_id, %e, "failed to send pull request");
                    }
                }
            }
        }

        debug!(peers = peer_ids.len(), "pull round complete");
    }
}

// ── Public engine ───────────────────────────────────────────────────────

/// Generic delta-CRDT engine.
///
/// `Clone` is cheap — it just clones the inner `Arc`.
pub struct CrdtEngine<C: DeltaCrdt> {
    inner: Arc<Mutex<EngineInner<C>>>,
    pull_interval: Option<Duration>,
}

impl<C: DeltaCrdt> Clone for CrdtEngine<C> {
    fn clone(&self) -> Self {
        CrdtEngine {
            inner: Arc::clone(&self.inner),
            pull_interval: self.pull_interval,
        }
    }
}

impl<C: DeltaCrdt> CrdtEngine<C> {
    pub fn new(
        node_id: NodeId,
        crdt_id: String,
        crdt: C,
        dissemination: SharedDissemination,
        peer_manager: PeerManager,
        pull_interval: Option<Duration>,
    ) -> Self {
        let dvv = DotVersionVector::new(node_id.clone());
        let inner = EngineInner {
            node_id,
            crdt_id,
            crdt,
            dvv,
            dissemination,
            peer_manager,
        };
        CrdtEngine {
            inner: Arc::new(Mutex::new(inner)),
            pull_interval,
        }
    }
    
    pub async fn get_random_element(&self) -> Option<String> {
        let inner = self.inner.lock().await;
        inner.crdt.get_random_element()
    }

    pub async fn print_state(&self) {
        let inner = self.inner.lock().await;
        inner.crdt.print_state();
    }

    /// Called when a client wants to perform a local write.
    pub async fn client_operation(&self, op: C::Op) {
        self.inner.lock().await.client_operation(op).await;
    }

    /// Called when a `CrdtOp` message arrives from the network.
    pub async fn server_message(
        &self,
        from_node: NodeId,
        crdt_id: String,
        payload: Vec<u8>,
        hlc_ts: u64,
    ) {
        self.inner
            .lock()
            .await
            .server_message(&from_node, &crdt_id, &payload, hlc_ts);
    }

    /// Spawn a background pull-round loop (no-op if dissemination doesn't
    /// support pull or `pull_interval` is not set).
    pub fn start_pull_loop(&self) -> tokio::task::JoinHandle<()> {
        let engine = self.clone();
        tokio::spawn(async move {
            let (supports_pull, interval) = {
                let inner = engine.inner.lock().await;
                (
                    inner.dissemination.supports_pull(),
                    engine.pull_interval.unwrap_or(Duration::from_secs(10)),
                )
            };

            if !supports_pull {
                return;
            }

            {
                let inner = engine.inner.lock().await;
                info!(
                    node_id = %inner.node_id,
                    crdt_id = %inner.crdt_id,
                    "pull loop started (interval = {:?})", interval
                );
            }
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                engine.inner.lock().await.do_pull_round();
            }
        })
    }
}
