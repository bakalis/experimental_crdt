//! Generic delta-CRDT replication engine.
//!
//! Owns a `DotVersionVector` + a `dyn DeltaCrdt`.
//! No delta log — the DVV computes minimal deltas on demand
//! via `dvv.delta_since()`.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::common::NodeId;
use crate::crdt::DeltaCrdt;
use crate::dissemination::SharedDissemination;
use crate::logical_clocks::dot_version_vector::{Dot, DotVersionVector};
use crate::peer_manager::PeerManager;
use crate::proto::{self, envelope::Payload, Envelope};

// ── Commands ────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum EngineCommand {
    /// A local application-level operation (already serialised).
    LocalOp { op_bytes: Vec<u8> },
    /// A remote CrdtOp received from the network.
    RemoteCrdtOp {
        from_node: NodeId,
        crdt_id: String,
        payload: Vec<u8>,
        hlc_ts: u64,
    },
}

/// Thread-safe handle for submitting commands to the engine.
#[derive(Clone)]
pub struct EngineHandle {
    cmd_tx: mpsc::Sender<EngineCommand>,
}

impl EngineHandle {
    pub async fn apply_local(
        &self,
        op_bytes: Vec<u8>,
    ) -> Result<(), mpsc::error::SendError<EngineCommand>> {
        self.cmd_tx
            .send(EngineCommand::LocalOp { op_bytes })
            .await
    }

    pub async fn apply_remote(
        &self,
        from_node: NodeId,
        crdt_id: String,
        payload: Vec<u8>,
        hlc_ts: u64,
    ) -> Result<(), mpsc::error::SendError<EngineCommand>> {
        self.cmd_tx
            .send(EngineCommand::RemoteCrdtOp {
                from_node,
                crdt_id,
                payload,
                hlc_ts,
            })
            .await
    }
}

// ── Engine ──────────────────────────────────────────────────────────────

pub struct CrdtEngine {
    node_id: NodeId,
    crdt_id: String,
    data: Box<dyn DeltaCrdt>,
    dvv: DotVersionVector,
    dissemination: SharedDissemination,
    peer_manager: PeerManager,
    pull_interval: Option<Duration>,
}

impl CrdtEngine {
    pub fn new(
        node_id: NodeId,
        crdt_id: String,
        initial_state: Box<dyn DeltaCrdt>,
        dissemination: SharedDissemination,
        peer_manager: PeerManager,
        pull_interval: Option<Duration>,
    ) -> (EngineHandle, mpsc::Receiver<EngineCommand>, Self) {
        let (cmd_tx, cmd_rx) = mpsc::channel(1024);

        let dvv = DotVersionVector::new(node_id.clone());
        let handle = EngineHandle { cmd_tx };

        let engine = Self {
            node_id,
            crdt_id,
            data: initial_state,
            dvv,
            dissemination,
            peer_manager,
            pull_interval,
        };

        (handle, cmd_rx, engine)
    }

    pub async fn run(mut self, mut cmd_rx: mpsc::Receiver<EngineCommand>) {
        info!(
            node_id = %self.node_id,
            crdt_id = %self.crdt_id,
            "CRDT engine started"
        );

        let pull_interval = if self.dissemination.supports_pull() {
            self.pull_interval.or(Some(Duration::from_secs(10)))
        } else {
            None
        };

        let mut pull_ticker = pull_interval.map(tokio::time::interval);
        let mut message_interval = tokio::time::interval(Duration::from_secs(10));
        let mut ctr = 0;

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        EngineCommand::LocalOp { op_bytes } => {
                            self.handle_local_op(op_bytes).await;
                        }
                        EngineCommand::RemoteCrdtOp { from_node, crdt_id, payload, hlc_ts } => {
                            self.handle_remote(&from_node, &crdt_id, &payload, hlc_ts);
                        }
                    }
                }

                _ = message_interval.tick(), if ctr <= 10 => {
                    let or_set_op = crate::crdt::or_set::OrSetOp::Add(format!("tick-{}-{}", self.node_id.clone(), chrono::Utc::now().timestamp()));
                    self.handle_local_op(serde_json::to_vec(&or_set_op).unwrap()).await;
                    ctr += 1;
                }

                _ = async {
                    if let Some(ref mut ticker) = pull_ticker {
                        ticker.tick().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    self.do_pull_round();
                }

                else => break,
            }
        }

        info!(node_id = %self.node_id, crdt_id = %self.crdt_id, "CRDT engine stopped");
    }

    async fn handle_local_op(&mut self, op_bytes: Vec<u8>) {
        debug!(node_id = %self.node_id, crdt_id = %self.crdt_id, "applying local op");

        let delta_bytes = self.data.apply_local(
            &mut self.dvv,
            &self.node_id,
            &op_bytes,
        );

        self.dissemination
            .push_delta(
                &self.node_id,
                &self.crdt_id,
                delta_bytes,
                self.dvv.dot.counter,
            )
            .await;
    }

    fn handle_remote(
        &mut self,
        from_node: &NodeId,
        crdt_id: &str,
        payload: &[u8],
        hlc_ts: u64,
    ) {
        if crdt_id != self.crdt_id {
            warn!(expected = %self.crdt_id, received = %crdt_id, "ignoring delta for unknown CRDT");
            return;
        }

        if hlc_ts == 0 {
            // Pull request — payload is a knowledge map.
            self.handle_pull_request(from_node, payload);
        } else {
            // Normal delta.
            debug!(from_node, crdt_id, "merging remote delta");
            self.data.apply_remote(&mut self.dvv, payload);
        }
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

        // Respond with full state (correct and simple).
        let state_bytes = self.data.encode_state(&self.dvv);

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
