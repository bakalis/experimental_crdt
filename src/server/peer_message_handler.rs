use tracing::{error, info};

use prost::Message;

use crate::common::NodeId;
use crate::metric;
use crate::proto::Envelope;
use crate::proto::envelope::Payload;
use crate::engine::crdt_engine::CrdtEngineRequest;
use crate::server::types::{CrdtType};

pub async fn handle_received_envelope(
    node_id: NodeId,
    envelope: Envelope,
    engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<CrdtType>>,
) {
    let len = envelope.encoded_len();
    metric!(node_id = node_id, event = "receive_envelope", size_bytes = len as u64);
    // Route CRDT operations to the engine.
    match envelope.payload {
        Some(Payload::CrdtOp(crdt_op)) => {
            let origin_id = crdt_op.origin_node_id;
            let cid = crdt_op.crdt_id;
            let requester_knowledge = crdt_op.requester_knowledge;
            if let Err(e) = engine_tx.send(CrdtEngineRequest::DeltaResponse(
                origin_id.clone(),
                cid.clone(),
                crdt_op.payload,
                crdt_op.knowledge_matrix.map(|km| {
                    km.entries
                        .into_iter()
                        .map(|(node_id, vc)| (node_id, vc.entries))
                        .collect()
                }),
            )).await {
                error!(%e, "failed to send server delta request to engine");
                return;
            }

            // Handle a piggybacked VV request: the sender wants us to
            // send a delta back for what they are missing.
            if let Some(vc) = requester_knowledge {
                if !vc.entries.is_empty() {
                    if let Err(e) = engine_tx.send(CrdtEngineRequest::DeltaRequest(
                        origin_id,
                        cid,
                        vc.entries
                    )).await {
                        error!(%e, "failed to send server pull request to engine");
                    }
                }
            }
        }
        Some(Payload::CrdtPullRequest(req)) => {
            let knowledge = req.knowledge.map(|vc| vc.entries).unwrap_or_default();
            if let Err(e) = engine_tx.send(CrdtEngineRequest::DeltaRequest(
                req.origin_node_id,
                req.crdt_id,
                knowledge
            )).await {
                error!(%e, "failed to send server pull request to engine");
            }
        }
        other => {
            info!(?other, "app received non-CRDT message");
        }
    }
}
