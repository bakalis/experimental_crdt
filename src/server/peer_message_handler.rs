use std::net::SocketAddr;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::server::connection;
use crate::proto::Envelope;
use crate::proto::envelope::Payload;
use crate::engine::crdt_engine::CrdtEngineRequest;
use crate::server::types::{CrdtType};
use crate::peers::peer_registry::PeerRegistry;

pub fn handle_accepted_connection(
    node_id: String,
    node_name: String,
    gc_replica: bool,
    registry: PeerRegistry,
    app_tx: mpsc::Sender<Envelope>,
    accept_result: std::io::Result<(tokio::net::TcpStream, SocketAddr)>,
) {
    match accept_result {
        Ok((stream, remote_addr)) => {
            info!(%remote_addr, "accepted inbound connection");
            tokio::spawn(async move {
                connection::handle_inbound(
                    stream,
                    remote_addr,
                    &node_id,
                    &node_name,
                    gc_replica,
                    &registry,
                    &app_tx,
                )
                .await;
            });
        }
        Err(e) => error!(%e, "accept failed"),
    }
}

pub async fn handle_received_envelope(
    envelope: Envelope,
    engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<CrdtType>>,
) {
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
