use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::server::types::{CrdtType, EngineType};
use crate::proto::Envelope;
use crate::engine::crdt_engine::CrdtEngineRequest;
use crate::server::client_requests_handler;
use crate::server::Server;
use crate::peers::peer_connector::PeerConnector;

pub async fn start_background_tasks(
    server: &Server,
    mut engine: EngineType,
    engine_tx: tokio::sync::mpsc::Sender<CrdtEngineRequest<CrdtType>>,
    app_tx: mpsc::Sender<Envelope>,
) -> anyhow::Result<(
    Vec<JoinHandle<()>>,
    TcpListener,
)> {
    let mut handles = vec![];

    if let Some(client_handle) = client_requests_handler::start_client_handle(server.client_port.clone(), engine_tx.clone()) {
        handles.push(client_handle);
    }

    let discovery_handle = start_discovery(server, app_tx).await?;
    handles.push(discovery_handle);

    handles.append(&mut engine.start_gc_loops(engine_tx.clone()).await);

    handles.push(tokio::spawn(async move {
        engine.run().await;
    }));

    
    let listen_addr: SocketAddr = format!("0.0.0.0:{}", server.listen_port).parse()?;

    let listener = TcpListener::bind(listen_addr).await?;
    info!(
        addr = %listen_addr,
        node_id = %server.node_id,
        "listening"
    );

    Ok((handles, listener)) 
}

pub async fn start_discovery(
    server: &Server,
    app_tx: mpsc::Sender<Envelope>,
) -> anyhow::Result<JoinHandle<()>> {
    server.discovery.register().await?;

    let connector = PeerConnector::new(server.node_id.clone(),
        server.node_name.clone(),
        server.gc_replica,
        server.registry.clone(),
        app_tx.clone(),
        Arc::clone(&server.outbound_tasks));

    // ── Spawn discovery reconciliation loop ──────────────────────
    let disc_registry = server.registry.clone();
    let discovery = Arc::clone(&server.discovery);
    let discovery_handle = tokio::spawn(async move {
        discovery.run_discovery_loop(disc_registry, connector).await;
    });
    Ok(discovery_handle)
}

