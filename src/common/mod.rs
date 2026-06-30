#![allow(dead_code)]

use std::net::SocketAddr;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub mod error;

pub type NodeId = String;
pub type Counter = u64;

pub async fn lookup(addr: &str) -> anyhow::Result<SocketAddr> {
    tokio::net::lookup_host(addr)
        .await?
        .next()
        .ok_or_else(|| anyhow::anyhow!("could not resolve address: {addr}"))
}

pub async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

pub async fn shutdown_processes(shutdown: CancellationToken, mut join_set: JoinSet<()>) -> anyhow::Result<()> {
    tracing::info!("shutdown signal received, cancelling all tasks");
    shutdown.cancel();

    // Wait for every spawned server to actually finish.
    while let Some(res) = join_set.join_next().await {
        if let Err(e) = res {
            eprintln!("task panicked: {:?}", e);
        }
    }

    tracing::info!("all servers shut down cleanly");
    Ok(())
}
