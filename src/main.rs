mod connection;
mod error;
mod peer_manager;
mod protocol;
mod server;
mod proto;

use clap::Parser;
use std::net::SocketAddr;

/// CRDT replication server.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// Address this node listens on (e.g. 0.0.0.0:9000).
    #[arg(short, long, default_value = "0.0.0.0:9000")]
    listen: SocketAddr,

    /// Initial peers to connect to, comma-separated (e.g. 10.0.0.2:9000,10.0.0.3:9000).
    #[arg(short, long, value_delimiter = ',')]
    peers: Vec<SocketAddr>,

    /// Optional human-readable node name.
    #[arg(short, long)]
    name: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured logging — honour RUST_LOG env var, default to info.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    let node_name = cli.name.unwrap_or_else(|| cli.listen.to_string());

    let server = server::Server::new(cli.listen, &node_name);
    server.run(cli.peers).await
}
