mod connection;
mod crdt;                // ← NEW
mod crdt_engine;         // ← NEW
mod discovery;
mod dissemination;       // ← NEW
mod error;
mod peer_manager;
mod protocol;
mod server;
mod common;
mod proto;
mod logical_clocks;

use clap::Parser;
use std::net::SocketAddr;
use dotenvy::dotenv;

/// CRDT replication server with S3-based peer discovery.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// Address this node listens on (e.g. 0.0.0.0:9000).
    #[arg(short, long, default_value = "0.0.0.0:9000")]
    listen: SocketAddr,

    /// The address that *other* nodes should use to reach us.
    #[arg(long)]
    advertise: Option<SocketAddr>,

    /// Optional human-readable node name.
    #[arg(short, long)]
    name: Option<String>,

    // ── S3 / MinIO discovery ────────────────────────────────────

    /// S3-compatible endpoint URL (e.g. http://localhost:9000).
    #[arg(long, env = "S3_ENDPOINT")]
    s3_endpoint: String,

    /// S3 bucket used for peer discovery.
    #[arg(long, env = "S3_BUCKET", default_value = "crdt-peers")]
    s3_bucket: String,

    /// S3 region (can be anything for MinIO).
    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    /// S3 access key.
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    s3_access_key: String,

    /// S3 secret key.
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    s3_secret_key: String,

    /// How often (in seconds) to poll S3 for peer changes.
    #[arg(long, default_value = "10")]
    discovery_interval_secs: u64,

    /// Seconds after which a registration without heartbeat refresh
    /// is considered stale and ignored.
    #[arg(long, default_value = "30")]
    registration_ttl_secs: u64,

    /// Optional address for the client operation listener (JSON-over-TCP).
    /// Clients connect and send newline-delimited JSON `OrSetOp<String>` values.
    #[arg(long)]
    client_addr: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    dotenv().ok();

    let cli = Cli::parse();
    let node_name = cli.name.unwrap_or_else(|| cli.listen.to_string());
    let advertise_addr = cli.advertise.unwrap_or(cli.listen);

    let discovery_cfg = discovery::DiscoveryConfig {
        endpoint: cli.s3_endpoint,
        bucket: cli.s3_bucket,
        region: cli.s3_region,
        access_key: cli.s3_access_key,
        secret_key: cli.s3_secret_key,
        poll_interval: std::time::Duration::from_secs(cli.discovery_interval_secs),
        registration_ttl: std::time::Duration::from_secs(cli.registration_ttl_secs),
    };

    let server = server::Server::new(cli.listen, advertise_addr, &node_name, cli.client_addr);
    server.run(discovery_cfg).await
}
