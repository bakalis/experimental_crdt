mod common;
mod crdt;
mod engine;
mod peers;
mod gc;
mod logical_clocks;
mod proto;
mod network;
mod storage;
mod server;
mod logging;

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use clap::Parser;
use core::option::Option;
use std::time::Duration;
use dotenvy::dotenv;
use std::collections::HashSet;
use crate::peers::discovery;
use crate::proto::Envelope;
use crate::network::tcp_connection::TcpNetwork;

/// CRDT replication server with S3-based peer discovery.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {

    #[arg(long, default_value = "localhost")]
    listen_host: String,

    #[arg(long, default_value = "9000")]
    listen_port: String,

    /// Optional human-readable node name.
    #[arg(short, long)]
    name: Option<String>,

    /// Optional address for the client operation listener (protobuf-over-TCP).
    /// Clients connect and send length-prefixed protobuf `ProtoClientCommand` messages.
    #[arg(long)]
    client_port: Option<String>,

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
    #[arg(long, default_value = "30")]
    discovery_interval_secs: u64,

    /// Seconds after which a registration without heartbeat refresh
    /// is considered stale and ignored.
    #[arg(long, default_value = "30")]
    registration_ttl_secs: u64,

    /// Optional comma-separated node IDs to connect to (env: DISCOVERY_CONNECT_NODE_IDS).
    #[arg(long, env = "DISCOVERY_CONNECT_NODE_IDS")]
    discovery_connect_node_ids: Option<String>,

    #[arg(short, long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    gc_replica: bool,

    /// Prefix under the configured S3 bucket for GC protocol objects.
    #[arg(long, default_value = "gc")]
    gc_prefix: String,

    /// Interval in seconds for periodic GC initiation attempts.
    #[arg(long, default_value = "300")]
    gc_initiate_interval_secs: u64,

    /// Interval in seconds for periodic ObserveEpochChange + clock publish.
    #[arg(long, default_value = "120")]
    gc_observe_interval_secs: u64,

    #[arg(long, env = "METRICS_FILE_PATH")]
    metrics_path: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let cli = Cli::parse();
    logging::initialize_logging(cli.metrics_path.clone());
    let node_name = cli.name.unwrap_or_else(|| cli.listen_host.to_string());
    let connect_node_ids: Option<HashSet<String>> = cli
        .discovery_connect_node_ids
        .as_deref()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(ToOwned::to_owned)
                .collect::<HashSet<_>>()
        })
        .filter(|set| !set.is_empty());

    let discovery_cfg = discovery::DiscoveryConfig {
        endpoint: cli.s3_endpoint,
        bucket: cli.s3_bucket.clone(),
        region: cli.s3_region,
        access_key: cli.s3_access_key,
        secret_key: cli.s3_secret_key,
        poll_interval: std::time::Duration::from_secs(cli.discovery_interval_secs),
        registration_ttl: std::time::Duration::from_secs(cli.registration_ttl_secs),
        gc_replica: cli.gc_replica,
        connect_node_ids,
    };

    let gc_replica = cli.gc_replica;

    let server_config = server::ServerConfig {
        listen_host: cli.listen_host,
        listen_port: cli.listen_port,
        gc_replica,
        experiment: false, // experiment is false for main server binary
        node_name: node_name.clone(),
        client_port: cli.client_port,
    };

    let gc_config = gc::GcConfig {
        gc_replica,
        observe_interval: std::time::Duration::from_secs(cli.gc_observe_interval_secs),
        storage_config: gc::GcStorageConfig {
            bucket: cli.s3_bucket.clone(),
            prefix: cli.gc_prefix.clone(),
        },
        gc_replica_config: if cli.gc_replica {
            Some(gc::GcReplicaConfig::new(Duration::from_secs(cli.gc_initiate_interval_secs)))
        } else {
            None
        },
    };
    let shutdown = CancellationToken::new();
    let mut join_set: JoinSet<()> = JoinSet::new();

    let (app_tx, app_rx) = mpsc::channel::<Envelope>(1024);
    let server = server::Server::new(server_config, discovery_cfg).await?;
    let network = Arc::new(TcpNetwork);
    let shutdown_clone = shutdown.clone();

    join_set.spawn(async move {
        if let Err(e) = server.run(gc_config, app_tx, app_rx, network, shutdown_clone).await {
            eprintln!("server run failed: {:?}", e);
        }
    });
    
    common::wait_for_shutdown_signal().await;
    common::shutdown_processes(shutdown, join_set).await
}


