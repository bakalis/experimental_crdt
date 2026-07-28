#[path = "../common/mod.rs"]
mod common;
#[path = "../crdt/mod.rs"]
mod crdt;
#[path = "../engine/mod.rs"]
mod engine;
#[path = "../peers/mod.rs"]
mod peers;
#[path = "../gc/mod.rs"]
mod gc;
#[path = "../logical_clocks/mod.rs"]
mod logical_clocks;
#[path = "../proto/mod.rs"]
mod proto;
#[path = "../network/mod.rs"]
mod network;
#[path = "../storage/mod.rs"]
mod storage;
#[path = "../server/mod.rs"]
mod server;
#[path = "../logging/mod.rs"]
mod logging;

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio::task::JoinSet;
use clap::Parser;
use core::option::Option;
use std::time::Duration;
use dotenvy::dotenv;
use std::collections::{HashSet, HashMap};

use crate::common::NodeId;
use crate::network::simulated::SimulatedNetwork;
use crate::peers::discovery;
use crate::proto::Envelope;

/// CRDT replication server with S3-based peer discovery.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {

    #[arg(long, default_value = "localhost")]
    listen_host: String,

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

    #[arg(long, default_value = "1")]
    num_gc_replicas: usize,

    #[arg(long, default_value = "1")]
    num_normal_replicas: usize,
    
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

#[derive(Clone)]
struct ConfigCliInfo {
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    discovery_interval_secs: u64,
    registration_ttl_secs: u64,
    gc_prefix: String,
    listen_host: String,
    gc_observe_interval_secs: u64,
    gc_initiate_interval_secs: u64,
    num_gc_replicas: usize,
    num_normal_replicas: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let cli = Cli::parse();
    logging::initialize_logging(cli.metrics_path);
    let num_gc_replicas = cli.num_gc_replicas;
    let num_normal_replicas = cli.num_normal_replicas;

    let mut channels_map: HashMap<String, (NodeId, bool, mpsc::Sender<Envelope>)> = HashMap::new();
    let mut replicas: Vec<(String, NodeId, usize, bool, mpsc::Receiver<Envelope>)> = vec![];

    for r in 1..num_gc_replicas + 1 {
        let gc_port = 8000 + r;
        let (app_tx, app_rx) = mpsc::channel::<Envelope>(1024);
        let node_name = format!("gc-server-{}", r);
        let node_addr = format!("{}:{}", cli.listen_host, gc_port);
        channels_map.insert(node_addr.clone(), (node_name.clone(), true, app_tx));
        replicas.push((node_addr, node_name, gc_port, true, app_rx));
    }

    for r in 1..num_normal_replicas + 1 {
        let gc_port = 6000 + r;
        let (app_tx, app_rx) = mpsc::channel::<Envelope>(1024);
        let node_name = format!("normal-server-{}", r);
        let node_addr = format!("{}:{}", cli.listen_host, gc_port);
        channels_map.insert(node_addr.clone(), (node_name.clone(), false, app_tx));
        replicas.push((node_addr, node_name, gc_port, false, app_rx));
    }
    let shutdown = CancellationToken::new();
    let mut join_set: JoinSet<()> = JoinSet::new();

    let network = Arc::new(SimulatedNetwork::new(channels_map.clone()));

    let config_info = ConfigCliInfo { endpoint: cli.s3_endpoint.clone(),
        bucket: cli.s3_bucket.clone(),
        region: cli.s3_region.clone(),
        access_key: cli.s3_access_key.clone(),
        secret_key: cli.s3_secret_key.clone(),
        discovery_interval_secs: cli.discovery_interval_secs,
        registration_ttl_secs: cli.registration_ttl_secs,
        gc_prefix: cli.gc_prefix.clone(),
        listen_host: cli.listen_host.clone(),
        gc_observe_interval_secs: cli.gc_observe_interval_secs,
        gc_initiate_interval_secs: cli.gc_initiate_interval_secs,
        num_gc_replicas: cli.num_gc_replicas,
        num_normal_replicas: cli.num_normal_replicas};
    for (node_addr, node_name, node_port, gc_replica, app_rx) in replicas {
        let app_tx = channels_map.get(&node_addr).unwrap().2.clone();
        let config_info_clone = config_info.clone();
        let network_clone = network.clone();
        let shutdown_clone = shutdown.clone();
        join_set.spawn(async move {
            if let Err(e) = init_and_run_server(config_info_clone, node_name, node_port, gc_replica, 
                (app_tx, app_rx), network_clone, shutdown_clone).await {
                eprintln!("Error running server {}: {:?}", node_addr, e);
            }
        });
    }

    common::wait_for_shutdown_signal().await;
    common::shutdown_processes(shutdown, join_set).await
}

async fn init_and_run_server(
    config_info: ConfigCliInfo,
    node_name: String,
    listen_port: usize,
    gc_replica: bool,
    app_mpsc: (mpsc::Sender<Envelope>, mpsc::Receiver<Envelope>),
    network: Arc<SimulatedNetwork>,
    shutdown: CancellationToken
) -> anyhow::Result<()> {
    let (app_tx, app_rx) = app_mpsc;

    let connect_node_ids = if config_info.num_normal_replicas == 0 { None }
    else {
        Some(compute_connect_node_ids(&node_name,
            gc_replica,
            config_info.num_gc_replicas,
            config_info.num_normal_replicas)
        )
    };
    let connect_node_ids_str: String = match &connect_node_ids {
        Some(ids) => ids.iter().cloned().collect::<Vec<_>>().join(","),
        Option::None => String::new(),
    };
    metric!(event = "network_topology", node_id = node_name.clone(), gc_replica = gc_replica, connect_node_ids = connect_node_ids_str);

    let discovery_cfg = discovery::DiscoveryConfig {
        endpoint: config_info.endpoint,
        bucket: config_info.bucket.clone(),
        region: config_info.region,
        access_key: config_info.access_key,
        secret_key: config_info.secret_key,
        poll_interval: Duration::from_secs(config_info.discovery_interval_secs),
        registration_ttl: Duration::from_secs(config_info.registration_ttl_secs),
        gc_replica,
        connect_node_ids,
    };

    let server_config = server::ServerConfig {
        listen_host: config_info.listen_host,
        listen_port: listen_port.to_string(),
        gc_replica,
        experiment: true,
        node_name,
        client_port: None
    };

    let gc_config = gc::GcConfig {
        gc_replica,
        observe_interval: Duration::from_secs(config_info.gc_observe_interval_secs),
        storage_config: gc::GcStorageConfig {
            bucket: config_info.bucket.clone(),
            prefix: config_info.gc_prefix.clone(),
        },
        gc_replica_config: if gc_replica {
            Some(gc::GcReplicaConfig::new(Duration::from_secs(
                config_info.gc_initiate_interval_secs,
            )))
        } else {
            None
        },
    };

    let server = server::Server::new(server_config, discovery_cfg).await?;
    server.run(gc_config, app_tx, app_rx, network, shutdown).await
}

fn compute_connect_node_ids(
    node_name: &str,
    gc_replica: bool,
    gc_count: usize,
    normal_count: usize
) -> HashSet<String> {
    if gc_replica {
        // Parse index from "gc-server-{i}"
        let idx: usize = node_name
            .strip_prefix("gc-server-")
            .and_then(|s| s.parse().ok())
            .expect("GC node name must be gc-server-{i}");

        // All other GC nodes
        let gc_peers = (1..=gc_count)
            .filter(|&j| j != idx)
            .map(|j| format!("gc-server-{j}"));

        // Normal nodes assigned to this GC node (round-robin: normal i → gc (i-1) % gc_count + 1)
        let normal_peers = (1..=normal_count)
            .filter(|&i| (i - 1) % gc_count + 1 == idx)
            .map(|i| format!("normal-server-{i}"));

        gc_peers.chain(normal_peers).collect()
    } else {
        // Parse index from "normal-server-{i}"
        let idx: usize = node_name
            .strip_prefix("normal-server-")
            .and_then(|s| s.parse().ok())
            .expect("Normal node name must be normal-server-{i}");

        // Exactly one GC node, round-robin
        let gc_idx = (idx - 1) % gc_count + 1;
        std::iter::once(format!("gc-server-{gc_idx}")).collect()
    }
}
