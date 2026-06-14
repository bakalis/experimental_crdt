//! S3-backed peer discovery and live-membership semantics.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::common::{Counter, NodeId};
use crate::peers::peer_registry::PeerRegistry;
use crate::storage::s3_client::S3Client;
use crate::peers::peer_connector::PeerConnector;

// ── Configuration ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct DiscoveryConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub poll_interval: Duration,
    pub registration_ttl: Duration,
    pub gc_replica: bool,
    pub connect_node_ids: Option<HashSet<NodeId>>,
}

// ── Registration record ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistration {
    pub node_id: NodeId,
    pub node_name: String,
    pub addr: String,
    pub gc_replica: bool,
    pub expires_at: DateTime<Utc>,
}

pub const DISCOVERY_NODES_PREFIX: &str = "nodes/";
pub const FINAL_DOTS_PREFIX: &str = "final_dots/";

fn registration_key(
    node_id: &NodeId,
    expires_at: DateTime<Utc>,
    gc_replica: bool,
    addr: String,
) -> String {
    format!(
        "{DISCOVERY_NODES_PREFIX}{node_id}__{}__{addr}__{}.json",
        if gc_replica { 1 } else { 0 },
        expires_at.timestamp_millis()
    )
}

fn registration_key_prefix(node_id: &NodeId) -> String {
    format!("{DISCOVERY_NODES_PREFIX}{node_id}__")
}

fn parse_registration_key(key: &str) -> anyhow::Result<NodeRegistration> {
    let rest = key
        .strip_prefix(DISCOVERY_NODES_PREFIX)
        .ok_or_else(|| anyhow::anyhow!("invalid discovery key prefix"))?;

    let stem = rest
        .strip_suffix(".json")
        .ok_or_else(|| anyhow::anyhow!("invalid discovery key suffix"))?;

    let mut parts = stem.splitn(4, "__");
    let node_id = parts
        .next()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing node_id in discovery key"))?
        .to_string();
    let gc_replica = match parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing gc_replica in discovery key"))?
    {
        "1" => true,
        "0" => false,
        other => anyhow::bail!("invalid gc_replica marker `{other}` in discovery key"),
    };
    let addr: String = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing socket address in discovery key"))?
        .to_string();
    let expires_millis: i64 = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing ttl timestamp in discovery key"))?
        .parse()?;
    let expires_at = DateTime::<Utc>::from_timestamp_millis(expires_millis)
        .ok_or_else(|| anyhow::anyhow!("invalid ttl timestamp in discovery key"))?;
    Ok(NodeRegistration {
        node_id: node_id.clone(),
        node_name: node_id.clone(),
        gc_replica,
        expires_at,
        addr,
    })
}

pub async fn list_live_registrations(
    client: &S3Client,
    bucket: &str,
) -> anyhow::Result<Vec<NodeRegistration>> {
    let keys = client
        .list_object_keys(bucket, DISCOVERY_NODES_PREFIX)
        .await?;
    let now = Utc::now();
    let mut by_node: HashMap<NodeId, NodeRegistration> = HashMap::new();

    for key in &keys {
        match parse_registration_key(key) {
            Ok(reg_key) if reg_key.expires_at > now => {
                let replace = by_node
                    .get(&reg_key.node_id)
                    .map(|existing| reg_key.expires_at > existing.expires_at)
                    .unwrap_or(true);
                if replace {
                    by_node.insert(reg_key.node_id.clone(), reg_key);
                }
            }
            Ok(reg_key) => {
                debug!(
                    node_id = %reg_key.node_id,
                    expires_at = %reg_key.expires_at,
                    "ignoring expired registration key"
                );
            }
            Err(e) => {
                warn!(key, %e, "failed to parse peer registration key");
            }
        }
    }

    Ok(by_node.into_values().collect())
}

pub async fn list_live_node_ids(client: &S3Client, bucket: &str) -> anyhow::Result<Vec<NodeId>> {
    let mut node_ids: Vec<NodeId> = list_live_registrations(client, bucket)
        .await?
        .into_iter()
        .map(|r| r.node_id)
        .collect();
    node_ids.sort();
    node_ids.dedup();
    Ok(node_ids)
}

// ── Discovery service ───────────────────────────────────────────────────

pub struct Discovery {
    pub client: S3Client,
    config: DiscoveryConfig,
    node_id: NodeId,
    _node_name: String,
    listen_host: String,
    listen_port: String,
}

impl Discovery {
    pub async fn new(
        client: S3Client,
        config: DiscoveryConfig,
        node_id: NodeId,
        _node_name: String,
        listen_host: String,
        listen_port: String,
    ) -> anyhow::Result<Self> {
        client.ensure_bucket(&config.bucket).await?;

        Ok(Self {
            client,
            config,
            node_id,
            _node_name,
            listen_host,
            listen_port
        })
    }

    // ── Register / deregister ───────────────────────────────────────

    pub async fn register(&self) -> anyhow::Result<()> {
        let ttl = chrono::Duration::from_std(self.config.registration_ttl)?;
        let expires_at = Utc::now() + ttl;
        let listen_addr = format!("{}:{}", self.listen_host, self.listen_port);
        let key = registration_key(
            &self.node_id,
            expires_at,
            self.config.gc_replica,
            listen_addr.clone(),
        );
        let body = serde_json::to_vec_pretty("")?;

        let current_keys = self.client.list_object_keys(&self.config.bucket, &registration_key_prefix(&self.node_id)).await?;
        for old_key in current_keys {
            if let Err(e) = self.client.delete_object(&self.config.bucket, &old_key).await {
                warn!(old_key, %e, "failed to remove previous registration key");
            }
        }

        self.client
            .put_object(&self.config.bucket, &key, body, "application/json")
            .await?;

        debug!(
            node_id = %self.node_id,
            addr = %listen_addr,
            expires_at = %expires_at,
            gc_replica = self.config.gc_replica,
            "registered / heartbeat refreshed in S3"
        );
        Ok(())
    }

    pub async fn deregister(&self, prefix: String, final_dot: Counter, final_state: Vec<u8>) -> anyhow::Result<()> {
        let current_keys = self.client.list_object_keys(&self.config.bucket, &registration_key_prefix(&self.node_id)).await?;
        self.client.put_object(&self.config.bucket, &format!("{}/{}{}_{}.json", prefix, FINAL_DOTS_PREFIX, &self.node_id, final_dot), final_state, "application/json").await?;
        for old_key in current_keys {
            if let Err(e) = self.client.delete_object(&self.config.bucket, &old_key).await {
                warn!(old_key, %e, "failed to remove previous registration key");
            }
        }

        info!(node_id = %self.node_id, "deregistered from S3");
        Ok(())
    }

    async fn list_live_peers(&self) -> anyhow::Result<Vec<NodeRegistration>> {
        let mut all = list_live_registrations(&self.client, &self.config.bucket).await?;
        all.retain(|reg| reg.node_id != self.node_id);

        if let Some(allowed_ids) = &self.config.connect_node_ids {
            all.retain(|reg| allowed_ids.contains(&reg.node_id));
        }
        Ok(all)
    }

    // ── Reconciliation loop ─────────────────────────────────────────

    pub async fn run_discovery_loop(&self, registry: PeerRegistry, connector: PeerConnector) {
        let mut interval = tokio::time::interval(self.config.poll_interval);
        loop {
            interval.tick().await;

            // 1. Refresh heartbeat.
            if let Err(e) = self.register().await {
                error!(%e, "failed to refresh registration heartbeat");
                continue;
            }

            // 2. List live peers.
            let live_peers = match self.list_live_peers().await {
                Ok(p) => p,
                Err(e) => {
                    error!(%e, "failed to list peers from S3");
                    continue;
                }
            };

            // 3. Reconcile.
            let desired: HashMap<NodeId, NodeRegistration> = live_peers
                .iter()
                .map(|r| (r.node_id.clone(), r.clone()))
                .collect();
            let current: HashSet<NodeId> = registry.peer_ids().into_iter().collect();

            let desired_ids: HashSet<NodeId> = desired.keys().cloned().collect();

            for node_id in desired_ids.difference(&current) {
                if self.node_id >= *node_id {
                    debug!(%node_id, "skipping peer with lower or equal node_id");
                    continue;
                }

                let registration = desired.get(node_id).unwrap();
                let addr = registration.addr.clone();

                info!(%node_id, %addr, "discovered new peer — connecting");
                let _ = connector.add_peer(node_id.to_string(), addr).await;
            }

            for node_id in current.difference(&desired_ids) {
                info!(%node_id, "peer departed — disconnecting");
                connector.remove_peer(node_id.to_string()).await;
            }

            debug!(
                live = desired.len(),
                connected = registry.len(),
                "discovery reconciliation complete"
            );
        }
    }
}
