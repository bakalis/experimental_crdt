//! S3-backed peer discovery.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::peer_manager::PeerManager;
use crate::s3_client::S3Client;
use crate::server::PeerConnector;
use crate::common::NodeId;

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
}

// ── Registration record ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistration {
    pub node_id: NodeId,
    pub node_name: String,
    pub addr: SocketAddr,
    pub heartbeat: DateTime<Utc>,
}

// ── Discovery service ───────────────────────────────────────────────────

pub struct Discovery {
    client: S3Client,
    config: DiscoveryConfig,
    node_id: NodeId,
    node_name: String,
    advertise_addr: SocketAddr,
}

impl Discovery {
    pub async fn new(
        config: DiscoveryConfig,
        node_id: NodeId,
        node_name: String,
        advertise_addr: SocketAddr,
    ) -> anyhow::Result<Self> {
        let client = S3Client::new(
            &config.endpoint,
            &config.region,
            &config.access_key,
            &config.secret_key,
        );

        client.ensure_bucket(&config.bucket).await?;

        Ok(Self {
            client,
            config,
            node_id,
            node_name,
            advertise_addr,
        })
    }

    fn self_key(&self) -> String {
        format!("nodes/{}.json", self.node_id)
    }

    // ── Register / deregister ───────────────────────────────────────

    pub async fn register(&self) -> anyhow::Result<()> {
        let registration = NodeRegistration {
            node_id: self.node_id.clone(),
            node_name: self.node_name.clone(),
            addr: self.advertise_addr,
            heartbeat: Utc::now(),
        };
        let body = serde_json::to_vec_pretty(&registration)?;

        self.client
            .put_object(&self.config.bucket, &self.self_key(), body, "application/json")
            .await?;

        info!(
            node_id = %self.node_id,
            addr = %self.advertise_addr,
            "registered / heartbeat refreshed in S3"
        );
        Ok(())
    }

    pub async fn deregister(&self) -> anyhow::Result<()> {
        self.client
            .delete_object(&self.config.bucket, &self.self_key())
            .await?;

        info!(node_id = %self.node_id, "deregistered from S3");
        Ok(())
    }

    // ── Peer listing ────────────────────────────────────────────────

    async fn list_live_peers(&self) -> anyhow::Result<Vec<NodeRegistration>> {
        let mut peers = Vec::new();
        let now = Utc::now();
        let ttl = chrono::Duration::from_std(self.config.registration_ttl)?;

        let keys = self.client.list_object_keys(&self.config.bucket, "nodes/").await?;

        for key in &keys {
            if *key == self.self_key() {
                continue;
            }

            match self.fetch_registration(key).await {
                Ok(reg) => {
                    let age = now - reg.heartbeat;
                    if age <= ttl {
                        debug!(
                            node_id = %reg.node_id,
                            addr = %reg.addr,
                            age_secs = age.num_seconds(),
                            "discovered live peer"
                        );
                        peers.push(reg);
                    } else {
                        warn!(
                            node_id = %reg.node_id,
                            addr = %reg.addr,
                            age_secs = age.num_seconds(),
                            "ignoring stale registration"
                        );
                    }
                }
                Err(e) => {
                    warn!(key, %e, "failed to read peer registration");
                }
            }
        }

        Ok(peers)
    }

    async fn fetch_registration(&self, key: &str) -> anyhow::Result<NodeRegistration> {
        let body = self.client.get_object(&self.config.bucket, key).await?;
        let reg: NodeRegistration = serde_json::from_slice(&body)?;
        Ok(reg)
    }

    // ── Reconciliation loop ─────────────────────────────────────────

    pub async fn run_discovery_loop(
        self,
        manager: PeerManager,
        connector: PeerConnector,
    ) {
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
            let current: HashSet<NodeId> = manager
                .peer_ids().into_iter().collect();

            let desired_ids: HashSet<NodeId> = desired.keys().cloned().collect();

            for node_id in desired_ids.difference(&current) {
                if self.node_id >= *node_id {
                    debug!(%node_id, "skipping peer with lower or equal node_id");
                    continue;
                }

                let addr = desired.get(node_id).unwrap().addr;

                info!(%node_id, %addr, "discovered new peer — connecting");
                connector.add_peer(node_id.to_string(), addr).await;
            }

            for node_id in current.difference(&desired_ids) {
                info!(%node_id, "peer departed — disconnecting");
                connector.remove_peer(node_id.to_string()).await;
            }

            debug!(
                live = desired.len(),
                connected = manager.len(),
                "discovery reconciliation complete"
            );
        }
    }
}
