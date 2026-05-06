use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::common::{Counter, NodeId};
use crate::s3_client::S3Client;

/// Abstraction over the durable GC storage backend.
///
/// Every method that [`GcCoordinator`](super::coordinator::GcCoordinator) needs from a storage
/// backend is declared here so that alternative implementations (e.g. in-memory for tests) can be
/// swapped in without touching coordinator logic.
#[async_trait]
pub trait GcStorage: Send + Sync {
    async fn write_clock(
        &self,
        node_id: &NodeId,
        clock: &HashMap<NodeId, Counter>,
    ) -> anyhow::Result<()>;

    async fn read_clock(
        &self,
        node_id: &NodeId,
    ) -> anyhow::Result<Option<HashMap<NodeId, Counter>>>;

    async fn delete_clock(&self, node_id: &NodeId) -> anyhow::Result<()>;

    async fn list_clock_replicas(&self) -> anyhow::Result<Vec<NodeId>>;

    async fn latest_epoch_entry(&self) -> anyhow::Result<u64>;

    async fn read_epoch_entry(
        &self,
        epoch: u64,
    ) -> anyhow::Result<HashMap<NodeId, Counter>>;

    async fn write_epoch_entry(
        &self,
        epoch: u64,
        stable: &HashMap<NodeId, Counter>,
    ) -> anyhow::Result<()>;

    async fn write_epoch_entry_if_absent(
        &self,
        epoch: u64,
        stable: &HashMap<NodeId, Counter>,
    ) -> anyhow::Result<bool>;

    async fn latest_bottom_state_epoch(&self) -> anyhow::Result<u64>;

    async fn read_bottom_state(&self, epoch: u64) -> anyhow::Result<Option<BottomStateRecord>>;

    async fn write_bottom_state(
        &self,
        epoch: u64,
        record: &BottomStateRecord,
    ) -> anyhow::Result<()>;

    async fn write_bottom_state_if_absent(
        &self,
        epoch: u64,
        record: &BottomStateRecord,
    ) -> anyhow::Result<bool>;

    async fn claim_gc_intent(&self, epoch: u64, initiator: &NodeId) -> anyhow::Result<bool>;

    async fn read_gc_intent(&self, epoch: u64) -> anyhow::Result<Option<GcIntentRecord>>;

    async fn release_gc_intent(&self, epoch: u64) -> anyhow::Result<()>;

    async fn cleanup_before_epoch(&self, latest: u64) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct GcStorageConfig {
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcIntentRecord {
    pub epoch: u64,
    pub initiator: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BottomStateRecord {
    pub state_payload: Vec<u8>,
    pub initiator_clock: HashMap<NodeId, Counter>,
}

#[derive(Clone)]
pub struct S3GcStorage {
    client: S3Client,
    config: GcStorageConfig,
}

impl S3GcStorage {
    pub fn new(client: S3Client, config: GcStorageConfig) -> Self {
        Self { client, config }
    }

    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }

    pub fn clock_key(&self, node_id: &str) -> String {
        self.key("clock", &format!("{node_id}.json"))
    }

    pub fn gc_intent_key(&self, epoch: u64) -> String {
        self.key("gc_intent", &format!("{epoch}.json"))
    }

    pub fn epoch_entry_key(&self, epoch: u64) -> String {
        self.key("epoch_entry", &format!("{epoch}.json"))
    }

    pub fn bottom_state_key(&self, epoch: u64) -> String {
        self.key("bottom_state", &format!("{epoch}.json"))
    }

    pub fn key(&self, namespace: &str, suffix: &str) -> String {
        format!(
            "{}/{}/{}",
            self.config.prefix.trim_end_matches('/'),
            namespace,
            suffix
        )
    }

    pub async fn list_epoch_entries(&self) -> anyhow::Result<Vec<u64>> {
        self.list_epoch_namespace("epoch_entry").await
    }

    pub async fn list_bottom_state_epochs(&self) -> anyhow::Result<Vec<u64>> {
        self.list_epoch_namespace("bottom_state").await
    }

    async fn list_epoch_namespace(&self, namespace: &str) -> anyhow::Result<Vec<u64>> {
        let prefix = format!(
            "{}/{}/",
            self.config.prefix.trim_end_matches('/'),
            namespace
        );
        let keys = self.client.list_object_keys(self.bucket(), &prefix).await?;
        let mut epochs: Vec<u64> = keys
            .iter()
            .filter_map(|k| self.parse_epoch_key(k, namespace))
            .collect();
        epochs.sort_unstable();
        epochs.dedup();
        Ok(epochs)
    }

    fn parse_epoch_key(&self, key: &str, namespace: &str) -> Option<u64> {
        let prefix = format!(
            "{}/{}/",
            self.config.prefix.trim_end_matches('/'),
            namespace
        );
        if key.starts_with(&prefix) && key.ends_with(".json") {
            return key[prefix.len()..key.len() - ".json".len()].parse().ok();
        }
        None
    }

    fn parse_node_key(&self, key: &str, namespace: &str) -> Option<NodeId> {
        let prefix = format!(
            "{}/{}/",
            self.config.prefix.trim_end_matches('/'),
            namespace
        );
        if key.starts_with(&prefix) && key.ends_with(".json") {
            let raw = &key[prefix.len()..key.len() - ".json".len()];
            if !raw.is_empty() {
                return Some(raw.to_string());
            }
        }
        None
    }

    pub fn ensure_configured(&self) -> anyhow::Result<()> {
        (!self.config.bucket.is_empty())
            .then_some(())
            .context("GC bucket is empty")
    }
}

#[async_trait]
impl GcStorage for S3GcStorage {
    async fn write_clock(
        &self,
        node_id: &NodeId,
        clock: &HashMap<NodeId, Counter>,
    ) -> anyhow::Result<()> {
        self.client
            .put_json(self.bucket(), &self.clock_key(node_id), clock)
            .await
    }

    async fn read_clock(
        &self,
        node_id: &NodeId,
    ) -> anyhow::Result<Option<HashMap<NodeId, Counter>>> {
        self.client
            .get_json_optional(self.bucket(), &self.clock_key(node_id))
            .await
    }

    async fn delete_clock(&self, node_id: &NodeId) -> anyhow::Result<()> {
        self.client
            .delete_object(self.bucket(), &self.clock_key(node_id))
            .await
    }

    async fn list_clock_replicas(&self) -> anyhow::Result<Vec<NodeId>> {
        let prefix = format!("{}/clock/", self.config.prefix.trim_end_matches('/'));
        let keys = self.client.list_object_keys(self.bucket(), &prefix).await?;
        let mut out = Vec::new();
        for key in keys {
            if let Some(node) = self.parse_node_key(&key, "clock") {
                out.push(node);
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn latest_epoch_entry(&self) -> anyhow::Result<u64> {
        Ok(self.list_epoch_entries().await?.into_iter().max().unwrap_or(0))
    }

    async fn read_epoch_entry(&self, epoch: u64) -> anyhow::Result<HashMap<NodeId, Counter>> {
        Ok(self
            .client
            .get_json_optional::<HashMap<NodeId, Counter>>(
                self.bucket(),
                &self.epoch_entry_key(epoch),
            )
            .await?
            .unwrap_or_default())
    }

    async fn write_epoch_entry(
        &self,
        epoch: u64,
        stable: &HashMap<NodeId, Counter>,
    ) -> anyhow::Result<()> {
        self.client
            .put_json(self.bucket(), &self.epoch_entry_key(epoch), stable)
            .await
    }

    async fn write_epoch_entry_if_absent(
        &self,
        epoch: u64,
        stable: &HashMap<NodeId, Counter>,
    ) -> anyhow::Result<bool> {
        self.client
            .put_json_if_absent(self.bucket(), &self.epoch_entry_key(epoch), stable)
            .await
    }

    async fn latest_bottom_state_epoch(&self) -> anyhow::Result<u64> {
        Ok(self
            .list_bottom_state_epochs()
            .await?
            .into_iter()
            .max()
            .unwrap_or(0))
    }

    async fn read_bottom_state(&self, epoch: u64) -> anyhow::Result<Option<BottomStateRecord>> {
        self.client
            .get_json_optional(self.bucket(), &self.bottom_state_key(epoch))
            .await
    }

    async fn write_bottom_state(
        &self,
        epoch: u64,
        record: &BottomStateRecord,
    ) -> anyhow::Result<()> {
        self.client
            .put_json(self.bucket(), &self.bottom_state_key(epoch), record)
            .await
    }

    async fn write_bottom_state_if_absent(
        &self,
        epoch: u64,
        record: &BottomStateRecord,
    ) -> anyhow::Result<bool> {
        self.client
            .put_json_if_absent(self.bucket(), &self.bottom_state_key(epoch), record)
            .await
    }

    async fn claim_gc_intent(&self, epoch: u64, initiator: &NodeId) -> anyhow::Result<bool> {
        self.client
            .put_json_if_absent(
                self.bucket(),
                &self.gc_intent_key(epoch),
                &GcIntentRecord {
                    epoch,
                    initiator: initiator.clone(),
                },
            )
            .await
    }

    async fn read_gc_intent(&self, epoch: u64) -> anyhow::Result<Option<GcIntentRecord>> {
        self.client
            .get_json_optional(self.bucket(), &self.gc_intent_key(epoch))
            .await
    }

    async fn release_gc_intent(&self, epoch: u64) -> anyhow::Result<()> {
        self.client
            .delete_object(self.bucket(), &self.gc_intent_key(epoch))
            .await
    }

    async fn cleanup_before_epoch(&self, latest: u64) -> anyhow::Result<()> {
        for epoch in 0..latest {
            let epoch_key = self.epoch_entry_key(epoch);
            let bottom_key = self.bottom_state_key(epoch);
            self.client.delete_object(self.bucket(), &epoch_key).await.ok();
            self.client.delete_object(self.bucket(), &bottom_key).await.ok();
        }
        Ok(())
    }
}
