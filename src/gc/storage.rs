use std::collections::{HashSet, HashMap};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::common::{Counter, NodeId};
use crate::gc::GcStorageConfig;
use crate::storage::s3_client::S3Client;

/// Abstraction over the durable GC storage backend.
///
/// Every method that [`GcCoordinator`](super::coordinator::GcCoordinator) needs from a storage
/// backend is declared here so that alternative implementations (e.g. in-memory for tests) can be
/// swapped in without touching coordinator logic.
#[async_trait]
pub trait GcStorage: Send + Sync {

    async fn read_epoch_state(&self) -> anyhow::Result<EpochState>;

    async fn write_epoch_state(
        &self,
        epoch_state: EpochState
    ) -> anyhow::Result<()>;

    async fn read_final_dots(&self) -> anyhow::Result<HashMap<NodeId, Counter>>;

    async fn claim_gc_intent(&self, epoch: u64, initiator: &NodeId) -> anyhow::Result<bool>;

    async fn read_gc_intent(&self, epoch: u64) -> anyhow::Result<Option<GcIntentRecord>>;

    async fn release_gc_intent(&self, epoch: u64) -> anyhow::Result<()>;

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochState {
    pub epoch: u64,
    pub v_stable: HashMap<NodeId, Counter>,
    pub obsolete_dots: HashSet<NodeId>,
    pub state_payload: Vec<u8>,
    pub initiator_clock: HashMap<NodeId, Counter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcIntentRecord {
    pub epoch: u64,
    pub initiator: NodeId,
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

    pub fn gc_intent_key(&self, epoch: u64) -> String {
        self.key("gc_intent", &format!("_{epoch}.json"))
    }

    pub fn epoch_entry_key(&self) -> String {
        self.key("epoch_state", ".json")
    }

    pub fn final_dots_key_prefix(&self) -> String {
        self.key("final_dots", "/")
    }

    pub fn key(&self, namespace: &str, suffix: &str) -> String {
        format!(
            "{}/{}{}",
            self.config.prefix.trim_end_matches('/'),
            namespace,
            suffix
        )
    }

    fn parse_final_dot_key(&self, key: &str) -> Option<(NodeId, Counter)> {
        let prefix = format!(
            "{}/final_dots/",
            self.config.prefix.trim_end_matches('/')
        );
        if key.starts_with(&prefix) && key.ends_with(".json") {
            let raw = &key[prefix.len()..key.len() - ".json".len()];
            let parts: Vec<&str> = raw.split('_').collect();
            if parts.len() == 2 {
                let node_id = parts[0].to_string();
                if let Ok(counter) = parts[1].parse() {
                    return Some((node_id, counter));
                }
            }
        }
        None
    }
}

#[async_trait]
impl GcStorage for S3GcStorage {
    async fn read_epoch_state(&self) -> anyhow::Result<EpochState> {
        Ok(self.client.get_json_optional(self.bucket(), &self.epoch_entry_key()).await?.unwrap_or(EpochState {
            epoch: 0,
            v_stable: HashMap::new(),
            obsolete_dots: HashSet::new(),
            state_payload: Vec::new(),
            initiator_clock: HashMap::new(),
        }))
    }
    async fn write_epoch_state(
        &self,
        epoch_state: EpochState,
    ) -> anyhow::Result<()> {
        self.client.put_json(
            self.bucket(),
            &self.epoch_entry_key(),
            &epoch_state
        ).await
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

    async fn read_final_dots(&self) -> anyhow::Result<HashMap<NodeId, Counter>> {
        Ok(self.client.list_object_keys(self.bucket(), &self.final_dots_key_prefix()).await?
            .into_iter()
            .filter_map(|key| self.parse_final_dot_key(&key))
            .collect::<HashMap<_, _>>())
    }
}
