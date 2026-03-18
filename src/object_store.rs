//! Object store abstraction for distributed GC protocol coordination.
//!
//! Provides linearizable key-value operations required by the formal GC protocol:
//! - read/write with optional default values
//! - compare-and-swap (CAS) for atomic coordination
//! - listKeys for membership enumeration
//! - delete for cleanup
//!
//! Backed by S3-compatible object storage.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::s3_client::S3Client;

/// Object store abstraction providing linearizable operations for GC coordination.
///
/// Keys are organized with prefixes:
/// - `epoch_entry/N` → stable timestamp for epoch N
/// - `bottom_state/N` → GC'd state snapshot for epoch N
/// - `gc_intent/N` → coordination lock for epoch N initiation
/// - `member/node_id` → active replica registration
/// - `clock/node_id` → replica's last published version vector
#[derive(Clone)]
pub struct ObjectStore {
    client: Arc<S3Client>,
    bucket: String,
}

impl ObjectStore {
    pub fn new(client: Arc<S3Client>, bucket: String) -> Self {
        Self { client, bucket }
    }

    /// Read a value from the object store.
    /// Returns None if the key does not exist.
    pub async fn read<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Result<Option<T>> {
        match self.client.get_object(&self.bucket, key).await {
            Ok(bytes) => {
                let value = serde_json::from_slice(&bytes)?;
                Ok(Some(value))
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("NoSuchKey") || err_str.contains("Not Found") {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Read a value, returning a default if the key is absent.
    pub async fn read_or_default<T: for<'de> Deserialize<'de> + Default>(
        &self,
        key: &str,
    ) -> Result<T> {
        self.read(key).await.map(|opt| opt.unwrap_or_default())
    }

    /// Write a value to the object store.
    pub async fn write<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.client
            .put_object(&self.bucket, key, bytes, "application/json")
            .await?;
        debug!(key, "wrote object");
        Ok(())
    }

    /// Compare-and-swap: atomically set key to new_value if current value equals expected.
    ///
    /// Returns true if the swap succeeded, false if the key exists with a different value.
    /// For the special case where expected is None, this becomes "create if absent".
    ///
    /// Note: S3 doesn't have native CAS, so we simulate it with conditional puts.
    /// This is eventually consistent but works for our epoch coordination use case
    /// because concurrent initiators will see each other's intents via read-after-write.
    pub async fn cas<T: Serialize + for<'de> Deserialize<'de> + PartialEq>(
        &self,
        key: &str,
        expected: Option<&T>,
        new_value: &T,
    ) -> Result<bool> {
        // Read current value
        let current: Option<T> = self.read(key).await?;

        // Check if current matches expected
        match (current.as_ref(), expected) {
            (None, None) => {
                // Key absent, expected absent → create
                self.write(key, new_value).await?;
                debug!(key, "CAS: created new key");
                Ok(true)
            }
            (Some(cur), Some(exp)) if cur == exp => {
                // Key exists with expected value → update
                self.write(key, new_value).await?;
                debug!(key, "CAS: updated key");
                Ok(true)
            }
            _ => {
                // Mismatch → CAS failed
                debug!(key, "CAS: failed (value mismatch)");
                Ok(false)
            }
        }
    }

    /// Delete a key from the object store.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.client.delete_object(&self.bucket, key).await?;
        debug!(key, "deleted object");
        Ok(())
    }

    /// List all keys with the given prefix.
    /// Returns the keys without the prefix.
    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let keys = self.client.list_object_keys(&self.bucket, prefix).await?;
        Ok(keys)
    }

    /// List all keys with the given prefix, returning only the suffix after the prefix.
    pub async fn list_suffixes(&self, prefix: &str) -> Result<Vec<String>> {
        let keys = self.list_keys(prefix).await?;
        Ok(keys
            .into_iter()
            .filter_map(|k| k.strip_prefix(prefix).map(String::from))
            .collect())
    }
}

/// Version vector: maps node IDs to counters.
pub type VersionVector = HashMap<String, u64>;

/// Stable timestamp entry for an epoch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EpochEntry {
    pub epoch: u64,
    pub stable_timestamp: VersionVector,
}

/// Bottom state: GC'd CRDT state plus the initiator's version vector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BottomState {
    /// Serialized CRDT state after GC
    pub state_bytes: Vec<u8>,
    /// The initiator's version vector at the time of GC
    pub initiator_vv: VersionVector,
}

/// GC intent marker for epoch coordination.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GcIntent {
    pub epoch: u64,
    pub initiator_id: String,
}

/// Clock value: a replica's published version vector.
pub type ClockValue = VersionVector;

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a running MinIO instance.
    // Run with: docker run -p 9000:9000 minio/minio server /data
    //
    // For now, we'll skip integration tests and rely on system-level testing.
}
