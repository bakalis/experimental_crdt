pub mod coordinator;
pub mod storage;

use std::time::Duration;

pub use coordinator::GcCoordinator;

#[derive(Debug, Clone)]
pub struct GcReplicaConfig {
    pub initiate_interval: Duration,
}

impl GcReplicaConfig {
    pub fn new(initiate_interval: Duration) -> Self {
        Self { initiate_interval }
    }
}

#[derive(Debug, Clone)]
pub struct GcStorageConfig {
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug, Clone)]
pub struct GcConfig {
    pub gc_replica: bool,
    pub observe_interval: Duration,
    pub storage_config: GcStorageConfig,
    pub gc_replica_config: Option<GcReplicaConfig>,
}
