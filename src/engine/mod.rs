pub mod crdt_engine;
pub mod engine_inner;

use std::collections::HashMap;
use crate::common::NodeId;

pub struct PullResponse {
    pub delta_payload: Option<Vec<u8>>,
    pub knowledge_matrix: Option<HashMap<NodeId, HashMap<NodeId, u64>>>,
    pub our_knowledge_request: Option<HashMap<NodeId, u64>>,
}
