use crate::common::NodeId;
use crate::server::OrSet;
use crate::engine::crdt_engine::CrdtEngine;
use std::collections::HashMap;
use tokio::task::JoinHandle;

pub type OutboundTasks = HashMap<NodeId, JoinHandle<()>>;
pub type CrdtType = OrSet<String>;
pub type EngineType = CrdtEngine<CrdtType>;
