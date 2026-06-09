use crate::common::NodeId;
use crate::server::OrSet;
use crate::engine::crdt_engine::CrdtEngine;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

pub type OutboundTasks = HashMap<NodeId, (SocketAddr, JoinHandle<()>)>;
pub type CrdtType = OrSet<String>;
pub type EngineType = CrdtEngine<CrdtType>;
