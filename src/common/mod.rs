#![allow(dead_code)]

use std::net::SocketAddr;

pub mod error;

pub type NodeId = String;
pub type Counter = u64;

pub async fn lookup(addr: &str) -> anyhow::Result<SocketAddr> {
    tokio::net::lookup_host(addr)
        .await?
        .next()
        .ok_or_else(|| anyhow::anyhow!("could not resolve address: {addr}"))
}
