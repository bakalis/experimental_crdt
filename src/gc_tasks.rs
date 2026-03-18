//! Background GC tasks for epoch advancement and tombstone collection.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::crdt::DeltaCrdt;
use crate::crdt_engine::CrdtEngine;
use crate::gc::{GcConfig, GcCoordinator};
use crate::peer_registry::PeerRegistry;
use crate::proto::{self, envelope::Payload, Envelope};

/// Start background GC tasks for the given engine.
///
/// Returns two join handles:
/// - Epoch advancement task (periodically advances the local epoch)
/// - Garbage collection task (periodically collects safe tombstones)
pub fn start_gc_tasks<C: DeltaCrdt>(
    engine: CrdtEngine<C>,
    config: GcConfig,
    peer_registry: PeerRegistry,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let epoch_handle = start_epoch_advancement_task(
        engine.clone(),
        config.epoch_interval,
        peer_registry,
    );

    let gc_handle = start_garbage_collection_task(
        engine,
        config.gc_interval,
    );

    (epoch_handle, gc_handle)
}

/// Background task that periodically advances the local GC epoch and
/// broadcasts announcements to all peers.
fn start_epoch_advancement_task<C: DeltaCrdt>(
    engine: CrdtEngine<C>,
    interval: Duration,
    peer_registry: PeerRegistry,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            let gc_coordinator = engine.gc_coordinator().await;
            let new_epoch = gc_coordinator.advance_epoch().await;

            // Get node_id snapshot from coordinator
            let peer_snapshot = gc_coordinator.peer_epoch_snapshot();

            // Broadcast epoch announcement to all peers
            let announcement = Envelope {
                payload: Some(Payload::EpochAnnounce(proto::EpochAnnounce {
                    epoch: new_epoch,
                    node_id: String::from("node"), // Will be filled by server
                })),
            };

            peer_registry.broadcast(announcement).await;

            debug!(
                epoch = new_epoch,
                peers = peer_registry.len(),
                "broadcast epoch announcement"
            );
        }
    })
}

/// Background task that periodically attempts to garbage collect tombstones.
fn start_garbage_collection_task<C: DeltaCrdt>(
    engine: CrdtEngine<C>,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            // Placeholder for future GC logic
            // TODO: Implement actual garbage collection when OrSet supports it
            let gc_coordinator = engine.gc_coordinator().await;
            let safe_epoch = gc_coordinator.safe_collection_epoch().await;

            if let Some(epoch) = safe_epoch {
                info!(
                    safe_epoch = epoch,
                    "GC: safe collection epoch determined (collection not yet implemented)"
                );
                // TODO: Call engine.garbage_collect(safe_epoch) when implemented
            } else {
                debug!("GC: no safe collection epoch yet");
            }
        }
    })
}
