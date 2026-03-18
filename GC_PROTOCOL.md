# Epoch-Fenced Garbage Collection Protocol

This document describes the garbage collection protocol implemented for the CRDT replication system.

## Overview

The garbage collection (GC) protocol safely removes tombstones from OR-Set CRDTs after all replicas have observed them. This prevents unbounded memory growth while maintaining causal consistency.

## Protocol Design

### Core Concepts

1. **Epochs**: Global monotonic counters that divide time into discrete intervals
2. **Epoch Fencing**: Tombstones are tagged with their creation epoch
3. **Peer Tracking**: Each node tracks the minimum epoch acknowledged by all peers
4. **Safe Collection**: Tombstones can be garbage collected when all active replicas have advanced past their creation epoch + a safety margin

### Safety Guarantees

- **Causal Consistency**: Tombstones are never collected while any replica might need them
- **No Coordination Required**: Epoch advancement requires no consensus or voting
- **Partition Tolerance**: Nodes can advance epochs independently; collection is conservative
- **Rejoin Support**: Replicas that rejoin after extended downtime catch up via full sync

## Implementation

### Components

1. **GcCoordinator** (`src/gc.rs`)
   - Tracks current local epoch
   - Records peer epoch acknowledgments
   - Calculates safe collection epochs

2. **GC Background Tasks** (`src/gc_tasks.rs`)
   - Epoch advancement task: periodically advances local epoch and broadcasts to peers
   - Collection task: periodically checks for safe tombstones to collect

3. **Protocol Messages** (`src/proto/replication.proto`)
   - `EpochAnnounce`: broadcasts current epoch to peers
   - `CrdtOp.gc_epoch`: tags operations with creation epoch

4. **CRDT Integration** (`src/crdt/mod.rs`, `src/crdt/or_set.rs`)
   - `DeltaCrdt::garbage_collect()`: trait method for GC
   - OrSet stub implementation (full implementation pending)

### Configuration

```rust
pub struct GcConfig {
    /// How often to advance the local epoch (default: 30s)
    pub epoch_interval: Duration,
    /// How often to attempt garbage collection (default: 60s)
    pub gc_interval: Duration,
}
```

The safety margin is hard-coded to 2 epochs (`EPOCH_SAFETY_MARGIN = 2`).

### Algorithm

**Epoch Advancement** (every `epoch_interval`):
1. Increment local epoch counter
2. Broadcast `EpochAnnounce` to all connected peers
3. Log new epoch

**Garbage Collection** (every `gc_interval`):
1. Query safe collection epoch from GC coordinator
2. Calculate: `safe_epoch = min(all_peer_epochs)` if `min + SAFETY_MARGIN ≤ current_epoch`
3. Call `CRDT.garbage_collect(safe_epoch)` (when implemented)
4. Remove tombstones with `creation_epoch ≤ safe_epoch`

**Peer Tracking**:
- On receiving `EpochAnnounce(peer_id, epoch)`: update `peer_epochs[peer_id] = max(current, epoch)`
- On peer disconnect: remove peer from tracking
- Safe collection epoch calculation accounts for minimum peer epoch

## Example Timeline

```
Time  Node1 Epoch  Node2 Epoch  Node3 Epoch  Safe Collection
----  -----------  -----------  -----------  ----------------
 0s        0            0            0             -
30s        1            0            0             -
35s        1            1            1             -
60s        2            1            1             -
65s        2            2            2             0 (min=0+2≤2)
90s        3            2            2             0
95s        3            3            3             1 (min=1+2≤3)
```

When Node1 reaches epoch 3 and all nodes have acknowledged epoch 1+, tombstones from epoch 0 can be collected. The safety margin (2) ensures robustness against delayed messages.

## Future Work

- **Epoch-Tagged Tombstones**: Extend `OrSet.tombstones` to store creation epochs
- **Actual Collection**: Implement full `OrSet::garbage_collect()` method
- **Metrics**: Add Prometheus metrics for GC rates, tombstone counts, epoch lag
- **Dynamic Safety Margin**: Adjust based on observed network latency
- **Compaction**: Periodic full-state transfer for severely lagging nodes

## Testing

Tests are located in:
- `src/gc.rs`: Unit tests for GC coordinator logic
- Future: Integration tests for multi-node GC scenarios

Run tests: `cargo test`

## References

- Original CRDT design: src/crdt/or_set.rs
- DVV causality tracking: src/logical_clocks/dot_version_vector.rs
- Replication engine: src/crdt_engine.rs
