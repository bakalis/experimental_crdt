# Formal Garbage Collection Protocol

This document describes the formal garbage collection protocol implemented for the CRDT replication system, based on object store coordination.

## Overview

The garbage collection (GC) protocol safely removes tombstones from OR-Set CRDTs after all replicas have observed them. This prevents unbounded memory growth while maintaining causal consistency. The protocol uses an object store with linearizable operations to coordinate GC epochs without requiring consensus.

## Protocol Design

### Core Concepts

1. **Epochs**: Global monotonic counters (u64) that divide time into discrete GC rounds
2. **Object Store Coordination**: Uses linearizable key-value operations with CAS for atomic coordination
3. **Stable Timestamps**: Version vectors representing the causally stable frontier
4. **Membership Tracking**: Replicas register in the object store and publish clocks
5. **Safe Collection**: Conservative collection based on stable timestamps and epoch fencing

### Safety Guarantees

- **Causal Consistency**: Tombstones are never collected while any replica might need them
- **No Consensus Required**: Epoch advancement uses CAS, not distributed consensus
- **Partition Tolerance**: Replicas can operate independently; collection is conservative
- **Closed-World Membership**: Membership changes during epoch initiation cause abort
- **Strict Progress**: GC only proceeds when the stable timestamp strictly advances

## Object Store Keys

The protocol uses the following key prefixes in the object store:

- **`epoch_entry/N`**: Stable timestamp (version vector) for epoch N
- **`bottom_state/N`**: GC'd CRDT state for epoch N (serialized state + initiator VV)
- **`gc_intent/N`**: Intent marker for coordinating epoch N initiation (CAS barrier)
- **`member/node_id`**: Membership marker for active replicas
- **`clock/node_id`**: Version vector published by each replica

## Protocol Procedures

### Normal Operation

#### LocalWrite (at Replica r)

```
PROCEDURE LocalWrite(r, operation op)
  dot.counter ← dot.counter + 1
  state ← apply(state, op, dot)
  write(clock[r], V)          // Periodically; not required on every write
END PROCEDURE
```

Implemented in `GcReplica::local_write()`.

#### MergeDelta (at Replica r)

```
PROCEDURE MergeDelta(r, message (δ, V_peer))
  state ← state ⊔ δ
  V ← V ⊔ V_peer
END PROCEDURE
```

Implemented in `GcReplica::merge_delta()`.

**Remark**: Delta exchange is unconditionally safe. Every dot in a delta generated after epoch N satisfies `d ≰ V_stable^N`, so deltas and GC operate on structurally disjoint dot sets.

### Computing the Stable Timestamp

```
FUNCTION ComputeStableTimestamp(Members)
  RETURN ⊓_{r ∈ Members} readOrZero(clock[r])  // Missing clock treated as 0
END FUNCTION
```

Implemented in `GcReplica::compute_stable_timestamp()`.

**Remark**: Missing clocks are treated as 0. This can happen when a new replica completes membership registration but hasn't yet written its clock. The resulting `V_stable = 0` causes the strict progress check to abort the epoch safely.

### GC Initiation

```
PROCEDURE InitiateGC(r)
  ObserveEpochChange(r)                 // Ensure r is fully current
  N_latest ← max{N | epoch_entry[N] is present}
  IF epoch ≠ N_latest THEN
    RETURN                               // r is behind; abort
  END IF

  N ← epoch + 1

  // --- Closed-world check ---
  Members_0 ← listKeys(member/)
  IF ¬cas(gc_intent[N], ⊥, N) THEN
    RETURN                               // Another replica claimed epoch; abort
  END IF
  Members_1 ← listKeys(member/)
  IF Members_1 ≠ Members_0 THEN
    delete(gc_intent[N])
    RETURN                               // Membership changed; abort
  END IF

  // --- Strict progress check ---
  V_stable ← ComputeStableTimestamp(Members_1)
  V_prev ← read(epoch_entry[N-1])
  IF V_stable ≤ V_prev THEN
    delete(gc_intent[N])
    RETURN                               // No advancement; abort
  END IF

  // --- Commit epoch ---
  state_gc ← gc(state, V_stable)
  write(epoch_entry[N], V_stable)
  write(bottom_state[N], (state_gc, V))

  // --- Apply GC locally ---
  state ← state_gc
  epoch ← N
  delete(gc_intent[N])                   // Release barrier
END PROCEDURE
```

Implemented in `GcReplica::initiate_gc()`.

**Remarks**:

- **Initiator Currency Enforcement**: `ObserveEpochChange()` ensures the initiator is current before proceeding
- **Closed-World Without Wait**: Reading membership before and after CAS eliminates need for wait period
- **Strict Progress**: The check `V_stable ≤ V_prev` ensures GC only proceeds when thresholds advance
- **CAS Coordination**: Multiple initiators can attempt simultaneously; only one succeeds via CAS

### Joining a New Replica

```
PROCEDURE Bootstrap(r_new)
  write(member[r_new])                   // Register freely; no coordination needed
  N_latest ← max{N | bottom_state[N] is present}

  IF read(gc_intent[N_latest + 1]) ≠ ⊥ THEN
    WAIT until bottom_state[N_latest + 1] is present
    N_latest ← N_latest + 1
  END IF

  (s_0, V_initiator) ← read(bottom_state[N_latest])
  V_stable ← read(epoch_entry[N_latest])

  state ← s_0
  V ← V_initiator ⊔ V_stable             // Cover all dots in s_0
  dot ← (r_new, 0)
  epoch ← N_latest
  write(clock[r_new], V)
END PROCEDURE
```

Implemented in `GcReplica::bootstrap()`.

**Remarks**:

- **Intent Check**: Only checks for `gc_intent[N_latest + 1]` specifically (not arbitrary intents)
- **Clock Write Timing**: Clock is written as final step; there's a window where member exists but clock absent
- **Safe Abort**: If new replica's clock is missing during GC, `ComputeStableTimestamp` returns 0, causing abort

### Observing an Epoch Change

```
PROCEDURE ObserveEpochChange(r)
  N_latest ← max{N | epoch_entry[N] is present}
  IF N_latest ≤ epoch THEN
    RETURN                               // Already up to date
  END IF

  V_stable ← read(epoch_entry[N_latest])
  state ← gc(state, V_stable)            // GC local state to new threshold
  epoch ← N_latest
  write(clock[r], V)                     // Publish current clock
END PROCEDURE
```

Implemented in `GcReplica::observe_epoch_change()`.

**Remarks**:

- **Single-Step Catch-Up**: A replica jumps directly to `N_latest` regardless of how many epochs missed
- **No Bottom State Merge**: Operations from initiator arrive via normal delta exchange
- **Clock Soundness**: GC only removes dots, so Clock Soundness is preserved

### Epoch Entry Cleanup

```
PROCEDURE Cleanup()
  N_latest ← max{N | epoch_entry[N] is present}
  FOR each N < N_latest DO
    delete(epoch_entry[N])
    delete(bottom_state[N])
  END FOR
END PROCEDURE
```

Implemented in `GcReplica::cleanup()`.

## Implementation

### Components

1. **ObjectStore** (`src/object_store.rs`)
   - Wraps S3-compatible object storage
   - Provides linearizable operations: read, write, CAS, delete, listKeys
   - Handles missing keys and serialization

2. **GcReplica** (`src/gc_protocol.rs`)
   - Main protocol implementation
   - Owns CRDT state, DVV, and object store reference
   - Implements all protocol procedures
   - Generic over CRDT type `C: DeltaCrdt`

3. **GcCoordinator** (`src/gc.rs`) - DEPRECATED in favor of formal protocol
   - Legacy peer epoch tracking (kept for backward compatibility)
   - Will be removed in future version

4. **GC Background Tasks** (`src/gc_tasks.rs`)
   - Epoch advancement task: calls `GcReplica::initiate_gc()` periodically
   - Collection happens as part of epoch initiation
   - Broadcasts epoch announcements to peers (legacy, to be updated)

### Configuration

The protocol is parameterless at the algorithmic level. Implementation-specific parameters:

```rust
pub struct GcConfig {
    /// How often to attempt GC initiation (default: 60s)
    pub gc_interval: Duration,
}
```

Trigger policy (see Remark on GC Trigger Policy):
- Minimum time interval between GC attempts
- Exponential backoff on abort
- Single designated initiator (recommended)

### Example Timeline

```
Time  Node1 Epoch  Node2 Epoch  Node3 Epoch  Action
----  -----------  -----------  -----------  ------
 0s        0            0            0       Nodes write clocks
30s        0            0            0       Node1 initiates GC epoch 1
30s        1            0            0       Node1 commits epoch 1
35s        1            1            1       All nodes observe epoch 1
60s        1            1            1       Node2 initiates GC epoch 2
60s        2            1            1       Node2 commits epoch 2
65s        2            2            2       All nodes observe epoch 2
```

Each epoch advancement:
1. Reads membership (closed-world check)
2. CAS to claim epoch (atomic coordination)
3. Computes stable timestamp from clocks
4. Checks strict progress
5. Commits epoch_entry and bottom_state
6. Applies GC locally

## Testing

Tests are located in:
- `src/gc_protocol.rs`: Unit tests for version vector comparison
- `src/gc.rs`: Unit tests for legacy GC coordinator (to be deprecated)
- Integration tests: TODO

Run tests: `cargo test`

## Comparison to Previous Design

The previous implementation used peer epoch tracking with simple safety margins. The formal protocol provides:

- **Stronger guarantees**: Mathematically proven safety via invariants
- **Explicit coordination**: CAS-based epoch claiming prevents races
- **Membership awareness**: Closed-world check prevents missing replicas
- **Strict progress**: Ensures GC only when causally safe
- **Object store primitives**: Enables distributed coordination without consensus

## Future Work

- **CRDT State Serialization**: Implement serialization/deserialization of CRDT state in bottom_state
- **GC Trigger Policy**: Implement sophisticated trigger policies (time-based, dot accumulation, backoff)
- **Metrics**: Add Prometheus metrics for epoch lag, tombstone counts, GC rates
- **Integration Tests**: Multi-node scenarios with object store
- **Designated Initiator**: Optional configuration for single-initiator mode
- **Cleanup Automation**: Periodic background cleanup of old epoch entries

## References

- Algorithmic notation: See problem statement
- CRDT design: `src/crdt/or_set.rs`
- DVV causality tracking: `src/logical_clocks/dot_version_vector.rs`
- Replication engine: `src/crdt_engine.rs`
- Object store: `src/object_store.rs`
