# experimental_crdt

An experimental Rust project for replicated CRDT state with peer-to-peer synchronization and S3/MinIO-backed node discovery.

## What this repository contains

- `crdt-server`: runs a CRDT replication node.
- `crdt-client`: sends client commands (add/remove/inspect) to a server over TCP.
- OR-Set CRDT implementation and logical clock machinery.
- Peer discovery and membership coordination via S3-compatible storage.
- Optional GC-replica mode for GC coordination/storage flows.

## Prerequisites

- Rust toolchain (edition 2021; `cargo` available)
- Docker + Docker Compose (for local MinIO)

## Local setup

1. Start MinIO:

   ```bash
   docker compose up -d
   ```

2. Create a `.env` file in the repository root (used by Docker Compose and CLI env args), for example:

   ```bash
   AWS_ACCESS_KEY_ID=minioadmin
   AWS_SECRET_ACCESS_KEY=minioadmin
   S3_ENDPOINT=http://localhost:9000
   S3_BUCKET=crdt-peers
   S3_REGION=us-east-1
   ```

## Build and test

From repository root:

```bash
cargo build
cargo test
```

## Run a server node

Example:

```bash
cargo run --bin crdt-server -- \
  --listen 127.0.0.1:9000 \
  --advertise 127.0.0.1:9000 \
  --name node-a \
  --client-addr 127.0.0.1:9100 \
  --s3-endpoint http://localhost:9000 \
  --s3-bucket crdt-peers \
  --s3-region us-east-1 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin
```

To run multiple nodes, start additional `crdt-server` processes with unique `--listen`, `--advertise`, `--name`, and `--client-addr` values.

## Run client commands

Against a node exposing `--client-addr 127.0.0.1:9100`:

```bash
cargo run --bin crdt-client -- --addr 127.0.0.1:9100 add hello
cargo run --bin crdt-client -- --addr 127.0.0.1:9100 remove hello
cargo run --bin crdt-client -- --addr 127.0.0.1:9100 print-state
cargo run --bin crdt-client -- --addr 127.0.0.1:9100 print-internals
cargo run --bin crdt-client -- --addr 127.0.0.1:9100 print-matrix
```

## Notes

- The server uses `.env` variables via `dotenvy`, but all values can also be passed as CLI flags.
- The configured discovery bucket is created automatically when needed.
