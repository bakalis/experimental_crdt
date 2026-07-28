# experimental_crdt

An experimental Rust project for replicated OR-Set CRDT state, S3/MinIO-backed peer discovery, and GC convergence experiments.

## Binaries

`Cargo.toml` defines three binaries:

- `crdt-server` (`src/main.rs`): regular CRDT replica process.
- `crdt-client` (`src/client/crdt_client.rs`): CLI client for sending operations/workloads.
- `experiment` (`src/experiment/experiment.rs`): multi-replica experiment runner using the simulated network.

## Prerequisites

- Rust toolchain (edition 2021)
- Docker + Docker Compose (optional, for containerized runs)
- Access to an S3-compatible endpoint (for local development: MinIO)

## Build

From repository root:

```bash
cargo build
```

## Environment variables

The binaries use `clap` with env support, and `crdt-server` / `experiment` also load `.env` via `dotenvy`.

Common variables:

- `S3_ENDPOINT` (required): e.g. `http://localhost:9000`
- `S3_BUCKET` (default: `crdt-peers`)
- `S3_REGION` (default: `us-east-1`)
- `AWS_ACCESS_KEY_ID` (required)
- `AWS_SECRET_ACCESS_KEY` (required)
- `METRICS_FILE_PATH` (required by all binaries)

Example `.env`:

```bash
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://localhost:9000
S3_BUCKET=crdt-peers
S3_REGION=us-east-1
METRICS_FILE_PATH=./logs/metrics.jsonl
```

## Regular server/client usage

### 1) Run a regular CRDT server (`crdt-server`)

```bash
cargo run --bin crdt-server -- \
  --listen-host 127.0.0.1 \
  --listen-port 9000 \
  --name node-a \
  --client-port 9100 \
  --metrics-path ./logs/server-node-a.jsonl \
  --s3-endpoint http://localhost:9000 \
  --s3-bucket crdt-peers \
  --s3-region us-east-1 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin
```

Useful optional flags:

- `--gc-replica`
- `--discovery-interval-secs <n>`
- `--registration-ttl-secs <n>`
- `--discovery-connect-node-ids id1,id2,...`
- `--gc-prefix <prefix>`
- `--gc-initiate-interval-secs <n>`
- `--gc-observe-interval-secs <n>`

### 2) Run the client (`crdt-client`)

`crdt-client` requires `--addr` and one subcommand.

Interactive mode:

```bash
cargo run --bin crdt-client -- \
  --addr 127.0.0.1:9100 \
  --metrics-path ./logs/client-interactive.jsonl \
  interactive
```

Then type commands in the prompt:

- `add <value>`
- `remove <value>`
- `remove-random`
- `print-state`
- `print-internals`
- `print-matrix`
- `quit`

Benchmark mode:

```bash
cargo run --bin crdt-client -- \
  --addr 127.0.0.1:9100 \
  --metrics-path ./logs/client-bench.jsonl \
  bench --requests 1000 --sleep-ms 10 --remove-chance 0.3 --key-space 100000
```

One request per discovered server name:

```bash
cargo run --bin crdt-client -- \
  --addr 127.0.0.1:9100 \
  --metrics-path ./logs/client-once.jsonl \
  one-request-per-server --num-gc-servers 2 --num-normal-servers 8
```

## Experiment binary (`experiment`)

`experiment` launches many in-process replicas and records metrics for convergence analysis.

### What it does

- Creates `num_gc_replicas` GC replicas (`gc-server-1`, `gc-server-2`, ...)
- Creates `num_normal_replicas` normal replicas (`normal-server-1`, ...)
- Uses the simulated network internally (no per-replica Docker services required)
- Still uses S3/MinIO for discovery + GC storage

### Run directly with Cargo

```bash
cargo run --bin experiment -- \
  --listen-host 127.0.0.1 \
  --metrics-path ./logs/experiment.jsonl \
  --num-gc-replicas 2 \
  --num-normal-replicas 8 \
  --s3-endpoint http://localhost:9000 \
  --s3-bucket crdt-peers \
  --s3-region us-east-1 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin
```

Important flags/env for experiment runs:

- `--listen-host` / `LISTEN_HOST`: base host used for generated replica addresses
- `--num-gc-replicas` / `NUM_GC_REPLICAS`: number of GC replicas
- `--num-normal-replicas` / `NUM_NORMAL_REPLICAS`: number of normal replicas
- `--client-port` / `CLIENT_PORT`: accepted by CLI (kept for compatibility)
- `--metrics-path` / `METRICS_FILE_PATH`: output JSONL metrics path
- same S3 vars as `crdt-server`

### Run with Docker Compose

Repository `docker-compose.yml` runs one containerized experiment process (`experiment-1`) from `experiment.dockerfile`.

```bash
docker compose up --build
```

How this compose setup works:

- Builds and runs the `experiment` binary, not separate `crdt-server` containers.
- Passes environment variables into the container (`LISTEN_HOST`, `NUM_GC_REPLICAS`, `NUM_NORMAL_REPLICAS`, S3 vars, `METRICS_FILE_PATH`).
- Mounts `./logs/experiment-1/` to `/logs/` so metrics persist on the host.
- Exposes `9000/9100` from the container as `9110/9111` on host.

Notes:

- The provided compose file does **not** include a MinIO service; point `S3_ENDPOINT` to a reachable S3/MinIO endpoint.
- If your S3 endpoint is on the host machine, use an address reachable from inside Docker (for example `host.docker.internal` on supported platforms).

## Metrics analysis and visualization pipeline

The data-processing flow is:

1. `analyze_metrics.py`
2. `reports_to_csv_summary.py`
3. `visualize_metrics.py`

Run from `/home/runner/work/experimental_crdt/experimental_crdt/data_processing`.

### 1) Analyze raw metrics JSONL into text report(s)

```bash
python analyze_metrics.py /path/to/metrics.jsonl > overlay_128.txt
```

`analyze_metrics.py` reads one interleaved `metrics.jsonl` and prints:

- per-GC and per-normal server sections
- GC summary
- normal summary (when present)
- final combined totals (GC + normal)

The parser in the next step expects report files named like:

- `overlay_128.txt`
- `overlay_50.txt`
- `fullmesh_128.txt`
- `fullmesh_50.txt`

### 2) Convert report text files to aggregated CSV

```bash
python reports_to_csv_summary.py . --output summary.csv
```

Options:

- positional `directory`: where `overlay_*.txt` / `fullmesh_*.txt` files are
- `-o, --output`: output CSV path (default `report_summary.csv`)
- `-r, --recursive`: search subdirectories

This produces one row per scenario in a machine-friendly CSV.

### 3) Generate plots from the CSV

```bash
python visualize_metrics.py summary.csv --out graphs
```

Outputs:

- PNG + HTML for each chart
- defaults to `graphs/` next to the CSV when `--out` is omitted

Current visualizations include dissemination rounds, total sent bytes comparisons, GC own-stability bandwidth, and overlay message-size comparisons.

## Test

From repository root:

```bash
cargo test
```
