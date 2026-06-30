# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM rust:slim-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/client \
    && mkdir -p src/experiment \
    && echo "fn main() {}" > src/main.rs \
    && echo "fn main() {}" > src/client/crdt_client.rs \
    && echo "fn main() {}" > src/experiment/experiment.rs
RUN cargo build --release --bin crdt-server --bin crdt-client --bin experiment
RUN rm -rf src

COPY src ./src
RUN touch src/main.rs src/client/crdt_client.rs
RUN cargo build --release --bin crdt-server --bin crdt-client --bin experiment

COPY src ./src
RUN touch src/main.rs src/experiment/experiment.rs
RUN cargo build --release --bin crdt-server --bin crdt-client --bin experiment

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/crdt-server ./crdt-server
COPY --from=builder /app/target/release/crdt-client ./crdt-client
COPY --from=builder /app/target/release/experiment ./experiment

RUN useradd --no-create-home --shell /bin/false nonroot
USER nonroot

EXPOSE 9000
EXPOSE 9100

SHELL ["/bin/bash", "-c"]

CMD exec ./experiment \
      --listen-host   "$LISTEN_HOST" \
      --client-port   "$CLIENT_PORT" \
      ${NUM_GC_REPLICAS:+--num-gc-replicas "$NUM_GC_REPLICAS"} \
      ${NUM_NORMAL_REPLICAS:+--num-normal-replicas "$NUM_NORMAL_REPLICAS"} \
      --s3-endpoint   "$S3_ENDPOINT" \
      --s3-bucket     "$S3_BUCKET" \
      --s3-region     "$S3_REGION" \
      --s3-access-key "$S3_ACCESS_KEY" \
      --s3-secret-key "$S3_SECRET_KEY"
