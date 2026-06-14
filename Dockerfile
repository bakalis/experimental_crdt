# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM rust:slim-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/client \
    && echo "fn main() {}" > src/main.rs \
    && echo "fn main() {}" > src/client/crdt_client.rs
RUN cargo build --release --bin crdt-server --bin crdt-client
RUN rm -rf src

COPY src ./src
RUN touch src/main.rs src/client/crdt_client.rs
RUN cargo build --release --bin crdt-server --bin crdt-client

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/crdt-server ./crdt-server
COPY --from=builder /app/target/release/crdt-client ./crdt-client

RUN useradd --no-create-home --shell /bin/false nonroot
USER nonroot

EXPOSE 9000
EXPOSE 9100

SHELL ["/bin/bash", "-c"]

# Set GC_REPLICA=true in docker-compose to append --gc-replica.
CMD exec ./crdt-server \
      --listen-host   "$LISTEN_HOST" \
      --listen-port   "$LISTEN_PORT" \
      --name          "$NODE_NAME" \
      --client-port   "$CLIENT_PORT" \
      --s3-endpoint   "$S3_ENDPOINT" \
      --s3-bucket     "$S3_BUCKET" \
      --s3-region     "$S3_REGION" \
      --s3-access-key "$S3_ACCESS_KEY" \
      --s3-secret-key "$S3_SECRET_KEY" \
      ${GC_REPLICA:+--gc-replica} \
      ${DISCOVERY_CONNECT_NODE_IDS:+--discovery-connect-node-ids "$DISCOVERY_CONNECT_NODE_IDS"}
