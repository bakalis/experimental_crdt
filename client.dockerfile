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

SHELL ["/bin/bash", "-c"]

CMD exec ./crdt-client \
      --addr   "$SERVER_ADDR" \
      bench \
      ${TOTAL_REQUESTS:+--requests "$TOTAL_REQUESTS"} \
      ${SLEEP_MS:+--sleep-ms "$SLEEP_MS"}
