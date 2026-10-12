FROM rust:1-bookworm AS builder
# SQLx links system sqlite3 while Grammers/libsql bundles its own engine.
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY Cargo.toml Cargo.toml
COPY .cargo .cargo
COPY src src
COPY static static
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --home-dir /app bridge
WORKDIR /app
COPY --from=builder /work/target/release/two-bm /usr/local/bin/two-bm
RUN mkdir /data && chown bridge:bridge /data
USER bridge
VOLUME ["/data"]
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/two-bm"]
