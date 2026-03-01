FROM rust:1.83-bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

ENV GITALY_PROTO_ROOT=/build/proto

RUN cargo build --release --workspace

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/gitaly    /usr/local/bin/gitaly
COPY --from=builder /build/target/release/gitaly-rs /usr/local/bin/gitaly-rs

COPY docker/config/gitaly-rs.toml /etc/gitaly/gitaly-rs.toml
COPY docker/config/gateway.toml   /etc/gitaly/gateway.toml
COPY docker/entrypoint.sh         /usr/local/bin/entrypoint.sh

VOLUME /var/gitaly/repositories

EXPOSE 2305 8080 2222

ENTRYPOINT ["entrypoint.sh"]
