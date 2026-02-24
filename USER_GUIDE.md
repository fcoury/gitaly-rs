# gitaly-rs User Guide

This guide covers local usage of the current `gitaly` executable and its
`server` subcommand.

It mirrors the practical structure of the Go docs (`README`, `doc/README.md`,
`doc/beginners_guide.md`, and config docs), but only documents behavior that
exists in the Rust code today.

## Current Scope

`gitaly server` boots a tonic gRPC server and serves `ServerService`
methods:

- `ServerInfo`
- `DiskStatistics`
- `ReadinessCheck`
- `ServerSignature`

This is early-phase functionality, intended for local development and smoke
testing.

## Prerequisites

- Rust toolchain compatible with this workspace (`rust-toolchain.toml`)
- `git` available in `PATH` (used to populate reported Git version)
- Optional: `grpcurl` for manual RPC testing

## Configuration

The server uses a TOML config file passed via `--config` or `GITALY_CONFIG`.

### Required fields

```toml
listen_addr = "127.0.0.1:2305"
internal_addr = "127.0.0.1:9236"

[[storages]]
name = "default"
path = "/tmp/gitaly-storage"
```

### Runtime section (optional)

```toml
[runtime.limiter]
concurrency_limit = 1024
queue_limit = 0

[runtime.cgroups]
enabled = true
bucket_count = 500
```

Runtime defaults:

- `runtime.limiter.concurrency_limit = 1024`
- `runtime.limiter.queue_limit = 0`
- `runtime.cgroups.enabled = true`
- `runtime.cgroups.bucket_count = 500`

Validation rules:

- `listen_addr` must be non-empty and parse as a socket address.
- At least one storage is required.
- Storage `name` must be non-empty and unique.
- Storage `path` must be absolute.
- `runtime.limiter.concurrency_limit` must be `> 0`.
- If `runtime.cgroups.enabled = true`, then `runtime.cgroups.bucket_count` must
  be `> 0`.

## Running

From `gitaly-rs/`:

```bash
cargo run -p gitaly -- server --config /path/to/gitaly.toml
```

Optional runtime directory:

```bash
cargo run -p gitaly -- server --config /path/to/gitaly.toml --runtime-dir /tmp/gitaly-rs-runtime
```

Environment variable equivalents:

- `GITALY_CONFIG=/path/to/gitaly.toml`
- `GITALY_RUNTIME_DIR=/tmp/gitaly-rs-runtime`

Help:

```bash
cargo run -p gitaly -- --help
```

## Smoke Test

1. Start server:

```bash
mkdir -p /tmp/gitaly-storage
cat > /tmp/gitaly-rs.toml <<'EOF'
listen_addr = "127.0.0.1:2305"
internal_addr = "127.0.0.1:9236"

[runtime.limiter]
concurrency_limit = 64
queue_limit = 8

[runtime.cgroups]
enabled = false
bucket_count = 500

[[storages]]
name = "default"
path = "/tmp/gitaly-storage"
EOF

cargo run -p gitaly -- server --config /tmp/gitaly-rs.toml
```

2. In another terminal (from repo root `/Users/fcoury/code/gitaly`):

```bash
grpcurl -plaintext -import-path ./proto -proto server.proto -d '{}' 127.0.0.1:2305 gitaly.ServerService/ServerInfo
grpcurl -plaintext -import-path ./proto -proto server.proto -d '{}' 127.0.0.1:2305 gitaly.ServerService/DiskStatistics
grpcurl -plaintext -import-path ./proto -proto server.proto -d '{}' 127.0.0.1:2305 gitaly.ServerService/ReadinessCheck
grpcurl -plaintext -import-path ./proto -proto server.proto -d '{}' 127.0.0.1:2305 gitaly.ServerService/ServerSignature
```

## Platform Behavior (Linux vs macOS)

- Limiter is always active using configured runtime values.
- Cgroup assignment is additive:
  - Linux: filesystem-backed manager is used.
  - non-Linux (including macOS): noop manager is used.
- Disabling cgroups (`runtime.cgroups.enabled = false`) skips cgroup manager
  wiring entirely.

## Troubleshooting

- `missing config path; pass --config <path> or set GITALY_CONFIG`
  - Provide `--config` or export `GITALY_CONFIG`.
- `invalid listen_addr`
  - Use `host:port`, for example `127.0.0.1:2305`.
- `runtime limiter concurrency limit must be greater than zero`
  - Set `runtime.limiter.concurrency_limit` to `1` or higher.
- `runtime cgroup bucket count must be greater than zero when cgroups are enabled`
  - Set `runtime.cgroups.bucket_count` to `1` or higher, or disable cgroups.
- `warning: ... Skipping cluster.proto ... missing proto/raftpb/raft.proto`
  - Known workspace warning from proto generation; unrelated to basic
    `ServerService` runtime behavior.
