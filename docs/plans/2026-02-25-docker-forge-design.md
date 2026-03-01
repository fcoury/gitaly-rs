# Docker Git Forge Environment — Design

> Approved 2026-02-25.

## Goal

Build a single Docker container running gitaly-rs as a minimal Git forge to
explore the full application surface: gRPC server, HTTP gateway, SSH gateway,
repository creation, push, pull, and clone.

## Architecture

```
┌─────────────────────────────────────────────┐
│  Docker Container                           │
│                                             │
│  ┌──────────────┐   ┌───────────────────┐   │
│  │ gitaly-rs    │   │ gitaly gateway    │   │
│  │ gRPC :2305   │◄──│ HTTP :8080        │   │
│  │              │   │ SSH  :2222        │   │
│  └──────┬───────┘   └───────────────────┘   │
│         │                                   │
│    /var/gitaly/repositories (volume)        │
└─────────────────────────────────────────────┘
```

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 2305 | gRPC     | Direct API access (CreateRepository, ServerInfo, etc.) |
| 8080 | HTTP     | Git smart HTTP (push/pull via `git-upload-pack` / `git-receive-pack`) |
| 2222 | SSH      | Git SSH transport |

### User Workflow

```bash
# 1. Create a repository via gRPC
grpcurl -plaintext \
  -import-path ./proto -proto repository.proto \
  -d '{"repository":{"storage_name":"default","relative_path":"my-project.git"}}' \
  localhost:2305 gitaly.RepositoryService/CreateRepository

# 2. Push code via HTTP
cd my-project && git remote add forge http://localhost:8080/my-project.git
git push forge main

# 3. Clone via SSH
git clone ssh://git@localhost:2222/my-project.git
```

## Container Design

### Build: multi-stage Dockerfile

- **Stage 1 (builder)**: Rust toolchain + protoc, compiles workspace in release mode
- **Stage 2 (runtime)**: debian-slim + git + openssh-client, copies binaries

### Configuration

Two TOML configs baked into the image:

**gitaly-rs.toml** (server):
```toml
listen_addr = "0.0.0.0:2305"
internal_addr = "0.0.0.0:2305"

[auth]
token = ""
transitioning = true

[[storages]]
name = "default"
path = "/var/gitaly/repositories"

[runtime.limiter]
concurrency_limit = 64
queue_limit = 8

[runtime.cgroups]
enabled = false
```

**gateway.toml**:
```toml
http_listen_addr = "0.0.0.0:8080"
ssh_listen_addr = "0.0.0.0:2222"
gitaly_addr = "http://127.0.0.1:2305"

[auth]
gitaly_token = ""
client_tokens = []
ssh_public_keys = []

[repositories]
storage_name = "default"
```

### Entrypoint

A shell script that:
1. Creates storage directory
2. Generates SSH host key (if not present)
3. Starts `gitaly-rs` server in background
4. Waits for gRPC port to be ready
5. Starts `gitaly gateway` in foreground
6. Traps SIGTERM for graceful shutdown of both

### Volume

`/var/gitaly/repositories` — mount for repository data persistence.

## Future: Sharding via Docker Compose

The single-container design is intentionally splittable:
- Server and gateway are separate binaries with separate configs
- Storage name is configurable (each shard gets a unique name)
- Gateway's `gitaly_addr` points to whichever server shard owns the storage

```yaml
# Future compose sketch
services:
  shard-1:
    image: gitaly-rs
    command: server --config /etc/gitaly/shard-1.toml
  shard-2:
    image: gitaly-rs
    command: server --config /etc/gitaly/shard-2.toml
  gateway:
    image: gitaly-rs
    command: gateway --config /etc/gitaly/gateway.toml
```

## Non-goals (for now)

- Push-to-create (requires gateway code change)
- Authentication enforcement (transitioning mode, no tokens required)
- Web UI
- Management REST API
- TLS
