# Docker Git Forge Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a single Docker container running gitaly-rs as a minimal Git forge with gRPC, HTTP, and SSH access.

**Architecture:** Multi-stage Dockerfile compiles the workspace in a builder stage, copies binaries into a debian-slim runtime with git and openssh. An entrypoint script starts the gRPC server, waits for it, then starts the gateway. Configs are baked in with sane defaults.

**Tech Stack:** Docker (multi-stage), Rust release build, debian bookworm-slim, git, openssh-client, grpcurl (for testing).

---

### Task 1: Create the server config file

**Files:**
- Create: `docker/config/gitaly-rs.toml`

**Step 1: Create the config**

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

**Step 2: Verify config is valid TOML**

Run: `cargo run -p gitaly -- server --config docker/config/gitaly-rs.toml --help`
Expected: prints usage (confirms config parsing path is correct)

**Step 3: Commit**

```bash
git add docker/config/gitaly-rs.toml
git commit -m "feat(docker): add server config for container"
```

---

### Task 2: Create the gateway config file

**Files:**
- Create: `docker/config/gateway.toml`

**Step 1: Create the config**

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

**Step 2: Commit**

```bash
git add docker/config/gateway.toml
git commit -m "feat(docker): add gateway config for container"
```

---

### Task 3: Create the entrypoint script

**Files:**
- Create: `docker/entrypoint.sh`

**Step 1: Write the entrypoint**

The script must:
1. Create the storage directory
2. Generate an SSH host key if missing
3. Start gitaly-rs server in background
4. Wait for gRPC port 2305 to accept connections
5. Start gateway in foreground
6. Trap SIGTERM to shut down both processes

```bash
#!/usr/bin/env bash
set -euo pipefail

STORAGE_DIR="/var/gitaly/repositories"
RUNTIME_DIR="/var/gitaly/runtime"
SSH_KEY_DIR="/var/gitaly/ssh"
SSH_HOST_KEY="${SSH_KEY_DIR}/host_ed25519"

# Create directories
mkdir -p "$STORAGE_DIR" "$RUNTIME_DIR" "$SSH_KEY_DIR"

# Generate SSH host key if not present
if [ ! -f "$SSH_HOST_KEY" ]; then
  ssh-keygen -t ed25519 -f "$SSH_HOST_KEY" -N "" -q
  echo "Generated SSH host key: ${SSH_HOST_KEY}"
fi

# Start gitaly-rs server in background
echo "Starting gitaly-rs server on :2305 ..."
gitaly server --config /etc/gitaly/gitaly-rs.toml --runtime-dir "$RUNTIME_DIR" &
SERVER_PID=$!

# Wait for gRPC port to be ready
echo "Waiting for gRPC server ..."
for i in $(seq 1 50); do
  if bash -c "echo > /dev/tcp/127.0.0.1/2305" 2>/dev/null; then
    echo "gRPC server ready."
    break
  fi
  if [ "$i" -eq 50 ]; then
    echo "ERROR: gRPC server did not start within 10 seconds."
    kill "$SERVER_PID" 2>/dev/null || true
    exit 1
  fi
  sleep 0.2
done

# Trap for graceful shutdown
cleanup() {
  echo "Shutting down ..."
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  exit 0
}
trap cleanup SIGTERM SIGINT

# Start gateway in foreground
echo "Starting gateway on HTTP :8080, SSH :2222 ..."
gitaly gateway --config /etc/gitaly/gateway.toml &
GATEWAY_PID=$!

# Wait for either process to exit
wait -n "$SERVER_PID" "$GATEWAY_PID" 2>/dev/null || true
cleanup
```

**Step 2: Make it executable**

Run: `chmod +x docker/entrypoint.sh`

**Step 3: Commit**

```bash
git add docker/entrypoint.sh
git commit -m "feat(docker): add entrypoint script"
```

---

### Task 4: Create the Dockerfile

**Files:**
- Create: `docker/Dockerfile`

**Step 1: Write the multi-stage Dockerfile**

```dockerfile
# ── Stage 1: Build ──────────────────────────────────────────────
FROM rust:1.83-bookworm AS builder

# Install protoc (required by gitaly-proto build.rs)
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

# Set proto root for build.rs discovery
ENV GITALY_PROTO_ROOT=/build/proto

# Build all workspace binaries in release mode
RUN cargo build --release --workspace

# ── Stage 2: Runtime ────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      git \
      openssh-client \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binaries from builder
COPY --from=builder /build/target/release/gitaly     /usr/local/bin/gitaly
COPY --from=builder /build/target/release/gitaly-rs  /usr/local/bin/gitaly-rs

# Copy config files
COPY docker/config/gitaly-rs.toml  /etc/gitaly/gitaly-rs.toml
COPY docker/config/gateway.toml    /etc/gitaly/gateway.toml

# Copy entrypoint
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh

# Storage volume
VOLUME /var/gitaly/repositories

# Expose ports: gRPC, HTTP, SSH
EXPOSE 2305 8080 2222

ENTRYPOINT ["entrypoint.sh"]
```

**Step 2: Commit**

```bash
git add docker/Dockerfile
git commit -m "feat(docker): add multi-stage Dockerfile"
```

---

### Task 5: Build the Docker image

**Step 1: Build from repo root**

Run: `docker build -f docker/Dockerfile -t gitaly-rs:dev .`
Expected: Multi-stage build completes. Builder stage compiles workspace. Runtime stage produces ~100-200MB image.

Note: First build may take 10-20 minutes for Rust compilation. Subsequent builds benefit from Docker layer caching.

**Step 2: Verify the image**

Run: `docker run --rm gitaly-rs:dev gitaly --help`
Expected: Prints usage showing `server` and `gateway` subcommands.

Run: `docker run --rm gitaly-rs:dev git --version`
Expected: Prints git version.

---

### Task 6: Run the container and smoke-test gRPC

**Step 1: Start the container**

Run:
```bash
docker run -d --name gitaly-forge \
  -p 2305:2305 \
  -p 8080:8080 \
  -p 2222:2222 \
  gitaly-rs:dev
```

**Step 2: Check logs**

Run: `docker logs gitaly-forge`
Expected: Shows "Starting gitaly-rs server", "gRPC server ready", "Starting gateway".

**Step 3: Smoke-test ServerInfo via gRPC**

Run:
```bash
grpcurl -plaintext \
  -import-path ./proto -proto server.proto \
  -d '{}' \
  localhost:2305 gitaly.ServerService/ServerInfo
```
Expected: JSON response with git version and storage info.

**Step 4: Smoke-test DiskStatistics**

Run:
```bash
grpcurl -plaintext \
  -import-path ./proto -proto server.proto \
  -d '{}' \
  localhost:2305 gitaly.ServerService/DiskStatistics
```
Expected: JSON response with disk usage for "default" storage.

---

### Task 7: Create a repository via gRPC

**Step 1: Call CreateRepository**

Run:
```bash
grpcurl -plaintext \
  -import-path ./proto -proto repository.proto \
  -d '{"repository":{"storage_name":"default","relative_path":"test-project.git"}}' \
  localhost:2305 gitaly.RepositoryService/CreateRepository
```
Expected: Empty JSON response `{}` (success).

**Step 2: Verify the repo exists on disk**

Run: `docker exec gitaly-forge ls /var/gitaly/repositories/test-project.git/`
Expected: Shows bare repo contents (HEAD, objects, refs, etc.).

**Step 3: Verify via RepositoryExists RPC**

Run:
```bash
grpcurl -plaintext \
  -import-path ./proto -proto repository.proto \
  -d '{"repository":{"storage_name":"default","relative_path":"test-project.git"}}' \
  localhost:2305 gitaly.RepositoryService/RepositoryExists
```
Expected: `{"exists": true}`

---

### Task 8: Push code via HTTP

**Step 1: Create a local project**

```bash
mkdir /tmp/test-project && cd /tmp/test-project
git init
git checkout -b main
echo "# Test Project" > README.md
git add README.md
git commit -m "Initial commit"
```

**Step 2: Push to the forge via HTTP**

```bash
git remote add forge http://localhost:8080/test-project.git
git push forge main
```
Expected: Push succeeds, shows refs transferred.

**Step 3: Verify via ls-remote**

Run: `git ls-remote http://localhost:8080/test-project.git`
Expected: Shows `refs/heads/main` with the commit SHA.

---

### Task 9: Clone via HTTP

**Step 1: Clone to a new directory**

```bash
git clone http://localhost:8080/test-project.git /tmp/test-clone-http
```
Expected: Clone succeeds.

**Step 2: Verify contents**

Run: `cat /tmp/test-clone-http/README.md`
Expected: Shows `# Test Project`.

---

### Task 10: Clone via SSH

**Step 1: Clone using SSH**

```bash
GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no -p 2222" \
  git clone git@localhost:/test-project.git /tmp/test-clone-ssh
```

Note: SSH may require configuring auth in gateway.toml (ssh_public_keys or password).
If the gateway requires SSH key auth, you may need to add your public key to the config
and rebuild/restart. If auth is open (empty ssh_public_keys), this should work directly.

Expected: Clone succeeds with repo contents.

**Step 2: Verify contents**

Run: `cat /tmp/test-clone-ssh/README.md`
Expected: Shows `# Test Project`.

---

### Task 11: Explore the full gRPC surface

Now that the forge is running, explore additional RPCs to understand the full surface.

**Step 1: List commits**

```bash
grpcurl -plaintext \
  -import-path ./proto -proto commit.proto \
  -d '{"repository":{"storage_name":"default","relative_path":"test-project.git"},"revision":"bWFpbg=="}' \
  localhost:2305 gitaly.CommitService/FindCommit
```

Note: `"bWFpbg=="` is base64 for `"main"`. Protobuf `bytes` fields use base64 in JSON.

**Step 2: List references**

```bash
grpcurl -plaintext \
  -import-path ./proto -proto ref.proto \
  -d '{"repository":{"storage_name":"default","relative_path":"test-project.git"},"patterns":["cmVmcy8="]}' \
  localhost:2305 gitaly.RefService/ListRefs
```

**Step 3: Check repository info**

```bash
grpcurl -plaintext \
  -import-path ./proto -proto repository.proto \
  -d '{"repository":{"storage_name":"default","relative_path":"test-project.git"}}' \
  localhost:2305 gitaly.RepositoryService/RepositoryInfo
```

**Step 4: Get repository size**

```bash
grpcurl -plaintext \
  -import-path ./proto -proto repository.proto \
  -d '{"repository":{"storage_name":"default","relative_path":"test-project.git"}}' \
  localhost:2305 gitaly.RepositoryService/RepositorySize
```

---

### Task 12: Push a second repo and verify isolation

**Step 1: Create another repository**

```bash
grpcurl -plaintext \
  -import-path ./proto -proto repository.proto \
  -d '{"repository":{"storage_name":"default","relative_path":"second-project.git"}}' \
  localhost:2305 gitaly.RepositoryService/CreateRepository
```

**Step 2: Push different content**

```bash
mkdir /tmp/second-project && cd /tmp/second-project
git init && git checkout -b main
echo "Second project" > README.md
git add . && git commit -m "Second initial commit"
git remote add forge http://localhost:8080/second-project.git
git push forge main
```

**Step 3: Verify both repos exist independently**

```bash
git ls-remote http://localhost:8080/test-project.git
git ls-remote http://localhost:8080/second-project.git
```

Expected: Different commit SHAs for each repo.

---

### Task 13: Cleanup and commit everything

**Step 1: Stop the container**

Run: `docker stop gitaly-forge && docker rm gitaly-forge`

**Step 2: Add a docker README**

Create `docker/README.md` with:
- Build instructions
- Run instructions
- Port reference
- Example workflow (create repo, push, clone)

**Step 3: Final commit**

```bash
git add docker/
git commit -m "feat(docker): complete Docker forge environment"
```

---

## Quick Reference

```bash
# Build
docker build -f docker/Dockerfile -t gitaly-rs:dev .

# Run
docker run -d --name gitaly-forge \
  -p 2305:2305 -p 8080:8080 -p 2222:2222 \
  gitaly-rs:dev

# Create repo
grpcurl -plaintext -import-path ./proto -proto repository.proto \
  -d '{"repository":{"storage_name":"default","relative_path":"my-repo.git"}}' \
  localhost:2305 gitaly.RepositoryService/CreateRepository

# Push
git remote add forge http://localhost:8080/my-repo.git
git push forge main

# Clone (HTTP)
git clone http://localhost:8080/my-repo.git

# Clone (SSH)
GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no -p 2222" \
  git clone git@localhost:/my-repo.git

# Logs
docker logs -f gitaly-forge

# Stop
docker stop gitaly-forge && docker rm gitaly-forge
```
