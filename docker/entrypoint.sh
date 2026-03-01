#!/usr/bin/env bash
set -euo pipefail

mkdir -p /var/gitaly/repositories /var/gitaly/runtime

# Start gitaly-rs server in background
echo "Starting gitaly-rs server on :2305 ..."
gitaly server --config /etc/gitaly/gitaly-rs.toml --runtime-dir /var/gitaly/runtime &
SERVER_PID=$!

# Wait for gRPC port
echo "Waiting for gRPC server ..."
for i in $(seq 1 50); do
  if bash -c "echo > /dev/tcp/127.0.0.1/2305" 2>/dev/null; then
    echo "gRPC server ready."
    break
  fi
  [ "$i" -eq 50 ] && echo "ERROR: server didn't start" && kill "$SERVER_PID" 2>/dev/null && exit 1
  sleep 0.2
done

# Graceful shutdown
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

wait -n "$SERVER_PID" "$GATEWAY_PID" 2>/dev/null || true
cleanup
