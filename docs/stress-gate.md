# Stress Gate

This repository includes a local load/stress gate for the Phase 4 validation
slice (`T11`).

## Included benches

- `stress_repository_exists`: concurrent `RepositoryExists` unary load.
- `stress_repository_metadata`: concurrent `ObjectFormat` unary load.
- `stress_server_info`: concurrent `ServerInfo` unary load.

Each bench prints a single parseable line:

```text
stress_gate bench=<name> requests=<count> elapsed_ms=<ms> rps=<float>
```

## Run the full gate

```bash
./scripts/run-stress-suite.sh
```

The gate runs all stress benches, records per-bench logs in
`artifacts/stress-gate/`, parses throughput, and enforces minimum RPS
thresholds.

## Threshold tuning

Default thresholds:

- `stress_repository_exists`: `100` RPS
- `stress_repository_metadata`: `80` RPS
- `stress_server_info`: `120` RPS

Override for your environment:

```bash
STRESS_MIN_RPS_REPOSITORY_EXISTS=200 \
STRESS_MIN_RPS_REPOSITORY_METADATA=150 \
STRESS_MIN_RPS_SERVER_INFO=200 \
./scripts/run-stress-suite.sh
```
