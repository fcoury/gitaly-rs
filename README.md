# gitaly-rs

Rust rewrite workspace for Gitaly.

Current focus: Phase 1 foundation crates (`gitaly-proto`, `gitaly-error`,
`gitaly-config`, `gitaly-auth`).

Primary local CLI binary: `gitaly`
- `gitaly server ...` runs the gRPC server
- `gitaly gateway ...` runs the HTTP/SSH gateway

## Docker

Use the compose file in `docker/` to run the containerized forge:

```bash
cp docker/.env.example docker/.env
docker compose -f docker/docker-compose.yml up --build
```

Repository storage is bind-mounted from the host into
`/var/gitaly/repositories` in the container.

- Default host path:
  `/Volumes/External/gitaly-workspace`
- Override by editing `GITALY_REPOSITORIES_DIR` in `docker/.env` or exporting
  it before `docker compose`.

## Documentation

- User guide: [`USER_GUIDE.md`](USER_GUIDE.md)
