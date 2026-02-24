# gitaly-rs

Rust rewrite workspace for Gitaly.

Current focus: Phase 1 foundation crates (`gitaly-proto`, `gitaly-error`,
`gitaly-config`, `gitaly-auth`).

Primary local CLI binary: `gitaly`
- `gitaly server ...` runs the gRPC server
- `gitaly gateway ...` runs the HTTP/SSH gateway

## Documentation

- User guide: [`USER_GUIDE.md`](USER_GUIDE.md)
