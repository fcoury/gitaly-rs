# gitaly-rs Execution Plan (Active)

Last updated: 2026-02-24

## Working Routine

- Keep this file as the single active tracker.
- At the end of each task:
  - update this file (status, notes, verification, next step)
  - create one commit that includes code changes + this plan update
- Status values: `pending`, `in_progress`, `done`, `blocked`.

## Phase Status Snapshot

- Phase 1: done
- Phase 2: mostly done
- Phase 3: partial (durability drills pending)
- Phase 4: partial (load/stress gate pending)
- Phase 5: partial (middleware baseline implemented, broader observability still pending)
- Phase 6: done
- Phase 7: partial (write semantics mostly scaffold)
- Phase 8: partial (many RPC/tooling gaps)
- Phase 9: not started
- Phase 10: not started (planned root test hierarchy missing)

## Task Queue

| ID | Task | Scope | Status | Notes |
|---|---|---|---|---|
| T00 | Archive existing docs and reset active tracker | `.aidocs/` | done | Moved previous docs to `.aidocs/historical/2026-02-24/` and created this plan |
| T01 | Productionize middleware chain foundations | `crates/gitaly-server/src/middleware/*` | done | Correlation/request-info/log-fields/logging/metrics/status/sidechannel/auth baselines implemented and wired |
| T02 | Close remaining Phase 6 read RPC gaps | `service/{commit,diff,ref_,blob,ssh}.rs` | done | Completed slices T02a–T02e with focused service tests |
| T02a | Implement remaining DiffService read RPCs | `service/diff.rs` | done | Implemented `commit_diff`, `commit_delta`, and `diff_blobs` with focused tests |
| T02b | Implement remaining RefService read RPCs | `service/ref_.rs` | done | Implemented remaining read stubs with focused service tests |
| T02c | Implement remaining BlobService read RPCs | `service/blob.rs` | done | Implemented list-all and LFS pointer RPCs with focused service tests |
| T02d | Implement remaining CommitService read RPCs | `service/commit.rs` | done | Implemented remaining read RPCs with git-backed baselines and focused tests |
| T02e | Implement SSH sidechannel read RPC baseline | `service/ssh.rs` | done | Implemented unary sidechannel baseline with negotiation stats parsing and validation |
| T03 | Implement real write semantics in hook and operation flows | `service/{hook,operations,smarthttp,ssh}.rs` + `gitaly-git` | in_progress | Split into focused slices for deterministic commits |
| T03a | Implement SmartHTTP upload-pack sidechannel write-path baseline | `service/smarthttp.rs` | done | Added sidechannel upload-pack execution with request validation and negotiation stats |
| T03b | Harden OperationService mutation RPC semantics | `service/operations.rs` | done | Added repository contract checks for unary/streaming mutation RPCs with focused tests |
| T03c | Wire git protocol/config options for streaming pack RPCs | `service/{ssh,smarthttp}.rs` | done | Applied validated git config/protocol handling in streaming SSH/SmartHTTP pack flows |
| T03d | Align remaining write-path command option handling | `service/{smarthttp,ssh}.rs` | in_progress | Extend config/protocol handling to remaining related RPC flows |
| T04 | Finish remote and repository remaining RPC surface | `service/{remote,repository}.rs` | pending | Remaining `unimplemented` methods |
| T05 | Add missing binaries and packaging path | `bins/*`, workspace wiring | pending | `gitaly-hooks`, `gitaly-ssh`, then backup/gpg/lfs/blackbox |
| T06 | Build root integration/chaos/stress test layout | `tests/*`, `benches/*` | pending | Create planned hierarchy and first end-to-end suites |
| T07 | Introduce `gitaly-cluster` and real raft integration | `crates/gitaly-cluster`, `service/raft.rs` | pending | Move from placeholder service to `openraft`-backed cluster state |

## Current Task Detail

### T03d - Align remaining write-path command option handling

Subtasks:
- Audit remaining pack/write-adjacent RPCs for option propagation gaps.
- Apply validated `git_config_options`/`git_protocol` handling where missing.
- Add focused tests for each updated RPC path.

Verification target:
- `cargo test -p gitaly-server --lib service::{ssh,smarthttp,hook}:: -- --test-threads=1`

## Changelog

- 2026-02-24: Created active tracker and started T01.
- 2026-02-24: Completed T01 middleware baseline and auth wiring via config/dependencies.
- 2026-02-24: Started T02 with T02a as current execution slice.
- 2026-02-24: Completed T02a by implementing remaining DiffService read RPC stubs.
- 2026-02-24: Completed T02b by implementing remaining RefService read RPC stubs.
- 2026-02-24: Completed T02c by implementing remaining BlobService read RPC stubs.
- 2026-02-24: Completed T02d by implementing remaining CommitService read RPC stubs and focused tests.
- 2026-02-24: Completed T02e by implementing SSH sidechannel upload-pack baseline and focused tests.
- 2026-02-24: Marked T02 complete and started T03 write-semantics phase.
- 2026-02-24: Completed T03a by implementing SmartHTTP sidechannel upload-pack baseline and focused tests.
- 2026-02-24: Completed T03b by adding OperationService mutation repository validation and contract tests.
- 2026-02-24: Completed T03c by wiring validated git protocol/config options in streaming SSH and SmartHTTP pack RPCs.
