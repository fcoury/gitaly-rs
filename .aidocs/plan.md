# gitaly-rs Execution Plan (Active)

Last updated: 2026-02-25

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
- Phase 7: mostly done
- Phase 8: partial (many RPC/tooling gaps)
- Phase 9: partial (cluster state baseline and raft wiring implemented; full consensus engine still pending)
- Phase 10: partial (root test hierarchy baseline implemented; broader suites pending)

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
| T03 | Implement real write semantics in hook and operation flows | `service/{hook,operations,smarthttp,ssh}.rs` + `gitaly-git` | done | Completed slices T03a–T03d for baseline write-path semantics |
| T03a | Implement SmartHTTP upload-pack sidechannel write-path baseline | `service/smarthttp.rs` | done | Added sidechannel upload-pack execution with request validation and negotiation stats |
| T03b | Harden OperationService mutation RPC semantics | `service/operations.rs` | done | Added repository contract checks for unary/streaming mutation RPCs with focused tests |
| T03c | Wire git protocol/config options for streaming pack RPCs | `service/{ssh,smarthttp}.rs` | done | Applied validated git config/protocol handling in streaming SSH/SmartHTTP pack flows |
| T03d | Align remaining write-path command option handling | `service/{smarthttp,ssh}.rs` | done | Added info-refs and streaming option propagation with validation tests |
| T04 | Finish remote and repository remaining RPC surface | `service/{remote,repository}.rs` | done | Completed remote + repository baseline RPC coverage with focused tests |
| T04a | Implement remaining RemoteService RPCs | `service/remote.rs` | done | Implemented `update_remote_mirror` baseline with focused tests |
| T04b | Implement remaining RepositoryService RPCs | `service/repository.rs` | done | Replaced remaining repository RPC stubs with validated baseline responses/streams |
| T04b1 | Implement repository format/branch/objects-size basics | `service/repository.rs` | done | Implemented `object_format`, `has_local_branches`, and `objects_size` baselines |
| T04b2 | Implement repository maintenance and bundle RPC slices | `service/repository.rs` | done | Closed all remaining repository RPC stubs with conservative baseline behavior and tests |
| T05 | Add missing binaries and packaging path | `bins/*`, workspace wiring | done | Added helper binaries and staging script for a shared install destination |
| T06 | Build root integration/chaos/stress test layout | `tests/*`, `benches/*` | done | Added root support harness, integration/chaos suites, and first stress bench target |
| T07 | Introduce `gitaly-cluster` and real raft integration | `crates/gitaly-cluster`, `service/raft.rs` | done | Added `gitaly-cluster` state manager and wired `RaftService` to cluster-derived partition/statistics responses |

## Current Task Detail

### Queue Status

Subtasks:
- All queued tasks (`T00` through `T07`) are complete.
- Next work should be added as new task IDs before implementation starts.

Verification target:
- `cargo test -p gitaly-cluster`
- `cargo test -p gitaly-server --lib service::raft:: -- --test-threads=1`

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
- 2026-02-24: Completed T03d by applying option handling to remaining SmartHTTP write-adjacent RPCs.
- 2026-02-24: Marked T03 complete and started T04 remote/repository RPC closure phase.
- 2026-02-24: Completed T04a by implementing `update_remote_mirror` baseline and focused remote service tests.
- 2026-02-24: Completed T04b1 by implementing repository object-format/branch/objects-size baseline RPCs and tests.
- 2026-02-25: Completed T04b2 by replacing remaining `RepositoryService` stubs with validated baseline responses/streams and focused tests.
- 2026-02-25: Marked T04 complete and started T05 binary/packaging work.
- 2026-02-25: Completed T05 by adding root helper binaries (`gitaly-hooks`, `gitaly-ssh`, `gitaly-backup`, `gitaly-gpg`, `gitaly-lfs-smudge`, `gitaly-blackbox`) and a staging script for shared install paths.
- 2026-02-25: Marked T05 complete and started T06 root test hierarchy work.
- 2026-02-25: Completed T06 by adding root `tests/support` harness, `integration_server_info`, `chaos_stream_shutdown`, and `stress_repository_exists` bench target.
- 2026-02-25: Marked T06 complete and started T07 cluster/raft integration work.
- 2026-02-25: Completed T07 by adding `crates/gitaly-cluster`, wiring `RaftService` to shared cluster state, and adding focused cluster/raft tests.
