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
- Phase 6: partial (read RPC gaps)
- Phase 7: partial (write semantics mostly scaffold)
- Phase 8: partial (many RPC/tooling gaps)
- Phase 9: not started
- Phase 10: not started (planned root test hierarchy missing)

## Task Queue

| ID | Task | Scope | Status | Notes |
|---|---|---|---|---|
| T00 | Archive existing docs and reset active tracker | `.aidocs/` | done | Moved previous docs to `.aidocs/historical/2026-02-24/` and created this plan |
| T01 | Productionize middleware chain foundations | `crates/gitaly-server/src/middleware/*` | done | Correlation/request-info/log-fields/logging/metrics/status/sidechannel/auth baselines implemented and wired |
| T02 | Close remaining Phase 6 read RPC gaps | `service/{commit,diff,ref_,blob,ssh}.rs` | in_progress | Split into focused slices for deterministic commits |
| T02a | Implement remaining DiffService read RPCs | `service/diff.rs` | pending | `commit_diff`, `commit_delta`, `diff_blobs` |
| T02b | Implement remaining RefService read RPCs | `service/ref_.rs` | pending | `find_all_branches`, `find_tag`, `find_all_remote_branches`, etc. |
| T02c | Implement remaining BlobService read RPCs | `service/blob.rs` | pending | `list_all_blobs`, LFS pointer listing RPCs |
| T02d | Implement remaining CommitService read RPCs | `service/commit.rs` | pending | remove `unimplemented` for remaining read methods |
| T02e | Implement SSH sidechannel read RPC baseline | `service/ssh.rs` | pending | `ssh_upload_pack_with_sidechannel` |
| T03 | Implement real write semantics in hook and operation flows | `service/{hook,operations,smarthttp,ssh}.rs` + `gitaly-git` | pending | Hook ordering, transactional behavior, meaningful responses |
| T04 | Finish remote and repository remaining RPC surface | `service/{remote,repository}.rs` | pending | Remaining `unimplemented` methods |
| T05 | Add missing binaries and packaging path | `bins/*`, workspace wiring | pending | `gitaly-hooks`, `gitaly-ssh`, then backup/gpg/lfs/blackbox |
| T06 | Build root integration/chaos/stress test layout | `tests/*`, `benches/*` | pending | Create planned hierarchy and first end-to-end suites |
| T07 | Introduce `gitaly-cluster` and real raft integration | `crates/gitaly-cluster`, `service/raft.rs` | pending | Move from placeholder service to `openraft`-backed cluster state |

## Current Task Detail

### T02a - Implement remaining DiffService read RPCs

Subtasks:
- Implement `commit_diff`.
- Implement `commit_delta`.
- Implement `diff_blobs`.
- Add/update tests for newly implemented methods.

Verification target:
- `cargo test -p gitaly-server --lib service::diff:: -- --test-threads=1`

## Changelog

- 2026-02-24: Created active tracker and started T01.
- 2026-02-24: Completed T01 middleware baseline and auth wiring via config/dependencies.
- 2026-02-24: Started T02 with T02a as current execution slice.
