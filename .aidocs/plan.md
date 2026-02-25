# gitaly-rs Execution Plan (Active)

Last updated: 2026-02-25

## Working Routine

- Keep this file as the single active tracker.
- At the end of each task:
  - update this file (status, notes, verification, next step)
  - create one commit that includes code changes + this plan update
- Status values: `pending`, `in_progress`, `done`, `blocked`.

## Remaining Areas Snapshot

- Phase 3: partial (durability drills pending)
- Phase 4: partial (load/stress gate pending)
- Phase 5: partial (broader observability pending)
- Phase 8: partial (remaining RPC/tooling gaps)
- Phase 9: partial (cluster baseline present, full consensus depth pending)
- Phase 10: partial (root test scaffolding present, broader suites pending)

## Task Queue

| ID | Task | Scope | Status | Notes |
|---|---|---|---|---|
| T08 | Close remaining Phase 8 API/tooling gaps | `service/*`, `bins/*`, tooling | done | Closed highest-impact gaps: helper binaries now functional and repository backup/restore RPCs implement real snapshot semantics |
| T09 | Complete Phase 5 observability rollout | middleware + service surfaces | done | Structured logs, metrics, and correlation propagation now cover middleware accept/reject paths with per-reason counters |
| T10 | Execute Phase 3 durability drills | write path + storage durability | in_progress | Crash/restart drills, fsync/atomicity checks, snapshot/restore corruption handling |
| T11 | Run Phase 4 load/stress gate | load harness + stress profiles | pending | Saturation/soak tests with explicit pass/fail SLO thresholds |
| T12 | Deepen Phase 9 cluster implementation | `gitaly-cluster`, `service/raft.rs` | pending | Move from baseline state manager to deeper OpenRaft lifecycle and persistence |
| T13 | Expand Phase 10 test program | `tests/*`, CI matrix, coverage | pending | Grow integration/chaos/compat/reliability suites and nightly matrix |

## Current Task Detail

### T10 - Execute Phase 3 durability drills

Subtasks:
- Add crash/restart durability drills for write-heavy repository flows.
- Validate atomic snapshot/restore behavior under partial or corrupted backup inputs.
- Verify WAL/commit ordering recovery behavior after interrupted write sequences.

Verification target:
- Focused storage + repository durability tests.
- `cargo test --workspace -- --test-threads=1`

## Changelog

- 2026-02-25: Archived completed T00-T07 plan to `.aidocs/historical/2026-02-25/plan-t00-t07-complete.md`.
- 2026-02-25: Created new active plan covering remaining areas (T08-T13).
- 2026-02-25: Completed T08 gap-closure slice by replacing helper binary placeholders with functional baseline CLIs (`gitaly-hooks`, `gitaly-ssh`, `gitaly-backup`, `gitaly-gpg`, `gitaly-lfs-smudge`, `gitaly-blackbox`) and implementing snapshot-based `backup_repository`/`restore_repository` semantics in `RepositoryService`.
- 2026-02-25: Marked T08 complete and started T09 observability rollout.
- 2026-02-25: Completed T09 by adding shared observability field extraction, structured auth/limiter decision logging, and per-reason rejection counters wired into middleware tests.
- 2026-02-25: Verified T09 with `cargo test -p gitaly-server --lib middleware:: -- --test-threads=1` and `cargo test --workspace -- --test-threads=1`.
- 2026-02-25: Marked T09 complete and started T10 durability drill execution.
