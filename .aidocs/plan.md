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
| T08 | Close remaining Phase 8 API/tooling gaps | `service/*`, `bins/*`, tooling | in_progress | Build full gap matrix, then close highest-impact RPC/binary/tooling gaps in slices |
| T09 | Complete Phase 5 observability rollout | middleware + service surfaces | pending | Structured logs, metrics, and correlation propagation across all major RPC paths |
| T10 | Execute Phase 3 durability drills | write path + storage durability | pending | Crash/restart drills, fsync/atomicity checks, snapshot/restore corruption handling |
| T11 | Run Phase 4 load/stress gate | load harness + stress profiles | pending | Saturation/soak tests with explicit pass/fail SLO thresholds |
| T12 | Deepen Phase 9 cluster implementation | `gitaly-cluster`, `service/raft.rs` | pending | Move from baseline state manager to deeper OpenRaft lifecycle and persistence |
| T13 | Expand Phase 10 test program | `tests/*`, CI matrix, coverage | pending | Grow integration/chaos/compat/reliability suites and nightly matrix |

## Current Task Detail

### T08 - Close remaining Phase 8 API/tooling gaps

Subtasks:
- Build an explicit RPC/binary/tooling gap matrix from current code.
- Prioritize high-impact missing RPCs and implement in bounded slices.
- Replace helper binary placeholders with minimal real functionality where feasible.

Verification target:
- `cargo test --workspace`
- Focused service tests per implemented slice.

## Changelog

- 2026-02-25: Archived completed T00-T07 plan to `.aidocs/historical/2026-02-25/plan-t00-t07-complete.md`.
- 2026-02-25: Created new active plan covering remaining areas (T08-T13).
