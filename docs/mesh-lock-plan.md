# Plan: replace the single mesh lock (smarty-dev#6477)

FINAL, 2026-10-07. Inputs: design.md, review-luna.md, review-opus.md, upstream-mesh-changes.md.

**Summary for Paul**

1. We move shared state off the single mesh lock into one SQLite database for each mesh. Events stay in the event file.
2. Both reviews approved with changes. We apply all of them: a safe rollback, waits that do not freeze a session, and a census of old processes before each switch.
3. The first relief (custody lock, bridge fixes, idle reads) installs tonight. The heartbeat fix, the registry fix and the lock metrics follow on Thursday morning.
4. On Thursday 09:00 UTC the metrics show which work holds the lock. If events hold it most, we also fix the event path at the same time.
5. Target: no single lock for state and zero lock timeouts at full load on Monday 12 October, 15:00 UTC. With delays: Tuesday 13 October.
6. "Done" means: for 24 hours at full load, each lock is busy less than 30% of the time and commswatch counts zero lock timeouts in each 10 minutes.

## 1. Decision: option B

Keyed state goes into `<mesh>/state.db` (SQLite WAL, `synchronous=NORMAL`, local disk only). Events, archive and dedupe receipts stay on file behind `.lock`. File custody uses `custody.lock`. Option A stays as the fallback for events only (E2). Option C adds nothing to B; its lock-domain split is R3. Upstream has no lock or store redesign; A1 classifies its new mesh-lock users (schedules, grants) at the next sync. We accept every reviewer change (L = Luna, O = Opus):

| # | Change (source) | How we apply it (lane) |
|---|---|---|
| R1 | Rollback loses no update (O P0-1, Q5; L P1-4) | Stop v3 writers and projector first. In-database flag `meta.backend=file`, `epoch+1`, read after `BEGIN IMMEDIATE`. Durable export by the normal encoder (new `readGeneration`, revision 2, fsync file and directory). Never rename open `state.db*`. Verify with the old reader (L4, L9a). |
| R2 | No synchronous 10 s busy wait (O P1-1) | `busy_timeout` 5 ms or less; async retry on `MeshLockTicket`; obey `writeSignal`, `withTryLock` (L2a). |
| R3 | Classify `exclusive()` sites (O P1-2; L P2-8) | 13 sites into state, events, custody, none (A1). State sites run in `BEGIN IMMEDIATE`, one test each (L2b). |
| R4 | L5 unsafe on a mixed fleet (O P1-3) | #591 uses "dual" mode (custody, then mesh); adoption stays on mesh. "own" mode only after the census (L4). |
| R5 | The fence is partial (O P1-4, P2-8; L P1-1) | The census is the fence: each writer, bridge agent and CLI shows `meshProtocol`, or cutover stops (L4). Table for each operation (A1). Durable marker first (L4). Projector stops on a foreign write, with alarm (L3). Fence test now (L1). |
| R6 | Stage-2 dual reads are stale (O P1-5) | Stage 2 is removed. Shadow goes directly to cutover. |
| R7 | Change detection uses `state.json` (O P1-6) | `publicationGeneration`, `stateStamp` users, the L6 witness use `data_version` or a `meta` counter (L2b). |
| R8 | Measure first (O P1-7, P3-5; L P2-6, P2-7) | L8: class, wait, hold, CPU, fsync time, bytes per hold; byte share per key family. Busy = holder-timed hold / wall time. G1 reads the split. Before and after numbers go on #6477. |
| R9 | B does not move publish I/O (O P1-7, Q6; L P2-10) | E1 (#550) moves fsync after unlock. If publishes still hold 50% or more, E2 adds group commit or an events-only appender. |
| R10 | WAL growth (O P2-1; L P2-5) | `wal_autocheckpoint=0` in clients; projector checkpoints (PASSIVE, TRUNCATE above 64 MB); no read open across `await`; WAL size alarm (L2a, L3, L8). |
| R11 | Callback I/O; `afterCommit` before `COMMIT` (L P1-2; O P2-2) | File reads before `BEGIN`, checked again by stamp. File effects after `COMMIT` from a durable idempotent outbox (L2b). Crash test (L9a). |
| R12 | Power-loss contract (L P1-3; O P3-3) | Each key family is replayable or uses a `FULL` connection (L1). Power-cut drill on a VM (L9b). |
| R13 | L6 keeps #24 evidence (O P2-3; L P2-8) | #592 uses a commit witness that only lock holders write, else takes the lock. Round 2 checks. |
| R14 | L7 keeps the tail invariant (O P2-4; L P2-8) | #590 appends at the exact offset and reads its bytes back. Round 2 checks; if it fails, L7 stops. |
| R15 | 32 MiB cap (O P2-5) | Enforced in the SQLite write until L10 (L2a). |
| R16 | Local disk only (O P2-7; L P2-5) | `statfs` guard: nfs, cifs, sshfs, 9p, virtiofs use `file` (L2a). |
| R17 | Mixed-version bridge (L P2-9; O P2-8) | Old and new side tests: mirror, replay, leases (L9a). Alarm: old bridge on a cut-over hub (L4). |
| R18 | Windows (O P3-1; L P2-5) | Bounded retry on `SQLITE_IOERR`, `SQLITE_CANTOPEN` (L2a). Concurrent-writer Windows smoke (L9b). |
| R19 | `node:sqlite`, Bun (O P3-2; L P2-5) | Adapter interface; Node 24+; conformance test also under Bun (L1). |
| R20 | Spec review, lock order, `control-seen` (O P3-4, P3-6 to P3-8) | Spec review of the adapter note (G2). Order: registries, then state transaction; `.lock` inside it only in migration tools (L0). `control-seen` stays on `file`. |

## 2. Lanes

Size in agent-hours (build plus review). A standard lane is 3.5 h: 1.5 h build, 2 h review.

| Lane | Exclusive files | Depends on | h | Status |
|---|---|---|---|---|
| #560, L5 (#591), #593, #595 | read paths; `custody-lock.ts`, `stall-alarms.ts`, `participant-files.ts`; `bridge.ts` | none | 0 | merged or in RC3.1.5x |
| L6 (#592), L7 (#590) | `participant-directory.ts`; `registry-store.ts`, `registry-payloads.ts` | none | 3 | round 2 |
| E1 (#550) fsync after unlock | `store.ts` publish path, `archive.ts` | none | ? | owner states ETA |
| L8 lock metrics | `commit-stats.ts`, `lock-metrics.ts` (new), timing hooks | none | 3.5 | started |
| L1 SQLite module | `state-sqlite.ts`, its test, `mesh-revision-fence.test.ts`, `docs/mesh-state-sqlite.md` | none | 5 | started |
| A1 lock-domain audit | `docs/mesh-lock-domains.md` | none | 2.5 | start now |
| C1 commswatch feed | commswatch parser (outside pi-fabric) | L8 format | 2.5 | |
| L0 split `store.ts` | `store.ts`; new `state-file.ts`, `event-log.ts`, `mesh-lock.ts` | L8, E1 | 3.5 | |
| L2a backend, async acquire | `state-backend.ts` (new), `state-file.ts`, `config.ts` | L0, L1, G1 | 3.5 | |
| L2b lock domains, callbacks | `bridge.ts`, `publication-generation.ts`, `host-reaper.ts`, `manager.ts`, `residency/host.ts`, `participant-directory.ts`, `commit-outbox.ts` (new) | L0, L1, A1, L6 | 5 | |
| L3 projector | `state-projector.ts` (new) | L2a | 3.5 | |
| L4 census, cutover, rollback | `backend-migration.ts`, `writer-census.ts`, `mesh-backend-cli.ts`, `bin/`, `host-leases.ts` | L2b, L3 | 5 | |
| L9a Linux tests | `tests/mesh-backend-*.test.ts` | L4 | 3.5 | |
| L9b Windows, power cut | `mesh-backend-windows.test.ts`, `test:smoke` list | L4 | 3.5 | |
| E2 event group commit | `event-log.ts`, `archive.ts` | L0, E1, G1 | 7 | only if G1 |
| L10 retire legacy | `read-journal.ts`, file state, custody "dual" | G8 | 3.5 | later |

## 3. Critical path and ETA (UTC)

Path: L8, E1, L0, G1, L2a, L3, L4, L9, RC, shadow, canary, hub, acceptance: 22.5 agent-hours of build. L2b (5 h) runs beside L2a and L3 (7 h). Each lane passes G0: independent review PASS, Linux CI and Windows CI.

| UTC | Step | Gate |
|---|---|---|
| Wed 07 21:30 | L8, L1, A1 start. RC3.1.5x installs; commswatch records the baseline. | |
| Thu 08 01:00 | L8, L6, L7 merge. L0 starts (E1 merged). | G0 |
| Thu 08 03:00 | RC with L6, L7, L8 on all hosts. | G0 |
| Thu 08 09:00 | Split from 6 h of L8 data on Ryzen 1. L2a, L2b (and E2 if needed) start. | G1 |
| Thu 08 16:00 | L3 merges; L4 starts. | G0 |
| Fri 09 03:00 | L9 done. RC 3.2 on all hosts in `shadow` mode. | G2 spec review; G3 Windows CI, power-cut drill |
| Sat 10 03:00 | Cutover ryzen5 (canary); one rollback drill, roll forward. | G4: 0 divergences in 24 h |
| Sun 11 03:00 | Cutover ryzen3, ryzen4. Restart the old processes that the Ryzen 1 census lists. | G5: canary 24 h, 0 timeouts |
| Sun 11 15:00 | Cutover Ryzen 1 (hub); custody "own" mode. | G6: census clean |
| **Mon 12 Oct 15:00** | **ETA: no single lock for state, 0 timeouts at full load.** | G7: section 4 |
| 2 releases later | L10 removes the legacy path. | G8: no rollback in 2 releases |

Margin (one more review round on L2b, L4, L9; slow census): **Tue 13 Oct 15:00 UTC**. Stage 0 does not block B. Only E1 can bring busy below 40%, and only if publishes dominate. If stage 0 misses that, state writes dominate: B is the fix, and we add no more stage-0 work.

## 4. Acceptance

Commswatch reads the L8 metrics of each lock (`.lock`, `custody.lock`, SQLite write lock) on each host in 10-minute windows. We accept when all 144 windows of 24 h on every host show: busy (holder-timed holds / 600 s) below 30% for each lock; 0 `FABRIC_MESH_LOCK_TIMEOUT`; 0 `SQLITE_BUSY` failures; 0 actor preparation timeouts. Full load: for 4 h or more, live Pi processes on Ryzen 1 are at or above their p95 of the 7 days before. If not, we extend the window. A failed window restarts the 24 h after the fix.

## 5. Rollback

| Stage | Rollback |
|---|---|
| 0 (L5 to L8, E1, #560, bridge) | Install the previous RC. No format change. L5 stays "dual" until cutover. |
| E2 | An environment flag returns to E1. The event format does not change. |
| 1 shadow | `mesh.stateBackend: file`. `state.db` has no authority; keep its files until no new process uses the root. |
| 3 cutover, each root | `mesh-backend rollback` from the new release (R1), then older binaries only if necessary. Roll forward is a fresh import. |
| 4 / L10 | L10 starts only after 2 releases with no rollback. Then: last release before L10, then the stage 3 rollback. |

## 6. Start now, in parallel

- L8 lock metrics and L1 SQLite module (with the fence test): started, no dependencies.
- A1 audit of `exclusive()` sites and of each operation: read-only, no dependencies.
- C1 commswatch feed, when L8 fixes its output format.
- L6, L7: finish review round 2. E1 (#550): its owner states an ETA today; it merges before L0.
- RC3.1.5x tonight: commswatch records the "before" numbers.
