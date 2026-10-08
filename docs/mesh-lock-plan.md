# Plan: replace the single mesh lock (smarty-dev#6477)

FINAL, round 2, 2026-10-07. Inputs: design.md, review-luna.md, review-opus.md, upstream-mesh-changes.md, the #599 round 1 review.

**Summary for Paul**

1. We move shared state off the single mesh lock into one SQLite database for each mesh. Events stay in the event file.
2. Both reviews approved with changes. We apply all of them: a safe rollback, waits that do not freeze a session, and a census of old processes before each switch.
3. The first relief (custody lock, bridge fixes, idle reads) installs tonight. The heartbeat fix, the registry fix and the lock metrics follow on Thursday morning.
4. On Thursday 09:00 UTC the metrics show which work holds the lock. If the event path alone keeps it busy 20% or more, we also fix the event path at the same time.
5. Target: no single lock for state and zero lock timeouts at full load on Monday 12 October, 15:00 UTC. With delays: Tuesday 13 October.
6. "Done" means: on each of the four hosts, 24 hours (144 windows of 10 minutes) at that host's full load, each lock busy less than 30% of the time and zero lock timeouts in commswatch.

## 1. Decision: option B

Keyed state goes into `<mesh>/state.db` (SQLite WAL, `synchronous=NORMAL`, local disk only). Events, archive and dedupe receipts stay on file behind `.lock`. File custody uses `custody.lock`. Option A stays as the fallback for events only (E2). Option C adds nothing to B; its lock-domain split is R3. Upstream has no lock or store redesign; A1 classifies its new mesh-lock users (schedules, grants) at the next sync. We accept every reviewer change (L = Luna, O = Opus):

| # | Change (source) | How we apply it (lane) |
|---|---|---|
| R1 | Rollback loses no update and exposes no stale `state.json` (O P0-1, Q5; L P1-4; #599) | Two-phase fence with a fixed commit order: section 5 (L4, L9a). |
| R2 | No synchronous 10 s busy wait (O P1-1) | `busy_timeout` 5 ms or less; async retry on `MeshLockTicket`; obey `writeSignal`, `withTryLock` (L2a). |
| R3 | Classify `exclusive()` sites (O P1-2; L P2-8) | 13 sites into state, events, custody, none (A1). State sites run in `BEGIN IMMEDIATE`, one test each (L2b). |
| R4 | L5 unsafe on a mixed fleet (O P1-3) | #591 uses "dual" mode (custody, then mesh); adoption stays on mesh. "own" mode only after the census (L4). |
| R5 | The fence is partial (O P1-4, P2-8; L P1-1) | The census is the fence: each writer, bridge agent and CLI shows `meshProtocol`, or cutover stops (L4). Table for each operation (A1). Durable marker first (L4). Projector stops on a foreign write, with alarm (L3). Fence test now (L1). |
| R6 | Stage-2 dual reads are stale (O P1-5) | Stage 2 is removed. Shadow goes directly to cutover. |
| R7 | Change detection uses `state.json` (O P1-6) | `publicationGeneration`, `stateStamp` users, the L6 witness use `data_version` or a `meta` counter (L2b). |
| R8 | Measure first (O P1-7, P3-5; L P2-6, P2-7) | L8: class, wait, hold, CPU, fsync time, bytes per hold; byte share per key family. Busy = holder-timed hold / wall time. G1 reads the split. Before and after numbers go on #6477. |
| R9 | B does not move publish I/O (O P1-7, Q6; L P2-10) | E1 (#550) moves the fsyncs of no-archive publish, recovery and live compaction after unlock. Archive-coupled barriers (begin, commit, lookup, index, digest, reboot recovery) and legacy keyed `publishBatch` receipts stay under `.lock` (smarty-dev#6000, owner fabric-v2). E2 trigger: section 3. |
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
| E1 (#550) fsync after unlock, no-archive path | `store.ts` publish, recovery, compaction | none | ? | owner states ETA |
| L8 lock metrics | `commit-stats.ts`, `lock-metrics.ts` (new), timing hooks | none | 3.5 | started |
| L1 SQLite module | `state-sqlite.ts`, its test, `mesh-revision-fence.test.ts`, `docs/mesh-state-sqlite.md` | none | 5 | started |
| A1 lock-domain audit | `docs/mesh-lock-domains.md` | none | 2.5 | start now |
| C1 commswatch feed | commswatch parser (outside pi-fabric) | L8 format | 2.5 | |
| L0 split `store.ts` | `store.ts`; new `state-file.ts`, `event-log.ts`, `mesh-lock.ts` | L8, E1 | 3.5 | |
| L2a backend, async acquire | `state-backend.ts` (new), `state-file.ts`, `config.ts` | L0, L1, G1 | 3.5 | |
| L2b lock domains, callbacks | `bridge.ts`, `publication-generation.ts`, `host-reaper.ts`, `manager.ts`, `residency/host.ts`, `participant-directory.ts`, `commit-outbox.ts` (new) | L0, L1, A1, L6 | 5 | |
| L3 projector | `state-projector.ts` (new) | L2a | 3.5 | |
| L4 census, cutover, rollback | `backend-migration.ts`, `writer-census.ts`, `mesh-backend-cli.ts`, `bin/`, `host-leases.ts` | L2b, L3 | 5 | |
| L9a Linux tests, load generator | `tests/mesh-backend-*.test.ts`, `scripts/mesh-load.ts` (from #550's `measure-mesh-holds`) | L4 | 3.5 | |
| L9b Windows, power cut | `mesh-backend-windows.test.ts`, `test:smoke` list | L4 | 3.5 | |
| E2 event group commit, archive barriers | `event-log.ts`, `archive.ts`, `publishBatch` | L0, E1, G1 | 7 | only if triggered |
| L10 retire legacy | `read-journal.ts`, file state, custody "dual" | G8 | 3.5 | later |

## 3. Critical path and ETA (UTC)

Path: L8, E1, L0, G1, L2a, L3, L4, L9, RC, shadow, canary, hub, acceptance: 22.5 agent-hours of build. L2b (5 h) runs beside L2a and L3 (7 h). Each lane passes G0: independent review PASS, Linux CI and Windows CI.

| UTC | Step | Gate |
|---|---|---|
| Wed 07 21:30 | L8, L1, A1 start. RC3.1.5x installs; commswatch records the baseline. | |
| Thu 08 01:00 | L8, L6, L7 merge. L0 starts (E1 merged). | G0 |
| Thu 08 03:00 | RC with L6, L7, L8 on all hosts. | G0 |
| Thu 08 09:00 | Split from 6 h of L8 data on Ryzen 1. L2a, L2b (and E2 if triggered) start. | G1 |
| Thu 08 16:00 | L3 merges; L4 starts. | G0 |
| Fri 09 03:00 | L9 done. RC 3.2 on all hosts in `shadow` mode. | G2 spec review; G3 Windows CI, power-cut drill |
| Sat 10 03:00 | Cutover ryzen5 (canary); one rollback drill, roll forward. | G4: 0 divergences in 24 h |
| Sun 11 03:00 | Cutover ryzen3, ryzen4. Restart the old processes that the Ryzen 1 census lists. | G5: canary 24 h, 0 timeouts |
| Sun 11 15:00 | Cutover Ryzen 1 (hub); custody "own" mode. Acceptance windows start on all four hosts. | G6: census clean |
| **Mon 12 Oct 15:00** | **ETA: no single lock for state, 0 timeouts at full load.** | G7: section 4 |
| 2 releases later | L10 removes the legacy path. | G8: no rollback in 2 releases |

Margin (one more review round on L2b, L4, L9; slow census): **Tue 13 Oct 15:00 UTC**. Stage 0 does not block B. Only E1 can bring busy below 40%, and only if publishes dominate. If stage 0 misses that, state writes dominate: B is the fix, and we add no more stage-0 work.

E2 trigger (owner fabric-v2): event-path holds on `.lock` (publish, archive barriers, `publishBatch` receipts) reach 20% or more of wall time in any full-load window, at G1 on Ryzen 1 or at G4 to G6 on any cut-over host. B leaves these holds on `.lock`, so 20% keeps 10 points of margin under the 30% limit of section 4. Started at G1, E2 runs beside L2a, L2b and L3 and keeps the ETA. Started at G4 to G6, G7 waits for it (margin date).

## 4. Acceptance

Commswatch reads the L8 metrics of each lock (`.lock`, `custody.lock`, SQLite write lock) on each cut-over host (Ryzen 1, ryzen3, ryzen4, ryzen5) in 10-minute windows. Full load on host H: live Pi processes and mesh writes per minute on H are both at or above H's own p95 of the 7 days before its cutover. Where natural load falls short, `mesh-load` (L9a) adds Pi workers with keyed publishes, presence heartbeats and registry writes until H reaches that level. A window counts only at full load. We accept when each host has 144 counted windows (576 in total) and each one shows: busy (holder-timed holds / 600 s) below 30% for each lock; 0 `FABRIC_MESH_LOCK_TIMEOUT`; 0 `SQLITE_BUSY` failures; 0 actor preparation timeouts. A window below full load neither counts nor fails; that host's run extends. A failed window restarts that host's 144 windows after the fix.

## 5. Rollback

| Stage | Rollback |
|---|---|
| 0 (L5 to L8, E1, #560, bridge) | Install the previous RC. No format change. L5 stays "dual" until cutover. |
| E2 | An environment flag returns to E1. The event format does not change. |
| 1 shadow | `mesh.stateBackend: file`. `state.db` has no authority; keep its files until no new process uses the root. |
| 3 cutover, each root | `mesh-backend rollback` from the new release with the fence below, then older binaries only if necessary. Roll forward is a fresh import. |
| 4 / L10 | L10 starts only after 2 releases with no rollback. Then: last release before L10, then the stage 3 rollback. |

**Rollback fence (R1).** `meta.backend` is `sqlite`, `importing`, `exporting` or `file`. `meta.epoch` only grows; `state.json` records the epoch of its export. Import and cutover (one fenced section: census or `--assume-no-writers`, `.lock` held throughout) roll forward marker BEFORE flag (pi-fabric#627 review round 5): fill SQLite at `backend=importing` E+1 (not authoritative; `state.json` still is), verify, replace `state.json` with the moved marker, then commit `backend=sqlite`. A rerun at `importing` without the marker redoes the import from `state.json`; with the marker it only commits `sqlite`. Rollback is the mirror (review round 4): while the marker is in place legacy writers fail closed; it is removed only by the last rollback step, after `backend=file` is committed. Commit order, steps 2 to 5 under one `.lock` hold:

1. Stop v3 writers and the projector; the census shows none left.
2. DB flag: one `BEGIN IMMEDIATE` transaction at `synchronous=FULL` checks `backend=sqlite`, sets `backend=exporting` and `epoch=E+1`, commits. Writers read the flag after `BEGIN IMMEDIATE` and wait on `exporting`; readers stay on SQLite.
3. File export: the committed snapshot through the normal encoder (`readGeneration`, revision 2, epoch E+1, snapshot digest) into `state.json.rollback-<E+1>.tmp`; fsync it and the directory; the old reader reads it back (epoch E+1, generation and digest must match). `state.json` is still the marker; `state.db*` stay in place.
4. Reader switch: `BEGIN IMMEDIATE` at `FULL` checks `backend=exporting` and `epoch=E+1`, sets `backend=file`, commits.
5. Replace: rename the verified temp over the marker, fsync the directory. Older binaries start only after this step.

A reader uses `state.json` only when `backend=file` and it is a real state file whose epoch equals `meta.epoch`, or `backend=importing` without the marker and a file epoch below `meta.epoch`. With `exporting`, or while `state.json` is the marker (whatever the flag), it reads SQLite; any other mismatch fails closed with an alarm. An interrupted rollback reruns from the stored flag: `exporting` repeats steps 3 to 5 (the fence blocked every write, so the export is identical); `file` with the marker repeats step 5 (after step 3 with the recorded generation if the temp is missing or unverifiable); `file` with a real `state.json` only verifies. `mesh-backend abort-rollback` takes `.lock` first, ensures the marker, then sets `exporting` back to `sqlite` and keeps E+1. The file epoch never exceeds the database epoch and equals it only after a verified export. L9a kills the tool after each step and checks this.

## 6. Start now, in parallel

L8, L1 (with the fence test) and A1 run now, with no dependencies. C1 follows the L8 format and counts acceptance windows per host. L6, L7 finish round 2. E1's owner states an ETA today; E1 merges before L0. RC3.1.5x tonight records the "before" numbers.
