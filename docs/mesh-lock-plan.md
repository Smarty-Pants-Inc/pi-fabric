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
| R4 | L5 unsafe on a mixed fleet (O P1-3) | #591 uses "dual" mode (custody, then mesh); adoption stays on mesh. "own" mode only after the cutover fence (L4); the advisory census only informs it. |
| R5 | The fence is partial (O P1-4, P2-8; L P1-1) | The census is advisory, never the fence (smarty-dev#6982): it lists each writer, bridge agent and CLI with `meshProtocol` for operators; cutover never blocks or proceeds on it (L4). Table for each operation (A1). Durable marker first (L4). Projector stops on a foreign write, with alarm (L3). Fence test now (L1). |
| R6 | Stage-2 dual reads are stale (O P1-5) | Stage 2 is removed. Shadow goes directly to cutover. |
| R7 | Change detection uses `state.json` (O P1-6) | `publicationGeneration`, `stateStamp` users, the L6 witness use `data_version` or a `meta` counter (L2b). |
| R8 | Measure first (O P1-7, P3-5; L P2-6, P2-7) | L8: class, wait, hold, CPU, fsync time, bytes per hold; byte share per key family. Busy = holder-timed hold / wall time. G1 reads the split. Before and after numbers go on #6477. |
| R9 | B does not move publish I/O (O P1-7, Q6; L P2-10) | E1 (#550) moves the fsyncs of no-archive publish, recovery and live compaction after unlock. Archive-coupled barriers (begin, commit, lookup, index, digest, reboot recovery) and legacy keyed `publishBatch` receipts stay under `.lock` (smarty-dev#6000, owner fabric-v2). E2 trigger: section 3. |
| R10 | WAL growth (O P2-1; L P2-5) | `wal_autocheckpoint=0` in clients; projector checkpoints (PASSIVE, TRUNCATE above 64 MB); no read open across `await`; WAL size alarm (L2a, L3, L8). |
| R11 | Callback I/O; `afterCommit` before `COMMIT` (L P1-2; O P2-2) | File reads before `BEGIN`, checked again by stamp. File effects after `COMMIT` from a durable idempotent outbox (L2b). Crash test (L9a). Exception (A1 X15): actor adoption (`manager.ts` `#confirmAdoption`) commits the actor registry inside a state transaction that writes nothing (a no-op `writeBatch`, zero-wait). That transaction is the read fence against `resumeLineage()`. The registry commit is the claim itself, decided on that snapshot, so it cannot go to the outbox. Order stays registry lock, then state. |
| R12 | Power-loss contract (L P1-3; O P3-3) | Each key family is replayable or uses a `FULL` connection (L1). Power-cut drill on a VM (L9b). |
| R13 | L6 keeps #24 evidence (O P2-3; L P2-8) | #592 uses a commit witness that only lock holders write, else takes the lock. Round 2 checks. |
| R14 | L7 keeps the tail invariant (O P2-4; L P2-8) | #590 appends at the exact offset and reads its bytes back. Round 2 checks; if it fails, L7 stops. |
| R15 | 32 MiB cap (O P2-5) | Enforced in the SQLite write until L10 (L2a). |
| R16 | Local disk only (O P2-7; L P2-5) | `statfs` guard: nfs, cifs, sshfs, 9p, virtiofs use `file` (L2a). |
| R17 | Mixed-version bridge (L P2-9; O P2-8) | Old and new side tests: mirror, replay, leases (L9a). Alarm: old bridge on a cut-over hub (L4). |
| R18 | Windows (O P3-1; L P2-5) | Bounded retry on `SQLITE_IOERR`, `SQLITE_CANTOPEN` (L2a). Concurrent-writer Windows smoke (L9b). |
| R19 | `node:sqlite`, Bun (O P3-2; L P2-5) | Adapter interface; Node 24+; conformance test also under Bun (L1). |
| R20 | Spec review, lock order, `control-seen` (O P3-4, P3-6 to P3-8) | Spec review of the adapter note (G2). Order: registries, then state transaction; `.lock` inside it only in migration tools (L0). `control-seen` stays on `file`. Bridge F2 fence (A1 finding 3, `bridge.ts` publish `data()`): after cutover, state writers no longer take `.lock`, so the fence moves into the state revision check. Under `.lock`, `data()` opens `BEGIN IMMEDIATE` on `state.db`, runs `holds()` on that snapshot and keeps the transaction open until the append returns, then rolls back (it writes no state). A native takeover commits either before this snapshot, so the event is refused, or after the append. This is the one allowed order of events, then state (bridge publish only; L2a hook, L9a old/new test). |

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
| Sun 11 15:00 | Cutover Ryzen 1 (hub); custody "own" mode. Acceptance windows start on all four hosts. | G6: census report reviewed (advisory, #6982) |
| **Mon 12 Oct 15:00** | **ETA: no single lock for state, 0 timeouts at full load.** | G7: section 4 |
| 2 releases later | L10 removes the legacy path. | G8: no rollback in 2 releases |

Margin (one more review round on L2b, L4, L9; slow census): **Tue 13 Oct 15:00 UTC**. Stage 0 does not block B. Only E1 can bring busy below 40%, and only if publishes dominate. If stage 0 misses that, state writes dominate: B is the fix, and we add no more stage-0 work.

E2 trigger (owner fabric-v2): event-path holds on `.lock` (publish, archive barriers, `publishBatch` receipts) reach 20% or more of wall time in any full-load window, at G1 on Ryzen 1 or at G4 to G6 on any cut-over host. B leaves these holds on `.lock`, so 20% keeps 10 points of margin under the 30% limit of section 4. Started at G1, E2 runs beside L2a, L2b and L3 and keeps the ETA. Started at G4 to G6, G7 waits for it (margin date).

## 4. Acceptance

Commswatch reads the L8 metrics of each lock (`.lock`, `custody.lock`, SQLite write lock) on each cut-over host (Ryzen 1, ryzen3, ryzen4, ryzen5) in 10-minute windows. Full load on host H: live Pi processes and mesh writes per minute on H are both at or above H's own p95 of the 7 days before its cutover. Where natural load falls short, `mesh-load` (L9a) adds Pi workers with keyed publishes, presence heartbeats and registry writes until H reaches that level. A window counts only at full load. We accept when each host has 144 counted windows (576 in total) and each one shows: busy (holder-timed holds / 600 s) below 30% for each lock; 0 `FABRIC_MESH_LOCK_TIMEOUT`; 0 `SQLITE_BUSY` failures; 0 actor preparation timeouts. A window below full load neither counts nor fails; that host's run extends. A failed window restarts that host's 144 windows after the fix.

### mesh-load hub profile

Measured on ryzen5 on 2026-10-08 (load average about 50 on 32 CPUs, from other lanes). Each run used a scratch mesh under `/srv/scratch/paul/tmp-fs-hold`, seeded to 4.8 MB (`--seed-state-mb 4.8`, the shadow soak's hub-sized state), with `nice -n 10 taskset -c 4-11`, `--max-in-flight 1` (the default) and about 200-290 s per run. Lock figures come from L8 `lock-stats` over complete minutes, excluding the first. Wait p99 is a histogram bucket upper bound. Generator CPU is (user + sys) / wall from `/usr/bin/time`, covering the controller and every worker.

The fleet hub runs at 55-65% busy with 25-40 timeouts/min. On a fresh mesh, holds last about 0.6 ms and handoff stops at about 150 acquisitions/s, so busy topped out near 23% with heavy timeouts. A keyed put rewrites the whole state under the lock, about 5 ms per MB, so a seeded state lengthens each hold. Puts alone stall near 40%, though. Every commit invalidates the snapshots the other writers prepared, so the lock sits idle while they parse the state again. Custody ops (`--custody-share`) read and parse the state under the lock without writing, and they fill that gap.

| setting | writes/min achieved | lock acq/min | hold mean | busy % (per minute) | wait p99 | timeouts/min | generator CPU |
|---|---|---|---|---|---|---|---|
| put 0.5, target 2400, 8 procs (→11) | 595 | 1052 | 20.9 ms (put 27.7) | 36.7 (37.5/35.8) | ≤1 s | 1.5 | 1.65 |
| put 0.5, target 2400, 18 procs (→21) | 410 | 1006 | 23.7 ms | 39.6 (36.8/42.5) | ≤1 s | 42.5 | 1.98 |
| put 0.4, custody 0.4, target 480, 10 procs (→14) | 322 | 578 | 47.7 ms (custody 100) | 46.0 (46.3/44.5/47.1) | ≤2.5 s | 1.7 | 0.79 |
| put 0.4, custody 0.4, target 2400, 10 procs (→13) | 483 | 1044 | 35.3 ms (custody 95) | 61.4 (1 full minute) | ≤2.5 s | 17 | 1.45 |
| **hub profile**: put 0.4, custody 0.4, target 2400, 12 procs fixed | 373 | 904 | 40.4 ms (custody 126, put 25.7) | **60.9 (64.1/57.7)** | ≤1 s | 19.5 | 1.15 |

Hub profile, about 60% busy at about 20 timeouts/min. Add processes or raise `--custody-share` to push the timeouts toward the hub's 25-40/min:

```sh
nice -n 10 taskset -c 4-11 bun scripts/mesh-load.ts --root <scratch-mesh> --seed-state-mb 4.8 \
  --put-share 0.4 --custody-share 0.4 --target-writes-per-min 2400 --target-processes 12 --max-workers 12 --duration 600
```

The target is set above what 12 workers can complete with one write in flight each. Each worker therefore paces at its own completion rate and skips (and counts) the writes that come due while it is busy and does not queue them. `--max-workers` pins the process count. With a reachable target, the controller adds a worker only when a whole control window (one that does not include a resize) falls below 90% of the target. It sheds a worker above 110% or above 65% lock busy.

## 5. Rollback

| Stage | Rollback |
|---|---|
| 0 (L5 to L8, E1, #560, bridge) | Install the previous RC. No format change. L5 stays "dual" until cutover. |
| E2 | An environment flag returns to E1. The event format does not change. |
| 1 shadow | `mesh.stateBackend: file`. `state.db` has no authority; keep its files until no new process uses the root. |
| 3 cutover, each root | `mesh-backend rollback` from the new release with the fence below, then older binaries only if necessary. Roll forward is a fresh import. |
| 4 / L10 | L10 starts only after 2 releases with no rollback. Then: last release before L10, then the stage 3 rollback. |

**Writer census is advisory (smarty-dev#6982).** `src/mesh/writer-census.ts` reports writers (release, lock protocol, backends) and `unknown` evidence, each with a reason, for logs and operators. It has no `clean` verdict and is never a cutover gate: lock owners and queue tickets name only a pid, so a remote writer whose pid matches a live local writer cannot be told apart (pi-fabric#638 review round 9). No startup, CLI or migration path blocks, permits or changes behaviour on it.

**Rollback fence (R1).** `meta.backend` is `sqlite`, `importing`, `exporting` or `file`. `meta.epoch` only grows; `state.json` records the epoch of its export. Import and cutover (one fenced section: `.lock` and `custody.lock` held throughout; nothing else gates it, the census only advises) roll forward marker BEFORE flag (pi-fabric#627 review round 5): fill SQLite at `backend=importing` E+1 (not authoritative; `state.json` still is), verify, replace `state.json` with the moved marker, then commit `backend=sqlite`. A rerun at `importing` without the marker redoes the import from `state.json`; with the marker it only commits `sqlite`. Rollback is the mirror (review round 4): while the marker is in place legacy writers fail closed; it is removed only by the last rollback step, after `backend=file` is committed. Commit order, steps 2 to 5 under one hold of the same fence (`custody.lock`, then `.lock`):

1. Stop v3 writers and the projector; check the advisory census report for any left (it informs the operator, it does not prove none remain).
2. DB flag: one `BEGIN IMMEDIATE` transaction at `synchronous=FULL` checks `backend=sqlite`, sets `backend=exporting` and `epoch=E+1`, commits. Writers read the flag after `BEGIN IMMEDIATE` and wait on `exporting`; readers stay on SQLite.
3. File export: the committed snapshot through the normal encoder (`readGeneration`, revision 2, epoch E+1, snapshot digest) into `state.json.rollback-<E+1>.tmp`; fsync it and the directory; the old reader reads it back (epoch E+1, generation and digest must match). `state.json` is still the marker; `state.db*` stay in place.
4. Reader switch: `BEGIN IMMEDIATE` at `FULL` checks `backend=exporting` and `epoch=E+1`, sets `backend=file`, commits.
5. Replace: rename the verified temp over the marker, fsync the directory. Older binaries start only after this step.

A reader uses `state.json` only when `backend=file` and it is a real state file whose epoch equals `meta.epoch`, or `backend=importing` without the marker and a file epoch below `meta.epoch`. With `exporting`, or while `state.json` is the marker (whatever the flag), it reads SQLite; any other mismatch fails closed with an alarm. An interrupted rollback reruns from the stored flag: `exporting` repeats steps 3 to 5 (the fence blocked every write, so the export is identical); `file` with the marker repeats step 5 (after step 3 with the recorded generation if the temp is missing or unverifiable); `file` with a real `state.json` only verifies. `mesh-backend abort-rollback` takes the fence first, ensures the marker, then sets `exporting` back to `sqlite` and keeps E+1. The file epoch never exceeds the database epoch and equals it only after a verified export. L9a kills the tool after each step and checks this.

**Operator commands (W1).** `fabric-mesh-backend` (package `bin`, `bin/fabric-mesh-backend`) runs on the host of the root, from the new release, with every writer stopped first. In shadow mode the residency host runs the L3 projector (database `<mesh>/state-projector/state.db`, never `<mesh>/state.db`, which is the cutover flag) and stops it on shutdown or reload, so stopping the host stops both.

```sh
fabric-mesh-backend census   --root <mesh>   # the L4a writer census, ADVISORY: "advisory: N writers, M unknown" and each entry; exit 0
fabric-mesh-backend status   --root <mesh>   # flag, epochs, digests, reader decision, advisory census; 0 fence holds, 3 violated
fabric-mesh-backend cutover  --root <mesh>   # file -> sqlite (the fenced section above); 0 done, 3 refused (also a reader not ready or no reader registered: docs/mesh-backend.md)
fabric-mesh-backend reader-proof --root <mesh> --name N --backend sqlite  # a reader proves a real read for the readiness gate
fabric-mesh-backend rollback --root <mesh>   # sqlite -> file, steps 1 to 5; 0 done (a rerun converges), 3 refused
fabric-mesh-backend abort-rollback --root <mesh>  # exporting -> sqlite at E+1; 0 done, 3 refused
```

Exit codes: 0 done, 1 error, 2 usage, 3 refused or fence violation (nothing unsafe was done). Census decision (org, 10-08; smarty-dev#6982): the census never gates the cutover. Import, cutover, rollback and abort-rollback are fenced only on the mesh `.lock` plus `custody.lock` (custody first, as `withMeshCustody`), each held for the whole section; a held lock waits up to `--lock-timeout-ms` and then fails with nothing changed. The census runs once inside the fence and is printed to stderr as `advisory: N writers, M unknown` (also `census` in `--json`); it is never "safe" or "clean" and never blocks or permits anything. `--assume-no-writers` is gone. The operator stops every writer (Pi sessions, actors, mesh-bridge, the projector) before a cutover; a legacy writer that queues on `.lock` meanwhile fails closed on the moved marker. `--json` prints the result as JSON.

## 6. Start now, in parallel

L8, L1 (with the fence test) and A1 run now, with no dependencies. C1 follows the L8 format and counts acceptance windows per host. L6, L7 finish round 2. E1's owner states an ETA today; E1 merges before L0. RC3.1.5x tonight records the "before" numbers.
