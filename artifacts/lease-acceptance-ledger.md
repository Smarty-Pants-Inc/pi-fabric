# Host-lease acceptance ledger

Starting revision: 828ab977ac8af47f8d17799be266dc44fcfdc46e.

| Requirement | Execution path / evidence | Status |
| --- | --- | --- |
| Renewal uses temp+rename and no fsync | `writeHostLease` -> `writeJsonAtomic({ durable: false })` -> nondurable `writeFileAtomic`; fsync and rename spies | PASS |
| Trace initial publication / replacement | Production callers `ParticipantDirectory.#renewFileLease` and `StoreBridgeSide.#mirror` (post-commit, owner-checked) both write soft liveness; first/replacement liveness is not execution ownership; real caller probe | PASS; baseline already nondurable |
| Retain correctness barriers | Resident acquisition lock untouched; immutable launch/custody snapshots and handover-state/serving-owner writes retain `durable: true`; real immutable/custody fsync spies plus residency-handover and atomic-write-durable suites | PASS |
| Per-host ±20% jitter | `hostLeaseRenewalInterval` SHA-256 fraction; setInterval uses helper; 128 stable independent values within bounds | PASS |
| Defaults and TTL | Nominal 5 s / TTL 15 s unchanged; configured floor 3x nominal; docs recommend 30 s / 90 s | PASS |
| Lapse and re-acquire unchanged | Public project listing drops lapsed participant and returns after lost lease re-created; directory/control/residency suites | PASS |
| Before/after bench ten leases / real 60 s | Actual baseline 0 fsyncs; after 0; durable reference 280, explicitly not baseline | PASS; reported fleet cause not present at this revision |
| Required targeted checks | Ten files: 431 passed, 28 skipped; final new probe 6 passed; typecheck, built lazy graph, fresh build | PASS |
| Scope | No live mesh, credentials, SSH/SCP, GitHub, push, or spawned agents | PASS |

Artifacts under `$TASK_OUT`: benchmark JSONs; dependencies.log; targeted-tests.log;
lease-tests.log; lease-final-tests.log; typecheck.log; lazy-graph.log; build.log;
pr-title.txt; pr-body.md; lease-acceptance-ledger.md; change.patch; head.txt.
