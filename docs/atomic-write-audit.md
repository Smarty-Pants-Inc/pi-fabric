# Atomic-write caller audit (smarty-dev#2479)

Audited `src/` at **3f6a2963**, after pi-fabric#180 / 87c5dd6d. All locations below are **baseline line numbers** (stable review references, not post-patch line numbers). Imported names, wrapper calls, synchronous/asynchronous renames and hard-link publication were searched. The shared helper internals (`core/atomic-write.ts:69,222,245,260,287,301`) are implementations, not additional policy callers: sync opt-in already fsyncs file -> rename -> physical/lexical containing namespace; async deliberately has no durable option.

## Acceptance ledger

- Enumerate each direct helper call, wrapper expansion, and raw temp-write/rename or marker publication.
- Classify correctness-bearing state independently from its filename: durable-before barriers for accepted identity/lineage, queues, receipts, journals, revision clocks and terminal results; no fsync for reconstructible/disposable hints.
- Test each fixed caller class through helper options or real `fs.fsyncSync` ordering; demonstrate failure against baseline and success after.
- Typecheck, targeted touched/integration suites only, fresh build, lazy graph, cold/idle/first-use startup checks; no graph-budget increase.
- Record runtime write cost and keep expiring presence, ordinary mesh event publication/reservation, read signals, caches and diagnostics fast.

`Today?` means **before this patch**. `ordering-dependent` identifies A-before-B protocols; `must-be-durable` identifies authoritative acknowledged state without a separate protocol edge. Harmless means loss/replay has no authority or transaction-corruption effect under the stated recovery contract, not that the data has no value.

## Shared atomic helper callers and wrapper expansions

| Baseline file:line (`src/`) | State / dependency | Risk class | Durable today? | Fix / disposition |
|---|---|---|---|---|
| actors/registry-store.ts:152 | Ordinary actor definitions and lineage ownership claims; claim precedes queue takeover | ordering-dependent | No | Request durable for every authoritative registry replacement, not just removals |
| actors/registry-store.ts:164 | Removal/revocation registry commit, after cleanup marker | ordering-dependent | Yes | Retain durable |
| actors/registry-store.ts:170 | Registry rollback restoring earlier lineage/definitions/removal | must-be-durable | Only when earlier removal exists | Always request durable rollback |
| actors/global-registry.ts:65 (save at 571) | User's global actor template library | must-be-durable | No | Request durable; not a regenerable catalog cache |
| actors/binding-store.ts:170 | Session-specific model/thinking overrides | must-be-durable | No | Request durable |
| actors/manager.ts:1054 | Native session header; archive rename precedes new header | ordering-dependent | Persistent actors only | Keep conditional durable header; add archive namespace barrier before replacement |
| actors/manager.ts:1942 | Cleanup obligation marker before registry revocation | ordering-dependent | Yes | Retain durable |
| actors/manager.ts:3338 (persist calls at 2062,2107,2174,2482,3293,3353,3464,3784,3887,3922) | Activation queue and validity revisions before mesh cursor advance / predecessor-source unlink | ordering-dependent | Only explicit security fence call (2482) | Default persistent queue writes to durable; require success for new/coalesced callerless ingress before delivery/cursor acceptance; sync empty-queue and predecessor unlink namespaces |
| actors/mesh-monitor.ts:284 | Restart cursor after activation queues are saved | ordering-dependent | No | Request durable checkpoint; retain ten-second batching and best-effort replay semantics |
| agents/manager.ts:502 (1600,2118,2189,2211,2229) | Host-produced stopped/failed/timed-out terminal status before settlement | ordering-dependent | No | Request durable |
| main-agent.ts:578 | Consumed IDs / halted state before source acknowledgement | ordering-dependent | Yes | Retain durable |
| main-agent.ts:583 | Follow-up/completion journal before acknowledging resident source | ordering-dependent | Yes | Retain durable |
| mesh/store.ts:350 -> 592,685 | Event sequence reservation / recovered maximum | harmless | No | Keep fast: next sequence is max(counter, surviving live tail); archive promotes synced events after reboot. Reservations without surviving events are not accepted receipts |
| mesh/store.ts:350 -> 1179 | Authoritative state envelope, CAS revision clock and tombstones (including receipts/grants/schema state) | must-be-durable | No | Request durable specifically for state commit; an issued revision must not regress/recur after power loss |
| mesh/store.ts:350 -> 1688 | Compaction generation after replacing live log | ordering-dependent | No | Request durable generation only after syncing replacement log and its published namespace |
| mesh/store.ts:1206 | Optional namespace digest/read-generation signal | harmless | No | Keep fast; readers validate canonical header/inode/stamp/digest and fall back on mismatch |
| mesh/bridge.ts:871 | Bidirectional forwarding cursor after destination publication | ordering-dependent | No | Request durable cursor; destination StoreBridgeSide.publish explicitly requests durable live append first |
| mesh/archive.ts:205 | PENDING process-crash rollback hint | harmless | No | Keep fast: on a new boot recovery treats complete synced archive lines as truth, not PENDING |
| mesh/archive.ts:462 | HEAD process-crash/catch-up hint | harmless | No | Keep fast: reboot recovery scans archived lines; it does not discard complete lines on an unsynced HEAD |
| mesh/archive.ts:468 | MESH descriptive provenance (meshRoot/host/start sequence) | harmless | No | Keep fast; not admission or an acknowledged receipt |
| mesh/archive.ts:516 | SEAL closed-day summary/hashes | harmless | No | Keep fast: source lines and day namespaces are synced first; absent seal means rescan, not lost events |
| topology/host-leases.ts:36 | Expiring renewable host liveness | harmless | No | Keep fast: reboot kills holders, expiry/re-registration restores presence; shared lease provides fallback |
| topology/participant-files.ts:60 | Conditional per-owner participant presence | harmless | No | Keep fast: re-registration and expiring owner-host lease govern authority, not reboot-persistent file presence |
| topology/participant-files.ts:87 | Unconditional participant presence (tools/tests) | harmless | No | Same as above |
| topology/participant-files.ts:169,192,194 | Per-key lock detach/recovery/restore | harmless | No | Keep fast: process synchronization, not persistent accepted ownership; dead holders are recoverable |
| residency/client.ts:74 -> 193,205,220 | Resident host config/model guidance before starting a host | ordering-dependent | No | Request durable config |
| residency/client.ts:74 -> 436 | completionConsumedAt receipt before completion retraction/source cleanup | ordering-dependent | No | Request durable receipt |
| residency/client.ts:74 -> 593 | Resident command request before host pickup/decision | ordering-dependent | No | Request durable request |
| residency/actor-client.ts:146 | Nested actor command request before pickup/decision | ordering-dependent | No | Request durable request |
| residency/host.ts:140 -> 338 | Saved terminal result before run-directory collection / completion delivery | ordering-dependent | No | Request durable result |
| residency/host.ts:140 -> 476 | Owner PID/start-token and ready status | harmless | No | Explicitly keep fast: stale owner is checked against live process/start identity, never reboot authority |
| residency/host.ts:140 -> 805 | Durable agent ownership metadata before fast completion publication | ordering-dependent | No | Request durable metadata |
| residency/host.ts:140 -> 917 | Command response before deleting processing request | ordering-dependent | No | Request durable response |
| residency/host.ts:140 -> 930 | Removal summary for client diagnostics | harmless | No | Explicitly keep fast: authoritative cleanup markers and registry already carry barriers |
| residency/host.ts:140 -> 954 | Interrupted-request response before deleting processing file | ordering-dependent | No | Request durable response |
| residency/host.ts:140 -> 1050 | Startup error diagnostic | harmless | No | Explicitly keep fast; cannot authorize or acknowledge work |
| schema/controller.ts:62 -> 371,381 | Prepared/applying before-images journal before consuming certificate / mutating workspace | ordering-dependent | No | Request durable journal |
| schema/controller.ts:62 -> 415,487,859,865 | Committed/rolled-back/recovery journal status | ordering-dependent | No | Request durable; sync workspace effects/restore-before-images before terminal marker and authoritative mesh outcome |
| storage/retention.ts:50 | Run-root owner heartbeat/closed/orphan-age collection hints | harmless | No | Keep fast: absent/invalid owner prevents deletion; live PID/descendant/unresolved checks still veto collection |
| storage/retention.ts:87 | unresolved-worker marker prohibiting deletion | ordering-dependent | No | Request durable before a later terminal status/cleanup decision can survive |
| storage/retention.ts:191 | Sweep throttling timestamp | harmless | No | Keep fast: losing it only permits an extra sweep, whose independent safety checks still run |
| repairs/store.ts:92,225 | Sync/async learned catalog repair table | harmless | No | Keep fast: bounded learned lookup hints, not authority; losing changes requires re-learning/explicit ref resolution |
| entropy/pool-store.ts:203,233 | Sync/async observation reservoir | harmless | No | Keep fast: optional learned observations, no receipts/source acknowledgement depend on persistence |
| entropy/compiled-store.ts:227,257 | Sync/async derived entropy surface | harmless | No | Keep fast: optional optimization artifact; absence/reversion disables/changes guidance, not authority |
| config.ts:1466 (custom writeJsonAtomic at 1431) | Validated user/project config, checked-source temp-file publication | must-be-durable | File + leaf parent only; POSIX EPERM swallowed | Retain checked-source/mode behavior; use shared namespace barrier for new ancestors/symlink parents and fail closed on POSIX |
| config.ts:1501,1590; components/configuration.ts:88 | Migration/update/component config through custom config writer | must-be-durable | Partial (delegated) | Inherit custom writer ancestry fix; not the general helper despite same name |

## Raw publication paths

| Baseline file:line (`src/`) | State / dependency | Risk class | Durable today? | Fix / disposition |
|---|---|---|---|---|
| records/client.ts:91-92 | Enrollment nonce before remote register commit; token before open returns | ordering-dependent | No | Replace raw write+rename with durable shared JSON writer (0600/0700/newline preserved); tests use synthetic local protocol only |
| records/server.ts:248,272 | Installer pending credential before DB rotation; publish pending -> canonical | ordering-dependent | Yes: file + directory ancestry before DB, parent after rename | Retain |
| records/server.ts:704 | Public factory health/status JSON | harmless | File sync only | Keep existing implementation; parent durability unnecessary for diagnostic status |
| records/anchor-export.ts:92 | Anchor segment before heartbeat/ack | ordering-dependent | Yes: pending file + parent/ancestry + directory after rename | Retain |
| records/anchor-export.ts:114 | Anchor heartbeat after durable anchor | ordering-dependent | Yes: file + directory | Retain |
| worker/run-record.ts:56,86 | Native-source worker terminal status before host settlement/receipt | ordering-dependent | No | Self-contained file fsync before rename + lexical/physical parent ancestry after; preserve native .ts boundary |
| worker/run-record.ts:56,86 (running/progress) | Replaceable progress display | harmless | No | Keep non-terminal writes unsynced |
| worker/reply-tool.ts:41-42 | Structured reply before terminal status / delivered response | ordering-dependent | No | File fsync before rename + parent ancestry after, without importing host APIs |
| worker/run-log.ts:373 | Bounded terminal log compaction | harmless | Temp file synced, no parent sync | Keep: old/new log both valid; status/receipts do not use compaction as authority |
| actors/manager.ts:996 | Session -> rotation backup before new session header/pruning | ordering-dependent | No | Fsync complete source inode, then rename and confirm archive namespace before dependent writes |
| actors/manager.ts:1052 | Malformed session -> preserved orphan before repaired header | ordering-dependent | No | Fsync complete source inode, then rename and confirm archive namespace before replacement |
| residency/host.ts:706 | Request -> processing pickup before decision/mutation | ordering-dependent | No | Sync both destination and source namespaces after cross-directory rename |
| residency/protocol.ts:84-86 | Immutable committed/abandoned decision hard-link CAS before mutation/exchange deletion | ordering-dependent | No | Fsync opened temp before link; sync published namespace before returning winner; on EEXIST confirm the actual winning inode's file and bound namespace barriers before accepting the decision |
| mesh/store.ts:1683-1684 | Compacted retained event-log bytes before new cursor generation | ordering-dependent | No | Fsync temp file -> rename -> namespace barrier -> durable generation |
| mesh/store.ts:676-685 | Recovered live archive lines before durable BOOT marker | ordering-dependent | File fdatasync, missing namespace barrier | Sync live-log namespace before BOOT can publish |
| mesh/archive.ts:140 | Durable BOOT identity distinguishing process crash from power loss | ordering-dependent | Yes: file sync -> rename -> parent sync | Retain; recovered live names now explicitly precede it |
| providers/mcp-descriptor-cache.ts:226 | Rediscoverable MCP descriptor catalog | harmless | No | Keep fast |
| ui/conversation-native-reader-checkpoint.ts:29-30 | Process-private suspended reader snapshot in scratch | harmless | No | Keep fast: no process/power-loss resume contract or surviving dependent receipts |
| core/damaged-file.ts:14 | Quarantined unreadable file evidence | harmless | No | Keep: non-authoritative damaged evidence; validation independently fails closed/recreates caches |
| actors/registry-store.ts:77,107 | Registry lock owner and recovery fence directory | harmless | No | Keep fast: all holders die on reboot; liveness/start checks govern reclaim, not durable lock entries |
| actors/binding-store.ts:202 | Binding lock owner | harmless | No | Keep fast; not a persistent binding record |
| topology/participant-files.ts:148-150 | Complete staging lock owner -> per-key lock directory | harmless | No | Keep fast; dead process synchronization only |
| core/file-lock.ts:59,65,175,181,210 | Sync/async dead-lock claim/restore and owner record | harmless | No | Keep fast; compare-and-reclaim dead process locks, no persistent mutation acknowledgement |
| mesh/store.ts:1511,1519,1533,1577,1621,1629 | Mesh lock owner, staging publish, release/recovery fences | harmless | No | Keep fast; never persistent resource grants/leases or receipt state |

## Dependent pairs and limits

Barriers added at A and B as needed: registry claim -> queue takeover; saved queue -> replay checkpoint/source unlink; saved terminal result -> completion/collection; consumed metadata -> completion retraction; request -> processing pickup -> immutable decision -> registry/agent metadata -> response -> processing deletion; prepared/applying journal -> workspace mutation -> synced effects -> authoritative workspace outcome -> terminal journal; structured reply -> terminal status -> completion receipt; live bridge destination -> forwarding cursor; compacted log -> generation; recovered live log -> BOOT.

The existing helper's POSIX directory durability assumption remains: the filesystem/device honors fsync, and directory barriers are skipped on Windows (file barriers still apply). Tests are barrier-contract tests, **not physical power-cut certification**. Ancestor/symlink namespace correctness remains provided by #180's shared barrier. Self-contained native worker modules sync lexical and real parent ancestry; arbitrary multi-hop symlink topology and actual Windows power-loss guarantees still require platform certification.

Ordinary **event publication without an archive is intentionally volatile**; enabling the archive provides its documented at-least-once reboot replay. Bridge publication now requests explicit live durability before cursor checkpointing. Event-log/generation replacement is still a two-file protocol: these barriers prevent a surviving generation from outrunning log bytes, but they do not make a stop between the two renames transactional or eliminate the existing lock-free cursor-read race. Exactly-once replay and that protocol redesign are owner gates, not claims of this patch.

Authoritative mesh state writes and ordinary actor registry writes are **not harmless hot caches**: they now pay fsync cost. There is no blanket change to the general helper default, graph budget, startup lifecycle, expiring presence, read signals, sequence hints or ordinary mesh publication. Review state/registry write latency on deployed storage before rollout; do not restore unsafe clock/lineage rollback merely to recover a benchmark number.

## Mesh state group-commit barrier (#2479)

State mutations now do **write temporary bytes -> atomic rename** under the mesh
mutation lock, with **zero fsyncs in that critical section**. Optional read signals
remain hints. Receipts, grants, schema outcomes and public put/delete/batch CAS
results retain their default durable acknowledgment: after releasing the mutation
lock, the caller waits for a completed barrier whose generation covers the
observed/committed `highWater`. No-op deletes and all-skipped CAS batches also wait
for the head they observed. A host-only `durable: false` opt-out returns a
**speculative** version, never a durable receipt; the public provider does not
expose or forward this option. Ordinary volatile events/reservations stay unchanged.

The allocation clock is the monotone commit-generation counter, serialized just
after `readGeneration` in the bounded canonical header. A lazily loaded engine
coalesces same-turn requests and elects a single cross-process barrier on
`.state-durability-lock`, using the existing dead-owner/PID-start lock protocol.
This is **not** the mutation lock: successors can continue writing while a file
fsync is in flight. A continuously queued/busy writer batches on a 250 ms cadence;
a newly idle queue flushes immediately (after microtask coalescing), without a
250 ms penalty for sequential operations. Peers join an already completed
covering generation rather than performing duplicate barriers.

Each steady-state elected barrier uses at most two fsync calls:

1. Hard-link the latest complete state inode to a private checkpoint temporary
   name, open it, and **asynchronously fsync the file once**.
2. Rename that pinned inode to `state.durable.json`, then **fsync the parent once**
   on POSIX. Publish a volatile completion hint only after both barriers succeed;
   resolve only receipts whose generation is covered.

Pinning retries a bounded transient ENOENT lookup race with concurrent atomic
replacement, before any fsync or receipt. Windows file barriers use a writable,
noncreating/nontruncating `r+` handle.

The hard link writes no second state payload. It is necessary: simply syncing
`state.json` and its parent after handoff is not enough when a later unsynced
rename can replace the acknowledged inode before a power failure. The checkpoint
retains the last synced snapshot independently of those speculative replacements.
It is itself replaced only **after** its successor's data fsync, with barrier
owners serialized, so a crash during checkpoint publication can recover either
complete synced snapshot, never an unsynced replacement of the last receipt.

**Recovery:** on a missing, torn/unreadable, or lower-clock canonical file, readers
and the next mutation recover the newer valid checkpoint. The next allocation
therefore exceeds every **ACKED** commit's clock. An unacknowledged speculative
commit may survive or be lost and its revision may be reissued; callers must not
promote speculative reads/versions into durable external receipts. Failed file or
parent barriers reject queued receipts, do not advance successful coverage, and
publish a failed-attempt hint for already queued peers. A subsequent request
retries the owed barrier. Failure is not rollback: a rejected mutation may remain
visible, and callers must re-read before retrying a CAS. A checkpoint published by
a failed parent barrier is not an acknowledgment; a later barrier must complete.

Namespace receipts still require a successful full physical/lexical ancestry and
symlink barrier, lazily prepared/revalidated **outside** the mutation lock. Setup
and namespace recovery/reconfirmation may owe additional fsyncs; the <=2 budget is per prepared
steady-state barrier, not per new process or changed namespace. All other audited
durable classes and the general atomic-write defaults are unchanged. Directory
fsync is unsupported/skipped on Windows as before; physical power-cut guarantees
still require platform/filesystem certification. Checkpoint files are immutable
rename-only snapshots: external in-place modification is outside this contract.
Recovery requires group-commit-aware hosts; older installed builds do not know
about the checkpoint, so mixed-version reboot is not covered by this guarantee.

Hard links change the canonical inode's ctime. The barrier restamps a read signal
only when its exact pre-link stamp, canonical UUID, pinned inode, size and mtime
still match. A concurrent successor or legacy copied-marker replacement forces
ordinary canonical fallback; no signal can hide a changed authoritative payload.
The engine has a stable package-local lazy entry, loads only at actual first
state use, and does not increase the eager graph budget.

Regression evidence covers generation/ack ordering, no acknowledgment before a
covering barrier, shared coalescing/250 ms busy cadence and immediate idle flush,
file/parent failure propagation and retry (including a peer/no-op receipt), fast
non-durable writes, zero mutation-lock fsyncs, <=2 prepared barrier calls,
namespace/symlink recovery, and unchanged read-generation/ABA/idle behavior.
Owned-child SIGKILL probes combine actual process crash with deterministic loss
of the volatile canonical namespace (missing/torn/older). They are contract tests,
**not physical power-cut certification**.

### Historical group-commit performance sample on ryzen2

This is the earlier passing sample, not unconditional owner approval. Round 2's
later nice-19 comparison recorded a 1,195 ms candidate maximum hold (limit <1 s),
so the owner performance gate remains on hold pending final-head measurement and
owner verification. Native Windows CI and the named Astra enrollment nonce/token
security pass are separate before-merge gates.

Three fresh 300-second runs, same 2,301,068-byte fixture and SHA256, five nice-0
writers, 300 mutations/min, 24 volatile publications/min. Each completed 1,500
mutations with zero errors and exact highWater/entry/sequence checks.

| Build | Held % | Hold p90 / max ms | Durable ACK p90 ms | In-lock fsyncs |
|---|---:|---:|---:|---:|
| main 3f6a2963 | 16.04 | 42.5 / 162.5 | N/A (volatile API 64.9) | 0 |
| 06b8df10 | 25.24 | 110.4 / 274.0 | 152.8 | 3000 |
| Group commit (this change) | 15.66 | 40.6 / 129.2 | 160.7 | 0 |

Gate PASS: -0.37 pp versus main (limit +7), max 129.2 ms (<1000), 0 locked fsyncs, 2874 / 1437 barriers (<=2 each). Durable ACK p90 160.7 ms; post-lock barrier wait p90 122.4 ms.

Namespace preparation/reconfirmation is additional outside-lock work, separately
reported in the retained harness results. A rare source-pin lookup race found in
an intermediate run was fixed with bounded pre-barrier retries; the final run
above is a new complete run of the hardened compiled artifact. Physical power-cut
and mixed-version recovery remain explicitly outside this certification.

### Historical in-lock barrier cost gate (`06b8df10`, before group commit)

Same 2,301,068-byte synthetic fixture, five independent Node v24.19.0 writers at
nice 0, 300-second runs, 300 state mutations/min and 24 volatile publications/min.
Both current control and final head completed 1,500 mutations with zero errors;
highWater, live entry count, and event sequence checks passed.

| Build | Held % | Holds/min | Hold median/p90/max ms | Fsync/locked state commit |
|---|---:|---:|---:|---:|
| main `3f6a2963` (fresh control) | 15.51 | 305.0 | 29.1 / 38.5 / 139.9 | 0 |
| PR `77168bf4` (previous retained run) | 29.18 | 303.6 | 39.5 / 122.5 / 280.2 | 6 |
| This follow-up | 25.84 | 305.2 | 36.8 / 99.9 / 270.2 | 2 |

**Occupancy gate FAIL:** +10.33 percentage points versus main, above +7. The
maximum-hold gate passes (270.2 ms <1 s). This is **not rollout acceptance**.
The older PR row is historical evidence, not a contemporaneous third control;
host load varies substantially. Time-weighted final/control occupancy is
26.13% / 15.65%, and also fails the relative gate.

Final head paid 3,000 in-lock commit fsyncs plus **1,105 outside-lock namespace
setup/reconfirmation fsyncs**, 4,105 total / 1,500 mutations (2.737 overall per
mutation). Unrelated ancestor entry changes on this busy host conservatively
invalidated receipts; initialization alone would cost 25 calls at this path.
Therefore the <=2 assertion applies to each locked commit and to total calls in
an unchanged prepared namespace, not to setup/recovery-inclusive API calls.
No publication fsyncs occurred. A separate sustained strace diagnostic found the
required full-state file barrier dominates the remaining cost; removing cheap
unchanged-ancestor barriers does not by itself make the relative gate pass.

Keep the owner performance gate blocked. Further reduction needs separately
reviewed batching/coalescing or a persistence-layout change, not omission of a
required file/parent barrier. Final targeted durability/cold/idle/first-use checks,
typecheck, fresh build, and lazy graph pass; retained artifacts include per-write
records, lock samples, strace, setup accounting, and exploratory runs.

## Round 3 confirmation and recovery fixes

- **Windows Schema:** workspace effect/rollback/recovery file barriers use `r+`
  (write-capable, noncreating, nontruncating); POSIX retains `r`. No barrier is skipped.
- **Queue admission:** durable persistence is part of new/coalesced callerless
  acceptance, not merely a requested option. Healthy cursor writes cannot advance
  beyond a failed queue file barrier. Replay retries after a fresh-runtime restart.
- **Prepared namespace receipts:** ancestor ctime evidence is checked both before
  and after completion, including after an asynchronous mesh file fsync. Same-inode
  detach/reattach or benign sibling churn requires changed-ancestor reconfirmation before
  acknowledgment. Reconfirmation retries are bounded (four attempts), identity
  replacement/barrier errors still fail closed, and failure invalidates the receipt.
- **Resident pickup:** renamed-but-unconfirmed requests remain in a same-host retry
  set. Both pickup namespaces must confirm before mutation/response. Already executing
  work is never added to this retry path; startup recovery remains indeterminate.
- **Existing decision:** a losing decision publisher confirms the actual winning
  inode's file and inode-bound namespace barriers before returning its decision.
  Existence after another publisher's failed/in-flight link barrier is not a receipt.
- **Session preservation:** persistent reset and malformed-header repair fsync the
  complete source inode before rename, then confirm the archive namespace bound to
  that inode before replacement header publication or dependent backup pruning.

### Incremental barrier cost

No unconditional mesh-commit fsync was added: unchanged prepared ancestry still
costs one asynchronous file fsync plus one POSIX leaf-directory fsync, outside the
mutation lock. F3 adds barriers only for ancestors whose evidence changes
before acknowledgment (including during the barrier); those required recovery
barriers are charged separately/inclusively in workload evidence. Queue ingress
and successful pickup use their existing barriers; pickup failure retries owed
barriers. F5 adds a winning-record file and namespace confirmation on `EEXIST`.
F6 adds one source-file fsync per persistent reset/repair, not per session append.
F1 changes handle access only. No expiring presence, ordinary publication, read
signal, startup lifecycle or graph budget was promoted into a new fsync hot path.

Final-head platform/performance results and exact artifact paths belong to the
round-3 task report. This audit does not grant owner performance acceptance or the
named Astra security pass; retain the before-merge hold until both are recorded.
Tests establish barrier contracts, not physical power-cut certification.

## Earlier verification on ryzen2 (nice 19)

The mechanical inventory covers **72 direct helper/raw-publication sites in 34 source files**, including the shared helper implementation sites, with zero missing audit references. Wrapper expansions are separately listed above. Search evidence and the machine-readable coverage manifest are retained in the task artifacts.

| Check | Result |
|---|---|
| New regression checks against baseline 3f6a2963 | 23 failed, 1 harmless-fast-path check passed (9 files); no unhandled errors |
| Same new checks after patch | 24 passed (9 files) |
| Persistence/actor/mesh/residency/schema/records/storage targeted suites | 777 passed (23 files) |
| Config and config-migration suites | 73 passed (2 files) |
| Native reply session, component configuration/provider, idle bridge suites | 29 passed (4 files) |
| Real dependent-publication probe suite, after strengthening live destination fsync assertions | 14 passed (1 already-counted file) |
| Cold/idle/first-use preview startup suites | 10 passed (2 files) |
| Direct native Node `.ts` worker terminal write/read probe | PASS |
| Typecheck / build / lazy graph / diff whitespace | PASS |

No full test suite was run. Dist's public MeshStore.publish declaration includes the optional host-only `durable` flag; StoreBridgeSide requests it unconditionally before its cursor can advance. No provider/command registration or configuration key was added, and no verified kernel, law, generated artifact, ABI or bridge generator was changed.

Build structural closure: baseline **45 startup files / 1,068,941 bytes / 43 stable lazy entries / 96 chunks**; final **45 / 1,068,578 / 43 / 96**. Lazy UI graph remains host-package-free (38 files), without changing a budget. Fresh-process host-preloaded warm-jiti samples were interleaved: baseline total samples 419.7/404.6/473.7 ms, patched 377.6/370.8/376.0 ms; timing spread on a busy machine is evidence, not a performance guarantee or CI threshold.

Three interleaved write probes (median wall ms; actual fsync counts invariant across samples):

| Operation | Count | Baseline ms | Patched ms | Fsync calls before -> after |
|---|---:|---:|---:|---:|
| Ordinary shared JSON helper | 300 | 23.96 | 22.36 | 0 -> 0 |
| Durable shared JSON helper (unchanged helper implementation) | 40 | 258.26 | 243.99 | 240 -> 240 |
| Ordinary mesh publication, no archive/compaction | 100 | 24.02 | 23.55 | 0 -> 0 |
| Authoritative mesh put | 40 | 15.11 | 144.54 | 0 -> 240 |
| Ordinary authoritative actor registry replacement | 40 | 3.24 | 189.31 | 0 -> 242 |

The registry/authoritative-state cost is deliberate and needs owner acceptance. It cannot honestly be described as 'all mesh/registry writes unchanged'. Persistent callerless activation ingress now requires successful queue persistence, including coalesced replacements. A failure restores the prior in-memory queue/activation sequence and throws before a new drain or delivery-memory acknowledgment; mesh dispatch propagates that error so the monitor retains the retryable consumed-prefix boundary for **all** topics. Cursor storage can succeed without passing that failed event. This is not a whole-filesystem transaction: a failed post-rename barrier can leave an unacknowledged queue image visible, and completed-work retirement remains an at-least-once replay boundary, not an exactly-once guarantee.

