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
| actors/manager.ts:3338 (persist calls at 2062,2107,2174,2482,3293,3353,3464,3784,3887,3922) | Activation queue and validity revisions before mesh cursor advance / predecessor-source unlink | ordering-dependent | Only explicit security fence call (2482) | Default persistent queue writes to durable; sync empty-queue and predecessor unlink namespaces |
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
| actors/manager.ts:996 | Session -> rotation backup before new session header/pruning | ordering-dependent | No | Sync archive namespace before dependent writes |
| actors/manager.ts:1052 | Malformed session -> preserved orphan before repaired header | ordering-dependent | No | Sync archive namespace before replacement |
| residency/host.ts:706 | Request -> processing pickup before decision/mutation | ordering-dependent | No | Sync both destination and source namespaces after cross-directory rename |
| residency/protocol.ts:84-86 | Immutable committed/abandoned decision hard-link CAS before mutation/exchange deletion | ordering-dependent | No | Fsync opened temp before link; sync published namespace before returning winner |
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

## Verification on ryzen2 (nice 19)

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

The registry/authoritative-state cost is deliberate and needs owner acceptance. It cannot honestly be described as 'all mesh/registry writes unchanged'. Queue checkpoint writes retain their pre-existing best-effort failure handling except the explicitly checked launch/security fence; making all disk-error paths transactional/fail-closed is distinct from requesting and ordering successful durability barriers and remains an owner review gate.

