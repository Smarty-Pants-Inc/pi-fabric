# Atomic-write core caller audit (smarty-dev#2479)

Split from pi-fabric#242 at round 6, source head `97a791c7`. This document describes
only the independent core receipts extracted onto current main. It is not evidence
that the deferred Schema, mesh checkpoint, or bridge defects have been fixed.

## Acceptance ledger

- Authoritative identity/lineage, activation queues, immutable request decisions,
  enrollment, terminal results and replies must establish their required file and
  namespace barriers before dependent acceptance or retirement.
- Restored visible bytes are not receipts: confirmation failure must remain
  retryable, without advancing an actor replay cursor or launching restored work.
- Keep reconstructible/expiring presence, sweep hints, progress and diagnostics
  non-durable. Do not change the general atomic helper default.
- Cover caller classes and injected failure/retry paths; typecheck, fresh build,
  artifact and lazy-graph checks, and changed core tests twice.
- Retain performance, platform and named security before-merge holds. Barrier
  contract tests are not physical power-cut certification.

## Included callers and ordering

| Path under `src/` | Correctness-bearing state / ordering |
|---|---|
| `actors/registry-store.ts` | Durable definitions and lineage claims before queue takeover; durable rollback also restores authoritative definitions |
| `actors/global-registry.ts` | Durable user template library |
| `actors/binding-store.ts` | Durable session model/thinking overrides |
| `actors/manager.ts` | Durable new/coalesced callerless admission before delivery acceptance; confirmation of restored queues before launch/cursor progress; predecessor unlink namespace barriers; durable source session inode and inode-bound archive namespace before header replacement/pruning; settlement/parking after prelaunch registry failures |
| `actors/mesh-monitor.ts` | Durable replay checkpoint after accepted queues, retaining existing checkpoint batching and replay behavior |
| `agents/manager.ts` | Durable host-produced terminal run record before settlement |
| `config.ts` | Published config namespace includes new ancestors and symlink parents; POSIX barrier errors fail closed |
| `records/client.ts` | Durable same-nonce republishing before each register attempt; durable issued credential before open returns |
| `residency/client.ts` | Durable metadata, completion-consumption state and resident request/receipt replacements |
| `residency/actor-client.ts` | Durable command before host execution |
| `residency/host.ts` | Confirm both pickup namespaces before execution; retry only this host's renamed-but-unconfirmed pickups; durable response before processing retirement; owner/error/removal hints remain non-durable |
| `residency/protocol.ts` | File barrier before immutable decision link, namespace barrier before acknowledging winner; on EEXIST confirm the actual winning inode and its bound namespace |
| `storage/retention.ts` | Durable unresolved-worker cleanup veto; sweep/heartbeat hints stay fast |
| `worker/run-record.ts` | Durable completed/failed/stopped/timed-out record; progress remains fast |
| `worker/reply-tool.ts` | Durable structured reply before dependent terminal status/completion |

The existing main `core/atomic-write.ts` supplies `writeJsonAtomic`,
`writeFileAtomic` and inode-bound `syncPathNamespace`; no new shared helper is
needed by these callers. Native worker modules keep their existing self-contained
source-loading boundary, syncing lexical and real parent ancestry.

## Retained prior-review fixes

The core carries F2 admission/restoration confirmation, F4 same-host pickup retry,
F5 confirmation of an existing immutable decision inode, F6 session data before
archive/replacement, F9 same-nonce register retry publication, and F10 prelaunch
settlement/parking. F7's grouped-mesh retirement-completion test observation moves
with M. It does not carry F1 Windows Schema handles,
F3 mesh completion namespace reconfirmation, F8 legacy mesh-envelope parsing or
F11 live-state read fallback refusal; those belong to the deferred slices.

## Deferred scope

- **S:** Schema controller/journals/effect barriers and `commit_unconfirmed`
  reconciliation, plus its tests/docs. Round-six **F12** requires construction-time
  recovery under the same cross-process commit lock and a paused two-process
  applying-journal regression.
- **M:** Mesh store/state-durability checkpoint engine, `DurableDirectory`, stable
  lazy build registrations, mesh regressions and grouped-barrier timing fixtures.
  Round-six **F13** requires validating the complete pinned snapshot before it
  can replace the last valid durability checkpoint.
- **B:** Durable bridge forwarding/publication and cursor tests. Round-six **F14**
  requires recovered destination append durability confirmation before restart
  deduplication can authorize durable source cursor advancement.

Core leaves `src/schema/controller.ts`, `src/mesh/store.ts`, `src/mesh/bridge.ts`,
`src/core/atomic-write.ts`, build registrations and Schema documentation unchanged
from its main base; it adds no `src/mesh/state-durability.ts`. Existing Schema/mesh/
bridge call paths therefore retain main behavior. Their durability guarantees are
not supplied by this split. Mixed regression files retain only core cases.

## Limits and before-merge holds

The filesystem/device must honor file and directory fsync. Windows skips
unsupported directory barriers while retaining file barriers; actual platform
power-loss guarantees require certification. Native worker lexical/real ancestry
handling is not a new arbitrary multi-hop symlink protocol. The earlier P2
follow-ups remain tracked in smarty-dev#3342; this split does not claim their fix.

BEFORE-MERGE: record owner acceptance that the final merged core head meets the
recorded performance limits; the earlier outside-lock cost acceptance is not
numeric final-head acceptance. Do not reuse the full PR's historical mesh workload
numbers as core measurements.

BEFORE-MERGE: successful required Ubuntu and native Windows checks on the final
head. The round-six full-PR Windows `ActorRegistryOwnershipError` is not a green
required check and still requires classification; Linux-local tests cannot waive it.

BEFORE-MERGE: obtain the named Astra security pass for enrollment nonce/token
publication in `records/client.ts`. Functional fake-server tests do not grant
security approval.
