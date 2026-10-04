# Atomic-write A caller audit (smarty-dev#2479)

PR A splits the independent registry/queue/request/marker/config/native-session
receipts from [pi-fabric#323](https://github.com/Smarty-Pants-Inc/pi-fabric/pull/323).
Task-terminal receipts and full-answer close handoff are deferred together to PR B.
This is a scope cut, not a waiver of the round-8 full-answer conservation finding.
The earlier Schema, mesh-checkpoint and bridge slices remain deferred separately.

## Acceptance ledger

- Authoritative identity/lineage, activation queues, immutable request decisions,
  enrollment and cleanup veto markers establish their required file and namespace
  barriers before dependent acceptance or retirement.
- Restored visible bytes are not receipts: queue confirmation failure stays retryable
  without advancing an actor replay cursor or launching restored work.
- Confirm the complete native session inode and archive namespace before dependent
  replacement/pruning/admission. Journal the inode-bound obligation before rename,
  retain it across failed barriers and process replacement, and retry it at the
  boundary even without a reset trigger. This is transcript preservation, not
  task-close handoff.
- Keep reconstructible/expiring presence, sweep hints and diagnostics non-durable;
  do not change the general atomic helper default.
- Keep actual-main task close/stop/restore and terminal/reply writers unchanged.
  No new terminalPending API, terminal confirmation, pending-result recovery,
  older-recovery wait or close handoff snapshot is supplied by A.
- Verify injected failure/retry paths, targeted actor/config/residency/retention
  tests, typecheck, fresh build/artifacts and lazy graph. Run residency-commit-fence
  twice; contract tests are not physical power-cut certification.

## Included callers and ordering

| Path under `src/` | Correctness-bearing state / ordering |
|---|---|
| `actors/registry-store.ts` | Every changed whole-registry image durably preserves definitions and lineage claims, even for soft status/history or setter calls; only a confirmed unchanged inode skips soft saves; failed barriers retain confirmation debt and durable rollback restores authoritative definitions |
| `actors/global-registry.ts` | Durable user template library |
| `actors/binding-store.ts` | Durable session model/thinking overrides |
| `actors/manager.ts` | Durable new/coalesced callerless admission before acceptance; restored queue confirmation before launch/cursor progress; predecessor unlink barriers; complete source session inode and inode-bound archive namespace before replacement/pruning; prelaunch failure settlement/parking; live adoption claim/copy retries; explicit and directive actor stop-publication retry; durable ordinary-completion backlog replacement and storage-only retry gate |
| `actors/mesh-monitor.ts` | Durable accepted replay checkpoint with existing batching |
| `config.ts` | Published config namespace includes new ancestors and symlink parents; POSIX barrier errors fail closed |
| `records/client.ts` | Durable same-nonce republishing before every register attempt; durable issued credential before open returns; reconfirm known-token enrollment on every successful open |
| `residency/client.ts` | Durable metadata, completion-consumption state and resident request/receipt replacements; reconfirm visible consumption before acknowledgement or notification retirement |
| `residency/actor-client.ts` | Durable command before host execution |
| `residency/host.ts` | Confirm both pickup namespaces before execution; retry only this host's never-executed pickups; durable response before processing retirement; live completed-response publication/retirement retry without mutation replay; owner/error/removal hints stay non-durable |
| `residency/protocol.ts` | File barrier before immutable decision link; namespace barrier before winner acknowledgement; EEXIST confirms actual winning inode and bound namespace |
| `storage/retention.ts` | Durable unresolved-worker veto plus shared fail-closed Herdr collection veto even when marker publication failed; sweep/heartbeat hints stay fast |

Main's existing `core/atomic-write.ts` supplies `writeJsonAtomic`, `writeFileAtomic`
and inode-bound `syncPathNamespace`; no shared helper/default change is required.
Resident saved-result persistence remains independent of ordinary task-close
handoff: main's existing `ResidentHost.onSettled` result save and failed-save
retention remain intact.

## Retained prior-review fixes

A retains F2 queue admission/restoration, F4 same-host pickup retry, F5 winning
immutable decision-inode confirmation, F6 native session preservation, F9 same-nonce
publication, F10 prelaunch settlement/parking, F17 shared Herdr veto, F21 live
adoption claim/copy retry and F22 idle actor stop-publication retry. The latter two
are required by ordinary registry durability, not task-terminal recovery.

## Deferred scope

- **PR B (task-terminal family):** manager/worker terminal-status fsync writers,
  native reply durability, terminal confirmation/publication retry, pending-result
  restoration and full-answer close/session handoff move together. B must preserve
  exact text, structured value, usage and identity before compaction, retirement or
  collection; a compact UI cache is never an authoritative answer. Include the
  round-8 transient settled-status read failure during session-only replacement.
  A does not claim F15/F16/F18/F19/F20 or terminal/reply durability fixed.
- **Schema slice:** controller/journals/effect barriers and `commit_unconfirmed`
  recovery, including F12 cross-process construction-time recovery.
- **Mesh slice:** checkpoint/state-durability engine and grouped-barrier fixtures,
  including F13 complete pinned-snapshot validation and F7 test observation.
- **Bridge slice (formerly also called B):** durable forwarding/publication/cursors,
  including F14 destination confirmation before restart deduplication.

Schema, mesh, bridge, shared atomic helper, build/provider registrations and
verified kernels remain actual-main. No new configuration keys are introduced.
The earlier P2 follow-ups remain tracked in smarty-dev#3342. PR A round 1 also
repairs Astra F2/security S1 ordinary-completion queue durability, S2/F25 known-token
confirmation, S3/F23 directive-stop publication, S4 completion-consumption
confirmation and S5 live storage-only resident response retry. These are queue,
enrollment and resident request/consumption receipts, not task-terminal machinery.
`atomic-review-r1.test.ts`, the completion/consumption process fixtures and the
resident-host/client regressions cover failed barriers, healthy retries and normal
process replacement without new ingress. They do not certify physical power loss.

## Round 2 archive-receipt recovery

F6 retains a live inode-bound archive obligation and durably journals it before
rename. Failed confirmation propagates to the drain retry gate instead of allowing
boundary continuation. Idle reset retries and prelaunch preparation repay the same
obligation before absent-source handling, replacement or pruning. A successor
reads the journal and confirms the original archive inode, not merely visible
bytes; a pre-rename failure finishes preservation from the bound source. Journal
retirement also confirms its unlink before dependent pruning, with live debt
retained if that barrier fails.

`actor-session-reset.test.ts` covers size/requested boundary continuation, immediate
idle reset retry, pre-rename intent failure, archive inode substitution and failed
journal retirement. `atomic-archive-receipt.test.ts` and its unclean-exit fixture
cover process replacement with accepted backlog and unavailable confirmation,
then storage-only recovery without new ingress or graceful close.

## Round 3 reset finalization and caller identity

- **F26:** a Claude reset journals its trigger and source byte count alongside
  archive inode identity. Preservation confirmation does not retire that native
  selector obligation. Boundary/idle retry clears `runnerSessionId`, durably
  publishes the registry, then retires the journal and prunes. A replacement host
  finishes the same reset without another size trigger. Pi header repair remains
  preservation-only and retains its existing retirement ordering.
- **S5:** the initial post-readiness request poll uses the same owned retry boundary
  as timer polls. A failed response-file barrier retains the live host, its timer
  and original completed response, without replaying the mutation.
- **F27:** configuration confirmation binds the temporary file's inode to both the
  actual publication path and the caller's original namespace. The publication
  directory receives its barrier even if a configuration symlink was retargeted;
  a namespace that identifies a different inode is rejected.
- **Windows fixture:** archive failure intercepts the post-rename `lstatSync` walk,
  not an unused `statSync` call. The fixture asserts the fault fired and its journal
  remains. The successor test verifies blocked admission before storage recovery;
  both native-platform and injected-win32 branches run without skipping Windows.

Regressions in `actor-session-reset.test.ts`, `atomic-archive-receipt.test.ts`,
`residency-host.test.ts` and `config-migrations.test.ts` cover live Claude recovery,
failed registry publication, unclean process replacement, pre-start queued
requests and three-directory configuration retargeting. Claude uses the actual
source worker and fake native CLI; recovered launches must omit `--resume`.
These are A's reset/config/resident-command receipts, not PR B's task terminals.

## Security round 3 response and coalescing recovery

- **S7/F28:** replacement startup retains processing custody and validates any
  existing command-response envelope. The owned request poll durably republishes
  the exact saved response before processing retirement. Read or confirmation
  failures retain storage-only debt; no interrupted mutation is replayed. Only
  absent or invalid responses receive an indeterminate outcome.
- **S8:** failed callerless coalescing restores the entire previous item snapshot,
  including absence of optional provenance and images. Older UNKNOWN input cannot
  acquire the principal of a rejected replacement.

`residency-host.test.ts` and `resident-response-retirement-crash.mjs` exercise a
real producer exit after durable success and failed retirement, with the original
caller held until replacement settlement. Unavailable reconfirmation and receipt
reads remain retryable; missing/invalid envelopes and saved refusals are controls.
`actor-manager.test.ts` covers UNKNOWN and known-principal rollback, ordinary
completion before rejected-event replay, persisted replacement, launch attribution
and output attribution. These remain A command/queue receipts, not B task terminals.

## Limits and before-merge holds

Filesystems/devices must honor fsync. Windows skips unsupported directory barriers
while retaining file barriers; native platform CI and power-loss guarantees are
not established by Linux tests. Without durable Herdr exit proof the shared
collector conservatively retains Herdr run trees, potentially indefinitely.
This strengthens collection safety without changing task-close control flow.

BEFORE-MERGE: re-run owner performance acceptance on the final A head; historical
full-PR mesh workload numbers and the prior close-only carry-forward are not A
caller-cost acceptance. BEFORE-MERGE: successful required final-head Ubuntu and
native Windows CI. BEFORE-MERGE: named Astra security pass for nonce/token
publication in `records/client.ts`; synthetic local-server tests do not grant it.
