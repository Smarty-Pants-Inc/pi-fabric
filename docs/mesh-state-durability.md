# Mesh state/checkpoint durability (Slice M, #2479)

Split from #242 at round 6 (`97a791c7`), on the independently landable core
#323. This inventory covers mesh only; it does not ship Schema reconciliation
or bridge cursor changes, and does not claim to fix F12 or F14.

## Caller inventory

| Caller/state | Contract |
| --- | --- |
| `MeshStore.put/delete/writeBatch`: authoritative envelope, CAS clock, versions and tombstones | Default acknowledgment waits for a covering state barrier after releasing the mutation lock. Unchanged deletes and all-skipped batches confirm their observed generation too. |
| Host-only `durable: false` state mutations | Speculative versions, not accepted authority or durable CAS receipts; state providers do not expose this opt-out. |
| Event sequence reservations, optional read-generation/digest signals | Volatile/reconstructible hints; no unconditional fsync. |
| Ordinary live `publish` without archive | Intentionally volatile. Explicit host `publish({ durable: true })` confirms the live file and namespace before returning. |
| Compacted live event log and cursor generation | Durable log replacement and namespace barrier precede durable generation publication. Not a two-file transaction. |
| Recovered archive lines and BOOT marker | Recover live file/namespace before durable BOOT acceptance. |

## Group barrier and recovery

State writes serialize temporary bytes and atomically rename under the mesh
mutation lock, with zero fsyncs in that critical section. A process-local queue
coalesces requests and elects a cross-process barrier using the separate
`.state-durability-lock`. Busy queues use a 250 ms cadence; idle queues flush
immediately after microtask coalescing. The engine loads only at first state use
via the stable lazy entry `mesh/state-durability.js`, registered in both build
and artifact checks. No idle lifecycle import or graph-budget increase is used.

The elected barrier pins a snapshot with a private hard link, validates its
complete bounded envelope through the opened descriptor, asynchronously fsyncs
that descriptor, checks that the publication name still denotes those validated
bytes/regular-file inode (not a symlink alias), atomically replaces `state.durable.json`, and confirms its parent
namespace. Only then does it publish a volatile completion hint and resolve
covered waiters. Bounded ENOENT pin retries never acknowledge or consume a file
barrier before successful pinning. Windows uses writable, noncreating,
nontruncating `r+` handles; unsupported directory fsync remains skipped.

**F13:** a UUID/highWater header is only a source-selection hint, never proof of
snapshot completeness. Writers carry the actual parsed recovery source through
the mutation-lock handoff. A no-op recovering acknowledged checkpoint N cannot
promote torn canonical N+1 by its header. Every candidate is fully validated on
the descriptor actually synced/published, including legacy envelopes and the
last complete envelope in the concatenated recovery model. Completion reuse
also validates its opened checkpoint and binds namespace confirmation to that
inode. Malformed envelopes, policy size failures, I/O failures, insufficient
covering generations and changed candidate pins fail closed before replacement;
the previous valid checkpoint is retained. A later request retries the barrier.

Recovery uses a valid checkpoint when canonical state is missing, damaged or
lower-clock. Size-policy and transient I/O failures are not evidence of loss
and cannot authorize fallback over another writer's live successor (F11).
Subsequent allocation exceeds the acknowledged checkpoint clock. Speculative
successors may survive or be lost; their revisions are not acknowledged receipts.
Failure is not rollback: rejected mutations can remain visible, and a checkpoint
published before a failed parent barrier is not a durable acknowledgment.

Prepared `DurableDirectory` receipts bind directory/link identities and ancestor
ctimes, reconfirm changed ancestry outside the mutation lock, and invalidate on
failure. Same-inode detach/reattach during async fsync or completion-hit lock waits
must reconfirm; bounded benign sibling churn never waives identity/ctime evidence.
Hard-link ctime changes only restamp read hints bound to the exact pinned inode,
UUID, size and mtime. Earlier F3/F8/F11 regressions are retained in mesh-only suites.
Main source-retirement observation waits for asynchronous delete completion (F7);
resident timing fixtures budget grouped receipts, not mere visible state.
Capability waiters observe actual blocked-drain settlement; resident mailbox
checks observe final persisted messages. Fence fixtures budget successful
participant/claim publication before testing explicit abort or held-owner
ACK timeout; existing uncertainty/isolation assertions and R budgets remain.

## Cost and acceptance boundaries

The historical <=2 fsync budget applies only to an elected barrier with unchanged
prepared ancestry: one async file fsync plus one POSIX leaf-directory fsync.
Setup and namespace recovery/reconfirmation may owe additional fsyncs and must
be reported separately. Full candidate parsing is outside the mutation lock.
Earlier whole-PR performance samples are historical, not approval of this head.
Final-head verification evidence belongs to the retained Slice M task artifacts;
owner acceptance requires occupancy increase <=7 percentage points and maximum
lock hold <1 second on the accepted workload. Keep owner/native-platform holds
until those checks and approvals are recorded on the final merged head.

Tests model restart and loss of volatile canonical/completion bytes, including
intact N+1 headers with malformed bodies, no-op delete/batch recovery, failed
validation, concurrent candidate replacement, barriers and retry. They are not
physical power-cut certification. Snapshots are immutable, rename-only files;
external in-place modification and mixed-version hosts unaware of checkpoints
are outside this recovery contract. Earlier nonblocking P2 follow-ups remain in
smarty-dev#3342; no before-merge approval is implied here.
