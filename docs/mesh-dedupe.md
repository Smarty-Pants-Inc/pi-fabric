# Exact mesh publication dedupe

`MeshStore.publish({ dedupeKey })` uses a durable per-key intent, not a search
window. Dedupe keys and receipts remain host-only. Trusted extension components
may now supply keys through the mesh provider; model-authored programs cannot.
Receipt names are the SHA-256 of the final, host-namespaced key.

## Provider publication: trusted host versus public guest

Trusted Pi extension components call `context.call("mesh.publish", args)` or
`context.call("mesh.publishBatch", { events })` after declaring those exact refs
in `requires`. Fabric captures their component instance ID in a private host
invocation token. A component-looking tool-call ID, event data, principal, or
caller-supplied trust field cannot grant this access.

- Host `mesh.publish` accepts an optional `dedupeKey`.
- Host `mesh.publishBatch` accepts `events` (1..256), each with the same publication
  fields (`topic`, optional `kind`, `to`, `text`, `data`, and `dedupeKey`). Both
  schemas retain `additionalProperties: false`; host store controls such as
  `from`, `principal`, `durable`, `admit`, and `fence` are not accepted.
- Each supplied key must be a **string of 1..512 UTF-8 bytes**. Null, empty,
  non-string, and overlong keys fail before any event in that request is appended.
  The provider prefixes it with the stable component ID, length-framed as
  `component:<UTF-8 ID byte length>:<component ID>:<raw key>`. This prevents
  collisions between component instances, including delimiter-containing IDs.
  The raw-key limit excludes the host prefix. The stored/returned event contains
  the namespaced key; component reloads and process restarts retain that namespace.
- The public catalog, QuickJS proxy, and guest declarations are unchanged:
  `mesh.publish` has no `dedupeKey` property and `publishBatch` is not exposed.
  A generic untrusted call supplying `dedupeKey` (even null) or requesting the
  host-only batch action fails with `MeshHostPublishError`, code
  `FABRIC_MESH_HOST_PUBLISH_REQUIRED`, `retryable: false`. The fixed error metadata
  survives the QuickJS boundary. A `data.dedupeKey` is ordinary payload, not a
  publication receipt key.

```ts host
// In a trusted extension component declaring both mesh publication requirements:
const events = [
  { topic: "github.delivery", kind: "delivery", dedupeKey: "delivery-1", data: first },
  { topic: "github.delivery", kind: "delivery", dedupeKey: "delivery-2", data: second },
];
const committed = await context.call("mesh.publishBatch", { events });
// If this reply is lost, retry the same keyed request, not fresh keys.
```

Each batch event uses the existing durable intent/receipt protocol independently,
on both file and SQLite mesh configurations (event receipts remain in the durable
file event log; SQLite selects the keyed-state backend). Batches commit a bounded
**prefix**, not an all-or-nothing transaction: the existing 50 ms/retained-tail work
bounds can return fewer events than requested. Results preserve input order and
include the **original events, IDs, and sequences** for already-committed keys;
only new keys append. Checkpoint the returned prefix and continue the remaining
suffix. After an uncertain reply, retrying the entire keyed batch safely recovers
its committed prefix and admits the suffix, subject to those same bounds. Returned
originals need not be at the current log head. No provider-side automatic batch
retry is performed. Unkeyed events retain ordinary at-least-once retry behavior.

One key means one publication across topics within that component: reusing it with
a changed payload/topic returns the first event, not an update or a new append.
Choose a stable key covering the source store ID, route, delivery ID, and topic;
key reuse for unrelated deliveries suppresses them. This is publication dedupe,
not exactly-once arbitrary consumer or external side effects.

## Locked protocol

Admission, append, recovery, and compaction hold the existing mesh lock. Without
an archive, the live-file barrier runs after unlock; receipt installation then
reacquires the lock and compare-and-sets the intent identity (`dedupeKey`,
`eventId`, `reservedSequence`). An already settled, evicted, or replaced intent
makes that finalizer a no-op. Single and batch publication await this finalizer.
Receipt durability and pending-file cleanup remain inside that CAS lock hold,
so a delayed finalizer cannot resurrect an evicted receipt outside the cap.
An existing/late receipt is first confirmed off-lock. If its matching intent still
exists, cleanup rechecks both receipt and intent identities under the CAS lock,
confirms the still-matching receipt file, closes it, and only then removes the
intent. A missing/replaced receipt or a failed barrier retains the intent; cleanup
never rewrites or recreates a receipt. The receipt-file barrier is portable even
on Windows, where directory fsync is unsupported. Windows unlink namespace
persistence remains subject to that existing platform limitation: if an intent
reappears after a crash, the authoritative receipt settles it without re-appending.
Unkeyed publication does not enter this receipt/intent path.

1. Read `event-receipts/<hash>.json`. A valid receipt is authoritative; confirm
   its file/namespace barriers, finish any pending-file cleanup, and return it.
2. If `event-receipts/<hash>.pending.json` exists, repair the live tail and finish
   the existing archive BOOT check and full reboot promotion **before settling
   the intent**, under the same lock. This restores all earlier synced archived
   events in sequence order, not just the retried key. In the same boot, this
   recovery pass defers pending cutback so the intent's archive evidence survives.
   Then read **one line at its recorded live byte offset** and match
   `reservedSequence`, `eventId`, and `dedupeKey`. If the line no longer matches (including after an old compactor
   rewrites the live tail), directly read
   `sequence-index/<floor(sequence/1024)>/<sequence>.json` in the archive, then
   seek exactly its segment/file offset and read the one recorded line.
   A matching archived event counts as committed, regardless of `committed: false`:
   if the live log has not reached its sequence, first append its exact bytes to live
   (durably update the intent offset first). Otherwise do not append behind the live
   sequence. Confirm its sidecar, finish any matching archive pending commit without
   sealing scans, write the durable receipt, and return the original event. A durable
   `{ sequence, absent: true }`, a positive exact-id abort, a different event at that
   sequence/address, or the day file ending at/before the indexed offset proves
   absence. Only then may a retry abandon the intent and publish a fresh sequence.
   A missing/corrupt/unreadable sidecar or missing/torn/corrupt segment is
   **unavailable**, not absent: retain the intent and return retryable
   `MeshDedupeRecoveryError`, never publish.
   The captured `archiveDir` also fences removal/change of archive configuration.
   Same-boot pending cleanup remains deferred until settlement, so it cannot
   cut back the exact archive-only reservation being retried.
3. Neither file means **new**, not "search history". Repair the live tail and
   finish ordinary archive recovery, check the hard receipt/intent capacity (evict
   non-protected receipts if necessary), then reserve a sequence and install a durable
   negative sequence sidecar in the archive. Create
   `{ dedupeKey, reservedSequence, eventId, liveOffset, archiveDir? }` atomically
   with file/namespace fsync **before** either event append.
4. The archive append syncs the event line, then durably replaces the sidecar
   with `{ sequence, id, file, offset, length, committed: false }` for keyed events
   in that same append operation (the confirmation is advisory),
   before the event can go live. A rollback installs the explicit negative record.
   Append live, commit archive metadata, confirm the live file, durably write
   the receipt, and only then unlink/sync the intent.

Archive lookup never scans a segment, index log, receipt directory, or day tree.
It is one sequence-addressed sidecar read and one positioned event-line read.
The archive is day/topic segmented, so the direct index preserves exact offsets
without backfilling or scanning historical segments. Old writers ignore the
sidecars but do not rewrite committed archive lines. An old pending cutback can
leave an obsolete sidecar at EOF or at a replacement event; direct lookup handles
those positive absence proofs. New compaction still settles
all pending intents before pruning receipts or rewriting and durably writes retained bytes/rename.
Tail repair only removes incomplete appends. An abandoned sequence is not reused.

Normal new-key publish work is independent of event-history size. The ordinary
fixed-size tail/sequence/archive-append metadata reads still happen. No
`MeshStore.read`, `MeshArchive.readAfter`, or full live-log read is used to decide
whether a new key has already published. New-key admission now enumerates the
receipt/intent directory once under the lock to enforce hard capacity across
writers; its work depends on the bounded dedupe population, not event history.
Below capacity it reads no receipt payloads. At capacity it reads/sorts only
non-protected settled receipts for eviction. Actual compaction enumerates the
directory to settle pending files; reboot recovery/archive catch-up/day sealing
retain their existing non-constant work. These operations are not claimed O(1).

## Recovery decision table (round-five F1)

Readers within the newest **actual live sequence** treat archived bytes as published
unless the day file has a **positive `ABORTED.json` mark for that exact event id**.
Neither `committed: false`, a negative sidecar, nor stale `PENDING.json` alone hides
archived bytes. Events beyond the live horizon remain invisible until promoted.

| Direct evidence on a key retry | Decision | Live-tail reader | Archive `nextEventAfter` reader |
| --- | --- | --- | --- |
| Valid receipt | Return it; finish intent cleanup | No append/replay | Original archived identity |
| Exact live event at saved offset | Confirm live/archive, receipt same id/sequence | Already-live event once; no retry append | Original event, even with false marker or stale PENDING |
| Exact archived id/sequence, no positive abort; live end below sequence | Append original bytes first; confirm and receipt | Original event now goes live once | Original event once within updated live horizon |
| Exact archived id/sequence, no positive abort; live has reached/passed sequence | Confirm and receipt original; **never abort or republish** | No retry append. A reader present before old compaction saw the original; a later live-only reader cannot recover compacted bytes | Original event once for a cursor not already beyond its sequence |
| Same as above, but event never went live and was overtaken | Same conservative decision: ambiguous with old compaction; prefer one archive delivery over loss | No original event and no replay/fresh id; live-only reader sees the later event(s) | Original archived event once for a cursor not already beyond it (including compacted `read({ after })` fallback) |
| No exact live match **and** positive archive absence: negative sidecar, positive abort, different exact event, or day EOF at/before indexed offset after old cutback | Abandon absent reservation; publish/receipt one fresh sequence. Never abort another archived identity | Fresh event once; original never appended on this evidence | Fresh event; any positively aborted original remains hidden |
| No exact live match and unavailable archive/config evidence | Retryable error; retain intent; no publication | No new event | No manufactured abort |

A cursor already beyond the ambiguous old sequence cannot be rewound safely;
this does not claim delivery of compacted or never-live bytes to a live-only tail.
The deliberate preference is duplicate-free archive delivery over destructive loss,
not replaying an old sequence at the tail. False markers are advisory confirmations
left by writers that old recovery cannot update, not proof of non-publication.

## Bounded receipt retention and expiry

Host-owned `MeshStoreOptions` (also accepted by `EventLogOptions`) configure:

- `dedupeReceiptTtlMs`: **7 days** by default (`604800000` ms), covering the
  forwarder restart-duplicate window of smarty-dev#7892. Age is measured from the
  receipt event's persisted `createdAt`, not file mtime or recovery time.
- `maxDedupeReceipts`: **100,000** by default, a **hard capacity on distinct
  receipt plus pending-intent keys**, not just settled receipts. A key with both
  files consumes one slot, and a same-key retry consumes no additional slot.

Both options must be positive safe integers. Before a **new dedupe-keyed** event
reserves a sequence, creates an intent, or appends live/archive bytes, admission
checks capacity **under the mesh lock**. If a new slot would exceed the cap,
remove expired non-protected receipts first, then the oldest remaining settled
receipts (including younger-than-TTL receipts) until there is room. Equal
publication times are ordered by sequence, then receipt filename. Removals are
namespace-synced before admitting the new event. Pending intents and any receipt
with the same key are protected and never evicted to make room.

If protected pending intents fill the capacity and eviction cannot make room,
reject the new keyed publication with exported `MeshDedupeStoreFullError`, code
`FABRIC_MESH_DEDUPE_STORE_FULL`, `retryable: true`, without a new intent, sequence
reservation, or event append. Resolve/retry the existing intents (or repair
unavailable durability/archive evidence) before retrying new keys. A failed
byte-triggered compaction cannot admit additional keyed appends past capacity.
Unkeyed publication has no dedupe-capacity admission check and retains its
existing compaction/error behavior. Batches use the same per-event admission
check and retain their bounded-prefix contract. Cooperating writers must share
these host-owned limits; older writers are not retroactively constrained. Any
inherited over-cap protected population is preserved, but admits no new keys.

The **existing byte-triggered compaction pass** still first settles all pending
intents, then expires receipts at or beyond their TTL and prunes count pressure.
Failed settlement blocks compaction/pruning, not protection. After settlement,
those receipts are eligible for eviction and new keyed publications can resume.
No maintenance timer or event-history lookup is added.

**Expiry is maintenance-driven, not a wall-clock timer:** below capacity, a
receipt remains authoritative until compaction removes it. At admission pressure,
expiry/count eviction may remove it before byte-triggered compaction. Once
removed, that key is **NEW** on its next publication: it gets a new event ID and
sequence, even if the original event still exists in the live log or archive.
There is no historical search or stale-finalizer receipt resurrection. Choose
limits large enough for the producer's uncertain-reply/restart retry window;
count pressure can shorten it below seven days. Durable intent recovery remains
protected while unresolved, without allowing fresh keyed events past the cap.

## Consumer and rollout contract

On an archived mesh, old writers/compactors may continue to rewrite the live log:
new pending intents remain recoverable through their exact sequence sidecars.
The archive must remain available and its append-only segment offsets and
sequence sidecars must be retained. Missing/corrupt sidecars never authorize a
second publish; they fail closed until repaired. A mesh without an archive
cannot recover bytes discarded by an old compactor and still requires
intent-aware compaction. Receipts have bounded retention as described above. Manual
deletion or rollback of receipts/archive evidence is not a supported recovery action.

The #460 callers (`src/topology/stall-alarms.ts`) publish only:

- `ops.owner` / `root.presence.alarm`: a presence notification;
- `ops.owner` / `inbox.age.alarm`: an age notification;
- `fleet.work.inbox-receipts` / `rerouted` or `undeliverable`: a disposition
  notification about a message already moved under durable inbox custody.

**None is an executable control command.** The reroute executes through
`MainInboxMaintenance`'s permanent per-message route claim and
`MainAgentController.receiveInboxItem`, not by consuming the disposition event.
The control plane executes only `fabric.control.command`; no current publisher
there supplies `dedupeKey`. RC2 (`e7bc3f24`) had no dedupe-keyed publications.

As defense in depth, RootInbox includes the mesh-wide host-only `dedupeKey` in its
existing confirmed session/inbox receipts. A replay with a different event id
but the same key does not cause another delivery/model execution once the
recipient has confirmed delivery. Queued/unconfirmed delivery is still
recoverable at least once; this does not claim exactly-once arbitrary external
side effects. Recipient-scoped receipt caching keeps its existing bounds and
horizon; canonical session receipts are checked too. Bridged disposition
notifications also already carry their original `data.messageId` identity.

## Only migration residual

The pre-intent `81c0f9f0` implementation, deployed from **2026-10-04 22:30Z until
this fix** (the approximately one-hour incident window), could append a
#460-era dedupe-keyed event and crash before its receipt write **inside that one
lock hold**. Such an event has neither receipt nor intent. A retry cannot be
distinguished from a new key without a historical migration/index, so one
duplicate notification is possible for that legacy case. This fix intentionally
does not search any live tail or the 687 MB archive to guess. Successfully
receipted legacy publishes are still exact; RC2 events have no dedupe keys.
New-protocol crashes are covered by intents regardless of event age or
compaction. This residual does not replay a control command or reroute the
original inbox carrier again.

## Acceptance and evidence

- New key: spy on `MeshArchive.readAfter`, `MeshStore.read`, the full live-log
  read: zero history reads/scans. New-key admission performs one bounded
  receipt/intent directory enumeration for capacity, not historical dedupe lookup.
- Crash after intent/before append: restart, abandon the reservation, publish
  one event, and repeat the retry; with and without the archive.
- Crash after append/before receipt: restart, recover exactly the original id
  and sequence with no history read; with and without the archive.
- Deep-history intent: direct seek remains exact with 10,000 newer events,
  exceeding the old 4096-event bound.
- Crash followed by unrelated byte-bounded compaction: the event leaves the
  live log only after its receipt is durable; retry still returns the event,
  with and without the archive. A failed receipt barrier prevents compaction.
- Actual RC3 (`81c0f9f0`) mixed-version regression: build the old commit in a
  scratch worktree, seed event 1, and SIGKILL the new keyed publisher at sequence 2
  **after live append/before archive commit**, leaving `committed: false`. The old
  writer's real recovery/publish of event 3 must leave that marker untouched, while
  a new normal cursor returns 2 then 3. Let that old writer compact event 2 away:
  retries return exactly event 2, with one archived publication, no abort/new
  sequence/live replay/history scan. The same probe preserves the earlier
  after-commit/before-receipt regression and adds actual old-writer pre-live
  cutback tests (same-topic replacement and other-topic exact EOF).
  Run `nice -n 19 node scripts/verify-mixed-version-dedupe.mjs`
  after building this head.
- Direct archive recovery: one exact sidecar plus one positioned event-line read,
  zero archive directory enumeration/history scans. Missing/corrupt/unreadable
  sidecar or missing/torn/corrupt segment preserves the intent and returns a
  retryable `MeshDedupeRecoveryError`; explicit negative reservation, positive
  abort, different exact event, or EOF at/before the indexed address permits
  publication only after the live anchor does not match.
- Reboot with live `[1]` and synced archive `[1, 2, 3]`, where ordinary event 2
  was acknowledged and keyed event 3 retains its intent/positive sidecar: retry
  key 3 as the first operation. `read({ after: 1 })` and the real paged RootInbox
  cursor both deliver 2 then 3; repeated retries do not append 3 again. Repeat
  with an unrelated ordinary publication first.
- Consumer replay: a different event id with the same dedupe key does not
  re-deliver after recipient reload, including a new maintenance sender; unrelated
  publication keys remain deliverable.
- Fill hard capacity with unresolved intents, force byte-triggered settlement
  failure, and reject repeated distinct keyed publications with the typed full
  error and identical live bytes/sequence. Same-key retry recovers the original;
  settlement plus compaction lets new keyed publication resume, with/without archive.
- An intent and receipt for one protected key count once; admission evicts only
  non-protected settled receipts. Unkeyed publication still bypasses capacity.
- Delay a no-archive finalizer at its live barrier; another store compacts,
  settles, and expires its receipt. Releasing the finalizer does not resurrect it
  or overwrite/remove a new intent that has since reused the same key.
- The legacy no-intent residual is an explicit regression test, not silently
  treated as exact recovery.

Reproduce the lock-hold probe in isolated temporary roots:

```sh
nice -n 19 bun scripts/benchmark-mesh-dedupe.ts
```

It seeds both live and archived history with 10,000 vs 200,000 events, interleaves
25 measured new-key publishes per case, measures **owner publication to lock
release** (not acquisition wait), and checks that metadata read work is identical
and history reads are zero, with one bounded capacity enumeration per new key.
Timing is evidence, not a flaky millisecond CI assertion.
