# Exact mesh publication dedupe

`MeshStore.publish({ dedupeKey })` uses a durable per-key intent, not a search
window. Dedupe keys and receipts remain host-only; the public mesh provider does
not accept them. Receipt names are the SHA-256 of the key.

## Locked protocol

All steps hold the existing mesh lock:

1. Read `event-receipts/<hash>.json`. A valid receipt is authoritative; confirm
   its file/namespace barriers, finish any pending-file cleanup, and return it.
2. Repair a torn live tail and finish ordinary archive recovery. If
   `event-receipts/<hash>.pending.json` exists, read **one line at its recorded
   byte offset** and match all of `reservedSequence`, `eventId`, and `dedupeKey`.
   An existing event is confirmed, receipted durably, and returned. An absent or
   incomplete/mismatched append has not committed that intent: delete the intent
   durably and start a fresh reservation. Corrupt intents/receipts and I/O or
   durability-barrier failures fail closed.
3. Neither file means **new**, not "search history". Reserve the sequence and
   create `{ dedupeKey, reservedSequence, eventId, liveOffset }` in the pending
   file with atomic rename and file/namespace fsync **before** any event append.
4. Perform the existing archive-before-live protocol; append the event with the
   reserved sequence and id. Confirm the live file, durably write the receipt,
   and only then unlink/sync the intent.

The offset is an exact direct-live lookup; no archive index or archive scan is
needed because **every live-log compaction settles every pending intent before
rewriting**. Each existing event gets a durable receipt first; each absent append
loses only its abandoned intent. Failure to settle aborts the rewrite. Compaction
also durably writes the retained bytes and rename, so subsequent intents cannot
name offsets in a generation whose data/rename is lost on reboot. Tail repair
only removes incomplete appends; archive reboot recovery restores synced events
before intent settlement. An abandoned sequence is not reused.

Normal new-key publish work is independent of event-history size. The ordinary
fixed-size tail/sequence/archive-append metadata reads still happen. No
`MeshStore.read`, `MeshArchive.readAfter`, full live-log read, or receipt-directory
enumeration is used to decide whether a new key has already published. Actual
compaction enumerates the receipt directory to find pending files, and reboot
recovery/archive catch-up/day sealing have their existing non-constant work;
these maintenance operations are not claimed to be O(1).

## Consumer and rollout contract

All writers/compactors of a shared mesh must use this intent-aware protocol;
rollback/mixed-version old compactors cannot be allowed to rewrite a log with
new pending intents. Retain receipts indefinitely. Manual deletion/corruption
of receipts or out-of-protocol log rewrites are not supported recovery actions.
These are storage/deployment assumptions, not a lossy dedupe window.

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
  read, and receipt-directory enumeration: zero history reads/scans.
- Crash after intent/before append: restart, abandon the reservation, publish
  one event, and repeat the retry; with and without the archive.
- Crash after append/before receipt: restart, recover exactly the original id
  and sequence with no history read; with and without the archive.
- Deep-history intent: direct seek remains exact with 10,000 newer events,
  exceeding the old 4096-event bound.
- Crash followed by unrelated byte-bounded compaction: the event leaves the
  live log only after its receipt is durable; retry still returns the event,
  with and without the archive. A failed receipt barrier prevents compaction.
- Consumer replay: a different event id with the same dedupe key does not
  re-deliver after recipient reload, including a new maintenance sender; unrelated
  publication keys remain deliverable.
- The legacy no-intent residual is an explicit regression test, not silently
  treated as exact recovery.

Reproduce the lock-hold probe in isolated temporary roots:

```sh
nice -n 19 bun scripts/benchmark-mesh-dedupe.ts
```

It seeds both live and archived history with 10,000 vs 200,000 events, interleaves
25 measured new-key publishes per case, measures **owner publication to lock
release** (not acquisition wait), and checks that metadata read work is identical
and history reads/receipt-directory scans are zero. Timing is evidence, not a
flaky millisecond CI assertion.
