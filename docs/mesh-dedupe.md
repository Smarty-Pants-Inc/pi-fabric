# Exact mesh publication dedupe

`MeshStore.publish({ dedupeKey })` uses a durable per-key intent, not a search
window. Dedupe keys and receipts remain host-only; the public mesh provider does
not accept them. Receipt names are the SHA-256 of the key.

## Locked protocol

All steps hold the existing mesh lock:

1. Read `event-receipts/<hash>.json`. A valid receipt is authoritative; confirm
   its file/namespace barriers, finish any pending-file cleanup, and return it.
2. If `event-receipts/<hash>.pending.json` exists, read **one line at its
   recorded live byte offset** and match `reservedSequence`, `eventId`, and
   `dedupeKey`. If the line no longer matches (including after an old compactor
   rewrites the live tail), directly read
   `sequence-index/<floor(sequence/1024)>/<sequence>.json` in the archive, then
   seek exactly its segment/file offset and read the one recorded line.
   A matching archived event counts as committed: finish any archive pending
   commit, write the durable receipt, and return the original event. Only an
   explicit durable `{ sequence, absent: true }` proves non-append. A missing,
   corrupt, unreadable, or mismatched index/segment is **unavailable**, not absent:
   retain the intent and return retryable `MeshDedupeRecoveryError`, never publish.
   The captured `archiveDir` also fences removal/change of archive configuration.
   Settle the intent before ordinary archive recovery can cut back its evidence.
3. Neither file means **new**, not "search history". Repair the live tail and
   finish ordinary archive recovery, reserve a sequence, and install a durable
   negative sequence sidecar in the archive. Create
   `{ dedupeKey, reservedSequence, eventId, liveOffset, archiveDir? }` atomically
   with file/namespace fsync **before** either event append.
4. The archive append syncs the event line, then durably replaces the sidecar
   with `{ sequence, id, file, offset, length }` in that same append operation,
   before the event can go live. A rollback installs the explicit negative record.
   Append live, commit archive metadata, confirm the live file, durably write
   the receipt, and only then unlink/sync the intent.

Archive lookup never scans a segment, index log, receipt directory, or day tree.
It is one sequence-addressed sidecar read and one positioned event-line read.
The archive is day/topic segmented, so the direct index preserves exact offsets
without backfilling or scanning historical segments. Old writers ignore the
sidecars but do not rewrite committed archive lines. New compaction still settles
all pending intents before rewriting and durably writes retained bytes/rename.
Tail repair only removes incomplete appends. An abandoned sequence is not reused.

Normal new-key publish work is independent of event-history size. The ordinary
fixed-size tail/sequence/archive-append metadata reads still happen. No
`MeshStore.read`, `MeshArchive.readAfter`, full live-log read, or receipt-directory
enumeration is used to decide whether a new key has already published. Actual
compaction enumerates the receipt directory to find pending files, and reboot
recovery/archive catch-up/day sealing have their existing non-constant work;
these maintenance operations are not claimed to be O(1).

## Consumer and rollout contract

On an archived mesh, old writers/compactors may continue to rewrite the live log:
new pending intents remain recoverable through their exact sequence sidecars.
The archive must remain available and its append-only segment offsets and
sequence sidecars must be retained. Missing/corrupt sidecars never authorize a
second publish; they fail closed until repaired. A mesh without an archive
cannot recover bytes discarded by an old compactor and still requires
intent-aware compaction. Retain receipts indefinitely. Manual deletion or
rollback of receipts/archive evidence is not a supported recovery action.

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
- Actual RC3 (`81c0f9f0`) mixed-version regression: build the old commit in a
  scratch worktree, SIGKILL the new publisher after append/before receipt, and
  let the old store compact until the original event leaves the live tail while
  its intent remains. Retry must return the exact original event with one
  archived publication. Run `nice -n 19 node scripts/verify-mixed-version-dedupe.mjs`
  after building this head.
- Direct archive recovery: one exact sidecar plus one positioned event-line read,
  zero archive directory enumeration/history scans. Missing/corrupt/unreadable
  sidecar or missing/short segment preserves the intent and returns a retryable
  `MeshDedupeRecoveryError`; a durable negative reservation permits publication.
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
