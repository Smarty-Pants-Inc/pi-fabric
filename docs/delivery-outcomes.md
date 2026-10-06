# Delivery outcome log (smarty-dev#5518, gate P)

Each mesh root keeps `delivery-outcomes/YYYYMMDD.jsonl`, partitioned by UTC date.
A line is an observation about a steer, followUp, or addressed publish:

```json
{"eventId":"send-id","to":"session:receiver","from":"session:sender","mode":"followUp","outcome":"delivered","reason":"consumed by Main as itself","at":1791324000000}
```

`at` is Unix time in milliseconds. `mode` is `steer`, `followUp`, or `publish`.
The outcomes mean:

- **delivered:** the receiver consumed this message **as itself**, not just a
  later replacement that carried its id. Main uses its confirmed native session
  receipt; the root inbox uses a confirmed batch receipt; actors require actual
  inference evidence. Admission, worker launch, and a queue ACK are not receipts.
- **superseded:** the receiver replaced a still-queued same-key message with a
  newer one. This is never subsequently counted as delivered through the carrier.
- **failed:** the sender observed a definite refusal, such as an unknown target
  or a full queue. An addressed publish to an unknown target is refused before
  publication. Known actor/root names and retained stale participants remain
  valid publication destinations; topic broadcasts are unchanged.
- **unknown:** the sender timed out without knowing the outcome. A receiver's
  later `delivered` line for the same event id is valid and does not erase unknown.

Mesh-carried sends use the original mesh event id. Local messages and
pre-publication refusals use a sender-owned send id. Identity metadata is supplied
by the host and retained separately from user message data in durable queues.
The side that knows writes its own mesh root: receivers write delivered and
superseded; senders write failed and unknown. The mesh bridge does not replicate
these files; a cross-host reader must collect the relevant roots.

Writes use one `O_APPEND` write, file fsync, and directory durability barriers,
without acquiring the mesh lock. Existing receiver/control ownership fences own
final observations; the log does not introduce a new mesh coordination lock.
An incremental read-only index of the retained log makes repeated observations
idempotent across queue-cleanup failure, reload, and UTC partition boundaries.
Unknown and delivered have distinct keys, so both can be retained. Parallel
writers for different sends cannot overwrite each other's records.

Today and the preceding six UTC date partitions are retained. Pruning runs when
an outcome is written (there is no idle timer). Main keeps a journalled receipt
until append succeeds; actors keep pending final receipts in the queue envelope,
separate from runnable activations, and retry them at existing owner polls.
Unrelated actor startup and idle hooks do not create outcome files.

The incoming inbox display also deduplicates event ids within a batch and against
earlier inbox carriers on the current branch. Repeated renders do not consume
or mutate messages; switching branches rebuilds the display's view. Collapsed,
auto, and expanded views hide duplicate event ids while retaining unseen rows.
