# U2: opt-in JetStream mesh event log

Refs smarty-dev#6477. This is an experimental event-log implementation, **not a runtime cutover**.
`MeshStore`, its synchronous reads, filesystem default, state storage, archive protocol and public
providers are unchanged. `pi-fabric/mesh` exports lazy `openJetStreamEventLog` and `openFileEventLog`
factories plus an asynchronous `MeshEventLogBackend` contract. Merely importing the public entry does
not load `nats`. The file adapter runs today's `MeshStore`, not a rewritten file log.

## Seam and current file behavior

The lane's available main is local `fv2-main` / `origin/fv2-main` at
`5eed0787da6be2ff356f73d89deee23d50925f4f`. Commit `edebc777` is not in this local object database;
no network/GitHub lookup was made. The current facade delegates event operations to `EventLog` in
`src/mesh/event-log.ts`; file reads/tails/cursor queries are synchronous. Therefore a remote backend
cannot be substituted without migrating callers to async operations. The added adapter is additive.

File publication reserves `sequence`, appends `events.jsonl` under `.lock`, and optionally confirms
fsync/receipt/archive durability. A file retry with `dedupeKey` returns the durable original receipt,
without a time limit. Batches commit an ordered prefix of 1..256 inputs, bounded by 50 ms/retained
bytes. `read({after})` uses **event sequence**, whereas `tail` uses generation+byte offset cursors.
With no `after`, `read` returns the recent matching suffix in ascending order. Publication freezes
ordinary payloads and provenance; data stamp callbacks run at the file commit time. Compaction is
64 MiB high-water / 16 MiB retained-tail by default. If configured, `MeshArchive` stores all events
since activation, in dated/topic files and sequence indexes. It has **no mesh-event age collector**.

## Stream, subjects and global order

- Root identity defaults to `path.resolve(root)`. Supply the same `rootId` on hosts with different
  mounts; that value is a deployment identity, not a secret.
- Root token = full SHA256 of the root identity. Stream = `FABRIC_<root-token>` (one/root).
- Subject = `fabric.<root-token>.<hex(UTF-8 topic)>`. Every topic occupies one token. Dots, colons,
  slashes, dashes and underscores escape injectively; `a.b`, `a/b`, `a:b` never collide. Fabric's
  existing 128-character ASCII topic grammar remains enforced. Exact topic filtering is server-side.
- The stream owns the global sequence across topics. Stored JSON contains no sequence; reads stamp
  the envelope with the authoritative `PubAck.seq` / message stream sequence. Consumer delivery
  sequence is **never** used. No local `sequence` file, log, receipts or mesh lock is needed.
- Streams use file storage, LimitsPolicy, DiscardOld, ACKs and explicit R1/R3 replication. Opening an
  existing stream validates policy/subjects/limits and fails on drift; it never silently updates them.

## Publication and duplicate retry

`prepare(input)` captures a pending Fabric envelope before network work. `publishPrepared(pending)`
sends `Nats-Msg-Id = pending.id` with an expected stream header. `publish(input)` combines the two.
The default id is UUID; an explicit host `eventId` is allowed; `dedupeKey` deterministically derives
an id scoped to this root, so retries/restarts reuse it. Payload, `createdAt`, identity, verification
and provenance are frozen. A stamp callback receives **pre-publication capture time**, not remote
commit time. A synchronous `input.fence` is explicitly rejected: it cannot fence an async remote
commit. JetStream publication always waits for the server ACK; `durable:false` does not weaken it.

The default duplicate window is **120 seconds**, configurable. This is bounded exactly-once
publication, not the filesystem receipt's indefinite guarantee. Do not claim safe retries outside
that window or after deleting/recreating the stream. Persist the pending envelope before sending if
you need to recover an unkeyed operation after process death. A missing ACK throws
`JetStreamPublishUncertainError` with those exact bytes/id. A duplicate ACK returns the original
stored event at its original stream sequence (not a new timestamp/payload). If retention has already
removed it, reconciliation fails rather than fabricating a changed event.

Batches are ordered and non-atomic, with 1..256 inputs. All payloads are captured/validated before
sending. Success returns the entire prefix (there is no local-lock 50 ms yield requirement). Failure
throws `JetStreamBatchPublishError` with `committed` ACKed prefix and `remaining` exact pending suffix;
checkpoint the prefix, then retry the suffix inside the duplicate window. Other writers may
interleave between events: stream order, not batch adjacency, is the guarantee.

## Reads, cursor mapping and waits

- `read({after,topic,to,limit})` and `tail(cursor)` use **stream sequence** cursors. These are not
  compatible with file generation/byte cursors. A migration must reconcile with a saved sequence/id;
  never feed a file offset into a JetStream cursor.
- One-shot reads capture a stream high-water boundary, create an ephemeral AckNone **pull** consumer
  starting at `after+1`, and use `consume` callbacks with server waits/heartbeats. Matching event
  order is checked; no `events.jsonl` polling. Exact topics filter on the server, recipients locally.
  With no `after`, all retained matches up to the boundary are scanned and only the recent suffix
  retained. This is correctness-first: a future last-N index can avoid scanning large archives.
- Consumer creation is an explicit bounded API request. The nats 2.x ordered helper's initial retry
  loop can hide errors for minutes. Callback `consume` also avoids the raw pull iterator's early-break
  close deadlock observed during fault work. Snapshot completion closes/deletes its ephemeral
  consumer; a 30-second deadline fails rather than silently returning an incomplete snapshot.
- `openReader({cursorId,after,topic,to})` creates/binds `cursor_<SHA256(cursorId)>` in this stream.
  The durable uses explicit ACKs, StartSequence, Instant replay, `max_ack_pending=1`, and inherits
  stream replication. `after` is the **initial** boundary only; an existing durable resumes its
  confirmed server checkpoint. Changing a cursor's filters fails. One active owner/cursor is the
  caller contract; duplicate active handles in one backend are rejected.
- `reader.next(waitMs)` uses the modern `consumer.next({expires})`: server-side blocking pull,
  default 30 s, minimum 1 s. An outstanding event must be ACKed before requesting another.
  `reader.ack(event)` uses double ACK (`ackAck`); only a matching delivered id/sequence can advance
  the checkpoint. Persist application work **before** ACK. Recipient-mismatched events advance
  the same durable cursor with confirmed ACK. Close leaves the durable, does not ACK unhandled
  events, and rejects while a next call is in progress. Reopen redelivers unhandled events.
- `deleteReader` is explicit and rejects while its local reader is active. `close()` closes readers
  and the connection; owners must settle outstanding `next` calls first.

**Reader delivery is at least once**, even with double ACK. In the local NATS 2.14.7 R1 abrupt
SIGKILL probe, an immediately confirmed consumer ACK was replayed after server restart, both with
server default sync and `sync_interval: always`. This is not a lost event. Clean server restart and
client/backend process restart preserve checkpoints in conformance. Do not advertise exactly-once
application side effects: persist/dedupe by event id and retain a stream-sequence checkpoint. R3
protects against one server/leader loss; it is not a proof against simultaneous disk/power failures.

## Retention and archive translation

- `retention:"live"` (default): 64 MiB `max_bytes`, no age expiry (`max_age=0`). JetStream continuously
  evicts oldest messages at the limit, rather than file compaction's 64-to-16 MiB hysteresis. Thus it
  retains **at least the intended recent tail**, but not byte-identical generations. NATS accounting
  includes envelope/subject/header overhead; don't equate bytes with JSONL payload bytes.
- `retention:"archive"`: unlimited bytes (`max_bytes=-1`) and age (`max_age=0`), matching today's
  unbounded MeshArchive history. Limits/age can be explicitly supplied for an approved policy, but
  no arbitrary default archive TTL is invented. This does not export dated/topic archive files or
  seals; operational archival/export is outside this lane.
- Saved nonzero cursors behind retained history fail with `JetStreamCursorExpiredError`, including
  complete expiry of all newer messages. `after:0` intentionally starts at the oldest retained event.
  Sequence is monotonic after eviction/age expiry. Slow durable readers are checked against retention
  before a pull. Topic-specific holes cannot always be diagnosed from the global first-sequence bound.

## Usage and reproducible checks

```ts
import { openJetStreamEventLog } from "pi-fabric/mesh";
const log = await openJetStreamEventLog({
  root: "/mesh/example", rootId: "example-mesh", servers: ["nats://127.0.0.1:4222"],
  replicas: 3, retention: "archive", duplicateWindowMs: 600_000,
});
const pending = log.prepare({ topic: "team.work", from: { id: "host", name: "host", kind: "main" }, data: { n: 1 } });
// Save pending durably if the operation must survive this process dying before/after the ACK.
const event = await log.publishPrepared(pending);
const reader = await log.openReader({ cursorId: "worker-A", after: 0, topic: "team.work" });
const next = await reader.next(30_000);
if (next) { /* durably apply/dedupe next.id first */ await reader.ack(next); }
await reader.close();
await log.close();
```

The lane contains the inherited official `nats-server v2.14.7` Linux amd64 release under
`.lane/nats/` (ignored), checked against its official SHA256SUMS before reuse. `nats@2.29.3` is a
branch-only dependency in `package.json` / `bun.lock`. No global client/server install or service.
Tests find an explicit `NATS_SERVER`, then `nats-server` on PATH, then that lane binary. Without it,
transport integration tests skip and file conformance remains available for optional local runs.
Set `NATS_SERVER_REQUIRED=1` to fail instead of skipping when the binary is missing. The Ubuntu CI
job downloads the official v2.14.7 Linux amd64 release, verifies its release SHA256SUMS and pinned
archive digest `e5c20b1cb2c0566b54c544312e91e011f9e130c5c80f16a14f4cf28ef30b8be2`, adds it to PATH,
and always runs all three suites below with the server required, independently of affected-test
selection. Windows does not install the Linux binary. All fixtures bind loopback, choose ports dynamically,
write configs/data under TMPDIR, keep server logs under TASK_OUT when supplied, and stop/wait children.

```sh
NATS_SERVER=/absolute/path/to/nats-server bunx vitest run \
  tests/mesh-event-log-conformance.test.ts tests/mesh-jetstream-event-log.test.ts \
  tests/mesh-jetstream-fault.test.ts
TASK_OUT=/kept/artifacts NATS_SERVER=/absolute/path/to/nats-server bun scripts/benchmark-event-log.ts
bun run typecheck
bun run build
```

The R3 fault test starts a 512-event burst and kills the elected stream leader on its first ACK
while other requests remain in flight (511 in the final run), reconciles stable ids
within a ten-minute duplicate window, checks every acknowledged id survived, checks all recovered ids
and sequences are unique/ordered, verifies a replicated durable cursor checkpoint, then
restarts/catches up the third replica. Benchmark uses 100 warm
publishes, 1000 measured sequential publishes, and three reads of a 10,000-event backlog with 256-byte
payload data. It includes file default and file explicit durable barriers, plus R1/R3 JetStream with
`sync_interval:always` fixture storage. This is same-container, warm local I/O evidence, not a fleet
network benchmark or a guarantee of fsync-equivalent latency under other server policies.
