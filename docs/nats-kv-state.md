# Experimental NATS KV mesh state (U2)

Refs smarty-dev#6477. Assignment carries Paul's approved NATS JetStream direction and research comment c6076003313: nats-server **2.14.7+**, `sync_interval: always`, production R3 across **three hosts**, and KV REVISION as the fencing token with leader reads. No GitHub was contacted in this lane.

## Status and integration boundary

This branch adds the official **`nats` 2.29.3 JS client dependency on this branch only**, `NatsKvStateStore`, an opt-in asynchronous selector, shared single-key contracts, protocol/codec tests, and reproducible local R3/latency tooling. It does **not** change Fabric's runtime `mesh.stateBackend` configuration, its environment override, or its default file backend.

The inspected main already has `StateBackend`, `StateFile`, and `SqliteStateStore`. Its reads are synchronous; `writeBatch`, snapshot tokens, synchronous prepare callbacks, and `withWriteFence` require atomic multi-key custody. NATS KV is asynchronous and atomic **per key**, not per batch. An event-driven cache cannot truthfully implement leader-read authority or that transaction interface. This lane therefore does not implement that interface, silently weaken batch/fence guarantees, block the event loop on network I/O, or advertise runtime compatibility. Further integration requires a separately reviewed async/single-key migration of those callers (or a genuine transaction design). The older exact-revision/batch differential suite runs unchanged for file/SQLite; the new common async contract explicitly excludes those unsupported guarantees.

Public API (`pi-fabric/mesh`):

```ts
const store = await openAsyncMeshStateStore(meshRoot, {
  backend: "nats-kv", // separate experimental selector, NOT mesh.stateBackend
  nats: {
    servers: ["nats://host-a:4222", "nats://host-b:4222", "nats://host-c:4222"],
    experimentalNatsKv: true,
  },
});
// get/list/listAll/put/delete are async; CAS is put({ ...input, ifVersion }).
await store.close();
```

`NatsKvStateStore.open` also exposes `version(key)` (including tombstones), `compareAndSwap`, and push `watch`. Without the flag, it refuses **before** client loading/connection. `openAsyncMeshStateStore(root)` defaults to file. No production config or services are modified.

## Bucket and key codec

One bucket per normalized mesh-root path: `FABRIC_STATE_<SHA256(path.resolve(root))>`, backing stream `KV_<bucket>`. All participating hosts must use the **same logical absolute mesh root string**. Local realpath/symlink resolution is deliberately not used; host filesystem aliases must not redefine distributed identity. Different roots isolate state and sequence spaces. Metadata binds the root digest and codec version. Existing streams must match all critical bounds/storage/replica/retention settings; incompatible streams fail closed and are never reconfigured.

Fabric valid keys: ASCII alphanumeric first character, then alphanumeric, `.`, `_`, `:`, `/`, `-`, at most 256 characters; `__proto__`, `prototype`, `constructor` slash/colon segments are rejected as in the existing stores. The codec is reversible and canonical:

- `k.` names the key-codec namespace.
- `/` becomes a NATS subject-token separator `.`.
- Every segment is prefixed by `s`; the empty segment is `s`. This preserves repeated/trailing slashes and prevents empty NATS tokens.
- Literal dots become `=2e`, colons become `=3a`; alphanumerics, `_`, and `-` remain literal. `=` is not a valid Fabric key character, so escape syntax cannot collide.
- Decode rejects unknown/uppercase/non-canonical escapes and validates the resulting Fabric key. No encoded key has `*`, `>`, `/`, or an empty subject token.

Examples: `a.b` -> `k.sa=2eb`; `a/b` -> `k.sa.sb`; `a:b` -> `k.sa=3ab`; `a//b/` -> `k.sa.s.sb.s`. Slash, dot, colon, underscore, dash and empty-segment forms remain distinct.

Prefix listing uses **`kv.keys(filterSubject)`**, not an unfiltered read/snapshot. For `topology/participants/`, the filter is `k.stopology.sparticipants.>`; for a partial final token (`topology/part`), NATS has no partial-token wildcard, so the filter is the complete parent `k.stopology.>` and the adapter applies `decodedKey.startsWith(prefix)`. A first-token prefix necessarily uses `k.>`. Results are `localeCompare` sorted and omit keys deleted before their individual leader read. Listing is **not an atomic multi-key snapshot**. Requested `limit` is applied after enumeration.

The retained local key-only sample fixture includes every key from both actual local state.json snapshots. Topology/participants, hosts, sessions, inbox/control-seen shapes are real. Actors and GitHub-ingress shapes are additionally tested as representative synthetic cases because these local snapshots contain none. No live state values are copied into artifacts or altered. The largest observed local real entry is around 1.1 KiB, not 100 KiB; conformance explicitly tests the requested **synthetic 100 KiB** JSON payload as the conservative worst-case size.

## Versions, CAS, deletes and fencing

`MeshStateEntry.version` is the acknowledged **KV revision / JetStream backing-stream sequence**. It is a bucket-global monotonically increasing number, not the existing file/SQLite per-key successor arithmetic, and not a wall clock. Bind a fencing token to its **bucket/root and key**; numeric tokens across buckets are not comparable. Revisions must be representable as safe JS integers. Updated metadata is a detached JSON envelope; `updatedAt` is informational and never the fence.

- Unconditional put: `kv.put(key, bytes)`, version from its PubAck.
- Positive conditional put: **`kv.update(key, bytes, expectedRevision)`**.
- Virgin-key CAS 0: `kv.put(..., {previousSeq: 0})`. The client's `kv.create` intentionally resurrects delete markers; it would wrongly accept stale version 0, so it is NOT used.
- Delete first reads the leader and compares the requested revision, including a retained tombstone. Missing/deleted with matching revision is a no-op; a mismatch is a CAS conflict.
- A live delete is **one publish** carrying `KV-Operation: DEL` and `Nats-Expected-Last-Subject-Sequence: <revision>`, exactly the wire semantics of `kv.delete({previousSeq})`. The official `kv.delete` discards its PubAck (`Promise<void>`); publishing with the official JS client retains the exact deletion revision even if another writer immediately recreates the key. No read-after-delete approximation is returned.
- Unconditional delete also uses a revision check to avoid deleting a concurrently replaced value, with at most eight CAS retries. Explicit CAS conflicts are never retried as a success. Sequence-conflict API codes 10071 and 10164 (the latter observed on official 2.14.7 during concurrent CAS) are mapped to the existing MeshBatchConflictError; connectivity/capacity errors are propagated, not mislabeled as conflict/absence.
- Delete markers are retained forever (history 1, no TTL, no purge); recreation consumes a new global revision and old tokens cannot win. Admin stream destruction/recreation would reset sequence history and **invalidates every old fence**; it is prohibited operationally and requires a coordinated new root/epoch, not transparent recovery.

Every read uses the official client with **`bindOnly: true, allow_direct: false`**. That takes `STREAM.MSG.GET` through the stream leader, not the direct-read path that may return follower data. There is no local authority cache. Stable server version is checked at open and before subsequent operations; a too-old reconnected server fails closed. All production replicas must run qualified releases.

## Watch and resource ownership

`watch(prefix)` is `kv.watch` with a filtered subject and `UpdatesOnly`; it returns an async iterator of PUT/DEL events with their KV revisions. No polling loop, timer-driven get/list, or synthetic event generation is used. `includeCurrent` opts into the last retained value per key; `resumeFromRevision` passes through a positive KV sequence. Watches carry ordered **notifications**, not an authoritative read cache; callers making authority decisions must re-read the leader and CAS. History 1 means a disconnected watcher cannot assume lossless replay of every overwritten intermediate value; it must rebuild on discontinuity. Atomic multi-key snapshots and a durable change journal are not claimed.

Caller `stop()`, iterator `return()` or `throw()` (even before first next), AbortSignal, and store `close()` stop consumers. Connection creation failures close the owned connection. Local test workers and the cluster runner await every subprocess and shut down the three servers in `finally`. No orphan services, system installation, agents, pushes, or credentials are involved.

## Limits and durability prerequisites

| Bound | Default / policy |
| --- | --- |
| Encoded JSON envelope | **256 KiB**, including identity/metadata; fail before publish when oversized |
| NATS stream max message | envelope limit + **512 bytes** reserved for headers |
| NATS max_payload | must permit envelope limit + 512; local runner sets **2 MiB** |
| Bucket bytes | **32 MiB**, configurable only at first provision |
| Keys | **100,000**, enforced by backing-stream `max_msgs` with history 1, counting live keys **and tombstone subjects** |
| Retention | one last message per subject; no max_age/TTL, no mirror/sources |
| Overflow | `DiscardNew`: reject writes rather than evicting values/fences; at a global cap, updates/deletes may also be refused depending on server enforcement |
| Administrative removal | `deny_delete`, `deny_purge` protect message history; admin stream/account destruction remains an operational prohibition |
| Replicas | **3**, File storage; no fallback to R1 or memory |
| Server | stable **2.14.7+**, homogeneous across hosts |

`sync_interval: always` is a **server deployment prerequisite**, not a client KV option. The client cannot attest filesystem/fsync policy from the KV API; the local runner writes the exact config and validates it with `nats-server -t`. Production R3 must span three separate hosts/fault domains. A three-process cluster on this one host only proves functional R3 conformance, not three-host availability or power-loss durability. No host networking/services are changed outside owned loopback test processes.

## Runtime integration options

### Blocker found in round 1

`StateBackend` (`src/mesh/state-backend.ts`) and its `MeshStore` delegates (`src/mesh/store.ts`) require synchronous `get`/`list`/`listAllShared`, snapshot tokens/change stamps, synchronous `prepare(view)` on one authoritative transaction, atomic multi-key `writeBatch` rollback, and synchronous `withWriteFence` custody while related effects are appended. Writes already return promises, but reads/callbacks do not. JetStream KV provides async network operations, bucket-global revisions and atomic **per-key** CAS only. Awaiting several puts is not a transaction; a watcher is not a leader-read snapshot; blocking the event loop cannot create distributed custody. The common async contract deliberately excludes the legacy batch interface.

### Option 1: migrate to an async authority interface

Introduce a versioned `AsyncStateBackend` and awaitable read/snapshot/fence APIs alongside the existing interface; wrap file/SQLite first with their current semantics. Inventory all synchronous callers, migrate one subsystem at a time, and make stale-read retries, cancellation/deadlines and shutdown explicit. Do not await network I/O inside local transaction custody: replace that use of `prepare` with immutable snapshot input plus revision-checked commit.

Enable bare NATS KV only for audited independent single-key families. Multi-key invariants still need a transactional backend, a bounded aggregate value committed with one CAS (within the 256 KiB envelope cap), or option 3. Global KV revisions do not preserve exact per-key successor arithmetic or synchronous snapshot tokens.

**Costs:** broad call-graph/API migration, promise propagation into synchronous registration/UI/event paths, authoritative-read network latency and failure handling, and separate designs for batch rollback and state/event fences. Async migration alone does **not** solve atomic multi-key transactions.

### Option 2: local write-ahead SQLite cache synchronized to KV

Commit the local batch plus a durable outbound intent in one SQLite WAL transaction. A synchronizer publishes intents with idempotency IDs and KV revision CAS, records acknowledgements, and an inbound projector transactionally applies changes. Persist outbound/inbound cursors, replay after crashes, and distinguish provisional local versions from acknowledged global fences.

This preserves synchronous local reads and local atomic batches, but is **local authority / eventual replication**, not a drop-in globally authoritative NATS backend. Two hosts can commit incompatible batches before remote CAS resolves them; remote readers can see partially mirrored batches. A local lease/fence does not exclude remote commits. Globally authoritative reads require an awaited catch-up/leader barrier (an async interface) or centralized writers.

**Costs:** two stores, durable outbox/cursor ownership and failure windows, conflict resolution/offline policy, bootstrap/resync, disk/checkpoint management, lag monitoring and backpressure. SQLite power-loss guarantees require `sync()` or a reviewed stronger policy, not default `synchronous=NORMAL`. Do not silently treat a watch-fed cache as authoritative.

### Option 3: one JetStream batch message applied by a projector

Encode a complete batch command (identity, idempotency ID, expected revisions, deterministic operations and schema version) in **one** durable R3 JetStream message on a dedicated command stream. A serialized authority/projector consumes stream order, validates all conditions against one prior snapshot, and atomically applies the accepted batch plus input cursor/result in SQLite. Rejections also advance the cursor with a durable rejection result; replay deduplicates command IDs. This is not a series of per-key KV puts.

A PubAck proves durable command admission, **not** successful state commit. Callers await the projector's committed/rejected result and fence subsequent reads to at least its applied sequence. Arbitrary JS `prepare` closures cannot travel over the wire: send deterministic commands or compute against explicit snapshot input and validate versions at commit. State/event outbox effects need the same ownership boundary. Projected KV keys are a convenience view, never an atomic batch read source.

**Costs:** a versioned command/result protocol, log retention/compaction/bootstrap, projector leadership and stale-leader fencing, idempotent replay, bounded message sizes, extra application/ack latency and async read barriers. Total log order plus a local SQL transaction alone does not prove globally fenced projector ownership; the protocol needs independent failure/conformance review.

### Recommendation

Keep this store experimental and leave `mesh.stateBackend` and its file default unchanged. Begin **option 1** as a separately reviewed migration: wrap file/SQLite, inventory synchronous and batch/fence callers, then allow KV only for genuinely independent single-key state. If whole-runtime distributed authority must preserve atomic multi-key batches, prototype **option 3** behind that async seam, requiring projector/result, replay, conflict, crash and ownership-fence gates before cutover. Use option 2 only when the product explicitly accepts local authority and eventual cross-host visibility, never as a transparent NATS authority cache. No option is implemented here; local one-host R3 success does not qualify production three-host or power-loss durability.

## Reproduction and evidence

The download restriction is unchanged: no GitHub, credentials, network downloads, alternate binaries or system installation were used. The caller staged the official **v2.14.7 linux-amd64 archive and release SHA256SUMS** at `/home/paul/lanes/nats-release/` on ryzen3. This continuation independently verified archive SHA256 `e5c20b1cb2c0566b54c544312e91e011f9e130c5c80f16a14f4cf28ef30b8be2` and extracted it only to the lane's `tools/nats-release/`. The runner also checks executable bytes against the archive member. Retained live/conformance/latency evidence is under the assigned `$TASK_OUT`; mock tests remain distinct from real R3 evidence.

The first live continuation exposed API error **10164** during concurrent expected-sequence publishes. The adapter handled only 10071, so some CAS losers escaped as generic NatsError and eight-process workers could not retry. This continuation maps only these two numeric sequence-conflict codes for put/delete, retains explicit-CAS no-retry and bounded unconditional-delete behavior, and adds regressions proving unrelated API/connectivity/capacity errors propagate. Initial failed-run evidence is retained separately.

After staging the official archive, its official SHA256SUMS and its extracted lane executable:

```sh
TASK_OUT=/absolute/retained/artifacts \
  bun scripts/run-nats-kv-conformance.ts \
  /absolute/lane/tools/nats-server \
  /absolute/lane/tools/nats-server-v2.14.7-linux-amd64.tar.gz \
  /absolute/lane/tools/SHA256SUMS
```

The runner verifies the archive against the official sums, hashes the extracted archive member and supplied executable to prove they match, checks `-v`, allocates owned loopback ports, validates `sync_interval: always` configs, starts R3, waits for server/metadata-leader **log events**, runs shared/live/eight-process tests, benchmarks, and shuts down/awaits every node. It saves configs, verification, stream/replica topology, watcher sequence evidence, eight-process revisions, logs and latency distributions. It does no downloads or authentication.

Latency probe runs 100 warmups + 1,000 samples each for get/put/CAS for **file, SQLite and NATS KV** on the same box, with identical 168 1-KiB topology values plus one 100-KiB actor value and the mutation key. It rotates/reverses backend order to balance first/middle/last positions; CAS publish latency excludes its separately measured pre-read. Both 1-KiB (1024-byte text) and 100-KiB (102400-byte text) mutations are measured; JSON/envelope bytes add overhead. File uses its ordinary-read cache/canonical-change gate and atomic-rename writer (no file-fsync claim). SQLite uses default WAL `synchronous=NORMAL` without per-operation `sync()`; NATS uses R3 `sync_interval: always`. This is a default-backend performance comparison, **not a durability-equivalent comparison**. Without endpoints, JSON explicitly marks `PARTIAL_NATS_BLOCKED` and reports only measured file/SQLite numbers, never fabricated NATS data. Raw samples are retained with nearest-rank p50/p99.
