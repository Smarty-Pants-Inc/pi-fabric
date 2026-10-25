# Read-only NATS ownership receipt

```sh
pi-fabric nats owner --session session:11111111-2222-4000-8000-000000000003 \
  --creds /absolute/machine.creds --ca /absolute/ca.pem \
  --servers tls://broker.example:4222 --machine reader-machine \
  --org example --path-b-machines reader-machine,owner-machine \
  --mesh /absolute/hub-mesh --timeout-ms 30000
```

`--session` accepts a lowercase UUID with or without `session:`. The connection
uses the same policy source as `nats send`: `PI_FABRIC_MESH_NATS`, otherwise
`PI_FABRIC_NATS_CUSTODY_STREAM`, `PI_FABRIC_NATS_LIVENESS_STREAM`, and
`PI_FABRIC_NATS_MACHINES`. Credentials must be the scoped **agent** credential for
`--machine`; an operator credential is not a substitute. The machine may also be
the owner's machine: the reader does not register an identity or acquire a lease.
Give exactly one of `--mesh <dir>` (read the file-side handoff marker) or
`--no-mesh` (an explicit NATS-only receipt, for a host without the hub mesh).

Stdout is one JSON object:

- `format: 1`, `checkedAt` (ISO timestamp), canonical `rootId`, `sessionId`, and
  `reader: { machine }`.
- `verdict`: `owned` only if the ready NATS participant view resolves one live
  route; `absent` if there are no rows; `ambiguous` if multiple authenticated
  machines hold rows (even when only one is live); otherwise `stale`.
- `owner`: for `owned`, `{ machine, hostId, identityId, incarnation, status,
  presenceRevision, lease: { revision, incarnation, updatedAt, expiresAt } }`;
  otherwise `null`.
- `nats.rows`: the participant view's ownership rows, including stale rows,
  authenticated machine, presence revision and available lease evidence.
- `file`: `null` with `--no-mesh`. Otherwise `{ meshRoot, handoff,
  mirror }`. The optional handoff is `{ link, handedOffAt, operator, hub,
  inForce }`; the optional mirror is `{ host, participant }`, with sanitized
  ownership/link fields and file-state versions. A returned native can void the
  handoff even if it is stale. No handoff/mirror is represented by `null`.
- On read failure, `unknown` names the incomplete source(s) and `error` is a
  fixed diagnostic. A file read failure can coexist with `verdict: "owned"`;
  **that receipt still exits 1**. Unknown NATS data is not reported as absent.

Exit **0** means `owned` and all requested sources read; **1** means not owned or
an incomplete read; **2** means usage (help is written to stderr). A receipt is a
point-in-time observation, not an atomic cross-backend snapshot, a reservation,
an ownership-transfer fence, or permission to release a host. Routing liveness
comes from `KvParticipantDirectory`, not a new timestamp comparison. Broker TTL
and the direct gets' current lease rows decide the snapshot's lease presence.

## Read-only boundary and limits

The reader uses `openNatsConnection(role: "agent")`, then
`openNatsKv(..., false)` for the existing presence and lease buckets. **No consumer
is created**, including ephemeral/ordered consumers. It lists subjects with
`STREAM INFO KV_fabric_presence` filtered to
`$KV.fabric_presence.*.p.<token(rootId)>`, then uses the existing KV get
(`STREAM.MSG.GET`, not `DIRECT.GET`) for each listed key. For leases it lists
`KV_fabric_leases` with `$KV.fabric_leases.*.h.<token(hostId)>` for the root and
every distinct `ownerHostId` in the presence rows, then gets those keys. Tokens
are the first 32 hex characters of SHA-256 of the ID. Deleted/purged or expired
rows returned as missing by get are omitted. Incomplete listings fail closed as
unknown NATS state/exit 1. Paged listings are conservatively refused: the installed
SDK merges pages and clears the original count, so this reader cannot verify that
such a listing is complete. It never accepts that unverifiable partial result.

The listing and gets are a **non-atomic observation**, as the earlier watch
snapshots were: a row can change or expire between reads. The command does not
subscribe for later changes or fence a move. In-memory snapshot iterators feed
`put` events and `ready` to `KvParticipantDirectory`, then wait for its close;
`owners()` and `resolve()` retain the participant view's validation and liveness
logic. No broker KV watch runs. The transport rejects send/inbox/write operations;
its lease gate is always false. It never provisions a stream, bucket or key and
invokes no publication, renewal or removal. Closing the unregistered directory
has no owned rows or cleanup intents.

`--mesh` supports the **file and SQLite backends**, selected by the runtime's
`meshStateSourceOf` reader rule using the moved marker and database metadata,
not the reader's configured default. SQLite is loaded at first use through
`process.getBuiltinModule('node:sqlite')`. The database must already exist;
`DatabaseSync(..., { readOnly: true })` executes one SELECT of schema/backend/epoch
and the relevant `kv` namespaces. It never initializes a database, runs write
PRAGMAs or checkpoints, or uses the runtime's writable SQLite opener. A WAL-mode
database must already have regular WAL/SHM sidecars: SQLite can create an empty
WAL even on a read-only open when those are absent, so that case is refused
before opening. A WAL reader may update SQLite's `-shm` read marks/locks; the
read-only guarantee covers DB/WAL bytes and durable mesh state, not that
shared-memory coordination file.

read-only: no DB/WAL writes; SQLite takes shared-memory read locks in state.db-shm

The `-shm` read marks are coordination memory, not durable state.

A missing root/state/database, unknown or NATS backend, malformed state/handoff,
unreadable participant or invalid lease fails closed. Direct bounded JSON reads,
read-only SQL and `readHostLeases(..., problems)` avoid `MeshStore` construction,
which creates writer-census files even in a write-blocking subclass. Handoff
validity and in-force semantics reuse `bridgeHandoffOf` and `bridgeHandoffScope`.
There is no mesh custody or filesystem lock creation. File outputs select
ownership fields; credentials, seeds, JWTs and message/participant content are
not serialized.

The tests compare path, size, mode and nanosecond mtime before/after real-handoff
readback (excluding only SQLite `-shm`), compare SQLite DB/WAL bytes, require zero
fake-KV writes/deletes, explicitly check SQLite DB/WAL nanosecond mtimes, and
exercise this command with a real authenticated `FabricRuntimeState` Main and
real broker lease expiry. Both KV streams' consumer counts must stay unchanged
around every real CLI invocation, including a two-machine ambiguous root whose
second presence and lease are written with the second machine's credential.
The real broker also confirms agent `DIRECT.GET` permission denials for both
buckets; the successful receipt exercises the permitted `STREAM.INFO` and
`STREAM.MSG.GET` path.
CLI dispatch/build wiring is maintained by the integration owner.
