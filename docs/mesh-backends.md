# Mesh state backends: the selector and migration to NATS

smarty-dev#7504 (under smarty-dev#6477). Owner: fabric-store (selector, migration);
fabric-v2 owns the NATS store (pi-fabric#708 KV lease, #717 event log).

## Selector

`createStateBackend` (`src/mesh/state-backend.ts`) resolves a kind and calls its entry in
`STATE_BACKEND_FACTORIES`. The kinds list is the import-free leaf `src/mesh/state-backend-kinds.ts`,
so `config.ts` parses `mesh.stateBackend` without loading SQLite. Precedence is unchanged:
an explicit option, then a valid `PI_FABRIC_MESH_STATE_BACKEND`, then `file`. An empty
environment value counts as unset. An unknown non-empty value now fails config loading and
store open. Before this, it was ignored, and a typo silently opened a different store than its peers.

| kind | factory | `unavailable` (falls back to `file`) |
|---|---|---|
| `file` | `StateFile` | none |
| `sqlite`, `shadow` | `SqliteStateBackend`, `ShadowStateBackend` | non-local filesystem (R16), no `node:sqlite` (R19) |
| `nats` | the backend a host prepared with `openNatsStateBackend` for that root (`src/mesh/state-backend-nats.ts`); otherwise throws `MeshStateBackendNotBuiltError` (`FABRIC_MESH_STATE_BACKEND_NOT_BUILT`) | none |

`nats` never falls back to `file`. A host that wrote local state while its peers wrote the
shared stream would split the mesh.

Phase 1 (smarty-dev#7504, single host) implements option (c) below.
- **The factory and the client:** the factory is synchronous and the connection is not, so a host first awaits
`openNatsStateBackend({ root, servers })`. That call loads the official nats.js v3 client
(`@nats-io/transport-node`, `@nats-io/jetstream`; a lazy entry point, never in the startup graph), refuses and
replays the stream. The factory then returns that prepared backend.
- **Refusals:** a server older than 2.11; a server whose `sync_always` is not proven true, unless the host passes
`acceptPeriodicSync` (then `sync()`, the #9313 barrier, rejects with `MeshStateNatsNotDurableError`: not durable); a retired stream; writes while the stream is importing; a `fresh` read while the projector lags
(busy, retryable); and a stream message that could not have passed the CAS (the backend fails closed).
- **Bridges (interim):** on `nats` the R20 state write fence excludes no other writer, so a bridge's held-event
ownership check (security F2) cannot hold. A root where a bridge holds events (a mirrored remote host record) refuses
`nats`, and a bridge refuses a `nats` store (`FABRIC_MESH_STATE_NATS_BRIDGE_REFUSED`). This lasts until the event log
moves onto the state stream (smarty-dev#9725) or a lease fences the append.
- **Lifecycle:** `import-begin` (ready, newer epoch) -> importing; `import-commit` (importing, same epoch) -> ready;
`retire` (ready or importing, newer epoch) -> retired, terminal. Any other control record, or a batch after `retire`, is a
protocol violation: the projector fails closed.
- **Transport:** a non-loopback server must be `tls://` (the client then requires TLS; `tls` passes CA and client
files) and a non-loopback monitoring URL `https://`; plaintext refuses before connecting
(`FABRIC_MESH_STATE_NATS_INSECURE`). Loopback (127.0.0.0/8, ::1, localhost) may be plaintext.
- **Streams:** one per mesh root, `FABRIC_MESH_` plus 32 hex characters of the SHA-256 of the root's realpath.
- **Replica placement:** an open or migration uses its explicit `replicas`, else the agent-layer
`mesh.nats.replicas`. It never silently lowers that value. JetStream `err_code: 10005` (no suitable peers for
placement, such as an offline peer) retries within `placementTimeoutMs`, default **30 000 ms**: enough for a normal
peer restart and metadata election, the same scale as cluster readiness. One budget from the first create covers
placement retries, backoff, subsequent pending requests and post-create leader readiness. Exhaustion throws
`MeshStateNatsUnavailableError` (`FABRIC_MESH_STATE_NATS_UNAVAILABLE`, `reason: "placement-unavailable"`,
`natsErrCode: 10005`), with the original sanitized cause. Other API/configuration/permission errors do not gain retries;
the existing bounded transport-election/lost-ack handling is unchanged. Restore the peers, then retry the operation.
- **A lost ack:** the CAS stored the batch, if at all, at exactly its base sequence plus 1. After a retry fails the
CAS, the backend reads that one message and compares the batch id, so the batch is never run twice. If a purge
removed that message meanwhile, the outcome is unknown (`FABRIC_MESH_STATE_NATS_OUTCOME_UNKNOWN`), never a rerun.
- **Bounded replay:** every `snapshotEvery` messages (default 10 000; `compact()` on demand) a writer publishes a
snapshot of the model, in chunks on the batch subject and one end record on `<stream>.snapshot`, as one CAS chain right
after the model's sequence. It then purges the stream below the first chunk. Any commit inside the chain abandons the
snapshot (its chunks are inert). A host opens from the latest end record, and a projector whose delivery skips
sequences (a purge passed it) reloads from it. A stream purged without a snapshot is refused.
- **Snapshot integrity:** the end record commits to the sha256 of the canonical body (sorted by key), the previous
snapshot's digest, the body's bytes and entries, and the lifecycle (status, epoch). A loading host re-checks all of it
after the join. Every live projector re-serializes its own model when it applies an end record and compares the
digest and the chain. Any mismatch (an altered, dropped or swapped chunk, a wrong epoch) fails closed
(`FABRIC_MESH_STATE_NATS_SNAPSHOT_INTEGRITY`). This is integrity, not authentication: a stream writer that forges a
whole self-consistent snapshot is caught by live projectors, not by a host that opens after it (the same trust as
forging a batch).
- **Bounds:** chunks, chunk size, bytes and entries are checked against the state bound (`maxStateBytes`, 32 MiB)
before anything is fetched or joined; the replay after the snapshot is checked against `maxReplay` (100 000) before
the consumer starts (`FABRIC_MESH_STATE_NATS_LIMIT`). Nothing is truncated.
- **Repeated purges:** a gap seen during a re-sync is recorded, not dropped; the re-sync re-checks the stream's first
sequence and loops (3 times), then the backend fails closed (`FABRIC_MESH_STATE_NATS_UNAVAILABLE`), never stale.
- **The read model:** in memory, rebuilt at open from the snapshot plus the messages after it. Persisting it in the `state.db` schema, the
cross-writer write fence, the migration flag and the conformance suite against a real server are phase 2. fabric-v2 plugs in by setting `STATE_BACKEND_FACTORIES.nats`
to `{ unavailable?, create }`, where `create` returns a `StateBackend`. Its `unavailable` must
also refuse (throw), never return a reason, for the same reason.

### Required nats-server configuration (pi-fabric#907)

No fleet NATS server is deployed (the backend is opt-in). Any server that hosts a mesh state stream MUST be configured
as below. The stream is `FABRIC_MESH_<32 hex>` (`natsStreamName`); its subjects are `<stream>.state` and
`<stream>.snapshot`.

```
max_payload: 1MB                    # 1048576; the adapter refuses a server whose max_payload is larger
jetstream { sync_interval: always } # the adapter refuses a server without sync_always (unless accepted)
authorization {                     # or an account; the adapter refuses a NON-loopback server without auth
  users = [
    { user: fleet, password: $FLEET_NATS_PASSWORD, permissions: {
        publish: { allow: [
          "FABRIC_MESH_*.state", "FABRIC_MESH_*.snapshot",
          "$JS.API.INFO", "$JS.API.STREAM.INFO.FABRIC_MESH_*", "$JS.API.STREAM.CREATE.FABRIC_MESH_*",
          "$JS.API.STREAM.UPDATE.FABRIC_MESH_*", "$JS.API.STREAM.MSG.GET.FABRIC_MESH_*",
          "$JS.API.STREAM.PURGE.FABRIC_MESH_*", "$JS.API.CONSUMER.CREATE.FABRIC_MESH_*",
          "$JS.API.CONSUMER.CREATE.FABRIC_MESH_*.>", "$JS.API.CONSUMER.MSG.NEXT.FABRIC_MESH_*.>",
          "$JS.API.CONSUMER.DELETE.FABRIC_MESH_*.>", "$JS.API.CONSUMER.INFO.FABRIC_MESH_*.>", "$JS.FC.FABRIC_MESH_*.>",
          "$JS.ACK.FABRIC_MESH_*.>" ] }
        subscribe: { allow: [ "_INBOX.>" ] } } }
    # Every other user: no publish on FABRIC_MESH_* and no $JS.API.STREAM.PURGE / DELETE / UPDATE on them.
  ]
}
```

- **Only the fleet user** may publish to the state subjects or purge the stream; every other user is denied (the
  default once `permissions` exist). `DELETE` of the stream is granted to nobody.
- The stream's `max_msg_size` is set to 1048576 when the adapter creates it, and a stream with no limit or a larger one
  is refused (`FABRIC_MESH_STATE_NATS_LIMIT`).
- **What the adapter can and cannot check:** it reads the server's INFO, and refuses a non-loopback server whose INFO
  does not say `auth_required`, a `max_payload` over 1 MiB, a server older than 2.11 and one without `sync_always`. It
  CANNOT see other users' permissions (NATS exposes no client API for them), so the publish and purge rules above are
  an operator requirement, checked when the server is deployed (phase 2's CI job uses this exact config).

## What the selector and migration need from fabric-v2: option (c)

`StateBackend` writes are already async (`put`, `delete`, `writeBatch` return promises). The parts
that must stay synchronous are reads (`get`, `list`, `stateToken`), `withWriteFence` (it runs
under `.lock` and cannot await), and the `prepare(view)` and conflict callbacks inside one
`writeBatch` snapshot.

- **(a) An async interface: no.** Every reader, the R20 fence and the residency commit paths
  would change, for file and sqlite too. Also, per-key KV puts are not atomic across keys, and
  they cost one fsync each. The bench measured 3-key batches at 271/257/280 batches/s (1/8/32
  writers) against 766–846/s for one SQLite FULL transaction. At 32 writers, p50 is 113 ms
  against 41 ms. The ADR-50 atomic batch does not help: 236–250/s.
- **(b) A local SQLite cache that syncs to KV later: no.** An acked write is then only local. Two
  hosts can ack conflicting batches, and a host that dies before it syncs loses acked writes.
  This breaks the guarantee in the last section.
- **(c) One JetStream message per batch, applied by a projector: yes.** One message costs one
  fsync, the same as SQLite FULL: 792/815/813 publishes/s against 853/809/809 single puts on
  `sqlite1c-FULL`, all at the 836/s disk floor. The projector applies the stream into a local
  read model in the `state.db` schema. Reads stay synchronous on that model. `prepare(view)`
  runs on the model at stream sequence S. The batch is published with
  `Nats-Expected-Last-Sequence: S` (optimistic concurrency for the whole batch) and a
  `Nats-Msg-Id` (dedupe of a retry after a lost ack). A sequence mismatch means another writer
  committed: catch up, re-run `prepare`, retry (the R11 re-read loop). The write promise
  resolves after the ack AND after the local projector has applied that sequence (read your own
  writes). The stream's sequence is the total order that the migration fence needs.

The cost is that durable NATS is ~800 batches/s on a consumer NVMe. Today's sqlite backend runs
at `synchronous=NORMAL` and gets 35,916 3-key batches/s (`sqlite1c-NORMAL`, 1 writer). Group
commit is the only way to recover throughput; neither backend has it today.

## Migration: file | sqlite -> nats, and rollback

This reuses `backend-migration.ts`. It holds the same fence (`custody.lock`, then `.lock`, for the
whole section), the same advisory census, the same `meshSnapshotDigest` verification both ways,
and the same order: roll forward puts the marker BEFORE the flag; roll back puts the flag BEFORE
the marker is removed. The new parts are a `nats` value for `MeshBackendFlag` and the
`MeshStateMovedMarker`, and the stream records below. Phases:

1. **Fence the source.** From `file`, hold `.lock`; file-mode writers serialize on it. From
   `sqlite`, run rollback steps 1–2: `exporting` at E+1, so `state-sqlite.ts` writers fail closed.
2. **Import.** Publish `import-begin{epoch E+1, digest}` to an empty stream (expected last
   sequence 0, so a second migrator fails). Then publish the snapshot in chunks that fit
   `max_payload` (the state cap is 32 MiB). While the stream head is `importing`, nats writers
   refuse. The migrator projects the stream and checks the digest against the source snapshot
   (as the `import-verify` step). It then re-reads the source and compares it with G0, as
   cutover does today.
3. **Commit.** Install the NATS moved marker (epoch E+1) over `state.json`. It is the SQLite
   marker's exact shape plus `natsStream`, so every release since pi-fabric#627 (main c58cec72)
   refuses it on its tolerant read as well (a new `format` would read as damage there, so as an
   EMPTY mesh). Releases before c58cec72 know no marker, the same gap the SQLite cutover accepts. Then
   publish `import-commit{E+1}` with the expected last sequence. Then set the local flag to
   `nats`. Every crash point reruns from the stream head plus the marker, as the `importing`
   rerun does now.
4. **Rollback (nats -> sqlite or file).** Publish `retire{E+2}` with the expected last sequence.
   Every later nats publish then fails its expected-sequence check, and projectors that see
   `retire` fail closed (the `MeshStateRetiredError` of NATS). Export the model at the
   `retire` sequence through the existing import (into `state.db`) or export (into
   `state.json.rollback-<E>.tmp`) path. Verify the digest, switch the local flag, and replace
   the marker. **Abort** publishes `unretire{E+2}`, as `abortMeshRollback` does.

**Built (smarty-dev#9670, `backend-migration-nats.ts`): file <-> nats.** `importMeshStateToNats`
and `rollbackMeshStateFromNats` run steps 2-4 for a file root (no `state.db`, epoch 0, so E+1 = 1
and `retire` is epoch 2). A sqlite root refuses: roll it back to file first (`rollbackMeshState`),
then import. The details:

- **Per-switch stream (smarty-dev#9690):** each import uses a new stream
  `natsStreamName(root)_<switch id>`. Under the fence the import first writes it to
  `state.json.nats-intent` (0600, exclusive, read back), then creates it. A rerun takes it from the
  intent, or after the marker from the marker. The commit removes the intent. A `beforeCommit` hook
  (the reader-readiness gate) runs before the marker and before `import-commit`; a throw stops that step.
- `import-begin` also records the source digest, `readGeneration` and the switch id.
- The chunks are deterministic batch records: entries in key order, then tombstones in eviction
  order, at most 512 KiB of changes each, with fixed message ids.
- The source is kept at `state.json.nats-cutover-<E+1>` before the marker goes in.
- **Rerun without the marker:** the same source resumes. Stored chunks must match byte for byte.
  A changed source refuses. An import never purges, and the stream name is derived from the root (pi-fabric#937).
  A leftover of a failed import (still importing, no marker) may be deleted on the server, after checking that no
  Fabric process serves the root from NATS. A stream with committed state refuses the import and is never deleted.
- **Rerun with the marker:** the stream is checked against the kept copy, then committed.
- **Rollback:** it retires the stream and exports the projection through the `state.json`
  encoder into `state.json.nats-rollback-<E+2>-<uuid>.tmp`. It reads the temp back, checks the digest
  and renames it over the marker. A rerun redoes the export, or only verifies after the rename.
  Both normal completion and a converged rerun remove this rolled-back switch's own intent, so a new import chooses
  a new stream. An intent naming another staged switch is left intact.
  The CLI's `rollback --stream S` stays validated against the root (`natsStreamName(root)` or its per-switch
  `natsStreamName(root)_<switch id>`) and is mapped to the migration's switch id: a per-switch name selects its id, the
  fixed-root name selects no id (the earlier layout). With a marker present, an `S` that is not the marker's stream
  refuses before anything is recorded; without one (a finished rollback's rerun) `S` picks the retired stream to verify.
- **Opening a root on NATS (decision B):** `openNatsStateBackend` opens the stream its valid own-root NATS marker
  names, never creating it. A root with no file state at all (no `state.json*`, no `state.db*`, no intent and no kept
  copy; an absent root too) bootstraps its fixed-root stream `natsStreamName(root)` through the normal create,
  replicas and readiness path, as before. Everything else refuses and nothing is bootstrapped: live file state, a staged
  import (`state.json.nats-intent`), leftovers without a marker, a partial, truncated or malformed marker, a marker for
  another root's stream, and any unreadable entry (its own error, e.g. EACCES). The decision is rechecked after the
  stream is opened (same kind, stream and root `dev:ino`); recheck and registration are lock-free, with no await
  between them. Opening a root takes neither `custody.lock` nor the mesh `.lock`; migration tools retain their fence.
  An absent root is never created. An observed change closes the backend and refuses, with nothing registered.
  This recheck is not a durable file-side fence: a concurrent or later file writer can still race the fresh-root
  bootstrap (known inherited gap smarty-dev#13524). Do not run a file writer and a NATS backend on the same empty
  root until that durable fence is implemented.
- **Fixed-root preflight:** before a new import writes its intent or a stream, the root's fixed-root stream is
  checked: if it exists and is not retired (live state of a bootstrapped root), the import refuses and never forks it.
  An absent or retired fixed-root stream lets the new switch begin.
- **Files:** every file is created with O_CREAT|O_EXCL|O_NOFOLLOW at 0600 and checked as our own
  regular file. An existing kept copy must be ours and hold the source's digest, else the import refuses.
- **Limits (`LIMIT: FABRIC_MESH_NATS_MIGRATION_LIMIT`):** `maxReplay` stream messages,
  `maxStateBytes` of projected state and of the export, and `maxStateTombstones` (bounded as the
  backend bounds them), and `maxPayloadBytes` per import chunk (default 1 MiB, the server default),
  checked before `import-begin`. A rollback counts its own `retire` record against `maxReplay`. A
  limit refuses before any retire, rename or commit.
- **CLI and the reader gate (smarty-dev#9690):**
  - `fabric-mesh-backend import --to nats --nats-server URL --accept-old-release-gap` stages S, then runs the #7815
    gate under the fence before the marker and before `import-commit`.
  - Each required reader proves `nats` with `reader-proof --backend nats --stream S`: a real read of the staged S on
    the proof-only path. The proof binds the switch id, S, its creation time, the head, the full digest and the
    target (`cluster-or-server/domain/account`, the account from `$SYS.REQ.USER.INFO`).
  - The preflight refuses only readers no proof can fix. A stopped projector deletes its consumer at once.
  - `--abandon` deletes exactly the intent's unfinished S and the intent. A present state.json must be readable,
    unambiguous file state: malformed markers, truncated/invalid state and unreadable state refuse before any stream
    inspection or deletion. An absent state.json is allowed for a crash before the source read.
  - The gate checks the binding even when no reader is required: S must be this root's stream
    (`isNatsStreamForRoot`), the server must report the target account, and S's `import-begin` must carry the
    challenge nonce (an import begun without one is abandoned and redone).
  - A rerun after the marker reopens the gate: before `import-commit` it checks again; a finished commit only
    converges (verified, no commit, no check). A malformed NATS marker refuses `import --to nats`, `--abandon` and `rollback`.
  - `rollback` of a NATS root is ungated and recorded. Its `--stream S` (only to verify a finished rollback, once
    the marker is gone) must be this root's stream and needs `--nats-server`; any other stream is a usage error (exit 2).
  - `--stream` is accepted only by `reader-proof --backend nats` and `rollback`, always root-bound.
  - A marked root opens only the stream its marker names (`create: false`); reads refuse while S is importing.
- **Deployment gate (pi-fabric#907):** before a fleet root moves, the install's nats-server ACLs let only
  credentialed fleet hosts publish to and purge or delete the state streams, and allow `$SYS.REQ.USER.INFO`.
  Owner: fabric-store.
- **Not built:** the sqlite-direct path and `unretire`.

The local locks fence only this host. Other hosts are fenced by the stream itself, because every
nats write carries an expected sequence. Each host's `.lock` and the census still stop its
file/sqlite writers. The #708 KV lease can carry `custody` across hosts. The migration then takes
that lease first, in the place of `custody.lock`.

## Durability and failure modes

| | file | sqlite (`synchronous=NORMAL`) | nats (`sync_interval: always`, R3) |
|---|---|---|---|
| acked write survives a process crash | yes (rename) | yes (WAL) | yes |
| survives a power loss / kernel crash | no: rename, no fsync | no for the latest commits; `await sync()` gives yes; migration flags run at FULL | yes on each replica in the quorum (one fsync per message) |
| atomic multi-key | yes (one file) | yes (one transaction) | yes with (c); no with per-key KV |
| leader loss | n/a | n/a | Raft elects a replica that holds every acked message. In-flight messages may appear without an ack (the bench saw 11,504 stored against 11,497 acked); `Nats-Msg-Id` makes the retry a no-op |
| partition | n/a (one host; NFS refused, R16) | n/a | the minority cannot ack: writes time out as `MeshStateBusy` (retryable); reads serve the last applied projection, flagged stale when the #708 lease lapses. The majority continues |

The bench (Intel 1, `b7504-nvme` @bf416807, `bench/jetstream-nvme/RESULTS.md`) supports the
following claims. With `always`, kill -9 lost 0 acked writes in 3 of 3 rounds (SQLite FULL also
lost 0). Throughput tracks one flush per ack.

Not tested:
- A power cut. kill -9 keeps the page cache, so the default-sync control also lost 0.
- R3 clusters, leader failover and partitions. The R3 rows above come from the NATS
  design, not from the bench.

The default `sync_interval` (2 min) is 30–260x faster, but an acked write then survives only a
process crash. The nats factory must refuse a server that reports `sync_always: false` in
`/varz`, unless the operator explicitly accepts that durability.

## Network prerequisite for a multi-host cluster

On our tailnet, lane hosts reach each other only on ICMP and TCP 22. The block is the tailnet
ACL, not the host firewalls (smarty-dev#7504 c6086641424). The 3-host bench never clustered for
this reason. An R3 cluster needs these TCP ports opened on `tailscale0` only:

| port | purpose | from | to |
|---|---|---|---|
| 6222 | cluster routes (replication, Raft) | each cluster node | each other cluster node |
| 4222 | clients (Mains, residents, bridges) | every fleet host that runs Fabric | each cluster node |
| 8222 | HTTP monitoring (`/varz`, `/jsz`) | the fleet host | each cluster node |

That ACL change needs the product owner's yes (smarty-dev#7504 c6095415747). Until then, tests run
on one host, or across hosts through `ssh -L` tunnels.
