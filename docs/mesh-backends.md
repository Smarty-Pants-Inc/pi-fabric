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
| `nats` | throws `MeshStateBackendNotBuiltError` (`FABRIC_MESH_STATE_BACKEND_NOT_BUILT`) | none |

`nats` never falls back to `file`. A host that wrote local state while its peers wrote the
shared stream would split the mesh. fabric-v2 plugs in by setting `STATE_BACKEND_FACTORIES.nats`
to `{ unavailable?, create }`, where `create` returns a `StateBackend`. Its `unavailable` must
also refuse (throw), never return a reason, for the same reason.

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
3. **Commit.** Install the moved marker (`backend: "nats"`, epoch E+1) over `state.json`. Then
   publish `import-commit{E+1}` with the expected last sequence. Then set the local flag to
   `nats`. Every crash point reruns from the stream head plus the marker, as the `importing`
   rerun does now.
4. **Rollback (nats -> sqlite or file).** Publish `retire{E+2}` with the expected last sequence.
   Every later nats publish then fails its expected-sequence check, and projectors that see
   `retire` fail closed (the `MeshStateRetiredError` of NATS). Export the model at the
   `retire` sequence through the existing import (into `state.db`) or export (into
   `state.json.rollback-<E>.tmp`) path. Verify the digest, switch the local flag, and replace
   the marker. **Abort** publishes `unretire{E+2}`, as `abortMeshRollback` does.

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
