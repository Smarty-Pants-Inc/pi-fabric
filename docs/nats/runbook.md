# Bring-up, health, backup, replacement and rollback

**Proposal only.** Paul/hosts-lead approve topology, credential item list, failure
domains and migration fence. Host owners perform installs. This U2 carrier does
not implement a Fabric backend or cut over the five bridge transports.

## Before bring-up

Record the exact five bridge **unit/child IDs**, owner, host, release,
known-host/forced-command record, cursor path and previous Fabric config. Local
inventory available to this task proves four work-host targets; the fifth is an
owner reconciliation gate (see [topology](topology.md)). Do not derive unit names
from host names or disable by glob. Capture SQLite consistent backups/checkpoints,
root/event sequences, command receipts and unknown outcomes. Do not share live
SQLite files between hosts or let both backends write authority.

Require independent disks/failure domains for Ryzen 1/2/4, 40 GiB free per core,
correct time, Tailscale-only 6222/7422 reachability, local-only 4222/8222,
TLS/custody/revocation gate and reviewed native hostd admission. No tailnet policy
or firewall change is authorized by this document. Pin an official nats-server
**2.14.7 or later**, with the exact tested release selected before rollout.

### Public release verification (local example, no install)

```sh
mkdir -p "$TMPDIR/nats-release"
cd "$TMPDIR/nats-release"
# Exact public official release assets; SHA256SUMS is the asset name.
curl -fSL --max-time 90 -o SHA256SUMS \
  https://github.com/nats-io/nats-server/releases/download/v2.14.7/SHA256SUMS
curl -fSL --max-time 90 -o nats-server-v2.14.7-linux-amd64.tar.gz \
  https://github.com/nats-io/nats-server/releases/download/v2.14.7/nats-server-v2.14.7-linux-amd64.tar.gz
sha256sum --check --ignore-missing SHA256SUMS
# Require the archive's explicit OK line; this is integrity via HTTPS, not a
# separately authenticated signature on the manifest.
tar -xzf nats-server-v2.14.7-linux-amd64.tar.gz
./nats-server-v2.14.7-linux-amd64/nats-server -v
```

Retain the **last fetched** official `SHA256SUMS`, archive and `sha256-check.txt` in
private evidence. Choose the correct platform/architecture asset for a later
native install; do not run Linux amd64 on m4/m5/i9. No fleet installation here.

## Bring-up order

1. Keep SQLite + all recorded mesh bridges authoritative. Stop application NATS
   writers/importers; bring-up is shadow transport only, not dual-write authority.
2. Deliver only each approved credential/runtime file through the existing owner
   route. Re-render native TLS/store paths, select one config per host, parse with
   `nats-server -t`. Confirm actual ACLs and stores. Do not use test certificates.
3. Admit/start `nats-core` on Ryzen 1, then Ryzen 2, then Ryzen 4. A single core may
   initially report unhealthy JS; proceed only to complete the intended quorum,
   not to accept traffic early. Wait until all three are known/current and share
   one metadata leader. Application control plane stays on Ryzen 1 irrespective
   of Raft leader.
4. Using owner-only `fabric.ops`, create the exact R3 definitions in
   `configs/streams.json` and approved pull-only explicit-ACK R3 durable consumers.
   Example reviewed CLI (operator-provided, version-pinned; not installed here):
   `nats --context fabric-ops --js-domain fleet stream add ROOT_ryzen3 --config <root-json>`.
   Split the manifest into one config per stream; audit every stream subject and
   replica count. KV streams are the `KV_STATE_H` backing streams with ten history
   revisions; SDK contexts must select domain `fleet` and H-prefixed inboxes.
   Reject unexpected pre-existing streams/subjects; do not overwrite to make a
   check pass. Consumers cannot have arbitrary push delivery subjects.
5. Admit leafs one at a time; each must have exactly one outgoing FABRIC link with
   all three failover URLs. Verify own-host state/event roundtrip, shared work
   topics and addressed hub delivery; deny other roots/KV, consumer creation,
   shared-host impersonation, hub control and system subjects.
6. U1/U3 owners prove adapter conformance, durable command ACK/dedupe, CAS/lease and
   owner binding, outage behavior, inventory, migration export/import and fence.
   Freeze old writes, drain/reconcile commands, commit one authority epoch/cursor,
   import and compare counts/hashes/version metadata, then select NATS using the
   backend's reviewed configuration. Do **not** invent a `backend=nats` setting
   or enable it before it exists. Only then stop the five individually recorded
   bridge transports; retain their files and previous releases for rollback.

## Health: observe data, not just the PID

Use approved contexts with separate system/ops certs; do not pass key values in
argv. Loopback HTTP is read-only but includes inventory: keep output private.

```sh
nats --context fabric-system server check connection
nats --context fabric-system server check jetstream
nats --context fabric-system server report jetstream
curl --fail --silent http://127.0.0.1:8222/healthz
curl --fail --silent 'http://127.0.0.1:8222/jsz?accounts=true&streams=true&consumers=true'
curl --fail --silent http://127.0.0.1:8222/routez
curl --fail --silent http://127.0.0.1:8222/leafz
nats --context fabric-ops --js-domain fleet stream info ROOT_ryzen3 --json
nats --context fabric-ops --js-domain fleet stream info KV_STATE_ryzen3 --json
nats --context fabric-ops --js-domain fleet consumer info ROOT_ryzen3 <durable> --json
```

`nats` CLI availability/version/command acceptance remains an operator preflight;
this task uses a stdlib protocol client and real server endpoints, not a claimed
CLI installation. Require `/healthz` status ok, metadata leader nonempty and
consistent across cores; two current metadata followers in steady state, each
R3 stream/KV/consumer one leader + two current followers (lag zero after quiesce).
Check core route topology, one leaf link, no leaf JetStream cluster/store,
confirmed published sequence/get and committed consumer ACK. Under one node
loss require new leader + one live current follower, R3 still configured, leaf
roundtrip and an acknowledged **metadata mutation**. Do not downgrade to R1/R2 to
make health green. Two failed cores halt writes, never automatic SQLite fallback.

Alarm on no meta/data leader, replica lag/current loss, leaf reconnect loop,
fsync/storage errors, payload/quota rejection, pending ACK growth, unknown
outcomes, TLS expiry or hostd crash-loop. Run through existing factory health
schedule, no new timer. SLO/latency and sustained fleet load need a separate real
Tailscale/fsync acceptance receipt; loopback does not set that SLO.

## Backups: stream snapshots, then isolated restore

Replication is not backup. Owner snapshots every root/KV/FLEET stream at least
hourly initially; retain 24 hourly + 7 daily copies (proposed capacity/RPO gate).
A file-backed stream snapshot is the supported interface, not a live copy of
`jetstream/`. Consumer definitions/ACK state are needed too:

```sh
# Protected operator backup directory, not the repository. Include consumers
# (do not pass --no-consumers). CLI defaults/options checked for pinned version.
nats --context fabric-ops --js-domain fleet stream backup ROOT_ryzen3 <private-dir>/ROOT_ryzen3
nats --context fabric-ops --js-domain fleet stream backup KV_STATE_ryzen3 <private-dir>/KV_STATE_ryzen3
nats --context fabric-ops --js-domain fleet stream backup FLEET <private-dir>/FLEET
# Repeat for EVERY stream in the reviewed manifest.
```

Record release/config hashes, stream configs/sequences, consumer definitions and
ACK floors, adapter authority epoch/CAS metadata, completion/dedupe records,
UTC/RPO and checksums in a manifest. Coordinate a Fabric writer fence for a
cross-stream consistent recovery point; sequential snapshots of a live account
are not atomic. Store snapshots encrypted/access-controlled off-node with the
existing backup owner; do not put snapshots or keys in Git/TASK_OUT smoke logs.
No backup credentials/resource are created here.

Restore drills run on an isolated **3-node** test cluster with a distinct domain,
no fleet routes/leaves or application writers. Use the pinned CLI's
`stream restore <backup-directory>` with that isolated context/domain. Verify
stream/KV subjects, configured R3/current followers, sequence/hash equality,
consumer ACK floors and adapter epoch, then replay only known-unconsumed work.
Never connect a restore clone to the production cluster, reset dedupe, resurrect
an acknowledged command or replay an unknown side effect. Neither real snapshot
restore nor cross-host recovery is claimed by this local smoke.

## One-core loss and node replacement

1. Freeze maintenance on the two survivors. Identify the failed core by server
   name/ID, confirmed host and release; verify surviving meta/data quorum and
   bounded leaf operations. No second restart until the replacement is current.
2. For a temporary outage, restart the **same** node/store with the reviewed
   binary/config; let Raft catch up. Do not erase a recoverable store or remove a
   peer simply because it is slow.
3. For permanent replacement, fence the old host/node from routes and writers
   first, preserve its disk privately, and approve a replacement route/transport
   identity and tailnet endpoint. Replace/revoke affected certificates through
   the custodian. Never reuse the old private key or run duplicate server names.
4. With live meta quorum, use the pinned CLI's **cluster peer-removal** operation
   against the exact failed server name/ID, after reviewing stream placement.
   Confirm `nats server cluster peer-remove --help`, its actual target syntax and
   cluster-administration account authorization in the owner's preflight; no
   removal command was tested here. The normal `fabric.sys` ACL is monitoring
   only, not a standing write-admin grant. Do not widen it to repair an auth
   failure; the owner must approve any maintenance-only permission/item first.
   This permanent Raft peer-removal action is not transient partition repair.
   If the pinned CLI cannot resolve/authorize it, stop and use the reviewed owner
   route; do not delete metadata manually.
5. Admit an empty local store on the approved new core, with intended cluster
   name/routes and unique replacement name. Update source configs/placement
   intentionally; wait for metadata discovery and R3 stream/consumer replica
   replacement/catch-up, checking every manifest stream. Raft placement may need
   an owner-directed stream update after peer removal; inspect, do not assume.
6. Require leader + two current followers, read/write/ACK and native hostd restart
   checks before unfreezing maintenance. Old disk is never copied live into a
   different peer. With two cores lost, stop; quorum disaster recovery is isolated
   restore plus a new fenced authority epoch, not force-accepting a minority.

## Rollback to SQLite (owner-fenced, not an availability fallback)

The last pre-NATS SQLite snapshot is stale after NATS accepts writes. **Do not**
simply select the old SQLite database. U1/U3 must deliver and test the inverse
migration/export, including versions/CAS, dedupe and command receipts, before any
production cutover. This U2 rollback contract is not that implementation.

1. Stop new agent/control writes via the reviewed maintenance fence. Quiesce all
   NATS producers/consumers, reconcile in-flight/unknown commands and persist the
   final authority epoch/cursors/ACK floors. Preserve NATS snapshots and stores.
2. Export the authoritative NATS state/events through the reviewed migrator into
   **new** SQLite files under private paths. Use the old snapshot only if it is
   proved no NATS-authoritative write ever occurred. Compare hashes, counts,
   versions, lease/ownership and dedupe/completion state; unknown effects remain
   unknown, not eligible for replay.
3. With exactly one writer set, select the reviewed SQLite backend/release and
   new files atomically; enable only the individually recorded five bridge
   services/children with their retained cursor/known-host/forced-command records.
   Verify addressed delivery, native owner binding, no duplicate events,
   command receipt reconciliation and SQLite consistent checkpoint/backups.
4. Disable/remove only the admitted NATS children through the hostd route after
   the adapter no longer uses them. Retain immutable NATS snapshots/store and
   receipts for the agreed rollback window. Confirm no NATS writer survives and
   no split-authority SQLite fork exists before leaving maintenance.

If NATS lacks quorum, no authoritative export or safe reconciliation is possible:
keep writers fenced, recover quorum/isolated backup first, then rollback. A
partition must never auto-fail open to SQLite. Owner accepts any snapshot-derived
RPO loss explicitly; do not silently discard acknowledged state.
