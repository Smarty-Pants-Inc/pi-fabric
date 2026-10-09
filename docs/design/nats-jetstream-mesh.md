# Fabric mesh on NATS JetStream: adapter design and first bench

Refs smarty-dev#7504 (owner fabric-store). Design only; gate lane fs-7504a, 2026-10-09, `main` 41a73f85.

Inputs: `src/mesh/` state-backend, state-sqlite, state-file, event-log, bridge, backend-fence,
backend-migration, mesh-backend-cli; the unlanded sibling lane `l7504-selector` (8bcd4f27: a
factory registry with a refusing `nats` slot; efc72fce: `docs/mesh-backends.md`). The parked
branch `fix/sd-6477-lockless` (edebc777) is not on `origin` or in any clone on this host, so this
note maps the seam on `main` (`StateBackend`, `state-backend.ts:191`). Re-check before the PR.

## 1. Bench (NATS 2.14.7, R3, `sync_interval: always`)

**Host.** `qs32729` (ryzen2 lane host), KVM guest, AMD Ryzen 9 7950X3D, 32 vCPU, 171 GiB, Ubuntu
26.04.1, kernel 7.0.0-38-generic. **Disk: `QEMU HARDDISK` 1.6 TB (SCSI `sd`), ext4 relatime, no
NVMe visible in the guest**: a virtual disk, not the target NVMe. `dd bs=4k oflag=dsync`: 6.2 ms
per synced write. All 3 nodes share it; load 7–9 and IO pressure ~3 % from other lanes.

**Cluster.** 3 `nats-server` v2.14.7 (release tarball, SHA-256 checked), listeners on 127.0.0.1
only, no auth, no monitor port, stores on ext4 under `~/.cache/<mktemp>`. Node 24.19.0, `nats`
2.29.3 in the temp dir; publishers spread over the 3 nodes, each awaiting its PubAck.

### Stream publish, R3 file stream (6 s per cell)

| Payload | Publishers | msg/s | p50 ms | p99 ms | Control: default sync (msg/s, p50, p99) |
|---|---|---|---|---|---|
| 256 B | 1 | 48 | 20.1 | 34.4 | 2563, 0.30, 3.2 |
| 256 B | 16 | 84 | 190 | 242 | 12330, 0.91, 5.8 |
| 256 B | 64 | 88 | 707 | 821 | 13400, 4.4, 11.8 |
| 4 KB | 1 | 47 | 19.9 | 38.4 | 1509, 0.46, 3.9 |
| 4 KB | 16 | 97 | 164 | 210 | 8513, 1.3, 10.3 |
| 4 KB | 64 | 113 | 540 | 661 | 11239, 4.8, 20.5 |

Errors 0 and stored = acked in every cell. The control column is the same cluster with the
default `sync_interval` (2 min), on fresh stores: it shows the cost is fsync, not NATS.

### KV (R3 bucket, history 1, 256 B values, 3 s per cell)

| Operation | Clients | ops | p50 ms | p99 ms |
|---|---|---|---|---|
| put | 1 / 16 | 146 / 283 | 19.8 / 179 | 34.5 / 240 |
| get, direct (any replica) | 1 / 16 | 15346 / 48021 | 0.18 / 0.64 | 0.65 / 3.3 |
| get, leader (`STREAM.MSG.GET`) | 1 / 16 | 12816 / 54299 | 0.22 / 0.77 | 0.63 / 3.7 |
| CAS `update(key, v, rev)` | 1 / 16 | 156 / 277 | 19.1 / 177 | 32.1 / 256 |
| CAS, 16 clients on 1 key | 16 | 135 wins, 13326 conflicts | 21.4 | 35.8 |

A leader read costs about as much as a direct read, so ownership reads can always use it.

### Leader kill under load (SIGKILL, 16 publishers + 6 lease contenders, 25 s run)

Publishers retry with the same `Nats-Msg-Id` until acked (dedupe window 2 min, timeout 2 s).
Lease: one KV key, TTL 3 s, renew every 500 ms, leader read then CAS on the read revision.

| Trial | Killed | Resume (first ack after kill) | Acked / stored / read back | Lost | Dup stored | Dup acks (deduped retries) | Client retries | Lease result |
|---|---|---|---|---|---|---|---|---|
| 1 | n3: stream + lease leader | 4.49 s | 2175 / 2175 / 2175 | 0 | 0 | 12 | 809 | holder 1 lost renewal; holder 2 took it 2.7 s after the old TTL ended; no overlap, no two grants on one revision |
| 2 | n2: stream leader | 7.56 s | 1540 / 1540 / 1540 | 0 | 0 | 11 | 1774 | lease leader not hit; 1 holder |
| 3 | n3: lease leader only | 0.03 s | 2088 / 2088 / 2088 | 0 | 0 | 0 | 0 | holder kept the lease through 11 failed renewals; no overlap |

Two holders: 0 in all trials (no two CAS wins on one revision, no overlapping hold windows).
Three trials are a smoke test, not the soak proof.

### Server features the design needs (`bench/nats-jetstream/features.mjs`, same R3 cluster)

| Feature (server version) | Result on 2.14.7 |
|---|---|
| `allow_atomic`, `allow_msg_ttl` stream config (2.12, 2.11) | accepted |
| Publish to `me.ev` fenced by `Nats-Expected-Last-Subject-Sequence-Subject: ms.lease` (2.11) | current revision: stored; stale revision: rejected `wrong last sequence: 3` |
| Per-message `Nats-TTL: 2s` (2.11) | message gone after 3.5 s |
| Atomic batch, 3 messages, `Nats-Batch-Id/Sequence/Commit` (2.12) | valid: one ack `count: 3`; one stale expectation in the batch: whole batch rejected, `last_seq` unchanged |

Finding: a route port in the ephemeral range (32768–60999) was taken by an outbound socket and a
restarted node failed `bind` twice. Production ports stay below 32768.

### Commands

```sh
B=$(mktemp -d -p "$HOME/.cache" fs7504a-bench.XXXXXX); cd "$B"   # disk, not /tmp (tmpfs here)
U=https://github.com/nats-io/nats-server/releases/download/v2.14.7; curl -sSLo SUMS $U/SHA256SUMS
curl -sSLo n.tgz $U/nats-server-v2.14.7-linux-amd64.tar.gz; sha256sum n.tgz; grep amd64.tar SUMS; tar xzf n.tgz
base=42420   # as run: client 42420-42422, routes 42423-42425 (prefer ports below 32768)
for i in 1 2 3; do cat > n$i.conf <<EOF
server_name: n$i, host: 127.0.0.1, port: $((base+i-1))
jetstream { store_dir: "$B/store$i", sync_interval: always, max_memory_store: 256MB, max_file_store: 20GB }
cluster { name: c7504, host: 127.0.0.1, port: $((base+2+i)),
  routes: [ nats-route://127.0.0.1:$((base+3)), nats-route://127.0.0.1:$((base+4)), nats-route://127.0.0.1:$((base+5)) ] }
EOF
smarty-reap run "$B/rec$i.json" -- "$B/nats-server-v2.14.7-linux-amd64/nats-server" -c "$B/n$i.conf" -l "$B/n$i.log"
done
echo '{"type":"module"}' > package.json; npm i --no-audit --no-fund nats@2; cp <pi-fabric>/bench/nats-jetstream/*.mjs .
node bench.mjs $base stream; node bench.mjs $base kv; node features.mjs $base
PIDS='{"n1":<pid>,"n2":<pid>,"n3":<pid>}' node bench.mjs $base fail   # kill stream leader; restart
PIDS='{...}' KILL=lease node bench.mjs $base fail                    # kill lease leader
dd if=/dev/zero of="$B/probe" bs=4k count=300 oflag=dsync   # raw fsync; control: no sync_interval, DUR=4000
smarty-reap stop "$B/rec$i.json"; rm -rf -- "$B"
```

### NVMe rerun (lane fs-7504b, 2026-10-09)

Same binary (v2.14.7, SHA-256 checked), same scripts and cells, same `nats` 2.29.3 and Node
24.19.0. Only the host and disks change.

**(A) Single host: `intel1`, bare metal** (`systemd-detect-virt`: none), Intel Core i9-14900K,
32 threads, 184 GiB, Ubuntu 26.04.1, kernel 7.0.0-38-generic. Load 6–12 and IO pressure ~1–2 %
from other lanes during the run. Nodes spread over both NVMe devices:

| Node | Device | Model | fs | `dd bs=4k oflag=dsync` |
|---|---|---|---|---|
| n1, n3 | nvme0n1p2 (`/`) | Samsung SSD 990 EVO Plus 4TB (fw 2B2QKXG7) | ext4, relatime | 1.27 ms per write |
| n2 | nvme1n1 (pool `work`, `/srv/scratch`) | Samsung SSD 990 EVO Plus 4TB (fw 2B2QKXG7) | ZFS, sync=standard, lz4, recordsize 128K | 0.58 ms per write |

Listeners on 127.0.0.1 only (client 24620–24622, routes 24623–24625; control 24630–24635).
The first stream run timed out on stream create 4 s after start (meta leader just elected); the
rerun 3 s later is the one below.

**(B) 3 hosts: not run.** From `intel1` the names `ryzen4-agent`, `ryzen5-agent`,
`epyc1-agent` and `forge-agent` do not resolve, `~/.ssh` has no config and no key for them, and
ssh to their Tailscale addresses (100.105.145.68, 100.86.144.100, 100.100.180.46, 100.78.65.112)
fails `Host key verification failed`. Enabling it is new access (ask first), so no RTT and no
cross-host numbers yet; the 3 target hosts (CT4000P3) are still unmeasured.

#### Stream publish, R3 file stream (6 s per cell; control 4 s)

| Payload | Publishers | msg/s | p50 ms | p99 ms | Control: default sync (msg/s, p50, p99) |
|---|---|---|---|---|---|
| 256 B | 1 | 208 | 4.61 | 7.70 | 4602, 0.20, 0.32 |
| 256 B | 16 | 429 | 36.9 | 51.2 | 18766, 0.73, 1.87 |
| 256 B | 64 | 434 | 146 | 165 | 18000, 3.94, 8.23 |
| 4 KB | 1 | 205 | 4.67 | 7.94 | 3342, 0.28, 0.46 |
| 4 KB | 16 | 422 | 37.6 | 50.7 | 17865, 0.76, 2.36 |
| 4 KB | 64 | 422 | 150 | 174 | 19611, 3.30, 8.62 |

Errors 0 and stored = acked in every cell. About 4× the virtual disk, but the rate stops at
~430 msg/s from 16 publishers on: payload size does not matter, so the limit is the number of
synced writes, not bytes. The control column is the same hosts and
disks with the default `sync_interval`, on fresh stores.

#### KV (R3 bucket, history 1, 256 B values, 3 s per cell)

| Operation | Clients | ops | p50 ms | p99 ms |
|---|---|---|---|---|
| put | 1 / 16 | 925 / 1349 | 3.05 / 35.9 | 5.96 / 43.3 |
| get, direct (any replica) | 1 / 16 | 19374 / 39188 | 0.15 / 0.88 | 0.33 / 18.3 |
| get, leader (`STREAM.MSG.GET`) | 1 / 16 | 14350 / 57074 | 0.21 / 0.88 | 0.35 / 2.06 |
| CAS `update(key, v, rev)` | 1 / 16 | 942 / 1216 | 3.02 / 37.6 | 6.06 / 76.7 |
| CAS, 16 clients on 1 key | 16 | 657 wins, 15638 conflicts | 3.89 | 12.5 |

CAS errors 1 / 16 are each client's first update on revision 0 (a bench artifact, one per
client); no other errors.

#### Leader kill under load (SIGKILL, 16 publishers + 6 lease contenders, 25 s run)

| Trial | Killed | Resume (largest ack gap after kill) | Acked / stored / read back | Lost | Dup stored | Dup acks (deduped retries) | Client retries | Lease result |
|---|---|---|---|---|---|---|---|---|
| 1 | n1: stream leader | 5.85 s (first ack after kill: 0 ms, in flight) | 9849 / 9849 / 9849 | 0 | 0 | 12 | 1216 | lease leader (n2) not hit; 1 holder |
| 2 | n3: stream + lease leader | 7.82 s | 10899 / 10899 / 10899 | 0 | 0 | 14 | 1840 | holder 5 lost renewal; holder 1 took it 7.96 s after the old TTL ended; no overlap, no two grants on one revision |
| 3 | n1: lease leader only | 0.03 s (25 ms gap) | 13054 / 13054 / 13054 | 0 | 0 | 2 | 2 | holder 3 lost renewal; holder 5 took it 7.94 s after the old TTL ended; no overlap, no two grants on one revision |

Two holders: 0 in all trials. Trial 2's 7.82 s resume is above the 7.5 s limit of section 6
(one trial; the soak decides).

Commands: as above, with `B=$(mktemp -d -p "$HOME/.cache" ...)` for n1, n3 and a second
`mktemp -d` under `/srv/scratch/paul` for n2's `store_dir`; base 24620; control on base 24630
without `sync_interval`, `DUR=4000`; `dd ... count=1000 oflag=dsync` in each store's directory.

### 3-host NVMe (lane fs-7504c, 2026-10-09): blocked, cluster did not form

Orchestrated from Ryzen 1 over ssh only. One node per host, bare metal (`systemd-detect-virt`:
none), store in a `mktemp -d` under `~/.cache` on the root ext4 NVMe. Same v2.14.7 tarball on every
host (SHA-256 `e5c20b1c…b8be2`, matches `SHA256SUMS`). Listeners on the host's Tailscale IPv4
only (client 28604, route 28605, no monitor port, no auth), `sync_interval: always`, routes to the
other two Tailscale IPs.

| Node | Host | Tailscale IPv4 | CPU | Load (1 min) | Store device | Model | fs | `dd bs=4k count=2000 oflag=dsync` |
|---|---|---|---|---|---|---|---|---|
| n1 | ryzen4 | 100.105.145.68 | Ryzen 9 7950X3D | 25–29 | nvme0n1p2 (`/`) | Crucial P3 4TB (CT4000P3PSSD8) | ext4, noatime | 3.21 ms per write |
| n2 | ryzen5 | 100.86.144.100 | Ryzen 9 7950X3D | 22–30 | nvme1n1p2 (`/`) | Crucial P3 4TB (CT4000P3PSSD8) | ext4, relatime | 2.87 ms per write |
| n3 | epyc1 | 100.100.180.46 | EPYC 4545P | 0.6 | nvme0n1p2 (`/`) | Crucial P3 4TB (CT4000P3PSSD8) | ext4, relatime | 2.72 ms per write |

RTT over Tailscale (`ping -c 20`, min / avg / max ms): ryzen4–ryzen5 0.50 / 0.89 / 1.31;
ryzen4–epyc1 0.32 / 0.79 / 1.16; ryzen5–ryzen4 0.58 / 0.85 / 1.17; ryzen5–epyc1 0.55 / 0.92 /
1.17; epyc1–ryzen4 0.49 / 16.6 / 66.5 (rerun 0.46 / 9.95 / 50.4); epyc1–ryzen5 0.69 / 1.00 / 1.18.

**No bench numbers.** All 3 nodes started and listened on their Tailscale IPs, but no route
formed: every route dial failed `i/o timeout`. Between these hosts only ICMP and TCP 22 pass over
Tailscale; TCP 80, 28604 and 28605 time out (dropped, not refused) in all 6 directions. The drop
is in the tailnet policy or the host firewalls (neither is readable without root); opening a port
range is new access, so the stream, KV and leader-kill cells were not run. Also, the 3 hosts
cannot ssh to each other (names do not resolve), so leader kills would have to go from Ryzen 1.

Verdict: no throughput verdict versus the single-host ~430 msg/s / CAS p50 3 ms. The disks alone
predict lower: each CT4000P3 fsync costs 2.7–3.2 ms, 2–5× the 990 EVO Plus (0.58–1.27 ms), with
sub-ms RTT, so a 3-host R3 sync-always run would likely be disk-bound below intel1.

## 2. Shape: one stream for state and events, local projection for sync reads

`StateBackend` reads are synchronous (`state-backend.ts:173-199`); `writeBatch` is atomic over
many keys (`:94-131`); `withWriteFence` (R20, `:211-219`) keeps state commits out while an
event is admitted and appended. Per-key KV CAS alone cannot do the last two. So:

- **One R3 stream `FABRIC_MESH_E<epoch>`** (section 4; file, `sync_interval: always`, `allow_direct: true`,
  `allow_atomic: true`, `allow_msg_ttl: true`, `max_msgs_per_subject: 1`, duplicate window ≥
  the longest publisher retry). Subjects: `ms.<key>` for state (1 message per subject, so it
  behaves as a KV bucket; tombstones as `KV-Operation: DEL` markers), `me.<topic>.<event-uuid>`
  for events (one subject per event, so the per-subject limit does not trim the log; read a
  topic with the filter `me.<topic>.>`; retention by `Nats-TTL` per event, not `max_age`, which
  would also expire state), `mm.<name>` for metadata (authority, epoch). Keys contain `/` and
  `:` (`state-sqlite.ts:493`): encode each as one subject token (base32).
- **One process per host owns the connection** (Envoy: one recovery owner,
  [nats.go#L131-L166](https://github.com/sjawhar/legion/blob/ef352f154d91656145ead11758f9562cc9c97381/packages/envoy/internal/bus/nats.go#L131-L166)).
  It keeps an in-memory **projection** of `ms.>` fed by an ordered consumer from the last
  applied sequence, and serves the synchronous reads from it. Ready = the projection reached the
  `last_seq` read at connect (Envoy marks ready after a timed-out scan; do not copy that,
  [kvwatch.go#L233-L321](https://github.com/sjawhar/legion/blob/ef352f154d91656145ead11758f9562cc9c97381/packages/envoy/internal/kvwatch/kvwatch.go#L233-L321)).
- **Fencing token = the subject's last sequence (the KV revision).** Every write carries
  `Nats-Expected-Last-Subject-Sequence`; ownership decisions (lease take, renew, takeover,
  conditional release) first do a leader read (`$JS.API.STREAM.MSG.GET`, 0.22 ms p50 above),
  never a projection or direct read. A receiver of fenced work rejects a lower revision than the
  last one it saw (Envoy's role CAS does not do this; we must).

### StateBackend → JetStream

| Operation (`state-backend.ts`) | JetStream mapping |
|---|---|
| `get`, `list`, `listAll`, `listAllShared` (`:194-197`) | Projection read; same `localeCompare` order and clamps as today. `fresh: true` first waits for the projection to reach a leader-read `last_seq` (bounded). |
| `stateToken` (`:198`) | Immutable projection snapshot at applied sequence S. |
| `stateStamp`, `cachedStateStamp` (`:200-201`) | `nats:<stream-created-ts>:<epoch>:<S>`; the created timestamp detects a recreated stream (revisions restart). |
| `put({ifVersion})` (`:206`) | Publish `ms.<key>` with `Nats-Expected-Last-Subject-Sequence: ifVersion` (0 = must not exist), `Nats-Msg-Id` = op id. Version = PubAck seq. Resolve after PubAck **and** local apply of S. |
| `delete({ifVersion})` (`:207`) | Same, with a DEL marker; the highWater/ABA rule holds because sequences never go back. |
| `writeBatch` (`:208`) | Atomic batch publish (`Nats-Batch-Id/Sequence/Commit`, server ≥ 2.12): every write op carries its expected subject sequence; keys the prepare **read** but does not write go in as checks on the commit message via `Nats-Expected-Last-Subject-Sequence-Subject`. Abort-on-conflict = the server rejects the whole batch; for `skip` ops, re-read and resend the batch without the conflicting ones. `afterCommit` runs after local apply; `commitOutbox` rows become messages in the same batch. Fallback if those headers fail tests: `Nats-Expected-Last-Sequence` on the stream (global serialization, as SQLite `BEGIN IMMEDIATE` does today). |
| `confirmWritable` (`:209`) | `STREAM.INFO`: the epoch stream exists and is not sealed (section 4); no lock. |
| `withWriteFence` (`:211-219`) | The event publish itself carries the fence: `Nats-Expected-Last-Subject-Sequence-Subject: ms.<identity key>` with the revision the admission read. A state commit that lands in between makes the append fail, and admission reruns. No cross-host lock. |
| `dropCache`, `diagnostics`, `close` | Drop and rebuild projection; counters (applied seq, lag, redeliveries, leader); drain the connection. |

### Event log → JetStream

| Operation (`event-log.ts`) | JetStream mapping |
|---|---|
| `publish` (`:588`) | Publish `me.<topic>.<uuid>` with `Nats-TTL` = retention; `Nats-Msg-Id` = `dedupeKey` or the event UUID. Sequence = stream seq (gaps from `ms.` messages are allowed: the log already permits gaps, `:524`). `durable` is implied by `sync_interval: always`. |
| `publishBatch` (`:606`) | Sequential publishes with Msg-Ids: same committed-prefix contract as today. |
| `read`, `nextEventAfter` (`:686`, `:703`) | Projection of recent `me.>` plus, for older ranges, an ordered consumer from `opt_start_seq` with a subject filter. |
| `oldestSequence`, `latestSequence` | Stream `first_seq`, `last_seq` (leader `STREAM.INFO`). |
| `latestCursor`, `tail` (`:880`, `:934`) | Cursor v2 = `{gen: stream-created-ts, seq}`; the old byte-offset cursors decode to "start of live". |
| Receipts / dedupe (`:277-376`) | Msg-Id inside the duplicate window. Past the window the server cannot find an event by Msg-Id, so keyed receipts for long retries stay as small `mr.<hash>` subjects (event UUID + seq), written in the same atomic batch as the event. Envoy aligns `MaxAge` with its 72 h duplicate window ([stream.go#L15-L68](https://github.com/sjawhar/legion/blob/ef352f154d91656145ead11758f9562cc9c97381/packages/envoy/internal/bus/stream.go#L15-L68)). |
| Compaction, archive (`:1175`) | Per-event `Nats-TTL` plus a sourced `FABRIC_MESH_ARCHIVE` stream with long retention; the private compactor and live/archive dual write go away. |

## 3. Bridge v1's timed loop becomes a push consumer

Today `bridge.ts:1367-1419` waits 250 ms after each pass, retries busy at 100 ms→2 s and WAL-cap
at 1 s→30 s, and moves events between two independent stores over SSH stdio RPC. With one
replicated stream there is nothing to copy: every host reads and writes the same stream.

- Each host gets one durable push consumer `host-<host>` on `me.>`, filtered to what it
  delivers (today's bridged topics: control, ack, `fleet.work`, owner wakes, `bridge.ts:83-96`),
  explicit ack, `max_ack_pending` 256, `ack_wait` 60 s, `max_deliver` 20, `inactive_threshold` 7 d
  (Envoy's values, [durable.go#L17-L51](https://github.com/sjawhar/legion/blob/ef352f154d91656145ead11758f9562cc9c97381/packages/envoy/cmd/listener/durable.go#L17-L51)).
  The admin creates it; the host binds and never deletes it on drain
  ([durable.go#L113-L159](https://github.com/sjawhar/legion/blob/ef352f154d91656145ead11758f9562cc9c97381/packages/envoy/cmd/listener/durable.go#L113-L159)).
  Ack after local delivery; delayed NAK on a recoverable failure
  ([delivery.go#L60-L84](https://github.com/sjawhar/legion/blob/ef352f154d91656145ead11758f9562cc9c97381/packages/envoy/cmd/listener/delivery.go#L60-L84)).
- The cursor file (`bridge.ts:914-929`) becomes the consumer's ack floor; restart dedupe by
  bridged IDs becomes event UUIDs plus the Msg-Id window.
- Presence: `ms.presence.*` with `Nats-TTL`, renewed by the host process; expiry is enforced on
  read from the message time, not from a delete event
  ([registry.go#L53-L64](https://github.com/sjawhar/legion/blob/ef352f154d91656145ead11758f9562cc9c97381/packages/envoy/internal/session/registry.go#L53-L64)).
  Leases and reservations stay in non-TTL keys: an outage expires reachability, not ownership.
- The run loop, its poll, the lock retry and the WAL-cap backoff leave the hot path; this closes
  the R-no-polling exception for bridge v1 (pi-fabric#694, smarty-dev#7485). Bridge v1 stays as
  the rollback path until the soak passes.

## 4. Migration and rollback

Extend the existing fence. `fabric-mesh-backend` keeps its steps (`mesh-backend-cli.ts:11-32`;
writers stopped; `custody.lock` then `.lock`, `backend-migration.ts:14-18`), plus `--to nats`:

1. `import --to nats`: strict read of the current authority and the G0 cross-check, as today.
   The admin creates stream `FABRIC_MESH_E<E+1>` (the epoch is in the name; create fails if it
   exists), publishes the state as atomic batches, reads it back by leader reads, compares the
   digest, writes `state.json.cutover-<E+1>` and the moved marker `movedTo: nats` (a new value
   for `backend-fence.ts:86-120`).
2. `cutover`: put `mm.authority` = `{backend: nats, epoch: E+1}`, then flip the local flag last.
3. `rollback`: **seal** the epoch stream (`sealed: true`): from then the server refuses every
   write on every host, with no client cooperation. Export the projection at the sealed
   `last_seq` to `state.json.rollback-<E+2>.tmp`, fsync, decode check, flip the flag to file,
   rename over the marker last (today's order, `backend-migration.ts:913-1105`). Measure its
   time in the soak (acceptance). Bridge v1 restarts; post-cutover events go to a JSONL archive.

A host that cannot reach NATS refuses writes and never falls back to file silently (the sibling
selector already refuses, `l7504-selector` `state-backend.ts:359-383`).

## 5. Per-host credentials (design only; no credential created)

- Decentralized JWT auth: one operator, one account `FABRIC`, one user per host
  (`fabric-<host>`) and one provisioning user `fabric-admin`. Paul approves each; seeds live in
  the credential store per the smarty-credentials skill and reach a host as a 0600 file pointer,
  never an env value (Envoy prefers a seed-file pointer and refuses blank or invalid NKeys,
  [nkey.go#L12-L66](https://github.com/sjawhar/legion/blob/ef352f154d91656145ead11758f9562cc9c97381/packages/envoy/internal/bus/nkey.go#L12-L66)).
- Host users may publish `ms.>`, `me.>`, `mm.>` and call `$JS.API.STREAM.MSG.GET.FABRIC_MESH_*`
  and `$JS.API.CONSUMER.*.FABRIC_MESH_*.host-<host>`; no stream create, update or delete. Only
  `fabric-admin` reconciles streams and consumers (Envoy's `ConnectOwningStream` split,
  [nats.go#L288-L312](https://github.com/sjawhar/legion/blob/ef352f154d91656145ead11758f9562cc9c97381/packages/envoy/internal/bus/nats.go#L288-L312)).
- Routes: TLS per node over Tailscale, ports below 32768; clients on the Tailscale address and
  127.0.0.1 only. Revocation = account JWT update. A stolen seed can still write within its
  grants; the `updatedBy` identity on each record makes that visible, not impossible.

## 6. etcd fallback trigger

Move **leases only** to etcd v3 (lease + `Txn` on `mod_revision` as the token) if the soak or a
later run shows: two CAS wins on one revision or overlapping holds; an acked lease write missing
after recovery; a minority-side leader read that grants ownership; or, on the target NVMe, lease
CAS p99 > 1 s or failover resume > 7.5 s (half the 15 s lease max).

## 7. Fault-injection plan for the soak

#6477 harness (80 hub Mains, 15 resident hosts, 4 spokes) on 3 hosts, R3, with the counters of
`bench/nats-jetstream/bench.mjs` (acked vs stored vs read back by Msg-Id, duplicates, resume
time, grants per expected revision, hold overlap) and the issue's acceptance.

| Fault | How (user space first) | Pass |
|---|---|---|
| Leader loss | SIGKILL the mesh stream leader, then the meta leader, then a node that leads both; restart after 60 s | 0 lost, 0 dup stored, resume < 7.5 s, no split lease |
| Partition | Isolate one node's routes (nftables on the host, needs root and an ask), both a follower and the leader; heal after 2× election time | Minority leader steps down; minority writes and leader reads fail; no split lease |
| Disk stall | SIGSTOP the leader for 10 s (user space); then a real stall with a `dm-delay` or cgroup `io.max` limit on the store (root) | Same as leader loss; the stalled node rejoins and catches up |
| Client loss | Kill a lease holder mid-renew; drop its connection without close | Lease passes only after its TTL; receiver rejects the old revision |

## 8. Top risks

1. **fsync budget.** On the virtual disk, `sync_interval: always` gave 47–113 msg/s for the
   whole cluster, 19 ms CAS p50, and 0.5–0.7 s waits at 64 publishers. On bare-metal NVMe
   (`intel1`, 990 EVO Plus, 0.6–1.3 ms per dsync write, 3 nodes on one host) the budget is
   ~205 msg/s at 1 publisher and a flat **~430 msg/s at 16 and 64 publishers**, CAS p50 3.0 ms
   (1 client) / 37.6 ms (16 clients), put p50 3.1 / 35.9 ms; all writes share one stream.
   **Verdict: no. `sync_interval: always` R3 does not reach 1,000 msg/s at 16 publishers on
   NVMe (429 msg/s, 43 %).** The 3-host run (Tailscale RTT, CT4000P3) is still open. If the
   soak's commit rate exceeds ~430/s, the design does not hold with one stream and fsync on
   every write.
2. **Failover window.** Publishes stopped 4.5 s and 7.6 s after a stream-leader kill. Lease TTL
   and renewals must cover that, and callers must treat a timeout as unknown (12 and 11 retried
   publishes had landed and were deduped only by Msg-Id).
3. **Semantic gap.** Sync reads, atomic `writeBatch` and R20 rest on a local projection and on
   server features probed once, not under failover. Ownership decided from the projection, not a
   leader read, breaks fencing; the server caps atomic batch size.

## 9. Open questions for fabric-store

1. The soak's peak state-commit + event rate (`commit-stats.ts`)? It decides risk 1.
2. One stream for state and events (R20 by subject sequence) or two (own limits, other fence)?
3. Read-only keys of a `writeBatch` as `...-Subject` checks in the batch, or serialize all
   batches on `Nats-Expected-Last-Sequence`? Largest batch we send vs the server cap?
4. Receipts past the duplicate window: keep `mr.<hash>` subjects, or bound retries to the window?
5. Which 3 hosts, and does each expose its NVMe to the guest (ryzen2 does not)?
6. Does edebc777 change the seam this note maps (it was not reachable from this host)?
7. Should cutover keep bridge v1 as a shadow for one soak, or go straight to push consumers?
