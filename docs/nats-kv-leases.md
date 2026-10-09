# U2: NATS JetStream KV host-lease prototype

**Scope.** An isolated async lease adapter, not a runtime migration. `HostLeaseStore` carries the existing `FabricHostLease` payload from `host-leases.ts` (including session/reload/writer metadata). The file backend has synchronous functions, not an existing pluggable interface; `ParticipantDirectory` still uses those functions unchanged. This prototype supplies the equivalent operations plus explicit ownership handles. Refs smarty-dev#7313, #6477; decision source: https://github.com/Smarty-Pants-Inc/smarty-dev/issues/6477#issuecomment-6076003313 (assignment-provided research).

## API mapping and ownership

| Operation | JetStream KV operation |
| --- | --- |
| acquire | Leader-read current value; fail if live; CAS-delete an expired value; `kv.create` (expected absent/tombstone revision). One winner. |
| renew | Validate identity and live old deadline; fresh leader check; `kv.update(key, value, expectedRevision)`; return the acknowledged new revision. |
| release | Validate identity/revision; `kv.delete(key, {previousSeq: revision})`; a stale close cannot remove a successor. |
| read / list | Leader API reads (`allow_direct: false`), decode and exclude expired/deleted entries. List is per-key, not an atomic snapshot. |
| acquireWaiting | Subscribe before the first acquire; retry only on watch hints or a one-shot known-expiry timer. Bound the wait with a monotonic deadline and abort signal. No periodic lease polling. |

Keys are SHA256(host id). Owner identity is `(id, rootId, identityId, startedAt, incarnation UUID)`. The UUID distinguishes equal-millisecond restarts; every successful mutation changes the **KV revision fencing token**. Watches only wake a retry: they never grant ownership. A future runtime must carry the token to protected resource operations and reject stale revisions there; a lease alone cannot stop a partitioned process.

## TTL and durability choice

Use **stream `max_age` + explicit per-record `expiresAt`**, not per-message PUT TTL. `max_age = maxLeaseMs`, history=1, R3, file storage. Shorter per-key logical TTLs are checked on every authority read and renewal; stale retirement is CAS-fenced. `markerTTL = maxLeaseMs` enables server expiry/delete-marker notifications (`allow_msg_ttl`), but those marker TTLs are not the lease's logical TTL. A one-shot expiry wake handles the shorter TTL. Reject future-dated publications and TTLs exceeding max_age.

Require **nats-server 2.14.7+** and **`sync_interval: always` on all three servers**. Open rejects incompatible existing buckets and mirrors/sources. The client cannot verify server disk-sync configuration; the harness records the actual configs. Production R3 means **three physical hosts**. This test intentionally uses three isolated loopback processes on **one Ryzen 2 host**: it does not prove host/power-loss resilience, clock-skew safety, or production latency.

## Failure contract and evidence

CAS resolves successor-between-check/write and close/successor races. A crash before/after publish leaves no process/file lock; an unacknowledged committed lease blocks only until TTL. On a timeout or uncertain acknowledgement, return no new ownership: stop protected work (at most retain the old token/deadline), and retry through a new watcher. Renewal acknowledgements after the prior deadline fail closed. No atomic multi-key lease transaction is offered. Never recreate or restore the bucket to a lower sequence under live tokens; fencing-epoch recovery is not prototyped. Logical expiry assumes disciplined clocks; cross-host skew/backward jumps and server-vs-client TTL timing need explicit margins and downstream fencing before runtime integration.

Conformance covers all five #695 race scenarios, lost acquire/renew acknowledgements, expiry, abort, identity validation and racing acquisition. Fault injection pins a renewal between leader read and CAS, kills the actual KV leader, and sweeps every acknowledged authority interval (including the old deadline after failure). `fault.json` must report `maxOverlappingOwners: 0`; failure triggers the approved **etcd fallback for leases only**, not a messaging rollback. Latency reports 200 sequential end-to-end acquire/renew samples after 20 warmups, p50/p99, with always-sync. NATS.js v3's official `@nats-io/transport-node`, `@nats-io/jetstream`, and `@nats-io/kv` are new branch-only dependencies.
