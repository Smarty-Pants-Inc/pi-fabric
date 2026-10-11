# U2: NATS JetStream KV host-lease prototype

**Scope / HOLD.** Isolated async lease adapter, not a runtime migration. `HostLeaseStore` preserves `FabricHostLease` (session/reload/writer metadata included). `ParticipantDirectory` and synchronous file leases remain unchanged. No runtime selector is added or enabled. Refs smarty-dev#7313, #6477; assignment-provided decision: https://github.com/Smarty-Pants-Inc/smarty-dev/issues/6477#issuecomment-6076003313.

## API mapping and ownership

| Operation | JetStream operation |
| --- | --- |
| acquire | Leader-read; refuse live owner; CAS-delete a logically expired retained record; `kv.create(key, value, remainingTTL)` with absent/tombstone CAS. |
| renew | Validate identity + predecessor deadline; fresh leader read; public `js.publish($KV.bucket.key, value, {expect: {lastSubjectSequence: revision}, ttl: remainingTTL})`; return acknowledged new revision. NATS.js 3.4 `kv.update` has no PUT TTL option. |
| release | `kv.delete(key, {previousSeq: revision})`; stale close cannot remove successor. |
| read / list | Leader API (`allow_direct: false`), exclude expired/deleted entries; list is not an atomic multi-key snapshot. |
| acquireWaiting | Watch before acquire; retry only on KV events (PUT/DEL/PURGE, including server expiry markers); exactly **one bounded failure deadline per call**, never recurring or re-armed on watch events. Abort and watcher teardown are awaited. |

Keys are SHA256(host id). Owner identity is `(id, rootId, identityId, startedAt, incarnation UUID)`; equal-millisecond starts remain distinct. Every mutation changes the KV revision fencing token. Watches only wake a retry; they never grant ownership. A downstream resource must enforce persistent monotonic fencing on **every protected write**, including a restore/recreate epoch. The prototype does not wire that into Fabric.

## Expiry events, not polling

Every acquire **and renew** publishes per-message `Nats-TTL` equal to `ceil((expiresAt - Date.now()) / 1000)` seconds at publication. NATS stores message TTL in whole seconds: rounding **up** prevents a 5999ms TTL from being truncated to 5s and deleting an acknowledged lease early. The R3/file/history=1 bucket also has `max_age = maxLeaseMs` as a cap and `subject_delete_marker_ttl = maxLeaseMs`, with `allow_msg_ttl: true`. NATS server expiry produces a KV deletion marker even for a lease shorter than `max_age`; the existing KV watch wakes acquisition. `expiresAt` remains the authority-read/renewal guard. Server timing and disciplined client clocks remain deployment assumptions.

There is **no client logical-expiry timer**, recurring read timer, or polling fallback. Open rejects configurations lacking per-message TTL/delete-marker support. Thus no R-no-polling exception is used. The only acquisition timer is the whole-call bounded failure deadline. Real integration leases are >=5 seconds; sub-second tests use fake timers. Conformance independently observes the server delete marker for a 5-second lease in a 10-second bucket. Fake-timer tests prove events do not re-arm the deadline and elapsed time alone does not retry acquisition.

## Fail-closed durability admission

Require **nats-server 2.14.7+**, R3 file/history=1, leader reads and **`sync_interval: always` on every eligible R3 member**. R3 and acknowledged revisions do not prove fsync durability: periodic sync can lose acknowledged revisions on power loss and break fencing.

`open(nc, options)` consults JetStream account info for availability; normal account info and connection server INFO do **not** expose disk-sync policy. It can verify trusted host-provided `monitoringUrls` (`/varz`, HTTPS except loopback HTTP): reports must include distinct server IDs, match the connected server, and cover all three **placed** stream member names. On v2.14.7 `/varz.jetstream.config.sync_always: true` is the relevant proof; its numeric `sync_interval` still reports the periodic default even when always-sync is enabled. An exposed literal `sync_interval: "always"` is also supported.

If monitoring is absent, unreachable or incomplete, admission refuses unless the operator explicitly supplies **`syncAlwaysAttested: true` in trusted host-only store options**. This attests always-sync on **all eligible members**, not merely the connected server. Never accept it from a lease payload, agent arguments, remote/user configuration or a runtime default. An observed periodic/unsafe configuration always rejects, even with attestation. Unavailable/unknown evidence is not interpreted as safe. All-member monitor coverage is checked before returning any usable store. Provisioning a rejected bucket never grants ownership. Do not silently grant $SYS privileges or infer monitoring URLs/credentials.

```ts
// Host-controlled admission, not agent/user data:
await NatsKvLeaseStore.open(nc, {
  bucket: "FABRIC_LEASES", maxLeaseMs: 10_000,
  monitoringUrls: trustedAllMemberVarzUrls,
});
// ONLY when verification is unavailable and the operator has checked ALL members:
await NatsKvLeaseStore.open(nc, {
  bucket: "FABRIC_LEASES", maxLeaseMs: 10_000, syncAlwaysAttested: true,
});
```

## Failure contract and independent evidence

CAS resolves successor-between-check/write and stale cleanup races. Crash before/after publish leaves no process/file lock; an uncertain committed lease blocks until server expiry. Timeouts/unknown acknowledgements return no new authority; stop protected work and retain at most the old token/deadline. Late renewal acknowledgements fail closed. Bucket restore/recreation must not reset sequence beneath live tokens. Cross-host clock skew/backward jumps, minority partitions and stale-follower reads remain pre-integration gates, as do TLS and least-privilege ACLs (an authorized bucket writer can forge a token).

The test-only protected resource runs on a separate isolated local R3 cluster (three additional processes, same physical host) and stores a persistent fence in a KV key and CAS-enforces every submitted owner write. A **separate connection's KV watch** observes accepted writes in resource-stream order, proving non-decreasing fences and no superseded-owner write after its successor. It does not use returned lease intervals or `Date.now()` as its evidence. Leader-SIGKILL owners write this resource during renewals. A separate real process is **SIGSTOPped across lease expiry**, the successor lands a protected write, then SIGCONT resumes the old owner and it actually attempts a write with its old token; the resource rejects it. `fault.json` and `paused-owner.json` record observer writes/rejections. Client authority-interval overlap is supplemental, not paused-owner proof. This resource is test-only, not a production integration or ACL/forged-token solution.

Latency is 200 sequential adapter end-to-end acquire/renew samples after 20 warmups, p50/p99 at always-sync. This harness runs three loopback processes on **one physical Ryzen 2 host**; it proves neither three-host/power-loss resilience nor production NVMe latency. Dependencies are official branch-only NATS.js v3 modules.

## OWNER GATE — fabric-v2 (held, not wired)

Owner: **fabric-v2**. Production acceptance requires an **R3 cluster across three physical hosts on the target NVMe**, all members admitted with **sync_interval: always**, and the **conformance + leader-kill fault + SIGSTOP/SIGCONT paused-owner protected-write tests green there**, with **p99 renew <20 ms**. Preserve exact deployment/sync/recovery and independent observer evidence. The owner must also clear power/clock/partition/TLS/ACL/epoch-fencing integration gates before enabling a selector. Local evidence does not satisfy this gate.

**Revert:** disable the selector flag; **file leases stay**. No selector exists or is enabled by this prototype. No automatic etcd or messaging rollback. CI/security review must pass on the exact head before merge; runtime wiring remains held until the owner accepts production evidence.
