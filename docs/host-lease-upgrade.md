# Host lease UUID rollout (#7313 / PR #742)

Native participant writers now serialize each file mutation and shared-state batch with the
current lease UUID in a physical-host-qualified commit gate. The gate remains held through the
state backend's CAS/transaction, including asynchronous acquisition; a successor cannot claim
between ownership validation and COMMIT. Participant keys precede the lease gate, and the lease
gate precedes shared state. No participant-key acquisition or registry acquisition occurs while
holding that gate. Supersession is terminal (`FABRIC_HOST_LEASE_SUPERSEDED`), never an invitation
to recreate an absent lease.

## Mixed pre-UUID writers: drain before starting a second live Pi

An in-place `/reload` already quiesces/fences the old runtime. The additional rollout hazard is
**a second live Pi on an old release**, including a SIGSTOP-paused process that later resumes.
Pre-UUID releases do not honor this head's commit gate and can replace a lease without a UUID.
The new release refuses to claim while a fresh legacy-format lease exists. It arms only one
abortable wake at that lease's captured expiry (or caller deadline), then attempts once. If an
old writer extends the lease in the meantime, it returns `FABRIC_HOST_LEASE_LEGACY_BUSY` with
`expiresAt`; it does not poll or rearm itself. Explicit zero-wait callers get that typed busy
error immediately.

### Mechanical first-UUID claim gate (smarty-dev#7947)

Before this physical host/UID's first UUID claim in a mesh, native admission takes a read-only,
bounded `/proc` census. A live Pi/Fabric process (including state `T`, SIGSTOP) must identify its
loaded release from environment/argv/maps paths under `fabric/releases/<sha>`, or a runtime release
record tied to the PID's kernel start ticks. The current active pin is never proof of loaded code.
Releases advertise `LEASE_FORMAT_UUID_MIN = 2` as `leaseFormat: 2` in `dist/worker-protocol.json`.
Missing/older markers, unreadable evidence and unknown live candidates fail closed with
`FabricPreUuidProcessAliveError` (`FABRIC_PRE_UUID_PROCESS_ALIVE`, message
`pre-UUID Fabric process alive`) listing every observed blocking PID. No lease or admission marker
is written on refusal. Other UIDs and exited/zombie processes are not live same-UID writers.

Only after the UUID lease write succeeds is a physical-machine/UID-qualified `.uuid-format-*.marker`
marker atomically written in `host-leases/`. Later claims on that host in that mesh do not rescan;
renewals retain the UUID CAS, and target reaping/final batch revalidation remain unchanged.
Retained reload authority also checks admission before its shared batch and at the final durable
commit boundary; an exact-claim, single-use census receipt lets the successful commit rotate its
UUID without discovering a new admission refusal only after COMMIT. A gate refusal is terminal
for that directory: renewal/publication timers and consumer admission stop, with no scan retries.
The snapshot is capped at 4096 processes, 1 MiB per observation, 32 MiB total and a 2 s deadline.
A fresh legacy predecessor is checked before its single expiry/deadline wake and rechecked
under custody before the lease write (at most two snapshots per gated operation); TTL waiting never admits an old
process. There is no process killing, pin mutation, scan retry loop or polling. Non-Linux/unavailable census
or physical host identity is unknown and refuses rather than silently bypassing the gate.

### Residual security gap: old code introduced after admission is still unfenced

The precise upgrade-window precondition is an **old-release, pre-UUID process paused for
longer than its host lease TTL**, then resumed **after a new UUID writer has claimed** the
expired lease. The new mechanical gate rejects such a process at the first-claim snapshot, so
this residual now requires an old writer to be introduced/reintroduced after that snapshot or
through an already-admitted host marker. The old process can execute its unfenced legacy lease write (and potentially
its old publication code) without taking the new host commit gate. Expiry proves neither
process exit nor quiescence. This does not describe an in-place `/reload`: that path quiesces
and fences the old runtime before the successor is admitted.

Every new lease-owned shared-state batch captures its lease UUID and re-validates it with a
fresh ownership CAS after preparation/operations at the durable commit boundary (file/shadow
state replacement or SQLite COMMIT). A legacy overwrite observed there aborts the entire batch
with `FABRIC_HOST_LEASE_CONTESTED`; a different UUID or absent lease aborts with
`FABRIC_HOST_LEASE_SUPERSEDED`. No post-commit error is presented as rollback. This closes the
in-flight acquisition/preparation window, not an atomic transaction across a legacy writer's
lease file and shared state: old code can still overwrite after the final check, or mutate
state itself. New code cannot retroactively fence that writer, and cannot claim coexistence
with it is safe.

**Mitigation:** the first-claim gate mechanically refuses until old/unknown processes have
stopped or have verifiably upgraded. Keep host process launches quiescent across that bounded
snapshot and the claim; upgrade by stopping all old-release processes sharing the native host lease
**before** allowing the new claim. Do not rely on waiting one TTL, a stale file, or SIGSTOP as
proof they have stopped. Keep the old processes stopped until their binaries are upgraded.

After UUID admission, seeing any legacy-format lease is terminal contention
(`FABRIC_HOST_LEASE_CONTESTED`, non-retryable). The directory stops renewal, publication retries,
and consumer admission, and never adopts the legacy write as a new migration predecessor.
A pre-UUID writer cannot be retroactively fenced by new code: drain/exit **all** old live Pis
sharing that native host lease before enabling a successor; expiry alone is not proof of process
exit. Mirrored remote leases remain bridge-owned compatibility records, not native admission.

## Recovery identity and unavailable notifications

Host-qualified commit receipts record `/etc/machine-id` and
`/proc/sys/kernel/random/boot_id`. Both must be readable and valid. Hostname and mesh `hostId`
are never physical-host evidence (even when the mesh label contains a hostname). A different
machine, or unknown identity, is FOREIGN and cannot be PID-recovered. The same machine and same
boot uses PID/incarnation death proof; a different boot of that same machine is recoverable
without probing the old PID because none of the old boot's processes survives. Non-Linux or
unreadable identity fails closed: stale gates require explicit operator recovery, not age steal.

Lock acquisition uses removal notifications while available. Unsupported watch or a busy
commit gate with no notification source returns typed `HostLeaseLockBusyError` after one attempt;
there is no 5–250 ms fallback loop. Its serialized `busyCode` is `FABRIC_HOST_LEASE_LOCK_BUSY`.
Directories do not arm their mesh-outage jittered publication-retry timer for that result; the
caller owns event/deadline-driven retry policy (ordinary heartbeat deadlines remain unchanged).
