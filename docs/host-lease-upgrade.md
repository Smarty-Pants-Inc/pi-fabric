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

### Residual security gap: a paused old release can outlive its TTL

The precise upgrade-window precondition is an **old-release, pre-UUID process paused for
longer than its host lease TTL**, then resumed **after a new UUID writer has claimed** the
expired lease. The old process can execute its unfenced legacy lease write (and potentially
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

**Mitigation:** upgrade by stopping all old-release processes sharing the native host lease
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
