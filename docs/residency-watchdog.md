# Resident launcher lease watchdog

The residency launcher detects and stops a **live but wedged** owned resident
child. This mitigates smarty-dev#4313; it does not fix the retention/event-loop
root cause.

**Automatic replacement is currently disabled, fail-closed.** Neither the direct
child's native exit nor sampled descendant cleanup proves complete attempt exit.
Workers can create separate sessions, systemd scopes or panes, and a replacement
could replay their interrupted actor work while they still make effects. The
watchdog uses the same `assertAutomaticReleaseRecovery` refusal as release
handover; no setting bypasses it. Explicit installer drain is still required to
recover service after owned shutdown.

Every 30 seconds it reads the child's own raw host lease, using the canonical
hashed filename under `<meshRoot>/host-leases/`. The owner must match the direct
child's PID/birth, root/identity and post-spawn lease. Healthy children, competing
owners and active release handovers are not recovery candidates.

## Residency configuration

The launcher's `<residency>/config.json` can contain:

```json
{
  "watchdog": {
    "enabled": true,
    "stallMs": 180000,
    "coldStartMs": 900000,
    "intervalMs": 30000,
    "maxRestartsPerHour": 3
  }
}
```

All durations are milliseconds. Omitted keys use these defaults; `enabled: false`
disables watchdog signals. Configuration is captured at launch, not hot settings
in `.pi/fabric.json`. `maxRestartsPerHour` is retained for configuration
compatibility only: the current safety gate permits **zero** automatic restarts.

A first post-spawn lease plus `coldStartMs` protects cold startup. Children without
a first lease are never classified as wedged. After the allowance a lease age
greater than `stallMs` identifies a candidate, not permission to signal it.

## Evidence and stale-generation revalidation

Before signaling, the launcher captures `<residency>/wedges/<ISO>/`:

- readable `/proc/<pid>/{status,stat,io,wchan,stack}` and thread stat rows;
- FD and `runs/` entry counts;
- the last 200 child-log lines (bounded to 4 MiB);
- the child's own lease bytes.

Only five wedge directories are retained. Capture, pruning and report-storage
errors are best-effort diagnostics, logged as bounded `watchdog-evidence-error`
records. A failed initial capture defers signals and retries at the next sampling
interval; it never exits supervision or removes owned shutdown handlers. Report
collection failures after signaling likewise cannot abandon native custody.

Children receive Node diagnostic-report flags via `NODE_OPTIONS`, including
`--report-on-signal --report-signal=SIGUSR2`, `--report-exclude-env` and a private
report directory. SIGUSR2 is followed by a five-second allowance; completed
reports for that PID are moved to the evidence directory.

The launcher holds the handover transaction lock across the report/signal
boundary. Immediately before **each** SIGUSR2, SIGTERM and SIGKILL, it freshly
checks the same owner generation, PID birth, unchanged stale lease and absence
of active handover. Any renewal (even if already stale again), ownership change,
healthy lease or unavailable check aborts recovery and logs `watchdog-aborted`.
It waits up to 30 seconds after TERM and five seconds after KILL. Windows cannot
prove the required POSIX lock/containment boundary and defers recovery.

## Checked exit and deferred replacement

After the child's native exit, observed-descendant cleanup remains best-effort
only. A separate necessary Linux session-exit check enumerates **all** `/proc`
members, including reparented children and zombies, and probes the owned process
group's existence. Read/enumeration errors and restricted visibility fail closed.
This checked session boundary is not an attempt-owned containment receipt:
external workers may have escaped before any ancestry sample.

Before the first signal (including SIGUSR2) the launcher durably writes
`watchdog-custody.json` under the handover transaction lock, then revalidates the
lease again. An unexpected exit during report capture must not reopen startup.
Clients and new launcher CLIs refuse startup while this marker exists, even after the old launcher dies; merely
expiring its PID would not fence escaped workers. A healthy current generation
may still be attached. The explicit installer drain must prove all old work is
gone before removing the marker; no automated removal is provided.

The launcher therefore logs `watchdog-deferred`, never starts a replacement and
keeps custody and SIGINT/SIGTERM handlers. It retries the exit proof at most 30
times, one second apart; then logs `watchdog-giving-up` once and stays in owned
custody until explicit shutdown/drain. An empty session alone still cannot enable
a second serving host. No `wedge-recovered` marker is published while recovery is
unproven, so automation cannot mistake deferral for service restoration.

Real-path regressions cover the compiled launcher CLI, a double-forked lingering
grandchild, report-window renewal, unwritable evidence and restored storage, plus
the real native Pi resident with in-flight actor work and an escaped helper.

## Rollout

This takes effect only when a residency relaunches onto a release containing it.
Main reload/build alone does not change an already-running launcher. No new
service, unit or daemon is introduced.
