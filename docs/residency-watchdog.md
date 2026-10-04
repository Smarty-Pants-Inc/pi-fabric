# Resident launcher lease watchdog

The residency launcher automatically recovers a **live but wedged** resident child.
This is containment for smarty-dev#4313, not a fix for the retention/event-loop root
cause being investigated in `fv2-retention`.

Every 30 seconds it reads only the child's own host lease, identified by the
same hashed filename under `<meshRoot>/host-leases/` and `updatedAt` field used by
peer liveness readers. It requires the owner PID to match its direct child,
matching root/identity metadata, and a lease written after that child started.
It does not signal healthy children, competing owners, or children participating
in an active release handover.

## Residency configuration

The launcher's `<residency>/config.json` can contain this optional section:

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

All durations are milliseconds. Omitted keys use the defaults above. Set
`enabled: false` to disable recovery. The restart limit may be lowered, but it is
always capped at **three restarts per rolling hour**. Configuration is captured
at launch, including the immutable launch snapshot when available; these are
residency-launch settings, not hot settings in `.pi/fabric.json`.

Startup is protected until the first observed post-spawn lease write **plus**
`coldStartMs`. A child that has not written its own lease is never classified as
wedged. This intentionally protects slow cold starts over large `runs/` trees.
Once that allowance has elapsed, a lease age greater than `stallMs` is a wedge.

## Evidence and recovery

Before sending signals, the launcher creates `<residency>/wedges/<ISO>/` with:

- `/proc/<pid>/status`, `stat`, `io`, `wchan`, and `stack` when readable;
- every readable `/proc/<pid>/task/<tid>/stat` (including per-thread CPU ticks);
- the FD count and `<residency>/runs/` entry count;
- the last 200 child-log lines (capture bounded to 4 MiB for pathological lines);
- the own host lease's original contents.

Every child is started with Node diagnostic reports enabled through
`NODE_OPTIONS`: `--report-on-signal --report-signal=SIGUSR2` and
`--report-directory=<residency>/wedges/reports`. `--report-exclude-env` keeps
inherited environment variables out of persisted reports. On a recoverable wedge the first
signal is SIGUSR2, followed by a five-second report allowance. Completed reports
for that PID are moved into the evidence directory. `/proc` evidence and
SIGUSR2 diagnostics are Linux/Unix facilities; Windows skips the report signal.

The direct child's observed Linux birth is rechecked before each signal, so a
reused PID cannot authorize a watchdog signal. The launcher then sends SIGTERM,
waits up to 30 seconds, sends SIGKILL if needed,
and requires a native child-exit receipt before respawning. It snapshots and
cleans up birth-validated observed descendants through the existing ownership
cleanup. The replacement uses the same launch configuration, entry, runtime,
and release snapshot. `launcher.log` records `watchdog-restart`, lease age,
CPU user/system ticks, restart count, and the evidence directory.

Only the latest five wedge directories are retained. When the rolling-hour
limit is exhausted the launcher captures evidence, logs `watchdog-giving-up`
once, keeps the child, and does not send recovery signals or restart again.

## Host daemon handoff

**The launcher has no mesh publisher. This implementation uses the marker-file
option**, not direct mesh publication. After the replacement has written its own
lease and claimed ownership, the launcher atomically replaces
`<residency>/wedges/latest.json` once per recovery:

```json
{
  "topic": "fleet.residency.<host id>",
  "kind": "wedge-recovered",
  "hostId": "<Fabric resident host id>",
  "evidenceDir": "<residency>/wedges/<ISO>",
  "restartCount": 1,
  "createdAt": 0
}
```

The dev-lead host daemon can consume this marker, run factory re-registration
(`activation.py`), and post to smarty-dev#4313. The evidence directory is the
unique recovery key; `latest.json` is a latest-state handoff, not a queued mesh
log. Implementing the daemon consumer or issue posting is outside this change.

## Rollout

This takes effect **when a residency relaunches onto a release that contains
it**. Already-running launchers do not gain the watchdog from a Main reload or a
build alone. No new service, unit, or daemon is introduced.
