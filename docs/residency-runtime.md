# Durable residency through Pi

## Purpose

Fabric participants with `residency: "durable"` continue after the originating
Pi session closes. They run in one background resident host per Fabric root.
The host owns durable actors and one-shot durable agents; mesh state and the
residency directory provide reconnection and control routing.

## Why the host starts through Pi

Pi packages intentionally leave Pi core packages as host-provided peers. A raw
Node process started from an installed Fabric package therefore cannot resolve
`@earendil-works/pi-coding-agent`. Pi's extension loader supplies those imports.

The resident host must consequently run inside a headless Pi process, rather
than directly under `node`.

## Process topology

```text
Fabric residency client
  -> detached Node launcher
    -> pi --mode rpc --no-session --no-tools --extension pi-entry.js
      -> Pi extension loader
        -> ResidentHost
```

`launcher.js` is Node-core-only. It keeps RPC stdin open because Pi RPC exits on
stdin EOF. `pi-entry.js` starts `ResidentHost` on `session_start`, aborts it on
`session_shutdown`, and shuts Pi down after the host reaches its idle exit.

## Files and responsibilities

- `src/residency/client.ts` writes host configuration and starts the launcher.
- `src/residency/launcher.ts` creates the detached headless Pi child.
- `src/residency/pi-entry.ts` bridges Pi lifecycle events to the host.
- `src/residency/host.ts` owns requests, mesh control, `owner.json`, and idle
  shutdown.
- `src/index.ts` registers `residency/launcher.js`; this registration must not
  point back to `host.js`.

`config.json`, `owner.json`, request, response, and mesh files remain the durable
interface. New hosts advertise `maintenanceReady: 1` in their immutable owner
record. The client also requires `maintenance-ready.json` with that exact owner
token before treating the host as usable. This receipt follows the first normal
post-lease maintenance slice; startup itself does not inspect archived runs.
Older hosts without that optional flag retain their existing readiness contract.
`starting.json` is a separate, birth-checked supervision identity while initial
publication waits: it is never a usable owner or successor-admission receipt.
The default usable-start observation budget includes the mandatory 30-second
empty legacy mesh-lock grace plus bounded boot/acquisition time; explicit caller
budgets and the fail-closed watchdog interval remain unchanged.
Before withdrawing `owner.json`, the host writes a same-birth `closed.json`
receipt. It grants only the existing bounded native Pi-exit grace, not
whole-attempt exit proof or permission for a successor. If native Pi remains
hung past that grace, the watchdog still latches its persistent alarm.

## Context and lifecycle

Residency does not share agent contexts. Each actor or agent retains its own
runner session. Shared work must use mesh state, files, or an explicit external
channel. Residency only keeps the execution owner available for later control
and reconnection.

The host exits after its normal idle grace once it owns no live durable actor or
running durable agent.

### Watchdog alarm: no automatic successor without complete exit proof

The launcher alarms after three stale lease-renewal intervals or a persistently
unreaped direct child. It retains owned-child TERM/KILL and native-exit joining,
but **does not restart**: sampled ancestry, process-group cleanup, native parent
exit and a free host fence cannot prove the exit of every attempt-owned helper.
This follows worker stop's existing rule that uncertain descendant exit retains
admission debt; no new process tracker or containment claim is introduced.

Before stopping the alarmed child, the launcher durably latches
`watchdog-alarm.json` in the residency root. The launcher, reconnecting residency
client and direct host startup all refuse admission while that marker exists.
The host checks again after acquiring its fence, before restoring actors or
publishing its lease. Corrupt/unreadable markers also fail closed. Nothing in
cold-start retry, native shutdown or lease expiry clears the alarm. Ordinary
unalarmed cold start and the existing shutdown deadlines are unchanged.

Recovery requires the existing explicit installer drain and complete descendant
exit proof before an operator removes the alarm marker. Removing it merely
because the parent exited or the lease expired is unsafe. Automatic watchdog
recovery remains unavailable until attempt-owned containment and checked exit
receipts cover detached/reparented descendants.

### Lease-fenced consumption and accepted-work custody

A file-only heartbeat is liveness, not permission to consume mesh work. Control,
actor mesh, and lifecycle consumers require a confirmed shared-lock renewal.
An overdue consumer requests a prompt renewal attempt, but stays fenced until
that real acquisition succeeds. Initial publication failure keeps the same
host start pending rather than publishing a usable owner prematurely.

Control admission checks the lease under both the shared claim lock and the
host-local claim lock. The resident rechecks after awaited binding resolution,
before the actual actor delivery. Claims carry no event sequence until the
outcome commit; sequence and ACK publication recheck under their actual locks.
Completed handlers retain their result while fenced, including a sequence-free
host-local outcome receipt, so renewal/restart does not run the handler again.
Partially owned claims and pending outcomes also block release checkpoints.

New actor queue snapshots record versioned evidence of an actual worker launch.
Only an interrupted launched run consumes the restoration budget. Failed host
starts, lease waits, and untouched queued events do not spend it. Legacy
unmarked snapshots retain conservative interrupted-run handling. Startup does
not pick up queued resident requests before the start succeeds. Restored actor
queues stay release-paused, and control/lifecycle delivery gates remain closed,
until owner publication commits. A failed start after confirmed lease publication
therefore also preserves accepted, unlaunched work.

Ordinary close retires only this host's mesh writers, including existing lock
waiters. It neither signals nor removes another process's lock. Durable actor
queues and the completion outbox retain accepted custody; worker exit is still
joined within the existing native shutdown deadlines. This prevents sequential
publication timeouts from turning a cooperative idle exit into watchdog alarm
debt. It does not relax the fail-closed alarm/re-entry policy above.

## Validation

Run:

```bash
bun run typecheck
bun run build
bunx vitest run tests/type-checker.test.ts tests/residency.test.ts tests/fabric-runtime-components.test.ts
```

Also validate a locally installed package in Pi: create a durable actor, verify
its `owner.json`, route `stop`, and confirm the actor becomes `stopped` with a
resident `ownerHostId`.

## Known Windows limitation

Durable residency E2E is POSIX-only today. On Windows, the launcher's spawn of
the `pi` binary through the installed `node_modules/.bin` shims hangs before
the child starts, so the resident host never starts.
The launcher, ownership observation, and protocol logic are platform-agnostic
and are tested on every operating system.

## Future direction

A native Pi extension-host subprocess API could replace `launcher.js` later.
Keep the launcher boundary isolated so that migration changes no residency
protocol or public Fabric API.
