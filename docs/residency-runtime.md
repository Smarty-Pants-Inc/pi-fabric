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
The default usable-start observation budget includes the mandatory 30-second
empty legacy mesh-lock grace plus bounded boot/acquisition time; explicit caller
budgets remain unchanged. A client attaching between owner publication and the
maintenance receipt waits for that existing generation without launching
a competitor.

Scope travels with each request. The host has no session scope of its own, so
a scoped session puts the full derived scope in a durable spawn
(`inheritedScope`) or actor create (`principalScope`). The host checks the
digest and refuses a malformed scope. It records the scope in the agent's
`agents/<id>.json` and in the actor registry, and launches every child and
actor turn with it. Hosted-run recovery checks the persisted scope again.
Requests are files writable by the same OS user, so this keeps the
single-user boundary of the mesh; see
[principal and scope](providers.md#principal-and-scope).

## Context and lifecycle

Residency does not share agent contexts. Each actor or agent retains its own
runner session. Shared work must use mesh state, files, or an explicit external
channel. Residency only keeps the execution owner available for later control
and reconnection.

The host exits after its normal idle grace once it owns no live durable actor or
running durable agent.

### PR #394 scope cut: recovery supervision deferred

This change keeps cold startup without an archive walk and lease-fenced mesh
consumption/cursor commits. It does not introduce a launcher watchdog, attempt
debt/alarm markers, startup/shutdown supervision receipts, host writer retirement,
or a forced host shutdown-deadline exit. The launcher and host shutdown paths
retain main's existing behavior; no new bounded-close or whole-attempt containment
guarantee is claimed. Those features require a separate fabric-v2 follow-up with
checked whole-attempt exit and completed-drain proofs before recovery admission.

### Lease-fenced consumption and accepted-work custody

A file-only heartbeat is liveness, not permission to consume mesh work. Control,
actor mesh, and lifecycle consumers require a confirmed shared-lock renewal.
An overdue consumer requests a prompt renewal attempt, but stays fenced until
that real acquisition succeeds. Initial publication failure keeps the same
host start pending and does not publish a usable owner prematurely.

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
[Scheduled mesh events](agents.md#scheduled-events) add one rule, computed by the
pure `residentIdleDecision`: pending schedules on any topic keep the host alive
while it owns at least one durable participant, and every idle check re-arms a
wake timer to the earliest due time. At that time the host releases due schedules
under the mesh lock, and its actor monitor delivers them. Pending schedules alone,
with no durable participant, do not keep the host running; Fabric adds no system
daemon, so such a schedule is released the next time any Fabric process touches
the mesh root.

### Stale commands after restart

A restarted host keeps its `residentHostId(rootId)` and replays the control log
from offset 0, so a stop or ask meant for the previous process could otherwise
reach a durable actor's next turn. Each host process publishes its participant
records with a fresh `ownerIncarnation`, and requesters stamp that value on each
command. The new host refuses an unclaimed command for an earlier incarnation
with an immediate rejection and does not execute it; the caller re-resolves the
participant and decides whether to resend. See
[stale commands after restart](agents.md#stale-commands-after-restart).

## Containers and PID namespaces

`kill(pid, 0)` only answers within the caller's PID namespace and boot. In a
container, the same number can name an unrelated process or nothing, and after
PID reuse it names a stranger. Fabric owners therefore record an identity:
`hostname`, the Linux PID namespace (`/proc/self/ns/pid`), the boot id, and the
process start time. The owner-liveness section of `src/core/atomic-write.ts`
judges an owner as follows:

- Same host, namespace and boot: the signal probe decides (`ESRCH` dead,
  `EPERM` alive). On Linux, a different `/proc/<pid>/stat` start time for the
  same PID means reuse, so the owner is dead.
- Another namespace, host or boot: only a heartbeat decides. A fresh heartbeat
  is alive, a stale one is dead, and none is `unknown`. On the same host, a
  different boot id with no heartbeat is dead, since no process outlives its
  boot.
- Records without identity fields keep the earlier signal-probe behaviour.

`unknown` is never treated as death. A lock whose owner might be alive is not
stolen.

The resident host is a long-lived owner. Its start lock and `owner.json` carry
additive `identity` and `heartbeatAt` fields, and the format stays `1`. The
heartbeat refreshes every 10 s, and a heartbeat older than 45 s is stale. The
Schema commit lock heartbeats the same way while a transaction runs. Short
mesh, actor-store and file locks carry an identity line only. Across
namespaces, their creation time serves as the heartbeat, with a 10-minute hold
ceiling (or the lock's stale window, if longer). Scratch markers record
identity and no heartbeat, so the sweeper leaves a foreign owner's scratch
alone.

Hosts and containers that share `.pi/fabric` need clocks that agree within the
heartbeat TTL.

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
