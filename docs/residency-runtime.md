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
maintenance receipt waits for that existing generation instead of launching
a competitor.

## Context and lifecycle

Residency does not share agent contexts. Each actor or agent retains its own
runner session. Shared work must use mesh state, files, or an explicit external
channel. Residency only keeps the execution owner available for later control
and reconnection.

## Dormant actors and event-owned wake (#6782 / #2264)

A durable actor with no in-flight activation, queued/overflow/parked/dead-letter
work, pending ask/reset/removal, or child reply/archive custody becomes
`dormant`. Its registry identity, subscriptions and session transcript stay on
disk. Activation workers already close at settlement; dormancy also drops the
completed drain and child-inbox runtime references. A new delivery returns the
actor to `queued`, restores its transcript, and uses its ordinary serial drain.
Session actors are unchanged. A live Main's event/directive supervisor and
actors subscribed to still-live participants are expected, not truly idle.

A host with only dormant/stopped actors, no running task, and no pending
request, response/publication or delivery-outbox obligation exits after the
existing **30-second** `IDLE_EXIT_MS` grace. Ponytail: this constant amortizes
close/delivery races; it is not a worker keep-warm policy. Before exit the host
pauses admission, checkpoints control/lifecycle and actor queues/cursors, and
confirms the actor mesh monitor is caught up. An unconsumed event or new request
cancels sleep. `config.json`, registry/session files, `wake-routes.json` and
cursors survive; `owner.json` and the live processes do not.

`MeshStore.publish`/`publishBatch` route wake nudges **after** the existing event
log durability barrier. Topic/address matches, direct control targets, and
lifecycle subscriptions select a retained residency configuration. A durable
`wake-request.json` nudge starts the existing launcher with `--wake`; the nudge
is not another inbox. One POSIX `wake.lock` serializes these launchers. A nudge
racing `sleeping.json` waits for the closing owner before launching a successor.
The sleep receipt covers earlier nudges, so the launcher exits too unless a new
delivery crossed the final close boundary. There is no polling hostd. The live
client watchdog explicitly ignores dormant definitions.

Direct steer/followUp to a dormant registry definition first wakes and waits for
the ordinary owner/readiness publication, then uses the existing lease-validated,
durable command and ACK path. Wake does not grant ownership, invent a participant,
or bypass stale-owner/bridge admission. An unacknowledged preflight is not an
accepted delivery. Once a command is committed, its target also uses the same
commit-before-wake hook as topic traffic.

Before sleeping an actor-bearing host, residency ensures the existing file event
archive is enabled under the mesh publication lock. It preserves an operator-selected
archive, or creates `<mesh>/wake-archive` plus `event-archive.json` when the mesh
previously had only a bounded live log. A bad/unavailable archive cannot authorize
sleep. This is retention configuration, not a new inbox or state backend.
Sleeping resident monitors replay subscribed ordinary topics from that existing
archive as well as `fleet.*`; a replay-age window cannot drop sleeping work.
Clean sleep/wake retains the existing queue dedupe and cursor boundaries, so
wake-window deliveries drain once in order (unless the actor explicitly opts
into coalescing). Crash/interrupted-run retry semantics are unchanged; this does
not promise exactly-once external effects across arbitrary process crashes.
Automatic wake uses the existing POSIX fence; durable Windows Pi launch remains
unsupported as noted below. File-backed state remains the authority.

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
