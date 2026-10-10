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
maintenance receipt waits for that existing generation and does not launch
a competitor.

## Event-driven completion retries

Main's residency client observes `residency-notifications/<sha256(key)>.json`
under the mesh root. `MeshStore.put`, successful deletes and applied batch
operations publish/remove these advisory per-key files only for
`residency/deliveries/` and `residency/completion-claims/`. File, shadow and SQLite
backends share that path; a lease-only batch adds no notification or outbox
snapshot. State, ownership, receipts and versioned CAS remain authoritative.
Malformed/spoofed hints never authorize delivery.

Generic `state.json`, read-journal/signal, heartbeat and lease writes do not retry
pending work. A changed key re-reads just that exact key, compares its attempted
signature and validates ownership. Completion/receipt and legacy producer-file
notifications retry only the corresponding known items. Unchanged failed work
is retained separately from admitted dirty work, including in the completion
journal. `ResidencyClient.retryDeliveries()` is an explicit recovery hook;
startup/reconnection also reconciles durable state. Lost notification publication
or a process crash can be recovered there, not by an idle timer. The minute
safety timer only repairs watch attachment, never discovers/delivers work.

The actor mesh monitor is separate. It retains the 5-second
`MESH_MONITOR_SAFETY_NET_MS` recovery drain and all manager `beforePoll`
maintenance from main, including registry, ownership, filters and child results.
Unsupported or failed actor watches retain the `actorPollMs` fallback. This PR
carries the non-drain parts of closed pi-fabric#724; it does not claim the
smarty-dev#7299 fault-armed exception exit. External registry, ownership and
child-completion wake triggers must be proven before that drain can be removed.

Successful Main settlement also reconciles young inbox work and re-arms its
exact known-item one-shot grace/cooldown deadline. A busy-time watch event cannot
cancel that hint permanently; the deadline still passes all normal idle,
preflight, capability and ownership gates, and does not admit unrelated records.

See [Linux idle CPU replay](idle-cpu-benchmark.md) for the 300-second same-seed
measurement and 600-second duration bounds.

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
