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

## Dormant actors and delivery-owned wake (#6782 / #7299)

A durable actor with no in-flight activation, queued/overflow/parked/dead-letter
work, pending ask/reset/removal, or child reply/archive custody becomes
`dormant`. Its registry identity, subscriptions and session transcript stay on
disk. Activation workers already close at settlement; dormancy also drops the
completed drain and child-inbox runtime references. A new delivery returns the
actor to `queued`, restores its transcript, and uses its ordinary serial drain.
Session-resident actors are unchanged. A live Main's event/directive supervisor
and actors subscribed to still-live participants are expected, not idle.
Startup/reload, open tool calls, running tasks and pending host obligations also
block dormancy. A process-backed call stays busy until its real tool promise
settles, even if cancellation has already returned to its caller. These holds
use session IDs, not retired Pi contexts, and survive a same-process reload.
A new runtime restores non-stopped actors as idle and rechecks eligibility under
its own work and owner custody; it does not inherit the old host's idle decision.

A host with only dormant/stopped actors, no running task, and no pending
request, response/publication or delivery-outbox obligation exits after the
existing **30-second** `IDLE_EXIT_MS` grace. This constant amortizes
close/delivery races; it is not a worker keep-warm policy. Before exit the host
pauses admission, checkpoints control/lifecycle and actor queues/cursors, and
confirms the actor mesh monitor is caught up. An unconsumed event or new request
cancels sleep. `config.json`, registry/session files, `wake-routes.json` and
cursors survive; `owner.json` and the live processes do not.

This is the dormancy-only split of [PR #704](https://github.com/Smarty-Pants-Inc/pi-fabric/pull/704).
The existing **50 ms request poll**, **100 ms V2 request-maintenance interval**,
retention overlay and collector behavior are unchanged. The idle request-polling
and maintenance-timer rework is separate
[smarty-dev#7885](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/7885).
Actor changes/final drain settlement, agent settlement, delivery-outbox drain and
actor presence-publication completion drive dormancy eligibility. Each event's
check commits eligible dormancy directly after proving native wake support and
revalidating current actor work and live-participant protection. There is no
actor quiet period, dormancy timeout, timed safety recheck or periodic retry;
these signals do not replace the existing request polling.
Dormancy also requires canonical JSON equality of the **entire** persisted
`config.json` and the host's exact restart configuration. Object key ordering is
irrelevant; no field is ignored, including `actorRoot`, `sessionActorRoot`,
`mesh.actorScope` and future authority-bearing fields. Any difference or an
unreadable snapshot keeps the actor/host resident and logs a one-line reason.
`wake-routes.json` retains that complete canonical config snapshot. Dormant
admission, delivery-owned launch and each successor start re-check equality;
a mismatch refuses with `ResidentWakeConfigMismatch`
(`RESIDENT_WAKE_CONFIG_MISMATCH`) without changing actor definitions. The
archive-coupled index retains retryable pending deliveries while config is
refused, so a later config repair and delivery event can retry without losing the
cursor; a completed terminal decision ACKs its nudge, as described below.
Old route snapshots without the config fence cannot authorize a wake. Explicit
client starts still use the ordinary resident startup protocol.
A throwing idle-check `queueMicrotask` enqueue clears its queued flag, allowing
the next eligibility event to schedule (smarty-dev#7988).
The proven native watcher belongs to that host lifetime. Close joins its native
`close` event, any failed/replaced watch handles and outstanding eligibility
proof before releasing the owner. Concurrent close callers join the same
teardown. A successor proves its own watcher, and Windows directory removal
cannot race a still-closing watched-directory handle.

`MeshStore.publish`/`publishBatch` route wake nudges **after** the existing event
log durability barrier. Topic/address matches, direct control targets, and
lifecycle subscriptions select a retained residency configuration. A durable
`wake-request.json` nudge starts the existing launcher with `--wake`; the nudge
is not another inbox. One POSIX `wake.lock` serializes delivery-triggered native
wake starts. Explicit/client starts retain the ordinary kernel-fenced startup
protocol, including an already-admitted watchdog challenger; they are not silently
suppressed by `wake.lock`. A short `wake-intent.lock` serializes durable request writes
with the launcher's final request/sleep snapshot **and release of `wake.lock`**.
A commit after that snapshot cannot write its nudge until lifetime custody is
released, so its launcher cannot lose the wake by finding a departing lock holder.
The child-owning launcher joins the native `ChildProcess` exit receipt. An external
pre-upgrade closing owner and explicit readiness waits use `fs.watch` with one
bounded failure deadline. The sleep receipt covers earlier nudges, so no launcher
remains warm after clean sleep. There is no polling hostd. The live client
watchdog explicitly ignores dormant definitions.

Wake failures are isolated per residency root. A spawn failure leaves its durable
request intact; `wake-failure.json` also retains the delivery reference if writing
intent fails.

A config mismatch does not become terminal at a single Main lease expiry. The
originating Main's valid native lease must have been expired for more than
`max(60_000 ms, 3 * (expiresAt - updatedAt))`, and the existing native liveness
and publication/lease fences must still permit terminalization. The named
`RESIDENT_WAKE_TERMINAL_MIN_GRACE_MS` minimum tolerates delayed renewals rather
than treating a brief scheduling lapse as permanent absence; longer recorded
lease TTLs receive at least three TTLs of grace. Missing or invalid lease timing
cannot authorize terminalization and keeps the delivery retryable.

A retryable config refusal leaves its delivery pending in the
archive-coupled index; ordinary maintenance or a new matching publication can
retry after config repair, subject to the existing backoff. This pending state is
different from a completed terminal decision: its original nudge is ACKed even
though the archived event remains. The `wake-terminal.json` receipt is saved
before that ACK, so interrupted completion can still leave the named delivery
pending for normal recovery. The receipt is per-delivery evidence, not a
disable/retire flag for the residency. For a completed terminal decision, repair
config to match the exact retained restart snapshot; config
repair alone, or moving/deleting the receipt, cannot restore the ACKed nudge.
Preserve the receipt as evidence. To wake and drain the retained archive after
repair, publish a new matching nudge or use the existing explicit client-start
path; an explicit launcher start drains the archived backlog normally. A different
or new pending delivery on the same root should process normally without moving
the historical receipt. No periodic retry service is added. If storage cannot
persist either intent nor the failure receipt, the archive-coupled unacknowledged
watermark remains pending; the already-committed archive remains the delivery
authority. This is reported, not represented as a successful wake.

Direct steer/followUp/ask never wakes during preflight. `wake-routes.json` retains
the host's published participant advertisement for dormant admission,
bound to the exact registry root, actor ownership token and resident host. This
is retained routing authority, not a live lease. Directory availability, owner,
capability, binding and ACK/control-channel checks run before command publication;
the existing control plane also checks its admission state before committing.
Only the admitted durable command's post-commit hook wakes the owner. Rejected
pre-publication requests write no wake request and start no host. The waking host
revalidates ownership/binding and uses the existing claim, outcome and ACK path;
no direct preflight or fabricated ACK constitutes accepted delivery. Older route
snapshots without a published advertisement cannot grant dormant direct admission.

The woken host runs under its own fresh owner incarnation, with no exception for
the retained route's older epoch: it refuses the waking command with
`FABRIC_CONTROL_STALE_INCARNATION` before any claim or handler, and once live it
rewrites `wake-routes.json` with its own published advertisement. The sending
router then re-reads that actor's participant once and, only when the same owner
and actor lineage advertise a different epoch, resends once with the same
idempotency key. The key keeps the command ID, so the owner's claim answers any
duplicate with the one outcome. A second refusal, or a refusal from a target that
was not resolved from a retained dormant route, reaches the caller unchanged.

Before sleeping an actor-bearing host, residency ensures the existing file event
archive is enabled under the mesh publication lock. It preserves an operator-selected
archive, or creates `<mesh>/wake-archive` plus `event-archive.json` when the mesh
previously had only a bounded live log. A bad/unavailable archive cannot authorize
sleep. This is wake archive configuration, not a request-retention collector change,
new inbox or state backend. Sleeping resident monitors replay subscribed ordinary
topics from that archive as well as `fleet.*`; a replay-age window cannot drop
sleeping work. Clean sleep/wake retains the existing queue dedupe and cursor
boundaries, so wake-window deliveries drain once in order (unless the actor
explicitly opts into coalescing). Crash/interrupted-run retry semantics are
unchanged; this does not promise exactly-once external effects across arbitrary
process crashes. Automatic wake uses the existing POSIX fence; durable Windows
Pi launch remains unsupported as noted below. File-backed state remains the authority.

### PR #394 scope cut: recovery supervision deferred

This change keeps cold startup without an archive walk and lease-fenced mesh
consumption/cursor commits. It does not introduce a launcher watchdog, attempt
debt/alarm markers, startup/shutdown supervision receipts, host writer retirement,
or a forced host shutdown-deadline exit. The launcher and host shutdown paths
retain main's existing behavior; no new bounded-close or whole-attempt containment
guarantee is claimed. Those features require a separate fabric-v2 follow-up with
checked whole-attempt exit and completed-drain proofs before recovery admission.

### Owner incarnation fencing

A resident's root-derived host ID (`residentHostId(rootId)`) and durable actor IDs survive relaunch, and a relaunched host replays the control log from offset 0, so a stop or ask meant for the previous process could otherwise reach a durable actor's next turn. Its control-owner incarnation does not survive: each new activation stamps its participant records and claims with a fresh random epoch. Predecessor-addressed unclaimed controls (`stop`, `ask`, `steer`, `followUp`, cancellation) are refused before actor admission, with no exception for a woken dormant actor's retained route. The caller must re-resolve the advertised epoch and explicitly decide whether a fresh request is still wanted; the only automatic resend is the router's single same-key resend of a message it addressed to a retained dormant route (see [dormant wake](#dormant-actors-and-delivery-owned-wake-6782--7299)). Senders addressing older resident releases with no published epoch retain the existing unfenced delivery path with a counted warning. Execution refusals use `FABRIC_CONTROL_STALE_INCARNATION`, while recorded predecessor outcomes retain their original epoch and are never re-executed. See [owner incarnation fencing](agents.md#owner-incarnation-fencing) for sender retry decisions, ACK matching, and the host-only `mesh.controlIncarnationFence` rollout policy: `"warn"` (default) admits older senders omitting an epoch with a counted sender-named warning at most once per hour; `"enforce"` refuses omission after every host has upgraded. Explicit stale epochs are refused in both modes.

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

Generation-private startup errors use `generations/<uuid>.error.json` (seven-day retention preview only; apply remains disabled under smarty-dev#10678); publication stays closed on Windows because Node exposes no verified `O_NOFOLLOW` create/open path there.
Durable residency E2E is POSIX-only today. On Windows, the launcher's spawn of
the `pi` binary through the installed `node_modules/.bin` shims hangs before
the child starts, so the resident host never starts.
The launcher, ownership observation, and protocol logic are platform-agnostic
and are tested on every operating system.

## Future direction

A native Pi extension-host subprocess API could replace `launcher.js` later.
Keep the launcher boundary isolated so that migration changes no residency
protocol or public Fabric API.
