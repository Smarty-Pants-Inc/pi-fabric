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

The residency protocol is unchanged: `config.json`, `owner.json`, request,
response, and mesh files remain the durable interface.

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
