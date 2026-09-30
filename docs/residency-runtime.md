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

## Removing actors after a Main rotation

A named native Main can explicitly remove a durable actor created by a dead
predecessor of the **same recorded project, agent name and fleet role**:

```ts
await agents.remove({ id: "<actor-id>", successor: true });
```

This is **remove-only**, not adoption. It covers durable actors stored in
both project and predecessor-session registries (including an old session's
`actors.json` that the new Main does not normally load). Foreign session
storage is looked up only by **exact full actor ID**, inside the configured
physical actor root, and a temporary manager denies all runnable ownership
and does not reap the caller's unrelated presence.
Accepted cleanup there can be retried with the same full ID after a reload;
Fabric does not eagerly scan every old session registry at startup.

Omitting `successor` keeps the existing ownership rules. Global templates and nested/remote callers cannot use this
permission. Use the actor's full ID, especially when retrying cleanup.

The path requires fresh absence of the predecessor Main's participants and
leases **and** proof that its recorded Main PID/start-time identity is dead.
An expired lease alone is not proof: a paused Main is refused as `owned by a
live root`. Both roots must have the same nonempty recorded `agentName` and
`role`; actor and root project identities and the actor registry must match.
Malformed/unreadable ownership or lease evidence fails closed.

Before accepting removal, Fabric retires the predecessor Main's process identity
and stops its host by the exact PID recorded in `owner.json`/`host.lock`, only
after checking its kernel start time, complete command line, and ownership token.
The retirement marker is written only after the pre-signal Main liveness
check passes. **After** its durable write, Fabric rechecks the Main owner/liveness,
owner/lock token, and host PID/start time/command line immediately before signalling.
Changed or unknown ownership refuses the stop without a signal and removes this
attempt's marker; an already-dead host needs no signal. It sends SIGTERM, waits up
to 30 seconds for normal host/worker
shutdown, and **does not force-kill** an uncooperative host. A mismatched/reused
PID is never signalled. The marker refuses a host whose configured
`rootOwner.processIdentity` matches the retired `mainIdentity`; a launcher with
no identity is also refused. Other durable actors and durable agent runs of
that old resident host also stop. This is an intentional host-wide shutdown,
not permission to execute its remaining work.

Retirement is **not permanent session retirement**. Resuming the predecessor's
Pi session (`pi --session ...`) keeps the same root id but creates a new live
Main process identity. `ResidencyClient.ensureHost` rewrites `rootOwner` with
that live identity before launch, so its durable actor and agent operations
work again; an old launcher carrying the dead Main's identity remains fenced.

A dead resident host does **not** prove its detached activations stopped. Before
acceptance, Fabric checks every predecessor resident run and the actor's retained
run records, including every recorded launch attempt and nested worker/runner.
Each recorded process must be kernel-proven dead or no longer match its recorded
start time/command line. Missing, malformed, unresolved or unknown evidence returns
`{ removed: false, pending: "Removal unaccepted: ..." }`; actor files and ownership
remain intact. Fabric never signals these foreign workers. A later explicit remove
can complete after they finish. A clean host close saves a durable settlement
receipt before its run directory disappears; a new host invalidates that receipt
before admitting work. Legacy runs without process evidence require explicit repair,
not an inference from an expired lease or absent owner files.

The registry lock rechecks the proof and accepts a stopped removal atomically.
The normal `ActorManager.remove` transaction then handles pending removals,
registry revocation and durable cleanup obligations. No queued activation is
adopted or run. Only the requesting Main may finish that accepted deletion;
reloads and cleanup retries use the same path. A durable
`actor-removals/<actor-id>` mesh receipt records the successor identity.

Process evidence is currently Linux `/proc` evidence bound to the same kernel
boot and PID namespace. A different host/boot/namespace is unknown, not local
process death, and is refused. Other platforms and
legacy dead roots without a recorded Main PID/start time are refused, rather
than guessing a PID from a name or command pattern. New roots record their
Main PID/start-time identity in participant/config records (without copying
Main command-line arguments); hosts record their full process identity in both
owner and lock records. There is no unsafe legacy override or `agents.adopt`
API in this change.

When replacing several actors, **abort before any create if a remove throws
or returns `cleaned: false`/`pending`**. Do not swallow removal failures and
create duplicate review/security actors.

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
