# Harness CLI composition

Fabric follows `jev-fabric`'s shell-first boundary: compose existing programs,
not application-specific tool bridges. Browser Harness JS and macOS Harness own
their connections, persistent state, native APIs, permission checks, and guarded
interaction. Fabric supplies its existing shell and task supervision; Jev is an
optional typed decision-maker. No harness-specific Fabric extension, SDK module
path, component entry, or provider registration is required.

## Browser Harness JS

Load the harness-owned `cdp` skill. After the user authorizes a browser scope,
use its normal CLI through `pi.bash`, with `--no-auto-allow` so Fabric does not
silently approve a browser debugging prompt. For a reviewed snippet saved by
`pi.write`, the command shape is:

```ts
return await pi.bash({cmd:"browser-harness-js --no-auto-allow < ./observe-browser.js",timeout:15,settle:true});
```

This is a composition pattern, not permission to connect or scan. The CLI owns a
persistent daemon, session, and explicit target routing; Fabric does not import
the SDK or create a second connection. Use the harness's authorized connection
setup and guarded controller at unknown UI boundaries. Preserve explicit target
scope and fresh observation handles. The CLI prints raw results, not a Fabric
provider envelope: validate the documented result, exit status, and size before
parsing. Empty output is not automatically valid JSON or proof of success.

A short CLI process exiting does not stop its daemon. Follow the harness's own
cleanup contract; stopping a Fabric task is not rollback or daemon shutdown.
Do not stop a shared daemon owned by another workflow.

## macOS Harness

Load the harness-owned `macos-harness` skill. Native access needs separately
approved OS permissions and explicit already-running app scope. The guarded
server is an ordinary command:

```sh
macos-harness serve --app com.apple.TextEdit
```

Keep **one** process alive for a multi-step workflow. Its stdin/stdout protocol
is newline-terminated UTF-8 JSON, `{id,method,args}` to `{id,result}` or
`{id,error}`; stderr contains diagnostics. A reviewed task-specific native script
can own the child, serialize requests, validate response IDs/errors, enforce
bounds/deadlines, and close stdin/terminate it during cleanup. Run that script
through the authorized shell, e.g. `pi.bash({cmd:"node ./desktop-task.mjs"})`.
This does not change Fabric's configured kernel or grant an unrestricted fallback.

`pi.bash` is not an interactive stdin handle. Do not launch `serve` as an isolated
background command and expect `tasks.watch` to become RPC. Monitors and log tails
are bounded observations and may omit/truncate data. Do not restart the server
between observe and act: native controller handles do not survive process exit.
For one-shot deterministic work, the harness's ordinary CLI/library can keep the
whole observe/act/check sequence in one invocation.

## Supervision and decisions

Use [background tasks](background-tasks.md) for finite shell deadlines,
`tasks.wait`, literal-filtered `tasks.watch`, and `tasks.stop`. UI-only monitors
never wake Main or invoke Jev. A program may explicitly evaluate a remaining
semantic question over minimized output; see [Jev](jev.md). Validate exact status
and protocol facts first, map typed answers to code-owned branches, and verify
postconditions after effects. Never run a model answer as shell code.

Preserve the harness's guards:

- `executed` means dispatched, not goal success; check fresh authoritative state.
- `stale` means re-observe; never reuse stale handles.
- `blocked` means resolve permission/scope or stop; raw APIs cannot bypass it.
- `outcome_unknown` means inspect, never blindly replay.
- Never approve permission dialogs, activate apps, or move the physical cursor
  as a workaround. Keep secrets and unrelated private data out of model state.

Shell execution runs with host privileges. QuickJS program isolation is not a
sandbox around the shell or a substitute for the harness's authorization rules.
Cancellation is not rollback; detached processes/daemons have separate lifetimes.

## Migration

Remove unneeded `browser-harness` / `macos-harness` entries from your Fabric
configuration and stop loading their optional `pi/` extensions when switching
to CLI composition. Do this deliberately after stopping their work: no automatic
configuration rewrite or cleanup is performed. Replace `browser.*` / `macos.*`
program grants with only the shell/task actions actually needed.

There are no concrete harness exports in `pi-fabric/jev` and no compatibility
bridge to maintain. The generic [component protocol](components.md) remains
available for independently installed providers; this change does not remove it
or modify the sibling harness repositories. Harness-specific docs/skills in those
packages remain authoritative for their standalone CLIs.

Offline checks use local fixture processes and mocked judgments. They do not
connect to a personal browser, control apps, install tools, or spend API credits.
