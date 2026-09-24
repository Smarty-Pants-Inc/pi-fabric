# Cooperative bash middleware

A captured `bash` override normally owns execution. Fabric cannot safely substitute
its local runner for an arbitrary override (for example, an SSH backend or a
security gate). Consequently such overrides do not automatically gain Fabric's
background jobs.

An extension implementing **standard local bash plus environment/output filters**
can explicitly hand nested `pi.bash` execution to Fabric by attaching a host-local
capability to its registered `ToolDefinition`:

```ts
import type { BashOperations } from "@earendil-works/pi-coding-agent";

const middleware = {
  version: 1 as const,
  options: { spawnHook, shellPath, commandPrefix },
  wrapOperations: (local: BashOperations): BashOperations => redactOutput(local),
};

pi.registerTool(Object.assign(standaloneBashDefinition, {
  [Symbol.for("pi-fabric:bash-middleware:v1")]: middleware,
}));
```

`FABRIC_BASH_MIDDLEWARE` and `FabricBashMiddlewareV1` are also exported from
`pi-fabric/protocol`. Extensions may mirror the symbol key to avoid a runtime
package dependency. Neither callbacks nor secret values belong in tool argument
schemas, guest payloads, or discovery metadata.

## Ownership and filter order

- The standalone definition remains registered. Without compatible Fabric, Pi
  executes it normally, so it must still include the same protections itself.
- Opt-in explicitly replaces the override's `execute` and argument preparation
  for nested `pi.bash` calls with Fabric's standard local-shell semantics. The
  schema gains `cwd` and `background`; aliases, lifecycle hooks, policy blocking,
  hard timeouts, cancellation, and session-owned job cleanup remain active.
- `options` accepts `shellPath`, `commandPrefix`, `spawnHook`, and
  `exposeSessionEnvironment` (Pi's `BashToolOptions` except `operations`). The spawn
  hook runs on a copy of the host environment including Pi session metadata;
  remove child-only secrets without mutating the host's provider credentials.
- Fabric supplies the local operations backend. `wrapOperations` must delegate
  command/cwd, environment, timeout, and abort signal to it. It may filter streamed
  output **before** forwarding `onData`; Fabric's job buffer/log and Pi's
  truncation file see only these filtered bytes. Handle split UTF-8/secret chunks
  and flush safely on exit, errors, and cancellation.
- A handoff never disables an explicit `timeout`: it can still kill an already
  backgrounded child. `executor.shellHangMs: 0` disables automatic handoff, not
  explicit `background: true`.
- Middleware factories and hooks may close over current policy. Fabric reads the
  active captured definition for each invocation without caching a prior
  extension generation. The selected protection remains pinned across that
  invocation's awaited lifecycle hooks. There is no load-order handshake or need to register another `bash` tool.
- Invalid capabilities or failing factories reject the call; Fabric never retries
  through an unfiltered runner. Closed-world managed hosts still execute their
  authorized override, not Fabric's native process backend.

Version 1 is intentionally bash-only. It is trusted extension cooperation, not a
sandbox or a security boundary. Unrelated overrides and direct `extensions.bash`
calls retain their original behavior.
