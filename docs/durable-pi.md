# Opt-in Pi durable hosted runner

`pi-fabric/durable` exports `createPiDurableRunner`, `PiDurableRunner`,
`PiDurableRunnerOptions`, `PiDurableStorageOptions`, `PiDurableStorageLease`, and
`PiDurableLocator`. It does not register a runner or change the default Pi runner.
Importing the entry and constructing the factory do not load either optional runtime.

This adapter targets the **published** experimental
`@earendil-works/pi-durable@1.0.0` and `@earendil-works/chord@1.0.0` APIs.
First start/attach checks both installed versions and loads them lazily. Missing or
incompatible peers produce an explicit error. Host credentials remain in `Models`
and the environment, never in the locator.

## Host setup

Install the optional peers only in a host that uses this adapter:

```sh
bun add @earendil-works/pi-durable@1.0.0 @earendil-works/chord@1.0.0
```

```ts
import { createPiDurableRunner } from "pi-fabric/durable";
import { registerAgentRunner } from "pi-fabric/runners";
import { NodeExecutionEnv } from "@earendil-works/pi-durable/env/node";

// Supply a configured pi-ai Models collection and a durable Registry.
// Install only trusted tool implementations in that registry.
const runner = createPiDurableRunner({
  id: "pi-durable", // default; must not collide with another registration
  models,
  registry,
  env: ({ cwd }) => new NodeExecutionEnv({ cwd: cwd! }),
  allowedModels: [{ provider: "openai", modelId: "your-model-id" }],
  storage: { kind: "jsonl", directory: "/absolute/private/durable-runs" },
  // Required for standard residency: "durable":
  // residentModule: "/absolute/path/to/host-registration.mjs",
});
const unregister = registerAgentRunner(runner);

// An AgentManager / agents.run call must explicitly select this runner,
// provider/model, and exact registered tool names:
// { runner: "pi-durable", model: "openai/your-model-id",
//   task: "...", tools: ["read"], recursive: false, thinking: "off" }

// Detach only after the host has stopped delivering requests to this instance.
await runner.close();
unregister();
```

`residentModule` must be a real host-owned ES module that registers the adapter
with the same storage and credentials in the resident process. Merely setting the
path does not create a daemon. A session-resident runner needs no resident module.
The factory does not discover providers or select a default model. Thinking is
passed explicitly (omission means `off` at the adapter boundary); unsupported
levels are rejected; the adapter does not clamp them.

## Storage and ownership

There is one isolated durable Session/root conversation **per Fabric run ID**.

### Built-in JSONL

`{ kind: "jsonl", directory }` opens the published Node JSONL backend with
`fsync: true`. Files live under `directory/sha256(runId)`; an atomic mkdir of
`directory/sha256(runId).writer` grants exclusive ownership. The base directory is
canonicalized before opening the lock and storage, so symlink aliases contend for
the same filesystem lock. A process-global ownership set also rejects two active
adapter instances with the same identity/run ID. An active run retains ownership,
including after settlement, until `close()`.

**No automatic stale-lock stealing.** Any existing lock (including malformed,
empty, unknown or remote ownership) is a refusal, never evidence of death. This
avoids both lease-timeout false deaths and the two-contender read/dead/unlink race.
Locks are intentionally empty and contain no credentials. Keep the directory
private to cooperating trusted hosts; this is not a hostile-filesystem sandbox.

After a crash, the host must:

1. Quiesce **all** recovery contenders/writers for this storage/run.
2. Positively establish that the previous process and any invocation using the
   storage have terminated (e.g. the supervised process's exit receipt). Timeouts,
   old mtimes, missing heartbeats, and unknown PID namespaces are insufficient.
3. Remove only the exact empty `sha256(runId).writer` directory while recovery is
   administratively serialized. Never blindly unlink a possibly replaced lock.
4. Reopen a new factory instance with the same directory and call attach using
   Fabric's persisted locator/context. Do not call start to repair missing work.

SIGKILL recovery is tested with exactly this manual restore procedure. Automatic
unattended crash recovery needs a host factory that supplies an appropriate lock.
JSONL durability remains subject to the published storage/filesystem contract;
this adapter does not promise power-loss-proof filesystem metadata.

### Host-provided storage

```ts
storage: {
  kind: "factory",
  identity: "tenant-a-durable-v1", // stable opaque identity, no secret required
  async acquire(runId) {
    // Acquire an exclusive process/host-wide lease; OS crash-released locking
    // can provide unattended recovery. Open the SAME isolated storage for runId.
    return { storage, async release() { /* release ownership, not work */ } };
  },
}
```

`acquire` must reject conflicting ownership, including other processes and
aliases, and must return a distinct isolated storage namespace per run ID.
It must clean up partial acquisition on failure. The adapter closes storage via
Harness before `release`, never releases while its invocations still run, and
joins accepted open/start operations during `close`. In-memory storage is useful
for tests but is not restart persistence. Failed closure/release fails closed;
its lease remains owned, and competing writers are refused. Well-behaved
host tools must observe cancellation: upstream close joins even tools that ignore
it, so such tools can block close indefinitely.

## Admission, attach, and reporting

Fabric persists the pure JSON locator before `start`. It contains backend/version,
a storage identity hash, and Fabric run ID only. `start` commits a request manifest
and submits with **requestId = Fabric run ID**. Repeated starts, including after
reopen, reuse the committed submission. A changed task/model/tools/cwd/thinking/
system prompt under that run ID is rejected.

`attach` looks up the existing root and `submissionByRequest`; it never creates a
conversation or submits an input. A locator with no admitted submission throws;
the existing HostedRun protocol records the outcome as **indeterminate**, not
retryable work. A crash between manifest creation and submission is therefore
not automatically repaired. Locators must be retained with Fabric's context.

Committed conversation snapshots feed progress (turns, calls, current tool,
partial text), cumulative model/tool usage, and transcript messages/tool events.
Transcript attachment reconstructs the current committed history; it does not
rerun effects or guarantee exactly-once transcript delivery across host restarts.
The final submission maps to completed/text, aborted/stopped, or unanswered/failed.
Liveness is running, settled, cancelled, interrupted, or unknown. Unowned/missing
work is unknown; explicit stop returns unconfirmed when no owned submission exists.

`stop` aborts the owned root's work, including background descendants, and joins
it. `close` instead seals the adapter, detaches observers, closes Harness without
writing an abort outcome, joins invocations, and releases ownership. A new
factory instance can attach afterward. `close` is idempotent, but a closed
instance cannot reopen. This is a resource lifecycle surface, not a sleep/wake
capability. Fabric's own explicit stop/shutdown/deadline policy is unchanged.

## Tools and limitations

Only names in `request.tools` are selected. Unknown names and unknown/disallowed
provider/model keys fail closed before admission. Registered tool definitions
are resolved last-installed-wins, host tool wrappers are applied in registry
order, and selected implementations are pinned in a private per-open registry.
Changes to the host registry do not rewrite in-flight code; reopen resolves the
host's current definitions and refuses missing requested names.

**This is a tool-only Registry integration.** Host extension sections, hooks,
and custom tasks are not installed in the private registry. The request system
prompt becomes conversation instructions; cwd is handed to the host environment.
Tool execution remains trusted host code, not a security sandbox. Host tools can
use their durable execution API; the adapter does not claim to confine arbitrary
host side effects or sub-work they create. Tool result `addTools` and `handoff`
controls are rejected. The requested allowlist stays fixed.

There is no built-in Fabric tool bridge, kernel, or JavaScript continuation
restoration. Replay is the published explicit task/checkpoint protocol: tools
are unsafe by default, and interruption returns an error. An uncertain side effect
is not replayed. Only a host's explicit replay-safe tool declaration
permits replay; the adapter never blanket-marks tools safe. A host-registered
`fabric_exec` implementation is always forced unsafe even if the host accidentally
marks it safe; no Fabric bridge is synthesized.

Unsupported capabilities fail closed: recursive Fabric, kernels, branch/session
seeding, actors/persistent sessions, write confinement, scopes, structured output
schemas, image input, routed questions, explicit compaction, sleep/wake, steering,
and follow-up. Steering/follow-up are intentionally absent: the published queue
semantics are not claimed to extend Fabric's one-submission completion boundary.

## Evidence

`tests/durable-runner.test.ts` exercises the real published packages with offline
faux models, MemoryStorage and fsync JSONL: real AgentManager execution, duplicate
admission, reopen/attach, cancellable unsafe interruption, a killed
process/manual recovery/no replay, exclusive ownership, stale-lock refusal,
close/open races, exact tools/model/thinking, usage/progress, cancellation and
unsupported requests. `tests/optional-durable-startup.test.ts` independently
checks dependency-free import and no automatic registration.
