# Agents, actors & mesh

Fabric exposes its multi-agent runtime through the model-facing APIs in [`skillsets/typescript/fabric-exec/references/agents.md`](../skillsets/typescript/fabric-exec/references/agents.md) and [`mesh.md`](../skillsets/typescript/fabric-exec/references/mesh.md). Reusable patterns are in the [skills](../skillsets/typescript/): `fabric-workflow`, `fabric-swarm`, `fabric-council`, `fabric-rlm`, `fabric-supervisor`, `fabric-advisor`, and `fabric-fusion`. For the `agents` and `mesh` settings, see [configuration](configuration.md).

Python uses the same public skill names from the [Python tree](../skillsets/python/), with native `agents.run` and `asyncio.gather` programs and its own [agent reference](../skillsets/python/fabric-exec/references/agents.md). The helper examples below are TypeScript-only.

## Workflows

Fabric programs keep orchestration and intermediate values in code. The workflow globals provide Claude Code-style names and progress phases without adding another JavaScript runtime.

Use these helpers:

- Use `workflow.agent(prompt, options)` or `agent(...)` to start one worker. Set `label` on each call.
- Use `workflow.parallel(thunks, { concurrency })` or `parallel(...)` for fan-out. Supply functions to these APIs.
- Use `workflow.pipeline(items, ...stages)` or `pipeline(...)` to run sequential stages for each item with concurrency across items.
- Use `workflow.configure({ name, description })` to name the activity surface.
- Use `workflow.phase(name, { id?, description?, total? })` or `phase(...)` to define progress groups.
- Use `workflow.item(...)` for non-agent work items that change status over time. An optional stable `id` (1–128 characters of `[A-Za-z0-9._:/-]`) keeps one item across updates; without one, each call creates `item-<n>` in invocation order. An optional `meta` plain JSON object (at most 2 KiB serialized) rides only on the host [`pi-fabric:workflow-item:v1`](providers.md#workflow-item-events) event. Invalid ids, statuses, or meta fail the call.
- Use `workflow.event(...)` to record important milestones in the dashboard feed.
- Use `workflow.log(...)` to add short progress notes.
- Read `workflow.budget` for token-budget observations.

You can give `fabric_exec` optional `agentBudget` and `tokenBudget` limits. Configuration sets a hard agent limit for each execution. Add a JSON Schema to an agent request to make the worker return validated structured data in `result.value`. Workflow helpers return this value directly. Without a schema value, they return the agent's final text. See [`/skill:fabric-workflow`](../skillsets/typescript/fabric-workflow/SKILL.md) for the complete pattern.

## Agents

Fabric injections carry structured [turn provenance](turn-provenance.md) on capable Pi hosts. The sender is the admitted participant; message text cannot select a human channel or principal.

`agents.wait({id})` waits for a spawned agent; `agents.join({id})` is an alias with identical arguments, result, progress, and notification behavior. A wait is bounded by `timeoutMs`: 5 minutes by default, and a larger value is clamped to 5 minutes, the limit of the foreground bash guard, because a wait holds its session in the foreground (smarty-dev#854). A child that is still running at the bound keeps running, the wait throws, and the child's result arrives as a completion message after the turn. In an interactive Main (TUI or RPC; not a task agent, actor, or print/JSON run), the bound is 60 seconds and reaching it is not an error: the wait returns the child's live status record (`status: "running"`) with `waitTimedOut: true`, so Main is back at a tool boundary where held followUps land (smarty-dev#2119). Use `wait` as the canonical spelling. The hosted `AgentService` and `AgentServiceClient` expose both methods too. [Jev programs](jev.md) follow the same `wait`/`join` naming.

### Background completion inbox

`agents.spawn` validates the request and returns a handle. With a free concurrency slot, Fabric launches the worker and returns a `running` handle. When every slot is occupied, it returns a `queued` handle without waiting for admission. `agents.list` and `agents.status` show queued runs with a one-based `queuePosition`. Fabric admits them in FIFO order as running children finish. Queued spawns count against `maxPerExecution` and the calling program's `agentBudget`; cancelling one does not refund that count.

`agents.wait`/`join` can wait on a queued handle through admission and completion. Their wait bound includes time spent queued. `agents.stop` removes a queued run without launching its worker and settles its result as `stopped`. A returned queued handle belongs to the session: returning from, timing out, or aborting the calling `fabric_exec` program leaves it queued. Session shutdown still stops session-owned queued and running children. These admission rules apply to every local worker transport. Queued receipts are currently session-only: a saturated durable spawn cancels its accepted queue entry, safely rejects, and returns no queued handle. If a cancellation races worker creation and exit cannot be confirmed, Fabric retains its run/worktree files, reports cleanup pending, and refuses cleanup until the worker is checked manually.

Independent work can continue without polling. With `agents.notifyOnComplete` enabled (the default), a concise UI notice appears when a detached run finishes. Full outcomes remain in agent activity and logs. Unread results are batched into Main's context after the current assistant turn's entire tool batch, without waiting for its final answer. If Main is idle, unread results wake it once.

`agents.wait`/`join`, supervisor-settled terminal `agents.status`, and cleanup acknowledge the result and retract any pending notification, including completion that arrived before the wait. Running status, provisional terminal attempt status during retry/resume, and UI/list polling do not acknowledge results. Acknowledgment means the Fabric program received the result: return the relevant outcome to Main when it needs to reason about it. Prefer `wait` over a polling loop. Fabric refuses a foreground `bash` call, native or through `pi.bash`, whose sleeps add up to more than 5 minutes: a long `sleep`, a sleep in a counted `for` loop, a sleep in a `while` or `until` loop without a `timeout`, a sleep whose length is not a literal (`sleep $((t-now))`), or a `flock -w` wait. The tool call's own `timeout` or a literal `timeout N` bounds the estimate. A session that waits in the foreground takes no steer or ask. Start the poll detached, or wait for a completion message or a mesh event, and end the turn.

Durable spawns use the same inbox. Undelivered envelopes survive disconnects; receipts survive reconnects. Escape or an errored Main turn parks pending results: Fabric does not start a turn to deliver them, and they join Main's next turn, whatever starts it (typed input, a peer message or another trigger). Explicit lifecycle subscriptions, actor messages, and trajectory handoffs retain their separate delivery policies. A terminal run can still report incomplete work; Main must inspect its result.

Inbox delivery receipts require the completion carrier in Pi's actual session JSONL, with a matching session header and successful file/namespace durability barriers. Pi's in-memory entry list, a failed append, a fresh unflushed session, and tree navigation are not receipts; the durable source stays unread. In-memory-only hosts cannot confirm durable inbox delivery.

Completion recovery keeps worker-attempt candidates separate from the supervisor's settled logical-run outcome. A failed startup or recoverable stop is not delivered or returned by a durable wait while its pinned supervisor can still retry it. An orphan candidate becomes eligible only after that supervisor process is gone; a settled outcome takes precedence. Legacy launch manifests without a supervisor identity cannot prove orphan settlement, so their worker status stays retained and is not automatically promoted.

Full journal results are readable only by the original recipient session or its validated, claimed exact-lane successor (same project root, cwd, name and role). Other sessions see bounded status metadata and the pending/addressed-to state, never private task, text, error, structured value, usage or log/session details; their reads do not acknowledge the recipient's result. Receipted outcomes do not replay to later successors; a successor cannot clean up or read logs for predecessor-owned runs. Journal result availability is not durable-run operational ownership: ordinary local runs continue to use their local manager for log and cleanup. Durable receipts fence replay. An unreadable, malformed, or identity-mismatched fence fails closed: no successor claim, private body access, delivery, or replacement receipt; the source is retained and a storage diagnostic is surfaced until repaired. Their live mesh claims are retired with ownership/version checks and crash leftovers are reconciled even when notifications are disabled.

### Actor children and reply targets

A child's immediate spawner is distinct from its lineage root. `agents.spawner()` returns `{ id, kind, runId? }`; `agents.followUp({ id: "spawner", message })` and `agents.steer` accept that bound target. Inside an actor-spawned task, it names the actor and its spawning activation run, not the implementing lead's Main. For process tasks, `agents.main()` / `id: "main"` also use the immutable immediate-spawner return address described below; the root topology fields stay unchanged. An absent spawner binding fails without falling back to the root; old workers must be respawned to get the new binding.

Automatic completion goes to the spawning actor's live run inbox. If that activation ends before consumption, a write-ahead result is transferred once to that actor's persisted mailbox for its next activation. Wait/status consumption or live-inbox delivery suppresses a later mailbox copy. The mailbox envelope includes `data.resultFile`, an actor-local JSON file containing the full result (including structured value), so a later activation can read more than the bounded notification. Consumed foreground/live results leave no full-result archive; a mailbox handoff retains its result until that activation consumes it. Unread results and receipt tombstones are bounded by `retention.actorRunArchiveMs` on the existing owner sweep (pending mailbox items are protected). Stopped actors retain unread results within that retention window; Fabric never silently delivers them to Main as Main-owned children. A session-owned child still stops when its spawning worker shuts down; that stopped outcome is preserved, not a promise that the child survives shutdown.

Live preparation never consumes individual outcomes: only a completely prepared batch gets one atomic pre-send receipt, and its full results remain until delivery cleanup. As with any uncertain `sendMessage` outcome, a committed pre-send claim chooses at-most-once delivery. Foreground `run`/`wait`/terminal `status` commits durable consumption **before returning the value to the actor program**, not in a best-effort post-delivery receipt. A transient receipt failure is retried once; persistent failure rejects the observation without returning an unrecorded result. Archive unlink failure does not undo consumption or fail a returned result. A mailbox activation refused by `validWhile`, unable to start its worker, or failing without actual model output/tool execution retains the full result in a persisted deferred-handoff side store, outside the runnable FIFO. The next valid activation receives labelled unread-child context with the original activation facts and `data.resultFile`; it does not retry the stale handoff as the current activation. Owner restart restores this side store but does not independently launch a retry. Successful inference consumes the attached snapshot and removes its full archives. Deferred handoffs expire with `retention.actorRunArchiveMs`.

**Review-role mitigation for older installations: synthetic (unit-tested), not independently native-Pi verified:** this mitigation's evidence is separate from the native actor-child routing proof; do not attribute that proof to this workaround. For bounded subtasks, keep the result in the same `fabric_exec` program and wait before deciding the verdict:

```ts
const result = await agents.run({
  task: "Review this bounded part of the diff; return your findings. Do not message Main.",
  transport: "process",
});
return result;
```

`agents.run` includes the wait; alternatively `spawn` followed by `await agents.wait({ id: child.id })` in the same program acknowledges the result. Inspect its terminal status/error and do not declare review complete if the wait or program deadline expired. On updated workers, addressed progress can use `id: "spawner"`; no `fabric_reply` action is required for ordinary task-agent final results. Do not substitute `id: "main"` for an actor reply target.

### Native runner session attribution

Pi runs record the live native Pi session ID in `runnerSessionId`, including
`--no-session` task agents and durable actors owned by Main or a resident host.
This is the ID sent upstream as the gateway's `session_id`; it is **not** the
transport `sessionId` (for example, a process PID) or the Fabric run ID.
`agents.status`, `agents.wait`, and run listings expose the latest native ID.
If Pi replaces its session during a run, `runnerSessionIds` keeps the distinct
observed IDs in first-seen order, while `runnerSessionId` follows the latest.

Run records also retain `mainAgentId` and `fabricSessionId` for the parent Main,
alongside the task name and, for actor activations, `actorId`/`actorName`.
Actor run copies retain these fields in `runs/<run-id>/status.json`.
The `pi.agent_start` and terminal `run.*` lifecycle payloads carry
`runnerSessionId` and the parent `fabricSessionId` when available.
Native identity comes from Pi's live session manager, not a pre-launch session
header: Pi can replace a header-only session's seeded ID during startup.
No new store or configuration is required.


### Stalled Pi error recovery

After a failed or aborted assistant response (including `Error: Terminated`), Fabric allows Pi's own retries to recover. The recovery watchdog honours native `auto_retry_start.delayMs` announcements with the announced delay plus 60 seconds of slack, capped at a 10-minute recovery incident. Errors and lifecycle chatter without a delay do not extend the deadline. Nonempty text/thinking/tool-call deltas refresh the no-progress timer; a successful assistant response clears recovery. Healthy inference and tool execution retain their existing deadlines, and the overall run deadline still applies.

Process Pi task children use native retry settings of `retry.maxRetries: 6`, `retry.baseDelayMs: 5000`, and `retry.maxAgentDelayMs: 160000` (5, 10, 20, 40, 80, 160 seconds) through a child-local agent settings file, only when neither the user's agent settings nor project settings specify `retry`. The original settings are unchanged. Each task keeps a native `session.jsonl` in its run directory. A child that exits on overload, transient throttling, server errors, or a premature disconnect before its final answer can resume that exact session after 30, 60, then 120 seconds, at most three times within the same 10-minute incident bound. Stops, aborts, account/quota limits, deterministic errors, and final answers prevent resume. Recovery announcements appear in run events, `run.resumed` lifecycle entries, and status warnings; exhausted recovery keeps the session on disk until normal run cleanup.

Child shutdown after RPC stdin closes is bounded: 5 seconds for graceful exit, then SIGTERM and a further 5 seconds before SIGKILL. Recovery failures settle `agents.wait`/`join` and notify detached callers normally. Same-session recovery preserves prior conversation and completed tool results; it sends a short continuation prompt without replaying the original task. Already-running workers retain their loaded implementation; newly launched workers use the rebuilt package.

### Fleet write attribution for process children

Ordinary process workers and their CLI children receive `SMARTY_ROLE=task-agent` before
exec. A valid parent `@SHA` stamp is retained as `task-agent@SHA`: `smarty-role` stamps
all session roles with the shared org repository revision, not a role-specific content hash.
An unstamped parent stays unstamped; Fabric does not invent provenance. The fleet write
governor derives their lane from the child's cwd, not from a role or a lane environment variable. Explicit actor runs retain their inherited role
and set `PI_FABRIC_ACTOR_NAME`; that actor identity takes precedence in the governor. An
ordinary task spawned by an actor clears `PI_FABRIC_ACTOR_NAME`, so its governed
writes count as the task-agent writer, not as the spawning actor. This is write attribution, not an authorization boundary.
Ordinary tasks drop the spawner's `PI_FABRIC_ROLE` override and `SMARTY_READ_CLASS`,
so participant discovery cannot still report the parent's role or critical-read class.
Explicit actor runs retain both. Parent environment and bound session/mesh routing are unchanged.

Task agents return status to their parent; they must not call `smarty-status` to update the
parent's status comment. That helper keys ordinary comments by role/worktree, so a task
agent's call would create a separate `task-agent/<worktree>` comment and leave the
parent's unchanged. The parent owns and writes its status updates. No parent-role environment variable
is exported for status impersonation.

The installed admin audit's `actor()` likewise records `PI_FABRIC_ACTOR_NAME`, else
`SMARTY_ROLE`. Non-actor session roles rendered by `smarty-role --format fabric` (including
security passes and acceptance auditors) therefore execute and are recorded as `task-agent`;
the role instructions describe the assignment, not a separate process identity. This coarse
attribution is intentional for delegated work: it identifies the actual task-agent writer
without claiming the parent's role. It does not identify the named review assignment; retain
that provenance in the Fabric run/task and review receipt. Work requiring a distinct session
role in the admin audit must use a separately role-launched root session, not an ordinary task
agent. Fabric does not change those external helpers or their audit schema.

### Spawn-bound task return address and escalation guard

For process-transport tasks, `agents.main()` returns the **immediate spawner's exact
participant id and native Pi session id**, captured at launch. Sending to `"main"`
uses that same address. Recursive tasks may return to their task/actor spawner;
Fabric does not discover a replacement by name, role, or principal. The original
`PI_FABRIC_MAIN_AGENT_ID` and `PI_FABRIC_SESSION_ID` remain the root topology and
storage fields; they are not overwritten with a nested task's return address.

A process task's `agents.steer`, `agents.followUp`, or `agents.tell` to a Main may
address only its bound spawner, that spawner's ancestor chain, or an explicitly
allowlisted exact Main session. Other Main-bound sends fail before publication
with `TaskEscalationTargetError` (`FABRIC_TASK_ESCALATION_TARGET_DENIED`), naming
all allowed ids. Task-to-task/actor sends retain existing routing; Mains, explicit
actor activations, and other transports are not subject to this guard. A task
spawned by an actor remains a task (the inherited actor id is cleared), while the
actor name remains available for existing fleet write attribution.

**Launcher/helper contract (smarty-dev#2950 / #2982):** set
`PI_FABRIC_TASK_ESCALATION_TARGETS` on the spawning Pi process **before its Fabric
runtime is initialized** to a JSON array of exact session targets, for example:

```sh
PI_FABRIC_TASK_ESCALATION_TARGETS='["session:<product-owner-org-session-uuid>"]' pi
```

The shared helper must obtain that session from the product owner's explicit org
instance binding, never from a role/name lookup. The default/unset value is `[]`;
invalid JSON or non-`session:` entries are rejected. This smallest implementation
uses the environment contract, not a new `fabric.json` key or public spawn option.
The manager snapshots the list and propagates it unchanged to recursive process
children, so later ambient changes cannot retarget a running child's sends.
Stop/respawn children to apply a changed binding or allowlist.

Fabric, not the external helper, sets `PI_FABRIC_SPAWNER_ID`,
`PI_FABRIC_SPAWNER_SESSION_ID` (the actual native Pi session, not a PID),
`PI_FABRIC_SPAWNER_CHAIN` (JSON exact ancestor ids), and
`PI_FABRIC_TASK_PROCESS_CHILD=1` through the manager-owned launch snapshot. Do not
set those manually. These are wrong-recipient safety checks within the trusted
local process boundary, not a sandbox against a hostile same-UID agent.

### Image-heavy lifecycle events

Pi repeats message history in `agent_end.messages` and tool results in `turn_end.toolResults`. Fabric streams past these redundant top-level fields, recording empty arrays in the worker event log. Large accumulated histories therefore do not trip the event-size guard or interrupt completion/retries. Authoritative message/tool events, final text, usage, and Pi's persisted session history are unchanged.

The 4,194,304-character guard still applies to other event data, including a single oversized authoritative message. It is not a model token limit. Already-running workers keep their loaded implementation; newly launched workers use the rebuilt/updated package.

### Reuse discovered model keys

Use `agents.models({ runner: "pi" })` and copy the returned `key` verbatim, or use an explicitly configured `models.aliases` name. Reuse the `model` returned by a successful spawn. Do not reconstruct it from an agent's display name. For example, an agent named “Sol” need not share the version number of one named “Astra”. Prior success with one key does not validate a different key.

`provider/model` selectors prefer an exact visible match. A near-miss ID resolves to the closest available model on the **same provider**, using Fabric's existing similarity ranking; ties prefer recent usage, then canonical key order. For example, `openai-codex/gpt-6-sol` resolves to `openai-codex/gpt-5.6-sol` when that is the closest visible model. The returned handle's `model` and the worker's `requestedModel` contain the canonical selection. Unknown providers and names without sufficient resemblance still fail before launch. Model IDs containing `/` stay provider-scoped. Configured aliases keep their ordered, exact-target fallback chains.

For independent launches, await `Promise.allSettled` and inspect every result. An uncaught `Promise.all` rejection ends the Fabric program and can abort sibling calls still in flight; it does not prove every requested model was unavailable. Already completed calls are not rolled back, so retain successful handles and retry only failed launches.

```ts
const requests = [
  { task: "Review src/guard.ts", model: "anthropic/claude-sonnet-4-6" },
  { task: "Review src/guard.ts", model: "openai-codex/gpt-5.6-sol" },
];
const results = await Promise.allSettled(requests.map(request => agents.spawn(request)));
return results.map(result => result.status === "fulfilled"
  ? { ok: true, handle: result.value }
  : { ok: false, error: String(result.reason) });
```

### Parent inheritance and fleet model policy

Without an explicit `model`, Pi children and live actors inherit the spawning run's **actual admitted model and thinking**, ahead of `agents.model` / `agents.thinking`. This includes actor/task parents, not just Main. Explicit model/effort choices still win; a different runner does not inherit a Pi model. Global actor templates retain deferred inheritance until import.

The trusted host's `<agentDir>/fabric.json` can set:

```json
{ "agents": {
  "deniedModels": ["cliproxyapi/gpt-6-astra", "cliproxyapi/gpt-6-sol"],
  "deniedModelReplacement": "cliproxyapi/gpt-6.1-sol"
} }
```

These policy keys are ignored in project/workspace `.pi/fabric.json`, even in trusted projects. The default deny-list is empty. Fabric checks requested selectors and canonical selections case-insensitively, including aliases, inherited models and defaults, before spawn/create or model-setter mutation. A denial raises `FabricModelDeniedError` (`code: "FABRIC_MODEL_DENIED"`), names #2236 and the configured replacement, and never silently falls back to another model. Alternate runners also admit the backend selector produced by their argv normalizer (including `veda/`, `claude/` and `anthropic/` routing forms). Under active policy, Claude aliases must have an allowed native CLI catalog `resolvedModel` (checked as both a runtime ID and `anthropic/<id>`); Veda requires backend `pi` and an exact concrete `provider/model` resolved by the Pi registry. Unknown targets, Veda aliases/bare IDs/defaults and other Veda backends fail closed with the same typed refusal before queueing or durable submission. Allowed canonical targets, not unresolved selectors, are forwarded to workers. In-place Prewalk checks policy at manual/automatic arm and again before switching Main, and denied binding clears preserve the old binding when the actual owning-session fallback is denied. The fixed refusal code is preserved in public TypeScript guest catches; arbitrary host error properties are not transferred. Deploy the host policy to enforce the fleet list; rebuilding does not retroactively change existing workers or resident owners. See the [public-path CLI proof and installation-only owner gate](model-policy-acceptance.md).

### Explicit model exceptions (#3134)

`agents.spawn`, `agents.run`, `agents.create` (including `createActor`) and actor
`agents.setModel` refuse an explicit Astra selector unless `modelReason` is a
non-blank string of at most 200 JavaScript characters. The default match is
`/(^|\/)gpt-6-astra/`: bare IDs and provider-qualified Astra variants are covered.
No model is silently substituted. The error names the configured runner role
default and tells the caller to omit `model` or supply a named exception:

```ts
await agents.spawn({ task: "Bounded compatibility probe", model: "cliproxyapi/gpt-6-astra",
  modelReason: "Named exception: reproduce an Astra-specific parser failure" });
```

Only the trusted host's `<agentDir>/fabric.json` may override the list:

```json
{ "agents": { "modelPolicy": { "requireReason": ["cliproxyapi/gpt-6-astra"] } } }
```

Bare entries match a model-ID prefix on any provider; provider-qualified entries
match that provider/model prefix. Matching is case-insensitive. The default list
is `["gpt-6-astra"]`; `[]` disables the gate (rollback). Project/workspace policy
entries are ignored even in trusted projects. Omitted `model`, inherited role or
project defaults, Sol and other unlisted models are unaffected. Existing actors
continue unchanged; an explicit `setModel` request needs its own exception.
Aliases are tested as the explicitly requested selector, not their resolved target.

The reason is retained verbatim on run records and actor definitions/session
bindings, and follows the effective binding into activation run records. Clearing
or replacing a binding clears its old reason. The `run.spawned` lifecycle event
is emitted after actual worker registration (not queue admission) with
`data.model` and, when supplied, `data.modelReason` for metering.

### Requested models are authoritative

For Pi workers, Fabric reapplies the resolved `provider/model` over RPC **after startup extensions finish**, reapplies the requested thinking level, and independently reads `get_state` before sending the task. A successful `set_model` response alone is insufficient: it can echo the requested model even when an extension switches away during `model_select`. Thinking is reported at Pi's effective, capability-clamped level.

With `thinking.bounds` configured, Fabric clamps each run's level into the bounds before launch and reports the original as `requestedThinking`. `agents.run` and `agents.spawn` also accept `thinkingBounds: {min?, max?}` inside the caller's bounds; children inherit the effective bounds and never widen them. See [thinking control](thinking.md#child-runs).

Startup waits for a correlated RPC readiness response. Startup, model admission, and task execution share the run's configured `timeoutMs`; there is no separate 15-second admission cap. RPC readiness does not guarantee a fast `set_model`: authentication checks and async `model_select` hooks can still wait on provider initialization or shared resources during concurrent launches. A slow handshake may use the remaining run budget, but it never resets or extends the overall deadline.

Selectors with no available match, rejected/malformed RPC responses, or a remaining model mismatch fail the run without sending its task. A startup or admission stall exhausts the overall deadline with `timed_out`, also without sending the task. Queued controls wait for admission too. After parent-side selector resolution, the worker must use that canonical model: it does not substitute an MRU model or fall back during admission. Standalone `AgentManager` callers can also supply a unique exact bare model ID; ambiguous IDs must be provider-qualified.

`requestedModel` preserves launch intent in run records; `model` follows verified child state and actual assistant attribution. The manager and participant UI preserve that observed value; the launch label cannot overwrite it. If assistant attribution drifts after admission, Fabric terminates the child and reports both requested and observed models in the failure. This verifies Pi's local provider/model identity, not a remote provider's internal routing.

Existing workers are not retroactively changed by rebuilding or reloading the parent. Stop and respawn affected workers to apply model admission.

Pi workers with extensions enabled also inherit the parent session's `pi-multiprovider` `/switch-account` pin, when that extension is installed. Fabric forwards the pin as `PI_MULTIPROVIDER_SESSION_PINS`; the child rebinds it to its own session id. This is host state, not a model argument. Claude and Veda runners, and `extensions: false` children, do not receive it.

### Choose the child's language

`agents.run`, `agents.spawn`, `agents.create`, and `agents.handoff` accept `kernel: "typescript" | "python" | "inherit"`. Omit it or use `"inherit"` to inherit the caller's `executor.kernel`; use a concrete value so a skill or model can choose its strongest language for the task. Workflow agents, `rlm.query`, and council members/synthesis forward the same option. This selects the **child's** Fabric language, not the language of the current `fabric_exec` program; that program still uses its configured kernel.

TypeScript caller example:

```ts
return agents.run({
  task: "Analyze the dataset and report a compact result.",
  runner: "pi",
  kernel: "python",
});
```

Python caller example:

```python
return await agents.run({
    "task": "Inspect TypeScript API compatibility and report concrete findings.",
    "runner": "pi",
    "kernel": "typescript",
})
```

Concrete kernels require the Pi runner with Fabric extensions enabled. Claude, Veda, and `extensions: false` reject concrete choices before launch; omitted/`inherit` keeps those runners native and their status has no Fabric kernel. Invalid kernel values reject without guessing. A supported run's handle, result, and status record expose the resolved `kernel`, never `inherit`. Explicit kernels and inherited Python load the Fabric extension/tool even when the parent is outside full-code mode.

Language and configured Python backend policy (`executor.pythonRuntime`, default `monty`) are frozen before launch and forwarded to workers, recursive children, alternate-cwd children, and resident handoffs. There is no public per-request backend selector. Monty is the sandboxed default and fails closed if its optional dependency is unavailable; it never falls back to CPython. CPython is an explicitly configured native escape hatch, trusted outside schema enforce; selecting Python does not promise the same isolation or library support across backends. See [execution kernels](kernels.md).

Persistent actors freeze their language and Python backend when created. `ask`/`tell` cannot switch either; recreate the actor for a different language. Do not mix languages in one persistent session. Global templates preserve omitted/`inherit` kernel selection until import into a new actor; an explicit template kernel stays explicit. A trajectory handoff freezes the caller choice when scheduled and keeps the original session history unchanged.

```ts
const result = await agents.run({
  name: "security-review",
  task: "Review the current diff for concrete security defects. Do not edit files.",
  transport: "process",
  tools: ["read", "grep", "find", "ls"],
});
return result;
```

Create background handles explicitly:

```ts
const handle = await agents.spawn({
  task: "Map the persistence layer and identify its public entry points.",
  transport: "process",
});

// Continue with independent work here.

return await agents.wait({ id: handle.id });
```

Set `cwd` on `agents.run()` or `agents.spawn()` to choose a leaf or recursive Pi child's filesystem execution directory, including another repository or a non-Git directory. Absolute paths are accepted; relative paths resolve from the parent Fabric manager's cwd. Fabric canonicalizes the directory (including symlinks) before launch, and reports that effective path in the handle, status, participant snapshot, result, and log status. Invalid, missing, inaccessible, or non-directory paths fail without falling back to the parent. Workflow helpers, `council.run()`, and `rlm.query()` forward `cwd` and `worktree`. Persistent actor definitions and trajectory handoff still do not accept `cwd`. Omission inherits the immediate caller’s cwd; descendants resolve relative paths from that effective directory, not the original project root. Project/mesh lineage, recursion depth/budget, and the caller’s resolved kernel/backend remain unchanged.

For Pi children, selecting `cwd` neither grants nor requires project trust. Fabric adds no trust gate and passes neither `--approve` nor `--no-approve`; Pi loads `AGENTS.override.md`, `AGENTS.md`, and `CLAUDE.md` under its normal context rules, while protected project resources remain governed by Pi's saved decisions and `defaultProjectTrust`. Each runner keeps its native startup behavior. A generated worktree is evaluated at its own canonical path.

Extension-enabled Pi children inherit the parent’s full-code mode, including ordinary non-recursive leaves. They retain `fabric_exec` as the outer tool, and their optional `tools` allowlist also constrains nested Pi and captured-tool calls. `extensions: false` explicitly opts out; Claude/Veda and parents outside full-code mode keep their existing tool surfaces. Recursive children additionally receive only the delegated `agent` risk. An inherited optional-tool allowlist is intersected with requested/default descendant tools before launch (including durable transfer); malformed inherited allowlists fail closed. Selecting another cwd cannot restore filtered tools. This is not filesystem isolation: enabled tools, trusted extensions, and native execution retain their normal host privileges. A launch-policy change requires `/reload` and newly launched children; it does not hot-swap an already running worker’s tools.

### Durable participant residency

`agents.spawn()` and `agents.create()` accept `residency: "session" | "durable"`. The default is `session`. It keeps the usual lifecycle: the current Pi host owns the participant and stops or suspends it when the host shuts down. The model chooses `durable` during execution. Users do not configure it as a setting.

```ts
const audit = await agents.spawn({
  name: "integration-audit",
  task: "Run the integration audit and report concrete failures.",
  residency: "durable",
  tools: ["read", "grep", "find", "ls", "bash"],
});

const supervisor = await agents.create({
  name: "migration-supervisor",
  residency: "durable",
  instructions: "Process migration messages until verification succeeds.",
  topics: ["team.migration"],
  delivery: "followUp",
  triggerTurn: true,
});

await agents.tell({ id: supervisor.id, message: "Own the remaining migration." });
return { audit, supervisor };
```

A Main session's actors keep a mesh cursor in the session's actor directory. After `/reload` or a restart of that session, they receive the topic events that were published while no runtime was reading them. A longer downtime replays only its last 10 minutes, and a new session starts at the current end of the log.

A local Main with `mesh.enabled` has a work inbox (smarty-dev#754): `fleet.*` events whose `to` is its session id or name, and that no steer or follow-up delivered. Main receives them as one `<fabric-inbox>` message at turn start and when a completed run settles. An event younger than 60 s waits, so its steer arrives first. Confirmed steer/follow-up and consumed inbox receipts are indexed from the whole session history and cached per recipient across reloads. Delivery IDs take precedence over message IDs, then work keys, then ref-only fallback: distinct deliveries sharing a ref or key are not collapsed. The persisted cache is age-pruned at the inbox horizon and capped at 1,024 hashes, evicting oldest receipts first; its serialized cursor, receipts, timestamps and pending metadata target at most 75% of `mesh.maxEventBytes`. Canonical session history still suppresses receipts evicted by the capacity cap; the cache is not an unbounded lifetime ledger after history deletion. Cursor/pending recovery is independent of receipt eviction. Queued native messages are not receipts. Addressed shadows older than 2 h are expired on the first drain, including saved pending batches, and counted in one passive `pi-fabric-inbox-summary` line without starting a model turn. `PI_FABRIC_INBOX_HORIZON_MS` configures the horizon in positive milliseconds; blank, invalid, zero or negative values use the 2 h default. Expiry changes only injection: the events remain available through `mesh.read`. On a Pi that queues a triggered message behind a live prompt preflight and reports that preflight with `ctx.isPromptPending()` (it sets `pi.hostCapabilities.triggeredMessageQueuesBehindPreflight`, Smarty-Pants-Inc/pi#74, and `promptPendingVisible`, Smarty-Pants-Inc/pi#76), an idle Main also reads the inbox every 15 s (`PI_FABRIC_INBOX_WAKE_MS`) and starts a turn for it (smarty-dev#1595): an event published to an idle Main starts a turn about 60–75 s later. Because each wake is a model turn, an idle Main wakes at most once per 5 minutes (`PI_FABRIC_INBOX_WAKE_COOLDOWN_MS`); an event of kind `p0` or `steer` wakes it at once. Each idle wake publishes one `fabric.inbox.wake` event (kind `idle-wake`, data `{count, reason, ids}`), so wakes can be counted from the mesh log. After a run that the user cancelled or that failed, the idle wake stays off until the next turn starts. While a prompt is in its preflight, the timer sends nothing: that prompt's turn start takes the batch. On an older Pi the idle wake is off: a wake could start a run while a prompt is still in its preflight, and that prompt would fail.

The first durable request starts one hidden resident host for the current root when needed. Every durable actor create/import and one-shot spawn uses its authoritative fenced request path, including the first actor in an empty registry. It publishes the owner in the standard participant directory. Fabric routes `steer`, `followUp`, `tell`, blocking `ask`, and `stop` through the acknowledged mesh control plane. The process uses the captured agent, mesh, timeout, recursion, and cost-ceiling configuration. It also uses the runner, model, and tool capabilities that the originating call explicitly authorized. Users do not configure a daemon profile or workflow policy.

The original TUI can shut down after the transfer. Durable agents continue until they reach a terminal status. A resumed copy of the same root can call `agents.status`, `agents.wait`, `agents.log`, and `agents.cleanup`. The mesh stores terminal notifications and active actor deliveries until Main resumes. Durable actors keep their registry definition, mailbox history, runner session, mesh subscriptions, and replay cursor. Main relays session-bound host events while it is available. The cross-process relay can omit oversized raw image blocks. The relay keeps their redacted media descriptors.

A durable actor has one resident execution owner. Other trusted sessions in the project can call `ask`, `tell`, `steer`, `followUp`, and `stop`; Fabric routes each call to that owner. Direct messages carry the caller's model and thinking binding. Actor status, mailbox history, logs, and definition export are shared project views. Only the owner can clear the mailbox or change tools, events, instructions, delivery policy, and project defaults. Fabric sends durable actor removal to the resident owner. The hidden host exits after a short idle grace when it owns no live durable actor or running durable agent. Durable residency requires a trusted project and `mesh.enabled`. Schema enforce mode does not support it.

Recursive cwd also works with durable `agents.spawn()`: the caller resolves explicit relative cwd before ownership transfer, and persisted handles retain effective cwd, kernel, and recursion metadata for later status/log reads. Startup retries reuse the original launch (including effective worktree cwd and capability flags); they do not resolve the target again. Resident-host recovery does not replay an interrupted spawn: its outcome is reported as indeterminate to avoid duplicate execution. This is not migration of a running session to a new cwd; existing workers must finish or be replaced by a new launch.

#### Resident startup fencing and legacy drain

Explicit resident starts acquire the same immutable-inode process-shared `flock` on every supported POSIX platform before constructing managers or admitting execution. Linux also requires `setpriv --pdeathsig KILL` for the lock helper. Missing/broken lock tooling fails closed; there is no PID-file/unlink fallback. Automated recovery remains Linux-only. Windows durable residency remains unsupported.

On POSIX, `host-fence.json` binds the established protocol to the exact device/inode of `host.lock`. The host never unlinks that kernel-fence inode, including on clean close. Corruption of PID diagnostics does not defeat an established kernel fence or block recovery after its holder exits. An empty/torn record, an unproven existing inode (even one naming a dead PID), or a replaced inode is instead refused before initialization, with a startup diagnostic directing the operator to verify a drain. A dead PID alone cannot exclude a legacy reclaimer already committed to inode replacement. The provenance record is not evidence that legacy processes were drained, and it must not be fabricated to bypass this refusal.

**Rollout and rollback remain coordinated operations:** prevent legacy-capable launchers/starters from entering, record the identities of the launchers/hosts/Pi groups being drained, confirm their exit with bounded owned termination, and retain before/after process and residency-root scans. Only after those checks establish no old owner or pending reclaimer may an operator remove the legacy/uncertain startup records (and obsolete provenance, if an inode was replaced) to allow a fresh claim. Do not remove live or unknown records, and do not downgrade into an established root alongside a newer host. This change does not claim that such a fleet drain has run.

A failed/timed-out start or client close keeps its owned launcher handle until termination is confirmed. Launcher stop waits up to two seconds for TERM, then escalates and verifies exit for up to two more seconds. Ordinary process workers instead have seven seconds to finish their existing five-second execution-child cleanup before outer escalation. Linux retains birth identities for observed descendants and their separately detached execution groups before stopping a custodian; exit/liveness includes all retained groups, not just the worker or launcher. Each signal revalidates an owned birth anchor; recycled or unknown groups fail closed. The real worker acknowledges its parent's cleanup custody before spawning execution, and reports native execution close over that same private IPC channel. Windows retains only its legacy native-worker stop contract, with destructive SIGTERM and no execution-tree custody IPC. Other POSIX platforms retain observed descendant groups through `ps` but never use unknown births to signal those groups or escalate their custodian before its drain completes. Unconfirmed cleanup stays an error. Failed verification retains custody for retry/close and remains an error.

Explicit Windows host starts retain the legacy atomic exclusive-create (`"wx"`) claim on `host.lock`; neither the host claim nor its establishment path spawns `flock`. Clean close removes only the token-owned claim, after closing its descriptor. Existing empty, torn, or stale claims are never automatically unlinked: a verified drain is required. This compatibility path is not an OS-backed execution-tree fence and does not enable durable Windows residency or automatic crash recovery.

Normal terminal process-worker publication is followed by a bounded natural-exit observation before destructive stop, so a final native-session flush is not interrupted. Settlement/admission still requires confirmed execution exit; a terminal record alone remains insufficient.

Readiness transfers custody only when the published per-attempt launch token matches this client's launcher. If a competing host wins, the client leaves that winner untouched but joins/stops its own losing attempt before returning success. A successfully ready owned durable host is not terminated by client close. POSIX CI without lock tooling uses an explicit **test-only, in-process** lock adapter for isolated request/commit harnesses; this does not enable native Windows durable residency or replace the product kernel fence.

#### Cancellation and uncertain durable outcomes

Resident mutations race cancellation on one immutable decision record. If abandonment wins before commitment, the host cannot create the requested actor/run/worker or perform cleanup later. Cleanup joins a running worker without foregrounding it or consuming its completion; these effects happen only after commitment. Caller cancellation is forwarded through the public provider, registry, executor and resident exchange, including offline cleanup.

If commitment wins, cancellation, an executor deadline, publication failure or host loss cannot prove rejection. The caller receives `ResidentOutcomeUnknownError` (including its name in public error text), with request/entity/owner IDs when known and **do not retry or reassign** guidance. The outer registry/QuickJS cancellation gates preserve all resident receipts, even if the guest cannot resume or leaves host calls unawaited. Reconcile via `agents.status` / `agents.actorStatus`, `agents.list` / `agents.actors`, and `agents.stop` with the known ID once registered. There is no local create/cede/compensating-remove/reclaim recovery for durable creation. New clients refuse mutation dispatch to live owners without `requestFence: 1`; restart/upgrade old owners at their safe boundary.

**Bounded request retention (P3):** upgraded resident hosts advertise `requestExpiry: 1`. Clients then use rollback-safe format-3 command envelopes with an immutable timestamp generation in the request ID. The host's existing request poll scans decisions, responses and consumption acknowledgements in 5 ms streaming slices, with a new scan every minute. Only terminal, explicitly acknowledged entries older than **24 hours** (including the acknowledgement age), without queued/processing requests or live/pending entity references, are collected. Cancellation that wins abandonment is a terminal acknowledgement; a committed but unacknowledged late response remains retained.

Actor `stopped` closes admission; it is **not worker-exit proof**. The host retains the creation commitment and its immutable actor/owner reconciliation IDs while the original activation drains or any owned writer remains live/unresolved. Collection of a stopped registry row requires the owning host's full writer/drain snapshot, including actor IDs on agent runs and checked process-exit/run-tree evidence. Unknown or incomplete recovered-worker evidence fails closed; it never authorizes deleting a commitment.

Before unlinking any eligible fence, the host durably advances a constant-size, monotonic `request-expiry.json` watermark. Dispatch and the first-mutation fence reject an expired generation with `ResidentRequestExpiredError` / `RESIDENT_REQUEST_EXPIRED`, even if a replay changes `createdAt`; the mutation fence rechecks after its hard-link CAS to close a collector race. An expired result is **not permission to rerun/reassign**: reconcile the original entity. Previous hosts reject format 3 before dispatch, so rollback cannot replay a collected request. Legacy format-1/2 requests keep their immutable fences indefinitely; compatibility with older fenced owners does not silently make their requests collectable.

The existing residency health/error note reads the sampled `request-retention.json`: retained count/bytes, unknown count, legacy count and collector errors. Unreadable/unsafe entries, unacknowledged outcomes, live references and orphan temporaries are kept and reported, never deleted to recover space. This is deliberately not a hard disk quota: unresolved/legacy debt needs operator reconciliation. Never delete or roll back the expiry watermark. Linux/local-filesystem checks do not establish Windows or network-filesystem guarantees.

For deterministic native-process regression proof only, the resident host accepts `PI_FABRIC_TEST_RESIDENT_DELAY_STAGE=before_commit|after_commit` plus `PI_FABRIC_TEST_RESIDENT_DELAY_MS` (integer 1–10000). Both are off by default; absent/invalid values do nothing, and no startup/idle work is delayed. The first stage pauses a picked-up request before preparation/commit; the second pauses a successful committed request before response publication. Use an isolated HOME/agent/mesh under a private temporary directory, and leave these variables unset in production. They change timing only, never the winning fence decision.

### Trajectory-preserving handoff

A handoff delegates work from Pi to Pi through a real fork of the caller's active session branch. It blocks while the delegated work runs. The worker receives more than a task string. One complete `fabric_exec` invocation forms the atomic frontier unit. An explicit `agents.handoff()` call records a deferred request in the guest. It does not create a child or stop the program at that line. Later sequential and parallel calls continue as usual.

Pi finalizes the native outer `fabric_exec` tool result after the complete Fabric program returns. At the `message_end` boundary, Fabric forks through the original assistant entry that contains the native `fabric_exec` call. It appends the exact finalized native `toolResult` to the child branch. Fabric then starts the selected executor in the same workspace and waits before Pi performs another Main inference. The child sees the outer call and frontier result exactly as finalized before handoff replacement. This context includes the Fabric source, output, and persisted trace. Fabric does not rewrite nested calls as synthetic assistant turns. It materializes an in-memory source in the same native Pi session format.

Fabric sets no special count or size limit for handoffs. The normal `fabric_exec` output and trace projection limits apply before the boundary. Handoff fails closed when Fabric cannot identify the active outer turn or when that turn belongs to an incomplete parallel top-level tool batch.

```ts
await pi.edit({
  path: "src/guard.ts",
  edits: [{ oldText: "return false;", newText: "return true;" }],
});
await agents.handoff({
  model: "anthropic/claude-haiku-4-5",
  task: "Continue from this completed Fabric invocation.",
  when: ({ count }) => count("pi.edit") >= 1,
});
await pi.bash({ command: "pnpm test guard" });
return "Frontier Fabric invocation completed";
```

`when` is an optional pure synchronous predicate that runs inside the Fabric guest. It receives immutable `{ calls, count(ref?) }` facts for each successful resolved bridge call that finished earlier in the same `fabric_exec` program. These calls include `pi.*`, `extensions.*`, `mcp.*`, external providers, and computed `tools.call()` refs. `count()` counts all calls. `count("pi.edit")` counts one ref. `count(["pi.edit", "schema.commit"])` counts a set. Fabric records each generic call under its resolved target. Fabric excludes failed calls. A false predicate does not start a child and reports a clear failure. The function never crosses the host bridge. Omit `when` to schedule unconditionally.

In the guest, `agents.handoff()` resolves to `{ scheduled: true, status: "deferred", boundary: "fabric_exec_end" }`. Code later in the same Fabric invocation cannot consume the child output. At the outer boundary, Fabric replaces Main's tool result with the compact completion `{ handedOff, completed, status, agent, implementation, error? }`. The `model` field is required. The target runner is Pi. The `worktree` field is unavailable because the implementation must remain visible in the caller's workspace. You can also set `task`, `name`, `kernel`, `transport`, `thinking`, `tools`, `timeoutMs`, `extensions`, `recursive`, `schema`, and `compact`. Fabric does not switch or rewrite the history of the source session.

After an explicit handoff settles, Fabric queues one visible `pi-fabric-handoff-complete` custom message as a follow-up and wakes Main. The TUI shows the executor name, model, status, and a bounded conclusion. Main summarizes the outcome and reported checks, preserving links and other concrete identifiers without redoing the delegated work. Failed, stopped, and timed-out handoffs (including launch errors) instead prompt Main to explain what happened and propose a next step without retrying or taking over unprompted. Internal continuation instructions are included in model context but omitted by the message renderer. Prewalk keeps its existing verification continuation.

**Executor-local failure continuation.** If a running trajectory executor encounters a failed nested handoff (including a launch or depth-limit error), Fabric queues one hidden `pi-fabric-handoff-continuation` follow-up in that same executor. It tells the executor what failed and to finish its original assignment directly in the existing workspace, preserving completed work and running verification without stopping at a handoff-failed report. It must not retry the handoff, spawn a replacement executor, or raise limits. Fabric cancels any local Prewalk arm so the next write cannot delegate again. The failed boundary remains failed; this is not a retry or a claim that the work succeeded.

This fallback is available once per executor run, with a persisted receipt that survives reload and worker recovery. Main, ordinary children, and actors keep their existing completion policy. Explicit stops, cancelled callers, timeouts, and token-limit termination do not trigger the fallback; the executor's original deadline, permissions, and token accounting remain in force. A blocked executor reports its blocker honestly. Fabric does not classify free-form final answers as failures or relaunch an agent merely because its answer contains failure text.

**Trajectory compaction.** Set `compact` to give the executor a compacted transcript in place of the full raw branch. A value of `true` applies the default summary. Use `{ instructions?, preserve? }` to add compaction instructions of up to 8K characters and as many as 16 explicit preserve facts of up to 2K characters each. These limits match `compact.request`. Fabric budgets the complete inherited context, including the finalized outer `fabric_exec` result and any thinking-transfer digest, before appending the compaction marker. The executor sees the projected summary plus a bounded, tool-pair-safe raw tail; an oversized outer call/result pair is summarized together to avoid leaving an orphan result. When available, the destination model window and its Pi compaction settings apply, including trusted project/model overrides. Source-model usage is not treated as destination calibration. Without model metadata, the raw tail is still bounded (20K estimated tokens by default). The append-only child file retains the full raw trajectory. Fabric records the successful outcome under `compaction` in the child's `pi-fabric-handoff` custom entry, including sections, tokens, and cut point. If requested compaction cannot produce a valid result, the handoff fails; the unbounded fork is never silently launched. Omit `compact` to keep the fork verbatim.

### Context-inheriting spawn

`agents.run()` and `agents.spawn()` accept `seed: "task" | "branch" | "snippet"` for Pi children. The default `"task"` keeps the historical behavior: the child receives the task alone. Other runners fail before launch when `seed` is not `"task"`.

`seed: "branch"` starts the child from a copy of the caller's current session branch, with the task appended as a new user turn. Unlike `agents.handoff()`, it never blocks Main or waits for the `fabric_exec` boundary; the caller keeps running and can `wait` later. The copy ends at the caller's last completed turn: Fabric drops the in-flight assistant turn that holds the outer `fabric_exec` call (the newest assistant entry after the latest user message with an unresolved tool call), so the child never sees a dangling tool call. Fabric materializes the copy with the same session machinery as handoff, applies thinking transfer when the child model's reasoning channel differs, and records a `pi-fabric-fork` custom entry with `boundary: "last_completed_turn"`. The seed works with `worktree: true`. Durable `agents.spawn()` refuses `seed: "branch"` because the resident host accepts no session seeds; use `"snippet"` there.

`seed: "snippet"` prefixes the task with the last `seedMessages` user and assistant messages (1 to 50, default 12) inside an `<inherited-conversation>` block. Only text survives: tool calls, tool results, thinking, and images are dropped, and each message is truncated to 4,000 characters. The cut is deterministic and works with durable residency.

A supervised fork pairs a mailbox actor with a branch-seeded worker. The worker inherits the conversation; the supervisor reviews each result:

```ts
const supervisor = await agents.create({
  name: "fork-supervisor",
  instructions: "Review a worker result. Reply APPROVE or a concrete correction.",
});
let task = "Implement the plan we just agreed on.";
for (let round = 0; round < 3; round++) {
  const worker = await agents.run({ task, seed: "branch", worktree: true });
  const verdict = await agents.ask({ id: supervisor.id, message: worker.text, data: worker.worktreeResult });
  if (verdict.text?.includes("APPROVE")) return worker;
  task = `Revise the previous attempt in a fresh fork: ${verdict.text}`;
}
return "Supervisor did not approve after 3 rounds";
```

### Automatic Fabric-boundary prewalk

`/fabric prewalk` adapts Can Bölük's [Prewalk research](https://stencil.so/blog/prewalk) for Fabric. OMP changes models inside one live agent loop at the first edit or write that a todo gates. Fabric uses a coarser atomic boundary. The first successful monitored mutation marks the current outer `fabric_exec`. All remaining nested calls settle before prewalk continues. This behavior preserves programmable sequential and parallel Fabric semantics.

```text
/fabric prewalk
/fabric prewalk Implement the token guard and run its tests
/fabric prewalk --status
/fabric prewalk --off
/fabric prewalk --disable
/fabric prewalk --enable
```

When you supply a task, Fabric arms prewalk and immediately submits the task to Main. Without a task, it captures the next user input. Select the executor in `/fabric settings` under **Prewalk**. **Always re-arm** uses `prewalk.model` to arm prewalk automatically at each Main session start without interaction. It also arms prewalk after each completed handoff. Child agents and actors do not auto-arm from inherited settings, so trajectory executors finish their assigned work without handing it off again on their first write. Explicit arming remains available. `/fabric prewalk --off` cancels only the current session arm. `/fabric prewalk --disable` persists the master switch to the project config when trusted (global config otherwise), cancels any live arm, and keeps prewalk inert after restart; `--enable` reverses it.

Host extensions that must serialize work after prewalk can use the acknowledged protocol exported from `pi-fabric/protocol`. Emit `FABRIC_PREWALK_REQUEST_EVENT` with `{ version: 1, context, claim, respond }`. Fabric calls `claim()` synchronously; the first claimant owns the request. It calls `respond({ ok: true })` only after prewalk is armed, or `respond({ ok: false, error })` after cancellation or failure. A request that is not claimed means no compatible Fabric runtime is installed. The protocol intentionally arms without submitting a task, so the caller can deliver its next queued row only after the acknowledgment.

The default value of `prewalk.mode` is `"in-place"`:

1. The arm owes a recorded plan before any handoff. With `prewalk.requirePlan` (default on), a mutation boundary reached without one is withheld: Fabric delivers a hidden plan checkpoint to Main that asks for `prewalk.plan({ outcome, steps, verification, risks })` inside `fabric_exec`. That recorded plan is the readiness signal: Fabric snapshots it at claim time and delivers it in the hidden continuation (in-place) or the executor task (trajectory), so the executor receives it even if the action result is discarded or the outer output is truncated. Main keeps working on the frontier model, nothing is selected or switched, and the arm stays armed.
2. Fabric detects the next successful `pi.edit`, `pi.write`, or `schema.commit`, then lets the full outer program settle. An audited mutation also consumes the shell-write drift window, so those edits cannot re-fire as `fs.drift` on a later read-only shell boundary. A successful `pi.bash` or `pi.powershell` can also trigger detection when no audited mutation occurred. In that case, a stat-baseline diff of the work tree identifies shell writes such as heredocs, `sed -i`, and formatter binaries as a filesystem trigger (`fs.drift`). Set `prewalk.detectShellWrites` to `false` to disable this behavior.
3. At the finalized outer result boundary, Fabric selects `prewalk.model` on Main.
4. Fabric sends one hidden continuation as a passive context message. It tells Main to continue the current task, complete the remaining implementation, check related call sites, and run verification. That single message carries the captured task text, the recorded plan when one exists, and, when the reasoning channel is not replayable, a bounded advisory digest of the frontier model's deliberation. Fabric also keeps it as the claim's canonical payload, and the request-context hook injects it into any request that would otherwise run without it.
5. The boundary does not terminate the outer tool. The same Main session keeps running on the executor model and its next request follows naturally, carrying the continuation exactly once. Claiming a prewalk-in-place handoff therefore suppresses `terminate` from the boundary result. Competing user steers drain in order into that request without delaying the continuation, and no completion-only turn follows. The passive message is persisted after the boundary turn's tool results, so later turns and runs read it from the transcript.
6. After the continuation settles, Fabric compacts the session with the configured compaction engine. Set `prewalk.compactOnReturn` to `false` to skip this step. Fabric then restores the Main model captured at the boundary.

In-place mode uses Main directly and keeps one transcript. It works with `agents.enabled` set to `false`. Pi's public extension model switch can leave the executor selected on Main, including in a later session that inherits it, so Fabric restores the captured Main model when the continuation settles, when a new session is still on that executor, and when prewalk is cancelled or reloaded. A switch can fail before continuation when the model is unavailable or unauthenticated. The outer result then reports the cause, and because the in-place boundary does not terminate, Main keeps running on its captured model and explains the failure in that same run; no queued follow-up repeats it. The return after a settled continuation can also fail. Fabric then drops the arm without re-arming, keeps the failure visible, and preserves the captured Main model: a later session start or `/fabric reload` retries the return, auto-arm stays skipped while Main is still on the executor, and an explicit `/fabric prewalk` arm overrides.

Set `prewalk.mode` to `"trajectory"` to use child-based behavior. Fabric forks the exact finalized outer call and result into a Pi child. It starts the selected executor in the shared workspace and waits. When the child finishes, Fabric replaces the boundary result with the executor report and queues a hidden continuation. Main verifies the implementation with the applicable checks and provides a summary. It does not repeat completed executor work, and it relays links, PR numbers, and commit hashes verbatim. If the child fails, stops, or times out, Fabric queues a hidden follow-up that reports the result and proposes an action. Main always tells the user how execution ended and what the child completed. Every boundary reports a result. The executor uses `prewalk.thinking` for reasoning effort. When unset, it inherits `agents.thinking`. The parent Fabric card and activity UI display the synthetic `agents.handoff` call, child identity, live status or current tool, nested preview, metrics, and terminal result. Users can see progress during the wait. Trajectory mode requires enabled agents. Explicit `agents.handoff()` also uses this behavior.

**Thinking transfer across models.** Providers define the shape of stored thinking blocks. Codex uses encrypted reasoning items. Anthropic requires valid signatures. Replaying these blocks to another provider can place them in unusable request fields or cause failure. Fabric applies a family policy when it writes the trajectory child session. It uses `preserved` when the executor has the same provider and API family as the source model. For openai-completions reasoning targets, it uses `re-signed`. This policy keeps the thinking text and normalizes the signature to `reasoning_content`, which lets preserve-thinking servers receive earlier reasoning. For other targets, Fabric uses `stripped`. Fabric removes the thinking blocks and foreign thought signatures, then adds a bounded digest custom message with entry IDs for continuity. The digest covers the current task's deliberation from the boundary model only (never an earlier task's or another model's reasoning) and is omitted when no current task boundary carries usable thinking. Fabric records the policy and counts under `thinkingTransfer` in the child's `pi-fabric-handoff` custom entry. In-place prewalk cannot rewrite Pi's session log. When channels are incompatible, it embeds the same digest in the hidden continuation message. Fabric never modifies the source session.

Prewalk does not add system-prompt instructions. It queues its hidden continuation only after a matching mutation boundary, so the continuation is not an open-ended prompt on each turn. The plan checkpoint is one hidden custom message per boundary that still owes a plan, delivered as a steer so it lands before the frontier model's next call, and it is never a system prompt. Readiness comes from the `prewalk.plan` action, and the claimed handoff carries the plan explicitly: the executor sees the full recorded plan in its first continuation request or task, not just a nested tool result. While a handoff is in flight, `prewalk.status` also reports the claim's `claimedReadiness` (`planned`, `disabled`, or `unplanned`), distinguishing a plan that was recorded and consumed from an arm that never recorded one. A boundary keeps withholding while the plan is missing and asks at most twice for a task, then hands off unplanned with a visible warning so an armed session cannot stall. A recorded plan survives a failed handoff and is dropped when the captured task changes; re-arming, cancelling, or reloading Fabric resets readiness so the next task plans again. Use `prewalk.status` inside `fabric_exec` for the arm state and its readiness flags. It also reports the loaded build identity of the extension entry and the lazy runtime module against the files currently on disk (`runtime.entry`, `runtime.lazyRuntime`, each with `loadedSha256`, `diskSha256`, and `stale`). Pi's loader native-imports `type: module` ESM extensions, and `/reload` clears only its factory cache, not the process ESM registry: an extension path the process has already imported keeps its first evaluation until the session process restarts. After rebuilding pi-fabric, restart the session to run the new build; `prewalk.status` is the one-call check for whether the loaded runtime is stale. A turn that settles without a handoff leaves prewalk armed. A matching mutation consumes the arm through an in-place switch or trajectory spawn. A completed explicit `agents.handoff()` also consumes it, as does `/fabric prewalk --off`. Fabric drops the settled turn's captured task text so that it captures the next prompt as new input. Both modes require full code mode. Schema enforce mode does not support them.

The filesystem fallback compares each file's size and mtime with a baseline captured when prewalk was armed. Fabric refreshes the baseline after every considered boundary and settle. In Git work trees, it lists files through the index, so ignored build output does not register. In trees without Git, it walks the files and skips only `.git` and `node_modules`. Both listings exclude Fabric's own state directory before the tracked-file cap, so runtime cache writes are bookkeeping that neither triggers prewalk nor consumes the tracking budget. Other tools keep their own directories, and Fabric does not encode them: a Git work tree follows the project's ignore rules, so adding a tool directory to `.gitignore` removes it from the listing. Artifact writes elsewhere then count as drift. The diff cannot identify who made a change. An external editor save during a shell-running window also counts. The fallback scans only programs that ran `pi.bash`, so read-only turns have no scan cost and cannot trigger it. Stat drift can occur without a content change, for example from rare `touch` churn. This drift triggers prewalk. The report lists affected files in `trigger.files`.

### Claude Code runner

Install and authenticate the official Claude Code CLI (`claude`) through its normal process. Fabric calls this binary directly. Select it for one call or for all calls:

```ts
const models = await agents.models({ runner: "claude" });
const haiku = models.find((model) => model.key === "claude/haiku");
return agents.run({
  runner: "claude",
  model: haiku?.key,
  task: "Review the current diff. Do not edit files.",
  tools: ["read", "grep", "find", "ls"],
});
```

`agents.models({ runner: "claude" })` requests the initialization model catalog from the installed CLI. The catalog includes aliases, resolved IDs, descriptions, and supported effort levels. Fabric does not hard-code this list. The handshake sends no user prompt or model inference request, so discovery has no model charge. The call starts the configured local binary. Starting the local binary gives model-authored `agents.models` calls Fabric's `execute` risk. Fabric caches the catalog for 60 seconds. Claude model keys have the form `claude/<runtime-value>`, such as `claude/default`, `claude/sonnet`, and `claude/haiku`. Fabric removes this namespace before passing the value to `--model`.

Claude runs call `claude -p` with stream-JSON input and output, partial messages, `--permission-mode dontAsk`, `--tools`, and `--allowedTools`. Fabric converts its portable core allowlist as shown below:

| Fabric tool  | Mapped Claude Code tool |
| ------------ | ----------------------- |
| `read`       | `Read`                  |
| `grep`       | `Grep`                  |
| `find`, `ls` | `Glob`                  |
| `bash`       | `Bash`                  |
| `edit`       | `Edit`                  |
| `write`      | `Write`                 |

Unknown tools cause failure before launch. Set `extensions: false` to start Claude in safe mode. The default value, `true`, keeps the user's standard Claude Code customizations. The explicit tool list continues to control the tools that the model can use. JSON schemas use Claude's native `--json-schema`. Fabric normalizes usage, cost, turns, tool activity, errors, and Claude's session ID into the standard Fabric result and dashboard transcript. It adds `--no-session-persistence` to one-shot runs.

Claude-backed children have no recursive Fabric capabilities. Fabric rejects `recursive: true`, `fabric_exec`, and direct `mesh.*` access. Choose `runner: "pi"` for RLM or recursive Fabric. For host-managed mailbox and event coordination, use a persistent actor backed by Claude.

### Veda runner

The `veda` runner starts the [Veda CLI](https://github.com/kennyfrc/veda) as a one-shot headless child. Install and authenticate the configured backend CLI with its normal procedure. Veda can use `agy`, `codex`, `claude-code`, `droid`, `pi`, and any backend that the installed Veda build registers. Fabric invokes `agents.veda.binary` with `-b <backend> -p <persona> --json`. It sends the task through stdin:

```ts
return agents.run({
  runner: "veda",
  persona: "frontend", // select a built-in or custom Veda persona
  model: "agy/gemini-3.1-pro-high", // send this value to the configured backend
  task: "Review the current implementation for architectural risks. Do not edit files.",
  tools: ["read", "grep", "find", "ls"],
});
```

With no active host policy, Fabric forwards Veda model values unchanged to the selected backend. Use `agents.veda.model` to set a backend-specific default. An explicit `agents.run({ model })` value has priority. If you omit both values and the host deny-list is empty, Veda selects its own backend default. Under an active host deny policy, Fabric refuses unknown defaults, aliases and bare selectors before queueing or durable submission; select `agents.veda.backend: "pi"` and configure `agents.veda.model` or pass an exact allowed `provider/model` present in the Pi registry. Other Veda backends are refused because their actual target cannot be established. The admitted concrete selection is forwarded to Veda. Personas do not depend on models. Add custom personas at `~/.config/veda/personas/<name>/AGENTS.md`. Set the global default with `agents.veda.persona`, or select a persona for one run with `agents.run({ persona })`. `agents.models({ runner: "veda" })` currently returns an empty advisory list. Fabric normalizes usage (`inputTokens`/`outputTokens`/`cachedTokens`), backend conversation ID, turns, and errors from the Veda `--json` envelope. The data appears in the standard Fabric result, dashboard, lifecycle events, and budget ledger.

For each run, Fabric passes `--tools <allowlist>`. It passes `--no-tools` for an empty allowlist. This setting has priority over tool frontmatter in the persona. The built-in read-only personas specify `tools: none`. The `worker` persona specifies `tools: all` with `sandbox: workspace-write`. Fabric does not pass `--sandbox`, so persona frontmatter defines the sandbox, and `worker` agents can change files. The `navigator-plan` persona also requires a `<program>` design block, and `worker` requires a `<worker_report>`. A failure in either protocol appears as a run error, so `navigator-chat` is the default for free-form tasks.

Each run gets an isolated `fabric-<run-id>` Veda session through `-S` and `--no-sel`. Parallel children cannot share selection or conversation state. Veda stores these sessions under `.veda/sessions/` at the project root. Outside a Git repository, it uses `~/.config/veda`. This repository includes `.veda/` in `.gitignore`.

Veda children do not have recursive Fabric capabilities. Fabric rejects `recursive: true`. Veda does not support steering, so steer and follow-up calls throw when called. It also cannot run persistent actors because each invocation executes one headless prompt. Use `runner: "pi"` when you need recursive Fabric or persistent coordination.

### Custom runners

For an opt-in implementation backed by the experimental Pi durable harness, see [Durable Pi runner](durable-pi.md). It uses this hosted-runner contract; importing it does not replace the default Pi runner or make arbitrary `fabric_exec` programs replay-safe.

A Pi extension can add a runner through the `pi-fabric/runners` subpath. The subpath is never loaded by the Fabric extension at startup.

```ts host
import { registerAgentRunner, listAgentRunners, getAgentRunner } from "pi-fabric/runners";

const unregister = registerAgentRunner(adapter); // FabricWorkerRunner | FabricHostedRunner
```

Every adapter has an `id` (`/^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/`, at most 64 characters; `pi`, `claude`, and `veda` cannot be replaced), a `label`, and a `capabilities` object in which every flag is a required boolean: `recursiveFabric`, `steer`, `followUp`, `persistentSessions`, `kernels`, `handoff`, `modelDiscovery`, `imageInput`, `compaction`, `questions`, `sleep`, `writePolicy`. Optional hooks are `models()`, `defaultModel()`, `normalizeModel()`, and `mapTools()`. The built-in runners declare their flags in the same table, so every capability check is uniform. Fabric refuses a request that needs an undeclared capability before admission, budget, or worktree side effects: `recursive: true` needs `recursiveFabric`, an explicit `kernel` needs `kernels`, `images` need `imageInput`, session seeds (trajectory handoff, `seed: "branch"`) need `handoff`, `readOnly`/`writableRoots`/`shell` or an inherited confinement need `writePolicy`, routed child dialogs need `questions`, `agents.compact` needs `compaction`, and actors need `persistentSessions`. A runner that declares `writePolicy` receives the effective policy in its launch context and must enforce it. In a scoped session, every launch context also carries `scope`: the host-issued [scope](providers.md#principal-and-scope) narrowed for that run. Runners pass it on to any provider that reads data for the run.

Two kinds exist:

- `kind: "worker"`: `launch(context)` returns `{ workerPath, workerArguments }`. Fabric runs that script under the selected transport and reads the [worker protocol](#worker-protocol) files. The context carries the run files, the task and launch facts, and `fabricWorker`, the launch Fabric would use for its own worker, so an adapter can wrap it. The optional `stop()` runs before the transport kills the process. A worker that dies before any progress is relaunched with the same launch (startup retry); Fabric never re-prompts a custom worker mid-run.
- `kind: "hosted"`: no Fabric process. The adapter owns execution, for example in a daemon. `prepare(context)` must be pure and returns a JSON locator of at most 8 KiB. Fabric writes it to the run record (`hosted.locator`) and `hosted.json` before it calls `start(locator, context, reporter)`. `context.idempotencyKey` is the Fabric run id, so a second submission with the same key must not start a second run. `liveness(locator)` answers `running`, `sleeping`, `settled`, `cancelled`, `interrupted`, or `unknown`. `stop(locator, reason)` is required and returns `{ confirmed }`. `abort`, `sleep`/`wake` (with the `sleep` capability), `steer`, and `followUp` are optional. The adapter delivers steer and follow-up messages itself; a delivery failure lands in the run transcript.

The hosted `reporter` has `progress({ turns, toolCalls, currentTool, text })`, `usage(total)` with the cumulative run usage (Fabric records the increase, which feeds budgets and `tokens.usage` lifecycle events), `transcript(event)`, `question(q)`, `finish({ status, output, structured? })`, and `fail({ error, retryable })`. `question` routes like a Pi child dialog: direct UI when the parent has one, otherwise a [decision](decisions.md#routed-child-questions), and only with `agents.childQuestions: "route"`. The run record shows `blockedOn` while it waits.

A hosted adapter returns `{ confirmed: true }` from `stop` only after execution has ceased and its workers no longer use the run's files. An accepted stop request, terminal result, or liveness response does not establish custody release. Fabric durably records an explicit confirmation in `hosted-exit.json`, bound to the run, runner, directory, start time, and prepared locator. Collection still checks native descendants and retains indeterminate, missing, changed, or linked receipts. Completed hosted runs without this explicit confirmation remain retained.

Fabric never relaunches or re-prompts a hosted run. A `sleeping` run stays `running` with `sleeping: true`. When liveness reports `interrupted` or `unknown` (or `settled` without a result), the run settles `failed` with `outcome: "indeterminate"`. The same outcome marks a stop the adapter did not confirm, and a durable spawn request the resident host was processing when it restarted. Hosted runs work with `agents.run`, `spawn`, `wait`, `status`, `stop`, `steer`, `followUp`, the dashboard, budgets, and `residency: "durable"`. Persistent actors need a worker runner.

Registration is process-local. The resident host runs Pi with `--no-extensions`, so a durable run of a custom runner requires `residentModule`: an absolute path to an ES module that registers the adapter when imported. Fabric refuses a durable spawn without it. The resident host imports the module before launch. On restart it imports it again and calls `attach(locator, context, reporter)` for every unfinished hosted run; a run whose runner cannot be loaded settles indeterminate. When the resident host shuts down, durable hosted runs are detached; they keep running and are re-attached on the next start. Session hosted runs are stopped with the session.

An illustrative daemon-backed adapter:

```ts host
import { registerAgentRunner, type FabricHostedRunner } from "pi-fabric/runners";

const daemon = "http://127.0.0.1:7777"; // hypothetical job daemon
const call = async (path: string, body?: unknown) =>
  (await fetch(`${daemon}${path}`, { method: body ? "POST" : "GET", body: JSON.stringify(body) })).json();

const runner: FabricHostedRunner = {
  kind: "hosted",
  id: "jobd",
  label: "Job daemon",
  residentModule: new URL(import.meta.url).pathname,
  capabilities: {
    recursiveFabric: false, steer: true, followUp: false, persistentSessions: false,
    kernels: false, handoff: false, modelDiscovery: false, imageInput: false,
    compaction: false, questions: false, sleep: false, writePolicy: false,
  },
  prepare: (context) => ({ job: context.idempotencyKey }),
  start: async ({ job }: any, context, reporter) => {
    await call("/jobs", { id: job, task: context.task, cwd: context.cwd }); // idempotent by id
    void follow(job, reporter);
  },
  attach: async ({ job }: any, _context, reporter) => void follow(job, reporter),
  liveness: async ({ job }: any) => (await call(`/jobs/${job}`)).state ?? "unknown",
  stop: async ({ job }: any) => ({ confirmed: (await call(`/jobs/${job}/stop`, {})).stopped === true }),
  steer: async ({ job }: any, message) => void (await call(`/jobs/${job}/input`, { message })),
};

async function follow(job: string, reporter: Parameters<FabricHostedRunner["start"]>[2]) {
  const result = await call(`/jobs/${job}/wait`); // replays the result after a restart
  reporter.usage(result.usage);
  reporter.finish({ status: result.ok ? "completed" : "failed", output: result.text });
}

registerAgentRunner(runner);
```

### Worker protocol

A worker runner process talks to Fabric through four files named in `context.files`. `pi-fabric/runners` exports a JSON Schema for each (`AgentRunRecordSchema`, `LifecycleLineSchema`, `TranscriptEventSchema`, `SteerCommandSchema`, protocol version `FABRIC_WORKER_PROTOCOL_VERSION = 1`). Readers ignore unknown fields.

| File | Direction | Content |
| --- | --- | --- |
| `statusFile` | worker writes | The run record, replaced atomically (write a temporary file, then rename). `status` moves from `running` to `completed`, `failed`, `stopped`, or `timed_out`; `text`, `value`, `error`, `turns`, `toolCalls`, `usage` (cumulative), `currentTool`, and `blockedOn` are the public fields. |
| `lifecycleFile` | worker appends | One `{ version: 1, event, occurredAt, data }` line per event: `tokens.usage` (a per-event increase), `question` (a dialog to route), and the `pi.*` lifecycle events. |
| `logFile` | worker appends | Transcript events for logs and the dashboard: `message_end` with a `user` or `assistant` message, `tool_execution_start`, `tool_execution_end`, and `extension_error`. |
| `steerFile` | Fabric appends | Commands with `id` and `ts`: `steer`, `follow_up`, `set_steering_mode`, `set_follow_up_mode`, `compact`, and `ui_response` (`requestId` plus `value`, `confirmed`, or `cancelled`) answering a routed `question`. |

A worker that exits without a terminal record is treated as a dead transport: it is relaunched only when it made no progress, and otherwise settles failed.

### Switching Main's session model

`agents.switchModel` changes the live Pi session model in place and keeps it there:

```ts
await agents.switchModel({ model: "anthropic/claude-opus-4-5" });
await agents.switchModel({ model: "cheap" });
```

The selector resolves in order: a `models.aliases` entry (a string alias is one target; an array is a fallback chain where the first authenticated target wins), an exact `provider/id`, an exact model id, then the closest match across provider, id, and display name. Closeness ties fall to the most recently used model, read from the [pi-model-sort](https://github.com/monotykamary/pi-model-sort) extension's usage store when it is installed, and then to the highest-sorting key, mirroring pi's newest-alias convention. An optional `provider` argument narrows every stage. Selectors with no resemblance to an authenticated entry and exhausted alias chains throw; the session model stays unchanged. `agents.models()` enumerates the authenticated registry entries this resolution runs against. The result reports the active model, the previous one, and how the selector resolved: `via` is the alias name for configured aliases, or `closest`, `recent`, or `latest` for fuzzy picks. A call naming the active model returns `{ switched: false, reason: "already-active" }`. The switch applies to the next model turn and later, unlike `prewalk`, which temporarily installs an executor model at a mutation boundary and then restores the boundary model.

Pi-runner `model` arguments on `agents.run`, `agents.spawn`, `agents.create`, and `agents.handoff`, plus actor defaults and activation overrides, resolve through the same selector logic (aliases, closest match, recency). The execution owner's `agents.models({ runner: "pi" })` result is authoritative: provider-qualified near-misses are matched only on that provider, selectors with no similar visible candidate and exhausted aliases fail with a session-availability error. Fabric revalidates the canonical model at the worker launch boundary, including remote and durable actor execution, so stale bindings cannot start a model that is no longer visible. Catalog-fresh or custom IDs must appear in the owner's visible registry before a Pi participant can use them. Claude and Veda model values keep their runner-specific behavior.

This is the host-level equivalent of the `pi-model-switch` extension's `switch_model` tool, with aliases moved into Fabric configuration so project and agent scopes behave like every other Fabric section.

### Changing this session's own live Main

`agents.setThinking` accepts this Main's exact `session:<id>`:

```ts
const main = await agents.main();
const effort = await agents.setThinking({ id: main.id, thinking: "high" });
```

This call uses Pi's synchronous thinking setter: the in-flight inference stays
unchanged, and the next model turn consumes the new effort. It returns Main's
native read-back state plus `previous: { model?, thinking? }` and `caller` (the
exact calling Main ID); `thinking` reports Pi's capability clamp.
Cancellation, deadline, authority and liveness are checked after the serialized
queue wait, with no await between the final fence and native state/journal commit.
Only session scope is supported; a Main thinking binding cannot be cleared.
Actor IDs and names (including an actor named `main`) keep their existing binding
behavior for both setters.

**Main `agents.setModel` is deferred entirely**, for own and remote targets:
`Main setModel is not supported yet (own or remote); see smarty-dev#4153`.
It refuses before registry resolution, native authentication, publication or
mutation. Pi's native model setter awaits authentication before mutation without
a requester cancellation/commit guard; same-process execution does not close
that window. The pre-existing `agents.switchModel` API is unchanged by this cut.

Only this session's own live Main may change thinking. Cross-process thinking
changes are refused, even for a recorded lead or org/product owner:
`remote Main model changes are not supported yet; see smarty-dev#4153`.
Nothing is queued or mutated. Follow-up support is tracked separately; this PR
contains no Pi-core patch. Participant format-1 capabilities stay unchanged and
`mainBindings` is false. Ordinary discovery and messaging remain compatible.

Successful own-session changes write a `pi-fabric.main-binding-change` entry in
this Main's native Pi journal with the action, caller, target and before/after
model/effort; the public calls retain their ordinary Fabric execution audit.

Native artifact proof (keyless, offline real Pi RPC; no external inference):
`nice -n 19 node scripts/prove-main-bindings.mjs dist/index.js "$TASK_OUT"`.

### Transports

**Execution-custody scope cut (smarty-dev#2566 / pi-fabric#218):** agent
admission currently supports only `process`; `auto` selects `process`.
Explicit `tmux`, `screen`, `localterm`, and `herdr` requests fail before launch.
Removing a session/pane does not prove its separately detached execution group
exited. Those adapters and their historical integration details below remain in
source, but must not be re-enabled until they retain birth-safe execution
custody, cooperatively drain the worker, and confirm the entire group's exit.
The follow-up belongs to [smarty-dev#2566](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/2566);
this is a fail-closed implementation scope cut, not product-owner risk acceptance.

Cancelled/revoked launches are registered for custody before attempting cleanup,
even if cancellation won before normal admission registration. Queued stop joins
that same fence; actor run/wait cannot finish its same-session drain while exit
is unconfirmed. A later positive receipt from the exact transport can discharge
a transient failed-cleanup mark and retry collection.

On the supported execution-tree custody path, terminal results are not exit
receipts. Public stop and manager close reject
unconfirmed execution cleanup, retain the admission permit and run handle, and
keep working files. Unknown process/group identity is never permission to signal
a recycled numeric ID or to discard custody. Worker crash publication waits for
execution drain; on Linux surviving same-birth group members remain cleanup
anchors after the leader exits. Portable POSIX leaderless groups without such
anchors remain unresolved rather than being signaled blindly.

**Windows scope cut:** the new execution-tree custody/exit receipt is unsupported
on Windows. Ordinary process workers preserve the pre-PR native-child cleanup
and worker-exit behavior, with no custody handshake or `fabric-execution-settled`
receipt. A native child close is **not** proof that its tool descendants exited;
the Linux/POSIX tree guarantee above does not apply to Windows. Durable Windows
residency remains unsupported. Re-enabling a Windows tree receipt requires an
OS-backed, birth-safe boundary (for example a Job Object), plus native Windows
stop/timeout/CLI-crash/custodian-death tests with pipe-independent descendants.
This is an implementation scope cut, not certification of legacy Windows tree
cleanup or a product-owner risk acceptance.

| Transport   | Operation                                                     | Command to attach            |
| ----------- | ------------------------------------------------------------- | ---------------------------- |
| `process`   | Runs a detached local worker process with the lowest overhead. This is the default transport | none |
| `tmux`      | Disabled pending execution-custody support                    | —                           |
| `screen`    | Disabled pending execution-custody support                    | —                           |
| `localterm` | Disabled pending execution-custody support                    | —                           |
| `herdr`     | Disabled pending execution-custody support                    | —                           |
| `auto`      | Selects `process` only                                        | none                        |

The following session-transport integration notes describe the disabled adapters
and their re-enablement requirements, not currently supported admission paths.

Herdr uses its local socket API to create an argv-backed background tab as one atomic operation. It does not change focus or require shell quoting. Automatic selection works only when the parent Pi process already runs in Herdr. This requires `HERDR_ENV=1` with an injected workspace and socket. Select `transport: "herdr"` under the same conditions. Use the attach command in the handle to open a child directly. Herdr workers inherit the server environment, not the parent shell. Fabric forwards an explicitly set `PI_CODING_AGENT_DIR` through the pane's environment map so the child uses the selected Pi profile; an unset selector leaves Herdr's default behavior unchanged. This does not copy profile files, credentials, `PATH`, or the rest of the parent environment. Instead, the parent resolves the Pi launcher and the worker runtime to absolute paths with its own `PATH` before launch. The worker runs JavaScript entrypoints and extensionless `#!/usr/bin/env node` launchers through its absolute runtime, and passes the selected launcher to nested Fabric as `PI_FABRIC_PI_BINARY`. Other commands inside the child still use Herdr's server `PATH`.

Each Herdr tab is labelled with the child's name and the first 12 characters of its run ID, so a person can tell which run it belongs to. The Herdr transport fails closed. When a `layout.apply` reply is dropped, the launch fails without a retry, because its outcome is unknown: Fabric neither adopts a pane nor launches again, and the error names the tab where a worker may still start. Fabric keeps that run's worktree and run files. A dropped `pane.get` does not end a run. Only `pane_not_found` from a reachable server ends a run at once. A Herdr server whose socket is gone, as in a live handoff, is given 5 minutes. After that the run fails as "Lost track of the worker", not as an exit: the worker may still run in its pane. Fabric never relaunches such a run. A run is also treated this way when a stop or its deadline cannot confirm that the worker exited. Each such run, and each unconfirmed launch, gets an `unresolved-worker.json` file in its run directory. While that file exists, `agents.cleanup`, the durable-agent cleanup, closing the manager and every retention sweep keep the run's files and worktree, also after the owning Pi session exits. Check the worker, then remove the files by hand. With other transports, Fabric relaunches a lost run only after its previous worker is gone for certain. A process worker that exited is never signalled afterwards, because its process id may already name another process.

Every launch takes a slot from a budget of 20 `layout.apply` calls per minute for each Herdr server (smarty-dev#266). On Unix the ledger is a directory next to the server's socket (`<socket>.pi-fabric-spawns`, created with mode 0700). The socket path is canonical: symlinks are followed by hand, so an alias whose target a live handoff removed still names the same server, and launches of one run through an alias and its target join. The ledger is shared by every Fabric process of that user, whatever its `TMPDIR`. When a minute's slots are taken, a launch waits for a later minute, for at most 2 minutes on a monotonic clock, jitter included. It then fails with an error that names transport `"process"`. Closing the agent manager ends the wait. Fabric turns the budget off, with one warning, when it cannot use the ledger safely: it is not a real directory, not owned by the user, or writable by other users; or one of its ancestors is a symlink, owned by another user (other than root), or writable by other users without the sticky bit. The budget is advisory: builds without it ignore the ledger, and it binds only among Fabric processes of one user on one host. On Windows the ledger is keyed by the pipe name in the temp directory, so two spellings of one pipe, or two `TEMP` directories, use separate budgets. Actors and background agents should use `transport: "process"`.

LocalTerm provides the required primitives that match tmux: detached creation, pinning, listing, capture, exec, attach, and kill. Pi Fabric requires no LocalTerm patch. Start the daemon before you select this transport:

```bash
localterm start
```

`/fabric agents` lists the children. Run `/fabric attach <id>` to show the correct attach command. A caller abort stops a run that never produced progress and detaches a run that already did: the child keeps working to its own terminal state and reports it, so a returned program or a cancelled tool call cannot discard a long participant's work. A worker that catches an external signal mid-run, or whose transport dies after doing work, is resumed, so an interruption the run can recover from never becomes a terminal stop. The manager relaunches the same run in the same directory, hands the child a continuation of the task, keeps the stopped attempt's turns and token usage in the run record, and emits `run.resumed`. An explicit stop, a run deadline, and the `agents.maxTokensPerChild` limit stay terminal. Three resumes bound one run, and the run deadline covers every attempt. When a program uses orchestration entry points such as `agent`/`workflow.agent`, `agents.run`/`agents.wait`/`agents.ask`, `council.run`, or `rlm.query`, Fabric increases the whole-program `executor.timeoutMs` to at least `agents.timeoutMs`. The same increase applies to `agents.*` refs called through `tools.call()` and to refs calculated at runtime. The parent deadline then cannot stop a child that remains within its own agent budget.

Set `worktree: true` to create a dedicated Git worktree and a `pi-fabric/<name>-<id>` branch from the repository containing the selected `cwd`. Fabric writes that worktree at `<repo>/.pi/fabric/worktrees/<id>` so copy-on-write cloning can keep ignored build artifacts on the same volume, and it records the path in the repository `.git/info/exclude` file. Simple `git worktree add` commands run through `pi.bash` take the same clone-first path. Fabric retains worktrees for inspection until you call `agents.cleanup()`. When the selected cwd is a repository subdirectory, Fabric uses the matching subdirectory in the generated worktree when it exists; otherwise it uses the worktree root. The reported effective cwd is the generated worktree path, and Pi evaluates that generated path as its own canonical cwd. The caller's project and mesh roots remain unchanged, so a child targeting another repository still belongs to the orchestrating Fabric topology. A recursive child in a worktree stays in the same participant directory and does not create another `.pi/fabric/mesh` inside that worktree.

At settlement, a `worktree: true` result and status carry `worktreeResult: { path, branch?, baseRef?, changedFiles, diffstat: { files, insertions, deletions }, kept, diffError? }` beside the existing `worktree` path string. `baseRef` is the commit the branch started from. `changedFiles` is the sorted union of tracked changes against `baseRef` (committed or not) and untracked, non-ignored files, capped at 500 entries; `diffstat.files` counts all of them. Untracked text files up to 1 MiB count their lines as insertions. Fabric computes the summary with two bounded Git calls (15 second timeout each); a Git failure leaves the counts at zero and sets `diffError`. `kept` reports whether the worktree still exists; worktrees stay until `agents.cleanup()`.

Set `agents.worktree.setup` in [configuration](configuration.md#agents) or `worktreeSetup` on one request (which wins) to run a shell command in the new worktree root before the child starts, for example `"bun install --frozen-lockfile"`. The command runs through `/bin/sh -c` (`cmd /c` on Windows) with a 10 minute limit. A non-zero exit or timeout fails the launch with the last 2,000 characters of output and removes the worktree and its branch.

### Write confinement

`agents.run()` and `agents.spawn()` accept `readOnly?: boolean`, `writableRoots?: string[]`, and `shell?: "deny" | "unconfined"` for Pi children. Setting any of them creates a write policy:

- `readOnly: true` refuses every `write` and `edit`.
- `writableRoots` (at most 32) resolve against the child's cwd, including a generated worktree. Omitted roots default to that cwd. A root must exist or be creatable inside the cwd; Fabric creates missing ones. `write` and `edit` outside the roots fail. Fabric checks the lexical absolute path and the real path of the nearest existing ancestor, so a symlink cannot escape a root, and an unresolvable path fails closed.
- `bash` and `powershell` are refused under any write policy unless the request sets `shell: "unconfined"`. Fabric does not parse shell commands: an unconfined shell can write anywhere the process can, so it is not a sandbox.

The child Pi process enforces the policy. The worker passes it as `PI_FABRIC_WRITE_POLICY` and loads a small guard extension with `-e`, so it applies even with `extensions: false`. The guard blocks top-level tool calls through Pi's `tool_call` hook and nested `pi.write`, `pi.edit`, and `pi.bash` calls inside `fabric_exec`. It covers tools named `write`, `edit`, `bash`, and `powershell`, including captured overrides with those names; other extension tools and MCP servers are outside it. Native Fabric executors (CPython, `node-process`, `bun-process`) run outside the hook: a confined launch that would use one fails before launch, and a confined child refuses to run one, unless `shell: "unconfined"`. QuickJS and Monty stay available.

Confinement is inherited and only narrows. A confined agent's own children inherit its policy when they request none; an explicit request must keep `readOnly`, cannot switch to `shell: "unconfined"`, and must name roots inside the caller's roots. Claude and Veda runners fail before launch under any policy because Fabric cannot enforce it there. A confined agent cannot start durable agents or actors, since the shared resident host does not inherit its confinement.

### Scope narrowing

In a session with a host-issued [principal and scope](providers.md#principal-and-scope), every child inherits that scope. `agents.run()` and `agents.spawn()` accept `scope: { grants: [{ resource, actions }] }` to narrow it: each grant must be covered by one parent grant, or the launch fails before admission. The principal always stays the parent's. An unscoped session refuses `scope`. Durable spawns and actors keep the scope: the session sends it to the resident host with the request, so the durable child launches with the same scope it would get locally.

```ts
await agents.run({ task: "Summarize build logs", scope: { grants: [{ resource: "mesh:jobs/build", actions: ["read"] }] } });
```

### Child environment contract

Fabric children may read these environment variables. They are stable and versioned where noted; every other `PI_FABRIC_*` variable is internal and may change.

| Variable | Contents |
| --- | --- |
| `PI_FABRIC_LINEAGE` | JSON `{ version: 1, rootSessionId, parentSessionId?, parentRunId?, runId, depth, childIndex, worker: true }`. `childIndex` is the launch ordinal within the parent process. `agents.self()` adds the same object as `lineage` inside a child. |
| `PI_FABRIC_WRITE_POLICY` | JSON `{ readOnly, writableRoots, shell }` with canonical absolute roots. Present only for confined children. Malformed values fail closed to read-only with shell denied. |
| `PI_FABRIC_THINKING_BOUNDS` | JSON `{ min?, max? }` thinking bounds; see [thinking](thinking.md). |
| `PI_FABRIC_SCOPE` | JSON `FabricScope` `{ version: 1, principal, grants, digest, parentDigest? }`. Present only when the parent is scoped: the inherited or narrowed scope. The worker always clears `PI_FABRIC_SCOPE_FILE`. A child Fabric reads it as its root scope and fails closed when it is malformed. See [principal and scope](providers.md#principal-and-scope). |
| `PI_FABRIC_DEPTH` | Recursion depth of the child (the root is 0). |
| `PI_FABRIC_AGENT_NAME` | The child's display name. |

[Model-guidance components](components.md#model-facing-guidance-components) can target participants by canonical provider/model. Direct agents and actors retain their role prompt and receive matching append guidance after it. Recursive Pi children load the project components and resolve their own replaceable Fabric execution slot, so the parent does not duplicate guidance. Durable owners use the latest atomically committed guidance snapshot for each launch. Guidance changes prompts only; it cannot widen tools, approvals, or committed capabilities. Task text, message envelopes, run IDs, and timestamps stay out of the guidance system prompt, so repeated runs with the same role, model, and component projection retain a byte-stable prefix.

## Unified participants and steering

Fabric uses one participant directory for each project. Every live entity has a fixed `kind` of `root`, `agent`, `actor`, or `provider` (work a provider registered as a [participant](providers.md#provider-participants), with `provider` set and no `runner`). It also has a `rootId`, an optional `parentId`, an `ownerHostId`, and an authenticated owner identity for the process that controls its lifecycle. **Main** is the local user-facing view of one root. **Peers** provide compatibility views of the other roots. These views do not use separate registries or control planes. **Fabric reserves Peer for another root Pi session. The term never means a child agent.** When asked about a peer, call `agents.peers()` first. `agents.list()` reports only child agents, so it cannot determine whether a peer root has settled.

`agents.self()` returns the participant record for the caller. `agents.sessions()` lists every live root Pi session, including the caller's root and peers, as symmetric participant records for session-to-session coordination. Call `agents.members({ scope?, kinds?, includeStale? })` to list all kinds. `agents.list({ scope? })` lists agents and uses `scope: "local"` by default. Set the scope to `"lineage"` for descendants of the same root across recursive runtimes. Use `"project"` for all live project agents. `agents.main()` and `agents.peers()` remain convenient compatibility projections. A root publishes its `role` (`PI_FABRIC_ROLE`, else `SMARTY_ROLE` without its `@` stamp) and its `project`: the checkout that owns its git common directory, so every linked worktree of one repository shares its main checkout's project. A root whose cwd is in another repository (a lead working from a worktree of a shared repository) sets `PI_FABRIC_PROJECT` to its own checkout, or any path in it; the root's actors and resident host use that project too. Roots also publish `repository`, their normalized Git origin identity, so moved lanes on other hosts can match the same repository despite different checkout paths. `agents.projectAgent()` uses that identity and the lead id captured at provider launch from `SMARTY_LEAD_SESSION` or the lane's `.local/lead` (a file containing the exact `session:<id>`). The recorded id wins when several same-origin roots exist and permits that exact bridge mirror; an unrecorded mirror cannot claim leadership. Without a recorded id, a single native `project-agent` is selected; multiple candidates throw `FabricProjectAgentAmbiguousError`, and an unavailable or wrong-repository recorded lead throws `FabricProjectAgentUnresolvedError` and does not choose another session. Legacy native records retain checkout-path matching, including an untagged root whose cwd is exactly the project checkout. Print/JSON roots publish `interactive: false`: they remain discoverable but cannot receive `followUp`/`steer`/`tell` or become project leads. A worktree agent reports to its project lead because `agents.main()` returns the worktree agent's own root. Standard discovery hides participants with expired execution-host leases. Shared summaries include operational metadata. They exclude agent prompts, results, and errors.

```ts
const main = await agents.main();
const sessions = await agents.sessions();
const peerRoot = sessions.find((participant) => participant.id !== main.id);
if (peerRoot) {
  await agents.steer({ id: peerRoot.id, message: "Coordinate on the shared migration." });
}
await agents.followUp({ id: main.id, message: "After the audit, reconcile the findings." });

const lineage = await agents.list({ scope: "lineage" });
return { self: await agents.self(), lineage };
```

Child dialogs (`select`, `confirm`, `input`, `editor`) are cancelled by default. With `agents.childQuestions: "route"` they reach the parent's UI, or a root-held [durable decision](decisions.md#routed-child-questions) when the parent is headless; the run record shows `blockedOn` while one waits.

For Main and one-shot agents, `steer` arrives after the tool calls in the current turn and before the next model call. `followUp` waits until the current run settles, or, for a busy Main, until the next tool boundary after it has waited `mesh.followUpFlushMs` (2 minutes by default). Each delivered message header carries `delivery` and `sent_at` (ISO UTC). A followUp to Main returns `pendingFollowUps` and `oldestAgeS`: how many of the caller's own followUps Fabric still holds for that Main, and the age of the oldest; switch to `steer` when they grow. When Main is idle and its oldest held followUp, from any sender, is older than `mesh.followUpStallSeconds` (10 minutes by default), the queue is stalled: the followUp (or `tell`) throws `Fabric followUp to <target> was accepted but is not being delivered: ...`, and the message stays held. A busy Main admits at most 50 held followUps or 256 KiB per sender, and 200 or 1 MiB in total; past that, the followUp is rejected with the reason. Held followUps are journalled under the mesh root until the session holds them, so a restart does not lose them. A followUp whose `data.coalesceKey` is a non-empty string (at most 200 characters) replaces a followUp from the same sender with the same key that Main still holds unread: the newest message and data take its place in the queue, the result reports `coalesced: true` and `replacedMessageId`, and the replaced one is never delivered, also after a restart. A replacement does not count twice against the quota. A followUp without a key, with another key, or from another sender is held as before, and one already handed to Main is never replaced. `agents.tell` to Main is a followUp and coalesces the same way. This is the Main parallel of an actor's mailbox `coalesceKey`; judging whether a notice is stale stays the sender's job. For actors, both operations add a message to the serial mailbox. `agents.status({ id })` accepts any participant ID. It returns complete details for a local run or actor and a bounded directory summary for a remote participant. `agents.setSteeringMode` and `setFollowUpMode` continue to control local one-shot runs.

Main message receipts include `triggered: true | false` when the owner can report it. `true` means delivery requested a new turn from an idle Main at admission; it is not a completion receipt. A normal `followUp` (including `tell` to Main) carries the same wake permission across mesh bridges as local delivery. A busy Main reports `false` while holding the followUp for the next eligible boundary, without starting an extra run. Passive, halted, provider-backoff-held, reload-held, and duplicate admissions also report `false`.

A provider/compaction failure holds triggering peer `followUp` and `steer` messages until 60 seconds after the failure; consecutive failures without a successful turn double this delay, capped at 30 minutes. Held receipts include `reason: "provider-backoff until <ISO timestamp>"`. Fabric retains the messages (with the existing durable journal and quotas) and schedules their release at the deadline, even with the ordinary busy followUp drain disabled. Only one byte/provenance-bounded triggering batch is handed to Pi for a retry whose outcome is still unknown: remaining batches and new peer wakes stay in Fabric, not Pi's native continuation queue. New held admissions during that attempt report `reason: "provider-retry in flight"`. One best-effort `fabric.main.wake` mesh event per released batch, with kind `provider-backoff-released`, reports its message IDs and deadline. A successful turn resets the delay. Successful manual compaction also releases held wakes once Pi becomes idle, even with `mesh.followUpFlushMs: 0`, without resetting the consecutive-failure count. Escape/owner halts still suppress wakes indefinitely and cancel the timer; passive and `nextTurn` context do not arm it. An older owner or non-Main target may omit the field: treat that as unknown, not as `false`. A replayed control acknowledgement retains the original admission report; it does not describe a new wake. Existing cancellation gates and `steer` delivery semantics are unchanged.

Exact-id `followUp`, `steer`, and `tell` use the same participant directory and mesh root as `peers()`, including participant files, state-only records and bridge mirrors. A cached miss is retried with a fresh read. If discovery lists the target but control presence is not yet admissible, the call throws `FabricParticipantNotYetMirroredError` (`code: FABRIC_PARTICIPANT_NOT_YET_MIRRORED`, `retryable: true`, “not yet mirrored”); retry after the next bridge presence refresh. Discovery never bypasses owner/capability or bridge admission checks. Messages to print/JSON roots fail with `FabricParticipantNonInteractiveError`.

Local routing returns `"main"` or `"local"`. For cross-process `steer`, `followUp`, and `stop`, Fabric resolves the exact owner of the target. It sends a control command addressed to that owner and waits for an acknowledgement that matches the version, target, and owner identity. Success returns `routed: "mesh", acknowledged: true` after this verified acknowledgement. Unknown IDs, stale owners, rejection, and timeout throw an error. The owner records each command it admits, so a replayed command is answered from the record and not run again. Each host keeps these records and their outcomes in its own store under the mesh root (`control-seen/`) until the command has expired and left the event log. Each claim is also made in the shared state, so owners that run an older Fabric version for the same host cannot run the command again. That shared copy is kept while the command is in the retained event log, unless the fleet owner has ended support for runtimes before Fabric B8 (see [architecture](architecture.md)). The dashboard actions `s`, `u`, and `x` use the same route. Set `mesh.enabled` to use cross-process control. See [`references/agents.md`](../skillsets/typescript/fabric-exec/references/agents.md).

A root session stays reachable for 5 minutes after its lease lapses, because a late heartbeat under mesh lock contention does not mean the session ended. Fabric sends the command to that root's last owner host, which acknowledges it when the session is alive. After 5 minutes, or when the target has no record on this mesh root, the `Unknown Fabric participant` error states the reason.

#### Stale commands after restart

An owner keeps its host ID across restarts (a Pi session's Main ID, or a resident host's root-derived ID), and a durable actor keeps its participant ID. Each control plane process therefore has a random `incarnation`, and every participant record it publishes carries it as `ownerIncarnation`. Requesters copy that value onto `steer`, `followUp`, `stop`, and `ask` commands. A restarted owner replays the retained control log. It refuses an unclaimed command that names an earlier incarnation and never executes it. The caller receives this error without change: `Fabric control command targets a previous owner incarnation; the owner restarted. Re-resolve the participant and retry.` Fabric does not retry. Read the participant again and decide whether to resend. A command claimed by the earlier process before it stopped still reports its recorded outcome, or an indeterminate one. Acknowledgements carry the acknowledging incarnation, and a requester ignores an acknowledgement from any other incarnation than the one it targeted, except the fencing refusal above. Commands and records without an incarnation, from older Fabric versions, behave as before. Fencing assumes one live process per owner host ID at a time.

### Peer labels and queue gates

Every root participant mints a project-scoped label such as `FAB-1` when it first publishes: the prefix derives from the project directory basename (initials for multi-word names, up to three letters for single-word names) and the number comes from a mesh-wide monotonic counter, so retired labels are never reused. Labels appear on participant records, peer projections, and the dashboard, giving other sessions' tooling a stable handle to show users in place of raw session ids.

Two host-local, versioned events on `pi.events` let queue extensions coordinate with peers:

- `pi-fabric:peers:cards:v1` claims and resolves with live peer cards (`{ id, label, status, model?, cwd?, startedAt, updatedAt, pendingMessages }`), sorted by creation time.
- `pi-fabric:peer:await-settle:v1` claims and resolves once every watched peer (a `selector` label/id, or all peers when omitted) has been quiet for `settledForMs` (default 3s) since its last observed run. Peers that vanish from the mesh count as settled. The request accepts an `AbortSignal` for cancellation, and an optional `update` callback reports per-peer waiting status. Requests fail when the mesh is disabled.

[pi-queue-steer](https://github.com/monotykamary/pi-queue-steer-factory) uses both: its `/fabric await LABEL` gate row holds queued rows until the target peers settle.

### Participant lifecycle subscriptions

Use durable, source-qualified subscriptions when one participant must respond to the Pi or run lifecycle of another participant. Subscriptions differ from `agents.status()` because the host manages them across turns. The model does not need to poll.

```ts
const [peer] = await agents.peers();
if (peer) {
  await agents.subscribe({
    from: peer.id,
    events: ["pi.agent_settled"],
    to: "main",
    delivery: "followUp",
    triggerTurn: true,
    once: true,
  });
}
```

`agents.subscribe` accepts an exact source participant ID. You can use `"main"` for the caller's root. Supply one or more lifecycle events and an optional target, which defaults to Main. Set a `steer` or `followUp` delivery mode, an explicit `triggerTurn` policy, and the optional `once` flag. Inspect routes with `agents.subscriptions()`, and remove one with `agents.unsubscribe({ id })`. A subscription starts at the current mesh sequence and does not replay earlier events. Its delivery cursor remains durable across host restarts. The saved cursor moves at once past a delivered or skipped matching event. Past events that match nothing, it can trail by up to one read page (`mesh.maxReadEvents`), which a restarted host scans again. Delivery is at least once when a host crashes between inserting the target message and saving the cursor. Consumers can use the lifecycle event `id` to deduplicate side effects.

Pi events use these names: `pi.input`, `pi.agent_start`, `pi.agent_end`, `pi.turn_end`, `pi.agent_settled`, `pi.tool_error`, and `pi.session_compact`. Runner-neutral terminal events use `run.completed`, `run.failed`, `run.stopped`, and `run.timed_out`. Recovery events use `run.resumed` for a relaunched attempt after an unexpected stop, and `run.detached` for a run that outlived its caller's cancellation. The `tokens.usage` event provides bounded usage. The `component.state` event reports supervised component transitions. Lifecycle envelopes contain source identity and bounded operational metadata. They do not contain session transcripts.

A detached local `agents.spawn()` has a smaller convenience route. When `agents.notifyOnComplete` is enabled, terminal completion automatically sends Main a triggered follow-up. A call to `agents.wait()` makes the run foreground work and disables that detached notification. A wait that reaches its bound detaches the run again, so the notification still arrives.

For a code-owned typed alternative to a reasoning actor, use [Jev Main-turn observers](jev.md#main-turn-advisors-and-supervisors). They consume bounded selected event context with `program.nextEvent()`, work without mesh, and can explicitly opt into freshness-checked advice. They are session-owned Jev runs, not participant subscription targets.

## Persistent actors

`agents.create()` (also spelled `agents.createActor()`) makes a named actor. It and `agents.setInstructions()` accept either inline `instructions` **or** the pair `instructionsFile` + `sha256`, never both:

```ts
return agents.createActor({
  name: "reviewer",
  instructionsFile: "/home/paul/.local/share/smarty-dev/factory/current/roles/reviewer.md",
  sha256: "<lowercase 64-hex SHA256 of the file bytes>",
});
// The same pair works on agents.setInstructions({ id, instructionsFile, sha256 }).
```

The **owning host** (Main for local actors, resident for durable actors) resolves the file under the realpath of `agents.instructionsRoot`, defaulting to `~/.local/share/smarty-dev/factory/current/`. The root is configurable only in host configuration. Traversal components (`..`), outside-root paths, symlink escapes, non-regular or missing files, files over 512 KiB, invalid UTF-8, and digest mismatches are refused before actor state changes. Existing configured actor instruction size limits also apply. In-root symlinks are allowed, including a `current` symlink to a factory generation. The host reads one bounded byte snapshot and applies its text without BOM/newline normalization; `instructionsDigest` equals the supplied digest. Only the text is persisted, not a file reference; later file changes do not affect the actor. The existing >80% shrink guard still requires `replace: true` for intentional replacements.

**Platform boundary:** File-backed instructions require Linux and a genuine, accessible `/proc/self/fd`. The owner pins the canonical root with an `O_DIRECTORY` handle, checks its identity, then opens each canonical path component relative to pinned directory descriptors with `O_NOFOLLOW`. An ancestor link swapped between containment checks and opening cannot redirect the read outside the root; all handles close on success or refusal. Static in-root symlinks still work because they are canonicalized before that no-follow walk. Node does not expose a portable handle-relative open or Windows reparse-safe equivalent, so Windows/macOS/other hosts refuse **all** file-backed sources (including reparse-point paths) before filesystem access or actor mutation; use inline `instructions` there. Missing/inaccessible procfs also fails closed; there is no pathname-only fallback. This assumes the host controls mount topology/procfs; it does not defend against a privileged mount replacement.

The actor has a fixed runner, persistent runner session, serial mailbox, and optional subscriptions to parent-session events or durable mesh topics:

```ts
return agents.create({
  name: "auth-supervisor",
  instructions: `Watch the main session until the auth migration is complete and tested.
Prefer silence. Reply with a directive only for material drift, a blocker, or verified completion.`,
  events: ["agent_settled", "tool_error"],
  responseMode: "directive",
  delivery: "steer",
  triggerTurn: true,
  thinking: "high",
  tools: ["read", "grep", "find", "ls"],
  requires: ["memory.recall", { ref: "mcp.github.search", optional: true }],
});
```

A host-managed Claude actor uses the same mailbox and event interface. It keeps Claude Code context between activations:

```ts
return agents.create({
  name: "claude-reviewer",
  runner: "claude",
  model: "claude/haiku",
  instructions: "Review each delivered event and report only concrete regressions.",
  events: ["agent_settled", "tool_error"],
  responseMode: "directive",
  delivery: "steer",
  triggerTurn: false,
  tools: ["read", "grep", "find", "ls"],
});
```

Claude actors can keep context and use mapped Claude Code tools to inspect or edit. They consume host events and mesh messages that Fabric delivers, then return text or directives. They cannot directly call `fabric_exec`, `agents.*`, or `mesh.*`. Use a Pi actor when the actor must coordinate recursively through Fabric.

In a scoped session, every actor is bound to the principal that created it. Actor info reports `principal: { id, digest }`, and every turn, durable ones included, launches with that scope. Fabric delivers a message whose sender does not cover the actor's scope as untrusted data from a different or narrower principal, so a narrower session cannot borrow the actor's authority. An unscoped actor treats any scoped sender this way. See [principal and scope](providers.md#principal-and-scope) for the trust table.

### Shared actors and session bindings

A project-scoped actor stores one definition for the project. Select storage per actor with `agents.create({ scope: "project" | "session", ... })`; omitted scope uses `mesh.actorScope` as a compatibility default. Fabric keeps three pieces of state:

- **Project definition.** The shared registry stores the actor ID, instructions, runner, subscriptions, tools, and project model and thinking defaults.
- **Session binding.** A mode-`0600` file stores only `model` and `thinking` for one Pi session ID. It survives a resume of that session and does not rewrite `actors.json`.
- **Runtime.** One owner keeps the stable actor identity, serial mailbox, and runner session. Other sessions send direct work to that owner.

Fabric resolves each activation in this order:

```text
call override → session binding → project default → Fabric or runner default
```

```ts
const actor = await agents.actorStatus({ id: "release-reviewer" });

// Change only this Pi session.
await agents.setModel({ id: actor.id, model: "anthropic/claude-sonnet-4-6" });
await agents.setThinking({ id: actor.id, thinking: "low" });

// Override one call without changing the session binding.
await agents.ask({
  id: actor.id,
  message: "Review the release diff.",
  model: "anthropic/claude-opus-4-6",
  thinking: "high",
});

// Change the shared project default. This requires the runtime owner.
await agents.setModel({
  id: actor.id,
  model: "anthropic/claude-sonnet-4-6",
  scope: "project",
});
```

Omit `model` or `thinking` to clear the selected layer. A cleared session binding inherits the project default. A cleared project default inherits Fabric or runner configuration.

`FabricActorInfo.model` and `thinking` show the effective values for the caller. `binding` shows the session layer. `projectDefaults` shows the shared definition layer.

Own-root requests queue only explicit per-call model/thinking pins; omitted fields resolve from the owner's current session/project defaults when the activation launches, including after mailbox restoration. Foreign requests queue an already-resolved caller view: later owner binding changes cannot alter it, and absent fields use Fabric or runner configuration, never the owner's private session layer. This distinction survives persistence and restoration. An already-launched activation keeps its launch-time binding. `ask` waits for the owner to return a result. `tell`, `steer`, and `followUp` enqueue through the same owner. Pi and Claude actors both support these direct-call bindings.

Actor status distinguishes accepted work from a worker: `preparing` reports bounded setup in
`preparing.phase`, while `waiting` reports an AgentManager admission receipt in
`preparing.runId` with its current `queuePosition`. Only an admitted, launched worker gets
`inFlightRun` and `running`. Each actor-side pre-launch await has a 30-second deadline,
independent of the run timeout and legitimate permit waiting. A timeout logs
`ActorPreparationTimeoutError` (`FABRIC_ACTOR_PREPARATION_TIMEOUT`) with the phase,
returns the unlaunched activation to its durable queue with `preparationAttempts` incremented,
not the execution/restart `attempts` counter, and re-arms dispatch after a one-second backoff.
Each activation allows three preparation requeues; a further retryable preparation failure
reaches terminal exhaustion and does not requeue again. Infrastructure rejections use
`ActorPreparationError` (`FABRIC_ACTOR_PREPARATION_FAILED`); finite unavailable-model
errors still fail the activation. A timed-out presence publisher remains serialized and
owes the latest state, but drains do not keep joining the same stalled mesh write.

The mailbox, history, and runner session remain shared. Host events and mesh subscriptions run once on the owner and use the owner's session binding. Opening another Pi session does not start another copy of the actor.

Use `scope: "session"` when one root Pi session needs its own definition, mailbox, history, and runtime; use `scope: "project"` for repository-wide guardians. Both run concurrently. Session identity propagates to recursive participant agents in the same lineage. Under project scope, every trusted session can read shared actor definitions, mailbox history, and logs. Do not store secrets in them.

Use `requires` to declare exact `provider.action` capabilities for every activation. An entry can use `{ ref, optional: true }`. Before launch, the host resolves and keeps one view identified by its descriptor hash. Pi children separately resolve the portable semantic digest. They run with a closed-world Fabric surface, so a live provider addition cannot expand the actor during a run. When required refs are missing, mailbox work stays queued. `missingCapabilities` reports which capabilities are available, separately from `idle | queued | preparing | waiting | running | stopped`. Changes to providers or catalogs retry dispatch. Actor status and run metadata include the normalized requirements and last committed digest. Claude actors receive the host availability commitment. They have no child Fabric surface to limit. If the private Claude session was removed, the next activation reports a clear failure and preserves actor context. Recreate the actor when you need a new Claude session.

This primitive supports emergent supervisors and advisors without another extension. Actors can observe all session-bound public Pi extension events. These include resource discovery; session start, info, switch, fork, compaction, tree, and shutdown events; input and before-agent-start; agent, turn, and message lifecycle; context and provider request or response lifecycle; tool call, result, and execution lifecycle; model and thinking changes; and user bash. Event names match the Pi extension names, such as `input`, `before_agent_start`, `tool_call`, and `tool_result`. Fabric also adds the synthetic `tool_error`. The only exception is `project_trust`, which fires before Fabric can read the trusted project actor registry. Actors only observe intercepting Pi hooks. An actor runs asynchronously and cannot block a tool, rewrite context, change provider headers, or return another extension hook result. Shutdown observations and observations during immediate session replacement are best effort because the owner runtime is shutting down.

Fabric sanitizes host-event JSON before placing it in the mailbox. The JSON includes a bounded snapshot of the recent session. Fabric redacts fields that resemble credentials and encoded blobs. Pi `ImageContent` blocks follow another path. Fabric replaces every persisted block with an indexed descriptor. It sends the raw image outside the mailbox with the transient activation and automatically submits it to the selected Pi or Claude actor model. There is no media opt-in flag because the explicit event subscription defines the trust boundary. Raw image bytes never enter `actors.json` or the actor mailbox record. The selected runner's persistent model session can retain images through its standard session behavior. Use `activation.signal.media` to read descriptor metadata for freshness predicates and correlation.

Actors handle one message at a time. By default, they coalesce repeated host events, which is useful for `message_update` and `tool_execution_update`. They restore from the trusted project actor registry.

Mesh events queue one by one. Past `mesh.actorQueueLimit`, an actor's callerless work waits in its own overflow (up to eight times the limit), which is saved with its queue and runs in order as the queue drains, so one busy actor never holds other actors' delivery. Past the overflow, an event is recorded on the actor as dropped. An `ask` to a full queue still fails at once. When an actor always acts on the latest state of a subject, set `coalesceKey` to a dotted path into the event's `data`. A queued event of the same topic with the same string or number there is replaced by the newer one and keeps its place in the queue. A running activation is never replaced, so an event that arrives during a run still gets its own activation. A review actor that reads the current pull request head is the typical case:

```ts
const reviewers = await agents.actors();
const reviewer = reviewers.find((actor) => actor.name === "review-astra");
if (reviewer) await agents.setCoalesceKey({ id: reviewer.id, coalesceKey: "payload.number" });
```

Pass `coalesceKey` to `agents.create` for a new actor, or `null` to `agents.setCoalesceKey` to clear it.

#### Activation filter

An actor that runs a model on every event spends most runs on events it always ignores. Set `activationFilter` to a list of skip rules. Fabric checks each queued mesh or host event against the rules just before it would run the model. When a rule matches, Fabric skips the event with no model call. A rule only skips: it never acts, replies or changes the event. Direct messages (`ask`, `tell`) are never filtered. Fabric checks an event when it arrives, before it can join or replace a queued item, and again just before the run (for items queued before the filter was set). So a skipped event never replaces a queued one by `coalesceKey`: a comment edit that arrives while its comment's creation waits in the queue is skipped, and the creation still runs.

Each skip adds a record to the actor's message log: direction `in`, the event's `source`, and reason `filtered: <rule id>`. `agents.actorStatus({ id })` returns `filterSkipped: { count, lastKey, lastTopic, lastAt }`. `count` counts rejections since the filter was last set or cleared; the last fields are `null` until a rejection. `lastKey` is the queue's coalesce key (the JSON tuple `["mesh", topic, value]`) when present, otherwise the mesh event ID used for deduplication (or the host item's ID); `lastTopic` is the mesh topic or host event name, and `lastAt` is the rejection time in epoch milliseconds. This soft telemetry is stored with actor state, coalesced at the existing poll boundary without an extra fsync per skip, and restored after a host restart. A crash may lose the latest unflushed poll window. Resident actors return the execution owner's telemetry through the existing owner status RPC, including when read by the owning Main. The legacy `filteredCount` and `lastFilteredAt` remain lifetime counters.

Two presets come ready to use. Each had zero false skips in 24 hours of supervisor runs (smarty-dev#1579):

- `hold` skips a GitHub event (`github.*` topic) when the item's labels (`data.payload.issue.labels` or `data.payload.pull_request.labels`, whichever the event carries) include `hold`. The event that removes `hold` (action `unlabeled` with `label.name` `hold`) is always delivered. An event with no labels field is delivered. The rule also reads `data.payload.labels`, the label names in the factory's projected webhook payload.
- `never-message-events` skips `issues.field_added`, `issues.typed` and `issue_comment.deleted` GitHub events, `host:tool_error`, and `ops.owner` events of kind `actions.minutes`.

A custom rule is an object: `{ id, source?, topic?, kind?, where?, unless? }`. `source` (`mesh:<topic>` or `host:<event>`), `topic` and `kind` are lists of names; a trailing `*` matches a prefix. `where` is a list of predicates that must all match. `unless` is the rule's exception: the rule skips only when some `unless` predicate is known to be false (its field is present and does not match). For example, a held comment (action `created`) rules out the unlabel exception although a comment has no `label` field. A predicate is `{ path, equals }`, `{ path, in: [...] }` or `{ path, exists: true }`. `path` is a dotted path into the queued payload: for a mesh event that is the event itself (`topic`, `kind`, `data.payload.action`); for a host event, the event data. An array on the path fans out, so `data.payload.issue.labels.name` reads each label's name. A list of paths gives alternatives: the first path with a value is used.

Unsure means deliver. A `where` predicate whose field is missing does not match. An `unless` exception that no present field rules out stays open, so an `unlabeled` event with no label name is delivered. `exists: false` is not allowed. `agents.create` and `agents.setActivationFilter` reject an invalid rule, an unknown preset or a duplicate rule id. A rule must name a source, topic, kind or `where` predicate, so no rule can skip everything.

A supervisor that wakes on GitHub webhooks can drop comment edits, bots that are not on an allow list, and items another agent owns. The factory's projected payload carries `author` (`login`, `type`, `association`), `labels` (names) and `owners` (the names on the first `Owner:` line of the issue or pull request body). A field the factory cannot read is omitted, so the event is delivered (smarty-dev#2004):

```ts
[
  "hold", "never-message-events",
  { id: "edited", topic: ["github.*"], where: [{ path: "data.payload.action", equals: "edited" }] },
  { id: "bot-not-allowed", topic: ["github.*"], where: [{ path: "data.payload.author.type", equals: "Bot" }],
    unless: [{ path: "data.payload.author.login", in: ["smarty-fleet-write[bot]"] }] },
  { id: "not-owned", topic: ["github.*"], unless: [{ path: "data.payload.owners", in: ["fabric-v2"] }] },
]
```

A deny list is a `where` rule on `data.payload.author.login` with `in`. Keep `coalesceKey: "payload.number"` so a burst on one issue that queues while a run is in progress becomes one wake.

```ts
const supervisor = (await agents.actors()).find((actor) => actor.name === "dev-supervisor");
if (supervisor) await agents.setActivationFilter({ id: supervisor.id, activationFilter: ["hold", "never-message-events"] });
```

Pass `activationFilter` to `agents.create` for a new actor, or `null` (or `[]`) to `agents.setActivationFilter` to clear it. A change applies from the next queued event and resets `filterSkipped`, even when setting the same filter; legacy lifetime counters stay.

For a temporary review claim, set a finite `expiresAt` (epoch milliseconds) on a live actor:

```ts
await agents.setActivationFilter({
  id: "release-reviewer", activationFilter: ["hold"], expiresAt: Date.now() + 60_000,
});
```

At or after that time, the next event or existing poll clears the filter **before** testing an event, resets `filterSkipped`, and records an actor message with source `actor:activation-filter` and reason `activationFilter cleared: expired`. Explicit clears record `activationFilter cleared: explicit`. The expiry survives restart and is visible as `activationFilterExpiresAt` in actor status; setting a filter without `expiresAt` removes any previous expiry. No additional timers are created. Global templates do not support expiry. `clearWhen` verdict-based clearing is not implemented: use expiry or explicitly clear after observing the PR's complete verdict.

A stored filter that this version cannot read (for example, one written by a newer version or edited by hand) never removes or rewrites its actor or global template. Fabric keeps the stored value unchanged, applies no filter (every event is delivered), logs a `PI_FABRIC_ACTIVATION_FILTER` warning, and shows the reason in `activationFilterError`. Set a valid filter to repair it.

### Native asynchronous vision handoff

A vision handoff does not require a separate extension that watches events. Create one persistent actor, select a multimodal model, and subscribe to `input`. Fabric automatically detects and attaches images from the prompt. Passive `steer` sends the description to Main without starting an unrelated idle turn. Set `coalesce: false` to preserve separate image prompts while the vision actor is busy:

```ts
return agents.create({
  name: "vision-handoff",
  instructions: `Inspect every attached image from the parent prompt.
Return { action: "silent" } when no image is attached.
Otherwise return { action: "message", message } with a precise, compact visual description
that Main can use without seeing the image. Do not answer the user's broader coding task.`,
  events: ["input"],
  runner: "pi",
  model: "provider/multimodal-model", // use a key from tools.models()
  responseMode: "directive",
  delivery: "steer",
  triggerTurn: false,
  coalesce: false,
  validWhile: ({ activation }) =>
    activation.kind !== "hostEvent" || (activation.signal?.media?.length ?? 0) > 0,
  tools: [],
  extensions: false,
});
```

`validWhile` removes input activations without images before a model run. Ordinary text prompts use no vision-agent inference. The actor persists. Dispatch stays asynchronous. Main never waits for the visual description during its current inference. Subscribe to `before_agent_start` when the actor needs Pi's expanded prompt or system context. A subscription to both events creates two activations for one user prompt.

`validWhile` supplies a programmable freshness guard for persistent actors. Fabric serializes the source of its pure synchronous function. It checks the function before starting queued work and before delivering completed work. Fabric also stores it with project actors and global templates. The immutable `activation` fact describes a `hostEvent`, `direct`, or `mesh` activation. The `current` object contains `latestActivationSequence`, `mainRevision`, `taskRevision`, `idle`, and `now`. Main revisions increase after completed tools and lifecycle events. A tool-error review can become stale after Main recovers. Return `false` or `{ valid: false, reason? }` to discard stale work. Fabric records invalidated fire-and-forget work as a silent stale outbox entry. An invalidated `agents.ask()` rejects. Predicates must be synchronous. They cannot call tools or use closures because their source must run after restoration.

```ts
return agents.create({
  name: "fresh-reviewer",
  instructions: "Review only the latest useful parent-session event.",
  events: ["tool_error", "agent_settled"],
  responseMode: "directive",
  delivery: "steer",
  triggerTurn: false,
  validWhile: ({ activation, current }) => {
    if (activation.kind !== "hostEvent") return true;
    if (activation.sequence !== current.latestActivationSequence) {
      return { valid: false, reason: "a newer activation exists" };
    }
    if (activation.event === "tool_error") {
      const signal = JSON.stringify(activation.signal ?? {});
      const incidental = /ENOENT|no matches found|exit code 1/i.test(signal);
      if (incidental && activation.mainRevision !== current.mainRevision) return false;
    }
    return activation.taskRevision === current.taskRevision;
  },
});
```

Pi actors keep model context in a Fabric-owned Pi session file. Claude actors keep the session ID returned by the official CLI and use `--resume <id>` after the first message. Every activation reapplies tools, permissions, schema, and system-prompt flags. Fabric also stores a runner-neutral stream transcript. `thinking` accepts `off`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max` and uses the precedence described above. In the dashboard, `e` changes this session and `E` changes the project default. The persisted `tools` array is an allowlist. Set it during creation, replace it with `agents.setTools({ id, tools })`, or press `o` in the dashboard. An empty array disables optional tools. Pi actors keep the host-required `fabric_exec` capability for mailbox and mesh coordination unless you create them with `extensions: false`. This setting removes Fabric from a Pi actor. Its activation then runs without `fabric_exec`, `agents.*`, or `mesh.*`. The host continues to manage its mailbox and delivery. The setting alone does not make the actor read-only because `tools` still defaults to `agents.defaultTools`. Also set `tools: ["read", "grep", "find", "ls"]` for a read-only persistent actor. Use `tools: []` for an actor without tools.

### Response modes and delivery

Choose one of these response modes:

- With `text`, each non-empty response becomes an actor outbox message.
- With `directive`, validated output in the form `{ action: "silent" | "message" | "stop", message?, data? }` lets the actor choose whether to intervene.

Delivery can stay in `mailbox` or enter the main session through `steer`, `followUp`, or `nextTurn`. For `steer` and `followUp`, explicitly set `triggerTurn: true | false`. A value of `true` starts Main when it is idle. A value of `false` is passive, and Fabric labels it as unable to start Main. The `mailbox` and `nextTurn` modes never start Main, so they reject `triggerTurn: true`. This policy keeps a delivered actor message from looking like a stalled continuation. Fabric applies no extra 8,000-character truncation to local actor or agent messages sent to Main. Standard limits for model context, providers, and cross-mesh event size still apply.

### Inference history per activation

Pi actors use full inference history by default. To exclude **prior activations** from model input without removing their journals, select `inferenceContext: "activation"` at creation or change the existing actor:

```ts
const actor = await agents.actorStatus({ id: "release-reviewer" });
await agents.setInferenceContext({ id: actor.id, inferenceContext: "activation" });
const selected = await agents.actorStatus({ id: actor.id });
// Restore the default policy explicitly:
await agents.setInferenceContext({ id: actor.id, inferenceContext: "full-history" });
```

Only the owner can change a live actor. The setting persists on the same ID and applies when the next activation starts; running work keeps its snapshot. `scope: "global"` changes a template. Export/import retains the policy but never imports history. The setting does not change instructions, model, effort, tools, subscriptions, checkpoints, delivery, quiet/hold rules, or retained task references.

Before starting Pi, the worker gives an activation-mode actor a header-only inference session. Preflight hooks therefore see no carried conversation either; a late context projection alone cannot protect preflight window checks. The native context hook remains a fail-closed boundary. After the child exits, the worker appends its complete audit to the actor's full session journal before publishing terminal status, linking its new root to the previous journal leaf. Activation-local compaction and context-edit records are retained inside non-message `custom` audit entries (`fabric-activation-context`, with their original records and activation ID), not as durable inference checkpoints. Restoring `full-history` therefore includes earlier activations and original tool outputs; genuine full-history native compactions still apply. No existing journal entries are rewritten or deleted. The full session journal remains available. User directions and assistant decisions remain in model input; earlier tool results may be compacted inside the activation, while the latest complete tool-call/result batch remains verbatim. Unmerged `.activation-<run id>.jsonl` files live beside the actor journal until retention succeeds, so a hard worker crash or retention error preserves recovery evidence beyond disposable run cleanup. This bounds prior-activation accumulation, and in-run tool compaction reclaims context before the next inference request. `extensions: false` still excludes Fabric and project extensions; the worker adds only the projection hook, with no tools or trust expansion.

Activation mode requires a Pi CLI with the process-local `--no-auto-compaction` option and its explicit `get_state` readback: `autoCompactionDisabledForProcess: true` and `autoCompactionEnabled: false`. This avoids old-history preflight compaction even with a large retained journal. Fabric never uses the persistent `set_auto_compaction` RPC setter. Unsupported runners/binaries, missing or mismatched hook readiness, and projection failure reject before inference. Manual compaction, unexpected threshold/overflow compaction, and session replacement are not supported during this mode. Compaction cannot bypass the projection to send historical context to a summarizer. A fatal projection/compaction error exits only the bound disposable child; the owner and retained journals remain intact. At a completed `turn_end`, Fabric estimates the last final provider payload plus the new assistant/tool results. At 80% of that dispatch model's window, it appends Pi native compaction and context-edit entries to bound **earlier** tool results to 1,024-character excerpts with original journal-entry references. This deterministic compaction makes no summarization model request and does not rewrite original entries. User directions, assistant decisions, call IDs, and the latest complete tool batch are retained; the checkpoint and every retained message are witnessed before accepting the next context. Idle registration does not load the compaction helper. A latest batch or fixed context that still cannot fit after compaction fails once, without truncating that batch or silently falling back to full history. After this turn-end compaction, the worker wraps native provider dispatch's awaited `onPayload` callback, then checks the final request after all `context`, `context_with_system`, and `before_provider_request` handlers. It uses the selected Pi AI's `estimateTextTokens` on the entire serialized JSON request, including system/messages, tool schemas, API-specific fields, and JSON framing, without trusting earlier assistant usage. This deliberately conservative estimate may reject near-limit requests or large encoded media. It dispatches the admitted JSON snapshot so retained references or stateful serializers cannot enlarge it after admission; unsupported non-JSON payloads fail closed. Custom providers must honor Pi's `onPayload` instrumentation contract before network dispatch. A late expansion cannot bypass admission, and a late reduction is not refused using its earlier raw size. A window overflow exits the disposable child once; neither native retry nor Fabric startup/resume recovery relaunches that refusal. Fabric resolves a discovered or configured Pi launcher to its physical artifact before the first launch and reuses that selection on recoverable retries, so retargeting a launcher symlink cannot change the retry's host capabilities. The owning host immediately records and publishes `actor.alarm` on `ops.owner` and sends Main a `followUp` with `triggerTurn: true`, even for mailbox actors. Repeated failures in the same streak do not spam the owner; a successful activation clears the streak. Full-history mode retains its native history/compaction policy and does not discard it as activation history.

The actor cannot change delivery from its response. The owner can update a live actor or global template and keep its history:

```ts
const actor = await agents.actorStatus({ id: "release-reviewer" });
await agents.setDeliveryPolicy({
  id: actor.id,
  delivery: "steer",
  triggerTurn: true,
});
```

Set `scope: "global"` to update a reusable template. In the dashboard, press `y` on an actor or template, then choose mailbox, steer, follow-up, or next-turn delivery. Lowercase `m` and `e` change this Pi session. Uppercase `M` and `E` change the owner-gated project defaults. These changes keep the Pi or Claude runner session. Use `agents.ask()` for a blocking exchange and `agents.tell()` for fire-and-forget mail. Both accept one-activation `model` and `thinking` overrides. Read shared history with `agents.messages()`. `agents.remove()` is local-owner-only for session actors and routes durable actors to the resident owner.

### Fresh actor sessions

A persistent actor keeps one Pi session across activations. The session can grow too large to compact, for example when compaction fails with `Summarization failed`. Call `agents.resetSession({ id })` to start the actor on a fresh session:

```ts
const actor = await agents.resetSession({ id: "release-reviewer" });
```

For a session actor, the call works only on the owning session or host; a foreign owner receives `Fabric actor is owned by another host: <id>`. For a durable resident actor, the owning root Main routes the repair to its resident host; foreign roots and inherited task/actor lineage cannot reset it. When a run is in progress, the call waits at the fenced activation boundary until the run settles, then returns the same actor to service. It does not interrupt the run or clear its mailbox. Request reset directly; it is non-destructive repair, not terminal cancellation. An explicit `agents.stop()` cancels a pending boundary reset, terminates the owned activation, and drops queued work; the reset reports `ACTOR_SESSION_RESET_CANCELLED` without rotating the journal. Resident status and other commands remain serviceable while reset or stop waits for the activation fence. Resetting an explicitly stopped actor changes its history only; it does not resume it. Fabric then moves `<actor dir>/session.jsonl` to `session.jsonl.<UTC stamp>.bak` in the same directory, for example `session.jsonl.20260927T145012345Z.bak`. Fabric keeps the two newest backups and deletes older ones. The next run starts a fresh Pi session. A Claude actor also drops its stored runner session ID. The call returns the new `FabricActorInfo` and publishes presence.

The reset keeps instructions, topics, bindings, events, the queue, the overflow, and the message log. Queued work goes to the fresh session. The message log records the reset as an `out` message from source `fabric-host`, with reason `session reset (requested)` or `session reset (size limit)` and data `{ sessionReset: { trigger: "requested" | "size", bytes, archived } }`. `archived` is the backup path, or `null` when there was no session file.

Fabric also resets a session automatically. Before a run starts, it checks the session file against `actors.maxSessionBytes` (default 20 MiB, `0` disables). A larger file gets the same reset with trigger `size`, so Fabric never starts a run that must compact a session past the limit. Durable actors use the same setting. See [configuration](configuration.md#actors).

`agents.compact()` compacts a running task agent only. With an actor ID, it throws `<name> (<id>) is a Fabric actor, not a task agent: ...` and points to `agents.resetSession()`.

## Paged agent logs

`agents.log()` reads bounded pages from JSONL logs. The first call returns the newest entries. A next-page call must pair its `before` byte offset with the returned `generation`, passed as `beforeGeneration`. For an actor run these fields are inside `run`; for an actor session use `sessionBefore` and `sessionGeneration` with `type: "session"`. An initial `type: "all"` returns both streams, but a bound next page must select one stream.

```ts
const { id } = await agents.actorStatus({ id: "release-reviewer" });
const newest = await agents.log({ id, type: "run", lines: 100 });
const page = "run" in newest ? newest.run : "events" in newest ? newest : undefined;
if (page?.hasMore) {
  return await agents.log({
    id, type: "run", lines: 100,
    before: page.before, beforeGeneration: page.generation,
    ...("runId" in page ? { runId: page.runId } : {}),
  });
}
return newest;
```

Terminal run-log compaction can replace the file atomically. A stale generation, or a public call with a bare `before` and no generation, returns the named `cursor-stale` error and reads no wrong bytes. Re-read from the start without the cursor pair, or supply the generation returned with that cursor. This deliberately tightens the old numeric-only paging contract. Same-file appends do not invalidate a bound cursor; internal descriptor readers remain compatible.

## Global actor templates

Persistent actors belong to a project mesh. If you want to reuse a persona in several projects, store it in the project-independent **template library**. This library is in your agent directory at `~/.pi/agent/fabric/actors/`. A template contains only an actor definition, including its name, instructions, subscriptions, and run settings. It contains no mailbox, session transcript, run logs, or other history. Templates are inactive. Import one into a project to run it.

```ts
// Store a reusable persona in the global registry. This does not create a live actor.
await agents.create({
  name: "security-reviewer",
  instructions: "Review changes for security defects. Reply with a directive only for material drift.",
  events: ["agent_settled"],
  responseMode: "directive",
  scope: "global",
});

// List the templates. Then create a new actor from one in the current project.
const [template] = await agents.actors({ scope: "global" });
const actor = await agents.import({ name: template.name });            // create it without inherited history
await agents.import({ name: "security-reviewer", as: "security-reviewer-2" }); // rename it if the name exists

// Copy a tuned project actor to the global library without its history.
await agents.export({ id: actor.id, write: true, overwrite: true }); // a global write: write: true is required

// Change the default instruction and continuation policy of a template.
await agents.setInstructions({ id: template.id, instructions: "Be brief.", scope: "global" });
return agents.setDeliveryPolicy({
  id: template.id,
  delivery: "steer",
  triggerTurn: false,
  scope: "global",
});
```

`agents.setInstructions` can also change a live project actor. Its default scope is `"project"`. The new instruction applies to the next queued actor message. `agents.instructions({ id })` reads the live text with its sha256 `instructionsDigest`; it writes nothing. `agents.export` requires `write: true`, so a caller cannot create a template by mistake. Only definitions cross the project⇄global boundary. Import and export never move history. Slash commands provide the same operations. `/fabric global` lists templates. `/fabric import <name> [as <new>]` creates one in the project. `/fabric export <id> [--overwrite]` promotes a project actor. The dashboard shows global templates with live actors. From there, you can import, export, delete, edit instructions, and change delivery policy without code. Existing persisted actors and templates continue to load as passive. New active delivery definitions must explicitly set `triggerTurn`.

## Councils

```ts
return council.run({
  task: "Review the current implementation and recommend whether it is ready to merge.",
  roles: ["correctness reviewer", "security reviewer", "test reviewer"],
  transport: "process",
  synthesize: true,
});
```

Council members run at the same time under the global agent semaphore. When `synthesize: true`, a final child agent combines their reports. See [`/skill:fabric-council`](../skillsets/typescript/fabric-council/SKILL.md).

## Recursive queries

```ts
return rlm.query({
  runner: "pi",
  task: "Recursively decompose this repository and produce a compact architecture map.",
  transport: "process",
});
```

`rlm.query()` calls `agents.run({ runner: "pi", recursive: true })` with Fabric enabled in the child; it accepts `cwd?: string` and `worktree?: boolean`, just like one-shot agent runs. Recursive spawning means Fabric agent composition, not recursive filesystem traversal. Fabric rejects Claude runners for recursive use. It also rejects recursion at `agents.maxDepth`. This setting accepts any non-negative safe integer, and `0` disables child spawning. Approval for the initial recursive call delegates only the `agent` risk capability to recursive children. It does not delegate approvals for network access, execution, or writes. Each Fabric process applies its own configured concurrency and timeout limits. When `agents.budgetUsd` is set, a shared append-only cost ledger limits total spending across the recursion tree. Each node writes the cost of its children to one ledger file that it receives through the environment. A node rejects a new child when accumulated spending reaches the budget. This check is best effort. Concurrent children can pass the check before another child records cost, so the tree can exceed the limit slightly. Use `agents.maxPerExecution` as the race-free ceiling. Results and live status for each recursive child include a `budget` summary with `limit`, `spent`, `remaining`, and `tokens`. Fabric keeps the latest bounded nested-agent status tree in memory. Session-owned nested agents stop when their owning Pi child exits; a detached spawn does not outlive that owner. The child manager leaves nested status and transcript files with the enclosing run so completed leaves remain inspectable in **Topology · Run** and `/fabric chat`. The enclosing run's cleanup, retention, or root-session shutdown removes those artifacts. Every terminal path refreshes the nested tree. A descendant without a retained terminal result is shown as failed with an owner-ended diagnostic, not indefinitely running; Fabric does not invent a successful result or rewrite the worker's status file. Independently durable descendants retain their own lifecycle. Fabric releases the snapshot when the parent run is cleaned up or the Fabric session shuts down.

`agents.nice` (0-19, default `0`) lowers the CPU priority of every child agent, and its IO priority on Linux (`ionice -c2 -n7`). The child's bash tools inherit it. A per-call `nice` on `agents.run`/`agents.spawn`, `agents.create({ nice })` or `agents.setNice({ id, nice })` for an actor can only raise it above the configured value, never lower it; it is clamped to 19. Details: [configuration](configuration.md).

`agents.maxTokensPerChild` limits cumulative token use for each child. Its default value, `0`, disables the limit. The wall-clock `timeoutMs` limits time, and `budgetUsd` limits cost. This limit caps the context of one runaway child before the host session compacts. Fabric stops the child with the same `timed_out` status and a `token limit` error. See [`/skill:fabric-rlm`](../skillsets/typescript/fabric-rlm/SKILL.md).

## Durable mesh coordination

The `mesh` API provides project-scoped, event-sourced coordination:

```ts
const event = await mesh.publish({
  topic: "team.auth",
  kind: "finding",
  text: "Refresh-token rotation is not atomic",
  data: { path: "src/auth/refresh.ts" },
});

const task = await mesh.put({
  key: "tasks/auth-review",
  value: { status: "ready", owner: null },
  ifVersion: 0,
});

const claimed = await mesh.put({
  key: task.key,
  value: { status: "claimed", owner: "security-reviewer" },
  ifVersion: task.version,
});
return { event, claimed };
```

Topics provide durable channels and direct messages with sequence cursors. `mesh.members({ scope?, kinds? })` returns the same combined directory of roots, agents, and actors as `agents.members()`. Versioned `get`, `put`, and `delete` operations provide compare-and-swap state for task claims, leases, reservations, and decisions. You can combine these operations with persistent actors to implement messenger-style swarms in Fabric code. Messenger-style swarms need no fixed planner and worker roles or user-managed daemon. When guest code requests durable residency, Fabric starts the hidden resident host described earlier. See [`/skill:fabric-swarm`](../skillsets/typescript/fabric-swarm/SKILL.md) for the pattern and [`references/mesh.md`](../skillsets/typescript/fabric-exec/references/mesh.md) for the complete API.

Host scripts that run outside Pi, such as schedulers and maintenance tools, import `MeshStore` from `pi-fabric/mesh`. It uses the same lock, revision clock and event log as Fabric, and it loads no extension or agent runtime. Point it at the same mesh root as the Fabric sessions it works with.
Each event carries a host-set `sender` with the publishing session's authority: `{ authority: "host" }` when unscoped, or its principal and scope digest. A scheduled event keeps the stamp from scheduling time. Events from older builds and grant posts have none. Actors use the stamp for the [principal trust rule](providers.md#principal-and-scope).

A headless resident host can also be woken by time and by an outside process. Both primitives are small: recurrence, retries and routing stay in actor code.

### Scheduled events

```ts
const pending = await mesh.publish({
  topic: "jobs.nightly",
  kind: "tick",
  notBefore: "2030-01-01T02:00:00Z", // or epoch ms, or afterMs: 3_600_000
  key: "nightly",                    // optional: replace or cancel by key
});
await mesh.unschedule({ key: "nightly" });       // { removed: boolean }
return await mesh.scheduled({ topic: "jobs.nightly" });
```

- `notBefore` (epoch milliseconds or an ISO 8601 date-time) or `afterMs` stores the event as a pending schedule and returns it with `scheduled: true`. A due time in the past is appended at once. Due times may be at most 366 days ahead, and a mesh root holds at most 1000 pending schedules.
- `key` (at most 128 characters) makes a schedule replaceable: publishing again with the same key replaces the pending one in the same locked write, so concurrent publishers never leave two. `mesh.unschedule({ key })` cancels it. `mesh.scheduled({ topic?, limit? })` lists pending schedules by due time; it is a read and is speculation-eligible.
- Release: whichever Fabric process polls the mesh first after the due time appends the event, under the mesh store lock, so two releasers cannot append it twice. Every actor mesh monitor checks for due schedules on each poll and arms a timer to the next due time; `mesh.publish`, `mesh.read` and `pi-fabric mesh post` release due schedules too. The released event keeps the schedule's id as its event id and carries `scheduled: { dueAt, key? }`. Fabric appends the events before it shrinks the schedule file, so a crash between those two writes re-appends with the same id: consumers that must be exactly-once deduplicate by event id.
- Recurrence stays in user code: an actor subscribed to the topic reschedules on each wake.
- The resident host stays alive while it owns a live durable participant, re-arms its wake timer to the next due time, and releases due schedules itself. **Limitation:** Fabric adds no system daemon. If no Fabric process for the project is running when a schedule falls due, the event is released the next time any Fabric process touches that mesh root, then delivered to subscribers as usual.
- Pending schedules live in `schedules.json` beside the mesh state. They are serialized by the mesh lock, outside the verified storage kernel's per-key revision table, which still owns every `mesh.put`/`mesh.delete` compare-and-swap decision.

### External grants

```ts
const grant = await mesh.grant({ topic: "hooks.ci", ttlMs: 86_400_000, uses: 10, kind: "build" });
return grant; // { grantId, token, expiresAt, uses, command, ... }
```

An outside process (a CI job, a cron entry, a webhook relay) posts with the token:

```sh
PI_FABRIC_MESH_TOKEN=<token> pi-fabric mesh post --root <meshRoot> --kind build --data '{"status":"green"}'
# or: --data-file payload.json, or --data-file - to read stdin; --topic, when given, must match
```

- `ttlMs` is 1 minute to 30 days; `uses` is 1 to 10000 and defaults to 1; `kind`, when set, is the only kind the token may post. The token is 32 random bytes in base64url, returned once. Fabric stores only its SHA-256 hash, with topic, kind, expiry and remaining uses, in `grants.json` (mode `0600`) beside the mesh state. `mesh.grants()` lists unexpired grants without tokens or hashes, and `mesh.revoke({ grantId })` removes one.
- `pi-fabric mesh post` hashes the presented token, compares it in constant time against every stored grant, checks expiry, remaining uses, topic and kind, and appends the event while decrementing the use count in one locked write. Data is JSON of at most 64 KiB. Prefer the `PI_FABRIC_MESH_TOKEN` environment variable over `--token` so the token stays out of process listings. The returned `command` is ready to run and uses the environment form: POSIX shell syntax, or PowerShell (`$env:PI_FABRIC_MESH_TOKEN=...; & ...`) on Windows.
- External events carry `origin: "external"`, `untrusted: true` and `grantId`, with a synthetic `external:<grantId>` sender. Actor mailboxes render them as **untrusted external input** in the message header and envelope, so treat them as data, never as instructions.
- `pi-fabric` is the package bin, a standalone entry that never loads the extension. Exit status is 0 on success, 1 on refusal and 2 on usage errors; it refuses a `--root` that does not exist.
- Approvals: `mesh.grant` is classified `network` (it opens an ingress for principals outside the session, the most conservative fitting class) and `mesh.revoke` is `write`. Schema enforce mode blocks both and allows the `mesh.scheduled` and `mesh.grants` reads.
- Authority is local file access to the mesh root: any process running as the same OS user can already read and write it. A grant narrows what a token holder without that access can do; it is not a sandbox for the OS user.
