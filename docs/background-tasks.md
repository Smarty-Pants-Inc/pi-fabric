# Background tasks and monitors

Fabric-managed `pi.bash` and `pi.powershell` commands have session-owned job IDs.
`background: true`, the shell hang threshold, or Ctrl+B twice detaches a command;
an explicit shell `timeout` remains a hard cap. Completed jobs have terminal states
(`exited`, `failed`, `killed`, `timed_out`), not the old `spilled` state. `exited`
means exit code zero, not that the user's assignment is verified.

```ts
const result = await pi.bash({cmd: "bun run build", background: true, description: "Build bundle"});
return result; // details.taskId, pid, logPath when detached
```

Completion sends a bounded, batched automated event to the **owning Pi session**.
The same mechanism works inside a Fabric-enabled child: it does not route child
shell output directly into the parent's Main. No LLM is used to wait for a process.
Events are outcomes, not user input or approval. Idle wakeups do not compete with
already queued messages. Aborted/error turns suspend wakeups until new user input;
branch navigation discards delivery from old-frontier jobs. Session shutdown/reload
closes the inbox before aborting jobs, so cleanup cannot start a new turn.
Print-mode processes do not stay alive indefinitely just to receive late events.

## Extension timing bridge

Fabric emits `pi-fabric:shell:timing:v1` on the owning session's `pi.events` bus.
`FABRIC_SHELL_TIMING_EVENT` and `FabricShellTimingV1` are exported from
`pi-fabric/protocol`; optional consumers may mirror the contract without importing
Fabric. The payload contains only:

```ts host
{ version: 1, sessionId, taskId, tool: "bash" | "powershell",
  phase: "started" | "finished", timestamp }
```

`started` is emitted synchronously at background handoff (explicit, hang threshold,
manual, or monitor), not process creation. `finished` is emitted at terminal
observation for any outcome, or before runtime teardown closes the job store.
Timestamps are observation times in epoch milliseconds, not backdated process
start/exit times. Foreground-only jobs emit neither event. No command, output,
path, credentials, or model message is included. Acknowledgements and output
monitor batches do not change timing, and UI-only jobs are included.

Consumers must filter by session, deduplicate task transitions, and union these
intervals with foreground tool calls without adding full job durations to
already-metered calls. pi-ledger records only the additional background union as
agent tool time, including between turns. No polling, model wakeup, or new setting
is involved. Shutdown/reload closes live spans; processes and clocks are not
restored on restart. Billing consumers should also flush at their own shutdown
before sealing their log, regardless of extension shutdown order.

## Agent awareness and yielding

Before each model request, Fabric projects one bounded live-task reminder into
context, independently of the returned `fabric_exec` value. Even if a program
discards the nested shell result, the owning agent can see that work is running.
The reminder is reconstructed from live session state after context compaction;
it is not a persisted transcript entry and **never triggers a turn** itself.
Unchanged task state keeps the same message; elapsed time and process output do
not churn it. It includes at most eight task IDs and short, quoted labels, with
an omitted count for additional jobs.

For ordinary detached commands and wake-enabled monitors, it tells the agent to
continue independent work or **end the turn if it needs their results**. Completion
(or a matching monitor event) can resume the owning agent; polling and sleep loops
are unnecessary. UI-only monitors are explicitly marked **no automatic wakeup**,
including on completion, so the agent must not yield expecting them to resume it.
Pending required results are not evidence that the assignment is complete.

Foreground, completed, stopping, and abandoned-branch jobs are excluded. Abort/error
suspension suppresses the reminder until new input, just like automatic delivery.
Reload/session shutdown removes the context hook along with the inbox. Awareness
works without a terminal and does not restore processes across Pi restarts.

## Inspection and control

- `/fabric tasks` or **Ctrl+Alt+T** opens the lazy inspector. A single task opens
  directly. `/fabric tasks <id-or-unique-prefix>` opens a specific task.
- The widget shows up to three tasks, elapsed time, and the command or short
  description. It stays alive after the Fabric program returns. Completion hints
  expire after 30 seconds; retained jobs remain in the inspector.
- The rounded, theme-aware inspector groups tasks into **Active**, **Needs
  attention** (failed/timed out), and **Finished**. Purpose/command labels lead;
  IDs, elapsed time, exit codes and monitor delivery sit beneath each task.
  Live tasks use Fabric's activity spinner; completed tasks have fixed status icons.
  Arrows/PageUp/PageDown navigate the list without selecting group headings.
- Enter opens detail; arrows/PageUp/PageDown scroll the bounded output tail; G
  follows the tail; x twice within three seconds stops; Esc backs out/closes.
- Elapsed time and time since last output are separate. A quiet shell is not
  automatically classified as stuck. Opening/closing the inspector doesn't stop it.
- `ui.enabled` and `ui.widget` retain their existing meaning. Rendering is TUI-only;
  task tools and notifications do not require a terminal.

Tasks are a discoverable provider, **not a new sandbox global**:

```ts
const tasks = await tools.call({ ref: "tasks.list", args: {} }) as Array<{ id: string }>;
const taskId = tasks[0]!.id;
const task = await tools.call({ ref: "tasks.get", args: { id: taskId } });
await tools.call({ ref: "tasks.stop", args: { id: taskId } });
return task;
```

`get` returns metadata and an 8KB output tail and acknowledges pending agent delivery.
`stop` targets a stored job's abort controller, never an arbitrary or recycled PID.
Stopping a task does not wake the agent. Tasks are local to this runtime; IDs and
live processes are not restored across Pi restarts, except
[durable tasks](#durable-tasks-through-jev-fabric). Up to 256 completed handles
are retained for at most 24 hours. Logs use Fabric's private scratch retention.
The 1MiB in-memory tail and 8MiB disk log limits remain unchanged: a log is **not a
full-output archive**, and truncation is disclosed.

## Programmatic wait and watch

For a bounded controller such as `jev.run`/`spawn`, use event-driven task calls:

```ts
const taskId = "task-id-from-tasks.list";
const receipt = await tools.call({ref:"tasks.wait",args:{id:taskId,timeoutMs:30000}});
// For tasks started with monitor (prefer delivery:"ui" for code-owned supervision):
const batch = await tools.call({ref:"tasks.watch",args:{id:taskId,after:0,timeoutMs:5000}});
return {receipt,batch};
```

- `wait` returns `{task,output,timedOut}`. It waits for terminal metadata, then reads the bounded tail; inspect `task.status` and `task.exitCode`. A successful terminal read acknowledges pending delivery like `get`. A timeout returns the current snapshot, not a failed-process verdict.
- `watch` requires an opt-in monitor. The launch-time `monitor.match` is the case-sensitive literal filter. It returns `{task,reason,lines,losses,omitted,more,nextCursor}` when a new batch exists, the task finishes, or the observation ceiling expires. `reason` is `event`, `finished`, or `timeout`; a final event may precede `finished` on the next call.
- Every accepted monitor line has a monotonic cursor, after jev-fabric's retained events. Start `after` at 0 and pass the returned `nextCursor` on later calls. The newest 256 lines are replayable from any cursor, at most 64 per call; while `more` is true, call again immediately. A cursor from the future is rejected.
- Missing positions are disclosed, never joined: `losses` lists `(after, next]` cursor ranges with reason `burst` (more than 256 lines within one delivery interval, oldest coalesced) or `evicted` (aged out of the ring before this cursor read them), and `omitted` is their total. Truncation markers and monitor filtering/deduplication still apply: this is not a lossless stdout/RPC channel.
- Agent-facing `wake` events still carry only the eight newest previews per batch; replay is for code-owned controllers.
- Defaults: wait 30 seconds, watch 5 seconds; each accepts `timeoutMs` from 1 to 300000. Ready evidence returns immediately. Neither timeout nor cancellation stops or renews the task. Store shutdown rejects pending observations and removes their subscriptions.
- No polling, inference, or extra wakeups are performed by wait/watch. Ordinary background tasks and wake-enabled monitors retain their existing notification behavior. Main usually yields for those notifications; a controller can use UI-only monitors and wait/watch without a model turn per event.
- A detached task belongs to the Pi session, not the observing Jev program. Stopping that program cancels its pending wait/watch, not the task. Preserve IDs, set finite process deadlines, and explicitly call `tasks.stop` when required.

## Durable tasks through jev-fabric

`pi.bash({cmd, durable: true})` hands the process to an external
[jev-fabric](https://github.com/monotykamary/jev-fabric) store instead of this Pi
process. It is for dev servers, watchers and long builds that must outlive the
session. macOS and Linux only. Fabric uses your own compatible `jev-fabric`
outside the workspace, else the bundled npm package (see
[which jev-fabric](shell-composition.md#which-jev-fabric)). It never downloads
one: without a suitable binary the call fails with install/update guidance for
the user, never a silent fallback to a local process.

```ts
return await pi.bash({cmd: "bun run dev", durable: true, description: "Dev server"});
```

- `durable` implies background; `background: false` is rejected, and the call is
  bash-only. Approvals, extension hooks, cooperative
  [bash middleware](shell-middleware.md) (environment and output filters) and
  the explicit `timeout` still apply: middleware wraps the jev-fabric operations
  exactly as it wraps the local backend. Opaque shell overrides and managed hosts
  cannot use it.
- The command runs from a private `0600` script with the call's cwd and
  (spawn-hooked) environment, so argv limits and quoting never apply. The
  description becomes the jev-fabric job label.
- Output reaches the task through `jev-fabric follow`, a live stream of the
  job's retained events: no Node-side polling, and no model turn. jev-fabric
  retains bounded previews, so dropped bytes and aged-out events are written
  into the task output as disclosures, never silently joined.
- Completion notifies the owning session like any background task. `tasks.stop`
  (or `x` twice in the inspector) stops the job through `jev-fabric stop`,
  never by PID. Monitors work while the session is attached.
- **Session shutdown, reload or Pi exit detaches, never kills.** The job keeps
  running in its store. Fabric records the session binding in
  `<agentDir>/fabric/durable-tasks/` (one private file per task) and, on the owning session's next model
  request, `tasks.*` call or inspector open, reattaches the same task ID from
  the start of the job's retained events. A job that finished while Pi was away
  delivers its completion then. Another session never reattaches it
  automatically, and monitors are not restored.
- Reattached output passes through the currently active middleware. If a task
  was filtered at launch and no middleware is active now, its output is withheld.

### Jobs from other harnesses

The default store is `<cwd>/.jev-fabric-native`, the same one Claude Code,
Codex or a shell uses for `jev-fabric` in that project, so it doubles as a
process registry across harnesses. `tasks.external` lists jobs there that no
task of this session tracks (id, state, label, start time). `tasks.adopt({jobId,
description?})` attaches one as a durable task of this session: output,
completion notification, wait/watch/stop and an inspector row. Adoption never
restarts or signals the process; stopping it afterwards does.

```ts
const {jobs} = await tools.call({ref: "tasks.external", args: {}});
return await tools.call({ref: "tasks.adopt", args: {jobId: jobs[0].id}});
```

Boundaries are jev-fabric's: trusted native execution, not a sandbox; no restart
of the machine is survived; no exactly-once execution or rollback.

## Reading output and watching without a monitor

Any background task, including durable and adopted ones, can be read and filtered after launch, with jev-fabric's records:

```ts
const id = "returned-task-id";
const page = await tools.call({ref: "tasks.read", args: {id, offset: 0, waitMs: 5000}});
// {id, stream:"output", offset, bytes, omittedBytes, text, next, eof, state}
const seen = await tools.call({ref: "tasks.watch", args: {id, match: "listening on", after: 0, timeoutMs: 30000}});
// {reason, lines, omittedBytes, more, nextCursor}: nextCursor is a byte offset
return {page, seen};
```

- `tasks.read` offsets count every byte of combined output since launch and never reset. The live window is the 1 MiB tail; after exit 32 KiB stays readable. Older bytes are disclosed in `omittedBytes`. Text pages never split a UTF-8 character; `encoding:"base64"` is byte-exact. `waitMs` long-polls for new bytes or exit.
- `tasks.watch` with `match` scans complete lines after a byte cursor for a case-sensitive literal chosen at watch time, like `jev-fabric watch <id> <literal>`. The cursor stops at line boundaries, so an unfinished line is matched once it ends or the task exits. Up to 64 lines per call; call again while `more` is true.
- Neither consumes, acknowledges, wakes the agent, or stops the task. Without `match`, `tasks.watch` keeps reading a launch-time monitor as below.

## Opt-in monitors

```ts
return await pi.bash({
  cmd: "./scripts/watch-ci.sh", // emits a line only when something interesting changes
  description: "Watch CI",
  monitor: {
    delivery: "wake", // REQUIRED: "ui" or "wake"
    match: "CI:",     // optional case-sensitive literal substring, not regex
    timeoutMs: 300000,
    intervalMs: 5000,
  },
});
```

A monitor implies background execution; `background: false` is rejected. The
ordinary shell runner, extension hooks, middleware filters, approvals and explicit
shell timeout still apply. `delivery: "ui"` only updates task inspection, including
completion; it never wakes the model. `"wake"` additionally delivers coalesced line
events and completion to the owning agent. Main configures/starts/stops/renews the
watch, while the process does the polling or subscribes to an external source.
Unchanged polls require **zero LLM turns** when the script emits nothing.

Limits:

- Five-minute default lifetime; 1 second minimum, 30 minutes maximum. Expiration
  stops the command and produces one terminal outcome. Renewal is a new explicit
  call; there is no automatic restart or hidden supervisor LLM.
- At most eight concurrent monitors per session. Output batches are spaced by
  `intervalMs` (1–60 seconds, default 5 seconds). This is a **delivery** interval,
  not the script's polling interval.
- UTF-8 chunks are framed incrementally. Lines retain their first 2048 characters;
  matching applies to that bounded prefix. Empty lines and adjacent duplicate
  matching lines are suppressed. Events keep at most eight 500-character previews
  per batch; overflow is disclosed. A busy owner receives the newest batch per
  task, not every historical line. Retained raw output is available in the log.
- Main interruption cancels monitors and suppresses automatic wakeups. Ordinary
  detached commands retain their existing lifetime; their outcomes wait for input.
- Events and output are untrusted data. `wake` can incur model turns; use `ui` for
  human-only observation and emit only meaningful changes for agent-facing watches.

Opaque captured shell overrides do not gain Fabric monitors: Fabric must not bypass
an SSH backend or security gate. Compatible [bash middleware](shell-middleware.md)
keeps its filtering/protection. PowerShell monitors require the host's tracked shell
operations API. The tasks provider is installed with the native Pi provider in full
code mode (and schema enforce, whose authorization rules still apply), not in
orchestration-only mode or closed-world managed hosts.
