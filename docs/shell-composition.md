# Shell composition

Fabric's shell surfaces follow jev-fabric's
[composition contract](https://github.com/fabric-runtime/jev-fabric/blob/main/docs/composition.md):
two axes, one set of verbs, the same records. An agent that knows `jev-fabric
-- start` / `follow` / `read` already knows `pi.bash({durable:true})` /
`tasks.read` / `sessions.read`, and a command can move between Fabric and a
standalone jev-fabric without being rewritten.

## Lifetime × I/O mode

| Lifetime | Owner | Batch (no stdin) | Interactive (stdin open) |
| --- | --- | --- | --- |
| `foreground` | the calling `fabric_exec` | `pi.bash({cmd})` | unsupported |
| `session` | this Pi session, or the Jev program that opened it | `pi.bash({cmd, background:true})` | `sessions.open({argv \| cmd})` |
| `durable` | the jev-fabric store; outlives Pi | `pi.bash({cmd, durable:true})` | `sessions.open({…, durable:true})` |

- **Batch session** tasks keep Fabric's own runner: approvals, extension hooks,
  cooperative [bash middleware](shell-middleware.md), Windows support and
  [background task](background-tasks.md) delivery.
- **Durable** and **interactive** children are owned by jev-fabric (macOS and
  Linux). Durable batch tasks still run through `pi.bash` and its middleware;
  interactive children run outside `pi.bash`, so they are unavailable while an
  extension overrides `bash`. This preserves the extension's gate.
- A Jev program's interactive `session` children end whenever the program ends.
  `fabric_exec` children stay with the Pi session, like background tasks.

## One set of verbs

| Verb | Batch task (`tasks.*`) | Interactive child (`sessions.*`) | jev-fabric CLI / serve |
| --- | --- | --- | --- |
| state or receipt | `tasks.get` | `sessions.status` | `status` |
| wait (never stops) | `tasks.wait` | `sessions.wait` | `wait` |
| bytes by offset | `tasks.read` (combined `output`) | `sessions.read` (`stdout`/`stderr`) | `read` |
| literal line filter | `tasks.watch({match})` | read + filter in code | `watch <id> <literal>` |
| retained events | monitor replay (`tasks.watch`, no match) | `sessions.events` | `events` / `follow` |
| input | not applicable | `sessions.write`, `sessions.closeInput` | `write`, `closeInput` |
| stop by ID | `tasks.stop` | `sessions.stop` | `stop` |
| discovery | `tasks.list`, `tasks.external`, `tasks.adopt` | `sessions.list` | `list` |

Read records share one shape everywhere:
`{id, stream, offset, bytes, omittedBytes, text | data, next, eof, state}`.
Offsets count bytes since launch and never reset; pass `next` back as the
following `offset`. `waitMs` long-polls. Bytes older than the retained window
are disclosed in `omittedBytes`, never joined silently. Fabric's batch tasks
expose one combined `output` stream because Pi's shell runner merges stdout and
stderr; jev-fabric children keep them separate.

## A realtime loop

A Jev program holds one interactive child and one typed decision per tick. The
child is a trusted, task-specific bridge (a game, a harness `serve` process, a
REPL); code owns every action it sends.

```ts
const run = await jev.spawn({
  input: null,
  program: {
    name: "bridge-controller",
    inputSchema: {type: "null"}, outputSchema: {},
    requires: ["sessions.open", "sessions.write", "sessions.read", "jev.evaluate"],
    limits: {timeoutMs: 600000, maxEvaluations: 2000, maxToolCalls: 20000},
    code: `
      const bridge = await tools.call({ref: "sessions.open", args: {argv: ["python3", "bridge.py"], label: "game bridge"}});
      let offset = 0;
      const request = async (message) => {
        await tools.call({ref: "sessions.write", args: {id: bridge.id, text: JSON.stringify(message) + "\\n"}});
        for (let buffered = "";;) {
          const page = await tools.call({ref: "sessions.read", args: {id: bridge.id, offset, waitMs: 5000}});
          if (page.eof) throw new Error("bridge exited");
          offset = page.next; buffered += page.text;
          const end = buffered.indexOf("\\n");
          if (end >= 0) return JSON.parse(buffered.slice(0, end));
        }
      };
      for (let tick = 0; tick < 1000; tick++) {
        const state = await request({op: "observe"});
        if (state.done) return state;
        const decision = await jev.evaluate({state, questions: {action: {type: "choice",
          instructions: "Which legal action best advances the goal in this state?", criteria: state.actions}}});
        await request({op: "act", action: decision.answers.action.choice, revision: state.revision});
      }
      return null;
    `,
  },
});
return {id: run.id};
```

The session ends with the program; stopping the run stops the bridge. Use
`durable: true` only for children that must outlive Pi, accepting one worker
tick (about 25 ms) of input latency. The Jev loop rules still apply: batch
independent questions, label degraded decisions, revalidate revisions before
acting. See [Jev realtime loops](jev.md#realtime-loops).

## Which jev-fabric

Durable tasks and sessions resolve a jev-fabric binary at first use, never at
startup, and show the choice in `/fabric` settings (Executor → jev-fabric binary):

1. `executor.jevFabric.binary`, when set to a path or name. It is used or the
   call fails; Fabric never falls back from an explicit binary.
2. Your own install: `jev-fabric` on `PATH`, then `~/.local/bin`. A candidate
   inside the workspace (or on a relative `PATH` entry) is skipped: a repository
   must not supply the binary that runs durable commands.
3. The bundled `jev-fabric` npm package, copied once to
   `<agentDir>/fabric/jev-fabric/<version>/` so running workers keep a stable path.

Each candidate answers `jev-fabric -- capabilities`; a binary is accepted when
its protocol and features cover the request, so a newer install is used as-is.
jev-fabric 0.4 (no `capabilities`) serves durable tasks as protocol 1; sessions
need protocol 2. Every harness sharing a project defaults to the same store,
`<cwd>/.jev-fabric-native`, whose format version keeps mixed versions safe.

Windows keeps Fabric's own batch tasks; durable tasks and sessions are not
offered there yet. Jev program decisions stay in-process unless you opt into
`jev.transport: "auto"` or `"jev-fabric"` ([Jev](jev.md#schemas-and-limits)).
