# External spawn router (smarty-dev#2890)

Fabric supplies an optional adapter for model-economics-lead's `bin/smarty-route`.
The policy/model economics engine is **not** implemented here.

Configure only in the selected host agent directory's `fabric.json` (workspace
configuration cannot execute a router or opt into task disclosure):

```json
{
  "agents": {
    "router": {
      "command": ["/absolute/path/to/bin/smarty-route"],
      "timeoutMs": 1500,
      "mode": "shadow",
      "includeTask": false
    }
  }
}
```

`command` is executable + argv, never shell source. The executable must be an
absolute path: bare names and relative paths are rejected, with no PATH lookup.
The router receives only `PATH=/usr/bin:/bin` and, when set, `HOME`, `LANG`, and
`TZ`; host credentials, agent variables, and loader hooks are not inherited.
`timeoutMs` defaults to 1500 and clamps to 200–5000 ms. `mode` defaults to `off` (also the kill switch).
`shadow` records a validated suggestion but keeps the existing static/default
binding; `enforce` uses the validated suggestion. A missing/failed command,
nonzero exit, timeout, oversized output, malformed JSON, unknown/denied model,
or invalid thinking level keeps the static default and records an error.
An explicit caller model **or thinking** always wins: no router process starts,
and enabled modes record `decision: "explicit"`. Existing explicit `model: "auto"`
routing is unchanged; this adapter does not run on `agents.run`, handoff, actor
activations, or global actor templates.

The hook runs once at public `agents.spawn` or live `agents.create`/`createActor`
admission, before session/durable forwarding. Models must be exact visible Pi
`provider/id`, exact model IDs, or configured aliases with visible targets;
there is no fuzzy matching or registry refresh in router validation. Non-Pi
configured backend default keys are also recognized. Fleet denied-model policy
still applies. Thinking must be `off`, `minimal`, `low`, `medium`, `high`,
`xhigh`, or `max`.

## Command contract for model-economics-lead

Read one JSON object from stdin (newline-terminated) and write one JSON object
to stdout, then exit 0 within the deadline. Example input:

```json
{
  "kind": "spawn",
  "role": "task-agent",
  "name": "implementation",
  "cwd": "/workspace/project",
  "project": "/workspace/project",
  "taskDigest": "<lowercase SHA-256 of UTF-8 task bytes>",
  "taskLength": 1842,
  "parentId": "session:<id>",
  "requestedComplexity": "normal",
  "host": "ryzen5.smartypants.ai",
  "defaults": { "model": "provider/model", "thinking": "medium" }
}
```

- `kind` is `spawn` or `actor`; actor instructions are the task input.
- `role` is the spawning participant's recorded role, else `task-agent`/`actor`.
- `name` is null if omitted; `project` is the participant's project root/project,
  else the runtime cwd. `parentId` is the immediate spawning participant, not
  necessarily Main.
- `taskLength` counts UTF-8 **bytes**. File-backed actor instructions remain
  owner-only: their supplied SHA-256 is forwarded and length is null; the hook
  does not read or forward the file path/text.
- `requestedComplexity` is present only if the spawn caller provides the
  optional `complexity: "simple" | "normal" | "complex" | "delicate"`; never
  guessed from text. All four values pass through verbatim. `delicate` is Paul's
  delicate-code class (the only class for Opus in the external router's policy),
  not an alias for `complex`; Fabric does not implement that model policy.
- The full `task` string is absent unless host `includeTask: true` explicitly
  allows it, and remains absent for an owner-only instruction file.
- `defaults.model` can be null when the backend default is not statically known.

Example output:

```json
{
  "model": "provider/model",
  "thinking": "high",
  "reason": "normal.implementation",
  "policyVersion": "2026-10-v1"
}
```

`model` and `thinking` are required. `reason` (at most 2000 characters) and
`policyVersion` (at most 256) are optional strings. Stdout is bounded to 64 KiB;
stderr is not captured in decisions. Timeout settles immediately without waiting
for `close`, closes local pipes, and kills the process tree (detached process
group on POSIX; `taskkill /T /F /PID` on Windows).

## Decision ledger and rollback

Enabled modes append one JSON line to `<mesh>/router/decisions.jsonl`:
`ts`, `requestDigest` (SHA-256 of request metadata excluding raw task), `kind`,
`mode`, `decision` (`explicit`/`default`/`enforce`), `pick`, `actual`, `latencyMs`,
and `error` (fixed adapter code or null). A valid pick includes its canonical
model/thinking and optional reason/version codes. Reason and version metadata
are persisted only as lowercase codes matching `[a-z0-9_.:-]+`, truncated to
64 characters; other values (including echoes of the supplied task) are replaced
with `redacted`. Free-form router reasons, raw tasks, and router stderr never
enter the ledger.

The router directory is created with mode 0700 and must be a real directory
(`lstat`, not a symlink), owned by the current uid with private permissions
where the OS exposes uid/mode checks. The ledger is opened append-only with
`O_NOFOLLOW` (where supported) and mode 0600; links and non-regular files are
refused. Before an append would exceed 8 MiB, the ledger rotates to
`decisions.jsonl.1`, replacing the single previous archive.

Logging failures warn to stderr without failing or changing spawn selection.
A read-only or unsafe mesh cannot guarantee a persisted decision; repair
permissions before relying on shadow evidence.

Rollback: set host `agents.router.mode` to `off` and reload Fabric (or remove
`agents.router`). No command, router module load, or decision write occurs in
off mode. Existing static/inherited defaults resume unchanged.
