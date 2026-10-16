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
On POSIX the router receives `PATH=/usr/bin:/bin`; on Windows it receives
`SystemRoot` (resolved case-insensitively from the host), a fixed
`PATH=<SystemRoot>\System32;<SystemRoot>`, and
`COMSPEC=<SystemRoot>\System32\cmd.exe`. Windows also explicitly copies the
process essentials that libuv would otherwise add to the child: `HOMEDRIVE`,
`HOMEPATH`, `LOGONSERVER`, `SYSTEMDRIVE`, `TEMP`, `USERDOMAIN`, `USERNAME`,
`USERPROFILE`, and `WINDIR`, when set (case-insensitive host lookup, canonical
uppercase child keys). Together with `PATH` and `SystemRoot`, these are libuv's
Windows required-variable allowlist; no other Windows variables, including
parent `PATHEXT`, are copied. A missing/relative Windows system root fails open
without starting a command. On either platform only `HOME`, `LANG`, and `TZ`
are otherwise copied when set; parent `PATH`/`Path`, `COMSPEC`, host credentials,
agent variables, and loader hooks are never inherited. Script
routers must name their interpreter explicitly, for example an absolute
`node.exe` followed by the `.mjs` script path on Windows; there is no shell repair.
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
  "policyVersion": "1.0.0"
}
```

`model` and `thinking` are required. `reason` (at most 2000 characters) and
`policyVersion` (at most 256) are optional strings. Stdout is bounded to 64 KiB;
stderr is not captured in decisions. Timeout, abort, and oversized output close
local pipes and retire the process tree (detached process group on POSIX;
`taskkill /T /F /PID` on Windows). Fallback joins the Windows helper (bounded to
1 second, with a direct-child SIGKILL fallback on helper failure) and waits up
to 1 second for direct-child exit, never for descendant-held `close`. These
retirement bounds are in addition to the command deadline; a failed native
tree kill cannot guarantee descendant exit.

## Decision ledger and rollback

Enabled modes append one decision JSON line to `<mesh>/router/decisions.jsonl`:

```json
{"ts":"2026-10-15T21:00:00.000Z","decisionId":"1465ef6c-0b75-4ddc-a685-44eb6d9c9a02","kind":"decision","spawnKind":"spawn","mode":"shadow","commandDigest":"e667b7cd7051b1cd581dc6eb32cf28e08eefe236fc6fbd7219f3fcf142a5b921","decision":"default","pick":{"model":"provider/suggested","thinking":"high"},"requested":{"model":"provider/model","thinking":"medium"},"actual":{"model":"provider/model","thinking":"medium"},"fallbackReason":null,"latencyMs":12.5,"error":null}
```

`requested` is the binding applied **to the request**, before model preparation
and thinking clamping. It is not proof of the native binding. `actual` is a
one-release compatibility alias with exactly the same value as `requested`;
remove it in a future release. A valid `pick` includes its canonical
model/thinking and optional sanitized reason/version codes.

For an authenticated admission, the admitting manager appends at most one
attributed start outcome. Only the start line's
`model` and `thinking` describe the final **applied binding**, after native model
preparation and clamping for a task, or the admitted binding for an idle actor:

```json
{"ts":"2026-10-15T21:00:00.100Z","kind":"start","decisionId":"1465ef6c-0b75-4ddc-a685-44eb6d9c9a02","agentId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sessionId":"8f3c78d2-ec8b-4567-b416-c13689740710","launch":{"runId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pid":12345,"processStartTime":"56801737"},"host":"ryzen1.smartypants.ai","model":"provider/model","thinking":"medium","verified":true,"evidenceRef":"residency/example/runs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/status.json"}
```

`verified: true` requires the admitting manager's own observation of its
running status record on disk at native registration. A queued handle is
not a verified start and does not write a start until its worker is launched
and registered. Actor birth is idle with no native run: its outcome has
`verified: false`, null launch metadata/evidence, and its admitted binding.
It does not attest to a later activation's binding. `launch` contains only
known manager metadata, with null for each unknown member; a transport PID is
never used as a Pi `sessionId`.

If record creation fails, append `kind: "start-failed"` instead, with the same
start shape: preserve `ts`, `decisionId`, and `host`; set unknown `agentId`,
`sessionId`, `model`, and `thinking` to null, all three `launch` members to null,
`verified` to false, and `evidenceRef` to null. Add only the fixed
`error: "start-failed"`, never exception text.

If the native transport launch outcome is unknown after launch custody may have
been created, append `kind: "start-unknown"` with that same null receipt shape
and only the fixed `error: "start-unknown"`. It is neither success attribution
nor evidence that a worker started; unresolved cleanup and admission custody
remain in force until the existing proof path confirms the worker is gone. The
same admission is consumed once, so a replay is refused rather than recorded as
another outcome.

For durable calls, local validation and host-startup failures are recorded by
the provider. Receipt custody transfers only at the native client's existing
request-publication fence, via an optional host-only callback (never a wire
field). From that point the receiver owns the sole outcome; a rejected or
uncertain response must not append another failure. The fence precedes the
atomic write because a publication error can itself be ambiguous.

| name | type | meaning | when null |
| --- | --- | --- | --- |
| `ts` | string | Timestamp of this ledger line. | Never. |
| `decisionId` | string or null | UUID joining the decision to its start outcome. | Null on a refusal of a malformed ID. |
| `kind` | `"decision"`, `"start"`, `"start-failed"`, `"start-unknown"`, or `"start-refused"` | Ledger line type; not the spawn type. | Never. |
| `spawnKind` | `"spawn"` or `"actor"` | Public admission type; decision only. | Never. |
| `mode` | `"shadow"` or `"enforce"` | Enabled router mode; decision only. | Never. |
| `commandDigest` | string | SHA-256 of `JSON.stringify(command)`, never raw argv; decision only. | Never. |
| `decision` | `"explicit"`, `"default"`, or `"enforce"` | Selection outcome; decision only. | Never. |
| `pick` | object or null | Validated canonical model/thinking with optional sanitized reason/version; decision only. | No valid suggestion. |
| `requested` | object | Binding applied to the request before preparation/clamping; decision only. | Never. |
| `requested.model` | string or null | Request model selection, not final native binding. | Model not statically known. |
| `requested.thinking` | string | Request thinking level before bounds are applied. | Never. |
| `actual` | object | One-release alias equal to `requested`; decision only, removed in a future release. | Never. |
| `fallbackReason` | string or null | Fixed router error when an error keeps the default; decision only. | No error-caused default fallback. |
| `latencyMs` | number | Router decision latency in milliseconds; decision only. | Never. |
| `error` | string or null | Fixed router code on a decision; `"start-failed"` on creation failure; `"start-unknown"` when native launch custody is unresolved; one of the admission refusal codes below on `start-refused`; absent on a start. | Decision without an error. |
| `agentId` | string or null | Admitted agent or actor id; start outcomes only. | Unknown on failed creation. |
| `sessionId` | string or null | Native Pi session header id, never transport PID or parent session. | No native session known at this boundary. |
| `launch` | object | Manager launch metadata; start outcomes only. | Never; unknown members are null. |
| `launch.runId` | string or null | Known manager run id. | No native run or unknown; null on start-failed/start-unknown. |
| `launch.pid` | number or null | Manager's captured local process PID. | No local process or unknown; null on start-failed/start-unknown. |
| `launch.processStartTime` | string or null | Manager's captured process birth identity. | Unavailable; null on start-failed/start-unknown. |
| `host` | string | `os.hostname()` on the admitting host. | Never. |
| `model` | string or null | Final prepared task model or admitted actor model, not the request alias. | Binding unknown; null on start-failed/start-unknown. |
| `thinking` | string or null | Final clamped task thinking or admitted actor thinking. | Binding unknown; null on start-failed/start-unknown. |
| `verified` | boolean | Own observation of a written running status record at registration. | Never; false for idle actor birth, start-failed, and start-unknown. |
| `evidenceRef` | string or null | Written status path relative to mesh (may use `..` for a local run root outside mesh). | No observed status record; null on start-failed/start-unknown. |

## Authenticated, single-use attribution

The decision UUID alone is not authority. At issuance, the router persists an
admission in the **existing MeshStore**, under the host-reserved
`decisions/spawn-router/admissions/<UUID>` namespace. Public `mesh.put` and
`mesh.delete` cannot mint or reset these keys. Admissions expire after 10 minutes;
issuance prunes expired entries under the same transaction and refuses new
admission storage at 4096 retained entries. This is an attribution bound, not a
launch limit. Consumed entries remain until expiry so replays have a fixed code.

The SHA-256 binding covers the requester session, target root and native host,
spawning participant role, spawn/actor kind, canonical cwd, requested model and
thinking, runner, name, and task/instruction digest. Relative task cwd is resolved
from the original manager **base cwd**, not from the already-selected router cwd.
For a durable request with omitted `cwd`, the cwd binding is null: the native
resident supplies its own configured cwd (normally the repository root), which
need not match the caller's base directory. This omission remains distinct from
any explicit cwd. Local omitted cwd still binds the caller's canonical base;
explicit task cwd still binds its canonical resolved location, including for
durable tasks. This changes attribution only, never launch cwd selection.
File-backed actor instructions bind their verified SHA-256 without disclosing the
file path or reading the file at the origin. Seeded tasks bind the submitted task
bytes after seeding. Admission state retains only the binding digest and bounded
issuance metadata, never task text or raw requester/target details.

The provider snapshots host-owned caller identity once before routing awaits and
passes it to local managers separately from request fields. Resident task callers
use the existing caller-bound spawn envelope and native directory authentication.
Actor proxies carry a separate, host-captured `routerCaller` envelope solely for
attribution: the receiver checks its ID, session, root, kind and owner against the
fresh native participant directory, then constructs authority from that directory
entry and its own target root/host. It grants no actor creation, mutation or
supervisor authority and adds no launch-policy check.

All attributed start-outcome writers atomically compare and consume
`issued → consumed` through `MeshStore.writeBatch` and the entry version CAS.
Only one concurrent writer can attribute an outcome. A missing, expired, damaged
or unavailable admission gives `decision-unknown`; an already-consumed admission
gives `decision-replayed`; a binding or caller-authority mismatch gives
`decision-mismatch`. Refusals append `kind: "start-refused"`, the fixed error,
and null agent/session/model/thinking/launch/evidence fields with
`verified: false`. They never append an attributed `start`, `start-failed`, or `start-unknown`.
**Actual launch remains fail-open; attribution is fail-closed.** A `start-unknown`
line is a privacy-safe custody meter, not an exactly-once launch journal. A request without
a decision ID keeps the original path, with no router import or state/log write.
A crash after consume but before ledger append can lose an outcome, never mint a
second successful attribution; this meter is not an exactly-once launch journal.

### Host-only integration API

`SpawnRouterAdmission` contains `{ mesh, requesterSessionId, role, targetRootId,
targetHostId }`. These are native caller/target snapshots, never action arguments
or task/actor request fields. Direct integrations and fixtures must pass:

- To `routeAgentCreation`: `admission`, `admissionRequest` (the actual prepared
  request before any router override), and `admissionCwd` (manager base cwd).
- To `recordSpawnRouterStart` or `recordSpawnRouterStartFailed`: the same native
  `admission`, `spawnKind`, final submitted `request` (including the selected
  override), and base `cwd`, in addition to the existing receipt fields.
- To `AgentManager.spawn`: optional **ninth native argument** `routerAdmission`.
  The eighth argument remains the authenticated task return address.
- To `ActorManager.create`: native options field `routerAdmission`, propagated by
  `ActorDirectory`. Neither manager reads admission identity from the request.
- Nested actor proxies accept host-only `routerCaller` as the fifth argument of
  `ResidentActorClient.createActor`; the receiving host independently validates it.

`routeProviderCreation` keeps provider routing preparation lazy;
`authenticatedSpawnRouterAdmission` constructs receiver authority only after
fresh directory validation. Missing authority never falls back to request fields.

Join a decision and its start outcome **only by `decisionId`**; do not join on
task text, model, timestamps, command digest, or launch identifiers. Ledger
failures never fail or alter creation. A route result carries the same
`decisionId` alongside the request binding; only the start outcome records
the applied binding. Only these structured grammars are preserved unchanged
(case-sensitive, full-string matches):

- `reason`: `^(policy:task/[a-z0-9-]{1,32}|capacity-hold|taskclass:[a-z0-9-]{1,32}|default|unavailable|normal\.implementation)$`.
  All other string reasons become `other`, including task-text fragments with
  spaces or more than 32 characters after `policy:task/` or `taskclass:`.
- `policyVersion`: `^v[0-9]+(-[a-z0-9.]{1,32})?$`,
  `^[0-9]+\.[0-9]+(\.[0-9]+)?$`, or `^[0-9a-f]{7,40}$`.
  For example, `v1-2026.10.09`, `1.2.3`, and a lowercase hexadecimal SHA pass
  through; all other string versions become `unknown`.

No whitespace (including trailing line terminators) or truncation is accepted.
Rejected raw values, raw tasks, and router stderr never enter the ledger.

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
