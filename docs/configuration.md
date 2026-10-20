# Configuration

Pi Fabric reads ordinary settings from two JSON files. Project values override global values, subject to the authority restrictions below.

1. `~/.pi/agent/fabric.json`: global defaults.
2. `<project>/.pi/fabric.json`: project overrides, only for **trusted** projects.

`/fabric settings` opens at project scope in trusted projects and at global scope in untrusted sessions. In a trusted project, press **Ctrl+G** anywhere in the settings view to move both the displayed values and the save destination between `<project>/.pi/fabric.json` and the global `~/.pi/agent/fabric.json`. The global view shows global defaults even when a project override stays effective in the current session, and the scope banner marks that precedence. Both views show persisted values. The affected setting notes when a runtime-only environment override still controls the live session. Untrusted sessions remain global-only. RPC hosts expose the same nested settings through standard select/input dialogs and provide a root save-scope action, so no terminal keybinding is required.

`configVersion` versions each configuration document. Fabric migrates each applicable file independently before it applies global/project precedence, then rewrites migrated files atomically. Version 0, the historical unversioned format, renames `subagents` to `agents`. Versions 2 and 3 rename legacy UI settings. Version 4 repairs `prewalk.enabled` string booleans emitted by the settings UI in affected builds. When both legacy and canonical sections exist, canonical values win conflicts and non-conflicting values survive. Fabric migrates trusted project files, and it never reads or rewrites untrusted project files. Add future schema changes as sequential migrations. Avoid runtime aliases.

## Root-owned host policy

Authority-relaxing host settings belong in **`/etc/smarty/fabric-policy.json`**, not the agent-writable global `fabric.json` (smarty-dev#7591). The production path is fixed at build time: no environment variable, agent directory, project setting or `fabric.json` key can redirect it. Tests substitute private temporary paths through a compile-time constant only.

Fabric honours the file only when `lstat` identifies a regular file owned by uid **0**, with no group/world write bits. Every directory from its parent through the filesystem root must likewise be root-owned, non-group/world-writable and a real directory, not a symlink. The reader opens with `O_NOFOLLOW`, verifies ownership/mode and matching device/inode with `fstat`, reads exactly that descriptor's initial size, then repeats `fstat`. A full read and identical size, device/inode, mtime/ctime, ownership and mode are required before parsing JSON. Symlinks, unsafe ownership/modes/ancestry, open races, short or metadata-changing reads, unreadable files and malformed/non-object JSON or invalid consumed authority-field schemas fail strict with one warning per process naming the path and failure reason. Recognized objects must be objects, Landlock mode must be `off`/`enforce`, kill-switch/escape values must be booleans, model lists must contain strings, and processSlice must be a slice name. **Publish policy updates by atomic rename** of a complete root-owned file in the trusted directory, never by rewriting the active inode in place. Fabric never creates, migrates or rewrites this root file.

Landlock `enforce` applies to the Fabric Bash route only. Actor/task launchers, process transports and trusted executor runtimes are not Landlock-enforced; non-Bash enforcement is tracked in [smarty-dev#7935](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/7935). Reserved `PI_FABRIC_LANDLOCK_*` environment controls are scrubbed at unrelated child-launch boundaries; scrubbing is not confinement.

Only these keys are consumed from root policy; other settings remain in the ordinary configuration files:

```json
{
  "executor": { "landlock": { "mode": "enforce", "disabled": false, "allowEscape": false } },
  "agents": {
    "processSlice": "batch.slice",
    "processMemoryMaxBytes": 25769803776,
    "deniedModels": ["provider/denied-model"],
    "modelPolicy": { "requireReason": ["gpt-6-astra", "opus", "claude"] }
  }
}
```

- `executor.landlock.disabled`: the fleet confinement kill switch. Agent-dir and project values are always ignored; only a valid root file can disable confinement. A provisioned host warns once per process when an agent-dir `true` is ignored; a missing or invalid root policy has its own once-per-process warning. The live reader revalidates root policy on every enforced Bash call, so a root flip reaches already-running lanes; an unsafe or missing root file revokes any previously loaded disable grant.
- `executor.landlock.mode`: an absent file (`ENOENT` before observing it) uses the built-in product default (`off`, with `disabled: false`), overlaid by a valid root policy. Present but unprovable policy (unreadable, unstable, invalid JSON/schema, or unsafe owner/mode/ancestry) selects strict `enforce`, `disabled: false`, and no escape grant. An observed file disappearing before open is invalid, not missing. Agent-dir and project `off` or unknown/permissive values are ignored, while `enforce` remains a tightening and applies. Only root policy may override the baseline with `off`; it cannot undo a session's agent/project tightening.
- `executor.landlock.allowEscape`: only literal `true` in a valid root policy grants a leading `PI_FABRIC_LANDLOCK_ESCAPE=1` assignment or an explicit/inherited child environment value of `1` an unconfined per-command escape at the Bash Landlock launch boundary. Absent/false values deny it; invalid field types invalidate the policy; agent-dir and project values are always ignored. Without the grant repeated leading prefixes are stripped, the reserved variable is removed from child environment, the command stays confined under `enforce`, and one warning is logged per process. The flag is consumed even for granted commands, never implicitly forwarded to descendants. Granted escapes still require a pre-spawn journal entry and emit a visible notice. Live root revalidation revokes previously loaded grants when removed or when the file becomes missing/unsafe/unstable; no reload is needed.
- `agents.processSlice`: only root policy may select/change the process-worker slice. Agent-dir and project values cannot override it.
- `agents.processMemoryMaxBytes`: only root policy may change the per-task memory cap (default 24 GiB, `25769803776`). Agent-dir and project values are ignored, so they can neither relax nor tighten it. In the root policy file, a value that is not a positive safe integer fails policy validation, like an invalid `processSlice`. A cap too small to admit the worker fails launch; it never falls back to an uncapped task.
- `agents.deniedModels`: root policy provides the baseline deny list. The agent-dir list is **unioned** with it; clearing the agent-dir list cannot remove a root deny. Root may remove its own denies, but any remaining agent-dir deny still tightens the result.
- `agents.modelPolicy.requireReason`: defaults to `["gpt-6-astra", "opus", "claude"]`. Root policy may replace/relax this baseline (including `[]`); the agent-dir list is **unioned** with the root/default baseline and can only add requirements. Workspace lists remain ignored. The root file consumes this rollback list only; `fallbackModel` and `fallbackThinking` remain profile/agent-dir settings.
- `agents.modelPolicy.fallbackModel` is an optional host/profile selector for indirect model resolution; it has no package default. Indirect resolution prefers a configured non-protected `agents.model`, then this host/profile fallback. `agents.modelPolicy.fallbackThinking` defaults to `"high"`. If the configured fallback is absent or unavailable, resolution continues through the inherited model when permitted and otherwise refuses with `agents.modelPolicy.fallbackModel` named. Fleet profile installation uses `~/.local/share/smarty-dev/pi-agent/fabric.json` with `fallbackModel: "cliproxyapi/gpt-6-luna"` and `fallbackThinking: "high"`; a non-fleet profile may not have that provider installed, so provisioning must verify the selector on each target host.
- Claude/Opus exceptions require host-owned admission from the fixed `/etc/smarty/model-exceptions.json`; no `fabric.json`, environment, agent directory, workspace or user config can redirect it. The v1 document is `{ "version": 1, "grants": [{ "id", "issue", "reason_url", "role", "lane", "host", "model", "thinking", "granted_by", "granted_at", "expires_at", "status" }] }` (field names shown; see the [complete JSON example and operator request flow](agents.md#explicit-model-exceptions-3134)). Every field is required. `role` is `task`, `actor:<name>` or `lane`; `status` is `active` or `revoked`. ONE active, unexpired grant must match the canonical issue in `modelReason`, ROOT-bound worktree lane, short host, exact role and exact resolved canonical model, with actual effective thinking at or below the ceiling. No prefixes or alternate provenance models match. The lane comes only from a host callback reading the bound ROOT participant's cwd basename; missing binding is `lane-mismatch`, never a manager/request cwd fallback. Bare `#N` means `Smarty-Pants-Inc/smarty-dev#N`; `repo#N`, full `Smarty-Pants-Inc/repo#N` and that organization's GitHub issue URLs normalize to the same issue. Provision only through `smarty-rootops`: root-owned regular file, at most 1 MiB, normally mode `0644`, no group/world write, and every parent through `/` root-owned and not group/world writable. The reader uses `O_NOFOLLOW`, fd `fstat`, bounded/stable reads. Missing files grant nothing (`file-unavailable`); unsafe metadata/symlinks or unsupported ownership platforms give `untrusted-source`. Duplicate ids, any malformed entry, unknown version/status, invalid ISO date-time or non-increasing timestamps invalidate the whole file (`malformed`). TTL over 72 hours gives `expiry-too-long`; expiration gives `expired` without any sweep, and revocation gives `revoked`. All refusals use `FABRIC_MODEL_EXCEPTION_DENIED` before worker launch. Leads/Main are unaffected. Each admitted launch appends one immutable-grant audit record with grant id, issue, lane, short host, role, model, effective thinking, run id and time; inherited-switch audit compatibility remains. The band file is only an untrusted meter, never authority.

**Fail-safe behavior:** missing policy preserves the built-in `off` baseline and grants no authority relaxations. Any other unprovable policy fails strict: Landlock `enforce`, `disabled: false`, no `allowEscape`, and no root authority relaxations. Its once-per-process warning names `/etc/smarty/fabric-policy.json` and the failure reason. Agent deny/reason additions and other tightening settings still apply. Already-loaded enforcement remains enforced; live rereads of invalid/unreadable/unstable policy also tighten an already-loaded `off` baseline and revoke kill-switch/escape grants. A later missing policy uses defaults again, without weakening session tightening.

**Owner gate:** hosts-lead provisions `/etc/smarty/fabric-policy.json` with each host's **current intended relaxations before install** (for example uid/gid 0, directory mode `0755`, file mode `0644` or `0600`), publishing updates by atomic rename. Per-command escapes are denied by default: set `executor.landlock.allowEscape: true` only with an explicit owner-approved escape grant; an existing escape prefix is not authorization. Fabric-v2 verifies that editing the agent-dir file no longer relaxes authority after install. Revert means restoring the **previous Fabric pin**, not making the root policy writable.

## Process task placement

`agents.placement` is **host-only**: put it in the selected `PI_CODING_AGENT_DIR/fabric.json`, never a workspace override. New process spawns check that file for changes and re-read placement when its filesystem identity/timestamps change; no `/reload` is needed. Running tasks keep their original launcher, receipt and cancellation policy. Removing placement (or the file) restores local spawning. Invalid live edits retain the last valid placement with a warning and are not rewritten; other agent settings still require config reload. Managed hosts keep their sealed policy. Absent means unchanged local spawns. When configured, `default` is `"local"` unless explicitly `"remote"`. A remote Main task launches through `command`, not the local worker; `needs?: string[]` on spawn/run routes unmet needs locally. `capabilities` defaults to `[]` and must describe guarantees on every target selected by the launcher. Needs and capabilities share canonical tokens: NFKC-normalize, lowercase, split comma/Unicode White_Space compounds, validate [a-z0-9._-], and deduplicate. Any canonical `local` token always pins execution to Main and is refused in `capabilities` with `AgentInputError` / `FABRIC_AGENT_INPUT_ERROR`. Optional spawn/run `requires` preflights absolute input paths on the selected host; remote commands receive repeated `--require PATH` arguments (the fleet launcher's current repeatable preflight flag, not legacy `--input`) before `--`. See [input preflight](agents.md#opt-in-process-task-placement) for validation bounds and local refusals.

Ryzen 1 example (the existing fleet launcher, **not executed by the offline tests**):

```json
{
  "agents": {
    "placement": {
      "default": "remote",
      "command": [
        "/home/paul/.local/bin/smarty-task-ryzen2",
        "{id}",
        "--host",
        "auto",
        "--minutes",
        "{minutes}",
        "--src",
        "{cwd}",
        "--model",
        "{model}",
        "--thinking",
        "{thinking}",
        "--",
        "{task}"
      ],
      "capabilities": [],
      "sshAliases": {
        "ryzen2": "ryzen2-agent",
        "ryzen3": "forge-agent",
        "ryzen4": "ryzen4-agent",
        "ryzen5": "ryzen5-agent"
      },
      "resultCommand": [
        "/usr/bin/ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "{sshAlias}",
        "python3 -c 'import json, pathlib, sys; p=pathlib.Path(\"/srv/scratch/paul/tasks/direct\")/sys.argv[1]; rc=p/\"rc\"; print(json.dumps({\"rc\":rc.read_text().strip(),\"text\":(p/\"result.md\").read_text(),\"stderr\":(p/\"stderr.log\").read_text() if (p/\"stderr.log\").exists() else \"\"} if rc.exists() else {\"rc\":None}))' {id}"
      ],
      "cancelCommand": [
        "/home/paul/.local/bin/smarty-task-ryzen2",
        "--host",
        "{host}",
        "--cancel",
        "{id}"
      ],
      "pollIntervalMs": 1000,
      "commandTimeoutMs": 30000
    }
  }
}
```

The Ryzen 1 Main's cwd (for example `/home/paul/smarty/smarty-pants/pi-fabric`) is a **source** workspace, not a target-local lane: `--src {cwd}` makes the launcher ship tracked and unignored files to the selected task's `OUT/src` and run there. Do not substitute `--cwd`, which the launcher permits only for existing target-local lanes under `/home/paul/lanes`, `/home/paul/repos`, or `/srv/scratch/paul/tasks`. With `--src {cwd}`, home/root, non-Git or ignored source directories stay local with audit reason `cwd-not-shippable`. A failed, timed-out or indeterminate Git ignore check also stays local. Fabric requires the launcher's tracked/unignored manifest branch; it never knowingly selects the recursive-copy fallback for ignored (potentially huge or private) source roots.

`sshAliases` copies smarty-dev's `setup/factory/work-hosts.json` mapping: `ryzen2 → ryzen2-agent`, **`ryzen3 → forge-agent`**, `ryzen4 → ryzen4-agent`, `ryzen5 → ryzen5-agent`. Keep this host-owned map synchronized with that file; Fabric does not read smarty-dev files or invent `{host}-agent`. `{sshAlias}` is looked up using the accepted host, and requires a nonempty map of safe host/alias tokens. An unmapped accepted host retains launch custody; it does not guess an alias or start a local duplicate. This example uses normal SSH host-key verification, with only read-only result polling added. Verify the aliases and shippable source workspace before enabling it. For a target without `python3`, use an equivalent host-owned reader that implements the JSON contract below. The hook itself does not call SSH implicitly or retrieve a remote file as if it were local. The launcher selects the host; its `RYZEN2_TASK_ACCEPTED <id> on <host> ...` line must match the Fabric run ID. `{host}` in polling/cancellation comes from that receipt, not from `auto`.

All commands are **argv arrays**, passed to `execFile` without a shell. Template substitution occurs once; task quotes/newlines/metacharacters remain a single argument. Available placeholders are `{id}`, `{cwd}`, `{task}`, `{minutes}` (rounded up from the run timeout), `{model}`, `{thinking}`; after acceptance, `{host}`, `{sshAlias}` (when mapped), and `{resultDir}` (when configured) are available. `{host}`/`{sshAlias}`/`{resultDir}` cannot be used on the launch command. Model and effort must be included to preserve the caller's selection. The adapter exports the parent Fabric session as `PI_SESSION_ID` for the launcher's existing completion/inbox binding.

Configure exactly one result source:

- `resultDirectory`: an **absolute** local/shared directory template, such as `/srv/shared/tasks/{id}`. Poll `rc` first, then `result.md` and optional `stderr.log`. The fleet's default result directory is target-local, **not** automatically shared with Ryzen 1.
- `resultCommand`: a bounded read-only argv template whose stdout is only JSON: `{"rc":null}` while pending, or `{"rc":0,"text":"full final result","stderr":"optional"}` after native execution exits. Integer/string exit receipts are supported. Nonzero rc maps to `failed`; 124 or a timeout string maps to `timed_out`. The reader must never emit terminal rc before the remote task/process tree has stopped. Poll failures are retried only until the task deadline; they do not relaunch the task.

`cancelCommand` is required and requests cancellation; a successful command alone does not prove exit. The adapter waits up to `min(commandTimeoutMs, 5000)` for the final receipt. Missing receipt retains exit debt and files/admission, and reports the stop/timeout with a warning; it does not invent successful cleanup. Launch errors or mismatched acceptance retain conservative custody (except a missing executable, which is known unlaunched). Remote launches are never automatically retried or replaced by a local spawn.

At extension `session_start`, configured placement gets a filesystem-only startup probe: the first `command` element must resolve to an existing executable regular file (an absolute path, a cwd-relative path, or a name on `PATH`; no dynamic executable placeholders). One clear `[pi-fabric] agents.placement startup probe:` line reports readiness or local fallback. Probe failure preserves the configured policy but keeps eligible tasks local, appending `placement.local` with `placement-probe-failed: command missing or not executable: ...`. Unconfigured placement does not probe or log. Config reload rechecks readiness without repeating an unchanged diagnostic. No launcher is invoked: the fleet launcher has `--dry-run`, not `--probe`, and even a dry run can select hosts/check inputs over SSH. A startup success does not attest remote availability, cwd shipment, or command arguments; post-invocation failures still retain conservative custody.

`pollIntervalMs` defaults to 1000 (range 10–60000). `commandTimeoutMs` defaults to 30000 (range 100–120000). No polling starts during registration or idle lifecycle hooks; the adapter is a stable lazy entry loaded at actual first remote use. The manager owns polling/cancellation, with no extra detached watcher in Fabric. The fleet launcher still has its own existing inbox watcher; this does not replace Fabric's wait result.

One-shot placement supports ordinary Main Pi tasks (up to 240 minutes, launcher-supported effort low/medium/high/xhigh/max). Actor/nested/durable/inherited/routed runs, non-Pi or Python kernels, recursive/worktree requests, custom tools/schema/images/system prompt, resolved account pins (request-supplied or inherited from the parent), effectively disabled extensions (including the host default), per-run niceness, and active token/cost ceilings remain local so they keep their required semantics. Those decisions append `placement.local` with their reason to the run event log. Remote runs do not preserve streaming, controls, telemetry/usage/cost, local transcript export/recovery, host instruction/permission/tool profile parity, native session identity or return-address/mesh bridge features. See [agents](agents.md#opt-in-process-task-placement).

### Native fleet proof and rollout gate

`scripts/probe-process-placement.mjs` is an **offline adapter proof** with a fake launcher and fake receipt; it is not evidence of a real Main-to-work-host model call. The separate opt-in `scripts/probe-native-process-placement.mjs` loads the installed Pi SDK and the freshly built extension into an isolated agent directory, invokes `agents.spawn` → `agents.wait` through the **real** fleet launcher, and retains its acceptance line, native terminal rc, full result, manager audit, exact candidate SHA and command. Its caller makes no inference calls and reads no host authentication files; only the real launcher uses the work host's existing native Pi profile. SSH proof is restricted to the approved Main hostnames `dev1.smartypants.ai` (also `dev1`) and `ryzen1` (also `ryzen1.smartypants.ai`); work hosts are rejected, and neither Main name may appear as a work-host key or SSH alias route. It ships a tiny public Git source packet, not the repository/private corpus or credential-bearing inputs.

From a **Ryzen 1 Main owner**, after a fresh build on the clean candidate:

```sh
PROOF_MODELS_FROM_PROFILE=1 nice -n 19 node scripts/probe-native-process-placement.mjs \
  /absolute/installed/pi-coding-agent/package \
  /absolute/kept/evidence-directory \
  /home/paul/.local/bin/smarty-task-ryzen2 ryzen2 \
  /home/paul/.local/share/smarty-dev/factory/current/setup/factory/work-hosts.json ssh
```

`PROOF_MODELS_FROM_PROFILE=1` opts into the selected host profile's model providers: before replacing `PI_CODING_AGENT_DIR`, the probe resolves that directory (default `~/.pi/agent`) and symlinks its `models.json` into the private isolated profile. The installed SDK consumes the symlink with network/model refresh disabled and in-memory authentication; the probe never copies or prints the models file, passes it on argv, or includes it or its contents in the evidence directory. No authentication store is linked. The scratch profile and symlink are removed on completion without modifying the host file. A missing models file fails closed; private failure diagnostics are omitted from evidence/stdout/stderr when this option is enabled. Leave the option unset for the inert offline provider metadata instead. Only the real work task makes an inference call through the target profile.

Use `local` in place of `ssh` only on the selected work host **if its real launcher contract permits a local run**. Work hosts currently install a deliberate refusal stub: `smarty-task-ryzen2` is a Ryzen 1 Main tool and lanes spawn locally. Do not bypass that stub with a retired/reference launcher, install peer SSH credentials, or treat its exit 2 as a successful proof. Retain the exact-head failed attempt and launcher contract as blocker evidence; the reference launcher's own header likewise excludes work-host lanes. A filesystem startup probe can correctly pass for the stub without attesting task admission.

When that deployment boundary prevents a pre-merge native receipt, the repository owner (the Main handling the PR) owns an explicit exact-head evidence gate on Ryzen 1 after push/install, **before placement is enabled fleet-wide**. Record: candidate SHA and clean build; command and caller hostname; installed SDK and extension paths; one `RYZEN2_TASK_ACCEPTED <id> on ryzen2`; `placement.remote` and `placement.result`; native rc 0; selected host's hostname and full `agents.wait` result; and kept evidence paths. Pending/partial results, fake launchers/receipts, or unconfirmed exit debt fail the gate. If the receipt/result fails, keep placement absent/local; do not claim rollout acceptance. If failure is discovered after installation/enabling, revert the candidate release and restore the saved host config.

Capacity-lead owns the subsequent Ryzen 1 host rollout: back up the selected `pi-agent/fabric.json`, enable host-only `default: "remote"` using the `--src`/host-map example above, canary one Main spawn/wait, then expand only after the same terminal receipt and full result pass. Revert means restoring that exact backup (and reverting the candidate release for an implementation failure). Re-measure Ryzen 1 spawned-agent CPU the next day; no workspace override enables the policy.

## Placement API v1

`agents.placementApi` is a separate host-only opt-in in `PI_CODING_AGENT_DIR/fabric.json`; workspace and project config cannot enable or override it. If absent or `enabled: false`, the existing spawn path is unchanged. Configure only `enabled: true`, `url`, and optional `requestTimeoutMs` / `watchTimeoutMs` (defaults 5,000 ms and 30,000 ms). Requests use unauthenticated plain HTTP to either `http://smarty-placement.smarty-island.svc:8080` inside the cluster or an island node's tailnet IPv4 address on NodePort `30880`.

Placement refusal, delay, network/protocol errors, or binding mismatch never fall back to a local worker. Only `process` and `remote` are supported; sandbox placement remains unsupported. A process admission must bind to this host; remote placement uses the separately configured host-owned `agents.placement.command` launcher. On NATS, process-placement presence is published before worker launch; a presence failure still prevents launch. A new pod UID for the same run ID is an ordinary restart, not a conflicting run. This alpha is tracked by `smarty-dev#15310` (private tracker).

After a proven worker exit, or when an admitted run never starts, Fabric makes a best-effort immutable terminal-record write (`finished`, `failed`, or `never-started`). FILE stores it beside the host lease; NATS uses the raw run id as the key in `fabric_placement_terminal` with a 900-second TTL. A receipt failure is logged without changing or delaying the child result. Fabric does not publish Placement's `placement.run.terminated` event.

## Execution kernels

`executor.kernel` selects the exclusive language for every `fabric_exec` call: `"typescript"` (default) or `"python"`. There is no per-call kernel selector or automatic language switching. Selecting Python is explicit opt-in; no additional enabled flag is required. `executor.pythonRuntime` defaults to `"monty"`, a sandboxed Python subset with VM-enforced resource limits and no ambient OS access. Monty is not CPython and cannot import arbitrary libraries; use the host bridge for I/O. Missing or invalid backend values choose Monty, and missing native Monty dependencies fail loudly without falling back. Set `executor.pythonRuntime: "cpython"` explicitly for the trusted native escape hatch, analogous to TypeScript's Node/Bun backends. `executor.cpython.binary` defaults to `"python3"` and accepts a CPython **3.10+** executable name or path, not shell arguments. Invalid kernel values fall back to TypeScript; absent, blank, or non-string binaries fall back to `python3`.

Set Python globally in `~/.pi/agent/fabric.json` or for one trusted project in `<project>/.pi/fabric.json`:

```json
{
  "executor": {
    "kernel": "python",
    "pythonRuntime": "monty"
  }
}
```

The same controls are under `/fabric settings` → **Executor** → **Kernel** / **Python runtime** / **CPython binary**. Python programs are async function bodies with `await` and `return`; host calls use the same namespaces and authoritative schema validation, without the static TypeScript check. See [execution kernels](kernels.md) for Python syntax, native dictionary results, payloads, parallel calls, and current guest-helper limitations. All `ts` code blocks and JavaScript-style call examples below are **TypeScript-only**.

`executor.runtime` affects **TypeScript only** (the **Runtime (TS)** setting); Python ignores it. It selects `"quickjs"` (the default isolated WASM runtime), `"node-process"` (a disposable native V8 process), or `"bun-process"` (a disposable native Bun/JavaScriptCore process). QuickJS memory limits stop at `4294967295` bytes, because its WASM32 `size_t` cannot represent 4 GiB. Fabric rejects larger values. It never wraps them. Node process limits can reach the detected physical memory, and Fabric passes them to V8 as `--max-old-space-size`. Bun process limits reach the same ceiling, but Bun ignores V8 heap flags, so the value is advisory, never an enforced cap.

Treat `node-process` and `bun-process` as an explicit escape hatch for trusted code. It offers no security sandbox. The runtime keeps Fabric's IPC host bridge, approvals, audit records, timeout, and cancellation in place. Node's and Bun's `vm` APIs provide no security boundary. Enable it only for workloads and projects whose generated code you accept running with the local user account's authority. Each invocation starts a fresh child process, and Fabric forcibly terminates that process when it settles, times out, or is cancelled. For TypeScript, schema enforce mode forces `quickjs`. Large limits in native runtimes can exhaust system memory or destabilize the machine.

Monty is always sandboxed, including under schema enforce, and does not require an installed CPython interpreter. Full CPython is an explicit trusted-native escape hatch with full local-user OS privileges outside schema enforce. Host approvals and audit cover bridge calls, not direct Python OS access. **Schema enforce preserves Python**; explicit CPython requires macOS `sandbox-exec` or Linux `bwrap` isolation; execution fails closed when isolation is unavailable, without falling back to unrestricted Python or TypeScript. `executor.memoryLimitBytes` uses `RLIMIT_AS` where the OS supports it, with a configuration ceiling of detected physical memory, not WASM32. This is an address-space limit, not a portable hard resident-memory cap. Process limits, timeouts, and cancellation are not a security sandbox; see [kernel isolation](kernels.md#isolation-and-resource-limits).

### Native Pi codemode API

`executor.codemodeProfile` is `"additive"` by default: existing Fabric APIs stay unchanged, while `nativeTools` and nonconflicting `tools.<name>` methods provide native Pi tool results. Set `"native"` for unmodified Pi JavaScript examples: `tools` and the discovery globals use native semantics, while `fabric.tools` retains Fabric discovery. Both profiles preserve `pi.*`, `mcp.*`, `extensions.*`, approvals and sPTC safety. The setting is TypeScript-kernel-only; Python ignores it. See [native codemode compatibility](codemode-parity.md) for collisions, result contracts, images and state interoperability. The UI setting is **Executor → Codemode API (TS)**.

### Executor timeouts and ceilings

`executor.timeoutMs` (default `120000`) bounds a whole `fabric_exec` program. Two mechanisms can raise it:

- **Per-invocation request**: `fabric_exec({ timeoutMs: 600000, code: ... })` asks for a longer whole-program deadline for that one call. It can never reduce the default: the effective timeout is `max(executor.timeoutMs, requested)`.
- **Per-ref floor**: `executor.hostCallTimeouts` maps exact host-call refs (no wildcards) to a minimum deadline in ms. A matching ref raises the enclosing deadline to at least the configured value without any tool-side timeout argument:

```json
{
  "executor": {
    "timeoutMs": 120000,
    "maxTimeoutMs": 3600000,
    "hostCallTimeouts": {
      "extensions.subagent": 3600000
    }
  }
}
```

Every raised deadline is capped by `executor.maxTimeoutMs` (default `900000`, i.e. 15 minutes: the former undocumented clamp, now explicit), which itself can be raised up to the hard implementation maximum of 24 hours. Values above a cap are visibly normalized down to the cap during config load and the effective values are shown in `/fabric` settings, never silently surprising. A per-invocation request or ref floor takes effect even when the ref is unknown to Fabric, so captured tools, MCP calls, and future host calls all run within an intentionally longer deadline without Fabric knowing their argument semantics. Existing `pi.bash` behavior (extending the deadline from an explicit `timeout` argument) is unchanged, and deadline expiry still cancels the active host call and any child process it owns.

**Interactive Main only** (TUI or RPC, not task agents or actors): `executor.mainMaxTimeoutMs` is a fixed whole-program ceiling, default `600000` (10 minutes). It overrides the orchestration floor, per-invocation requests, exact-ref floors, and explicit shell-timeout floors, including programs that repeatedly wait or sleep. It normalizes to `60000`–`executor.maxTimeoutMs`; if the executor maximum is itself below 60 seconds, that smaller maximum wins. Hitting this ceiling returns `MainExecutionCeilingError` and ends only the foreground program/observation: spawned agents, durable runs, and detached tasks keep running, and agents report their results as completion messages. Check `agents.status` / `agents.list`. Main `agents.run`, `agents.wait`, and `agents.join` also bound each observation to 60 seconds and return live status with `waitTimedOut: true`, without consuming the later result. Noninteractive runs and task/actor/residency hosts retain their existing behavior.

`executor.humanWaitRefs` (default `["extensions.ask"]`) lists exact host-call refs (no wildcards) that wait for a person. While at least one such call is in flight, the program deadline is **paused**: a foreground question can wait as long as the person needs. When the last one settles, the program continues with the budget it had left, so guest work before and after the wait still counts. In interactive Main the fixed `executor.mainMaxTimeoutMs` ceiling still applies during a wait. A host-call floor that arrives during the pause raises that remaining budget. Cancelling `fabric_exec` (Esc or an aborted signal) still stops the program and the pending call at once, and CPU-slice and memory limits are unchanged. Set `[]` to bound human waits by the normal deadline again:

```json
{
  "executor": {
    "humanWaitRefs": ["extensions.ask"]
  }
}
```

`executor.shellHangMs` (default `120000` / 2 minutes, max `600000` / 10 minutes, `0` disables) is a nested-shell wait budget, not a program deadline. When a `pi.bash` / `pi.powershell` await exceeds it, Fabric **settles the await successfully** (`ok: true`) with a still-running notice, pid, and live output path while the process keeps writing that file. `background: true` (alias `run_in_background`) detaches immediately with the same envelope. Inspect with `pi.read(logPath)` and stop by running `kill <pid>` through `pi.bash`. Do not poll. An explicit shell `timeout` remains a hard cap. **ctrl+b twice** spills early (tmux-safe); **ctrl+k** kills the waiting command. Session shutdown aborts leftover processes, except `durable: true` tasks, which detach to their jev-fabric store. Captured shell overrides normally keep their own execution semantics; an extension can opt into [Fabric-owned bash execution with middleware](shell-middleware.md) to preserve its environment/output filters while gaining the same background handling.

`executor.jevFabric` configures the optional jev-fabric backend for [durable tasks](background-tasks.md#durable-tasks-through-jev-fabric) and [interactive sessions](shell-composition.md). `binary` (default `""`, also `"auto"`) picks your compatible install outside the workspace, then the bundled package; an explicit executable name or trusted absolute path (no shell arguments) is used or the call fails, never falling back. See [which jev-fabric](shell-composition.md#which-jev-fabric). `home` empty uses `JEV_FABRIC_HOME`, else `<cwd>/.jev-fabric-native`: the store other harnesses in the same project share by default. `timeoutMs` (default one hour, up to 24 hours) is a durable job's lifetime when the call gives no explicit `timeout`. Nothing runs or loads until the first `durable: true` call.

The precedence across all sources is:

```text
effective timeout = min(
  maxTimeoutMs,
  max(executor.timeoutMs, matching hostCallTimeouts[ref], fabric_exec.timeoutMs)
)
```

where absent values do not participate, and time spent inside a `humanWaitRefs` call is not counted. Outside interactive Main, orchestration programs (`agents.run` / `agents.wait` / `agents.ask`, `workflow.agent`, ...) keep their separate `agents.timeoutMs` floor, which is unaffected by `executor.maxTimeoutMs`. In interactive Main, the fixed `executor.mainMaxTimeoutMs` ceiling takes precedence over every source above; repeated host calls cannot extend it.

`executor.powershell` (default `"refuse"`) gates PowerShell (smarty-dev#9826). By default `pi.powershell` is **refused**, and so is every captured or extension `powershell` override, whether it is reached through `pi.powershell` or `extensions.powershell`. The refusal names smarty-dev#9826 and happens before any `tool_call` hook or execution. The reason: no Fabric guard judges PowerShell grammar, and the fleet's command guards are bash-only. Showing them PowerShell source as a bash call is not a safety mechanism, because PowerShell-only spellings (for example `Set-Location <dir>; …`, the `&` call operator or `-EncodedCommand`) do not parse the way a bash guard reads them. Set `"powershell": "allow"` under `executor` in the host profile's `PI_CODING_AGENT_DIR/fabric.json` to opt in. The key is **host-only**: a workspace `.pi/fabric.json` cannot set or withdraw it. **The opt-in accepts that grammar gap.** With it, each call is first shown to guards as a `bash` `tool_call` (`input.shell: "powershell"`, a non-session `cwd` as a leading `cd`), then as the `powershell`-named `tool_call`. A block from either refuses the call. A hook that rewrites either input also refuses it, so the command runs only exactly as judged. Any other value refuses. Pi's own model-facing `powershell` tool, used outside full code mode, is not gated by this key.

## Full reference

```json
{
  "configVersion": 4,
  "fullCodeMode": true,
  "executor": {
    "kernel": "typescript",
    "cpython": { "binary": "python3" },
    "jevFabric": { "binary": "jev-fabric", "home": "", "timeoutMs": 3600000 },
    "runtime": "quickjs",
    "timeoutMs": 120000,
    "maxTimeoutMs": 900000,
    "mainMaxTimeoutMs": 600000,
    "hostCallTimeouts": {},
    "humanWaitRefs": ["extensions.ask"],
    "shellHangMs": 120000,
    "powershell": "refuse",
    "memoryLimitBytes": 67108864,
    "maxOutputChars": 100000,
    "maxNestedResultChars": 2000000,
    "resultFormat": "auto"
  },
  "approvals": {
    "read": "allow",
    "write": "allow",
    "execute": "allow",
    "network": "allow",
    "agent": "allow",
    "headless": "deny",
    "headlessTimeoutMs": 300000
  },
  "capture": {
    "enabled": true,
    "hideFromModel": true,
    "keepVisible": ["fabric_exec"],
    "defaultRisk": "execute",
    "risks": {
      "read": "read",
      "grep": "read",
      "find": "read",
      "ls": "read",
      "edit": "write",
      "write": "write",
      "bash": "execute",
      "fovea_sketch": "read",
      "fovea_focus": "read",
      "fovea_dwell": "read",
      "fovea_impact": "read"
    }
  },
  "foreground": {
    "tools": [],
    "maxTools": 4
  },
  "mcp": {
    "enabled": true,
    "disableOAuth": true,
    "allowDynamicServers": true,
    "callTimeoutMs": 120000,
    "cache": {
      "enabled": true,
      "revalidate": "changed",
      "revalidateBudgetMs": 60000
    }
  },
  "prewalk": {
    "enabled": true,
    "mode": "in-place",
    "alwaysRearm": false,
    "detectShellWrites": true
  },
  "models": {
    "aliases": {
      "cheap": "google/gemini-2.5-flash",
      "budget": ["openai/gpt-5-mini", "google/gemini-2.5-flash"]
    }
  },
  "agents": {
    "enabled": true,
    "runner": "pi",
    "transport": "process",
    "claude": {
      "binary": "claude"
    },
    "veda": {
      "binary": "veda",
      "backend": "agy",
      "persona": "navigator-chat"
    },
    "modelPolicy": {
      "requireReason": ["gpt-6-astra", "opus", "claude"],
      "fallbackThinking": "high"
    },
    "thinking": "medium",
    "maxConcurrent": 4,
    "maxPerExecution": 100,
    "maxDepth": 2,
    "timeoutMs": 86400000,
    "extensions": true,
    "defaultTools": ["read", "bash", "edit", "write", "grep", "find", "ls"],
    "retainRuns": false,
    "notifyOnComplete": true,
    "budgetUsd": 0,
    "maxTokensPerChild": 0,
    "sessionExport": true,
    "sessionExportDir": "",
    "nice": 0,
    "deadRootFilter": { "mode": "off", "exempt": [] },
    "stuckPreparingMs": 600000,
    "childQuestions": "cancel",
    "childQuestionTimeoutMs": 600000
  },
  "components": [
    {
      "id": "project-service",
      "component": "registered-definition",
      "config": {},
      "disabled": false
    }
  ],
  "ui": {
    "enabled": true,
    "widget": "auto",
    "maxRows": 6,
    "refreshMs": 500,
    "eventHistory": 80,
    "haltOnEscape": true,
    "showAgentToolPreview": true,
    "toolDisplay": "compact",
    "principalView": "auto",
    "updateDebounceMs": 100
  },
  "compaction": {
    "engine": "fabric",
    "outputReserveTokens": 0,
    "pressureBands": { "warn": 0.6, "urgent": 0.8 },
    "repairOrphans": true
  },
  "retention": {
    "orphanedTempRunMs": 21600000,
    "oneShotRunMs": 86400000,
    "actorRunArchiveMs": 604800000,
    "terminalRunEventsAgeMs": 86400000,
    "terminalRunEventsMaxBytes": 262144
  },
  "mesh": {
    "controlIncarnationFence": "warn",
    "lockProtocol": 1,
    "stateBackend": "file",
    "enabled": true,
    "announce": false,
    "actorScope": "project",
    "maxEventBytes": 262144,
    "maxReadEvents": 500,
    "actorPollMs": 250,
    "residentIdleExitMs": 600000,
    "idleReadCoalesceMs": 5000,
    "actorQueueLimit": 32,
    "eventContextChars": 40000,
    "followUpFlushMs": 120000,
    "followUpStallSeconds": 600,
    "rootPresenceAlarmMs": 900000,
    "undeliveredAlarmMs": 1800000,
    "rootGoneTtlMs": 7200000
  },
  "actors": {
    "maxSessionBytes": 20971520,
    "transientBlockCooldownMs": 600000,
    "transientBlockMaxMs": 3600000
  },
  "memory": {
    "enabled": true,
    "sources": [
      { "id": "laptop", "kind": "fs", "root": "/home/me/pi-archive" }
    ]
  },
  "trace": {
    "assessment": false
  }
}
```

## Jev System One

`jev` configures TypeSafe typed judgments and session-owned foreground/background programs. It is enabled by default but makes no inference requests until called. `/login jev` stores API-key credentials through Pi; `TYPESAFE_API_KEY` and an explicitly configured `jev.credentialCommand` argv are also supported. Bare `jev.model` aliases use TypeSafe; `typesafe/...` or `~typesafe/...` model IDs use OpenRouter's decisions endpoint with the existing `openrouter` credential (`/login openrouter`, `OPENROUTER_API_KEY`, or `TYPESAFE_OPENROUTER_API_KEY`); `typesafe-ai/...` model IDs use Vercel AI Gateway's TypeSafe-compatible endpoint with the existing `vercel-ai-gateway` credential (`/login vercel-ai-gateway` or `AI_GATEWAY_API_KEY`). Never store the resolved secret in `fabric.json`.

Host ceilings include `maxDurationMs`, `maxEvaluations`, `maxToolCalls`, `maxTokens`, `maxConcurrentRuns`, and `maxRetainedRuns`. Request controls are `model`, `requestTimeoutMs`, and `maxRequestBytes`. Per-program limits cannot raise these ceilings. Per-program `maxEvaluations: 0` disables program inference for deterministic shell orchestration. See [Jev programs](jev.md) for typed decisions, shell/task composition, and cancellation; harness CLIs need no component configuration. Jev is unavailable in Schema enforce and managed-host modes.

## Lossless decision profiles

`jev.decisionProfiles` is a portable `{version:1, defaultProfile?, profiles}`
document; global/project precedence replaces it atomically and never merges
fields from different providers. `jev.decisionProfile` optionally selects one
of its names. `null` restores the document default and can clear an inherited
global selector; an empty or unknown name is invalid.

In `/fabric settings` → **Decisions**, select a configured profile or
**Document default**. This updates defaults for future direct calls/new programs
without reloading approval policy. Already-running programs retain their
captured selection and all calls retain their existing budgets. This setting
never changes Main's chat model or `approvals.model`.

See [lossless decision models](jev-decisions.md) for configuration examples,
capability discovery, native binary requirements and credential boundaries.

## Components

`components` is a root array of declarative supervised instances. Each `id` gives one instance a stable identity, and `component` names its definition in the versioned protocol. Fabric passes `config` to `activate(context, config)`. The `disabled` field removes an instance from the active graph and preserves its declaration. An empty array is the default, with a limit of 256 valid entries. The runtime installs enabled first-party providers as pinned `fabric.provider.*` components whose reserved IDs sit outside this array.

Unknown definitions stay visible as waiting. They do not fail the Fabric runtime. Late discovery activates them. Once the runtime is active, trusted file edits reconcile automatically without `/fabric reload`; idle bootstrap remains lazy and first use rereads the component configuration. Invalid live edits keep the last working state and are never repaired or renamed by the watcher. `components.describe`, `components.plan`, `components.apply`, and `components.reconcile` provide the same control plane to programs. Changes default to session scope; global/project persistence is explicit, and untrusted project writes are rejected, never redirected. Project component arrays replace global arrays; session overrides apply by ID on top. Definitions may declare `configSchema` for pre-activation validation. When a definition re-registers with `overwrite: true`, Fabric uses the same rollback-capable replacement path. See [components, effects, and committed capabilities](components.md#live-configuration-control).

## Speculation

`speculation` configures opportunistic pre-launch of read-class calls while the model streams a `fabric_exec` program; see [speculative PTC](speculation.md) for the correctness contract. `speculation.enabled` (default `true`) masters the feature. `speculation.maxConcurrent` (1-32, default 4) caps in-flight speculative calls. `speculation.maxEntries` (1-1024, default 64) bounds retained unserved entries per turn. `speculation.maxBufferBytes` (64 KiB-64 MiB, default 2 MiB) caps the per-stream partial-argument buffer. `speculation.entryTtlMs` (5 s-30 min, default 180000) expires unserved entries. `speculation.mcpAllowlist` (default empty) enables Tier-B speculation of read-only MCP tools with `server.tool` or `server.*` patterns.

## Thinking bounds

`thinking.bounds` (`{"min"?: level, "max"?: level}`, default `{}`) bounds every thinking level Fabric selects: [`thinking.set`](thinking.md), each child run's `thinking`, and the bounds a child inherits. Levels are `off`, `minimal`, `low`, `medium`, `high`, `xhigh`, and `max`. An omitted end leaves that side to the active model's supported levels. Unknown keys, unknown level names, and `min` above `max` fail configuration loading. A Fabric child intersects this value with its parent's `PI_FABRIC_THINKING_BOUNDS` and never widens it.

```json
{ "thinking": { "bounds": { "min": "low", "max": "high" } } }
```

## Prewalk executor

`prewalk.enabled` defaults to `true` and is the persistent master switch. Turn it off under **Prewalk → Enabled** in `/fabric settings`, or run `/fabric prewalk --disable`; both save to project scope in a trusted project and global scope otherwise. Disabling also cancels any live arm. `/fabric prewalk --enable` turns it back on. `/fabric prewalk --off` only cancels the current arm for this session and does not change the saved master switch.

`prewalk.model` is the optional Pi `provider/model` that `/fabric prewalk` selects. `prewalk.mode` chooses how execution continues:

- `"in-place"` (default) switches Main to the executor model, queues a hidden follow-up in the same session, and restores Main's boundary model when the continuation settles, when a new session inherited the executor, or when prewalk is cancelled.
- `"trajectory"` forks the finalized outer Fabric call and result to a visible Pi child, then waits for it. After the child finishes, a hidden continuation asks Main to verify the work and report its findings.

```json
{
  "prewalk": {
    "enabled": true,
    "mode": "in-place",
    "model": "anthropic/claude-haiku-4-5",
    "thinking": "high",
    "alwaysRearm": true,
    "compactOnReturn": true
  }
}
```

`prewalk.thinking` sets the optional reasoning effort for the trajectory child executor. Its values are `off` / `minimal` / `low` / `medium` / `high` / `xhigh` / `max`, clamped to each model's supported levels. When you leave it unset, the executor inherits `agents.thinking`. In-place mode keeps Main's session level.

`prewalk.alwaysRearm` defaults to `false`. When enabled, prewalk returns to an armed, taskless state after each completed handoff (in-place return or trajectory completion); a failed in-place return drops the arm without completing it. Every Main session then starts armed automatically, non-interactively from `prewalk.model`, and `/fabric reload` re-arms Main as well. Child agents and actors never auto-arm from inherited settings; explicit arming is unaffected. `/fabric prewalk --off` cancels the armed state until the next session start or reload. Turns that settle without a handoff never disarm prewalk, regardless of this setting. The settings UI labels an unset model **Ask each time**. Non-interactive sessions must configure a model. In-place mode does not require child agents. Trajectory mode requires `agents.enabled`. It shows child spawn, progress, nested tools, metrics, and completion in Main's Fabric activity UI.

`prewalk.detectShellWrites` defaults to `true`. When armed, a `fabric_exec` boundary that ran a successful `pi.bash` or `pi.powershell` without an audited `pi.edit` / `pi.write` / `schema.commit` claims the handoff if file size or mtime stats drifted from the arm-time baseline. This routes shell heredocs and formatter binaries to the executor as well. The report's `trigger.files` lists the bounded drifted paths. An audited mutation consumes the shell-write drift window, so earlier edits cannot re-fire on a later read-only shell boundary. Fabric's own state directory never registers and does not consume the tracked-file cap. Other tool directories follow the project's ignore rules, so a Git work tree excludes them through `.gitignore`. Set this option to `false` to accept audited mutations only.

`prewalk.requirePlan` defaults to `true`. An armed task owes a recorded plan before its mutation boundary can hand off, whether the trigger is an audited `pi.edit` / `pi.write` / `schema.commit` or shell drift under `detectShellWrites`. A boundary reached without a plan is withheld: Fabric delivers a hidden plan checkpoint to Main that asks for `prewalk.plan({ outcome, steps, verification, risks })` inside `fabric_exec`. That recorded plan is the readiness signal: Fabric snapshots it at claim time and delivers it in the executor's hidden continuation or child task, so delivery does not depend on the outer tool result surviving. The arm stays armed across the checkpoint and the frontier model keeps working. Fabric asks at most twice per task, then hands off unplanned with a visible warning so an armed session cannot stall. A recorded plan survives a failed handoff that returns to armed, and Fabric drops it when the captured task changes; cancelling, re-arming, or reloading Fabric resets readiness so the next task plans again. Checkpoint delivery is a hidden custom message, never a system prompt. Set this option to `false` to hand off on the first mutation. `prewalk.status` inside `fabric_exec` reports `planRequired`, `planReady`, and the reminder count for the current session.

`prewalk.plan` is an executor handoff checkpoint, not a general task planner, checklist, or acceptance-ledger store. Maintain ordinary acceptance checks in reasoning or concise progress notes without a tool call. Submit a handoff plan only when Fabric's armed advisory or checkpoint requests it; no preliminary `prewalk.status` call is needed. Discovery (`tools.list`, `tools.search`, `tools.catalog`, and `tools.describe`) exposes `prewalk.plan` only while the current session owes one. Once recorded, the action and planning directives retire until the next arm/task needs a plan; host validation still rejects stale calls. `prewalk.status` remains available for diagnostics.

`prewalk.compactOnReturn` defaults to `true`. When an in-place continuation settles, Fabric requests a compaction with the configured `compaction.engine` and commits it while the executor is still the active model. Main's restored model receives the compacted transcript. Set this option to `false` when Main must receive the complete transcript.

Each in-place handoff captures Main's active model at the boundary and restores it when the continuation settles, when a new session is still on the executor, and when prewalk is cancelled (`/fabric prewalk --off` / `--disable`) or reloaded. A process that restarts while a continuation is still pending restores that captured model from the persisted continuation at the next session start, before the session auto-arms. Pi's public `setModel` extension API may also update the session model that a later session inherits, so restoring the captured Main model repairs that too. When the return itself fails, the arm is dropped, not re-armed and the failure is reported: the captured Main model is preserved so a later session start or `/fabric reload` retries the return, auto-arm is skipped while Main is still on the executor, and an explicit `/fabric prewalk` arm overrides.

## Models

`models.aliases` names model selectors for `agents.switchModel` and for Pi-runner `model` arguments on `agents.run`, `agents.spawn`, `agents.create`, and `agents.handoff` (see [Agents](agents.md#switching-mains-session-model)). Each alias is either one `provider/model` target, an ordered fallback chain, or an object `{"model": <target or chain>, "thinking": <level>}`. Resolution walks the chain and uses the first authenticated target. Alias names match case-insensitively and take priority over bare model ids and fuzzy matching. Aliases live in normal Fabric configuration, so a project `.pi/fabric.json` can extend the agent-level `fabric.json`; entries with malformed names or targets are ignored at load, and an unrecognized `thinking` level is dropped while the alias survives. An alias `thinking` level is the default effort for every run that selects it: an explicit `thinking` on the call or actor wins, and `agents.thinking` applies only when the alias sets none. `agents.switchModel` changes only the session model, so an alias thinking level does not apply there.

```json
{
  "models": {
    "aliases": {
      "cheap": "google/gemini-2.5-flash",
      "budget": ["openai/gpt-5-mini", "google/gemini-2.5-flash"],
      "shallow": { "model": "google/gemini-2.5-flash", "thinking": "low" }
    }
  }
}
```

## Third-party model providers

Fabric selects every model through Pi's model registry and ships no provider list of its own. The same `provider/id` keys work in `agents.model`, `models.aliases` (including fallback chains), `prewalk.model`, `approvals.model`, the `model` argument of `agents.run`, `agents.spawn`, `agents.create`, and `agents.handoff`, and the `/fabric settings` model pickers. A provider that Pi's `models.json` defines works without extension discovery. A provider that a Pi extension registers with `pi.registerProvider()` requires that extension to be enabled in the child as well as the parent.

Credentials stay with Pi. Fabric hands a child agent only the model key, and the child resolves the credential through Pi as usual: a stored entry in `auth.json` (for example from `/login`), the environment variable the provider names, or the credential store. Fabric never puts keys on a child's command line or in its logs.

With `extensions: false` (per call, or as `agents.extensions`), a Pi child loads none of the user's extensions, including their provider-registration code. Built-in models and models declared in Pi's `models.json` remain available. An extension-only provider requires an explicit `extensions: true` request and Pi's normal project-trust checks. Fabric does not probe provider-named paths or load other enabled extensions to find it: an isolated registration runtime is not a sandbox for module code. Unknown models fail before the task is sent, without a fallback model. Restricted judgment workers keep their fixed host-hook allowlist and never load a provider bridge. This covers both Pi runners (`pi-durable` and `pi`); strict model admission is unchanged. Claude, Veda, and custom runners use their own model catalogs and are unaffected.

### Bundled Coral Bricks provider

Fabric bundles the Coral Bricks provider extension, [`pi-coralbricks-provider`](https://www.npmjs.com/package/pi-coralbricks-provider), so `coralbricks/*` models are available after installing Fabric alone. Disclosure: Fabric's maintainers work at Coral Bricks.

Add a key once, in either form:

```sh
/login coralbricks                # inside Pi; stores the key in auth.json
export CORALBRICKS_API_KEY=...    # or in the environment
```

The provider registers keys such as `coralbricks/glm-5.3-fast` and `coralbricks/deepseek-v4.1-flash-fast`. Without a key it serves the model list shipped in the package and makes no network request. With a key it refreshes that list from Coral's API at session start and after `/login`. It loads as its own entry in Fabric's package (`node_modules/pi-coralbricks-provider/index.ts`), outside Fabric's startup graph, so `extensions: false` children resolve `coralbricks/*` models through the bridge above without loading Fabric's main extension.

You can also install `pi-coralbricks-provider` yourself, for example to pick up a newer release before Fabric bundles it. When both copies load, the newest version registers the provider and the other one stays idle, so `/model` lists each model once.

To leave the bundled provider out, replace the plain `npm:pi-fabric` entry in `~/.pi/agent/settings.json` (or a project's `.pi/settings.json`) with the package object form and exclude its path:

```json
{
  "packages": [
    { "source": "npm:pi-fabric", "extensions": ["!node_modules/pi-coralbricks-provider/**"] }
  ]
}
```

Fabric's own extension keeps loading. Use the same `source` you installed from (`git:github.com/fabric-runtime/pi-fabric` or a local path). After opting out, `coralbricks/*` models resolve only if you install the provider separately, and an `extensions: false` child that asks for one fails before the task is sent.

Use the Coral Bricks keys wherever Fabric takes a model, for example in aliases that mix providers:

```json
{
  "models": {
    "aliases": {
      "fast": ["coralbricks/deepseek-v4.1-flash-fast", "google/gemini-2.5-flash"],
      "deep": { "model": "coralbricks/glm-5.3-fast", "thinking": "high" }
    }
  }
}
```

Alias resolution uses the first target with a credential, so `fast` falls back to Gemini on a machine without a Coral Bricks key. A program can also run the same task on two providers side by side:

```ts
const task = "Summarize the error handling in src/worker.ts in five bullets.";
const [coral, gemini] = await Promise.all([
  agents.run({ model: "coralbricks/deepseek-v4.1-flash-fast", task, extensions: true }),
  agents.run({ model: "google/gemini-2.5-flash", task, extensions: false }),
]);
return { coral: coral.text, gemini: gemini.text };
```

### Other providers

Every other provider works the same way. Install its Pi extension with `pi install`, or define it in Pi's `models.json`, and its `provider/id` keys work in each place listed at the start of this section, including `extensions: false` children.

## Result formatting

`executor.resultFormat` sets the default for `fabric_exec` return values. Find it under `/fabric settings` → **Executor**. `"auto"` keeps strings as text and renders structured values as syntax-highlighted YAML. `"yaml"`, `"json"`, and `"text"` each force their named behavior. A call-level `resultFormat` parameter overrides the configured default.

Configure the compaction engine under `/fabric settings` → **Compaction**. Select `"fabric"` for deterministic compaction, or `"pi"` to hand compaction to Pi core.

## Code modes

In the default full code mode, `fabric_exec` owns Pi core tool execution. The parent model sees one programmable tool. The direct `read`, `bash`, `powershell`, `edit`, `write`, `grep`, `find`, and `ls` schemas stay hidden. Fabric programs reach those capabilities through `pi.*`:

```ts
const files = await pi.find({ pattern: "**/*.ts", path: "src" });
const matches = await pi.grep({ pattern: "TODO", path: "src" });
return { files, matches };
```

Run independent calls in parallel:

```ts
const [packageJson, readme] = await Promise.all([
  pi.read({ path: "package.json" }),
  pi.read({ path: "README.md" }),
]);
return {
  package: JSON.parse(packageJson).name,
  readmeLines: readme.split("\n").length,
};
```

Pi core calls reject when the native tool reports an error. Successful `bash`, `powershell`, `edit`, and `write` calls return the `{ ok: true, output, details }` shape. Catch a rejection when recovery is local. Shell tools reject on an ordinary nonzero exit. Pass `settle: true` (for example `pi.bash({ command, settle: true })` or the Windows-only `pi.powershell({ command, settle: true })`) to receive `{ ok: false, output, details: null, exitCode, error }` on a nonzero exit. Timeout, cancellation, approval, security, and spawn failures still reject.

### Full code mode (default)

`fullCodeMode: true` is the default. Fabric removes the active Pi core tools from the parent model and exposes their implementations only inside `fabric_exec` through `pi.*`. Fabric also captures registered overrides such as security gates and code previews, so `pi.read()` keeps routing through the override.

Fabric records which native core tools were active before it takes ownership. Switching to orchestration-only mode or unloading Fabric restores that selection. Fabric applies full-mode ownership only when the session initializes or the mode changes. It never resets an explicitly selected active tool set from input, agent-start, turn-end, or settled lifecycle hooks. The system prompt carries the full-mode execution rule.

Pi core shows its model-visible skill catalog only while the native `read` tool is active. Full code mode restores the same catalog from Pi's structured skill registry and changes only the loader instruction, so `pi.read` runs inside `fabric_exec`. Native core tools stay hidden. Packaged skills mark cross-document paths with `<skill-dir>`. Fabric replaces that marker inline from Pi's expanded skill `location` or the actual `SKILL.md` read path. It never matches skill names or enumerates directories. Ordinary document reads stay unchanged. When an expanded skill invokes another installed skill, Fabric adds an exact name-to-path resolution hint for that turn, and the delegated `SKILL.md` loads before task work.

### Orchestration-only mode

Some users want Fabric for MCP, agents, ambient actors, parallel workflows, councils, and recursive delegation while Pi's core tools remain fully native. Those users can opt out of full code mode:

```json
{
  "fullCodeMode": false
}
```

In orchestration-only mode:

- Pi's `read`, `bash`, `powershell`, `edit`, `write`, `grep`, `find`, and `ls` tools stay on Pi's normal model-facing and execution paths. Fabric applies the configured risk approval policy through Pi's native `tool_call` preflight, and it leaves their execution and rendering untouched.
- Registered extension tools also remain in Pi's native registry. Fabric does not hide, wrap, or expose them through `extensions.*`. Model-requested direct calls use exact `capture.risks` overrides or the conservative `capture.defaultRisk` approval class.
- `pi.*`, `extensions.*`, and equivalent `tools.call()` references are unavailable inside `fabric_exec`, regardless of the configured kernel or whether TypeScript checks run.
- MCP and stable Fabric providers remain available through `mcp.*`, `memory.*`, `state.*`, `schema.*`, `components.*`, and `compact.*`. Generic discovery and computed refs still work through `tools.*`. One-shot and recursive agents, persistent ambient actors, dynamic workflows, mesh coordination, councils, explicit Fabric providers, and the Fabric TUI keep their full behavior.
- Child agents continue using their allowed Pi tools directly, so parallel and ambient setups never route their coding operations back through Fabric code mode.

### Where to set `fullCodeMode`

`fullCodeMode` defaults to `true`. Set the flag in `.pi/fabric.json` for one project, or globally in `~/.pi/agent/fabric.json` for every project. `/fabric settings` toggles it as well.

## Self-reload onto a newer release (`autoReload`)

A top-level Main (TUI or RPC; not a task agent, an actor or `pi -p`) watches the Pi profile's `settings.json`. When its `packages` list activates a different local `pi-fabric` package than the one this Main loaded, the Main reloads itself after a run settles, once nothing a reload would stop is still running: no task agent it started, no actor it hosts with a run in flight or a queue draining (both actor scopes), no live Fabric shell job (`pi.bash`/`pi.powershell` in the background, a monitor, or a command the hang threshold detached), and no running Jev program or observer. A long-lived monitor or observer therefore holds the reload until it ends; stop it, or run `/reload`, to move sooner. While it is busy, it re-checks every 5 seconds while idle. After the reload the pane shows `Fabric reloaded: <old> → <new>` (the release directory names; also in the footer until the next input), actors are re-armed, and the mesh gets `ops.fabric.reloaded` with `{ old, new, sessionId }`. A Fabric loaded from anywhere other than the release the profile activated at load (for example `pi -e` or a project package) never follows the profile, even after a later swap. The reload waits until no prompt is in preflight and no `agent_settled` handler is running. While it reloads, the Main stays in the participant directory as `reloading` for up to 180 seconds (a reload on a loaded host can take a minute or more); the new release's first heartbeat replaces that grace, and a Main that dies mid-reload drops out once it ends.

Automatic reloads share **6 slots per host/user** by default (`"selfReloadConcurrency": 6` in `fabric.json`). A new target adds 0–30 seconds of random jitter before its first automatic attempt; a full limiter keeps the target pending on the same 5-second idle retry. Busy Mains are never made to wait inside an admission call. Slots live in `/tmp/pi-fabric-reload-slots-<uid>` on POSIX (the user's temp directory on Windows), independent of profile, worktree, mesh settings and session-specific `TMPDIR`. They expire after 120 seconds, or immediately when the recorded PID/Linux birth identity is dead, and are released on reload completion, failure or the new `session_start`. Use the same concurrency setting across Mains on a host; mixed settings cannot enforce one common cap. `"selfReloadConcurrency": 0` restores unlimited automatic reloads without jitter. Manual `/reload` and `/fabric-release-reload` bypass admission.

Opt out per session with `PI_FABRIC_NO_AUTO_RELOAD=1`, or with `"autoReload": false` in `fabric.json`. An opted-out Main shows once that a newer release is active; `/reload` or `/fabric-release-reload` loads it.

## Captured extension tools

When `fullCodeMode` is enabled, Fabric intercepts Pi's `ExtensionRunner.getAllRegisteredTools()` registry chokepoint. This captures tools that other extensions register at startup or later through `pi.registerTool()`. Whether an extension loads before or after Fabric makes no difference.

Captured custom tools leave the model's active tool set by default. Their schemas, snippets, and guidelines stop consuming the parent model context, and the model reaches them only through `fabric_exec`. The tools stay **registered** in Pi's runtime, so `pi.getAllTools()` keeps listing them. Host extensions that gate or audit tool calls by name (for example `@gotgenes/pi-permission-system`, which blocks names missing from that list before its own rules run) still see them as registered, and they evaluate nested captured calls through their normal policy and prompts. The owning extension remains loaded: its commands, event handlers, state, and UI continue to work. Only model-facing exposure and invocation become lazy.

```ts
const matches = await tools.search({ query: "deployment status" });
const schema = await tools.describe({ ref: matches[0].ref });
const result = await tools.call({
  ref: schema.ref,
  args: { environment: "staging" },
});
return result;
```

For tool names valid as JavaScript properties, use the shorter proxy:

```ts
const result = await extensions.project_status({ verbose: true });
return result.text;
```

The result keeps `content`, exposes text content as `text`, and carries `details`, `structuredContent` when supplied, `isError`, `terminate`, and source provenance. On Pi 1.0, callable captures use native `ctx.executeTool()` with nested IDs, validation, middleware, and usage accounting. Shells and tools with `prepareArguments()` retain Fabric's adapted execution boundary to preserve scoped cwd, nonzero-exit settlement before redaction, and exactly-once preparation. Both paths use the owning extension's tool context and apply `tool_call`, `tool_result`, and `tool_execution_*` handlers. Tools withdrawn with native `exposure: "hidden"` are not captured.

In full code mode and Schema enforce mode, `fabric_exec` is the only model-declared tool unless a [foreground policy](#foreground-tools) names others in full code mode. Pi 1.0 native `prepareLoadout` and per-request transcript projection hide all other declarations without removing tools from the native callable set. This includes native `codemode`, `tool_search`, MCP tools, and late registrations; `setActiveTools()` cannot open a second model-facing path. Inside Fabric, `pi.read`, `pi.bash`, and other built-ins still route through captured overrides when present. `extensions.read` exposes the override's native result shape. Native MCP tools are also callable by their captured names, for example `extensions.mcp__server__tool(...)`.

### Foreground tools

Some extension tools only work as a direct model turn: a question to the human, a turn-steering control, a context or skill loader. `foreground` keeps a short list of them declared beside `fabric_exec` in full code mode:

```json
{
  "foreground": {
    "tools": [
      { "name": "ask_user_question", "owner": "pi-ask", "reason": "human-input" }
    ],
    "maxTools": 4
  }
}
```

- Each entry needs `name`, `owner` (free text naming the extension that registers the tool), and `reason`: `turn-steering`, `human-input`, `context-control`, or `skill-loading`. At most 64 entries; `maxTools` is an integer from 0 to 8 (default 4). A malformed entry or `fabric_exec` as a name fails config loading.
- Fabric resolves the list in order against the live registry. It refuses Pi core tools (`read`, `bash`, `powershell`, `edit`, `write`, `grep`, `find`, `ls`), other Pi built-ins such as native `codemode`, unregistered or `hidden` tools, inactive tools, tools outside an inherited child tool allowlist, duplicates, and entries beyond `maxTools`.
- Schema enforce mode and managed hosts refuse the whole policy: only `fabric_exec` stays declared. Orchestration-only mode already declares Pi's active set, so the policy has no effect there.
- Refusals are never silent. Fabric shows one warning per session (or logs it without a UI), and `/fabric status` lists the declared and refused foreground tools with each reason.
- A foreground tool keeps its program path: it stays callable as `extensions.<name>`. A direct model call runs Pi's `tool_call` hooks and Fabric's approval policy with the same ref (`extensions.<name>`), the same `capture.risks` class, and the same `approvals.actions` override as the program call.
- The declared foreground set is part of the prompt-cache fence beside active tool names, and the [tool placement query](providers.md#tool-placement-query) reports these tools as `model` and lists them in `programCallable`.

A compatible exact-name core override is an additive extension of its existing `pi.<name>` slot. In effective full-code execution (including Schema enforce mode, which treats execution as full-code even when `fullCodeMode` is false), the current override schema contributes a bounded, schema-derived object overload without replacing Fabric's built-in positional, bare-string, shorthand, or alias forms. Fabric keeps each slot's established normalized result contract (`string` for read-like tools and `{ ok, output, details }` for bash/edit/write). The registry still validates the normalized arguments authoritatively; Fabric does not prove that an override schema is a superset of the built-in schema. Schema enforce mode still applies its host gate: read-like core refs remain available, while protected mutations and external effects are blocked or must use the schema transaction path. An override's `promptSnippet` and `promptGuidelines`, when present, are appended as guidance for the corresponding `pi.<name>` identity and are not advertised as a second extension tool. Registration, replacement, reload, and removal are observed on the next execution and prompt build; no generated declaration or prompt state is persisted. Generated overloads widen the known numeric fields (`offset`, `limit`, `timeout`, `context`) to `number | string`, matching built-in runtime normalization; an override with a stricter numeric schema still rejects the string form at registry validation, so read the error and retry. Each generated overload takes a single object argument; the built-in two-argument signature such as `pi.read(args, options?)` remains available from the base slot unchanged.

## Approvals and risk

Fabric risk classes are `read`, `write`, `execute`, `network`, and `agent`. Approval policy values are `allow`, `ask`, `auto`, or `deny`. Policies cover actions invoked inside `fabric_exec` and top-level model-requested tools left on Pi's native path. Native calls keep Pi's original implementation, result shape, and renderer. Fabric adds only the supported interception hook that runs before execution. The native-path check starts with the session, before Fabric's runtime activates on first use, so the same direct call gets the same answer before and after the first `fabric_exec`. `/fabric settings` and `/fabric reload` are commands, not tool calls, so a restrictive policy can always be changed in-session.

- Captured and directly registered tools default to the conservative `execute` risk because Pi tool definitions do not declare effects. Add exact tool-name overrides under `capture.risks`. Fovea's verified graph-navigation tools (`fovea_sketch`, `fovea_focus`, `fovea_dwell`, and `fovea_impact`) are read-only exceptions that default to `read`.
- `capture.hideFromModel` and `capture.keepVisible` remain accepted legacy preferences, but cannot override exclusive full-code or Schema enforce mode. Native active tools stay registered/callable while their declarations are hidden.
- In orchestration-only mode, Pi's active set and each tool's native exposure govern model visibility.
- Extension tool names appear in the prompt as a names-only roster; descriptions and schemas are resolved on demand via `tools.list` / `tools.search` / `tools.describe` before first use.
- An `ask` policy emits a warning notification and opens an explicit **Allow once** / **Allow for this session** / **Deny** permission prompt. These options match Claude-style approval scopes. **Allow once** authorizes only the requested action. **Allow for this session** keeps that risk class authorized until the current Pi session ends. The TUI uses an inline wizard. RPC clients receive the equivalent `select` dialog.
- Fabric serializes concurrent requests so a one-time approval never silently widens to sibling calls. Session-wide grants apply to native calls and to `fabric_exec`. Escape, dismissal, unavailable interactive UI, and session restart all fail closed.
- `approvals.headless` (default `"deny"`) chooses what an approval does without an interactive UI. `"decision"` raises a durable user-held approval decision for `fabric_exec` actions and waits up to `approvals.headlessTimeoutMs` (default `300000`, bounded to 1 s..24 h). Only an explicit approve answer from `/fabric decisions` or `pi-fabric decisions answer` runs the action once; deny, cancel, expiry, and abort deny it. Any other value keeps `"deny"`. See [durable decisions](decisions.md#headless-approvals).

### Per-action approval overrides

`approvals.actions` overrides the risk-class mode for individual actions (default: none):

```json
{
  "approvals": {
    "write": "allow",
    "actions": {
      "delegate.*": "ask",
      "delegate.status": "allow",
      "pi.bash": "deny"
    }
  }
}
```

Keys are an exact ref (`provider.action`, including multi-segment refs such as `mcp.github.search`) or a provider wildcard (`provider.*`). Values are `allow`, `ask`, or `deny`. An exact key beats a wildcard, which beats the risk-class mode. Other wildcard forms, unknown values, and more than 256 entries are configuration errors; Fabric never drops them silently. Overrides apply to every approval path keyed by a ref: actions inside `fabric_exec`, top-level native tools (`pi.<tool>` for Pi built-ins, `extensions.<tool>` otherwise), and Jev program observation. `deny` is absolute: inherited child risk grants and **Allow for this session** cannot lift it. `allow` and `ask` replace the risk-class mode and otherwise keep its semantics, including session grants for `ask`. Speculative prefetch only considers refs whose effective mode is `allow`. Overrides do not bypass Schema enforce or other host gates.

### Auto approval mode

An `auto` policy sends each validated call and its prepared arguments to a separate Pi model or Jev classifier before invocation. Configure **Auto model** under `/fabric settings` → **Approvals**, or set the optional canonical `provider/model` key in `fabric.json`:

```json
{
  "approvals": {
    "model": "anthropic/claude-opus-4-6",
    "write": "auto",
    "execute": "auto",
    "network": "auto",
    "agent": "auto"
  }
}
```

Choose **Inherit** in the model picker to omit `approvals.model` and use the active Pi session model. Built-in and custom models dispatch through Pi's effective provider runtime, including providers with custom API identifiers. Older supported Pi versions fall back to their compatibility provider registry. Read access stays independently configurable, and most setups leave it at `allow`.

The classifier receives the exact action, bounded prepared arguments, cwd, user-message text, and assistant tool calls. Fabric excludes assistant prose and tool outputs, so model-authored reasoning and retrieved hostile content cannot directly instruct the classifier. The classifier has no executable tools and must return a structured `allow` or `escalate` verdict. An `allow` verdict applies only to that call. `escalate`, malformed output, missing authentication, timeout, cancellation, or any classifier error falls back to the explicit **Allow once** / **Allow for this session** / **Deny** prompt. Headless runs fail closed when that prompt cannot be shown. Fabric attaches classifier token usage and cost to the resulting `fabric_exec` or native tool result, and execution traces record each nested verdict as `fabric.approval.auto`.

`deny` stays deterministic and runs before the classifier. Schema enforcement, project trust, budgets, and other host gates remain authoritative. Auto mode is a model-based policy advisor and provides no stronger sandbox boundary. Its initial conservative policy escalates destructive or irreversible actions, shared/external/production changes, credential or sensitive-data exposure, safety bypasses, actions beyond explicit user intent, and actions whose safety is uncertain. Fabric adapts the policy architecture described in Claude Code's [permission modes](https://code.claude.com/docs/en/permission-modes), [auto-mode configuration](https://code.claude.com/docs/en/auto-mode-config), and Anthropic's [auto-mode engineering write-up](https://www.anthropic.com/engineering/claude-code-auto-mode), adapted to Pi's model registry and Fabric's existing per-risk policy gate.

### Jev as the auto-mode classifier

Select a Jev entry in **Approvals → Auto model** after `/login jev` (TypeSafe route), `/login openrouter` (OpenRouter route), or `/login vercel-ai-gateway` (Vercel AI Gateway route), or configure:

```json
{
  "approvals": {
    "model": "pi-fabric/typesafe/jev-latest",
    "write": "auto",
    "execute": "auto",
    "network": "auto",
    "agent": "auto"
  }
}
```

Jev is an auth-only provider, not a chat model. This picker offers `pi-fabric/typesafe/jev-latest`, `pi-fabric/typesafe/jev-1.13`, `pi-fabric/typesafe/jev-1.13.0`, and `pi-fabric/typesafe/jev-preview` only in the approvals list, plus the configured `jev.model` if different. It also offers OpenRouter-served `pi-fabric/openrouter/jev-latest` and `pi-fabric/openrouter/jev-1.13`, which reuse the existing `openrouter` credential (`/login openrouter`, `OPENROUTER_API_KEY`, or `TYPESAFE_OPENROUTER_API_KEY`); OpenRouter has no `jev-preview` alias and serves Jev on its Decisions API, not `/chat/completions`. Vercel AI Gateway-served `pi-fabric/vercel-ai-gateway/jev-latest` reuses the existing `vercel-ai-gateway` credential (`/login vercel-ai-gateway` or `AI_GATEWAY_API_KEY`) and resolves to `typesafe-ai/jev` on the gateway's TypeSafe-compatible endpoint. Legacy `jev/<model-id>` overrides normalize to `pi-fabric/typesafe/<model-id>` when loaded. `/login jev` credentials and raw TypeSafe request IDs (`jev-latest`, `jev-1.13`) are unchanged. **Inherit** still means the active Pi chat model, never Jev. Selecting Jev for approvals is independent of `jev.enabled`, which controls Fabric's Jev tool provider.

The host asks four typed Noul questions in one request: whether the exact action is safe to run without human approval, whether it touches secrets or sensitive data, whether it is destructive or irreversible without a user-named target, and whether it targets only artifacts this session created. Auto-allow requires the safety probability to be **at least `jev.autoApprovalThreshold` (default 0.50)** **and** the secrets and destructive probabilities to stay below 0.5; all four verdicts and the effective threshold are recorded with the decision. Selecting a Jev model reveals **Approvals → Jev minimum probability**, an editable number from 0 to 1 in both terminal and RPC settings. The setting persists in the selected global/project scope, remains saved when switching models, and applies only to Jev classification. For example, `"jev": { "autoApprovalThreshold": 0.95 }` requires a probability of at least 0.95. Missing, non-numeric, non-finite, or out-of-range configuration values use the 0.50 default; valid decimals and zero are preserved. Upgrading from the former fixed 0.99 cutoff uses 0.50 unless you explicitly configure another value.

Lower thresholds permit more actions. **0 allows every valid judgment whose secrets and destructive verdicts are clean**, while 1 requires a safety probability of 1; a secrets or destructive probability at or above 0.5 escalates regardless of threshold, and so do missing user text, malformed answers, and errors. Reasons report the four numeric judgments and the effective threshold, not generated explanations. This is a policy cutoff, **not a calibrated security guarantee**. Use `ask` or `deny` when a probabilistic advisor is inappropriate.

Jev receives the exact bounded arguments, the **latest user message and subsequent assistant tool calls**, and a bounded projection of earlier session actions - direct tool calls and nested Fabric actions with their arguments, host-recorded failures marked `"ok":false`. Earlier turns' prose, thinking, images, and tool outputs are excluded, so vague follow-ups cannot borrow authority from omitted history and retrieved content cannot instruct the classifier. Missing user text still requires explicit approval without inference. Oversized arguments, transcript clipping, and projection limits are disclosed to Jev as `evidence`/`session.truncated` facts without aborting classification (16,000 argument characters, 6,000 per user message/tool-call batch, 24,000 total evidence characters, 12,000 session-projection characters). Starting a new explicit user turn resets the conversational evidence window; the session-action projection spans the session.

Selecting this remote classifier authorizes sending that evidence to TypeSafe; it can contain private paths, code, or values from tool arguments. Do not select it for data that must stay local. Classification is a host-side request, not a recursive `jev.evaluate` tool call, so it does not recursively invoke the network approval policy. Normal tool permissions still apply after classification. Missing auth, malformed answers, HTTP errors, cancellation and timeouts never fall back to another model or auto-allow. They use the existing explicit approval flow (or deny in headless mode).

Authentication uses `/login jev`/`TYPESAFE_API_KEY` on the TypeSafe route, the existing `openrouter` credential (`/login openrouter`, `OPENROUTER_API_KEY`, or `TYPESAFE_OPENROUTER_API_KEY`) on the OpenRouter route, and the existing `vercel-ai-gateway` credential (`/login vercel-ai-gateway` or `AI_GATEWAY_API_KEY`) on the Vercel AI Gateway route, then trusted `jev.credentialCommand`; the command is resolved per classification and not cached across decisions. `jev.maxRequestBytes` and `jev.requestTimeoutMs` apply, with a 30-second classifier timeout ceiling and no automatic retries. Typed token usage is included in approval accounting. TypeSafe does not return prices: cost fields are zero/unpriced, **not evidence that inference is free**.

## Temporal retention

Fabric clears inactive run artifacts by age. It never truncates active JSONL files. The defaults are:

- `retention.orphanedTempRunMs`: reclaim a managed temporary run root six hours after a sweep **first notices** its owner is dead, provided its contents and descendant liveness can be verified. Live owners/descendants are preserved. Closed, shutdown-confirmed incomplete runs use the same grace from close.
- `retention.oneShotRunMs`: retain terminal one-shot agent run artifacts for 24 hours. An explicit `agents.cleanup()` may remove them sooner. Graceful shutdown with `agents.retainRuns: true` marks managed roots closed; empty roots are removed immediately. `retainRuns: false` requests deletion after child transports stop, including for managed temporary roots. Inherited nested roots belong to the enclosing agent run: a child manager stops its session children but leaves their terminal status and transcripts for that run's cleanup/retention or the root-session shutdown.
- `retention.actorRunArchiveMs`: retain terminal actor run archives for seven days. Fabric always preserves the latest run for each actor.
- `retention.terminalRunEventsAgeMs`: after 6 hours (configurable, one hour to one year), the existing actor archive and resident request sweeps compact safe terminal (`completed`, `failed`, `stopped`, `timed_out`) `events.jsonl` files. Queued/live/unknown runs, unresolved workers, unsafe trees and actor `lastRunId` references are untouched.
- `retention.terminalRunEventsMaxBytes`: retain at most 262144 bytes (256 KiB; configurable, 1 KiB to 16 MiB), including a JSON truncation marker and at most the last 200 complete trailing event lines. Compaction reads only a bounded suffix and writes atomically; it never changes `status.json`, reply/result files, or live recording. A single final event larger than the cap is dropped, so no invalid partial JSON is retained. Already bounded files are not rewritten. Ordinary actor `session.jsonl.<stamp>[-n].bak` rotation history keeps only the newest backup; malformed-session orphan backups remain recovery evidence.

Run housekeeping begins on actual agent storage use (not manager startup), continues during use, and runs best-effort on close. It never truncates live run JSONL or actor `session.jsonl` files. A `pi-durable` run's own `durable/` journal is a known run artifact and expires with its run, subject to the same custody checks. Existing actor archive expiry and residency cleanup still apply after compaction; result files are never compacted. Caller-owned run roots retain their existing explicit-cleanup semantics. Symlink roots/markers, wrong-uid files, malformed ownership, unknown contents, and unverifiable incomplete descendants are preserved. `/fabric settings` exposes these values under **Retention**. Changing them requires `/fabric reload`.

For a one-time **offline** mesh compaction, run `node dist/storage/retention-cli.js <mesh-root> --dry-run` (the default): it lists eligible paths and before/after bytes without creating or changing files. `--apply` performs the same compaction and backup pruning; normal expiry ages remain unchanged. The sweep requires existing Linux host flocks and proven-dead diagnostic holders, and skips live, legacy, unfenced or unreadable custody. Actors must belong to a fenced resident root; latest-run references and malformed-session recovery backups remain protected. On Windows, `--apply` deletes only with the bundled `dist/native/fabric-win-remove.exe` whose sha256 matches the digest built into the CLI: it checks each entry's owner SID, volume serial and file index on an open handle (and, for a file, its judged last-write time and size, with writes denied while it is held, so a file refreshed after the scan is kept), never follows a reparse point, and deletes by handle (smarty-dev#7858). Without it, `--apply` deletes nothing there.

Persistent actor hosts also start this CLI themselves, **once a day per mesh**, with `--apply --runs-older-than <retention.actorRunArchiveMs>`. Whichever host claims the day's slot file (`<mesh>/.mesh-retention-sweep-<day>.json`) runs it as a detached, niced process. Single flight is a kernel lock: the host takes an exclusive non-blocking `flock` on `<mesh>/.mesh-retention-sweep.lock` before it claims the slot, and hands the locked descriptor to the sweep, which holds it until it exits (a crash releases it). While a sweep holds the lock, no new sweep starts. If the lock state is uncertain (for example `flock` is missing), no sweep runs: the host logs one line and publishes one `fabric.alarm.retention-sweep-lock` event. Linux only; elsewhere the CLI refuses `--apply`.

### Temporary output and reader scratch

New model-output spills and shell logs use private directories with a versioned `.fabric-scratch.json` ownership marker. They expire 24 hours after completion; oldest eligible caches may be removed sooner above **128 MiB or 256 items**, pooled across these two classes. Completed output is protected for its first hour. These aggregate limits are **soft** while files are active/recent or cannot be safely attributed. Model-output artifacts remain complete (not truncated). Their links are temporary.

Shell hang tracking keeps a **1 MiB RAM tail per running command**, then releases it on finish. Completed handles are capped at **256** and expire after 24 hours on subsequent store access. Each shell log is capped at **8 MiB including notices**; it starts with the retained pre-spill tail, not necessarily the command's beginning. The returned notice and log header explicitly say this is bounded, **not a full-output archive**; reaching the disk cap appends a truncation notice. Further output continues to the normal shell consumer but not the log. PID files are retired on finish; logs remain subject to the cache policy. A disk write error also stops logging without interrupting command execution; bounded logs never promise completeness.

Reader checkpoints are lossless live state: they are **never pressure-evicted**. Dispose/finalization removes them normally. New marked scratch left by a killed host can be recovered only six hours after housekeeping first observes a dead owner; shell scratch additionally preserves a live recorded child PID. PID reuse and permission uncertainty preserve data. Sweeps are asynchronous, coalesced and throttled to once per minute on actual scratch allocation/close, with no idle startup scan. Nothing expires until a later storage use triggers housekeeping. Directory identity, file metadata and owner/child liveness are rechecked before removal; hardlinked files are excluded. `sweepScratch({ tempRoot, dryRun: true })` reports `eligible` and first-observed `orphaned` directories without deleting or updating markers.

**Conservative recovery limits:** legacy unmarked output/shell/checkpoint artifacts are not automatically deleted. Shared recursion budget ledgers lack descendant ownership leases, and temporary actor roots may contain shared/adopted work; crash orphans of those two classes are deliberately left alone, not deleted merely because a parent PID is dead or old. Normal owned-budget/ephemeral-actor close cleanup remains in place (budget initialization failures now remove their partial allocation). Persistent/caller-owned actor roots are not cache sweep targets. Unverifiable legacy incomplete runs, unknown files, malformed markers, and symlinks likewise require an operator's ownership/liveness review; do not use a broad prefix deletion.

## Agents

`agents.runner` selects the default harness: `"pi"` (default in this fork, the Pi CLI), `"pi-durable"` ([isolated durable Pi host](durable-pi.md)), `"claude"`, `"veda"`, or the id of a runner registered through `pi-fabric/runners` ([custom runners](agents.md#custom-runners)). A well-formed id that no extension registers is kept, and launches fail closed until the runner is registered. Before the first turn of each session, Fabric warns once when the configured runner is still unregistered, suggests a close built-in or registered id for likely typos, and lists the registered runners. Malformed ids fall back to `"pi"`. Explicit existing runner settings are preserved. `agents.model` is the optional Pi `provider/id` override. `agents.claude.model` is the optional canonical Claude runtime key. `agents.claude.binary` defaults to `claude`. You can supply an absolute path or a wrapper. `PI_FABRIC_CLAUDE_BINARY` overrides it for the current process. `/fabric settings` enumerates Claude models from that binary in the background and stores the two runner defaults independently. `agents.modelAdmission` defaults to `strict`: a Pi child run fails when the model it reports after selection differs from the requested key. Set it to `permissive` when a virtual provider key (for example a `pi-multiprovider` entry) resolves to a concrete backend at stream time; Fabric then records the reported attribution and continues, including when a later assistant frame names that backend. Permissive admission still fails a child that reports no model or starts work before admission.

The `veda` runner drives the [Veda CLI](https://github.com/kennyfrc/veda) as the child harness. `agents.veda.binary` defaults to `veda`. An absolute path or wrapper works, and `PI_FABRIC_VEDA_BINARY` overrides it for the current process. `agents.veda.backend` selects which backend Veda wraps: `agy` (Antigravity CLI, the default), `codex`, `claude-code`, `droid`, `pi`, or another backend registered by the installed Veda build. Fabric passes this value through unchanged and never hardcodes AGY. `agents.veda.model` is an optional backend-specific model or Veda alias. With no active host model policy, omitting it lets Veda select its own backend default. Under a nonempty host `agents.deniedModels` policy, Veda requires `agents.veda.backend: "pi"` and an exact concrete `provider/model` resolved by the Pi registry; bare IDs, aliases, unknown selectors and backend defaults are refused before admission. Claude aliases are admitted only after the native CLI catalog establishes an allowed `resolvedModel`. `agents.veda.persona` picks the global Veda persona: `navigator-plan`, `navigator-chat` (default), `reviewer`, `worker`, or a custom persona under `~/.config/veda/personas/<name>/AGENTS.md`. Per-run selection overrides it through `agents.run({ persona })`. You can also edit the Veda backend, persona, and model in the Fabric settings panel under Agents. Each child runs one headless `veda --json` prompt with an isolated `fabric-<run-id>` session, so parallel children never share Veda selection or conversation state. Veda sessions lack persistence, and steering is unsupported. Veda children are **not** recursively Fabric-equipped (`recursive: true` is rejected), and they cannot back persistent actors.

A JS runtime launches each Fabric worker module. Fabric reuses the current runtime when `process.execPath` names `node` or `bun`. For a Bun-compiled Pi binary, `process.execPath` names the `pi` executable. Fabric then uses `PI_FABRIC_NODE_BINARY` or the first `node` or `bun` on `PATH`. The resolved runtime launches the workers. `PI_FABRIC_NODE_BINARY` overrides this choice for the current process. The Node-process executor (`executor.runtime: "node-process"`) requires Node.js because it uses `--eval` and `--input-type=module`; the Bun-process executor (`executor.runtime: "bun-process"`) requires Bun because it uses `--eval`.

Pi worker tool-call streams have an independent stall guard: a consecutive interval of only whitespace argument deltas is bounded to **90 seconds**, even when tokens keep arriving. Any non-whitespace argument delta resets that interval; text/thinking deltas and tool execution are unaffected. `PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS` overrides the interval in the worker environment (positive integer milliseconds, at most `2147483647`; invalid values use 90000). The existing 64 KiB all-whitespace argument-prefix cap remains; real JSON content exempts only that byte cap, not the time bound.

On the first stall, the worker aborts and drains its owned Pi child, then retries once from the **exact durable session** with the same admitted model and effort, not the original task or a fallback model. A second stall fails with `errorCode: "RUNAWAY_TOOL_CALL_STREAM"` and an explicit repeated-stall error. A missing durable session/model admission or active compaction fails safely and does not replay completed work. Both attempts emit `stall.whitespace-toolcall` in the event log and lifecycle telemetry with `taskId`/`runId`. The overall child timeout and failed-provider recovery limits still apply.

Workers require Node.js 24+ (or Bun) and report an explicit startup failure on older Node versions. Pi-managed installs intentionally omit physical host peers such as `typebox`; do not repair this by installing all Pi peers. Fabric ships a private, self-contained worker validator while keeping the extension's TypeBox host-mapped. Missing worker dependencies or bootstrap modules are reported in the run status before any model call. The process transport also retains a bounded stderr tail for failures that cannot write a status record.

Other agent settings:

- `router`: optional **host-only** external spawn/actor model selection hook: argv `command` (absolute executable path, minimal `PATH`/`HOME`/`LANG`/`TZ` environment plus fixed Windows `SystemRoot`/`COMSPEC`), `timeoutMs` (1500 by default, clamped 200–5000), `mode` (`off`/`shadow`/`enforce`, default `off`), and `includeTask` (default `false`). Explicit caller model/thinking bypasses it; failures keep static defaults. See [router command contract, privacy, decisions and rollback](spawn-router.md).

- `modelPolicy.requireReason`: trusted-host-only model prefixes requiring a named `modelReason` on explicit `agents.spawn`, `run`, `create` and actor `setModel` requests. Defaults to `["gpt-6-astra", "opus", "claude"]` (any provider); provider-qualified entries restrict that provider/model prefix. `opusReason` is accepted as the request-level alias for `modelReason`; the config API is unchanged. Reasons must be non-blank and at most 200 characters. For a Claude model or the `claude` runner, the reason must name an issue (`#1234`) or contain an issue URL. Explicit protected selections refuse without that reason; indirect/default resolution may use the configured fallback instead. Omitted models/defaults are otherwise unchanged. Workspace configuration cannot override this list. Set `[]` in the root-owned `/etc/smarty/fabric-policy.json` to roll back; agent-dir values only add requirements once that file is provisioned. See [root-owned host policy](#root-owned-host-policy) for the missing-file rollout window and [explicit model exceptions](agents.md#explicit-model-exceptions-3134).
- `modelPolicy.fallbackModel` is optional and host/profile-only; when set, it is the model used for an indirect fallback, paired with `fallbackThinking` (default `high`). It is not a package-wide default. If no usable configured fallback exists, an inherited Claude binding is refused with `agents.modelPolicy.fallbackModel` named. Fabric does not silently select an uninstalled provider. Successful fallback receipts/results identify the transition as `model: inherited <old> -> <new> <thinking> (#11863)`. Allowed exception and inherited-switch records append one JSON object to `~/.local/state/smarty/route/opus-exceptions.jsonl` using one `O_APPEND` write: `{ts, role, model, ref, reason, launcher, host}` where `ts` is ISO UTC, `role` is `task` or `actor:<name>`, `model` is the original Claude model, `ref` is an issue reference or `null`, `reason` is supplied text or `inherited-switch`, `launcher` is `fabric`, and `host` is the host name. A logging failure is surfaced but does not block the run.

- `thinking`: default reasoning effort (`off`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`), default `medium`.
- `maxConcurrent`: child concurrency semaphore. A session spawn beyond this limit returns a `queued` handle immediately. Queued children start in FIFO order as slots become free; `queuePosition` in list/status is one-based. Queued receipts are session-only for now: saturated durable spawns cancel their queue entry, safely reject, and return no queued handle.
- `maxPerExecution`: hard cap on accepted child launches per `fabric_exec` invocation, including queued spawns. Cancelling a queued child does not refund that invocation's launch count.
- `maxDepth`: nesting bound for child agent calls, including `rlm.query()`. It accepts any non-negative safe integer. A value of `0` disables child spawning. `/fabric settings` provides free-form numeric entry.
- `timeoutMs`: default wall-clock budget per child and the floor for per-call overrides (24 hours by default, which is also the policy ceiling). Fabric ignores lower per-call values. The default matches the ceiling on purpose: an orchestration program inherits this value as its own whole-program deadline floor, so a lower default would cut a long participant short well inside the allowed maximum. Lower it to bound a class of runs, and raise a single run with a per-call value.
- `extensions`: whether children load the user's normal extensions (Pi) or Claude Code customizations (Claude). Pi children with `false` use only built-in or `models.json` model definitions, not extension-registered providers; see [third-party model providers](#third-party-model-providers).
- `defaultTools`: the default tool allowlist for children.
- `budgetUsd`: shared append-only cost ledger across a recursion tree (0 disables).
- `maxTokensPerChild`: cumulative token bound per child (0 disables).
- `nice`: Unix niceness 0-19 for every child agent: task agents, actors (supervisors, review agents) and durable resident children. The default, `0`, leaves priority unchanged. Fabric calls `os.setPriority(child, nice)` right after the spawn. On Linux it also sets best-effort IO priority with `ionice -c2 -n7`, when `ionice` is installed. The child's own tools (its bash commands) inherit both on Linux and macOS. On Windows `os.setPriority` maps to a priority class. A failure is logged once per worker in the run's `events.jsonl` (`fabric_priority_error`) and never stops the run. Values outside 0-19 are clamped; there is no environment override. A per-call `nice` on `agents.run`/`agents.spawn`, actor creation or `agents.setNice` can only raise it.
- `deadRootFilter`: **host-only** skip of durable-actor activations whose owning root is positively dead (smarty-dev#6062). `mode`: `"off"` (default) or `"on"`; `exempt`: actor ids, id prefixes or exact names that always run. Fail-open: only an expired-for-over-10-minutes root lease with no live participant skips. See [agents](agents.md#dead-root-activation-filter).
- `stuckPreparingMs`: **host-only** threshold in ms (default `600000`, ten minutes; clamped to 1 s to 7 days). An activation hung in preparation never fails, so the three-failure notice cannot fire for it (smarty-dev#6337). The resident host's maintenance tick (at most every 10 s) therefore tells the actor's root Main once per stuck episode: `Fabric host notice: actor NAME stuck in preparing for N min (since T); its queued events are waiting.` Preparation time adds up across phases; a permit wait (`waiting`) does not count. The episode ends when the activation leaves preparing. Each notice appends one line to `<mesh>/metrics/stuck-preparing.jsonl` (`stuckMs`, `waitedMs`, and `queued` including the overflow backlog).
- `notifyOnComplete`: show concise detached `agents.spawn()` completion notices and batch unread results for Main at a safe tool-turn boundary (or wake idle Main). `wait`/`join` and terminal `status` retract pending notifications; running/UI status does not. Escape/error parks results until new input.
- `processSlice`: optional **root-policy-only** Linux systemd user slice for process-transport workers, for example `"batch.slice"`. Unset by default; configure it in `/etc/smarty/fabric-policy.json`. Agent-dir and workspace configuration cannot enable or override it; other platforms keep direct launches. When enabled, Fabric uses `systemd-run --user --scope --slice=<slice> --quiet --collect -- <worker command>`. Scope admission and the worker execute in place, preserving worker PID/PGID and the existing exit/custody fences. If the executable is unavailable or admission fails, Fabric logs one warning per transport instance and launches directly, only after the failed attempt confirms native close; an already-admitted worker is never replayed. Admission is bounded to five seconds; an unconfirmed teardown vetoes fallback. This changes placement only: configure CPU/IO limits on the slice separately. With an explicit slice, process **tasks** also receive the memory cap below whenever the host passes the delegation check. Actors never do.
- `processMemoryMaxBytes`: **root-policy-only** byte limit for each process **task** in its own transient scope (smarty-dev#8257). The default is `24 * 1024 ** 3` (24 GiB) and swap is capped at 0. Root policy must give a positive safe integer; anything else fails validation. A task is eligible only on Linux when all of these hold: cgroup v2 is unified, the explicit user-bus environment is usable (`$XDG_RUNTIME_DIR/bus` or a `unix:path=` `DBUS_SESSION_BUS_ADDRESS` socket), the user manager's `cgroup.subtree_control` delegates `memory`, and `systemd-run` exists. These are file checks only; Fabric runs no D-Bus command. Eligible tasks use `processSlice`, or `app.slice` when it is unset. Ineligible hosts, scrubbed environments such as CI, and actors keep the legacy launch exactly. Without an explicit `processSlice` they do not run `systemd-run`, and an explicit slice still yields an uncapped scope. Task environments drop any inherited `FABRIC_TASK_MEMORY_MAX`/`FABRIC_TASK_CGROUP`/`FABRIC_TASK_SYSTEMD_RUN_WRAPPER`. The launcher exports them only after it admits a capped receipt, so an uncapped fallback cannot claim a cap. A capped task also gets a task-local `systemd-run` shim first on `PATH`. The shim runs `--user --scope` commands in place inside the capped leaf, and `MemoryMax=` there can only lower the whole task's cap. See [agents](agents.md#task-local-systemd-run). The capped cgroup path appears as `cgroup` on the run record and handle.
- `instructionsRoot`: host-only allowed root for actor `instructionsFile` inputs (unset defaults to `~/.local/share/smarty-dev/factory/current/`). Configure it in the host agent directory's `fabric.json`; workspace config cannot widen or replace it. Main and resident owners realpath the root at first use, so `current` may point to an installed factory generation.
- `wakeText`: optional **host-only**, default off: `{ "receiptDb": "/abs/path/ingress.sqlite", "maxChars": 2000, "trustedPublishers": ["session:<forwarder session id>"], "repositories": { "<actor id>": ["owner/repo"] } }` (smarty-dev#6144). The mesh carries a projection of a GitHub webhook, never the comment or review body. When set, an actor activation for a projected `github.webhook` mesh event (`issue_comment`, `pull_request_review`, `pull_request_review_comment`, with `data.payloadProjected`, `data.sequence`, `data.repository`, the GitHub delivery id `data.id`, `data.digest` and `data.storeId`) reads that receipt from the factory ingress SQLite file on the same host, read-only (`node:sqlite`, `SELECT repository, event, id, digest, payload FROM deliveries WHERE sequence = ? AND id = ?` plus the `metadata` `store_id`). Any mesh publisher can send such an envelope, so hydration fails closed unless all of these hold: (1) provenance: the event's sender as the mesh store recorded it (`event.from.id`, stamped from the publisher's own runtime identity, never from the payload; `verification` `"mesh"`, so not bridge-relayed) is listed in `trustedPublishers`; (2) identity: the row's delivery id, digest, repository and event and the store's `store_id` equal the envelope's; (3) repository: the receipt's full `owner/repository` equals the envelope's exactly and the GitHub payload's `repository.full_name` agrees, the topic names it (`github.<repository name, lowercased>` or `github.<name>.<suffix>`, the forwarder's convention), the actor subscribes to that topic (an addressed delivery alone is not enough), and the full `owner/repository` is on that actor's entry in `repositories`. The topic carries no owner (`acme/demo` and `other/demo` are both `github.demo`), so the topic never authorizes on its own. `repositories` maps an exact actor ID (the 32 lowercase hex characters `agents.actorStatus` returns) to full `owner/repo` names (case-insensitive; bare names, wildcards and malformed entries are dropped; at most 64 actors and 32 repositories each) and defaults to none: an actor without an entry gets no text. Only the actor ID authorizes: actor names are not unique across sessions, projects and roots, so a key that is not an actor ID (an actor name included) is dropped and a namesake actor never inherits a grant. `trustedPublishers` holds exact sender IDs (no wildcards, at most 16) and defaults to none: the live forwarder (`smarty-factory-host@github-factory`, `pi --mode rpc --no-session`) publishes as `session:<uuid>` with a new UUID at each start, so there is no stable identity to trust by default; pin the forwarder's session (for example `pi --session-id <uuid>`) and list `session:<uuid>`. It keeps the first `maxChars` characters of the body (at most 2,000), the author login and the author association, and appends them to the activation input as one fenced JSON line between `UNTRUSTED_WAKE_TEXT_DATA_JSON` and `END_UNTRUSTED_WAKE_TEXT_DATA_JSON` (untrusted data; the body cannot spell a marker or break a line). The same object is visible to activation filter paths as `wakeText.*` and to `validWhile` facts as `wakeText`. Nothing is published, persisted in the actor queue or fetched from GitHub. An untrusted sender, a missing or unreadable database, a missing or mismatched row, or any other error gives no text, and the activation runs as before. The policy is read at every activation, not captured at start: a config reload that enables, narrows or removes it applies to the next activation in Main and in its resident host (Main rewrites the host's `config.json` on reload), so removal revokes at once without a restart; if the resident host cannot read an accepted `config.json`, it hydrates nothing and does not fall back to its startup policy. Workspace configuration cannot set it.
- `sessionExport`: export each agent run's usage as an attributed pi-format session file (on by default).
- `sessionExportDir`: override the export store root. The default is pi's agent dir: `PI_CODING_AGENT_DIR` when set, else `~/.pi/agent`. `PI_FABRIC_AGENT_DIR` takes precedence over both.
- `worktree.setup`: optional shell command (`/bin/sh`, or `cmd.exe` on Windows) run in each new `worktree: true` checkout before the child starts (unset by default; blank values are ignored). A per-request `worktreeSetup` overrides it, and a non-zero exit fails the launch. See [worktree results and setup](agents.md#transports).
- `childQuestions`: `"cancel"` (default) cancels every dialog a Pi child opens; `"route"` forwards it to the parent's UI, or to a root-held [decision](decisions.md#routed-child-questions) when the parent has no UI.
- `childQuestionTimeoutMs`: default deadline for a routed child dialog (default `600000`, bounded to 1 s..24 h); a dialog's own timeout wins.

### Usage tracking with external tools

Fabric children run with `--no-session`, so token trackers that scrape session files (tokscale, ccusage, …) cannot see subagent token usage or cost. With `sessionExport` enabled (the default), every child writes one usage-only session file (tokens and cost, never transcript content) to:

```text
<pi agent dir>/sessions/.fabric/<encoded-cwd>/<run>.jsonl
```

`<pi agent dir>` is the parent's profile: `PI_CODING_AGENT_DIR` when set, else `~/.pi/agent`. These files record usage only. They are not the child's Pi session: the child runs on the parent's profile and settings.

Fabric attributes each file through a `session_info` marker (`fabricagent-<name>`). This placement works because tokscale and ccusage walk pi's session store recursively, and pi's own resume picker reads only its immediate `<encoded-cwd>` directory. **Both trackers count Fabric subagents with zero configuration, and pi's session UI never lists these files**. The exported sessions behave like a co-hosted namespace inside pi's store.

- **tokscale**: counted under the Pi client automatically. A small dedicated-client patch (senpi-style, pointing at `~/.pi/agent/sessions/.fabric`) turns it into a separate "Pi Fabric" row with per-`fabricagent-*` attribution.
- **ccusage**: counted in the default pi footprint automatically (`ccusage daily`, `ccusage pi …`). For an ad-hoc Fabric-only view, run `ccusage pi daily --pi-path ~/.pi/agent/sessions/.fabric`.
- **Isolated store**: to keep usage files fully outside pi's store, set `agents.sessionExportDir` (or `PI_FABRIC_AGENT_DIR`) to `~/.pi-fabric/agent`, then register a ccusage named store for a dedicated `fabric` agent section:

  ```json
  { "pi": { "stores": [ { "name": "fabric", "path": "~/.pi-fabric/agent/sessions/.fabric" } ] } }
  ```

  ccusage's double-count guard rejects a named store that overlaps the default pi store, so the isolated-row form requires the separate directory.

See [agents, actors & mesh](agents.md) for the runner and transport details.

## MCP

### Opt-in Pi-owned servers

By default Fabric still uses mcporter. To borrow specific servers already configured in Pi's `mcp.json`, set exact server names in Fabric configuration, or use **/fabric settings → MCP → Pi-owned servers**:

```json
{ "mcp": { "nativeServers": ["github", "project-docs"] } }
```

Run `/fabric reload` after changing this selection. Configuring or enabling a native server itself remains Pi's job (`/mcp`, `pi mcp`, and `/reload`). SDK hosts must supply Pi's `createMcpExtension()` and bind extensions; Fabric does not install it or start another native client. No existing MCP configuration or credentials are migrated. An empty list restores the default ownership.

Selected servers keep Fabric's `mcp.<server>.<tool>` names, sanitizer aliases, discovery, static argument checking, normalized `{ text, content, structuredContent }` results, and `display: { name, description }` execution metadata. Raw MCP identity comes from Pi's namespace/label metadata, not its potentially hashed tool identifier. Tool descriptions and input schemas come from the live native registration; Pi's `CallToolResult` output-schema envelope is unwrapped. Schema differences invalidate old normal-form plans; Fabric does not conceal them. Other servers keep the existing mcporter behavior. Selected tools are advertised once under `mcp.*`; existing `extensions.mcp__...` references remain callable when extension capture is enabled, but are not repeated in discovery or the extension roster.

Both Fabric policy and Pi's tool middleware apply. Native redactions, progress, nested usage and cancellation remain authoritative; one Fabric invocation emits one Fabric trace operation. Read-only MCP annotations do not change Fabric's conservative network risk. Pi-owned tools remain subject to hidden/deferred exposure and child tool allowlists. A missing, withdrawn, blocked or failed native tool never falls back to mcporter, even if mcporter has a server with the same name. Fabric does not connect or revalidate that duplicate. Ambiguous sanitized names require exact names via `mcp.call` (an exact legacy server name retains precedence over a native alias). Resolving a native server alias may load mcporter's configuration names to check ambiguity, but does not connect those servers.

Fabric still owns its call deadline. Reload/shutdown cancels its borrowed calls without disconnecting Pi's shared servers. `mcp.servers()` includes selected native servers with `transport: "pi"` (an ownership marker, not a wire protocol), tool counts, and `stale: true` when no callable tools are currently registered. `mcp.reload()` only reloads mcporter; it does not reconnect Pi servers. `mcp.register()` cannot replace a Pi-owned name. Native resource tools remain on Pi's existing captured surface; they are not renamed into per-server tool namespaces.

Pi owns native startup connections and OAuth; this option does not make Pi's own session-start connections lazy. Fabric adds no connection work to registration or idle hooks. Use `/mcp login` for native authentication. To prevent *Pi itself* from also connecting a legacy-owned server, disable/remove its entry in Pi's MCP configuration; Fabric never changes that user setting.

### mcporter and shared options

- `mcp.nativeServers`: exact Pi-owned server names, default `[]`. Invalid selections fail without changing ownership.
- `mcp.disableOAuth`: for mcporter, calls may use cached credentials but cannot launch new interactive OAuth flows. Pi-owned servers use Pi's authentication settings instead.
- `mcp.callTimeoutMs`: per-call timeout bound.
- `mcp.allowDynamicServers`: permit `mcp.register()` of ephemeral servers.
- `mcp.enabled`: set to `false` to disable the MCP surface.

Fabric keeps a per-project MCP descriptor cache at `.pi/fabric/mcp-cache.json`. The cache uses the same config layers as [mcporter](https://github.com/openclaw/mcporter): global settings from `~/.mcporter/mcporter.json` and project settings from `config/mcporter.json`. Tool discovery (`tools.list`/`search`/`catalog`) reads these cached descriptors. Sessions reuse them while the config stays equal. Config state alone controls validity. Per-server definition hashes preserve entries when another server changes. Whitespace-only edits also keep the entries valid.

Fabric handles staleness in stale-while-revalidate style. Sessions adopt the cache instantly and re-list servers in the background per policy. When a server fails, its last-known tools stay available, marked `stale` in `mcp.$servers`. Fabric always re-lists a server the first time a call connects to it.

- `mcp.cache.enabled`: turn the descriptor cache on (default: true). When false, discovery lists tools live with a 60s in-memory TTL, matching the pre-cache behavior.
- `mcp.cache.revalidate`: background re-listing scope at session start, one of `"changed"` (only added or reconfigured servers, the default), `"all"`, or `"off"` (explicit `tools.list({ provider: "mcp", namespace })` probes still fetch exactly that server).
- `mcp.cache.revalidateBudgetMs`: wall-clock budget for one background revalidation pass (default 60000). A leftover queue tail restarts with a fresh budget.
- `mcp.jev.semanticSearch`: opt-in Jev ranking for `tools.search({ query, searchMode: "semantic" })` (default false). Default `tools.search` stays local and lexical. Enable it under **/fabric settings → MCP → Jev semantic search**.
- `mcp.jev.blockedServers`: MCP servers whose tool metadata must not be sent to Jev. Empty (the default) allows every server, including ones that are not cached yet. **/fabric settings → MCP → Block from Jev** lists cached servers so you can opt individual ones out.
- `mcp.jev.semanticCandidateLimit`: max tools sent to Jev (2–127, default 127). Half the slots are lexical hits; the rest recover tools the query would not name.
- `mcp.jev.semanticMinProbability`: minimum head probability to accept a match (0–1, default 0.2). Below that, or if Jev chooses `none`, search abstains. Timeout, rate-limit, and 5xx responses fall back to lexical ranking and mark `backend.degraded`.

See the [TypeScript MCP reference](../skillsets/typescript/fabric-exec/references/mcp.md) or [Python MCP reference](../skillsets/python/fabric-exec/references/mcp.md) for the selected call surface.

## UI

- `ui.widget` is `auto`, `always`, or `hidden`. `auto` shows active or retained Fabric runs and worker activity. Rows list agents from this session's lineage (agents owned by other sessions of the same project stay out of the widget) plus actor workers. Completed agents remain listed after their run ends, newest first, until the row budget or an explicit dismissal retires them. Their recent nested tools appear beneath them when enabled.
- `ui.refreshMs` defaults to `500`. Refreshes are driven by activity, actor, agent, shell, and the existing actor monitor's mesh events, not by a recurring dashboard poll. Widget notifications coalesce over at most `100` ms (or a smaller `ui.refreshMs`); a visible dashboard coalesces bursts at a minimum interval of `max(1000, ui.refreshMs)` ms. A hidden dashboard or non-TUI session has no controller poll timer. If the widget is also hidden and no transcript is open, notifications do not queue refresh timers either. Visible widgets redraw only on events and omit live elapsed labels (completed work keeps its fixed duration); there is no elapsed clock timer. Recent completed-shell rows expire on the next observed event; no timer expires them. Visible conversation transcripts watch only their active source files. Quiet external state or lease changes become visible on the next observed event or when the view is reopened; there is no heartbeat polling.
- `ui.maxRows` defaults to `6` and clamps the widget to `1..20` rows. The effective budget is also bounded by half the live terminal height, so a short pane or a tmux split cannot let the animated box fill the viewport and keep pi's scroll region moving under the editor. Rows beyond the budget collapse into a dim `+N` marker on the last line.
- `ui.showAgentToolPreview` defaults to `true` and controls the child-agent and actor tool rows in both the parent `fabric_exec` card and the widget. Recursive agents render their full descendant tree, bounded by the preview depth/node budget. The version 2 config migration renamed this key from `ui.showNestedToolCalls`.
- `ui.toolDisplay` is `"compact"` (default) or `"full"`. Compact elevates the declared display name and description and keeps bounded nested tool detail visible; full retains the outer Fabric program transcript. Pi's tool-expand keybinding (`ctrl+o` by default) expands a compact card to the full transcript and collapses it again. Invalid values fall back to `"compact"`. If configuration fails to load, rendering falls back to full so a degraded startup never hides the transcript. Change it under `/fabric settings` → **UI**; successful changes apply immediately to live and completed cards.
- `ui.principalView` is `"auto"` (default), `"on"`, or `"off"`. Auto enables for `org` and `org-agent` instances: `PI_FABRIC_ROLE` takes precedence over `SMARTY_ROLE`, with the `@stamp` suffix removed (`bin/smarty-role` exports `SMARTY_ROLE=org-agent@SHA`). Change it with **`/principal-view [on|off|auto]`**, **Ctrl+Alt+P**, or `/fabric settings` → **UI** → **Principal view**. No command argument toggles on/off. Commands persist to project `fabric.json` when trusted, otherwise the isolated agent directory's global `fabric.json`. On shows incoming agent/actor/mail chatter as one dim `↳ sender: body preview` line (~80 body characters), hides delivered inbox shadows, and collapses tool output using Pi's public UI API. **Ctrl+O** temporarily expands incoming/tool details. Off restores full native incoming rendering and the tool expansion state from before entering principal view. The old `ui.incomingMessages` preference is still read per config layer (`collapsed` → `on`, `expanded` → `off`) unless that layer specifies `principalView`; it is no longer a second settings row. Model context and assistant replies remain unchanged. Thinking visibility and native user-message styling are left alone because this Pi version exposes no display setter for them. See [Principal view](principal-view.md) for exact feed parity and the smallest proposed Pi-fork rendering hook.
- `ui.updateDebounceMs` defaults to `100`. It applies one execution-wide coalescing interval to every live `fabric_exec` card update: nested calls, progress text, and agent tool previews. Continuous streams emit at most once per interval, so a long call no longer postpones every render until completion. Set it to `0` to emit every update. Accepted values clamp to `0..2000`. The version 3 config migration renamed this key from `ui.nestedToolDebounceMs`.
- The widget renders above the chat, like `pi-supervisor`. Set `ui.enabled` to `false` to disable both the widget and the dashboard controller.

See the [interface reference](interface.md).

## Mesh

Mesh data lives at `<project>/.pi/fabric/mesh` by default. Set `mesh.root` to a relative or absolute path to relocate durable topics, shared state, and actor sessions. Add `.pi/fabric/mesh/` to the project's ignore file unless you version the coordination log on purpose. Set `mesh.enabled` to `false` to disable both mesh actions and ambient actor restoration.

Sessions that share one `mesh.root` share one participant directory, so each sees the others through `agents.sessions()` and can `steer` or `followUp` them. A Main normally joins that directory when it first uses Fabric. Set `mesh.announce` to `true` in the project configuration to join at session start instead, so an idle peer is reachable. Announcing loads the Fabric runtime during startup, so avoid it in a global configuration that applies to every project.

`mesh.stateBackend` selects where keyed mesh state lives: `"file"` (the default, `state.json` behind the mesh
`.lock`), `"shadow"` (`state.json` stays the authority; committed values are also mirrored to
`<mesh>/state-shadow/state.db` for divergence checks, and a SQLite failure never fails a write) or `"sqlite"`
(`<mesh>/state.db`, SQLite WAL, no mesh `.lock` for state; see `src/mesh/state-backend.ts`). The environment
variable `PI_FABRIC_MESH_STATE_BACKEND` overrides it. A root on a network filesystem or a runtime without
`node:sqlite` uses `"file"`. Switching an existing mesh to `"sqlite"` needs the migration of smarty-dev#6477 (L4); `fabric-mesh-backend cutover` is
fenced only on the mesh `.lock` plus `custody.lock`; its writer census is advisory only (printed as
`advisory: N writers, M unknown`) and never a cutover gate (smarty-dev#6982). Do not set it by hand on a shared mesh.

`mesh.lockProtocol` accepts only numeric `1`, `2` or `3` and defaults to `1`. It is captured
when each mesh store is constructed; editing configuration does not switch an existing
store. Protocol 1 keeps the B68 canonical-directory mkdir and three-line token/PID/time
wire without an incarnation: if a dead holder's PID is reused by a live process, v1
protects the receipt and can time out until trusted repair after fencing all writers/cleaners.
It publishes its owner exclusively and verifies the canonical directory/record
before entering the critical section. An initializer whose canonical directory has been
replaced aborts with `FABRIC_MESH_LOCK_OWNERSHIP_LOST` and never overwrites a successor.
Protocol 2 uses fully initialized private-directory publication. Both require the complete
owner record to match and detach the owned directory before recursive release. Recovery
requires a complete recorded owner and proof that its PID is absent (native `ESRCH`), or
that its native incarnation differs. An empty directory with no owner record is stale
strictly after the 30-second grace (source-level `staleLockMs`). Recovery uses atomic
empty-directory removal, not rename or recursive deletion: an initializer that publishes
an owner before removal prevents it, even if the recoverer paused after its last check.
Fresh ownerless directories, empty/torn/corrupt owner files and nonempty unrecorded
directories fail closed; recorded live owners never expire. Unrecoverable unrecorded
orphans need trusted repair after all possible writers/cleaners are fenced out. Immediate
proven-dead-holder recovery, retained recovery receipts and bounded jitter/backoff remain.

Protocol 3 (smarty-dev#9569) is protocol 2 plus the holder's PID namespace on its own owner line:
`linux:<machine-id>:<boot_id>:<pid-ns inode>`, `darwin:<IOPlatformUUID>:<bootsessionuuid>:host` or
`win32:<MachineGuid>:<boot ticks>:host`. The mesh `.lock` and `custody.lock` (plain and host-qualified) write it.

- **Why:** a container, VM or other host sharing a mesh root sees ESRCH, or an unrelated process, for a LIVE holder
  in another namespace. Reclaiming that holder lets two writers into the critical section.
- **Pid evidence only in our own namespace:** a reclaimer uses pid evidence (ESRCH, another incarnation) only for a
  holder of ITS OWN namespace.
- **No earlier-boot inference:** another boot id with the same machine id is NOT taken as proof of death (two running
  VM clones look exactly like that), so it is unproven (pi-fabric#923).
- **Precondition: a mesh root lives on ONE host's local filesystem** (pi-fabric#923 round 3). A live VM snapshot clone
  can reproduce the whole namespace id, but two clones can share a root only over shared storage. So pid evidence is
  used only when the root is on a known local filesystem:
  - **Linux:** `statfs` type ext2/3/4, xfs, btrfs, tmpfs, zfs or f2fs, read through the held root fd
    (`/proc/self/fd/N`). overlay is not on the list: its backing filesystem is not checked.
  - **Not detected: one disk attached to two hosts at once** (a multi-attach block volume, a shared SAN LUN) and
    mounted with one of those filesystems. The type looks local, but two hosts share the root. Such a mount is
    unsupported (these filesystems are corrupted by it anyway). It is an accepted security gap, smarty-dev#9813
    (fabric-store, 2026-10-11), whose holder-lease design closes it.
  - **Anything else is unproven:** overlay, nfs, cifs/smb, 9p, virtiofs, fuse, ceph, gfs2, ocfs2, an unknown or
    unreadable type.
  - **macOS fails closed:** Node exposes only `statfs`'s numeric `f_type`, which macOS does not keep stable. A crashed
    holder there needs `--unsafe-holder-stopped`.
  - **Windows fails closed** (below).
- **Never reclaimed automatically:** a holder of another namespace, or one whose namespace is unknown or malformed
  (`FABRIC_MESH_LOCK_FOREIGN_HOLDER` / `FABRIC_MESH_LOCK_UNPROVEN_HOLDER`, still a `MeshLockTimeoutError`).
  - **Under protocol 3, a record without a namespace (a holder from before protocol 3) is UNPROVEN too**
    (pi-fabric#923). Its pid may name nothing here although it is alive in another namespace.
    No exception: a crashed pre-3 holder, of any lock, needs the operator step after the switch.
  - The error, and a `[pi-fabric] ALARM` line (once per holder), name the lock path and the exact command, with the
    root and the token shell-quoted: `fabric-mesh-backend lock-recover --root 'DIR' --lock mesh|custody --holder 'TOKEN'`.
    That command acts only on a holder of its own namespace whose pid is gone; run it inside the holder's namespace.
  - Its labelled `--unsafe-holder-stopped` override recovers on the operator's word that the holder is stopped,
    prints an ALARM, and appends an audit line naming the operator (uid, user, pid, parent pid and executable, host) to
    `<root>/lock-recoveries.jsonl`. It records the parent's executable path and pid, never its command line. The
    audit is opened without following a symlink and must be a private (0600) regular file of this user. It rotates
    once at 1 MiB (`.1`). On Windows it is only ever created exclusively: an existing audit file refuses the next
    recovery until it is moved aside. If it cannot be written, nothing is recovered.
- **Rollout:**
  - Old releases accept only 4- or 5-field (custody: 7-field) records, so they can never reclaim a protocol-3
    record.
  - New releases at protocol 1 or 2 reclaim legacy records by today's rule. At protocol 3 they do not. So switch a
    root to 3 only after its writers run a release that knows it, and recover any crashed old holder with the
    command above.
- **Windows fails closed:** the namespace id there is a machine and boot identity, which cannot tell Windows silos
  (containers) apart. So under protocol 3 a Windows holder is never reclaimed on pid evidence, even one whose
  namespace equals ours. A crashed Windows holder needs `--unsafe-holder-stopped`, until the handle-based release of
  smarty-dev#9569 (d) can prove liveness.
- **Not covered:** the synchronous SQLite create fence (`holdMeshFenceSync`, a single brief attempt) still writes
  the legacy record.
- **Residual off Linux:** a holder's release by name is still exposed to a forced rename (smarty-dev#9569 (b)/(d)).
  Linux releases through the pinned directory fd.
These safeguards do not repair old B68 binaries still running on the root. Even on
protocol 1, claiming this new-only safety boundary requires fencing every older capable
writer/cleaner and preventing respawn or later resumption. Rollback needs the same
boundary or isolated roots; changing the protocol default alone is not a migration.
There is no environment fallback,
runtime marker, transition guard or hot reload for this selector.

Keep `1` for compatibility with B68 writers. Protocol 2 activation is deferred to the
coordinated rollout in smarty-dev#2570: drain/terminate all old-format-capable writers
and prevent their restart or rollback on the shared root before selecting `2`. Mixed
protocol 1/2 operation on one root is not safe. Standalone `mesh-bridge` does not load
Fabric config: set `--lock-protocol 1|2` separately on each `run` and `agent` startup
(default `1`); an SSH forced command must pin the remote agent's selection explicitly.

When several projects share a root, project-scoped actors are shared too: any live Main on that root can adopt a project actor whose owner has gone. Set `mesh.actorScope` to `"session"` so new actors default to their root Pi session, which other sessions do not load or adopt. This is only the default for `agents.create`: existing project actors, and actors created with an explicit `scope: "project"`, stay shared. Session actors do not survive `/new`.

`mesh.actorScope` is the default storage scope for `agents.create`; each actor can override it with `scope: "project"` or `scope: "session"`. Both scopes run concurrently:

- `"project"` (default) uses `.pi/fabric/mesh/actors/`. Actors survive `/new` and appear in every trusted Pi session for the project.
- `"session"` uses `.pi/fabric/mesh/actors/<sessionId>/`. Actors are isolated to the root Pi session and remain available to participant agents in that lineage. Use this for task-specific supervisors and private history.

In project scope, one host owns each actor runtime. Only that host drains host events and mesh subscriptions. Other sessions can read the shared definition, mailbox, and logs; set their own model and thinking binding; and route `ask`, `tell`, `steer`, `followUp`, and `stop` through the owner. They do not start another actor runtime.

If the owner lease and lineage root both disappear, a matching trusted host can adopt the actor. Main adopts session-resident actors. The resident host adopts durable actors. Adoption stores a new `rootId` and an `adoptedAt` fence under the registry lock. Concurrent starters converge on one owner. The 30-second fence gives that owner time to publish its participant record. Until every registry row has a matching owner, create or import can fail with `registry is owned by another host`.

Registry writes take a stale-safe lock and merge only actors owned by the writer. A local save preserves newer records from another owner.

Each live actor publishes a presence record in the shared mesh state. When a session has been gone for a day, with no host lease, no legacy session entry and no presence write in that time, any runtime on the root removes its leftover presence records on its retention sweep. Each removal is checked against the record's version. A session that comes back publishes its presence again when it loads its actors.

`agents.setModel` and `agents.setThinking` change the current Pi session by default. In project scope, their binding files are separate from `actors.json`. Pass `scope: "project"` to change the shared default; only the owner can do so. Values passed to `ask` or `tell` affect one activation. Fabric resolves values in this order:

```text
call override → session binding → project default → Fabric default
```

`mesh.controlIncarnationFence` is a **host-only** setting in the host's `fabric.json`: `"warn"` (default) or `"enforce"`. Project configuration cannot override it. In `"warn"`, an older sender that omits an upgraded destination's advertised `ownerIncarnation` can still deliver controls and messages, subject to the existing identity, target, bridge-link, lease, deadline and deduplication checks. The receiving owner emits a counted warning naming the sender at most once per sender per hour. An explicit wrong or stale incarnation is refused in **both** modes. In `"enforce"`, omission is also refused with `FABRIC_CONTROL_STALE_INCARNATION`. Keep `"warn"` while installs roll host by host: advertising an epoch alone does not make an old sender epoch-aware. **Flip to `"enforce"` only after every host runs a release containing this PR**, including Main and resident senders; see [the rollout cutover rule (smarty-dev#7514)](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/7514). Reload the Main or restart the resident after changing this captured setting.

`mesh.bridgeControlTimeoutMs` (default 30000, range 30000–300000) is the admission window for control commands to a validated participant mirrored from another host. It covers outbound bridge queueing; senders also allow up to 15 seconds for the acknowledgement's return leg. A longer explicit request timeout wins. Mirrored steer/followUp waits are bounded from admission even during a mesh-lock wait; lease renewal cannot extend them. ASK, bridge and explicitly bounded requests retain their timeout policy. Native steer/followUp commands with a validated fresh owner lease have a 60-second admission/ACK window plus the existing (up to 15-second) return-leg grace. While a message is pending, the sender polls only the captured owner's lease file; an owner more than the routing grace overdue produces retryable `FABRIC_PARTICIPANT_STALE`, not an acknowledgement timeout.

`PI_FABRIC_PARTICIPANT_LEASE_GRACE_MS` configures resolution-only lease grace (default 45000 ms, range 0–300000; invalid values use the default). It does not lengthen heartbeat TTLs, discovery liveness, or residency consumption fences. When an exact native participant's lease is recently late and the mesh is write-stalled/lock-contended, or its matching lease file advanced beyond stored state, resolution waits at most 10 seconds, polling that file every 100 ms. A renewal restores ordinary resolution; no renewal gives `FABRIC_PARTICIPANT_STALE` (`retryable: true`, “lease late by Ns; retry”). A lease older than the grace or an explicit terminal session receipt remains Unknown. Same-name presence stays ambiguous even if one owner's lease is unavailable; there is no replacement-participant fallback.

Background mesh paths (participant heartbeat, lifecycle and control polls, actor presence, resident delivery and the queues behind them) retry a `FABRIC_MESH_LOCK_TIMEOUT` with backoff. Each path prints at most one lock-retry line per window (default 60 s), even across short outages: the first warning is immediate and names the lock holder; a later warning ends with `(N similar retries suppressed in the last Ns)`. When an outage ends inside a window, the first real lock acquisition after the window prints `mesh lock recovered (N similar retries suppressed in the last Ns)`, and that line is the window's line. Errors other than a lock timeout are printed every time. `PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS` sets the window in positive milliseconds; blank or invalid values use 60000.

For a stale remote `agents.steer` or `agents.followUp`, retry once with the **same `idempotencyKey` and unchanged target/message/data**. Callers may supply the key up front; otherwise a generated key is included in the stale error text and metadata. The sender/owner/target/operation-scoped key maps to the owner's persisted command claim, so a lost ACK is re-observed, not redelivered. The existing bounded resend after positive `notRun` proof uses a stable second-attempt sub-key under the same logical key; it cannot replay a handler that already ran. Retry promptly while the retained claim is available. An absent ACK alone never proves non-delivery; do not retry with a new key.

`mesh.followUpFlushMs` (default 120000) bounds how late an agent `followUp` reaches a busy Main. Fabric holds such a followUp while Main works. At the next boundary between tool calls, it sends every followUp that has waited this long as one batched steer, oldest first, behind any steer already queued. When the run is about to settle (`agent_before_settle`), it hands the rest to Pi's followUp queue, so Pi continues the run for them unless the user cancelled. `0` keeps Pi's own followUp queue, which Pi reads only when Main has no more work. Pi hosts older than 0.87.0 have no `agent_before_settle` and always keep Pi's queue.

`mesh.followUpStallSeconds` (default 600) makes a stuck followUp queue visible to its sender. When Main is idle and the oldest followUp that Fabric still holds for it, from any sender, is at least this old, no boundary will release the queue: the owner marks its acknowledgement `stalled: true`, and `agents.followUp` and `agents.tell` to that Main throw `Fabric followUp to <target> was accepted but is not being delivered: <n> held, oldest <age> s, target idle.` The message stays held, not withdrawn; use `agents.steer` meanwhile. A busy Main is never reported stalled, because a long turn holds followUps until its next boundary. `0` disables the check. Owners older than this setting never report `stalled`.

The owning host's existing committed presence heartbeat also checks runtime stalls (at most once per minute; no extra timer):

- `mesh.rootPresenceAlarmMs` (default 900000, 15 minutes) publishes one `ops.owner` / `root.presence.alarm` per root absence episode when actors or agents remain after their root disappears from live discovery. It reports member counts by kind and status, including idle/stopped members. The durable identity is the root plus its first observed absence time; a returning root re-arms it. This is an alert, not proof that permits orphan adoption.
- `mesh.undeliveredAlarmMs` (default 1800000, 30 minutes) publishes `ops.owner` / `inbox.age.alarm` to the sender and target owner for an unconfirmed Main followUp or steer, once per message/address. A queue ACK is not delivery: only the canonical synced native session receipt counts. Addressed age alarms cross the existing mesh bridge; unrelated `ops.owner` kinds remain excluded.
- `mesh.rootGoneTtlMs` (default 7200000, 2 hours) supplies an explicit `undeliverable: root gone` receipt when a departed root has no recorded successor. A lapsed lease alone never moves a live writer's queue.

Native `new`/`resume` session replacement records its explicit `targetSessionFile` after closing the old drainer. Only that exact successor inherits undelivered Main messages. A durable per-message claim precedes source removal; successor admission reuses the original native message ID and the existing journal/delivery-ID receipt machinery. A crash between move, admission and receipt is recoverable without delivery into both inboxes. The sender receives `rerouted: <old> -> <new>` through `fleet.work.inbox-receipts`, the existing root inbox and bridge work-event path. Recorded native deliveries never move. Reload does not rotate; labels, cwd matches and missing leases cannot invent a successor. The pre-switch abort boundary cannot flush to the old inbox; if another extension cancels a switch, explicit owner input reopens that inbox.

`mesh.eventContextChars` bounds the sanitized JSON context attached to each host-event activation. Fabric extracts images first. It stores redacted image descriptors in the mailbox and registry, then sends the raw images to the actor out of band. The character limit never truncates image base64 because base64 is not part of that JSON context.

Explicit background display observations of shared state and participant files reuse a
snapshot for at most `mesh.idleReadCoalesceMs` (default 5000 ms; accepted range 0–10000,
with a 1000 ms background-only runtime floor). Ordinary runtime reads remain exact on
change, regardless of invocation activity. File changes and UI remote-generation checks
do not bypass the background window while Main is idle. A running turn or pending Main
message shortens only the background window to 1000 ms; explicit fresh reads, including
Schema state bindings, always bypass it. Resident participant-file lists retain their
configured legacy observation TTL without a floor; fresh ownership/maintenance lists
bypass it, and resident shared-state/routing reads remain exact on change.
Actor watches poll immediately after a quiet actor cadence, then retain one trailing
poll per `max(1000, mesh.actorPollMs)` ms for continuous bursts. Windows and unsupported
watchers retain `mesh.actorPollMs` polling. Residency delivery drains use the 1000 ms
floor; explicit scheduling/catch-up stays prompt.
Listings can lag by this window; expiration does not slide on cache hits.
Fresh readers revalidate canonical physical generation and skip parsing an unchanged file
again. Stores share parsed reader snapshots within a process, and a bounded optional read
journal advances changed snapshots incrementally. Missing/legacy/invalid journals fall back
to canonical reads. The added UUID/hash fields and sidecar remain backward-compatible;
writer cadence and CAS revisions are unchanged. See [mesh state read gates](mesh-state-reads.md).

Mesh topics, shared state, and the participant directory remain project-scoped. Every runtime publishes one short-lived host lease and records for the roots, agents, and actors it owns. `agents.members()` and `mesh.members()` read those records. `agents.main()` and `agents.peers()` project roots. When a lease expires, its records leave normal discovery together. A host that stops without a clean shutdown leaves them in the shared state, so each runtime removes, every 15 minutes, the records of hosts whose lease expired more than 6 hours ago. Each removal is checked against the record's version. Existing needed directory writes also compact at most 64 expired native/resident host records per batch, without touching participant or delivery retention. `mesh.actorPollMs` controls fallback polling for actor events and owner-addressed commands when filesystem notifications are unavailable. For the NATS-only host-local participant and lease projection, including its KV-authority and watch-recovery rules, see [NATS local presence projection](nats-local-projection.md).

A resident host exits cleanly when its root Main is no longer live (including the existing 180-second reload lease), it owns no actors (including idle or stopped actors), and it has no queued/processing requests or active agents continuously for `mesh.residentIdleExitMs` (default `600000`, 10 minutes; range 0–7 days). `0` disables this exit policy. Actor/request activity or a renewed root lease resets the window. The host logs `resident exiting: root dead, no actors, idle N min`, withdraws its lease, and leaves an intentional-exit marker so its launcher does not restart it. A later explicit spawn or actor-create request clears the marker and starts a new host normally. Accepted same-release configuration updates apply the setting during resident maintenance; rollback is a revert or setting `mesh.residentIdleExitMs` to `0`.

Shared state keeps a persistent revision clock (`highWater` in the mesh `state.json`). A new key takes the next clock revision, and an update takes its key's version plus one. Compare revisions only through `ifVersion`; a new key seldom starts at 1. Fabric builds from before this clock can still write to a shared root. A newer Fabric then raises its clock to the highest retained revision, so both can use one root.

If `state.json` is empty or unparseable, reads return an empty table and every write fails with `invalid state format`. This barrier keeps Fabric from issuing a revision that an earlier caller still holds. To repair a root, stop every Fabric process that uses it, inspect the file, fix it or move it aside, and then start the processes again. Moving the file aside restarts revisions. That is safe only while no process runs, because each process reads fresh revisions after it starts.

## Records

`records` (off by default) connects Fabric to the org's records service for `records.*`: `enabled`, `socket` (the
service's unix socket), `credentialFile` (an operator-issued credential, for the importer or the mirror), `relayCredentialFile` (the relay's
credential: this process then publishes nudges and raises alarms), `alarmTo`,
`watchdogMs` and `consumerLagSeconds`. Database access, roles and archive admission are the service's own
configuration, never a caller's. It needs the mesh. See [records](records.md).

## Actors

`actors.transientBlockCooldownMs` (default `600000`, 10 minutes; from 1000 to 86400000) is how long a transient activation block gates routing. Transient blocks come from provider stream errors, timeouts, and preparation or lock timeouts. After the cooldown, the actor is routable again and its next activation is a probe. Each transient failure of a probe doubles the backoff, up to `actors.transientBlockMaxMs` (default `3600000`, 60 minutes). Hard blocks, such as credentials, configuration or a model that is not allowed, never expire. See [actor activation blocks](agents.md) (smarty-dev#6509).

`actors.maxSessionBytes` limits the size of a persistent actor's Pi session file, in bytes. The default is `20971520` (20 MiB). Set it to `0` to disable the limit. Before a run starts, Fabric checks the session file. A larger file gets the same reset as `agents.resetSession()`, with trigger `size`: Fabric archives the file and the run starts a fresh session. So Fabric never starts a run that must compact a session past the limit. Durable actors use the same setting. See [fresh actor sessions](agents.md#fresh-actor-sessions).

## Memory sources

`memory.enabled` (default: true) toggles the `memory.*` provider. `memory.sources` registers portable host sources so remote or archived session trees answer source-qualified calls such as `memory.recall({ source: "laptop", ... })`; `memory.sessions` and `memory.expand` accept the same `source` argument. Without `sources`, memory behavior is unchanged: source-less calls keep their filesystem scopes and source-qualified calls fail with `source_not_found`.

Each entry has three keys:

- `id`: the `args.source` label. Lowercase letters, digits, dot, `_`, or dash; must start with a letter or digit and be unique across entries.
- `kind`: the adapter kind. Only `"fs"` exists today; anything else is a configuration error at load time.
- `root`: absolute path the `fs` adapter walks recursively for `*.jsonl` files. Native agent trees (`sessions/<encoded-cwd>/*.jsonl`) and flat archive directories both work; keys are root-relative paths, and traversal outside `root` is refused.

The `fs` adapter derives each session's `revision` from the SHA-256 of the file bytes, so mtime-only touches keep follow pointers valid while content changes invalidate them. Enumeration is bounded by `memory.maxSessions` and reported through coverage reasons (`fs_source_max_sessions`, `fs_source_scan_capped`); a capped archive is never presented as complete. Ranking, branches, and expansion follow the normal engine paths described in [memory recall](memory-recall.md#portable-host-sources).

## Extractive history (opt-in)

In `/fabric settings`, open **Extractive history**. Choose **Consent / mode** and a **Native classifier** from Pi core's registry. Model selection alone does not enable inference. This uses Pi core's `classify()` API, not Fabric's Jev connector.

```json
{
  "memory": {
    "extractive": {
      "enabled": false,
      "provider": "typesafe",
      "model": "jev-latest",
      "maxViewBytes": 8192,
      "maxCandidates": 128,
      "maxSourceChars": 24000,
      "maxEvaluationsPerTurn": 1,
      "timeoutMs": 3000
    }
  }
}
```

Set `enabled: true` to opt in. With `maxEvaluationsPerTurn: 1`, bounded active-branch user/assistant text is sent to the selected classifier and may incur API charges; there is no secret scanner. Set the budget to `0` for local deterministic extraction with no classifier calls. `memory.enabled: false` disables the feature too.

The bounded view preserves complete source quotes, attribution and omission notices. Scores indicate salience, **not truth**. Missing credentials, invalid answers and timeouts fall back deterministically; stale branch/session responses are discarded. It supplements request context without deleting history or replacing Pi compaction, and introduces no generative summarizer. See [Extractive history](extractive-history.md) for bounds and source navigation.

## Principal and scope

Scope is host-issued and has no config key. A project or global setting cannot grant or widen a principal.

- `PI_FABRIC_SCOPE`: JSON `{ version?: 1, principal: { id, issuer?: "host" }, grants: [{ resource, actions }], digest?, parentDigest? }`. At most 64 grants; `actions` from `read`, `write`, `execute`.
- `PI_FABRIC_SCOPE_FILE`: path to the same JSON, at most 64 KiB. Set only one of the two.

Fabric reads both once when the extension initializes. An embedding process can call `issueRootScope()` from `pi-fabric/scope` before `session_start`. Malformed JSON, an unreadable file, a digest mismatch, or conflicting issuance makes every provider call fail with a clear error. Fabric never falls back to an unscoped session. Fabric children receive their derived scope through `PI_FABRIC_SCOPE`. See [principal and scope](providers.md#principal-and-scope).

## Compaction

The deterministic, LLM-free compaction engine is on by default. It keeps Pi's bounded `keepRecentTokens` continuity tail. `compaction.targetContextRatio` sets a hard occupancy ceiling. Set `compaction.engine` to `"pi"` to restore pi-core compaction. When pi-vcc is also installed, Fabric takes precedence for automatic compaction. An explicit `/pi-vcc` command always uses pi-vcc's engine. See [compaction](compaction.md) for invariants, loss guarantees, sections, and limits.

`compaction.outputReserveTokens` (default `0`, disabled; integer up to 100,000,000) compacts at the settled boundary once the window headroom falls below the reserve. `compaction.pressureBands` (default `{ "warn": 0.6, "urgent": 0.8 }`, requires `0 < warn < urgent < 1`, otherwise defaults) labels `compact.pressure()` readings. `compaction.repairOrphans` (default `true`) drops orphaned tool results and fills missing ones in the outgoing context. See [headroom trigger](compaction.md#headroom-trigger) and [orphaned tool-result repair](compaction.md#orphaned-tool-result-repair).

## Execution trace

`trace.assessment` (default `false`) adds a redacted `FabricAssessmentTraceV1` beside each `fabric_exec` execution trace: per-operation durations, model/Jev/classifier attribution, and token usage and cost where an operation reports them, plus program totals. It never stores arguments, results, code, or error prose, and the deterministic trace is unchanged. See [assessment projection](audit-trace.md#assessment-projection).

## Catalog repairs

Silent invocation repairs are on by default. `repairs.enabled` controls the catalog-scoped table at `~/.pi/agent/fabric/repairs/current.json`. Inspect it with `/fabric repairs`. See [catalog repairs](repairs.md).

Static compatibility compilation is on by default. `entropy.compile` enables proof-checked normal forms behind the existing tool interface; canonical schemas, enum domains, and action availability never change. Rules are available on the first call without a corpus, and the background loop persists schema-bound plans to `<agent dir>/fabric/entropy/compiled.json`. Version 1 restriction artifacts are inert and migrate automatically. Set `entropy.compile: false` to disable the normal-form path; `repairs.enabled` independently controls the learned catalog alias table. No extra model tools, arguments, or repair confirmations are introduced. Inspect witnesses and the observed invocation rejection rate with `/fabric entropy`. See [tool entropy](entropy.md).
