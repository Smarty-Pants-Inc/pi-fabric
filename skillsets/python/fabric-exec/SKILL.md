---
name: fabric-exec
description: >-
  On-demand Python API and workflow reference for fabric_exec.
  Load when authoring nontrivial programs, checking exact tool/provider contracts,
  batching work, or recovering from argument, payload, or runtime errors.
---

# fabric_exec — Python reference

Write a Python async function body in `code`: top-level `await` and `return`, no enclosing function or event-loop runner. Every invocation starts fresh. Only the returned JSON-compatible value reaches the model; `print()` goes to activity logs. Use `True`, `False`, `None`, native dicts/lists, and dict access for results. Never switch interpreters through shell commands to perform Fabric orchestration.

## Runtime boundary

The default Python backend is Monty, a sandboxed Python subset. Arbitrary imports, third-party packages, native filesystem/network/environment access, and some Python syntax are unavailable. Use host tools for effects. `import asyncio` and `asyncio.gather` are supported. Missing native dependencies fail without fallback. Explicit CPython is trusted native code outside Schema enforce; enforce requires OS isolation and fails closed when unavailable. Neither backend exposes guest-local workflow helpers, memory visitor callbacks, or handoff predicates.

## Program workflow

Batch independent, bounded Fabric operations in one Python program with `asyncio.gather`; await dependent steps sequentially. Keep search→read and edit→verify sequential when the first output decides the next action. Filter large results inside the program and return compact decisions, failures, and evidence, not raw logs or unused intermediate values. Preserve successful effects or launch handles when another operation fails; do not replay the entire batch blindly.

For coding tasks, keep an acceptance ledger in reasoning or concise progress notes, not a separate tool call: define concrete checks, trace the execution path before editing, implement end to end, then run targeted tests and direct behavioral probes. Mechanically confirm requested public symbols, registrations, and configuration entries. Use the smallest checks that cover the ledger; escalate for failures or cross-cutting risk. Inspect failures and iterate instead of rerunning unchanged passing checks. A build alone is not completion.

Use `display.name` and objective `display.description`; Fabric pairs them with verified outcomes in deterministic compaction. In a Main, a `fabric_exec` program ends after 10 min and `agents.run/wait` return after 60 s; spawned agents keep running and report back. A timed-out observation does not cancel the underlying work.

## Core tools

Full code mode (also Schema enforce) exposes `pi` and captured `extensions`; in orchestration-only mode they are unavailable inside Fabric, so read, search, edit, write, and run shell commands with Pi's native tools directly.

```python
import asyncio
manifest, matches = await asyncio.gather(
    pi.read("package.json"),
    pi.grep(pattern="TODO", path="src", limit=20),
)
return {"manifest": manifest, "matches": matches}
```

Search before bounded reads. Escape regex metacharacters or use `literal=True` for exact punctuated text; keep fan-out limits small and widen only on misses: `await pi.find(pattern="*.py", path="src", limit=20)`, then `await pi.read(path="src/main.py", offset=1, limit=80)`. Read continuation notices; unbounded reads cap at 2000 lines or 50KB. `read`, `grep`, `find`, and `ls` return payloads. Shell, edit, and write return dictionaries:

```python
r = await pi.bash(command="git status --short", settle=True)
return {"ok": r["ok"], "output": r["output"], "exitCode": r.get("exitCode")}
```

For test/probe nonzero exits, use `settle=True` and inspect `r['ok']`, `r['output']`, and `r.get('exitCode')`; summarize the evidence instead of returning raw logs. Shell nonzero exits raise unless `settle=True`; timeout, cancellation, security and approval failures still raise. Set a known long suite's shell `timeout` in seconds once instead of retrying a timed-out call. `background=True` (alias `run_in_background`) returns immediately with `ok: True`, a pid, and a live output path while the process keeps running — do not poll; `pi.read` the path when you need output, or `kill <pid>`. Nested shells that exceed `executor.shellHangMs` (default 2 minutes) auto-spill the same way. No stdin option: write content to a file first, then use its path. Do not interpolate untrusted content into shell commands.

Use top-level `payloads` for multiline content. Values are literal strings, not file loaders or placeholders: read files with native tools first; `@/path`, `file://` and bare `__TOKEN__` values are rejected. Only exact keys supplied in this call exist: `π.body` or `payloads["body"]`. `π` is an attribute object; `payloads` is a separate dict. For a supplied `body` key, write with `await pi.write(path="notes.txt", content=π.body)`. Edit with `await pi.edit(path="notes.txt", edits=[{"oldText": "before", "newText": "after"}])`. Coalesce independent edits from one snapshot; use `all=True` only for intentional repeated anchors.

## Discovery and host actions

Known actions use `await mcp.<server>.<tool>(...)`, `await extensions.<name>(...)`, or stable providers `memory`, `state`, `schema`, `compact`, `thinking`, `decisions`, `programs`, `components`, `agents`, and `mesh`. Underscore-prefixed capability names require `tools.call` with the exact discovered ref. Host methods accept one dict or keyword arguments, not callback functions. Supply acyclic JSON-compatible arguments.

```python
hits = await tools.search(query="deployment status", limit=3)
if not hits:
    return None
return await tools.describe(ref=hits[0]["ref"])
```

Inspect `inputSchema` and `outputSchema` before calling a dynamic action. `await tools.call(ref=ref, args=args)` invokes a computed ref; bare tool names are invalid. `tools` provides discovery and generic calls, not core I/O. Captured extension results are dicts containing `text`, `content`, `details`, and `isError`; MCP results are server-defined. Do not assume attribute access on returned dictionaries.

## Memory, state, and orchestration

`state.*` logs transitions rather than storing arbitrary application values. Keep your own values in files or return them; use `mesh.put`/`mesh.get` only for small values another agent needs to read.

`memory.recall` searches evidence; catalog descriptions are navigation, not evidence. Follow returned `follow` and `next` dictionaries with `await tools.call(hit["follow"])` and `await tools.call(result["next"])`. Use `memory.expand` and explicit loops for paging, not guest callback helpers. Do not parse Pi session JSONL manually. Discover exact schemas with `tools.describe` rather than guessing fields.

`state` records claims and verification; `await state.verify(binding={"commit": sha})` binds the certificate (a commit that differs from git HEAD fails closed). Under Schema enforce, protected changes must use `schema.hypothesize`, `schema.verify`, and `schema.commit` in one invocation; do not bypass host gates. `compact.request` requests compaction rather than running an alternate executor. `await compact.pressure()` returns a read-only dict with `band` (`ok`/`warn`/`urgent`/`unknown`), `headroomTokens`, and `owner`; `await compact.carry(add=["fact"])` keeps a bounded focus list in every Fabric summary until `compact.carry(clear=True)` (no arguments reads `{"items": [...]}`). `await thinking.set(level="high", scope="turn")` raises Main's reasoning effort until the next agent_end (`scope="turns"` with `turns=1..20`, or `"session"` until `await thinking.reset()`); levels clamp into `thinking.bounds` and the model's levels, and `await thinking.status()` reports `level`, `available`, `bounds`, `baseline`, and any `override`. `d = (await decisions.raise(title="Deploy?", input="confirm"))["id"]` stores a durable pending decision (`holder` `"user"` by default, so only a person answers through `/fabric decisions` or `pi-fabric decisions answer`); `await decisions.wait(id=d)` returns the record once answered, expired, or cancelled, and `decisions.list`, `decisions.answer(id=..., optionId=...)`, and `decisions.cancel(id=...)` cover the rest. `onExpire="escalate"` with `escalation={"hopTimeoutMs": 600000}` hands an unanswered decision up its chain (default supervisor, root, user) at each deadline; the current holder passes it up now with `await decisions.escalate(id=d, reason="...")`. Never answer a decision you raised. `await programs.save(name="triage", code=source, inputSchema={...})` stores a content-addressed candidate and returns `{"ref": "triage@<digest12>", "digest": ...}`; `await programs.run(ref="triage", input={...})` runs a saved program nested with this program's capabilities and approvals, its `input` global set from `input`, and returns its value. `programs.list(name=..., status=...)` and `programs.get(ref=...)` read; only the user promotes or retires with `/fabric programs`.

Advanced agent/mesh workflows require explicit user intent. Branch pointers: read `<skill-dir>/references/agents.md` for child agents and persistent actors, `<skill-dir>/references/mesh.md` for coordination, and `<skill-dir>/references/mcp.md` for MCP naming/management only when the task needs those surfaces. Peer means another root Pi session: query `await agents.peers()` first, not child-agent lists. Agent calls inherit this kernel unless a child's language is explicitly selected; that does not change this program's language. Omit agent `timeoutMs` unless requesting longer than the configured default. Use discovered host actions with ordinary loops and `asyncio.gather`; use the selected Python skill tree, never launch another interpreter to run workflow programs.

## Jev judgments and programs

Prefer existing CLIs and deterministic code for deterministic work: omit `jev.evaluate` and set `maxEvaluations:0` (host auto approvals remain independent). Use judgments only for explicit authorized batched questions. Code owns commands; never execute a model answer as shell source. `tasks.wait/watch` await bounded task receipts or monitor batches without polling/inference; prefer UI-only monitors when Main need not wake. Browser/macOS tools need no Fabric bridge. `jev.status/stop` control programs; `tasks.stop` separately stops detached tasks. Keep task IDs and finite process deadlines because cancelling an observation does not stop work.

`wait` is canonical for both providers. `agents.join(id=...)` aliases `agents.wait(id=...)`; `tools.call(ref="jev.join", args={"id": ...})` aliases `jev.wait`. Each alias preserves its provider's lifecycle and notification behavior.

Use `await tools.call(ref="jev.evaluate", args={...})` for typed Choice/Noul/Score judgments. `jev.run`/`jev.spawn` take `{"program": ..., "input": ...}`; `jev.status` takes optional `{"id": ..., "after": ...}`, and `jev.wait`/`jev.stop` take `{"id": ...}`. Results are dictionaries: evaluate returns `model`, `answers`, and token `usage`; run/wait/stop return a run envelope with `id`, `state`, `result`/`error`, counts, usage, and bounded progress. Spawn initially returns `running` and optionally accepts `observe` for event-driven Main-turn advisors. Inside the TypeScript artifact, `program.nextEvent()` waits without polling; `program.advise({eventId,message})` requires `jev.advise` plus explicit delivery. The public `jev.advise` call takes `{"id": ..., "eventId": ..., "message": ...}` and returns `{"delivered": bool, "reason": ...}` (reason only when suppressed); agent approvals, freshness, and feedback gates apply. Return the observer ID without waiting inside Main. Status without id reports credential presence and run summaries without retrieving a key.

The outer Fabric program remains Python. Jev's `program.code` is a TypeScript artifact executed by its dedicated QuickJS manager, not a Python runtime or shell escape hatch. Runs are session-owned, not restart-durable; cancelling wait cancels only the wait, and stop cannot roll back effects. Credentials stay host-side via `/login jev`/`TYPESAFE_API_KEY` for direct aliases, the existing openrouter credential for `typesafe/…` ids, the existing `vercel-ai-gateway` credential for `typesafe-ai/…` ids, or a trusted command. Jev is unavailable in Schema enforce and managed hosts. State sent to TypeSafe consumes credits; confidence is not authorization.

Recommend `/skill:fabric-jev` for guided authoring; never load it autonomously. Only after direct invocation, `<skill-dir>/../fabric-jev/SKILL.md` is its workflow pointer. `<skill-dir>/../../../docs/jev.md` is a branch pointer for exact host contracts, limits, and browser composition.

## Recovery

Read the reported user line and recovery hint, describe the failing action, then repair only the failed syntax or call. Do not replay successful effects blindly. Convert sets, bytes, paths, and datetimes to JSON-compatible data before returning. Unsupported Monty syntax/imports are not permission to enable native execution or switch languages.
