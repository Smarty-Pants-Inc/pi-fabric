# Task final-report interruption acceptance ledger

Refs: smarty-dev#7567, smarty-dev#6730; PR #720 round 2.

Original base: `5eed0787da6be2ff356f73d89deee23d50925f4f`.
Round 2 base: `cc2b90e5f9d9a331ffc650ca938a9dd71d6dd9b7`.
P1-a commit: `de21ea8d6abac9cbb19f18df9d9743b6a9b696f4`.

## Design and execution path

- Native Pi stdout -> worker `processEvent` -> `updateRunRecord` -> atomic PID temporary file + rename -> run-directory `status.json`.
- `AgentManager.#monitor` consumes terminal `status.json`, drains the process transport, and returns the full record through `wait`/`status`; `agents.join` aliases `wait`. Completion journals retain that same record.
- Retain `lastCompleteText` and `partialText` separately, bounded with `latestRunText`; never expose thinking/tool argument deltas as prose.
- A qualifying interrupted ordinary Pi task keeps **`status: failed`**, the original stream error, and the native child exit code. `partialText` and `text` expose retained raw partial output (falling back to the preceding message); `warnings` separately explains the interruption. Never convert a cut report to `completed`, including when a valid durable reply or verdict prefix exists.
- Track unresolved streamed/generated/executing calls in the interrupted turn. A prior completed tool turn is not sufficient when later tool work is unresolved. Matching execution IDs preserve pending parallel siblings.
- Fence Fabric's same-session resume only when an eligible partial report is available: no model call, replay, or generated summary.
- Compact handoff results preserve partial text, warnings, native exit code and `completed: false`; background/handoff messages identify partial output and preserve the warning. Completed-only judge/structured consumers remain fail-closed.
- Do not change state backend configuration or actor semantics.

## Round 2 acceptance

- [x] P1-a source suite: completed first tool turn, then cuts at tool start/delta/end, snapshot-only call, generated `toolUse`, execution, and unresolved parallel sibling all remain failures without retention warning. A genuinely completed streamed tool turn still qualifies.
- [x] Source and freshly built dist probes: partial text remains readable with non-success status and native exit code; repeated wait/status and persisted record match.
- [x] Completed-only judge rejects a partial verdict even with an otherwise valid durable reply.
- [x] Background/handoff delivery preserves warnings, labels partial output, and never reports success.
- [x] Normal completion, atomic records, retries, reply/schema handling, settlement and selected worker e2e remain unchanged.
- [x] Final typecheck and fresh build pass.

## Evidence

- P1-a: 39 source interruption tests and typecheck passed before its separate fleet-identity commit. A final no-current-prose case also covers stale preceding text.
- Final interruption probes: 41 passed against source and 41 against freshly built `dist/worker.js`; caller-visible result/status/persisted-record snapshots retain identical partial text, warnings, native rc and error.
- Consumer checks: 113 inbox/handoff/judge tests passed (alongside the then-40 source interruption probes, 153 total); 3 selected provider handoffs passed, including failed partial-output mapping. The judge rejects a partial verdict even with an otherwise valid durable reply.
- Focused record/retry/reply/settlement/crash/tool-stream/structured-result checks: 148 passed across 8 suites.
- Selected built-worker e2e: 22 passed, 31 deliberately unselected. Native Pi offline tool-call recovery: 3 passed, with owned native child closure checked.
- Baseline reproduction: all 13 selected regressions fail against a task-private local archive of `cc2b90e5`, at the incorrect completed-result assertions (not fixture/module errors).
- Final whole-program typecheck and fresh build pass. The build includes verified artifact, native Landlock helper and lazy build-artifact gates.
Final head, delta, exact check logs, direct caller-visible JSON snapshots, updated PR body and one round comment are recorded under `$TASK_OUT` in `result.md`.
Intel1 has no ambient C compiler; the supported `CC` override uses public GCC packages unpacked only under this task's private TMPDIR. No package installation, build-check bypass, state-backend edit, credentials, agents, remote access or push.
