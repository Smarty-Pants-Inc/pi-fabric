# Task final-report interruption acceptance ledger

Refs: smarty-dev#7567, smarty-dev#6730.

Base: `5eed0787da6be2ff356f73d89deee23d50925f4f` (bundle `fv2-main`).
Runtime implementation head: `37339de7f6231e652f7b713bd826d5e04fe19ee2`.

## Design and execution path

- Native Pi stdout -> worker `processEvent` -> `updateRunRecord` -> atomic PID temporary file + rename -> run-directory `status.json`.
- `AgentManager.#monitor` consumes terminal `status.json`, drains the process transport, and returns the full record through `wait`/`status`; `agents.join` aliases `wait`. Completion journals retain that same record.
- Retain `lastCompleteText` and `partialText` separately, bounded with `latestRunText`; never expose thinking/tool argument deltas as prose.
- Use the existing `completed` status with an explicit warning (also appended to returned text) for interrupted ordinary Pi tasks with a completed tool turn and persisted assistant output. Keep the original stream error and native child exit code. This status does not certify that the requested work is complete. Structured reply/schema validation stays authoritative.
- Fence Fabric's same-session resume when this retained interrupted result is available: no model call, replay, or generated summary.
- Do not change state backend configuration or actor semantics.

## Checked acceptance

- [x] A fake Pi final stream cut after completed tools returns persisted partial text + warning through the real process manager.
- [x] With no final text, return the previous complete assistant message + warning.
- [x] Persist both previous complete text and each final text snapshot/delta while status remains running.
- [x] A stream error before any output and tool-less failures remain failures.
- [x] A tool start without a completed tool turn cannot qualify.
- [x] Deterministic errors, stops, timeouts, structured validation, and actor results are not converted into success.
- [x] Normal final output remains unchanged, with no interruption warning.
- [x] Atomic update test proves destination readers see old or new JSON, never partially written JSON; a failed rename leaves the preceding published record intact.
- [x] Typecheck and fresh build; focused worker/record/retry/reply regression tests.

## Evidence

- Final interruption suite: 32 tests passed against source and separately against freshly built `dist/worker.js`.
- Atomic run-record tests: 19 tests passed, including two new atomic replacement/failure probes.
- Focused built-worker/record/retry/reply/settlement regressions: 82 tests passed before the final task-only exclusion refinement (the refined interruption suite was rerun afterward).
- Selected existing worker e2e tests: 25 passed, 28 deliberately unselected; normal output, inference attribution, retries, oversized results, abort/stop and worker crashes.
- Final `bun run typecheck` passed. Full `bun run build` passed, including verified artifact, native Landlock helper and lazy build-artifact gates.
- Intel1 lacks an ambient `cc`; the unchanged build used public GCC packages unpacked only under the private task TMPDIR via the supported `CC` environment override. No host package installation, build-check bypass, state-backend edit, credentials, agents, remote host access or push.

Artifact closeout (delta, logs, caller-visible JSON snapshots, head report, PR draft, bundle, final SHA256 manifest and verification) is recorded in `$TASK_OUT/result.md`.
