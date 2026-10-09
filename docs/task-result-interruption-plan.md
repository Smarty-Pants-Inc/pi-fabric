# Task final-report interruption acceptance ledger

Refs: smarty-dev#7567, smarty-dev#6730.

Base: `5eed0787da6be2ff356f73d89deee23d50925f4f` (bundle `fv2-main`).

## Design and execution path

- Native Pi stdout -> worker `processEvent` -> `updateRunRecord` -> atomic PID temporary file + rename -> run-directory `status.json`.
- `AgentManager.#monitor` consumes terminal `status.json`, drains the process transport, and returns the full record through `wait`/`status`; `agents.join` aliases `wait`. Completion journals retain that same record.
- Retain `lastCompleteText` and `partialText` separately, bounded with `latestRunText`; never expose thinking/tool argument deltas as prose.
- Use the existing `completed` status with an explicit warning (also appended to returned text) for interrupted ordinary Pi tasks with a completed tool turn and persisted assistant output. Keep the original stream error. Structured reply/schema validation stays authoritative.
- Fence Fabric's same-session resume when this retained interrupted result is available: no model call, replay, or generated summary.
- Do not change state backend configuration or actor semantics.

## Checks

- [ ] A fake Pi final stream cut after completed tools returns persisted partial text + warning through the real process manager.
- [ ] With no final text, return the previous complete assistant message + warning.
- [ ] Persist both previous complete text and each final text snapshot/delta while status remains running.
- [ ] A stream error before any output and tool-less failures remain failures.
- [ ] A tool start without a completed tool turn cannot qualify.
- [ ] Deterministic errors, stops, timeouts, structured validation, and actor results are not converted into success.
- [ ] Normal final output remains unchanged, with no interruption warning.
- [ ] Atomic update test proves destination readers see old or new JSON, never partially written JSON.
- [ ] Typecheck and fresh build; focused worker/record/retry/reply regression tests.
- [ ] Fleet commits, delta, logs, head report, PR draft, bundle, final SHA256 manifest and verification under TASK_OUT.
