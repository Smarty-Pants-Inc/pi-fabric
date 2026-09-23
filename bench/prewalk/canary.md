# Prewalk canary runner and verifier

Maintained, deterministic harness for measuring and verifying Prewalk behavior in
isolated copies. Both tools are diagnostic only: they never modify Prewalk
runtime code, benchmark expectations or the repository under test.

## Runner

`bench/prewalk/prewalk-canary-run.mjs` spawns one fresh `pi` process with the
compiled dogfood extension at the repo root plus the maintained recorder
(`bench/prewalk/prewalk-canary-telemetry.ts`), records the raw stream and never
replays a started cell.

```sh
node bench/prewalk/prewalk-canary-run.mjs --out <cellDir> --cwd <workDir> \
  --prompt-file <prompt.txt> --model <provider/model> --timeout-seconds 600 \
  [--turns 16] [--rpc --rpc-runs 2] [--extension <path>]... \
  [--request-contract <contract.json>]
```

- Default mode is one-shot `--mode json`: prompt on argv, stdin ignored.
- `--rpc` keeps a persistent session: the prompt is an RPC command and stdin
  stays open until `--rpc-runs` agent runs have settled **and** `get_state`
  reports idle with an empty queue, then the runner sends EOF. Use it whenever
  an extension may queue a follow-up (for example a recovery message) during
  settlement; JSON mode exits at the first settlement and abandons that queue.
- `--turns` sets the recorder turn cap (`PREWALK_CANARY_MAX_TURNS`, default 16).
  The chosen cap is recorded in `session_start.turnCap`; a run past the cap
  aborts and writes `turn_limit`.
- `--request-contract` opts a cell into positional request evidence: the JSON
  is validated before launch, its path and SHA-256 are frozen into
  `started.json`, and the contract path is exported to the recorder.

Artifacts per cell: `started.json` (replay guard, cap, mode, runtime hashes),
`events.jsonl` (raw stdout JSONL), `arrival.jsonl` (arrival clock),
`telemetry.jsonl`, `stderr.log`, `sessions/`, `finished.json` (problems and
verdict). RPC cells add `rpc.json` (settlements, correlated command responses,
final state) and `rpc-entries.json`.

## Verifier

`bench/prewalk/verify-prewalk-canary.mjs` reads a finished cell and emits a tri-state
check ledger. A check is `pass`, `fail` or `unobserved`, and an unobserved
check blocks `ok` exactly like a failure.

```sh
node bench/prewalk/verify-prewalk-canary.mjs --run <cellDir> [--json <newFile>] \
  [--report <newFile>] [--dist <distDir>] [--main <provider/model>] \
  [--executor <provider/model>] [--recovery-marker <text>] \
  [--expect-prewalk <n>] [--request-contract <contract.json>] \
  [--work-dir <absoluteDir>]
```

`--work-dir` supplies the cell's work directory when neither the task receipt nor
`started.json` records an absolute `cwd`. Without a resolved directory the
`task-verification` check is `unobserved` instead of passing on the receipt's own
hashes.

Checks: `recording-complete` (lifecycle parse; aborted assistants allowed only
with `--recovery-marker`), `finished-ok`, `runtime-identity` (structured parse
of the indented `prewalk.status` result; loaded == disk, `stale: false`, paths
inside `--dist`), `session-identity` (RPC `get_state.sessionId` == telemetry),
`in-place-roundtrip` (needs both `--main` and `--executor`),
`persisted-prewalk`, `request-contract-evidence`, `request-contract-payload`
(payload semantics need `--executor`), `task-verification` (opt-in
`--task-check-report`), `scope-no-writes-after-marker` (opt-in `--scope-report`),
and with `--recovery-marker` (`--main`
alone is valid here and attributes the recovery reply):
`recovery-abort-recorded`, `recovery-delivered-once`, `recovery-tools`,
`recovery-reply`, `recovery-no-writes-after-cancel`,
`recovery-work-preserved` (needs `probe.jsonl` snapshots; otherwise
unobserved). The report's `info` records compaction as
`compactor`/`version`/`fromHook`/`usageRecorded` — Fabric compaction is
deterministic and LLM-free, so absent usage is never reported as a missing paid
call.

`--json` writes a NEW report file and refuses to overwrite; the verifier never
writes inside the run directory. `--report <newFile>` writes a human-readable
summary rendered from the same JSON verdict — one analysis, never a second
opinion — and refuses to overwrite too.

The report's top-level `metrics` is informational, never a check: per-model
`requests`/`input`/`cacheRead`/`output`/`totalTokens`/`recordedCostEstimateUsd`
aggregated from recorded `message_end` usage, phase attribution
(`handoffAt`/`returnAt`/`preHandoffMainMs`/`executorIntervalMs`/`returnMs`, each
with a provenance string), an optional per-model reasoning breakdown that is
never added into totals, observed assistant `error`/`aborted` stops and
`toolResult` errors, and explicit caveats. Incomplete usage keeps
`available: false` with the reason: missing data is never reported as zero,
and provider-internal retries are not observable. A deliberate recovery
abort stays green while its metrics stay unavailable.

## Request-contract evidence

A contract declares literal markers over request text:

```json
{
  "markers": { "task": "TASK-MARKER", "plan": "PLAN-MARKER" },
  "exactlyOnce": ["plan"],
  "present": ["task"],
  "ordered": ["steer1", "steer2"],
  "absentBefore": ["continuation"]
}
```

All lists reference declared markers; a marker cannot be both `exactlyOnce`
and `present`. The runner validates the file before launch and freezes
`{path, sha256}` into `started.json`; the recorder loads the same file and
stamps its SHA into every `request_context` record. Each record carries
`requestIndex`, the active model, the message count, and per-marker matches as
`[messageIndex, partIndex, offset]` triples — positions only, never message
text (only user and custom message text is scanned).

`request-contract-evidence` fails on non-contiguous indices, sha mismatches,
truncated scans, unrecorded markers, or malformed position tuples (each must
be an ascending nonnegative safe-integer triple). `request-contract-payload`
then checks executor requests: `exactlyOnce` markers appear once and `present`
markers appear in every executor request, `ordered` markers first appear in
order, and `absentBefore` markers are absent before the executor switch.
Malformed positions always fail and never fall through to count or order
semantics.

The evidence is the `context`-hook request layout after Fabric's
filter/injection in extension load order — not a captured provider wire
payload (custom-API providers never fire `before_provider_request`), and later
`context` handlers can still change the delivered messages.

## Independent task verification (opt-in)

Lifecycle success and artifact quality are separate verdicts: a clean cell exit
says nothing about whether the artifact it produced passes its own tests. A task
check is therefore recorded in its own receipt and reported on its own axis.

```sh
node bench/prewalk/prewalk-canary-run.mjs ... --task-check <spec.json>
```

The spec names only relative paths inside `--cwd`:

```json
{ "testFile": "tests/native.test.mjs", "artifact": "tests/native.test.mjs" }
```

`testFile` must be a relative Node test module and `artifact` defaults to it;
absolute paths, `..` traversal, unknown fields and non-module files are rejected
before launch, and the spec path plus SHA-256 are frozen into `started.json`. The
runner never takes command text from the cell: it always issues one fixed
`node --test --test-reporter=tap <testFile>` in `--cwd`. The receipt
(`task-check.json`) records the exit code, the observed
`tests`/`pass`/`fail`/`skipped`/`todo` counts, and the verified artifact's
content hash before and after the check. A failing suite is artifact evidence,
never a runner problem, so `finished.ok` stays lifecycle-only.

```sh
node bench/prewalk/verify-prewalk-canary.mjs --run <cellDir> \
  --task-check-report <cellDir>/task-check.json
```

`task-verification` passes only when the receipt is internally consistent and the
artifact still hashes to the content that was verified; failing tests, an edited
artifact, a missing artifact or a malformed receipt fail, and a missing receipt
is `unobserved`. Write-scope compliance comes from an opt-in recorder report:

```sh
node bench/prewalk/verify-prewalk-canary.mjs --run <cellDir> --scope-report <scope.json>
```

`{ "marker": "<scope message>", "ok": true, "writesAfter": [] }` passes only when
nothing was written after the marker; an unreadable report is `unobserved`.

Every check carries an axis (`lifecycle`, `scope` or `artifact`) and the report
groups them under `axes`, so a green lifecycle can never be read as passing
tests.

## Compaction visibility

`compaction_attempt` (reason, `tokensBefore`, effective
`reserveTokens`/`keepRecentTokens`) is recorded only when the host reaches
`session_before_compact`: small-session skips happen before that hook, and an
earlier extension's cancel can short-circuit its delivery.
`compaction`/`compaction_failed` carry the terminal outcome (`fromExtension`,
`aborted`, `willRetry`).

## Offline probe fixture

`bench/prewalk/fixtures/prewalk-contract-probe.ts` registers the offline scripted
provider `prewalk-probe` (`main`, `executor`): plan + status request, one
complete two-write batch (the mutation boundary), three competing steers (the
last one a read-only scope steer), then scripted executor turns. Environment
knobs: `PREWALK_PROBE_REQUESTS` (request log),
`PREWALK_PROBE_EXECUTOR_DELAY_MS` (hold executor responses open for Escape
probes), `PREWALK_PROBE_EXECUTOR_FILLER_KB` (padding for default-budget large
sessions). Synthetic usage is estimated with the host token estimator so
compaction calibration is realistic; executor streams honor abort signals with
exactly one terminal event; the fixture never resets the model, so a reload
failure can never be masked by the fixture itself. Scripted responses prove
control flow only — never model obedience or task quality.

## Evidence finalization

`bench/prewalk/finalize-prewalk-evidence.mjs` closes an evidence archive in a fixed
order: a successful `preservation.json` (`ok: true`) and a non-empty
`report.md` are prerequisites; then it writes a once-only `manifest.json`. It
hashes regular files only and records special entries (dead tmux sockets,
FIFOs, symlinks) under `skipped` with their type instead of trying to open
them, self-verifies every digest before committing, and reads the manifest
back. It refuses a missing or not-ok preservation report, an empty report,
and an existing manifest; it never modifies anything else in the archive.
`--verify` re-checks an existing manifest and exits nonzero on digest or
coverage mismatches without writing.

```sh
node bench/prewalk/finalize-prewalk-evidence.mjs --archive <dir> [--verify]
```

## Real-task pilot (spend-gated)

A bounded matched pair on one real task, using the maintained tools only:

- Both cells share source and build (identical `dist` runtime hashes), host,
  main/executor models, thinking level, settings, prompt file and task
  fixture; the only difference is Prewalk in-place: the baseline cell's workdir
  `.pi/fabric.json` omits the `prewalk` block, the Prewalk cell keeps it. Never
  alter global settings.
- Run with `bench/prewalk/prewalk-canary-run.mjs --request-contract <contract>` and
  verify each cell with `bench/prewalk/verify-prewalk-canary.mjs`.
- Record semantic success first (task checks plus workdir drift through
  `snapshotTree`/`compareSnapshots`), then per-model tokens, latency and cost
  through `analyzeCell`/`aggregateUsage`/`attributePhases`.
- No paid execution before the user sets an explicit spend cap. Do not feed
  live-cell JSON to `bench/prewalk/compare-prewalk-runs.mjs`: it compares the
  synthetic queue/drift benchmark schema. A comparison against the published
  blog is not established until a matched real-task pair exists.

## Limits

RPC mode is not keyboard Escape: Pi documents Escape as clear-queue followed by
abort, so TUI interruption is a separate scenario. After a keyboard abort,
queued steers are restored into the editor — type commands only after clearing
it, and right after a rapid post-abort sequence the first Enter may need one
retry. One recovery cell (or one performance pair) is indicative, not
production-readiness proof.
