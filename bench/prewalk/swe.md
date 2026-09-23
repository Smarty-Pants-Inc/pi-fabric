# Native SWE benchmark tooling

Maintained scripts for the in-place Prewalk SWE-bench Pro dogfood work. They repair the harness defects recorded by the 2026-09-20 paired pilot (misleading no-op control gating, candidate failures hidden as invalid grading, patch capture blocked by a shared Git index lock, HTTP response headers counted as settled usage, and skipped schedule entries counted as executed pairs) without touching frozen experiment evidence or the legacy adapters under the experiment roots.

## Tools

| Path | Role |
| --- | --- |
| `bench/prewalk/lib/prewalk-swe-evidence.mjs` | Canonical control/candidate classification, request-lifecycle accounting, truthful attempted/skipped/graded counters, checkpoint resume planning and intent-to-treat pair comparison. |
| `bench/prewalk/lib/prewalk-patch.mjs` | Lock-safe patch capture through a private temporary Git index with isolated object writes; never touches the repository's real index, `index.lock`, HEAD or refs. |
| `bench/prewalk/prewalk-swe-run.mjs` | Config-driven coordinator around the existing native worker/grader commands: canonical preflight before any paid call, write-ahead checkpoints, lifecycle accounting, truthful terminal exits. |
| `bench/prewalk/prewalk-swe-recover.mjs` | Offline reclassification and lock-safe recovery of finished-but-ungraded attempts; regrades recovered patches with the experiment's own frozen grader; writes derived verdicts plus a receipt into a fresh directory. |

## Classifier guardrails

- Gold controls stay strict: a gold arm passes only when it is valid, fully resolved, and its test command ran.
- A no-op control is acceptable only with proof the test command ran **and** an evidenced expected compile/import failure of code the gold patch introduces (Go `undefined:`, `[build failed]`, Python `ImportError` / `cannot import name` / `ModuleNotFoundError` / `NameError:`), or observed failing required tests. Empty results, missing tools (`command not found`), broken build environments and unexecuted commands stay rejected.
- A candidate failure is attributed to the model patch only when preflight controls passed and the test command ran — including candidate `SyntaxError` with zero collected tests. Otherwise it is unobserved infrastructure evidence, never silently dropped from a comparison denominator.
- Raw grader reports are never rewritten; derived verdicts are written next to the hashes of the inputs they consumed.

## Request lifecycle and budget semantics

- An HTTP response event proves headers only, never settled token usage. A provider request is settled by a finalized usage record (assistant end or compaction) or it stays unsettled.
- Unsettled requests hold a conservative per-model cap and keep the run uncertain; aborted requests keep their partial usage **plus** a hold. Duplicated or missing request identities are rejected rather than guessed.
- Budgets are monitored targets, not guaranteed provider-side caps (`hardCapGuaranteed: false`).

## Checkpoints and resume

- The coordinator writes a `starting` checkpoint **before** any paid command; an attempt with a checkpoint may never be issued again. `--resume` is required once checkpoints exist and only schedules never-started entries.
- The result row is persisted **before** the `finished` checkpoint, so a crash between the two leaves the attempt recoverable (`starting`) instead of silently dropping paid work that has no recorded result.
- Checkpoints, results and control verdicts go through a synced temporary file and an atomic rename, and the parent directory is synced as well on POSIX. Windows cannot fsync a directory, so there a power loss can still lose a just-renamed file, although it is never observed half-written.
- A deadline sends `SIGTERM` to the worker's process group and `SIGKILL` after `killGraceMs` (default 30 s); the coordinator resolves only after that escalation, so descendants holding the log descriptor cannot outlive the attempt.
- Started-but-unfinished attempts surface as `needs-attention` for offline recovery (`prewalk-swe-recover`), never as a re-run. Finished model work with a missing grade is recoverable, not runnable.
- Terminal states exit nonzero with an actionable reason (`needs-attention`, `budget-stopped`); counters separate processed, attempted, skipped, graded and executed pairs.

## Graceful deadline abort

`bench/prewalk/prewalk-canary-run.mjs --abort-grace-seconds <n>` (RPC only): on timeout the runner first sends pi's documented RPC `abort` command and waits a bounded grace for a clean exit, then escalates to the original hard process-group termination. The timeout still fails the cell; `finished.json` records `gracefulAbort` so partial evidence is labeled instead of manufactured. Without the flag the behavior is unchanged.

## Recovery usage

```sh
node bench/prewalk/prewalk-swe-recover.mjs --source <experiment-root> --out <fresh-dir> \
  [--attempt <id>] [--grade-python <path>] [--grade-script <path>] [--grade-timeout-ms <ms>]
```

`--grade-python` defaults to `<source>/work/.venv/bin/python` when present, otherwise `python3`. Recovery makes no model calls, removes no locks, and leaves the source archive byte-identical; all outputs (verdicts, corrected comparison, remaining-work plan, `report.md` receipt) are written only to `--out`.

## Explicitly unresolved

- Automatic Main review after handoff return is a product decision and is **not** implemented here; returning the selected model is not a review inference.
- Latency attribution and tuning still need phase decomposition (provider, tools/tests, planning, compaction).
- Remaining real-model in-place coverage: later scope changes, read-only steers, competing steers, cancellation/reload recovery.
- Historical LQ1 continuation-ordering and B2 drift-hash defects stay closed behind their existing regressions; do not reopen them without a new reproduction.
