# PR #462 Windows CI classification

## Acceptance ledger

- Verified starting HEAD: `474d7a1a4ff983390a8e935127bfbd4ce85e6248`.
- Main `eebea3820b6d70564e1da96aeeb47e2707ba4923` (#430, completion fsync) is already an ancestor, merged by `29b2ec5d`; both sides remain present. No additional merge is needed.
- Classify the reported Windows failure without weakening assertions or increasing budgets.
- Run the reported failing suite three times, all directly touched suites, typecheck, lazy graph, and a fresh build, at `nice -n 19`.

| Test | Classification | Fix / disposition |
| --- | --- | --- |
| `test-fleet-isolation.test.ts`: worker setup fences inherited fleet paths before initialization/config writes | Pre-existing Windows nested-process timeout susceptibility; inferred from unchanged baseline and the failing execution path, not a main Windows rerun | No production change or budget increase justified; preserve assertions and record evidence. |
| `actor-manager.test.ts`: ownership once per matching event with several subscribers/full queues | Additional pre-existing local ownership-call-count failure, reproduced on main `eebea382` | Preserve the `< 1_200` assertion; report baseline reproduction separately. |

## Windows evidence

Source log: `/srv/scratch/paul/tasks/direct/jw462.log`, lines 6859–6893.
The outer Node child exits 1 because `assertChildSnapshot` sees its nested child's
`status === null`, not 0, with empty stderr. That nested `spawnSync` has a 3,000 ms
timeout and only prints inherited environment variables; this is consistent with
Node startup timing out, not a failed isolation assertion. The original log does
not expose the nested child's `error`, so the exact OS-level cause is not proven.

The stack points to `[eval1]:64`, the second `assertChildSnapshot()` immediately
after importing the worker setup. It precedes the first real source import
(`src/core/agent-dir.ts`), including every source module changed by this PR.
No added completion durability or I/O is executed in this failing path.

`tests/test-fleet-isolation.test.ts`, `tests/fleet-isolation-setup.ts`,
`scripts/test-temp.ts`, and `vitest.config.ts` are unchanged between `eebea382`
and the starting HEAD (matching SHA-256s). The isolation test's hash is
`8f8b24969efb5233c32b2641ef1cc33278d37aafb440cda797f4c405cd9cb6a0`.
The `origin/main...HEAD` diff does not touch the test. Windows CI selected it as a
related test; main has the identical test and pre-source execution path, hence
shares this startup-budget susceptibility. No main Windows log was obtained:
this task prohibited GitHub access, so this is a baseline code-path classification,
not a claim that a particular main Windows run failed.

## Local checks (ryzen2)

All checks ran with `nice -n 19`:

- `bunx vitest run tests/test-fleet-isolation.test.ts`, three runs: each 3/3 passed.
- All 17 test suites directly changed by `origin/main...HEAD`: 16 suites passed,
  1 failed; 917 tests passed, 1 failed. The only failure was the ownership-count
  test above (1,468 calls against `< 1_200`).
- Isolated ownership test: starting HEAD failed with 1,480 calls; a detached
  `eebea382` scratch worktree with the same dependencies/runtime failed with
  1,536 calls. The scratch worktree was removed. The supplied Windows log shows
  this test passing (line 744), so this is not its reported Windows failure.
- `bun run typecheck`: passed.
- `bun run assert:lazy-graph`: passed (38 host-free UI graph files).
- `bun run build`: passed, including compiled artifact/lazy startup assertions.

Detailed evidence and logs are retained under
`/srv/scratch/paul/tasks/direct/fv2-462-win3/artifacts/`:
`classification-evidence.log`, `fleet-isolation-{1,2,3}.log`,
`touched-suites.txt`, `touched-suites.log`, `ownership-probe.log`,
`ownership-main-probe.log`, `typecheck.log`, `assert-lazy-graph.log`, and `build.log`.
No assertions, timing budgets, production sources, or artifacts were committed
as part of this classification task.
