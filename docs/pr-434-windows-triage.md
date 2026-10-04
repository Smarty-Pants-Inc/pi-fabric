# PR #434 Windows failure triage

Baseline: main `eebea3820b6d70564e1da96aeeb47e2707ba4923`.
Investigated PR head: `d1d8c373e3bc159f7aae9014d512c7c20f79f71e`.
Main is already retained by merge `80061198`; no second merge is needed.

| Test | Classification | Action |
| --- | --- | --- |
| `residency-commit-fence.test.ts` / round 1 public cancellation contract / durable create never enters activation compensation when committed removal would be unknown | Pre-existing main timing flake, inferred from unchanged fixture and execution path; not a separately observed main Windows failure | Preserve assertions and the existing 200 ms deadline; record evidence, no PR code fix |

## Evidence

The supplied Windows log `jw434.log` identifies checkout `68185a21` as a
synthetic merge of PR head `d1d8c373` into `c14aee6a`. Lines 7334–7378 show
`ResidentOutcomeUnknownError` for **createActor**, with a committed actor ID
and a cause of `Timed out waiting for Fabric residency request`. The failure
is the expected durable-success assertion, not activation compensation or
committed removal. The test took 758 ms including setup/teardown; its command
deadline is only 200 ms.

`git diff eebea382...d1d8c373` does not change this test, the residency client,
actor directory/registry store, residency protocol, or atomic-write helper.
The complete `ActorManager.create` and provider `#createActor` methods are
unchanged. The fixture requests an explicit model, no route class, and no
actor activation. The PR's live-route selection and worker/archive durability
therefore do not execute on the request being timed. Host changes concern
model-routing overlays and route preparation, not the createActor handler.
The same 200 ms successful-create expectation and filesystem exchange exist
on main, so Windows scheduling/filesystem contention can produce the same
post-commit unknown outcome there. No main Windows run was accessed; this is
the execution-path reasoning classification, not a claim of a reproduced
main Windows failure.

Main #430's shared durability changes are already merged on both sides.
There is no demonstrated PR-added latency in this request to justify raising
its budget. A future main-owned fixture repair should separately control
successful-create setup and the intentionally blocked removal deadline,
without weakening ownership or compensation assertions.

## Targeted verification (ryzen2/Linux, nice -n 19)

- Three completed runs of the entire failing suite: 228 tests passed per run.
- All 13 PR-touched suites against main eebea382: 794 tests passed.
- Whole-program typecheck and lazy-graph assertion passed.
- Fresh build passed, including build-artifact/static-closure assertions.
- Exact-content comparison against main passed for the fixture, five shared
  exchange/storage files, and both complete create methods; fingerprints
  are retained in the task's external `main-path-comparison.json`.

The initial three-run wrapper's 540-second timeout interrupted its third
iteration after two successful full runs. That partial log is retained as
`residency-commit-fence-3-interrupted.log`; a separate third full run passed.
Linux checks do not constitute Windows revalidation. No assertions or budgets
were changed, and no artifacts are part of this evidence commit.
