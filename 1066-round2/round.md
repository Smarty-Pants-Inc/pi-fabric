# CI failure round 2 — pi-fabric#1066

Base under review: `4461c2a32978f7e274aa2d91ab4645ae9a24d271`.

Final round head: `7fb13ee177839666cd003b1819f2f11742e1cd6e` (`fix(agents): preserve hosted control contracts`).

Both hosted CI failtails (`job-115491825096-failtail.txt` and `job-115491825066-failtail.txt`) named the same seven tests across both operating systems. I reproduced each named path on the source head before editing. The 45-minute fresh grant covered the PR source plus `tests/agent-service.test.ts`, `tests/agents-provider.test.ts`, and `tests/control-plane.test.ts`.

## Diagnosis and disposition

| Failure | Classification | Evidence and change |
| --- | --- | --- |
| `agent-service.test.ts`: closes pending preparations and rejects unsupported requests even without discovery | Expectation update | The public hosted service now intentionally exposes the `result` alias alongside `wait`/`join`. The old exact descriptor list omitted that new public action. Added `result` to the expected list; no service behavior changed. The test still proves pending preparation is aborted and unsupported requests are rejected. |
| `agent-service.test.ts`: follows up a running child and routes session/peer delivery through host topology | Expectation update | Same intentional `result` descriptor addition. Added it to the expected public action list; routing, follow-up and topology assertions remain unchanged. |
| `agents-provider.test.ts`: exposes model-programmable residency on spawn and create | Expectation update | Remote capability spawn has an intentionally spawn-specific idempotency-key description: unchanged keys persist route/outcome across caller/owner restart and interrupted owner claims return `FABRIC_REMOTE_TASK_LOST`. Actor `create` retains its existing resident-local description. Replaced the old whole-object equality with shared shape checks plus the two intended description assertions. |
| `control-plane.test.ts`: rejects an interrupted durable claim as indeterminate after restart | Product regression | The remote-task extension changed the shared `interruptedAcceptance` error text for all operations. Restored the legacy exact message for ordinary control operations; remote task operations retain the additional no-replay guidance and typed `FABRIC_REMOTE_TASK_LOST` code. |
| `control-plane.test.ts`: expired owner case, claimed message without outcome | Product regression | Same shared-message regression. The ordinary follow-up error now remains exactly `Fabric control outcome is indeterminate after owner restart`; no retry/notRun semantics changed. |
| `control-plane.test.ts`: expired owner case (e), older runtime claim with no outcome | Product regression | Same source fix; the existing no-retry/no-notRun behavior is preserved and the legacy text is restored. |
| `control-plane.test.ts`: expired owner case (f), cached read missed later claim | Product regression | Same source fix; the existing indeterminate ACK remains, with the legacy message for non-remote control. |

The first six control expired tests were rerun by the named selection, including cases with and without recorded outcomes. The full control suite passed after the source fix.

## Verification

Commands used all ran through `systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0` and `nice -n 19`; no destructive test path was run.

- Targeted reproduction before edits: all seven CI names reproduced.
- Targeted repair suites: service 2/2, provider 1/1, control selected 10/10.
- Full granted suites after edits: `agent-service.test.ts`, `agents-provider.test.ts`, `control-plane.test.ts`, and `remote-capability-tasks.test.ts`: **4 files, 693 tests passed** (`.local/out/round2-proof.log`).
- Typecheck: **PASS** (`.local/out/round2-typecheck.log`).
- Fresh build and artifact/lazy graph checks: **PASS** (`.local/out/round2-build.log`).
- Existing remote two-root proof remains covered by the full remote-capability suite.
- `git diff --check`: **PASS**.

No fixture changes were needed. The only test edits update expectations for public API/schema additions that are part of this PR; the product fix is source-only in `src/topology/control-plane.ts`. The final commit contains only these three granted files and the exact reproduced fixes.
