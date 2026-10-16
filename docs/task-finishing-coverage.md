# Task finishing acceptance

Owner: fabric-v2. Refs pi-fabric#722 and smarty-dev#4313.

| Decision | Unit coverage | Named E2E and owner | Remaining gate |
| --- | --- | --- | --- |
| C: unit tests supplement the user path, not replace it | `tests/process-finishing-events.test.ts`: worker close without tree exit; final-answer receipt; no finishing polling; killed-tree receipt; failed custody; legacy adapters. `tests/execution-exit.test.ts` and mocked child-tree tests cover event/deadline and signal contracts. | A lead Main's task whose child process outlives the Pi worker shows `finishing` until the child exits, then `done` (API status `completed`). Owner: fabric-v2. `tests/process-finishing.test.ts` covers native transport/manager; `tests/worker-finishing.test.ts` covers the built worker and no-polling census. | Hosted Linux killed-tree tests and Windows native helper checks; installed lead-Main UI proof. Local native probes cover only natural exit, with destructive signals blocked. |

The final-answer receipt still freezes input and preserves the exact sender's late-control refusals. It does not release wait/join or execution custody before captured exit. Unconfirmed cleanup returns a failure, never success, while retaining admission and files.
