# pi-fabric PR #430 — completion-journal round 2

## Change

Reconcile upgrade-era completion claims that contain only `{rootId, sessionId}`. A consumed envelope is now retained while an existing claim is not yet eligible for retirement, preserving the last trusted legacy lane evidence for a later same-lane successor. Eligible claims are retired through the existing fresh namespace confirmation and versioned cleanup path; successor-owned legacy claims continue to recover their address from the envelope. Foreign-lane authorization is unchanged, and no cache was introduced.

The bounded asynchronous scan and `<16 ms` synchronous-slice behavior from round 1 remain intact.

## Real-Main benchmark (`fv2-430-r2`)

| metric | before | after |
| --- | ---: | ---: |
| max | 8,049 ms | 211 ms |
| p99 | 64 ms | 1.8 ms |
| fsyncSync | 660/s | 0 |
| envelopes left | 100 | 0 |

## Verification

| check | result |
| --- | --- |
| completion-journal | pass |
| agent-completion-flow | pass |
| completion-successor, named, named-native | pass |
| residency, residency-host | pass |
| root-inbox | pass |
| agent-manager | pass |
| atomic-write-durable | pass |
| typecheck | pass |
| lazy graph | pass — 38 files checked |
| fresh build | pass — artifacts and lazy startup graph verified |

Final regression total: **10 test files, 411 tests passed**. All required commands ran at nice(19).
