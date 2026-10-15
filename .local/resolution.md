# PR #1070 merge resolution

- Merge base: `origin/main` at `0f5f9efb`
- PR head before merge: `9928588e`
- Repository: `Smarty-Pants-Inc/pi-fabric`
- Conflict file: `src/mesh/bridge.ts`
- Result: one textual conflict hunk; no design-choice blocker.

| Hunk | Ours (`HEAD`) | `origin/main` | Decision |
| --- | --- | --- | --- |
| `MeshBridge.#directionLoop`, retry-streak log (around line 2064 after merge) | Preserve the PR's `BridgeMirrorDeferredError` classification as `bridge mirror degraded`; retain the PR's existing WAL-cap/lock-timeout wording. | Use main's `meshBusyLabel(error)`, which reports the new #12036 `mesh incarnation pending` and snapshot-retry refusal labels, as well as WAL-cap/lock-timeout labels. | Kept both intents: `BridgeMirrorDeferredError` still logs `bridge mirror degraded`; all other retryable errors use main's `meshBusyLabel(error)`. This preserves #1070's remote mirror-liveness diagnostics while retaining main's #12036 refusal diagnostics. The surrounding retry-delay logic remains the merged result: mirror-deferred and WAL-cap errors use WAL-cap backoff, while the other retryable errors use lock retry backoff. |

| Post-resolution audit finding (same conflicted file, `RemoteBridgeSide` late mirror response handling) | The merged PR path tolerated only a late `FABRIC_MESH_LOCK_TIMEOUT` response. Main's #12036 agent can also return `FABRIC_MESH_INCARNATION_PENDING` and `FABRIC_MESH_SNAPSHOT_RETRIES_EXHAUSTED`, both retryable and already allow-listed when constructing the error; a late response with either code would otherwise close the transport. | Extend the existing late-response exception to all three allow-listed retryable refusal codes. Permanent late mirror errors still close the transport, while pending/snapshot/lock refusals release the mirror gate and trigger a fresh presence notification. This is a fail-closed correction to preserve main's refusal semantics, not a new authority path. |

No conflict markers remain. The non-conflicting changes from main (including #10391 lock-hold/fence work, #12312 short-prefix hold work, and #12036 incarnation-pending refusal plumbing) were accepted by Git's merge and were not manually altered. The post-resolution audit correction is limited to the conflicted `src/mesh/bridge.ts` file.
