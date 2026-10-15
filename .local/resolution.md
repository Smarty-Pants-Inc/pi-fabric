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

# fv2-12486m: merge current main into the audited #12486 fix

- First parent: `02ac48c222989a5c87d4bd99f8dff28713029075` (`fv2-base-12486m`), retaining both reviewed fleet-bot commits.
- Second parent: `4249647665236f3121a13fa84b47acc5c3396761` (`fv2-main-42496476`). Merge, not rebase.
- Main provenance: PR #1070 (`3e16dcbb7`, remote source-liveness and unified mirror renewal wait, including PR #1136's peer-lease-stale contract); PR #1180 (`424964766`, authenticated owner-root-gone tombstones). The earlier #1070 resolution above is retained history, not this merge's conflict list.

| Conflict hunk | Audited #12486 side | Main side | Resolution and behavioral checks |
| --- | --- | --- | --- |
| `host-leases.ts`: `hostLiveness` | Export `hostLeaseMatches` and require matching id/root/identity/incarnation before accepting a lease; native fallback also uses this check. | PR #1070's `HostLeaseOwner`, `matchingHostLease`, `hostMirrorLease`, and source-aware mirror TTL translation; missing required observation is doubt, not source death. | Keep the exported strict matcher and use it inside `matchingHostLease` so both mirror provenance and ordinary liveness share the same fence. Retain all main mirror/source-clock and missing-observation branches. Check native fallback + custody/CAS negatives and mirrored-root/remote-renewal cases. |
| `participant-directory.ts`: imports | Exact Linux process-birth observation plus the older inline mirror-wait imports. | PR #1070 moves mirror waiting into `waitForMirroredLeaseRenewal` in host-leases. | Keep `processStartTime`; drop only now-unused `watchBridgeStore`, `FabricPeerLeaseStaleError`, and `PEER_LEASE_WAIT_MS` imports. Main's shared wait remains intact, no second wait/timer. |
| `participant-directory.ts`: `list` stale predicate | An expired local Main remains reachable only with positive exact native birth, current matching lease custody, permitted status, and no terminal closure. | PR #1070 distinguishes remote source lease-stale from bridge-stale; remote transport doubt remains visible, not falsely dead. PR #1180's terminal filtering auto-merges around this hunk. | Dispatch on `owner.remoteHost`: remote uses main's source status; local uses expiry AND failed native-root proof. No remote owner can take the local native path. Check local live/dead/reused/missing-file cases plus source-live/bridge-stale/source-dead mirrors. |
| `participant-directory.ts`: `get` fast path | Same exact-birth local fallback, with no admission/confirmation extension. | PR #1070 uses normalized owner liveness and retains bridge-stale remote routing for its bounded source-aware wait. | Remote keeps `status !== lease-stale`; local accepts normalized `owner.expiresAt >= now` OR exact native-root proof. `#liveHosts` already applies `hostLiveness`, avoiding a second effective-liveness pass. Keep identity/root checks and main's bridgeStale result flag. Check the same get/list counterexamples and real router/ACK tests. |

Non-conflicting merge paths retained: the single heartbeat's independent admitted file renewal; native captured-ACK authority recheck; receiver's present matching lease-file requirement; main's one-budget notification-driven remote wait and control-plane mirror attribution; authenticated terminal actor projection/retention. No executor defaults, admission, consumer confirmation, lease budgets, or startup budgets changed.

