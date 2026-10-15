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

# PR #1076: merge main 5619bb51 (fv2-1076n)

Why/fits: preserve reviewed cross-spoke delivery while incorporating main's remote-liveness safety; this unblocks reliable fleet messaging without changing either feature's scope.

- Reviewed PR parent: `787742428d66dec8c67e0339a10dbf6cec6e41e0` (`fv2-base-1076m`).
- Main parent: `5619bb51d99302a3f0ab626e5a8cbf35852098fc` (`fv2-main-5619bb51`).
- Operation: `git merge fv2-main-5619bb51`; no rebase or history rewrite.
- Main attribution: `git log fv2-base-1076m..fv2-main-5619bb51 -- <four topology/bridge files>` identifies #1070 (`3e16dcbb7`, source-clock mirror liveness/renewal) and #1171 (`b8cd63a50`, terminal-steer/directory fences). All textual conflicts below are with #1070's bridge/liveness changes; #1171's changes merged without textual conflict.
- Dependencies: copied the existing installed `fv2-1076m/node_modules` into this lane (no package install); its lockfile SHA-256 matches this lane: `abdb63b873a3395b66b082344b8d8fc9049e57c645cfab626ac739c0b7c04c26`.

## Per textual conflict hunk

| File / hunk | Reviewed #1076 side | Main side (PR #1070) | Combined behavior |
| --- | --- | --- | --- |
| bridge.ts / BridgeLease | Incarnation token in mirror effect | Adds source-clock `mirror` evidence and `mirrorLease` marker | Keep incarnation plus both source-clock fields. |
| bridge.ts / incarnationOf | Nonempty token via `.length > 0` | Equivalent nonempty string truth test | Keep `.length > 0`; behavior is identical. |
| bridge.ts / unlease effect | Root/identity/start/token ownership fence; legacy undefined tokens allowed together | Same fence expressed as exact token equality | Use exact equality, which preserves legacy undefined/undefined and rejects mismatched tokens. |
| bridge.ts / notification digest | Include other links' relay changes and token identity | Exclude volatile per-read `observedAt` from native content digest | Exclude observation time and retain relay changes in both digests. |
| bridge.ts / incoming host loop | Trusted-hub-only, one-hop re-export with origin retained | Read each host's source-clock observation; reject arbitrary mirrored imports | Retain trusted hub/bridgeVia admission and pass source observation into the unchanged clock translation. |
| bridge.ts / mirrored host value | `remoteHost` retains origin on re-export | Sampled hosts require `mirrorLease: 1` | Retain origin and bridgeVia plus sampled marker. The effect sample's remoteHost also names the origin so hostMirrorLease/hostLiveness can validate it. |
| bridge.ts / applyLease incarnation and cap | Start/token fence before effects | Same fence plus cap every sampled effect, including old outbox replay, to original translated deadline | Exact token equality and main's cap. |
| bridge.ts / replay guard | Only same-incarnation disk lease suppresses replay | Compare source observations rather than expiry for sampled effects | Require the same incarnation before applying main's observation/legacy-expiry ordering. Replacement incarnations remain writable. |
| bridge.ts / live effect ordering | Same-incarnation monotonic lease ordering | Sample observation/source-renewal dedupe, legacy expiry ordering | Apply main's sample/legacy ordering under the reviewed full same-incarnation fence. |
| bridge.ts / presenceArg | Admit only natives or a validated one-hop hub export | Preserve finite per-host source observedAt | Keep both admission and observation data. |
| bridge.ts / mirror dispatch | Set trusted `fromHub` and validate hub against side.peer | Anchor translation at RPC receipt before queue/lock wait | Pass `presenceArg(args, side.peer), receivedAt, true`. |
| bridge.ts / expiry scheduling | Include relay host expiries | Translate sampled source-clock durations to the local sync clock | Include all three host collections with main's translation. |
| bridge.ts / direction retry | Idempotency-body conflicts are permanent | Mirror-deferred errors use isBridgeRetryable (rather than store-busy only) | Body conflicts stay permanent; main's expanded retry classification handles all other errors. |
| control-plane.ts / admission | Frozen semantic body/fingerprint, duplicate-key conflict, pinned relay hub/event id | One mirror-renewal wait budget, source-lapse refusal, original destination/incarnation revalidation | Retain frozen input/relay capture and then perform main's bounded mirror admission before pending/publication. |
| control-plane.ts / publish start | Skip publication on cached settlement and pin event id | Reusable publishOnce for zero-append commit-time renewal retry | publishOnce retains pinned id; cached settlements skip it entirely. |
| control-plane.ts / publication data and renewal catch | Publish the canonical snapshotted body | Under-lock lease/incarnation revalidation; one bounded renewal retry with original admission timer | Keep the under-lock checks and renewal catch, returning the frozen commandBody rather than rebuilding from caller input. |
| types.ts / FabricHostRecord | `bridgeVia` trusted one-hop link | `mirrorLease` source-evidence requirement | Keep both optional fields. |

## Necessary non-conflicting bridge join

`relayPresence()` now attaches the hub-local observation time to its already bounded live projection. This is the same source-clock sampling main added to native presence: the second hop translates the hub's remaining duration, not an absolute timestamp from a different host. The first-hop expiry is never extended. `mirror.remoteHost` is the retained origin (not bridgeVia), matching the existing host-leases reader's origin binding. No new route, authority, retry budget or feature is introduced.

`participant-directory.ts` was auto-merged. Its origin/link validation from #1076 and main's source-evidence, stale-bridge renewal and directory lifecycle fences are retained; no manual edits were made there.

## Acceptance ledger and evidence

1. Merge commit has the reviewed PR head as first parent and supplied main as second parent; no reviewed commits rewritten.
2. All 17 textual conflict hunks preserve both behaviors above; no conflict markers or whitespace errors.
3. Targeted bridge/spoke/incarnation/relay, control-plane, participant-directory and remote-liveness suites pass under the requested 24 GiB / zero-swap scope and smarty-test-env.
4. Whole-program typecheck and fresh build pass under the same scope; startup <= 1,261,568 bytes, file count unchanged versus supplied main.
5. One independent Luna max audit of frozen resolution bytes only, with a 10-minute brief; verdict and gate counts are supplied in the handoff and `.local/` logs.

Gate commands and results are in `.local/gates.sh`, `.local/tests.log`, `.local/typecheck.log`, `.local/build.log`, `.local/gate-status`. Items 3–5 are checks to be established, not claims of completion in this document.
