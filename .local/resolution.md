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
| control-plane.ts / admission | Frozen semantic body/fingerprint, duplicate-key conflict, pinned relay hub/event id | One mirror-renewal wait budget, source-lapse refusal, original destination/incarnation revalidation | Retain frozen input/relay capture; authenticate a cached receipt before lease renewal. Cache misses retain main's bounded mirror admission before pending/publication. |
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
5. One independent Luna max audit of frozen resolution bytes only, with a 10-minute brief, plus the lead-authorized delta audit below; verdicts and gate counts are supplied in the handoff and `.local/` logs.

Gate commands and results are in `.local/gates.sh`, `.local/tests.log`, `.local/typecheck.log`, `.local/build.log`, `.local/gate-status`. Items 3–5 are final-candidate checks to be established, not claims of completion in this document.

## Authorized fixture compatibility and audit repair

- The first gate run preceded `dist/` and three CLI tests could not import the built entry. After building, all eight handoff tests passed. The full gate on `f687897c1d5f799ae10a3c6ae9c1694515761721` then passed: 33 suites / 731 tests, six existing skips, typecheck and build exit 0. Candidate and exact supplied main both measured 51 startup files / 1,261,478 bytes (90 bytes below the requested cap).
- Main #1070 correctly rejects a fabricated caller epoch absent from the directory. The lead authorized `f687897c1`'s fixture-only repair: assert that refusal with no append, then update the real origin participant file and mirror the actual replacement epoch before the valid recovery counterexample.
- The single merge audit (`deca6365c87e4e6ab898acbeb6a8aab6`, Luna max) found one P2: mirror-renewal admission ran before #1076's cached-ACK settlement. A validated cached ACK for the same owner/epoch/link needs no publication and must not wait for a bridge sample to renew. Report: `.local/merge-audit.md`.
- The lead issued a fresh exclusive grant at `f687897c1d5f799ae10a3c6ae9c1694515761721` for `src/topology/control-plane.ts`, the relay-safety test, and this ledger, superseding their freeze, and authorized one delta Luna audit (deadline extended to ~07:15Z).
- The repair first uses the existing cached-receipt verifier against freshly captured owner/epoch/link/body authority, including main's synchronous incarnation revalidation. A cache hit uses the unchanged settlement/skip-publication path. A miss performs the unchanged initial mirror admission; the existing second cache check still catches an ACK received during renewal. Commit-time publication fences and the single renewal budget are unchanged. The verifier's parameter type is narrowed to the authority/fingerprint fields it actually reads; no new public API or behavior is added.
- Regression/counterexample: the real receiving handler's ACK is carried through both links and cached at A, then A's mirror observation is made bridge-stale. The same-key retry must settle without appending or executing again; a distinct, uncached request under that same stale sample must still fail with `FabricPeerLeaseStaleError` and append nothing. The fresh-sample case remains alongside it.
- Recorded red before the source repair: the fresh case passed, the stale cached-ACK case failed (`.local/cached-ack-red.log`). After repair the complete relay-safety suite passed 26/26 (`.local/cached-ack-green.log`). Final full-gate and delta-audit outcomes are recorded in the handoff after their runs finish.

# PR #1189: merge updated #1076 (fv2-7397c)

Why/fits: retain replay-resistant bridge incarnation authority while incorporating the reviewed cross-spoke/source-clock work; reliable authenticated fleet messaging serves the mission without widening scope.

- First parent: `ad1c70100331e7eed5c50640a0c3915658f8e34c`.
- Second parent: `eedcf6d23990209e349193cc393bb95ab6c1781c` (`fv2-1076-eedcf6d2`).
- Merge, not rebase: all reviewed commits remain in ancestry.
- Physical dependencies installed locally with `bun install --frozen-lockfile` under the requested 24 GiB / zero-swap scope and smarty-test-env.

| Conflict | #1189 behavior retained | #1076 behavior retained | Resolution |
| --- | --- | --- | --- |
| `BridgeLease` | Superseded-token list, first-seen ordinals, recovery floor | Source-clock mirror evidence/marker | Union of fields. |
| Mirror host admission | Refuse superseded tokens before liveness/removal; merge local history; equal-token age floor | Trusted one-hop relay attribution; source-clock observation and translated deadline | Keep stale-admission check first, then source-clock liveness, and write both history and mirror evidence to state/effect. |
| `#applyLease` | Reject superseded pending effects; preserve same-token age floor; merge state/file/outbox histories before any no-op | Same-incarnation observation/source-renewal ordering and original sampled deadline cap | Select the newer permitted liveness sample without mixing its observation and deadline; clamp the selected sample, then repair history even if liveness is unchanged. Unsampled same-token effects retain liveness high-water. |

Auto-merged `host-leases.ts` and `types.ts` retain both sets of optional fields and readers. Acceptance: preserved parents; no markers; reported tokenless sequence plus allowed legacy/renewal/fresh-token counterexamples; requested targeted suites, types, build and startup budget; one frozen-head Luna max audit. Gate receipts and final counts will be in `.local/round-comment.md`.
