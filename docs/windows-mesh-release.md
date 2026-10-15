# Windows mesh lock release

Issue: smarty-dev#9569. This is the **release slice**, not a whole-root Windows safety claim. Acquisition recovery, legacy canonical-debris repair, the mixed-version gate fixture and macOS characterization are a separate change. Do not advertise the stronger root-wide guarantee until those gates pass.

## Release contract

A Windows holder prepares a digest-pinned native capability **before publishing its owner**. The release closure retains that capability and the original complete owner record. There is no Windows pathname-rename or recursive-delete fallback when the helper is unavailable.

The native release checks exact directory and owner identities, owner bytes, ownership and location while holding the directory without delete sharing. A canonical lock must initially contain exactly `owner`; an extra or unknown entry refuses release with the owner intact. The helper closes the verified owner read handle, but keeps the directory guard, then renames the **held directory object** to the same-parent `r.<64hex>` name without replacement. This rename is the commit point. Only afterward may it dispose the verified owner by handle and remove the retired directory if empty. Cleanup never resolves the canonical lock again.

A late unjudged child is not deleted. It may leave a reported retirement sibling, but it cannot block the next canonical acquisition. Foreign collision targets are not replaced or removed; an existing exact original retirement receipt may be reconciled as described below. Linux's directory-fd-pinned release is unchanged. macOS retains its weaker pathname-based behavior: this change does **not** provide Mac parity.

## Outcomes and application errors

A terminal native result distinguishes `not-owner`, `released` (clean or cleanup pending), and `refused-before-retirement`. The caller additionally distinguishes a helper that is unavailable before start from `release-outcome-unknown`: a started child without a valid terminal result **and** matching process exit might already have committed the rename. Empty output, a timeout or a nonzero exit is not proof that nothing happened.

After joining an unknown child, the caller may make one bounded reconciliation attempt using the **identical original framed request**, including original directory and owner snapshots, full record and retirement destination. It does not resnapshot a successor or infer completion from an absent canonical name. It never replays the application operation. A surfaced `MeshLockReleaseError` has code `FABRIC_MESH_LOCK_RELEASE_FAILED`, explicit operation and release states, `retryable: false` for application replay, and `retryRelease()` for release only. `operationCompleted` means the operation returned; an operation that threw may still have side effects. A primary operation error stays primary, even for non-Error throws; release failures are secondary and observable. Nested fences attempt both releases.

## Same-record reconciliation after interruption or parent death

A repeated native request first opens its deterministic retirement destination with DELETE access and no delete sharing. It verifies the **original directory identity**, real-directory kind, SID and final location under held ancestors before any cleanup. Only confirmed absence permits the ordinary canonical release path. A different directory, reparse point, occupied foreign target or unreadable target fails closed. An authenticated retired directory resumes the existing W5 cleanup; this branch never opens the canonical lock, even if B now owns it.

W5 requires the full original owner bytes and exact owner identity, time and size before deleting that owner by handle. Alternatively, the held original directory must be empty, in which case no file is deleted. A rediscovered empty receipt uses zero owner ids/time (the request still contains full R and its length); if an owner appears before native inspection, that request has no file-delete authority. Empty-only directory disposition preserves any late unjudged entry. Extra, foreign, torn or otherwise unverified content is retained and reported pending/refused, never recursively removed. The existing watchdog and per-mutation deadline checks apply to resumed cleanup too; every child remains bounded and joined.

Parent death loses the JS request. An **explicit release-only call with the original full record** can rediscover it without a cache, background collector or new durable file protocol. Only when canonical state is absent or its record differs does JS inspect the same parent nonrecursively with `opendir`, capped at 256 entries (at most 257 reads to prove completion/cap exhaustion). Normal matching canonical releases do not scan. A candidate must have the exact `r.<64 lowercase hex>` name equal to the existing hash of canonical basename, that candidate's bigint directory identity, and original full record. Arbitrary empty `r.*` siblings and nonmatching receipts are not candidates. Exactly one identity-bound real directory may be judged: a stable full original owner snapshot, or two empty observations of the same directory. Its contents are also boundedly inspected. Native still performs the authoritative held-handle checks.

Multiple matching receipts, an exhausted listing cap, changed proof or inaccessible state yields an explicit operator refusal before a child starts; nothing is guessed clean. A successful retry cleans only the original T, leaving B's full owner and both identities untouched. Repetition after T is gone returns `not-owner` without starting a child. This is original-receipt release authority, **not** PID/death-based reclaim authority for live, foreign or protocol-unproven records. Pending cleanup is not a claim that T was removed.

## Retained helper cache and package maintenance

The per-user, non-temp cache is outside the package's `dist/` tree. Each published digest directory contains one immutable helper build. A trusted packaged bootstrap (or an already established retained capability) checks exact bytes, regular-file/no-reparse status, real-directory ancestry and Windows owner/DACL privacy before that executable is used. An unvetted cache executable must never validate itself.

New versions publish from private staging without overwriting an existing digest directory. Concurrent publishers verify the winner. A running holder captures its exact absolute path, digest and mesh ABI; each spawn rehashes that path. Package rebuild, update and uninstall may remove package `dist/` but **must not remove published digest cache directories**. Old and new helper digests coexist. There is no automatic cache collector.

To collect the user-wide helper cache, first stop admission and automatic restart from **all roots and launchers** using that cache, drain all resident Fabric processes and holders, and join their terminal process exits. Only then may an independently authorized maintenance operation remove cache versions and restart admission. An empty canonical lock or a PID snapshot is not a global drain. A per-root writer drain is not sufficient for this user-wide cache operation.

Deleting or rewriting a retained digest during a live section is unsupported tampering or undrained maintenance. Release fails closed; restore the exact trusted bytes and use the same-record release retry, without repeating application work.

## Windows rollout restriction

All updated owner mutators must obey the directory-guard rule. A paused old pathname-based releaser can still damage a new holder; installing a new helper does not stop it. Before enabling a stronger guarantee for a shared root, stop every writer launcher and auto-restart path, inventory holders/waiters/recoverers (including paused ones), and join all old processes. Install the updated build, verify its exact helper capability, and pass the installed release/recovery canary on a test root. Restart only updated writers. Incomplete inventory keeps the gate closed. Rolling back to an old writer repeats the drain and withdraws the stronger claim.

The executable mixed-version gate and acquisition-recovery work are not delivered by this release slice. Until they land, this document is a restriction, not a claim that a root passed the rollout gate.

## Protected historical state

Automatic cleanup may apply only to empty directories or records whose death is independently proven under the current namespace policy. Recordless nonempty, torn, live, foreign and unproven canonical state is never garbage merely because it is old or has a release-looking filename. Protocol-3 Windows PID evidence is not sufficient for reclaim.

Unknown canonical debris requires typed operator handling and a quiesced quarantine: stop and join affected writers, preserve a forensic snapshot, verify root identity and no reparse points, and obtain authority to quarantine the exact reported directory. Do not use an unattended recursive deletion. Sibling `r.*` and old `.released.*` artifacts do not gate a free canonical lock. Retained `.dead.<64hex>` recovery fences are not release garbage and must not be collected.

## Evidence gates

Hosted Windows CI on the exact PR head is required for actual sharing, handle rename/disposition, delete-pending behavior, process-kill partitions and the retained-capability update boundary. Linux skips are not Windows evidence. Report helper child-process p50/p95 and full-operation serial/contended measurements in the PR, without a flaky millisecond correctness threshold. The installed Ryzen 1 mesh smoke and independent acceptance audit remain delivery gates.

The logical normalized absolute mesh path must be under 240 UTF-16 code units, excluding only the Win32 namespace transport prefix and terminating NUL. Namespaced API arguments are measured separately; a longer API argument is not silently counted as a shorter logical path. No digest or ownership token is truncated to fit.
