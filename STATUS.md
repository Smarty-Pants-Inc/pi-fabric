# Fleet-freeze checkpoint — bridge membership refresh scope

Paused for Paul's 2026-10-19 23:53Z fleet freeze. No push, deployment, service change, branch/worktree recreation, or Git identity change.

## Done

- Code checkpoint: `a6d2b4e822aacaf3d4a1b5ac6ab76497536d0924` (`fix(mesh): WIP: scope missing-recipient refreshes`).
- `src/mesh/bridge.ts`: gate ONLY the outbound `not addressed across` lapsed-recipient refresh. Skip it for the existing handoff-aware native/other-link `reservedRecipients`, or a control command explicitly destined to null / another syntactically valid bridge name. This gate grants no routing authority.
- Unknown/absent destinations, malformed destinations, same-link discovery, inbound refresh, `#staleMembership`, and publish/ownership fences are unchanged. Work and ACK destination fields are not interpreted as command routing metadata.
- `tests/mesh-bridge-refresh-scope.test.ts`: real SQLite MeshStores and StoreBridgeSides, suppressed `subscribeChanges`, fake timers, `bridge.run()` notification mode, and manual `bridge.step()` while idle. Covers native/foreign exclusions, new same-link discovery, absent/malformed destinations, work/ACK fields, handoffs, native takeover, and a 1,000-event SQL-driver measurement.
- Fresh `bun run build` completed; compiled `dist/` is present (ignored by Git).

## Verification observed

Focused final-source command (bounded to 120 seconds):

```text
timeout 120s bunx vitest run tests/mesh-bridge-refresh-scope.test.ts -t '^(?!.*measures 1000)'
Test Files  1 passed (1)
     Tests  21 passed | 1 skipped (22)
  Duration  91.28s (transform 16.14s, setup 1.02s, import 17.11s, tests 68.41s, environment 0ms)
EXIT=0
```

The skipped test was deliberately excluded from this bounded freeze check. The pre-freeze full run subsequently completed the measurement successfully (below). The test emitted a MaxListenersExceededWarning from repeated fixture stores, without a failure.

Build result:

```text
Verified kernel artifact matches its proof sources and bridge.
build artifacts and lazy startup graph verified (53 startup files, 1245837 startup bytes, 15731 bytes headroom, 81 stable lazy entries, 181 chunks)
```

The build warns that local native helpers use the unpinned host gcc compiler; this is a local build, not a release artifact.

Pre-fix measured 1,000-event baseline (actual `DatabaseSync.prototype.exec` wrapper, counting the drain only, excluding fixture/event setup):

```text
refresh-scope: events=1000 syncPresence=1000 BEGIN_IMMEDIATE=2000 forwarded=0
```

That baseline failed the fixed-behavior assertion as expected. The pre-freeze after-fix full run subsequently completed:

```text
refresh-scope: events=1000 syncPresence=0 BEGIN_IMMEDIATE=0 forwarded=0
Test Files  1 passed (1)
     Tests  22 passed (22)
  Duration  379.98s (transform 17.42s, setup 393ms, import 19.03s, tests 357.36s, environment 0ms)
```

Both measurement runs used the same earlier 16 MiB per-event byte cap; the checked-in fixture corrects that to the normal 64 KiB cap (covered by the focused 21-test run, but its measurement has not been rerun). Counts include real 1,000-event drains and actual SQL transactions, not inferred call-site counts. The source gate was identical to the committed gate in the successful after run; the subsequent test changes only corrected the byte cap and renamed the native-takeover test to avoid overstating its seam.

## Still pending at this status snapshot

- The corrected 64 KiB fixture's 1,000-event measurement has not been rerun; the comparable before/after runs at 16 MiB completed with 1,000/2,000 -> 0/0 refresh/transaction counts.
- As of 23:59:30Z, the pre-freeze `bun run typecheck` is still active with only `$ tsc --noEmit` in its log. No passing exit status is claimed.
- Existing `tests/mesh-bridge*.test.ts` suites were NOT started before the freeze and remain unverified.
- Optional mirror-plan out-of-lock precompute was NOT implemented. Initial recovery, last-attempt fallback, all-link identity reservations, anti-spoofing, and CAS remain exactly as on the base commit.
- No independent fabric-v2 code/security review has occurred. This is a WIP handoff, not a ready-to-merge or delivery-latency-fix claim.

## Existing run custody / evidence

No new long run was launched after the freeze. A post-freeze focused run was capped at 120 seconds and completed. Attempts to stop the two pre-freeze test jobs through native `kill` were refused by the fleet PID guard (attributed to fabric-store); no bypass was attempted. Both test jobs have since exited naturally. The remaining pre-freeze typecheck reads source and writes only its log; no source edits continue.

Evidence directory: `/tmp/bridge-membership-evidence/`:

- `gate-before.log`: initial RED suite, including two fixture defects subsequently fixed (undefined JSON serialization and recursive spy binding).
- `counts-before.log`: completed baseline and expected failing zero-refresh assertion.
- `gate-focused.log`: final-source 21-pass bounded run.
- `gate-after.log`: completed pre-freeze full new-test run, 22 passed, including the after counts.
- `typecheck.log`: pre-freeze typecheck, still pending at snapshot.
- `build-gate.log`: completed fresh build and artifact verification.
- `*.pid`: launched command handles. Pending as of 23:59:30Z: typecheck bun PID 2610086, tsc PID 2610092. The baseline and after-test processes are no longer present. Inspect actual native state before any lifecycle action.

## Remaining work (requires a fresh post-freeze scope grant)

1. Consume the outstanding typecheck result and record its settled exit status.
2. Run the full new test file with its corrected fixture, then the existing mesh-bridge test files; do not run the whole repository suite.
3. Have fabric-v2 review the narrow gate, especially handoffs, stale ownership, and command-destination exclusion.
4. Separately assess the optional mirror-plan work, preserving all-link reservations and CAS; do not drop foreign rows as an optimization.
5. Rebuild after any future source change. The lead owns publication; this agent never pushes.

## Six-line draft PR description (WIP; not published)

1. What: bound outbound missing-recipient refreshes to traffic that could belong to this bridge.
2. Why: shared-log foreign/native traffic amplifies presence writes; https://github.com/Smarty-Pants-Inc/smarty-dev/issues/15305#issuecomment-6279623733 .
3. Why/limit: preserve all-link ownership protection; lock analysis https://github.com/Smarty-Pants-Inc/smarty-dev/issues/15305#issuecomment-6279784834 ; mirror precompute remains deferred.
4. Proof: real SQLite notification-mode tests 22 passed in pre-freeze run; 1,000 events: refreshes 1,000 -> 0, BEGIN IMMEDIATE 2,000 -> 0; final 64 KiB fixture focused tests 21 passed; build passed.
5. Risk: routing/ownership trust boundary; only exclusion skips a refresh, never admission; stale-membership and publish fences unchanged; not a proven complete latency fix.
6. Reviewer: fabric-v2; WIP until outstanding typecheck, final-fixture measurement rerun, and existing bridge suites settle green.
