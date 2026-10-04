# Mixed-release legacy renewal cadence (smarty-dev#4383)

## Acceptance ledger

- Read the exact old `6b15d905` and RC2 `e7bc3f24` implementations, not an inferred rate.
- Count successful `state.json` atomic replacements and bytes in an isolated mixed fixture.
- Attribute commits to each Main and inspect the batch keys (session/host versus participant changes).
- Preserve the old session-only reader's fixed 15-second expiry; never infer a 26-second safe interval from aggregate I/O.
- Prove the half-life boundary, paired commit, default heartbeat cadence, and lack of participant rewrites.
- Run the requested targeted suites, typecheck, lazy graph, and fresh build.

## Source findings

At **both** revisions, `ParticipantDirectory` defaults to a 5,000 ms heartbeat and a
15,000 ms lease. Without `topology/liveness = { version: 1, hostLeases: "files" }`,
its unchanged refresh skips while the stored host has more than half its lease
remaining AND the legacy session has more than half its fixed TTL remaining.
The threshold is **7,500 ms**, not 26 seconds. On the normal heartbeat this means
one shared-state renewal at 10, 20, 30, ... seconds: **six commits per minute**.
Session and host are already operations in the **same** `writeBatch` at both revisions;
there is no separate session-only heartbeat commit to coalesce.

The old `isLiveLegacyRootEntry` uses `now - entry.updatedAt > 15_000`.
It does **not** consult a stored TTL, `value.updatedAt`, or `value.expiresAt`.
A session-only old reader is live exactly at 15,000 ms and stale at 15,001 ms.
The old directory also supports native participant records and matching **host**
lease files; native participant visibility is not equivalent to session-only visibility.

An important policy difference: old `6b15d905` honors the explicit host-file policy
and only renews unchanged shared records every **600,000 ms**. RC2's #453
compatibility negotiation overrides that policy while a live peer lacks the new
`livenessLeaseFiles: 1` advertisement, restoring half-life legacy renewal.
Thus a policy-enabled host can see a genuine increase from 0.1 to six idle
commits/minute. That is **not** a change from a 26-second old session-renewal timer.
Blindly restoring the old policy exception can leave session-only peers stale;
it is not a safe universal compatibility fix.

## Reproducible isolated benchmark

`compat-cadence-bench.ts` in the kept task outputs imports archived old source and
current source. It uses their actual stores/directories, ten old and ten RC2 Mains
on one isolated root, real session/host/participant publications, approximately
2.35 MB of state, and a virtual wall clock advanced through 120 seconds of 5-second
heartbeats. It counts **successful state.json renames**, reads the serialized
byte count, attributes each renewal to the active Main and its batch keys, and
checks the old session-only reader. Startup and teardown are excluded.

This measures protocol cadence and logical state-file bytes, **not** multiprocess
CPU contention, real-time scheduling, kernel block writes, or full-Main ancillary writes.

| Fixture | Old commits/Main/min | RC2 commits/Main/min | Old state bytes/Main/s | RC2 state bytes/Main/s |
| --- | ---: | ---: | ---: | ---: |
| Default policy, 20 mixed Mains | 6 | 6 | 235,001.09 | 235,001.10 |
| Explicit host-file policy, 20 mixed Mains | 0 during two-minute window (0.1 long-run) | 6 | 0 during window | 235,000 |

The default-policy initial state is 2,350,003 bytes. Its 0.01 byte/s difference is
serialized revision-digit growth from alternating writers, not an extra write;
the commit cadence and paired keys are identical. Every renewal contains exactly
`sessions/<self>` plus `topology/hosts/<hash(self)>`; no participant record renewal.
There are zero stale old session-only observations in the default-policy fixture.
Under the explicit policy the old writers' own legacy sessions lapse (210 sampled
stale observations); RC2 keeps its own compatibility sessions refreshed.

## Light's canary numbers and disposition

Light observed reads **6.6 -> 1.0-1.3 MB/s** and writes **91 -> 277-278 KB/s**
on Ryzen 1, with around 100 old Mains and a 2.35 MB state. The isolated fixture
**does not reproduce a 3x per-Main commit-cadence increase without the file policy**.
The reported 91 KB/s divided into 2.35 MB yields an *effective byte-rate interval*,
not proof of a 26-second renewal scheduler. Source and measured paired commits
contradict that interpretation.

To reconcile the host observation precisely, the caller needs the canary's policy,
per-PID successful commit trace (RC2 `PI_FABRIC_COMMIT_TRACE` supplies keys, bytes,
caller and timestamp), measurement window/metric (logical `wchar`, physical
`write_bytes`, or net of cancelled writes), and delayed/skipped heartbeat or lock
outage evidence. Different policy, partial measurement windows, ancillary writes,
and contention/retry/event-loop delays are distinct possibilities; none is claimed
as established for the live host by this isolated task.

**No safe 26-second cadence patch is justified.** This change strengthens regression
coverage and documents the actual old contract. It deliberately leaves production
renewal behavior unchanged rather than weakening the fixed 15-second compatibility
requirement or pretending that the measured six-versus-six result is a write reduction.
