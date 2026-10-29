# Idle Pi mesh cost

## Result and acceptance

The 2026-10-23 private-mesh comparison reduces idle CPU and read calls, but **does not meet the requested 20× reduction**. It is an incremental fix, not proof that an idle Pi performs no mesh I/O.

One actual headless Pi SDK `AgentSession` loaded the compiled Fabric extension in each sequential run. Each private mesh exposed 100 live participants, 10 logical owner hosts and about 3,000 keyed records before and after the window. A separate parent renewed the nine fixture hosts' lease files every five seconds: 24 passes in each 120-second window. The measured Pi had no turns, inbound work, provider calls or child processes during the window. This is not a live fleet or a TUI rendering benchmark.

| Measured child-PID counter | Base `0c03497f66088f9a33184bc4b2f010de45a6f9af` | Candidate | Reduction |
| --- | ---: | ---: | ---: |
| CPU cores | 0.005166 | 0.002749 | 46.8% |
| Read calls/s (`syscr`) | 44.20 | 26.54 | 39.9% |
| Bytes read/s (`rchar`) | 2,249.58 | 1,731.89 | 23.0% |
| Window, seconds | 120.0222 | 120.0228 | — |

CPU is delta(utime + stime) / `CLK_TCK` / monotonic elapsed seconds. `syscr` counts read calls, not all filesystem syscalls; it excludes `stat`. `rchar` includes page-cache reads and is not physical-disk traffic. CPU totals were 62 and 33 scheduler ticks, so this single pair is not a precise estimate of a small effect. Both children shut down orderly with exit 0; both enclosing command receipts also exited 0.

The original live profile measured a different loaded Fabric release, `c42f7f34125abe2ebbe660758253c9ec4d7765dd`, and a different mesh. Its 0.083-core / approximately 1,445-read-call/s result is **not** the before value in this comparison. Do not divide it by the private candidate to claim a 20× win.

## Frame and driver map

The historical profile's complete chunk URLs identify the loaded Fabric release. Its three hot chunks map to these current source families:

| Historical frames | Current source | Driver / interval / purpose |
| --- | --- | --- |
| `chunk-ZH7NZUW5.js`: `#select`, `#current`, `applySqliteDelta` | `src/mesh/state-backend.ts` | No timer of their own. Directory/state readers validate the SQLite epoch/commit and update their snapshot from the delta feed. Prefix selection walks the snapshot. |
| `chunk-EJY7LIL3.js`: `list`, `participantFromEntry`, `#liveHosts`, `hostFromEntry`, `#reportRootCollisions` | `src/topology/participant-directory.ts` | Default **5 s** participant heartbeat publishes/renews local presence and confirms writability. Compatibility checks could enumerate every host lease up to three times per pass; collision invalidation could trigger another full listing after ordinary peer renewals. |
| `chunk-IMJ7RNXY.js`: `readHostLeases`, `cachedLease` | `src/topology/host-leases.ts` | No refresh timer. Every caller validates the lease directory and stats every lease; changed files are read and parsed. The cache previously saved parsing, not enumeration. |
| Lifecycle `#poll` / `#drain` | `src/lifecycle/broker.ts` | Startup, explicit operations, state-projection changes and broad event/presence/lease notifications. Healthy operation has no interval, but unrelated fleet lease notifications repeatedly scanned all subscriptions. Failed work backs off to 30 s; exhausted watch recovery has a degraded-only 60 s carrier. |
| `presencePass`, `MainInboxMaintenance.run`, bridge alarms | `src/fabric-runtime-state.ts`, `src/topology/stall-alarms.ts`, `src/topology/bridge-alarms.ts` | Existing **25 s** Main maintenance performs two fresh directory listings plus a lease scan. Separate resident maintenance defaults to 60 s. These reads remain. |
| Actor `beforePoll` | `src/actors/mesh-monitor.ts`, `src/actors/manager.ts` | Event-driven with a healthy **5 s** safety net; failed watches use `actorPollMs` (default 250 ms). Empty-actor and scoped ownership reuse already existed in the historical release. Do not attribute the lifecycle hot drain to this branch. |
| Pending control lease watchdog | `src/topology/control-plane.ts` | Default **250 ms**, only while relevant requests are outstanding. Captured remote-owner checks currently scan fleet leases; this is not a permanent empty-idle poll. |
| Root inbox / UI | `src/topology/root-inbox-wake.ts`, `src/ui/controller.ts` | Already event-driven in the historical release. Inbox timers are exact held-work deadlines. UI's default 500 ms is a throttle, not an idle refresh interval. |

## What changes

- The directory's existing structural state/file/local-root signals can prove that lease changes cannot affect a negative compatibility answer or produce a new collision. Only then may the memo omit the all-host lease read. The proof considers stale candidates and both file/state copies, not merely the currently live listing.
- Any incompatible reader or possible colliding candidate retains lease-sensitive invalidation, including lease-only revival. Unknown data does not become authority. The existing full collision safety pass remains at one lease period (normally **15 s**).
- The lifecycle drain reuses canonical subscription entries only when SQLite's existing namespace change-feed proof establishes that no subscription changed. Revision capture precedes the listing and every delivery await. Unknown revisions, incomplete feeds and other backends retain canonical scans. Every drain still checks target publication, ownership, liveness and delivery gates.
- No timer, lease window, routing API, publication/confirmation/CAS fence, bridge implementation or `host-leases.ts` code changes. Explicit refresh/routing reads remain fresh. A file-only renewal or routing read still does not grant consumption or advance the shared-write confirmation receipt.

## Correctness evidence

- `tests/idle-participant-directory-fast-path.test.ts`: real file/SQLite state with 96 renewing modern peers performs zero foreign lease stats and zero collision listings before the safety pass, while retaining 5 s renewals and 15 s leases. Old host/session/participant joins, expiry, lease-only revival, shadowed incompatible files, duplicate-name/session revival, registration/rename and fresh routing reads are covered. Reintroducing fleet enumeration or skipping candidate-sensitive collision checks makes the respective negative controls fail.
- `tests/idle-lifecycle-subscriptions.test.ts`: native FILE/SQLite two-broker flows cover lease/state churn, state-only subscription join/cursor mutation/delete, target revival/local ownership, gate reopening and a join during awaited delivery. SQLite makes zero additional full subscription scans while that namespace is unchanged; FILE keeps its conservative scans. Unknown revisions and a trimmed feed fall back to canonical reads.
- Directory checks: 351 passed, one existing skip across 11 suites; final new/idle suites: 28 passed. Lifecycle focused checks: 48 passed, **one explicitly filtered existing cross-process test**: `tests/lifecycle-state-projection-wakes.test.ts` → `a subscription committed by another process wakes the receiver through state.db notifications, with no event-file write`. Its child exits 0 with `committed\n`, but line 261's exact-empty-stderr assertion receives `hosted-only (outside-temp deletion reporting): push and let CI run it` before the delivery assertion. The same failure was reproduced on an otherwise tracked-clean detached checkout of the supplied main base `0c03497f66088f9a33184bc4b2f010de45a6f9af`, with the same dependencies and test-environment wrapper. This is not claimed passing; hosted CI must run it unfiltered.
- `tests/idle-remote-send-freshness.test.ts` plus three selected existing cases: **19 passed**, including 16 new file/SQLite cases. Two private meshes exercise the actual bridge, directory, control admission/ACK and Main-controller path after the structural caches warm. Lease-only renewal/shortening remains fresh; expired, removed, foreign-owner and changed-incarnation mirror leases refuse publication. A mismatched native control activation is fenced before Main admission. Valid sends admit once, and the existing captured-ACK counterexample remains allowed. This proves component flow, not a live model turn or a physical-host/SSH route.
- An extra same-idempotency-key replay experiment timed out after its successful initial admission on both backends, with no second Main admission observed. It remains a separate unresolved bridge-receipt limitation; no bridge code was changed, and no broader duplicate-replay guarantee is claimed.
- The candidate build, artifact/lazy-graph assertion and whole-program typecheck passed. No full test-suite sweep was run.

## Reproduce and inspect

Use `scripts/benchmark-idle-mesh.mjs --help`. Unlike the older CPU-profile-oriented `bench/participant-refresh-idle.mjs`, this harness records child-PID `/proc` counters and verifies actual native directory membership at both window boundaries. It preserves all private profiles, native session snapshots, receipts and logs.

Prepare immutable built roots for the supplied base and candidate, each with `dist/`, `package.json`, `skillsets/`, `native/`, `config/` and installed `node_modules/`. Baseline additionally needs `source-head` containing the exact base SHA. Use the same native Node ≥24, installed Pi package, dependencies and unchanged harness for both runs. Neither run imports editable product `src/`. Use an owned `smarty-reap` command with a finite deadline and consume its actual terminal receipt, not merely the printed launch PID.

```sh
node scripts/benchmark-idle-mesh.mjs --fabric-root BASE_ARTIFACT --label baseline --out BASE_RESULT --backend sqlite --scenario renewal --warmup-seconds 30 --seconds 120
node scripts/benchmark-idle-mesh.mjs --fabric-root CANDIDATE_ARTIFACT --label candidate --out CANDIDATE_RESULT --backend sqlite --scenario renewal --warmup-seconds 30 --seconds 120
node scripts/benchmark-idle-mesh.mjs --compare BASE_RESULT CANDIDATE_RESULT --out COMPARISON
```

`--startup-only` is a real native-startup/config/membership check, not a measurement. `--self-test` checks counter math and structured-output parsing, not performance. `--scenario static` is a separately labelled quiet control. The renewal scenario contains no unrelated keyed-state churn or active peers; do not generalize its rates to a complete fleet.

Lane evidence is retained in `.local/idle-mesh-baseline-renewal2.json`, `.local/idle-mesh-candidate-renewal.json` and `.local/idle-mesh-comparison.json`. These contain the immutable artifact manifests, exact runtime and harness hashes, private evidence paths, raw counter samples, membership proofs and renewal counts. The raw historical profile and the full driver map are retained with the lane handoff. The failed first measurement is preserved separately and excluded: its final tool-output parser included a trailing native diagnostic. The corrected parser uses the tool's declared JSON line range; both accepted windows used the corrected, identical harness.
