# Mixed-release renewal cadence (smarty-dev#4383)

## What old `6b15d905` actually does

There are two different meanings of “session liveness”; the previous RC3 analysis conflated them.
All citations below are to the historical source, obtained with `git show 6b15d905:<path>`.

- **Raw legacy `sessions/<id>` fallback:** `src/topology/legacy-root-liveness.ts:4,17-24`
  fixes its TTL at 15 s and checks `entry.updatedAt`. It does not read a file or the
  policy. Old `src/topology/host-leases.ts:24-30` has only a host lease, not a nested
  legacy-session lease. So old does **not** renew the raw SESSION entry through files.
- **Native Main/session discovery:** old `src/topology/participant-directory.ts:965-975`
  implements `sessions()` by listing native roots, and `peers()` through `sessions()`.
  Its `list()` first populates native participants (`:556-580`); raw session fallbacks
  cannot replace a native participant (`:591-602`). Native host liveness is the later
  of shared-state and matching file expiry (`:674-683` and
  `src/topology/host-leases.ts:140-148`). `get()` uses the same file-aware rule
  (`src/topology/participant-directory.ts:824-835`). This is how one old Main sees
  another old Main's session remain live even after its raw fallback becomes stale.
- **Explicit `{ version: 1, hostLeases: "files" }` policy:** old
  `src/topology/participant-directory.ts:1325-1353` excludes the raw session from the
  renewal decision when `fileOnly` is true (`:1331-1342`). Its file is renewed every
  heartbeat (`:1414-1423`); unchanged shared host/session batches are skipped until
  the 600 s host policy threshold (`src/topology/host-leases.ts:15-20`). Old may still
  include a raw session put with initial publication or a genuine change
  (`src/topology/participant-directory.ts:1150-1175`), but it does not periodically
  renew that raw entry under the explicit policy.
- **Absent/default policy:** genuinely state-only fallback readers keep the fixed
  half-TTL threshold, 7.5 s (`:1340-1342`), resulting in a commit every 10 s on the
  default 5 s heartbeat. Host and session due records share one batch.

There is no contradiction and no need for six extra RC3 commits/minute under the
explicit policy. The fixed 15 s rule applies to the fallback, not to native Main discovery.

## Correction

RC3 now never publishes or renews `sessions/` under the explicit host-file policy,
including in a mixed fleet. A retained **owned** legacy session is deleted once when
switching to that policy. Native participant publication and file leases keep old
readers live. Genuine changes still publish immediately, the old-reader compatibility
host renewal remains at 600 s, and absence of the policy retains the old fixed session
half-life cadence. Switching back restores the legacy advertisement.

The existing all-capable-fleet path still renews only files. Participant-file migration,
per-key ownership/CAS checks, lock confirmation, and original lease TTLs are unchanged.

## Actual old-source benchmark

The retained harness imports both actual `6b15d905` and RC3 source, rather than assuming
an old baseline. It runs 10 old + 10 RC3 Mains sharing an isolated ~2.36 MB state with
5 s virtual heartbeats, and counts successful **state.json atomic renames** and the
full byte size of each renamed payload. Startup/registration is outside the measured
idle window; order is interleaved. A realistic seven-digit revision fixture avoids
incidental digit-boundary byte growth. Both releases write the same shared state.

| Policy / window | Old commits/Main/min | RC3 commits/Main/min | Old bytes/Main/s | RC3 bytes/Main/s | Old-reader stale native observations |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default / 120 s | 6 | 6 | 236,848.20 | 236,848.20 | 0 / 480 |
| Explicit host-file / 120 s | 0 | 0 | 0 | 0 | 0 / 480 |
| Explicit host-file / 1,200 s | 0.1 | 0.1 | 3,938.16 | 3,938.16 | 0 / 4,800 |

The strict RC3 <= old target holds for both commits and full state.json bytes under
**each** policy, including the explicit policy's long-run 600 s maintenance cadence.

## Mixed live test, not a TTL assumption

Separate Bun processes run an actual old writer, RC3 writer, and actual old reader
against each isolated policy root. Real 5 s timers run for at least **120 s**. Each
second, the old reader checks both Mains through `sessions()`, `peers()`, and fresh
`get()`. Both policies produced **0 stale observations**. Default policy wrote 12
commits and 27,969,204 bytes per Main; explicit policy wrote **0 commits and 0 bytes**.
The explicit-policy old writer's raw fallback was stale in **105** samples while its
native Main remained live in every sample. This directly resolves the old-Mains/zero-
commits contradiction. All child processes were closed and checked.

Evidence is retained under `/srv/scratch/paul/tasks/direct/fv2-compat3/artifacts/`:
`compat-cadence-bench.ts`, `bench.json`, `compat-live.ts`, `live-default.json`,
`live-files.json`, writer reports, minute counter JSONL files, and `old-citations.txt`.
The old checkout is reproducible with `git archive 6b15d905`; only private TMPDIR
roots were used and the scratch checkout is not part of the commit.

## Per-process counter for Light

Set `PI_FABRIC_COMMIT_STATS=/existing/private/directory/commits.jsonl` **before starting**
the Pi process. The opt-in is captured on module load; changing it later does not turn
collection on or redirect the sink. No credentials, strace, or fleet topology change
is needed. Use a distinct path per process, or share a JSONL file and distinguish `pid`.

Once per minute it appends one version-1 JSON line containing `pid`, `since`, `at`,
interval `commits`, `bytesWritten`, and `byReason`. Reasons group changed-key domains,
such as `host-lease+legacy-session`, `host-lease+participant`, and `participant`;
a coalesced commit counts exactly once and carries its complete UTF-8 payload size.
Values and stacks are not collected. Startup/semantic-change commits are intentionally
included; an idle minute still emits an explicit zero row. No final partial row is
emitted at exit. Sink errors cannot fail a commit and retain the interval for the next
successful append.

Accounting is at the one MeshStore state commit boundary immediately after successful
atomic replacement. Failed writes/CAS, absent deletes, empty/no-op batches, events,
file leases, signal files, and `confirmWritable()` are not state.json commits. Multiple
MeshStore instances in the process share the counter. Off by default: no timer,
counters, key classification, stack capture, extra serialization, filesystem work, or
per-commit environment lookup; only the disabled optional hook remains.

Tests pin opt-in capture, zero idle rows, byte accuracy including Unicode, aggregation,
reason coalescing, failed/no-op exclusion, failure isolation, default off, mixed native
reader visibility, explicit-policy zero writes/600 s cadence, and policy transitions.
