# Mesh lock benchmark

`scripts/benchmark-mesh-lock.mjs` runs a fixed, seeded, reproducible load against a synthetic mesh
root and reports what the mesh lock costs per operation. It is the acceptance number of the
mesh-lock redesign (smarty-dev#6477, lane L9a) and its CI ratchet (smarty-dev#6676, see [CI](#ci)).
It never opens a live mesh: it seeds its own root under `$TMPDIR` and removes it.

```sh
bun run bench:mesh-lock                                   # build, then run with the defaults
nice -n 19 node scripts/benchmark-mesh-lock.mjs --out result.json   # dist/ already built
bun run bench:mesh-lock:check                             # the CI ratchet: dist/ built, default load vs the baseline
node scripts/benchmark-mesh-lock.mjs --from result.json --baseline bench/mesh-lock-baseline.json  # compare only
```

The JSON result goes to stdout (and `--out FILE`), a short table to stderr. Exit status: 0 ok,
1 ratchet regression, 2 invalid run or bad usage. On a shared host run it under `nice -n 19`.

## Load

Every number is a flag; the defaults model today's idle fleet of Mains (an estimate, not a
recorded trace: calibrate against `fabric-mesh-lock-stats` from the fleet).

| Flag | Default | Meaning |
|---|---|---|
| `--participants` / `--processes` | 80 / 8 | participants, spread over worker processes (10 each); one `MeshStore` per participant, configured as a Main's runtime store, idle |
| `--seed` | 6477 | the RNG seed of the whole schedule |
| `--duration` / `--warmup` | 120 / 15 s | measured window; warm-up ops run but are excluded |
| `--state-mb` | 4.8 | `state.json` is seeded to this size (decimal MB) with fleet-like actor-registry entries |
| `--heartbeat-s` | 5 | each participant's idle tick: presence file lease, a read of its own host record, `confirmWritable` |
| `--host-renew-s` | 600 | host record renewal (`writeBatch`), the files-policy cadence |
| `--put-rate` / `--delete-rate` | 1 / 0.25 per s | registry-like keyed writes over `--registry-keys` (200) keys |
| `--publish-rate` | 2 per s | messages |
| `--steer-rate` | 0.2 per s | steer-like addressed publish after a fresh directory read of the target; the target's process acks with an addressed receipt |
| `--dir-read-rate` | 4 per s | fresh `topology/participants/` listings |
| `--poll-ms` | 100 | each process's event-log tail (the control plane's poll) |
| `--lock-timeout-ms` / `--lock-protocol` | 10000 / 1 | the store's defaults |

Rates are fleet-wide. Heartbeats and renewals are periodic with a seeded phase; the op mix arrives
as seeded Poisson streams, each op on a seeded participant. Every worker derives the same plan from
the seed, so the number and kind of ops in the window are fixed: a slow host runs them late
(`scheduleLagP95Ms`), never fewer. The run is invalid (exit 2) when an op is lost, an op fails
with anything but the lock timeout, a receipt is missing after a 30 s drain, or L8's own stats
files disagree with what the harness saw.

## Metrics

Lock numbers come from L8's recorder (`PI_FABRIC_LOCK_STATS=1` in the workers, see
[mesh-lock-stats.md](mesh-lock-stats.md)) and bytes from the commit counter
(`PI_FABRIC_COMMIT_STATS`). Each worker wraps the recorder the store captured, so every record is
attributed (through `AsyncLocalStorage`) to the op that caused it and its phase, and L8 still
writes its files; the harness checks that their acquisition counts and wait/hold histograms equal
its own.

**Load-insensitive (the ratchet).** Per op in the window: `acquisitionsPerOp` (lock requests:
holds, timeouts and bounded tries), `holdsPerOp` (successful custody), `stateRewritesPerOp`,
`bytesRewrittenPerOp` (whole `state.json` rewrites), and `timeoutsPer10kOps`
(`FABRIC_MESH_LOCK_TIMEOUT` at the generous default 10 s budget). They follow from the schedule and
the store's code path, not from the host's speed; three runs on a loaded host agree within a few
percent (optimistic put retries under contention are the one load-dependent term).

**Timing (reported; gated only with `--gate-timing`).** `lockWaitP95Ms`, `lockHoldP95Ms` (exact,
from the samples L8 recorded), `receiptP95Ms` (publish of a steer to the sender seeing its
receipt) and `lockBusyPct` (summed hold over the window). Also reported: lock acquisitions per
second and by class, the L8 histogram p95 buckets (as `fabric-mesh-lock-stats` would print them),
and `FABRIC_DIRECTORY_UNAVAILABLE` per 10,000 ops: a steer or directory read that failed, or ran
while its participant was write-stalled (last heartbeat hit the lock timeout, or none committed for
two intervals; the directory's peer-lapse refinement is not modelled).

Why two classes: on a shared host CPU steal, disk and other jobs move wait, hold, receipt time and
busy % by tens of percent between identical runs, so gating them there would flake. Per-op counts
do not move, so they are what a shared CI runner can ratchet. Timing is gated only on a dedicated
runner, against a baseline recorded on that runner.

## Ratchet

`--baseline FILE --max-regress PCT` (default 10) fails (exit 1) when a load-insensitive metric
exceeds `baseline * (1 + PCT/100)`; a zero baseline allows only zero. With `--gate-timing` the
timing metrics are checked the same way. A baseline recorded under another load (any load flag or
default changed) fails the comparison: re-record it. `--write-baseline FILE --note TEXT` writes a
baseline from a valid run.

`bench/mesh-lock-baseline.json` is the file store's baseline (lock protocol 1), recorded on this
branch on ryzen5 (32 CPUs, shared and loaded, `nice -n 19`) with the default load; its `note`,
`host` and `load` fields say so. The SQLite store replaces it at the cutover.

## CI

The `mesh-lock-bench` job of `.github/workflows/test.yml` (ubuntu-latest, Node 24, on every
pull request and push to `main`) builds and runs the default load against the committed baseline:

```sh
bun run build
bun run bench:mesh-lock:check --out mesh-lock.json   # = --baseline bench/mesh-lock-baseline.json --max-regress 10
```

and uploads `mesh-lock.json`. It fails (exit 1) when a load-insensitive metric is more than 10%
above the baseline (all of them are lower-is-better; `timeoutsPer10kOps` has a zero baseline, so one
timeout fails), and exit 2 when the run is invalid. Timing metrics are printed in the table on
stderr (class `timing`, with their delta) but never gate there: a shared runner moves them by tens
of percent (one local pass shows them at +27-33% while the ratchet metrics stay within 0.05%). The
default load takes about 2.5 minutes (seeding, 15 s warm-up, 120 s window, drain); the job about
4. It is not a required check; making it one is a separate decision in `.mergify.yml`. A dedicated
runner can add `--gate-timing` against a baseline recorded on that runner.

**Updating the baseline intentionally.** A change that is meant to move the numbers (an
improvement that should tighten the ratchet, a load or default change, the SQLite cutover)
re-records the baseline in the same pull request, so the review sees the old and new numbers in the
diff of `bench/mesh-lock-baseline.json`:

```sh
bun run build
nice -n 19 node scripts/benchmark-mesh-lock.mjs --write-baseline bench/mesh-lock-baseline.json \
  --note "what changed, the commit, the host and its load" > /dev/null
git diff bench/mesh-lock-baseline.json
```

`--write-baseline` refuses an invalid run. Run it two or three times and keep a representative run;
the ratchet metrics should agree within a fraction of a percent. A pull request that only loosens
the baseline (higher numbers, same load) needs the reason in its description.
