Refs smarty-dev#4383 (host-lease renewals without fsync)

**Owner:** fabric-v2

## Measured cause and checkout caveat

Capacity-lead measured Ryzen 4's DRAM-less QLC SSD at IO PSI full 55%, with 36 Pi processes in D-state on `jbd2_log_wait_commit`: 117 host-lease files rewritten every 15 s with fsync, roughly eight fsynced rewrites/s/host, plus session files.

This pi-fabric lane starts at `828ab977ac8af47f8d17799be266dc44fcfdc46e`. Its host-lease writer already omitted durability and its default nominal heartbeat/TTL is 5 s / 15 s. We did **not** reproduce the reported durable host-lease implementation here and do not claim an actual baseline fsync reduction. The deployed fabric-mesh writer/version still needs separate reconciliation by the owner; this PR protects pi-fabric against regressing the soft-state contract.

## Change and durability trace

- Explicitly pass `{ durable: false }` to `writeHostLease`: write temp file + atomic rename, no fsync. `ParticipantDirectory.#renewFileLease` is its sole production caller, used on heartbeat/refresh and around per-key waits.
- First publication and replacement identity use the same soft write: the host-lease file grants liveness, not execution ownership. Lost renewal lapses and is re-created; callers do not require a durability fence here.
- Keep residency acquisition's lock and all existing durable handover/custody/phase receipts, takeover serving-owner publication, Main-generation authorization/closure, committed response/result, delivery outbox and request-expiry floor writes. These fence custody or acknowledged/replayed work before acting, unlike leases. Initial resident diagnostic owner/readiness writes were already rename-only and stay so.
- Derive a stable per-host SHA-256 cadence multiplier in `[0.8, 1.2]`, applied to the periodic heartbeat timer only. Manual/change-driven and per-key refreshes remain immediate. No eager optional dependency is added.
- Defaults stay 5 s / 15 s. Enforce TTL >= three **nominal** heartbeat intervals for supplied options; actual default timer spans 4–6 s. Document the recommended configurable 30 s / 90 s pair (actual timers 24–36 s) without adding mesh configuration keys or changing defaults.

See `docs/host-lease-io.md` and `artifacts/lease-acceptance-ledger.md`.

## Before/after fsync count

Isolated, real 60 s measurement on ryzen2, nice 19, ten leases at a fixed 15 s nominal cadence, 40 renewals (t=0,15,30,45 s). Acquisition is excluded and the directory exists before counting. Counts include real file and namespace fsync calls; this is a writer/barrier benchmark, not live fleet I/O or a jitter timing benchmark.

| Writer | Renewal writes | File fsyncs | Directory fsyncs | Total fsyncs | fsyncs/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Actual starting revision | 40 | 0 | 0 | 0 | 0 |
| After | 40 | 0 | 0 | 0 | 0 |
| Explicit durable reference, **not historical baseline** | 40 | 40 | 240 | 280 | 4.67 |

Reproduce with `nice -n 19 bun scripts/benchmark-host-lease-io.ts`; baseline `--module` and `--durable-reference` are documented in the script/docs. Preserved external evidence: `benchmark-before.json`, `benchmark-after.json`, `benchmark-durable-reference.json` under `$TASK_OUT`.

## Test table

All commands ran at `nice -n 19`; no full suite was run.

| Check | Result |
| --- | --- |
| participant-directory, control-plane, residency, residency-host, mesh-store | PASS (included in 10-suite run) |
| host-lease-io, host-leases, residency-handover, atomic-write-durable, participant-heartbeat-contention | PASS; total 431 passed / 28 skipped across 10 files |
| Final strengthened host-lease-io probe | PASS, 6/6: no fsync, temp rename, initial/replacement identity, public lapse/re-acquire, jitter spread/bounds/wiring, TTL floor, custody durability |
| `bun run typecheck` | PASS |
| `bun run assert:lazy-graph` | PASS, 38 host-free lazy UI files |
| Fresh `bun run build` | PASS; artifact/lazy startup checks: 45 startup files, 1,071,489 startup bytes, 55 stable lazy entries, 110 chunks |
| Isolated 60 s before/after/reference benchmark | PASS; counts above |

Lane preparation required `bun install --frozen-lockfile` (the lane had no installed toolchain). An initial lazy-graph attempt correctly failed before dist existed; rebuilding and rerunning passed. These setup failures are not hidden test failures.

## Revert condition

Revert the jitter/TTL-floor change if it causes reproducible premature peer lapse or startup/confirmation regressions under supported load. If an independent review identifies any host-lease write as an actual execution/custody durability fence, stop rollout and restore durability **only at that fence**, with a crash/recovery test; do not blanket-fsync soft renewals. The separately deployed durable writer remains an owner follow-up, not a claimed fix in this lane.
