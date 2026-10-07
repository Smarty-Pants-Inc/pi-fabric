# Host-lease I/O and renewal cadence

Refs smarty-dev#4383 (host-lease renewals without fsync).

`writeHostLease()` has two production callers. `ParticipantDirectory.#renewFileLease()`
runs before participant publication, between contended per-key operations, and at
heartbeat/refresh completion. `StoreBridgeSide.#mirror()` (`src/mesh/bridge.ts`)
re-stamps mirrored peer leases inside its post-commit, owner-checked transaction;
that write is the same soft liveness. Every host-lease file
write, including first publication and replacement identity, uses a temporary
file followed by atomic rename **without fsync**. These files are soft liveness,
not ownership or custody receipts. A crash can lose the latest renewal; the
lease lapses, and the next refresh re-creates it. Readers check root/identity,
and do not treat a longer lease as authority to take over execution.

This checkout already had rename-only host-lease writes. Explicit
`{ durable: false }` plus fsync-spy tests pins that contract; this PR cannot claim
to remove a barrier that was absent at its starting revision.

## Correctness fences (unchanged)

- ResidentHost acquisition uses the resident lock (`flock` on Linux; guarded
  process identity on fallback platforms), not host-leases. Initial owner and
  maintenance-ready records were already rename-only; no fsync is added to them.
- `residency/handover.ts`: `writeHandoverImmutable` (launch snapshots, immutable
  custody/cancellation decisions and outcome receipts) and `writeHandoverState`
  keep `durable: true`. Custody/phase receipts must survive before the prior
  resident releases its stable lock or the successor acts.
- `ResidentHost.#advanceRelease` keeps the durable serving-owner write after
  takeover. Residency client Main-generation publications/closure, delivery
  outbox, committed responses/results, and request-expiry floors retain their
  durability: these are authorization, replay, or acknowledged-work fences,
  not liveness renewals. MeshStore host-state writes were already non-durable;
  this PR does not weaken them or change archive durability.

## Cadence and configuration

`ParticipantDirectoryOptions.heartbeatMs` is the **nominal** interval;
`leaseMs` is the TTL. Defaults stay **5 s / 15 s**. The TTL floor is three nominal
intervals (formerly two). A SHA-256-derived per-host multiplier in `[0.8, 1.2]`
spreads periodic timers reproducibly without loading any optional dependency.
A 5 s nominal interval therefore schedules at 4–6 s. At the slowest cadence the
default TTL tolerates exactly one missed actual tick, not two: with a renewal at
t=0 and a 6 s interval, misses at t=6 and t=12 put the next renewal at t=18, after
the t=15 expiry, so readers may drop a still-running host for up to about 3 s
until that renewal re-creates it. The same holds for 30 s / 90 s (36 s cadence:
recovery after two misses at t=108, expiry at t=90) and for the 3x TTL floor. The
policy is three *nominal* intervals; it is not a two-missed-tick guarantee. Manual/change-driven
refreshes and per-key liveness renewals remain immediate, not delayed by jitter.

For deployments supplying these constructor options, recommend **30 s renewal /
90 s TTL** (`{ heartbeatMs: 30_000, leaseMs: 90_000 }`), scheduling at 24–36 s
(also one missed tick at the slowest cadence).
These are internal directory options, not new user-facing mesh config keys.

## Isolated benchmark

Run `nice -n 19 bun scripts/benchmark-host-lease-io.ts` for ten pre-existing lease
files over a real 60 s observation window at 15 s nominal cadence, with all data
under `$TMPDIR`. The window includes renewals at 0, 15, 30, and 45 s (40 writes).
`--module <path>` selects a preserved baseline module; `--durable-reference`
measures the explicitly durable atomic writer as a **reference**, not as the
checkout's historical implementation. Report actual baseline and after counts
separately from that reference. It never opens a live mesh. File and namespace
fsyncs are counted separately by fstat of each synced descriptor.
