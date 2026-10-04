# Host-lease I/O and renewal cadence

Refs smarty-dev#4383 (host-lease renewals without fsync).

`ParticipantDirectory.#renewFileLease()` is the sole production caller of
`writeHostLease()`. It runs before participant publication, between contended
per-key operations, and at heartbeat/refresh completion. Every host-lease file
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
A 5 s nominal interval therefore schedules at 4–6 s; a default TTL allows at
least two missed actual ticks even at the slowest cadence. Manual/change-driven
refreshes and per-key liveness renewals remain immediate, not delayed by jitter.

For deployments supplying these constructor options, recommend **30 s renewal /
90 s TTL** (`{ heartbeatMs: 30_000, leaseMs: 90_000 }`), scheduling at 24–36 s.
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
