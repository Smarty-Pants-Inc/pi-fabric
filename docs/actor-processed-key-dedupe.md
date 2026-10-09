# Durable actor processed-key deduplication

Completed-key deduplication is **opt-in**. For a persistent actor with
`residency: "durable"` and `dedupeKey: "data.key"`, Fabric remembers the last
**256 successfully completed mesh occurrences** per actor. An occurrence is the
configured path, event topic and scalar value at that path in the **full mesh
event**. Strings and numbers remain distinct. The path is validated as a dotted
path of at most 200 characters. Missing or non-scalar values are not deduplicated.

```ts
await agents.create({
  name: "owner-alarm",
  instructions: "Handle owner alarms.",
  residency: "durable",
  topics: ["ops.owner"],
  dedupeKey: "data.key",
});
```

If a producer crashes after publishing `stuck.work` and before recording its
`told` flag, publishing a new event ID with the same `data.key` does not run that
receiver again. A different key runs normally. A repeat does not refresh its
retention position; once the oldest key falls out of the 256-key window it may
run again. Each actor has its own window. Changing the dedupe path or topic uses
a different occurrence identity. The option is set at creation and round-trips
through global templates and actor registry reloads.

## Resource coalescing is not occurrence deduplication

`coalesceKey` is independent and remains **queued-only**. It is a path into
`event.data`, such as `payload.number`, and lets a newer queued event for a
resource replace an older queued one. It never supplies a completed dedupe key.
An actor without `dedupeKey` behaves as before: a durable reviewer with
`coalesceKey: "payload.number"` runs a new head or security event for the same PR
after its earlier activation completes. Do not use a PR number as `dedupeKey`:
that would explicitly opt into suppressing genuinely new work for that resource.
Both options may be supplied, but they serve different purposes.

With `dedupeKey`, a duplicate queued while the first activation runs is checked
again before starting inference. Dead-letter replay and restored queues also
check completed occurrences. Failed, interrupted, preparation-only, filtered and
parked activations do not create a completion entry. Successfully completed
inference is terminal even if a later ownership or output-delivery check fails,
as for existing queue completion.

The bounded window is an optional `processedKeys` array in the existing format-1
lineage queue snapshot. Empty queues retain this completion fence, successful
completions durably rewrite it, and restart/adoption restores it with queue and
revision state. Old snapshots without the array remain valid; legacy resource
completion fences are not occurrence keys. Session actors, actors without
`dedupeKey`, direct messages and event-ID deduplication keep their previous
behavior. No separate setter action is added.

This is a bounded receiver replay defense, not a transactional exactly-once
side-effect guarantee. A receiver crash before its completion checkpoint can
still replay an interrupted activation.

Regression coverage: `tests/actor-processed-key.test.ts`. Related behavior:
`tests/actor-coalesce-key.test.ts`, queue/overflow and actor restart suites.
