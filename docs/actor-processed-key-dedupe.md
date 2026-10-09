# Durable actor processed-key deduplication

For a persistent actor with `residency: "durable"` and `coalesceKey: "key"`,
Fabric remembers the last **256 successfully completed mesh subjects** per actor.
A subject is the configured path, event topic and scalar value at that path in
`event.data`. Strings and numbers remain distinct. Use an occurrence-specific
`data.key` for an alarm that should wake once:

```ts
await agents.create({
  name: "owner-alarm",
  instructions: "Handle owner alarms.",
  residency: "durable",
  topics: ["ops.owner"],
  coalesceKey: "key",
});
```

If a producer crashes after publishing `stuck.work` and before recording its
`told` flag, publishing a new event ID with the same `data.key` does not run that
receiver again. A different key runs normally. A repeat does not refresh its
retention position; once the oldest key falls out of the 256-key window it may
run again. Each actor has its own window. Changing the coalesce path or topic
uses a different subject; missing or non-scalar values are not deduplicated.

Pending items still use existing queued-only coalescing. A duplicate queued
while the first activation runs is checked again before starting inference.
Dead-letter replay and restored queues also check completed subjects. Failed,
interrupted, preparation-only, filtered and parked activations do not create a
completion entry. Successfully completed inference is terminal even if a later
ownership or output-delivery check fails, as for existing queue completion.

The bounded window is an optional `processedKeys` array in the existing format-1
lineage queue snapshot. Empty queues retain this completion fence, successful
completions durably rewrite it, and restart/adoption restores it with queue and
revision state. Old snapshots without the array remain valid. Session actors,
actors without a coalesce key, direct messages and event-ID deduplication keep
their previous behavior; no new public action or configuration option is needed.

This is a bounded receiver replay defense, not a transactional exactly-once
side-effect guarantee. A receiver crash before its completion checkpoint can
still replay an interrupted activation. A durable subscription that must run
again for each revision should include the revision in its subject key, rather
than use only a long-lived resource identifier such as a pull-request number.

Regression coverage: `tests/actor-processed-key.test.ts`. Related behavior:
`tests/actor-coalesce-key.test.ts`, queue/overflow and actor restart suites.
