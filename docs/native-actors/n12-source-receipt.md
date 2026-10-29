# Bridge ledger compatibility and recovery contract

This note describes ledger invariants and the evidence needed for a mixed-release compatibility claim. It is not a test receipt or proof about any installed artifact.

## Durable record behavior

The bridge ledger is single-writer state keyed by mapped command identity. Claims retain routing and outcome information needed to deduplicate transport delivery and return the recorded result to the original authority. A semantic mismatch, route mismatch, or changed body must not be treated as a harmless duplicate.

Where ingress receipts are enabled, the projection is derived from the ledger-owned claim and written before the file event is exposed or its source delivery is acknowledged. A receipt cannot be reconstructed from an untrusted public event. The ledger retains body evidence where required for replay and repair, so receipt metadata and retained message bodies are separate items in the data-retention inventory.

The current bridge contract retains claims through the later of their command deadline or claim time, plus the configured 24-hour deduplication horizon. This is a transport-replay horizon, not a caller response deadline or a short liveness lease. See [the horizon constant](../../src/mesh/bridge-nats-types.ts) and [claim implementation](../../src/mesh/bridge-nats-ledger.ts).

## Append and crash assumptions

A record is encoded completely, written in full, and fsynced before the mutation is reported as durable. A write or fsync failure fail-stops further mutation until recovery. Compaction and receipt publication also require their namespace durability barriers where supported.

Recovery accepts at most one unfinished final append under sequential single-writer operation, successful fsync ordering, and an honest filesystem that preserves the previously durable prefix. This does not guarantee power-loss atomicity or tolerate arbitrary media corruption. A damaged record before later valid records, or multiple damaged suffix records, is corruption and must refuse to open. Checksums detect accidental damage but do not authenticate a same-user writer.

## Compatibility gate

Source-to-source tests are not evidence that an installed older package can read candidate-written bytes. Pin and run the actual installed reader against an isolated copy containing valid candidate records and a real process-crash tail; verify open, append, compact, reopen, deduplication, outcomes, and any sidecar receipt behavior. Preserve an untouched original before a reader that may truncate a tail or compact the file. Never use a live ledger as the compatibility fixture.

An older reader may ignore new optional fields or sidecar files without preserving them. Prove the exact behavior of each supported reader; do not assume forward compatibility from similar source or format labels. If the required reader is unavailable or the test cannot bind the executed bytes, the compatibility result is unknown.

## Public references

- [Ledger implementation](../../src/mesh/bridge-nats-ledger.ts).
- [Claim horizon and bridge contract](../../src/mesh/bridge-nats-types.ts).
- [Carrier delivery ordering](../../src/mesh/bridge-nats-carrier.ts).
- [Ledger tests](../../tests/bridge-nats-ledger.test.ts) and [rollback tests](../../tests/nats-auth-path-b-bridge-ledger-rollback.test.ts).
