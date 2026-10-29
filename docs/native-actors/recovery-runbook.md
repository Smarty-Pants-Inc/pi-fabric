# Bridge ledger recovery runbook

This is a fail-closed recovery procedure. It does not authorize production operation or replace the owner of the affected system's recovery decision.

## Stop and preserve

1. Stop new ingress and replay for the affected link. If the ledger or receipt namespace is shared across traffic classes, hold the whole link rather than only native deliveries.
2. Stop the link under its normal custody protocol. Separately settle or stop resident callbacks and confirm their terminal state. Launcher exit, a timeout, or a missing heartbeat alone is not proof that an owner process has stopped.
3. Preserve an untouched copy before opening it with a reader that may truncate an incomplete tail or compact records. Record hashes, sizes, modes, owner, reader and candidate artifact identities, cursor/configuration, outstanding claims/outcomes, and relevant error evidence. Work only on separately verified copies.
4. Do not edit damaged records, delete claims, fabricate acknowledgements, automatically create a fresh ledger, or replay work with an uncertain outcome.

## Classify the file

| Observation | Required action |
|---|---|
| Valid ledger | Test the identified reader on a copy; opening alone does not prove safe replay or traffic restoration. |
| One incomplete final append | The built-in single-tail recovery may apply only under the documented single-writer, sequential append, fsync, and durable-prefix assumptions. Preserve the original and compare it with the recovered copy. |
| Damage before later valid records, multiple damaged suffix records, or unknown corruption | Stop and preserve evidence. Do not repair or replay automatically. |
| Receipt directory, ownership, mode, or recipe conflict | Keep the shared link stopped; record the failure and use the owner-approved recovery path. Do not bypass the conflict or repair permissions automatically. |

A removed tail is not proof that business work never started. A transport acknowledgement, missing receipt, or absent result does not resolve an uncertain external effect.

## Compatibility and reconciliation

Test the exact installed reader and candidate writer bytes on isolated copies, including valid data, a real process-crash tail, append, compaction, and reopen. Source-level compatibility is not installed-artifact compatibility. Preserve the reader identity and the untouched original in the evidence record.

If a trusted backup or fresh-ledger recovery is considered, an authorized owner must choose it explicitly and accept the resulting deduplication loss. Reconcile every affected operation against its actual application idempotency/effect store using owner-reviewed schema and tooling. Do not invent a query from a synthetic fixture. Generic bridge operations may have no application idempotency key; resolving one application flow does not clear the ordinary-traffic residual.

## Resume criteria

Resume only after custody is understood, the exact reader passed on a copy, outstanding transport and application outcomes are reconciled, replay positions are reviewed, and the authorized owner approves the scope. Keep traffic unavailable if any operation, reader, or external effect remains unknown. Preserve the original, working copy, commands, terminal results, and readbacks.

## Public references

See [ledger implementation](../../src/mesh/bridge-nats-ledger.ts), [claim horizon](../../src/mesh/bridge-nats-types.ts), and [compatibility contract](n12-source-receipt.md).
