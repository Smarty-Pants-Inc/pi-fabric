# Native owner and release compatibility

This page records reusable startup, custody, and compatibility requirements. It is not a receipt that a package, service, or deployment has passed them.

## Session writer ownership

A native owner must take its exclusive session-writer fence before any SDK session-manager or session-file access. Keep the lock descriptor for the owning process lifetime; an exit hook, launcher exit, timeout, or stale heartbeat does not prove that the process or its callback has terminated. Provisioning and verification paths that touch the same session must use the same ordering.

The session-file fence protects writers to that session. It is distinct from the resident's owner lease and callback custody. A resident must not inherit the Main's writer lock. A same-build Main reconstruction may reuse a live resident only under the existing owner protocol; changing the resident generation requires draining or stopping the old callback owner and verifying terminal state before replacing its artifacts.

A replacement process that cannot acquire the fence must fail without modifying session bytes. Confirm the actual contender's exit and compare the session and configuration bytes; do not treat a PID file, elapsed deadline, or launcher death as a lock receipt.

## Native SDK and startup contract

The no-model requirement means that no real model may be selected or invoked. If the pinned SDK constructs a placeholder configuration internally, accept only the exact documented shape for that SDK build; reject a registered model, mutation, or unexpected provider call. A model-policy setting alone is not proof that no model ran.

Load the native handler only from an immutable, host-configured module path after owner custody is acquired and before readiness is published. Any missing module, changed binding, unsafe path, or session identity mismatch fails closed. Do not create a replacement session identity automatically.

## Compatibility and proof boundaries

The persisted native marker is an execution boundary, not decoration. Every serializer and importer must preserve or reject it before legacy runner defaults are applied. An older writer that drops unknown fields can silently change meaning; do not open a native-bearing registry with an unqualified older writer.

Qualify the actual compiled owner entry and the actual installed package separately from test fixtures. A fixture that demonstrates lock behavior does not prove the product entry takes the lock before SDK access. Record the exact artifact executed and verify both positive owner operation and a competing-writer refusal on isolated state.

## Residuals

A local process with the same operating-system identity may still access files that its owner can access; filesystem modes and checksums are not a defense against arbitrary same-user code. A lock release on process death establishes writer availability, not whether an interrupted external operation committed. That outcome requires application-level idempotency and reconciliation.

## Public references

See [native owner entry](../../src/worker/native-owner-entry.ts), [session writer lock](../../src/residency/file-lock.ts), [resident host](../../src/residency/host.ts), and [actor execution](../../src/actors/manager.ts).
