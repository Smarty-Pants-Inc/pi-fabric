# Native execution decisions and residual risks

This is a concise technical decision record. It does not attribute private reviews, approvals, issue discussions, or deployment decisions.

## Chosen boundaries

- Native execution is host-registered and bound to an exact actor, root, scope, and handler identity. Configuration and persisted markers are validated independently.
- Native execution is session-scoped and uses the existing durable actor owner. It does not add project-scoped native execution, a new transport, or an arbitrary guest module loader.
- Only authenticated direct asks reach the callback. Admission proof is out of band and is checked before durable claim lookup, replay, or cancellation.
- The bridge's durable ledger is the authority for transport claims and outcomes. Event labels and caller-provided context are not authentication evidence.
- The callback promise retains ownership until actual settlement. A timeout or abort does not authorize overlapping execution or automatic replay.

## Explicit non-guarantees

- Transport deduplication is not exactly-once external effects. Application code owns durable idempotency, authorization, and uncertain-commit reconciliation.
- Credential-bound machine presence is not per-session cryptographic authentication. Same-user local processes remain inside the existing host trust boundary.
- A checksum detects corruption, not a privileged or same-user forgery. A process lock coordinates cooperating writers; it is not a sandbox.
- A source test, source build, or fixture does not prove the behavior of an installed binary, package, configuration, or service.
- A supported single incomplete final ledger append is not arbitrary corruption recovery or power-loss atomicity.

## Change-control requirements

Changes to the persisted marker, receipt format, ledger ordering, identity mapping, or owner startup sequence require focused compatibility tests and a fresh built-artifact review. Mixed-release claims must name and exercise the actual reader and writer bytes. Keep ordinary bridge behavior covered when adding native-only admission, because shared ledger failures can affect both paths.

## Public references

See [native actor design](../../DESIGN.md), [bridge ingress](../../src/mesh/bridge-nats-ingress.ts), [control admission](../../src/topology/control-plane.ts), and [ledger behavior](../../src/mesh/bridge-nats-ledger.ts).
