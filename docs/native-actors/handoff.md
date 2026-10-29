# Native actor implementation handoff

This page is an implementation map and acceptance checklist. It describes the current source contracts; it is not evidence that every listed gate or an installed deployment has passed.

## Implementation boundaries

| Boundary | Public source | Required behavior |
|---|---|---|
| Host registration and binding | [native handler](../../src/actors/native-handler.ts), [actor manager](../../src/actors/manager.ts) | Exact configured actor/root/handler binding; no model fallback or arbitrary module selection. |
| Persisted identity | [actor registry](../../src/actors/manager.ts), [global registry](../../src/actors/global-registry.ts) | Preserve the reserved native marker; reject or quarantine unknown/inconsistent rows before legacy defaults. |
| Bridge evidence | [ingress](../../src/mesh/bridge-nats-ingress.ts), [ledger](../../src/mesh/bridge-nats-ledger.ts), [carrier](../../src/mesh/bridge-nats-carrier.ts) | Build evidence only from verified delivery and retained claim data; fail closed on missing or conflicting evidence. |
| Duplicate retry | [ledger](../../src/mesh/bridge-nats-ledger.ts), [carrier](../../src/mesh/bridge-nats-carrier.ts) | Ignore only per-attempt timestamps when deciding whether an existing command is a duplicate; replay from the immutable first claim and receipt. |
| Control admission | [control plane](../../src/topology/control-plane.ts), [message router](../../src/providers/agents-message-router.ts) | Verify native admission before claim lookup, replay, or cancellation; pass proof out of band. |
| Resident lifecycle | [resident host](../../src/residency/host.ts), [native resident loader](../../src/residency/native-actors.ts) | Load configured handlers under owner custody before readiness; retain custody until callback settlement. |
| Session writer fence | [native owner entry](../../src/worker/native-owner-entry.ts) | Acquire the writer fence before SDK/session access; do not equate launcher exit or timeout with child death. |

### Duplicate and replay invariant

For an existing mapped command, the claim comparison excludes only `requestCreatedAt`, `requestedAt`, and `deadlineAt` attempt stamps. It does not exclude payload, binding, principal, sender, target, route, owner identity, or other semantic fields. `FileBridgeNatsLedger.claim()` returns the original record on a duplicate without rewriting its body, source event ID, deadline, expiry, or outcome. The carrier uses that retained record for projection/replay, and the native receipt remains bound to the first exact event. Direct receipt verification does not normalize away changed timestamps.

This is transport retry behavior, not business idempotency. Application effects still require application-owned authorization and durable idempotency; a timeout after callback entry may leave an outcome unknown.

## Focused verification plan

Run focused checks against a frozen candidate; a broad suite is complementary and does not replace boundary coverage:

- Typecheck and targeted native actor, registry, ingress, ledger, control-admission, and residency suites.
- Exercise the real bridge composition for positive admission, forged/missing evidence, retimed same-key replay, changed semantic input, cancellation, and receipt-write failure.
- Exercise callback settlement and owner custody across process boundaries; abort alone must not permit a successor.
- Run the lazy-graph assertion and a fresh build, then inspect the emitted artifact paths and hashes.
- Test mixed-release ledger compatibility with the actual installed reader on an isolated copy, including valid data and a real process-crash tail.

Relevant targeted suites include `tests/native-actor.test.ts`, `tests/native-actor-registry.test.ts`, `tests/native-actor-control-admission.test.ts`, `tests/native-actor-residency.test.ts`, `tests/bridge-nats-native-ingress.test.ts`, `tests/bridge-nats-ledger.test.ts`, `tests/nats-auth-path-b-bridge-ask.test.ts`, `tests/nats-auth-path-b-bridge-followup.test.ts`, and `tests/nats-auth-path-b-bridge-ledger-rollback.test.ts`.

## Evidence levels and remaining gates

A source test proves only the source path and fixture it exercised. A build proves that source was compiled into a particular artifact. An installed-reader test must identify and execute the installed bytes, not a current source import. A live integration test must also bind the actual configuration, credentials, process ownership, and external side-effect semantics. Keep these results separate; do not infer installation or production behavior from unit coverage.

The real bridge ask replay test exercises the authenticated broker/link path and proves transport replay for its fixture; it does not prove native callback residency or application-side exactly-once effects. Actual SDK Main/resident acceptance, the frozen-provider-to-native-resident user path, the external caller's bounded uncertainty behavior, application crash-atomic idempotency, and the installed older-reader compatibility gate remain distinct acceptance requirements until independently exercised on their required artifacts and environments.

## Public references

See [native actor design](../../DESIGN.md), [ledger recovery](n12-source-receipt.md), and [timer boundaries](timer-budget.md).
