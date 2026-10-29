# Liveness, deadlines, and asynchronous work

This page separates current source defaults from guarantees. The values below are implementation defaults, not a host-load SLA; check the code and active configuration for the runtime under test.

## Distinct timing boundaries

| Boundary | Current source behavior | Meaning |
|---|---|---|
| File participant heartbeat | 5-second default; mesh-consumption admission requires a confirmed refresh younger than two heartbeat intervals (10 seconds at the default) and no refresh error. | A blocked event loop can prevent both the timer and the freshness gate from progressing. |
| Legacy session-root liveness | 15-second lease in the legacy root reader. | Root liveness is not resident ownership or callback settlement. |
| Control acknowledgement grace | Capped at 15 seconds. | A transport grace period is not a caller-visible end-to-end deadline. |
| Bridge deduplication horizon | 24 hours after the later of the command deadline or claim time. | Retention for transport replay, not a liveness or completion budget. |
| Resident idle behavior | Generic resident idle exit defaults to 10 minutes; dormant durable-actor release has a separate 30-second path. Session-scoped actors do not use project dormancy. | Idle policy, scope, and liveness are independent. |

These values are established in [participant-directory.ts](../../src/topology/participant-directory.ts), [legacy-root-liveness.ts](../../src/topology/legacy-root-liveness.ts), [control-plane.ts](../../src/topology/control-plane.ts), [bridge-nats-types.ts](../../src/mesh/bridge-nats-types.ts), [config.ts](../../src/config.ts), and [resident host](../../src/residency/host.ts). Configuration and transport mode can change applicable behavior.

## Blocking work and cancellation

Synchronous JavaScript blocks the event loop, including heartbeat, lease, and deadline callbacks. An async wrapper around synchronous filesystem or lock work does not remove that block. Prefer awaited asynchronous operations with explicit cancellation and bounded cleanup; verify that canceled work actually settles and releases its resources.

A callback deadline, bridge acknowledgement grace, presence-publication wait, resident idle timer, and caller timeout are separate clocks. Do not add them together as if they were one guaranteed response budget. A timeout after callback entry can mean outcome unknown; it is not proof of non-execution. The external caller must apply its own bounded observation policy and idempotent retry strategy.

Heartbeat loss is not ownership transfer. File locks and resident ownership are not time-to-live leases: a still-live process may retain custody after its routing record becomes stale. A successor must rely on the actual ownership protocol and confirmed termination or settlement, not elapsed time alone.

## Measuring a handler path

Measure worst-phase lock contention across the whole success and refusal paths, including preflight, authorization, reads, writes, and audit work. Observe actual committed heartbeats and lease renewals from separate processes while the handler runs. Include cancellation before and during acquisition, at handoff, and after effect commit; verify lock release, promise settlement, and outcome classification.

Do not raise timers to hide synchronous stalls or use mocked time as a substitute for cross-process contention. Record the exact source and artifact tested. A healthy Main process does not prove that a separately owned resident is live, and a recovered heartbeat does not erase a prior lapse.

## Public references

See [participant freshness](../../src/topology/participant-directory.ts), [session root lease](../../src/topology/legacy-root-liveness.ts), [control deadlines](../../src/topology/control-plane.ts), [resident idle policy](../../src/residency/host.ts), and [file locking](../../src/residency/file-lock.ts).
