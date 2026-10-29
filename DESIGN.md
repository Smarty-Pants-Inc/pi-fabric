# Native actor execution

This note describes the host-registered native execution path and its safety boundaries. It is a design reference, not evidence of a build, installation, or runtime acceptance.

## Binding and execution

A native handler is registered by trusted host code against an exact actor, owning root, scope, and handler key. The resident configuration supplies these pins; a handler cannot select its own identity or import an arbitrary module. Session scope and durable residency are separate properties. Native execution is limited to its configured session owner; project-scoped execution is not implied.

The persisted native marker is reserved. Loaders, serializers, imports, and merges must preserve it or quarantine the row. Unknown or inconsistent markers fail closed and must never fall through to a model runner. Only an authenticated, direct ask may invoke the handler; model controls, subscriptions, and callerless queue restoration are not alternate activation paths.

The handler runs in the owning resident through the actor's existing serial queue. Its promise remains in custody until it settles. An abort signal is a cancellation request, not proof that synchronous or non-cooperative work stopped. Do not release ownership or start a replacement callback merely because a caller timed out.

## Bridge ingress and retry semantics

Identity comes from the bridge's verified presence and credential-bound source machine, not from message fields, a serialized context, or a generic bridge stamp. The bridge records bounded ingress evidence in its owned ledger and makes it durable before exposing the corresponding file event or acknowledging the source. The receiving control plane validates that evidence before claims, cached outcomes, replay, or cancellation. A public mesh publication cannot mint the receipt.

A same-key duplicate claim is a retry of the original command, not a new command body. The duplicate comparison ignores only the per-attempt time fields `requestCreatedAt`, `requestedAt`, and `deadlineAt`; it continues to compare the retained semantic body and route, sender, principal, and owner identity. A duplicate returns the original immutable ledger record. Projection and response replay use the first stored body, source event ID, deadline, and receipt; newer attempt stamps never replace those values or extend the original claim's horizon. Changed payload, principal, owner incarnation, or other semantic input remains a mismatch. The receipt verifier still requires the exact first projected event, so ignoring attempt stamps for claim deduplication does not authorize a differently timed event as native-ingress evidence.

The receipt is a local evidence reference, not a bearer token or independent cryptographic signature. The ledger's mapped command identity and retained outcome provide transport deduplication within the configured horizon; they do not make arbitrary external effects exactly once. Application code must provide its own durable idempotency and authorization for effects that may be retried after a process crash.

Receipt projection is a durability boundary for bridged traffic, not just native targets. A receipt or ledger failure can stop the shared link and affect ordinary traffic. Actor-history redaction does not remove body copies retained by control events, the bridge ledger, or broker retention.

## Ownership and failure

A live root, a resident owner lease, a session-file writer lock, and an in-flight callback are different forms of evidence. A stale heartbeat or elapsed timeout is not proof of process death. The Main's session-writer fence must be acquired before SDK/session access and held until the owning process dies; resident custody is separate. A successor requires confirmed settlement or confirmed termination under the applicable owner protocol.

The supported ledger recovery case is at most one incomplete final append, under its single-writer, sequential-append, full-write, fsync, and honest-filesystem assumptions. It is not power-loss atomicity. Middle corruption or multiple damaged tail records fail closed. Checksums detect corruption; they do not authenticate a same-user writer.

## Trust and retention limits

The bridge's machine credential authenticates a machine boundary, not a cryptographic identity for every process or session on that machine. Filesystem ownership and modes do not defend against arbitrary same-UID code that can access the same files. These are explicit trust assumptions, not sandbox guarantees.

Native message bodies should not be broadcast as actor lifecycle notifications or retained in native actor history. This does not change retention for control, ledger, broker, or application records; those copies need their own inventory and policy.

## Source and installed-artifact boundary

Source inspection and unit tests describe the source bytes they exercise. A fresh build, package artifact, installed reader, configured runtime, and production service are separate evidence. Mixed-release compatibility must be tested against the actual installed artifact on an isolated copy; source similarity alone is insufficient.

## Public implementation references

- [Native handler registration](src/actors/native-handler.ts) and [actor execution](src/actors/manager.ts).
- [Bridge ingress receipts](src/mesh/bridge-nats-ingress.ts), [ledger](src/mesh/bridge-nats-ledger.ts), and [carrier](src/mesh/bridge-nats-carrier.ts).
- [Control admission](src/topology/control-plane.ts), [resident ownership](src/residency/host.ts), and [participant freshness](src/topology/participant-directory.ts).
