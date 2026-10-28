# Gateway request joins

`src/recovery/gateway-joins.ts` is an append-only **consumer** of separately owned
gateway evidence. It uses the shared `RecoveryJournal`; it does not implement a
provider, gateway interceptor, request scheduler, effect deduper or billing system.
The gateway/capacity owner must supply the source described below and connect it
to the **normal routing path**, not a successful-response-only test endpoint.

No installed native, Kubernetes, replicated-storage or live gateway integration
is qualified by this module or its fixture tests. Repeated provider computation
and billing are possible after interruption. Join completeness is not a claim
that provider billing or business effects are exactly once.

## Source contract to implement

The exported `GatewayCapacitySource` port is:

```ts
interface GatewayCapacitySource {
  readonly sourceId: string;
  readGatewayRequests(request: {
    readonly scopeId: string;
    readonly deadlineMs: number; // Original absolute Unix milliseconds.
    readonly signal?: AbortSignal;
  }): Promise<GatewaySourceSnapshot>;
}

interface GatewaySourceSnapshot {
  readonly snapshotId: string;
  readonly complete: boolean;
  readonly reason?: string; // Required when complete is false.
  readonly rows: readonly GatewayRequestRow[];
}
```

`scopeId` names an independently frozen workload/source enumeration. It is not a
time-window join. `snapshotId` is the immutable identity of this source snapshot.
The source must honor the supplied deadline/signal; the consumer does not restart
that deadline, poll, retry a source call or dispatch any model request. A hung
source that ignores its bounds remains an integration failure, not an empty
snapshot. A thrown source read is retained as `unavailable` if the journal can
still commit within the original bounds. Journal failure/abort/deadline propagates;
it cannot be converted to authoritative absence.

`complete: true` must enumerate **all physical gateway/upstream invocation
attempts in the scope**, including:

- requests that started but never returned any response, response ID or usage;
- every request retry, provider retry and reconnect that creates another actual
  invocation, whether it succeeded, failed, was cancelled or was interrupted;
- the original request prefix on the old runtime and retries on the successor;
- requests whose runtime correlation is missing, ambiguous, wrong or not found
  in the caller's attempt denominator.

An unavailable retained range makes the snapshot incomplete, not an empty
complete response. The consumer retains every supplied row without filtering by
success, billing, tool call, effect, known session or timestamp. It cannot detect
an invocation the source omits from a falsely complete enumeration; independent
source coverage and a frozen expected-invocation denominator are still required.

### Immutable rows and source data

Each `GatewayRequestRow` contains:

| Field | Contract |
| --- | --- |
| `sourceId`, `scopeId` | Exact source and frozen enumeration identity. |
| `sourceRecordId` | Stable source event ID; changed lifecycle observations use new IDs. |
| `gatewayRequestId` | Source-issued identity for **one physical invocation**, unique within the source, not merely a logical conversation or retry group ID. |
| `providerAttemptId?` | Exact upstream ID, if known. It may first appear on a later observation of the same invocation. Two different known upstream IDs cannot rebind one gateway invocation. |
| `retryOf?` | `{ gatewayRequestId, providerAttemptId? }` identifying the original physical invocation; retries have their own new gateway request IDs. Missing parents are reported, not discarded. |
| `stage` | `started`, `updated`, `finished` or `interrupted`; a start with no terminal observation remains a visible prefix, not an invented failure/success. |
| `requestStartedAt` | Original source timestamp (string, number or explicit `null`), never the consumer's import time. Immutable across observations. |
| `timestamps` | Source timestamp names and values (string, number or explicit `null`), with their source units/clock conventions. No inferred durations or clock conversions. |
| `status` | Source JSON status, including an explicit `null` for unknown. No normalized success inferred from the presence of a result. |
| `billing` | Source JSON charge/usage data, including explicit `null` for unavailable/unknown. Preserve units, currency, precision, estimates/finality and source provenance in this object. No charge calculation or token-to-price conversion. |
| `metadata?` | Optional source JSON provenance, original route/group IDs and native-unit/clock conventions. Preserved, not used as a correlation heuristic. |
| `correlation` | One of the explicit correlation states below. |

If the gateway normally reuses a request ID for several upstream invocations, its
source adapter must expose its own stable, source-issued **per-invocation** IDs
and preserve the original group/provider IDs in source metadata. The consumer
never generates request IDs or collapses retries into one successful request.
A provider ID learned from a response is added in a **new source event**; it does
not rewrite the initial event that had no provider ID.

Lifecycle observations can repeat charge/usage values. They are event history,
not an additive billing ledger: summing their `billing` fields would double count.
Reconciliation of charge adjustments/finality belongs to the source owner.

Only lossless JSON is accepted. Undefined optional fields must be omitted; `NaN`,
infinities, negative zero, sparse/decorated arrays, accessors, exotic objects,
symbol fields and cyclic values are rejected rather than silently reserialized.
Inputs are detached before asynchronous queuing, and reads return detached data.
Canonical object-key order is used solely for import identity/storage, not to
rederive source timestamps, status or billing values. This is a typed-row capture,
not a substitute for retaining the source's exact raw transport trace bytes.

### Model invocation identity, before tool/effect identity exists

The structural `GatewayAttemptCorrelation` fields are:

```ts
{
  sessionId: string;
  acceptedInputId: string;
  attemptId: string;
  hostId: string;
  podId: string;
  bootId: string;
  ownerIncarnation: string;
  authorityRevision: string | number;
  toolCallId?: string;
  effectKey?: string;
}
```

`attemptId` identifies the actual physical invocation. A **model attempt can
precede every tool call and business effect**. Its correlation needs only the
logical session/input, model-attempt ID and exact physical/authority identity;
`toolCallId` and `effectKey` are omitted, not invented or required to be completed.
A retry can have a new model-attempt ID and a new host/pod/boot binding. Several
gateway requests may also explicitly reference one runtime attempt; every source
request is still individually visible.

Later tool/nested-effect evidence can reference the saved model `attemptId` and
`gatewayRequestId` through the composition owner. Do not retrofit the original
model request with a newly produced tool/effect identity, and do not pass the
entire tool/effect attempt ledger as if every row must have invoked a model.
The caller supplies the independent **gateway-causing invocation** denominator.
Structural typing permits effect-attempt identities when they actually describe
a gateway invocation; it requires no edits/import dependency on an effect ledger.

`correlation` is one of:

- `{ kind: 'exact', value: GatewayAttemptCorrelation }`;
- `{ kind: 'missing', reason: string }`;
- `{ kind: 'ambiguous', reason: string, candidates: GatewayAttemptCorrelation[] }`
  with at least two candidates.

Joining compares **all named identity fields**, including optional tool/effect
IDs when present and the exact type/value of the authority revision. There is no
ID suffix normalization, host substitution, model-text matching, timestamp
proximity heuristic or promotion of a source's ambiguous row to exact. Missing
and ambiguous correlation states remain explicit. A later contradictory binding
requires source-side investigation; it cannot overwrite the original observation.

## Normal-route integration surface

The composition owner constructs `new GatewayJoinCapture(journal)` on an already
opened/recovered shared `RecoveryJournal`. The capture does not open or close that
journal and never reads/writes native Pi files. Record keys are isolated under
`recovery:gateway-join:v1:`; unrelated journal namespaces remain owned elsewhere.

1. The normal model/gateway dispatch path supplies the immutable logical-input,
   model-attempt and physical/authority correlation **before invoking** the
   gateway. The capacity-owned gateway source allocates its request/event IDs
   and retains the start before awaiting a response. Where the route directly
   supplies rows to this consumer, `await capture.append(row, { deadlineMs,
   signal? })` must succeed before dispatch; it commits the supplied row, not the
   model call or any business effect. A response-only callback is insufficient.
2. Each physical retry is another start row with a distinct gateway request ID.
   Status/billing/end observations append new `sourceRecordId`s; the prior start
   stays retained. Interruptions with no callback still have their original start.
3. `capture.captureFrom(source, { sourceId, scopeId, captureId, deadlineMs,
   signal? })` reads the capacity-owned source once. `source` may be undefined:
   that commits an explicit `missing` source observation instead of pretending
   capture succeeded with no requests. Source identity mismatch is an error.
4. For externally transported/replayed source data, call
   `capture.importSnapshot(sameRequestShape, snapshot)`. All rows are preflighted;
   each is durably appended, then the source enumeration observation is appended
   **last**. An interrupted import can retain a prefix of rows without a source
   receipt. Reopen exposes that prefix and reports missing/incomplete coverage.
5. Call `capture.inventory({ attempts, expectedSources, expectedRequests? })` with
   independently frozen invocation identities and exact
   `{ sourceId, scopeId, captureId }` expectations. Optional expected request IDs
   use `{ sourceId, gatewayRequestId, providerAttemptId? }`. The parent composes
   this report with raw attempts, destination evidence and the full effect ledger.

The absent pieces are specifically the capacity-owned normal-route start/retry/
response feed, its durable completeness enumeration, runtime model-attempt
identity propagation and composition with later nested effects/raw traces. A
source fixture or a caller-supplied `complete` flag does not establish any of them.

## Replay, conflicts and completeness

Identical `(sourceId, sourceRecordId)` imports reproduce the existing journal
sequence without another append. Object-key ordering is immaterial. Reusing that
identity with different data is `GatewayJoinError('record-conflict', ...)`.
Reusing `(sourceId, scopeId, captureId)` with a changed source snapshot/state/list
also conflicts; a later observation uses a new capture ID rather than deleting
history. A changed source row's scope is `source-conflict` at snapshot import.
Rebinding a physical gateway request's runtime identity, scope, original start,
retry parent or conflicting known provider ID is `request-conflict`. Conflicts
leave the original bytes intact. Whole-snapshot validation/conflict preflight
happens before any new snapshot rows are written; an actual journal failure can
still leave a retained partial import, never a falsely sealed enumeration.

`records()` and `inventory()` read the actual journal each time. Closed,
inaccessible or corrupt storage throws; it is not reported as an authoritative
empty set. Malformed/conflicting records in this namespace throw rather than
being skipped. The capture preserves journal append failures and their original
closed reason union; it does not widen the shared recovery adapter contracts.

Inventory returns every retained request observation, the full source observation
history, each expected source's status, every expected invocation's request joins,
missing expected request IDs (including source-declared retry parents) and rows
not covered by an expected complete source enumeration. Request statuses are
`matched`, `missing-correlation`, `ambiguous-correlation`, `unmatched-attempt`,
`identity-mismatch` or `ambiguous-attempt`. An expected invocation with no request
is `missing-request`; duplicate exact invocation identities are ambiguous.

`complete` is true only with at least one expected source, all exact expected
source observations complete, all retained requests unambiguously matched,
every expected invocation matched, no missing expected request/retry-parent ID
and no unsealed source row. Historical absence is always returned in
`sourceHistory`, even when the caller explicitly requests a later complete capture.
It is not silently erased. This boolean addresses **supplied source/identity join
coverage only**. It neither authenticates a source's completeness assertion nor
certifies billing availability, raw trace custody, storage replication, effect
settlement or the full workload denominator. Those require independent evidence.
