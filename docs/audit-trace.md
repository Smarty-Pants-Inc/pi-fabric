# Fabric execution trace V1

The final `fabric_exec` result details form a bounded durable envelope that holds `success`, `trace`, the rich render `audits`, `phases`, and `error`. The privacy-projected trace stays the functional record for compaction, memory, and tool ownership. The audits persist verbatim with full arguments, results, and previews. A resumed transcript re-renders and expands the same way as the live one. Collapsed cards provide the visual boundary. The record itself keeps the full data. Final session JSONL details never contain logs, return values, type errors, media payloads, or in-memory media notes.

The serialized final details object never exceeds 512 KiB. Consumers use current traces structurally. The chat renderer allows one compatibility exception. It can match a pre-change bash digest against string literals or named strings already visible in the outer `fabric_exec` arguments, so old previews can show the original command.

## Envelope

```ts host
interface FabricPersistedExecutionDetailsV1 {
  success: boolean;
  trace: FabricExecutionTraceV1;
  assessment?: FabricAssessmentTraceV1; // only with trace.assessment
  audits: FabricLegacyRenderAudit[];
  phases: string[];
  error?: string;
}

interface FabricExecutionTraceV1 {
  kind: "pi-fabric.execution";
  version: 1;
  outcome: "succeeded" | "failed" | "aborted" | "timed_out";
  phases: string[];
  operations: FabricExecutionTraceOperationV1[];
  counts: {
    droppedValues: number;
    truncatedValues: number;
    redactedValues: number;
    droppedOperations: number;
  };
  error?: string;
}
```

A trace excludes run and call timestamps, elapsed durations, random call IDs, source code, media payloads, and arbitrary argument or result content. Runtime and call errors become fixed stage and outcome messages. Provider, validator, approval, or guest exception prose never enters the trace.

`phases` records occurrences in order. Repeated transitions stay in place, so `A → B → A` appears as `["A", "B", "A"]`.

## Call operation

```ts
interface FabricExecutionTraceOperationV1 {
  type: "call";
  sequence: number;
  ref: string;
  provider?: string;
  action?: string;
  args: Record<string, JsonValue>;
  outcome: "succeeded" | "failed" | "aborted" | "timed_out";
  failureStage?: "resolve" | "prepare" | "validate" | "approve" | "invoke" | "guard";
  error?: string;
  result?: JsonValue;
}
```

The host bridge assigns `sequence` when it receives any durable operation. A parallel completion only updates the existing record, and operation order stays unchanged. Fabric issues action attempts before reference resolution, preparation, schema validation, approval, and execution guards. Discovery and workflow attempts go out before their guards, lookups, validation, or activity mutation, so failures in those stages stay visible. The configured executor returns a typed termination reason. Trace sealing uses that reason for deadline and cancellation outcomes, and it never classifies exception text.

V1 keeps `type: "call"` for wire compatibility. Exact internal refs separate discovery, lifecycle, and combinator operations from provider action calls. V1 also leaves `result` optional, and discovery, workflow lifecycle, and combinator operations never persist one. The generic recorder drops provider results, with a single exception: the exact `{ created: true }` creation outcome from `pi.write`. No output or provider details accompany that outcome. Argument projection follows the exact reference:

- `pi.read`: local `path`, numeric `offset`, numeric `limit`
- `pi.grep`: local `path`, numeric `context`, numeric `limit`. Drops pattern and query
- `pi.find`, `pi.ls`: local `path`, numeric `limit`. Drops pattern and query
- `pi.edit`, `pi.write`: local `path` only. Drops edit replacements and write content. `pi.write` can keep `{ created: true }`
- `pi.bash`, `pi.powershell`: bounded command text
- selected `agents.*` lifecycle calls: `id` only. Drops task, message, instructions, names, model options, and outputs
- `mesh.publish`/`read`: topic/address and numeric cursor/limit. Drops payload text and data
- `mesh.get`/`put`/`delete`/`list`: key or prefix and limit. Drops values
- memory, state, schema, compact, MCP, extension, unknown, and external calls: no arguments or results

### Discovery operations

Read-only discovery still bypasses mutation authorization and approval budgets. Fabric persists every attempt in the same `sequence` space as actions and workflow activity:

- `fabric.discovery.providers`: no arguments or results
- `fabric.discovery.models`: no arguments or results
- `fabric.discovery.catalog`: identifier-shaped `provider` plus numeric `limit`. Drops catalog metadata and results
- `fabric.discovery.list`: identifier-shaped `provider` and `namespace`, plus numeric `limit`. Drops the free-form `query` and results
- `fabric.discovery.search`: numeric `limit`. Drops the free-form `query` and results
- `fabric.discovery.describe`: identifier-shaped action `ref`. Drops results

Each discovery operation records `succeeded`, `failed`, `aborted`, or `timed_out`, along with the applicable `guard`, `resolve`, or `invoke` stage. Model-registry enumeration keeps its best-effort empty-list behavior when enumeration throws. Fabric marks the corresponding operation failed.

### Workflow lifecycle operations

Declarative workflow calls still feed transient activity updates to the live UI. Each call also persists as a durable occurrence record:

- `fabric.workflow.configure`: `name`. Drops the description
- `fabric.workflow.phase`: `name`, identifier-shaped `id`, numeric `total`. Drops the description
- `fabric.workflow.item`: identifier-shaped `id`, `status`, `phase`, and `kind`, plus numeric `total` and `completed`. Drops label, detail, current value, data, and `meta`. The trace records the arguments as called, so an omitted `id` stays omitted. Status transitions also go to host listeners as [`pi-fabric:workflow-item:v1`](providers.md#workflow-item-events)
- `fabric.workflow.event`: identifier-shaped `level`. Drops message and data
- `fabric.workflow.progress`: no arguments. Drops the message

Fabric records these operations in bridge issue order, alongside actions and discovery. The separate `phases` compatibility field keeps occurrence order and retains repeated transitions.

### Workflow combinator spans

The shared guest implementation instruments calls to `workflow.parallel` and `workflow.pipeline` and records them as `fabric.workflow.parallel` and `fabric.workflow.pipeline`. Start creates one operation, and end updates that same operation. Persisted metadata stays limited to `kind`, numeric `itemCount`, numeric `stageCount` for pipelines, and the effective bounded `concurrency` for parallel calls. Empty combinators produce records too. A pipeline nests its parallel fan-out, so the pipeline operation goes out before the nested parallel operation, and both precede stage actions.

Guest span IDs are deterministic execution-local bridge correlation values. Fabric never persists them, and the internal start/end bridge stays closure-private, outside the guest API. Internal span calls skip provider resolution, authorization, approval, and agent-budget accounting. A thrown stage closes active spans as failed. A runtime failure, deadline, or cancellation seals any still-open operation with the typed final execution outcome.

### Owned-work stops

When a program is cancelled or times out, Fabric stops the [provider participants](providers.md#provider-participants) it owns and records one `fabric.participant.stop` operation per participant after the program's own operations. Arguments keep the identifier-shaped `ref` and `reason` (`program_cancelled`). The result keeps `outcome` (`confirmed` or `unconfirmed`) and, for an unconfirmed stop, `detail` (`declined`, `error`, or `timeout`). A confirmed stop succeeds and an unconfirmed one fails at the `invoke` stage.

### Saved program runs

Each [saved program](programs.md) that `programs.run` executes records one `fabric.program.run` operation right after the `programs.run` call, in the same sequence space. Its arguments hold `program` (`name@<full digest>`) and, for runs started by `/fabric run` or the program run event, `invokedBy: "host"`. The nested program's own operations follow it in the caller's trace. A type error fails the operation at stage `prepare`; a failed, aborted, or timed-out program fails it with that outcome.

Traces retain only plain local paths. They drop URL paths, together with credentials and query/fragment data. Plain paths lose their query/fragment suffixes as well. Sensitive-key normalization, media/base64 rejection, JSON safety, depth/node limits, and UTF-8 truncation still run after projection and add defense in depth. Projection provides the primary secrecy mechanism.

Identifiers (`ref`, `provider`, `action`), outcomes, failure stage, operation sequence, and occurrence-ordered phase labels stay durable. These fields, the retained local paths and mesh addresses, and bash command text are not secret containers. Callers must never place credentials in identifiers, local filenames, topics, keys, phase names, or commands.

## Assessment projection

With `trace.assessment: true` (default `false`), the details envelope also carries a separate, non-deterministic `assessment`. `FabricExecutionTraceV1` stays byte-for-byte what it would be without it.

```ts host
interface FabricAssessmentTraceV1 {
  kind: "pi-fabric.assessment";
  version: 1;
  outcome: "succeeded" | "failed" | "aborted" | "timed_out";
  durationMs: number; // whole program
  operations: Array<{
    sequence: number; // joins with the trace operation
    ref: string;
    outcome: "succeeded" | "failed" | "aborted" | "timed_out";
    durationMs: number;
    source?: "agent" | "jev" | "classifier" | "provider";
    model?: string;
    usage?: { input: number; output: number; cacheRead: number; cacheWrite: number; totalTokens: number; cost?: number };
  }>;
  totals: { operations: number; succeeded: number; failed: number; usage: Usage };
  counts: { droppedOperations: number };
}
```

Usage and model come only from what an operation already reports: agent run results (`usage`, `model`, including handoff `agent`), Jev `input_tokens`/`output_tokens`, provider results that carry the same shapes, and auto-approval classifier decisions. The projection holds no arguments, results, code, or error prose. It keeps at most 1,024 operation rows and 128 KiB. Totals still count every observed operation. Inside the 512 KiB envelope, assessment rows trim after display audits and before any trace operation. `isFabricAssessmentTraceV1` and `readFabricAssessmentTraceV1`, exported from the lightweight `pi-fabric/assessment` subpath, guard it.

## Reading and rendering traces

The package exports `isFabricExecutionTraceV1`, `isFabricExecutionTraceOperationV1`, `readFabricExecutionTraceV1`, `createFabricPersistedExecutionDetails`, and `readFabricExecutionRenderDetails`. Its guards reject malformed envelopes, extra fields, oversized data, and unknown versions.

Current sessions render resumed cards from the persisted `details.audits`. These audits win over the trace, and they restore full nested read bodies, edit diffs, bash output, and previews on expand. Sessions written before audit persistence hold the trace alone. Their audits come from operation metadata, which keeps bash command text. Each reconstructed audit carries a trace-derived marker, and its missing payloads show `not retained across reload`. The renderer shows no fabricated content. Old sessions that contain `details.audits` and `details.phases` still render through the legacy adapter. For old digest-only bash traces, the renderer matches the digest against literal and named strings that the outer `fabric_exec` arguments already expose. When the renderer finds no exact command, it drops the digest and shows no hash.

Compaction and memory read only `toolResult.details.trace`, and both pass it through the trace guard. Compaction emits phases and operations in sequence order with stable `entryId/subordinal` addresses. Memory emits one normalized child per operation with address `<outer-entry-id>/<sequence>`. Neither consumer parses `fabric_exec` source, outer output, operation results, or rendered audit prose to recover calls, files, or failures.

An invalid or unknown trace, when present, blocks semantic legacy reinterpretation. Compaction can use its separate strict old-session `details.audits` adapter only when the trace field is absent. Memory indexes trace operations only.

## Limitations

Persisted audits are verbatim. Resumed final rendering shows read bodies, edit diffs, write bodies, bash output, agent tasks, and provider results. Durable traces still store bounded projections. Bash command text stays in the trace, and arbitrary argument or result content never enters it. That exclusion covers discovery queries, workflow descriptions and data, external and MCP arguments, and provider results. When arguments are omitted from the trace, generic failure resolution keeps only the ref identity. The envelope stays bounded. Past 512 KiB, display-only audits trim first. Trace-only cards, old or trimmed, render `not retained across reload` markers for content the session record does not hold. Workflow activity content stays available only while the live execution result or activity store remains in memory.
