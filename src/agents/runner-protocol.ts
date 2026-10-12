import { Type, type Static } from "typebox";

/**
 * JSON Schemas for the file protocol between Fabric and a worker process
 * (custom worker runners). Paths come from the launch context `files`.
 * Readers ignore unknown fields, so writers may add their own.
 */
export const FABRIC_WORKER_PROTOCOL_VERSION = 1 as const;

const usage = Type.Object({
  input: Type.Number({ minimum: 0 }),
  output: Type.Number({ minimum: 0 }),
  cacheRead: Type.Number({ minimum: 0 }),
  cacheWrite: Type.Number({ minimum: 0 }),
  cost: Type.Number({ minimum: 0 }),
});

/** `statusFile`: the worker atomically replaces this record (write temp, rename). */
export const AgentRunRecordSchema = Type.Object(
  {
    id: Type.String(),
    name: Type.String(),
    task: Type.String(),
    status: Type.Union([
      Type.Literal("queued"),
      Type.Literal("running"),
      Type.Literal("completed"),
      Type.Literal("failed"),
      Type.Literal("stopped"),
      Type.Literal("timed_out"),
    ]),
    runner: Type.String(),
    transport: Type.String(),
    cwd: Type.String(),
    startedAt: Type.Number(),
    updatedAt: Type.Number(),
    finishedAt: Type.Optional(Type.Number()),
    turns: Type.Integer({ minimum: 0 }),
    toolCalls: Type.Integer({ minimum: 0 }),
    /** Latest assistant text; the final answer once completed. */
    text: Type.String(),
    /** Structured result when the run had a schema. */
    value: Type.Optional(Type.Unknown()),
    error: Type.Optional(Type.String()),
    usage,
    model: Type.Optional(Type.String()),
    thinking: Type.Optional(Type.String()),
    currentTool: Type.Optional(Type.String()),
    runnerSessionId: Type.Optional(Type.String()),
    /** Set while a routed dialog waits for an answer. */
    blockedOn: Type.Optional(Type.Object({ since: Type.Number(), decisionId: Type.Optional(Type.String()) })),
    /** Hosted runs only. */
    hosted: Type.Optional(Type.Object({ locator: Type.Unknown() })),
    sleeping: Type.Optional(Type.Literal(true)),
    outcome: Type.Optional(Type.Literal("indeterminate")),
    retryable: Type.Optional(Type.Boolean()),
  },
  { additionalProperties: true },
);
export type AgentRunRecordWire = Static<typeof AgentRunRecordSchema>;

const tokenUsage = Type.Object({
  runId: Type.String(),
  name: Type.String(),
  runner: Type.String(),
  depth: Type.Integer({ minimum: 0 }),
  actorId: Type.Optional(Type.String()),
  actorName: Type.Optional(Type.String()),
  cumulativeTokens: Type.Number({ minimum: 0 }),
  input: Type.Number({ minimum: 0 }),
  output: Type.Number({ minimum: 0 }),
  cacheRead: Type.Number({ minimum: 0 }),
  cacheWrite: Type.Number({ minimum: 0 }),
  cost: Type.Number({ minimum: 0 }),
});

const childQuestion = Type.Object({
  requestId: Type.String({ minLength: 1, maxLength: 256 }),
  method: Type.Union([Type.Literal("select"), Type.Literal("confirm"), Type.Literal("input"), Type.Literal("editor")]),
  title: Type.String({ maxLength: 1_000 }),
  message: Type.Optional(Type.String({ maxLength: 8_000 })),
  options: Type.Optional(Type.Array(Type.String(), { maxItems: 64 })),
  placeholder: Type.Optional(Type.String({ maxLength: 8_000 })),
  prefill: Type.Optional(Type.String({ maxLength: 8_000 })),
  timeout: Type.Integer({ minimum: 1, maximum: 86_400_000 }),
});

/** `lifecycleFile`: one JSON object per line, appended. */
export const LifecycleLineSchema = Type.Union([
  Type.Object({
    version: Type.Literal(1),
    event: Type.Literal("tokens.usage"),
    occurredAt: Type.Number(),
    /** Per-event increase, never a running total. */
    data: tokenUsage,
  }),
  Type.Object({
    version: Type.Literal(1),
    event: Type.Literal("question"),
    occurredAt: Type.Number(),
    /** Answered by a `ui_response` steer command with the same requestId. */
    data: childQuestion,
  }),
  Type.Object({
    version: Type.Literal(1),
    event: Type.Union([
      Type.Literal("pi.input"),
      Type.Literal("pi.agent_start"),
      Type.Literal("pi.agent_end"),
      Type.Literal("pi.turn_end"),
      Type.Literal("pi.agent_settled"),
      Type.Literal("pi.tool_error"),
      Type.Literal("pi.session_compact"),
    ]),
    occurredAt: Type.Number(),
    data: Type.Optional(Type.Unknown()),
  }),
]);
export type LifecycleLine = Static<typeof LifecycleLineSchema>;

/** `logFile`: transcript events, one per line; other lines are ignored by the reader. */
export const TranscriptEventSchema = Type.Union([
  Type.Object({
    type: Type.Literal("message_end"),
    message: Type.Object({
      role: Type.Union([Type.Literal("user"), Type.Literal("assistant")]),
      content: Type.Union([
        Type.String(),
        Type.Array(Type.Object({ type: Type.Literal("text"), text: Type.String() })),
      ]),
    }),
  }),
  Type.Object({
    type: Type.Literal("tool_execution_start"),
    toolCallId: Type.Optional(Type.String()),
    toolName: Type.String(),
    args: Type.Optional(Type.Unknown()),
  }),
  Type.Object({
    type: Type.Literal("tool_execution_end"),
    toolCallId: Type.Optional(Type.String()),
    toolName: Type.String(),
    result: Type.Optional(Type.Unknown()),
    isError: Type.Optional(Type.Boolean()),
  }),
  Type.Object({ type: Type.Literal("extension_error"), error: Type.String() }),
]);
export type TranscriptEvent = Static<typeof TranscriptEventSchema>;

const steeringMode = Type.Union([Type.Literal("all"), Type.Literal("one-at-a-time")]);

/** `steerFile`: Fabric appends one command per line; the worker tails it. */
export const SteerCommandSchema = Type.Union([
  Type.Object({
    type: Type.Literal("steer"),
    id: Type.String(),
    ts: Type.Number(),
    message: Type.String(),
    data: Type.Optional(Type.Unknown()),
  }),
  Type.Object({
    type: Type.Literal("follow_up"),
    id: Type.String(),
    ts: Type.Number(),
    message: Type.String(),
    data: Type.Optional(Type.Unknown()),
  }),
  Type.Object({ type: Type.Literal("set_steering_mode"), id: Type.String(), ts: Type.Number(), mode: steeringMode }),
  Type.Object({ type: Type.Literal("set_follow_up_mode"), id: Type.String(), ts: Type.Number(), mode: steeringMode }),
  Type.Object({
    type: Type.Literal("compact"),
    id: Type.String(),
    ts: Type.Number(),
    instructions: Type.Optional(Type.String()),
  }),
  Type.Object({
    type: Type.Literal("ui_response"),
    id: Type.String(),
    ts: Type.Number(),
    requestId: Type.String(),
    value: Type.Optional(Type.String()),
    confirmed: Type.Optional(Type.Boolean()),
    cancelled: Type.Optional(Type.Literal(true)),
  }),
]);
export type SteerCommand = Static<typeof SteerCommandSchema>;
