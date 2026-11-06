type EventRecord = Record<string, unknown>;

const isRecord = (value: unknown): value is EventRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Persistence projection only, never a redactor or an input to worker accounting.
 * Pi's public final toolResult message has passed the session's result/message
 * hooks. Execution frames and history copies are not that boundary. In
 * particular, details, structuredContent and nested-call metadata are not
 * generally scrubbed by result guards and must not be copied to the run log.
 */
export const projectToolResultLogEvent = (event: EventRecord, runner: string): EventRecord | undefined => {
  // Raw ends still close native/nested calls, including calls with no later
  // canonical message. Construct metadata from native scalars only: never
  // persist their result or any opaque/supplemental execution fields.
  if (event.type === "tool_execution_end") {
    if (typeof event.toolCallId !== "string") return undefined;
    return {
      type: "tool_execution_end",
      toolCallId: event.toolCallId,
      ...(typeof event.toolName === "string" ? { toolName: event.toolName } : {}),
      ...(typeof event.isError === "boolean" ? { isError: event.isError } : {}),
      ...(typeof event.durationMs === "number" && Number.isFinite(event.durationMs)
        ? { durationMs: event.durationMs } : {}),
    };
  }
  if (event.type === "tool_execution_update" || event.type === "tool_result") return undefined;

  // Preserve lifecycle field presence for history consumers, never its payload.
  // Handle turn_end before toolResult messages so its message is always stripped.
  if (event.type === "turn_end") {
    const { message: _message, toolResults: _toolResults, ...metadata } = event;
    return { ...metadata, ...(Object.hasOwn(event, "toolResults") ? { toolResults: [] } : {}) };
  }
  if (event.type === "agent_end") {
    const { messages: _messages, ...metadata } = event;
    return { ...metadata, ...(Object.hasOwn(event, "messages") ? { messages: [] } : {}) };
  }

  const message = isRecord(event.message) ? event.message : undefined;
  if (message?.role === "toolResult") {
    if (event.type !== "message_end" || (runner !== "pi" && runner !== "pi-durable") ||
      typeof message.toolCallId !== "string" || typeof message.toolName !== "string") return undefined;
    // Keep the existing live transcript/raw-log result shape, but emit it only
    // from final guarded content. Do not also persist the repeated message body.
    return {
      type: "tool_execution_end",
      toolCallId: message.toolCallId,
      toolName: message.toolName,
      result: { content: message.content ?? [] },
      isError: message.isError === true,
      ...(typeof message.durationMs === "number" && Number.isFinite(message.durationMs)
        ? { durationMs: message.durationMs } : {}),
    };
  }

  // History is used in memory for admission/recovery, not as another source of
  // result payloads. Even post-hook messages can still contain unsanitized details.
  if (event.type === "response" && event.command === "get_messages") {
    return {
      type: "response",
      ...(typeof event.id === "string" ? { id: event.id } : {}),
      command: "get_messages",
      success: event.success === true,
    };
  }
  if (event.type === "entry_appended") return undefined;
  // Claude tool inputs have no guard boundary either. Project tool-use blocks
  // rather than copying opaque arguments; retain native non-tool content as-is.
  if (runner === "claude" && event.type === "assistant" && message?.role === "assistant" &&
    Array.isArray(message.content) &&
    message.content.some(part => isRecord(part) && part.type === "tool_use")) {
    const content = message.content.map(part =>
      isRecord(part) && part.type === "tool_use" ? {
        type: "tool_use",
        ...(typeof part.id === "string" ? { id: part.id } : {}),
        ...(typeof part.name === "string" ? { name: part.name } : {}),
      } : part);
    return { ...event, message: { ...message, content } };
  }
  // Claude has no Pi final-result guard boundary. Keep only native completion
  // IDs/error flags for the transcript, never content, details, other mixed
  // blocks or the duplicate top-level tool_use_result envelope. Accounting still
  // receives the original event; do not invent a fallback for missing native IDs.
  if (runner === "claude" && event.type === "user" &&
    (Object.hasOwn(event, "tool_use_result") || (Array.isArray(message?.content) &&
      message.content.some(part => isRecord(part) && part.type === "tool_result")))) {
    const content = Array.isArray(message?.content) ? message.content.flatMap(part =>
      isRecord(part) && part.type === "tool_result" && typeof part.tool_use_id === "string"
        ? [{ type: "tool_result", tool_use_id: part.tool_use_id, is_error: part.is_error === true, content: [] }]
        : []) : [];
    return content.length > 0 ? { type: "user", message: { role: "user", content } } : undefined;
  }

  return event;
};
