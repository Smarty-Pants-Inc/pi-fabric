type EventRecord = Record<string, unknown>;
const isRecord = (value: unknown): value is EventRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Normalize legacy Pi RPC and native stream envelopes for both worker liveness observers.
 * Prefer the legacy field when both are present; ignore malformed envelopes.
 * Preserve delta bytes verbatim: whitespace is bounded, not recovery progress.
 */
export const assistantStreamEvent = (event: EventRecord): EventRecord | undefined => {
  if (event.type !== "message_update") return undefined;
  if (isRecord(event.assistantMessageEvent)) return event.assistantMessageEvent;
  if (isRecord(event.event)) return event.event;
  return undefined;
};
