// smarty-dev#2004 (row 11): a run's events.jsonl kept one line per streamed
// token delta, about 95% of lines, and every cumulative tool_execution_update.
// Readers need only the result: the transcript parser and progress preview use
// message_end and tool_execution_*; the live conversation view folds deltas by
// concatenation and shows only the latest partial tool result. So, at write time:
//   - merge consecutive message_update deltas of one content block into one line,
//   - keep only the latest tool_execution_update per tool call,
//   - drop toolResult message_start (its message_end follows at once, same message),
//   - elide tool_execution_end.result except near the worker's character cap
//     (canonical toolResult message_end holds content/details; preserve extras),
//   - drop turn_end.message (the assistant message_end just before repeats it),
// and flush what is held at least every RUN_LOG_FLUSH_MS, so live tails still
// stream. Held lines are written before the next other line, so every other line
// keeps its order; a held delta and a held tool update may swap with each other.
export const RUN_LOG_FLUSH_MS = 500;

// Shared with stdout admission: this is a UTF-16 character cap, not a byte cap.
export const MAX_EVENT_LINE_CHARS = 4 * 1024 * 1024;
// Pi's canonical envelope reuses IDs/name/content/details/usage from the end;
// only role/timestamp and envelope syntax grow (at most 65 chars for Pi's
// record results, including a 24-char JSON number timestamp and missing content
// normalized to []; message_start is two chars larger than message_end).
// ponytail: reserve 128 chars and keep near-cap ends whole, rather than buffer
// parallel completions awaiting canonical admission. This covers envelope-cap
// loss, not a missing canonical event after interruption or other filtering.
const CANONICAL_ENVELOPE_RESERVE_CHARS = 128;

const DELTA_TYPES = new Set(["text_delta", "thinking_delta", "toolcall_delta"]);

type EventRecord = Record<string, unknown>;

const isRecord = (value: unknown): value is EventRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export interface RunLogWriter {
  /** One stdout event line; `event` is its parsed form when the line is a JSON object. */
  event(line: string, event: EventRecord | undefined): void;
  /** A worker-authored record (already newline-terminated); written after anything held. */
  raw(text: string): void;
  flush(): void;
}

export const createRunLogWriter = (
  append: (text: string) => void,
  flushMs = RUN_LOG_FLUSH_MS,
): RunLogWriter => {
  let delta: EventRecord | undefined;
  const toolUpdates = new Map<string, string>();
  let timer: ReturnType<typeof setTimeout> | undefined;

  const flush = (): void => {
    if (timer) clearTimeout(timer);
    timer = undefined;
    let text = delta ? `${JSON.stringify(delta)}\n` : "";
    delta = undefined;
    for (const line of toolUpdates.values()) text += `${line}\n`;
    toolUpdates.clear();
    if (text) append(text);
  };
  const hold = (): void => {
    if (timer) return;
    timer = setTimeout(flush, flushMs);
    timer.unref?.();
  };

  return {
    event(line, event) {
      if (event?.type === "message_update") {
        const next = event.assistantMessageEvent;
        if (isRecord(next) && DELTA_TYPES.has(String(next.type)) && typeof next.delta === "string") {
          const held = delta?.assistantMessageEvent as EventRecord | undefined;
          if (held && held.type === next.type && held.contentIndex === next.contentIndex) {
            // The latest event's other fields (usage, any partial message) win.
            delta = { ...event, assistantMessageEvent: { ...next, delta: `${String(held.delta)}${next.delta}` } };
          } else {
            flush();
            delta = event;
          }
          hold();
          return;
        }
      }
      if (event?.type === "tool_execution_update" && typeof event.toolCallId === "string") {
        // partialResult is cumulative, so the latest update carries all earlier ones.
        toolUpdates.set(event.toolCallId, line);
        hold();
        return;
      }
      flush();
      if (event?.type === "message_start" && isRecord(event.message) && event.message.role === "toolResult") return;
      if (event?.type === "tool_execution_end" && event.result !== undefined &&
        line.length <= MAX_EVENT_LINE_CHARS - CANONICAL_ENVELOPE_RESERVE_CHARS) {
        // Count the JSON payload's UTF-8 bytes, not JS UTF-16 code units.
        // Never mutate the live event: the worker still consumes the raw result.
        const bytes = Buffer.byteLength(JSON.stringify(event.result), "utf8");
        // Pi's canonical message omits tool-specific fields such as terminate.
        const { content: _content, details: _details, ...resultMetadata } = isRecord(event.result) ? event.result : {};
        append(`${JSON.stringify({
          ...event,
          result: { elided: true, bytes },
          ...(Object.keys(resultMetadata).length > 0 ? { resultMetadata } : {}),
        })}\n`);
        return;
      }
      if (event?.type === "turn_end" && event.message !== undefined) {
        const { message: _repeated, ...rest } = event;
        append(`${JSON.stringify(rest)}\n`);
        return;
      }
      append(`${line}\n`);
    },
    raw(text) {
      flush();
      append(text);
    },
    flush,
  };
};
