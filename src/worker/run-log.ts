// smarty-dev#2004 (row 11): a run's events.jsonl kept one line per streamed
// token delta, about 95% of lines, and every cumulative tool_execution_update.
// Readers need only the result: the transcript parser and progress preview use
// message_end and tool_execution_*; the live conversation view folds deltas by
// concatenation and shows only the latest partial tool result. So, at write time:
//   - merge consecutive message_update deltas of one content block into one line,
//   - keep only the latest tool_execution_update per tool call,
//   - drop toolResult message_start (its message_end follows at once, same message),
//   - retain every full tool_execution_end during the active run,
//   - drop turn_end.message (the assistant message_end just before repeats it),
// and flush what is held at least every RUN_LOG_FLUSH_MS, so live tails still
// stream. Held lines are written before the next other line, so every other line
// keeps its order; a held delta and a held tool update may swap with each other.
import fs from "node:fs";
import { randomUUID } from "node:crypto";

export const RUN_LOG_FLUSH_MS = 500;

// Shared with stdout admission: this is a UTF-16 character cap, not a byte cap.
export const MAX_EVENT_LINE_CHARS = 4 * 1024 * 1024;
// Keep the conservative cap band even at terminal compaction: rare duplication
// is preferable to loss. Actual on-disk canonical equivalence is required below.
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

interface LogRecord {
  offset: number;
  bytes: Buffer;
}

// Memory is bounded by one admitted record, not run length or tool count.
const MAX_LOG_RECORD_BYTES = MAX_EVENT_LINE_CHARS * 3 + 2;
function* logRecords(descriptor: number, size: number): Generator<LogRecord> {
  let offset = 0;
  let start = 0;
  let pending: Buffer[] = [];
  let pendingBytes = 0;
  while (offset < size) {
    const chunk = Buffer.allocUnsafe(Math.min(64 * 1024, size - offset));
    const count = fs.readSync(descriptor, chunk, 0, chunk.length, offset);
    if (count <= 0) throw new Error("Run log changed during compaction");
    let head = 0;
    for (let index = 0; index < count; index++) {
      if (chunk[index] !== 10) continue;
      const part = chunk.subarray(head, index + 1);
      if (pendingBytes + part.length > MAX_LOG_RECORD_BYTES) throw new Error("Oversized run log record");
      const bytes = pending.length ? Buffer.concat([...pending, part], pendingBytes + part.length) : part;
      yield { offset: start, bytes };
      start = offset + index + 1;
      pending = [];
      pendingBytes = 0;
      head = index + 1;
    }
    if (head < count) {
      pending.push(chunk.subarray(head, count));
      pendingBytes += count - head;
      if (pendingBytes > MAX_LOG_RECORD_BYTES) throw new Error("Oversized run log record");
    }
    offset += count;
  }
  // Incomplete tails stay byte-identical and cannot be canonical evidence.
  if (pendingBytes) yield { offset: start, bytes: Buffer.concat(pending, pendingBytes) };
}

const parseLogRecord = (bytes: Buffer): EventRecord | undefined => {
  if (bytes.at(-1) !== 10) return undefined;
  try {
    const value: unknown = JSON.parse(bytes.toString("utf8"));
    return isRecord(value) ? value : undefined;
  } catch {
    return undefined;
  }
};

// Scan instead of an unbounded index: worst-case quadratic terminal scan time,
// memory independent of run length. Ambiguous/reused IDs are kept in full.
const hasEquivalentCanonical = (
  descriptor: number, size: number, offset: number, end: EventRecord,
): boolean => {
  const result = end.result;
  if (!isRecord(result) || !Array.isArray(result.content) || typeof end.toolCallId !== "string" ||
    Object.hasOwn(end, "resultMetadata")) return false;
  let ends = 0;
  let starts = 0;
  let canonicals = 0;
  let equivalent = false;
  for (const record of logRecords(descriptor, size)) {
    const event = parseLogRecord(record.bytes);
    if (!event) continue;
    if (event.toolCallId === end.toolCallId) {
      if (event.type === "tool_execution_end") ends++;
      if (event.type === "tool_execution_start") starts++;
    }
    const message = event.message;
    if (event.type !== "message_end" || !isRecord(message) || message.role !== "toolResult" ||
      message.toolCallId !== end.toolCallId) continue;
    canonicals++;
    const { content: _content, details: _details, ...metadata } = result;
    const rehydrated = {
      content: message.content,
      ...(Object.hasOwn(message, "details") ? { details: message.details } : {}),
      ...metadata,
    };
    // Readers redact with an ordered node/character budget. Even semantically
    // equal objects with different key order can render different clipped
    // details. Require the exact reconstructed JSON shape/order, not just deep
    // equality; otherwise conservatively retain the main-kept full result.
    equivalent = record.offset > offset && message.toolName === end.toolName &&
      (message.isError === true) === (end.isError === true) &&
      JSON.stringify(rehydrated) === JSON.stringify(result);
  }
  return ends === 1 && starts <= 1 && canonicals === 1 && equivalent;
};

export interface RunLogCompaction {
  compacted: number;
  beforeBytes: number;
  afterBytes: number;
  error?: string;
}

/** Only the quiescent worker finish path may call this, never a live-log tailer. */
export const compactTerminalRunLog = (filePath: string, status: string): RunLogCompaction => {
  const outcome: RunLogCompaction = { compacted: 0, beforeBytes: 0, afterBytes: 0 };
  if (!["completed", "failed", "stopped", "timed_out"].includes(status)) return outcome;
  let source: number | undefined;
  let target: number | undefined;
  let temporary: string | undefined;
  try {
    const noFollow = typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0;
    source = fs.openSync(filePath, fs.constants.O_RDONLY | noFollow);
    const original = fs.fstatSync(source);
    if (!original.isFile()) return outcome;
    outcome.beforeBytes = outcome.afterBytes = original.size;
    // Canonical must actually exist in this durable file, not only in stdout
    // or a best-effort append callback. Fail closed if fsync is rejected.
    fs.fsyncSync(source);
    const candidate = `${filePath}.${process.pid}.${randomUUID()}.compact.tmp`;
    target = fs.openSync(candidate, "wx", original.mode & 0o777);
    temporary = candidate; // Cleanup only after exclusive ownership established.
    let compacted = 0;
    let afterBytes = 0;
    for (const record of logRecords(source, original.size)) {
      let bytes = record.bytes;
      const event = parseLogRecord(bytes);
      if (event?.type === "tool_execution_end" &&
        bytes.toString("utf8").trimEnd().length <= MAX_EVENT_LINE_CHARS - CANONICAL_ENVELOPE_RESERVE_CHARS &&
        hasEquivalentCanonical(source, original.size, record.offset, event)) {
        const result = event.result as EventRecord;
        const { content: _content, details: _details, ...resultMetadata } = result;
        bytes = Buffer.from(`${JSON.stringify({
          ...event,
          result: { elided: true, bytes: Buffer.byteLength(JSON.stringify(result), "utf8") },
          ...(Object.keys(resultMetadata).length ? { resultMetadata } : {}),
        })}\n`);
        compacted++;
      }
      let written = 0;
      while (written < bytes.length) {
        const count = fs.writeSync(target, bytes, written, bytes.length - written);
        if (count <= 0) throw new Error("Incomplete compacted log write");
        written += count;
      }
      afterBytes += bytes.length;
    }
    if (!compacted) return outcome;
    fs.fsyncSync(target);
    fs.closeSync(target);
    target = undefined;
    const current = fs.lstatSync(filePath);
    const held = fs.fstatSync(source);
    if (!current.isFile() || current.dev !== original.dev || current.ino !== original.ino ||
      current.size !== original.size || current.mtimeMs !== original.mtimeMs ||
      held.size !== original.size || held.mtimeMs !== original.mtimeMs) {
      throw new Error("Run log changed during terminal compaction");
    }
    // ponytail: retained-byte optimization, NOT fewer physical writes. Commit
    // last: earlier failures preserve the full source. Old FDs retain the full
    // terminal transcript. No live rewrite, journal, fake canonical, or
    // post-rename fallible commit step that could misreport preservation.
    fs.renameSync(temporary, filePath);
    temporary = undefined;
    outcome.compacted = compacted;
    outcome.afterBytes = afterBytes;
  } catch (error) {
    outcome.error = error instanceof Error ? error.message : String(error);
  } finally {
    if (target !== undefined) { try { fs.closeSync(target); } catch {} }
    if (source !== undefined) { try { fs.closeSync(source); } catch {} }
    if (temporary !== undefined) { try { fs.unlinkSync(temporary); } catch {} }
  }
  return outcome;
};
