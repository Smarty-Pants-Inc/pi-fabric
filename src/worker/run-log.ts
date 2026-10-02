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
import { performance } from "node:perf_hooks";

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
function* logRecords(descriptor: number, size: number, checkWork: () => void): Generator<LogRecord> {
  let offset = 0;
  let start = 0;
  let pending: Buffer[] = [];
  let pendingBytes = 0;
  while (offset < size) {
    checkWork();
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

// ponytail: two streaming passes plus one exact-offset reread per unique
// canonical. Keep only counts/positions, never payloads. Total source reads
// are at most 3 * file size, and record visits at most 3 * admitted records.
// Exceptional raw-full fallback bounds synchronous terminal work, not the run
// timeout. The manager starts its independent clock after transport launch and
// allows 1s exit grace; worker.startedAt + timeoutMs is NOT that deadline.
export const MAX_TERMINAL_LOG_BYTES = 64 * 1024 * 1024;
export const MAX_TERMINAL_LOG_RECORDS = 100_000;
export const MAX_TERMINAL_LOG_WORK_MS = 500;

// ponytail: raw events.jsonl consumers (including smarty-install's child probe)
// read small tool results directly, without transcript rehydration. Keep them
// verbatim; only larger duplicate payloads justify a canonical-result pointer.
export const MIN_ELIDED_TOOL_RESULT_BYTES = 8 * 1024;

interface ToolCorrelation {
  ends: number;
  starts: number;
  lastStart: number;
  canonicals: number;
  otherCompletions: number;
  canonicalOffset: number;
  canonicalBytes: number;
}

const hasEquivalentCanonical = (
  descriptor: number, offset: number, end: EventRecord,
  correlation: ToolCorrelation | undefined, checkWork: () => void,
): boolean => {
  const result = end.result;
  if (!isRecord(result) || !Array.isArray(result.content) || typeof end.toolCallId !== "string" ||
    Object.hasOwn(end, "resultMetadata") || !correlation || correlation.ends !== 1 ||
    correlation.starts > 1 || correlation.canonicals !== 1 || correlation.otherCompletions !== 0 ||
    correlation.lastStart > offset || correlation.canonicalOffset <= offset) return false;
  checkWork();
  const bytes = Buffer.allocUnsafe(correlation.canonicalBytes);
  let read = 0;
  while (read < bytes.length) {
    checkWork();
    const count = fs.readSync(descriptor, bytes, read, bytes.length - read, correlation.canonicalOffset + read);
    if (count <= 0) throw new Error("Run log changed during compaction");
    read += count;
  }
  const event = parseLogRecord(bytes);
  const message = event?.message;
  if (event?.type !== "message_end" || !isRecord(message) || message.role !== "toolResult" ||
    message.toolCallId !== end.toolCallId) return false;
  const { content: _content, details: _details, ...metadata } = result;
  const rehydrated = {
    content: message.content,
    ...(Object.hasOwn(message, "details") ? { details: message.details } : {}),
    ...metadata,
  };
  // Readers redact with an ordered node/character budget. Preserve exact JSON
  // shape/key order, not merely semantic equality, as before.
  return message.toolName === end.toolName &&
    (message.isError === true) === (end.isError === true) &&
    JSON.stringify(rehydrated) === JSON.stringify(result);
};

export interface RunToolSettlement {
  format: 1;
  state: "settled" | "in_flight" | "unknown";
  reason?: string;
}

/** Settlement is not ancestor-PID absence or terminal status. Reuse the complete,
 * bounded run-log record reader: only matching execution ends discharge starts.
 * Unreadable, partial, lossy or changed logs cannot be positive cleanup evidence. */
export const readRunToolSettlement = (filePath: string, toolCalls?: unknown): RunToolSettlement => {
  let descriptor: number | undefined;
  const started = performance.now();
  const unknown = (reason: string): RunToolSettlement => ({ format: 1, state: "unknown", reason });
  try {
    const noFollow = typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0;
    descriptor = fs.openSync(filePath, fs.constants.O_RDONLY | noFollow);
    const original = fs.fstatSync(descriptor);
    if (!original.isFile() || fs.realpathSync.native(filePath) !== filePath) return unknown("unsafe tool execution log");
    if (!original.size || original.size > MAX_TERMINAL_LOG_BYTES) return unknown("missing or over-budget tool execution log");
    const checkWork = (): void => {
      if (performance.now() - started >= MAX_TERMINAL_LOG_WORK_MS) throw new Error("tool execution log inspection exceeded work bound");
    };
    const pending = new Map<string, string>();
    let records = 0;
    let starts = 0;
    for (const record of logRecords(descriptor, original.size, checkWork)) {
      if (++records > MAX_TERMINAL_LOG_RECORDS) return unknown("tool execution log inspection exceeded record bound");
      const event = parseLogRecord(record.bytes);
      if (!event || typeof event.type !== "string") return unknown("malformed or truncated tool execution log");
      if (event.type === "worker_warning" && typeof event.warning === "string" && event.warning.startsWith("Dropped an oversized agent event line")) {
        return unknown("tool execution log contains a dropped event");
      }
      if (!["tool_execution_start", "tool_execution_end", "tool_execution_update"].includes(event.type)) continue;
      if (typeof event.toolCallId !== "string" || !event.toolCallId || typeof event.toolName !== "string" || !event.toolName) {
        return unknown("missing tool execution identity");
      }
      const id = event.toolCallId;
      if (event.type === "tool_execution_start") {
        if (pending.has(id)) return unknown(`duplicate in-flight tool execution ${id}`);
        pending.set(id, event.toolName);
        starts++;
      } else {
        if (pending.get(id) !== event.toolName) return unknown(`unmatched tool execution ${event.type} for ${id}`);
        if (event.type === "tool_execution_end") pending.delete(id);
      }
    }
    const final = fs.fstatSync(descriptor);
    const named = fs.lstatSync(filePath);
    if (final.size !== original.size || final.mtimeMs !== original.mtimeMs || final.ctimeMs !== original.ctimeMs ||
        named.dev !== original.dev || named.ino !== original.ino || !named.isFile()) return unknown("tool execution log changed during inspection");
    if (toolCalls !== undefined && (!Number.isSafeInteger(toolCalls) || (toolCalls as number) < 0 || (toolCalls as number) > starts)) {
      return unknown("tool execution count does not match durable starts");
    }
    return pending.size ? { format: 1, state: "in_flight", reason: `${pending.size} in-flight tool execution(s): ${[...pending.keys()].slice(0, 10).join(", ")}` }
      : { format: 1, state: "settled" };
  } catch (error) {
    return unknown(`cannot verify tool execution log: ${error instanceof Error ? error.message : String(error)}`);
  } finally { if (descriptor !== undefined) fs.closeSync(descriptor); }
};

export interface RunLogCompaction {
  compacted: number;
  beforeBytes: number;
  afterBytes: number;
  error?: string;
  compactionSkipped?: string;
}

/** Only the quiescent worker finish path may call this, never a live-log tailer. */
export const compactTerminalRunLog = (filePath: string, status: string): RunLogCompaction => {
  const outcome: RunLogCompaction = { compacted: 0, beforeBytes: 0, afterBytes: 0 };
  if (!["completed", "failed", "stopped", "timed_out"].includes(status)) return outcome;
  let source: number | undefined;
  let target: number | undefined;
  let temporary: string | undefined;
  const started = performance.now();
  const skip = (bound: string): never => {
    outcome.compactionSkipped = `Terminal run-log compaction skipped: ${bound} work bound exceeded; full log retained`;
    throw new Error(outcome.compactionSkipped);
  };
  const checkWork = (): void => {
    if (performance.now() - started >= MAX_TERMINAL_LOG_WORK_MS) {
      skip(`MAX_TERMINAL_LOG_WORK_MS=${MAX_TERMINAL_LOG_WORK_MS}`);
    }
  };
  try {
    const noFollow = typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0;
    // Windows FlushFileBuffers (fsync) requires GENERIC_WRITE. O_RDWR grants
    // that capability without creating, truncating, or writing the source;
    // failure to obtain write access must retain the full log, like fsync failure.
    source = fs.openSync(filePath, fs.constants.O_RDWR | noFollow);
    const original = fs.fstatSync(source);
    if (!original.isFile()) return outcome;
    outcome.beforeBytes = outcome.afterBytes = original.size;
    if (original.size > MAX_TERMINAL_LOG_BYTES) {
      skip(`MAX_TERMINAL_LOG_BYTES=${MAX_TERMINAL_LOG_BYTES}`);
    }
    checkWork();
    // Canonical must actually exist in this durable file, not only in stdout
    // or a best-effort append callback. Fail closed if fsync is rejected.
    fs.fsyncSync(source);
    const index = new Map<string, ToolCorrelation>();
    const correlation = (id: string): ToolCorrelation => {
      let entry = index.get(id);
      if (!entry) {
        if (index.size >= MAX_TERMINAL_LOG_RECORDS) skip(`MAX_TERMINAL_LOG_RECORDS=${MAX_TERMINAL_LOG_RECORDS} correlation entries`);
        entry = { ends: 0, starts: 0, lastStart: -1, canonicals: 0, otherCompletions: 0, canonicalOffset: -1, canonicalBytes: 0 };
        index.set(id, entry);
      }
      return entry;
    };
    let records = 0;
    for (const record of logRecords(source, original.size, checkWork)) {
      checkWork();
      if (++records > MAX_TERMINAL_LOG_RECORDS) skip(`MAX_TERMINAL_LOG_RECORDS=${MAX_TERMINAL_LOG_RECORDS}`);
      const event = parseLogRecord(record.bytes);
      if (!event) continue;
      if (typeof event.toolCallId === "string") {
        if (event.type === "tool_execution_end") correlation(event.toolCallId).ends++;
        if (event.type === "tool_execution_start") {
          const entry = correlation(event.toolCallId);
          entry.starts++;
          entry.lastStart = record.offset;
        }
      }
      const message = event.message;
      // Legacy session/Claude starts also restart IDs in the existing readers.
      // They only add a boundary: retain the original explicit-start count guard.
      if ((event.type === "message" || event.type === "assistant") && isRecord(message) &&
        message.role === "assistant" && Array.isArray(message.content)) {
        for (const part of message.content) {
          if (isRecord(part) && part.type === (event.type === "message" ? "toolCall" : "tool_use") &&
            typeof part.id === "string") correlation(part.id).lastStart = record.offset;
        }
      }
      // Other receiver-recognized completions can finish a prior same-ID call.
      // Count ambiguity only: message_end remains the sole canonical evidence.
      if (event.type === "message" && isRecord(message) && message.role === "toolResult" &&
        typeof message.toolCallId === "string") correlation(message.toolCallId).otherCompletions++;
      if (event.type === "user" && isRecord(message) && Array.isArray(message.content)) {
        for (const part of message.content) {
          if (isRecord(part) && part.type === "tool_result" && typeof part.tool_use_id === "string") {
            correlation(part.tool_use_id).otherCompletions++;
          }
        }
      }
      if (event.type === "message_end" && isRecord(message) && message.role === "toolResult" &&
        typeof message.toolCallId === "string") {
        const entry = correlation(message.toolCallId);
        entry.canonicals++;
        entry.canonicalOffset = record.offset;
        entry.canonicalBytes = record.bytes.length;
      }
    }
    checkWork();
    const candidate = `${filePath}.${process.pid}.${randomUUID()}.compact.tmp`;
    target = fs.openSync(candidate, "wx", original.mode & 0o777);
    temporary = candidate; // Cleanup only after exclusive ownership established.
    let compacted = 0;
    let afterBytes = 0;
    for (const record of logRecords(source, original.size, checkWork)) {
      checkWork();
      let bytes = record.bytes;
      const event = parseLogRecord(bytes);
      const resultBytes = event?.type === "tool_execution_end" && isRecord(event.result)
        ? Buffer.byteLength(JSON.stringify(event.result), "utf8") : 0;
      if (event?.type === "tool_execution_end" && resultBytes > MIN_ELIDED_TOOL_RESULT_BYTES &&
        bytes.toString("utf8").trimEnd().length <= MAX_EVENT_LINE_CHARS - CANONICAL_ENVELOPE_RESERVE_CHARS &&
        hasEquivalentCanonical(source, record.offset, event, index.get(String(event.toolCallId)), checkWork)) {
        const result = event.result as EventRecord;
        const { content: _content, details: _details, ...resultMetadata } = result;
        bytes = Buffer.from(`${JSON.stringify({
          ...event,
          result: { elided: true, bytes: resultBytes },
          ...(Object.keys(resultMetadata).length ? { resultMetadata } : {}),
        })}\n`);
        compacted++;
      }
      let written = 0;
      while (written < bytes.length) {
        checkWork();
        const count = fs.writeSync(target, bytes, written, bytes.length - written);
        if (count <= 0) throw new Error("Incomplete compacted log write");
        written += count;
      }
      afterBytes += bytes.length;
    }
    if (!compacted) return outcome;
    checkWork();
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
    // Windows may reject replacement while our source handle is open. Both
    // source and temp have been synced; release every owned handle before commit.
    fs.closeSync(source);
    source = undefined;
    // ponytail: keep this native-source-worker module self-contained, like
    // run-record's rename retry (a .js dependency cannot resolve under Node's
    // direct .ts worker entry). Never unlink the destination to force success.
    // Commit last: every failure retains the full source, with no post-rename
    // fallible step that could misreport preservation. External readers may
    // briefly contend, so retry only transient codes with bounded backoff.
    for (let attempt = 1; ; attempt++) {
      checkWork();
      // Closing the handle and waiting must not relax the source-generation
      // guard: recheck identity, size and mtime immediately before EACH attempt.
      const latest = fs.lstatSync(filePath);
      if (!latest.isFile() || latest.dev !== original.dev || latest.ino !== original.ino ||
        latest.size !== original.size || latest.mtimeMs !== original.mtimeMs) {
        throw new Error("Run log changed during terminal compaction");
      }
      try {
        fs.renameSync(temporary, filePath);
        break;
      } catch (error) {
        const code = typeof error === "object" && error !== null && "code" in error ? String(error.code) : undefined;
        if (attempt >= 8 || !["EPERM", "EACCES", "EEXIST", "EBUSY"].includes(code ?? "")) throw error;
        const delay = Math.min(25 * attempt, Math.max(0, MAX_TERMINAL_LOG_WORK_MS - (performance.now() - started)));
        try { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delay); } catch {
          // Atomics.wait unavailable: retry immediately, still bounded by count/time.
        }
      }
    }
    temporary = undefined;
    outcome.compacted = compacted;
    outcome.afterBytes = afterBytes;
  } catch (error) {
    if (!outcome.compactionSkipped) outcome.error = error instanceof Error ? error.message : String(error);
  } finally {
    if (target !== undefined) { try { fs.closeSync(target); } catch {} }
    if (source !== undefined) { try { fs.closeSync(source); } catch {} }
    if (temporary !== undefined) { try { fs.unlinkSync(temporary); } catch {} }
  }
  return outcome;
};
