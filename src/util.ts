import fs from "node:fs";
import { detachedPreview } from "./core/preview.js";
import { HEADLINE_ARG_KEYS } from "./core/call-preview.js";
import { forEachRawTextLine } from "./ui/preview-lines.js";
import { MAX_WRITE_DIFF_BYTES } from "./providers/write-diff-limits.js";
import { readJsonlPageFromDescriptor } from "./log-tail.js";
import type { FabricLogLine } from "./agents/types.js";
import type { PreviewLineEntry } from "./ui/preview-lines.js";
import type {
  FabricAgentTranscript,
  FabricAgentToolPreview,
  FabricAgentToolPreviewNode,
  FabricTranscriptEntry,
  FabricTranscriptSource,
} from "./ui/transcript.js";
import {
  missingToolStartIds,
  toolLifecycleContext,
  parsedEvents,
  parseRaw,
  TranscriptAccumulator,
} from "./ui/transcript-parser.js";
import { recordOf } from "./ui/transcript-sanitization.js";

// Keep the shared preview and transcript runtime in this existing eager owner.
// Public UI/core/provider modules retain their compatibility bindings.

export const countNewlines = (value: string): number => {
  let count = 0;
  for (let index = 0; index < value.length; index++) {
    if (value.charCodeAt(index) === 10) count++;
  }
  return count;
};

export const truncateMiddle = (value: string, maxChars: number): string => {
  if (value.length <= maxChars) return detachedPreview(value, value.length);
  const marker = `\n\n... ${value.length - maxChars} characters omitted by Pi Fabric ...\n\n`;
  const available = Math.max(0, maxChars - marker.length);
  const head = Math.ceil(available / 2);
  const tail = Math.floor(available / 2);
  return `${detachedPreview(value, head)}${marker}${detachedPreview(value.slice(value.length - tail), tail)}`;
};

// A shared, provider-agnostic heuristic for the one-line "headline" argument
// of a nested Fabric call — the value shown beside a tool's name in the chat
// preview (e.g. `recall <query>`) and appended to its ref in the dashboard
// (e.g. `extensions.vcc_recall · <query>`). Pi core tools and the first-class
// Fabric providers (agents/mesh/mcp management) keep their bespoke, tuned
// previews; this picks up everything else — captured extension tools and
// arbitrary MCP tools — so they no longer render as a bare tool name.
//
// The key order preserves the dashboard's prior task -> path -> query ->
// message preference for the fields it already recognized, then extends to
// other common payload names. The final first-string fallback covers tools
// whose argument names are unfamiliar, skipping structural/metadata keys
// (label/title/mode/limit/...) that describe the call rather than its
// payload. Control characters are not stripped here: the inline preview
// already tolerates them (matching the existing task fallback) and the
// dashboard sanitizes labels via safeText at render time.
const HEADLINE_SKIP_KEYS = new Set([
  "label", "title", "type", "kind", "mode", "format", "resultFormat",
  "limit", "max", "offset", "start", "concurrency", "overwrite", "id",
  "provider", "namespace", "server", "tool", "ref", "recursive", "synthesize",
  "commandDigest",
]);

const cleanOneLine = (value: string, max: number): string => {
  const single = value.replace(/\s+/g, " ").trim();
  if (!single) return "";
  return single.length <= max
    ? detachedPreview(single, max)
    : `${detachedPreview(single, Math.max(1, max - 1))}…`;
};

/**
 * Pick the best single-line argument value to display beside a nested call's
 * tool name, for tools that have no bespoke preview. Returns `undefined`
 * when no string payload argument is present.
 */
export const headlineArg = (
  args: Record<string, unknown> | undefined,
  max = 96,
): string | undefined => {
  if (!args) return undefined;
  for (const key of HEADLINE_ARG_KEYS) {
    const value = args[key];
    if (typeof value === "string") {
      const cleaned = cleanOneLine(value, max);
      if (cleaned) return cleaned;
    }
  }
  for (const [key, value] of Object.entries(args)) {
    if (HEADLINE_SKIP_KEYS.has(key)) continue;
    if (typeof value === "string") {
      const cleaned = cleanOneLine(value, max);
      if (cleaned) return cleaned;
    }
  }
  return undefined;
};

// Adapted from pi-code-previews; see THIRD_PARTY_NOTICES.md.

export function countContentLines(content: string): number {
  if (!content) return 0;
  let terminators = 0;
  for (let index = 0; index < content.length; index++) {
    const code = content.charCodeAt(index);
    if (code === 13) {
      terminators++;
      if (content.charCodeAt(index + 1) === 10) index++;
    } else if (code === 10) {
      terminators++;
    }
  }
  const finalCode = content.charCodeAt(content.length - 1);
  return terminators + (finalCode === 10 || finalCode === 13 ? 0 : 1);
}

export function selectPreviewTextLines(
  text: string,
  limit: number,
): { entries: Array<PreviewLineEntry<string>>; shown: number; hidden: number; total: number } {
  if (!Number.isInteger(limit)) {
    const lines = collectPreviewTextLines(text);
    return { ...selectPreviewLines(lines, limit), total: lines.length };
  }

  const entries: Array<PreviewLineEntry<string>> = [];
  const split = limit >= 8;
  const head = split ? Math.ceil(limit * 0.65) : limit;
  const tailLimit = split ? Math.max(1, limit - head - 1) : 0;
  const tail: Array<string | undefined> = [];
  let tailSize = 0;
  let tailCursor = 0;
  let total = 0;
  forEachPreviewTextLine(text, (line, index) => {
    total++;
    if (limit <= 0 || index < limit) entries.push({ kind: "line", line, index });
    if (!split || index < head) return;
    tail[tailCursor] = line;
    if (++tailCursor === tailLimit) tailCursor = 0;
    if (tailSize < tailLimit) tailSize++;
  });

  if (limit <= 0 || total <= limit) return { entries, shown: total, hidden: 0, total };
  if (!split) return { entries, shown: limit, hidden: total - limit, total };
  const hidden = total - head - tailLimit;
  const selected = entries.slice(0, head);
  selected.push({ kind: "hidden", hidden });
  let tailSlot = tailSize === tailLimit ? tailCursor : 0;
  for (let offset = 0; offset < tailSize; offset++) {
    const line = tail[tailSlot];
    if (line === undefined) throw new RangeError(`Missing preview tail line ${offset}`);
    selected.push({ kind: "line", line, index: total - tailSize + offset });
    if (++tailSlot === tailLimit) tailSlot = 0;
  }
  return { entries: selected, shown: head + tailLimit, hidden, total };
}

function selectPreviewLines<T>(
  lines: T[],
  limit: number,
): { entries: Array<PreviewLineEntry<T>>; shown: number; hidden: number } {
  if (lines.length <= limit || limit <= 0) {
    return {
      entries: lines.map((line, index) => ({ kind: "line", line, index })),
      shown: lines.length,
      hidden: 0,
    };
  }
  if (limit < 8) {
    return {
      entries: lines.slice(0, limit).map((line, index) => ({ kind: "line", line, index })),
      shown: limit,
      hidden: lines.length - limit,
    };
  }
  const head = Math.ceil(limit * 0.65);
  const tail = Math.max(1, limit - head - 1);
  const hidden = lines.length - head - tail;
  return {
    entries: [
      ...lines.slice(0, head).map((line, index) => ({ kind: "line" as const, line, index })),
      { kind: "hidden", hidden },
      ...lines.slice(-tail).map((line, offset) => ({
        kind: "line" as const,
        line,
        index: lines.length - tail + offset,
      })),
    ],
    shown: head + tail,
    hidden,
  };
}

function collectPreviewTextLines(text: string): string[] {
  const lines: string[] = [];
  forEachPreviewTextLine(text, (line) => lines.push(line));
  return lines;
}

function forEachPreviewTextLine(
  text: string,
  callback: (line: string, index: number) => void,
): void {
  let index = 0;
  let pendingEmpty = 0;
  forEachRawTextLine(text, (line) => {
    if (line === "") {
      pendingEmpty++;
      return;
    }
    while (pendingEmpty > 0) {
      callback("", index++);
      pendingEmpty--;
    }
    callback(line, index++);
  });
  if (index === 0 && pendingEmpty > 0 && text.length > 0) callback("", index);
}

// Pure write-diff size guards shared by the write-preview tool wrapper and the
// dashboard's core-tool-render. Keep this module free of imports from
// "@earendil-works/pi-coding-agent": it is part of the lazily imported
// dashboard graph, which cannot resolve pi's host package in managed
// installs (issue #13).

const configuredMaxChangedLineCells = Number.parseInt(
  process.env.CODE_PREVIEW_MAX_WRITE_DIFF_CHANGED_LINE_CELLS ?? "",
  10,
);
const MAX_WRITE_DIFF_CHANGED_LINE_CELLS =
  Number.isFinite(configuredMaxChangedLineCells) && configuredMaxChangedLineCells > 0
    ? configuredMaxChangedLineCells
    : 1_000_000;

export const writeContentForPreview = (content: string): string | undefined =>
  Buffer.byteLength(content, "utf8") <= MAX_WRITE_DIFF_BYTES ? content : undefined;

export const shouldSkipWriteDiffBytes = (...texts: string[]): boolean => {
  let total = 0;
  for (const text of texts) {
    total += Buffer.byteLength(text, "utf8");
    if (total > MAX_WRITE_DIFF_BYTES) return true;
  }
  return false;
};

export const shouldSkipWriteDiffComplexity = (before: string, after: string): boolean => {
  const beforeLines = before.split("\n");
  const afterLines = after.split("\n");
  const sharedLimit = Math.min(beforeLines.length, afterLines.length);
  let prefix = 0;
  while (prefix < sharedLimit && beforeLines[prefix] === afterLines[prefix]) prefix++;

  let suffix = 0;
  const suffixLimit = sharedLimit - prefix;
  while (
    suffix < suffixLimit &&
    beforeLines[beforeLines.length - suffix - 1] ===
      afterLines[afterLines.length - suffix - 1]
  ) {
    suffix++;
  }

  const changedBefore = beforeLines.length - prefix - suffix;
  const changedAfter = afterLines.length - prefix - suffix;
  return changedBefore * changedAfter > MAX_WRITE_DIFF_CHANGED_LINE_CELLS;
};

const PAGE_LINES = 40;
const TOOL_LIFECYCLE_CONTEXT_LINES = PAGE_LINES * 4;
const MAX_PAGE_BYTES = 512 * 1024;
const MAX_CACHE_ENTRIES = 32;
const FORWARD_READ_CHUNK_BYTES = 64 * 1024;

// Windows file IDs can exceed Number.MAX_SAFE_INTEGER. Keep identity exact while
// retaining safe numeric byte offsets and the reader's existing timestamp API.
interface DescriptorStats {
  dev: bigint;
  ino: bigint;
  size: number;
  mtimeMs: number;
  isFile(): boolean;
}
const descriptorStats = (descriptor: number): DescriptorStats => {
  const stat = fs.fstatSync(descriptor, { bigint: true });
  const size = Number(stat.size);
  if (!Number.isSafeInteger(size)) throw new Error("file size exceeds safe byte offsets");
  return { dev: stat.dev, ino: stat.ino, size, mtimeMs: Number(stat.mtimeNs) / 1_000_000,
    isFile: () => stat.isFile() };
};

interface CachedTranscript {
  device: bigint;
  inode: bigint;
  modifiedAt: number;
  offset: number;
  completeEnd: number;
  pageStart: number;
  pageEnd: number;
  hasMore: boolean;
  transcript: FabricAgentTranscript;
}

interface ForwardTranscriptPage {
  lines: FabricLogLine[];
  end: number;
}

const completeLogEnd = (descriptor: number, size: number, fallback = 0): number => {
  if (size <= 0) return 0;
  const scanFloor = Math.max(0, size - MAX_PAGE_BYTES);
  let scanEnd = size;
  while (scanEnd > scanFloor) {
    const scanStart = Math.max(scanFloor, scanEnd - FORWARD_READ_CHUNK_BYTES);
    const chunk = Buffer.allocUnsafe(scanEnd - scanStart);
    const bytesRead = fs.readSync(descriptor, chunk, 0, chunk.length, scanStart);
    if (bytesRead <= 0) return 0;
    for (let index = bytesRead - 1; index >= 0; index--) {
      if (chunk[index] === 0x0a) return scanStart + index + 1;
    }
    scanEnd = scanStart;
  }
  return Math.min(fallback, size);
};

const readForwardPage = (
  descriptor: number,
  start: number,
  end: number,
): ForwardTranscriptPage => {
  const lines: FabricLogLine[] = [];
  const readLimit = Math.min(end, Math.max(0, start) + MAX_PAGE_BYTES);
  let readOffset = Math.max(0, start);
  let pending = Buffer.alloc(0);
  let pendingOffset = readOffset;
  let pageEnd = readOffset;

  while (readOffset < readLimit && lines.length < PAGE_LINES) {
    const chunkSize = Math.min(FORWARD_READ_CHUNK_BYTES, readLimit - readOffset);
    const chunk = Buffer.allocUnsafe(chunkSize);
    const bytesRead = fs.readSync(descriptor, chunk, 0, chunkSize, readOffset);
    if (bytesRead <= 0) break;
    const data = pending.length > 0
      ? Buffer.concat([pending, chunk.subarray(0, bytesRead)])
      : chunk.subarray(0, bytesRead);
    const dataOffset = pending.length > 0 ? pendingOffset : readOffset;
    let lineStart = 0;
    for (let index = 0; index < data.length; index++) {
      if (data[index] !== 0x0a) continue;
      const raw = data.subarray(lineStart, index).toString("utf8").replace(/\r$/, "");
      pageEnd = dataOffset + index + 1;
      if (raw) {
        const offset = dataOffset + lineStart;
        const parsed = parseRaw(raw);
        lines.push({ offset, raw, ...(parsed ? { parsed } : {}) });
        if (lines.length >= PAGE_LINES) return { lines, end: pageEnd };
      }
      lineStart = index + 1;
    }
    pending = Buffer.from(data.subarray(lineStart));
    pendingOffset = dataOffset + lineStart;
    readOffset += bytesRead;
  }

  return { lines, end: readOffset >= end ? end : pageEnd };
};

export class AgentTranscriptReader {
  readonly #cache = new Map<string, CachedTranscript>();

  read(
    source: FabricTranscriptSource,
    followLatest = true,
  ): FabricAgentTranscript {
    const filePath = source.logFile;
    if (!filePath) {
      return { entries: [], truncated: false, hasMore: false, hasNewer: false };
    }
    const cached = this.#cache.get(filePath);
    let descriptor: number | undefined;
    try {
      const noFollow = typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0;
      descriptor = fs.openSync(filePath, fs.constants.O_RDONLY | noFollow);
      const stat = descriptorStats(descriptor);
      if (!stat.isFile()) {
        return cached?.transcript ?? {
          entries: [],
          truncated: false,
          hasMore: false,
          hasNewer: false,
        };
      }
      const sameFile = cached?.device === stat.dev && cached.inode === stat.ino;
      const sameSizeRewrite =
        sameFile && cached.offset === stat.size && cached.modifiedAt !== stat.mtimeMs;
      let state: CachedTranscript;
      if (!cached || !sameFile || stat.size < cached.offset || sameSizeRewrite) {
        state = this.#latestState(descriptor, stat);
      } else if (stat.size !== cached.offset || stat.mtimeMs !== cached.modifiedAt) {
        const wasAtTail = cached.pageEnd >= cached.completeEnd;
        const completeEnd = completeLogEnd(descriptor, stat.size, cached.completeEnd);
        if (
          cached.pageEnd > completeEnd ||
          (followLatest && wasAtTail && completeEnd > cached.completeEnd)
        ) {
          state = this.#latestState(descriptor, stat, completeEnd);
        } else {
          state = {
            ...cached,
            modifiedAt: stat.mtimeMs,
            offset: stat.size,
            completeEnd,
            transcript: {
              ...cached.transcript,
              hasNewer: cached.pageEnd < completeEnd,
              updatedAt: stat.mtimeMs,
            },
          };
        }
      } else {
        state = cached;
      }
      this.#remember(filePath, state);
      return state.transcript;
    } catch {
      return cached?.transcript ?? {
        entries: [],
        truncated: false,
        hasMore: false,
        hasNewer: false,
      };
    } finally {
      if (descriptor !== undefined) {
        try { fs.closeSync(descriptor); } catch {}
      }
    }
  }

  loadOlder(source: FabricTranscriptSource): boolean {
    const filePath = source.logFile;
    if (!filePath) return false;
    this.read(source, false);
    const cached = this.#cache.get(filePath);
    if (!cached?.hasMore || cached.pageStart <= 0) return false;
    let descriptor: number | undefined;
    try {
      const noFollow = typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0;
      descriptor = fs.openSync(filePath, fs.constants.O_RDONLY | noFollow);
      const stat = descriptorStats(descriptor);
      if (!stat.isFile() || stat.dev !== cached.device || stat.ino !== cached.inode) return false;
      const completeEnd = completeLogEnd(descriptor, stat.size, cached.completeEnd);
      const pageEnd = Math.min(cached.pageStart, completeEnd);
      const page = readJsonlPageFromDescriptor(
        descriptor,
        PAGE_LINES,
        pageEnd,
        stat.size,
        MAX_PAGE_BYTES,
      );
      const pageStart = page.lines[0]?.offset;
      if (pageStart === undefined) return false;
      const state = this.#stateForPage(
        descriptor,
        stat,
        completeEnd,
        page.lines,
        pageStart,
        pageEnd,
        page.hasMore,
      );
      this.#remember(filePath, state);
      return true;
    } catch {
      return false;
    } finally {
      if (descriptor !== undefined) {
        try { fs.closeSync(descriptor); } catch {}
      }
    }
  }

  loadNewer(source: FabricTranscriptSource): boolean {
    const filePath = source.logFile;
    if (!filePath) return false;
    this.read(source, false);
    const cached = this.#cache.get(filePath);
    if (!cached || cached.pageEnd >= cached.completeEnd) return false;
    let descriptor: number | undefined;
    try {
      const noFollow = typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0;
      descriptor = fs.openSync(filePath, fs.constants.O_RDONLY | noFollow);
      const stat = descriptorStats(descriptor);
      if (!stat.isFile() || stat.dev !== cached.device || stat.ino !== cached.inode) return false;
      const completeEnd = completeLogEnd(descriptor, stat.size, cached.completeEnd);
      const page = readForwardPage(descriptor, cached.pageEnd, completeEnd);
      if (page.lines.length === 0 || page.end <= cached.pageEnd) return false;
      const state = this.#stateForPage(
        descriptor,
        stat,
        completeEnd,
        page.lines,
        cached.pageEnd,
        page.end,
        cached.pageEnd > 0,
      );
      this.#remember(filePath, state);
      return true;
    } catch {
      return false;
    } finally {
      if (descriptor !== undefined) {
        try { fs.closeSync(descriptor); } catch {}
      }
    }
  }

  loadLatest(source: FabricTranscriptSource): boolean {
    const filePath = source.logFile;
    if (!filePath) return false;
    this.read(source, false);
    const cached = this.#cache.get(filePath);
    let descriptor: number | undefined;
    try {
      const noFollow = typeof fs.constants.O_NOFOLLOW === "number" ? fs.constants.O_NOFOLLOW : 0;
      descriptor = fs.openSync(filePath, fs.constants.O_RDONLY | noFollow);
      const stat = descriptorStats(descriptor);
      if (!stat.isFile()) return false;
      if (cached && (stat.dev !== cached.device || stat.ino !== cached.inode)) return false;
      const completeEnd = completeLogEnd(descriptor, stat.size, cached?.completeEnd ?? 0);
      const state = this.#latestState(descriptor, stat, completeEnd);
      this.#remember(filePath, state);
      return true;
    } catch {
      return false;
    } finally {
      if (descriptor !== undefined) {
        try { fs.closeSync(descriptor); } catch {}
      }
    }
  }

  clear(): void {
    this.#cache.clear();
  }

  #latestState(
    descriptor: number,
    stat: DescriptorStats,
    knownCompleteEnd?: number,
  ): CachedTranscript {
    const completeEnd = knownCompleteEnd ?? completeLogEnd(descriptor, stat.size);
    const page = readJsonlPageFromDescriptor(
      descriptor,
      PAGE_LINES,
      completeEnd,
      stat.size,
      MAX_PAGE_BYTES,
    );
    return this.#stateForPage(
      descriptor,
      stat,
      completeEnd,
      page.lines,
      page.lines[0]?.offset ?? completeEnd,
      completeEnd,
      page.hasMore,
    );
  }

  #stateForPage(
    descriptor: number,
    stat: DescriptorStats,
    completeEnd: number,
    lines: FabricLogLine[],
    pageStart: number,
    pageEnd: number,
    hasMore: boolean,
  ): CachedTranscript {
    const events = parsedEvents(lines);
    const missingStarts = missingToolStartIds(events);
    const lifecycleContext: Array<Record<string, unknown>> = [];
    if (missingStarts.size > 0 && pageStart > 0) {
      const contextPage = readJsonlPageFromDescriptor(
        descriptor,
        TOOL_LIFECYCLE_CONTEXT_LINES,
        pageStart,
        stat.size,
        MAX_PAGE_BYTES,
      );
      const contextEvents = parsedEvents(contextPage.lines);
      lifecycleContext.push(...toolLifecycleContext(contextEvents, missingStarts));
    }
    const accumulator = new TranscriptAccumulator();
    accumulator.append([...lifecycleContext, ...events]);
    const transcript = {
      ...accumulator.snapshot(hasMore, stat.mtimeMs, Number.MAX_SAFE_INTEGER),
      hasNewer: pageEnd < completeEnd,
    };
    return {
      device: stat.dev,
      inode: stat.ino,
      modifiedAt: stat.mtimeMs,
      offset: stat.size,
      completeEnd,
      pageStart,
      pageEnd,
      hasMore: transcript.hasMore ?? false,
      transcript,
    };
  }

  #remember(filePath: string, state: CachedTranscript): void {
    this.#cache.delete(filePath);
    this.#cache.set(filePath, state);
    while (this.#cache.size > MAX_CACHE_ENTRIES) {
      const oldest = this.#cache.keys().next().value as string | undefined;
      if (!oldest) break;
      this.#cache.delete(oldest);
    }
  }
}

export const projectAgentTranscript = (
  events: Array<Record<string, unknown>>,
  olderAvailable = false,
): FabricAgentTranscript => {
  const accumulator = new TranscriptAccumulator();
  accumulator.append(events);
  return accumulator.snapshot(olderAvailable);
};

const PREVIEW_TREE_GUARD_MAX_DEPTH = 8;

const isFabricAgentToolPreviewNode = (
  value: unknown,
  depth: number,
): value is FabricAgentToolPreviewNode => {
  const record = recordOf(value);
  if (
    !record ||
    typeof record.id !== "string" ||
    typeof record.name !== "string" ||
    (record.text !== undefined && typeof record.text !== "string") ||
    (record.currentTool !== undefined && typeof record.currentTool !== "string") ||
    !Array.isArray(record.tools)
  ) {
    return false;
  }
  if (record.agents === undefined) return true;
  if (depth >= PREVIEW_TREE_GUARD_MAX_DEPTH || !Array.isArray(record.agents)) return false;
  return record.agents.every((child) => isFabricAgentToolPreviewNode(child, depth + 1));
};

export const isFabricAgentToolPreview = (value: unknown): value is FabricAgentToolPreview =>
  recordOf(value)?.kind === "fabric-agent-tools" && isFabricAgentToolPreviewNode(value, 0);

export const recentTranscriptTools = (
  transcript: FabricAgentTranscript,
  limit = 2,
): FabricTranscriptEntry[] => {
  const tools = transcript.entries.filter((entry) => entry.kind === "tool");
  const boundedLimit = Math.max(1, limit);
  const running = tools.filter((entry) => entry.status === "running");
  const completed = tools.filter((entry) => entry.status !== "running");
  const completedSlots = Math.max(0, boundedLimit - Math.min(running.length, boundedLimit));
  const retained = new Set([
    ...running.slice(-boundedLimit),
    ...completed.slice(-completedSlots),
  ]);
  return tools
    .filter((entry) => retained.has(entry))
    .slice(-boundedLimit)
    .map((entry) => ({ ...entry }));
};
