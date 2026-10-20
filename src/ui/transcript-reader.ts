import { createHash } from "node:crypto";
import { openRunLogSource, runLogReadErrorDetail, type RunLogSource } from "../run-log-source.js";
import { readJsonlPageFromDescriptor } from "../log-tail.js";
import type { FabricLogLine } from "../agents/types.js";
import {
  missingToolStartIds,
  toolLifecycleContext,
  parsedEvents,
  parseRaw,
  TranscriptAccumulator,
} from "./transcript-parser.js";
import type { FabricAgentTranscript, FabricTranscriptSource } from "./transcript.js";

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
const descriptorStats = (descriptor: RunLogSource): DescriptorStats => {
  const stat = descriptor.stat;
  const size = descriptor.size;
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
  /** Raw page and tool-context evidence, independent of sanitized projection. */
  pageDigest: string;
  transcript: FabricAgentTranscript;
}

interface ForwardTranscriptPage {
  lines: FabricLogLine[];
  end: number;
}

const completeLogEnd = (descriptor: RunLogSource, size: number, fallback = 0): number => {
  if (size <= 0) return 0;
  const scanFloor = Math.max(0, size - MAX_PAGE_BYTES);
  let scanEnd = size;
  while (scanEnd > scanFloor) {
    const scanStart = Math.max(scanFloor, scanEnd - FORWARD_READ_CHUNK_BYTES);
    const chunk = Buffer.allocUnsafe(scanEnd - scanStart);
    const bytesRead = descriptor.read(chunk, 0, chunk.length, scanStart);
    if (bytesRead <= 0) return 0;
    for (let index = bytesRead - 1; index >= 0; index--) {
      if (chunk[index] === 0x0a) return scanStart + index + 1;
    }
    scanEnd = scanStart;
  }
  return Math.min(fallback, size);
};

const readForwardPage = (
  descriptor: RunLogSource,
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
    const bytesRead = descriptor.read(chunk, 0, chunkSize, readOffset);
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
    let descriptor: RunLogSource | undefined;
    try {
      descriptor = openRunLogSource(filePath);
      const stat = descriptorStats(descriptor);
      const sameFile = cached?.device === stat.dev && cached.inode === stat.ino;
      if (cached && !sameFile && stat.size === cached.offset && cached.pageEnd > cached.pageStart) {
        // Compression/restore changes the physical generation, not decoded
        // offsets. Prove the raw pinned page AND hydration context before
        // retaining its bookmarks; changed content still takes the reset path.
        const completeEnd = completeLogEnd(descriptor, stat.size, cached.completeEnd);
        const page = readForwardPage(descriptor, cached.pageStart, Math.min(cached.pageEnd, completeEnd));
        const rebound = this.#stateForPage(descriptor, stat, completeEnd, page.lines,
          cached.pageStart, page.end, cached.hasMore);
        if (page.end === cached.pageEnd && rebound.pageDigest === cached.pageDigest) {
          this.#remember(filePath, rebound);
          return rebound.transcript;
        }
      }
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
    } catch (error) {
      if ((error as NodeJS.ErrnoException)?.code === "ENOENT" && !cached) {
        return { entries: [], truncated: false, hasMore: false, hasNewer: false };
      }
      const reason = runLogReadErrorDetail(error).slice(0, 300);
      return {
        ...(cached?.transcript ?? { truncated: false, hasMore: false, hasNewer: false }),
        unavailable: true,
        error: reason,
        entries: [{ id: "run-log-unavailable", kind: "error", label: "Transcript unavailable", status: "failed",
          text: `${reason}${cached ? " · Showing cached history; source could not be verified." : ""}` },
          ...(cached?.transcript.entries ?? [])],
      };
    } finally {
      if (descriptor !== undefined) {
        try { descriptor.close(); } catch {}
      }
    }
  }

  loadOlder(source: FabricTranscriptSource): boolean {
    const filePath = source.logFile;
    if (!filePath) return false;
    this.read(source, false);
    const cached = this.#cache.get(filePath);
    if (!cached?.hasMore || cached.pageStart <= 0) return false;
    let descriptor: RunLogSource | undefined;
    try {
      descriptor = openRunLogSource(filePath);
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
        try { descriptor.close(); } catch {}
      }
    }
  }

  loadNewer(source: FabricTranscriptSource): boolean {
    const filePath = source.logFile;
    if (!filePath) return false;
    this.read(source, false);
    const cached = this.#cache.get(filePath);
    if (!cached || cached.pageEnd >= cached.completeEnd) return false;
    let descriptor: RunLogSource | undefined;
    try {
      descriptor = openRunLogSource(filePath);
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
        try { descriptor.close(); } catch {}
      }
    }
  }

  loadLatest(source: FabricTranscriptSource): boolean {
    const filePath = source.logFile;
    if (!filePath) return false;
    this.read(source, false);
    const cached = this.#cache.get(filePath);
    let descriptor: RunLogSource | undefined;
    try {
      descriptor = openRunLogSource(filePath);
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
        try { descriptor.close(); } catch {}
      }
    }
  }

  clear(): void {
    this.#cache.clear();
  }

  #latestState(
    descriptor: RunLogSource,
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
    descriptor: RunLogSource,
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
    const evidence: FabricLogLine[] = [];
    if (missingStarts.size > 0 && pageStart > 0) {
      const contextPage = readJsonlPageFromDescriptor(
        descriptor,
        TOOL_LIFECYCLE_CONTEXT_LINES,
        pageStart,
        stat.size,
        MAX_PAGE_BYTES,
      );
      evidence.push(...contextPage.lines);
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
      pageDigest: createHash("sha256").update(JSON.stringify([...evidence, ...lines].map(({ offset, raw }) => [offset, raw]))).digest("hex"),
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
