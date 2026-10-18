import fs from "node:fs";
import { openRunLogSource, RunLogReadError, type RunLogSource } from "./run-log-source.js";
import type { FabricLogLine } from "./agents/types.js";

const READ_CHUNK_BYTES = 64 * 1024;
const DEFAULT_READ_MAX_BYTES = 8 * 1024 * 1024;

export interface JsonlPage {
  lines: FabricLogLine[];
  hasMore: boolean;
  before?: number;
  /** Device/inode of the opened descriptor; bind the next before to this value. */
  generation?: string;
}

const parseLine = (offset: number, raw: string): FabricLogLine => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { offset, raw };
  }
  return { offset, raw, parsed };
};

interface BufferedLine {
  offset: number;
  raw: string;
}

const completeLines = (buffer: Buffer, bufferStart: number, fileEnd: number): BufferedLine[] => {
  const lines: BufferedLine[] = [];
  let start = 0;
  let first = true;
  for (let index = 0; index < buffer.length; index++) {
    if (buffer[index] !== 10) continue;
    if (!first || bufferStart === 0) {
      const raw = buffer.subarray(start, index).toString("utf8").replace(/\r$/, "");
      if (raw) lines.push({ offset: bufferStart + start, raw });
    }
    first = false;
    start = index + 1;
  }
  if (fileEnd === bufferStart + buffer.length && start < buffer.length) {
    if (!first || bufferStart === 0) {
      const raw = buffer.subarray(start).toString("utf8").replace(/\r$/, "");
      if (raw) lines.push({ offset: bufferStart + start, raw });
    }
  }
  return lines;
};

/** Read a bounded JSONL page backwards from an already validated descriptor. */
export const readJsonlPageFromDescriptor = (
  descriptor: number | RunLogSource,
  limit: number,
  before?: number,
  knownSize?: number,
  maxBytes?: number,
  beforeGeneration?: string,
): JsonlPage => {
  try {
    const stat = typeof descriptor === "number" ? fs.fstatSync(descriptor, { bigint: true }) : descriptor.stat;
    const generation = `${stat.dev}:${stat.ino}`;
    if (beforeGeneration !== undefined && beforeGeneration !== generation) {
      const error = new Error("cursor-stale: Log generation changed; re-read from start without before or beforeGeneration");
      error.name = "cursor-stale";
      throw error;
    }
    // Sample identity and read bytes from the same FD, including across rename.
    const size = knownSize ?? (typeof descriptor === "number" ? Number(stat.size) : descriptor.size);
    const fileEnd = typeof before === "number" && Number.isSafeInteger(before)
      ? Math.max(0, Math.min(before, size))
      : size;
    const boundedLimit = Math.max(1, Math.trunc(limit));
    const boundedBytes = Math.max(
      1,
      Math.trunc(maxBytes ?? DEFAULT_READ_MAX_BYTES),
    );
    const chunks: Buffer[] = [];
    let bufferStart = fileEnd;
    let bufferedBytes = 0;
    let newlineCount = 0;

    while (
      bufferStart > 0 &&
      newlineCount <= boundedLimit &&
      bufferedBytes < boundedBytes
    ) {
      const length = Math.min(READ_CHUNK_BYTES, bufferStart, boundedBytes - bufferedBytes);
      const chunkStart = bufferStart - length;
      const chunk = Buffer.allocUnsafe(length);
      const bytesRead = typeof descriptor === "number" ? fs.readSync(descriptor, chunk, 0, length, chunkStart)
        : descriptor.read(chunk, 0, length, chunkStart);
      if (bytesRead <= 0) break;
      const captured = chunk.subarray(0, bytesRead);
      chunks.push(captured);
      for (const byte of captured) {
        if (byte === 0x0a) newlineCount += 1;
      }
      bufferedBytes += bytesRead;
      bufferStart = chunkStart;
    }

    const buffer = Buffer.concat(chunks.reverse(), bufferedBytes);
    const records = completeLines(buffer, bufferStart, fileEnd);

    const selected = records.slice(-boundedLimit);
    const hasMore = selected.length > 0 && (records.length > selected.length || bufferStart > 0);
    return {
      lines: selected.map((line) => parseLine(line.offset, line.raw)),
      hasMore,
      generation,
      ...(hasMore ? { before: selected[0]!.offset } : {}),
    };
  } catch (error) {
    if ((typeof descriptor !== "number" && error instanceof RunLogReadError) ||
      (error instanceof Error && error.name === "cursor-stale")) throw error;
    return { lines: [], hasMore: false };
  }
};

const readPage = (
  filePath: string,
  limit: number,
  before?: number,
  maxBytes?: number,
  beforeGeneration?: string,
  runLog = false,
): JsonlPage => {
  let descriptor: number | RunLogSource | undefined;
  try {
    descriptor = runLog ? openRunLogSource(filePath) : fs.openSync(filePath, "r");
    return readJsonlPageFromDescriptor(descriptor, limit, before, undefined, maxBytes, beforeGeneration);
  } catch (error) {
    if ((runLog && error instanceof RunLogReadError) || (error instanceof Error && error.name === "cursor-stale")) throw error;
    return { lines: [], hasMore: false };
  } finally {
    if (descriptor !== undefined) {
      try { if (typeof descriptor === "number") fs.closeSync(descriptor); else descriptor.close(); } catch {}
    }
  }
};

/** Plain JSONL paging; legacy bare before offsets remain unbound. */
export const readJsonlPage = (
  filePath: string, limit: number, before?: number, maxBytes?: number, beforeGeneration?: string,
): JsonlPage => readPage(filePath, limit, before, maxBytes, beforeGeneration);

/** Run events use decoded byte offsets and the selected artifact's physical generation. */
export const readRunLogPage = (
  filePath: string, limit: number, before?: number, maxBytes?: number, beforeGeneration?: string,
): JsonlPage => readPage(filePath, limit, before, maxBytes, beforeGeneration, true);
