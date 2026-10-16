// Configured filesystem memory source: a PortableMemorySource over a local
// directory of Pi session JSONL files. Native agent trees
// (sessions/<encoded-cwd>/*.jsonl) and flat archive directories enumerate
// through the same recursive walk; only the root-relative path becomes the
// opaque session key.

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { FabricMemorySourceConfig } from "../config.js";
import { readSessionHeader } from "./normalize.js";
import {
  MEMORY_SOURCE_INTERFACE_VERSION,
  createMemorySourceRegistry,
  defineMemorySource,
  type MemorySourceListPage,
  type MemorySourceRecord,
  type MemorySourceRegistry,
  type MemorySourceSessionDescriptor,
  type MemorySourceSnapshot,
  type PortableMemorySource,
} from "./portable.js";

/** Stable machine-readable coverage reasons (engine pattern-validated). */
const REASON_MAX_SESSIONS = "fs_source_max_sessions";
const REASON_SCAN_CAPPED = "fs_source_scan_capped";

/** Defensive walk bound: directory entries examined per list call. */
const SCAN_ENTRY_LIMIT = 100_000;

export interface FileSystemMemorySourceOptions {
  id: string;
  root: string;
}

interface DiscoveredSession {
  file: string;
  sessionKey: string;
  mtimeMs: number;
}

const statMtimeMs = (file: string): number => {
  try {
    return fs.statSync(file).mtimeMs;
  } catch {
    return 0;
  }
};

const compareByRecency = (left: DiscoveredSession, right: DiscoveredSession): number => {
  if (right.mtimeMs !== left.mtimeMs) return right.mtimeMs - left.mtimeMs;
  return left.file < right.file ? -1 : left.file > right.file ? 1 : 0;
};

/** Collect every *.jsonl file under root, newest first (mtime, then path).
 *  Symlinks are skipped: cycles cannot hang a listing, and a config should
 *  declare the real archive path. */
const discoverSessionFiles = (root: string): { sessions: DiscoveredSession[]; scanCapped: boolean } => {
  const sessions: DiscoveredSession[] = [];
  let examined = 0;
  let scanCapped = false;
  const walk = (dir: string): void => {
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (examined >= SCAN_ENTRY_LIMIT) {
        scanCapped = true;
        return;
      }
      examined += 1;
      if (entry.isDirectory()) {
        walk(path.join(dir, entry.name));
      } else if (entry.isFile() && entry.name.endsWith(".jsonl")) {
        const file = path.join(dir, entry.name);
        sessions.push({
          file,
          sessionKey: path.relative(root, file).split(path.sep).join("/"),
          mtimeMs: statMtimeMs(file),
        });
      }
    }
  };
  walk(path.resolve(root));
  sessions.sort(compareByRecency);
  return { sessions, scanCapped };
};

/** Resolve a session key inside root; absolute keys and traversal are rejected. */
const resolveSessionFile = (root: string, sessionKey: string): string | null => {
  if (!sessionKey || path.isAbsolute(sessionKey)) return null;
  const segments = sessionKey.split("/");
  if (segments.some((segment) => segment === "" || segment === "." || segment === "..")) return null;
  const rootResolved = path.resolve(root);
  const resolved = path.resolve(rootResolved, ...segments);
  if (resolved !== rootResolved && !resolved.startsWith(rootResolved + path.sep)) return null;
  return resolved;
};

/** Content-sensitive revision matching the historical filesystem source:
 * SHA-256 over the UTF-8-decoded file string. An mtime-only touch keeps the
 * revision while any content change invalidates follow pointers. */
const revisionForFile = async (file: string): Promise<string> => {
  const hash = crypto.createHash("sha256");
  const input = fs.createReadStream(file, { encoding: "utf8" });
  try {
    for await (const chunk of input) hash.update(chunk);
    return hash.digest("hex");
  } finally {
    input.destroy();
  }
};

const sessionIdFor = (file: string, header: { sessionId?: string } | null): string =>
  header?.sessionId || path.basename(file, ".jsonl");

/** Hash and parse the same decoded stream: reopening after hashing can pair
 * a stale revision with newer records. Hash raw decoded chunks, not parsed
 * records, so whitespace, invalid lines and malformed UTF-8 remain significant.
 * Only the unfinished line is buffered alongside the full parsed records. */
const parseRecords = async (
  file: string,
  signal?: AbortSignal,
): Promise<{ revision: string; records: MemorySourceRecord[] }> => {
  const input = fs.createReadStream(file, { encoding: "utf8", signal });
  const hash = crypto.createHash("sha256");
  const records: MemorySourceRecord[] = [];
  let pending = "";
  const parseLine = (line: string): void => {
    signal?.throwIfAborted();
    const trimmed = line.trim();
    if (!trimmed) return;
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
        records.push(parsed as MemorySourceRecord);
      }
    } catch {
      // Match normalizeSession's tolerant handling of torn/invalid lines.
    }
  };
  try {
    for await (const chunk of input) {
      signal?.throwIfAborted();
      hash.update(chunk);
      pending += chunk;
      let newline = pending.indexOf("\n");
      while (newline !== -1) {
        parseLine(pending.slice(0, newline));
        pending = pending.slice(newline + 1);
        newline = pending.indexOf("\n");
      }
    }
    signal?.throwIfAborted();
    if (pending.length > 0) parseLine(pending);
    return { revision: hash.digest("hex"), records };
  } finally {
    pending = "";
    input.destroy();
  }
};

/** Build a filesystem adapter for one configured source entry. */
export const createFileSystemMemorySource = (
  options: FileSystemMemorySourceOptions,
): PortableMemorySource =>
  defineMemorySource({
    interfaceVersion: MEMORY_SOURCE_INTERFACE_VERSION,
    id: options.id,
    async listSessions({ limit, signal }) {
      signal?.throwIfAborted();
      const boundedLimit = Math.max(0, Math.floor(limit));
      const { sessions: discovered, scanCapped } = discoverSessionFiles(options.root);
      const descriptors: MemorySourceSessionDescriptor[] = [];
      for (const found of discovered.slice(0, boundedLimit)) {
        signal?.throwIfAborted();
        let revision: string;
        try {
          revision = await revisionForFile(found.file);
        } catch {
          continue;
        }
        const header = readSessionHeader(found.file);
        descriptors.push({
          sessionKey: found.sessionKey,
          sessionId: sessionIdFor(found.file, header),
          revision,
          metadata: {
            sessionId: sessionIdFor(found.file, header),
            cwd: header?.cwd ?? "",
            updatedAt: found.mtimeMs,
          },
        });
      }
      if (!scanCapped && discovered.length <= boundedLimit) return descriptors;
      const page: MemorySourceListPage = {
        sessions: descriptors,
        coverage: {
          complete: false,
          reason: scanCapped ? REASON_SCAN_CAPPED : REASON_MAX_SESSIONS,
        },
      };
      return page;
    },
    async loadSession(sessionKey, { signal }) {
      signal?.throwIfAborted();
      const file = resolveSessionFile(options.root, sessionKey);
      if (!file) return null;
      let parsed: Awaited<ReturnType<typeof parseRecords>>;
      try {
        parsed = await parseRecords(file, signal);
      } catch {
        if (signal?.aborted) signal.throwIfAborted();
        return null;
      }
      // Keep the existing bounded (8,192-byte) header-reader semantics.
      const header = readSessionHeader(file);
      const { revision, records } = parsed;
      return {
        sessionKey,
        sessionId: sessionIdFor(file, header),
        revision,
        metadata: {
          sessionId: sessionIdFor(file, header),
          cwd: header?.cwd ?? "",
          updatedAt: statMtimeMs(file),
        },
        records,
      } satisfies MemorySourceSnapshot;
    },
  });

/** Build a source registry from validated fabric.json entries. The kind
 *  dispatch lives here so the runtime wiring stays a single call. */
export const createConfiguredMemorySourceRegistry = (
  entries: readonly FabricMemorySourceConfig[],
): MemorySourceRegistry => {
  const registry = createMemorySourceRegistry();
  for (const entry of entries) {
    registry.register(createFileSystemMemorySource({ id: entry.id, root: entry.root }));
  }
  return registry;
};
