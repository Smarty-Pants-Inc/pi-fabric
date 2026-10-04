// Configured filesystem memory source: a PortableMemorySource over a local
// directory of Pi session JSONL files. Native agent trees
// (sessions/<encoded-cwd>/*.jsonl) and flat archive directories enumerate
// through the same recursive walk; only the root-relative path becomes the
// opaque session key.

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { FabricMemorySourceConfig } from "../config.js";
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
const REASON_UNAVAILABLE = "fs_source_unavailable";
const REASON_INCOMPLETE = "fs_source_incomplete";

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

const isInside = (root: string, target: string): boolean => {
  const relative = path.relative(root, target);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
};

const statMtimeMs = (file: string, onFailure?: () => void): number => {
  try {
    return fs.statSync(file).mtimeMs;
  } catch {
    onFailure?.();
    return 0;
  }
};

interface ConfinedFile {
  content: string;
  mtimeMs: number;
}

/** Read only a regular file whose canonical target remains inside root. */
const readConfinedFile = (root: string, file: string): ConfinedFile | null => {
  const rootLexical = path.resolve(root);
  let rootReal: string;
  let canonical: string;
  try {
    rootReal = fs.realpathSync(rootLexical);
    canonical = fs.realpathSync(file);
  } catch {
    return null;
  }
  if (!isInside(rootReal, canonical)) return null;
  let current = rootLexical;
  const relative = path.relative(rootLexical, file);
  for (const segment of relative.split(path.sep)) {
    if (!segment || segment === ".") continue;
    current = path.join(current, segment);
    try {
      if (fs.lstatSync(current).isSymbolicLink()) return null;
    } catch {
      return null;
    }
  }
  let fd: number | undefined;
  try {
    const expected = fs.statSync(canonical);
    const noFollow = fs.constants.O_NOFOLLOW ?? 0;
    fd = fs.openSync(file, fs.constants.O_RDONLY | noFollow);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.dev !== expected.dev || stat.ino !== expected.ino) return null;
    // Linux exposes the opened handle's actual target. A parent may have been
    // replaced after lexical/component checks; never read that escaped handle.
    if (process.platform === "linux" && !isInside(rootReal, fs.realpathSync(`/proc/self/fd/${fd}`))) return null;
    if (fs.realpathSync(file) !== canonical) return null;
    return { content: fs.readFileSync(fd, "utf8"), mtimeMs: stat.mtimeMs };
  } catch {
    return null;
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); } catch { /* already closed */ }
    }
  }
};

const headerFromContent = (content: string): { sessionId?: string; cwd?: string } | null => {
  try {
    const firstLine = content.split("\n", 1)[0]!.trim();
    if (!firstLine) return null;
    const raw = JSON.parse(firstLine) as Record<string, unknown>;
    if (raw.type !== "session") return null;
    return {
      ...(typeof raw.id === "string" ? { sessionId: raw.id } : {}),
      ...(typeof raw.cwd === "string" ? { cwd: raw.cwd } : {}),
    };
  } catch {
    return null;
  }
};

const compareByRecency = (left: DiscoveredSession, right: DiscoveredSession): number => {
  if (right.mtimeMs !== left.mtimeMs) return right.mtimeMs - left.mtimeMs;
  return left.file < right.file ? -1 : left.file > right.file ? 1 : 0;
};

/** Collect every *.jsonl file under root, newest first (mtime, then path).
 *  Symlinks are skipped: cycles cannot hang a listing, and a config should
 *  declare the real archive path. */
const discoverSessionFiles = (root: string): { sessions: DiscoveredSession[]; scanCapped: boolean; unavailable: boolean; incomplete: boolean } => {
  const sessions: DiscoveredSession[] = [];
  let examined = 0;
  let scanCapped = false;
  let unavailable = false;
  let incomplete = false;
  const rootResolved = path.resolve(root);
  const walk = (dir: string): void => {
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      if (dir === rootResolved) unavailable = true;
      else incomplete = true;
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
          mtimeMs: statMtimeMs(file, () => { incomplete = true; }),
        });
      }
    }
  };
  walk(rootResolved);
  sessions.sort(compareByRecency);
  return { sessions, scanCapped, unavailable, incomplete };
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

/** Content-sensitive revision matching the filesystem index fingerprintSource
 *  convention: SHA-256 over the raw file bytes, so an mtime-only touch keeps
 *  the revision while any content change invalidates follow pointers. */
const revisionForContent = (content: string): string =>
  crypto.createHash("sha256").update(content).digest("hex");

const sessionIdFor = (file: string, header: { sessionId?: string } | null): string =>
  header?.sessionId || path.basename(file, ".jsonl");

/** Parse JSONL with the same tolerance as normalizeSession: blank and
 *  unparsable lines are skipped; only plain object records are kept. */
const parseRecords = (content: string): MemorySourceRecord[] => {
  const records: MemorySourceRecord[] = [];
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
        records.push(parsed as MemorySourceRecord);
      }
    } catch {
      continue;
    }
  }
  return records;
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
      const { sessions: discovered, scanCapped, unavailable, incomplete } = discoverSessionFiles(options.root);
      let readFailed = false;
      const descriptors: MemorySourceSessionDescriptor[] = [];
      for (const found of discovered.slice(0, boundedLimit)) {
        signal?.throwIfAborted();
        const loaded = readConfinedFile(options.root, found.file);
        if (!loaded) {
          readFailed = true;
          continue;
        }
        const content = loaded.content;
        const header = headerFromContent(content);
        descriptors.push({
          sessionKey: found.sessionKey,
          sessionId: sessionIdFor(found.file, header),
          revision: revisionForContent(content),
          metadata: {
            sessionId: sessionIdFor(found.file, header),
            cwd: header?.cwd ?? "",
            updatedAt: loaded.mtimeMs,
          },
        });
      }
      if (!unavailable && !incomplete && !readFailed && !scanCapped && discovered.length <= boundedLimit) return descriptors;
      const page: MemorySourceListPage = {
        sessions: descriptors,
        coverage: {
          complete: false,
          reason: unavailable ? REASON_UNAVAILABLE
            : incomplete || readFailed ? REASON_INCOMPLETE
            : scanCapped ? REASON_SCAN_CAPPED : REASON_MAX_SESSIONS,
        },
      };
      return page;
    },
    async loadSession(sessionKey, { signal }) {
      signal?.throwIfAborted();
      const file = resolveSessionFile(options.root, sessionKey);
      if (!file) return null;
      const loaded = readConfinedFile(options.root, file);
      if (!loaded) return null;
      const content = loaded.content;
      const header = headerFromContent(content);
      return {
        sessionKey,
        sessionId: sessionIdFor(file, header),
        revision: revisionForContent(content),
        metadata: {
          sessionId: sessionIdFor(file, header),
          cwd: header?.cwd ?? "",
          updatedAt: loaded.mtimeMs,
        },
        records: parseRecords(content),
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
