import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../config.js";
import { stableJsonHash } from "../core/stable-hash.js";
import { isWatchLimitError } from "../core/watch-fallback.js";
import type { FabricComponentEntry } from "./types.js";
import { componentEntries } from "./validation.js";

export type ComponentConfigScope = "session" | "global" | "project";
export interface ComponentConfigSource {
  scope: "global" | "project";
  path: string;
  trusted: boolean;
  present: boolean;
  selected: boolean;
}
export interface ComponentConfigSnapshot {
  revision: string;
  entries: FabricComponentEntry[];
  layers: { global: FabricComponentEntry[]; project?: FabricComponentEntry[] };
  sources: ComponentConfigSource[];
  warnings: string[];
}
export interface ComponentConfigurationStore {
  read(): ComponentConfigSnapshot;
  write(scope: "global" | "project", entries: FabricComponentEntry[], expectedRevision: string): ComponentConfigSnapshot;
}
interface Document {
  source: string | null;
  value: Record<string, unknown>;
}
interface Documents { global: Document; project: Document | undefined; trusted: boolean }

/** Unlike startup recovery, a live read never repairs, renames, or drops damaged data. */
function readDocument(file: string): Document {
  let source: string;
  try {
    if (fs.statSync(file).size > 1024 * 1024) throw new Error(`Fabric configuration exceeds 1 MiB: ${file}`);
    source = fs.readFileSync(file, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { source: null, value: {} };
    throw error;
  }
  let value: unknown;
  try { value = JSON.parse(source); } catch { throw new Error(`Invalid JSON in ${file}; live components were not changed`); }
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`Expected an object in ${file}`);
  return { source, value: value as Record<string, unknown> };
}

export class FabricComponentConfiguration implements ComponentConfigurationStore {
  readonly paths: readonly string[];
  constructor(readonly options: { cwd: string; agentDir: string; projectTrusted: () => boolean }) {
    this.paths = [path.join(options.agentDir, "fabric.json"), path.join(options.cwd, ".pi", "fabric.json")];
  }

  #read() {
    const trusted = this.options.projectTrusted();
    return { global: readDocument(this.paths[0]!), project: trusted ? readDocument(this.paths[1]!) : undefined, trusted };
  }

  #snapshot(documents: Documents): ComponentConfigSnapshot {
    const { global, project, trusted } = documents;
    const globalEntries = Object.hasOwn(global.value, "components") ? componentEntries(global.value.components) : [];
    const projectEntries = project && Object.hasOwn(project.value, "components") ? componentEntries(project.value.components) : undefined;
    const projectPresent = project ? project.source !== null : fs.existsSync(this.paths[1]!);
    return {
      revision: stableJsonHash({ global: global.source, project: project?.source, trusted }),
      entries: structuredClone(projectEntries ?? globalEntries),
      layers: { global: globalEntries, ...(projectEntries ? { project: projectEntries } : {}) },
      sources: [
        { scope: "global", path: this.paths[0]!, trusted: true, present: global.source !== null, selected: projectEntries === undefined },
        { scope: "project", path: this.paths[1]!, trusted, present: projectPresent, selected: projectEntries !== undefined },
      ],
      warnings: !trusted && projectPresent ? [`Project configuration ${this.paths[1]} is ignored because the project is not trusted; use session scope or explicitly select global scope.`] : [],
    };
  }

  read(): ComponentConfigSnapshot { return this.#snapshot(this.#read()); }

  write(scope: "global" | "project", entries: FabricComponentEntry[], expectedRevision: string): ComponentConfigSnapshot {
    const documents = this.#read();
    const snapshot = this.#snapshot(documents);
    if (snapshot.revision !== expectedRevision) throw new Error("Component configuration changed; plan again before applying");
    if (scope === "project" && !documents.trusted) throw new Error("Cannot write project components in an untrusted project");
    const document = scope === "project" ? documents.project! : documents.global;
    const value = { ...document.value, components: componentEntries(entries) };
    const source = `${JSON.stringify(value, null, 2)}\n`;
    if (Buffer.byteLength(source) > 1024 * 1024) throw new Error("Fabric configuration exceeds 1 MiB");
    writeJsonAtomic(this.paths[scope === "global" ? 0 : 1]!, value, document.source);
    documents[scope] = { source, value };
    return this.#snapshot(documents);
  }
}

const WATCH_REARM_BASE_MS = 1_000;
const WATCH_REARM_MAX_MS = 60_000;
const WATCH_REARM_ATTEMPTS = 6;

/** Truthful recovery line: one rescan plus bounded one-shot re-arms; nothing polls. */
function watchRecoveryMessage(target: string, error: unknown, delayMs: number): string {
  const cause = isWatchLimitError(error)
    ? "ENOSPC: inotify watch limit, not disk space (raise fs.inotify.max_user_watches)"
    : error instanceof Error ? error.message : String(error);
  return `[pi-fabric] component configuration: fs.watch(${target}) failed (${cause}); rescanned once, re-arming in ` +
    `${delayMs} ms with bounded retries. There is no polling: changes made while unarmed are seen only at a re-arm.`;
}

/**
 * Directory watches, not file watches: they survive atomic replacement (a rename onto the file),
 * see a missing file or missing `.pi` directory being created (the nearest existing ancestor is
 * watched for the next path component), and follow a symlinked file to its target's directory.
 * Notifications are hints; a stat signature decides whether anything changed. Idle costs no
 * timer. A watcher error is one rescan plus a bounded one-shot re-arm, never a standing poll.
 */
export function watchComponentConfiguration(paths: readonly string[], changed: () => void): () => void {
  // The baseline is taken now, when watching starts (smarty-dev#883), and compared again once
  // the watchers are armed, so a write between the caller's reconcile and arming is reported.
  const signature = (file: string): string => {
    let link = "";
    try { link = fs.readlinkSync(file); } catch {}
    try {
      const stat = fs.statSync(file);
      return `${link}|${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
    } catch {
      return `${link}|missing`;
    }
  };
  const identity = (directory: string): string | undefined => {
    try {
      const stat = fs.statSync(directory);
      return stat.isDirectory() ? `${stat.dev}:${stat.ino}` : undefined;
    } catch {
      return undefined;
    }
  };
  const seen = new Map(paths.map((file) => [file, signature(file)] as const));
  const watchers = new Map<string, { identity: string; names: Set<string>; watcher: fs.FSWatcher }>();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let rearmTimer: ReturnType<typeof setTimeout> | undefined;
  let attempts = 0;
  let closed = false;
  let warned = false;

  // Watched directory -> entry names whose changes matter there.
  const targets = (): Map<string, Set<string>> => {
    const wanted = new Map<string, Set<string>>();
    const add = (directory: string, name: string): void => {
      let names = wanted.get(directory);
      if (!names) wanted.set(directory, names = new Set());
      names.add(name);
    };
    const cover = (file: string, depth: number): void => {
      let child = file;
      let directory = path.dirname(file);
      while (identity(directory) === undefined && path.dirname(directory) !== directory) {
        child = directory;
        directory = path.dirname(directory);
      }
      add(directory, path.basename(child));
      // The parent's parent sees the containing directory itself being replaced or removed.
      const up = path.dirname(directory);
      if (child === file && up !== directory) add(up, path.basename(directory));
      if (depth >= 4) return;
      let link: string;
      try { link = fs.readlinkSync(file); } catch { return; }
      cover(path.resolve(path.dirname(file), link), depth + 1);
    };
    for (const file of paths) cover(file, 0);
    return wanted;
  };

  const closeWatchers = (): void => {
    for (const { watcher } of watchers.values()) watcher.close();
    watchers.clear();
  };

  const check = (): void => {
    if (closed) return;
    let differs = false;
    for (const file of paths) {
      const current = signature(file);
      if (current !== seen.get(file)) {
        seen.set(file, current);
        differs = true;
      }
    }
    if (!differs) return;
    clearTimeout(timer);
    timer = setTimeout(() => { timer = undefined; if (!closed) changed(); }, 50);
    timer.unref?.();
  };

  const fail = (error: unknown): void => {
    closeWatchers();
    if (closed) return;
    const delay = Math.min(WATCH_REARM_MAX_MS, WATCH_REARM_BASE_MS * 2 ** attempts);
    if (!warned) {
      warned = true;
      try { console.warn(watchRecoveryMessage(paths.join(", "), error, delay)); } catch {}
    }
    check(); // one rescan
    if (rearmTimer || attempts >= WATCH_REARM_ATTEMPTS) return;
    attempts++;
    rearmTimer = setTimeout(() => {
      rearmTimer = undefined;
      if (closed) return;
      try { arm(); } catch (rearmError) { fail(rearmError); return; }
      check(); // changes while unarmed
    }, delay);
    rearmTimer.unref?.();
  };

  const onEvent = (directory: string, name: string | Buffer | null): void => {
    if (closed) return;
    const entry = watchers.get(directory);
    if (name !== null && entry && !entry.names.has(String(name))) return;
    attempts = 0; // a delivered notification proves the watch works
    try { arm(); } catch (error) { fail(error); return; }
    check();
  };

  // Bind watchers to the current targets; rebind a directory that was replaced.
  const sync = (): boolean => {
    let raced = false;
    const wanted = targets();
    for (const [directory, entry] of watchers) {
      if (!wanted.has(directory) || identity(directory) !== entry.identity) {
        entry.watcher.close();
        watchers.delete(directory);
      }
    }
    for (const [directory, names] of wanted) {
      const existing = watchers.get(directory);
      if (existing) { existing.names = names; continue; }
      const current = identity(directory);
      if (current === undefined) { raced = true; continue; }
      let watcher: fs.FSWatcher;
      try {
        watcher = fs.watch(directory, (_event, name) => onEvent(directory, name));
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") { raced = true; continue; }
        throw error;
      }
      watcher.on("error", fail);
      watchers.set(directory, { identity: current, names, watcher });
    }
    return !raced;
  };
  const arm = (): void => {
    for (let attempt = 0; attempt < 3; attempt++) if (sync()) return;
    throw new Error("configuration directories changed while being watched");
  };

  try { arm(); } catch (error) { fail(error); }
  check();
  return () => {
    closed = true;
    clearTimeout(timer);
    clearTimeout(rearmTimer);
    closeWatchers();
  };
}
