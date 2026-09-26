import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../config.js";
import { stableJsonHash } from "../core/stable-hash.js";
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

/** Stat watching handles missing files and atomic replacement on all supported hosts. */
export function watchComponentConfiguration(paths: readonly string[], changed: () => void): () => void {
  // The baseline is taken now, when watching starts (smarty-dev#883). fs.watchFile took it on its
  // first asynchronous poll, so a write between the caller's reconcile and that poll was never
  // reported. Few files, polled every 250 ms like before.
  const signature = (file: string): string => {
    try {
      const stat = fs.statSync(file);
      return `${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
    } catch {
      return "missing";
    }
  };
  const seen = new Map(paths.map((file) => [file, signature(file)] as const));
  let timer: ReturnType<typeof setTimeout> | undefined;
  let closed = false;
  const poll = setInterval(() => {
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
    timer.unref();
  }, 250);
  poll.unref();
  return () => {
    closed = true;
    clearTimeout(timer);
    clearInterval(poll);
  };
}
