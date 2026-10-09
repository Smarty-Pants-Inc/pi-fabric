// The consumer readiness gate of the backend switch (smarty-dev#7815). Each reader of a mesh root writes
// `<mesh>/readers/<name>.json` once it has really read the target backend; `fabric-mesh-backend cutover`
// (and import) refuses while a registered reader lacks a fresh proof. File format: docs/mesh-backend.md.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { importMeshState, meshBackendStatus, readMeshStateMovedMarker } from "./backend-migration.js";
import { MeshStore } from "./store.js";

export type ReaderBackend = "file" | "sqlite";

export interface ReaderProof {
  name: string;
  version: string;
  /** Backends this reader has read for real. */
  backends: string[];
  /** ISO 8601 time of the proof. */
  provedAt: string;
  /** The mesh backend epoch (`fabric-mesh-backend status`, 0 for a legacy file root) when it proved. */
  provedEpoch: number;
}

export interface UnreadyReader { name: string; reason: string }
export interface ReaderReadiness {
  backend: string;
  /** The root's backend flag now (`none` for a legacy file root). */
  current: string;
  epoch: number;
  ready: string[];
  unready: UnreadyReader[];
}

const NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
export const isReaderName = (name: string): boolean => NAME.test(name);
export const readersDir = (root: string): string => path.join(root, "readers");

const packageVersion = (): string => {
  try {
    const text = fs.readFileSync(new URL("../../package.json", import.meta.url), "utf8");
    return String((JSON.parse(text) as { version?: unknown }).version ?? "unknown");
  } catch { return "unknown"; }
};

const writeProof = (root: string, proof: ReaderProof): string => {
  const dir = readersDir(root);
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const file = path.join(dir, `${proof.name}.json`);
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, `${JSON.stringify(proof, null, 2)}\n`, { mode: 0o600 });
  fs.renameSync(temporary, file);
  return file;
};

/**
 * A real read of `backend` through the installed store read path. For sqlite: copy state.json to a
 * scratch root, import it there (the cutover's own code) and list every entry through MeshStore; a root
 * already cut over is read in place. Throws when the read fails or loses entries.
 */
const provedRead = async (root: string, backend: ReaderBackend): Promise<number> => {
  const read = (at: string): number => {
    const store = new MeshStore(at, 64 * 1024, 1_000, { stateBackend: backend });
    try { return store.listAll("").length; } finally { store.closeState(); }
  };
  if (backend === "sqlite" && readMeshStateMovedMarker(root)) return read(root);
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-reader-proof-"));
  try {
    const source = path.join(root, "state.json");
    if (fs.existsSync(source)) fs.copyFileSync(source, path.join(scratch, "state.json"));
    if (backend === "file") return read(scratch);
    const imported = await importMeshState(scratch);
    const count = read(scratch);
    if (count !== imported.entries) throw new Error(`read ${count} entries from the migrated copy, imported ${imported.entries}`);
    return count;
  } finally { fs.rmSync(scratch, { recursive: true, force: true }); }
};

/** `reader-proof`: read `backend` for real, then add it to `<mesh>/readers/<name>.json`. */
export const proveReader = async (root: string, input: { name: string; backend: ReaderBackend; version?: string; now?: Date }):
  Promise<{ file: string; proof: ReaderProof; entries: number }> => {
  if (!isReaderName(input.name)) throw new Error(`Bad reader name ${JSON.stringify(input.name)} (letters, digits, . _ -; at most 64)`);
  const entries = await provedRead(root, input.backend);
  const { epoch } = await meshBackendStatus(root);
  const previous = readProof(root, input.name);
  // ponytail: a proof at an older epoch is stale, so its backends are not carried forward.
  const kept = previous && "proof" in previous && previous.proof.provedEpoch === epoch ? previous.proof.backends : [];
  const proof: ReaderProof = { name: input.name, version: input.version ?? packageVersion(),
    backends: [...new Set([...kept, input.backend])].sort(), provedAt: (input.now ?? new Date()).toISOString(), provedEpoch: epoch };
  return { file: writeProof(root, proof), proof, entries };
};

const readProof = (root: string, name: string): { proof: ReaderProof } | { error: string } | undefined => {
  const file = path.join(readersDir(root), `${name}.json`);
  let value: Partial<ReaderProof>;
  try { value = JSON.parse(fs.readFileSync(file, "utf8")) as Partial<ReaderProof>; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    return { error: `unreadable proof (${(error as Error).message})` };
  }
  if (typeof value !== "object" || value === null || value.name !== name || !Array.isArray(value.backends)
    || typeof value.provedEpoch !== "number" || typeof value.provedAt !== "string") {
    return { error: "invalid proof (needs name equal to the file name, backends[], provedAt, provedEpoch)" };
  }
  return { proof: value as ReaderProof };
};

/** Every registered reader (`readers/*.json`) and whether it proved `backend` at the current epoch. */
export const readerReadiness = async (root: string, backend: string): Promise<ReaderReadiness> => {
  const { epoch, backend: current } = await meshBackendStatus(root);
  const names = fs.existsSync(readersDir(root))
    ? fs.readdirSync(readersDir(root)).filter(file => file.endsWith(".json")).map(file => file.slice(0, -5)).sort() : [];
  const ready: string[] = [];
  const unready: UnreadyReader[] = [];
  for (const name of names) {
    const read = readProof(root, name);
    if (!read) continue;
    if ("error" in read) unready.push({ name, reason: read.error });
    else if (!read.proof.backends.includes(backend)) unready.push({ name, reason: `missing ${backend} (has ${read.proof.backends.join(",") || "none"})` });
    else if (read.proof.provedEpoch < epoch) unready.push({ name, reason: `stale: proved at epoch ${read.proof.provedEpoch}, mesh is at ${epoch}` });
    else ready.push(name);
  }
  return { backend, current, epoch, ready, unready };
};

/** One line per switch in `<mesh>/backend-switches.jsonl`: who was ready and which unready readers were accepted. */
export const recordSwitch = (root: string, record: Record<string, unknown>): string => {
  const file = path.join(root, "backend-switches.jsonl");
  fs.appendFileSync(file, `${JSON.stringify({ at: new Date().toISOString(), ...record })}\n`, { mode: 0o600 });
  return file;
};
