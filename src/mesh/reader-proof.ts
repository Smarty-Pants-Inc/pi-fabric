// The consumer readiness gate of the backend switch (smarty-dev#7815). Each reader of a mesh root has
// `<mesh>/readers/<name>.json`: its installed release root and its proof of a real read of the target
// backend. `fabric-mesh-backend cutover` (and import) refuses while a registered reader is not ready,
// re-checking under the migration fence right before the commit. File format: docs/mesh-backend.md.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { importMeshState, meshBackendStatus, readMeshStateMovedMarker } from "./backend-migration.js";
import { MeshStore } from "./store.js";

export type ReaderBackend = "file" | "sqlite";

/** The read path of `fabric-mesh-backend reader-proof`; only such readers may be proved by it. */
export const FABRIC_IMPLEMENTATION = "fabric-meshstore";

export interface ReaderProof {
  name: string;
  /** Free-form; `fabric-meshstore` marks a reader whose read path is Fabric's MeshStore. */
  implementation?: string;
  version: string;
  /** Absolute release root of the reader; `<installRoot>/current` points to the installed release. */
  installRoot: string;
  /** basename(realpath(`<installRoot>/current`)) when the proof ran. */
  release: string;
  /** Backends this reader has read for real. */
  backends: ReaderBackend[];
  /** ISO 8601 time of the proof. */
  provedAt: string;
  /** The mesh backend epoch (`fabric-mesh-backend status`, 0 for a legacy file root) when it proved. */
  provedEpoch: number;
}

export interface UnreadyReader { name: string; reason: string }
export interface ReaderReadiness {
  backend: string;
  epoch: number;
  /** False when `<mesh>/readers/` does not exist. */
  registry: boolean;
  ready: string[];
  unready: UnreadyReader[];
}

/** A proof is valid this long after `provedAt`. */
export const PROOF_MAX_AGE_MS = 24 * 60 * 60 * 1000;
/** `provedAt` may be this far ahead of the gate's clock. */
export const PROOF_MAX_SKEW_MS = 5 * 60 * 1000;

const NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const KEYS = new Set(["name", "implementation", "version", "installRoot", "release", "backends", "provedAt", "provedEpoch"]);
const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;
export const isReaderName = (name: string): boolean => NAME.test(name);
export const readersDir = (root: string): string => path.join(root, "readers");

const packageVersion = (): string => {
  try {
    const text = fs.readFileSync(new URL("../../package.json", import.meta.url), "utf8");
    return String((JSON.parse(text) as { version?: unknown }).version ?? "unknown");
  } catch { return "unknown"; }
};

/** basename(realpath(`<installRoot>/current`)): the installed release of a reader. Throws when unresolvable. */
export const installedRelease = (installRoot: string): string => path.basename(fs.realpathSync(path.join(installRoot, "current")));

const fsyncDirectory = (directory: string): void => {
  if (process.platform === "win32") return;
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
};

const writeProof = (root: string, proof: ReaderProof): string => {
  const dir = readersDir(root);
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const file = path.join(dir, `${proof.name}.json`);
  const temporary = `${file}.${process.pid}.${randomUUID()}.tmp`;
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try { fs.writeSync(descriptor, `${JSON.stringify(proof, null, 2)}\n`); fs.fsyncSync(descriptor); }
  finally { fs.closeSync(descriptor); }
  fs.renameSync(temporary, file);
  fsyncDirectory(dir);
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

/** The registration fields of an existing entry, read leniently (a registration has no proof yet). */
const registration = (root: string, name: string): { installRoot?: string; implementation?: string } => {
  let value: unknown;
  try { value = JSON.parse(fs.readFileSync(path.join(readersDir(root), `${name}.json`), "utf8")); }
  catch { return {}; }
  if (typeof value !== "object" || value === null) return {};
  const { installRoot, implementation } = value as Record<string, unknown>;
  return { ...(typeof installRoot === "string" ? { installRoot } : {}), ...(typeof implementation === "string" ? { implementation } : {}) };
};

/**
 * `reader-proof`: read `backend` for real with Fabric's MeshStore, then write `<mesh>/readers/<name>.json`.
 * Only for a reader whose read path IS MeshStore (name `fabric` or `implementation: "fabric-meshstore"`):
 * any other reader writes its own proof with its own read code (docs/mesh-backend.md).
 */
export const proveReader = async (root: string, input: { name: string; backend: ReaderBackend; version?: string; installRoot?: string; now?: Date }):
  Promise<{ file: string; proof: ReaderProof; entries: number }> => {
  if (!isReaderName(input.name)) throw new Error(`Bad reader name ${JSON.stringify(input.name)} (letters, digits, . _ -; at most 64)`);
  const registered = registration(root, input.name);
  if (input.name !== "fabric" && registered.implementation !== FABRIC_IMPLEMENTATION) {
    throw new Error(`reader ${input.name} does not read through Fabric's MeshStore (implementation ${registered.implementation ?? "unset"}): `
      + "it must write its own proof with its own read code (docs/mesh-backend.md)");
  }
  const installRoot = input.installRoot ?? registered.installRoot;
  if (!installRoot || !path.isAbsolute(installRoot)) throw new Error(`reader ${input.name} needs an absolute installRoot (--install-root DIR)`);
  const release = installedRelease(installRoot);
  const entries = await provedRead(root, input.backend);
  const { epoch } = await meshBackendStatus(root);
  const proof: ReaderProof = { name: input.name, implementation: FABRIC_IMPLEMENTATION, version: input.version ?? packageVersion(),
    installRoot, release, backends: [input.backend], provedAt: (input.now ?? new Date()).toISOString(), provedEpoch: epoch };
  return { file: writeProof(root, proof), proof, entries };
};

/** Strict schema check of one proof; the reason it is invalid, or the proof. */
export const parseProof = (text: string, name: string): ReaderProof | string => {
  let value: unknown;
  try { value = JSON.parse(text); } catch (error) { return `invalid JSON (${(error as Error).message})`; }
  if (typeof value !== "object" || value === null || Array.isArray(value)) return "invalid proof: not an object";
  const proof = value as Record<string, unknown>;
  const unknown = Object.keys(proof).filter(key => !KEYS.has(key));
  if (unknown.length > 0) return `invalid proof: unknown field ${unknown.join(", ")}`;
  if (proof.name !== name) return `invalid proof: name ${JSON.stringify(proof.name)} is not the file name ${name}`;
  if (proof.implementation !== undefined && (typeof proof.implementation !== "string" || proof.implementation === "")) return "invalid proof: implementation";
  if (typeof proof.version !== "string" || proof.version.trim() === "") return "invalid proof: version";
  if (typeof proof.installRoot !== "string" || !path.isAbsolute(proof.installRoot)) return "invalid proof: installRoot must be an absolute path";
  if (typeof proof.release !== "string" || proof.release.trim() === "") return "invalid proof: release";
  if (!Array.isArray(proof.backends) || proof.backends.length === 0 || !proof.backends.every(item => item === "file" || item === "sqlite")) {
    return "invalid proof: backends must be a non-empty list of file, sqlite";
  }
  if (typeof proof.provedAt !== "string" || !ISO.test(proof.provedAt) || !Number.isFinite(Date.parse(proof.provedAt))) return "invalid proof: provedAt is not ISO 8601";
  if (typeof proof.provedEpoch !== "number" || !Number.isSafeInteger(proof.provedEpoch) || proof.provedEpoch < 0) return "invalid proof: provedEpoch";
  return proof as unknown as ReaderProof;
};

/** Why the reader `name` is not ready for `backend` at `epoch`, or undefined when it is. */
const unreadyReason = (root: string, name: string, backend: string, epoch: number, now: number): string | undefined => {
  const file = path.join(readersDir(root), name);
  let text: string;
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isFile()) return `not a regular file (${stat.isSymbolicLink() ? "symlink" : "other"})`;
    text = fs.readFileSync(file, "utf8");
  } catch (error) { return `unreadable (${(error as NodeJS.ErrnoException).code ?? (error as Error).message})`; }
  const proof = parseProof(text, name.slice(0, -5));
  if (typeof proof === "string") return proof;
  if (!proof.backends.includes(backend as ReaderBackend)) return `missing ${backend} (has ${proof.backends.join(",")})`;
  if (proof.provedEpoch < epoch) return `stale: proved at epoch ${proof.provedEpoch}, mesh is at ${epoch}`;
  if (proof.provedEpoch > epoch) return `future epoch: proved at epoch ${proof.provedEpoch}, mesh is at ${epoch}`;
  const at = Date.parse(proof.provedAt);
  if (at - now > PROOF_MAX_SKEW_MS) return `provedAt ${proof.provedAt} is in the future`;
  if (now - at > PROOF_MAX_AGE_MS) return `over-age: proved at ${proof.provedAt}, more than 24 h ago`;
  let release: string;
  try { release = installedRelease(proof.installRoot); }
  catch (error) { return `installed release unresolvable (${path.join(proof.installRoot, "current")}: ${(error as NodeJS.ErrnoException).code ?? (error as Error).message})`; }
  if (release !== proof.release) return `release mismatch: proved ${proof.release}, installed ${release}`;
  return undefined;
};

/**
 * Every listed reader (`readers/*.json`) and whether it proved `backend` at `epoch`. Synchronous so that
 * it runs under the migration fence. A listed entry that cannot be read is unready, never skipped;
 * only an absent `readers/` directory is "no registry". Anything else wrong with the directory throws.
 */
export const readerReadiness = (root: string, backend: string, epoch: number, now = Date.now()): ReaderReadiness => {
  const dir = readersDir(root);
  let names: string[];
  try {
    if (!fs.lstatSync(dir).isDirectory()) throw new Error(`${dir} is not a directory`);
    names = fs.readdirSync(dir).filter(file => file.endsWith(".json")).sort();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { backend, epoch, registry: false, ready: [], unready: [] };
    throw error;
  }
  const ready: string[] = [];
  const unready: UnreadyReader[] = [];
  for (const file of names) {
    const name = file.slice(0, -5);
    const reason = unreadyReason(root, file, backend, epoch, now);
    if (reason === undefined) ready.push(name);
    else unready.push({ name, reason });
  }
  return { backend, epoch, registry: true, ready, unready };
};

/** Readiness under the overrides: what still blocks the switch (empty means it may go ahead). */
export const blockingReaders = (readiness: ReaderReadiness, accept: { unready: string[]; emptyRegistry: boolean }): string[] => {
  const blocking = readiness.unready.filter(reader => !accept.unready.includes(reader.name)).map(reader => `${reader.name} (${reader.reason})`);
  if (readiness.ready.length === 0 && readiness.unready.length === 0 && !accept.emptyRegistry) {
    blocking.push(`no reader registered (${readiness.registry ? "readers/ is empty" : "no readers/ directory"}); pass --accept-empty-registry`);
  }
  return blocking;
};

/** Appends one line to `<mesh>/backend-switches.jsonl` and fsyncs the file and the directory. Throws on failure. */
export const recordSwitch = (root: string, record: Record<string, unknown>): string => {
  const file = path.join(root, "backend-switches.jsonl");
  const descriptor = fs.openSync(file, "a", 0o600);
  try { fs.writeSync(descriptor, `${JSON.stringify({ at: new Date().toISOString(), ...record })}\n`); fs.fsyncSync(descriptor); }
  finally { fs.closeSync(descriptor); }
  fsyncDirectory(root);
  return file;
};
