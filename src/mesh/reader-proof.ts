// The consumer readiness gate of the backend switch (smarty-dev#7815). The REQUIRED readers come from an
// authoritative inventory, never from the registry: every distinct Fabric release of a live writer
// (host leases and process records, through the writer census) and a built-in list of installed
// non-Fabric readers with trusted install roots. Each one needs a valid proof in `<mesh>/readers/`,
// written under the migration fence; the switch re-checks them under that fence right before each
// flag commit. File format and the writer protocol: docs/mesh-backend.md.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { importMeshState, meshBackendStatus, readMeshStateMovedMarker } from "./backend-migration.js";
import { holdMeshFence } from "./fence-lock.js";
import { MeshStore } from "./store.js";

export type ReaderBackend = "file" | "sqlite";

/** The read path of `fabric-mesh-backend reader-proof`: Fabric's MeshStore. */
export const FABRIC_IMPLEMENTATION = "fabric-meshstore";

export interface ReaderProof {
  name: string;
  implementation?: string;
  version: string;
  /** Informational only: the gate uses the trusted install root of the inventory, never this. */
  installRoot?: string;
  /** The reader's release: basename(realpath(<install root>/current)), or a Fabric release SHA. */
  release: string;
  backends: ReaderBackend[];
  provedAt: string;
  provedEpoch: number;
}

/** A non-Fabric reader the gate always requires when it is installed here, with its TRUSTED release root. */
export interface InstalledReader { name: string; installRoot: string }

/** Built in, never read from the mesh directory. Extended per switch by `--require-reader name=root`. */
export const BUILTIN_READERS: readonly InstalledReader[] = [
  { name: "factory", installRoot: path.join(os.homedir(), ".local", "share", "smarty-dev", "factory") },
];

export type RequiredReader =
  | { name: string; kind: "installed"; installRoot: string; builtin: boolean }
  | { name: string; kind: "fabric"; release: string; writers: string[] }
  | { name: string; kind: "unattributed"; writers: string[] };

export interface UnreadyReader { name: string; reason: string }
export interface ReaderReadiness {
  backend: string;
  fromEpoch: number;
  toEpoch: number;
  required: RequiredReader[];
  ready: string[];
  unready: UnreadyReader[];
}

export const PROOF_MAX_AGE_MS = 24 * 60 * 60 * 1000;
export const PROOF_MAX_SKEW_MS = 5 * 60 * 1000;

const NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const KEYS = new Set(["name", "implementation", "version", "installRoot", "release", "backends", "provedAt", "provedEpoch"]);
const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;
export const isReaderName = (name: string): boolean => NAME.test(name);
export const readersDir = (root: string): string => path.join(root, "readers");
const errno = (error: unknown): string => (error as NodeJS.ErrnoException).code ?? (error as Error).message;

/** basename(realpath(`<installRoot>/current`)). Throws when unresolvable. */
export const installedRelease = (installRoot: string): string => path.basename(fs.realpathSync(path.join(installRoot, "current")));

/**
 * This Fabric release, as its writer records name it (host-leases.ts meshWriterLeaseRecord), else the
 * release directory of the running package.
 */
export const ownFabricRelease = (): string => process.env.PI_FABRIC_RELEASE_SHA ?? process.env.PI_FABRIC_BUILD_SHA ?? process.env.GITHUB_SHA
  ?? path.basename(fs.realpathSync(new URL("../..", import.meta.url)));

const packageVersion = (): string => {
  try { return String((JSON.parse(fs.readFileSync(new URL("../../package.json", import.meta.url), "utf8")) as { version?: unknown }).version ?? "unknown"); }
  catch { return "unknown"; }
};

const validRelease = (release: unknown): release is string => typeof release === "string" && release.length > 0 && release !== "unknown";

/**
 * The required readers: installed built-ins (a built-in whose release root does not exist here is not
 * installed on this host), every `--require-reader` (always), one per distinct release of the live writers,
 * and `fabric@unknown` for live writers without a release (it can never be proved; only accepted).
 */
export const requiredReaders = (writers: ReadonlyArray<{ pid: number; release: string; mode: string }>,
  builtins: readonly InstalledReader[], extra: readonly InstalledReader[]): RequiredReader[] => {
  const required: RequiredReader[] = [];
  for (const reader of builtins) {
    if (fs.existsSync(reader.installRoot)) required.push({ name: reader.name, kind: "installed", installRoot: reader.installRoot, builtin: true });
  }
  for (const reader of extra) {
    const index = required.findIndex(item => item.name === reader.name);
    const entry: RequiredReader = { name: reader.name, kind: "installed", installRoot: reader.installRoot, builtin: false };
    if (index >= 0) required[index] = entry; else required.push(entry);
  }
  const releases = new Map<string, string[]>();
  const unattributed: string[] = [];
  for (const writer of writers) {
    const label = `pid ${writer.pid} ${writer.mode}`;
    if (validRelease(writer.release)) releases.set(writer.release, [...(releases.get(writer.release) ?? []), label]);
    else unattributed.push(label);
  }
  for (const [release, list] of [...releases].sort()) required.push({ name: `fabric@${release}`, kind: "fabric", release, writers: list });
  if (unattributed.length > 0) required.push({ name: "fabric@unknown", kind: "unattributed", writers: unattributed });
  return required;
};

/** Owned by this user and not group/other-writable, or why not. Windows has no POSIX owner. */
const ownership = (stat: fs.Stats, euid: number | undefined): string | undefined => {
  if (euid === undefined) return undefined;
  if (stat.uid !== euid) return `owned by uid ${stat.uid}, not ${euid}`;
  if ((stat.mode & 0o022) !== 0) return `group/other-writable (mode ${(stat.mode & 0o777).toString(8)})`;
  return undefined;
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
  if (proof.installRoot !== undefined && typeof proof.installRoot !== "string") return "invalid proof: installRoot";
  if (typeof proof.release !== "string" || proof.release.trim() === "") return "invalid proof: release";
  if (!Array.isArray(proof.backends) || proof.backends.length === 0 || !proof.backends.every(item => item === "file" || item === "sqlite")) {
    return "invalid proof: backends must be a non-empty list of file, sqlite";
  }
  if (typeof proof.provedAt !== "string" || !ISO.test(proof.provedAt) || !Number.isFinite(Date.parse(proof.provedAt))) return "invalid proof: provedAt is not ISO 8601";
  if (typeof proof.provedEpoch !== "number" || !Number.isSafeInteger(proof.provedEpoch) || proof.provedEpoch < 0) return "invalid proof: provedEpoch";
  return proof as unknown as ReaderProof;
};

/** Every `readers/*.json` (lstat, owner, mode, strict schema): the proof, or why it is not one. */
const loadRegistry = (root: string, euid: number | undefined): Map<string, ReaderProof | string> => {
  const dir = readersDir(root);
  const proofs = new Map<string, ReaderProof | string>();
  let stat: fs.Stats;
  try { stat = fs.lstatSync(dir); } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return proofs;
    throw new Error(`reader registry ${dir} is unreadable (${errno(error)})`);
  }
  if (!stat.isDirectory()) throw new Error(`reader registry ${dir} is not a directory`);
  const bad = ownership(stat, euid);
  if (bad) throw new Error(`reader registry ${dir} is ${bad}`);
  for (const file of fs.readdirSync(dir).filter(item => item.endsWith(".json"))) {
    const name = file.slice(0, -5);
    try {
      const entry = fs.lstatSync(path.join(dir, file));
      if (!entry.isFile()) { proofs.set(name, `not a regular file (${entry.isSymbolicLink() ? "symlink" : "other"})`); continue; }
      const wrong = ownership(entry, euid);
      if (wrong) { proofs.set(name, `proof file is ${wrong}`); continue; }
      proofs.set(name, parseProof(fs.readFileSync(path.join(dir, file), "utf8"), name));
    } catch (error) { proofs.set(name, `unreadable (${errno(error)})`); }
  }
  return proofs;
};

/** Why a parsed proof does not prove `backend` for this switch, or undefined. */
const proofProblem = (proof: ReaderProof, backend: string, window: { fromEpoch: number; toEpoch: number }, now: number): string | undefined => {
  if (!proof.backends.includes(backend as ReaderBackend)) return `missing ${backend} (has ${proof.backends.join(",")})`;
  // fromEpoch: proved before the switch; toEpoch: proved while this switch was in progress (a crashed importing).
  if (proof.provedEpoch < window.fromEpoch) return `stale: proved at epoch ${proof.provedEpoch}, the switch starts at ${window.fromEpoch}`;
  if (proof.provedEpoch !== window.fromEpoch && proof.provedEpoch !== window.toEpoch) {
    return `future epoch: proved at epoch ${proof.provedEpoch}, the switch starts at ${window.fromEpoch}`;
  }
  const at = Date.parse(proof.provedAt);
  if (at - now > PROOF_MAX_SKEW_MS) return `provedAt ${proof.provedAt} is in the future`;
  if (now - at > PROOF_MAX_AGE_MS) return `over-age: proved at ${proof.provedAt}, more than 24 h ago`;
  return undefined;
};

/**
 * Whether every required reader proved `backend` for the switch fromEpoch -> toEpoch. Synchronous, so it
 * runs under the migration fence. Throws (a refusal no override lifts) when the registry directory is
 * unreadable, not a directory, foreign-owned or group/other-writable.
 */
export const readerReadiness = (root: string, backend: string, required: readonly RequiredReader[],
  window: { fromEpoch: number; toEpoch: number }, now = Date.now(), euid = process.geteuid?.()): ReaderReadiness => {
  const proofs = loadRegistry(root, euid);
  const ready: string[] = [];
  const unready: UnreadyReader[] = [];
  for (const reader of required) {
    let reason: string | undefined;
    if (reader.kind === "unattributed") {
      reason = `live Fabric writer(s) without a release SHA: ${reader.writers.join("; ")}`;
    } else if (reader.kind === "installed") {
      const proof = proofs.get(reader.name);
      let release: string | undefined;
      try { release = installedRelease(reader.installRoot); }
      catch (error) { reason = `installed release unresolvable (${path.join(reader.installRoot, "current")}: ${errno(error)})`; }
      if (reason === undefined) {
        if (proof === undefined) reason = `no proof from installed reader ${reader.name} (release ${release})`;
        else if (typeof proof === "string") reason = proof;
        else if (proof.release !== release) reason = `release mismatch: proved ${proof.release}, installed ${release} (${reader.installRoot})`;
        else reason = proofProblem(proof, backend, window, now);
      }
    } else {
      const candidates = [...proofs.values()].filter((proof): proof is ReaderProof =>
        typeof proof !== "string" && proof.implementation === FABRIC_IMPLEMENTATION && proof.release === reader.release);
      const problems = candidates.map(proof => proofProblem(proof, backend, window, now));
      if (candidates.length === 0) reason = `no proof from installed reader fabric release ${reader.release} (live: ${reader.writers.join("; ")})`;
      else if (!problems.includes(undefined)) reason = problems.join("; ");
    }
    if (reason === undefined) ready.push(reader.name);
    else unready.push({ name: reader.name, reason });
  }
  return { backend, ...window, required: [...required], ready, unready };
};

const fsyncDirectory = (directory: string): void => {
  if (process.platform === "win32") return;
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
};

/** Writes a proof atomically. The caller holds the migration fence. */
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

const FENCE_MS = 60_000;

/**
 * A real read of `backend` with Fabric's MeshStore. For sqlite: copy state.json to a scratch root, import
 * it there (the cutover's own code) and list every entry; a root already cut over is read in place.
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

/**
 * `reader-proof --backend B`: THIS Fabric release reads B for real, then writes `readers/<name>.json`
 * (default `fabric-<release>`) with its release, under the migration fence.
 */
export const proveFabricReader = async (root: string, input: { backend: ReaderBackend; name?: string; version?: string; now?: Date }):
  Promise<{ file: string; proof: ReaderProof; entries: number }> => {
  const release = ownFabricRelease();
  const name = input.name ?? `fabric-${release.slice(0, 40)}`.slice(0, 64);
  if (!isReaderName(name) || !name.startsWith("fabric")) throw new Error(`Bad Fabric reader name ${JSON.stringify(name)} (fabric*, letters, digits, . _ -)`);
  const entries = await provedRead(root, input.backend);
  return holdMeshFence(root, { lockTimeoutMs: FENCE_MS }, async () => {
    const { epoch } = await meshBackendStatus(root);
    const proof: ReaderProof = { name, implementation: FABRIC_IMPLEMENTATION, version: input.version ?? packageVersion(), release,
      backends: [input.backend], provedAt: (input.now ?? new Date()).toISOString(), provedEpoch: epoch };
    return { file: writeProof(root, proof), proof, entries };
  });
};

/**
 * `reader-proof --name N --proof-file F`: installs the proof a non-Fabric reader made with its OWN read
 * code (docs/mesh-backend.md), under the migration fence, after the strict schema check.
 */
export const installReaderProof = async (root: string, name: string, text: string): Promise<{ file: string; proof: ReaderProof }> => {
  if (!isReaderName(name) || name.startsWith("fabric")) throw new Error(`Bad reader name ${JSON.stringify(name)}: fabric* proofs come from reader-proof --backend`);
  const proof = parseProof(text, name);
  if (typeof proof === "string") throw new Error(`proof for ${name}: ${proof}`);
  if (proof.implementation === FABRIC_IMPLEMENTATION) throw new Error(`proof for ${name}: implementation ${FABRIC_IMPLEMENTATION} is reserved for reader-proof --backend`);
  return holdMeshFence(root, { lockTimeoutMs: FENCE_MS }, () => ({ file: writeProof(root, proof), proof }));
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
