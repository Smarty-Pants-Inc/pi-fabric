import fs from "node:fs";
import { fabricDataRoot } from "./temp-root.js";
import path from "node:path";
import { currentOwnerIdentity, ownerIdentityFields, ownerLiveness } from "../core/atomic-write.js";

export const SCRATCH_OWNER_FILE = ".fabric-scratch.json";
export const SCRATCH_MAX_AGE_MS = 24 * 60 * 60 * 1_000;
export const SCRATCH_RECENT_MS = 60 * 60 * 1_000;
export const SCRATCH_ORPHAN_GRACE_MS = 6 * 60 * 60 * 1_000;
export const SCRATCH_MAX_BYTES = 128 * 1024 * 1024;
export const SCRATCH_MAX_ITEMS = 256;
type ScratchKind = "output" | "shell" | "checkpoint";
const prefixes: Record<ScratchKind, string> = {
  output: "pi-fabric-output-", shell: "pi-fabric-shell-", checkpoint: "pi-native-reader-",
};
const contents: Record<ScratchKind, Set<string>> = {
  output: new Set(["output.txt"]), shell: new Set(["output.log", "child.pid"]),
  checkpoint: new Set(["checkpoint", "pending"]),
};
interface Owner {
  app: "pi-fabric-scratch";
  version: 1;
  kind: ScratchKind;
  pid: number;
  createdAt: number;
  closedAt?: number;
  orphanedAt?: number;
  // Optional owner identity (namespace-safe liveness); absent on old markers.
  hostname?: string;
  pidNamespace?: string;
  bootId?: string;
  startedAt?: number;
}
const timestamp = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value >= 0;
export const processAlive = (pid: number): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return true; // uncertainty is not death
  try { process.kill(pid, 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ESRCH"; }
};
// Identity-less (old) markers keep the plain signal probe. Scratch owners do
// not heartbeat, so a foreign namespace stays "unknown", which is not death.
const ownerAlive = (owner: Owner): boolean => {
  const identity = ownerIdentityFields(owner);
  return ownerLiveness(identity ? { ...identity, pid: owner.pid } : { pid: owner.pid }, {
    legacyAlive: processAlive,
  }) !== "dead";
};
// A shell child lives in its owner's namespace; its start time is unrecorded.
const childAlive = (owner: Owner, pid: number): boolean => {
  const identity = ownerIdentityFields(owner);
  if (!identity || !Number.isSafeInteger(pid) || pid <= 0) return processAlive(pid);
  const { startedAt: _startedAt, ...place } = identity;
  return ownerLiveness({ ...place, pid }, { legacyAlive: processAlive }) !== "dead";
};
export const ownedStat = (file: string): fs.Stats | undefined => {
  try {
    const stat = fs.lstatSync(file);
    if (stat.isSymbolicLink() || (stat.isFile() && stat.nlink !== 1) || (process.getuid && stat.uid !== process.getuid())) return;
    return stat;
  } catch { return; }
};
const readOwner = (directory: string): Owner | undefined => {
  try {
    const file = path.join(directory, SCRATCH_OWNER_FILE);
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > 4096) return;
    const owner = JSON.parse(fs.readFileSync(file, "utf8")) as Owner;
    if (!owner || owner.app !== "pi-fabric-scratch" || owner.version !== 1 ||
      !Object.hasOwn(prefixes, owner.kind) || !Number.isSafeInteger(owner.pid) || owner.pid <= 0 ||
      !timestamp(owner.createdAt) || (owner.closedAt !== undefined && !timestamp(owner.closedAt)) ||
      (owner.orphanedAt !== undefined && !timestamp(owner.orphanedAt))) return;
    const suffix = path.basename(directory).slice(prefixes[owner.kind].length);
    if (!path.basename(directory).startsWith(prefixes[owner.kind]) || !/^[A-Za-z0-9]{6}$/.test(suffix)) return;
    return owner;
  } catch { return; }
};

export const createScratch = (kind: ScratchKind, tempRoot = fabricDataRoot()): string => {
  const directory = fs.mkdtempSync(path.join(tempRoot, prefixes[kind]));
  try {
    fs.chmodSync(directory, 0o700);
    const { pid: _pid, ...identity } = currentOwnerIdentity();
    fs.writeFileSync(path.join(directory, SCRATCH_OWNER_FILE), JSON.stringify({
      app: "pi-fabric-scratch", version: 1, kind, pid: process.pid, createdAt: Date.now(), ...identity,
    } satisfies Owner), { mode: 0o600, flag: "wx" });
  } catch (error) {
    fs.rmSync(directory, { recursive: true, force: true });
    throw error;
  }
  scheduleScratchSweep(tempRoot);
  return directory;
};

/** Only used on directories this process allocated, never on caller-owned roots. */
export const closeScratch = (directory: string): void => {
  try {
    if (!ownedStat(directory)?.isDirectory()) return;
    const owner = readOwner(directory);
    if (!owner || owner.pid !== process.pid) return;
    fs.writeFileSync(path.join(directory, SCRATCH_OWNER_FILE), JSON.stringify({ ...owner, closedAt: Date.now() }), { mode: 0o600 });
    scheduleScratchSweep(path.dirname(directory));
  } catch { /* housekeeping must not fail the operation */ }
};

export interface ScratchSweepOptions {
  tempRoot: string;
  now?: number;
  dryRun?: boolean;
  maxAgeMs?: number;
  recentMs?: number;
  orphanGraceMs?: number;
  maxBytes?: number;
  maxItems?: number;
  /** Session release: only these allocated, closed roots of this process, regardless of age. */
  released?: ReadonlySet<string>;
}
export interface ScratchSweepResult {
  eligible: string[];
  removed: string[];
  orphaned: string[];
}

const unchanged = (before: fs.Stats, after: fs.Stats | undefined): boolean => !!after
  && before.dev === after.dev && before.ino === after.ino && before.uid === after.uid
  && before.mode === after.mode && before.nlink === after.nlink && before.size === after.size
  && before.mtimeMs === after.mtimeMs && before.ctimeMs === after.ctimeMs;

/** No legacy prefix-only deletion. Unknown files, symlinks and malformed markers veto cleanup. */
export const sweepScratch = async (options: ScratchSweepOptions): Promise<ScratchSweepResult> => {
  const result: ScratchSweepResult = { eligible: [], removed: [], orphaned: [] };
  const now = options.now ?? Date.now();
  const candidates: { directory: string; root: fs.Stats; files: Map<string, fs.Stats>; owner: Owner; childPid?: number; age: number; bytes: number; cache: boolean; expired: boolean }[] = [];
  let entries: Pick<fs.Dirent, "name" | "isDirectory">[];
  try { entries = options.released
    ? [...options.released].filter(directory => path.dirname(directory) === options.tempRoot)
      .map(directory => ({ name: path.basename(directory), isDirectory: () => true }))
    : await fs.promises.readdir(options.tempRoot, { withFileTypes: true }); }
  catch { return result; }
  let totalBytes = 0;
  let totalItems = 0;
  for (const entry of entries) {
    if (!entry.isDirectory() || !Object.values(prefixes).some((prefix) => entry.name.startsWith(prefix))) continue;
    // Yield between roots; no scan in registration or idle lifecycle hooks.
    await new Promise<void>((resolve) => setImmediate(resolve));
    const directory = path.join(options.tempRoot, entry.name);
    try {
      const root = ownedStat(directory);
      if (!root?.isDirectory()) continue;
      const owner = readOwner(directory);
      if (!owner) continue;
      if (options.released && (owner.pid !== process.pid || owner.closedAt === undefined)) continue;
      let bytes = 0;
      const files = new Map<string, fs.Stats>();
      let safe = true;
      for (const name of await fs.promises.readdir(directory)) {
        const stat = ownedStat(path.join(directory, name));
        if (!stat?.isFile() || (name !== SCRATCH_OWNER_FILE && !contents[owner.kind].has(name))) { safe = false; break; }
        files.set(name, stat);
        bytes += stat.size;
      }
      if (!safe) continue;
      const cache = owner.kind !== "checkpoint";
      if (cache) { totalBytes += bytes; totalItems++; }
      let childPid: number | undefined;
      if (owner.kind === "shell" && files.has("child.pid")) {
        childPid = Number(fs.readFileSync(path.join(directory, "child.pid"), "utf8").trim());
        if (childAlive(owner, childPid)) continue;
      }
      if (owner.closedAt === undefined) {
        if (ownerAlive(owner)) continue;
        if (owner.orphanedAt === undefined) {
          result.orphaned.push(directory);
          if (!options.dryRun) await fs.promises.writeFile(path.join(directory, SCRATCH_OWNER_FILE), JSON.stringify({ ...owner, orphanedAt: now }), { mode: 0o600 });
          continue;
        }
        if (now - owner.orphanedAt < (options.orphanGraceMs ?? SCRATCH_ORPHAN_GRACE_MS)) continue;
      }
      const age = now - (owner.closedAt ?? owner.orphanedAt ?? owner.createdAt);
      candidates.push({ directory, root, files, owner, ...(childPid === undefined ? {} : { childPid }), age, bytes, cache, expired: options.released?.has(directory) === true || owner.closedAt === undefined || age >= (options.maxAgeMs ?? SCRATCH_MAX_AGE_MS) });
    } catch { /* disappearing or inaccessible artifacts are not cleanup authority */ }
  }
  candidates.sort((a, b) => b.age - a.age);
  for (const candidate of candidates) {
    const pressure = candidate.cache && (totalBytes > (options.maxBytes ?? SCRATCH_MAX_BYTES) || totalItems > (options.maxItems ?? SCRATCH_MAX_ITEMS));
    if (!candidate.expired && !(pressure && candidate.age >= (options.recentMs ?? SCRATCH_RECENT_MS))) continue;
    result.eligible.push(candidate.directory);
    if (options.dryRun) {
      if (candidate.cache) { totalBytes -= candidate.bytes; totalItems--; }
      continue;
    }
    try {
      // Recheck identity, contents and liveness, not merely that a directory exists.
      if (!unchanged(candidate.root, ownedStat(candidate.directory))) continue;
      const names = await fs.promises.readdir(candidate.directory);
      if (names.length !== candidate.files.size || names.some(name => !candidate.files.has(name))) continue;
      if ([...candidate.files].some(([name, stat]) => !unchanged(stat, ownedStat(path.join(candidate.directory, name))))) continue;
      if (candidate.owner.closedAt === undefined && ownerAlive(candidate.owner)) continue;
      if (candidate.childPid !== undefined && childAlive(candidate.owner, candidate.childPid)) continue;
      await fs.promises.rm(candidate.directory, { recursive: true, force: true });
      result.removed.push(candidate.directory);
      if (candidate.cache) { totalBytes -= candidate.bytes; totalItems--; }
    } catch { /* best effort */ }
  }
  return result;
};

/** Tracks exact allocations, not prefixes or process-wide roots: sessions cannot release each other. */
export class ScratchScope {
  readonly #directories = new Map<string, { dev: number; ino: number }>();
  #closed = false;
  #closePromise: Promise<void> | undefined;

  create(kind: ScratchKind, tempRoot = fabricDataRoot()): string {
    if (this.#closed) throw new Error("Fabric scratch scope is closed");
    const directory = createScratch(kind, tempRoot);
    const { dev, ino } = fs.lstatSync(directory);
    this.#directories.set(directory, { dev, ino });
    return directory;
  }

  forget(directory: string): void { this.#directories.delete(directory); }

  close(): Promise<void> {
    this.#closed = true;
    return this.#closePromise ??= this.#release();
  }

  async #release(): Promise<void> {
    const roots = new Set([...this.#directories.keys()].map(directory => path.dirname(directory)));
    for (const tempRoot of roots) {
      const released = new Set([...this.#directories].filter(([directory, identity]) => {
        const stat = ownedStat(directory);
        return path.dirname(directory) === tempRoot && stat?.dev === identity.dev && stat.ino === identity.ino;
      }).map(([directory]) => directory));
      // Walk only this scope, not the shared root. Concurrent sweeps recheck file identity.
      await sweepScratch({ tempRoot, released });
    }
    this.#directories.clear();
  }
}

const pending = new Map<string, Promise<unknown>>();
const lastSweep = new Map<string, number>();
export const scheduleScratchSweep = (tempRoot = fabricDataRoot()): void => {
  if (pending.has(tempRoot) || Date.now() - (lastSweep.get(tempRoot) ?? 0) < 60_000) return;
  lastSweep.set(tempRoot, Date.now());
  if (lastSweep.size > 8) lastSweep.delete(lastSweep.keys().next().value!);
  const task = new Promise<void>((resolve) => setImmediate(resolve))
    .then(() => sweepScratch({ tempRoot })).catch(() => undefined)
    .finally(() => pending.delete(tempRoot));
  pending.set(tempRoot, task);
};
