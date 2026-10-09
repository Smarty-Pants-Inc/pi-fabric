// The consumer readiness gate of the backend switch (smarty-dev#7815). The REQUIRED readers come from an
// authoritative inventory, never from the registry: every distinct Fabric release of a live writer
// (host leases and process records, through the writer census) and a built-in list of installed
// non-Fabric readers with trusted install roots. Each one needs a valid proof in `<mesh>/readers/`,
// written under the migration fence; the switch re-checks them under that fence right before each
// flag commit. File format and the writer protocol: docs/mesh-backend.md.
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
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
  | { name: string; kind: "unattributed"; writers: string[] }
  | { name: string; kind: "holder"; pid: number; reason: string };

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
  unattributedEvidence: ReadonlyArray<{ pid: number; release: string; mode: string }>,
  builtins: readonly InstalledReader[], extra: readonly InstalledReader[], holders: readonly StateDbHolder[] = []): RequiredReader[] => {
  const required: RequiredReader[] = [];
  for (const reader of builtins) {
    if (fs.existsSync(reader.installRoot)) required.push({ name: reader.name, kind: "installed", installRoot: reader.installRoot, builtin: true });
  }
  for (const reader of extra) {
    // Flags only add readers: a built-in (or earlier) name keeps its trusted root.
    if (!required.some(item => item.name === reader.name)) required.push({ name: reader.name, kind: "installed", installRoot: reader.installRoot, builtin: false });
  }
  const releases = new Map<string, string[]>();
  const unattributed = unattributedEvidence.map(item => `pid ${item.pid} ${item.mode}`);
  for (const writer of writers) {
    const label = `pid ${writer.pid} ${writer.mode}`;
    if (validRelease(writer.release)) releases.set(writer.release, [...(releases.get(writer.release) ?? []), label]);
    else unattributed.push(label);
  }
  for (const [release, list] of [...releases].sort()) required.push({ name: `fabric@${release}`, kind: "fabric", release, writers: list });
  if (unattributed.length > 0) required.push({ name: "fabric@unknown", kind: "unattributed", writers: unattributed });
  for (const holder of holders) required.push({ name: holder.name ?? `holder@${holder.pid}`, kind: "holder", pid: holder.pid, reason: holder.reason });
  return required;
};

/** A process other than the gate that holds (or may hold) the mesh's SQLite files open. */
export interface StateDbHolder { pid: number; reason: string; /** Required-reader name; default holder@<pid>. */ name?: string }

/** Injection points of the holder scan (tests). */
export interface ProcScanOptions {
  platform?: NodeJS.Platform;
  procRoot?: string;
  selfPid?: number;
  uid?: number;
  /** Which pids to scan; default every numeric entry of procRoot. */
  listPids?: () => number[];
  /** Lists `<procRoot>/<pid>/fd`; throws as fs.readdirSync does. */
  readFdDir?: (dir: string) => string[];
  /** Reads /proc/locks; throws when unreadable. */
  readLocks?: () => string;
}

const SQLITE_FILES = ["state.db", "state.db-wal", "state.db-shm"].flatMap(name => [name, path.join("state-shadow", name)]);
const inodeKey = (stat: fs.BigIntStats): string => `${stat.dev}:${stat.ino}`;

/**
 * The `major:minor:inode` of a file as /proc/locks prints it (major and minor in hex there; here decimal),
 * from stat's dev (the Linux dev_t encoding of glibc's gnu_dev_major/gnu_dev_minor).
 */
export const procLocksKey = (dev: bigint, ino: bigint): string => {
  const major = ((dev >> 8n) & 0xfffn) | ((dev >> 32n) & ~0xfffn);
  const minor = (dev & 0xffn) | ((dev >> 12n) & ~0xffn);
  return `${major}:${minor}:${ino}`;
};

interface LockEntry { pid: number; type: string; key: string }

/** Every /proc/locks line; throws on any line it cannot parse (the caller then fails closed). */
const parseProcLocks = (text: string): LockEntry[] => text.split("\n").filter(line => line.trim() !== "").map(line => {
  const match = /^\d+:\s+(?:->\s+)?(\S+)\s+\S+\s+\S+\s+(-?\d+)\s+([0-9a-fA-F]+):([0-9a-fA-F]+):(\d+)\s+\S+\s+\S+\s*$/.exec(line);
  if (!match) throw new Error(`unparsable /proc/locks line: ${line}`);
  return { type: match[1]!, pid: Number(match[2]), key: `${BigInt(`0x${match[3]}`)}:${BigInt(`0x${match[4]}`)}:${match[5]}` };
});

/**
 * smarty-dev#7936: who holds the mesh's SQLite files open. The targets are the inodes of state.db, -wal and
 * -shm (and under state-shadow/) plus those the gate's OWN fds point at (readlink and stat of
 * `/proc/self/fd/*`). The holders are the UNION of (a) every other same-uid pid whose readable
 * `/proc/<pid>/fd` holds a target and (b) every /proc/locks entry on a target by another pid (a WAL
 * connection always holds a lock on -shm). A lock entry with pid -1 (OFD) or a pid that does not resolve is
 * ambiguous. An unreadable `/proc/<pid>/fd` is a holder only when /proc/locks itself cannot be read or
 * parsed (fail closed). Residual: a process with an unreadable fd dir that holds state.db open WITHOUT any
 * SQLite lock (not a WAL connection) is not detected. Undefined off Linux: no scan, the caller fails closed.
 */
export const scanStateDbHolders = (root: string, scan: ProcScanOptions = {}): { own: string[]; holders: StateDbHolder[] } | undefined => {
  if ((scan.platform ?? process.platform) !== "linux") return undefined;
  const procRoot = scan.procRoot ?? "/proc";
  const selfPid = scan.selfPid ?? process.pid;
  const uid = scan.uid ?? process.geteuid?.();
  const readFdDir = scan.readFdDir ?? ((dir: string) => fs.readdirSync(dir));
  const readLocks = scan.readLocks ?? (() => fs.readFileSync(path.join(procRoot, "locks"), "utf8"));
  const files = new Set(SQLITE_FILES.map(name => path.join(root, name)));
  const targets = new Set<string>();
  const lockTargets = new Map<string, string>();
  const target = (stat: fs.BigIntStats, file: string): void => {
    targets.add(inodeKey(stat));
    lockTargets.set(procLocksKey(stat.dev, stat.ino), file);
  };
  for (const file of files) {
    const stat = fs.statSync(file, { bigint: true, throwIfNoEntry: false });
    if (stat?.isFile()) target(stat, file);
  }
  const own: string[] = [];
  const ownFds = path.join(procRoot, String(selfPid), "fd");
  for (const fd of (() => { try { return readFdDir(ownFds); } catch { return []; } })()) {
    try {
      const link = fs.readlinkSync(path.join(ownFds, fd));
      if (!files.has(link)) continue;
      const stat = fs.statSync(path.join(ownFds, fd), { bigint: true });
      target(stat, link);
      own.push(`${fd} ${link} ${inodeKey(stat)}`);
    } catch { /* closed meanwhile */ }
  }
  const holders = new Map<number, StateDbHolder>();
  const hold = (pid: number, reason: string): void => { if (!holders.has(pid)) holders.set(pid, { pid, reason }); };
  if (targets.size === 0) return { own, holders: [] };
  const comm = (pid: number): string => { try { return fs.readFileSync(path.join(procRoot, String(pid), "comm"), "utf8").trim(); } catch { return "?"; } };
  // (b) /proc/locks: world-readable, so it sees holders whose fd directory this user cannot read.
  let locksError: string | undefined;
  try {
    for (const entry of parseProcLocks(readLocks())) {
      const file = lockTargets.get(entry.key);
      if (file === undefined || entry.pid === selfPid) continue;
      if (entry.pid <= 0 || !fs.existsSync(path.join(procRoot, String(entry.pid)))) {
        hold(entry.pid, `ambiguous state.db holder: a ${entry.type} lock on ${file} by pid ${entry.pid}, which does not resolve to a process`);
      } else {
        hold(entry.pid, `foreign state.db holder: pid ${entry.pid} (${comm(entry.pid)}) holds a ${entry.type} lock on ${file}`);
      }
    }
  } catch (error) { locksError = (error as Error).message; }
  // (a) the readable fd directories of this user's processes.
  const pids = scan.listPids?.() ?? fs.readdirSync(procRoot).filter(name => /^\d+$/.test(name)).map(Number);
  for (const pid of pids) {
    if (pid === selfPid || holders.has(pid)) continue;
    const owner = fs.statSync(path.join(procRoot, String(pid)), { throwIfNoEntry: false });
    if (!owner || (uid !== undefined && owner.uid !== uid)) continue;
    const fdDir = path.join(procRoot, String(pid), "fd");
    let fds: string[];
    try { fds = readFdDir(fdDir); } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") continue; // exited
      // ponytail: known limit, an accepted security gap (smarty-dev#7936,
      // https://github.com/Smarty-Pants-Inc/smarty-dev/issues/7936#issuecomment-6089556187):
      // an unreadable fd dir whose process holds state.db open WITHOUT any SQLite lock (not a WAL
      // connection) is not detected HERE; WAL connections always hold a lock on -shm, which /proc/locks shows.
      // The write-lease probe (probeStateDbLeases) catches such a holder when it opened before the probe;
      // one that opens after it, under the fence and before the commit, remains undetected.
      // Without /proc/locks an unreadable fd directory may hide a holder: fail closed.
      if (locksError !== undefined) {
        hold(pid, `ambiguous state.db holder: pid ${pid} (${comm(pid)}) ${fdDir} unreadable (${errno(error)}) and /proc/locks unusable (${locksError})`);
      }
      continue;
    }
    for (const fd of fds) {
      let stat: fs.BigIntStats;
      try { stat = fs.statSync(path.join(fdDir, fd), { bigint: true }); } catch { continue; }
      if (!targets.has(inodeKey(stat))) continue;
      let link = "?";
      try { link = fs.readlinkSync(path.join(fdDir, fd)); } catch { /* best effort */ }
      hold(pid, `foreign state.db holder: pid ${pid} (${comm(pid)}) holds ${link}`);
      break;
    }
  }
  return { own, holders: [...holders.values()] };
};

/** Injection points of the lease probe (tests). */
export interface LeaseProbeOptions { platform?: NodeJS.Platform; helper?: string; procRoot?: string; leasesEnable?: string }

/** The installed lease helper, built beside fabric-landlock (scripts/build-landlock.mjs). */
export const leaseHelperPath = (): string => fileURLToPath(new URL("../../dist/native/fabric-mesh-lease", import.meta.url));
const NFS_SUPER_MAGIC = 0x6969;
const LEASE_DOC = "smarty-dev#7936";

/**
 * smarty-dev#7936: a write-lease probe of state.db, -wal and -shm (and under state-shadow/). The kernel
 * refuses a write lease (EAGAIN) while ANY other open file description of the inode exists, whatever the
 * holder's dumpability and with or without SQLite locks; the helper releases it and closes at once. Run it
 * only while THIS process holds none of those files open: its own fd would make the probe refuse too, so
 * that case fails closed. Each held file is the holder `holder@lease:<file>` ("unidentified holder");
 * a probe that cannot run (not Linux, NFS, leases-enable=0, a missing helper, any helper error) is the
 * holder `holder@lease-probe`. Both are never ready; only --accept-unready by name passes them.
 */
export const probeStateDbLeases = (root: string, probe: LeaseProbeOptions = {}): StateDbHolder[] => {
  const files = SQLITE_FILES.map(name => path.join(root, name)).filter(file => fs.lstatSync(file, { throwIfNoEntry: false }) !== undefined);
  if (files.length === 0) return [];
  const unavailable = (why: string): StateDbHolder[] =>
    [{ pid: 0, name: "holder@lease-probe", reason: `state.db lease probe unavailable (${LEASE_DOC}): ${why}` }];
  if ((probe.platform ?? process.platform) !== "linux") return unavailable("not Linux");
  const procRoot = probe.procRoot ?? "/proc";
  const ownFds = path.join(procRoot, "self", "fd");
  for (const fd of (() => { try { return fs.readdirSync(ownFds); } catch { return []; } })()) {
    let link: string;
    try { link = fs.readlinkSync(path.join(ownFds, fd)); } catch { continue; }
    if (files.includes(link)) return unavailable(`this process itself holds ${link} open (fd ${fd}) at the probe`);
  }
  let enable = probe.leasesEnable;
  if (enable === undefined) {
    try { enable = fs.readFileSync(path.join(procRoot, "sys/fs/leases-enable"), "utf8").trim(); }
    catch (error) { return unavailable(`/proc/sys/fs/leases-enable unreadable (${errno(error)})`); }
  }
  if (enable !== "1") return unavailable(`/proc/sys/fs/leases-enable is ${enable}`);
  try { if (fs.statfsSync(root).type === NFS_SUPER_MAGIC) return unavailable(`${root} is on NFS`); }
  catch (error) { return unavailable(`statfs ${root} failed (${errno(error)})`); }
  const helper = probe.helper ?? leaseHelperPath();
  try { fs.accessSync(helper, fs.constants.X_OK); } catch { return unavailable(`helper ${helper} is missing (bun run build)`); }
  // Runtime integrity: the helper must be the twice-built binary the package's build manifest records
  // (scripts/build-landlock.mjs). A missing manifest or another sha256 fails closed.
  const manifestFile = path.join(path.dirname(helper), "manifest.json");
  let expected: unknown;
  try { expected = (JSON.parse(fs.readFileSync(manifestFile, "utf8")) as { helpers?: Record<string, unknown> }).helpers?.["fabric-mesh-lease"]; }
  catch (error) { return unavailable(`helper integrity: build manifest ${manifestFile} unreadable (${errno(error)})`); }
  let actual: string;
  try { actual = createHash("sha256").update(fs.readFileSync(helper)).digest("hex"); }
  catch (error) { return unavailable(`helper integrity: ${helper} unreadable (${errno(error)})`); }
  if (typeof expected !== "string" || expected !== actual) {
    return unavailable(`helper integrity: ${helper} sha256 ${actual} does not match the build manifest (${String(expected)})`);
  }
  const result = spawnSync(helper, files, { encoding: "utf8", timeout: 10_000, stdio: ["ignore", "pipe", "pipe"] });
  if (result.error || (result.status !== 0 && result.status !== 3)) {
    return unavailable(`helper failed (${result.error?.message ?? `exit ${String(result.status)}`}${result.stdout ? `: ${result.stdout.trim()}` : ""})`);
  }
  const holders: StateDbHolder[] = [];
  const seen = new Set<string>();
  for (const line of result.stdout.split("\n").filter(Boolean)) {
    const match = /^(free|held|absent) (.+)$/.exec(line);
    if (!match || !files.includes(match[2]!)) return unavailable(`unexpected helper output: ${line}`);
    seen.add(match[2]!);
    if (match[1] === "held") {
      holders.push({ pid: 0, name: `holder@lease:${path.relative(root, match[2]!)}`,
        reason: `unidentified holder of ${match[2]} (a write lease was refused: another process has it open; ${LEASE_DOC})` });
    }
  }
  if (seen.size !== files.length) return unavailable("the helper did not report every file");
  return holders;
};

/**
 * Owned by this user and not group/other-writable, or why not. Without process.geteuid (Windows) nothing can
 * be verified, so it fails closed (Windows ACL checks: smarty-dev#7548).
 */
const ownership = (stat: fs.Stats, euid: number | undefined): string | undefined => {
  if (euid === undefined) return "unverifiable: this platform has no POSIX owner (smarty-dev#7548)";
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
  // Without an euid (Windows) an existing registry cannot be verified: the same non-overridable refusal as
  // an unsafe one (smarty-dev#7548). An absent registry has nothing to verify.
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
  // No POSIX owner (Windows): no proof or registry can be verified, so the gate refuses outright; the
  // caller turns this into a refusal no --accept-unready lifts (Windows ACL checks: smarty-dev#7548).
  if (euid === undefined) throw new Error("the readiness gate cannot verify reader proofs on this platform (no POSIX owner; Windows ACL checks: smarty-dev#7548)");
  const proofs = loadRegistry(root, euid);
  const ready: string[] = [];
  const unready: UnreadyReader[] = [];
  for (const reader of required) {
    let reason: string | undefined;
    if (reader.kind === "holder") {
      reason = reader.reason;
    } else if (reader.kind === "unattributed") {
      reason = `live writer evidence without an attributable release: ${reader.writers.join("; ")}`;
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
  // Refuse before creating anything: an unverifiable readers/ would block every later switch (smarty-dev#7548).
  if (process.geteuid === undefined) throw new Error(`reader registry ${dir} is ${ownership(fs.statSync(root), undefined)}: proof not written`);
  try { fs.mkdirSync(dir, { mode: 0o700 }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  const stat = fs.lstatSync(dir);
  const unsafe = stat.isDirectory() ? ownership(stat, process.geteuid?.()) : "not a directory (or a symlink)";
  if (unsafe) throw new Error(`reader registry ${dir} is ${unsafe}: proof not written`);
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
export const proveFabricReader = async (root: string, input: { backend: ReaderBackend; name?: string; version?: string; now?: Date;
  /** Test hook: runs between the read and the epoch re-sample. */
  afterRead?: () => void }):
  Promise<{ file: string; proof: ReaderProof; entries: number }> => {
  const release = ownFabricRelease();
  const name = input.name ?? `fabric-${release.slice(0, 40)}`.slice(0, 64);
  if (!isReaderName(name) || !name.startsWith("fabric")) throw new Error(`Bad Fabric reader name ${JSON.stringify(name)} (fabric*, letters, digits, . _ -)`);
  // The fence first, then the epoch, the read and the epoch again: the proof records only an epoch that
  // bracketed the read (a switch needs the same fence, so a change means the read is not that epoch's).
  return holdMeshFence(root, { lockTimeoutMs: FENCE_MS }, async () => {
    const { epoch } = await meshBackendStatus(root);
    const entries = await provedRead(root, input.backend);
    input.afterRead?.();
    const after = (await meshBackendStatus(root)).epoch;
    if (after !== epoch) throw new Error(`the mesh epoch changed during the read (${epoch} -> ${after}): proof not written`);
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
