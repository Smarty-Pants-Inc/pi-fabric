import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { windowsHelperBootstrap } from "./verified-remove.js";

/**
 * smarty-dev#9569 Commit A: the Windows mesh lock release primitive (design sections 4, 5 and 7). A release must never
 * delete or detach a canonical lock directory that a successor owns, and must never leave an ownerless canonical tree:
 *
 * - H (prepareWindowsMeshCapability): before any owner publication, the digest-pinned native helper is materialized
 *   ONCE PER BUILD DIGEST in a private, retained, non-temp cache (`%LOCALAPPDATA%\pi-fabric\native\<digest>\`),
 *   outside the package tree that `bun run build` and package updates replace. The TRUSTED packaged bootstrap (bytes
 *   equal to the digest embedded at build time) publishes it from a closed and rehashed staging directory without ever
 *   replacing a published digest directory, and checks the cache chain's ownership, DACL and real-directory ancestry;
 *   the cache executable never vouches for itself. The capability `{absolutePath, digest, meshAbi}` is captured with the
 *   lock and its bytes are rehashed before EVERY spawn; release never re-resolves through the package. No automatic
 *   garbage collection: removing a published digest needs a global maintenance drain (design H4).
 * - W (releaseWindowsMeshLock): W1 here: an exact bigint snapshot of the lock directory and its owner, and the owner's
 *   FULL bytes compared with the ORIGINAL record of this acquisition. Natively (`--mesh-retire-v1`, one bounded framed
 *   stdin request): W2 hold the directory with DELETE and no delete sharing, W3 verify the owner's ids and full bytes on
 *   its handle and the entry set exactly {owner}, W4 COMMIT rename the HELD directory object to the sibling
 *   `r.<sha256>` (never replacing), W5 only then dispose of the owner and the empty directory by handle below the
 *   retired object, W6 one terminal line. A native watchdog and a parent timeout bound the child; spawnSync joins it.
 * - Outcomes are strict: a started child without a valid terminal line paired with its exit status is
 *   `release-outcome-unknown` (never a guessed pre-commit refusal). Only then ONE bounded same-record reconciliation
 *   runs (at most two children); no application work is ever replayed here.
 *
 * There is no Windows by-name fallback anywhere in this module. Delivery boundary (Commit A): this is the release
 * primitive only. The root-wide guarantee also needs every Windows owner mutator routed through it, acquisition
 * recovery (Commit B) and the mixed-version drain gate; it is not advertised by this module alone.
 */

export interface WindowsMeshCapability { readonly absolutePath: string; readonly digest: string; readonly meshAbi: 1 }

export type WindowsMeshReleaseOutcome =
  | { outcome: "not-owner"; history?: readonly string[] }
  | { outcome: "released"; cleanup: "clean" | "pending"; retiredPath: string; history?: readonly string[] }
  | { outcome: "refused-before-retirement" | "helper-unavailable/not-started" | "release-outcome-unknown"; reason: string; history?: readonly string[] };

/** One joined native release child (attempt 1, or 2 for the reconciliation): for diagnostics and the hosted benchmark. */
export interface WindowsMeshReleaseChildSample {
  readonly elapsedMs: number; readonly absolutePath: string; readonly digest: string; readonly outcome: string; readonly attempt: 1 | 2;
}

export type WindowsMeshCheckpoint = "before-commit" | "after-commit" | "before-owner-dispose" | "after-owner" | "after-directory";

/**
 * Explicit, bounded test and diagnostic seams (in-process only; nothing reads them from the environment). Each keeps
 * the production checks: substitute bootstrap candidates are still digest-verified, a substitute cache root is still
 * created, published and privacy-checked by the trusted bootstrap, and the deadline stays within the native cap.
 */
export const windowsMeshReleaseTestHooks: {
  /** Packaged bootstrap candidates instead of dist/native beside the bundle (each still digest-verified). */
  bootstrapCandidates?: readonly string[];
  /** Cache base directory instead of %LOCALAPPDATA%\pi-fabric\native (same publication and privacy checks). */
  cacheRoot?: string;
  /** Observe exactly one closed, rehashed unpublished stage, before any publisher starts. Tests may pause their
   * plain Node parent here with a bounded Atomics.wait; production has no environment-controlled staging hook. */
  afterCacheStaged?: (staging: string) => void;
  /** Pause the native child at a checkpoint: it writes DIR\<name>.paused (its pid) and waits for DIR\go.
   * At before-owner-dispose, exact go bytes "expire-deadline\n" only shorten its logical deadline, never its watchdog. */
  checkpoint?: { name: WindowsMeshCheckpoint; dir: string };
  /** Native watchdog deadline per child, integer ms in [1, 30000] (default 4000); the parent timeout is 1 s later. */
  deadlineMs?: number;
  /** Called once per joined release child (never for cache preparation children); its exceptions are ignored. */
  afterChild?: (sample: WindowsMeshReleaseChildSample) => void;
} = {};

const WIN_HELPER = "fabric-win-remove.exe";
const MAX_RECORD = 4096;
const DEFAULT_DEADLINE_MS = 4000;
const MAX_DEADLINE_MS = 30_000;
const PREP_TIMEOUT_MS = 10_000;
const CAPABILITY_LINE = "FMRCAP1 mesh-retire-v1 abi=1";
const TEST_ENV = "FABRIC_WIN_MESH_TEST_";

const realWindows = (): boolean => process.platform === "win32" && os.type() === "Windows_NT";
const sha256 = (bytes: Uint8Array | string): string => createHash("sha256").update(bytes).digest("hex");
const errno = (error: unknown): string => (error as NodeJS.ErrnoException)?.code ?? "error";

let established: WindowsMeshCapability | undefined;
/** Tests: forget this process's established capability (the next prepare runs H1-H3 again). */
export const forgetWindowsMeshCapability = (): void => { established = undefined; };

/** The file at `file` is a regular file (not a link) whose bytes equal `digest`. */
const holdsDigest = (file: string, digest: string): boolean => {
  try {
    const stat = fs.lstatSync(file);
    return stat.isFile() && !stat.isSymbolicLink() && sha256(fs.readFileSync(file)) === digest;
  } catch { return false; }
};

/** A bounded child of a digest-verified executable (rehashed right before the spawn), no shell. */
const runVerified = (file: string, digest: string, args: string[]): { status: number | null; stdout: string } | undefined => {
  if (!holdsDigest(file, digest)) return undefined;
  const result = spawnSync(file, args, { encoding: "utf8", timeout: PREP_TIMEOUT_MS, windowsHide: true, maxBuffer: 64 * 1024, env: childEnv(undefined) });
  return { status: result.error ? null : result.status, stdout: String(result.stdout ?? "") };
};

/** The child environment: never an inherited test checkpoint; only this module's explicit hook sets one. */
const childEnv = (checkpoint: { name: WindowsMeshCheckpoint; dir: string } | undefined): NodeJS.ProcessEnv => {
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) if (!key.toUpperCase().startsWith(TEST_ENV)) env[key] = value;
  if (checkpoint) { env[`${TEST_ENV}CHECKPOINT`] = checkpoint.name; env[`${TEST_ENV}DIR`] = checkpoint.dir; }
  return env;
};

/** Resolve aliases without creating anything, including a not-yet-created suffix below an existing ancestor. */
const canonicalCacheLocation = (file: string): string => {
  let ancestor = path.resolve(file);
  const suffix: string[] = [];
  for (;;) {
    try { return path.join(fs.realpathSync.native(ancestor), ...suffix.reverse()); }
    catch (error) {
      if (errno(error) !== "ENOENT") throw error;
      const parent = path.dirname(ancestor);
      if (parent === ancestor) throw error;
      suffix.push(path.basename(ancestor));
      ancestor = parent;
    }
  }
};

/** The cache chain: the directories this module owns (first to last) and the base that holds digest directories. */
const cacheChain = (): { chain: string[]; base: string } => {
  const hook = windowsMeshReleaseTestHooks.cacheRoot;
  if (hook !== undefined) {
    if (!path.isAbsolute(hook)) throw new Error("Windows mesh helper cache root must be absolute");
    return { chain: [path.resolve(hook)], base: path.resolve(hook) };
  }
  const local = process.env.LOCALAPPDATA;
  if (!local || !path.isAbsolute(local)) throw new Error("Windows mesh helper cache needs an absolute %LOCALAPPDATA% (refusing a temp or shared fallback)");
  const canonicalLocal = canonicalCacheLocation(local);
  // H2's retained executable cannot live under a supported temp cleaner. Compare canonical aliases, case-insensitively
  // and on a component boundary: Local is a valid parent of Local\Temp, and Temp-other is not below Temp.
  const comparable = (file: string): string => path.toNamespacedPath(path.normalize(file)).toLowerCase();
  const selected = comparable(canonicalLocal);
  const top = path.join(canonicalLocal, "pi-fabric");
  const selectedCache = comparable(canonicalCacheLocation(path.join(top, "native")));
  for (const temp of [os.tmpdir(), process.env.TEMP, process.env.TMP]) {
    if (!temp) continue;
    const canonicalTemp = comparable(canonicalCacheLocation(temp));
    const prefix = canonicalTemp.endsWith(path.sep) ? canonicalTemp : `${canonicalTemp}${path.sep}`;
    if (selected === canonicalTemp || selected.startsWith(prefix) || selectedCache === canonicalTemp || selectedCache.startsWith(prefix)) {
      throw new Error("Windows mesh helper cache %LOCALAPPDATA% must not be in a temporary location (refusing before any cache or lock publication)");
    }
  }
  return { chain: [top, path.join(top, "native")], base: path.join(top, "native") };
};

/**
 * H1-H4: the retained helper capability, before any Windows owner publication. Throws (nothing published, no lock
 * taken) when no trusted capability is available. Real Windows only.
 */
export const prepareWindowsMeshCapability = (): WindowsMeshCapability => {
  if (!realWindows()) throw new Error("Windows mesh release capability is available on real Windows only");
  // H1: a running process keeps the capability it established, even after the package dist is gone.
  if (established && holdsDigest(established.absolutePath, established.digest)) return established;
  established = undefined;
  const boot = windowsHelperBootstrap(windowsMeshReleaseTestHooks.bootstrapCandidates);
  if (!boot) throw new Error("no trusted packaged Windows helper (missing, or not the digest embedded in this build); refusing before any lock publication");
  const { digest } = boot;
  // H2: establish private inheritable ACLs BEFORE Node creates staging or writes executable bytes. Windows mkdir
  // inherits the profile's DACL; POSIX mode bits do not make it private. Only the digest-verified packaged bootstrap
  // creates missing components. Existing unsafe components (even empty ones) are refused, never silently ACL-healed.
  const { chain, base } = cacheChain();
  const creation = runVerified(boot.file, digest, ["--cache-create-v1", ...chain.map(entry => path.toNamespacedPath(entry))]);
  if (!creation || creation.status !== 0 || !/^ok created\r?\n?$/.test(creation.stdout)) {
    const detail = creation?.stdout.trim() || (creation ? `exit ${String(creation.status)}` : "bootstrap changed");
    const index = /^refused ([0-9]+) /.exec(detail)?.[1];
    const component = index === undefined ? base : chain[Number(index)] ?? base;
    throw new Error(`Windows mesh helper cache private creation refused at ${component} (${detail}); existing components are never ACL-healed. Stop all cache users and have the operator inspect this exact component before global cache maintenance; no helper or lock was published`);
  }
  for (const dir of chain) {
    const stat = fs.lstatSync(dir);
    if (stat.isSymbolicLink() || !stat.isDirectory()) throw new Error(`Windows mesh helper cache component is not a real directory: ${dir}`);
  }
  const realBase = fs.realpathSync.native(base);
  const realChain = chain.length === 2 ? [path.dirname(realBase), realBase] : [realBase];
  const digestDir = path.join(realBase, digest);
  const exe = path.join(digestDir, WIN_HELPER);
  // Its own preflight (not a mesh-root path): CreateProcess needs a short absolute application path.
  if (exe.length >= 240 || exe.includes("\0")) throw new Error(`Windows mesh helper cache path is too long (${exe.length} UTF-16 units)`);
  let present = true;
  try { fs.lstatSync(digestDir); } catch (error) { if (errno(error) !== "ENOENT") throw error; present = false; }
  if (!present) {
    // Unpublished staging: a crash leaves only an ignored `.staging-*` sibling, never a partial digest directory.
    const staging = path.join(realBase, `.staging-${process.pid}-${randomUUID().slice(0, 8)}`);
    const staged = path.join(staging, WIN_HELPER);
    fs.mkdirSync(staging);
    let published = false;
    try {
      fs.writeFileSync(staged, boot.bytes, { flag: "wx" });
      if (!holdsDigest(staged, digest)) throw new Error("staged Windows mesh helper does not rehash to its digest");
      windowsMeshReleaseTestHooks.afterCacheStaged?.(staging);
      // Read back actual inherited directory/file privacy before publication too, using the trusted bootstrap.
      const stagedPrivacy = runVerified(boot.file, digest, ["--cache-private-v1", ...[...realChain, staging, staged].map(entry => path.toNamespacedPath(entry))]);
      if (!stagedPrivacy || stagedPrivacy.status !== 0 || !/^ok private\r?\n?$/.test(stagedPrivacy.stdout)) {
        throw new Error(`unpublished Windows mesh helper staging is not private (${stagedPrivacy?.stdout.trim() || "bootstrap changed or privacy check failed"})`);
      }
      const publish = runVerified(boot.file, digest, ["--cache-publish-v1", path.toNamespacedPath(staging), path.toNamespacedPath(digestDir)]);
      if (!publish) throw new Error("the packaged Windows helper no longer matches its digest");
      if (publish.status === 0 && /^ok published\r?\n?$/.test(publish.stdout)) published = true;
      // A concurrent publisher won: its directory is verified below, never replaced.
      else if (publish.status !== 6) throw new Error(`Windows mesh helper cache publication failed (${publish.stdout.trim() || `exit ${String(publish.status)}`})`);
    } finally {
      if (!published) {
        // Only this attempt's own unpublished staging (exact names it created).
        try { fs.unlinkSync(staged); } catch { /* absent */ }
        try { fs.rmdirSync(staging); } catch { /* left as ignored staging */ }
      }
    }
  }
  if (!holdsDigest(exe, digest)) {
    throw new Error(`the retained Windows mesh helper at ${exe} is not the exact digest ${digest}; it is never overwritten (restore the exact bytes, or run the global cache maintenance drain)`);
  }
  // Privacy and ancestry, judged by the trusted bootstrap, not by the cache executable.
  const privacy = runVerified(boot.file, digest, ["--cache-private-v1", ...[...realChain, digestDir, exe].map(entry => path.toNamespacedPath(entry))]);
  if (!privacy || privacy.status !== 0 || !/^ok private\r?\n?$/.test(privacy.stdout)) {
    throw new Error(`the Windows mesh helper cache is not private (${privacy ? privacy.stdout.trim() || `exit ${String(privacy.status)}` : "bootstrap changed"})`);
  }
  // H3: the versioned capability of the exact retained executable.
  const query = runVerified(exe, digest, ["--capability-v1"]);
  if (!query || query.status !== 0 || query.stdout.replace(/\r?\n$/, "") !== CAPABILITY_LINE) {
    throw new Error(`the retained Windows mesh helper does not offer ${CAPABILITY_LINE}`);
  }
  established = Object.freeze({ absolutePath: exe, digest, meshAbi: 1 as const });
  return established;
};

/** The fields of one `--mesh-retire-v1` request (exact ids as bigint; paths namespaced). */
export interface MeshRetireRequest {
  lockPath: string; retiredPath: string;
  dirVolume: bigint; dirIndex: bigint;
  ownerVolume: bigint; ownerIndex: bigint; ownerFileTime: bigint; ownerSize: bigint;
  record: Uint8Array; deadlineMs: number;
}

/** The framed request: "FMRQ1", then ten fields of 8 lowercase hex digits of length plus bytes, nothing after. */
export const encodeMeshRetireRequest = (request: MeshRetireRequest): Buffer => {
  const id = (value: bigint): string => {
    if (value < 0n || value > 0xffff_ffff_ffff_ffffn) throw new RangeError("id out of range");
    return value.toString(16);
  };
  if (!Number.isInteger(request.deadlineMs) || request.deadlineMs < 1 || request.deadlineMs > MAX_DEADLINE_MS) throw new RangeError("deadline out of range");
  if (request.record.length < 1 || request.record.length > MAX_RECORD || request.record.includes(0) || request.ownerSize !== BigInt(request.record.length)) {
    throw new RangeError("record must be 1..4096 bytes without NUL and match owner size");
  }
  // Mirror widePath's bounded strict UTF-8 conversion, not Buffer's replacement of lone UTF-16 surrogates.
  for (const file of [request.lockPath, request.retiredPath]) {
    const bytes = Buffer.from(file, "utf8");
    if (!file.startsWith("\\\\?\\") || file.includes("\0") || bytes.length > 32_767 || file.length > 32_767 || bytes.toString("utf8") !== file) {
      throw new RangeError("path must be namespaced, bounded valid UTF-8 without NUL");
    }
    // E3 is the logical on-disk mesh budget, including the owner leaf. The namespace prefix is not a disk component.
    if (windowsLogicalPathUnits(`${file}\\owner`) >= 240) throw new RangeError("mesh owner path must be below 240 logical UTF-16 units");
  }
  const parent = (file: string): string => file.slice(0, file.lastIndexOf("\\"));
  const lockBase = request.lockPath.slice(request.lockPath.lastIndexOf("\\") + 1);
  const destBase = request.retiredPath.slice(request.retiredPath.lastIndexOf("\\") + 1);
  if (parent(request.lockPath).length < 4 || parent(request.lockPath) !== parent(request.retiredPath) ||
      !lockBase || lockBase === "." || lockBase === ".." || !/^r\.[0-9a-f]{64}$/.test(destBase)) {
    throw new RangeError("retirement must be an r.<64 lowercase hex> sibling of the lock");
  }
  const fields: Uint8Array[] = [request.lockPath, request.retiredPath, id(request.dirVolume), id(request.dirIndex), id(request.ownerVolume),
    id(request.ownerIndex), id(request.ownerFileTime), id(request.ownerSize)].map(text => Buffer.from(text, "utf8"));
  fields.push(request.record, Buffer.from(String(request.deadlineMs), "ascii"));
  const parts: Uint8Array[] = [Buffer.from("FMRQ1", "ascii")];
  for (const field of fields) parts.push(Buffer.from(field.length.toString(16).padStart(8, "0"), "ascii"), field);
  const message = Buffer.concat(parts);
  if (message.length > 65_536) throw new RangeError("request exceeds 64 KiB");
  return message;
};

/** The deterministic retirement name: a sibling `r.<sha256>` of lock kind, exact directory id and the full record. */
export const meshRetirementName = (lockBase: string, dirVolume: bigint, dirIndex: bigint, record: Uint8Array): string =>
  `r.${createHash("sha256").update(`fabric-mesh-retire-v1\0${lockBase}\0${dirVolume.toString(16)}\0${dirIndex.toString(16)}\0`).update(record).digest("hex")}`;

/** E3: the logical on-disk length of an absolute Windows path in UTF-16 units: the drive or UNC root, every separator,
 * component and leaf, with only the Win32 namespace transport syntax (`\\?\`, or `\\?\UNC\` for `\\`) excluded.
 * Mesh-generated paths must stay below 240 units at a 150-unit root (the retired `r.<64 hex>\owner` is 223). */
export const windowsLogicalPathUnits = (file: string): number =>
  file.startsWith("\\\\?\\UNC\\") ? file.length - 6 : file.startsWith("\\\\?\\") ? file.length - 4 : file.length;
/** E3: the API string metric, measured separately: the exact string handed to Win32 (namespace prefix included). */
export const windowsApiPathUnits = (file: string): number => file.length;

/** A joined child as spawnSync reports it. */
export interface MeshRetireChildResult {
  status: number | null; signal: NodeJS.Signals | null; stdout: Uint8Array | string | null | undefined;
  error?: Error | undefined; pid?: number | undefined;
}
const TERMINAL = /^FMR1 (released|not-owner|refused) (parse|guard|owner|commit|cleanup) ([a-z0-9-]{1,48})\n$/;
// Closed ABI: every meshOut/refusedAt emission in native/fabric-win-remove.c (including conditional details).
// A new native tuple needs an explicit adapter update; a plausible phase or pending-* spelling is not commit proof.
const TERMINAL_DETAILS: Readonly<Record<string, readonly string[]>> = {
  "0 released cleanup": ["clean", "pending-location", "pending-entries-unreadable", "pending-extra-entries", "pending-deadline",
    "pending-owner-unverified", "pending-record-unreadable", "pending-owner-delete", "pending-directory-not-empty"],
  "5 not-owner guard": ["absent", "directory-replaced"],
  "5 not-owner owner": ["absent", "record-differs"],
  "3 refused parse": ["watchdog-unavailable", "stdin-unreadable", "message-too-large", "bad-magic", "bad-field-length",
    "truncated-field", "trailing-bytes", "bad-path", "bad-id", "bad-record", "bad-deadline", "bad-placement", "mesh-path-too-long"],
  "3 refused guard": ["ancestor-not-held", "directory-not-held", "directory-unverified", "directory-location"],
  "3 refused owner": ["path-too-long", "owner-not-held", "owner-modified", "owner-unverified", "record-unreadable",
    "owner-outside-directory", "entries-unreadable", "unexpected-entries"],
  "3 refused commit": ["deadline", "no-memory", "destination-exists", "rename-denied", "rename-failed"],
};

/**
 * Strict interpretation: exactly one terminal line, paired with its exit status (released 0, not-owner 5, refused 3).
 * A child that never started is `helper-unavailable/not-started`; every other mismatch (timeout, signal, crash,
 * empty, truncated or malformed output, an unpaired status) is `release-outcome-unknown`.
 */
export const interpretMeshRetireResult = (result: MeshRetireChildResult, retiredPath: string): WindowsMeshReleaseOutcome => {
  if (result.error && !result.pid) return { outcome: "helper-unavailable/not-started", reason: `the helper did not start (${errno(result.error)})` };
  const exit = result.signal ? `signal ${result.signal}` : `exit ${String(result.status)}`;
  if (result.error) return { outcome: "release-outcome-unknown", reason: `the helper did not finish (${errno(result.error)}, ${exit})` };
  if (result.signal || result.status === null) return { outcome: "release-outcome-unknown", reason: `the helper was terminated (${exit})` };
  const text = typeof result.stdout === "string" ? result.stdout : Buffer.from(result.stdout ?? new Uint8Array()).toString("utf8");
  const line = TERMINAL.exec(text);
  if (!line || line[0] !== text) return { outcome: "release-outcome-unknown", reason: `no valid terminal record (${exit}, ${text.length} bytes of output)` };
  const [, verdict, phase, detail] = line as unknown as [string, string, string, string];
  if (!TERMINAL_DETAILS[`${result.status} ${verdict} ${phase}`]?.includes(detail)) {
    return { outcome: "release-outcome-unknown", reason: `unrecognized terminal record or exit pairing (${verdict} ${phase} ${detail}, ${exit})` };
  }
  if (verdict === "released") {
    if (detail === "clean") return { outcome: "released", cleanup: "clean", retiredPath };
    return { outcome: "released", cleanup: "pending", retiredPath, history: [`cleanup ${detail}`] };
  }
  if (verdict === "not-owner") return { outcome: "not-owner" };
  return { outcome: "refused-before-retirement", reason: `${phase}: ${detail}` };
};

type Snapshot = { dir: fs.BigIntStats; owner: fs.BigIntStats; bytes: Buffer; names: string[] };
const sameSnapshot = (a: Snapshot, b: Snapshot): boolean =>
  a.dir.dev === b.dir.dev && a.dir.ino === b.dir.ino && a.owner.dev === b.owner.dev && a.owner.ino === b.owner.ino &&
  a.owner.mtimeNs === b.owner.mtimeNs && a.owner.size === b.owner.size && a.bytes.equals(b.bytes) &&
  a.names.length === b.names.length && a.names.every((name, i) => name === b.names[i]);

class Early { constructor(readonly outcome: WindowsMeshReleaseOutcome) {} }
const refused = (reason: string): never => { throw new Early({ outcome: "refused-before-retirement", reason }); };
const notOwner = (): never => { throw new Early({ outcome: "not-owner" }); };

/** W1: one exact snapshot; absence is not-owner, any other doubt is a refusal (nothing changed). */
const snapshot = (lock: string, bounded = false): Snapshot => {
  const ownerPath = path.join(lock, "owner");
  let dir: fs.BigIntStats;
  try { dir = fs.lstatSync(lock, { bigint: true }); } catch (error) { return errno(error) === "ENOENT" ? notOwner() : refused(`lock directory unreadable (${errno(error)})`); }
  if (dir.isSymbolicLink() || !dir.isDirectory()) refused("the lock path is not a real directory");
  let owner: fs.BigIntStats;
  try { owner = fs.lstatSync(ownerPath, { bigint: true }); } catch (error) { return errno(error) === "ENOENT" ? notOwner() : refused(`owner unreadable (${errno(error)})`); }
  if (owner.isSymbolicLink() || !owner.isFile()) refused("the owner is not a regular file");
  let bytes: Buffer;
  if (owner.size > BigInt(MAX_RECORD)) bytes = Buffer.alloc(0); // cannot equal a record of at most 4096 bytes
  else {
    try { bytes = fs.readFileSync(ownerPath); } catch (error) { return errno(error) === "ENOENT" ? notOwner() : refused(`owner unreadable (${errno(error)})`); }
  }
  let names: string[];
  try { names = (bounded ? boundedNames(lock, 1) : fs.readdirSync(lock)).sort(); }
  catch (error) { if (error instanceof Early) throw error; return errno(error) === "ENOENT" ? notOwner() : refused(`lock directory unreadable (${errno(error)})`); }
  return { dir, owner, bytes, names };
};

/** The judged last-write time as a FILETIME (100 ns since 1601), exactly as the helper reads it. */
const fileTime = (stat: fs.BigIntStats): bigint => stat.mtimeNs / 100n + 116_444_736_000_000_000n;

const MAX_RECEIPT_PARENT_ENTRIES = 256;
/** Finite, nonrecursive inspection, including unrelated names. Never mutate while enumerating. One extra read
 * distinguishes an exhausted cap from a complete listing; always close before a child can start. */
const boundedNames = (directory: string, cap: number): string[] => {
  const listing = fs.opendirSync(directory, { bufferSize: 32 });
  try {
    const names: string[] = [];
    for (let i = 0; i <= cap; i++) {
      const entry = listing.readSync();
      if (!entry) return names;
      if (i === cap) refused(`retired receipt inspection exceeds ${cap} entries at ${directory} (operator action needed)`);
      names.push(entry.name);
    }
    return names;
  } finally { listing.closeSync(); }
};

/** Parent death loses JS state. The durable T name binds canonical basename + ORIGINAL directory S + FULL R.
 * Inspect at most 256 parent entries, then judge ONE matching real directory, or refuse ambiguity. Names alone,
 * foreign owner bytes, and arbitrary empty siblings confer no authority. Native rechecks everything under guard. */
const rediscoverReceipt = (parent: string, base: string, record: Buffer): { retiredPath: string; dir: fs.BigIntStats; owner?: fs.BigIntStats } => {
  const names = boundedNames(parent, MAX_RECEIPT_PARENT_ENTRIES);
  let candidate: { retiredPath: string; dir: fs.BigIntStats } | undefined;
  for (const name of names) {
    if (!/^r\.[0-9a-f]{64}$/.test(name)) continue;
    const retiredPath = path.join(parent, name);
    let dir: fs.BigIntStats;
    try { dir = fs.lstatSync(retiredPath, { bigint: true }); }
    catch (error) { if (errno(error) === "ENOENT") continue; return refused(`retired receipt unreadable (${errno(error)}; operator action needed)`); }
    if (meshRetirementName(base, dir.dev, dir.ino, record) !== name) continue;
    if (dir.isSymbolicLink() || !dir.isDirectory()) refused("matching retired receipt is not a real directory (operator action needed)");
    if (candidate) refused("multiple identity-bound same-R retired receipts (operator action needed; none touched)");
    candidate = { retiredPath, dir };
  }
  if (!candidate) return notOwner();
  const { retiredPath, dir } = candidate;
  const entries = boundedNames(retiredPath, 1);
  if (entries.length === 0) {
    const again = fs.lstatSync(retiredPath, { bigint: true });
    if (!again.isDirectory() || again.isSymbolicLink() || again.dev !== dir.dev || again.ino !== dir.ino || boundedNames(retiredPath, 1).length !== 0) {
      refused("empty retired receipt changed while judged (operator action needed)");
    }
    return candidate; // synthetic owner fields are unusable: native may ONLY dispose an empty original directory
  }
  if (entries[0] !== "owner") refused("unjudged entries in matching retired receipt (operator action needed; nothing touched)");
  // Snapshot's absence convention is NOT enough for an already matched T: lost/torn proof refuses, not not-owner.
  let judged: Snapshot, again: Snapshot;
  try { judged = snapshot(retiredPath, true); again = snapshot(retiredPath, true); }
  catch (error) { if (error instanceof Early) return refused("matching retired receipt proof unavailable (operator action needed)"); throw error; }
  if (!sameSnapshot(judged, again) || judged.dir.dev !== dir.dev || judged.dir.ino !== dir.ino ||
      !judged.bytes.equals(record) || judged.names.length !== 1 || judged.names[0] !== "owner") {
    refused("matching retired receipt proof differs or changed (operator action needed; nothing touched)");
  }
  return { ...candidate, owner: judged.owner };
};

type PreparedRelease = { request: Buffer; retiredPath: string; deadlineMs: number };
/** Judge once. A canonical match stays on the normal fast path (NO parent scan). Only absence/record mismatch
 * permits bounded rediscovery. Neither discovery nor immediate unknown reconciliation ever snapshots B as S. */
const prepareRelease = (lock: string, record: Buffer): PreparedRelease => {
  let parent: string;
  try { parent = fs.realpathSync.native(path.dirname(path.resolve(lock))); }
  catch (error) { return errno(error) === "ENOENT" ? notOwner() : refused(`lock root unresolvable (${errno(error)})`); }
  const base = path.basename(lock);
  const lockPath = path.join(parent, base);
  let proof: { retiredPath: string; dir: fs.BigIntStats; owner?: fs.BigIntStats };
  try {
    let judged = snapshot(lockPath);
    let again = snapshot(lockPath);
    if (!sameSnapshot(judged, again)) { judged = again; again = snapshot(lockPath); }
    if (!sameSnapshot(judged, again)) refused("the lock changed while it was judged (retry the release)");
    if (!judged.bytes.equals(record)) notOwner();
    if (judged.names.length !== 1 || judged.names[0] !== "owner") refused("unexpected entries in the lock directory (owner kept as proof; operator action needed)");
    proof = { retiredPath: path.join(parent, meshRetirementName(base, judged.dir.dev, judged.dir.ino, record)), dir: judged.dir, owner: judged.owner };
  } catch (error) {
    if (!(error instanceof Early) || error.outcome.outcome !== "not-owner") throw error;
    proof = rediscoverReceipt(parent, base, record);
  }
  const hook = windowsMeshReleaseTestHooks.deadlineMs;
  const deadlineMs = Number.isInteger(hook) && hook! >= 1 && hook! <= MAX_DEADLINE_MS ? hook! : DEFAULT_DEADLINE_MS;
  const request = encodeMeshRetireRequest({
    lockPath: path.toNamespacedPath(lockPath), retiredPath: path.toNamespacedPath(proof.retiredPath),
    dirVolume: proof.dir.dev, dirIndex: proof.dir.ino, ownerVolume: proof.owner?.dev ?? 0n, ownerIndex: proof.owner?.ino ?? 0n,
    ownerFileTime: proof.owner ? fileTime(proof.owner) : 0n, ownerSize: proof.owner?.size ?? BigInt(record.length), record, deadlineMs,
  });
  return { request, retiredPath: proof.retiredPath, deadlineMs };
};

const attempt = (prepared: PreparedRelease, capability: WindowsMeshCapability, number: 1 | 2): WindowsMeshReleaseOutcome => {
  // The captured capability, rehashed right before this spawn; never re-resolved through the package.
  if (capability.meshAbi !== 1 || !holdsDigest(capability.absolutePath, capability.digest)) {
    return { outcome: "helper-unavailable/not-started", reason: `the retained Windows mesh helper ${capability.absolutePath} is missing or not digest ${capability.digest} (restore the exact bytes, then retry the release)` };
  }
  const started = performance.now();
  let outcome: WindowsMeshReleaseOutcome;
  try {
    const child = spawnSync(capability.absolutePath, ["--mesh-retire-v1"], {
      input: prepared.request, timeout: prepared.deadlineMs + 1000, windowsHide: true, maxBuffer: 4096, env: childEnv(windowsMeshReleaseTestHooks.checkpoint),
    });
    outcome = interpretMeshRetireResult({ status: child.status, signal: child.signal, stdout: child.stdout, error: child.error, pid: child.pid }, prepared.retiredPath);
  } catch (error) {
    // Without a joined spawn receipt, do not guess that a child never started or that W4 did not execute.
    outcome = { outcome: "release-outcome-unknown", reason: `helper invocation failed without a terminal receipt (${errno(error)})` };
  }
  try {
    windowsMeshReleaseTestHooks.afterChild?.({ elapsedMs: performance.now() - started, absolutePath: capability.absolutePath, digest: capability.digest, outcome: outcome.outcome, attempt: number });
  } catch { /* an observer never alters a release */ }
  return outcome;
};

/**
 * Release the Windows mesh lock directory `lock` whose owner holds exactly `originalRecord` (the bytes this
 * acquisition published), with the capability captured at acquisition. Synchronous; never throws for an outcome.
 * `not-owner`: no canonical match AND no identity-bound same-R retirement receipt - nothing touched. `released`: the
 * held directory was retired or its exact receipt resumed at `retiredPath` (cleanup `pending` leaves it off-canonical).
 * Explicit same-R release-only retries rediscover full-proof/empty receipts without any process-global state.
 * A first `release-outcome-unknown`
 * gets ONE same-record reconciliation; only released/not-owner establishes a safe canonical result. Any other second
 * outcome retains the original unknown and both attempts' history, allowing an explicit release-only retry.
 */
export const releaseWindowsMeshLock = (lock: string, originalRecord: string, capability: WindowsMeshCapability): WindowsMeshReleaseOutcome => {
  if (!realWindows()) return { outcome: "helper-unavailable/not-started", reason: "the Windows mesh release runs on real Windows only" };
  const record = Buffer.from(originalRecord, "utf8");
  if (record.length < 1 || record.length > MAX_RECORD || record.includes(0)) return { outcome: "refused-before-retirement", reason: "the original record must be 1..4096 bytes without NUL" };
  let prepared: PreparedRelease;
  try { prepared = prepareRelease(lock, record); }
  catch (error) {
    if (error instanceof Early) return error.outcome;
    return { outcome: "refused-before-retirement", reason: `release check failed (${errno(error)})` };
  }
  // Retain this exact original frame across unknown -> reconciliation. Explicit release-only calls from a new JS
  // state recover the same S/R binding from T, never from a successor or a process-global request cache.
  const first = attempt(prepared, capability, 1);
  if (first.outcome !== "release-outcome-unknown") return first;
  const history = [`attempt 1: release-outcome-unknown (${first.reason})`];
  const second = attempt(prepared, capability, 2);
  // Only these prove this holder no longer owns canonical L. A refusal says nothing about the first child's commit.
  if (second.outcome === "released" || second.outcome === "not-owner") {
    return { ...second, history: [...history, ...(second.history ?? []), `attempt 2: ${second.outcome}`] };
  }
  return { outcome: "release-outcome-unknown", reason: `${first.reason}; reconciliation: ${second.outcome} (${second.reason})`,
    history: [...history, ...(second.history ?? []), `attempt 2: ${second.outcome} (${second.reason})`] };
};
