import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { windowsDataRoot, windowsRootSpelling } from "./windows-temp-root.js";
import { posixDataRoot } from "./temp-root.js";
import type { ScratchHostEpoch } from "./scratch-process-census.js";
import { processStartTime } from "../residency/process-identity.js";
import { ownedStat } from "./scratch.js";
import { checkedProcessScratchScope, createProcessScratchScope, removeEmptyProcessScratchScope, type ProcessScratchScope, type ScopedScratchLaunch } from "./process-scratch-scope.js";

export const RUN_TMP_DIRECTORY = "tmp";
export const UNRESOLVED_SCRATCH_FILE = "unresolved-scratch.json";
export const NEVER_STARTED_FILE = "never-started.json";
export const JOINED_SCRATCH_FILE = "scratch-scope-joined.json";

/** Validate the selected explicit/env/default root before the manager writes
 * even task.txt. Windows direct callers retain the native ACL proof; the
 * AgentManager Windows admission path deliberately bypasses this for the
 * inheritance-only #4800 scope. */
export const prepareRunRoot = (root: string): string => {
  if (process.platform !== "win32") return posixDataRoot(root, { create: true });
  const missing: string[] = [];
  // Refuse Windows spellings before any directory/ACL access or mkdir. Keep
  // POSIX-native orchestration seams on their existing validation/call path;
  // on native Windows every spelling still gets the early lexical proof.
  const posixSpelling = path.sep === "/" && root.startsWith("/") && !root.startsWith("//");
  let current = posixSpelling ? path.resolve(root) : windowsRootSpelling(root);
  for (;;) {
    try { fs.lstatSync(current); break; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      missing.unshift(current);
      const parent = path.dirname(current);
      if (parent === current) throw error;
      current = parent;
    }
  }
  let checked = windowsDataRoot(current, { private: true });
  for (const directory of missing) {
    fs.mkdirSync(directory, { mode: 0o700 });
    checked = windowsDataRoot(directory, { private: true });
  }
  // The final snapshot already includes a canonical root and its full chain.
  // A different caller spelling still needs the original lexical policy (e.g.
  // drive-relative paths or dot components); never normalize away a refusal.
  return root === path.resolve(root) ? checked : windowsDataRoot(root, { private: true });
};

/** Only a fresh, confirmed pre-spawn failure can issue this receipt. It never
 * overrides a scratch fence, a PID-bearing status, or descendant custody. */
export const hasNeverStartedReceipt = (runDirectory: string): boolean => {
  try {
    for (const name of [RUN_TMP_DIRECTORY, UNRESOLVED_SCRATCH_FILE, JOINED_SCRATCH_FILE, "unresolved-worker.json", "status.json"]) {
      try { fs.lstatSync(path.join(runDirectory, name)); return false; }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return false; }
    }
    const file = path.join(runDirectory, NEVER_STARTED_FILE);
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > 4096) return false;
    const receipt = JSON.parse(fs.readFileSync(file, "utf8"));
    return receipt.version === 1 && receipt.workerNeverStarted === true && receipt.runDirectory === path.resolve(runDirectory);
  } catch { return false; }
};

const same = (file: string, stat: Pick<fs.Stats, "dev" | "ino"> | undefined): boolean => {
  const current = ownedStat(file);
  return !!stat && !!current && stat.dev === current.dev && stat.ino === current.ino;
};
interface ScratchFence {
  version: 2 | 3; runDirectory: string; scope?: ProcessScratchScope; launchNonce?: string;
  root?: { dev: number; ino: number }; scratch?: { dev: number; ino: number };
  allocatedAt?: number; allocatedUptime?: number; lastLaunchAt?: number; hostEpoch?: ScratchHostEpoch; closedPid?: number; closedAt?: number; retryPending?: boolean;
}
const validUnscopedFence = (value: ScratchFence): boolean => value.version === 3 && value.scope === undefined &&
  typeof value.allocatedAt === "number" && Number.isFinite(value.allocatedAt) && value.allocatedAt >= 0 &&
  typeof value.lastLaunchAt === "number" && Number.isFinite(value.lastLaunchAt) && value.lastLaunchAt >= value.allocatedAt &&
  typeof value.launchNonce === "string" && value.launchNonce.length > 0;

// Serialize allocation/refusal/disposal across processes. An abandoned or
// replaced lock is uncertainty, never a reason to steal custody or repair it.
const lockScratchCustody = (runDirectory: string): (() => void) => {
  if (!ownedStat(runDirectory)?.isDirectory()) throw new Error("Unsafe scratch custody root");
  if (process.platform === "win32") windowsDataRoot(runDirectory, { private: true });
  else posixDataRoot(runDirectory);
  const lock = path.join(runDirectory, ".scratch-custody-lock");
  fs.mkdirSync(lock, { mode: 0o700 });
  const identity = ownedStat(lock);
  return () => { if (same(lock, identity)) { try { fs.rmdirSync(lock); } catch { /* retain unknown custody */ } } };
};
const readFence = (runDirectory: string): ScratchFence | undefined => {
  try {
    const file = path.join(runDirectory, UNRESOLVED_SCRATCH_FILE);
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > 8192) return;
    const value = JSON.parse(fs.readFileSync(file, "utf8"));
    if (![2, 3].includes(value.version) || value.runDirectory !== path.resolve(runDirectory)) return;
    return value;
  } catch { return; }
};

/** Compatibility scope cut (#3076): unscoped terminal runs collect immediately,
 * as on main. Native close is generation-bound worker custody, NOT proof that
 * ordinary descendants exited. Age-gated holder retention is deferred to #4010.
 * Keep namespace identity, unsafe-content and unresolved-worker fences. */
const disposeUnscopedRunTmp = (runDirectory: string, receipt: ScratchFence, expired: () => boolean): boolean => {
  if (!validUnscopedFence(receipt) || expired()) return false;
  const statusFile = path.join(runDirectory, "status.json"), statusStat = ownedStat(statusFile);
  if (!statusStat?.isFile() || statusStat.size > 1024 * 1024) return false;
  const status = JSON.parse(fs.readFileSync(statusFile, "utf8"));
  if (status.transport !== "process" || !["completed", "failed", "stopped", "timed_out"].includes(status.status)) return false;
  const closed = Number.isSafeInteger(receipt.closedPid) && receipt.closedPid! > 0 &&
    typeof receipt.closedAt === "number" && Number.isFinite(receipt.closedAt) && receipt.closedAt >= receipt.lastLaunchAt!;
  // A fresh launch has no old status to confuse with this generation. Offline
  // collection uses main's persisted PID/birth proof even if its owner died
  // before recording native close. A retry must join its own captured close.
  if (receipt.retryPending && !closed) return false;
  const savedPid = typeof status.sessionId === "string" && /^\d+$/.test(status.sessionId) ? Number(status.sessionId) : undefined;
  if (status.sessionId !== undefined && (!Number.isSafeInteger(savedPid) || savedPid! <= 0)) return false;
  const pid = savedPid ?? (closed ? receipt.closedPid : undefined);
  if (!Number.isSafeInteger(pid) || pid! <= 0 || (closed && savedPid !== undefined && savedPid !== receipt.closedPid)) return false;
  try {
    process.kill(pid!, 0);
    const savedStart = typeof status.processStartTime === "string" && /^\d+$/.test(status.processStartTime) ? status.processStartTime : undefined;
    const currentStart = savedStart ? processStartTime(pid!) : undefined;
    if (currentStart === undefined || currentStart === savedStart) return false;
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") return false; }
  for (const name of ["unresolved-worker.json", JOINED_SCRATCH_FILE]) {
    try { fs.lstatSync(path.join(runDirectory, name)); return false; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return false; }
  }
  const directory = runTmpDirectory(runDirectory), fence = path.join(runDirectory, UNRESOLVED_SCRATCH_FILE), custody = ownedStat(fence);
  if (!safeRunTmpTree(directory, expired) || expired()) return false;
  if (!same(runDirectory, receipt.root) || !same(directory, receipt.scratch) || !same(fence, custody) || !same(statusFile, statusStat)) return false;
  fs.rmSync(directory, { recursive: true });
  fs.unlinkSync(fence);
  return true;
};

/** Scoped scratch requires the pinned kernel scope's atomic empty/removal
 * receipt. Unscoped terminal runs keep main's immediate collection policy;
 * their native-close receipt proves only the worker, not descendant exit.
 * Unknown launches and legacy scratch stay fenced.
 * Calling this later allows cleanup/retention after a background tool exits. */
/** Windows scratch disposal/custody is deliberately outside this PR's scope. */
let windowsScratchScopeCutLogged = false;
export const logWindowsScratchScopeCut = (): void => {
  if (windowsScratchScopeCutLogged) return;
  windowsScratchScopeCutLogged = true;
  console.warn("[pi-fabric] Scope cut: Smarty-Pants-Inc/smarty-dev#4010 (Windows scratch disposal and custody); no-op, scratch retained for Main's existing collection path");
};
export const disposeRunTmpDirectory = (runDirectory: string, expired: () => boolean = () => false): boolean => {
  let unlock: (() => void) | undefined;
  try {
    if (process.platform === "win32") { logWindowsScratchScopeCut(); return false; }
    if (expired()) return false;
    // No scratch custody means nothing to dispose. Do not create/remove a lock
    // merely to inspect an ordinary retained run: that mutates mtime/ctime and
    // can indefinitely restart residency's directory-based expiry clock.
    if (!readFence(runDirectory)) return false;
    unlock = lockScratchCustody(runDirectory);
    const receipt = readFence(runDirectory);
    const scope = checkedProcessScratchScope(receipt?.scope);
    const directory = runTmpDirectory(runDirectory);
    if (!same(runDirectory, receipt?.root) || !same(directory, receipt?.scratch)) return false;
    if (receipt?.version === 3) return disposeUnscopedRunTmp(runDirectory, receipt, expired);
    if (!scope) return false;
    posixDataRoot(runDirectory);
    const fence = path.join(runDirectory, UNRESOLVED_SCRATCH_FILE), custody = ownedStat(fence);
    const joinedFile = path.join(runDirectory, JOINED_SCRATCH_FILE), joinedStat = ownedStat(joinedFile);
    if (!joinedStat?.isFile() || joinedStat.size > 8192 || !receipt?.launchNonce) return false;
    const joined = JSON.parse(fs.readFileSync(joinedFile, "utf8"));
    if (joined.launchNonce !== receipt.launchNonce || joined.directory !== scope.directory ||
        joined.dev !== scope.dev || joined.ino !== scope.ino || joined.bootId !== scope.bootId) return false;
    if (!safeRunTmpTree(directory, expired) || expired()) return false;
    // Once rmdir succeeds the kernel cannot accept a new member. Never clear
    // custody on worker exit alone, an empty census, or a missing cgroup path.
    if (!removeEmptyProcessScratchScope(scope)) return false;
    if (!same(runDirectory, receipt?.root) || !same(directory, receipt?.scratch) || !same(fence, custody) || !same(joinedFile, joinedStat)) return false;
    fs.rmSync(directory, { recursive: true });
    fs.unlinkSync(joinedFile);
    fs.unlinkSync(fence);
    return true;
  } catch { return false; }
  finally { unlock?.(); }
};

export const runScratchExitVeto = (runDirectory: string, expired: () => boolean = () => false, disposeScratch = true): string | undefined => {
  // Scope cut: no Windows scratch custody sweep, including negative stats.
  // Main's existing worker/descendant and artifact-allowlist gates still apply.
  if (process.platform === "win32") { logWindowsScratchScopeCut(); return; }
  if (disposeScratch) disposeRunTmpDirectory(runDirectory, expired);
  for (const name of [UNRESOLVED_SCRATCH_FILE, RUN_TMP_DIRECTORY]) {
    try { fs.lstatSync(path.join(runDirectory, name)); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
      return "scratch writer exit is unconfirmed: custody inspection failed";
    }
    return "scratch writer exit is unconfirmed: no complete descendant scope exit receipt";
  }
};
export const runTmpDirectory = (runDirectory: string): string => path.resolve(runDirectory, RUN_TMP_DIRECTORY);

/** Runner-owned namespace, never the caller's TMPDIR. */
export const createRunTmpDirectory = (runDirectory: string): string => {
  if (!ownedStat(runDirectory)?.isDirectory()) throw new Error("Unsafe Fabric run directory for scratch");
  if (process.platform === "win32") windowsDataRoot(runDirectory, { private: true });
  else posixDataRoot(runDirectory);
  const directory = runTmpDirectory(runDirectory);
  try {
    fs.mkdirSync(directory, { mode: 0o700 });
    if (process.platform !== "win32") fs.chmodSync(directory, 0o700);
  }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  const stat = ownedStat(directory);
  if (!stat?.isDirectory() || (process.platform !== "win32" && (stat.mode & 0o777) !== 0o700)) {
    throw new Error("Unsafe Fabric run scratch directory (requires owner-only access)");
  }
  if (process.platform === "win32") windowsDataRoot(directory, { private: true });
  const fence = path.join(runDirectory, UNRESOLVED_SCRATCH_FILE);
  try {
    fs.lstatSync(fence);
    if (!ownedStat(fence)?.isFile()) throw new Error("Unsafe Fabric scratch custody fence");
    // A retry must preserve the first launch's scope or unresolved custody.
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    writeJsonAtomic(fence, { version: 1, reason: "uncontained process descendants", runDirectory: path.resolve(runDirectory) }, { durable: true });
  }
  return directory;
};

/** Pin a fresh allocation across runtime resolution. Only this instance may
 * dispose it on a checked failure before spawn; retries cannot erase old custody. */
export const allocateRunTmpDirectory = (runDirectory: string): { directory: string; scope?: ScopedScratchLaunch; neverStarted(): void; workerClosed(pid: number): void } => {
  const unlock = lockScratchCustody(runDirectory);
  try { return allocateRunTmpDirectoryLocked(runDirectory); }
  finally { unlock(); }
};
const allocateRunTmpDirectoryLocked = (runDirectory: string): { directory: string; scope?: ScopedScratchLaunch; neverStarted(): void; workerClosed(pid: number): void } => {
  const root = ownedStat(runDirectory);
  const fresh = [RUN_TMP_DIRECTORY, UNRESOLVED_SCRATCH_FILE, NEVER_STARTED_FILE, JOINED_SCRATCH_FILE, "status.json", "unresolved-worker.json", "nested"]
    .every(name => {
      try { fs.lstatSync(path.join(runDirectory, name)); return false; }
      catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT"; }
    });
  const directory = createRunTmpDirectory(runDirectory), scratch = ownedStat(directory);
  const fence = path.join(runDirectory, UNRESOLVED_SCRATCH_FILE);
  const prior = fresh ? undefined : readFence(runDirectory);
  if (!fresh && !prior) {
    // A corrupt/unknown prior receipt must not turn a scoped retry into an
    // uncontained launch that a later repaired receipt could accidentally erase.
    const legacy = JSON.parse(fs.readFileSync(fence, "utf8"));
    if (legacy?.version !== 1 || legacy.reason !== "uncontained process descendants" || legacy.runDirectory !== path.resolve(runDirectory)) {
      throw new Error("Unproved prior scratch custody; refusing launch");
    }
  }
  const scope = fresh ? createProcessScratchScope() : checkedProcessScratchScope(prior?.scope);
  if (prior && ((prior.version === 2 ? !scope : !validUnscopedFence(prior)) || !same(runDirectory, prior.root) || !same(directory, prior.scratch))) {
    throw new Error("Scratch scope identity changed; refusing an uncontained retry");
  }
  const launchNonce = randomUUID();
  if (scope) {
    try {
      writeJsonAtomic(fence, { version: 2, runDirectory: path.resolve(runDirectory), scope, launchNonce,
        root: { dev: root!.dev, ino: root!.ino }, scratch: { dev: scratch!.dev, ino: scratch!.ino } }, { durable: true });
    } catch (error) { if (fresh) removeEmptyProcessScratchScope(scope); throw error; }
  }
  if (!scope && (fresh || prior?.version === 3)) {
    // Unscoped custody is available on every supported host/filesystem. It
    // does not require D4's deferred host-epoch or potential-holder inventory.
    const now = Date.now();
    writeJsonAtomic(fence, { version: 3, reason: "uncontained process descendants", runDirectory: path.resolve(runDirectory),
      launchNonce, allocatedAt: prior?.allocatedAt ?? now, lastLaunchAt: now, retryPending: !fresh,
      root: { dev: root!.dev, ino: root!.ino }, scratch: { dev: scratch!.dev, ino: scratch!.ino } }, { durable: true });
  }
  const custody = ownedStat(fence);
  // Pin the previous usable joined generation. Only a confirmed pre-spawn
  // refusal may restore it; an ambiguous spawn must retain the newer fence.
  const joinedFile = path.join(runDirectory, JOINED_SCRATCH_FILE);
  const priorJoined = ownedStat(joinedFile);
  let restorePrior = false;
  if (prior && scope && priorJoined?.isFile() && priorJoined.size <= 8192) {
    try {
      const joined = JSON.parse(fs.readFileSync(joinedFile, "utf8"));
      restorePrior = !!prior.launchNonce && joined.launchNonce === prior.launchNonce &&
        joined.directory === scope.directory && joined.dev === scope.dev &&
        joined.ino === scope.ino && joined.bootId === scope.bootId;
    } catch { /* an unproved previous generation stays fenced */ }
  }
  return {
    directory, ...(scope ? { scope: { ...scope, launchNonce, joinedFile: path.join(path.resolve(runDirectory), JOINED_SCRATCH_FILE) } } : {}),
    workerClosed(pid: number) {
      // The transport calls this only after an owned native close without
      // lost contact. It identifies the generation, NOT descendant exit.
      // A pending/ambiguous retry has no such receipt and stays fenced.
      let unlock: (() => void) | undefined;
      try {
        if (!Number.isSafeInteger(pid) || pid <= 0) return;
        unlock = lockScratchCustody(runDirectory);
        const current = readFence(runDirectory);
        if (!current || current.launchNonce !== launchNonce ||
            !same(runDirectory, root) || !same(directory, scratch)) return;
        if (scope) {
          // A captured native close also joins a gate stopped before its JS
          // attachment receipt. No process from this launch can attach later;
          // descendants already attached still require the kernel's atomic
          // empty/removal proof in disposeRunTmpDirectory. Never infer emptiness
          // from close, nor arm a different/pending retry generation.
          const checked = checkedProcessScratchScope(current.scope);
          if (current.version !== 2 || !checked || checked.directory !== scope.directory ||
              checked.dev !== scope.dev || checked.ino !== scope.ino || checked.bootId !== scope.bootId) return;
          writeJsonAtomic(joinedFile, { ...scope, launchNonce }, { durable: true });
          return;
        }
        if (!validUnscopedFence(current)) return;
        if (current.closedPid === pid && typeof current.closedAt === "number" && current.closedAt >= current.lastLaunchAt!) return;
        writeJsonAtomic(fence, { ...current, closedPid: pid, closedAt: Date.now(), retryPending: false }, { durable: true });
      } catch { /* unproved native completion remains fenced */ }
      finally { unlock?.(); }
    },
    neverStarted() {
      if (process.platform === "win32") { logWindowsScratchScopeCut(); return; }
      let unlock: (() => void) | undefined;
      try {
        unlock = lockScratchCustody(runDirectory);
        if (!same(runDirectory, root) || !same(directory, scratch) || !same(fence, custody)) return;
        if (!fresh) {
          if ((restorePrior && same(joinedFile, priorJoined) && checkedProcessScratchScope(prior?.scope)) || (prior && validUnscopedFence(prior))) {
            writeJsonAtomic(fence, prior, { durable: true });
          }
          return;
        }
        for (const name of ["status.json", "unresolved-worker.json", JOINED_SCRATCH_FILE, "nested"]) {
          try { fs.lstatSync(path.join(runDirectory, name)); return; }
          catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return; }
        }
        if (!safeRunTmpTree(directory, () => false) || (scope && !removeEmptyProcessScratchScope(scope))) return;
        // Crashes during disposal stay fenced. The receipt is valid only after
        // both tmp and the unresolved custody fence have been removed.
        writeJsonAtomic(path.join(runDirectory, NEVER_STARTED_FILE), {
          version: 1, workerNeverStarted: true, runDirectory: path.resolve(runDirectory),
        }, { durable: true });
        fs.rmSync(directory, { recursive: true });
        fs.unlinkSync(fence);
      } catch { /* contention/identity changes never clear custody */ }
      finally { unlock?.(); }
    },
  };
};

/** Retention owns arbitrary scratch names, not arbitrary paths/foreign objects.
 * Pin the run filesystem, including at tmp itself, before entering directories.
 * A mount is not a symlink: owner/type checks alone do not authorize its deletion. */
export const safeRunTmpTree = (directory: string, expired: () => boolean, depth = 0): boolean => {
  if (expired() || depth > 32) return false;
  const device = ownedStat(path.dirname(directory))?.dev;
  return device !== undefined && safeRunTmpTreeOnDevice(directory, expired, depth, device);
};
const onScratchDevice = (file: string, stat: fs.Stats, device: number): boolean => {
  if (stat.dev === device) return true;
  console.warn(`[pi-fabric] Scratch retained: filesystem boundary at ${JSON.stringify(file)} (expected device ${device}, found ${stat.dev})`);
  return false;
};
const safeRunTmpTreeOnDevice = (directory: string, expired: () => boolean, depth: number, device: number): boolean => {
  if (expired() || depth > 32) return false;
  const root = ownedStat(directory);
  if (!root?.isDirectory() || !onScratchDevice(directory, root, device)) return false;
  try {
    for (const name of fs.readdirSync(directory)) {
      if (expired()) return false;
      const file = path.join(directory, name);
      const stat = ownedStat(file);
      if (!stat || !onScratchDevice(file, stat, device)) return false;
      if (stat.isFile()) continue;
      if (stat.isDirectory() && safeRunTmpTreeOnDevice(file, expired, depth + 1, device)) continue;
      return false;
    }
    return true;
  } catch { return false; }
};
