import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { windowsDataRoot } from "./windows-temp-root.js";
import { posixDataRoot } from "./temp-root.js";
import { ownedStat } from "./scratch.js";
import { checkedProcessScratchScope, createProcessScratchScope, removeEmptyProcessScratchScope, type ProcessScratchScope, type ScopedScratchLaunch } from "./process-scratch-scope.js";

export const RUN_TMP_DIRECTORY = "tmp";
export const UNRESOLVED_SCRATCH_FILE = "unresolved-scratch.json";
export const NEVER_STARTED_FILE = "never-started.json";
export const JOINED_SCRATCH_FILE = "scratch-scope-joined.json";

/** Validate the selected explicit/env/default root before the manager writes
 * even task.txt. Windows missing descendants inherit only a checked private
 * parent; POSIX uses the exact temp-root policy, including safe sticky ancestors. */
export const prepareRunRoot = (root: string): string => {
  if (process.platform !== "win32") return posixDataRoot(root, { create: true });
  const missing: string[] = [];
  let current = path.resolve(root);
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
  windowsDataRoot(current, { private: true });
  for (const directory of missing) {
    fs.mkdirSync(directory);
    windowsDataRoot(directory, { private: true });
  }
  return windowsDataRoot(root, { private: true });
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
const readFence = (runDirectory: string): { scope?: ProcessScratchScope; launchNonce?: string; root?: {dev: number; ino: number}; scratch?: {dev: number; ino: number} } | undefined => {
  try {
    const file = path.join(runDirectory, UNRESOLVED_SCRATCH_FILE);
    const stat = ownedStat(file);
    if (!stat?.isFile() || stat.size > 8192) return;
    const value = JSON.parse(fs.readFileSync(file, "utf8"));
    if (value.version !== 2 || value.runDirectory !== path.resolve(runDirectory)) return;
    return value;
  } catch { return; }
};

/** Remove scratch only with a pinned kernel scope's atomic empty/removal
 * receipt. Unknown launches, legacy scratch and unsupported hosts stay fenced.
 * Calling this later allows cleanup/retention after a background tool exits. */
export const disposeRunTmpDirectory = (runDirectory: string, expired: () => boolean = () => false): boolean => {
  try {
    if (expired()) return false;
    const receipt = readFence(runDirectory);
    const scope = checkedProcessScratchScope(receipt?.scope);
    const directory = runTmpDirectory(runDirectory);
    if (!scope || !same(runDirectory, receipt?.root) || !same(directory, receipt?.scratch)) return false;
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
};

export const runScratchExitVeto = (runDirectory: string, expired: () => boolean = () => false): string | undefined => {
  disposeRunTmpDirectory(runDirectory, expired);
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
export const allocateRunTmpDirectory = (runDirectory: string): { directory: string; scope?: ScopedScratchLaunch; neverStarted(): void } => {
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
  if (prior && (!scope || !same(runDirectory, prior.root) || !same(directory, prior.scratch))) {
    throw new Error("Scratch scope identity changed; refusing an uncontained retry");
  }
  const launchNonce = randomUUID();
  if (scope) {
    try {
      writeJsonAtomic(fence, { version: 2, runDirectory: path.resolve(runDirectory), scope, launchNonce,
        root: { dev: root!.dev, ino: root!.ino }, scratch: { dev: scratch!.dev, ino: scratch!.ino } }, { durable: true });
    } catch (error) { if (fresh) removeEmptyProcessScratchScope(scope); throw error; }
  }
  const custody = ownedStat(fence);
  return {
    directory, ...(scope ? { scope: { ...scope, launchNonce, joinedFile: path.join(path.resolve(runDirectory), JOINED_SCRATCH_FILE) } } : {}),
    neverStarted() {
      if (!fresh || !same(runDirectory, root) || !same(directory, scratch) || !same(fence, custody)) return;
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
    },
  };
};

/** Retention owns arbitrary scratch names, not arbitrary paths/foreign objects. */
export const safeRunTmpTree = (directory: string, expired: () => boolean, depth = 0): boolean => {
  if (expired() || depth > 32 || !ownedStat(directory)?.isDirectory()) return false;
  try {
    for (const name of fs.readdirSync(directory)) {
      if (expired()) return false;
      const file = path.join(directory, name);
      const stat = ownedStat(file);
      if (!stat) return false;
      if (stat.isFile()) continue;
      if (stat.isDirectory() && safeRunTmpTree(file, expired, depth + 1)) continue;
      return false;
    }
    return true;
  } catch { return false; }
};
