import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { MeshLockTimeoutError, ownProcessIncarnation, processIncarnation, validProcessIncarnation, readPhysicalHostIdentity, readPhysicalMachineId, validBootId } from "../core/atomic-write.js";

/**
 * File custody lock (smarty-dev#6477 L5). `exclusive()` users that only guard files beside the
 * mesh (Main inbox owner/successor/route files, participant key-lock recovery and its sweep) take
 * `<mesh>/custody.lock` instead of the shared `<mesh>/.lock`, so their durable writes no longer
 * queue mesh publishes and state writes.
 *
 * Mixed-release safety: releases up to 04930dfd guard the same files with the mesh lock only.
 * The default "dual" mode therefore takes BOTH locks, always in the fixed order custody, then
 * mesh: it excludes old processes (mesh) and own-mode processes (custody). Nothing waits for
 * custody while holding the mesh lock (mesh critical sections are synchronous), so the order
 * cannot invert. Registry fences come before both (adoption keeps registry -> mesh).
 * `PI_FABRIC_MESH_CUSTODY_LOCK=own` drops the mesh lock from custody once no pre-L5 process
 * writes this mesh root; set it back to "dual" (or unset) before running a pre-L5 release again.
 */
export const MESH_CUSTODY_LOCK_NAME = "custody.lock";
const CUSTODY_LOCK_TIMEOUT_MS = 10_000;
// Same conservative 30s staleness bound as mesh/exclusive locks; age alone NEVER proves death.
export const CUSTODY_LOCK_STALE_MS = 30_000;

/** Not a contention timeout: callers must surface this refusal, not silently retry/skip it. */
export class MeshCustodyUnrecoverableError extends Error {
  override readonly name = "MeshCustodyUnrecoverableError";
  readonly code = "FABRIC_MESH_CUSTODY_UNRECOVERABLE";
  readonly kind = "custody-unrecoverable";
  readonly retryable = false;
  constructor(readonly lock: string) {
    super(`Fabric custody-unrecoverable: ${lock} has no valid owner pid; recovery refused`);
  }
}

export type MeshCustodyMode = "dual" | "own";

/** Anything but an explicit "own" keeps the transition-safe dual lock. */
export const meshCustodyMode = (value = process.env.PI_FABRIC_MESH_CUSTODY_LOCK): MeshCustodyMode =>
  value?.trim().toLowerCase() === "own" ? "own" : "dual";

export interface MeshCustodyStore {
  readonly root: string;
  exclusive<T>(operation: () => T, lockTimeoutMs?: number): Promise<T>;
  /** The active MeshStore.withTryLock budget, if any: custody honours it like the mesh lock. */
  readonly tryLockBudgetMs?: number | undefined;
}

const errorCode = (error: unknown): string | undefined =>
  error instanceof Error && "code" in error && typeof error.code === "string" ? error.code : undefined;

const processAlive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; }
  catch (error) { return errorCode(error) !== "ESRCH"; }       // only ESRCH proves death
};

const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

/** Holds the custody lock of a mesh root until the returned release is called. */
export const acquireMeshCustodyLock = async (root: string, timeoutMs = CUSTODY_LOCK_TIMEOUT_MS,
  options: { hostQualified?: boolean; ownIncarnation?: string | undefined } = {}): Promise<() => void> => {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
  const ownerPath = path.join(lock, "owner");
  const deadline = Date.now() + Math.max(0, timeoutMs);
  const token = randomUUID();
  const started = Object.hasOwn(options, "ownIncarnation") ? options.ownIncarnation
    : await ownProcessIncarnation().catch(() => undefined);
  const physical = options.hostQualified ? readPhysicalHostIdentity() : undefined;
  const machineId = physical?.machineId ?? (options.hostQualified ? readPhysicalMachineId() : undefined);
  const record = options.hostQualified
    ? `${token}\n${process.pid}\n${Date.now()}\n${started ?? ""}\n${machineId ?? ""}\n${physical?.bootId ?? ""}\n`
    : `${token}\n${process.pid}\n${Date.now()}\n${started ? `${started}\n` : ""}`;
  const readOwner = (): string | undefined => {
    try { return fs.readFileSync(ownerPath, "utf8"); }
    catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
  };
  let attempts = 0;
  let maxGapMs = 0;
  let lastAttemptAt = Date.now();
  for (;;) {
    const now = Date.now();
    if (attempts > 0) maxGapMs = Math.max(maxGapMs, now - lastAttemptAt);
    attempts += 1;
    lastAttemptAt = now;
    // Publish a complete owner by rename: the canonical name is never ownerless.
    const staging = fs.mkdtempSync(`${lock}.pending.${token}.`);
    try {
      fs.writeFileSync(path.join(staging, "owner"), record, { encoding: "utf8", flag: "wx", mode: 0o600 });
      try {
        fs.lstatSync(lock);
        throw Object.assign(new Error("Fabric mesh custody lock already exists"), { code: "EEXIST" });
      } catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
      fs.renameSync(staging, lock);
      break;
    } catch (error) {
      const code = errorCode(error);
      if (code !== "EEXIST" && code !== "ENOTEMPTY" && code !== "EPERM" && code !== "EACCES") throw error;
    } finally {
      fs.rmSync(staging, { recursive: true, force: true });
    }
    try {
      if (await clearDeadCustodyLock(lock, readOwner, deadline, started, options.hostQualified)) continue;
    } catch (error) {
      if (error instanceof MeshCustodyUnrecoverableError) {
        try { console.warn(`[pi-fabric] ALARM ${error.kind}: ${error.message}`); } catch { /* alarm never masks refusal */ }
      }
      throw error;
    }
    if (Date.now() >= deadline) {
      const owner = (() => { try { return readOwner(); } catch { return undefined; } })();
      const pid = owner?.split("\n")[1];
      throw new MeshLockTimeoutError(` (custody lock ${lock}${pid ? `, held by pid ${pid}` : ""})`, attempts, maxGapMs);
    }
    await delay(Math.min(Math.max(0, deadline - Date.now()), 5 + Math.floor(Math.random() * 20)));
  }
  return () => {
    try {
      if (readOwner() !== record) return;
      // Detach before removal: never recursively delete through the canonical name.
      const released = `${lock}.released.${token}`;
      fs.renameSync(lock, released);
      fs.rmSync(released, { recursive: true, force: true });
    } catch {
      // Already recovered or replaced: never delete a successor.
    }
  };
};

// A complete receipt of a dead pid, or of a live pid with another incarnation, is recovered at
// once by renaming it to a retained, identity-named fence (as the mesh lock does): a paused
// second recoverer cannot rename a successor onto the same nonempty fence.
const clearDeadCustodyLock = async (lock: string, readOwner: () => string | undefined, deadline: number,
  ownStart: string | undefined, hostQualified = false): Promise<boolean> => {
  try {
    const stat = fs.lstatSync(lock);
    if (!stat.isDirectory()) return false;
    const owner = readOwner();
    const fields = owner?.split("\n") ?? [];
    const [token, pidText, createdText, recordedStart] = fields;
    const pid = Number(pidText);
    if (hostQualified && (!pidText?.trim() || !Number.isSafeInteger(pid) || pid <= 0)) {
      // A concurrent release/replacement is not a corrupt receipt. Alarm only the same
      // canonical lock we actually observed, just as recovery fences the same inode/bytes.
      const current = fs.lstatSync(lock);
      if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino || readOwner() !== owner) return false;
      throw new MeshCustodyUnrecoverableError(lock);
    }
    if (owner === undefined || !owner.endsWith("\n")) return false;    // never ownerless by protocol
    // A host-lease commit gate is never recovered by a foreign/unknown PID. Matching
    // machine evidence is still mandatory when boot/process identity is unavailable.
    const physical = hostQualified ? readPhysicalHostIdentity() : undefined;
    const machineId = physical?.machineId ?? (hostQualified ? readPhysicalMachineId() : undefined);
    if (hostQualified && (!machineId || fields.length !== 7 || fields[4] !== machineId)) return false;
    const bootKnown = hostQualified && physical !== undefined && validBootId(fields[5]);
    const previousBoot = bootKnown && fields[5] !== physical!.bootId;
    if (!token || !Number.isSafeInteger(pid) || pid <= 0 || !Number.isFinite(Number(createdText)) ||
      (fields.length !== 4 && fields.length !== 5 && !(hostQualified && fields.length === 7))) return false;
    if (hostQualified && !bootKnown) {
      // ESRCH on THIS machine plus old namespace/owner metadata, never expiry of a LIVE
      // holder. Do not compare incarnations or infer a reboot from missing evidence.
      const ownerStat = fs.lstatSync(path.join(lock, "owner"));
      if (!ownerStat.isFile() || Date.now() - Math.max(stat.mtimeMs, ownerStat.mtimeMs) <= CUSTODY_LOCK_STALE_MS ||
        processAlive(pid)) return false;
    } else if (!previousBoot && processAlive(pid)) {
      if (!validProcessIncarnation(recordedStart)) return false;
      // Our own pid with another incarnation is a dead predecessor; with ours, a live caller.
      const remaining = deadline - Date.now();
      const actual = pid === process.pid ? ownStart
        : remaining > 0 ? await processIncarnation(pid, remaining).catch(() => undefined) : undefined;
      if (!actual || actual === recordedStart) return false;
    }
    const current = fs.lstatSync(lock);
    if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino || readOwner() !== owner) return false;
    fs.renameSync(lock, `${lock}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:${owner}`).digest("hex")}`);
    return true;
  } catch (error) {
    if (error instanceof MeshCustodyUnrecoverableError) throw error;
    return false;
  }
};

/**
 * Runs a synchronous file-custody operation under the custody lock and, in "dual" mode, also
 * under the mesh lock (custody first). `timeoutMs` bounds each acquisition; an active
 * withTryLock budget bounds the custody acquisition too.
 */
export const withMeshCustody = async <T>(mesh: MeshCustodyStore, operation: () => T, timeoutMs?: number): Promise<T> => {
  const budget = Math.min(timeoutMs ?? CUSTODY_LOCK_TIMEOUT_MS, mesh.tryLockBudgetMs ?? Number.POSITIVE_INFINITY);
  const release = await acquireMeshCustodyLock(mesh.root, budget);
  try {
    if (meshCustodyMode() === "own") return operation();
    return await (timeoutMs === undefined ? mesh.exclusive(operation) : mesh.exclusive(operation, timeoutMs));
  } finally {
    release();
  }
};
