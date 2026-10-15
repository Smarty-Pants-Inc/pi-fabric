import { createHash, randomBytes, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { FOREIGN_LOCK, pinFenceLock, releaseFenceLock } from "./fence-lock.js";
import { verifiedRemove } from "../storage/verified-remove.js";
import { removeWindowsMeshDebris, retireWindowsMeshOwner } from "../storage/windows-mesh-recovery.js";
import type { WindowsMeshCapability } from "../storage/windows-mesh-release.js";
import {
  captureMeshLegacyDebris, judgeMeshLegacyDebris, meshRecoveryChildren, meshRecoverySucceeded,
  MeshLegacyDebrisUnrecoverableError, readMeshRecoveryOwner, requireMeshOwnerOnly,
} from "./legacy-debris.js";
import {
  failBeforeEntry, MeshLockReleaseError, prepareMeshLockRelease, runUnderMeshLocksAsync, windowsMeshReleaser, windowsPreEntryRefusal,
  type MeshLockOperationState, type MeshLockReleaseHandle, type MeshLockReleaser,
} from "./lock-release.js";
import {
  alarmForeignHolder, MeshLockForeignHolderError, MeshLockTimeoutError, ownProcessIncarnation, processIncarnation, validProcessIncarnation,
  readPhysicalHostIdentity, readPhysicalMachineId, validBootId, type StaleLockNote,
} from "../core/atomic-write.js";

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
  /** The mesh lock protocol; custody records follow it (protocol 3 names the holder's PID namespace). */
  readonly lockProtocol?: 1 | 2 | 3;
}

const errorCode = (error: unknown): string | undefined =>
  error instanceof Error && "code" in error && typeof error.code === "string" ? error.code : undefined;

const processAlive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; }
  catch (error) { return errorCode(error) !== "ESRCH"; }       // only ESRCH proves death
};

const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

export interface MeshCustodyLockOptions { hostQualified?: boolean; ownIncarnation?: string | undefined; lockProtocol?: 1 | 2 | 3 }

/**
 * Holds the custody lock of a mesh root until the returned release is called. The release is repeatable. On Windows a
 * release that did not resolve ownership throws MeshLockReleaseError (`operationState` defaults to "returned"; pass
 * "threw" from an error path). A caller with a primary error to preserve should prefer the shared finalizer
 * (withMeshCustody / lock-release.ts) over a bare `finally { release() }`.
 */
export const acquireMeshCustodyLock = async (root: string, timeoutMs = CUSTODY_LOCK_TIMEOUT_MS,
  options: MeshCustodyLockOptions = {}): Promise<(operationState?: MeshLockOperationState) => void> => {
  const handle = await acquireMeshCustodyHandle(root, timeoutMs, options);
  return (operationState: MeshLockOperationState = "returned") => {
    const failure = handle.release();
    if (failure) throw new MeshLockReleaseError(handle.lock, operationState, failure, handle.release);
  };
};

/** The custody lock as a finalizer handle (lock-release.ts); its release is repeatable on every route. */
export const acquireMeshCustodyHandle = async (root: string, timeoutMs = CUSTODY_LOCK_TIMEOUT_MS,
  options: MeshCustodyLockOptions = {}): Promise<MeshLockReleaseHandle> => {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
  const ownerPath = path.join(lock, "owner");
  const token = randomUUID();
  // Keep full UUID ownership receipts; only transient filesystem names need a short token.
  const pathToken = randomBytes(4).toString("hex");
  const started = Object.hasOwn(options, "ownIncarnation") ? options.ownIncarnation
    : await ownProcessIncarnation().catch(() => undefined);
  const physical = options.hostQualified ? readPhysicalHostIdentity() : undefined;
  const machineId = physical?.machineId ?? (options.hostQualified ? readPhysicalMachineId() : undefined);
  // Protocol 3 (smarty-dev#9569): the holder's PID namespace on one more line (plain: 6 fields, hostQualified: 8),
  // so readers from before protocol 3, which accept only 4/5 or 7 fields, never reclaim it.
  // The namespace reader is a lazy module (startup graph budget): loaded only for a protocol-3 record.
  const namespace = options.lockProtocol === 3
    ? await import("../core/pid-namespace.js").then(module => module.ownPidNamespace()).catch(() => undefined) : undefined;
  const nsLine = options.lockProtocol === 3 ? `${namespace ?? ""}\n` : "";
  const record = options.hostQualified
    ? `${token}\n${process.pid}\n${Date.now()}\n${started ?? ""}\n${machineId ?? ""}\n${physical?.bootId ?? ""}\n${nsLine}`
    : options.lockProtocol === 3
      ? `${token}\n${process.pid}\n${Date.now()}\n${started ?? ""}\n${nsLine}`
      : `${token}\n${process.pid}\n${Date.now()}\n${started ? `${started}\n` : ""}`;
  const readOwner = (): string | undefined => {
    try { return fs.readFileSync(ownerPath, "utf8"); }
    catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
  };
  const blocked: StaleLockNote = {};
  // smarty-dev#9569 A: Windows materializes the retained native capability BEFORE any owner publication (throws: none).
  const capability = prepareMeshLockRelease();
  const windowsRelease = capability ? windowsMeshReleaser(lock, record, capability) : undefined;
  // The relative wait budget starts at the first acquisition attempt, after setup (incarnation and PID-namespace
  // reads): slow setup must not spend it before the lock was tried.
  const deadline = Date.now() + Math.max(0, timeoutMs);
  let attempts = 0;
  let maxGapMs = 0;
  let lastAttemptAt = Date.now();
  for (;;) {
    const now = Date.now();
    if (attempts > 0) maxGapMs = Math.max(maxGapMs, now - lastAttemptAt);
    attempts += 1;
    lastAttemptAt = now;
    // Publish a complete owner by rename: the canonical name is never ownerless.
    const staging = fs.mkdtempSync(`${lock}.p.${pathToken}.`);
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
      // ponytail: no judgment precedes this. `staging` is this attempt's own unpublished mkdtemp name (random suffix,
      // never published under it, never a recovery or rename target); after a successful publish the name is empty
      // and `force` makes this a no-op. smarty-dev#8907 inventory.
      fs.rmSync(staging, { recursive: true, force: true });
    }
    try {
      if (await clearDeadCustodyLock(lock, readOwner, deadline, started, options.hostQualified, blocked, options.lockProtocol ?? 1, capability)) continue;
    } catch (error) {
      if (error instanceof MeshCustodyUnrecoverableError) {
        try { console.warn(`[pi-fabric] ALARM ${error.kind}: ${error.message}`); } catch { /* alarm never masks refusal */ }
      }
      throw error;
    }
    if (Date.now() >= deadline) {
      const owner = (() => { try { return readOwner(); } catch { return undefined; } })();
      const pid = owner?.split("\n")[1];
      const holder = ` (custody lock ${lock}${pid ? `, held by pid ${pid}` : ""})`;
      if (blocked.verdict) {
        const refused = new MeshLockForeignHolderError(holder, attempts, maxGapMs, blocked.verdict, blocked.namespace ?? "", lock, blocked.token ?? "");
        alarmForeignHolder(refused);
        throw refused;
      }
      throw new MeshLockTimeoutError(holder, attempts, maxGapMs);
    }
    await delay(Math.min(Math.max(0, deadline - Date.now()), 5 + Math.floor(Math.random() * 20)));
  }
  // smarty-dev#8777: release through the directory pinned now (the publication fence's fd-pinned release),
  // never a check-then-rename by name that a reclaimed, stalled holder could run against its successor.
  // Main's release by name. Non-Linux keeps it; the stalled-holder race there is tracked in smarty-dev#9569.
  const releaseByName: MeshLockReleaser = () => {
    // Windows dispatches first: never the by-name branch or its rename fallback below.
    if (windowsRelease) return windowsRelease();
    try {
      // smarty-dev#8907: the stat captured before the owner judgment; only that inode is removed (detached into
      // verifiedRemove's graveyard first). A swapped-in successor is refused, never removed.
      const judged = fs.lstatSync(lock);
      if (readOwner() !== record) return;
      verifiedRemove(lock, judged, { deleter: "meshCustodyLockRelease", fallback: () => {
        // Off Linux, main's release. Detach before removal: never recursively delete through the canonical name.
        const released = `${lock}.released.${pathToken}`;
        fs.renameSync(lock, released);
        fs.rmSync(released, { recursive: true, force: true });
        return true;
      } });
    } catch {
      // Already recovered or replaced: never delete a successor.
    }
    return undefined;
  };
  const byName: MeshLockReleaseHandle = { lock, release: releaseByName };
  const pin = pinFenceLock(lock, record);
  // ponytail: fence-lock.ts stays main's code, so its verdicts are split here. On Linux the pin must hold:
  // FOREIGN_LOCK also comes from a failed open (EMFILE, EACCES...) and undefined from an unreadable
  // /proc/self/fd/<fd>/owner. Still OUR record: we hold the lock we just published (no successor can acquire
  // while it is there), so release it by name and fail retryably; never hand out an unpinned custody on Linux.
  // Not ours: reclaimed before entry, so release nothing.
  if (pin === FOREIGN_LOCK || (pin === undefined && process.platform === "linux")) {
    let ours = false;
    try { ours = readOwner() === record; } catch { /* unreadable: not provably ours */ }
    if (ours) releaseByName();
    throw new MeshLockTimeoutError(` (custody lock ${lock} could not be pinned or was reclaimed before entry)`, attempts, maxGapMs);
  }
  // Windows pre-entry: exactly {owner} holding our original record, or refuse before the operation.
  if (windowsRelease) {
    const refusal = windowsPreEntryRefusal(lock, record);
    if (refusal) failBeforeEntry(refusal, [byName]);
    return byName;
  }
  // The pinned release closes its fd, so it runs once; main's release may be called again after a failure.
  let done = false;
  return {
    lock,
    release: () => {
      if (pin !== undefined) {
        if (done) return undefined;
        done = true;
        releaseFenceLock(lock, record, {}, pin);
        return undefined;
      }
      return releaseByName();
    },
  };
};

// A complete receipt of a dead pid, or of a live pid with another incarnation, is recovered at
// once by renaming it to a retained, identity-named fence (as the mesh lock does): a paused
// second recoverer cannot rename a successor onto the same nonempty fence.
const clearDeadCustodyLock = async (lock: string, readOwner: () => string | undefined, deadline: number,
  ownStart: string | undefined, hostQualified = false, note: StaleLockNote = {}, lockProtocol: 1 | 2 | 3 = 1,
  capability?: WindowsMeshCapability): Promise<boolean> => {
  delete note.verdict; delete note.namespace; delete note.token;
  const pins: { unchanged(): boolean; close(): void }[] = [];
  try {
    // Capture exact initial bigint S and full R BEFORE any awaited death/namespace evidence.
    const stat = fs.lstatSync(lock, { bigint: true });
    if (capability && (stat.isSymbolicLink() || !stat.isDirectory())) throw new MeshLegacyDebrisUnrecoverableError(lock, lock, "link-or-nondirectory");
    if (!stat.isDirectory()) return false;
    let ownerStat: fs.BigIntStats | undefined;
    let owner: string | undefined;
    if (capability) {
      const captured = readMeshRecoveryOwner(lock);
      ownerStat = captured?.stat; owner = captured?.record;
    } else {
      try { ownerStat = fs.lstatSync(path.join(lock, "owner"), { bigint: true }); }
      catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
      owner = readOwner();
    }
    if (owner === undefined) {
      if (capability && meshRecoveryChildren(lock).length > 0) {
        const entries = captureMeshLegacyDebris(lock, stat);
        await judgeMeshLegacyDebris(lock, entries, entry => {
          const parent = entries.find(candidate => candidate.path === path.dirname(entry.path))!;
          return deadCustodyOwnerCategory(lock, entry.record!, parent.stat, entry.stat, deadline,
            ownStart, hostQualified, note, lockProtocol, pins);
        });
        if (pins.some(pin => !pin.unchanged())) throw new MeshLegacyDebrisUnrecoverableError(lock, lock, "unproven-root");
        const remaining = deadline - Date.now();
        if (remaining <= 0) return false;
        return meshRecoverySucceeded(lock, removeWindowsMeshDebris(lock, entries, capability, Math.min(4000, remaining)));
      }
      // E precedes the host-qualified missing-pid alarm. A torn file NEVER takes this branch.
      if (BigInt(Date.now()) * 1_000_000n - stat.mtimeNs <= BigInt(CUSTODY_LOCK_STALE_MS) * 1_000_000n) return false;
      const current = fs.lstatSync(lock, { bigint: true });
      if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino ||
        current.mtimeNs !== stat.mtimeNs || readOwner() !== undefined) return false;
      fs.rmdirSync(lock); // atomic empty-only; late owner/unknown content survives
      return true;
    }
    if (capability) requireMeshOwnerOnly(lock);
    const pidText = owner.split("\n")[1];
    const pid = Number(pidText);
    if (hostQualified && (!pidText?.trim() || !Number.isSafeInteger(pid) || pid <= 0)) {
      const current = fs.lstatSync(lock, { bigint: true });
      if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino || readOwner() !== owner) return false;
      throw new MeshCustodyUnrecoverableError(lock);
    }
    if (!ownerStat || await deadCustodyOwnerCategory(lock, owner, stat, ownerStat, deadline,
      ownStart, hostQualified, note, lockProtocol, pins)) return false;
    const current = fs.lstatSync(lock, { bigint: true });
    if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino || readOwner() !== owner) return false;
    if (pins.some(pin => !pin.unchanged())) {
      const fields = owner.split("\n");
      note.verdict = "unproven";
      note.namespace = hostQualified ? (fields.length === 8 ? fields[6]! : "") : (fields.length === 6 ? fields[4]! : "");
      note.token = fields[0]!;
      return false;
    }
    if (capability) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) return false;
      return meshRecoverySucceeded(lock, retireWindowsMeshOwner(lock, owner, stat, ownerStat, capability, Math.min(4000, remaining)));
    }
    // Retain the legacy Number-Stats F NAME so a paused old recoverer cannot
    // rename a successor into an absent alternate F. All identity/death authority stays bigint.
    fs.renameSync(lock, `${lock}.dead.${createHash("sha256").update(`${Number(stat.dev)}:${Number(stat.ino)}:${owner}`).digest("hex")}`);
    return true;
  } catch (error) {
    if (error instanceof MeshCustodyUnrecoverableError || error instanceof MeshLegacyDebrisUnrecoverableError) throw error;
    return false;
  } finally { for (const pin of pins) pin.close(); }
};

/** C and D share this caller's EXISTING host/namespace/incarnation policy, independently for every full record. */
const deadCustodyOwnerCategory = async (lock: string, owner: string, stat: fs.BigIntStats, ownerStat: fs.BigIntStats,
  deadline: number, ownStart: string | undefined, hostQualified: boolean, note: StaleLockNote, lockProtocol: 1 | 2 | 3,
  pins: { unchanged(): boolean; close(): void }[]): Promise<string | undefined> => {
  const fields = owner.split("\n");
  const [token, pidText, createdText, recordedStart] = fields;
  const pid = Number(pidText);
  if (!owner.endsWith("\n")) return "torn-record";
  const physical = hostQualified ? readPhysicalHostIdentity() : undefined;
  const machineId = physical?.machineId ?? (hostQualified ? readPhysicalMachineId() : undefined);
  if (hostQualified && (!machineId || (fields.length !== 7 && fields.length !== 8) || fields[4] !== machineId)) return "foreign-host-or-unproven";
  const bootKnown = hostQualified && physical !== undefined && validBootId(fields[5]);
  let previousBoot = bootKnown && fields[5] !== physical!.bootId;
  if (!token || !Number.isSafeInteger(pid) || pid <= 0 || !Number.isFinite(Number(createdText)) ||
    (hostQualified ? fields.length !== 7 && fields.length !== 8 : fields.length !== 4 && fields.length !== 5 && fields.length !== 6)) return "torn-record";
  const recordNamespace = hostQualified ? (fields.length === 8 ? fields[6]! : null) : (fields.length === 6 ? fields[4]! : null);
  if (lockProtocol === 3) previousBoot = false; // NEVER infer a previous boot under protocol 3
  if (recordNamespace !== null || lockProtocol === 3) {
    const namespaces = await import("../core/pid-namespace.js");
    const result = recordNamespace === null ? { verdict: "unproven" as const }
      : namespaces.reclaimVerdict(recordNamespace, await namespaces.ownNamespaceForReclaim(deadline, false), path.dirname(lock));
    if ("pin" in result && result.pin) pins.push(result.pin);
    if (result.verdict !== "pid") {
      note.verdict = result.verdict; note.namespace = recordNamespace ?? ""; note.token = token!;
      return result.verdict;
    }
  }
  if (hostQualified && !bootKnown && !previousBoot) {
    // Unknown boot: retain the existing old-metadata + ESRCH-only policy; no incarnation/reboot inference.
    const latest = stat.mtimeNs > ownerStat.mtimeNs ? stat.mtimeNs : ownerStat.mtimeNs;
    if (!ownerStat.isFile() || BigInt(Date.now()) * 1_000_000n - latest <= BigInt(CUSTODY_LOCK_STALE_MS) * 1_000_000n) return "unproven";
    if (processAlive(pid)) return "live-owner";
  } else if (!previousBoot && processAlive(pid)) {
    if (!validProcessIncarnation(recordedStart)) return "live-owner";
    const remaining = deadline - Date.now();
    const actual = pid === process.pid ? ownStart
      : remaining > 0 ? await processIncarnation(pid, remaining).catch(() => undefined) : undefined;
    if (!actual) return "unproven";
    if (actual === recordedStart) return "live-owner";
  }
  return undefined;
};

/**
 * Runs a synchronous file-custody operation under the custody lock and, in "dual" mode, also
 * under the mesh lock (custody first). `timeoutMs` bounds each acquisition; an active
 * withTryLock budget bounds the custody acquisition too.
 */
export const withMeshCustody = async <T>(mesh: MeshCustodyStore, operation: () => T, timeoutMs?: number): Promise<T> => {
  const budget = Math.min(timeoutMs ?? CUSTODY_LOCK_TIMEOUT_MS, mesh.tryLockBudgetMs ?? Number.POSITIVE_INFINITY);
  const custody = await acquireMeshCustodyHandle(mesh.root, budget, { lockProtocol: mesh.lockProtocol ?? 1 });
  // Dual mode: the mesh lock finalizes its own release inside exclusive(); custody is finalized here by the same
  // utility, so both are released and a primary error (or the inner release error) is never masked.
  let entered = false;
  const section = (): T => { entered = true; return operation(); };
  return runUnderMeshLocksAsync(() => meshCustodyMode() === "own" ? section()
    : timeoutMs === undefined ? mesh.exclusive(section) : mesh.exclusive(section, timeoutMs), [custody],
  { started: () => entered });
};
