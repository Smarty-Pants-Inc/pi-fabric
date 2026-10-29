import { createHash, randomBytes, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { FOREIGN_LOCK, pinFenceLock, releaseFenceLock } from "./fence-lock.js";
import { verifiedRemove } from "../storage/verified-remove.js";
import {
  alarmForeignHolder, MeshLockForeignHolderError, MeshLockTimeoutError, ownProcessIncarnation, processIncarnation, validProcessIncarnation,
  readPhysicalHostIdentity, readPhysicalMachineId, renderRecoveryCommand, validBootId, type StaleLockNote,
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
// Same conservative 30s staleness bound used for dead-owner checks; it is not used to reclaim
// ownerless directories because no observation can prove an uninterrupted ownerless interval.
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

const CUSTODY_RELEASE_GUARD_PREFIX = `.${MESH_CUSTODY_LOCK_NAME}.releasing.`;
const custodyAlarmedLocks = new Set<string>();

/** A sibling receipt lets the operator distinguish a crashed release from a live holder paused after owner unlink. */
const createCustodyReleaseGuard = (lock: string, record: string, token: string): { file: string; directory: fs.Stats } | undefined => {
  let fd: number | undefined;
  const file = path.join(path.dirname(lock), `${CUSTODY_RELEASE_GUARD_PREFIX}${token}`);
  try {
    const directory = fs.lstatSync(lock);
    if (!directory.isDirectory() || fs.readFileSync(path.join(lock, "owner"), "utf8") !== record) return undefined;
    let namespace: string | null = null;
    if (process.platform === "linux") {
      namespace = fs.readlinkSync("/proc/self/ns/pid");
      if (!/^pid:\[\d+\]$/.test(namespace)) return undefined;
    }
    const receipt = `${JSON.stringify({ format: 1, pid: process.pid, namespace, dev: String(directory.dev), ino: String(directory.ino), token })}\n`;
    fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    const opened = fs.fstatSync(fd);
    if (!opened.isFile() || opened.nlink !== 1) throw new Error("release receipt is not a private regular file");
    fs.fchmodSync(fd, 0o600);
    fs.writeSync(fd, receipt);
    fs.fsyncSync(fd);
    fs.closeSync(fd); fd = undefined;
    return { file, directory };
  } catch {
    if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* best effort */ } }
    try { fs.unlinkSync(file); } catch { /* no partial receipt is trusted */ }
    return undefined;
  }
};

const removeCustodyReleaseGuard = (guard: { file: string }): void => {
  try { fs.unlinkSync(guard.file); } catch { /* a stale receipt is harmless; owner-bearing locks still refuse */ }
};

const processAlive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; }
  catch (error) { return errorCode(error) !== "ESRCH"; }       // only ESRCH proves death
};

const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));

/**
 * pi-fabric#1562 (review/astra P1): where there is no machine id (/etc/machine-id: Windows, macOS, a Linux without it), a
 * host-qualified record names its host in the machine slot instead: `host:` plus the URI-encoded, lower-cased
 * `os.hostname()`. It can never equal a machine id (32 hex digits), so a reader that has one, and every reader from before
 * this label, keeps such a gate: its own machine id differs, or it has none and refuses (fail closed).
 */
const HOST_LABEL_PREFIX = "host:";
const ownHostLabel = (): string | undefined => {
  try {
    const name = os.hostname().trim().toLowerCase();
    return name ? `${HOST_LABEL_PREFIX}${encodeURIComponent(name)}` : undefined;
  } catch { return undefined; }
};

/** Holds the custody lock of a mesh root until the returned release is called. */
export const acquireMeshCustodyLock = async (root: string, timeoutMs = CUSTODY_LOCK_TIMEOUT_MS,
  options: { hostQualified?: boolean; ownIncarnation?: string | undefined; lockProtocol?: 1 | 2 | 3 } = {}): Promise<() => void> => {
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
  // Without a machine id, the host label (ownHostLabel): the proof a later process on this host needs to recover the gate after
  // this one dies (clearDeadCustodyLock), on ESRCH or, with the incarnation above, a reused PID. Without a valid incarnation
  // (none passed, or the native read failed) only ESRCH recovers it. Linux with a machine id writes what it always did.
  const machineSlot = machineId ?? (options.hostQualified ? ownHostLabel() : undefined);
  // Protocol 3 (smarty-dev#9569): the holder's PID namespace on one more line (plain: 6 fields, hostQualified: 8),
  // so readers from before protocol 3, which accept only 4/5 or 7 fields, never reclaim it.
  // The namespace reader is a lazy module (startup graph budget): loaded only for a protocol-3 record.
  const namespace = options.lockProtocol === 3
    ? await import("../core/pid-namespace.js").then(module => module.ownPidNamespace()).catch(() => undefined) : undefined;
  const nsLine = options.lockProtocol === 3 ? `${namespace ?? ""}\n` : "";
  const record = options.hostQualified
    ? `${token}\n${process.pid}\n${Date.now()}\n${started ?? ""}\n${machineSlot ?? ""}\n${physical?.bootId ?? ""}\n${nsLine}`
    : options.lockProtocol === 3
      ? `${token}\n${process.pid}\n${Date.now()}\n${started ?? ""}\n${nsLine}`
      : `${token}\n${process.pid}\n${Date.now()}\n${started ? `${started}\n` : ""}`;
  const readOwner = (): string | undefined => {
    try { return fs.readFileSync(ownerPath, "utf8"); }
    catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
  };
  const blocked: StaleLockNote = {};
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
      if (await clearDeadCustodyLock(lock, readOwner, deadline, started, options.hostQualified, blocked, options.lockProtocol ?? 1)) continue;
    } catch (error) {
      if (error instanceof MeshCustodyUnrecoverableError) {
        if (!custodyAlarmedLocks.has(error.lock)) {
          custodyAlarmedLocks.add(error.lock);
          const recovery = renderRecoveryCommand(["fabric-mesh-backend", "lock-recover", "--root", path.dirname(error.lock), "--lock", "custody", "--ownerless"]);
          const alarm = `[pi-fabric] ALARM ${error.code}: ${error.message}; recover a confirmed dead release with: ${recovery}\n`;
          try { process.stderr.write(alarm); } catch { /* alarm never masks refusal */ }
        }
      }
      throw error;
    }
    if (Date.now() >= deadline) {
      const owner = (() => { try { return readOwner(); } catch { return undefined; } })();
      const pid = Number(owner?.split("\n")[1]);
      const holder = ` (custody lock ${lock}, ${Number.isSafeInteger(pid) && pid > 0 ? `held by pid ${pid}` : "owner pid unreadable"})`;
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
  const releaseByName = (): void => {
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
  };
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
  // A sibling receipt is written before unlinking owner. If a process dies or pauses in the unlink/rmdir seam,
  // ownerless recovery can prove whether that exact releaser is still alive in this PID namespace.
  let done = false;
  return () => {
    if (pin !== undefined) {
      if (done) return;
      const guard = createCustodyReleaseGuard(lock, record, pathToken);
      if (!guard) return; // Fail closed: leave our owner receipt in place if release cannot be witnessed.
      done = true;
      let result: ReturnType<typeof releaseFenceLock> | undefined;
      try { result = releaseFenceLock(lock, record, {}, pin); }
      catch { /* a release callback must never leak an unhandled filesystem error */ }
      let removeGuard = result?.released === true;
      try {
        const current = fs.lstatSync(lock, { throwIfNoEntry: false });
        if (!current?.isDirectory() || current.dev !== guard.directory.dev || current.ino !== guard.directory.ino) removeGuard = true;
        else {
          const owner = readOwner();
          const entries = fs.readdirSync(lock);
          removeGuard = owner !== undefined || entries.length !== 0;
        }
      } catch { /* an ambiguous observation keeps the liveness receipt */ }
      if (removeGuard) removeCustodyReleaseGuard(guard);
      return;
    }
    releaseByName();
  };
};

// A complete receipt of a dead pid, or of a live pid with another incarnation, is recovered at
// once by renaming it to a retained, identity-named fence (as the mesh lock does): a paused
// second recoverer cannot rename a successor onto the same nonempty fence.
const clearDeadCustodyLock = async (lock: string, readOwner: () => string | undefined, deadline: number,
  ownStart: string | undefined, hostQualified = false, note: StaleLockNote = {}, lockProtocol: 1 | 2 | 3 = 1): Promise<boolean> => {
  delete note.verdict; delete note.namespace; delete note.token; // describes the CURRENT holder only
  let pin: { unchanged(): boolean; close(): void } | undefined;
  try {
    const stat = fs.lstatSync(lock);
    if (!stat.isDirectory()) return false;
    const owner = readOwner();
    if (owner === undefined) {
      // Fail closed. The owner may have been created and removed between reads (or by an older
      // release), so neither inode metadata nor an observation timer proves a continuous ownerless
      // generation. Confirm the same still-ownerless directory before surfacing operator recovery.
      const current = fs.lstatSync(lock);
      if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino || readOwner() !== undefined) return false;
      throw new MeshCustodyUnrecoverableError(lock);
    }
    const fields = owner.split("\n");
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
    // pi-fabric#1562 (review/astra P1): with no machine id here (Windows, macOS, a Linux without /etc/machine-id), the
    // mesh-wide host-lease additions gate (and every host-qualified gate) is recovered on the participant locks' process
    // proof instead (participant-files.ts holderAlive): the record names THIS host (ownHostLabel, so its holder had no
    // machine id either); then its PID must be gone here (ESRCH) or, with a native incarnation of this platform, now be
    // another process (a different native incarnation), below. review-fleet-lead P1 on pi-fabric#1562: a record with no
    // (or an invalid) incarnation was kept busy BEFORE the ESRCH probe, so its dead holder's gate was never recovered. An
    // unfenced session runtime writes none until its own native read has settled (participant-directory.ts
    // #leaseLockIncarnation), nor does a failed read. Like holderAlive (participant-files.ts), ESRCH recovers it; a live
    // or EPERM PID has nothing to compare with and keeps it. Unsure keeps the gate: another host, a machine id in the
    // record, a record from an older build (no label), a denied, unknown or timed-out native read, an equal one.
    // Protocol 3 still refuses first (a Windows namespace never proves a holder dead).
    const hostLabel = hostQualified && !machineId ? ownHostLabel() : undefined;
    if (hostQualified && ((fields.length !== 7 && fields.length !== 8) || (machineId ? fields[4] !== machineId
      : !hostLabel || fields[4] !== hostLabel))) return false;
    const bootKnown = hostQualified && physical !== undefined && validBootId(fields[5]);
    let previousBoot = bootKnown && fields[5] !== physical!.bootId;
    if (!token || !Number.isSafeInteger(pid) || pid <= 0 || !Number.isFinite(Number(createdText)) ||
      (hostQualified ? fields.length !== 7 && fields.length !== 8 : fields.length !== 4 && fields.length !== 5 && fields.length !== 6)) return false;
    // smarty-dev#9569: a protocol-3 record names its holder's PID namespace. Before EVERY branch below (the plain one,
    // and hostQualified with known, previous or unknown boot): pid evidence only for our namespace.
    const recordNamespace = hostQualified ? (fields.length === 8 ? fields[6]! : null) : (fields.length === 6 ? fields[4]! : null);
    // pi-fabric#923 (fabric-store decision): under protocol 3 no previous-boot inference at all. Two running VM clones
    // share a machine id with different boot ids, so "another boot" does not prove the holder dead.
    if (lockProtocol === 3) previousBoot = false;
    if (recordNamespace !== null || lockProtocol === 3) {
      const namespaces = await import("../core/pid-namespace.js");
      // Under protocol 3 a record without a namespace is UNPROVEN too: recovered only by the operator.
      const result = recordNamespace === null ? { verdict: "unproven" as const }
        : namespaces.reclaimVerdict(recordNamespace, await namespaces.ownNamespaceForReclaim(deadline, false), path.dirname(lock));
      pin = "pin" in result ? result.pin : undefined;
      const verdict = result.verdict;
      if (verdict !== "pid") {
        note.verdict = verdict; note.namespace = recordNamespace ?? ""; note.token = token!;
        return false;
      }
    }
    if (hostQualified && !hostLabel && !bootKnown && !previousBoot) {
      // ESRCH on THIS machine plus old namespace/owner metadata, never expiry of a LIVE
      // holder. Do not compare incarnations or infer a reboot from missing evidence.
      const ownerStat = fs.lstatSync(path.join(lock, "owner"));
      if (!ownerStat.isFile() || Date.now() - Math.max(stat.mtimeMs, ownerStat.mtimeMs) <= CUSTODY_LOCK_STALE_MS ||
        processAlive(pid)) return false;
    } else if (!previousBoot && processAlive(pid)) {
      // A live or unprobeable (EPERM) PID: recovered only when its native incarnation, read now, differs from the record's.
      if (!validProcessIncarnation(recordedStart)) return false;
      // Our own pid with another incarnation is a dead predecessor; with ours, a live caller.
      const remaining = deadline - Date.now();
      const actual = pid === process.pid ? ownStart
        : remaining > 0 ? await processIncarnation(pid, remaining).catch(() => undefined) : undefined;
      if (!actual || actual === recordedStart) return false;
    }
    const current = fs.lstatSync(lock);
    if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino || readOwner() !== owner) return false;
    // pi-fabric#923 round 5: the root checked as host-local must still be the root this path names, right now.
    if (pin && !pin.unchanged()) { note.verdict = "unproven"; note.namespace = recordNamespace ?? ""; note.token = token!; return false; }
    fs.renameSync(lock, `${lock}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:${owner}`).digest("hex")}`);
    return true;
  } catch (error) {
    if (error instanceof MeshCustodyUnrecoverableError) throw error;
    return false;
  } finally { pin?.close(); }
};

/**
 * Runs a synchronous file-custody operation under the custody lock and, in "dual" mode, also
 * under the mesh lock (custody first). `timeoutMs` bounds each acquisition; an active
 * withTryLock budget bounds the custody acquisition too.
 */
export const withMeshCustody = async <T>(mesh: MeshCustodyStore, operation: () => T, timeoutMs?: number): Promise<T> => {
  const budget = Math.min(timeoutMs ?? CUSTODY_LOCK_TIMEOUT_MS, mesh.tryLockBudgetMs ?? Number.POSITIVE_INFINITY);
  const release = await acquireMeshCustodyLock(mesh.root, budget, { lockProtocol: mesh.lockProtocol ?? 1 });
  try {
    if (meshCustodyMode() === "own") return operation();
    return await (timeoutMs === undefined ? mesh.exclusive(operation) : mesh.exclusive(operation, timeoutMs));
  } finally {
    release();
  }
};
