// Operator recovery of a mesh `.lock` or `custody.lock` that automatic recovery refuses (smarty-dev#9569, D2). Under
// lock protocol 3 a holder of another PID namespace (a container, a VM, another host), or one whose namespace cannot
// be proven, is never reclaimed on pid evidence: its pid means nothing here. This is the manual step for such a holder.
//
// - Safe form: only when the holder's namespace is OURS and pid evidence proves it dead (ESRCH). That is the
//   automatic rule, run on demand; the best place to run it is INSIDE the holder's namespace (`docker exec`), where its
//   pid means something. A legacy record, another boot, or Windows: never the safe form (pi-fabric#923).
// - Unsafe override (`unsafeHolderStopped`): the operator states that the holder is stopped. Nothing here can check
//   that. If the holder is in fact still running, its later release by name can remove its successor's lock (off Linux,
//   until smarty-dev#9569 (b)/(d)), so two writers could run. The result says so and the caller must surface it.
//
// Both forms name the exact holder token, re-check the directory inode and the owner bytes right before acting, and
// rename the directory to the same retained `.dead.<hash>` fence the automatic recovery uses.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ownPidNamespace, reclaimVerdict, type PidNamespaceVerdict } from "../core/pid-namespace.js";
import { MESH_CUSTODY_LOCK_NAME } from "./custody-lock.js";

export class MeshLockRecoverRefusedError extends Error {
  readonly code = "FABRIC_MESH_LOCK_RECOVER_REFUSED";
  constructor(message: string) {
    super(`FABRIC_MESH_LOCK_RECOVER_REFUSED: ${message}`);
    this.name = "MeshLockRecoverRefusedError";
  }
}

export const OWNERLESS_CUSTODY_RECOVERY_GRACE_MS = 2_000;

export interface MeshLockRecoverOptions {
  lock: "mesh" | "custody";
  /** The holder's token (the owner record's first line): recovery never acts on any other holder. */
  holder: string;
  /** The labelled unsafe override: the operator states that the holder is stopped. */
  unsafeHolderStopped?: boolean;
  /** Test seam: the pid evidence (default: `kill(pid, 0)`; only ESRCH proves death). */
  alive?: (pid: number) => boolean;
  /** The audit file's bound before it rotates (default 1 MiB). */
  auditMaxBytes?: number;
  /** Test seam: the platform the audit rules follow (default `process.platform`). */
  platform?: NodeJS.Platform;
}

const AUDIT_MAX_BYTES = 1024 * 1024;
/**
 * Appends one audit line (pi-fabric#923 round 2): opened without following a symlink (O_NOFOLLOW), then the SAME
 * descriptor must be a regular file, ours, mode 0600. Past `maxBytes` it rotates once (`.1`, the previous one
 * replaced), so the audit is bounded to two files. Any failure refuses the recovery, before anything is renamed.
 */
const appendAudit = (file: string, line: string, maxBytes: number, platform: NodeJS.Platform = process.platform): void => {
  if (platform === "win32") {
    // pi-fabric#923 round 4: Windows has no O_NOFOLLOW, and an lstat-then-open can be raced. Fail closed: the audit is
    // only ever CREATED, exclusively (wx); an existing audit file is never appended to. The operator moves it aside
    // (keeping it) before the next recovery on that root.
    try { fs.writeFileSync(file, line, { flag: "wx", mode: 0o600 }); }
    catch (error) {
      throw new MeshLockRecoverRefusedError(`the audit ${file} ${(error as NodeJS.ErrnoException).code === "EEXIST" ? "already exists; on Windows it is only created, never appended: move it aside (keep it) and run again" : `could not be created (${(error as Error).message})`}; nothing was recovered`);
    }
    return;
  }
  const open = (extra: number): number => {
    const noFollow = fs.constants.O_NOFOLLOW;
    // Without O_NOFOLLOW (Windows), refuse a symlink seen by lstat right before the open.
    if (noFollow === undefined && fs.lstatSync(file, { throwIfNoEntry: false })?.isSymbolicLink()) throw new Error("a symbolic link");
    // O_NONBLOCK: a FIFO planted at the audit path must not block the open (with no reader it fails with ENXIO);
    // the fstat below then refuses anything that is not a regular file.
    return fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT | (noFollow ?? 0) |
      (fs.constants.O_NONBLOCK ?? 0) | extra, 0o600);
  };
  try {
    let fd = open(0);
    try {
      const check = (): fs.Stats => {
        const stat = fs.fstatSync(fd);
        const ours = typeof process.getuid !== "function" || stat.uid === process.getuid();
        if (!stat.isFile() || !ours || (process.platform !== "win32" && (stat.mode & 0o777) !== 0o600)) {
          throw new Error(`not a private regular file (mode ${(stat.mode & 0o777).toString(8)}, uid ${stat.uid})`);
        }
        return stat;
      };
      if (check().size + Buffer.byteLength(line) > maxBytes) {
        fs.closeSync(fd);
        fd = -1;
        fs.renameSync(file, `${file}.1`);
        fd = open(fs.constants.O_EXCL);
        check();
      }
      fs.writeSync(fd, line);
      fs.fsyncSync(fd);
    } finally { if (fd >= 0) fs.closeSync(fd); }
  } catch (error) {
    throw new MeshLockRecoverRefusedError(`the audit ${file} could not be written safely (${(error as Error).message}); nothing was recovered`);
  }
};

export interface MeshLockRecoverResult {
  recovered: true;
  mode: "safe" | "unsafe";
  lock: string;
  fence: string;
  holderPid: number;
  holderNamespace: string | null;
  verdict: PidNamespaceVerdict;
  /** Set for the unsafe override: what the operator accepted. */
  warning?: string;
  /** Who recovered it (pi-fabric#923); also appended to `<root>/lock-recoveries.jsonl`. */
  audit: LockRecoverAudit;
}

export interface LockRecoverAudit {
  at: string; root: string; lock: string; mode: "safe" | "unsafe"; holder: string; holderPid: number; holderNamespace: string | null;
  verdict: PidNamespaceVerdict; operator: { uid: number | null; user: string; pid: number; ppid: number; parentExecutable: string; host: string };
}

export interface OwnerlessCustodyRecoveryResult {
  recovered: true; mode: "ownerless"; lock: string; graceMs: number; holderPid: number;
  audit: { at: string; root: string; lock: string; mode: "ownerless"; releasePid: number; releaseNamespace: string; releaseToken: string;
    operator: ReturnType<typeof operator> };
}

/**
 * The parent process's EXECUTABLE path: who ran the recovery (Linux /proc/<ppid>/exe; elsewhere unknown, never
 * guessed). Never its command line: arguments can carry secrets (pi-fabric#923 round 4).
 */
const parentExecutable = (): string => {
  try { return fs.readlinkSync(`/proc/${process.ppid}/exe`); }
  catch { return "unknown"; }
};
const operator = (): LockRecoverAudit["operator"] => {
  let user = "unknown";
  try { user = os.userInfo().username; } catch { /* no passwd entry */ }
  return { uid: typeof process.getuid === "function" ? process.getuid() : null, user, pid: process.pid, ppid: process.ppid,
    parentExecutable: parentExecutable(), host: os.hostname() };
};

const alive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; } catch (error) { return (error as NodeJS.ErrnoException).code !== "ESRCH"; }
};

interface CustodyReleaseReceipt { format: 1; pid: number; namespace: string; dev: string; ino: string; token: string }
interface OwnerlessObservation { stat: fs.Stats; receipt: CustodyReleaseReceipt; guardFile: string; guardBytes: string; guardStat: fs.Stats }
const OWNERLESS_GUARD_PREFIX = ".custody.lock.releasing.";
const wait = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
const sameStat = (left: fs.Stats, right: fs.Stats): boolean => left.dev === right.dev && left.ino === right.ino &&
  left.ctimeMs === right.ctimeMs && left.mtimeMs === right.mtimeMs;
const ownerlessRefused = (message: string): never => { throw new MeshLockRecoverRefusedError(message); };
const ownPidNamespaceReceipt = (): string => {
  if (process.platform !== "linux") return ownerlessRefused("ownerless custody recovery requires a verifiable Linux PID namespace");
  let namespace: string;
  try { namespace = fs.readlinkSync("/proc/self/ns/pid"); }
  catch { return ownerlessRefused("the operator PID namespace cannot be verified; nothing was recovered"); }
  if (!/^pid:\[\d+\]$/.test(namespace)) return ownerlessRefused("the operator PID namespace is malformed; nothing was recovered");
  return namespace;
};
const inspectOwnerless = (root: string, lock: string, expected?: OwnerlessObservation): OwnerlessObservation => {
  const stat = fs.lstatSync(lock, { throwIfNoEntry: false });
  if (!stat?.isDirectory()) return ownerlessRefused(`${lock} is not an ownerless custody directory`);
  if (expected && !sameStat(stat, expected.stat)) return ownerlessRefused(`${lock} changed during the ownerless recovery grace; nothing was recovered`);
  let names: string[];
  try { names = fs.readdirSync(lock); }
  catch { return ownerlessRefused(`${lock} could not be read; nothing was recovered`); }
  let ownerExists = false;
  try { fs.lstatSync(path.join(lock, "owner")); ownerExists = true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") return ownerlessRefused(`${lock}/owner could not be checked; nothing was recovered`);
  }
  if (ownerExists || names.length !== 0) return ownerlessRefused(`${lock} is not still empty and ownerless; nothing was recovered`);

  const matches = fs.readdirSync(root).filter(name => name.startsWith(OWNERLESS_GUARD_PREFIX));
  const valid: OwnerlessObservation[] = [];
  for (const name of matches) {
    const guardFile = path.join(root, name);
    let guardStat: fs.Stats;
    let guardBytes: string;
    try {
      guardStat = fs.lstatSync(guardFile);
      if (!guardStat.isFile() || guardStat.nlink !== 1 || (guardStat.mode & 0o777) !== 0o600 ||
        (typeof process.getuid === "function" && guardStat.uid !== process.getuid()) || guardStat.size > 4096) {
        return ownerlessRefused(`${guardFile} is not a safe release receipt; nothing was recovered`);
      }
      guardBytes = fs.readFileSync(guardFile, "utf8");
    } catch { return ownerlessRefused(`${guardFile} could not be safely read; nothing was recovered`); }
    let receipt: CustodyReleaseReceipt;
    try { receipt = JSON.parse(guardBytes) as CustodyReleaseReceipt; }
    catch { return ownerlessRefused(`${guardFile} is malformed; nothing was recovered`); }
    const token = name.slice(OWNERLESS_GUARD_PREFIX.length);
    if (!receipt || receipt.format !== 1 || !Number.isSafeInteger(receipt.pid) || receipt.pid <= 0 ||
      typeof receipt.namespace !== "string" || !/^pid:\[\d+\]$/.test(receipt.namespace) ||
      typeof receipt.dev !== "string" || typeof receipt.ino !== "string" || receipt.token !== token || !/^[0-9a-f]{8}$/.test(token)) {
      return ownerlessRefused(`${guardFile} has an invalid release receipt; nothing was recovered`);
    }
    if (receipt.dev === String(stat.dev) && receipt.ino === String(stat.ino)) {
      valid.push({ stat, receipt, guardFile, guardBytes, guardStat });
    }
  }
  if (valid.length !== 1) return ownerlessRefused(`${lock} has no unique matching release receipt; its holder cannot be proven dead`);
  const observation = valid[0]!;
  if (expected && (observation.guardFile !== expected.guardFile || observation.guardBytes !== expected.guardBytes ||
    observation.guardStat.dev !== expected.guardStat.dev || observation.guardStat.ino !== expected.guardStat.ino ||
    observation.guardStat.ctimeMs !== expected.guardStat.ctimeMs || observation.guardStat.mtimeMs !== expected.guardStat.mtimeMs)) {
    return ownerlessRefused(`${lock} release receipt changed during the grace; nothing was recovered`);
  }
  return observation;
};
const refuseIfReleaseHolderLive = (receipt: CustodyReleaseReceipt): void => {
  const namespace = ownPidNamespaceReceipt();
  if (receipt.namespace !== namespace) ownerlessRefused("the release holder is in another or unproven PID namespace; run recovery there");
  if (alive(receipt.pid)) ownerlessRefused(`release holder pid ${receipt.pid} is still live; nothing was recovered`);
};

/** Recover only a dead release witnessed by our sibling receipt; unknown ownerless directories remain fail-closed. */
export const recoverOwnerlessCustodyLock = async (root: string): Promise<OwnerlessCustodyRecoveryResult> => {
  const lock = path.join(root, MESH_CUSTODY_LOCK_NAME);
  const ownNamespace = ownPidNamespaceReceipt();
  const first = inspectOwnerless(root, lock);
  if (first.receipt.namespace !== ownNamespace) ownerlessRefused("the release holder is in another or unproven PID namespace; run recovery there");
  refuseIfReleaseHolderLive(first.receipt);
  await wait(OWNERLESS_CUSTODY_RECOVERY_GRACE_MS);
  const afterGrace = inspectOwnerless(root, lock, first);
  refuseIfReleaseHolderLive(afterGrace.receipt);

  const audit = { at: new Date().toISOString(), root, lock, mode: "ownerless" as const, releasePid: afterGrace.receipt.pid,
    releaseNamespace: afterGrace.receipt.namespace, releaseToken: afterGrace.receipt.token, operator: operator() };
  appendAudit(path.join(root, "lock-recoveries.jsonl"), `${JSON.stringify(audit)}\n`, AUDIT_MAX_BYTES);
  // Revalidate after the audit I/O. Atomic rmdir is deliberate: unlike rename-by-name, it fails if a new holder's
  // complete owner receipt replaced this directory after our last identity check.
  const final = inspectOwnerless(root, lock, first);
  refuseIfReleaseHolderLive(final.receipt);
  try { fs.rmdirSync(lock); }
  catch (error) {
    throw new MeshLockRecoverRefusedError(`${lock} could not be atomically removed (${(error as NodeJS.ErrnoException).code ?? "filesystem error"}); nothing else was removed`);
  }
  try {
    const current = fs.lstatSync(final.guardFile);
    if (current.isFile() && current.dev === final.guardStat.dev && current.ino === final.guardStat.ino &&
      fs.readFileSync(final.guardFile, "utf8") === final.guardBytes) fs.unlinkSync(final.guardFile);
  } catch { /* recovery already succeeded; an orphan receipt is safe and retained for audit */ }
  return { recovered: true, mode: "ownerless", lock, graceMs: OWNERLESS_CUSTODY_RECOVERY_GRACE_MS,
    holderPid: final.receipt.pid, audit };
};

/** The holder's namespace line, or null for a legacy record; undefined when the record is not a lock record. */
const namespaceOf = (fields: string[], kind: "mesh" | "custody"): string | null | undefined => {
  if (fields.length === 6) return fields[4]!;
  if (kind === "custody" && fields.length === 8) return fields[6]!;
  if (fields.length === 4 || fields.length === 5 || (kind === "custody" && fields.length === 7)) return null;
  return undefined;
};

/**
 * The only owner-metadata boundary for safe diagnostics, audit and return values. The caller has already validated
 * the complete record and positive safe-integer pid. Match atomic-write's bounded native namespace/token grammar
 * locally, without an eager dependency. Never use these labels for token equality, namespace verdicts or fencing.
 * Only these three public fields leave this boundary; other owner-record fields are deliberately not retained.
 */
const projectOwner = (holder: unknown, holderPid: number, holderNamespace: unknown): Pick<LockRecoverAudit, "holder" | "holderPid" | "holderNamespace"> => {
  const uuid = "[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}";
  const token = typeof holder === "string" && (holder.length === 32 || holder.length === 36) &&
    new RegExp(`^(?:[0-9A-Fa-f]{32}|${uuid})$`).test(holder) ? holder : "invalid-token";
  const namespace = holderNamespace === null ? null : holderNamespace === "" ? "" :
    typeof holderNamespace === "string" && holderNamespace.length <= 128 &&
      new RegExp(`^(?:linux:(?:[0-9a-f]{32}|-):${uuid}:\\d+|darwin:(?:${uuid}|-):${uuid}:host|win32:${uuid}:\\d+:host)(?![\\s\\S])`).test(holderNamespace)
        ? holderNamespace : "invalid-namespace";
  return { holder: token, holderPid, holderNamespace: namespace };
};

export const recoverMeshLock = async (root: string, options: MeshLockRecoverOptions): Promise<MeshLockRecoverResult> => {
  const lock = path.join(root, options.lock === "mesh" ? ".lock" : MESH_CUSTODY_LOCK_NAME);
  const stat = fs.lstatSync(lock, { throwIfNoEntry: false });
  if (!stat?.isDirectory()) throw new MeshLockRecoverRefusedError(`${lock} is not held`);
  let owner: string;
  try { owner = fs.readFileSync(path.join(lock, "owner"), "utf8"); }
  catch { throw new MeshLockRecoverRefusedError(`${lock} has no readable owner record`); }
  const fields = owner.split("\n");
  const pid = Number(fields[1]);
  const namespace = namespaceOf(fields, options.lock);
  if (!owner.endsWith("\n") || namespace === undefined || !Number.isSafeInteger(pid) || pid <= 0) {
    throw new MeshLockRecoverRefusedError(`${lock} does not hold a complete owner record`);
  }
  const diagnostic = projectOwner(fields[0], pid, namespace);
  if (fields[0] !== options.holder) {
    throw new MeshLockRecoverRefusedError(`${lock} is held by ${diagnostic.holder}, not ${projectOwner(options.holder, pid, namespace).holder}`);
  }
  // pi-fabric#923 (fabric-store decisions): a record without a namespace is UNPROVEN (its pid may name nothing here
  // although it is alive in another namespace); no previous-boot inference (VM clones share a machine id). The safe
  // form recovers only a holder of OUR namespace whose pid is gone; everything else is the audited unsafe override.
  const result = namespace === null ? { verdict: "unproven" as PidNamespaceVerdict } : reclaimVerdict(namespace, await ownPidNamespace(), root);
  const pin = "pin" in result ? result.pin : undefined;
  try {
  const verdict: PidNamespaceVerdict = result.verdict;
  const provenDead = verdict === "pid" && !(options.alive ?? alive)(pid);
  if (!provenDead && options.unsafeHolderStopped !== true) {
    throw new MeshLockRecoverRefusedError(verdict === "pid"
      ? `holder pid ${diagnostic.holderPid} is running here: it is not recovered`
      : `the holder is in ${verdict === "foreign" ? `another PID namespace (${String(diagnostic.holderNamespace)})` : "a PID namespace this process cannot prove"}; run this inside that namespace, or pass the unsafe override once the holder is stopped`);
  }
  // The same directory and the same bytes as checked above, right before acting.
  const current = fs.lstatSync(lock, { throwIfNoEntry: false });
  let again: string | undefined;
  try { again = fs.readFileSync(path.join(lock, "owner"), "utf8"); } catch { /* changed */ }
  if (!current?.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino || again !== owner) {
    throw new MeshLockRecoverRefusedError(`${lock} changed while it was checked; nothing was done`);
  }
  // pi-fabric#923 round 5: the safe form's host-local proof holds only while the root path names the root it checked.
  if (provenDead && pin && !pin.unchanged()) throw new MeshLockRecoverRefusedError(`${root} was replaced after its filesystem was checked; nothing was done`);
  const mode = provenDead ? "safe" : "unsafe";
  // Project BEFORE serialization/append, not just CLI formatting: a safe audit remains safe even if the audit or
  // rename fails. Only an admitted UNSAFE recovery retains the historical raw metadata and warning contract.
  const metadata = provenDead ? diagnostic : { holder: options.holder, holderPid: pid, holderNamespace: namespace };
  const audit: LockRecoverAudit = { at: new Date().toISOString(), root, lock, mode, ...metadata, verdict, operator: operator() };
  // The audit is written BEFORE the rename: a recovery that happened is always on record. A failed audit refuses.
  appendAudit(path.join(root, "lock-recoveries.jsonl"), `${JSON.stringify(audit)}\n`, options.auditMaxBytes ?? AUDIT_MAX_BYTES, options.platform);
  const fence = `${lock}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:${owner}`).digest("hex")}`;
  fs.renameSync(lock, fence);
  return { recovered: true, mode, lock, fence, holderPid: metadata.holderPid, holderNamespace: metadata.holderNamespace, verdict, audit,
    ...(provenDead ? {} : { warning: `UNSAFE override by ${audit.operator.user} (uid ${String(audit.operator.uid)}, pid ${audit.operator.pid}, parent pid ${audit.operator.ppid} ${audit.operator.parentExecutable}, host ${audit.operator.host}): ${lock} of holder ${options.holder} (pid ${pid}, namespace ${String(namespace)}) was recovered on the operator's word that the holder is stopped. If it is still running, its release can remove its successor's lock and two writers can run.` }) };
  } finally { pin?.close(); }
};
