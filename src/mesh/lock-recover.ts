// Operator recovery of a mesh `.lock` or `custody.lock` that automatic recovery refuses (smarty-dev#9569, D2). Under
// lock protocol 3 a holder of another PID namespace (a container, a VM, another host), or one whose namespace cannot
// be proven, is never reclaimed on pid evidence: its pid means nothing here. This is the manual step for such a holder.
//
// - Safe form: only when the holder's namespace is OURS and pid evidence proves it dead (ESRCH). That is the
//   automatic rule, run on demand; the best place to run it is INSIDE the holder's namespace (`docker exec`), where its
//   pid means something. A legacy record, another boot, or Windows: never the safe form (pi-fabric#923).
// - Unsafe override (`unsafeHolderStopped`): the operator states that the holder is stopped. Nothing here can check
//   that. On Windows native C protects the recorded directory, but does not fence a live holder's application writes:
//   two writers could still run. Elsewhere a live holder's later release by name can remove its successor's lock (off
//   Linux, until smarty-dev#9569 (d)). The result says so and the caller must surface it.
//
// Both forms name the exact holder token and retain the same `.dead.<hash>` fence as automatic recovery. Windows
// submits the ORIGINAL pre-judgment bigint S/full R to native C: a delayed operator must never detach a successor.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ownPidNamespace, reclaimVerdict, type PidNamespaceVerdict } from "../core/pid-namespace.js";
import { MESH_CUSTODY_LOCK_NAME } from "./custody-lock.js";
import { prepareWindowsMeshCapability, type WindowsMeshCapability } from "../storage/windows-mesh-release.js";
import { retireWindowsMeshOwner, windowsMeshRecoveryFence } from "../storage/windows-mesh-recovery.js";

export class MeshLockRecoverRefusedError extends Error {
  readonly code = "FABRIC_MESH_LOCK_RECOVER_REFUSED";
  // Keep the existing class/code for callers, but never collapse a possibly committed recovery into a refusal.
  constructor(message: string, readonly recoveryOutcome: "refused" | "helper-unavailable/not-started" | "recovery-outcome-unknown" = "refused") {
    super(`FABRIC_MESH_LOCK_RECOVER_REFUSED: ${message}`);
    this.name = "MeshLockRecoverRefusedError";
  }
}

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

/** The holder's namespace line, or null for a legacy record; undefined when the record is not a lock record. */
const namespaceOf = (fields: string[], kind: "mesh" | "custody"): string | null | undefined => {
  if (fields.length === 6) return fields[4]!;
  if (kind === "custody" && fields.length === 8) return fields[6]!;
  if (fields.length === 4 || fields.length === 5 || (kind === "custody" && fields.length === 7)) return null;
  return undefined;
};

export const recoverMeshLock = async (root: string, options: MeshLockRecoverOptions): Promise<MeshLockRecoverResult> => {
  const lock = path.join(root, options.lock === "mesh" ? ".lock" : MESH_CUSTODY_LOCK_NAME);
  // options.platform is ONLY the audit seam. A Windows mutator never takes the JS rename path.
  const windows = process.platform === "win32";
  const stat = fs.lstatSync(lock, { bigint: true, throwIfNoEntry: false });
  if (!stat?.isDirectory()) throw new MeshLockRecoverRefusedError(`${lock} is not held`);
  let owner: string;
  let ownerStat: fs.BigIntStats | undefined;
  try {
    const ownerPath = path.join(lock, "owner");
    if (windows) {
      // Bind S and full R on the SAME descriptor before ANY namespace/death/operator judgment. Native C verifies
      // this original owner object, its bytes and its location under the original directory; never resnapshot B.
      const entry = fs.lstatSync(ownerPath, { bigint: true });
      if (!entry.isFile() || entry.isSymbolicLink()) throw new Error("owner is not a regular file");
      const fd = fs.openSync(ownerPath, "r");
      try {
        ownerStat = fs.fstatSync(fd, { bigint: true });
        if (!ownerStat.isFile() || entry.dev !== ownerStat.dev || entry.ino !== ownerStat.ino ||
            entry.mtimeNs !== ownerStat.mtimeNs || entry.size !== ownerStat.size) throw new Error("owner changed while opened");
        owner = fs.readFileSync(fd, "utf8");
      } finally { fs.closeSync(fd); }
    } else owner = fs.readFileSync(ownerPath, "utf8");
  } catch { throw new MeshLockRecoverRefusedError(`${lock} has no readable owner record`); }
  const fields = owner.split("\n");
  const pid = Number(fields[1]);
  const namespace = namespaceOf(fields, options.lock);
  if (!owner.endsWith("\n") || namespace === undefined || !Number.isSafeInteger(pid) || pid <= 0) {
    throw new MeshLockRecoverRefusedError(`${lock} does not hold a complete owner record`);
  }
  if (fields[0] !== options.holder) throw new MeshLockRecoverRefusedError(`${lock} is held by ${fields[0]}, not ${options.holder}`);
  let capability: WindowsMeshCapability | undefined;
  if (windows) {
    try { capability = prepareWindowsMeshCapability(); }
    catch (error) {
      throw new MeshLockRecoverRefusedError(`Windows recovery helper-unavailable/not-started: ${error instanceof Error ? error.message : String(error)}; no recovery was started`);
    }
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
      ? `holder pid ${pid} is running here: it is not recovered`
      : `the holder is in ${verdict === "foreign" ? `another PID namespace (${String(namespace)})` : "a PID namespace this process cannot prove"}; run this inside that namespace, or pass the unsafe override once the holder is stopped`);
  }
  // The same directory and the same bytes as checked above, right before acting.
  const current = fs.lstatSync(lock, { bigint: true, throwIfNoEntry: false });
  let again: string | undefined;
  try { again = fs.readFileSync(path.join(lock, "owner"), "utf8"); } catch { /* changed */ }
  if (!current?.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino || again !== owner) {
    throw new MeshLockRecoverRefusedError(`${lock} changed while it was checked; nothing was done`);
  }
  // pi-fabric#923 round 5: the safe form's host-local proof holds only while the root path names the root it checked.
  if (provenDead && pin && !pin.unchanged()) throw new MeshLockRecoverRefusedError(`${root} was replaced after its filesystem was checked; nothing was done`);
  const mode = provenDead ? "safe" : "unsafe";
  const audit: LockRecoverAudit = { at: new Date().toISOString(), root, lock, mode, holder: options.holder, holderPid: pid,
    holderNamespace: namespace, verdict, operator: operator() };
  // The audit is written BEFORE native C / rename: a recovery that happened is always on record. Failure refuses.
  appendAudit(path.join(root, "lock-recoveries.jsonl"), `${JSON.stringify(audit)}\n`, options.auditMaxBytes ?? AUDIT_MAX_BYTES, options.platform);
  // Only naming rounds to legacy Number-Stats DECIMAL on ALL platforms; native identity authority stays bigint.
  const fence = windows ? windowsMeshRecoveryFence(lock, stat, owner)
    : `${lock}.dead.${createHash("sha256").update(`${Number(stat.dev)}:${Number(stat.ino)}:${owner}`).digest("hex")}`;
  if (windows) {
    if (!capability || !ownerStat) throw new MeshLockRecoverRefusedError("Windows recovery helper-unavailable/not-started: original authority unavailable; no recovery was started");
    const outcome = retireWindowsMeshOwner(lock, owner, stat, ownerStat, capability);
    if (outcome.outcome !== "recovered") {
      // Unknown is NOT a clean refusal/no-mutation claim. No fallback, retry, reconciliation or new S/R here.
      throw new MeshLockRecoverRefusedError(`Windows recovery ${outcome.outcome}: ${outcome.reason}; recovery success is not established`, outcome.outcome);
    }
  } else fs.renameSync(lock, fence);
  const unsafeConsequence = windows
    ? "If it is still running, native recovery does not fence its application writes and two writers can run."
    : "If it is still running, its release can remove its successor's lock and two writers can run.";
  return { recovered: true, mode, lock, fence, holderPid: pid, holderNamespace: namespace, verdict, audit,
    ...(provenDead ? {} : { warning: `UNSAFE override by ${audit.operator.user} (uid ${String(audit.operator.uid)}, pid ${audit.operator.pid}, parent pid ${audit.operator.ppid} ${audit.operator.parentExecutable}, host ${audit.operator.host}): ${lock} of holder ${options.holder} (pid ${pid}, namespace ${String(namespace)}) was recovered on the operator's word that the holder is stopped. ${unsafeConsequence}` }) };
  } finally { pin?.close(); }
};
