// Operator recovery of a mesh `.lock` or `custody.lock` that automatic recovery refuses (smarty-dev#9569, D2). Under
// lock protocol 3 a holder of another PID namespace (a container, a VM, another host), or one whose namespace cannot
// be proven, is never reclaimed on pid evidence: its pid means nothing here. This is the manual step for such a holder.
//
// - Safe form: only when the holder's namespace is OURS and pid evidence proves it dead (ESRCH), or it ran in an
//   earlier boot of this machine. That is the automatic rule, run on demand; the best place to run it is INSIDE the
//   holder's namespace (`docker exec`, `wsl`), where its pid means something.
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
import { readPhysicalHostIdentity, readPhysicalMachineId, validBootId } from "../core/atomic-write.js";
import { ownPidNamespace, pidNamespaceVerdict, previousBootTrusted, type PidNamespaceVerdict } from "../core/pid-namespace.js";
import { MESH_CUSTODY_LOCK_NAME } from "./custody-lock.js";

export class MeshLockRecoverRefusedError extends Error {
  readonly code = "FABRIC_MESH_LOCK_RECOVER_REFUSED";
  constructor(message: string) {
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
}

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
  verdict: PidNamespaceVerdict; operator: { uid: number | null; user: string; pid: number; ppid: number; parentCommand: string; host: string };
}

/** The parent process's command line: who ran the recovery (Linux /proc; elsewhere unknown, never guessed). */
const parentCommand = (): string => {
  try { return fs.readFileSync(`/proc/${process.ppid}/cmdline`, "utf8").split("\0").filter(Boolean).join(" "); }
  catch { return "unknown"; }
};
const operator = (): LockRecoverAudit["operator"] => {
  let user = "unknown";
  try { user = os.userInfo().username; } catch { /* no passwd entry */ }
  return { uid: typeof process.getuid === "function" ? process.getuid() : null, user, pid: process.pid, ppid: process.ppid,
    parentCommand: parentCommand(), host: os.hostname() };
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
  if (fields[0] !== options.holder) throw new MeshLockRecoverRefusedError(`${lock} is held by ${fields[0]}, not ${options.holder}`);
  // pi-fabric#923: a record without a namespace is UNPROVEN (its pid may name nothing here although it is alive in
  // another namespace), except a host-qualified custody record whose machine id is ours and whose boot ended.
  let verdict: PidNamespaceVerdict;
  if (namespace === null) {
    const physical = options.lock === "custody" && fields.length === 7 ? readPhysicalHostIdentity() : undefined;
    const machine = physical?.machineId ?? (options.lock === "custody" && fields.length === 7 ? readPhysicalMachineId() : undefined);
    verdict = physical !== undefined && !!machine && fields[4] === machine && validBootId(fields[5]) && fields[5] !== physical.bootId
      ? "previous-boot" : "unproven";
  } else verdict = pidNamespaceVerdict(namespace, await ownPidNamespace());
  // Previous-boot ids come from the owner file: honored only if nobody else could have written it.
  if (verdict === "previous-boot" && !previousBootTrusted(lock)) verdict = "unproven";
  const provenDead = verdict === "previous-boot" || (verdict === "pid" && !(options.alive ?? alive)(pid));
  if (!provenDead && options.unsafeHolderStopped !== true) {
    throw new MeshLockRecoverRefusedError(verdict === "pid"
      ? `holder pid ${pid} is running here: it is not recovered`
      : `the holder is in ${verdict === "foreign" ? `another PID namespace (${String(namespace)})` : "a PID namespace this process cannot prove"}; run this inside that namespace, or pass the unsafe override once the holder is stopped`);
  }
  // The same directory and the same bytes as checked above, right before acting.
  const current = fs.lstatSync(lock, { throwIfNoEntry: false });
  let again: string | undefined;
  try { again = fs.readFileSync(path.join(lock, "owner"), "utf8"); } catch { /* changed */ }
  if (!current?.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino || again !== owner) {
    throw new MeshLockRecoverRefusedError(`${lock} changed while it was checked; nothing was done`);
  }
  const mode = provenDead ? "safe" : "unsafe";
  const audit: LockRecoverAudit = { at: new Date().toISOString(), root, lock, mode, holder: options.holder, holderPid: pid,
    holderNamespace: namespace, verdict, operator: operator() };
  // The audit is written BEFORE the rename: a recovery that happened is always on record.
  fs.appendFileSync(path.join(root, "lock-recoveries.jsonl"), `${JSON.stringify(audit)}\n`, { mode: 0o600 });
  const fence = `${lock}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:${owner}`).digest("hex")}`;
  fs.renameSync(lock, fence);
  return { recovered: true, mode, lock, fence, holderPid: pid, holderNamespace: namespace, verdict, audit,
    ...(provenDead ? {} : { warning: `UNSAFE override by ${audit.operator.user} (uid ${String(audit.operator.uid)}, pid ${audit.operator.pid}, parent "${audit.operator.parentCommand}", host ${audit.operator.host}): ${lock} of holder ${options.holder} (pid ${pid}, namespace ${String(namespace)}) was recovered on the operator's word that the holder is stopped. If it is still running, its release can remove its successor's lock and two writers can run.` }) };
};
