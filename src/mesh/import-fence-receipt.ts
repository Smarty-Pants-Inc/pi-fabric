/**
 * smarty-dev#9501: the fence receipt of an import or cutover, so the exact rerun of a SIGKILLed one converges.
 *
 * A killed import leaves its migration fence behind: `custody.lock`, the mesh `.lock` and that lock's FIFO ticket.
 * The rerun's readiness gate runs its writer census BEFORE it takes the fence, sees a `.lock` owner (and ticket) whose
 * pid no writer record names, and requires `fabric@unknown`: the rerun refused forever, although the fence itself would recover the dead holder.
 *
 * Under the fence, before `beforeOpen` and before any database file, the import writes one durable receipt naming
 * the exact owner bytes of both fence locks, the root's identity (realpath, dev, ino) and this process's identity
 * (pid, start time, machine id, boot id, PID namespace). The CLI preflight, before the gate, acts only when the
 * receipt is complete, the root is the same inode, this process runs on the same machine and boot, and both locks
 * still hold exactly the receipt's owner bytes. That is provenance only, never evidence of death. It then calls the
 * mesh lock's ordinary stale recovery (`MeshLock.clearStaleLock`) with that owner and the receipt's PID namespace as
 * the expected context: the owner is compared INSIDE the recovery after its read, so a successor's lock is never
 * touched, and a record without a namespace line (protocol 1/2) is judged with the receipt's namespace by the
 * existing reclaimVerdict: our own namespace and a host-local root, pinned and rechecked right before the rename.
 * Machine and boot alone are not enough: a live holder in another PID namespace on this host shares both. That
 * recovery alone decides death (ESRCH or another incarnation); under protocol 3 a record without a namespace stays
 * unproven, the receipt never upgrades it. Inside that recovery, after its sole proof but BEFORE its native canonical
 * rename, the dead holder's own ticket (same pid and owner token) is renamed out of the ticket namespace, never
 * deleted. A SIGKILL after that rename leaves no stranded ticket; before it, the canonical owner remains to reprove.
 * Ticket retirement must succeed (or the queue be absent) before detaching the owner; an interrupted partial retirement
 * is idempotent on rerun. No second death proof, dead-lock resume scan, or general census weakening is introduced.
 * Live/foreign holders and unproven legacy records still refuse. `custody.lock` is left to the fence's own recovery.
 *
 * Everything else does nothing and the gate decides as before: no, torn or unrecognized receipt, another root, host,
 * boot or PID namespace (or an unknown one), a non-local root, other owner bytes, a live holder (the gate then refuses
 * it as `fabric@unknown`), a foreign or unproven protocol-3 holder. Compatibility limit: residue of a release from
 * before this receipt (no receipt, a pid-only owner) intentionally stays `fabric@unknown`; once the operator has
 * verified the holder is gone, `--accept-unready fabric@unknown` is the documented override.
 * A lazy module (startup graph budget): loaded only by an import and the CLI.
 */
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { ownProcessIncarnation, readPhysicalHostIdentity, validProcessIncarnation } from "../core/atomic-write.js";
import { MESH_CUSTODY_LOCK_NAME } from "./custody-lock.js";
import { meshLockQueueDirectory } from "./lock-queue.js";
import { MeshLock } from "./mesh-lock.js";

export const IMPORT_FENCE_RECEIPT = "import-fence.receipt.json";
const KIND = "fabric-mesh-import-fence";
const RECOVERY_BUDGET_MS = 5_000;
/** lock-queue.ts's ticket name (anchored): `<24-digit hrtime>-<pid>-<token>`. */
const TICKET = /^\d{24}-\d+-[a-f0-9-]+$/;

interface Receipt {
  format: 1; kind: typeof KIND; root: string; rootDev: string; rootIno: string;
  pid: number; start: string; machineId: string; bootId: string; namespace: string; mesh: string; custody: string;
}

const rootIdentity = (root: string): { root: string; rootDev: string; rootIno: string } => {
  const real = fs.realpathSync(root);
  const stat = fs.statSync(real, { bigint: true });
  if (!stat.isDirectory()) throw new Error("not a directory");
  return { root: real, rootDev: String(stat.dev), rootIno: String(stat.ino) };
};

const readOwner = (root: string, lock: string): string | undefined => {
  try { return fs.readFileSync(path.join(root, lock, "owner"), "utf8"); } catch { return undefined; }
};

/** A complete owner record of `pid`: its start line, when present, must be `start`. */
const ownedBy = (owner: string | undefined, pid: number, start: string): owner is string => {
  if (!owner?.endsWith("\n")) return false;
  const fields = owner.split("\n");
  return fields.length >= 4 && !!fields[0] && fields[1] === String(pid) && (!fields[3] || fields[3] === start);
};

/** This process's PID namespace (the lazy reader mesh-lock.ts uses too); undefined when unknown or malformed. */
const ownNamespace = async (): Promise<string | undefined> => {
  const namespaces = await import("../core/pid-namespace.js");
  const namespace = await namespaces.ownPidNamespace().catch(() => undefined);
  return namespaces.validPidNamespace(namespace) ? namespace : undefined;
};

/**
 * Under the fence (both locks held by this process), before beforeOpen. Best effort: an aid for a rerun, it never
 * fails or changes the import; without a provable process, host and namespace identity no receipt is written.
 */
export const writeImportFenceReceipt = async (root: string): Promise<boolean> => {
  let temporary: string | undefined;
  try {
    const start = await ownProcessIncarnation().catch(() => undefined);
    const host = readPhysicalHostIdentity();
    const namespace = await ownNamespace();
    if (!host || !namespace || !start || !validProcessIncarnation(start)) return false;
    const identity = rootIdentity(root);
    const mesh = readOwner(identity.root, ".lock");
    const custody = readOwner(identity.root, MESH_CUSTODY_LOCK_NAME);
    if (!ownedBy(mesh, process.pid, start) || !ownedBy(custody, process.pid, start)) return false;
    const receipt: Receipt = { format: 1, kind: KIND, ...identity, pid: process.pid, start, ...host, namespace, mesh, custody };
    const target = path.join(identity.root, IMPORT_FENCE_RECEIPT);
    temporary = `${target}.${process.pid}.${randomUUID()}.tmp`;
    const fd = fs.openSync(temporary, "wx", 0o600);
    try { fs.writeFileSync(fd, `${JSON.stringify(receipt)}\n`); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    fs.renameSync(temporary, target);
    temporary = undefined;
    const directory = fs.openSync(identity.root, "r");
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
    return true;
  } catch {
    return false;
  } finally {
    // ponytail: no judgment precedes this. `temporary` is this call's own unpublished wx name (random UUID), never a
    // recovery or rename target, and is undefined once published: only a failed write reaches this removal.
    if (temporary !== undefined) try { fs.rmSync(temporary, { force: true }); } catch { /* best effort */ }
  }
};

const parseReceipt = (text: string): Receipt | undefined => {
  let value: unknown;
  try { value = JSON.parse(text); } catch { return undefined; }
  const r = value as Partial<Receipt> | null;
  const strings = ["root", "rootDev", "rootIno", "start", "machineId", "bootId", "namespace", "mesh", "custody"] as const;
  if (typeof r !== "object" || r === null || r.format !== 1 || r.kind !== KIND || !Number.isSafeInteger(r.pid) || r.pid! <= 0 ||
    strings.some(key => typeof r[key] !== "string" || r[key] === "")) return undefined;
  return r as Receipt;
};

/**
 * The CLI preflight of import and cutover, before the readiness gate. Returns an audit line when it recovered the
 * killed importer's `.lock`, otherwise undefined (nothing changed). Never throws.
 */
export const recoverKilledImportLock = async (root: string, lockProtocol: 1 | 2 | 3 = 1): Promise<string | undefined> => {
  try {
    const identity = rootIdentity(root);
    let text: string;
    try { text = fs.readFileSync(path.join(identity.root, IMPORT_FENCE_RECEIPT), "utf8"); } catch { return undefined; }
    const receipt = parseReceipt(text);
    if (!receipt || receipt.pid === process.pid) return undefined;
    // Provenance: this root (same inode), this machine, boot and PID namespace, both owners the receipt's exact bytes.
    const host = readPhysicalHostIdentity();
    if (receipt.root !== identity.root || receipt.rootDev !== identity.rootDev || receipt.rootIno !== identity.rootIno ||
      !host || host.machineId !== receipt.machineId || host.bootId !== receipt.bootId ||
      !ownedBy(receipt.mesh, receipt.pid, receipt.start) || !ownedBy(receipt.custody, receipt.pid, receipt.start) ||
      readOwner(identity.root, ".lock") !== receipt.mesh || readOwner(identity.root, MESH_CUSTODY_LOCK_NAME) !== receipt.custody) return undefined;
    // The sole death proof: the mesh lock's own stale recovery, acting only on exactly the receipt's owner bytes. A
    // record without a namespace line is judged with the receipt's namespace by the same reclaimVerdict (our own PID
    // namespace AND a host-local root, pinned and rechecked before the rename); protocol 3 still refuses such records.
    const lock = new MeshLock(identity.root, { lockProtocol, lockTimeoutMs: RECOVERY_BUDGET_MS }, () => undefined);
    const token = receipt.mesh.split("\n")[0]!;
    const tickets: string[] = [];
    if (!await lock.clearStaleLock(path.join(lock.lockPath, "owner"), Date.now() + RECOVERY_BUDGET_MS, {},
      { owner: receipt.mesh, namespace: receipt.namespace, beforeRename: () => {
        // Only the exact dead holder's pid+token, under the recovery's existing proof. Rename, never delete, in the
        // same queue (no EXDEV), to names outside the anchored ticket grammar. Already retired names are ignored.
        // Fail closed on any error except an absent queue: do not lose the canonical owner needed for a rerun.
        const queue = meshLockQueueDirectory(identity.root);
        let names: string[];
        try { names = fs.readdirSync(queue); }
        catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
        for (const name of names.filter(item => TICKET.test(item) && item.endsWith(`-${receipt.pid}-${token}`))) {
          fs.renameSync(path.join(queue, name), path.join(queue, `${name}.recovered`));
          tickets.push(name);
        }
      } })) return undefined;
    return `recovered the mesh .lock of killed import pid ${receipt.pid} (token ${token}, ${IMPORT_FENCE_RECEIPT}`
      + `${tickets.length > 0 ? `; ticket ${tickets.join(",")} retired` : ""}); custody.lock is left to the fence's own recovery`;
  } catch {
    return undefined;
  }
};
