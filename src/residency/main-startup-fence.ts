import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { lockFile } from "./file-lock.js";
import { residentRoot } from "./protocol.js";
import { processStartTime } from "./process-identity.js";
import { writeJsonAtomic } from "../core/atomic-write.js";

/** Startup and prune share this persistent inode. Keep it until all writers drain. */
export const nativeMainStartupLock = (meshRoot: string, rootId: string): string =>
  path.join(residentRoot(meshRoot, rootId), "main-start.lock");

/** Observational custody only: process death is never deletion authority. */
export const nativeMainProcessRecord = (meshRoot: string, rootId: string): string =>
  path.join(residentRoot(meshRoot, rootId), "main-process.json");

export const nativeMainCleanCloseReceipt = (meshRoot: string, rootId: string): string =>
  path.join(residentRoot(meshRoot, rootId), "main-clean-close.json");

const hex = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const hash = (value: string): string => createHash("sha256").update(value).digest("hex");
const readRecord = (file: string): Record<string, unknown> => {
  if (!fs.lstatSync(file).isFile()) throw new Error("Not a regular ownership record");
  const value: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid ownership record");
  return value as Record<string, unknown>;
};

/** Positive authority, checked again while prune holds both startup and registry locks.
 * The proof is held only by this generation's Main until successful orderly shutdown;
 * the public process record commits to it but cannot synthesize a close receipt. */
export const assertNativeMainCleanClose = (meshRoot: string, rootId: string): string => {
  const unknown = (): never => { throw new Error(`Cannot prove lineage ${rootId} dead: missing or invalid native Main clean-close receipt`); };
  try {
    const owner = readRecord(nativeMainProcessRecord(meshRoot, rootId));
    const receipt = readRecord(nativeMainCleanCloseReceipt(meshRoot, rootId));
    if (owner.format !== 1 || owner.meshRoot !== meshRoot || owner.rootId !== rootId ||
        owner.legacyOwnershipUnknown !== false || !hex(owner.generation) || !hex(owner.closeCommitment) ||
        receipt.format !== 1 || receipt.meshRoot !== meshRoot || receipt.rootId !== rootId ||
        receipt.generation !== owner.generation || !hex(receipt.closeProof) ||
        hash(receipt.closeProof) !== owner.closeCommitment ||
        typeof receipt.closedAt !== "number" || !Number.isFinite(receipt.closedAt) || receipt.closedAt < 0) return unknown();
    return owner.generation;
  } catch { return unknown(); }
};

export type NativeMainOwnershipRelease = (() => void) & { cleanClose(): void };

/** Unknown legacy custody is sticky. Project/current-session absence is not complete
 * root custody: look across every storage session, and preserve existing root evidence. */
const legacyRows = (meshRoot: string, rootId: string, actorRoots: readonly string[]): boolean => {
  const roots = new Set(actorRoots);
  const base = path.join(meshRoot, "actors"); roots.add(base);
  try {
    for (const entry of fs.readdirSync(base, { withFileTypes: true })) {
      if (entry.isDirectory() || entry.isSymbolicLink()) roots.add(path.join(base, entry.name));
    }
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") return true; }
  return [...roots].some(at => {
    try {
      const saved = readRecord(path.join(at, "actors.json"));
      return saved.format !== 1 || !Array.isArray(saved.actors) || saved.actors.some(row =>
        !row || typeof row !== "object" || typeof row.rootId !== "string" || row.rootId === rootId);
    } catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
  });
};

/** Even an empty registry cannot reconcile an unfenced predecessor. Expired root
 * presence is uncertainty, not absence/death proof. Reuse maintenance's uncached
 * evidence reader only at real native admission, never at import or idle lifecycle. */
const legacyPresence = async (meshRoot: string, rootId: string): Promise<boolean> => {
  try {
    const { assertPruneOwnershipDead } = await import("../topology/prune-ownership.js");
    const evidence = assertPruneOwnershipDead(meshRoot, rootId);
    if ([...evidence.state, ...evidence.participants].some(entry =>
      entry.updatedBy.id === rootId || (entry.value as { rootId?: unknown } | null)?.rootId === rootId)) return true;
    // File-only lease publication can be older than (or absent from) shared state.
    const dir = path.join(meshRoot, "host-leases");
    try { return fs.readdirSync(dir).filter(name => name.endsWith(".json")).some(name => readRecord(path.join(dir, name)).rootId === rootId); }
    catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
  } catch { return true; }
};

/** Admit native Main under the shared startup boundary. Reload, reinitialization,
 * failed startup, abrupt exit and ordinary release DO NOT certify clean shutdown. */
export const acquireNativeMainStartupFence = async (meshRoot: string, rootId: string, actorRoots: readonly string[]): Promise<NativeMainOwnershipRelease> => {
  if (process.platform !== "linux") return Object.assign(() => {}, { cleanClose() {} });
  const file = nativeMainStartupLock(meshRoot, rootId);
  const rootExisted = fs.existsSync(path.dirname(file));
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const fd = await lockFile(file, 120, true);
  try {
    const held = fs.fstatSync(fd), current = fs.lstatSync(file);
    if (!current.isFile() || current.dev !== held.dev || current.ino !== held.ino) throw new Error("Native Main startup fence changed");
    const started = processStartTime(process.pid);
    const bootId = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
    if (!started || !/^\d+$/.test(started) || !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(bootId)) throw new Error("Native Main process identity unavailable");
    let legacyOwnershipUnknown = false;
    try {
      const previous = readRecord(nativeMainProcessRecord(meshRoot, rootId));
      legacyOwnershipUnknown = previous.format !== 1 || previous.meshRoot !== meshRoot || previous.rootId !== rootId ||
        !Number.isSafeInteger(previous.pid) || Number(previous.pid) <= 0 || Number(previous.pid) > 2_147_483_647 ||
        typeof previous.processStartTime !== "string" || !/^\d+$/.test(previous.processStartTime) ||
        typeof previous.bootId !== "string" || !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(previous.bootId) ||
        previous.legacyOwnershipUnknown !== false || !hex(previous.generation) || !hex(previous.closeCommitment);
    } catch (error) {
      legacyOwnershipUnknown = (error as NodeJS.ErrnoException).code !== "ENOENT" || rootExisted ||
        legacyRows(meshRoot, rootId, actorRoots) || await legacyPresence(meshRoot, rootId);
    }
    const generation = randomBytes(32).toString("hex");
    const closeProof = randomBytes(32).toString("hex");
    // Invalidate the old receipt BEFORE admitting writers, including in-place reloads.
    fs.rmSync(nativeMainCleanCloseReceipt(meshRoot, rootId), { force: true });
    writeJsonAtomic(nativeMainProcessRecord(meshRoot, rootId), {
      format: 1, meshRoot, rootId, generation, closeCommitment: hash(closeProof),
      pid: process.pid, processStartTime: started, bootId, legacyOwnershipUnknown,
    }, { durable: true });
    let released = false;
    return Object.assign(() => { if (!released) { fs.closeSync(fd); released = true; } }, {
      cleanClose() {
        if (released) throw new Error("Native Main ownership already released");
        // This Main cannot certify a pre-upgrade predecessor's orderly shutdown.
        if (legacyOwnershipUnknown) return;
        const held = fs.fstatSync(fd), current = fs.lstatSync(file);
        const owner = readRecord(nativeMainProcessRecord(meshRoot, rootId));
        if (!current.isFile() || current.dev !== held.dev || current.ino !== held.ino ||
            owner.generation !== generation || owner.closeCommitment !== hash(closeProof) ||
            owner.rootId !== rootId || owner.meshRoot !== meshRoot || owner.legacyOwnershipUnknown !== false) {
          throw new Error("Native Main clean-close custody changed");
        }
        writeJsonAtomic(nativeMainCleanCloseReceipt(meshRoot, rootId), {
          format: 1, meshRoot, rootId, generation, closeProof, closedAt: Date.now(),
        }, { durable: true });
      },
    });
  } catch (error) { fs.closeSync(fd); throw error; }
};
