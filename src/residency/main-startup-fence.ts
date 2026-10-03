import fs from "node:fs";
import path from "node:path";
import { lockFile } from "./file-lock.js";
import { residentRoot } from "./protocol.js";
import { processStartTime } from "./process-identity.js";
import { writeJsonAtomic } from "../core/atomic-write.js";

/** Native Main lifetime ownership fence, separate from the resident host's host.lock.
 * The startup path acquires it before publication/load, and retains it until shutdown.
 * Prune holds BOTH fences through its commits. Never unlink either inode. */
export const nativeMainStartupLock = (meshRoot: string, rootId: string): string =>
  path.join(residentRoot(meshRoot, rootId), "main-start.lock");

/** Persistent root/mesh-bound process identity; a vacant flock is NOT death proof. */
export const nativeMainProcessRecord = (meshRoot: string, rootId: string): string =>
  path.join(residentRoot(meshRoot, rootId), "main-process.json");

/** Acquire under the existing startup boundary; the caller must keep the returned
 * release until ownership writers drain at shutdown/reinitialization (or load fails).
 * Process death drops the flock even if every heartbeat has expired.
 * Non-Linux cannot destructively prune. On Linux a missing/broken helper fails closed:
 * another Pi process may have a different PATH, so its capability probe is not ours. */
export const acquireNativeMainStartupFence = async (meshRoot: string, rootId: string, actorRoots: readonly string[]): Promise<() => void> => {
  if (process.platform !== "linux") return () => {};
  const file = nativeMainStartupLock(meshRoot, rootId);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const fd = await lockFile(file, 120, true);
  try {
    const held = fs.fstatSync(fd), current = fs.lstatSync(file);
    if (!current.isFile() || current.dev !== held.dev || current.ino !== held.ino) throw new Error("Native Main startup fence changed");
    const started = processStartTime(process.pid);
    const bootId = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
    if (!started || !/^\d+$/.test(started) || !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(bootId)) throw new Error("Native Main process identity unavailable");
    // Resuming a persisted legacy lineage cannot retroactively identify its unfenced
    // writers. Keep that uncertainty sticky across upgrades, even after this PID exits.
    let legacyOwnershipUnknown = false;
    const identityFile = nativeMainProcessRecord(meshRoot, rootId);
    try {
      if (!fs.lstatSync(identityFile).isFile()) throw new Error("Unknown native Main identity custody");
      const previous = JSON.parse(fs.readFileSync(identityFile, "utf8"));
      legacyOwnershipUnknown = previous?.format !== 1 || previous.meshRoot !== meshRoot || previous.rootId !== rootId ||
        !Number.isSafeInteger(previous.pid) || previous.pid <= 0 || previous.pid > 2_147_483_647 ||
        typeof previous.processStartTime !== "string" || !/^\d+$/.test(previous.processStartTime) ||
        typeof previous.bootId !== "string" || !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(previous.bootId) ||
        (previous.legacyOwnershipUnknown !== undefined && previous.legacyOwnershipUnknown !== false);
    } catch (error) {
      legacyOwnershipUnknown = (error as NodeJS.ErrnoException).code !== "ENOENT" || actorRoots.some(at => {
        try {
          const saved = JSON.parse(fs.readFileSync(path.join(at, "actors.json"), "utf8"));
          return saved?.format !== 1 || !Array.isArray(saved.actors) ||
            saved.actors.some((row: { rootId?: unknown } | null) => !row || typeof row.rootId !== "string" || row.rootId === rootId);
        } catch (readError) { return (readError as NodeJS.ErrnoException).code !== "ENOENT"; }
      });
    }
    // Publish before any actor writer is admitted, while holding the common fence.
    // Retain after shutdown: only process death, not a released lock, permits prune.
    writeJsonAtomic(nativeMainProcessRecord(meshRoot, rootId), {
      format: 1, meshRoot, rootId, pid: process.pid, processStartTime: started, bootId, legacyOwnershipUnknown,
    }, { durable: true });
    return () => fs.closeSync(fd);
  } catch (error) { fs.closeSync(fd); throw error; }
};
