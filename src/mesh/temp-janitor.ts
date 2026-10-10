import fs from "node:fs";
import path from "node:path";

/** A staged state temp is reclaimable only when this old (smarty-dev#6622). */
export const ABANDONED_TEMP_MIN_AGE_MS = 60_000;
/** Bounds one janitor pass; the next reaper sweep continues. */
const MAX_TEMP_REMOVALS = 64;

// `state.json.<pid>.<uuid>.prepared.tmp`, and the inner atomic-write staging of it
// (`...prepared.tmp.<pid>.<uuid>.tmp`). The FIRST pid is the staging writer's.
const PREPARED_TEMP = /^state\.json\.(\d+)\.[0-9a-f-]{36}\.prepared\.tmp(?:\.\d+\.[0-9a-f-]{36}\.tmp)?$/;

/** Whether a pid is known dead. EPERM (another user's live process) and any other
 * uncertainty count as alive: the janitor fails closed and keeps the file. */
export const pidDead = (pid: number): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  if (pid === process.pid) return false;
  try { process.kill(pid, 0); return false; } catch (error) {
    return (error as { code?: unknown }).code === "ESRCH";
  }
};

/**
 * Removes mesh state temps abandoned by a writer that died mid-write (kill -9, OOM):
 * `*.prepared.tmp` whose pid is dead and that are older than `minAgeMs`. A temp of a
 * live pid (including this process) is never touched. Returns the removed names.
 */
export const sweepAbandonedStateTemporaries = (
  meshRoot: string,
  options: { now?: number; minAgeMs?: number; dead?: (pid: number) => boolean } = {},
): string[] => {
  const now = options.now ?? Date.now();
  const minAgeMs = options.minAgeMs ?? ABANDONED_TEMP_MIN_AGE_MS;
  const dead = options.dead ?? pidDead;
  let names: string[];
  try { names = fs.readdirSync(meshRoot); } catch { return []; }
  const removed: string[] = [];
  for (const name of names) {
    if (removed.length >= MAX_TEMP_REMOVALS) break;
    const match = PREPARED_TEMP.exec(name);
    if (!match) continue;
    const pid = Number(match[1]);
    if (pid === process.pid || !dead(pid)) continue;
    const file = path.join(meshRoot, name);
    try {
      const stat = fs.lstatSync(file);
      if (!stat.isFile() || now - stat.mtimeMs <= minAgeMs) continue;
      fs.rmSync(file, { force: true });
      removed.push(name);
    } catch {
      // Renamed or removed meanwhile.
    }
  }
  return removed;
};
