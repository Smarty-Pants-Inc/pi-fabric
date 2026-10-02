import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";

export const RELOAD_SLOT_STALE_MS = 120_000;

// TMPDIR can be session-specific (especially in task launchers). POSIX /tmp is host-local
// and shared across profiles/worktrees; the uid scopes its private directory to this user.
export const reloadSlotsDirectory = (): string => path.join(
  process.platform === "win32" ? os.tmpdir() : "/tmp",
  `pi-fabric-reload-slots-${process.getuid?.() ?? os.userInfo().username}`,
);

const birthOf = (pid: number): string | undefined => {
  if (process.platform !== "linux") return undefined;
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
  } catch { return undefined; }
};
const alive = (pid: number, birth: string | null): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; }
  const current = birth ? birthOf(pid) : undefined;
  // Unknown/denied identities are not evidence of death. Non-Linux uses live pid + expiry.
  return !birth || current === undefined || current === birth;
};
const removeEmpty = (directory: string): void => {
  try { fs.rmdirSync(directory); } catch { /* occupied, gone, or unreadable: leave it alone */ }
};

/** Delete only immutable, uniquely named expired owners, never a successor's slot directory. */
const reclaim = (slot: string): void => {
  try {
    for (const name of fs.readdirSync(slot)) {
      if (!/^owner-[\w-]+\.json$/.test(name)) continue;
      const file = path.join(slot, name);
      const stat = fs.statSync(file);
      let expired = Date.now() - stat.mtimeMs > RELOAD_SLOT_STALE_MS;
      try {
        const owner = JSON.parse(fs.readFileSync(file, "utf8")) as { pid: number; birth: string | null; startedAt: number };
        if (Number.isFinite(owner.startedAt)) expired = Date.now() - owner.startedAt > RELOAD_SLOT_STALE_MS;
        if (Number.isSafeInteger(owner.pid) && (owner.birth === null || typeof owner.birth === "string")) {
          expired ||= !alive(owner.pid, owner.birth);
        }
      } catch { /* a damaged record expires by mtime, not immediately */ }
      if (expired) {
        try { fs.unlinkSync(file); } catch { /* another reclaimer may have removed this owner */ }
      }
    }
    removeEmpty(slot);
  } catch { /* missing/unreadable slot: publication below decides admission */ }
};

/**
 * Nonblocking per-host admission. A slot is published atomically with its O_EXCL owner record
 * already inside. Rename cannot replace a nonempty slot. Reclamation/late release unlinks
 * only the observed UUID owner, then rmdir: even two reclaimers cannot remove a successor.
 * No polling or sleeping here; the controller keeps its ordinary idle retry when full.
 */
export const tryAcquireReloadSlot = (concurrency: number, directory = reloadSlotsDirectory()): (() => void) | undefined => {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const owner = `owner-${randomUUID()}.json`;
  const staging = path.join(directory, `.staging-${owner}`);
  fs.mkdirSync(staging, { mode: 0o700 });
  let acquired: string | undefined;
  try {
    fs.writeFileSync(path.join(staging, owner), JSON.stringify({
      pid: process.pid, birth: birthOf(process.pid) ?? null, startedAt: Date.now(),
    }), { flag: "wx", mode: 0o600 });
    for (let index = 0; index < concurrency; index++) {
      const slot = path.join(directory, `slot-${index}`);
      // One bounded reclamation attempt; no wait and no recursive removal of canonical slots.
      for (let attempt = 0; attempt < 2; attempt++) {
        try { fs.renameSync(staging, slot); acquired = slot; break; }
        catch (error) {
          const code = (error as NodeJS.ErrnoException).code;
          if (!["EEXIST", "ENOTEMPTY", "EPERM", "EACCES"].includes(code ?? "")) throw error;
          if (attempt === 0) reclaim(slot);
        }
      }
      if (acquired) break;
    }
  } finally {
    if (!acquired) fs.rmSync(staging, { recursive: true, force: true });
  }
  if (!acquired) return undefined;
  const slot = acquired;
  let released = false;
  const release = (): void => {
    if (released) return;
    released = true;
    clearTimeout(timeout);
    try { fs.unlinkSync(path.join(slot, owner)); } catch { /* expired/reclaimed or inaccessible */ }
    removeEmpty(slot);
  };
  // Survives native session_shutdown and handoff claim until the new activation settles.
  const timeout = setTimeout(release, RELOAD_SLOT_STALE_MS);
  timeout.unref?.();
  return release;
};
