import fs from "node:fs";
import path from "node:path";
import { vi } from "vitest";

/** Fault the real actor-tree mutation, not the verifier. Since smarty-dev#16444 (R-keep-all-traces) an actor tree is
 * archived, never deleted: one rename of the judged directory into `<meshRoot>/session-archive/<root>/`, on every
 * platform. A failed move leaves the original tree for retry. (A rename into a `.retention-graveyard-*` directory, the
 * old verifiedRemove move, is faulted too.) */
export const failActorTreeRemoval = (target: string, fail: () => void): { mockRestore(): void } => {
  const judged = fs.lstatSync(target);
  const rename = fs.renameSync.bind(fs);
  const spy = vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
    const sourceStat = fs.lstatSync(source, { throwIfNoEntry: false });
    let parent = "";
    try { parent = fs.realpathSync(path.dirname(String(destination))); }
    catch { /* An unrelated rename may not have an existing destination parent. */ }
    const archive = path.basename(path.dirname(parent)) === "session-archive" ||
      path.basename(parent).startsWith(".retention-graveyard-");
    if (sourceStat?.dev === judged.dev && sourceStat?.ino === judged.ino && archive) fail();
    return rename(source, destination);
  });
  let restored = false;
  return { mockRestore() { if (!restored) { restored = true; spy.mockRestore(); } } };
};
