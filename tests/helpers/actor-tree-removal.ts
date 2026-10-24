import fs from "node:fs";
import path from "node:path";
import { vi } from "vitest";

/** Fault the real actor-tree mutation, not the verifier: Linux moves the judged inode to its graveyard before
 * unlinking; other platforms use the manager's rmSync fallback. A failed move leaves the original tree for retry. */
export const failActorTreeRemoval = (target: string, fail: () => void): { mockRestore(): void } => {
  const judged = process.platform === "linux" ? fs.lstatSync(target) : undefined;
  const rename = fs.renameSync.bind(fs);
  const rm = fs.rmSync.bind(fs);
  const spy = process.platform === "linux"
    ? vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
        const sourceStat = fs.lstatSync(source, { throwIfNoEntry: false });
        let graveyard = false;
        try {
          graveyard = path.basename(fs.realpathSync(path.dirname(String(destination)))) === `.retention-graveyard-${process.pid}`;
        } catch { /* An unrelated rename may not have an existing destination parent. */ }
        if (sourceStat?.dev === judged!.dev && sourceStat?.ino === judged!.ino && graveyard) fail();
        return rename(source, destination);
      })
    : vi.spyOn(fs, "rmSync").mockImplementation((at, options) => {
        if (at === target) fail();
        return rm(at, options);
      });
  let restored = false;
  return { mockRestore() { if (!restored) { restored = true; spy.mockRestore(); } } };
};
