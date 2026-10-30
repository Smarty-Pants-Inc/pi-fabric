import fs from "node:fs";
import path from "node:path";
import { retainCleanupRoot } from "../agents/worktree-manager.js";

/** Move a superseded attempt output into receipt-backed custody before its name is reused. */
export const retainPriorAttemptFile = (file: string): string | undefined => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return undefined;
  if (!stat.isFile() || stat.isSymbolicLink() || (process.getuid && stat.uid !== process.getuid())) {
    throw new Error(`Refusing unsafe prior-attempt artifact: ${file}`);
  }

  const custody = retainCleanupRoot(file, `${path.dirname(file)}.trace-archive`, path.basename(file),
    "superseded prior-attempt artifact", undefined, { expected: { dev: stat.dev, ino: stat.ino, mtimeMs: stat.mtimeMs } });
  return custody?.destination;
};
