import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { syncDirectoryChain } from "../core/atomic-write.js";

/** Move a superseded attempt output aside before the next attempt reuses its canonical filename. */
export const retainPriorAttemptFile = (file: string): string | undefined => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return undefined;
  if (!stat.isFile() || stat.isSymbolicLink() || (process.getuid && stat.uid !== process.getuid())) {
    throw new Error(`Refusing unsafe prior-attempt artifact: ${file}`);
  }

  const retained = `${file}.prior-${randomUUID()}`;
  fs.renameSync(file, retained);
  const moved = fs.lstatSync(retained);
  if (!moved.isFile() || moved.dev !== stat.dev || moved.ino !== stat.ino) {
    throw new Error(`Prior-attempt artifact changed during retention: ${file}`);
  }
  syncDirectoryChain(path.dirname(file));
  return retained;
};
