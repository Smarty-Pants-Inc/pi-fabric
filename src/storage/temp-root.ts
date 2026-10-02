import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { windowsDataRoot } from "./windows-temp-root.js";

/** File data only: socket/pipe paths must keep their short transport-specific roots. */
export const fabricDataRoot = (): string => {
  const root = process.env.PI_FABRIC_TMPDIR;
  if (!root) return os.tmpdir();
  if (process.platform === "win32") return windowsDataRoot(root);
  if (!path.isAbsolute(root)) throw new Error("PI_FABRIC_TMPDIR must be an absolute path");
  const fail = (directory: string, reason: string): never => {
    throw new Error(`PI_FABRIC_TMPDIR is unsafe: ${directory} ${reason}`);
  };
  // Match the Herdr ledger's namespace criteria. Walk from the filesystem root so no
  // mkdir/write traverses an unchecked ancestor. Never chmod an existing path.
  const directory = path.resolve(root);
  const chain: string[] = [];
  for (let current = directory; ; current = path.dirname(current)) {
    chain.unshift(current);
    if (path.dirname(current) === current) break;
  }
  const uid = process.getuid!();
  for (const current of chain) {
    let stat: fs.Stats;
    try { stat = fs.lstatSync(current); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      try { fs.mkdirSync(current, { mode: 0o700 }); }
      catch (creationError) {
        if ((creationError as NodeJS.ErrnoException).code !== "EEXIST") throw creationError;
      }
      stat = fs.lstatSync(current);
    }
    if (stat.isSymbolicLink() || !stat.isDirectory()) fail(current, "is not a real directory");
    const final = current === directory;
    if (stat.uid !== uid && (final || stat.uid !== 0)) fail(current, "is owned by another user");
    if ((stat.mode & 0o022) !== 0 && (final || (stat.mode & 0o1000) === 0)) fail(current, "is writable by other users");
  }
  return directory;
};
