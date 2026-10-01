import fs from "node:fs";
import os from "node:os";
import path from "node:path";

/** File data only: socket/pipe paths must keep their short transport-specific roots. */
export const fabricDataRoot = (): string => {
  const root = process.env.PI_FABRIC_TMPDIR;
  if (!root) return os.tmpdir();
  if (!path.isAbsolute(root)) throw new Error("PI_FABRIC_TMPDIR must be an absolute path");
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  return root;
};
