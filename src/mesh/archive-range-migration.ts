import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export const RETIRED_ARCHIVE_RANGES = "retired-range-sidecars";
// Only names emitted by the retired implementation. Lookalikes are not migration inputs.
const legacyName = (name: string): boolean => name === ".ranges.json" || /^\.range-[a-f0-9]{64}\.json$/.test(name);
const identity = (s: fs.BigIntStats, renamed = false): string =>
  [s.dev, s.ino, s.mode, s.nlink, s.size, s.mtimeNs, ...(renamed ? [] : [s.ctimeNs])].join(":");
const stat = (file: string): fs.BigIntStats | undefined => fs.lstatSync(file, { bigint: true, throwIfNoEntry: false });
const same = (file: string, expected: fs.BigIntStats): boolean => {
  const current = stat(file);
  return !!current && identity(current) === identity(expected);
};
const directory = (file: string): void => {
  if (!stat(file)?.isDirectory()) throw new Error(`Expected a real directory: ${file}`);
};
const syncDirectory = (file: string): void => {
  if (process.platform === "win32") return;
  const fd = fs.openSync(file, "r");
  try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
};
const digest = (file: string, expected: fs.BigIntStats): string => {
  if (expected.isSymbolicLink()) return `symlink:${fs.readlinkSync(file)}`;
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    if (identity(fs.fstatSync(fd, { bigint: true })) !== identity(expected)) throw new Error(`Sidecar changed during verification: ${file}`);
    const hash = createHash("sha256"), buffer = Buffer.allocUnsafe(64 * 1024);
    for (;;) {
      const count = fs.readSync(fd, buffer, 0, buffer.length, null);
      if (!count) break;
      hash.update(buffer.subarray(0, count));
    }
    if (identity(fs.fstatSync(fd, { bigint: true })) !== identity(expected)) throw new Error(`Sidecar changed during verification: ${file}`);
    return hash.digest("hex");
  } finally { fs.closeSync(fd); }
};
export interface RetiredArchiveRange { source: string; destination: string; identity: string; digest: string }

/** Explicit maintenance, NOT a read hook. Rename only; never unlink, overwrite,
 * repair or follow a sidecar symlink. Readers of event data can remain online.
 * Stop old sidecar-producing software first; see docs/archive-range-cache.md.
 * On any failed verification, stop and preserve every name for operator inspection.
 */
export function retireArchiveRangeSidecars(root: string): RetiredArchiveRange[] {
  root = path.resolve(root);
  directory(root);
  const moved: RetiredArchiveRange[] = [];
  const children = (parent: string, pattern: RegExp): string[] => fs.readdirSync(parent).filter(name => pattern.test(name) && stat(path.join(parent, name))?.isDirectory()).sort();
  for (const year of children(root, /^\d{4}$/)) for (const month of children(path.join(root, year), /^\d{2}$/)) {
    for (const day of children(path.join(root, year, month), /^\d{2}$/)) {
      const sourceDir = path.join(root, year, month, day), seal = path.join(sourceDir, "SEAL.json");
      const sealed = stat(seal);
      if (!sealed?.isFile()) continue; // No mutation in an unsealed day, nor via symlinked ancestry.
      let targetDir: string | undefined;
      for (const name of fs.readdirSync(sourceDir).filter(legacyName).sort()) {
        const source = path.join(sourceDir, name), before = stat(source);
        if (!before) continue; // Another migration may already have retired it.
        if (!before.isFile() && !before.isSymbolicLink()) throw new Error(`Unsupported legacy sidecar type: ${source}`);
        const checksum = digest(source, before);
        if (!targetDir) {
          const retired = path.join(root, RETIRED_ARCHIVE_RANGES);
          try { fs.mkdirSync(retired, { mode: 0o700 }); }
          catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
          directory(retired);
          // Exclusive unique parent: rename cannot replace a previous retired file.
          targetDir = fs.mkdtempSync(path.join(retired, `${year}-${month}-${day}-`));
          syncDirectory(root); syncDirectory(retired);
        }
        const destination = path.join(targetDir, name);
        directory(root); directory(path.join(root, year)); directory(path.join(root, year, month)); directory(sourceDir);
        directory(path.join(root, RETIRED_ARCHIVE_RANGES)); directory(targetDir);
        if (!same(seal, sealed) || !same(source, before) || stat(destination)) {
          throw new Error(`Archive changed before range retirement: ${source}`);
        }
        fs.renameSync(source, destination);
        // Rename may change ctime, never inode/device/mode/links/size/mtime or bytes.
        const after = stat(destination);
        if (!after || identity(after, true) !== identity(before, true) || digest(destination, after) !== checksum ||
            !same(destination, after) || stat(source) || !same(seal, sealed)) {
          throw new Error(`Range retirement verification failed; preserve and inspect ${source} and ${destination}`);
        }
        syncDirectory(targetDir); syncDirectory(sourceDir);
        moved.push({ source, destination, identity: identity(after), digest: checksum });
      }
    }
  }
  return moved;
}
