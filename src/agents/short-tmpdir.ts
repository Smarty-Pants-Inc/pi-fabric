import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/** smarty-dev#15878: the fleet bash guard binds `<canonical TMPDIR>/p.XXXXXX` and blocks the whole session's Bash
 * when that needs more than 60 UTF-8 bytes. A nested `<run dir>/tmp` child TMPDIR needed 81 under `/tmp/p.XXXXXX`. */
export const BASH_GUARD_TMPDIR_BYTES = 60;
const GUARD_CHILD = "/p.XXXXXX";

/** Where a child's short private TMPDIR may go, read at each spawn. */
export interface ShortTmpdirSource {
  /** The caller's own TMPDIR (Fabric's data root): the first candidate root. */
  tmpdir: string;
  /** smarty-role's SMARTY_SESSION_STORE. Its parent is the second candidate root, only while tmpdir lies inside it. */
  sessionStore?: string;
  /** Tests only: the guard's byte budget. */
  budgetBytes?: number;
}

export const shortTmpdirName = (runId: string): string =>
  `r.${createHash("sha256").update(runId).digest("hex").slice(0, 10)}`;

const canonical = (directory: string): string => {
  try { return fs.realpathSync.native(directory); } catch { return path.resolve(directory); }
};
const inside = (parent: string, child: string): boolean => {
  const relative = path.relative(parent, child);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
};

/** Candidate directories in order, with the bytes the guard's `p.XXXXXX` child needs under each. */
export const shortTmpdirCandidates = (runId: string, source: ShortTmpdirSource): { directory: string; bytes: number; fits: boolean }[] => {
  const budget = source.budgetBytes ?? BASH_GUARD_TMPDIR_BYTES;
  const tmpdir = canonical(source.tmpdir);
  const roots = [tmpdir];
  if (source.sessionStore && path.isAbsolute(source.sessionStore)) {
    const store = canonical(source.sessionStore);
    if (inside(store, tmpdir)) roots.push(path.dirname(store));
  }
  return [...new Set(roots)].map((root) => {
    const directory = path.join(root, shortTmpdirName(runId));
    const bytes = Buffer.byteLength(`${directory}${GUARD_CHILD}`, "utf8");
    return { directory, bytes, fits: bytes <= budget };
  });
};

/**
 * Create this run's short private TMPDIR (exclusive mkdir, exact 0700, ours) under the first private root of ours
 * that fits the guard budget. Undefined when none fits: the caller keeps its old TMPDIR (no lockout). Windows has
 * no fleet bash guard and keeps the old layout.
 */
export const allocateShortTmpdir = (runId: string, source: ShortTmpdirSource): string | undefined => {
  if (process.platform === "win32") return undefined;
  const uid = process.getuid?.();
  for (const candidate of shortTmpdirCandidates(runId, source)) {
    if (!candidate.fits) continue;
    try {
      const root = fs.lstatSync(path.dirname(candidate.directory));
      // Never a shared (group/other-writable) root such as /tmp, a link, or another user's directory.
      if (!root.isDirectory() || root.isSymbolicLink() || (root.mode & 0o022) !== 0 || (uid !== undefined && root.uid !== uid)) continue;
      fs.mkdirSync(candidate.directory, { mode: 0o700 }); // An existing entry is never adopted.
    } catch { continue; }
    try {
      fs.chmodSync(candidate.directory, 0o700);
      const stat = fs.lstatSync(candidate.directory);
      if (stat.isDirectory() && !stat.isSymbolicLink() && (stat.mode & 0o777) === 0o700 && (uid === undefined || stat.uid === uid)) {
        return candidate.directory;
      }
    } catch { /* not provably private: never handed out */ }
    try { fs.rmdirSync(candidate.directory); } catch { /* retained, never adopted */ }
  }
  return undefined;
};

/** This run's recorded short TMPDIR: exactly its own name, outside its run directory. */
export const recordedShortTmpdir = (runDirectory: string, runId: string | undefined, known?: unknown): string | undefined => {
  let value = known;
  if (value === undefined) {
    try { value = (JSON.parse(fs.readFileSync(path.join(runDirectory, "status.json"), "utf8")) as { temporaryDirectory?: unknown }).temporaryDirectory; }
    catch { return undefined; }
  }
  if (typeof runId !== "string" || typeof value !== "string" || !path.isAbsolute(value)) return undefined;
  if (path.basename(value) !== shortTmpdirName(runId) || inside(path.resolve(runDirectory), path.resolve(value))) return undefined;
  return value;
};

/** The judged stat of a real directory of ours; undefined when absent, a link, or another user's. */
export const ownedShortTmpdir = (directory: string): fs.Stats | undefined => {
  try {
    const stat = fs.lstatSync(directory);
    if (!stat.isDirectory() || stat.isSymbolicLink() || (process.getuid && stat.uid !== process.getuid())) return undefined;
    return stat;
  } catch { return undefined; }
};
