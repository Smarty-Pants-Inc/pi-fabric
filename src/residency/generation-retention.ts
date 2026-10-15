import fs from "node:fs";
import path from "node:path";
import { countRefusal, verifiedRemove } from "../storage/verified-remove.js";

/** Generation diagnostics are useful while recent; keep older files for seven days. */
export const RESIDENT_GENERATION_RECORD_RETENTION_MS = 7 * 24 * 60 * 60 * 1_000;
const DELETER = "residentGenerationRecords";
const TOKEN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const RECORD = /^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.(?:exit|launcher-exit|error)\.json$/;
const privateOwned = (stat: fs.Stats | undefined): stat is fs.Stats => !!stat && typeof process.getuid === "function" &&
  stat.uid === process.getuid() && (stat.mode & 0o022) === 0;
const readOwnerToken = (root: string): string | undefined => {
  const file = path.join(root, "owner.json");
  const before = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!before) return undefined;
  if (!before.isFile() || before.nlink !== 1 || !privateOwned(before)) throw new Error("unsafe owner.json");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  try {
    const held = fs.fstatSync(fd);
    if (!held.isFile() || held.nlink !== 1 || !privateOwned(held) || held.dev !== before.dev || held.ino !== before.ino || held.size > 64 * 1024) {
      throw new Error("owner.json changed");
    }
    const owner = JSON.parse(fs.readFileSync(fd, "utf8")) as Record<string, unknown>;
    if (typeof owner.token !== "string" || !TOKEN.test(owner.token)) throw new Error("owner.json token is invalid");
    const after = fs.lstatSync(file);
    if (after.dev !== held.dev || after.ino !== held.ino || after.size !== held.size || after.mtimeMs !== held.mtimeMs || after.ctimeMs !== held.ctimeMs) {
      throw new Error("owner.json changed while read");
    }
    return owner.token;
  } finally { fs.closeSync(fd); }
};

/** Preview stale generation records under the caller's proven host.lock fence. This helper can never delete. */
export const sweepResidentGenerationRecords = (root: string, now: number, veto: () => boolean, dryRun: true,
  onPreview: (change: { path: string; beforeBytes: number; afterBytes: 0 }) => void): void => {
  if (!dryRun || process.platform !== "linux") return;
  const generations = path.join(root, "generations");
  let ownerToken: string | undefined;
  let rootStat: fs.Stats;
  let dirStat: fs.Stats;
  try {
    rootStat = fs.lstatSync(root);
    if (!rootStat.isDirectory() || !privateOwned(rootStat)) return;
    ownerToken = readOwnerToken(root);
    dirStat = fs.lstatSync(generations, { throwIfNoEntry: false })!;
    if (!dirStat) return;
    if (!dirStat.isDirectory() || !privateOwned(dirStat)) {
      countRefusal(DELETER, "generations directory is not private and owned", generations);
      return;
    }
  } catch {
    countRefusal(DELETER, "generation ownership or path is unreadable", generations);
    return;
  }

  let names: string[];
  try { names = fs.readdirSync(generations); }
  catch { countRefusal(DELETER, "generations directory is unreadable", generations); return; }
  for (const name of names) {
    const match = RECORD.exec(name);
    if (!match || match[1] === ownerToken) continue;
    const target = path.join(generations, name);
    let judged: fs.Stats | undefined;
    try { judged = fs.lstatSync(target); }
    catch { countRefusal(DELETER, "generation record is unreadable", target); continue; }
    if (!judged.isFile() || judged.isSymbolicLink() || judged.nlink !== 1 || !privateOwned(judged)) {
      countRefusal(DELETER, !judged.isFile() || judged.isSymbolicLink() ? "generation record is not a regular file" :
        judged.nlink !== 1 ? "generation record has multiple links" : "generation record is not private and owned", target,
      judged.isFile() ? judged.size : 0);
      continue;
    }
    if (judged.mtimeMs > now - RESIDENT_GENERATION_RECORD_RETENTION_MS) continue;
    // Selection is revalidated under the existing lock fence immediately before handing the judged inode to the helper.
    if (veto()) return;
    try {
      const currentToken = readOwnerToken(root);
      if (currentToken !== ownerToken || currentToken === match[1]) return;
      const currentRoot = fs.lstatSync(root), currentDir = fs.lstatSync(generations), currentFile = fs.lstatSync(target);
      if (currentRoot.dev !== rootStat.dev || currentRoot.ino !== rootStat.ino || currentDir.dev !== dirStat.dev || currentDir.ino !== dirStat.ino ||
          currentFile.dev !== judged.dev || currentFile.ino !== judged.ino || currentFile.mtimeMs !== judged.mtimeMs) continue;
      if (veto()) return;
      const result = verifiedRemove(target, judged, { deleter: DELETER, dryRun: true });
      if (result.removed) onPreview({ path: target, beforeBytes: result.bytes, afterBytes: 0 });
    } catch { countRefusal(DELETER, "generation selection changed or became unreadable", target, judged.size); }
  }
};
