/**
 * The durable records of one resident host generation (smarty-dev#7950): `<root>/generations/<token>/exit.json` and
 * `error.json` (the host's own failure, which used to go to the shared root `error.json`).
 *
 * The exit record: `<root>/generations/<token>/exit.json`,
 * where `token` is the generation's owner.json token. The owning host creates the directory while it holds the root;
 * the record is created once, with O_CREAT|O_EXCL, by whoever comes first: the host itself at its release (its own
 * reason), or else its launcher after the child exited without one (a crash, SIGKILL, or an exit before the report).
 *
 * The path names one generation, so writing it never changes a file of any other generation: a successor that owns
 * the root meanwhile finds its own files byte-unchanged, and no ownership check is needed before the write. O_EXCL
 * also makes the write safe without O_NOFOLLOW/O_NONBLOCK: an existing path of any kind (a planted symlink or FIFO, or
 * an earlier record) fails with EEXIST, so the record exists at most once and nothing is followed or blocked.
 */
import fs from "node:fs";
import path from "node:path";
import { verifiedRemove } from "../storage/verified-remove.js";

export interface ResidentGenerationExit {
  format: 1;
  /** The generation: its owner.json token. */
  generation: string;
  hostId?: string;
  pid?: number;
  processStartTime?: string;
  /** The host's own reason, or `unreported`: the child exited without writing one. */
  reason: "idle-exit" | "handover-release" | "stopped" | "error" | "unreported";
  writer: "host" | "launcher";
  code?: number | null;
  signal?: string | null;
  at: number;
}

const GENERATION = /^[A-Za-z0-9-]{8,64}$/;
/** Exit records older than this are pruned when a launcher starts a child. */
const RESIDENT_GENERATION_RETENTION_MS = 7 * 24 * 60 * 60 * 1_000;

const generationsDirectory = (root: string): string => path.join(root, "generations");
export const residentGenerationExitPath = (root: string, generation: string): string =>
  path.join(generationsDirectory(root), generation, "exit.json");

const ownDirectory = (directory: string): boolean => {
  const stat = fs.lstatSync(directory, { throwIfNoEntry: false });
  return !!stat?.isDirectory() && (process.getuid === undefined || stat.uid === process.getuid());
};

/** The owning host, at claim: its generation's directory (never a recursive mkdir through a planted link). */
export const createResidentGeneration = (root: string, generation: string): void => {
  if (!GENERATION.test(generation)) throw new Error("Resident generation token is malformed");
  for (const directory of [generationsDirectory(root), path.dirname(residentGenerationExitPath(root, generation))]) {
    try { fs.mkdirSync(directory, { mode: 0o700 }); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    if (!ownDirectory(directory)) throw new Error(`Resident generation directory is not an owned directory: ${directory}`);
  }
};

/** The host's own failure, in its generation (it used to be the shared root error.json). */
export interface ResidentGenerationError {
  format: 1;
  generation: string;
  error: string;
  occurredAt: number;
  launcherPid?: number;
  launcherBirth?: string;
}

type RecordName = "exit.json" | "error.json";

/**
 * Create one of the generation's records, once. False when it already exists, the generation directory is absent
 * (the host never claimed the root) or not an owned directory, or the write fails. Never throws: diagnostics never
 * block a release or a launcher exit.
 */
const writeRecord = (root: string, generation: string, name: RecordName, record: object): boolean => {
  if (!GENERATION.test(generation)) return false;
  const file = path.join(generationsDirectory(root), generation, name);
  let fd: number | undefined;
  try {
    if (!ownDirectory(generationsDirectory(root)) || !ownDirectory(path.dirname(file))) return false;
    fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    fs.writeSync(fd, JSON.stringify(record));
    fs.fsyncSync(fd);
    return true;
  } catch { return false; }
  finally { if (fd !== undefined) { try { fs.closeSync(fd); } catch { /* closed */ } } }
};

export const writeResidentGenerationExit = (root: string, record: Omit<ResidentGenerationExit, "format" | "at">): boolean =>
  writeRecord(root, record.generation, "exit.json", { format: 1, ...record, at: Date.now() } satisfies ResidentGenerationExit);

export const writeResidentGenerationError = (root: string, record: Omit<ResidentGenerationError, "format">): boolean =>
  writeRecord(root, record.generation, "error.json", { format: 1, ...record } satisfies ResidentGenerationError);

export const readResidentGenerationExit = (root: string, generation: string): ResidentGenerationExit | undefined => {
  try { return JSON.parse(fs.readFileSync(residentGenerationExitPath(root, generation), "utf8")) as ResidentGenerationExit; }
  catch { return undefined; }
};

/**
 * The generations' records named `name`, newest first (at most `limit`), for start diagnostics (client.ts). Unreadable
 * entries are skipped. ponytail: a readdir per call; pruning keeps the directory to about a week of generations.
 */
export const residentGenerationRecords = <T>(root: string, name: RecordName, limit = 1):
  Array<{ generation: string; text: string; record: T | undefined; mtimeMs: number }> => {
  const found: Array<{ generation: string; text: string; record: T | undefined; mtimeMs: number }> = [];
  try {
    for (const generation of fs.readdirSync(generationsDirectory(root))) {
      if (!GENERATION.test(generation)) continue;
      const file = path.join(generationsDirectory(root), generation, name);
      try {
        const stat = fs.lstatSync(file);
        if (!stat.isFile()) continue;
        const text = fs.readFileSync(file, "utf8");
        let record: T | undefined;
        try { record = JSON.parse(text) as T; } catch { record = undefined; }
        found.push({ generation, text: text.trim(), record, mtimeMs: stat.mtimeMs });
      } catch { /* absent or unreadable */ }
    }
  } catch { return []; }
  return found.sort((a, b) => b.mtimeMs - a.mtimeMs).slice(0, limit);
};

/**
 * Remove generation records whose exit.json is older than the retention, then their (empty) directory. A generation
 * without a record (still running, or its launcher died too) is kept. Diagnostics only; the next start retries.
 */
export const pruneResidentGenerations = (root: string, now = Date.now()): void => {
  try {
    const directory = generationsDirectory(root);
    if (!ownDirectory(directory)) return;
    for (const name of fs.readdirSync(directory)) {
      if (!GENERATION.test(name)) continue;
      const file = residentGenerationExitPath(root, name);
      const stat = fs.lstatSync(file, { throwIfNoEntry: false });
      if (!stat?.isFile() || now - stat.mtimeMs <= RESIDENT_GENERATION_RETENTION_MS) continue;
      verifiedRemove(file, stat, { deleter: "residentGenerations", fallback: () => { fs.rmSync(file, { force: true }); return true; } });
      try { fs.rmdirSync(path.dirname(file)); } catch { /* not empty or gone: kept */ }
    }
  } catch { /* diagnostics only */ }
};
