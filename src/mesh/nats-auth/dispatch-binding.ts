import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { withExclusiveFileLock, writeFileAtomic } from "../../core/atomic-write.js";
import { openPrivateDir } from "../../storage/private-dir.js";

export const DISPATCH_RUN_BINDING_TTL_MS = 7 * 24 * 60 * 60 * 1_000;
const MAX_BINDING_BYTES = 256;
const BINDING_FILE = /^[a-f0-9]{64}\.json$/;

export class DispatchBindingError extends Error {
  constructor(message: string) { super(message); this.name = "DispatchBindingError"; }
}

interface BindingRecord {
  format: 1 | 2;
  contentSha256: string;
  createdAt: number;
  stat: fs.Stats;
}

const errorCode = (error: unknown): string | undefined => (error as NodeJS.ErrnoException)?.code;
const sha256 = (value: string): string => createHash("sha256").update(value, "utf8").digest("hex");
const sameFile = (left: fs.Stats, right: fs.Stats): boolean => left.dev === right.dev && left.ino === right.ino;
const sameSnapshot = (left: fs.Stats, right: fs.Stats): boolean => sameFile(left, right) &&
  left.size === right.size && left.mtimeMs === right.mtimeMs && left.ctimeMs === right.ctimeMs;

const readBindingRecord = (file: string, uid: number): BindingRecord => {
  const before = fs.lstatSync(file);
  if (!before.isFile() || before.isSymbolicLink() || before.uid !== uid || before.nlink !== 1 ||
      (before.mode & 0o7777) !== 0o600 || before.size > MAX_BINDING_BYTES) throw new Error("unsafe binding file");
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.uid !== uid || stat.nlink !== 1 || (stat.mode & 0o7777) !== 0o600 ||
        !sameFile(before, stat) || stat.size !== before.size || stat.size > MAX_BINDING_BYTES) throw new Error("unsafe binding file");
    const stored = fs.readFileSync(fd, "utf8");
    if (Buffer.byteLength(stored, "utf8") !== stat.size) throw new Error("malformed binding file");
    const current = /^\{"format":2,"createdAt":(0|[1-9][0-9]*),"contentSha256":"([a-f0-9]{64})"\}\n$/u.exec(stored);
    if (current) {
      const createdAt = Number(current[1]);
      if (!Number.isSafeInteger(createdAt)) throw new Error("malformed binding file");
      return { format: 2, createdAt, contentSha256: current[2]!, stat };
    }
    const legacy = /^\{"format":1,"contentSha256":"([a-f0-9]{64})"\}\n$/u.exec(stored);
    if (legacy) return { format: 1, createdAt: stat.mtimeMs, contentSha256: legacy[1]!, stat };
    throw new Error("malformed binding file");
  } finally { fs.closeSync(fd); }
};

/** Expire old records under the store lock; the embedded creation time cannot be refreshed by touching a file. */
const pruneExpiredBindings = (directory: string, directoryFd: number, uid: number, now: number): void => {
  let removed = false;
  for (const name of fs.readdirSync(directory)) {
    if (!BINDING_FILE.test(name)) continue;
    const file = path.join(directory, name);
    let before: fs.Stats;
    try { before = fs.lstatSync(file); }
    catch (error) { if (errorCode(error) === "ENOENT") continue; throw error; }
    if (!before.isFile() || before.isSymbolicLink() || before.uid !== uid || before.nlink !== 1 ||
        (before.mode & 0o7777) !== 0o600) continue;
    let createdAt = before.mtimeMs;
    try { createdAt = readBindingRecord(file, uid).createdAt; }
    catch { /* A damaged record still fails closed on use; age out its exact unchanged inode below. */ }
    if (!Number.isFinite(createdAt) || now - createdAt < DISPATCH_RUN_BINDING_TTL_MS) continue;
    try {
      const current = fs.lstatSync(file);
      if (!sameSnapshot(before, current) || !current.isFile() || current.isSymbolicLink() || current.uid !== uid ||
          current.nlink !== 1 || (current.mode & 0o7777) !== 0o600) continue;
      fs.unlinkSync(file);
      removed = true;
    } catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
  }
  if (removed) fs.fsyncSync(directoryFd);
};

/** Persist one hash-only binding per run, scoped to the actual sender and private state-directory inode. */
export function bindDispatchRunContent(runId: string, task: string, targetId: string, senderId: string): void {
  const configuredStateHome = process.env.XDG_STATE_HOME?.trim() || path.join(os.homedir(), ".local", "state");
  if (!path.isAbsolute(configuredStateHome) || configuredStateHome.includes("\0")) {
    throw new DispatchBindingError("XDG_STATE_HOME must be an absolute path");
  }
  let directory: ReturnType<typeof openPrivateDir> | undefined;
  try {
    directory = openPrivateDir(path.join(configuredStateHome, "pi-fabric", "nats-dispatch"));
    const uid = process.getuid?.();
    if (directory.fd === undefined || uid === undefined) throw new Error("private binding state unavailable");
    const root = fs.fstatSync(directory.fd);
    if (!root.isDirectory() || root.uid !== uid || (root.mode & 0o7777) !== 0o700) throw new Error("unsafe binding directory");
    const stateDirectoryIdentity = { dev: root.dev, ino: root.ino, uid: root.uid };
    const contentSha256 = sha256(JSON.stringify([task, targetId, senderId, stateDirectoryIdentity]));
    const file = path.join(directory.path, `${sha256(runId)}.json`);
    const directoryPath = directory.path;
    const directoryFd = directory.fd;
    withExclusiveFileLock({ directory: directoryPath, lockName: "bindings.lock", timeoutMessage: "dispatch binding store is busy" }, () => {
      const now = Date.now();
      pruneExpiredBindings(directoryPath, directoryFd, uid, now);
      let existing: BindingRecord | undefined;
      try { existing = readBindingRecord(file, uid); }
      catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
      if (existing) {
        if (existing.format !== 2 || existing.contentSha256 !== contentSha256) {
          throw new DispatchBindingError("run id reused with different content");
        }
        // Confirm existing bytes before any command is sent; preserve the original bounded TTL.
        const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
        try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
        fs.fsyncSync(directoryFd);
        return;
      }
      const bytes = Buffer.from(`${JSON.stringify({ format: 2, createdAt: now, contentSha256 })}\n`, "utf8");
      writeFileAtomic(file, bytes, { durable: true, mode: 0o600, syncDirectory: () => fs.fsyncSync(directoryFd) });
    });
  } catch (error) {
    if (error instanceof DispatchBindingError) throw error;
    throw new DispatchBindingError("could not safely persist dispatch run binding");
  } finally { directory?.close(); }
}
