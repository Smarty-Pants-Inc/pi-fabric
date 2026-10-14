import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execFileSync } from "node:child_process";
import { debuglog } from "node:util";
import type { FabricWritePolicy } from "../agents/write-guard.js";
import { randomUUID } from "node:crypto";
import { withExclusiveFileLock } from "../core/atomic-write.js";
import { openPrivateDir, writeArchiveFile } from "../storage/private-dir.js";

export interface FinalAnswerReceipt {
  version: 1;
  id: string;
  runId: string;
  recordedAt: number;
  text: string;
}

export const MAX_FINAL_ANSWER_BYTES = 8 * 1024 * 1024;
export const taskControlDirectory = (runDirectory: string): string => path.resolve(runDirectory) + ".ctl";
export const taskSteerFile = (runDirectory: string): string => path.join(taskControlDirectory(runDirectory), "steer.jsonl");

export class FabricTaskControlDirectoryError extends Error {
  override readonly name = "FabricTaskControlDirectoryError";
  readonly code = "FABRIC_TASK_CONTROL_UNSAFE";
}

const withDirectory = <T>(directory: string, privateMode: boolean, operation: (directory: string, fd: number) => T): T => {
  const before = fs.lstatSync(directory);
  if (before.isSymbolicLink() || fs.realpathSync(directory) !== path.resolve(directory)) throw new FabricTaskControlDirectoryError("Task control directory must not traverse a link");
  const fd = fs.openSync(directory, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isDirectory() || stat.dev !== before.dev || stat.ino !== before.ino || (process.getuid && stat.uid !== process.getuid()) ||
        (process.platform !== "win32" && (stat.mode & (privateMode ? 0o077 : 0o022)) !== 0)) {
      throw new FabricTaskControlDirectoryError("Task control directory must be an owned private directory");
    }
    // Paths through a held fd are synchronous-only; never retain one for an ACK.
    return operation(process.platform === "linux" ? `/proc/self/fd/${fd}` : directory, fd);
  } finally { fs.closeSync(fd); }
};
/** Strict WRITE/lock namespace. A reader never creates or repairs this sibling. */
export const withTaskControlDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T =>
  withDirectory(taskControlDirectory(runDirectory), true, operation);

/** Check every intermediate directory too, not only the no-follow file leaf. */
const withControlPath = <T>(directory: string, relative: string, create: boolean, operation: (file: string, parent: string, fd: number) => T): T => {
  const parts = relative.split(/[\\/]/);
  if (path.isAbsolute(relative) || parts.some(part => !part || part === "." || part === "..")) throw new FabricTaskControlDirectoryError("Invalid task control relative path");
  const walk = (parent: string, index: number): T => {
    const fd = fs.openSync(index === 0 ? parent + path.sep + "." : parent, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
    try {
      const stat = fs.fstatSync(fd);
      if (!stat.isDirectory() || (process.getuid && stat.uid !== process.getuid()) || (process.platform !== "win32" && (stat.mode & 0o022))) {
        throw new FabricTaskControlDirectoryError("Unsafe task control subdirectory");
      }
      const pinned = process.platform === "linux" ? `/proc/self/fd/${fd}` : parent;
      if (index === parts.length - 1) return operation(path.join(pinned, parts[index]!), pinned, fd);
      const child = path.join(pinned, parts[index]!);
      if (create) {
        try { fs.mkdirSync(child, { mode: 0o700 }); fs.chmodSync(child, 0o700); if (process.platform !== "win32") fs.fsyncSync(fd); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      }
      return walk(child, index + 1);
    } finally { fs.closeSync(fd); }
  };
  return walk(directory, 0);
};
const siblingExists = (runDirectory: string): boolean => {
  try { fs.lstatSync(taskControlDirectory(runDirectory)); return true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
};
export const readTaskRunStatus = <T>(runDirectory: string): T => withDirectory(path.resolve(runDirectory), false,
  directory => JSON.parse(readBoundedControlFile(path.join(directory, "status.json"), 1024 * 1024)) as T);
/** Status is advisory: it may EXCLUDE old runs, never authorize fallback under a sibling. */
const legacyRecord = (runDirectory: string): boolean => {
  const value = readTaskRunStatus<Record<string, unknown>>(runDirectory);
  // Old retention records may omit route identity, and archives keep the
  // original id under an alias. This selects READ compatibility only; native
  // exit/identity predicates still decide collection. A layout field always
  // excludes fallback, and an existing sibling requires host-owned authority.
  return !!value && typeof value === "object" && !Array.isArray(value) &&
    ["running", "completed", "failed", "stopped", "timed_out"].includes(String(value.status)) && !("taskControlLayout" in value);
};
const legacyIdentity = (runDirectory: string): { version: 1; runId: string; dev: string; ino: string } => {
  const stat = fs.lstatSync(runDirectory, { bigint: true });
  return { version: 1, runId: path.basename(runDirectory), dev: String(stat.dev), ino: String(stat.ino) };
};
const hasLegacyAuthority = (runDirectory: string, directory: string): boolean => {
  try {
    const value = JSON.parse(readBoundedControlFile(path.join(directory, ".legacy-read.json"), MAX_TASK_CONTROL_BYTES));
    return JSON.stringify(value) === JSON.stringify(legacyIdentity(runDirectory));
  } catch { return false; }
};
/** Exported selection for retention. Validate even when the selected file is missing. */
export const taskControlReadDirectory = (runDirectory: string): string => {
  if (siblingExists(runDirectory)) return withTaskControlDirectory(runDirectory, () => taskControlDirectory(runDirectory));
  if (legacyRecord(runDirectory)) return path.resolve(runDirectory);
  throw new FabricTaskControlDirectoryError("Missing protected task control directory; legacy fallback refused");
};
export const withTaskControlReadDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T => {
  const directory = taskControlReadDirectory(runDirectory);
  return withDirectory(directory, directory !== path.resolve(runDirectory), operation);
};
/** Existing legacy rows remain readable after host recovery first creates the sibling. */
export const withLegacyTaskControlDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T | undefined => {
  const allowed = siblingExists(runDirectory)
    ? withTaskControlDirectory(runDirectory, directory => hasLegacyAuthority(runDirectory, directory)) : legacyRecord(runDirectory);
  return allowed ? withDirectory(path.resolve(runDirectory), false, operation) : undefined;
};
/** Host bootstrap only. Never recreate a missing sibling for a marked/protected run. */
export const initializeTaskControlDirectory = (runDirectory: string): void => {
  if (siblingExists(runDirectory)) { withTaskControlDirectory(runDirectory, () => undefined); return; }
  let legacy = false;
  try {
    legacy = legacyRecord(runDirectory);
    if (!legacy) throw new FabricTaskControlDirectoryError("Missing protected task control directory; refusing recreation");
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  const control = taskControlDirectory(runDirectory), parent = openPrivateDir(path.dirname(control), path.dirname(control));
  try {
    const child = path.join(parent.path, path.basename(control));
    let created = false;
    try { fs.mkdirSync(child, { mode: 0o700 }); created = true; fs.chmodSync(child, 0o700); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    withTaskControlDirectory(runDirectory, (_directory, fd) => { if (process.platform !== "win32") fs.fsyncSync(fd); });
    if (created && legacy) writeTaskControlJson(runDirectory, ".legacy-read.json", legacyIdentity(runDirectory));
    if (parent.fd !== undefined) fs.fsyncSync(parent.fd);
  } finally { parent.close(); }
};
/** Atomic publication stays on the held namespace; no realpath escape or /proc ancestor fsync. */
export const writeTaskControlJson = (runDirectory: string, relative: string, value: unknown): void => withTaskControlDirectory(runDirectory, directory =>
  withControlPath(directory, relative, true, (_file, parent, fd) => {
    const bytes = Buffer.from(JSON.stringify(value));
    if (bytes.length > MAX_TASK_CONTROL_BYTES) throw new FabricTaskControlDirectoryError("Task control file exceeds its byte bound");
    if (!writeArchiveFile({ path: parent, fd, close() {} }, path.basename(relative), { bytes }, true)) {
      throw new FabricTaskControlDirectoryError("Refusing replacement of unsafe task control file");
    }
  }));

/** Cleanup never mutates a legacy source and never follows a linked subdirectory. */
export const removeTaskControlFile = (runDirectory: string, relative: string): void => withTaskControlDirectory(runDirectory, directory => {
  try { withControlPath(directory, relative, false, file => {
    const fd = openTaskControlFile(file); fs.closeSync(fd);
    fs.rmSync(file);
  }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
});

/** Run-root readers skip ONLY a validated sibling with a surviving owned run. */
export const isTaskControlDirectoryEntry = (root: string, name: string): boolean => {
  if (!name.endsWith(".ctl")) return false;
  if (name.slice(0, -4).endsWith(".ctl")) throw new FabricTaskControlDirectoryError("A task control directory cannot admit another control sibling");
  const run = path.join(root, name.slice(0, -4));
  withDirectory(path.resolve(run), false, () => undefined);
  withTaskControlDirectory(run, () => undefined);
  return true;
};

const taskControlDebug = debuglog("pi-fabric:task-control");
let unconfinedOverlapLogged = false;
/** Check the control parent too: its grant permits renaming the protected sibling. */
export const assertTaskControlWriteRoots = (runDirectory: string, cwd: string, policy?: FabricWritePolicy): void => {
  const physical = (value: string): string => {
    const resolved = path.resolve(value);
    let ancestor = resolved;
    for (;;) {
      try { fs.lstatSync(ancestor); break; }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      const parent = path.dirname(ancestor);
      if (parent === ancestor) throw new FabricTaskControlDirectoryError("Cannot resolve task control grant");
      ancestor = parent;
    }
    return path.join(fs.realpathSync(ancestor), path.relative(ancestor, resolved));
  };
  const control = physical(taskControlDirectory(runDirectory)), parent = path.dirname(control);
  const roots = [...(policy?.readOnly ? [] : policy?.writableRoots ?? []), cwd, runDirectory, path.join(runDirectory, "tmp"),
    ...[".cache", ".npm", ".bun"].map(name => path.join(os.homedir(), name)), "/dev/null", "/dev/tty", "/dev/pts"];
  try { roots.push(execFileSync("git", ["rev-parse", "--path-format=absolute", "--git-common-dir"], { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim()); }
  catch { /* A non-Git cwd has no common-Git write grant. */ }
  const inside = (root: string, target: string): boolean => target === root || target.startsWith(root.endsWith(path.sep) ? root : root + path.sep);
  for (const root of roots) {
    const writable = physical(root);
    if (inside(writable, parent) || inside(control, writable)) {
      if (!policy) {
        // Preserve reviewed #11308: undefined policy is unconfined same-uid
        // authority, not an implicit write policy. Relocation cannot isolate it.
        if (!unconfinedOverlapLogged) {
          unconfinedOverlapLogged = true;
          taskControlDebug(`unconfined task control directory overlaps ${root}; accepted residual smarty-dev#11308`);
        }
        return;
      }
      throw new FabricTaskControlDirectoryError(
        `Task control directory ${control} or its rename-capable parent overlaps writable root ${root}; choose a runRoot outside task write grants`);
    }
  }
};

export const MAX_TASK_CONTROL_BYTES = 64 * 1024;
export const MAX_TASK_CONTROL_ROWS = 256;
/** Shared journal/receipt/registry validation; never open a FIFO in blocking mode. */
export const taskControlFileStat = (fd: number): fs.BigIntStats => {
  const stat = fs.fstatSync(fd, { bigint: true });
  if (!stat.isFile() || stat.nlink !== 1n || (process.getuid && stat.uid !== BigInt(process.getuid()))) {
    throw new FabricTaskControlDirectoryError("Task control file must be an owned regular file with one link");
  }
  return stat;
};
export const openTaskControlFile = (file: string): number => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
  try { taskControlFileStat(fd); return fd; }
  catch (error) { fs.closeSync(fd); throw error; }
};
const readBoundedControlFile = (file: string, maxBytes: number): string => {
  const fd = openTaskControlFile(file);
  try {
    if (taskControlFileStat(fd).size > BigInt(maxBytes)) throw new FabricTaskControlDirectoryError("Task control file exceeds its byte bound");
    // A regular file can grow after fstat. Read at most the bound plus one byte,
    // not readFileSync(fd), which follows an attacker-controlled growing EOF.
    const chunks: Buffer[] = [], buffer = Buffer.alloc(16 * 1024);
    let bytes = 0;
    for (;;) {
      const length = fs.readSync(fd, buffer, 0, Math.min(buffer.length, maxBytes + 1 - bytes), bytes);
      if (!length) break;
      bytes += length;
      if (bytes > maxBytes) throw new FabricTaskControlDirectoryError("Task control file exceeds its byte bound");
      chunks.push(Buffer.from(buffer.subarray(0, length)));
    }
    return Buffer.concat(chunks, bytes).toString("utf8");
  } finally { fs.closeSync(fd); }
};
export const withTaskControlSubdirectory = <T>(directory: string, relative: string, operation: (directory: string, fd: number) => T): T =>
  withControlPath(directory, path.join(relative, ".entry"), false, (_file, parent, fd) => operation(parent, fd));
/** The returned FILE fd remains valid after the directory callback closes; no fd path escapes. */
export const openLegacyTaskControlFile = (runDirectory: string, relative: string): number | undefined =>
  withLegacyTaskControlDirectory(runDirectory, directory => withControlPath(directory, relative, false, openTaskControlFile));
export const openTaskControlReadFile = (runDirectory: string, relative: string): number => {
  const open = (directory: string): number => withControlPath(directory, relative, false, openTaskControlFile);
  try { return withTaskControlReadDirectory(runDirectory, open); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    const legacy = withLegacyTaskControlDirectory(runDirectory, open);
    if (legacy !== undefined) return legacy;
    throw error;
  }
};
export const readTaskControlJsonAt = <T>(directory: string, relative: string, maxBytes = MAX_TASK_CONTROL_BYTES): T | undefined => {
  try { return JSON.parse(withControlPath(directory, relative, false, file => readBoundedControlFile(file, maxBytes))) as T; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
};
export const readTaskControlFile = (runDirectory: string, relative: string, maxBytes = MAX_TASK_CONTROL_BYTES): string | undefined => {
  const read = (directory: string): string => withControlPath(directory, relative, false, file => readBoundedControlFile(file, maxBytes));
  try { return withTaskControlReadDirectory(runDirectory, read); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    try { return withLegacyTaskControlDirectory(runDirectory, read); }
    catch (legacyError) { if ((legacyError as NodeJS.ErrnoException).code === "ENOENT") return; throw legacyError; }
  }
};
export const readTaskControlJson = <T>(runDirectory: string, relative: string, maxBytes = MAX_TASK_CONTROL_BYTES): T | undefined => {
  const contents = readTaskControlFile(runDirectory, relative, maxBytes);
  return contents === undefined ? undefined : JSON.parse(contents) as T;
};
const readReceipt = (directory: string, runId: string, legacyRun?: string): FinalAnswerReceipt | undefined => {
  try {
    const value = JSON.parse(readBoundedControlFile(path.join(directory, "final-answer.json"), MAX_FINAL_ANSWER_BYTES));
    if (value?.version === 1 && value.runId === runId && typeof value.id === "string" && value.id.length > 0 &&
        Number.isSafeInteger(value.recordedAt) && value.recordedAt > 0 && typeof value.text === "string") return value;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" && legacyRun && hasLegacyAuthority(legacyRun, directory)) {
      return withDirectory(path.resolve(legacyRun), false, legacy => readReceipt(legacy, runId));
    }
    /* Missing or invalid bounded receipts never establish finality. */
  }
  return undefined;
};
export const readFinalAnswerReceipt = (runDirectory: string, runId: string): FinalAnswerReceipt | undefined => {
  try {
    const value = readTaskControlJson<FinalAnswerReceipt>(runDirectory, "final-answer.json", MAX_FINAL_ANSWER_BYTES);
    if (value?.version === 1 && value.runId === runId && typeof value.id === "string" && value.id.length > 0 &&
        Number.isSafeInteger(value.recordedAt) && value.recordedAt > 0 && typeof value.text === "string") return value;
  } catch (error) {
    if (error instanceof FabricTaskControlDirectoryError && !/byte bound|regular file/.test(error.message)) throw error;
    // Malformed/unsafe receipts never establish finality or authorize fallback.
  }
  return undefined;
};

const locked = <T>(directory: string, operation: () => T): T => withExclusiveFileLock({
  directory, lockName: ".task-final-answer.lock", attempts: 200, delayMs: 5,
  timeoutMessage: "Task final-answer publication/injection lock timed out",
}, operation);
/** Reader and lock use the SAME pinned host-owned directory. */
export const withTaskFinalAnswerLock = <T>(runDirectory: string, operation: (read: (runId: string) => FinalAnswerReceipt | undefined) => T): T =>
  withTaskControlDirectory(runDirectory, directory => locked(directory, () => operation(runId => readReceipt(directory, runId, runDirectory))));

/** Serialize, size-check and fsync staging BEFORE the publication/injection lock. */
export const saveFinalAnswerReceipt = (runDirectory: string, runId: string, text: string): FinalAnswerReceipt => {
  if (Buffer.byteLength(text) > MAX_FINAL_ANSWER_BYTES) throw new RangeError("Final answer exceeds the 8 MiB receipt bound");
  const receipt: FinalAnswerReceipt = { version: 1, id: randomUUID(), runId, recordedAt: Date.now(), text };
  const contents = JSON.stringify(receipt);
  if (Buffer.byteLength(contents) > MAX_FINAL_ANSWER_BYTES) throw new RangeError("Final answer exceeds the 8 MiB receipt bound");
  return withTaskControlDirectory(runDirectory, (directory, directoryFd) => {
    const temporary = path.join(directory, `.final-answer.${process.pid}.${receipt.id}.tmp`);
    const fd = fs.openSync(temporary, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    try { fs.writeFileSync(fd, contents); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    try {
      const published = locked(directory, () => {
        const file = path.join(directory, "final-answer.json");
        if (fs.lstatSync(file, { throwIfNoEntry: false }) || (hasLegacyAuthority(runDirectory, directory) &&
            withDirectory(path.resolve(runDirectory), false, legacy => !!fs.lstatSync(path.join(legacy, "final-answer.json"), { throwIfNoEntry: false })))) return false; // never replace an existing result, including legacy
        fs.renameSync(temporary, file); // no serialization, payload read or fsync under this lock
        return true;
      });
      if (process.platform !== "win32") fs.fsyncSync(directoryFd);
      const result = published ? receipt : readReceipt(directory, runId, runDirectory);
      if (!result) throw new Error("Invalid or mismatched final-answer receipt already exists");
      return result;
    } finally { fs.rmSync(temporary, { force: true }); }
  });
};

/** Text alongside tool calls is progress, not a final answer; truncation/retry isn't success. */
export const finalAssistantText = (message: unknown): string | undefined => {
  if (!message || typeof message !== "object") return undefined;
  const value = message as { role?: unknown; stopReason?: unknown; content?: unknown };
  if (value.role !== "assistant" || value.stopReason !== "stop" || !Array.isArray(value.content) ||
      value.content.some(block => block?.type === "toolCall")) return undefined;
  return value.content.filter(block => block?.type === "text" && typeof block.text === "string")
    .map(block => block.text).join("");
};
