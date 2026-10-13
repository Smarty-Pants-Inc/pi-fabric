import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { withExclusiveFileLock } from "../core/atomic-write.js";

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

/** Manager creates this sibling before launch; a shell's writable run never owns it. */
export const withTaskControlDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T => {
  const directory = taskControlDirectory(runDirectory);
  if (fs.realpathSync(directory) !== directory) throw new FabricTaskControlDirectoryError("Task control directory must not traverse a link");
  const fd = fs.openSync(directory, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isDirectory() || (process.getuid && stat.uid !== process.getuid()) || (process.platform !== "win32" && (stat.mode & 0o077) !== 0)) {
      throw new FabricTaskControlDirectoryError("Task control directory must be an owned private directory");
    }
    // Linux Landlock grants bind inodes. Relative operations through this fd
    // cannot be redirected by a pathname replacement after the trusted open.
    return operation(process.platform === "linux" ? `/proc/self/fd/${fd}` : directory, fd);
  } finally { fs.closeSync(fd); }
};

const readReceipt = (directory: string, runId: string): FinalAnswerReceipt | undefined => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(path.join(directory, "final-answer.json"), fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.nlink !== 1 || stat.size > MAX_FINAL_ANSWER_BYTES) return;
    const value = JSON.parse(fs.readFileSync(fd, "utf8"));
    if (value?.version === 1 && value.runId === runId && typeof value.id === "string" && value.id.length > 0 &&
        Number.isSafeInteger(value.recordedAt) && value.recordedAt > 0 && typeof value.text === "string") return value;
  } catch { /* Missing or invalid bounded receipts never establish finality. */ }
  finally { if (fd !== undefined) fs.closeSync(fd); }
  return undefined;
};
export const readFinalAnswerReceipt = (runDirectory: string, runId: string): FinalAnswerReceipt | undefined => {
  try { return withTaskControlDirectory(runDirectory, directory => readReceipt(directory, runId)); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
};

const locked = <T>(directory: string, operation: () => T): T => withExclusiveFileLock({
  directory, lockName: ".task-final-answer.lock", attempts: 200, delayMs: 5,
  timeoutMessage: "Task final-answer publication/injection lock timed out",
}, operation);
/** Reader and lock use the SAME pinned host-owned directory. */
export const withTaskFinalAnswerLock = <T>(runDirectory: string, operation: (read: (runId: string) => FinalAnswerReceipt | undefined) => T): T =>
  withTaskControlDirectory(runDirectory, directory => locked(directory, () => operation(runId => readReceipt(directory, runId))));

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
        if (fs.existsSync(file)) return false; // never replace an existing result
        fs.renameSync(temporary, file); // no serialization, payload read or fsync under this lock
        return true;
      });
      if (process.platform !== "win32") fs.fsyncSync(directoryFd);
      const result = published ? receipt : readReceipt(directory, runId);
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
