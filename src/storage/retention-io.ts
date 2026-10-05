import fs from "node:fs";
import type { FileHandle } from "node:fs/promises";
import { writeFileAtomic, writeFileAtomicAsync } from "../core/atomic-write.js";

// One custody policy, two drivers. Windows archive scans await every filesystem
// crossing instead of putting an unbounded native NTFS call on the RPC turn.
// The async driver is sequential: no fan-out, background jobs or carried queue.
type TreeHandle = number | FileHandle;
type TreeIO = { op: "stat" | "read" | "list" | "exists" | "open"; file: string }
  | { op: "handleStat" | "close"; handle: TreeHandle }
  | { op: "tail"; handle: TreeHandle; buffer: Buffer; offset: number; length: number; position: number }
  | { op: "write"; file: string; contents: Buffer }
  | { op: "times"; file: string; atime: number; mtime: number };
export type TreeWalk<T> = Generator<TreeIO, T, unknown>;
export function* treeStat(file: string): TreeWalk<fs.Stats> { return (yield { op: "stat", file }) as fs.Stats; }
export function* treeRead(file: string): TreeWalk<string> { return (yield { op: "read", file }) as string; }
export function* treeList(file: string): TreeWalk<string[]> { return (yield { op: "list", file }) as string[]; }
export function* treeExists(file: string): TreeWalk<boolean> { return (yield { op: "exists", file }) as boolean; }
export function* treeOpen(file: string): TreeWalk<TreeHandle> { return (yield { op: "open", file }) as TreeHandle; }
export function* treeHandleStat(handle: TreeHandle): TreeWalk<fs.Stats> { return (yield { op: "handleStat", handle }) as fs.Stats; }
export function* treeClose(handle: TreeHandle): TreeWalk<void> { yield { op: "close", handle }; }
export function* treeTail(handle: TreeHandle, buffer: Buffer, offset: number, length: number, position: number): TreeWalk<number> {
  return (yield { op: "tail", handle, buffer, offset, length, position }) as number;
}
export function* treeWrite(file: string, contents: Buffer): TreeWalk<void> { yield { op: "write", file, contents }; }
export function* treeTimes(file: string, atime: number, mtime: number): TreeWalk<void> { yield { op: "times", file, atime, mtime }; }

const syncIO = (request: TreeIO): unknown => {
  switch (request.op) {
    case "stat": return fs.lstatSync(request.file);
    case "read": return fs.readFileSync(request.file, "utf8");
    case "list": return fs.readdirSync(request.file);
    case "exists": return fs.existsSync(request.file);
    case "open": return fs.openSync(request.file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
    case "handleStat": return fs.fstatSync(request.handle as number);
    case "close": return fs.closeSync(request.handle as number);
    case "tail": return fs.readSync(request.handle as number, request.buffer, request.offset, request.length, request.position);
    case "write": return writeFileAtomic(request.file, request.contents);
    case "times": return fs.utimesSync(request.file, request.atime, request.mtime);
  }
};
const asyncIO = async (request: TreeIO): Promise<unknown> => {
  switch (request.op) {
    case "stat": return fs.promises.lstat(request.file);
    case "read": return fs.promises.readFile(request.file, "utf8");
    case "list": return fs.promises.readdir(request.file);
    case "exists": try { await fs.promises.lstat(request.file); return true; } catch { return false; }
    case "open": return fs.promises.open(request.file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
    case "handleStat": return (request.handle as FileHandle).stat();
    case "close": return (request.handle as FileHandle).close();
    case "tail": return (await (request.handle as FileHandle).read(request.buffer, request.offset, request.length, request.position)).bytesRead;
    case "write": return writeFileAtomicAsync(request.file, request.contents);
    case "times": return fs.promises.utimes(request.file, request.atime, request.mtime);
  }
};
export const inspectTreeSync = <T>(walk: TreeWalk<T>): T => {
  let step = walk.next();
  while (!step.done) {
    let value: unknown;
    try { value = syncIO(step.value); }
    catch (error) { step = walk.throw(error); continue; }
    step = walk.next(value);
  }
  return step.value;
};
export const inspectTreeAsync = async <T>(walk: TreeWalk<T>): Promise<T> => {
  let step = walk.next();
  while (!step.done) {
    let value: unknown;
    try { value = await asyncIO(step.value); }
    catch (error) { step = walk.throw(error); continue; }
    step = walk.next(value);
  }
  return step.value;
};
