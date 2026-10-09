import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { MeshLockTimeoutError } from "../core/atomic-write.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "./custody-lock.js";
import { MeshLock } from "./mesh-lock.js";

/**
 * The migration fence (smarty-dev#6477, org decision 10-08): `custody.lock` then the mesh `.lock` (the
 * order of withMeshCustody). Every operation that installs or removes the moved marker or moves the
 * backend flag holds both for its whole section: the import, cutover and rollback (backend-migration.ts)
 * and a fixture's `initialize: "create"` (state-sqlite.ts). File-mode writers commit under `.lock` and
 * check the fence (assertFileStateWritable) right before their rename, so a writer either commits
 * before the fenced section (which then sees its state.json) or is refused after it.
 * A leaf module: state-sqlite.ts and backend-migration.ts both use it without importing each other.
 */
export interface MeshFenceOptions {
  /** The mesh `.lock` protocol. Default 1 (the migration tool's). */
  lockProtocol?: 1 | 2;
  /** Budget for each fence lock (`custody.lock`, then `.lock`). */
  lockTimeoutMs: number;
}

/** Holds the fence across awaits until the operation settles. */
export const holdMeshFence = async <T>(root: string, options: MeshFenceOptions,
  operation: (lock: MeshLock) => Promise<T> | T): Promise<T> => {
  const releaseCustody = await acquireMeshCustodyLock(root, options.lockTimeoutMs);
  try {
    const lock = new MeshLock(root, { lockProtocol: options.lockProtocol ?? 1, lockTimeoutMs: options.lockTimeoutMs }, () => undefined);
    return await lock.withLockAcrossAwait(async () => operation(lock), options.lockTimeoutMs, "other");
  } finally { releaseCustody(); }
};

const sleepSync = (ms: number): void => { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms); };
const codeOf = (error: unknown): string | undefined => (error as NodeJS.ErrnoException | undefined)?.code;

/** One attempt at `custody.lock` with its own wire (a complete owner published by rename). */
const tryCustody = (lock: string, token: string, record: string): boolean => {
  const staging = fs.mkdtempSync(`${lock}.pending.${token}.`);
  try {
    fs.writeFileSync(path.join(staging, "owner"), record, { encoding: "utf8", flag: "wx", mode: 0o600 });
    if (fs.lstatSync(lock, { throwIfNoEntry: false }) !== undefined) return false;
    try { fs.renameSync(staging, lock); return true; } catch (error) {
      if (["EEXIST", "ENOTEMPTY", "EPERM", "EACCES"].includes(codeOf(error) ?? "")) return false;
      throw error;
    }
  } finally { fs.rmSync(staging, { recursive: true, force: true }); }
};

/** One attempt at the mesh `.lock` with the protocol-1 wire (mkdir, then the owner record). */
const tryMeshLock = (lock: string, record: string): boolean => {
  try { fs.mkdirSync(lock, { mode: 0o700 }); } catch (error) { if (codeOf(error) === "EEXIST") return false; throw error; }
  try { fs.writeFileSync(path.join(lock, "owner"), record, { encoding: "utf8", flag: "wx", mode: 0o600 }); return true; }
  catch (error) {
    if (codeOf(error) === "EEXIST") return false; // a recoverer's successor owns it now: never clean it
    try { fs.rmdirSync(lock); } catch { /* not ours to clean any more */ }
    throw error;
  }
};

/** Detach an owned lock before removal, as both lock modules do: never delete a successor. */
const release = (lock: string, token: string, record: string): void => {
  try {
    if (fs.readFileSync(path.join(lock, "owner"), "utf8") !== record) return;
    const released = `${lock}.released.${token}`;
    fs.renameSync(lock, released);
    fs.rmSync(released, { recursive: true, force: true });
  } catch { /* already recovered or replaced */ }
};

/**
 * The fence was busy for a synchronous acquirer. It carries SQLITE_BUSY's errcode so a synchronous open's
 * caller retries it exactly as a busy database (state-backend.ts falls back to the asynchronous open).
 */
export class MeshFenceBusyError extends MeshLockTimeoutError {
  readonly errcode = 5;
}

/**
 * The same fence for a synchronous caller (`SqliteStateStore.openSync` with `initialize: "create"`): attempts
 * at each lock until `timeoutMs` (0: one attempt) with short synchronous sleeps, the locks' own wire formats,
 * no stale-owner recovery (a dead holder is recovered by the next asynchronous acquirer). A synchronous
 * waiter blocks the event loop, so it can never outwait an in-process holder: callers pass 0 and retry.
 */
export const holdMeshFenceSync = <T>(root: string, timeoutMs: number, operation: () => T): T => {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const deadline = Date.now() + Math.max(0, timeoutMs);
  const acquire = (lock: string, attempt: (token: string, record: string) => boolean): (() => void) => {
    const token = randomUUID();
    const record = `${token}\n${process.pid}\n${Date.now()}\n`;
    let attempts = 0;
    let maxGapMs = 0;
    let last = Date.now();
    for (;;) {
      const now = Date.now();
      if (attempts > 0) maxGapMs = Math.max(maxGapMs, now - last);
      attempts += 1;
      last = now;
      if (attempt(token, record)) return () => release(lock, token, record);
      if (Date.now() >= deadline) {
        let pid: string | undefined;
        try { pid = fs.readFileSync(path.join(lock, "owner"), "utf8").split("\n")[1]; } catch { /* ownerless or gone */ }
        throw new MeshFenceBusyError(` (${path.basename(lock)} ${lock}${pid ? `, held by pid ${pid}` : ""})`, attempts, maxGapMs);
      }
      sleepSync(Math.min(Math.max(1, deadline - Date.now()), 2 + Math.floor(Math.random() * 4)));
    }
  };
  const custody = path.join(root, MESH_CUSTODY_LOCK_NAME);
  const releaseCustody = acquire(custody, (token, record) => tryCustody(custody, token, record));
  try {
    const mesh = path.join(root, ".lock");
    const releaseMesh = acquire(mesh, (_token, record) => tryMeshLock(mesh, record));
    try { return operation(); } finally { releaseMesh(); }
  } finally { releaseCustody(); }
};
