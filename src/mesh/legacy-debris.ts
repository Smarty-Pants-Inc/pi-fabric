import fs from "node:fs";
import path from "node:path";
import type { WindowsMeshRecoveryEntry, WindowsMeshRecoveryOutcome } from "../storage/windows-mesh-recovery.js";

/** Protected canonical state, not contention and never permission for an application retry or recursive cleanup. */
export class MeshLegacyDebrisUnrecoverableError extends Error {
  override readonly name = "MeshLegacyDebrisUnrecoverableError";
  readonly code = "FABRIC_MESH_LEGACY_DEBRIS_UNRECOVERABLE" as const;
  readonly kind = "legacy-debris-unrecoverable" as const;
  readonly retryable = false as const;
  constructor(readonly lock: string, readonly path: string, readonly category: string) {
    super(`FABRIC_MESH_LEGACY_DEBRIS_UNRECOVERABLE: ${lock}: ${path} (${category}); ` +
      "preserved; quiesce and join affected writers before separately authorized forensic quarantine");
  }
}

const fail = (lock: string, file: string, category: string): never => {
  throw new MeshLegacyDebrisUnrecoverableError(lock, file, category);
};
const errno = (error: unknown): string | undefined => (error as NodeJS.ErrnoException)?.code;
const same = (a: fs.BigIntStats, b: fs.BigIntStats): boolean =>
  a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs;
const MAX_ENTRIES = 64; // includes top L
const MAX_DEPTH = 4;
const MAX_RECORD = 4096;
const released = (name: string): boolean => /^owner\.released\.[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(name) && name.startsWith("owner.released.");
const retired = (name: string): boolean => /^r\..+/.test(name) && !/[\x00-\x1f<>:"/\\|?*]|[. ]$/.test(name);

/** Bounded enumeration, not readdir followed by a cap on an already unbounded array. Never enumerates siblings. */
export const meshRecoveryChildren = (lock: string, directory = lock): string[] => {
  let handle: fs.Dir | undefined;
  try {
    const stat = fs.lstatSync(directory, { bigint: true });
    if (!stat.isDirectory() || stat.isSymbolicLink()) fail(lock, directory, "link-or-nondirectory");
    handle = fs.opendirSync(directory);
    const names: string[] = [];
    for (;;) {
      const entry = handle.readSync();
      if (!entry) return names.sort();
      names.push(entry.name);
      if (names.length >= MAX_ENTRIES) fail(lock, directory, "too-many-entries");
    }
  } catch (error) {
    if (error instanceof MeshLegacyDebrisUnrecoverableError) throw error;
    return fail(lock, directory, `inaccessible:${errno(error) ?? "unknown"}`);
  } finally {
    try { handle?.closeSync(); }
    catch (error) { fail(lock, directory, `inaccessible:${errno(error) ?? "unknown"}`); }
  }
};

export interface MeshRecoveryOwner { readonly stat: fs.BigIntStats; readonly record: string }

/** Exact initial S and full R, bounded and no-follow. ENOENT alone means absent; empty/torn is still a record.
 * Native subsequently verifies these SAME snapshots under its directory guard; no late resnapshot grants authority. */
export const readMeshRecoveryOwner = (lock: string, file = path.join(lock, "owner")): MeshRecoveryOwner | undefined => {
  let fd: number | undefined;
  try {
    let stat: fs.BigIntStats;
    try { stat = fs.lstatSync(file, { bigint: true }); }
    catch (error) { if (errno(error) === "ENOENT") return undefined; throw error; }
    if (stat.isSymbolicLink() || !stat.isFile()) fail(lock, file, "link-or-nonrecord");
    if (stat.size > BigInt(MAX_RECORD)) fail(lock, file, "oversize-record");
    fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
    const held = fs.fstatSync(fd, { bigint: true });
    if (!held.isFile() || !same(stat, held)) fail(lock, file, "record-changed");
    const bytes = Buffer.alloc(MAX_RECORD + 1);
    let length = 0;
    while (length < bytes.length) {
      const n = fs.readSync(fd, bytes, length, bytes.length - length, null);
      if (n === 0) break;
      length += n;
    }
    if (length > MAX_RECORD) fail(lock, file, "oversize-record");
    if (BigInt(length) !== stat.size || !same(stat, fs.fstatSync(fd, { bigint: true }))) fail(lock, file, "record-changed");
    const data = bytes.subarray(0, length);
    const record = data.toString("utf8");
    if (data.includes(0) || !Buffer.from(record, "utf8").equals(data)) fail(lock, file, "invalid-record-encoding");
    return { stat, record };
  } catch (error) {
    if (error instanceof MeshLegacyDebrisUnrecoverableError) throw error;
    return fail(lock, file, `inaccessible:${errno(error) ?? "unknown"}`);
  } finally {
    try { if (fd !== undefined) fs.closeSync(fd); }
    catch (error) { fail(lock, file, `inaccessible:${errno(error) ?? "unknown"}`); }
  }
};

/** C may move only {owner}; a single death proof never authorizes additional content. */
export const requireMeshOwnerOnly = (lock: string): void => {
  const names = meshRecoveryChildren(lock);
  if (names.length !== 1 || names[0] !== "owner") fail(lock, lock, "owner-with-extra-entries");
};

/** D snapshot phase. This does NO death inference and NO mutation. Every regular file is its OWN authority.
 * lstat/NOFOLLOW reject links; native all-before-any held-object verification is the final reparse/tree fence. */
export const captureMeshLegacyDebris = (lock: string, directory: fs.BigIntStats): readonly WindowsMeshRecoveryEntry[] => {
  const entries: WindowsMeshRecoveryEntry[] = [];
  let count = 1;
  let requestBytes = 256; // conservative framing/ids budget, including the top directory
  const account = (file: string, record = ""): void => {
    requestBytes += Buffer.byteLength(file, "utf8") + Buffer.byteLength(record, "utf8") + 256;
    if (requestBytes > 65_536) fail(lock, file, "oversize-inventory");
  };
  const visit = (dir: string, initial: fs.BigIntStats, depth: number): void => {
    for (const name of meshRecoveryChildren(lock, dir)) {
      const file = path.join(dir, name);
      if (++count > MAX_ENTRIES) fail(lock, file, "too-many-entries");
      if (depth + 1 > MAX_DEPTH) fail(lock, file, "too-deep");
      const stat = fs.lstatSync(file, { bigint: true });
      if (stat.isSymbolicLink()) fail(lock, file, "link-or-reparse");
      if (stat.isDirectory()) {
        if (!retired(name)) fail(lock, file, "unknown-directory");
        visit(file, stat, depth + 1);
      } else if (stat.isFile()) {
        if (!(released(name) || (depth > 0 && name === "owner"))) fail(lock, file, "unknown-file");
        const owner = readMeshRecoveryOwner(lock, file);
        if (!owner) throw new MeshLegacyDebrisUnrecoverableError(lock, file, "record-changed");
        if (!same(stat, owner.stat)) fail(lock, file, "record-changed");
        entries.push({ path: file, stat: owner.stat, record: owner.record });
        account(file, owner.record);
      } else fail(lock, file, "unknown-entry-type");
    }
    const current = fs.lstatSync(dir, { bigint: true });
    if (!current.isDirectory() || current.isSymbolicLink() || !same(initial, current)) fail(lock, dir, "directory-changed");
    entries.push({ path: dir, stat: initial }); // child-first, top L LAST
    account(dir);
  };
  try { visit(lock, directory, 0); }
  catch (error) {
    if (error instanceof MeshLegacyDebrisUnrecoverableError) throw error;
    fail(lock, lock, `inaccessible:${errno(error) ?? "unknown"}`);
  }
  return entries;
};

/** Judge the COMPLETE inventory before native is called. No deletion callback, filename/age proof or cached verdict. */
export const judgeMeshLegacyDebris = async (lock: string, entries: readonly WindowsMeshRecoveryEntry[],
  judge: (entry: WindowsMeshRecoveryEntry) => Promise<string | undefined>): Promise<void> => {
  for (const entry of entries) {
    if (entry.record === undefined) continue;
    let protectedCategory: string | undefined;
    try { protectedCategory = await judge(entry); }
    catch (error) {
      if (error instanceof MeshLegacyDebrisUnrecoverableError) throw error;
      fail(lock, entry.path, `evidence-unavailable:${errno(error) ?? "unknown"}`);
    }
    if (protectedCategory) fail(lock, entry.path, protectedCategory);
  }
};

/** A refused/partial/unknown native pass never becomes success and never selects a by-name fallback or replay. */
export const meshRecoverySucceeded = (lock: string, result: WindowsMeshRecoveryOutcome): boolean => {
  if (result.outcome === "recovered") return true;
  return fail(lock, lock, `${result.outcome}:${result.reason}`);
};
