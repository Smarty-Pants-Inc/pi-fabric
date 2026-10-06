import type { Storage } from "@earendil-works/pi-durable";
import { constants } from "node:fs";
import { chmod, lstat, mkdir, open, readdir, realpath } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";

// Reserve before touching lease descriptors: POSIX locks can be dropped by
// closing another descriptor for the same inode in the owning process.
const ownedDirectories = new Map<string, symbol>();

/** Caller must close Harness/storage before releasing ownership. Never unlink the lease. */
export async function openDurableWorkerStorage(directory: string): Promise<{
  storage: Storage;
  release(): Promise<void>;
}> {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const canonical = await realpath(directory);
  if (ownedDirectories.has(canonical)) throw new Error(`Durable worker storage is locked: ${canonical}`);
  const ownership = Symbol(canonical);
  ownedDirectories.set(canonical, ownership);
  const forget = () => {
    if (ownedDirectories.get(canonical) === ownership) ownedDirectories.delete(canonical);
  };
  try {
    return await acquire(canonical, forget);
  } catch (error) {
    forget();
    throw error;
  }
}

async function acquire(canonical: string, forget: () => void): Promise<{ storage: Storage; release(): Promise<void> }> {
  await chmod(canonical, 0o700);
  const leasePath = join(canonical, "lease.sqlite");
  const regular = async (path: string, missing = false) => {
    try {
      const info = await lstat(path);
      if (!info.isFile() || info.nlink !== 1) throw new Error(`Durable file must be a regular, singly-linked file (no symlinks/hardlinks): ${path}`);
    } catch (error) {
      if (!missing || (error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  };
  // Precreate privately, before SQLite opens the database. Never replace the
  // inode: aliases must contend on the same OS-owned exclusive transaction.
  await regular(leasePath, true);
  try {
    const leaseFile = await open(leasePath, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
    await leaseFile.close();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  await regular(leasePath);
  await chmod(leasePath, 0o600);
  for (const suffix of ["-journal", "-wal", "-shm"]) await regular(leasePath + suffix, true);
  const { DatabaseSync } = await import("node:sqlite");
  const lease = new DatabaseSync(leasePath, { timeout: 0 });
  let released = false;
  const release = async (): Promise<void> => {
    if (released) return;
    // close rolls back the exclusive transaction and releases the OS lock.
    lease.close();
    released = true;
    forget();
  };
  try {
    // DELETE mode makes EXCLUSIVE exclude other connections, not just writers
    // as in WAL mode. No PID files, expiry, retries, or lock stealing.
    lease.exec("PRAGMA busy_timeout = 0; PRAGMA journal_mode = DELETE; BEGIN EXCLUSIVE;");
    const store = join(canonical, "store");
    await mkdir(store, { recursive: true, mode: 0o700 });
    if (!(await lstat(store)).isDirectory()) throw new Error("Durable store must be a directory, not a symlink");
    await chmod(store, 0o700);
    // Existing stores are input, not trusted paths. Reject linked payloads before
    // recovery reads or truncates them, and repair legacy permissive file modes.
    for (const name of await readdir(store)) {
      const path = join(store, name);
      await regular(path);
      await chmod(path, 0o600);
    }
    const [{ JsonlStorage }, { NodeExecutionEnv }, { FileError }, { BACKGROUND_CONTEXT }] = await Promise.all([
      import("@earendil-works/pi-durable/storage/jsonl"),
      import("@earendil-works/pi-durable/env/node"),
      import("@earendil-works/pi-durable/env"),
      import("@earendil-works/chord/context"),
    ]);
    const fs = new NodeExecutionEnv({ cwd: canonical });
    const checked = async (path: string) => {
      if (dirname(resolve(path)) !== store || !/^(?:main|(?:doc|task)-(?:0|[1-9]\d*))\.jsonl(?:\.reclaim)?$/.test(basename(path))) {
        throw new Error(`Durable path escapes storage: ${path}`);
      }
      if (!(await lstat(store)).isDirectory()) throw new Error("Durable store directory was replaced");
      await regular(path, true);
    };
    const syncDirectory = async () => {
      // Windows does not expose directory fsync through Node. File fsync below
      // remains mandatory on every platform; no failed file flush is ignored.
      if (process.platform === "win32") return;
      const dir = await open(store, constants.O_RDONLY | constants.O_NOFOLLOW);
      try { await dir.sync(); } finally { await dir.close(); }
    };
    // The published JSONL adapter fsyncs sidecars, not every main commit marker.
    // Use one no-follow descriptor for mutation + fsync; do not reopen by name
    // between writing and flushing, or resolve a commit before its marker flush.
    for (const method of ["appendFile", "writeFile"] as const) {
      fs[method] = async (path, content, context) => {
        try {
          context.abortSignal?.throwIfAborted();
          await checked(path);
          const file = await open(path, constants.O_CREAT | constants.O_WRONLY | constants.O_NOFOLLOW |
            (method === "appendFile" ? constants.O_APPEND : 0), 0o600);
          try {
            const info = await file.stat();
            if (!info.isFile() || info.nlink !== 1) throw new Error("Durable file was replaced");
            await file.chmod(0o600);
            if (method === "writeFile") await file.truncate(0);
            await file.writeFile(content);
            await file.sync();
          } finally { await file.close(); }
          await syncDirectory();
          return { ok: true, value: undefined };
        } catch (error) {
          return { ok: false, error: new FileError("unknown", String(error), path, error instanceof Error ? error : undefined) };
        }
      };
    }
    // Recovery also reads and truncates incomplete tails; validate those paths.
    for (const method of ["openTextLineReader", "flushFile"] as const) {
      const original = fs[method].bind(fs);
      // These methods have different result types; retain their public signatures.
      const guarded = async (path: string, context: Parameters<typeof original>[1]) => {
        await checked(path);
        return original(path, context);
      };
      Object.defineProperty(fs, method, { value: guarded });
    }
    const truncate = fs.truncateFile.bind(fs);
    fs.truncateFile = async (path, size, context) => {
      await checked(path);
      const result = await truncate(path, size, context);
      return result.ok ? fs.flushFile(path, context) : result;
    };
    const storage = await JsonlStorage.open(store, fs, BACKGROUND_CONTEXT, { fsync: true });
    return { storage, release };
  } catch (error) {
    await release();
    throw new Error(`Cannot acquire durable worker storage at ${canonical}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
  }
}
