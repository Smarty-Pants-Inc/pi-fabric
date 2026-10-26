import * as fs from "node:fs";

/**
 * Reader report: which processes pin a SQLite WAL by holding a WAL read mark.
 *
 * SQLite's WAL-mode readers hold a POSIX fcntl() SHARED (F_RDLCK) lock on one read-mark byte of the
 * `-shm` file for as long as their read transaction lasts; while that lock is held, checkpoints
 * cannot reset the WAL past the reader's snapshot, so the WAL grows.
 *
 * Lock layout (SQLite src/wal.c and src/os_unix.c):
 *   wal.c:     #define WAL_WRITE_LOCK 0, WAL_CKPT_LOCK 1, WAL_RECOVER_LOCK 2,
 *              #define WAL_READ_LOCK(I) (3+(I)), WAL_NREADER (SQLITE_SHM_NLOCK-3)  (SQLITE_SHM_NLOCK = 8 -> 5 readers)
 *              #define WALINDEX_LOCK_OFFSET (sizeof(WalIndexHdr)*2+offsetof(WalCkptInfo,aLock))  = 48*2+24 = 120
 *   os_unix.c: #define UNIX_SHM_BASE ((22+SQLITE_SHM_NLOCK)*4)  = 120   ("first lock byte"); lock i is byte UNIX_SHM_BASE+i,
 *              taken with fcntl(F_SETLK) via unixShmSystemLock(), i.e. a classic per-process POSIX lock.
 *   https://www.sqlite.org/walformat.html "the 8 bytes of the shm file starting at offset 120 are used as locks":
 *              WAL_READ_LOCK(0..4) = shm byte offsets 123..127.
 *
 * /proc/locks (fs/locks.c lock_get_status) prints `N: POSIX  ADVISORY  READ  <pid> <maj:min hex>:<inode> <start> <end|EOF>`;
 * blocked waiters carry a `->` marker and are skipped. Linux only; elsewhere every function returns empty.
 * The writer holds an exclusive POSIX WRITE lock on WAL_WRITE_LOCK, byte 120, for its whole write transaction
 * (`BEGIN IMMEDIATE` through COMMIT/ROLLBACK, and a FULL/RESTART/TRUNCATE checkpoint): walWriterPid names it.
 */

export interface WalReader {
  pid: number;
  /** Read-mark slot i (0..4), lock byte 123+i. */
  slot: number;
  /** Printable kernel /proc/PID/comm, at most 15 UTF-8 bytes; never a command line. */
  comm?: string;
}

const WALINDEX_LOCK_OFFSET = 120;
const WAL_READ_LOCK_FIRST = WALINDEX_LOCK_OFFSET + 3; // 123
const WAL_NREADER = 5; // 123..127
const COMM_MAX_BYTES = 15;

const readText = (path: string): string | undefined => {
  try { return fs.readFileSync(path, "utf8"); } catch { return undefined; }
};

const PROC_LOCKS_CACHE_MS = 10_000;
const PROC_LOCKS_DISABLED_PATH = `${process.env.HOME ?? ""}/.config/smarty/fabric-proc-locks.disabled`;

/** Injectable process-wide diagnostics cache, exposed for deterministic cache tests. */
export function createProcLocksDiagnostics(
  read: () => string | undefined,
  statFlag: () => boolean,
  now: () => number = Date.now,
): { locks: () => string | undefined; disabled: () => boolean } {
  let locksAt = Number.NEGATIVE_INFINITY;
  let locksValue: string | undefined;
  let flagAt = Number.NEGATIVE_INFINITY;
  let flagValue = false;
  const disabled = (): boolean => {
    const time = now();
    if (time - flagAt >= PROC_LOCKS_CACHE_MS) {
      flagValue = statFlag();
      flagAt = time;
    }
    return flagValue;
  };
  return {
    locks() {
      if (disabled()) return undefined;
      const time = now();
      if (time - locksAt >= PROC_LOCKS_CACHE_MS) {
        locksValue = read();
        locksAt = time;
      }
      return locksValue;
    },
    disabled,
  };
}

const procLocksDiagnostics = createProcLocksDiagnostics(
  () => readText("/proc/locks"),
  () => { try { fs.statSync(PROC_LOCKS_DISABLED_PATH); return true; } catch { return false; } },
);

/** Linux dev_t decoding (glibc gnu_dev_major/minor). */
const devMajor = (dev: bigint): bigint => ((dev >> 8n) & 0xfffn) | ((dev >> 32n) & ~0xfffn);
const devMinor = (dev: bigint): bigint => (dev & 0xffn) | ((dev >> 12n) & ~0xffn);

const safeComm = (value: unknown): string | undefined =>
  typeof value === "string" && value.length > 0 && Buffer.byteLength(value, "utf8") <= COMM_MAX_BYTES
    && !/[\p{C}\p{Zl}\p{Zp}]/u.test(value) ? value : undefined;

/** Fresh kernel fact for a validated PID. Never read cmdline or shorten arbitrary text into a prefix. */
export const walReaderComm = (pid: number): string | undefined => {
  if (process.platform !== "linux" || !Number.isSafeInteger(pid) || pid <= 0) return undefined;
  const text = readText(`/proc/${pid}/comm`);
  // Remove only procfs's record delimiter, not whitespace within the kernel name.
  return safeComm(text?.endsWith("\n") ? text.slice(0, -1) : text);
};

/**
 * Processes holding a WAL read mark (POSIX READ lock on shm bytes 123..127) on `dbPath`'s `-shm` file.
 * Never throws; returns what it found before any error. Empty on non-Linux.
 */
export function walReaderPids(dbPath: string): WalReader[] {
  const found: WalReader[] = [];
  if (process.platform !== "linux") return found;
  try {
    const st = fs.statSync(`${dbPath}-shm`, { bigint: true });
    const inode = st.ino;
    const major = devMajor(st.dev);
    const minor = devMinor(st.dev);
    if (procLocksDiagnostics.disabled()) return found;
    const locks = procLocksDiagnostics.locks();
    if (locks === undefined) return found;
    const exact: { pid: number; slot: number }[] = [];
    const inodeOnly: { pid: number; slot: number }[] = [];
    for (const line of locks.split("\n")) {
      if (line.includes("->")) continue; // blocked waiter, not a holder
      const m = /^\d+:\s+POSIX\s+\S+\s+READ\s+(-?\d+)\s+([0-9a-fA-F]+):([0-9a-fA-F]+):(\d+)\s+(\d+)\s+(\d+|EOF)\s*$/.exec(line.trim());
      if (!m) continue;
      const pid = Number(m[1]);
      if (!Number.isSafeInteger(pid) || pid <= 0) continue;
      if (BigInt(m[4]!) !== inode) continue;
      const start = Number(m[5]);
      const end = m[6] === "EOF" ? Number.POSITIVE_INFINITY : Number(m[6]);
      const devMatch = BigInt(`0x${m[2]}`) === major && BigInt(`0x${m[3]}`) === minor;
      for (let slot = 0; slot < WAL_NREADER; slot++) {
        const byte = WAL_READ_LOCK_FIRST + slot;
        if (start <= byte && byte <= end) (devMatch ? exact : inodeOnly).push({ pid, slot });
      }
    }
    // Some filesystems (btrfs subvolumes) report a stat st_dev that differs from the superblock
    // s_dev /proc/locks prints; fall back to an inode-only match when nothing matched both.
    const hits = exact.length > 0 ? exact : inodeOnly;
    const seen = new Set<string>();
    const names = new Map<number, string | undefined>();
    for (const hit of hits) {
      const key = `${hit.pid}:${hit.slot}`;
      if (seen.has(key)) continue;
      seen.add(key);
      if (!names.has(hit.pid)) names.set(hit.pid, walReaderComm(hit.pid));
      const comm = names.get(hit.pid);
      found.push({ pid: hit.pid, slot: hit.slot, ...(comm === undefined ? {} : { comm }) });
    }
    found.sort((a, b) => a.pid - b.pid || a.slot - b.slot);
  } catch { /* never throw: return what we have */ }
  return found;
}

/**
 * The process holding SQLite's WAL writer lock (POSIX WRITE lock on shm byte 120) on `dbPath`'s `-shm`
 * file, for a waiter that gave up on it (smarty-dev#9290). The kernel's lock table is the writer lock's only
 * owner record: SQLite keeps none. Read only on that rare path, never per transaction. Never throws;
 * undefined on non-Linux, without a `-shm` file, or when no single process holds the byte.
 */
export function walWriterPid(dbPath: string): number | undefined {
  if (process.platform !== "linux") return undefined;
  try {
    const st = fs.statSync(`${dbPath}-shm`, { bigint: true });
    const major = devMajor(st.dev);
    const minor = devMinor(st.dev);
    if (procLocksDiagnostics.disabled()) return undefined;
    const locks = procLocksDiagnostics.locks();
    if (locks === undefined) return undefined;
    const exact = new Set<number>();
    const inodeOnly = new Set<number>();
    for (const line of locks.split("\n")) {
      if (line.includes("->")) continue; // blocked waiter, not a holder
      const m = /^\d+:\s+POSIX\s+\S+\s+WRITE\s+(-?\d+)\s+([0-9a-fA-F]+):([0-9a-fA-F]+):(\d+)\s+(\d+)\s+(\d+|EOF)\s*$/.exec(line.trim());
      if (!m) continue;
      const pid = Number(m[1]);
      if (!Number.isSafeInteger(pid) || pid <= 0 || BigInt(m[4]!) !== st.ino) continue;
      const start = Number(m[5]);
      const end = m[6] === "EOF" ? Number.POSITIVE_INFINITY : Number(m[6]);
      if (!(start <= WALINDEX_LOCK_OFFSET && WALINDEX_LOCK_OFFSET <= end)) continue;
      (BigInt(`0x${m[2]}`) === major && BigInt(`0x${m[3]}`) === minor ? exact : inodeOnly).add(pid);
    }
    // As for readers: fall back to an inode-only match (btrfs subvolumes) when nothing matched both.
    const hits = exact.size > 0 ? exact : inodeOnly;
    return hits.size === 1 ? [...hits][0] : undefined;
  } catch { return undefined; }
}

/** Only validated pid + comm facts, e.g. "pid 1234 (node)"; no internal slot or surplus fields. */
export function formatWalReaders(list: readonly Pick<WalReader, "pid" | "comm">[]): string {
  const lines: string[] = [];
  for (const reader of list) {
    const pid = reader?.pid;
    if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) continue;
    const comm = safeComm(reader.comm);
    lines.push(`pid ${pid}${comm === undefined ? "" : ` (${comm})`}`);
  }
  return lines.join("; ") || "no WAL readers found";
}
