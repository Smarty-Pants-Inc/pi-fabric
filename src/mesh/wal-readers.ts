import * as fs from "node:fs";

/**
 * Reader-age report: which processes pin a SQLite WAL by holding a WAL read mark.
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
 */

export interface WalReader {
  pid: number;
  /** Read-mark slot i (0..4), lock byte 123+i. */
  slot: number;
  /** /proc/PID/stat state letter (R, S, D, Z, ...). */
  state?: string;
  /** Process age in milliseconds. */
  ageMs?: number;
  /** Short command line. */
  cmd?: string;
}

const WALINDEX_LOCK_OFFSET = 120;
const WAL_READ_LOCK_FIRST = WALINDEX_LOCK_OFFSET + 3; // 123
const WAL_NREADER = 5; // 123..127
const CLK_TCK = 100;
const CMD_MAX = 60;

const readText = (path: string): string | undefined => {
  try { return fs.readFileSync(path, "utf8"); } catch { return undefined; }
};

/** Linux dev_t decoding (glibc gnu_dev_major/minor). */
const devMajor = (dev: bigint): bigint => ((dev >> 8n) & 0xfffn) | ((dev >> 32n) & ~0xfffn);
const devMinor = (dev: bigint): bigint => (dev & 0xffn) | ((dev >> 12n) & ~0xffn);

const procDetails = (pid: number, uptimeSec: number | undefined): Pick<WalReader, "state" | "ageMs" | "cmd"> => {
  const out: Pick<WalReader, "state" | "ageMs" | "cmd"> = {};
  try {
    const stat = readText(`/proc/${pid}/stat`);
    if (stat !== undefined) {
      // Fields after "comm" (which may contain spaces/parens): [0]=state (field 3) ... [19]=starttime (field 22).
      const rest = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
      if (rest[0]) out.state = rest[0];
      const startTicks = Number(rest[19]);
      if (uptimeSec !== undefined && Number.isFinite(startTicks)) {
        const age = Math.round((uptimeSec - startTicks / CLK_TCK) * 1000);
        if (Number.isFinite(age) && age >= 0) out.ageMs = age;
      }
    }
    // comm only (the executable's short name), never cmdline: arguments may hold secrets (smarty-dev#6787).
    const comm = readText(`/proc/${pid}/comm`)?.trim();
    if (comm) out.cmd = comm.length > CMD_MAX ? comm.slice(0, CMD_MAX) : comm;
  } catch { /* report what we have */ }
  return out;
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
    const locks = readText("/proc/locks");
    if (locks === undefined) return found;
    const exact: { pid: number; slot: number }[] = [];
    const inodeOnly: { pid: number; slot: number }[] = [];
    for (const line of locks.split("\n")) {
      if (line.includes("->")) continue; // blocked waiter, not a holder
      const m = /^\d+:\s+POSIX\s+\S+\s+READ\s+(-?\d+)\s+([0-9a-fA-F]+):([0-9a-fA-F]+):(\d+)\s+(\d+)\s+(\d+|EOF)\s*$/.exec(line.trim());
      if (!m) continue;
      const pid = Number(m[1]);
      if (!(pid > 0)) continue;
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
    let uptimeSec: number | undefined;
    try { uptimeSec = Number((readText("/proc/uptime") ?? "").split(/\s+/)[0]); if (!Number.isFinite(uptimeSec)) uptimeSec = undefined; } catch { uptimeSec = undefined; }
    const details = new Map<number, Pick<WalReader, "state" | "ageMs" | "cmd">>();
    for (const hit of hits) {
      const key = `${hit.pid}:${hit.slot}`;
      if (seen.has(key)) continue;
      seen.add(key);
      let d = details.get(hit.pid);
      if (d === undefined) { d = procDetails(hit.pid, uptimeSec); details.set(hit.pid, d); }
      found.push({ pid: hit.pid, slot: hit.slot, ...d });
    }
    found.sort((a, b) => a.pid - b.pid || a.slot - b.slot);
  } catch { /* never throw: return what we have */ }
  return found;
}

const formatAge = (ms: number): string => {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h`;
  return `${Math.floor(h / 24)}d`;
};

/** One short line per reader, e.g. "pid 1234 (D, 12m, node ...) slot 2"; "none" when empty. */
export function formatWalReaders(list: readonly WalReader[]): string {
  if (list.length === 0) return "no WAL readers found";
  return list.map((r) => {
    const info = [r.state, r.ageMs === undefined ? undefined : formatAge(r.ageMs), r.cmd].filter((x): x is string => x !== undefined && x.length > 0);
    return `pid ${r.pid}${info.length > 0 ? ` (${info.join(", ")})` : ""} slot ${r.slot}`;
  }).join("; ");
}
