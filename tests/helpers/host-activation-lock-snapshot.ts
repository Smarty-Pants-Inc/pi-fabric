import fs from "node:fs";
import path from "node:path";

const lockKey = (file: string): string => {
  const { dev, ino } = fs.statSync(file, { bigint: true });
  // Linux's dev_t encoding; /proc/locks renders major/minor in hex.
  const major = ((dev >> 8n) & 0xfffn) | ((dev >> 32n) & 0xfffff000n);
  const minor = (dev & 0xffn) | ((dev >> 12n) & 0xffffff00n);
  return `${major.toString(16)}:${minor.toString(16)}:${ino}`;
};

/** One kernel lock-table read, restricted to this proof's token inodes.
 * Unlike per-PID fdinfo scans, a slot transfer cannot be counted twice.
 * Include every token file (not just N) so an extra-token regression fails. */
export const heldHostTokenLocks = (directory: string, snapshot?: string): string[] => {
  const tokens = new Set(fs.readdirSync(directory).filter(name => /^token-\d+\.lock$/.test(name))
    .map(name => lockKey(path.join(directory, name))));
  const locks = snapshot ?? fs.readFileSync("/proc/locks", "utf8");
  return locks.split("\n").flatMap(line => {
    // Waiting lock requests (->), POSIX locks and queue.lock are not slots.
    const match = line.match(/^\d+:\s+FLOCK\s+ADVISORY\s+WRITE\s+-?\d+\s+([0-9a-f]+):([0-9a-f]+):(\d+)\s/i);
    if (!match) return [];
    const key = `${BigInt(`0x${match[1]}`).toString(16)}:${BigInt(`0x${match[2]}`).toString(16)}:${match[3]}`;
    return tokens.has(key) ? [key] : [];
  });
};
