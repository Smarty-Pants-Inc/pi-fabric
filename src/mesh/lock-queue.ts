import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/** Advisory receipts live beside, never inside, the mesh's data namespace.
 * Canonical paths make symlink aliases share admission; the mesh's parent (not
 * process-specific TMPDIR) keeps FIFO shared by all contenders for the same root.
 */
export function meshLockQueueDirectory(root: string): string {
  const canonical = fs.realpathSync(root);
  const key = createHash("sha256").update(canonical).digest("hex");
  return path.join(path.dirname(canonical), `.pi-fabric-mesh-lock-${process.getuid?.() ?? "user"}`, key);
}

/** Advisory admission only: the unchanged .lock directory is the sole exclusion fence.
 * Old releases ignore this queue. After a bounded wait we join their plain contest,
 * never extend the caller's acquisition deadline, and never remove a live lock.
 */
export class MeshLockTicket {
  readonly #directory: string;
  readonly #name: string;
  readonly #file: string;
  readonly #fallbackAt: number;
  readonly #staleMs: number;
  #closed = false;

  constructor(root: string, token: string, budgetMs: number) {
    this.#directory = meshLockQueueDirectory(root);
    // hrtime is a host-wide monotonic clock, unlike a wall clock that can step backwards.
    this.#name = `${process.hrtime.bigint().toString().padStart(24, "0")}-${process.pid}-${token}`;
    this.#file = path.join(this.#directory, this.#name);
    this.#fallbackAt = Date.now() + Math.min(10_000, Math.max(0, budgetMs * 0.8));
    this.#staleMs = Math.max(30_000, budgetMs * 2);
    const namespace = path.dirname(this.#directory);
    // No external sibling exists for a filesystem root. Admission is optional: a
    // read-only parent or unsafe namespace must not break otherwise writable meshes.
    const canonical = fs.realpathSync(root);
    if (path.dirname(canonical) === canonical) { this.#closed = true; return; }
    try {
      fs.mkdirSync(namespace, { recursive: true, mode: 0o700 });
      const stat = fs.lstatSync(namespace);
      if (!stat.isDirectory() || (process.getuid && stat.uid !== process.getuid()) ||
        (process.platform !== "win32" && (stat.mode & 0o077) !== 0)) { this.#closed = true; return; }
      fs.mkdirSync(this.#directory, { recursive: true, mode: 0o700 });
      fs.writeFileSync(this.#file, "", { flag: "wx", mode: 0o600 });
    } catch (error) {
      if (!["EACCES", "EPERM", "EROFS", "EEXIST", "ENOTDIR"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
      this.#closed = true;
    }
  }

  /** Non-head waiters only list the queue and inspect its oldest receipt, not .lock/owner. */
  mayContend(): boolean {
    if (this.#closed) return true;
    if (Date.now() >= this.#fallbackAt) { this.close(); return true; }
    const names = fs.readdirSync(this.#directory).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort();
    for (const name of names) {
      if (name === this.#name) return true;
      const file = path.join(this.#directory, name);
      try {
        const stat = fs.statSync(file);
        const pid = Number(name.split("-")[1]);
        let alive = true;
        try { process.kill(pid, 0); }
        catch (error) { alive = (error as NodeJS.ErrnoException).code !== "ESRCH"; }
        if (alive && Date.now() - stat.mtimeMs <= this.#staleMs) return false;
        // Ticket names are unique and never reused; unlink cannot remove a successor.
        fs.unlinkSync(file);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
    }
    // A stale reaper may have removed our receipt while we were descheduled. Admission
    // is advisory, so continue the bounded plain contest rather than wait on a lost ticket.
    this.close();
    return true;
  }

  get queued(): boolean { return !this.#closed; }

  close(): void {
    if (this.#closed) return;
    fs.rmSync(this.#file, { force: true });
    this.#closed = true;
    // Keep the empty directory OUTSIDE the mesh: removing/recreating it races enqueue.
  }
}
