import fs from "node:fs";
import path from "node:path";

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
    this.#directory = path.join(root, ".lock.q");
    // hrtime is a host-wide monotonic clock, unlike a wall clock that can step backwards.
    this.#name = `${process.hrtime.bigint().toString().padStart(24, "0")}-${process.pid}-${token}`;
    this.#file = path.join(this.#directory, this.#name);
    this.#fallbackAt = Date.now() + Math.min(10_000, Math.max(0, budgetMs * 0.8));
    this.#staleMs = Math.max(30_000, budgetMs * 2);
    fs.mkdirSync(this.#directory, { recursive: true, mode: 0o700 });
    fs.writeFileSync(this.#file, "", { flag: "wx", mode: 0o600 });
  }

  /** Non-head waiters only list the queue and inspect its oldest receipt, not .lock/owner. */
  mayContend(): boolean {
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
    // Keep the empty directory: removing/recreating it races another enqueue.
  }
}
