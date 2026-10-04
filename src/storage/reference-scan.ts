import fs from "node:fs";
import { ownedStat } from "./scratch.js";

/** Bound even a single safety unit before predicates that use readdirSync.
 * A big/unsafe/time-limited tree is kept; no partial walk can authorize exit. */
export const boundedRunTree = (root: string, expired: () => boolean, maxEntries = 64): boolean => {
  let count = 0;
  const visit = (directory: string, depth: number): boolean => {
    if (expired() || depth > 32 || !ownedStat(directory)?.isDirectory()) return false;
    const cursor = fs.opendirSync(directory);
    try {
      let entry: fs.Dirent | null;
      while (!expired() && (entry = cursor.readSync())) {
        if (++count > maxEntries) return false;
        const file = `${directory}/${entry.name}`, stat = ownedStat(file);
        if (!stat || (!stat.isDirectory() && !stat.isFile())) return false;
        if (stat.isDirectory() && !visit(file, depth + 1)) return false;
      }
      return !expired();
    } finally { cursor.closeSync(); }
  };
  try { return visit(root, 0); } catch { return false; }
};

/** A progressing, bounded preparation cursor. Incomplete or stale snapshots are
 * wildcard vetoes, never partial authority to collect. No caller walks the full
 * run set, including queued/managed handles, in one turn. */
export class RetentionReferenceScan {
  #cursor: Generator<void> | undefined;
  #generation = "";
  #nextRefresh = 0;
  #refs = new Set<string>();
  #complete = false;
  #expired: () => boolean = () => true;
  readonly intervalMs = 60_000;
  rebuilds = 0;

  static directoryGeneration(root: string): string {
    try {
      const stat = fs.lstatSync(root);
      if (!ownedStat(root)?.isDirectory()) return "unsafe";
      return `${stat.dev}:${stat.ino}:${stat.mtimeMs}:${stat.ctimeMs}`;
    } catch (error) {
      return (error as NodeJS.ErrnoException).code === "ENOENT" ? "absent" : "unsafe";
    }
  }

  snapshot(generation: string, scan: (protect: (id: string, actorId?: string) => void, expired: () => boolean) => Generator<void>,
    options: { now?: number; budgetMs?: number; maxEntries?: number; refresh?: boolean } = {}): Set<string> {
    const now = options.now ?? Date.now();
    const started = performance.now();
    const expired = () => performance.now() - started >= (options.budgetMs ?? 2);
    this.#expired = expired;
    // Activity queues a delta pass, never discards an in-flight historical
    // cursor. The factory must reconcile changes before it finishes.
    if (options.refresh || (!this.#cursor && generation !== this.#generation) || (this.#complete && now >= this.#nextRefresh)) {
      this.close(); this.#generation = generation; this.#refs = new Set(); this.#complete = false;
    }
    if (!this.#complete && !this.#cursor) {
      this.rebuilds++;
      const protect = (id: string, actorId?: string) => {
        // Returning/copying a reference set must be bounded too. Overflow vetoes
        // everything rather than truncating custody references silently.
        for (const value of actorId ? [id, actorId] : [id]) {
          if (this.#refs.size < 1024) this.#refs.add(value); else this.#refs.add("*");
        }
      };
      this.#cursor = scan(protect, () => this.#expired());
    }
    try {
      for (let count = 0; this.#cursor && count < (options.maxEntries ?? 64) && !expired(); count++) {
        if (this.#cursor.next().done) {
          // Keep the generation captured when the cursor started. A change
          // during preparation queues a fresh delta on the next call, even if
          // the factory has already reconciled filesystem additions.
          this.#cursor = undefined; this.#complete = true; this.#nextRefresh = now + this.intervalMs;
        }
      }
    } catch { this.#refs.add("*"); this.close(); this.#complete = true; this.#nextRefresh = now + this.intervalMs; }
    const refs = new Set(this.#refs);
    if (!this.#complete) refs.add("*");
    return refs;
  }

  current(generation: string, now = Date.now()): boolean {
    return this.#complete && generation === this.#generation && now < this.#nextRefresh;
  }

  close(): void {
    try { this.#cursor?.return(undefined); } finally { this.#cursor = undefined; }
  }
}
