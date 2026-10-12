/** Retained monitor lines per task; burst gaps between them stay disclosed. */
export const SHELL_REPLAY_LINES = 256;
/** Lines returned by one tasks.watch call; `more` signals the rest. */
export const SHELL_WATCH_PAGE_LINES = 64;

/**
 * A cursor range `(after, next]` with no retained line. `burst` positions were
 * coalesced by the monitor inside one delivery interval; `evicted` positions
 * aged out of the replay ring before this cursor read them.
 */
export interface ShellReplayLoss {
  after: number;
  next: number;
  reason: "burst" | "evicted";
}

export interface ShellReplayPage {
  lines: string[];
  losses: ShellReplayLoss[];
  omitted: number;
  nextCursor: number;
  more: boolean;
}

type Entry = { cursor: number; line: string } | { cursor: number; gap: number };

/**
 * Bounded cursor replay for monitor lines, after jev-fabric's retained events:
 * every accepted line has a monotonic cursor, readers page from any cursor,
 * and missing positions are disclosed as loss records, never silently joined.
 */
export class ShellReplay {
  #entries: Entry[] = [];
  #lines = 0;
  #floor = 0;
  #cursor = 0;

  get cursor(): number { return this.#cursor; }

  /** Records one flushed batch; `omitted` lines preceded `lines` in that interval. */
  record(lines: readonly string[], omitted: number): void {
    if (omitted > 0) {
      this.#cursor += omitted;
      this.#entries.push({ cursor: this.#cursor, gap: omitted });
    }
    for (const line of lines) this.#entries.push({ cursor: ++this.#cursor, line });
    this.#lines += lines.length;
    // A gap entry needs a following line, so gaps stay bounded by retained lines.
    while (this.#lines > SHELL_REPLAY_LINES) {
      const evicted = this.#entries.shift()!;
      if ("line" in evicted) this.#lines -= 1;
      this.#floor = evicted.cursor;
    }
  }

  page(after: number, limit = SHELL_WATCH_PAGE_LINES): ShellReplayPage {
    const lines: string[] = [];
    const losses: ShellReplayLoss[] = [];
    let next = after;
    if (next < this.#floor) {
      losses.push({ after: next, next: this.#floor, reason: "evicted" });
      next = this.#floor;
    }
    for (const entry of this.#entries) {
      if (entry.cursor <= next) continue;
      if ("gap" in entry) {
        losses.push({ after: Math.max(next, entry.cursor - entry.gap), next: entry.cursor, reason: "burst" });
        next = entry.cursor;
        continue;
      }
      if (lines.length >= limit) break;
      lines.push(entry.line);
      next = entry.cursor;
    }
    return { lines, losses, omitted: next - after - lines.length, nextCursor: next, more: next < this.#cursor };
  }
}
