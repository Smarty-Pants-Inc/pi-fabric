import fs from "node:fs";
import path from "node:path";
import { ownedStat } from "../storage/scratch.js";
import { ACTOR_RUN_ARCHIVE_PENDING_FILE } from "./archive-custody.js";

interface Marker { format: 1; runId: string; actorId: string; sessionFile: string }
interface CachedMarker { identity: string; marker?: Marker }
const ID = /^[a-f0-9]{32}$/;
const identity = (stat: fs.BigIntStats): string =>
  `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}`;

/** Discovery hints only. Custody release always reads the exact marker afresh.
 * One host-wide pass replaces the old full scan for EVERY actor after EVERY run.
 * Local writers refresh one run; external/restarted writers are discovered on
 * lookups coalesced to one pass per second. Unchanged marker bytes are not reparsed.
 */
export class ActorArchiveIndex {
  readonly #markers = new Map<string, CachedMarker>();
  #rootIdentity: string | undefined;
  #runIds: string[] = [];
  #nextScanAt = 0;
  constructor(readonly root: string) {}

  sources(actorId: string, sessionFile: string, now = Date.now()): Map<string, string> {
    this.#refresh(now);
    const sources = new Map<string, string>();
    for (const [runId, { marker }] of this.#markers) {
      if (marker?.actorId === actorId && marker.sessionFile === sessionFile) {
        sources.set(runId, path.join(this.root, runId));
      }
    }
    return sources;
  }

  /** Called after this manager publishes/retires a custody marker. No root traversal. */
  refreshRun(runId: string): void {
    if (ID.test(runId)) this.#read(runId, false);
  }

  /** Cache hits are never authority for unlink, archival or ownership changes. */
  confirmedSource(runId: string, actorId: string, sessionFile: string): string | undefined {
    if (!ID.test(runId) || !ownedStat(this.root)?.isDirectory()) return undefined;
    const marker = this.#read(runId, false);
    return marker?.actorId === actorId && marker.sessionFile === sessionFile
      ? path.join(this.root, runId) : undefined;
  }

  #refresh(now: number): void {
    if (now < this.#nextScanAt) return;
    try {
      if (!ownedStat(this.root)?.isDirectory()) { this.#reset(); return; }
      const rootIdentity = identity(fs.lstatSync(this.root, { bigint: true }));
      if (this.#rootIdentity !== rootIdentity) {
        this.#runIds = fs.readdirSync(this.root).filter(runId => ID.test(runId));
        this.#rootIdentity = rootIdentity;
        const live = new Set(this.#runIds);
        for (const runId of this.#markers.keys()) if (!live.has(runId)) this.#markers.delete(runId);
      }
      for (const runId of this.#runIds) this.#read(runId, true);
      this.#nextScanAt = now + 1_000;
    } catch { this.#reset(); } // Unknown directory state is not a cached answer.
  }

  #read(runId: string, reuse: boolean): Marker | undefined {
    const directory = path.join(this.root, runId);
    const file = path.join(directory, ACTOR_RUN_ARCHIVE_PENDING_FILE);
    try {
      if (!ownedStat(directory)?.isDirectory() || !ownedStat(file)?.isFile()) {
        this.#markers.delete(runId); return undefined;
      }
      const stat = fs.lstatSync(file, { bigint: true });
      if (stat.size > 4096n) { this.#markers.delete(runId); return undefined; }
      const stamp = identity(stat);
      const cached = this.#markers.get(runId);
      if (reuse && cached?.identity === stamp) return cached.marker;
      const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
      try {
        if (identity(fs.fstatSync(fd, { bigint: true })) !== stamp) {
          this.#markers.delete(runId); return undefined;
        }
        const value = JSON.parse(fs.readFileSync(fd, "utf8"));
        const marker: Marker | undefined = value?.format === 1 && value.runId === runId &&
          typeof value.actorId === "string" && typeof value.sessionFile === "string" ? value : undefined;
        // Bind bytes to the opened inode AND the current path; equal path stats
        // alone cannot detect an A -> B -> A replacement/restore race.
        if (identity(fs.fstatSync(fd, { bigint: true })) !== stamp ||
            identity(fs.lstatSync(file, { bigint: true })) !== stamp) {
          this.#markers.delete(runId); return undefined;
        }
        this.#markers.set(runId, { identity: stamp, ...(marker ? { marker } : {}) });
        return marker;
      } finally { fs.closeSync(fd); }
    } catch { this.#markers.delete(runId); return undefined; }
  }

  #reset(): void {
    this.#markers.clear(); this.#runIds = []; this.#rootIdentity = undefined; this.#nextScanAt = 0;
  }
}
