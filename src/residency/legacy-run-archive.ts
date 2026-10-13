import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { setImmediate as yieldTurn } from "node:timers/promises";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { isWatchLimitError } from "../core/watch-fallback.js";
import { retentionV2Enabled } from "../storage/retention-platform.js";
import { processAlive } from "../storage/scratch.js";

export interface LegacyRunArchivePolicy {
  legacyRunArchiveEnabled?: boolean;
  legacyRunArchiveAgeMs?: number;
}
const DEFAULT_AGE_MS = 48 * 60 * 60 * 1000;
const terminal = new Set(["completed", "failed", "stopped"]);
const files = new Set([
  "task.txt", "task.txt.provenance.json", "status.json", "events.jsonl", "lifecycle.jsonl", "reply.json",
  "schema.json", "images.json", "steer.jsonl", "relaunches.jsonl", "session.jsonl", "route-session.jsonl",
]);
const gone = (error: unknown): boolean => ["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "");
const owned = async (file: string): Promise<fs.Stats> => {
  const stat = await fsp.lstat(file);
  if (stat.isSymbolicLink() || (stat.isFile() && stat.nlink !== 1) || (process.getuid && stat.uid !== process.getuid())) throw new Error("unsafe ownership/link");
  return stat;
};
/** Keep source and destination in an owned, unaliased namespace. Revalidate
 * around asynchronous custody checks; never follow an operator-created alias. */
const unaliasedDirectory = async (directory: string): Promise<boolean> => {
  try {
    const absolute = path.resolve(directory);
    // Only absolute, normalized spellings can be used for lexical containment.
    if (directory !== absolute || await fsp.realpath(absolute) !== absolute) return false;
    let ancestor = absolute;
    for (;;) {
      const stat = await fsp.lstat(ancestor);
      if (stat.isSymbolicLink() || !stat.isDirectory()) return false;
      const parent = path.dirname(ancestor);
      if (parent === ancestor) return true;
      ancestor = parent;
    }
  } catch { return false; }
};
interface TreeProof { fingerprint: string; status: string; finishedAt: number }
/** Observation only (never authority): a run whose root status is not terminal,
 * or not yet written, will close by a status.json event worth watching. */
const openRun = async (directory: string): Promise<boolean> => {
  try {
    const stat = await owned(path.join(directory, "status.json"));
    if (!stat.isFile() || stat.size > 1024 * 1024) return false;
    return !terminal.has(JSON.parse(await fsp.readFile(path.join(directory, "status.json"), "utf8"))?.status);
  } catch (error) { return gone(error); }
};

/** Separate legacy proof, NOT a relaxation of runTreeExitVeto or deletion.
 * Async directory cursors yield between files. Oversized/unknown/pending trees
 * are skipped, not partially authorized. Every descendant needs terminal proof. */
export const legacyRunTreeProof = async (directory: string, now: number, ageMs: number): Promise<TreeProof | undefined> => {
  const identities: unknown[] = [];
  let count = 0;
  let rootRecord: { status: string; finishedAt: number } | undefined;
  const walk = async (dir: string, depth: number): Promise<void> => {
    if (depth > 32 || ++count > 4096) throw new Error("tree limit");
    const dirStat = await owned(dir);
    if (!dirStat.isDirectory() || (dirStat.mode & 0o077) !== 0) throw new Error("unsafe run directory");
    identities.push([path.relative(directory, dir), dirStat.dev, dirStat.ino, dirStat.mode]);
    const statusFile = path.join(dir, "status.json"), statusStat = await owned(statusFile);
    if (!statusStat.isFile() || statusStat.size > 1024 * 1024) throw new Error("unsafe status");
    const record = JSON.parse(await fsp.readFile(statusFile, "utf8"));
    if (!record || !terminal.has(record.status) || !Number.isSafeInteger(record.finishedAt) || record.finishedAt < 0 ||
        now - record.finishedAt < ageMs || record.cleanupPending || record.pending || record.queuedArchiveCommitted === false ||
        (record.transport !== undefined && record.transport !== "process")) throw new Error("terminal/age/custody proof missing");
    if (depth === 0) {
      if (record.sessionId !== undefined || record.processStartTime !== undefined) throw new Error("not a legacy run");
      rootRecord = { status: record.status, finishedAt: record.finishedAt };
    } else if (record.sessionId !== undefined) {
      if (typeof record.sessionId !== "string" || !/^\d+$/.test(record.sessionId) || processAlive(Number(record.sessionId))) throw new Error("live/unknown descendant PID");
    }
    const entries = await fsp.opendir(dir);
    for await (const entry of entries) {
      if (++count > 4096) throw new Error("tree limit");
      await yieldTurn();
      const file = path.join(dir, entry.name), stat = await owned(file);
      identities.push([path.relative(directory, file), stat.dev, stat.ino, stat.mode, stat.size, stat.mtimeMs, stat.ctimeMs]);
      if (stat.isFile() && (files.has(entry.name) || /^oversized-event-prefix(-\d+)?\.txt$/.test(entry.name))) continue;
      if (stat.isDirectory() && ["deliveries", "follow-ups"].includes(entry.name)) {
        // Even final-looking receipts are custody until the normal replay has
        // discharged them. Empty worker ingress directories are normal.
        const children = await fsp.opendir(file);
        try { if (await children.read()) throw new Error("pending delivery"); } finally { await children.close(); }
        continue;
      }
      if (stat.isDirectory() && entry.name === "nested") {
        const children = await fsp.opendir(file);
        for await (const child of children) await walk(path.join(file, child.name), depth + 1);
        continue;
      }
      // unresolved-worker, queued-result, completion-recipient and all unknown
      // artifacts veto archival. They are not inferred settled from status.
      throw new Error("unknown/pending run artifact");
    }
  };
  try {
    if (!Number.isSafeInteger(ageMs) || ageMs < 0) return;
    if (!await unaliasedDirectory(directory)) return;
    await walk(directory, 0);
    if (!await unaliasedDirectory(directory)) return;
    identities.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    return rootRecord && { ...rootRecord, fingerprint: JSON.stringify(identities) };
  } catch { return; }
};

interface ArchiveOptions {
  actorRoots?: readonly string[];
  isRetained: (id: string) => boolean;
}

const RUN_ID = /^[A-Za-z0-9_-]+$/;
/** Full-pass cadence. One unref'd one-shot; never an idle sub-second poll. It
 * covers age eligibility (no filesystem event fires when 48h elapse), policy
 * re-enable and custody release, which have no event carrier in this module. */
export const LEGACY_ARCHIVE_DEADLINE_MS = 60_000;
/** Bounded queue of event-named run IDs; overflow is covered by the next full pass. */
const MAX_PENDING = 256;
/** Bounded per-active-run status watchers (oldest evicted first). */
const MAX_RUN_WATCHERS = 64;
/** Watcher re-arm backoff: one-shot retries 1s..32s, then stop (log once). */
const REARM_BASE_MS = 1_000, REARM_ATTEMPTS = 6;

/** Off-request-path retirement by same-filesystem rename only. Sources remain
 * ordinary directories forever: opaque descriptor custody cannot lose bytes.
 * The legacy public names/policy keys are retained for configuration compatibility. */
export class ResidentLegacyRunArchive {
  #directory: fs.Dir | undefined;
  #nextScan = 0;
  #deadline: NodeJS.Timeout | undefined;
  #immediate: NodeJS.Immediate | undefined;
  #rearm: NodeJS.Timeout | undefined;
  #rearmAttempts = 0;
  #watchFailureLogged = false;
  #started = false;
  #runsWatcher: fs.FSWatcher | undefined;
  readonly #runWatchers = new Map<string, fs.FSWatcher>();
  /** Runs whose status watcher errored this full pass: one re-arm each, no spin. */
  readonly #runWatchErrors = new Set<string>();
  readonly #pending = new Set<string>();
  #work: Promise<void> | undefined;
  #closed = false;
  #blocked = false;
  readonly health = { checked: 0, archived: 0, skipped: 0, error: "" };
  constructor(readonly root: string, readonly policy: LegacyRunArchivePolicy, readonly options: ArchiveOptions) {}

  #enabled(): boolean { return this.policy.legacyRunArchiveEnabled !== false; }

  /** Event-driven: root/runs entry events and bounded per-run status.json
   * events queue the named run; a cursor with remaining work continues with
   * setImmediate; otherwise only the slow full-pass deadline is armed. */
  start(): void {
    if (!retentionV2Enabled() || this.#started || this.#closed) return;
    this.#started = true;
    this.#watchRuns();
    this.#run();
  }

  async close(): Promise<void> {
    this.#closed = true;
    if (this.#deadline) clearTimeout(this.#deadline);
    if (this.#rearm) clearTimeout(this.#rearm);
    if (this.#immediate) clearImmediate(this.#immediate);
    this.#deadline = this.#rearm = this.#immediate = undefined;
    this.#runsWatcher?.close(); this.#runsWatcher = undefined;
    for (const watcher of this.#runWatchers.values()) watcher.close();
    this.#runWatchers.clear(); this.#pending.clear(); this.#runWatchErrors.clear();
    await this.#work;
    const directory = this.#directory; this.#directory = undefined;
    await directory?.close();
  }

  #enqueue(id: string): void {
    if (this.#closed) return;
    // Overflow is not lost work: the next full pass visits every run.
    if (this.#pending.size < MAX_PENDING) this.#pending.add(id);
    if (!this.#work && !this.#immediate) this.#immediate = setImmediate(() => { this.#immediate = undefined; this.#run(); });
  }

  #run(): void {
    if (this.#closed || this.#work) return;
    if (this.#deadline) clearTimeout(this.#deadline);
    this.#deadline = undefined;
    if (!this.#runsWatcher && this.#rearmAttempts < REARM_ATTEMPTS && !this.#rearm) this.#watchRuns();
    this.#work = this.sweep().catch(error => { this.health.error = String(error); }).finally(() => {
      this.#work = undefined;
      if (this.#closed) return;
      // Remaining work continues without a timer; idle arms one slow deadline.
      if (!this.#blocked && this.#enabled() && (this.#directory || this.#pending.size)) {
        if (!this.#immediate) this.#immediate = setImmediate(() => { this.#immediate = undefined; this.#run(); });
        return;
      }
      // A blocked (faulted) archive stays inert until restart: no standing poll.
      if (this.#blocked) return;
      this.#deadline = setTimeout(() => { this.#deadline = undefined; this.#run(); }, LEGACY_ARCHIVE_DEADLINE_MS);
      this.#deadline.unref();
    });
  }

  #watchRuns(): void {
    if (this.#closed || this.#runsWatcher) return;
    const runs = path.join(this.root, "runs");
    try {
      const watcher = fs.watch(runs, { persistent: false }, (_event, name) => {
        const id = name === null ? undefined : String(name);
        if (id === undefined) { this.#rescan(); return; }
        if (!RUN_ID.test(id)) return;
        this.#watchRun(id);
        this.#enqueue(id);
      });
      watcher.on("error", error => this.#watchFailed(watcher, error));
      this.#runsWatcher = watcher;
      this.#rearmAttempts = 0;
    } catch (error) {
      // Absent runs directory: the full pass retries the watch; not a fault.
      if (!gone(error)) this.#watchFailed(undefined, error);
    }
  }

  /** Watcher error = one rescan plus bounded one-shot re-arm; never a poll. */
  #watchFailed(watcher: fs.FSWatcher | undefined, error: unknown): void {
    watcher?.close();
    if (watcher && this.#runsWatcher !== watcher) return;
    this.#runsWatcher = undefined;
    if (this.#closed) return;
    if (!this.#watchFailureLogged) {
      this.#watchFailureLogged = true;
      // Truthful degraded mode: no polling. One rescan, bounded one-shot re-arms,
      // then the 60s full-pass deadline alone. ENOSPC here is the inotify limit.
      const cause = isWatchLimitError(error) ? "inotify watch limit reached (fs.inotify.max_user_watches), not disk space" : String(error);
      console.warn(`[fabric] legacy run archive watcher on ${path.join(this.root, "runs")} failed: ${cause}; ` +
        `rescanning once and re-arming with bounded backoff (${REARM_ATTEMPTS} attempts), then full passes every ${LEGACY_ARCHIVE_DEADLINE_MS / 1000}s`);
    }
    this.#rescan();
    if (this.#rearm || this.#rearmAttempts >= REARM_ATTEMPTS) return;
    const delay = REARM_BASE_MS * 2 ** this.#rearmAttempts++;
    this.#rearm = setTimeout(() => { this.#rearm = undefined; this.#watchRuns(); if (this.#runsWatcher) this.#rescan(); }, delay);
    this.#rearm.unref();
  }

  #rescan(): void {
    if (this.#closed) return;
    this.#nextScan = 0;
    if (!this.#work && !this.#immediate) this.#immediate = setImmediate(() => { this.#immediate = undefined; this.#run(); });
  }

  #watchRun(id: string): void {
    if (this.#closed || this.#runWatchers.has(id)) return;
    try {
      const watcher = fs.watch(path.join(this.root, "runs", id), { persistent: false }, (_event, name) => {
        // Run close is an atomic status.json replacement; ignore event streams.
        if (name === null || String(name) === "status.json") this.#enqueue(id);
      });
      // Error: drop the watcher and queue one check; that check re-arms it once.
      // A repeat error in the same full pass stays unwatched until the next pass
      // (the slow deadline), so a permanently failing watch cannot spin.
      watcher.on("error", () => {
        const repeat = this.#runWatchErrors.has(id);
        this.#runWatchErrors.add(id);
        this.#unwatchRun(id, watcher);
        if (!repeat) this.#enqueue(id);
      });
      this.#runWatchers.set(id, watcher);
      if (this.#runWatchers.size > MAX_RUN_WATCHERS) {
        const [oldest] = this.#runWatchers.keys();
        this.#unwatchRun(oldest!);
      }
    } catch { /* gone or not a directory: the queued check skips it */ }
  }

  #unwatchRun(id: string, expected?: fs.FSWatcher): void {
    const watcher = this.#runWatchers.get(id);
    if (!watcher || (expected && watcher !== expected)) { expected?.close(); return; }
    this.#runWatchers.delete(id); watcher.close();
  }

  async sweep(now = Date.now(), maxEntries = 32, budgetMs = 20): Promise<void> {
    if (this.#closed) return;
    if (!retentionV2Enabled()) {
      // Unix mode bits are not an ownership/privacy proof on native Windows.
      // Explicit platform cut: no discovery, destination creation or mutation.
      // No new timer or audit output on Windows: preserve main's behavior.
      // All Windows retention changes are deferred to smarty-dev#5132.
      return;
    }
    if (this.#blocked || !this.#enabled() || (now < this.#nextScan && !this.#pending.size)) return;
    const scanning = now >= this.#nextScan;
    const runs = path.join(this.root, "runs");
    const started = performance.now(), candidates: Array<{ id: string; proof: TreeProof }> = [];
    try {
      if (!await unaliasedDirectory(runs)) {
        await fsp.lstat(runs); // An absent runs directory is not a permanent fault.
        throw new Error("aliased/unknown run namespace");
      }
      // At least one entry per slice so a setImmediate continuation always progresses.
      for (let count = 0; count < maxEntries && (count === 0 || performance.now() - started < budgetMs) && !this.#closed; count++) {
        let id: string;
        const queued = this.#pending.values().next();
        if (!queued.done) {
          id = queued.value; this.#pending.delete(id);
          try { if (!(await fsp.lstat(path.join(runs, id))).isDirectory()) continue; }
          catch { this.#unwatchRun(id); continue; }
        } else if (scanning) {
          if (!this.#directory) {
            if (!(await owned(runs)).isDirectory()) return;
            this.#directory = await fsp.opendir(runs);
          }
          const entry = await this.#directory.read();
          if (!entry) {
            await this.#directory.close(); this.#directory = undefined; this.#nextScan = now + LEGACY_ARCHIVE_DEADLINE_MS;
            this.#runWatchErrors.clear();
            break;
          }
          if (!entry.isDirectory()) continue;
          id = entry.name;
        } else break;
        if (!RUN_ID.test(id) || candidates.some(run => run.id === id) || this.options.isRetained(id)) continue;
        this.health.checked++;
        const proof = await legacyRunTreeProof(path.join(runs, id), now, this.policy.legacyRunArchiveAgeMs ?? DEFAULT_AGE_MS);
        if (proof) candidates.push({ id, proof });
        else {
          this.health.skipped++;
          // Pre-existing (scan) and re-armed (queued) open runs get the bounded
          // status watcher, so their atomic close wakes without the deadline.
          if (!this.#closed && !this.#runWatchers.has(id) && await openRun(path.join(runs, id))) this.#watchRun(id);
        }
        await yieldTurn();
      }
      if (!candidates.length || this.#closed) return;
      const ids = candidates.map(run => run.id), fence = await this.#referenceFence(ids);
      if (fence === undefined) return;
      const retired = path.join(this.root, "runs-retired"), day = path.join(retired, new Date(now).toISOString().slice(0, 10));
      // Validate each component BEFORE creating children: do not follow a
      // pre-existing destination symlink, even temporarily.
      for (const directory of [retired, day]) {
        try { await fsp.mkdir(directory, { mode: 0o700 }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
        const stat = await owned(directory);
        if (!stat.isDirectory() || (stat.mode & 0o077) !== 0 || !await unaliasedDirectory(directory)) throw new Error("unsafe retirement root");
      }
      if ((await owned(runs)).dev !== (await owned(day)).dev) throw new Error("retirement must stay on the same filesystem");
      // A private unique slice prevents overwriting a previously retired ID.
      const batch = await fsp.mkdtemp(path.join(day, "slice-"));
      const moved: typeof candidates = [];
      for (const run of candidates) {
        if (this.options.isRetained(run.id) || this.#closed || !this.#enabled()) continue;
        const source = path.join(runs, run.id);
        if ((await legacyRunTreeProof(source, now, this.policy.legacyRunArchiveAgeMs ?? DEFAULT_AGE_MS))?.fingerprint !== run.proof.fingerprint) continue;
        if (await this.#referenceFence(ids) !== fence || this.options.isRetained(run.id) || this.#closed || !this.#enabled()) break;
        if (!await unaliasedDirectory(runs) || !await unaliasedDirectory(batch)) throw new Error("run namespace changed");
        // No copy fallback on EXDEV, no compression and no source unlink.
        await fsp.rename(source, path.join(batch, run.id));
        // A directory watch follows the inode: stop observing retired bytes.
        this.#unwatchRun(run.id);
        moved.push(run); this.health.archived++;
      }
      // Known live actors/managers can reopen by path, so retain their original
      // namespace. Opaque descriptors need no census: rename preserves inodes.
      if (await this.#referenceFence(ids) !== fence || moved.some(run => this.options.isRetained(run.id)) || !this.#enabled()) {
        for (const run of moved) {
          if (!await unaliasedDirectory(runs) || !await unaliasedDirectory(batch)) throw new Error("namespace changed; moved bytes retained");
          const destination = path.join(runs, run.id);
          try { await fsp.lstat(destination); throw new Error("run ID reused; moved bytes retained"); }
          catch (error) { if (!gone(error)) throw error; }
          await fsp.rename(path.join(batch, run.id), destination); this.health.archived--;
        }
      }
      // Persist both sides of the rename on platforms supporting directory fsync.
      for (const directory of [batch, day, retired, runs]) {
        const handle = await fsp.open(directory, "r");
        try { await handle.sync(); } finally { await handle.close(); }
      }
    } catch (error) {
      // A partial slice is already a valid retained source, not an ambiguous
      // compression transaction. Never remove it, even when recording a fault.
      if (!gone(error)) { this.health.error = String(error); this.#blocked = true; }
    } finally {
      if (this.health.checked) {
        try { writeJsonAtomic(path.join(this.root, "archive-retention.json"), this.health); } catch { /* next slice retries */ }
      }
    }
  }

  /** Known path-based writers and publication custody veto a move. Generation
   * checks bracket renames; this is NOT authority to delete retained bytes. */
  async #referenceFence(ids: readonly string[]): Promise<string | undefined> {
    const identities: unknown[] = [];
    const add = async (file: string, directory: boolean): Promise<fs.Stats | undefined> => {
      try {
        const stat = await owned(file);
        if (directory ? !stat.isDirectory() : !stat.isFile()) throw new Error("unsafe reference");
        identities.push([file, stat.dev, stat.ino, stat.mode, stat.size, stat.mtimeMs, stat.ctimeMs]);
        return stat;
      } catch (error) { if (gone(error)) { identities.push([file, null]); return; } throw error; }
    };
    try {
      for (const kind of ["requests", "processing", "delivery-outbox"]) {
        const directory = path.join(this.root, kind);
        if (await add(directory, true)) {
          const entries = await fsp.opendir(directory);
          try { if (await entries.read()) return; } finally { await entries.close(); }
        }
      }
      for (const id of ids) {
        // Legacy admitted handles with no surviving manager are still custody;
        // do not infer publication from their immutable admission snapshot.
        if (await add(path.join(this.root, "agents", `${id}.json`), false)) return;
      }
      for (const root of this.options.actorRoots ?? []) {
        if (!await add(root, true)) continue;
        const entries = await fsp.opendir(root);
        for await (const entry of entries) { if (entry.name.startsWith("removal-")) return; await yieldTurn(); }
        const registry = path.join(root, "actors.json"), stat = await add(registry, false);
        if (!stat) continue;
        if (stat.size > 1024 * 1024) return;
        const value = JSON.parse(await fsp.readFile(registry, "utf8"));
        if (!Array.isArray(value?.actors) || value.actors.length > 10_000) return;
        for (const actor of value.actors) {
          if (!actor || typeof actor.id !== "string" || (actor.lastRunId !== undefined && typeof actor.lastRunId !== "string")) return;
          if (ids.includes(actor.lastRunId) || ids.includes(typeof actor.inFlightRun === "string" ? actor.inFlightRun : actor.inFlightRun?.id)) return;
          await yieldTurn();
        }
      }
      return JSON.stringify(identities);
    } catch { return; }
  }

}
