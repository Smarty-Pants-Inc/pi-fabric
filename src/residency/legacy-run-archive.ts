import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { setImmediate as yieldTurn } from "node:timers/promises";
import { writeJsonAtomic } from "../core/atomic-write.js";
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

/** Off-request-path retirement by same-filesystem rename only. Sources remain
 * ordinary directories forever: opaque descriptor custody cannot lose bytes.
 * The legacy public names/policy keys are retained for configuration compatibility. */
export class ResidentLegacyRunArchive {
  #directory: fs.Dir | undefined;
  #nextScan = 0;
  #timer: NodeJS.Timeout | undefined;
  #work: Promise<void> | undefined;
  #closed = false;
  #blocked = false;
  readonly health = { checked: 0, archived: 0, skipped: 0, error: "" };
  constructor(readonly root: string, readonly policy: LegacyRunArchivePolicy, readonly options: ArchiveOptions) {}

  #enabled(): boolean { return this.policy.legacyRunArchiveEnabled !== false; }

  start(): void {
    if (!retentionV2Enabled() || this.#timer || this.#closed) return;
    this.#timer = setInterval(() => {
      if (!this.#work) {
        this.#work = this.sweep().catch(error => { this.health.error = String(error); }).finally(() => { this.#work = undefined; });
      }
    }, 250);
    this.#timer.unref();
  }

  async close(): Promise<void> {
    this.#closed = true;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
    await this.#work;
    const directory = this.#directory; this.#directory = undefined;
    await directory?.close();
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
    if (this.#blocked || !this.#enabled() || now < this.#nextScan) return;
    const runs = path.join(this.root, "runs");
    const started = performance.now(), candidates: Array<{ id: string; proof: TreeProof }> = [];
    try {
      if (!await unaliasedDirectory(runs)) {
        await fsp.lstat(runs); // An absent runs directory is not a permanent fault.
        throw new Error("aliased/unknown run namespace");
      }
      if (!this.#directory) {
        if (!(await owned(runs)).isDirectory()) return;
        this.#directory = await fsp.opendir(runs);
      }
      for (let count = 0; count < maxEntries && performance.now() - started < budgetMs && !this.#closed; count++) {
        const entry = await this.#directory.read();
        if (!entry) { await this.#directory.close(); this.#directory = undefined; this.#nextScan = now + 60_000; break; }
        if (!entry.isDirectory() || !/^[A-Za-z0-9_-]+$/.test(entry.name) || this.options.isRetained(entry.name)) continue;
        this.health.checked++;
        const proof = await legacyRunTreeProof(path.join(runs, entry.name), now, this.policy.legacyRunArchiveAgeMs ?? DEFAULT_AGE_MS);
        if (proof) candidates.push({ id: entry.name, proof }); else this.health.skipped++;
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
