import fs from "node:fs";
import { createHash } from "node:crypto";
import { diagnostics } from "../core/diagnostics.js";
import fsp from "node:fs/promises";
import path from "node:path";
import { setImmediate as yieldTurn } from "node:timers/promises";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { isWatchLimitError } from "../core/watch-fallback.js";
import { retentionV2Enabled } from "../storage/retention-platform.js";
import { processAlive } from "../storage/scratch.js";
import { retainCleanupRoot, restoreCleanupRoot, type CleanupRootCustody } from "../agents/worktree-manager.js";

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
type LegacyRunManifest = { format: 1; entries: Array<{ path: string; type: "directory" } | { path: string; type: "file"; size: number; sha256: string }> };
const legacyRunManifest = (root: string): LegacyRunManifest => {
  const entries: LegacyRunManifest["entries"] = [];
  const walk = (file: string, relative: string): void => {
    const stat = fs.lstatSync(file);
    if (stat.isSymbolicLink()) throw new Error(`legacy run manifest refuses a link: ${relative || "."}`);
    if (stat.isDirectory()) {
      entries.push({ path: relative || ".", type: "directory" });
      for (const name of fs.readdirSync(file).sort()) walk(path.join(file, name), relative ? `${relative}/${name}` : name);
      return;
    }
    if (!stat.isFile() || stat.nlink !== 1) throw new Error(`legacy run manifest refuses an unsafe entry: ${relative}`);
    if (!fs.constants.O_NOATIME) throw new Error("legacy run hashing requires no-atime reads");
    const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | fs.constants.O_NOATIME);
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.dev !== stat.dev || opened.ino !== stat.ino || opened.size !== stat.size) throw new Error(`legacy run changed before hashing: ${relative}`);
      const hash = createHash("sha256"), buffer = Buffer.alloc(64 * 1024); let size = 0;
      for (;;) { const count = fs.readSync(fd, buffer, 0, buffer.length, null); if (!count) break; size += count; hash.update(buffer.subarray(0, count)); }
      const current = fs.lstatSync(file);
      if (size !== stat.size || current.isSymbolicLink() || current.dev !== stat.dev || current.ino !== stat.ino || current.size !== stat.size || current.mtimeMs !== stat.mtimeMs) {
        throw new Error(`legacy run changed while hashing: ${relative}`);
      }
      entries.push({ path: relative, type: "file", size, sha256: hash.digest("hex") });
    } finally { fs.closeSync(fd); }
  };
  walk(root, "");
  return { format: 1, entries };
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
/** Pending custody that can discharge with no event this module observes. */
class HeldCustody extends Error {}
/** Named in-run custody artifacts (vs unknown bytes, which stay vetoed until an event/rescan). */
const custodyArtifacts = new Set(["unresolved-worker.json", "queued-result.json", "completion-recipient.json"]);
/** Proof, or the earliest time a recheck can change the verdict (age or held custody). */
type TreeVerdict = { proof: TreeProof } | { retryAt?: number };
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
  const verdict = await inspectLegacyRunTree(directory, now, ageMs);
  return "proof" in verdict ? verdict.proof : undefined;
};

const inspectLegacyRunTree = async (directory: string, now: number, ageMs: number): Promise<TreeVerdict> => {
  const identities: unknown[] = [];
  let count = 0, newest = Number.NEGATIVE_INFINITY;
  let rootRecord: { status: string; finishedAt: number } | undefined;
  const walk = async (dir: string, depth: number): Promise<void> => {
    if (depth > 32 || ++count > 4096) throw new Error("tree limit");
    const dirStat = await owned(dir);
    if (!dirStat.isDirectory() || (dirStat.mode & 0o077) !== 0) throw new Error("unsafe run directory");
    identities.push([path.relative(directory, dir), dirStat.dev, dirStat.ino, dirStat.mode]);
    const statusFile = path.join(dir, "status.json"), statusStat = await owned(statusFile);
    if (!statusStat.isFile() || statusStat.size > 1024 * 1024) throw new Error("unsafe status");
    const record = JSON.parse(await fsp.readFile(statusFile, "utf8"));
    if (!record || (record.transport !== undefined && record.transport !== "process")) throw new Error("not a process run");
    if (depth === 0 && (record.sessionId !== undefined || record.processStartTime !== undefined)) throw new Error("not a legacy run");
    // An open root is woken by its status.json watcher; an open descendant has no carrier.
    if (!terminal.has(record.status)) throw depth === 0 ? new Error("open run") : new HeldCustody("open descendant");
    if (!Number.isSafeInteger(record.finishedAt) || record.finishedAt < 0) throw new Error("terminal proof missing");
    // Age is checked after the whole walk so a young tree yields its exact due time.
    newest = Math.max(newest, record.finishedAt);
    if (record.cleanupPending || record.pending || record.queuedArchiveCommitted === false) throw new HeldCustody("custody pending");
    if (depth === 0) rootRecord = { status: record.status, finishedAt: record.finishedAt };
    else if (record.sessionId !== undefined) {
      if (typeof record.sessionId !== "string" || !/^\d+$/.test(record.sessionId)) throw new Error("unknown descendant PID");
      if (processAlive(Number(record.sessionId))) throw new HeldCustody("live descendant PID");
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
        try { if (await children.read()) throw new HeldCustody("pending delivery"); } finally { await children.close(); }
        continue;
      }
      if (stat.isDirectory() && entry.name === "nested") {
        const children = await fsp.opendir(file);
        for await (const child of children) await walk(path.join(file, child.name), depth + 1);
        continue;
      }
      // unresolved-worker, queued-result, completion-recipient and all unknown
      // artifacts veto archival. They are not inferred settled from status.
      if (stat.isFile() && custodyArtifacts.has(entry.name)) throw new HeldCustody("pending run artifact");
      throw new Error("unknown run artifact");
    }
  };
  // Earliest instant age alone could stop vetoing (descendants seen so far).
  const ageDue = () => Number.isFinite(newest) && now - newest < ageMs ? newest + ageMs : undefined;
  try {
    if (!Number.isSafeInteger(ageMs) || ageMs < 0) return {};
    if (!await unaliasedDirectory(directory)) return {};
    await walk(directory, 0);
    if (!await unaliasedDirectory(directory)) return {};
    const due = ageDue();
    if (due !== undefined) return { retryAt: due };
    identities.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    return rootRecord ? { proof: { ...rootRecord, fingerprint: JSON.stringify(identities) } } : {};
  } catch (error) {
    // Held custody: recheck no earlier than the slow cadence and not before age allows.
    if (error instanceof HeldCustody) return { retryAt: Math.max(now + LEGACY_ARCHIVE_DEADLINE_MS, ageDue() ?? 0) };
    return {};
  }
};

interface ArchiveOptions {
  actorRoots?: readonly string[];
  isRetained: (id: string) => boolean;
}

const RUN_ID = /^[A-Za-z0-9_-]+$/;
/** Slow cadence for state with no event carrier: held-custody rechecks of the
 * named run, policy re-enable, and full passes only while the runs watch is
 * degraded. A healthy idle archive arms no timer; age eligibility arms one
 * unref'd one-shot at the earliest real due time. Never a sub-second poll. */
export const LEGACY_ARCHIVE_DEADLINE_MS = 60_000;
/** Bounded queue of event-named run IDs; overflow requests a full pass. */
const MAX_PENDING = 256;
/** Bounded due-run map; overflow requests a full pass at the dropped due time. */
const MAX_DUE = 4096;
/** setTimeout's maximum delay; longer dues re-arm on expiry. */
const MAX_DELAY_MS = 2 ** 31 - 1;
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
  /** Run ID -> earliest time a targeted recheck can change its verdict. */
  readonly #due = new Map<string, number>();
  /** Due IDs not yet seen by the open full pass (bounded by MAX_DUE); a
   * completed pass that never saw one cancels its stale obligation. */
  #scanMissing: Set<string> | undefined;
  /** Full pass owed at this time (due-map overflow). */
  #rescanAt: number | undefined;
  #work: Promise<void> | undefined;
  /** A full pass was requested while a slice ran; continue once it ends. */
  #scanAgain = false;
  /** Disabled policy observed runs it would otherwise inspect. */
  #disabledHeld = false;
  #closed = false;
  #blocked = false;
  readonly health = { checked: 0, archived: 0, skipped: 0, error: "" };
  constructor(readonly root: string, readonly policy: LegacyRunArchivePolicy, readonly options: ArchiveOptions) {}

  #enabled(): boolean { return this.policy.legacyRunArchiveEnabled !== false; }

  /** Event-driven: root/runs entry events and bounded per-run status.json
   * events queue the named run; a cursor with remaining work continues with
   * setImmediate; otherwise one one-shot is armed only for a real due item
   * (or the degraded slow fallback, or a disabled policy holding runs), and
   * none when nothing is due. */
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
    this.#runWatchers.clear(); this.#pending.clear(); this.#runWatchErrors.clear(); this.#due.clear(); this.#scanMissing = undefined;
    await this.#work;
    const directory = this.#directory; this.#directory = undefined;
    await directory?.close();
  }

  #enqueue(id: string): void {
    if (this.#closed) return;
    // Overflow is not lost work: it requests a full pass that visits every run.
    if (this.#pending.size < MAX_PENDING) this.#pending.add(id);
    else this.#nextScan = 0;
    if (!this.#work && !this.#immediate) this.#immediate = setImmediate(() => { this.#immediate = undefined; this.#run(); });
  }

  /** Record a targeted recheck; the earliest due item drives the one-shot. */
  #hold(id: string, at: number): void {
    if (this.#closed) return;
    this.#scanMissing?.delete(id); // A fresh obligation is not pruned by the open pass.
    if (this.#due.has(id) || this.#due.size < MAX_DUE) this.#due.set(id, at);
    else this.#rescanAt = Math.min(this.#rescanAt ?? at, at);
  }

  /** True when no event carrier covers the whole namespace: a degraded runs
   * watch, or a disabled policy holding runs (re-enable is a live config
   * mutation). A disabled policy over an empty namespace arms nothing. */
  #fallback(): boolean { return !this.#runsWatcher || (!this.#enabled() && this.#disabledHeld); }

  /** Arm at most one unref'd one-shot for the earliest real due item; none when idle. */
  #schedule(): void {
    if (this.#closed || this.#blocked || this.#work || this.#immediate) return;
    if (this.#deadline) clearTimeout(this.#deadline);
    this.#deadline = undefined;
    const now = Date.now();
    const full = this.#fallback();
    let at = full ? now + LEGACY_ARCHIVE_DEADLINE_MS : Number.POSITIVE_INFINITY;
    // Disabled: due/rescan obligations cannot drain; only the slow fallback may arm.
    if (this.#enabled()) {
      if (this.#rescanAt !== undefined) at = Math.min(at, this.#rescanAt);
      for (const due of this.#due.values()) at = Math.min(at, due);
    }
    if (at === Number.POSITIVE_INFINITY) return;
    this.#deadline = setTimeout(() => {
      this.#deadline = undefined;
      // Armed as a fallback (incl. disabled-then-re-enabled): one full pass.
      if (full || this.#fallback()) this.#nextScan = 0;
      this.#run();
    }, Math.min(MAX_DELAY_MS, Math.max(0, at - now)));
    this.#deadline.unref();
  }

  #run(): void {
    if (this.#closed || this.#work) return;
    if (this.#deadline) clearTimeout(this.#deadline);
    this.#deadline = undefined;
    if (!this.#runsWatcher && this.#rearmAttempts < REARM_ATTEMPTS && !this.#rearm) this.#watchRuns();
    // Promote due runs to targeted checks (bounded; the rest drain next slice).
    const now = Date.now();
    if (this.#rescanAt !== undefined && this.#rescanAt <= now) { this.#rescanAt = undefined; this.#nextScan = 0; }
    for (const [id, at] of this.#due) {
      if (this.#pending.size >= MAX_PENDING) break;
      if (at <= now) { this.#due.delete(id); this.#pending.add(id); }
    }
    this.#work = this.sweep().catch(error => { this.health.error = String(error); }).finally(() => {
      this.#work = undefined;
      if (this.#closed) return;
      // Remaining work (or a rescan requested mid-slice) continues without a timer.
      // An open cursor keeps the request for a fresh pass after this one.
      const again = this.#scanAgain && !this.#directory;
      if (again) { this.#scanAgain = false; this.#nextScan = 0; }
      if (!this.#blocked && this.#enabled() && (this.#directory || this.#pending.size || again)) {
        if (!this.#immediate) this.#immediate = setImmediate(() => { this.#immediate = undefined; this.#run(); });
        return;
      }
      // A blocked (faulted) archive stays inert until restart: no standing poll.
      this.#schedule();
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
      // then the 60s degraded full-pass deadline alone. ENOSPC here is the inotify limit.
      const cause = isWatchLimitError(error) ? "inotify watch limit reached (fs.inotify.max_user_watches), not disk space" : String(error);
      diagnostics.warn("src/residency/legacy-run-archive.ts", `[fabric] legacy run archive watcher on ${path.join(this.root, "runs")} failed: ${cause}; ` +
        `rescanning once and re-arming with bounded backoff (${REARM_ATTEMPTS} attempts), then full passes every ${LEGACY_ARCHIVE_DEADLINE_MS / 1000}s`);
    }
    this.#rescan();
    if (this.#rearm || this.#rearmAttempts >= REARM_ATTEMPTS) { this.#schedule(); return; }
    const delay = REARM_BASE_MS * 2 ** this.#rearmAttempts++;
    this.#rearm = setTimeout(() => { this.#rearm = undefined; this.#watchRuns(); if (this.#runsWatcher) this.#rescan(); }, delay);
    this.#rearm.unref();
  }

  #rescan(): void {
    if (this.#closed) return;
    this.#nextScan = 0;
    if (this.#work) this.#scanAgain = true;
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
      // A repeat error stays unwatched until a slow targeted recheck re-arms it,
      // so a permanently failing watch cannot spin.
      watcher.on("error", () => {
        const repeat = this.#runWatchErrors.has(id);
        this.#runWatchErrors.add(id);
        this.#unwatchRun(id, watcher);
        if (!repeat) this.#enqueue(id);
        else { this.#hold(id, Date.now() + LEGACY_ARCHIVE_DEADLINE_MS); this.#schedule(); }
      });
      this.#runWatchers.set(id, watcher);
      if (this.#runWatchers.size > MAX_RUN_WATCHERS) {
        // The evicted open run has no carrier left: a slow targeted recheck.
        const [oldest] = this.#runWatchers.keys();
        this.#unwatchRun(oldest!);
        this.#hold(oldest!, Date.now() + LEGACY_ARCHIVE_DEADLINE_MS);
        this.#schedule();
      }
    } catch (error) {
      // Gone or not a directory: the queued check skips it. Otherwise (watch
      // limit) the run has no carrier: a slow targeted recheck.
      if (!gone(error) && (error as NodeJS.ErrnoException).code !== "ENOTDIR") {
        this.#hold(id, Date.now() + LEGACY_ARCHIVE_DEADLINE_MS); this.#schedule();
      }
    }
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
    if (this.#blocked) return;
    const runs = path.join(this.root, "runs");
    if (!this.#enabled()) {
      // Presence only (no proof, mutation or audit): does disabling hold any run?
      try {
        const directory = await fsp.opendir(runs);
        try { this.#disabledHeld = !!await directory.read(); } finally { await directory.close(); }
      } catch (error) { this.#disabledHeld = !gone(error); } // Unknown error is not confirmed absence.
      return;
    }
    if (now < this.#nextScan && !this.#pending.size) return;
    const scanning = now >= this.#nextScan;
    const started = performance.now(), candidates: Array<{ id: string; proof: TreeProof }> = [];
    let heldCandidates = (): void => undefined;
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
          try {
            // Confirmed non-directory: no run remains to recheck.
            if (!(await fsp.lstat(path.join(runs, id))).isDirectory()) { this.#due.delete(id); continue; }
          } catch (error) {
            this.#unwatchRun(id);
            // Confirmed absence cancels the due obligation; an unknown error
            // (EACCES, ...) is no negative evidence: keep it, and keep the pass from pruning it.
            if (gone(error) || (error as NodeJS.ErrnoException).code === "ENOTDIR") this.#due.delete(id);
            else this.#hold(id, Math.max(this.#due.get(id) ?? 0, Date.now() + LEGACY_ARCHIVE_DEADLINE_MS));
            continue;
          }
          this.#scanMissing?.delete(id);
        } else if (scanning) {
          if (!this.#directory) {
            if (!(await owned(runs)).isDirectory()) return;
            this.#directory = await fsp.opendir(runs);
            this.#scanMissing = new Set(this.#due.keys());
          }
          const entry = await this.#directory.read();
          if (!entry) {
            // A run present throughout the pass is listed; an unseen due ID was removed.
            for (const missing of this.#scanMissing ?? []) this.#due.delete(missing);
            this.#scanMissing = undefined;
            // No standing full pass: the next one is an explicit rescan or the degraded fallback.
            await this.#directory.close(); this.#directory = undefined; this.#nextScan = Number.POSITIVE_INFINITY;
            this.#runWatchErrors.clear();
            break;
          }
          if (!entry.isDirectory()) continue;
          id = entry.name;
          this.#scanMissing?.delete(id);
        } else break;
        if (!RUN_ID.test(id) || candidates.some(run => run.id === id)) continue;
        this.#due.delete(id);
        // Path custody (live manager, handover) has no event carrier: slow targeted recheck.
        if (this.options.isRetained(id)) { this.#hold(id, now + LEGACY_ARCHIVE_DEADLINE_MS); continue; }
        this.health.checked++;
        const verdict = await inspectLegacyRunTree(path.join(runs, id), now, this.policy.legacyRunArchiveAgeMs ?? DEFAULT_AGE_MS);
        if ("proof" in verdict) candidates.push({ id, proof: verdict.proof });
        else {
          this.health.skipped++;
          // Exact age due time or held in-tree custody; permanent vetoes wait for an event/rescan.
          if (verdict.retryAt !== undefined) this.#hold(id, verdict.retryAt);
          // Pre-existing (scan) and re-armed (queued) open runs get the bounded
          // status watcher, so their atomic close wakes without the deadline.
          if (!this.#closed && !this.#runWatchers.has(id) && await openRun(path.join(runs, id))) this.#watchRun(id);
        }
        await yieldTurn();
      }
      if (!candidates.length || this.#closed) return;
      // Every eligible candidate not retired by this slice is held custody
      // (reference fence, retention, policy, changed tree): slow targeted recheck.
      const settled = new Set<string>();
      heldCandidates = () => { for (const run of candidates) if (!settled.has(run.id)) this.#hold(run.id, now + LEGACY_ARCHIVE_DEADLINE_MS); };
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
      const moved: Array<{ run: (typeof candidates)[number]; custody: CleanupRootCustody }> = [];
      for (const run of candidates) {
        if (this.options.isRetained(run.id) || this.#closed || !this.#enabled()) continue;
        const source = path.join(runs, run.id);
        if ((await legacyRunTreeProof(source, now, this.policy.legacyRunArchiveAgeMs ?? DEFAULT_AGE_MS))?.fingerprint !== run.proof.fingerprint) continue;
        if (await this.#referenceFence(ids) !== fence || this.options.isRetained(run.id) || this.#closed || !this.#enabled()) break;
        if (!await unaliasedDirectory(runs) || !await unaliasedDirectory(day)) throw new Error("run namespace changed");
        const sourceStat = await owned(source), manifest = legacyRunManifest(source);
        const custody = retainCleanupRoot(source, day, run.id, "legacy resident run archival", undefined, {
          expected: { dev: sourceStat.dev, ino: sourceStat.ino, mtimeMs: sourceStat.mtimeMs },
          manifest,
          beforeRename: () => {
            if (this.options.isRetained(run.id) || this.#closed || !this.#enabled()) throw new Error("legacy run custody changed before rename");
          },
          verifyDestination: destination => {
            if (JSON.stringify(legacyRunManifest(destination)) !== JSON.stringify(manifest)) throw new Error("legacy run custody manifest mismatch");
          },
        });
        if (!custody) continue;
        // A directory watch follows the inode: stop observing retired bytes.
        this.#unwatchRun(run.id);
        moved.push({ run, custody }); this.health.archived++; settled.add(run.id);
      }
      // Known live actors/managers can reopen by path, so restore their original
      // namespace if the final reference fence changed. The receipt records both moves.
      if (await this.#referenceFence(ids) !== fence || moved.some(({ run }) => this.options.isRetained(run.id)) || !this.#enabled()) {
        for (const { run, custody } of moved) {
          if (!await unaliasedDirectory(runs)) throw new Error("run namespace changed; moved bytes retained");
          restoreCleanupRoot(custody); this.health.archived--; settled.delete(run.id);
        }
      }
    } catch (error) {
      // A partial slice is already a valid retained source, not an ambiguous
      // compression transaction. Never remove it, even when recording a fault.
      if (!gone(error)) { this.health.error = String(error); this.#blocked = true; }
    } finally {
      if (!this.#blocked) heldCandidates();
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
