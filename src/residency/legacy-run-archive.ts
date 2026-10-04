import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { setImmediate as yieldTurn } from "node:timers/promises";
import { writeJsonAtomic } from "../core/atomic-write.js";
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
    await walk(directory, 0);
    identities.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    return rootRecord && { ...rootRecord, fingerprint: JSON.stringify(identities) };
  } catch { return; }
};

/** Full Linux /proc observation, including open descriptors, cwd/root and mmap
 * after fd close. Permission errors are UNKNOWN (not absence). procRoot is an
 * injection seam for isolated tests, never a configurable production bypass. */
export const linuxRunFilesIdle = async (directories: readonly string[], procRoot = "/proc"): Promise<boolean> => {
  if (process.platform !== "linux") return false;
  const contains = (value: string) => {
    const target = value.replace(/ \(deleted\)$/, "");
    return directories.some(dir => target === dir || target.startsWith(dir + path.sep));
  };
  const started = performance.now();
  let inspected = 0;
  const expired = () => ++inspected > 32_768 || performance.now() - started > 1_000;
  const births = new Map<string, string>();
  try {
    const processes = await fsp.opendir(procRoot);
    for await (const entry of processes) {
      if (!/^\d+$/.test(entry.name)) continue;
      if (expired()) return false;
      await yieldTurn();
      const processDir = path.join(procRoot, entry.name);
      try {
        // Zombies have already released files/fs/mm; kernel threads have no
        // userspace file-table custody. Missing cwd for any other process is
        // still uncertainty, not permission to ignore it.
        const stat = await fsp.readFile(path.join(processDir, "stat"), "utf8");
        const fields = stat.slice(stat.lastIndexOf(") ") + 2).trim().split(/\s+/);
        if (!/^\d+$/.test(fields[19] ?? "")) return false;
        births.set(entry.name, fields[19]!);
        if (fields[0] === "Z" || fields[0] === "X" || (Number(fields[6]) & 0x00200000) !== 0) continue;
        if (!fields[0] || !Number.isSafeInteger(Number(fields[6]))) return false;
        for (const name of ["cwd", "root"]) if (contains(await fsp.readlink(path.join(processDir, name)))) return false;
        const descriptors = await fsp.opendir(path.join(processDir, "fd"));
        for await (const fd of descriptors) {
          if (expired()) return false;
          await yieldTurn();
          try { if (contains(await fsp.readlink(path.join(processDir, "fd", fd.name)))) return false; }
          catch (error) { if (!gone(error)) throw error; }
        }
        const mapsFile = path.join(processDir, "maps");
        if ((await fsp.stat(mapsFile)).size > 16 * 1024 * 1024) return false;
        const maps = await fsp.readFile(mapsFile, "utf8");
        if (maps.length > 16 * 1024 * 1024) return false;
        for (const line of maps.split("\n")) {
          const target = /^\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+(.*)$/.exec(line)?.[1];
          if (target && contains(target.replace(/\\([0-7]{3})/g, (_, octal: string) => String.fromCharCode(parseInt(octal, 8))))) return false;
        }
      } catch (error) {
        // An exited process cannot hold files. A missing cwd/fd/maps in a still
        // present process, unlike a descriptor closing during enumeration, is
        // incomplete proof (including zombies/kthreads with inaccessible data).
        try { await fsp.stat(processDir); } catch (missing) { if (gone(missing)) continue; }
        return false;
      }
    }
    // A new/reused PID may have opened a run after the first directory cursor
    // passed its slot. Require the second census to contain no new identities;
    // vanished processes have released their file tables and are harmless.
    const current = await fsp.opendir(procRoot);
    for await (const entry of current) {
      if (!/^\d+$/.test(entry.name)) continue;
      if (expired()) return false;
      await yieldTurn();
      try {
        const stat = await fsp.readFile(path.join(procRoot, entry.name, "stat"), "utf8");
        const birth = stat.slice(stat.lastIndexOf(") ") + 2).trim().split(/\s+/)[19];
        if (!birth || births.get(entry.name) !== birth) return false;
      } catch (error) { if (!gone(error)) return false; }
    }
    return !expired();
  } catch { return false; }
};

interface ArchiveOptions {
  actorRoots?: readonly string[];
  isRetained: (id: string) => boolean;
  /** Must establish the entire process-file proof, not just worker PID death. */
  processFilesIdle?: (directories: readonly string[]) => Promise<boolean>;
}

/** Off-request-path archival. One bounded discovery slice and one compression
 * child at a time; close joins it. Staging and journal remain on any failure.
 * A daily bundle is concatenated gzip/tar members: restore with --ignore-zeros. */
export class ResidentLegacyRunArchive {
  #directory: fs.Dir | undefined;
  #nextScan = 0;
  #timer: NodeJS.Timeout | undefined;
  #work: Promise<void> | undefined;
  #closed = false;
  #blocked = false;
  #archiveChecked = false;
  readonly health = { checked: 0, archived: 0, skipped: 0, error: "", processProofIncomplete: 0 };
  constructor(readonly root: string, readonly policy: LegacyRunArchivePolicy, readonly options: ArchiveOptions) {}

  #enabled(): boolean { return this.policy.legacyRunArchiveEnabled !== false; }

  start(): void {
    if (this.#timer || this.#closed) return;
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
    if (this.#closed || this.#blocked || !this.#enabled() || process.platform !== "linux" || now < this.#nextScan) return;
    const runs = path.join(this.root, "runs"), archive = path.join(this.root, "archive");
    const idle = this.options.processFilesIdle ?? linuxRunFilesIdle;
    const started = performance.now(), candidates: Array<{ id: string; proof: TreeProof }> = [];
    let stagedBatch: string | undefined;
    try {
      if (!this.#archiveChecked) {
        try {
          const archiveStat = await owned(archive);
          if (!archiveStat.isDirectory() || (archiveStat.mode & 0o077) !== 0) throw new Error("unsafe archive root");
          const existing = await fsp.opendir(archive);
          let count = 0;
          for await (const entry of existing) {
            if (++count > 1024 || entry.name.startsWith(".staging-")) throw new Error("interrupted archive slice needs operator recovery");
            await yieldTurn();
          }
        } catch (error) { if (!gone(error)) throw error; }
        this.#archiveChecked = true;
      }
      if (!this.#directory) {
        // If Linux cannot observe the process file tables at all, no run can
        // acquire a complete archival proof. Do not spend idle CPU traversing
        // thousands of trees just to rediscover that global veto per batch.
        // This is only an early veto, never cached authorization: eligible
        // batches still repeat the full target-specific proof at every boundary.
        if (!this.options.processFilesIdle && !await linuxRunFilesIdle([])) {
          this.health.processProofIncomplete++; this.#nextScan = now + 60_000; return;
        }
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
      const fence = await this.#referenceFence(candidates.map(run => run.id));
      if (fence === undefined) return;
      if (!await idle(candidates.map(run => path.join(runs, run.id)))) { this.health.processProofIncomplete++; return; }
      if (!this.#enabled() || this.#closed) return;
      await fsp.mkdir(archive, { recursive: true, mode: 0o700 });
      const archiveStat = await owned(archive);
      if (!archiveStat.isDirectory() || (archiveStat.mode & 0o077) !== 0) throw new Error("unsafe archive root");
      if (await this.#referenceFence(candidates.map(run => run.id)) !== fence) return;
      const batch = await fsp.mkdtemp(path.join(archive, ".staging-"));
      stagedBatch = batch;
      const staged: typeof candidates = [];
      for (const run of candidates) {
        if (this.options.isRetained(run.id) || this.#closed) continue;
        const source = path.join(runs, run.id);
        if ((await legacyRunTreeProof(source, now, this.policy.legacyRunArchiveAgeMs ?? DEFAULT_AGE_MS))?.fingerprint !== run.proof.fingerprint) continue;
        await fsp.rename(source, path.join(batch, run.id)); staged.push(run);
      }
      if (!staged.length) { await fsp.rmdir(batch); return; }
      // Recheck after the rename: Linux fd/map paths track renamed inodes.
      if (!await idle(staged.map(run => path.join(batch, run.id))) || staged.some(run => this.options.isRetained(run.id)) ||
          await this.#referenceFence(candidates.map(run => run.id)) !== fence) {
        for (const run of staged) {
          const destination = path.join(runs, run.id);
          try { await fsp.lstat(destination); throw new Error("run ID reused; staged bytes retained"); }
          catch (error) { if (!gone(error)) throw error; }
          await fsp.rename(path.join(batch, run.id), destination);
        }
        await fsp.rmdir(batch); return;
      }
      await this.#commitBatch(batch, staged, now, fence, candidates.map(run => run.id));
    } catch (error) {
      if (stagedBatch || !gone(error)) { this.health.error = String(error); this.#blocked = true; }
    } finally {
      if (this.health.checked || this.health.processProofIncomplete) {
        try { writeJsonAtomic(path.join(this.root, "archive-retention.json"), this.health); } catch { /* next slice retries */ }
      }
    }
  }

  /** Bound asynchronous custody preparation. A large/unreadable registry or any
   * pending resident exchange/outbox is a veto. Check the generation again at
   * staging, publication and deletion; never cache an authorization across I/O. */
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

  async #commitBatch(batch: string, runs: Array<{ id: string; proof: TreeProof }>, now: number, fence: string, fenceIds: readonly string[]): Promise<void> {
    const archive = path.dirname(batch), day = new Date(now).toISOString().slice(0, 10);
    const bundle = path.join(archive, `${day}.tar.gz`), manifest = path.join(archive, `${day}.manifest.jsonl`);
    const fragment = path.join(batch, `${randomUUID()}.tar.gz`);
    const output = await fsp.open(fragment, "wx", 0o600);
    try {
      await new Promise<void>((resolve, reject) => {
        const child = spawn("tar", ["-czf", "-", "-C", batch, "--", ...runs.map(run => run.id)], { stdio: ["ignore", output.fd, "ignore"] });
        child.once("error", reject);
        child.once("exit", (code, signal) => code === 0 ? resolve() : reject(new Error(`archive tar failed: ${code ?? signal}`)));
      });
      await output.sync();
    } finally { await output.close(); }
    // tar cannot authorize deletion if anything changed while compression ran.
    if (!await (this.options.processFilesIdle ?? linuxRunFilesIdle)(runs.map(run => path.join(batch, run.id)))) throw new Error("process proof changed; staged bytes retained");
    for (const run of runs) {
      if (this.options.isRetained(run.id) || (await legacyRunTreeProof(path.join(batch, run.id), now, this.policy.legacyRunArchiveAgeMs ?? DEFAULT_AGE_MS))?.fingerprint !== run.proof.fingerprint) {
        throw new Error("run proof changed; staged bytes retained");
      }
    }
    if (await this.#referenceFence(fenceIds) !== fence || !this.#enabled()) throw new Error("custody changed; staged bytes retained");
    const openAppend = async (file: string) => {
      const handle = await fsp.open(file, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_APPEND | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
      const stat = await handle.stat();
      if (!stat.isFile() || stat.nlink !== 1 || (process.getuid && stat.uid !== process.getuid())) { await handle.close(); throw new Error("unsafe archive file"); }
      return handle;
    };
    const target = await openAppend(bundle);
    let offset: number;
    const length = (await owned(fragment)).size;
    try {
      offset = (await target.stat()).size;
      // Crash recovery retains the original directories and compressed fragment.
      // The journal records the last good append boundary for tail repair.
      writeJsonAtomic(path.join(batch, "pending.json"), { bundle, offset, length, ids: runs.map(run => run.id) }, { durable: true });
      const source = await fsp.open(fragment, "r");
      try {
        const buffer = Buffer.alloc(64 * 1024);
        for (;;) {
          const { bytesRead } = await source.read(buffer, 0, buffer.length, null);
          if (!bytesRead) break;
          let written = 0;
          while (written < bytesRead) written += (await target.write(buffer, written, bytesRead - written)).bytesWritten;
          await yieldTurn();
        }
      } finally { await source.close(); }
      await target.sync();
    } finally { await target.close(); }
    const receipt = await openAppend(manifest);
    try {
      for (const run of runs) await receipt.writeFile(JSON.stringify({ format: 1, id: run.id, status: run.proof.status,
        finishedAt: run.proof.finishedAt, archivedAt: now, bundle: path.basename(bundle), offset, length }) + "\n");
      await receipt.sync();
    } finally { await receipt.close(); }
    // Only fsynced bundle + fsynced manifest discharge the staged bytes. Recheck
    // even after slow compression/append; new custody keeps both copies.
    if (await this.#referenceFence(fenceIds) !== fence || !this.#enabled() ||
        runs.some(run => this.options.isRetained(run.id)) ||
        !await (this.options.processFilesIdle ?? linuxRunFilesIdle)(runs.map(run => path.join(batch, run.id)))) {
      throw new Error("final custody/process proof changed; staged bytes retained");
    }
    for (const run of runs) {
      if ((await legacyRunTreeProof(path.join(batch, run.id), now, this.policy.legacyRunArchiveAgeMs ?? DEFAULT_AGE_MS))?.fingerprint !== run.proof.fingerprint) {
        throw new Error("final tree proof changed; staged bytes retained");
      }
    }
    const archiveDirectory = await fsp.open(archive, "r");
    try { await archiveDirectory.sync(); } finally { await archiveDirectory.close(); }
    await fsp.rm(batch, { recursive: true });
    this.health.archived += runs.length;
  }
}
