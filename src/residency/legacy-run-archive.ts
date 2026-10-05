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
/** /proc reports physical names, not the spelling used to open a file. Veto
 * every symlink component (including mesh/residency ancestors), rather than
 * treating a lexical containment miss as absence of custody. Revalidate at
 * each archive boundary; this is never cached authorization. */
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

/** A stable visible PID list is not a complete census. Require a real,
 * unfiltered procfs rooted in the initial PID/user/cgroup namespaces and the
 * host init's mount namespace. Linux reserves these initial namespace inodes;
 * an unknown kernel/view is a veto, never a fallback to visible PIDs. Comparing
 * PID 1's mount namespace also excludes host-PID containers/private mounts.
 * Repeat the whole observation around BOTH censuses, not just at preflight. */
const linuxProcessCensusFence = async (procRoot: string): Promise<string | undefined> => {
  try {
    if (!await unaliasedDirectory(procRoot) || (await fsp.statfs(procRoot)).type !== 0x9fa0) return;
    const namespaces: string[] = [];
    for (const [name, initial] of [["pid", "pid:[4026531836]"], ["user", "user:[4026531837]"], ["cgroup", "cgroup:[4026531835]"]] as const) {
      const value = await fsp.readlink(path.join(procRoot, "self", "ns", name));
      if (value !== initial) return;
      namespaces.push(value);
    }
    const mountNamespace = await fsp.readlink(path.join(procRoot, "self", "ns", "mnt"));
    if (!/^mnt:\[\d+\]$/.test(mountNamespace) || mountNamespace !== await fsp.readlink(path.join(procRoot, "1", "ns", "mnt"))) return;
    namespaces.push(mountNamespace);
    const mountinfo = await fsp.readFile(path.join(procRoot, "self", "mountinfo"), "utf8");
    if (mountinfo.length > 1024 * 1024) return;
    const decode = (value: string) => value.replace(/\\([0-7]{3})/g, (_, octal: string) => String.fromCharCode(parseInt(octal, 8)));
    let roots = 0;
    for (const line of mountinfo.trim().split("\n")) {
      const [before, after] = line.split(" - ");
      if (!before || !after) return;
      const fields = before.split(" "), superblock = after.split(" ");
      const target = decode(fields[4] ?? "");
      if (target === procRoot) {
        if (++roots !== 1 || decode(fields[3] ?? "") !== "/" || superblock[0] !== "proc") return;
        const options = `${fields[5]},${superblock[2]}`.split(",");
        if (options.some(option => option.startsWith("hidepid=") && option !== "hidepid=0" || option.startsWith("subset="))) return;
      } else if (target.startsWith(procRoot + path.sep)) {
        // Only kernel-global subtrees may be separately mounted. A bind mount
        // over a PID/task, self, or any unknown part can hide custody.
        if (!/^(sys|fs|irq|bus)(\/|$)/.test(target.slice(procRoot.length + 1))) return;
      }
    }
    if (roots !== 1) return;
    const stat = await fsp.stat(procRoot);
    return JSON.stringify([namespaces, stat.dev, stat.ino, mountinfo]);
  } catch { return; }
};

/** Full Linux /proc observation, including open descriptors, cwd/root and mmap
 * after fd close. Permission errors are UNKNOWN (not absence). procRoot is an
 * injection seam for isolated tests, never a configurable production bypass. */
export const linuxRunFilesIdle = async (directories: readonly string[], procRoot = "/proc"): Promise<boolean> => {
  if (process.platform !== "linux") return false;
  for (const directory of directories) if (!await unaliasedDirectory(directory)) return false;
  const contains = (value: string) => {
    const target = value.replace(/ \(deleted\)$/, "");
    return directories.some(dir => target === dir || target.startsWith(dir + path.sep));
  };
  const started = performance.now();
  let inspected = 0;
  const expired = () => ++inspected > 32_768 || performance.now() - started > 1_000;
  const births = new Map<string, string>();
  const fields = async (directory: string) => {
    const stat = await fsp.readFile(path.join(directory, "stat"), "utf8");
    const value = stat.slice(stat.lastIndexOf(") ") + 2).trim().split(/\s+/);
    if (!value[0] || !/^\d+$/.test(value[19] ?? "") || !Number.isSafeInteger(Number(value[6]))) throw new Error("unknown task identity");
    return value;
  };
  const census = async (verify: boolean): Promise<boolean> => {
    const processes = await fsp.opendir(procRoot);
    for await (const entry of processes) {
      if (!/^\d+$/.test(entry.name)) continue;
      if (expired()) return false;
      const processDir = path.join(procRoot, entry.name);
      try {
        const leader = await fields(processDir);
        const deadLeader = leader[0] === "Z" || leader[0] === "X";
        const tasks = await fsp.opendir(path.join(processDir, "task"));
        let taskCount = 0;
        for await (const task of tasks) {
          if (!/^\d+$/.test(task.name)) continue;
          if (expired()) return false;
          await yieldTurn();
          const taskDir = path.join(processDir, "task", task.name), key = `${entry.name}/${task.name}`;
          try {
            const identity = await fields(taskDir);
            taskCount++;
            if (verify) {
              if (births.get(key) !== identity[19]) return false;
              // A formerly dead task cannot become live without invalidating
              // the proof, even when its leader's identity has not changed.
              if (deadLeader && identity[0] !== "Z" && identity[0] !== "X") return false;
              continue;
            }
            births.set(key, identity[19]!);
            // pthread_exit can leave a zombie leader with surviving writers.
            // Such a group is held, never inferred dead from leader stat/fd.
            if (deadLeader && identity[0] !== "Z" && identity[0] !== "X") return false;
            if (identity[0] === "Z" || identity[0] === "X" || (Number(identity[6]) & 0x00200000) !== 0) continue;
            for (const name of ["cwd", "root"]) if (contains(await fsp.readlink(path.join(taskDir, name)))) return false;
            const descriptors = await fsp.opendir(path.join(taskDir, "fd"));
            for await (const fd of descriptors) {
              if (expired()) return false;
              await yieldTurn();
              try { if (contains(await fsp.readlink(path.join(taskDir, "fd", fd.name)))) return false; }
              catch (error) { if (!gone(error)) throw error; }
            }
            const mapsFile = path.join(taskDir, "maps");
            if ((await fsp.stat(mapsFile)).size > 16 * 1024 * 1024) return false;
            const maps = await fsp.readFile(mapsFile, "utf8");
            if (maps.length > 16 * 1024 * 1024) return false;
            for (const line of maps.split("\n")) {
              const target = /^\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+(.*)$/.exec(line)?.[1];
              if (target && contains(target.replace(/\\([0-7]{3})/g, (_, octal: string) => String.fromCharCode(parseInt(octal, 8))))) return false;
            }
          } catch (error) {
            try { await fsp.stat(taskDir); } catch (missing) { if (gone(missing)) continue; }
            return false;
          }
        }
        if (!taskCount) return false;
      } catch (error) {
        try { await fsp.stat(processDir); } catch (missing) { if (gone(missing)) continue; }
        return false;
      }
    }
    return true;
  };
  // Both censuses include TIDs, not only process leaders. New/reused tasks
  // invalidate the proof; disappeared tasks have released their custody.
  try {
    const fence = await linuxProcessCensusFence(procRoot);
    if (!fence || !await census(false) || await linuxProcessCensusFence(procRoot) !== fence ||
        !await census(true) || await linuxProcessCensusFence(procRoot) !== fence || expired()) return false;
    for (const directory of directories) if (!await unaliasedDirectory(directory)) return false;
    return true;
  }
  catch { return false; }
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

  async #filesIdle(directories: readonly string[]): Promise<boolean> {
    // The injection seam may supply a complete isolated process view, but it
    // must not bypass the namespace veto. Check both sides of asynchronous I/O.
    for (const directory of directories) if (!await unaliasedDirectory(directory)) return false;
    if (!await (this.options.processFilesIdle ?? linuxRunFilesIdle)(directories)) return false;
    for (const directory of directories) if (!await unaliasedDirectory(directory)) return false;
    return true;
  }

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
    const idle = (directories: readonly string[]) => this.#filesIdle(directories);
    const started = performance.now(), candidates: Array<{ id: string; proof: TreeProof }> = [];
    let stagedBatch: string | undefined;
    try {
      if (!await unaliasedDirectory(runs)) {
        await fsp.lstat(runs); // An absent runs directory is not a permanent fault.
        throw new Error("aliased/unknown run namespace");
      }
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
      if (!archiveStat.isDirectory() || (archiveStat.mode & 0o077) !== 0 || !await unaliasedDirectory(archive)) throw new Error("unsafe archive root");
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
    if (!await this.#filesIdle(runs.map(run => path.join(batch, run.id)))) throw new Error("process proof changed; staged bytes retained");
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
        !await this.#filesIdle(runs.map(run => path.join(batch, run.id)))) {
      throw new Error("final custody/process proof changed; staged bytes retained");
    }
    for (const run of runs) {
      if ((await legacyRunTreeProof(path.join(batch, run.id), now, this.policy.legacyRunArchiveAgeMs ?? DEFAULT_AGE_MS))?.fingerprint !== run.proof.fingerprint) {
        throw new Error("final tree proof changed; staged bytes retained");
      }
    }
    const archiveDirectory = await fsp.open(archive, "r");
    try { await archiveDirectory.sync(); } finally { await archiveDirectory.close(); }
    if (!await unaliasedDirectory(batch) || !await unaliasedDirectory(path.join(this.root, "runs"))) {
      throw new Error("final namespace changed; staged bytes retained");
    }
    await fsp.rm(batch, { recursive: true });
    this.health.archived += runs.length;
  }
}
