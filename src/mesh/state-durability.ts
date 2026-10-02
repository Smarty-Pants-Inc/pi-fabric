import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { type DurableDirectory, renameAtomic, writeJsonAtomic } from "../core/atomic-write.js";

const INTERVAL_MS = 250;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
interface Completion { id: string; generation: number; at: number; error?: string }
interface Waiter { generation: number; resolve: () => void; reject: (error: unknown) => void }
const sameSnapshot = (left: fs.Stats, right: fs.Stats): boolean =>
  left.dev === right.dev && left.ino === right.ino && left.size === right.size && left.mtimeMs === right.mtimeMs;
const samePublishedSnapshot = (snapshot: fs.Stats, file: string): boolean => {
  const published = fs.lstatSync(file);
  return published.isFile() && sameSnapshot(snapshot, published);
};

/** A process-local queue, with cross-process election on a separate lock. Never
 * runs under the mesh mutation lock. Completion hints are published ONLY after
 * the file + directory barrier; failure is observable by an already queued peer.
 * The hard-linked checkpoint preserves the last synced inode when a later,
 * unsynced state.json replacement is lost or torn by power failure. */
export class MeshStateDurability {
  readonly checkpoint: string;
  readonly #completion: string;
  #target = 0;
  #source: string;
  #covered = 0;
  #running = false;
  #busy = false;
  #queuedAt = 0;
  #waiters: Waiter[] = [];

  constructor(
    readonly statePath: string, readonly directory: DurableDirectory,
    readonly stateCommitGeneration: (file: string) => number,
    readonly withBarrierLock: <T>(operation: () => Promise<T>) => Promise<T>,
    readonly restampSignal: (before: fs.Stats, after: fs.Stats) => void,
    readonly validatePinnedGeneration: (fd: number) => number,
  ) {
    this.#source = statePath;
    this.checkpoint = path.join(path.dirname(statePath), "state.durable.json");
    this.#completion = path.join(path.dirname(statePath), "state.durability-completion.json");
  }

  commit(generation: number, durable: boolean, source = this.statePath): Promise<void> | undefined {
    if (!this.#running || generation >= this.#target) this.#source = source;
    if (!this.#running) {
      this.#covered = 0; this.#target = generation; this.#busy = false; this.#queuedAt = Date.now();
    }
    else { this.#target = Math.max(this.#target, generation); this.#busy = true; }
    const receipt = durable ? new Promise<void>((resolve, reject) => {
      this.#waiters.push({ generation, resolve, reject });
    }) : undefined;
    if (!this.#running) {
      this.#running = true;
      // Microtasks coalesce same-turn writers; an idle queue flushes immediately.
      void this.#drain();
    }
    return receipt;
  }

  #readCompletion(): Completion | undefined {
    try {
      const value = JSON.parse(fs.readFileSync(this.#completion, "utf8")) as Completion;
      return typeof value.id === "string" && Number.isSafeInteger(value.generation) &&
        Number.isFinite(value.at) && (value.error === undefined || typeof value.error === "string") ? value : undefined;
    } catch { return undefined; }
  }

  async #drain(): Promise<void> {
    try {
      await Promise.resolve();
      await Promise.resolve();
      while (this.#covered < this.#target) {
        const observed = this.#readCompletion()?.id;
        const wanted = this.#target;
        const covered = await this.withBarrierLock(async () => {
          const previous = this.#readCompletion();
          if (previous?.error && previous.id !== observed && previous.generation >= wanted) {
            throw new Error(previous.error);
          }
          if (previous && !previous.error && previous.generation >= wanted &&
              this.stateCommitGeneration(this.checkpoint) >= previous.generation) {
            // Preparation can precede a mutation-lock wait. A cached file barrier
            // is not a receipt for a namespace detached/reattached during that wait.
            return this.#confirmCompletion(previous.generation);
          }
          // Only the elected barrier sleeps. Peers join its completed generation.
          if (previous && (this.#busy || this.#queuedAt < previous.at)) {
            await sleep(Math.min(INTERVAL_MS, Math.max(0, previous.at + INTERVAL_MS - Date.now())));
          }
          return this.#barrier();
        });
        this.#covered = Math.max(this.#covered, covered);
        const pending: Waiter[] = [];
        for (const waiter of this.#waiters) {
          if (waiter.generation <= this.#covered) waiter.resolve();
          else pending.push(waiter);
        }
        this.#waiters = pending;
      }
    } catch (error) {
      for (const waiter of this.#waiters) waiter.reject(error);
      this.#waiters = [];
      // No success is cached. A subsequent commit retries the owed barrier.
    } finally { this.#running = false; }
  }

  #confirmCompletion(generation: number): number {
    const fd = fs.openSync(this.checkpoint, "r");
    try {
      if (this.validatePinnedGeneration(fd) < generation) throw new Error("Invalid Fabric mesh durability completion");
      const validated = fs.fstatSync(fd);
      this.directory.sync(path.dirname(this.statePath));
      if (!sameSnapshot(validated, fs.fstatSync(fd)) || !samePublishedSnapshot(validated, this.checkpoint)) {
        throw new Error("Fabric mesh completion snapshot changed during confirmation");
      }
      return generation;
    } finally { fs.closeSync(fd); }
  }

  async #barrier(): Promise<number> {
    const temporary = `${this.checkpoint}.${process.pid}.${randomUUID()}.tmp`;
    let fd: number | undefined;
    let generation = this.#target;
    try {
      this.directory.prepare();
      const wanted = this.#target;
      // Pin the latest complete state inode. A concurrent mesh rename cannot
      // change the bytes we sync, or what the checkpoint will recover.
      let before: fs.Stats;
      for (let attempt = 0; ; attempt++) {
        try {
          // A no-op that recovered N must not independently promote a torn
          // canonical N+1 merely because its bounded header survived.
          const source = this.#source === this.checkpoint ||
            this.stateCommitGeneration(this.checkpoint) > this.stateCommitGeneration(this.statePath)
            ? this.checkpoint : this.statePath;
          before = fs.statSync(source);
          fs.linkSync(source, temporary);
          break;
        } catch (error) {
          // Linux link can lose its looked-up source inode to a concurrent
          // replacement. Re-resolve/pin the latest head, without acknowledging
          // anything or consuming a file/directory fsync on the failed lookup.
          if ((error as NodeJS.ErrnoException).code !== "ENOENT" || attempt >= 7) throw error;
        }
      }
      // FlushFileBuffers requires a writable handle on Windows; r+ never creates/truncates.
      fd = fs.openSync(temporary, process.platform === "win32" ? "r+" : "r");
      generation = this.validatePinnedGeneration(fd);
      if (generation < wanted) throw new Error("Fabric mesh pinned snapshot does not cover requested generation");
      const validated = fs.fstatSync(fd);
      await new Promise<void>((resolve, reject) => fs.fsync(fd!, (error) => error ? reject(error) : resolve()));
      // Validate once, then bind both the descriptor and publication name to
      // that immutable snapshot across the asynchronous barrier. No pathname
      // re-open can substitute an unvalidated successor.
      if (!sameSnapshot(validated, fs.fstatSync(fd)) || !samePublishedSnapshot(validated, temporary)) {
        throw new Error("Fabric mesh pinned snapshot changed during barrier");
      }
      renameAtomic(temporary, this.checkpoint);
      this.directory.sync(path.dirname(this.statePath));
      // link/rename change inode ctime, not payload identity. Repair only a hint
      // bound to the exact pre-link stamp; copied-marker replacements cannot qualify.
      this.restampSignal(before, fs.fstatSync(fd));
      writeJsonAtomic(this.#completion, { id: randomUUID(), generation, at: Date.now() });
      return generation;
    } catch (error) {
      // A failed directory barrier may have published a synced checkpoint but
      // is NOT a receipt. Only the post-barrier completion admits success.
      writeJsonAtomic(this.#completion, {
        id: randomUUID(), generation, at: Date.now(), error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    } finally {
      if (fd !== undefined) fs.closeSync(fd);
      fs.rmSync(temporary, { force: true });
    }
  }
}
