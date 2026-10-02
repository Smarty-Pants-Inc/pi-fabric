import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { type DurableDirectory, renameAtomic, writeJsonAtomic } from "../core/atomic-write.js";

const INTERVAL_MS = 250;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
interface Completion { id: string; generation: number; at: number; error?: string }
interface Waiter { generation: number; resolve: () => void; reject: (error: unknown) => void }

/** A process-local queue, with cross-process election on a separate lock. Never
 * runs under the mesh mutation lock. Completion hints are published ONLY after
 * the file + directory barrier; failure is observable by an already queued peer.
 * The hard-linked checkpoint preserves the last synced inode when a later,
 * unsynced state.json replacement is lost or torn by power failure. */
export class MeshStateDurability {
  readonly checkpoint: string;
  readonly #completion: string;
  #target = 0;
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
  ) {
    this.checkpoint = path.join(path.dirname(statePath), "state.durable.json");
    this.#completion = path.join(path.dirname(statePath), "state.durability-completion.json");
  }

  commit(generation: number, durable: boolean): Promise<void> | undefined {
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
            this.directory.sync(path.dirname(this.statePath));
            return previous.generation;
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

  async #barrier(): Promise<number> {
    const temporary = `${this.checkpoint}.${process.pid}.${randomUUID()}.tmp`;
    let fd: number | undefined;
    let generation = this.#target;
    try {
      this.directory.prepare();
      // Pin the latest complete state inode. A concurrent mesh rename cannot
      // change the bytes we sync, or what the checkpoint will recover.
      let before: fs.Stats;
      for (let attempt = 0; ; attempt++) {
        try {
          const source = this.stateCommitGeneration(this.checkpoint) > this.stateCommitGeneration(this.statePath)
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
      generation = this.stateCommitGeneration(temporary);
      if (generation === 0) throw new Error("Missing Fabric mesh commit generation");
      // FlushFileBuffers requires a writable handle on Windows; r+ never creates/truncates.
      fd = fs.openSync(temporary, process.platform === "win32" ? "r+" : "r");
      await new Promise<void>((resolve, reject) => fs.fsync(fd!, (error) => error ? reject(error) : resolve()));
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
