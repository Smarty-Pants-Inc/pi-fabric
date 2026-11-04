import { diagnostics } from "./diagnostics.js";
import { MeshBackgroundRetry, meshBackgroundGiveupMs, MESH_OUTAGE_ALARM_AFTER_MS, isMeshLockTimeout, MeshLockForeignHolderError, meshDeferredSummary, type MeshOutageAlarm } from "./atomic-write.js";

/** One-shot notifications have no natural next tick. An owned serial queue retains them;
 * typed deferred writes retry (no write occurred), bounded per job on the monotonic clock.
 * Idle costs no timer. Awaiting enqueue waits for the first attempt, never an indefinitely locked mesh.
 */
type MeshBackgroundJob = {
  operation: () => unknown | Promise<unknown>;
  attempted: () => void;
  id: number;
  startedAt: number | undefined;
  attempts: number;
  error: unknown;
};
export class MeshBackgroundQueue {
  readonly #retry: MeshBackgroundRetry;
  readonly #giveupMs = meshBackgroundGiveupMs();
  readonly #pending: MeshBackgroundJob[] = [];
  #nextJobId = 0;
  #timer: NodeJS.Timeout | undefined;
  #draining: Promise<void> | undefined;
  #closed = false;
  #failed = false;
  constructor(label: string, minMs = 100, maxMs = 5_000) { this.#retry = new MeshBackgroundRetry(label, minMs, maxMs); }

  alarmsTo(sink: (alarm: MeshOutageAlarm) => Promise<unknown> | void, afterMs = MESH_OUTAGE_ALARM_AFTER_MS): this {
    this.#retry.alarmsTo(sink, afterMs);
    return this;
  }

  enqueue(operation: () => unknown | Promise<unknown>): Promise<void> { return this.#enqueue(operation); }

  #enqueue(operation: () => unknown | Promise<unknown>, deferred?: { error: unknown; startedAt: number }): Promise<void> {
    if (this.#closed) return Promise.resolve();
    // Bounded best-effort notifications. Durable protocol cursors are not stored here.
    if (this.#pending.length >= 1_000) {
      this.#failed = true;
      diagnostics.warn("src/core/atomic-write.ts", "[pi-fabric] background mesh notification queue full; dropping newest notification");
      return Promise.resolve();
    }
    const admission = new Promise<void>(resolve => this.#pending.push({ operation, attempted: resolve,
      id: ++this.#nextJobId, startedAt: deferred?.startedAt, attempts: deferred ? 1 : 0, error: deferred?.error }));
    // A hook behind an already queued/outage write must not wait for mesh recovery.
    // The queue still owns its retry; close also settles every outstanding admission.
    if (this.#pending.length > 1 || this.#retry.waitMs > 0) this.#pending[this.#pending.length - 1]!.attempted();
    this.#schedule();
    return admission;
  }

  retry(operation: () => unknown | Promise<unknown>, error: unknown): Promise<void> {
    if (this.#closed) return Promise.resolve();
    const startedAt = performance.now();
    this.#retry.failure(error);
    return this.#enqueue(operation, { error, startedAt });
  }

  #remaining(item: MeshBackgroundJob): number {
    return item.startedAt === undefined ? this.#giveupMs : this.#giveupMs - (performance.now() - item.startedAt);
  }

  #giveUp(item: MeshBackgroundJob): void {
    this.#failed = true;
    this.#pending.shift(); // remove before diagnostics or an alarm sink can reenter admission
    item.attempted();
    try {
      diagnostics.error("src/core/atomic-write.ts", `FABRIC_MESH_BACKGROUND_GIVEUP ${JSON.stringify({
        label: this.#retry.label, job: item.operation.name || "notification", jobId: item.id,
        lock: isMeshLockTimeout(item.error)
          ? item.error instanceof MeshLockForeignHolderError ? item.error.lockPath : "Fabric mesh lock" : undefined,
        attempts: item.attempts, elapsedMs: Math.round(performance.now() - item.startedAt!), budgetMs: this.#giveupMs,
        error: meshDeferredSummary(item.error),
      })}`);
    } catch { /* A diagnostic cannot retain an expired job. */ }
    this.#retry.giveUp();
  }

  #schedule(): void {
    if (this.#closed || this.#draining || this.#timer || !this.#pending.length) return;
    const remainingMs = this.#remaining(this.#pending[0]!);
    if (remainingMs > 0 && this.#retry.waitMs > 0) {
      this.#timer = setTimeout(() => { this.#timer = undefined; this.#schedule(); }, Math.min(this.#retry.waitMs, remainingMs));
      this.#timer.unref();
      return;
    }
    // Assign before the microtask starts, so a burst has exactly one drain.
    this.#draining = Promise.resolve(); // fence reentrant admission before first operation
    const work = this.#drain();
    this.#draining = work;
    void work.finally(() => { this.#draining = undefined; this.#schedule(); }).catch(error => {
      this.#retry.failure(error); // an owned boundary, not a global bug-hiding handler
    });
  }

  async #drain(): Promise<void> {
    while (!this.#closed && this.#pending.length) {
      const item = this.#pending[0]!;
      if (this.#remaining(item) <= 0) { this.#giveUp(item); continue; }
      const result = await this.#retry.run(async () => {
        item.startedAt ??= performance.now(); // queued time behind another job does not spend this job's budget
        item.attempts++;
        try { return await item.operation(); }
        catch (error) { item.error = error; throw error; }
      });
      item.attempted();
      if (result === "retry" && !this.#closed && this.#remaining(item) <= 0) { this.#giveUp(item); continue; }
      if (result === "retry" || result === "skipped") return;
      if (result === "failed") this.#failed = true;
      this.#pending.shift(); // permanent failures are visible but do not poison later notices
    }
  }

  /** Admission only waits for a first attempt; release needs a confirmed empty queue. */
  async checkpointForRelease(): Promise<void> {
    await this.#draining;
    if (this.#pending.length || this.#failed) throw new Error("Background mesh release has unconfirmed publication obligations");
  }

  async close(): Promise<void> {
    this.#closed = true;
    this.#retry.close();
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    await this.#draining;
    for (const item of this.#pending.splice(0)) item.attempted();
  }
}
