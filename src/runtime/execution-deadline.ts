import type { FabricSandboxOptions, FabricSandboxResult } from "./kernel.js";

/** One absolute clock and explicit cause for every runtime boundary. */
export class ExecutionDeadline {
  readonly startedAt: number;
  #at: number;
  #timer: NodeJS.Timeout | undefined;
  #clampedToMaximum = false;
  #expiredReason: Error | undefined;

  constructor(private readonly options: Pick<FabricSandboxOptions, "timeoutMs" | "maximumDeadlineAt" | "maximumDeadlineReason">, startedAt = Date.now()) {
    this.startedAt = startedAt;
    this.#at = this.#clamp(startedAt + options.timeoutMs);
  }

  #clamp(at: number): number {
    const maximum = this.options.maximumDeadlineAt ?? Infinity;
    this.#clampedToMaximum = at >= maximum;
    return Math.min(at, maximum);
  }

  get at(): number { return this.#at; }
  get timeoutMs(): number { return this.#at - this.startedAt; }
  get reached(): boolean { return this.#expiredReason !== undefined || Date.now() >= this.#at; }
  get reason(): Error {
    if (this.#expiredReason) return this.#expiredReason;
    const reason = (this.#clampedToMaximum && this.reached ? this.options.maximumDeadlineReason : undefined) ??
      new Error(`Execution timed out after ${this.timeoutMs}ms`);
    if (this.reached) this.#expiredReason = reason;
    return reason;
  }

  timeoutResult(logs: string[]): FabricSandboxResult {
    const reason = this.reason;
    return { value: undefined, logs, terminationReason: "timed_out", error: reason.message,
      ...(reason === this.options.maximumDeadlineReason ? { deadlineReason: reason } : {}),
    };
  }

  extend(requested: number | undefined): boolean {
    // An expired program cannot revive itself with another floor.
    if (this.reached || typeof requested !== "number" || !Number.isFinite(requested)) return false;
    const next = Math.min(Date.now() + Math.max(1, Math.floor(requested)), this.options.maximumDeadlineAt ?? Infinity);
    if (next <= this.#at) return false;
    this.#at = this.#clamp(next);
    return true;
  }

  /** Human-wait pause clock (upstream deadline-pause.ts): budget left before the running deadline. */
  remainingMs(): number { return this.#at - Date.now(); }

  /**
   * A host call waiting for a person suspends the program deadline. The host-owned
   * absolute ceiling is never suspended: the deadline rises to it, not past it. An
   * expired program stays expired.
   */
  suspend(): void {
    if (this.reached) return;
    this.#at = this.#clamp(Infinity);
  }

  /** Resume a suspended deadline with this budget from now, still clamped to the ceiling. */
  resume(remainingMs: number): void {
    if (this.#expiredReason) return;
    this.#at = this.#clamp(Date.now() + Math.max(0, remainingMs));
  }

  scheduleDeadline(expire: () => void, unref = false): void {
    this.clear();
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      // Host timers may fire early (or the wall clock may move backwards).
      // No host work is aborted until this exact absolute deadline is reached.
      if (!this.reached) { this.scheduleDeadline(expire, unref); return; }
      expire();
    }, Math.min(2_147_483_647, Math.max(0, this.#at - Date.now())));
    if (unref) this.#timer.unref?.();
  }

  clear(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }
}
