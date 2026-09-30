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
