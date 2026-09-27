export const PI_RECOVERY_TIMEOUT_MS = 60_000;

/** Bounds stalled Pi error recovery, not healthy inference or tool execution. */
export class PiRecoveryWatchdog {
  #timer: ReturnType<typeof setTimeout> | undefined;
  #reason: string | undefined;
  #disposed = false;

  constructor(private readonly fail: (error: string) => void) {}

  arm(reason: string): void {
    if (this.#disposed) return;
    this.#reason = reason;
    // Repeated errors, retry announcements and lifecycle chatter are not progress.
    if (this.#timer) return;
    this.#timer = setTimeout(() => {
      const error = this.#reason;
      this.dispose();
      this.fail(`${error}; Pi made no recovery progress for ${PI_RECOVERY_TIMEOUT_MS}ms; terminating child`);
    }, PI_RECOVERY_TIMEOUT_MS);
    this.#timer.unref?.();
  }

  progress(): void {
    if (!this.#reason || this.#disposed) return;
    const reason = this.#reason;
    this.clear();
    this.arm(reason);
  }

  clear(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#reason = undefined;
  }

  dispose(): void {
    this.clear();
    this.#disposed = true;
  }
}
