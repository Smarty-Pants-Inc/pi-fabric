/** Shared carrier after bounded filesystem watcher recovery is exhausted. */
export class DegradedObservation {
  #timer: NodeJS.Timeout | undefined;
  #degraded = false;
  #closed = false;

  constructor(
    readonly component: string,
    readonly scan: () => void,
    readonly rearm: () => boolean,
  ) {}

  get degraded(): boolean { return this.#degraded; }
  get health(): string | undefined {
    return this.#degraded ? `observation-degraded:${this.component}` : undefined;
  }

  /** Brokers may start again after close; reopening itself arms no timer. */
  open(): void { this.#closed = false; }

  enter(): void {
    if (this.#closed || this.#degraded) return;
    this.#degraded = true;
    this.#warn(`${this.health}: watcher recovery exhausted; scanning and re-arming every 60 s (degraded only).`);
    this.#schedule();
  }

  // ponytail: R-no-polling exception, degraded-only. One minute is the fixed
  // fleet exception; normal operation never owns this scan/re-arm timer.
  #schedule(): void {
    if (this.#closed || !this.#degraded || this.#timer) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      if (this.#closed) return;
      let restored = false;
      try { restored = this.rearm(); } catch {}
      if (this.#closed) return;
      // Bind before the catch-up scan so a write racing recovery is not lost.
      // Existing coalesced drains keep authority fences and contain scan faults.
      try { this.scan(); } catch {}
      if (this.#closed) return;
      if (restored) {
        this.#degraded = false;
        this.#warn(`observation-restored:${this.component}: filesystem notifications restored; degraded scans stopped.`);
      } else this.#schedule();
    }, 60_000);
    this.#timer.unref?.();
  }

  #warn(message: string): void {
    try { console.warn(`[pi-fabric] ${message}`); } catch {}
  }

  close(): void {
    this.#closed = true;
    this.#degraded = false;
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }
}
