interface SemaphoreWaiter {
  resolve(release: () => void): void;
  reject(error: Error): void;
  signal: AbortSignal | undefined;
  abortHandler: (() => void) | undefined;
  priority: boolean;
}

export class Semaphore {
  #active = 0;
  #normalActive = 0;
  readonly #waiters: SemaphoreWaiter[] = [];

  /** All capacity may be reserved; normal requests then wait until cancelled. */
  constructor(readonly limit: number, readonly reserved = 0) {
    if (!Number.isInteger(limit) || limit < 1) throw new Error("Semaphore limit must be positive");
    if (!Number.isSafeInteger(reserved) || reserved < 0 || reserved > limit) {
      throw new Error("Semaphore reserved capacity must be a safe integer between zero and limit");
    }
  }

  #eligible(priority: boolean): boolean {
    return this.#active < this.limit && (priority || this.#normalActive < this.limit - this.reserved);
  }

  #reserve(priority: boolean): () => void {
    this.#active++;
    if (!priority) this.#normalActive++;
    return this.#releaseFunction(priority);
  }

  /** Reserve synchronously without overtaking an eligible FIFO waiter. */
  tryAcquire(signal?: AbortSignal, priority = false): (() => void) | undefined {
    if (signal?.aborted) throw new Error("Operation aborted");
    if (!this.#eligible(priority) || this.#waiters.some((waiter) => this.#eligible(waiter.priority))) return undefined;
    return this.#reserve(priority);
  }

  acquire(signal?: AbortSignal, priority = false): Promise<() => void> {
    if (signal?.aborted) return Promise.reject(new Error("Operation aborted"));
    const release = this.tryAcquire(signal, priority);
    if (release) return Promise.resolve(release);
    return new Promise((resolve, reject) => {
      const waiter: SemaphoreWaiter = {
        resolve,
        reject,
        signal,
        abortHandler: undefined,
        priority,
      };
      if (signal) {
        waiter.abortHandler = () => {
          const index = this.#waiters.indexOf(waiter);
          if (index >= 0) this.#waiters.splice(index, 1);
          reject(new Error("Operation aborted"));
        };
        signal.addEventListener("abort", waiter.abortHandler, { once: true });
      }
      this.#waiters.push(waiter);
    });
  }

  #drainWaiters(): void {
    while (this.#active < this.limit) {
      // A normal waiter at its quota must not block admission to the P0 reserve.
      const index = this.#waiters.findIndex((waiter) => this.#eligible(waiter.priority));
      if (index < 0) return;
      const waiter = this.#waiters.splice(index, 1)[0]!;
      if (waiter.signal && waiter.abortHandler) {
        waiter.signal.removeEventListener("abort", waiter.abortHandler);
      }
      // Reserve before resolving so concurrent callers cannot take this slot.
      waiter.resolve(this.#reserve(waiter.priority));
    }
  }

  #releaseFunction(priority: boolean): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.#active--;
      if (!priority) this.#normalActive--;
      this.#drainWaiters();
    };
  }
}
