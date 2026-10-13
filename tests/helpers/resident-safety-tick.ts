// smarty-dev#11107: the resident host's 50 ms request poll and 100 ms maintenance tick are gone.
// Clock-mocked tests capture the host's slow (5 s) safety interval callback and fire it
// explicitly instead of waiting for a removed cadence. Production has no test seam.
const SAFETY_TICK_MS = 5_000;
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

export interface SafetyTickCapture {
  /** Fire the most recently created host safety interval, then let its drain/idle/maintenance settle. */
  tick(settleMs?: number): Promise<void>;
  /** Periods of every setInterval created while installed. */
  periods(): unknown[];
  restore(): void;
}

/** Install before host.start(); restore in afterEach (after vi.restoreAllMocks()). */
export function captureSafetyTick(): SafetyTickCapture {
  const real = globalThis.setInterval;
  const callbacks: Array<() => void> = [];
  const periods: unknown[] = [];
  const capturing = ((handler: (...args: unknown[]) => void, ms?: number, ...args: unknown[]) => {
    periods.push(ms);
    if (ms === SAFETY_TICK_MS && typeof handler === "function") callbacks.push(() => handler(...args));
    return real(handler, ms, ...args);
  }) as typeof setInterval;
  globalThis.setInterval = capturing;
  return {
    async tick(settleMs = 30) {
      const callback = callbacks.at(-1);
      if (!callback) throw new Error("no resident safety interval was captured; install before host.start()");
      callback();
      // Bounded settle: the coalesced drain/maintenance run on setImmediate, the idle sample after it.
      for (let turn = 0; turn < 3; turn++) await new Promise<void>(resolve => setImmediate(resolve));
      await sleep(settleMs);
    },
    periods: () => [...periods],
    restore() { if (globalThis.setInterval === capturing) globalThis.setInterval = real; },
  };
}
