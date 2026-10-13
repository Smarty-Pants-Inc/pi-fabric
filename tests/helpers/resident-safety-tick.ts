// smarty-dev#11107: the resident host has no standing request poll, maintenance tick or safety
// interval. Clock-mocked tests capture the host's unref'd ONE-SHOT deadline timers (idle deadline,
// retention due datetime, stuck-preparing deadline, bounded retries) and fire the pending ones
// explicitly, as if their real deadline had arrived. Production has no test seam.
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

export interface SafetyTickCapture {
  /** Fire captured host deadlines (unref'd one-shots) whose datetime has arrived, then settle. */
  tick(settleMs?: number): Promise<void>;
  /** Periods of every setInterval created while installed (standing timers). */
  periods(): unknown[];
  /** Delays of every unref'd one-shot deadline captured while installed. */
  deadlines(): unknown[];
  restore(): void;
}

/** Install before host.start(); restore in afterEach (after vi.restoreAllMocks()). */
export function captureSafetyTick(): SafetyTickCapture {
  const realInterval = globalThis.setInterval;
  const realTimeout = globalThis.setTimeout;
  const realClear = globalThis.clearTimeout;
  const periods: unknown[] = [];
  const delays: unknown[] = [];
  // Each host deadline keeps its datetime on the (possibly mocked) Date.now() clock.
  const pending = new Map<NodeJS.Timeout, { at: number; run: () => void }>();
  const capturingInterval = ((handler: (...args: unknown[]) => void, ms?: number, ...args: unknown[]) => {
    periods.push(ms);
    return realInterval(handler, ms, ...args);
  }) as unknown as typeof setInterval;
  const capturingTimeout = ((handler: (...args: unknown[]) => void, ms?: number, ...args: unknown[]) => {
    let timer!: NodeJS.Timeout;
    const at = Date.now() + Math.max(0, Number(ms) || 0);
    const run = () => { pending.delete(timer); handler(...args); };
    timer = (realTimeout as unknown as (fn: () => void, ms?: number) => NodeJS.Timeout)(run, ms);
    if (typeof handler === "function" && typeof timer?.unref === "function") {
      const unref = timer.unref.bind(timer);
      // Host deadlines are unref'd; harness sleeps are not and are never fired early.
      timer.unref = () => { delays.push(ms); pending.set(timer, { at, run }); return unref(); };
    }
    return timer;
  }) as unknown as typeof setTimeout;
  // A cleared (cancelled or replaced) deadline must never fire.
  const capturingClear = ((timer?: NodeJS.Timeout | string | number) => {
    if (timer && typeof timer === "object") pending.delete(timer);
    return realClear(timer as NodeJS.Timeout);
  }) as unknown as typeof clearTimeout;
  globalThis.setInterval = capturingInterval;
  globalThis.setTimeout = capturingTimeout;
  globalThis.clearTimeout = capturingClear;
  return {
    async tick(settleMs = 30) {
      // Only deadlines whose datetime has arrived on the test clock fire; nothing fires early.
      const now = Date.now();
      const due = [...pending.entries()].filter(([, deadline]) => deadline.at <= now);
      for (const [timer] of due) realClear(timer);
      // A deadline cleared by an earlier one in this batch stays cancelled.
      for (const [timer, deadline] of due) if (pending.has(timer)) deadline.run();
      // Bounded settle: coalesced drain/maintenance run on setImmediate, the idle sample after it.
      for (let turn = 0; turn < 3; turn++) await new Promise<void>(resolve => setImmediate(resolve));
      await sleep(settleMs);
    },
    periods: () => [...periods],
    deadlines: () => [...delays],
    restore() {
      if (globalThis.setInterval === capturingInterval) globalThis.setInterval = realInterval;
      if (globalThis.setTimeout === capturingTimeout) globalThis.setTimeout = realTimeout;
      if (globalThis.clearTimeout === capturingClear) globalThis.clearTimeout = realClear;
      pending.clear();
    },
  };
}
