import { vi } from "vitest";

/** Freeze only the budget clock until the guest reaches the boundary under test.
 * IPC, worker startup and timers remain real. Absolute-deadline watchdogs rearm
 * while Date is frozen; after admission they see elapsed time from that point,
 * never the interpreter's startup time. The independent real guard bounds hangs.
 */
export async function executeAfterAdmission<T>(
  execute: (signal: AbortSignal, startClock: () => void) => Promise<T>,
  admitted: () => boolean,
  onAdmitted?: () => void | Promise<void>,
): Promise<T> {
  const now = Date.now.bind(Date);
  const startedAt = now();
  const clock = vi.spyOn(Date, "now").mockReturnValue(startedAt);
  const safety = new AbortController();
  const guard = setTimeout(() => safety.abort(new Error("Guest did not settle within the 12-second admission hang guard")), 12_000);
  let execution: Promise<T> | undefined;
  let settled = false;
  let clockStarted = false;
  // Synchronous boundary probes may starve the observer below. They must start
  // elapsed time inline, before entering their deliberately blocking work.
  const startClock = () => {
    if (clockStarted) return;
    clockStarted = true;
    const admittedAt = now();
    clock.mockImplementation(() => startedAt + now() - admittedAt);
  };
  try {
    execution = execute(safety.signal, startClock);
    const observed = execution.then(
      value => { settled = true; return value; },
      error => { settled = true; throw error; },
    );
    // A readiness assertion may throw before we reach the final await. Cleanup
    // still drains execution, and this observer must not leak a rejection then.
    void observed.catch(() => undefined);
    while (!admitted()) {
      if (settled) {
        const result = await observed;
        throw new Error(`Execution settled before guest admission: ${JSON.stringify(result)}`);
      }
      if (safety.signal.aborted) throw safety.signal.reason;
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([observed, new Promise<void>(resolve => { timer = setTimeout(resolve, 10); })]);
      } finally { clearTimeout(timer); }
    }
    startClock();
    await onAdmitted?.();
    return await observed;
  } finally {
    // A failed readiness assertion must not leave a native guest/host call alive.
    if (!settled) safety.abort(new Error("Admission test cleanup"));
    await execution?.catch(() => undefined);
    clearTimeout(guard);
    clock.mockRestore();
  }
}
