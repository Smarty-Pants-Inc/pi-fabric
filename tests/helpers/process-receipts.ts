import { expect, vi } from "vitest";
import { ProcessTransport } from "../../src/agents/transports/process-transport.js";
import type { AgentTransportHandle } from "../../src/agents/types.js";

/** Deliver the transport's ONE final census without advancing worker/lease clocks.
 * The named callback is an internal test seam, not a runtime deadline override.
 * Every intercepted deadline still has the production 60-second lower bound.
 */
export function captureTreeCensusDeadline() {
  const setTimeout = globalThis.setTimeout;
  const pending = new Map<ReturnType<typeof setTimeout>, () => void>();
  const hook = vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: unknown[]) => void, ms?: number, ...args: unknown[]) => {
    if (callback.name !== "finalTreeCensus") return setTimeout(callback, ms, ...args);
    expect(ms).toBeGreaterThanOrEqual(60_000);
    const timer = setTimeout(() => { pending.delete(timer); callback(...args); }, ms);
    pending.set(timer, () => callback(...args));
    return timer;
  }) as typeof setTimeout);
  return {
    pending: () => pending.size,
    fire: () => {
      const deadlines = [...pending];
      pending.clear();
      for (const [timer, callback] of deadlines) { clearTimeout(timer); callback(); }
      return deadlines.length;
    },
    restore: () => hook.mockRestore(),
  };
}

/** Await the captured native receipt, never terminal status or a PID sleep. */
export function captureProcessCloseReceipts() {
  const launch = ProcessTransport.prototype.launch;
  const handles = new Map<string, AgentTransportHandle>();
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function(this: ProcessTransport, request) {
    const handle = await launch.call(this, request);
    handles.set(request.id, handle);
    return handle;
  });
  return {
    wait: async (id: string) => {
      const handle = handles.get(id);
      expect(handle?.closed, "fixture requires its captured native close receipt").toBeDefined();
      await handle!.closed;
      expect(await handle!.isAlive()).toBe(false);
    },
  };
}
