import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { sendFabricMessage } from "../src/fabric-provenance.js";
import { registerMainProviderRecovery } from "../src/main-provider-recovery.js";

vi.mock("../src/fabric-provenance.js", () => ({
  resolveFabricIdentity: () => ({ identity: { kind: "main", id: "main" }, mainAgentId: "main" }),
  sendFabricMessage: vi.fn(),
}));
vi.mock("@earendil-works/pi-ai/compat", () => ({ isContextOverflow: () => false }));

type Handler = (event: unknown, context: ExtensionContext) => unknown;
const harness = () => {
  const handlers = new Map<string, Handler>();
  let idle = false;
  let settling = false;
  let halted = false;
  let owner = new AbortController();
  const context = {
    get signal() { return owner.signal; },
    isIdle: () => idle,
    isSettling: () => settling,
    sessionManager: {
      getSessionId: () => "main",
      getEntries: () => [{ type: "message", message: {
        role: "assistant", stopReason: "error", errorMessage: "disconnect\nprivate diagnostic",
      } }],
    },
    ui: { notify: vi.fn() },
  } as unknown as ExtensionContext;
  const report = vi.fn(async () => undefined);
  registerMainProviderRecovery({ on: (name: string, handler: Handler) => handlers.set(name, handler) } as unknown as ExtensionAPI,
    { halted: () => halted, report });
  return {
    emit: async (name: string) => handlers.get(name)!({ outcome: "error" }, context),
    report, context,
    idle: () => { idle = true; },
    settling: (value: boolean) => { settling = value; },
    abort: () => { owner.abort(); },
    halt: () => { halted = true; },
    resume: () => { owner = new AbortController(); halted = false; },
  };
};

beforeEach(() => { vi.useFakeTimers(); vi.clearAllMocks(); });
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });

describe("Main provider recovery waiting for settlement compaction", () => {
  it("drops a late report failure after session shutdown without touching the stale ctx (#5962)", async () => {
    const h = harness(); h.idle();
    await h.emit("agent_settled"); await vi.advanceTimersByTimeAsync(1_000);
    let entered!: () => void; let reject!: (error: Error) => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    h.report.mockImplementation(() => { entered(); return new Promise((_yes, no) => { reject = no; }); });
    const reporting = h.emit("agent_settled"); await started;
    await h.emit("session_shutdown");
    Object.defineProperty(h.context, "ui", { get() { throw new Error("stale ctx"); } });
    reject(new Error("retired report"));
    await expect(reporting).resolves.toBeUndefined();
  });

  it("keeps the wake pending while busy, then retries once and reports once without a third attempt", async () => {
    const h = harness();
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(2_500);
    expect(sendFabricMessage).not.toHaveBeenCalled();
    expect(h.report).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(1);
    h.idle();
    await vi.advanceTimersByTimeAsync(100);
    expect(sendFabricMessage).toHaveBeenCalledExactlyOnceWith(expect.anything(),
      expect.objectContaining({ customType: "pi-fabric-provider-retry" }), { deliverAs: "followUp", triggerTurn: true });
    await h.emit("agent_start");
    await h.emit("agent_settled");
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(5_000);
    expect(sendFabricMessage).toHaveBeenCalledTimes(2); // retry + non-triggering BLOCKED
    expect(sendFabricMessage).toHaveBeenLastCalledWith(expect.anything(),
      expect.objectContaining({ customType: "pi-fabric-provider-blocked", content: "BLOCKED: provider error: disconnect" }),
      { deliverAs: "followUp", triggerTurn: false });
    expect(h.report).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("waits for the remaining settlement hooks even after Pi is idle", async () => {
    const h = harness();
    h.idle();
    h.settling(true);
    await h.emit("agent_settled");
    await vi.advanceTimersByTimeAsync(2_500);
    expect(sendFabricMessage).not.toHaveBeenCalled();
    h.settling(false);
    await vi.advanceTimersByTimeAsync(100);
    expect(sendFabricMessage).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["input", "agent_start", "session_before_switch", "session_tree", "session_shutdown", "owner-abort", "owner-halt"])(
    "%s cancels a busy wake without consuming the recovery budget", async fence => {
      const h = harness();
      await h.emit("agent_settled");
      await vi.advanceTimersByTimeAsync(1_200); // cancellation after the original timer sampled busy
      if (fence === "owner-abort") h.abort();
      else if (fence === "owner-halt") h.halt();
      else await h.emit(fence);
      h.idle();
      await vi.advanceTimersByTimeAsync(5_000);
      expect(sendFabricMessage).not.toHaveBeenCalled();
      expect(h.report).not.toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
      h.resume();
      await h.emit("agent_settled");
      await vi.advanceTimersByTimeAsync(1_000);
      expect(sendFabricMessage).toHaveBeenCalledExactlyOnceWith(expect.anything(),
        expect.objectContaining({ customType: "pi-fabric-provider-retry" }), { deliverAs: "followUp", triggerTurn: true });
      expect(h.report).not.toHaveBeenCalled();
    });
});
