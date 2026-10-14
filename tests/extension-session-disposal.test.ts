import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { sendFabricMessage } from "../src/fabric-provenance.js";
import { registerMainProviderRecovery } from "../src/main-provider-recovery.js";

vi.mock("../src/fabric-provenance.js", () => ({
  resolveFabricIdentity: () => ({ identity: { kind: "main", id: "main" }, mainAgentId: "main" }),
  sendFabricMessage: vi.fn(),
}));
vi.mock("@earendil-works/pi-ai/compat", () => ({ isContextOverflow: () => false }));

// smarty-dev#8881 / smarty-dev#10718: AgentSession.dispose() (and session replacement) invalidate the
// extension ctx and pi without emitting session_shutdown to the timers they captured. Every getter then
// throws Pi's stale-ctx error. The provider-error wake read ctx.signal from a setTimeout callback, so the
// first tick after dispose threw out of the timer: an uncaught exception that exits Pi (seen on Windows).
const STALE = "This extension ctx is stale after session replacement or reload.";

type Handler = (event: unknown, context: ExtensionContext) => unknown;
const harness = () => {
  const handlers = new Map<string, Handler>();
  let idle = false;
  let stale = false;
  const owner = new AbortController();
  const target = {
    get signal() { return owner.signal; },
    get hasUI() { return true; },
    isIdle: () => idle,
    isSettling: () => false,
    sessionManager: {
      getSessionId: () => "main",
      getEntries: () => [{ type: "message", message: { role: "assistant", stopReason: "error", errorMessage: "disconnect" } }],
    },
    model: { contextWindow: 0 },
    ui: { notify: vi.fn() },
  };
  // Mirrors ExtensionRunner.invalidate(): every read of the disposed ctx throws.
  const context = new Proxy(target, {
    get(object, key, receiver) {
      if (stale) throw new Error(STALE);
      return Reflect.get(object, key, receiver);
    },
  }) as unknown as ExtensionContext;
  const report = vi.fn(async () => undefined);
  registerMainProviderRecovery({ on: (name: string, handler: Handler) => handlers.set(name, handler) } as unknown as ExtensionAPI,
    { halted: () => false, report, ownsNonRetryable: () => false });
  return {
    settle: async () => handlers.get("agent_settled")!({ outcome: "error" }, context),
    idle: () => { idle = true; },
    dispose: () => { stale = true; },
    report,
  };
};

beforeEach(() => { vi.useFakeTimers(); vi.clearAllMocks(); });
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });

describe("session disposal stops ctx-bound delayed work (smarty-dev#8881, #10718)", () => {
  it("a session disposed inside the provider-error backoff never reads the stale ctx from its timer", async () => {
    const h = harness();
    await h.settle();
    expect(vi.getTimerCount()).toBe(1);
    h.dispose();                                       // no session_shutdown: Pi's dispose emits none
    await expect(vi.advanceTimersByTimeAsync(5_000)).resolves.toBe(vi);  // a timer that throws rejects here
    expect(sendFabricMessage).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);                // the wake retired; it does not poll a dead session
  });

  it("a session disposed while the wake polls for idle stops the poll without throwing", async () => {
    const h = harness();
    await h.settle();
    await vi.advanceTimersByTimeAsync(1_250);          // busy: re-armed 100 ms polls
    expect(vi.getTimerCount()).toBe(1);
    h.dispose();
    await expect(vi.advanceTimersByTimeAsync(1_000)).resolves.toBe(vi);
    expect(sendFabricMessage).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a session disposed during the lazy classifier import arms no wake", async () => {
    const h = harness();
    const settling = h.settle();
    h.dispose();
    await expect(settling).resolves.toBeUndefined();
    await expect(vi.advanceTimersByTimeAsync(5_000)).resolves.toBe(vi);
    expect(sendFabricMessage).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a live session still gets its one automatic retry", async () => {
    const h = harness();
    h.idle();
    await h.settle();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sendFabricMessage).toHaveBeenCalledExactlyOnceWith(expect.anything(),
      expect.objectContaining({ customType: "pi-fabric-provider-retry" }), { deliverAs: "followUp", triggerTurn: true });
  });
});
