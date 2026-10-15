import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ResolvedFabricAction } from "../src/core/action-registry.js";
import type { FabricAutoApprovalDecision } from "../src/core/auto-approval-classifier.js";

// The classifier facade must stay out of startup: its implementation (policies,
// evidence projection, provider and Jev calls) loads only at the first
// classify(). A failed lazy load is a trust-boundary event: it must escalate to
// an explicit decision, never auto-allow.

const IMPL = "../src/core/auto-approval-classifier-impl.js";

const action: ResolvedFabricAction = {
  ref: "demo.write",
  provider: "demo",
  name: "write",
  description: "Write data",
  inputSchema: {},
  risk: "write",
};

const autoPolicies = {
  read: "allow" as const,
  write: "auto" as const,
  execute: "deny" as const,
  network: "ask" as const,
  agent: "ask" as const,
};

const usage = {
  input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

const deferred = <T = void>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
};

/** Mocks the impl module; `gate` holds the lazy import pending until released. */
const mockImpl = (gate?: Promise<void>) => {
  const classifyAutoApproval = vi.fn(async (
    _classifier: unknown,
    act: ResolvedFabricAction,
    args: Record<string, unknown>,
    context: ExtensionContext,
    modelKey?: string,
  ): Promise<FabricAutoApprovalDecision> => {
    (context as unknown as { provider?: () => void }).provider?.();
    return { decision: "allow", reason: `${act.ref}:${JSON.stringify(args)}:${modelKey ?? "-"}`, model: String((context as unknown as { tag?: string }).tag), usage };
  });
  const entered = deferred();
  const factory = vi.fn(async () => {
    entered.resolve();
    await gate;
    return { classifyAutoApproval };
  });
  vi.doMock(IMPL, factory);
  return { factory, classifyAutoApproval, entered: entered.promise };
};

const ctx = (extra: Record<string, unknown> = {}): ExtensionContext =>
  ({ hasUI: false, ...extra } as unknown as ExtensionContext);

beforeEach(() => { vi.resetModules(); });
afterEach(() => { vi.doUnmock(IMPL); vi.resetModules(); });

describe("auto-approval classifier lazy startup", () => {
  it("import, construction and idle never evaluate the impl; first classify loads it once", async () => {
    const { factory, classifyAutoApproval } = mockImpl();
    const { FabricAutoApprovalClassifier } = await import("../src/core/auto-approval-classifier.js");
    const { ApprovalController } = await import("../src/core/approval-controller.js");
    const classifier = new FabricAutoApprovalClassifier();
    new ApprovalController(autoPolicies, ctx(), undefined, classifier);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(factory).not.toHaveBeenCalled();

    await expect(classifier.classify(action, { a: 1 }, ctx({ tag: "m" }), "k"))
      .resolves.toMatchObject({ decision: "allow", reason: 'demo.write:{"a":1}:k' });
    await classifier.classify(action, {}, ctx({ tag: "m" }));
    expect(factory).toHaveBeenCalledOnce();
    expect(classifyAutoApproval).toHaveBeenCalledTimes(2);
    expect(classifyAutoApproval.mock.calls[0]?.[0]).toBe(classifier);
  });

  it("a failed lazy import escalates through the real controller and never auto-allows", async () => {
    const factory = vi.fn(async () => { throw new Error("chunk missing"); });
    vi.doMock(IMPL, factory);
    const { ApprovalController } = await import("../src/core/approval-controller.js");

    // No UI: explicit denial, audited as an escalation.
    const audits: unknown[] = [];
    const headlessCtl = new ApprovalController(autoPolicies, ctx(), undefined, undefined, audit => audits.push(audit));
    await expect(headlessCtl.approve(action, { path: "x" })).rejects.toThrow("requires approval, but no interactive UI is available");
    expect(factory).toHaveBeenCalled();
    expect(audits).toEqual([expect.objectContaining({
      action: "demo.write",
      decision: "escalate",
      reason: "Classifier unavailable; explicit approval required",
      // vitest wraps a throwing mock factory; any import failure must escalate.
      error: expect.any(String),
    })]);

    // A human deny is honoured; a human allow-once is the only way through.
    const denyCustom = vi.fn(async () => "deny");
    const denyCtl = new ApprovalController(autoPolicies, { hasUI: true, mode: "tui", ui: { custom: denyCustom, notify: vi.fn() } } as unknown as ExtensionContext);
    await expect(denyCtl.approve(action)).rejects.toThrow("User denied write access for demo.write");

    const custom = vi.fn(async () => "allow-once");
    const notify = vi.fn();
    const uiAudits: unknown[] = [];
    const uiCtl = new ApprovalController(autoPolicies, { hasUI: true, mode: "tui", ui: { custom, notify } } as unknown as ExtensionContext, undefined, undefined, audit => uiAudits.push(audit));
    await expect(uiCtl.approve(action)).resolves.toBeUndefined();
    expect(custom).toHaveBeenCalledOnce();
    expect(notify).toHaveBeenCalledWith(expect.stringContaining("Auto mode could not determine safety"), "warning");
    expect(notify).toHaveBeenCalledWith("Allowed once: demo.write", "info");
    expect(uiAudits).toEqual([expect.objectContaining({ decision: "escalate" })]);
  });

  it("concurrent first calls through the real lazy impl keep their own args, context and result", async () => {
    // Unmocked: vitest serves the original module to a re-entrant import while a
    // mock factory is pending, so concurrency is proven on the real cold import.
    // Each context fails auth distinctly, so no provider is ever called.
    const { FabricAutoApprovalClassifier } = await import("../src/core/auto-approval-classifier.js");
    const classifier = new FabricAutoApprovalClassifier();
    const registry = (tag: string) => ({
      find: vi.fn((provider: string, id: string) => ({ provider, id })),
      getApiKeyAndHeaders: vi.fn(async (model: { provider: string; id: string }) =>
        ({ ok: false, error: `auth-${tag}:${model.provider}/${model.id}` })),
    });
    const a = registry("A");
    const b = registry("B");
    const [first, second] = await Promise.allSettled([
      classifier.classify(action, { n: 1 }, ctx({ modelRegistry: a }), "pa/ma"),
      classifier.classify({ ...action, ref: "demo.other" }, { n: 2 }, ctx({ modelRegistry: b }), "pb/mb"),
    ]);
    expect(first).toMatchObject({ status: "rejected", reason: { message: "auth-A:pa/ma" } });
    expect(second).toMatchObject({ status: "rejected", reason: { message: "auth-B:pb/mb" } });
    expect(a.find).toHaveBeenCalledExactlyOnceWith("pa", "ma");
    expect(b.find).toHaveBeenCalledExactlyOnceWith("pb", "mb");
  });

  it("pre-abort never imports; abort during the pending import never reaches impl or provider", async () => {
    const gate = deferred();
    const { factory, classifyAutoApproval, entered } = mockImpl(gate.promise);
    const { FabricAutoApprovalClassifier } = await import("../src/core/auto-approval-classifier.js");
    const classifier = new FabricAutoApprovalClassifier();
    const provider = vi.fn();

    const preAborted = new AbortController();
    preAborted.abort(new Error("cancelled early"));
    await expect(classifier.classify(action, {}, ctx({ signal: preAborted.signal, provider })))
      .rejects.toThrow("cancelled early");
    expect(factory).not.toHaveBeenCalled();

    const controller = new AbortController();
    const pending = classifier.classify(action, {}, ctx({ signal: controller.signal, provider }));
    await entered;
    expect(factory).toHaveBeenCalledOnce();
    controller.abort(new Error("cancelled mid-load"));
    gate.resolve();
    await expect(pending).rejects.toThrow("cancelled mid-load");
    expect(classifyAutoApproval).not.toHaveBeenCalled();
    expect(provider).not.toHaveBeenCalled();
  });
});
