import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import {
  ApprovalController,
  FabricSessionApprovals,
} from "../src/core/approval-controller.js";
import type { FabricAutoApprovalClassifier } from "../src/core/auto-approval-classifier.js";
import type { ResolvedFabricAction } from "../src/core/action-registry.js";

const action: ResolvedFabricAction = {
  ref: "demo.write",
  provider: "demo",
  name: "write",
  description: "Write data",
  inputSchema: {},
  risk: "write",
};

const policies = {
  read: "allow" as const,
  write: "ask" as const,
  execute: "deny" as const,
  network: "ask" as const,
  agent: "ask" as const,
};

const tuiContext = (
  custom: (...args: unknown[]) => Promise<unknown>,
  notify = vi.fn(),
): ExtensionContext => ({
  hasUI: true,
  mode: "tui",
  ui: { custom, notify },
} as unknown as ExtensionContext);

describe("ApprovalController", () => {
  it("fails closed when approval is required without a UI", async () => {
    const controller = new ApprovalController(policies, { hasUI: false } as ExtensionContext);
    await expect(controller.approve(action)).rejects.toThrow("no interactive UI");
  });

  it("allows only the selected call when Allow once is chosen", async () => {
    const custom = vi.fn(async () => "allow-once");
    const notify = vi.fn();
    const controller = new ApprovalController(policies, tuiContext(custom, notify));

    await controller.approve(action);
    await controller.approve({ ...action, ref: "demo.writeAgain" });

    expect(custom).toHaveBeenCalledTimes(2);
    expect(notify).toHaveBeenCalledWith("Allowed once: demo.write", "info");
    expect(notify).toHaveBeenCalledWith("Allowed once: demo.writeAgain", "info");
  });

  it("shares an Always allow grant across the Pi session", async () => {
    const custom = vi.fn(async () => "allow-session");
    const notify = vi.fn();
    const session = new FabricSessionApprovals();
    const firstExecution = new ApprovalController(
      policies,
      tuiContext(custom, notify),
      session,
    );
    const laterExecution = new ApprovalController(
      policies,
      tuiContext(custom, notify),
      session,
    );

    await firstExecution.approve(action);
    await laterExecution.approve({ ...action, ref: "demo.writeLater" });

    expect(custom).toHaveBeenCalledOnce();
    expect(session.approvedRisks).toContain("write");
    expect(notify).toHaveBeenLastCalledWith(
      "Allowed write access for this Pi session",
      "info",
    );
  });

  it("uses an RPC-compatible three-choice dialog", async () => {
    const select = vi.fn(async () => "Allow write access for this session");
    const notify = vi.fn();
    const controller = new ApprovalController(policies, {
      hasUI: true,
      mode: "rpc",
      ui: { select, notify },
    } as unknown as ExtensionContext);

    await controller.approve(action);

    expect(select).toHaveBeenCalledWith(
      "Pi Fabric permission · demo.write requests write access. Write data",
      ["Allow once", "Allow write access for this session", "Deny"],
    );
  });

  it("auto-allows only the exact classified action", async () => {
    const classify = vi.fn(async () => ({
      decision: "allow" as const,
      reason: "Routine task-aligned local write",
      model: "anthropic/classifier",
      usage: {
        input: 10,
        output: 2,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 12,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
    }));
    const classifier = { classify } as unknown as FabricAutoApprovalClassifier;
    const controller = new ApprovalController(
      { ...policies, write: "auto", model: "anthropic/classifier" },
      { hasUI: false } as ExtensionContext,
      new FabricSessionApprovals(),
      classifier,
    );
    const args = { path: "src/index.ts", content: "safe" };

    await controller.approve(action, args);

    expect(classify).toHaveBeenCalledWith(
      action,
      args,
      expect.anything(),
      "anthropic/classifier",
    );
  });

  it("escalates an unsafe auto decision to the approval wizard", async () => {
    const classifier = {
      classify: vi.fn(async () => ({
        decision: "escalate" as const,
        reason: "Command modifies shared production state",
        model: "anthropic/classifier",
        usage: {
          input: 10,
          output: 2,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 12,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
      })),
    } as unknown as FabricAutoApprovalClassifier;
    const custom = vi.fn(async () => "allow-once");
    const notify = vi.fn();
    const controller = new ApprovalController(
      { ...policies, write: "auto" },
      tuiContext(custom, notify),
      new FabricSessionApprovals(),
      classifier,
    );

    await controller.approve(action, { path: "production" });

    expect(custom).toHaveBeenCalledOnce();
    expect(notify).toHaveBeenCalledWith(
      expect.stringContaining("Auto mode escalated"),
      "warning",
    );
  });

  it("fails closed to explicit approval when the classifier is unavailable", async () => {
    const classifier = {
      classify: vi.fn(async () => { throw new Error("model unavailable"); }),
    } as unknown as FabricAutoApprovalClassifier;
    const custom = vi.fn(async () => "deny");
    const controller = new ApprovalController(
      { ...policies, write: "auto" },
      tuiContext(custom),
      new FabricSessionApprovals(),
      classifier,
    );

    await expect(controller.approve(action)).rejects.toThrow("User denied");
    expect(custom).toHaveBeenCalledOnce();
  });

  it("fails closed and notifies when the user denies or dismisses", async () => {
    const custom = vi.fn(async () => "deny");
    const notify = vi.fn();
    const controller = new ApprovalController(policies, tuiContext(custom, notify));

    await expect(controller.approve(action)).rejects.toThrow(
      "User denied write access for demo.write",
    );
    expect(notify).toHaveBeenLastCalledWith(
      "Denied write access for demo.write",
      "warning",
    );
  });

  it("serializes concurrent one-time requests instead of widening the grant", async () => {
    const custom = vi.fn(async () => "allow-once");
    const controller = new ApprovalController(policies, tuiContext(custom));

    await Promise.all([
      controller.approve(action),
      controller.approve({ ...action, ref: "demo.parallelWrite" }),
    ]);

    expect(custom).toHaveBeenCalledTimes(2);
  });

  it("lets a queued request inherit a session grant without a second prompt", async () => {
    const custom = vi.fn(async () => "allow-session");
    const controller = new ApprovalController(policies, tuiContext(custom));

    await Promise.all([
      controller.approve(action),
      controller.approve({ ...action, ref: "demo.parallelWrite" }),
    ]);

    expect(custom).toHaveBeenCalledOnce();
  });

  it.each(["ask", "auto"] as const)("internal %s refuses before a held queue, without TUI/RPC/classifier work or grants", async mode => {
    const session = new FabricSessionApprovals();
    let release!: () => void;
    const held = session.serialize(() => new Promise<void>(resolve => { release = resolve; }));
    await Promise.resolve();
    const serialize = vi.spyOn(session, "serialize");
    const custom = vi.fn(async () => "allow-session");
    const classify = vi.fn();
    const controller = new ApprovalController(
      { ...policies, write: mode }, tuiContext(custom), session,
      { classify } as unknown as FabricAutoApprovalClassifier, undefined, undefined, true,
    );
    try {
      await expect(controller.approve(action)).rejects.toThrow(/approval is unsupported for internal routing/);
      expect(serialize).not.toHaveBeenCalled(); expect(custom).not.toHaveBeenCalled(); expect(classify).not.toHaveBeenCalled();
      expect(session.approvedRisks.size).toBe(0);
    } finally { release(); await held; }
    const drained = vi.fn(async () => {});
    await session.serialize(drained);
    expect(drained).toHaveBeenCalledOnce();
  });

  it.each(["ask", "auto"] as const)("internal %s preserves session-granted and brokered network controls without queuing", async mode => {
    const network = { ...action, ref: "jev.evaluate", provider: "jev", risk: "network" as const };
    const session = new FabricSessionApprovals();
    session.approvedRisks.add("network");
    const serialize = vi.spyOn(session, "serialize");
    const custom = vi.fn();
    // The execution broker may be present without granting Jev: existing session
    // grants still count exactly as in the ordinary serialized approval path.
    await new ApprovalController({ ...policies, network: mode }, tuiContext(custom), session, undefined, undefined, () => false, true).approve(network);
    session.approvedRisks.clear();
    await new ApprovalController({ ...policies, network: mode }, tuiContext(custom), session, undefined, undefined, () => true, true).approve(network);
    expect(serialize).not.toHaveBeenCalled(); expect(custom).not.toHaveBeenCalled();
    expect(session.approvedRisks.size).toBe(0);
  });

  it.each(["tui", "rpc"] as const)("security round: fences a late %s session grant and releases a cancelled queue slot", async mode => {
    let finish!: (choice: string) => void;
    const prompt = vi.fn((..._args: unknown[]) => new Promise<string>(resolve => { finish = resolve; }));
    const session = new FabricSessionApprovals();
    const context = { hasUI: true, mode, ui: { custom: prompt, select: prompt, notify: vi.fn() } } as unknown as ExtensionContext;
    const controller = new ApprovalController(policies, context, session);
    const abort = new AbortController();
    const pending = controller.approve(action, {}, abort.signal);
    const outcome = pending.then(() => undefined, error => error as Error);
    await vi.waitFor(() => expect(prompt).toHaveBeenCalledOnce());
    abort.abort(new Error("execution ended"));
    const drained = vi.fn(async () => {});
    const slot = session.serialize(drained);
    try {
      await vi.waitFor(() => expect(drained).toHaveBeenCalledOnce(), { timeout: 200 });
    } finally {
      finish(mode === "tui" ? "allow-session" : "Allow write access for this session");
      await Promise.allSettled([pending, slot]);
    }
    expect(await outcome).toMatchObject({ message: "execution ended" });
    expect(session.approvedRisks.size).toBe(0);
    expect(context.ui.notify).not.toHaveBeenCalledWith("Allowed write access for this Pi session", "info");
    if (mode === "rpc") expect(prompt.mock.calls[0]?.[2]).toMatchObject({ signal: abort.signal });
  });

  it("security round: dismisses the custom dialog through its completion callback on abort", async () => {
    const abort = new AbortController();
    const done = vi.fn();
    const custom = vi.fn(async (factory: any) => {
      const component = factory({ requestRender() {} }, { fg: (_: string, text: string) => text, bold: (text: string) => text }, {}, done);
      abort.abort(new Error("dialog cancelled"));
      component.dispose?.();
      return "allow-session";
    });
    const session = new FabricSessionApprovals();
    await expect(new ApprovalController(policies, tuiContext(custom), session).approve(action, {}, abort.signal)).rejects.toThrow("dialog cancelled");
    expect(done).toHaveBeenCalledWith("deny");
    expect(session.approvedRisks.size).toBe(0);
  });

  it("denies actions blocked by policy without prompting", async () => {
    const custom = vi.fn(async () => "allow-once");
    const controller = new ApprovalController(policies, tuiContext(custom));
    await expect(controller.approve({ ...action, risk: "execute" })).rejects.toThrow(
      "denied by the Fabric execute policy",
    );
    expect(custom).not.toHaveBeenCalled();
  });
});
