import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { ApprovalController, FabricSessionApprovals } from "../src/core/approval-controller.js";
import { FabricExecutionService } from "../src/execution-service.js";

const context = { cwd: process.cwd(), mode: "print", hasUI: false, sessionManager: { getSessionId: () => "headless-cancel" } } as unknown as ExtensionContext;
const descriptor = { name: "write", description: "requires approval", risk: "write" as const, inputSchema: { type: "object", additionalProperties: false } };
const action = { ...descriptor, ref: "demo.write", provider: "demo" };

describe("headless approval cancellation", () => {
  it("A9 cancels the effective timed-out wait and releases the next execution's approval slot", async () => {
    const registry = new ActionRegistry();
    const invoke = vi.fn(async () => "approved");
    registry.register({ name: "demo", description: "test", async list() { return [descriptor]; }, async describe() { return descriptor; }, invoke });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.timeoutMs = 100;
    config.approvals.write = "ask"; config.approvals.headless = "decision";
    const service = new FabricExecutionService(registry, config);
    const signals: Array<AbortSignal | undefined> = [];
    let cleanup: (() => void) | undefined;
    service.setHeadlessApproval(async (_action, _reason, signal) => {
      signals.push(signal);
      if (signals.length > 1) return true;
      return new Promise<boolean>((resolve) => {
        cleanup = () => resolve(false);
        if (signal?.aborted) resolve(false);
        else signal?.addEventListener("abort", () => resolve(false), { once: true });
      });
    });
    const run = (id: string) => service.execute({ code: 'return await tools.call({ ref: "demo.write", args: {} });', context, signal: undefined, parentToolCallId: id, onPartial() {} });
    try {
      expect((await run("first")).success).toBe(false);
      expect((await run("next")).value).toBe("approved");
      expect(signals[0]?.aborted).toBe(true);
      expect(invoke).toHaveBeenCalledOnce();
    } finally { cleanup?.(); await registry.close(); }
  });
  it("A9 rejects a cancelled queued request before creating its headless decision", async () => {
    const session = new FabricSessionApprovals();
    let release!: () => void;
    const held = session.serialize(() => new Promise<void>(resolve => { release = resolve; }));
    await Promise.resolve();
    const headless = vi.fn(async () => true);
    const controller = new ApprovalController({ ...DEFAULT_FABRIC_CONFIG.approvals, write: "ask", headless: "decision" }, context, session, undefined, undefined, undefined, headless);
    const abort = new AbortController();
    const queued = controller.approve(action, {}, abort.signal);
    const check = expect(queued).rejects.toThrow("queued stop");
    abort.abort(new Error("queued stop")); release();
    await held; await check;
    expect(headless).not.toHaveBeenCalled();
  });
});
