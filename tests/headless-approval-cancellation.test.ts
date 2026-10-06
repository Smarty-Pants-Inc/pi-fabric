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
  it.each((["provider", "view"] as const).flatMap(source => (["entered", "queued"] as const).map(stage => [source, stage] as const)))(
    "A9 propagates %s revocation to an %s approval while the guest catches and continues", async (source, stage) => {
      const registry = new ActionRegistry();
      const invoke = vi.fn(async () => "approved");
      for (const name of ["demo", "next"]) registry.register({ name, description: "test", async list() { return [descriptor]; }, async describe() { return descriptor; }, invoke });
      let caught!: () => void;
      const caughtPromise = new Promise<void>(resolve => { caught = resolve; });
      const stop = new AbortController();
      const lease = await registry.acquireCapabilityView(["demo.write"], { cwd: context.cwd, signal: undefined, parentToolCallId: "view", nestedToolCallId: "view", update() {}, extensionContext: context });
      const config = structuredClone(DEFAULT_FABRIC_CONFIG);
      config.executor.timeoutMs = 3000; config.approvals.write = "ask"; config.approvals.headless = "decision";
      const service = new FabricExecutionService(registry, config);
      if (source === "view") service.setCapabilityView(lease.view);
      let releaseQueue: (() => void) | undefined;
      const held = stage === "queued" ? service.sessionApprovals.serialize(() => new Promise<void>(resolve => { releaseQueue = resolve; })) : Promise.resolve();
      await Promise.resolve();
      const serialize = vi.spyOn(service.sessionApprovals, "serialize");
      const signals: Array<AbortSignal | undefined> = [];
      let releaseDecision: (() => void) | undefined;
      service.setHeadlessApproval(async (action, _reason, signal) => {
        signals.push(signal);
        if (action.provider === "next") return true;
        return new Promise<boolean>(resolve => {
          releaseDecision = () => resolve(false);
          if (signal?.aborted) resolve(false);
          else signal?.addEventListener("abort", () => resolve(false), { once: true });
        });
      });
      const run = (code: string, id: string, signal?: AbortSignal) => service.execute({ code, context, signal, parentToolCallId: id, onPartial(partial) { if (partial.progress === "caught") caught(); } });
      let settled = false;
      const guest = run('try { await tools.call({ref:"demo.write",args:{}}); } catch {} await tools.progress({message:"caught"}); return await new Promise(() => {});', "revoked-guest", stop.signal).finally(() => { settled = true; });
      try {
        await vi.waitFor(() => expect(stage === "queued" ? serialize.mock.calls.length : signals.length).toBe(1));
        if (source === "provider") registry.revokeProvider("demo"); else await lease.release();
        await caughtPromise;
        expect(settled).toBe(false);
        if (stage === "entered") expect(signals[0]?.aborted).toBe(true);
        releaseQueue?.(); await held;
        service.setCapabilityView(undefined);
        expect((await run('return await tools.call({ref:"next.write",args:{}});', "next-guest")).value).toBe("approved");
        if (stage === "entered") expect(signals[0]?.aborted).toBe(true);
        else expect(signals).toHaveLength(1); // Only next.write, never the obsolete demo decision.
        expect(invoke).toHaveBeenCalledOnce();
      } finally {
        stop.abort(); releaseQueue?.(); releaseDecision?.(); await held; await guest; await lease.release(); await registry.close();
      }
    });
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
