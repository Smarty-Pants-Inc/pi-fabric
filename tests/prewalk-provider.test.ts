import { describe, expect, it, vi } from "vitest";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { FabricInvocationContext } from "../src/protocol.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { PrewalkController } from "../src/prewalk/controller.js";
import { checkedPrewalkPlan } from "../src/prewalk/plan.js";
import { captureLoadedFileIdentity } from "../src/build-identity.js";
import { PrewalkProvider } from "../src/providers/prewalk-provider.js";

const contextFor = (sessionId = "session-1"): FabricInvocationContext =>
  ({
    cwd: "/tmp",
    signal: undefined,
    parentToolCallId: "call-1",
    nestedToolCallId: "call-1_nested",
    extensionContext: { sessionManager: { getSessionId: () => sessionId } },
    update: vi.fn(),
  }) as unknown as FabricInvocationContext;

const plan = {
  outcome: "Guard the boundary",
  steps: ["Edit src/a.ts"],
  verification: ["bun run test:related -- src/a.ts"],
  risks: "None",
};

describe("prewalk provider", () => {
  it.each([
    ["idle", false], ["awaiting", true], ["disabled", false], ["other-session", false],
    ["recorded", false], ["handing-off", false], ["continuation", false],
    ["cancelled", false], ["rearmed", true],
  ] as const)("keeps all discovery surfaces live and session-scoped (%s)", async (phase, available) => {
    const controller = new PrewalkController();
    const provider = new PrewalkProvider(controller);
    const registry = new ActionRegistry();
    registry.register(provider);
    const context = contextFor();
    const arm = { model: "anthropic/executor", sessionId: "session-1", requirePlan: true };
    try {
      // Discover before every transition to catch stale cached availability.
      expect((await registry.list({}, context)).map(action => action.ref)).toEqual(["prewalk.status"]);
      if (phase !== "idle") controller.arm({
        ...arm,
        sessionId: phase === "other-session" ? "other" : arm.sessionId,
        requirePlan: phase !== "disabled",
      });
      if (["recorded", "handing-off", "continuation", "rearmed"].includes(phase)) {
        await provider.invoke("plan", plan, context);
      }
      if (phase === "handing-off" || phase === "continuation") {
        controller.claim([{ ref: "pi.write", nestedToolCallId: "write", startedAt: 1, success: true }], "session-1");
        if (phase === "continuation") controller.beginContinuation("continuation-1", "anthropic/frontier");
      }
      if (phase === "cancelled") controller.cancel();
      if (phase === "rearmed") controller.arm(arm);

      const expected = available ? ["plan", "status"] : ["status"];
      expect((await provider.list({}, context)).map(action => action.name)).toEqual(expected);
      expect((await provider.list({ query: "PLAN" }, context)).some(action => action.name === "plan")).toBe(available);
      expect(await provider.describe("unknown", context)).toBeUndefined();
      expect(await provider.describe("status", context)).toMatchObject({ name: "status" });
      expect((await registry.list({ provider: "prewalk" }, context)).map(action => action.name)).toEqual(expected);
      expect((await registry.search("prewalk.plan", context)).some(action => action.ref === "prewalk.plan")).toBe(available);
      const catalog = await registry.catalog(context, { provider: "prewalk" });
      expect(catalog.providers[0]?.actions.map(action => action.name)).toEqual(expected);
      if (available) {
        expect(await provider.describe("plan", context)).toMatchObject({ name: "plan" });
        const descriptor = await registry.describe("prewalk.plan", context);
        expect(descriptor.description).toContain("only when this session is awaiting one");
        expect(descriptor.description).toContain("Not a general task planner, checklist, or acceptance-ledger store");
        // No status preflight: discovery already supplies eligibility.
        const result = await registry.invoke("prewalk.plan", plan, {
          ...context, approve: async () => {}, audits: [], maxResultChars: 10_000,
        });
        expect(result).toMatchObject({ recorded: true });
        expect((await registry.list({}, context)).map(action => action.name)).toEqual(["status"]);
        await expect(registry.describe("prewalk.plan", context)).rejects.toThrow(/Unknown Fabric action/);
      } else {
        expect(await provider.describe("plan", context)).toBeUndefined();
        await expect(registry.describe("prewalk.plan", context)).rejects.toThrow(/Unknown Fabric action/);
        await expect(provider.invoke("plan", plan, context)).rejects.toThrow(/not awaiting/);
      }
    } finally {
      await registry.close();
    }
  });

  it("still validates a call whose descriptor was discovered before cancellation", async () => {
    const controller = new PrewalkController();
    const provider = new PrewalkProvider(controller);
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    expect(await provider.describe("plan", contextFor())).toMatchObject({ name: "plan" });
    controller.cancel();
    await expect(provider.invoke("plan", plan, contextFor())).rejects.toThrow(/not awaiting/);
  });

  it("records the plan that closes the readiness gate", async () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    const provider = new PrewalkProvider(controller);

    const result = await provider.invoke("plan", plan, contextFor());

    expect(result).toMatchObject({ recorded: true, readiness: "ready" });
    expect(controller.planReady("session-1")).toBe(true);
    // The recorded plan is the text the executor inherits from the transcript.
    expect(String((result as { plan: string }).plan)).toContain("Guard the boundary");
  });

  it("refuses a plan when no arm is awaiting one", async () => {
    const provider = new PrewalkProvider(new PrewalkController());
    await expect(provider.invoke("plan", plan, contextFor())).rejects.toThrow(/not awaiting/);
  });

  it("reports readiness for the armed session", async () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    const provider = new PrewalkProvider(controller);

    expect(await provider.invoke("status", {}, contextFor())).toMatchObject({
      state: "armed",
      planRequired: true,
      planReady: false,
      planPrompts: 0,
    });

    await provider.invoke("plan", plan, contextFor());
    expect(await provider.invoke("status", {}, contextFor())).toMatchObject({
      planRequired: false,
      planReady: true,
    });
  });

  it.each(["planned", "disabled", "unplanned"] as const)("reports claimed readiness through continuation and clears it on rearm (%s)", async (kind) => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: kind !== "disabled", alwaysRearm: true });
    const provider = new PrewalkProvider(controller);
    if (kind === "planned") await provider.invoke("plan", plan, contextFor());
    const audits = [{ ref: "pi.write", nestedToolCallId: "write", startedAt: 1, success: true }];
    if (kind === "unplanned") {
      controller.claim(audits, "session-1");
      controller.claim(audits, "session-1");
    }
    const claim = controller.claim(audits, "session-1");
    expect(claim?.kind).toBe("prewalk-claim");
    expect(await provider.invoke("status", {}, contextFor())).toMatchObject({
      state: "handing_off", planRequired: false, planReady: false, claimedReadiness: kind,
    });
    controller.beginContinuation("continuation-1", "anthropic/frontier");
    expect(await provider.invoke("status", {}, contextFor())).toMatchObject({
      state: "continuation_pending", claimedReadiness: kind,
    });
    expect(await provider.invoke("status", {}, contextFor("other"))).toMatchObject({ claimedReadiness: null });
    controller.acceptContinuation("session-1", "continuation-1");
    controller.takeContinuationSettlement("session-1");
    controller.finishContinuation("session-1", "continuation-1");
    expect(await provider.invoke("status", {}, contextFor())).toMatchObject({ state: "armed", claimedReadiness: null });
    controller.cancel();
    expect(await provider.invoke("status", {}, contextFor())).toMatchObject({ state: "idle", claimedReadiness: null });
  });

  it("reports loaded versus current-disk runtime identity", async () => {
    const dir = mkdtempSync(join(tmpdir(), "fabric-identity-"));
    const file = join(dir, "lazy.js");
    writeFileSync(file, "export const generation = 1;\n");
    const loaded = captureLoadedFileIdentity(pathToFileURL(file).href);
    expect(loaded).not.toBeNull();
    if (!loaded) throw new Error("expected loaded identity");
    writeFileSync(file, "export const generation = 2;\n");
    const provider = new PrewalkProvider(new PrewalkController(), {
      buildIdentity: () => ({ entry: null, lazyRuntime: loaded }),
    });
    const status = (await provider.invoke("status", {}, contextFor())) as {
      runtime: { entry: unknown; lazyRuntime: { stale: boolean; loadedSha256: string; diskSha256: string } };
    };
    expect(status.runtime.entry).toBe(null);
    expect(status.runtime.lazyRuntime).toMatchObject({
      stale: true,
      loadedSha256: loaded.sha256,
    });
    expect(status.runtime.lazyRuntime.diskSha256).not.toBe(loaded.sha256);

    const current = captureLoadedFileIdentity(pathToFileURL(file).href);
    expect(current).not.toBeNull();
    if (!current) throw new Error("expected current identity");
    const freshProvider = new PrewalkProvider(new PrewalkController(), {
      buildIdentity: () => ({ entry: current, lazyRuntime: current }),
    });
    const fresh = (await freshProvider.invoke("status", {}, contextFor())) as {
      runtime: { entry: { stale: boolean }; lazyRuntime: { stale: boolean } };
    };
    expect(fresh.runtime.entry).toMatchObject({ stale: false });
    expect(fresh.runtime.lazyRuntime).toMatchObject({ stale: false });
  });

  it("does not throw when the loaded file cannot be hashed", () => {
    expect(captureLoadedFileIdentity(pathToFileURL(join(tmpdir(), "missing-fabric-identity.js")).href)).toBeNull();
  });

  it("rejects an incomplete plan", async () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    const provider = new PrewalkProvider(controller);
    const planInput = (value: Record<string, unknown>) =>
      provider.invoke("plan", value, contextFor());

    await expect(planInput({ ...plan, steps: [] })).rejects.toThrow(/steps and verification/);
    await expect(planInput({ ...plan, risks: "   " })).rejects.toThrow(/nonblank/);
    await expect(planInput({ ...plan, extra: true })).rejects.toThrow(/Unknown prewalk plan field/);
    const longStep = "x".repeat(4001);
    expect(() => checkedPrewalkPlan({ ...plan, steps: [longStep] as never })).toThrow(/nonblank/);
  });
});
