import { describe, expect, it } from "vitest";
import type { FabricCallAudit } from "../src/core/action-registry.js";
import {
  PrewalkController,
  type FabricPrewalkClaimOutcome,
} from "../src/prewalk/controller.js";

const audit = (
  ref: string,
  success: boolean,
  sequence = 1,
): FabricCallAudit => ({
  ref,
  nestedToolCallId: `call-${sequence}`,
  startedAt: sequence,
  endedAt: sequence + 1,
  success,
});

const claimSeq = (outcome: FabricPrewalkClaimOutcome | undefined): number | undefined =>
  outcome?.kind === "prewalk-claim" ? outcome.seq : undefined;

describe("PrewalkController", () => {
  it("arms a one-shot executor and captures the next task when omitted", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });

    expect(controller.isArmed("session-1")).toBe(true);
    controller.observeTask("session-1", "  Implement the guard  ");
    controller.observeTask("session-1", "Do not replace the first task");

    expect(controller.status()).toMatchObject({
      state: "armed",
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
  });

  it("keeps the arm across read-only settles, dropping only the settled task", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });

    expect(controller.settleTask("session-1")).toBe(true);
    expect(controller.status().state).toBe("armed");
    controller.observeTask("session-1", "Inspect without changing anything");
    expect(controller.settleTask("session-2")).toBe(false);
    expect(controller.settleTask("session-1")).toBe(true);
    expect(controller.status()).toMatchObject({ state: "armed", sessionId: "session-1" });
    expect(controller.status()).not.toHaveProperty("task");
  });

  // Regression: a plan-first turn (reads only) must not burn the arm — the
  // next matching mutation boundary still claims the handoff.
  it("claims a mutation that lands after earlier read-only settles", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });

    controller.observeTask("session-1", "Survey the module");
    expect(controller.settleTask("session-1")).toBe(true);
    controller.observeTask("session-1", "Implement it now");
    expect(
      controller.claim(
        [audit("pi.read", true), audit("pi.write", true, 2)],
        "session-1",
      ),
    ).toMatchObject({
      arm: { model: "anthropic/executor", task: "Implement it now" },
      mutation: { ref: "pi.write" },
    });
  });

  it("re-arms without leaking the previous task when always re-arm is enabled", () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Inspect without changing anything",
      alwaysRearm: true,
    });

    expect(controller.settleTask("session-1")).toBe(true);
    expect(controller.status()).toMatchObject({
      state: "armed",
      model: "anthropic/executor",
      sessionId: "session-1",
      alwaysRearm: true,
    });
    expect(controller.status()).not.toHaveProperty("task");

    controller.observeTask("session-1", "Implement the next task");
    expect(controller.status()).toMatchObject({ task: "Implement the next task" });
  });

  it("claims only the first successful recognized mutation", () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement",
    });

    expect(
      controller.claim(
        [audit("pi.read", true), audit("pi.edit", false, 2)],
        "session-1",
      ),
    ).toBeUndefined();
    const claim = controller.claim(
      [audit("pi.read", true), audit("pi.write", true, 2)],
      "session-1",
    );

    expect(claim).toMatchObject({
      arm: { model: "anthropic/executor", task: "Implement" },
      mutation: { ref: "pi.write", success: true },
    });
    expect(controller.status()).toMatchObject({ state: "handing_off" });
    expect(controller.claim([audit("schema.commit", true)], "session-1")).toBeUndefined();
  });

  it("does not cross session boundaries", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });

    expect(controller.claim([audit("pi.edit", true)], "session-2")).toBeUndefined();
    expect(controller.isArmed("session-1")).toBe(true);
  });

  it("disarms when the program already performed an explicit handoff", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });

    expect(
      controller.claim(
        [audit("pi.edit", true), audit("agents.handoff", true, 2)],
        "session-1",
      ),
    ).toBeUndefined();
    expect(controller.status()).toEqual({ state: "idle" });
  });

  it("orders claims with a session-monotonic seq that survives re-arms and cancels", () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      alwaysRearm: true,
    });

    expect(claimSeq(controller.claim([audit("pi.edit", true)], "session-1"))).toBe(1);
    expect(controller.completeTask()).toMatchObject({ state: "armed" });

    expect(claimSeq(controller.claimFsDrift("session-1", ["a.ts"]))).toBe(2);
    controller.cancel();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });
    expect(claimSeq(controller.claim([audit("pi.write", true)], "session-1"))).toBe(3);
    // Other sessions start their own sequence.
    controller.arm({ model: "anthropic/executor", sessionId: "session-2" });
    expect(claimSeq(controller.claimFsDrift("session-2", ["b.ts"]))).toBe(1);
  });

  it("claims filesystem drift with a synthesized fs.drift mutation audit", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", task: "Implement" });

    const claim = controller.claimFsDrift("session-1", ["src/a.ts", "src/b.ts"]);

    expect(claim).toMatchObject({
      arm: { model: "anthropic/executor", task: "Implement", mode: "in-place" },
      mutation: {
        ref: "fs.drift",
        success: true,
        args: { files: ["src/a.ts", "src/b.ts"] },
      },
    });
    expect(controller.status()).toMatchObject({ state: "handing_off" });
    expect(controller.claimFsDrift("session-1", ["src/c.ts"])).toBeUndefined();
  });

  it("refuses filesystem claims when idle, busy, or across sessions", () => {
    const controller = new PrewalkController();
    expect(controller.claimFsDrift("session-1", ["a.ts"])).toBeUndefined();

    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });
    expect(controller.claimFsDrift("session-2", ["a.ts"])).toBeUndefined();

    controller.claimFsDrift("session-1", []);
    expect(controller.status()).toMatchObject({ state: "handing_off" });
    expect(controller.claimFsDrift("session-1", ["a.ts"])).toBeUndefined();
  });

  it("falls back to armed after a failed filesystem handoff", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });

    controller.claimFsDrift("session-1", ["a.ts"]);
    expect(controller.failHandoff()).toMatchObject({ state: "armed" });
  });

  it("exposes the canonical continuation payload only while the continuation is pending", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });
    controller.claim([audit("pi.edit", true)], "session-1");
    const message = {
      role: "custom" as const,
      customType: "pi-fabric-prewalk-continue",
      content: "Continue the existing task.",
      display: false,
      details: { mode: "in-place", continuationId: "cont-9" },
      timestamp: 5,
    };
    controller.beginContinuation("cont-9", "anthropic/frontier", message);

    expect(controller.pendingContinuationMessage("session-1")).toEqual({
      continuationId: "cont-9",
      message,
    });
    expect(controller.pendingContinuationMessage("session-2")).toBeUndefined();
    // Read-only exposure: callers cannot mutate the canonical payload.
    controller.pendingContinuationMessage("session-1")!.message.content = "tampered";
    expect(controller.pendingContinuationMessage("session-1")!.message.content)
      .toBe("Continue the existing task.");

    expect(controller.acceptContinuation("session-1", "cont-9")).toBe(true);
    expect(controller.takeContinuationSettlement("session-1"))
      .toMatchObject({ continuationId: "cont-9" });
    expect(controller.finishContinuation("session-1", "cont-9")).toBe(true);
    expect(controller.pendingContinuationMessage("session-1")).toBeUndefined();
  });

  it("keeps the borrowed Main model across cancel so new sessions can restore it", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });
    controller.claim([audit("pi.edit", true)], "session-1");
    controller.beginContinuation("cont-1", "anthropic/frontier");

    expect(controller.borrowedReturn()).toEqual({
      returnModel: "anthropic/frontier",
      executorModel: "anthropic/executor",
    });
    controller.cancel();
    expect(controller.status()).toEqual({ state: "idle" });
    expect(controller.borrowedReturn()).toEqual({
      returnModel: "anthropic/frontier",
      executorModel: "anthropic/executor",
    });
    controller.clearBorrowed();
    expect(controller.borrowedReturn()).toBeUndefined();
  });

  it("adopts a recovered borrow only when no live lifecycle owns it", () => {
    const recovered = { returnModel: "anthropic/frontier", executorModel: "anthropic/executor" };
    const controller = new PrewalkController();

    expect(controller.hydrateBorrowedMain(recovered)).toBe(true);
    expect(controller.borrowedReturn()).toEqual(recovered);
    expect(
      controller.hydrateBorrowedMain({ returnModel: "anthropic/other", executorModel: "anthropic/executor" }),
    ).toBe(false);
    expect(controller.borrowedReturn()).toEqual(recovered);

    const handingOff = new PrewalkController();
    handingOff.arm({ model: "anthropic/executor", sessionId: "session-1" });
    handingOff.claim([audit("pi.edit", true)], "session-1");
    expect(handingOff.hydrateBorrowedMain(recovered)).toBe(false);
    expect(handingOff.borrowedReturn()).toBeUndefined();

    const hostile = new PrewalkController();
    expect(hostile.hydrateBorrowedMain({ returnModel: "  ", executorModel: "anthropic/executor" })).toBe(false);
    expect(
      hostile.hydrateBorrowedMain({ returnModel: "anthropic/executor", executorModel: "anthropic/executor" }),
    ).toBe(false);
    expect(hostile.borrowedReturn()).toBeUndefined();
  });
});

// Frontier-first planning: the boundary that would hand off asks for the plan
// first, so the executor inherits frontier deliberation instead of raw intent.
describe("PrewalkController plan checkpoint", () => {
  const recordedPlan = {
    outcome: "Guard the boundary",
    steps: ["Edit src/a.ts"],
    verification: ["bun run test:related -- src/a.ts"],
    risks: "None",
  };

  it("owns its recorded plan and snapshots readiness before changing state", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    const input = structuredClone(recordedPlan);
    controller.submitPlan("session-1", input);
    input.steps[0] = "caller mutation must not change the plan";
    const claim = controller.claim([audit("pi.edit", true)], "session-1");
    expect(claim).toMatchObject({ readiness: { kind: "planned", plan: recordedPlan } });
    expect(controller.status().state).toBe("handing_off");
    controller.failHandoff();
    expect(controller.claimFsDrift("session-1", ["a.ts"])).toMatchObject({
      readiness: { kind: "planned", plan: recordedPlan },
    });
  });

  it("withholds the handoff until the frontier records a plan", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });

    const checkpoint = controller.claim([audit("pi.edit", true)], "session-1");
    expect(checkpoint).toMatchObject({
      kind: "prewalk-plan",
      mutation: { ref: "pi.edit" },
      arm: { model: "anthropic/executor" },
    });
    // Nothing was consumed: the arm stays armed and no handoff started.
    expect(controller.status()).toMatchObject({ state: "armed" });
    expect(claimSeq(checkpoint)).toBeUndefined();
    // Delivery is not readiness: the gate stays closed until a plan exists.
    expect(controller.planCheckpointRequired("session-1")).toBe(true);

    expect(controller.submitPlan("session-1", recordedPlan)).toBe(true);
    expect(controller.planReady("session-1")).toBe(true);
    expect(claimSeq(controller.claim([audit("pi.write", true, 2)], "session-1"))).toBe(1);
    expect(controller.status()).toMatchObject({ state: "handing_off" });
  });

  it("does not repeat the checkpoint after a failed handoff", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    expect(controller.claim([audit("pi.edit", true)], "session-1")).toMatchObject({ kind: "prewalk-plan" });
    controller.submitPlan("session-1", recordedPlan);
    expect(controller.claim([audit("pi.write", true, 2)], "session-1")).toMatchObject({ kind: "prewalk-claim" });
    expect(controller.failHandoff()).toMatchObject({ state: "armed" });
    expect(controller.claim([audit("pi.edit", true, 3)], "session-1")).toMatchObject({ kind: "prewalk-claim" });
  });

  it("owes a fresh checkpoint after always-rearm", () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      requirePlan: true,
      alwaysRearm: true,
    });
    controller.submitPlan("session-1", recordedPlan);
    expect(controller.claim([audit("pi.edit", true)], "session-1")).toMatchObject({ kind: "prewalk-claim" });
    expect(controller.completeTask()).toMatchObject({ state: "armed" });
    expect(controller.planCheckpointRequired("session-1")).toBe(true);
    expect(controller.claim([audit("pi.edit", true, 2)], "session-1")).toMatchObject({ kind: "prewalk-plan" });
  });

  it("claims the first mutation when the gate is off", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });
    expect(controller.planCheckpointRequired("session-1")).toBe(false);
    expect(controller.claim([audit("pi.edit", true)], "session-1")).toMatchObject({ kind: "prewalk-claim" });
  });

  it("gates filesystem drift claims through the same checkpoint", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    expect(controller.claimFsDrift("session-1", ["src/a.ts"])).toMatchObject({
      kind: "prewalk-plan",
      mutation: { ref: "fs.drift", args: { files: ["src/a.ts"] } },
    });
    controller.submitPlan("session-1", recordedPlan);
    expect(controller.claimFsDrift("session-1", ["src/a.ts"])).toMatchObject({ kind: "prewalk-claim" });
  });

  it("reopens the checkpoint when delivery failed", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    expect(controller.claim([audit("pi.edit", true)], "session-1")).toMatchObject({ kind: "prewalk-plan" });
    controller.reopenPlanCheckpoint();
    // The refunded nudge asks again, and still never hands off unplanned.
    expect(controller.claim([audit("pi.edit", true, 2)], "session-1")).toMatchObject({ kind: "prewalk-plan" });
    expect(controller.planReady("session-1")).toBe(false);
  });

  it("never gates an explicit agents.handoff", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    expect(controller.claim([audit("agents.handoff", true)], "session-1")).toBeUndefined();
    expect(controller.planCheckpointRequired("session-1")).toBe(false);
  });

  it("keeps withholding while the plan is missing and fails open once the budget is spent", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });

    expect(controller.claim([audit("pi.edit", true)], "session-1")).toMatchObject({ kind: "prewalk-plan" });
    expect(controller.claim([audit("pi.edit", true, 2)], "session-1")).toMatchObject({ kind: "prewalk-plan" });
    // Budget spent: an armed session must not deadlock behind a model that
    // ignores the checkpoint, so the next boundary hands off unplanned.
    expect(controller.planRequired("session-1")).toBe(true);
    expect(claimSeq(controller.claim([audit("pi.edit", true, 3)], "session-1"))).toBe(1);
  });

  it("does not carry a recorded plan into the next task", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    controller.observeTask("session-1", "task A");
    expect(controller.submitPlan("session-1", recordedPlan)).toBe(true);
    expect(controller.settleTask("session-1")).toBe(true);

    controller.observeTask("session-1", "unrelated task B");

    expect(controller.planReady("session-1")).toBe(false);
    expect(controller.planCheckpointRequired("session-1")).toBe(true);
    expect(controller.claim([audit("pi.edit", true)], "session-1")).toMatchObject({ kind: "prewalk-plan" });
  });

  it("refuses a plan that is not awaited", () => {
    const controller = new PrewalkController();
    expect(controller.submitPlan("session-1", recordedPlan)).toBe(false);
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });
    expect(controller.submitPlan("session-1", recordedPlan)).toBe(false);
  });
});
