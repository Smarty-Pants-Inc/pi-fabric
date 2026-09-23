import {
  SessionManager,
  convertToLlm,
  type ExtensionAPI,
  type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Agent } from "@earendil-works/pi-agent-core";
import { createAssistantMessageEventStream, type AssistantMessage, type Model } from "@earendil-works/pi-ai";
import type { AgentToolResultMessage } from "../src/agents/types.js";
import type { FabricExecutionResult } from "../src/execution-service.js";
import { PrewalkController } from "../src/prewalk/controller.js";
import { CompactController } from "../src/core/compact-controller.js";
import { normalizeFabricConfig } from "../src/config.js";
import { createFabricExecTool } from "../src/fabric-exec-tool.js";
import type { FabricState } from "../src/fabric-state.js";
import { prewalkPlanText } from "../src/prewalk/plan.js";
import { PrewalkProvider } from "../src/providers/prewalk-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { defaultCodePreviewSettings } from "../src/ui/code-preview.js";
import { createPassiveHostSession } from "../scripts/lib/passive-host-session.mjs";
import {
  PREWALK_ARMED_MESSAGE_TYPE,
  PREWALK_PLAN_MESSAGE_TYPE,
  deliverPrewalkPlanCheckpoint,
  filterPrewalkContinuationMessages,
  filterPrewalkPlanningDirectives,
  hasPrewalkArmedPrompt,
  prewalkArmedPrompt,
  withTrajectoryRearmDirective,
} from "../src/prewalk/messages.js";
import {
  restoreBorrowedInPlaceMain,
  settleInPlacePrewalk,
} from "../src/prewalk/return.js";
import {
  claimFabricFsDriftHandoff,
  claimFabricHandoff,
  runFabricHandoffAtBoundary,
  type PendingFabricHandoff,
} from "../src/prewalk/handoff.js";

// Fixtures here arm without requirePlan, so a mutation boundary always produces a
// handoff. A plan checkpoint would mean the fixture drifted, not the assertion.
const claimHandoff = (
  ...args: Parameters<typeof claimFabricHandoff>
): PendingFabricHandoff | undefined => {
  const outcome = claimFabricHandoff(...args);
  if (outcome?.kind === "prewalk-plan") {
    throw new Error("unexpected prewalk plan checkpoint in a handoff fixture");
  }
  return outcome;
};

const claimDriftHandoff = (
  ...args: Parameters<typeof claimFabricFsDriftHandoff>
): PendingFabricHandoff | undefined => {
  const outcome = claimFabricFsDriftHandoff(...args);
  if (outcome?.kind === "prewalk-plan") {
    throw new Error("unexpected prewalk plan checkpoint in a fs-drift fixture");
  }
  return outcome;
};

const execution = (): FabricExecutionResult => ({
  success: true,
  value: "complete outer result",
  logs: [],
  audits: [
    {
      ref: "pi.read",
      nestedToolCallId: "read",
      startedAt: 1,
      endedAt: 2,
      success: true,
      args: { path: "src/a.ts" },
      result: "source",
    },
    {
      ref: "pi.edit",
      nestedToolCallId: "edit-one",
      startedAt: 3,
      endedAt: 4,
      success: true,
      args: { path: "src/a.ts" },
      result: { ok: true },
    },
    {
      ref: "pi.write",
      nestedToolCallId: "edit-two",
      startedAt: 5,
      endedAt: 6,
      success: true,
      args: { path: "src/b.ts" },
      result: { ok: true },
    },
  ],
  phases: [],
  trace: {
    kind: "pi-fabric.execution",
    version: 1,
    outcome: "succeeded",
    counts: {
      droppedValues: 0,
      truncatedValues: 0,
      redactedValues: 0,
      droppedOperations: 0,
    },
    operations: [],
    phases: [],
  },
  elapsedMs: 1,
});

const outerResult = (): AgentToolResultMessage => ({
  role: "toolResult",
  toolCallId: "outer",
  toolName: "fabric_exec",
  content: [{ type: "text", text: "complete outer result" }],
  details: { success: true },
  isError: false,
  timestamp: 10,
});

// Passive delivery semantics live in scripts/lib/passive-host-session.mjs so
// the benchmark drives the same host path.

const context = () => {
  const source = SessionManager.inMemory();
  vi.spyOn(source, "getSessionId").mockReturnValue("session-1");
  source.appendMessage({ role: "user", content: "Implement everything", timestamp: 1 });
  source.appendMessage({
    role: "assistant",
    content: [{
      type: "toolCall",
      id: "outer",
      name: "fabric_exec",
      arguments: { code: "await pi.edit(...); return 'complete outer result';" },
    }],
    api: "anthropic",
    provider: "anthropic",
    model: "frontier",
    usage: {
      input: 0,
      output: 0,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "toolUse",
    timestamp: 2,
  });
  const target = { provider: "anthropic", id: "executor" };
  const sourceModel = { provider: "anthropic", id: "frontier" };
  const nextMainModel = { provider: "anthropic", id: "main-next" };
  const setStatus = vi.fn();
  return {
    value: {
      cwd: process.cwd(),
      signal: undefined,
      model: sourceModel,
      modelRegistry: {
        find: (provider: string, id: string) => {
          if (provider === target.provider && id === target.id) return target;
          if (provider === sourceModel.provider && id === sourceModel.id) return sourceModel;
          if (provider === nextMainModel.provider && id === nextMainModel.id) {
            return nextMainModel;
          }
          return undefined;
        },
      },
      sessionManager: source,
      ui: { setStatus, notify: vi.fn() },
    } as unknown as ExtensionContext,
    setStatus,
    target,
    sourceModel,
    nextMainModel,
  };
};

const extension = () => {
  const setModel = vi.fn().mockResolvedValue(true);
  const sendMessage = vi.fn();
  return {
    value: { setModel, sendMessage } as unknown as ExtensionAPI,
    setModel,
    sendMessage,
  };
};

const unusedRunner = () => ({ executeHandoff: vi.fn() });

const bashExecution = (): FabricExecutionResult => ({
  ...execution(),
  audits: [
    {
      ref: "pi.bash",
      nestedToolCallId: "bash-one",
      startedAt: 1,
      endedAt: 2,
      success: true,
      args: { cmd: "sed -i '' s/old/new/ src/guard.ts" },
      result: { ok: true },
    },
  ],
});

describe("trajectory executor handoff failure continuation", () => {
  const continuationType = "pi-fabric-handoff-continuation";
  beforeEach(() => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "trajectory-1");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
  });
  afterEach(() => vi.unstubAllEnvs());

  const prepare = (kind: "explicit" | "prewalk-trajectory" | "prewalk-in-place" = "explicit", seeded = true) => {
    const ctx = context();
    const session = ctx.value.sessionManager as SessionManager;
    const assistant = session.getLeafEntry()!;
    if (assistant.type !== "message" || assistant.message.role !== "assistant") throw new Error("Missing fixture assistant turn");
    const assistantMessage = assistant.message;
    session.branch(assistant.parentId!);
    if (seeded) session.appendCustomEntry("pi-fabric-handoff", {
      sourceSessionId: "parent-session", boundary: "fabric_exec_end",
    });
    const ext = extension();
    ext.value.appendEntry = vi.fn((type, data) => { session.appendCustomEntry(type, data); });
    const controller = new PrewalkController();
    const invoke = async (outcome = "failed", implementation = "Partial work in guard.ts; commit abc123") => {
      // Pi persists the native assistant turn before the outer result hook.
      session.appendMessage(assistantMessage);
      controller.arm({ mode: kind === "prewalk-in-place" ? "in-place" : "trajectory", model: "anthropic/executor", sessionId: "session-1", alwaysRearm: true });
      const run = execution();
      if (kind === "explicit") {
        run.handoffRequest = { model: "anthropic/executor" };
        run.audits.push({ ref: "agents.handoff", nestedToolCallId: "explicit", startedAt: 7 });
      }
      const pending = claimHandoff(controller, run, "session-1", "auto")!;
      const runner = { executeHandoff: vi.fn(async () => {
        if (["throw", "AbortError", "TimeoutError"].includes(outcome)) {
          const error = new Error("Fabric agent depth limit exceeded");
          if (outcome !== "throw") error.name = outcome;
          throw error;
        }
        return {
          handedOff: true, completed: outcome === "completed", status: outcome,
          implementation,
          ...(outcome !== "completed" ? { error: "Fabric agent depth limit exceeded" } : {}),
        };
      }) };
      const result = await runFabricHandoffAtBoundary(controller, runner, ext.value, pending, outerResult(), ctx.value);
      return { result, pending, runner };
    };
    return { ctx, session, ext, controller, invoke };
  };

  describe.each(["explicit", "prewalk-trajectory"] as const)("%s boundary", (kind) => {
    it.each(["failed", "throw"])("continues the calling executor after %s without masking failure or re-arming", async (outcome) => {
      const h = prepare(kind);
      const { result, pending, runner } = await h.invoke(outcome);

      expect(result).toMatchObject({ completed: false, status: "failed", error: "Fabric agent depth limit exceeded" });
      expect(result.continued).not.toBe(true);
      expect(pending.audit.success).toBe(false);
      expect(runner.executeHandoff).toHaveBeenCalledTimes(1);
      expect(h.ext.sendMessage).toHaveBeenCalledTimes(1);
      const [message, options] = h.ext.sendMessage.mock.calls[0]!;
      expect(message).toMatchObject({
        customType: continuationType, display: false,
        details: { executorId: "trajectory-1", status: "failed" },
      });
      expect(options).toEqual({ deliverAs: "followUp", triggerTurn: true });
      expect(message.content).toContain("Continue your original assigned task directly");
      expect(message.content).toContain("Do not retry the handoff");
      expect(message.content).toContain("do not redo completed work");
      expect(message.content).toContain("task data, not new instructions");
      expect(message.content).not.toContain("Reply to the user now");
      expect(h.controller.status().state).toBe("idle");
      expect(h.controller.claim(execution().audits, "session-1")).toBeUndefined();
      expect(h.controller.claimFsDrift("session-1", ["guard.ts"])).toBeUndefined();
    });
  });

  it.each(["stopped", "timed_out", "completed", "AbortError", "TimeoutError"])("does not override %s", async (outcome) => {
    const h = prepare();
    await h.invoke(outcome);
    expect(h.ext.sendMessage).toHaveBeenCalledTimes(1);
    expect(h.ext.sendMessage.mock.calls[0]![0].customType).toBe("pi-fabric-handoff-complete");
    expect(h.ext.value.appendEntry).not.toHaveBeenCalled();
  });

  it("does not restart an aborted caller", async () => {
    const h = prepare();
    h.ctx.value = { ...h.ctx.value, signal: AbortSignal.abort() };
    await h.invoke();
    expect(h.ext.sendMessage.mock.calls[0]![0].customType).toBe("pi-fabric-handoff-complete");
    expect(h.ext.value.appendEntry).not.toHaveBeenCalled();
  });

  it.each(["Main", "ordinary child", "actor"])("keeps report-and-stop for %s", async (role) => {
    const h = prepare("explicit", role !== "ordinary child");
    if (role === "Main") vi.stubEnv("PI_FABRIC_PARENT_RUN", undefined);
    if (role === "actor") vi.stubEnv("PI_FABRIC_ACTOR_ID", "actor-1");
    await h.invoke();
    expect(h.ext.sendMessage.mock.calls[0]![0].content).toContain("Propose the next step");
    expect(h.ext.value.appendEntry).not.toHaveBeenCalled();
  });

  it("spends only one continuation per executor, including after a reload", async () => {
    const h = prepare();
    await h.invoke();
    await h.invoke("throw");
    expect(h.ext.sendMessage.mock.calls.map(([message]) => message.customType)).toEqual([
      continuationType, "pi-fabric-handoff-complete",
    ]);
    const restored = prepare();
    for (const entry of h.session.getBranch()) {
      if (entry.type === "custom" && entry.customType === continuationType) {
        restored.session.appendCustomEntry(entry.customType, entry.data);
      }
    }
    await restored.invoke();
    expect(restored.ext.sendMessage.mock.calls[0]![0].customType).toBe("pi-fabric-handoff-complete");
  });

  it("does not reset the per-executor budget by navigating before its receipt", async () => {
    const h = prepare();
    const leaf = h.session.getLeafId()!;
    h.session.appendCustomEntry(continuationType, { executorId: "trajectory-1" });
    h.session.branch(leaf);
    await h.invoke();
    expect(h.ext.sendMessage.mock.calls[0]![0].customType).toBe("pi-fabric-handoff-complete");
  });

  it("does not classify a normally completed answer by its failure wording", async () => {
    const h = prepare();
    const implementation = "The handoff failed because Fabric agent depth was reached";
    const { result } = await h.invoke("completed", implementation);
    expect(result).toMatchObject({ completed: true, status: "completed", implementation });
    expect(h.ext.value.appendEntry).not.toHaveBeenCalled();
  });

  it("does not turn an in-place model-switch failure into trajectory recovery", async () => {
    const h = prepare("prewalk-in-place");
    h.ext.setModel.mockResolvedValue(false);
    const { result, runner } = await h.invoke();
    expect(result).toMatchObject({ completed: false, status: "failed" });
    expect(runner.executeHandoff).not.toHaveBeenCalled();
    expect(h.ext.value.appendEntry).not.toHaveBeenCalled();
    // The in-place boundary keeps the run alive with the failed result in
    // context, so no queued follow-up explains the same failure twice.
    expect(h.ext.sendMessage).not.toHaveBeenCalled();
  });

  it("does not spend another executor's inherited continuation receipt", async () => {
    const h = prepare();
    h.session.appendCustomEntry(continuationType, { executorId: "parent-executor" });
    await h.invoke();
    expect(h.ext.sendMessage.mock.calls[0]![0].customType).toBe(continuationType);
  });

  it("preserves the original failure when continuation delivery throws", async () => {
    const h = prepare();
    h.ext.sendMessage.mockImplementation(() => { throw new Error("queue unavailable"); });
    const { result, pending } = await h.invoke();
    expect(result).toMatchObject({ completed: false, status: "failed", error: "Fabric agent depth limit exceeded" });
    expect(pending.audit.success).toBe(false);
  });
});

describe("persisted in-place Prewalk recovery", () => {
  it("restores a persisted continuation with a fresh controller without replaying it", async () => {
    const ctx = context();
    const session = ctx.value.sessionManager as SessionManager;
    session.appendModelChange("anthropic", "executor");
    session.appendCustomMessageEntry("pi-fabric-prewalk-continue", "Continue the existing task", false, {
      mode: "in-place",
      model: "anthropic/executor",
      returnModel: "anthropic/frontier",
      continuationId: "restart-cont",
    });
    ctx.value.model = ctx.target as typeof ctx.value.model;
    const controller = new PrewalkController();
    const ext = extension();
    ext.setModel.mockImplementation(async (model) => {
      ctx.value.model = model;
      session.appendModelChange(model.provider, model.id);
      return true;
    });

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(true);
    expect(ext.setModel.mock.calls).toEqual([[ctx.sourceModel]]);
    expect(controller.status()).toEqual({ state: "idle" });
    expect(controller.borrowedReturn()).toBeUndefined();
    expect(controller.pendingContinuationMessage("session-1")).toBeUndefined();
    expect(ext.sendMessage).not.toHaveBeenCalled();
    expect(await restoreBorrowedInPlaceMain(new PrewalkController(), ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).toHaveBeenCalledOnce();
  });

  const appendContinuation = (session: SessionManager, details: Record<string, unknown>): void => {
    session.appendCustomMessageEntry(
      "pi-fabric-prewalk-continue",
      "Continue the existing task",
      false,
      details,
    );
  };

  it("never resurrects a handoff a later model change superseded", async () => {
    const ctx = context();
    const session = ctx.value.sessionManager as SessionManager;
    session.appendModelChange("anthropic", "executor");
    appendContinuation(session, {
      mode: "in-place",
      model: "anthropic/executor",
      returnModel: "anthropic/frontier",
      continuationId: "settled-cont",
    });
    session.appendModelChange("anthropic", "frontier");
    ctx.value.model = ctx.target as typeof ctx.value.model;
    const controller = new PrewalkController();
    const ext = extension();

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).not.toHaveBeenCalled();
    expect(controller.borrowedReturn()).toBeUndefined();
  });

  it("leaves a manual model choice alone but keeps the adopted record inert", async () => {
    const ctx = context();
    const session = ctx.value.sessionManager as SessionManager;
    session.appendModelChange("anthropic", "executor");
    appendContinuation(session, {
      mode: "in-place",
      model: "anthropic/executor",
      returnModel: "anthropic/frontier",
      continuationId: "manual-pick-cont",
    });
    ctx.value.model = ctx.nextMainModel as typeof ctx.value.model;
    const controller = new PrewalkController();
    const ext = extension();

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).not.toHaveBeenCalled();
    expect(controller.borrowedReturn()).toEqual({
      returnModel: "anthropic/frontier",
      executorModel: "anthropic/executor",
    });
  });

  it("ignores trajectory and malformed continuation records", async () => {
    const ctx = context();
    const session = ctx.value.sessionManager as SessionManager;
    session.appendModelChange("anthropic", "executor");
    appendContinuation(session, {
      mode: "trajectory",
      model: "anthropic/executor",
      returnModel: "anthropic/frontier",
      continuationId: "trajectory-cont",
    });
    appendContinuation(session, {
      mode: "in-place",
      model: "anthropic/executor",
      returnModel: "anthropic/frontier",
      continuationId: "",
    });
    ctx.value.model = ctx.target as typeof ctx.value.model;
    const controller = new PrewalkController();
    const ext = extension();

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).not.toHaveBeenCalled();
    expect(controller.borrowedReturn()).toBeUndefined();
  });

  it("does not recover a continuation that is off the active branch", async () => {
    const ctx = context();
    const session = ctx.value.sessionManager as SessionManager;
    const beforeHandoff = session.getLeafId()!;
    session.appendModelChange("anthropic", "executor");
    appendContinuation(session, {
      mode: "in-place",
      model: "anthropic/executor",
      returnModel: "anthropic/frontier",
      continuationId: "abandoned-branch-cont",
    });
    session.branch(beforeHandoff);
    ctx.value.model = ctx.target as typeof ctx.value.model;
    const controller = new PrewalkController();
    const ext = extension();

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).not.toHaveBeenCalled();
    expect(controller.borrowedReturn()).toBeUndefined();
  });

  it("recovers through a compaction recorded after the continuation", async () => {
    const ctx = context();
    const session = ctx.value.sessionManager as SessionManager;
    const firstKept = session.getLeafId()!;
    session.appendModelChange("anthropic", "executor");
    appendContinuation(session, {
      mode: "in-place",
      model: "anthropic/executor",
      returnModel: "anthropic/frontier",
      continuationId: "compacted-cont",
    });
    session.appendCompaction("Executor completed the batch", firstKept, 1_000);
    ctx.value.model = ctx.target as typeof ctx.value.model;
    const controller = new PrewalkController();
    const ext = extension();

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(true);
    expect(ext.setModel.mock.calls).toEqual([[ctx.sourceModel]]);
    expect(controller.borrowedReturn()).toBeUndefined();
  });

  it("retains the adopted record when the return model is unavailable", async () => {
    const ctx = context();
    const session = ctx.value.sessionManager as SessionManager;
    session.appendModelChange("anthropic", "executor");
    appendContinuation(session, {
      mode: "in-place",
      model: "anthropic/executor",
      returnModel: "anthropic/retired",
      continuationId: "unavailable-cont",
    });
    ctx.value.model = ctx.target as typeof ctx.value.model;
    const controller = new PrewalkController();
    const ext = extension();

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).not.toHaveBeenCalled();
    expect(controller.borrowedReturn()).toEqual({
      returnModel: "anthropic/retired",
      executorModel: "anthropic/executor",
    });
    expect(ctx.value.ui.notify).toHaveBeenLastCalledWith(
      "Prewalk left Main on the executor; could not restore unavailable model anthropic/retired.",
      "error",
    );
  });
});

describe("outer-boundary Prewalk", () => {
  it("switches Main in place and queues a hidden follow-up by default", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const run = execution();
    const pending = claimHandoff(controller, run, "session-1", "json");

    expect(run.audits.map((audit) => audit.ref)).toEqual([
      "pi.read",
      "pi.edit",
      "pi.write",
      "fabric.prewalk",
    ]);
    expect(pending).toMatchObject({
      kind: "prewalk-in-place",
      args: { model: "anthropic/executor", task: "Implement the guard" },
      triggerRef: "pi.edit",
    });

    const ctx = context();
    const ext = extension();
    const runner = unusedRunner();
    const activity = vi.fn();
    const result = await runFabricHandoffAtBoundary(
      controller,
      runner,
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
      activity,
    );

    expect(runner.executeHandoff).not.toHaveBeenCalled();
    expect(ext.setModel).toHaveBeenCalledWith(ctx.target);
    expect(ctx.value.ui.notify).toHaveBeenCalledWith(
      "Prewalk is continuing in Main with anthropic/executor, then returning to anthropic/frontier.",
      "info",
    );
    expect(ext.sendMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        customType: "pi-fabric-prewalk-continue",
        display: false,
        content: expect.stringContaining("Continue the existing task"),
        details: expect.objectContaining({
          continuationId: expect.any(String),
          returnModel: "anthropic/frontier",
        }),
      }),
      { triggerTurn: false },
    );
    expect(result).toMatchObject({
      prewalk: true,
      mode: "in-place",
      continued: true,
      status: "continued",
      trigger: { ref: "pi.edit" },
    });
    expect(activity).toHaveBeenCalledWith(expect.objectContaining({ type: "progress" }));
    expect(controller.status()).toMatchObject({
      state: "continuation_pending",
      model: "anthropic/executor",
      returnModel: "anthropic/frontier",
      accepted: false,
    });
    expect(ctx.setStatus).toHaveBeenLastCalledWith(
      "fabric-prewalk",
      "continuing Main → anthropic/executor",
    );
  });


  it("returns Main to its boundary model and re-arms only after the matching continuation settles", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
      alwaysRearm: true,
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();

    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    const continuation = ext.sendMessage.mock.calls.find(
      ([message]) => message.customType === "pi-fabric-prewalk-continue",
    )?.[0] as { details: { continuationId: string } };

    expect(controller.acceptContinuation("session-1", "stale-id")).toBe(false);
    expect(await settleInPlacePrewalk(controller, ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).toHaveBeenCalledTimes(1);

    expect(controller.acceptContinuation(
      "session-1",
      continuation.details.continuationId,
    )).toBe(true);
    ctx.value.model = ctx.target as typeof ctx.value.model;
    expect(await settleInPlacePrewalk(controller, ext.value, ctx.value)).toBe(true);

    expect(ext.setModel.mock.calls).toEqual([[ctx.target], [ctx.sourceModel]]);
    expect(controller.status()).toMatchObject({
      state: "armed",
      model: "anthropic/executor",
      alwaysRearm: true,
    });
    expect(controller.status()).not.toHaveProperty("task");
    expect(ctx.value.ui.notify).toHaveBeenLastCalledWith(
      "Prewalk complete. Main returned to anthropic/frontier and re-armed for the next task.",
      "info",
    );

    expect(await settleInPlacePrewalk(controller, ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).toHaveBeenCalledTimes(2);
  });

  it("filters stale continuation messages and accepts only the pending identity", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();
    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    const continuation = ext.sendMessage.mock.calls.find(
      ([message]) => message.customType === "pi-fabric-prewalk-continue",
    )?.[0] as { details: { continuationId: string } };
    const stale = {
      role: "custom",
      customType: "pi-fabric-prewalk-continue",
      content: "stale",
      details: { mode: "in-place", continuationId: "stale-id" },
    };
    const current = { ...continuation, role: "custom" };
    const ordinary = { role: "user", content: "keep me" };

    const filtered = filterPrewalkContinuationMessages(
      [stale, current, ordinary],
      (continuationId) => controller.acceptContinuation("session-1", continuationId),
    );

    expect(filtered).toEqual({ messages: [current, ordinary], changed: true });
    expect(controller.status()).toMatchObject({ accepted: true });
  });

  it("keeps trajectory continuation prompts out of the in-place identity filter", () => {
    const trajectory = {
      role: "custom",
      customType: "pi-fabric-prewalk-continue",
      content: "Prewalk trajectory handoff complete: verify and summarize.",
      details: { mode: "trajectory", model: "anthropic/executor", trigger: "pi.edit" },
    };

    const result = filterPrewalkContinuationMessages([trajectory], () => {
      throw new Error("trajectory prompts must never reach the acceptance gate");
    });

    expect(result).toEqual({ messages: [trajectory], changed: false });
  });

  describe("planning directive retirement", () => {
    const armedDirective = {
      role: "custom",
      customType: PREWALK_ARMED_MESSAGE_TYPE,
      content: "Prewalk armed → neuralwatt/kimi-k3 (in-place): this session owes a recorded plan before handoff.",
      details: { mode: "in-place", model: "neuralwatt/kimi-k3" },
    };
    const checkpointDirective = {
      role: "custom",
      customType: PREWALK_PLAN_MESSAGE_TYPE,
      content: "Prewalk plan checkpoint → neuralwatt/kimi-k3: Record the plan now with prewalk.plan.",
      details: { mode: "in-place", model: "neuralwatt/kimi-k3", trigger: "pi.edit" },
    };
    const failureMessage = {
      role: "custom",
      customType: "pi-fabric-prewalk-failure",
      content: "Prewalk handoff failed; the session re-armed.",
      details: {},
    };
    const ordinaryMessage = { role: "user", content: "keep me" };

    it("keeps planning directives visible while the arm is live", () => {
      const result = filterPrewalkPlanningDirectives(
        [armedDirective, checkpointDirective, failureMessage, ordinaryMessage],
        true,
      );
      expect(result).toEqual({
        messages: [armedDirective, checkpointDirective, failureMessage, ordinaryMessage],
        changed: false,
      });
    });

    it("drops armed and checkpoint directives once the arm is claimed or off", () => {
      const result = filterPrewalkPlanningDirectives(
        [armedDirective, checkpointDirective, failureMessage, ordinaryMessage],
        false,
      );
      expect(result).toEqual({ messages: [failureMessage, ordinaryMessage], changed: true });
    });

    it("leaves directive-free requests untouched when planning is hidden", () => {
      const result = filterPrewalkPlanningDirectives([failureMessage, ordinaryMessage], false);
      expect(result).toEqual({ messages: [failureMessage, ordinaryMessage], changed: false });
    });

    it("derives visibility from live controller state across claim and cancel", () => {
      const controller = new PrewalkController();
      controller.arm({ model: "neuralwatt/kimi-k3", sessionId: "session-1" });
      const projected = (messages: unknown[]) =>
        filterPrewalkPlanningDirectives(messages as never[], controller.isArmed("session-1")).messages;
      expect(projected([armedDirective, ordinaryMessage])).toEqual([armedDirective, ordinaryMessage]);
      const pending = claimHandoff(controller, execution(), "session-1", "json");
      expect(pending).toBeDefined();
      expect(projected([armedDirective, checkpointDirective, ordinaryMessage])).toEqual([ordinaryMessage]);
      controller.cancel();
      expect(projected([armedDirective, ordinaryMessage])).toEqual([ordinaryMessage]);
    });

    it("directs the executor to report once and stop after verified completion", async () => {
      const controller = new PrewalkController();
      controller.arm({ model: "anthropic/executor", sessionId: "session-1", task: "Implement the guard" });
      const pending = claimHandoff(controller, execution(), "session-1", "json");
      const ctx = context();
      const ext = extension();
      await runFabricHandoffAtBoundary(
        controller, unusedRunner(), ext.value, pending!, outerResult(), ctx.value,
      );
      const continuation = ext.sendMessage.mock.calls.find(
        ([message]) => message.customType === "pi-fabric-prewalk-continue",
      )?.[0] as { content: string };
      expect(String(continuation.content)).toContain("report completion once and stop");
      expect(String(continuation.content)).toContain("reopen work only for a failed check, contradicting evidence, or a changed request");
    });
  });

  it("compacts before restoring Main when compactOnReturn is enabled", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();

    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    const continuation = ext.sendMessage.mock.calls.find(
      ([message]) => message.customType === "pi-fabric-prewalk-continue",
    )?.[0] as { details: { continuationId: string } };
    controller.acceptContinuation("session-1", continuation.details.continuationId);
    ctx.value.model = ctx.target as typeof ctx.value.model;

    const compact = {
      request: vi.fn(),
      maybeCommit: vi.fn(async () => {}),
    };
    expect(
      await settleInPlacePrewalk(controller, ext.value, ctx.value, {
        compactOnReturn: true,
        compact,
      }),
    ).toBe(true);

    expect(compact.request).toHaveBeenCalledWith(
      expect.objectContaining({
        reason: "in-place prewalk return",
        requestedBy: "prewalk",
      }),
    );
    expect(compact.maybeCommit).toHaveBeenCalledWith(ctx.value);
    const compactionOrder = compact.maybeCommit.mock.invocationCallOrder[0]!;
    const switchOrder = ext.setModel.mock.invocationCallOrder;
    expect(compactionOrder).toBeGreaterThan(switchOrder[0]!);
    expect(compactionOrder).toBeLessThan(switchOrder[1]!);
    expect(ext.setModel.mock.calls).toEqual([[ctx.target], [ctx.sourceModel]]);
    expect(controller.status()).toEqual({ state: "idle" });
  });

  it.each([
    { error: "Nothing to compact (session too small)", status: "cancelled", existing: false },
    { error: "Nothing to compact (session too small)", status: "cancelled", existing: true },
    { error: "Provider unavailable", status: "failed", existing: false },
    { error: undefined, status: "committed", existing: false },
  ])("settles actual compaction ($status, existing=$existing) before returning Main", async ({ error, status, existing }) => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", task: "Implement the guard" });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();
    await runFabricHandoffAtBoundary(controller, unusedRunner(), ext.value, pending!, outerResult(), ctx.value);
    const continuation = ext.sendMessage.mock.calls.find(
      ([message]) => message.customType === "pi-fabric-prewalk-continue",
    )?.[0] as { details: { continuationId: string } };
    controller.acceptContinuation("session-1", continuation.details.continuationId);
    ctx.value.model = ctx.target as typeof ctx.value.model;
    const compact = new CompactController();
    if (existing) compact.request({ requestedBy: "model", instructions: "Keep my request" });
    const hostCompact = vi.fn<ExtensionContext["compact"]>((options) => {
      if (error) options?.onError?.(new Error(error));
      else options?.onComplete?.({ summary: "Executor report and verification", firstKeptEntryId: "kept", tokensBefore: 30_000 });
    });
    ctx.value.compact = hostCompact;

    expect(await settleInPlacePrewalk(controller, ext.value, ctx.value, { compact })).toBe(true);
    expect(compact.status().last).toMatchObject({ status, requestedBy: existing ? "model" : "prewalk" });
    expect(compact.status().pending).toBeUndefined();
    if (existing) expect(hostCompact.mock.calls[0]?.[0]?.customInstructions).toBe("Keep my request");
    expect(hostCompact.mock.invocationCallOrder[0]).toBeLessThan(ext.setModel.mock.invocationCallOrder[1]!);
    expect(ext.setModel.mock.calls).toEqual([[ctx.target], [ctx.sourceModel]]);
    expect(controller.status().state).toBe("idle");
    expect(await settleInPlacePrewalk(controller, ext.value, ctx.value, { compact })).toBe(false);
    await compact.maybeCommit(ctx.value);
    expect(hostCompact).toHaveBeenCalledTimes(1);
  });

  it("restores Main without compacting when compactOnReturn is disabled", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();

    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    const continuation = ext.sendMessage.mock.calls.find(
      ([message]) => message.customType === "pi-fabric-prewalk-continue",
    )?.[0] as { details: { continuationId: string } };
    controller.acceptContinuation("session-1", continuation.details.continuationId);
    ctx.value.model = ctx.target as typeof ctx.value.model;

    const compact = {
      request: vi.fn(),
      maybeCommit: vi.fn(async () => {}),
    };
    expect(
      await settleInPlacePrewalk(controller, ext.value, ctx.value, {
        compactOnReturn: false,
        compact,
      }),
    ).toBe(true);

    expect(compact.request).not.toHaveBeenCalled();
    expect(compact.maybeCommit).not.toHaveBeenCalled();
    expect(ext.setModel.mock.calls).toEqual([[ctx.target], [ctx.sourceModel]]);
  });



  it("automatically returns Main and becomes idle after a one-shot in-place continuation", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();

    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    const continuation = ext.sendMessage.mock.calls.find(
      ([message]) => message.customType === "pi-fabric-prewalk-continue",
    )?.[0] as { details: { continuationId: string } };
    controller.acceptContinuation("session-1", continuation.details.continuationId);
    ctx.value.model = ctx.target as typeof ctx.value.model;

    expect(await settleInPlacePrewalk(controller, ext.value, ctx.value)).toBe(true);
    expect(ext.setModel.mock.calls).toEqual([[ctx.target], [ctx.sourceModel]]);
    expect(controller.status()).toEqual({ state: "idle" });
    expect(ctx.setStatus).toHaveBeenLastCalledWith("fabric-prewalk", undefined);
  });

  it("restores Main after cancel when the session is still on the executor", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();

    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    controller.cancel();
    ctx.value.model = ctx.target as typeof ctx.value.model;

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(true);
    expect(ext.setModel.mock.calls).toEqual([[ctx.target], [ctx.sourceModel]]);
    expect(ctx.value.ui.notify).toHaveBeenLastCalledWith(
      "Restored Main to anthropic/frontier after in-place prewalk.",
      "info",
    );
  });

  it("does not steal a new session that already loaded Main or a later pick", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();

    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    controller.cancel();

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(false);
    ctx.value.model = ctx.nextMainModel as typeof ctx.value.model;
    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).toHaveBeenCalledTimes(1);
  });

  it("restores Main on a new session that inherited the executor", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();

    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    expect(await settleInPlacePrewalk(controller, ext.value, ctx.value)).toBe(false);
    controller.cancel();
    ctx.value.model = ctx.target as typeof ctx.value.model;

    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(true);
    expect(ext.setModel.mock.calls).toEqual([[ctx.target], [ctx.sourceModel]]);
  });
  it("automatically repeats Main → executor → Main with a freshly captured Main model", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "First task",
      alwaysRearm: true,
    });
    const ctx = context();
    const ext = extension();

    const runCycle = async () => {
      const pending = claimHandoff(controller, execution(), "session-1", "json");
      await runFabricHandoffAtBoundary(
        controller,
        unusedRunner(),
        ext.value,
        pending!,
        outerResult(),
        ctx.value,
      );
      const continuation = ext.sendMessage.mock.calls
        .filter(([message]) => message.customType === "pi-fabric-prewalk-continue")
        .at(-1)?.[0] as { details: { continuationId: string; returnModel: string } };
      expect(controller.acceptContinuation(
        "session-1",
        continuation.details.continuationId,
      )).toBe(true);
      ctx.value.model = ctx.target as typeof ctx.value.model;
      expect(await settleInPlacePrewalk(controller, ext.value, ctx.value)).toBe(true);
      expect(controller.status()).toMatchObject({
        state: "armed",
        model: "anthropic/executor",
        alwaysRearm: true,
      });
      expect(controller.status()).not.toHaveProperty("task");
      return continuation.details.returnModel;
    };

    expect(await runCycle()).toBe("anthropic/frontier");

    ctx.value.model = ctx.nextMainModel as typeof ctx.value.model;
    controller.observeTask("session-1", "Second task");
    expect(await runCycle()).toBe("anthropic/main-next");

    expect(ext.setModel.mock.calls).toEqual([
      [ctx.target],
      [ctx.sourceModel],
      [ctx.target],
      [ctx.nextMainModel],
    ]);
    expect(ext.sendMessage.mock.calls.filter(
      ([message]) => message.customType === "pi-fabric-prewalk-continue",
    )).toHaveLength(2);
  });

  it("returns Main and keeps the task armed when queuing the continuation fails", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
      alwaysRearm: true,
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();
    ext.sendMessage.mockImplementationOnce(() => {
      throw new Error("queue unavailable");
    });

    const result = await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );

    expect(result).toMatchObject({
      status: "failed",
      continued: false,
      error: "queue unavailable",
    });
    expect(ext.setModel.mock.calls).toEqual([[ctx.target], [ctx.sourceModel]]);
    expect(controller.status()).toMatchObject({
      state: "armed",
      task: "Implement the guard",
      alwaysRearm: true,
    });
  });
  it.each(["rejected", "thrown"] as const)("retains Main recovery when delivery and rollback both fail (%s)", async (failure) => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
      alwaysRearm: true,
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json")!;
    const ctx = context();
    const ext = extension();
    let failReturn = true;
    ext.setModel.mockImplementation(async (model) => {
      if (model === ctx.sourceModel && failReturn) {
        if (failure === "thrown") throw new Error("return unavailable");
        return false;
      }
      ctx.value.model = model;
      return true;
    });
    let borrowedAtDelivery: ReturnType<PrewalkController["borrowedReturn"]>;
    ext.sendMessage.mockImplementationOnce(() => {
      borrowedAtDelivery = controller.borrowedReturn();
      throw new Error("queue unavailable");
    });

    const result = await runFabricHandoffAtBoundary(
      controller, unusedRunner(), ext.value, pending, outerResult(), ctx.value,
    );
    expect(result).toMatchObject({ status: "failed", continued: false });
    const borrowed = { returnModel: "anthropic/frontier", executorModel: "anthropic/executor" };
    expect(borrowedAtDelivery).toEqual(borrowed);
    expect(controller.borrowedReturn()).toEqual(borrowed);
    expect(controller.status()).toEqual({ state: "idle" });
    expect(controller.settleTask("session-1")).toBe(false);
    expect(claimHandoff(controller, execution(), "session-1", "json")).toBeUndefined();
    expect(ctx.value.model).toBe(ctx.target);

    // Cancel/bootstrap must retain the recovery record until Main can return.
    controller.cancel();
    failReturn = false;
    expect(await restoreBorrowedInPlaceMain(controller, ext.value, ctx.value)).toBe(true);
    expect(ctx.value.model).toBe(ctx.sourceModel);
    expect(ext.setModel.mock.calls).toEqual([[ctx.target], [ctx.sourceModel], [ctx.sourceModel]]);
  });

  it("pauses after restoration failure without losing Main on the next task", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
      alwaysRearm: true,
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();
    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    const continuation = ext.sendMessage.mock.calls.find(
      ([message]) => message.customType === "pi-fabric-prewalk-continue",
    )?.[0] as { details: { continuationId: string } };
    controller.acceptContinuation("session-1", continuation.details.continuationId);
    ctx.value.model = ctx.target as typeof ctx.value.model;
    ext.setModel.mockResolvedValueOnce(false);

    expect(await settleInPlacePrewalk(controller, ext.value, ctx.value)).toBe(false);
    expect(controller.status()).toMatchObject({ state: "idle" });
    expect(controller.borrowedReturn()).toEqual({
      returnModel: "anthropic/frontier", executorModel: "anthropic/executor",
    });
    // The host's generic settle fallback must not replace the failure chip.
    expect(controller.settleTask("session-1")).toBe(false);
    expect(ctx.setStatus).toHaveBeenLastCalledWith(
      "fabric-prewalk",
      "return failed → anthropic/frontier",
    );
    controller.observeTask("session-1", "A later unrelated task");
    expect(claimHandoff(controller, execution(), "session-1", "json")).toBeUndefined();
    expect(controller.borrowedReturn()?.returnModel).toBe("anthropic/frontier");
    expect(await settleInPlacePrewalk(controller, ext.value, ctx.value)).toBe(false);
    expect(ext.setModel).toHaveBeenCalledTimes(2);
  });
  it("keeps the armed task when the executor model cannot be selected", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
      alwaysRearm: true,
    });
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const ctx = context();
    const ext = extension();
    ext.setModel.mockResolvedValueOnce(false);

    const result = await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );

    expect(result).toMatchObject({ status: "failed", continued: false });
    // No continuation and no queued failure report: the in-place boundary no
    // longer terminates, so this same run carries the failed result back to
    // Main instead of queueing a second explaining turn.
    expect(ext.sendMessage).not.toHaveBeenCalled();
    expect(controller.status()).toMatchObject({
      state: "armed",
      task: "Implement the guard",
      alwaysRearm: true,
    });
  });

  it.each(["one-at-a-time", "all"] as const)("delivers task, plan and foreign thinking in one executor request (%s)", async (followUpMode) => {
    const controller = new PrewalkController();
    controller.arm({
      model: "neuralwatt/kimi-k3",
      sessionId: "session-1",
      task: "Implement the guard",
      requirePlan: true,
    });
    const plan = {
      outcome: "Guard the boundary",
      steps: ["Implement EXACT-FIRST-REQUEST-PLAN"],
      verification: ["Run the queue regression"],
      risks: "Preserve unrelated work",
    };

    const kimiModel = {
      provider: "neuralwatt",
      id: "kimi-k3",
      api: "openai-completions",
      reasoning: true,
      compat: { requiresReasoningContentOnAssistantMessages: true },
    };
    const codexModel = {
      provider: "openai-codex",
      id: "gpt-5.6-sol",
      api: "openai-responses",
      reasoning: true,
    };
    const source = SessionManager.inMemory();
    vi.spyOn(source, "getSessionId").mockReturnValue("session-1");
    source.appendMessage({ role: "user", content: "Implement everything", timestamp: 1 });
    const thinkingEntryId = source.appendMessage({
      role: "assistant",
      content: [
        {
          type: "thinking",
          thinking: "**Plan the guard**\n\nsteps",
          thinkingSignature: '{"id":"rs_x","type":"reasoning","encrypted_content":"gAAA"}',
        },
        {
          type: "toolCall",
          id: "outer",
          name: "fabric_exec",
          arguments: { code: "await pi.edit(...); return 'complete outer result';" },
        },
      ],
      api: "openai-responses",
      provider: "openai-codex",
      model: "gpt-5.6-sol",
      usage: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "toolUse",
      timestamp: 2,
    });
    const ctx = {
      cwd: process.cwd(),
      signal: undefined,
      model: { provider: "openai-codex", id: "gpt-5.6-sol" },
      modelRegistry: {
        find: (provider: string, id: string) =>
          provider === "neuralwatt" && id === "kimi-k3"
            ? kimiModel
            : provider === "openai-codex" && id === "gpt-5.6-sol"
              ? codexModel
              : undefined,
      },
      sessionManager: source,
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const ext = extension();
    await new PrewalkProvider(controller).invoke("plan", plan, {
      extensionContext: ctx, update() {},
    } as unknown as FabricInvocationContext); // Do not return the plan to the executor.
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const completed: AssistantMessage = {
      role: "assistant", content: [{ type: "text", text: "Done" }],
      api: "openai-completions", provider: kimiModel.provider, model: kimiModel.id,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      stopReason: "stop", timestamp: 3,
    };
    const requests: string[] = [];
    const armedDirective = {
      role: "custom",
      customType: PREWALK_ARMED_MESSAGE_TYPE,
      content: prewalkArmedPrompt("in-place", "neuralwatt/kimi-k3"),
      details: { mode: "in-place", model: "neuralwatt/kimi-k3" },
      display: false,
      timestamp: 1,
    } as const;
    const agent = new Agent({
      followUpMode,
      initialState: { model: kimiModel as unknown as Model<"openai-completions">, messages: [armedDirective, completed] },
      convertToLlm,
      // Mirrors src/index.ts: continuation filter first, then planning
      // directives retire with the claimed arm (isArmed=false).
      transformContext: async (messages) => filterPrewalkPlanningDirectives(
        filterPrewalkContinuationMessages(
          messages,
          (id) => controller.acceptContinuation("session-1", id),
          controller.pendingContinuationMessage("session-1"),
        ).messages,
        controller.isArmed("session-1"),
      ).messages,
      streamFn: (_model, context) => {
        requests.push(JSON.stringify(context.messages));
        const stream = createAssistantMessageEventStream();
        stream.push({ type: "start", partial: completed });
        stream.push({ type: "done", reason: "stop", message: completed });
        return stream;
      },
    });
    const hostSession = createPassiveHostSession(agent, source);
    ext.sendMessage.mockImplementation(async (message, options) => {
      await hostSession.sendCustomMessage(message, options);
    });

    // Production shape: the boundary turn continues naturally with the outer
    // result in the transcript, so the executor's request follows in the same
    // run and no queued turn replays the continuation later.
    agent.state.messages.push(outerResult());
    const result = await runFabricHandoffAtBoundary(
      controller, unusedRunner(), ext.value, pending!, outerResult(), ctx, vi.fn(),
    );
    expect(agent.hasQueuedMessages()).toBe(false);
    await agent.continue();
    expect(requests).toHaveLength(1);
    expect(requests[0]).toContain("Implement the guard");
    expect(requests[0]).toContain("EXACT-FIRST-REQUEST-PLAN");
    expect(requests[0]).not.toContain("Prewalk armed →");
    expect(controller.status()).toMatchObject({ state: "continuation_pending", accepted: true });
    expect(ext.setModel).toHaveBeenCalledWith(kimiModel);
    expect(ext.sendMessage).toHaveBeenCalledTimes(1);
    const continuation = ext.sendMessage.mock.calls[0];
    expect(continuation?.[0]).toMatchObject({
      customType: "pi-fabric-prewalk-continue",
      display: false,
      details: {
        mode: "in-place",
        model: "neuralwatt/kimi-k3",
        continuationId: expect.any(String),
        thinkingTransfer: { policy: "re-signed", citedBlocks: 1 },
      },
    });
    expect(String(continuation?.[0].content)).toContain(prewalkPlanText(plan));
    expect(String(continuation?.[0].content)).toContain(`[entry ${thinkingEntryId}]`);
    expect(String(continuation?.[0].content)).toContain("Plan the guard");
    expect(continuation?.[1]).toEqual({ triggerTurn: false });
    // The passive copy persists after the boundary turn's tool results, so the
    // transcript — and every later run that snapshots it — keeps the payload.
    expect(source.getEntries().some((entry) =>
      entry.type === "custom_message" && entry.customType === "pi-fabric-prewalk-continue",
    )).toBe(true);
    expect(agent.state.messages.some((message) => message.role === "custom")).toBe(true);
    expect(result).toMatchObject({ mode: "in-place", status: "continued" });
    // The digest is context-only: Pi's ground-truth log above is untouched.
    expect(
      JSON.stringify(source.getBranch()).includes("reasoning_content"),
    ).toBe(false);
  });

  it("front-loads task, plan and digest ahead of pending steers (one-at-a-time)", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "neuralwatt/kimi-k3",
      sessionId: "session-1",
      task: "Implement the guard",
      requirePlan: true,
    });
    const plan = {
      outcome: "Guard the boundary",
      steps: ["Implement EXACT-FIRST-REQUEST-PLAN"],
      verification: ["Run the queue regression"],
      risks: "Preserve unrelated work",
    };
    const kimiModel = {
      provider: "neuralwatt",
      id: "kimi-k3",
      api: "openai-completions",
      reasoning: true,
      compat: { requiresReasoningContentOnAssistantMessages: true },
    };
    const codexModel = {
      provider: "openai-codex",
      id: "gpt-5.6-sol",
      api: "openai-responses",
      reasoning: true,
    };
    const source = SessionManager.inMemory();
    vi.spyOn(source, "getSessionId").mockReturnValue("session-1");
    source.appendMessage({ role: "user", content: "Implement everything", timestamp: 1 });
    const thinkingEntryId = source.appendMessage({
      role: "assistant",
      content: [
        {
          type: "thinking",
          thinking: "**Plan the guard**\n\nsteps",
          thinkingSignature: '{"id":"rs_x","type":"reasoning","encrypted_content":"gAAA"}',
        },
        {
          type: "toolCall",
          id: "outer",
          name: "fabric_exec",
          arguments: { code: "await pi.edit(...); return 'complete outer result';" },
        },
      ],
      api: "openai-responses",
      provider: "openai-codex",
      model: "gpt-5.6-sol",
      usage: {
        input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
      stopReason: "toolUse",
      timestamp: 2,
    });
    const ctx = {
      cwd: process.cwd(),
      signal: undefined,
      model: { provider: "openai-codex", id: "gpt-5.6-sol" },
      modelRegistry: {
        find: (provider: string, id: string) =>
          provider === "neuralwatt" && id === "kimi-k3"
            ? kimiModel
            : provider === "openai-codex" && id === "gpt-5.6-sol"
              ? codexModel
              : undefined,
      },
      sessionManager: source,
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const ext = extension();
    await new PrewalkProvider(controller).invoke("plan", plan, {
      extensionContext: ctx, update() {},
    } as unknown as FabricInvocationContext);
    const pending = claimHandoff(controller, execution(), "session-1", "json");
    const completed: AssistantMessage = {
      role: "assistant", content: [{ type: "text", text: "Done" }],
      api: "openai-completions", provider: kimiModel.provider, model: kimiModel.id,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      stopReason: "stop", timestamp: 3,
    };
    const requests: string[] = [];
    const armedDirective = {
      role: "custom",
      customType: PREWALK_ARMED_MESSAGE_TYPE,
      content: prewalkArmedPrompt("in-place", "neuralwatt/kimi-k3"),
      details: { mode: "in-place", model: "neuralwatt/kimi-k3" },
      display: false,
      timestamp: 1,
    } as const;
    const agent = new Agent({
      followUpMode: "one-at-a-time",
      steeringMode: "one-at-a-time",
      initialState: { model: kimiModel as unknown as Model<"openai-completions">, messages: [armedDirective, completed] },
      convertToLlm,
      transformContext: async (messages) => filterPrewalkPlanningDirectives(
        filterPrewalkContinuationMessages(
          messages,
          (id) => controller.acceptContinuation("session-1", id),
          controller.pendingContinuationMessage("session-1"),
        ).messages,
        controller.isArmed("session-1"),
      ).messages,
      streamFn: (_model, context) => {
        requests.push(JSON.stringify(context.messages));
        const stream = createAssistantMessageEventStream();
        stream.push({ type: "start", partial: completed });
        stream.push({ type: "done", reason: "stop", message: completed });
        return stream;
      },
    });
    const hostSession = createPassiveHostSession(agent, source);
    ext.sendMessage.mockImplementation(async (message, options) => {
      await hostSession.sendCustomMessage(message, options);
    });

    agent.state.messages.push(outerResult());
    await runFabricHandoffAtBoundary(
      controller, unusedRunner(), ext.value, pending!, outerResult(), ctx, vi.fn(),
    );
    // Three competing user steers queue after the boundary turn; none of them
    // delays the continuation, which rides the boundary turn's own context.
    for (let i = 0; i < 3; i++) {
      agent.steer({ role: "user", content: `USER-STEER-${i}: redirect step ${i}`, timestamp: 4 });
    }
    await agent.continue();

    // Surface host-shim failures instead of misdiagnosing an aborted loop as
    // changed steering semantics after a Pi upgrade.
    expect(agent.state.error).toBeUndefined();
    // Three legitimate steering turns, with no late Fabric-only continuation.
    expect(requests).toHaveLength(3);
    // The executor's first request already carries task, plan and digest.
    expect(requests[0]).toContain("Implement the guard");
    expect(requests[0]).toContain("EXACT-FIRST-REQUEST-PLAN");
    expect(requests[0]).not.toContain("Prewalk armed →");
    expect(requests[0]).toContain("Plan the guard");
    expect(requests[0]).toContain(`[entry ${thinkingEntryId}]`);
    // Steers are preserved in submission order, and the canonical payload
    // rides every request exactly once — injected until the persisted copy
    // exists, then carried by the transcript itself.
    for (let i = 0; i < 3; i++) {
      expect(requests[i]).toContain(`USER-STEER-${i}`);
    }
    for (const request of requests) {
      expect(request).toContain("Implement the guard");
      expect(request.split("EXACT-FIRST-REQUEST-PLAN").length - 1).toBe(1);
    }
    expect(controller.status()).toMatchObject({ state: "continuation_pending", accepted: true });
    // Injection-time acceptance still settles exactly once, back to Main.
    (ctx as { model?: unknown }).model = kimiModel;
    expect(await settleInPlacePrewalk(controller, ext.value, ctx, { compactOnReturn: false })).toBe(true);
    expect(await settleInPlacePrewalk(controller, ext.value, ctx)).toBe(false);
  });

  it("keeps trajectory handoff opt-in and exposes child activity", async () => {
    const controller = new PrewalkController();
    controller.arm({
      mode: "trajectory",
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "auto");
    expect(pending).toMatchObject({
      kind: "prewalk-trajectory",
      audit: { ref: "agents.handoff" },
    });

    const ctx = context();
    const ext = extension();
    let transferredSeed: unknown;
    const runner = {
      executeHandoff: vi.fn(async (_args, invocation, seed) => {
        transferredSeed = seed;
        invocation.activity?.({
          type: "entity",
          id: "child-1",
          kind: "agent",
          name: "Prewalk trajectory executor",
        });
        invocation.update("Agent Prewalk trajectory executor: running · edit");
        invocation.attachPreview?.({ kind: "fabric-agent-tools" });
        return {
          handedOff: true,
          completed: true,
          status: "completed",
          implementation: "implemented",
          agent: { id: "child-1" },
        };
      }),
    };
    const activity = vi.fn();
    const result = await runFabricHandoffAtBoundary(
      controller,
      runner,
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
      activity,
    );

    expect(ext.setModel).not.toHaveBeenCalled();
    expect(runner.executeHandoff).toHaveBeenCalledWith(
      {
        model: "anthropic/executor",
        name: "Prewalk trajectory executor",
        task: "Implement the guard",
      },
      expect.objectContaining({ parentToolCallId: "outer", activity: expect.any(Function) }),
      expect.any(Object),
    );
    expect(transferredSeed).toMatchObject({
      sourceBranch: [
        { type: "message", message: { role: "user" } },
        { type: "message", message: { role: "assistant" } },
      ],
      outerToolResult: { toolCallId: "outer", toolName: "fabric_exec" },
    });
    expect(activity).toHaveBeenCalledWith(expect.objectContaining({ type: "entity", id: "child-1" }));
    expect(activity).toHaveBeenCalledWith(expect.objectContaining({ type: "progress" }));
    expect(result).toMatchObject({
      prewalk: true,
      mode: "trajectory",
      handedOff: true,
      completed: true,
      implementation: "implemented",
    });
    expect(ext.sendMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        customType: "pi-fabric-prewalk-continue",
        display: false,
        content: expect.stringContaining("do not redo it"),
        details: expect.objectContaining({ mode: "trajectory" }),
      }),
      { deliverAs: "followUp", triggerTurn: true },
    );
    const verifyCall = ext.sendMessage.mock.calls.find(
      ([message]) => message.customType === "pi-fabric-prewalk-continue",
    );
    expect(String(verifyCall?.[0]?.content)).toContain("verbatim");
    expect(ext.sendMessage).not.toHaveBeenCalledWith(
      expect.objectContaining({ customType: "pi-fabric-prewalk-failure" }),
      expect.anything(),
    );
    expect(ctx.setStatus).toHaveBeenLastCalledWith(
      "fabric-prewalk",
      "trajectory executor implemented",
    );
  });

  it("queues a hidden report-and-propose reply after a failed trajectory handoff", async () => {
    const controller = new PrewalkController();
    controller.arm({
      mode: "trajectory",
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "auto");
    const ctx = context();
    const ext = extension();
    const runner = {
      executeHandoff: vi.fn(async () => ({
        handedOff: true,
        completed: false,
        status: "failed",
        error: "child crashed",
      })),
    };
    const result = await runFabricHandoffAtBoundary(
      controller,
      runner,
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );

    expect(result).toMatchObject({ prewalk: true, mode: "trajectory", completed: false });
    expect(ext.sendMessage).not.toHaveBeenCalledWith(
      expect.objectContaining({ customType: "pi-fabric-prewalk-continue" }),
      expect.anything(),
    );
    expect(ext.sendMessage).toHaveBeenCalledTimes(1);
    expect(ext.sendMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        customType: "pi-fabric-prewalk-failure",
        display: false,
        content: expect.stringContaining("without completing"),
        details: expect.objectContaining({
          mode: "trajectory",
          model: "anthropic/executor",
          status: "failed",
          error: "child crashed",
          trigger: "pi.edit",
        }),
      }),
      { deliverAs: "followUp", triggerTurn: true },
    );
  });

  it("queues a hidden failure reply when the trajectory handoff throws", async () => {
    const controller = new PrewalkController();
    controller.arm({
      mode: "trajectory",
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "auto");
    const ctx = context();
    const ext = extension();
    const runner = {
      executeHandoff: vi.fn(async () => {
        throw new Error("child process died");
      }),
    };

    const result = await runFabricHandoffAtBoundary(
      controller,
      runner,
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );

    expect(result).toMatchObject({
      prewalk: true,
      mode: "trajectory",
      status: "failed",
      error: "child process died",
    });
    expect(ext.sendMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        customType: "pi-fabric-prewalk-failure",
        content: expect.stringContaining("at this boundary failed"),
        details: expect.objectContaining({
          mode: "trajectory",
          error: "child process died",
        }),
      }),
      { deliverAs: "followUp", triggerTurn: true },
    );
  });

  it("threads the configured thinking level into the trajectory executor args", async () => {
    const controller = new PrewalkController();
    controller.arm({
      mode: "trajectory",
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
      thinking: "high",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "auto");
    expect(pending!.args).toMatchObject({ model: "anthropic/executor", thinking: "high" });

    const ctx = context();
    const ext = extension();
    let receivedArgs: Record<string, unknown> | undefined;
    const runner = {
      executeHandoff: vi.fn(async (args) => {
        receivedArgs = args;
        return { handedOff: true, completed: true, status: "completed", implementation: "done" };
      }),
    };
    await runFabricHandoffAtBoundary(
      controller,
      runner,
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );
    expect(receivedArgs).toMatchObject({ thinking: "high" });
  });

  it("keeps thinking out of in-place continuation args", () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      thinking: "high",
    });
    const pending = claimHandoff(controller, execution(), "session-1", "auto");
    expect(pending!.kind).toBe("prewalk-in-place");
    expect(pending!.args).not.toHaveProperty("thinking");
  });

  it("preserves the thinking level across a re-armed trajectory handoff", async () => {
    const controller = new PrewalkController();
    controller.arm({
      mode: "trajectory",
      model: "anthropic/executor",
      sessionId: "session-1",
      thinking: "xhigh",
      alwaysRearm: true,
    });
    const pending = claimHandoff(controller, execution(), "session-1", "auto");
    await runFabricHandoffAtBoundary(
      controller,
      { executeHandoff: vi.fn(async () => ({ handedOff: true, completed: true, status: "completed" })) },
      extension().value,
      pending!,
      outerResult(),
      context().value,
    );
    expect(controller.status()).toMatchObject({
      state: "armed",
      thinking: "xhigh",
      alwaysRearm: true,
    });
  });

  it("keeps continuous in-place prewalk pending until its continuation settles", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
      alwaysRearm: true,
    });
    const pending = claimHandoff(controller, execution(), "session-1", "auto");
    const ctx = context();
    await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      extension().value,
      pending!,
      outerResult(),
      ctx.value,
    );

    expect(controller.status()).toMatchObject({
      state: "continuation_pending",
      mode: "in-place",
      model: "anthropic/executor",
      alwaysRearm: true,
      task: "Implement the guard",
    });
    expect(ctx.setStatus).toHaveBeenLastCalledWith(
      "fabric-prewalk",
      "continuing Main → anthropic/executor",
    );
  });

  it.each(["completed", "failed", "stopped", "timed_out", "throw"])(
    "reports an explicit %s handoff visibly and queues Main's conclusion once",
    async (status) => {
      const controller = new PrewalkController();
      controller.arm({ model: "anthropic/automatic", sessionId: "session-1" });
      const run = execution();
      run.handoffRequest = { model: "anthropic/executor", name: "Guard executor" };
      run.audits.push({ ref: "agents.handoff", nestedToolCallId: "explicit", startedAt: 7, args: run.handoffRequest });
      const pending = claimHandoff(controller, run, "session-1", "auto")!;
      expect(pending.kind).toBe("explicit");
      const ext = extension();
      const workerResult = {
        handedOff: true, completed: status === "completed", status,
        agent: { id: "child-1", name: "Guard executor", model: "anthropic/executor" },
        implementation: "Implemented guard. Tests passed. PR https://example.com/pull/42 commit abc123",
        ...(status !== "completed" ? { error: "executor interrupted" } : {}),
      };
      const result = await runFabricHandoffAtBoundary(
        controller,
        { executeHandoff: vi.fn(async () => {
          expect(ext.sendMessage).not.toHaveBeenCalled();
          if (status === "throw") throw new Error("launch failed");
          return workerResult;
        }) },
        ext.value, pending, outerResult(), context().value,
      );
      expect(ext.sendMessage).toHaveBeenCalledTimes(1);
      const [message, options] = ext.sendMessage.mock.calls[0]!;
      expect(options).toEqual({ deliverAs: "followUp", triggerTurn: true });
      expect(message).toMatchObject({
        customType: "pi-fabric-handoff-complete", display: true,
        details: { status: status === "throw" ? "failed" : status },
      });
      expect(message.details.displayText).toContain("Guard executor");
      expect(message.details.displayText).toContain("anthropic/executor");
      expect(message.details.displayText).not.toContain("Reply to the user now");
      expect(message.content).toContain("Reply to the user now");
      expect(message.content).toContain("verbatim");
      if (status === "throw") {
        expect(result).toMatchObject({ completed: false, error: "launch failed" });
        expect(message.details.displayText).toContain("launch failed");
      } else {
        expect(result).toEqual(workerResult);
        expect(message.details.displayText).toContain("https://example.com/pull/42 commit abc123");
      }
      expect(message.content).toContain(status === "completed" ? "Do not redo the work" : "do not retry");
    },
  );

  it("preserves an explicit handoff result when follow-up delivery throws", async () => {
    const controller = new PrewalkController();
    const run = execution();
    run.handoffRequest = { model: "anthropic/executor" };
    run.audits.push({ ref: "agents.handoff", nestedToolCallId: "explicit", startedAt: 7, args: run.handoffRequest });
    const pending = claimHandoff(controller, run, "session-1", "auto")!;
    const ext = extension();
    ext.sendMessage.mockImplementation(() => { throw new Error("queue unavailable"); });
    const workerResult = { completed: true, status: "completed", implementation: "done" };
    const result = await runFabricHandoffAtBoundary(
      controller, { executeHandoff: vi.fn(async () => workerResult) },
      ext.value, pending, outerResult(), context().value,
    );
    expect(result).toEqual(workerResult);
    expect(pending.audit.success).toBe(true);
    expect(ext.sendMessage).toHaveBeenCalledTimes(1);
  });

  it("gives an explicit deferred trajectory request precedence", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/automatic", sessionId: "session-1" });
    const run = execution();
    run.audits.push({
      ref: "agents.handoff",
      nestedToolCallId: "explicit",
      startedAt: 7,
      endedAt: 8,
      success: true,
      args: { model: "anthropic/explicit" },
      result: { status: "deferred" },
    });
    run.handoffRequest = { model: "anthropic/explicit", task: "Use explicit executor" };

    expect(claimHandoff(controller, run, "session-1", "auto")).toMatchObject({
      kind: "explicit",
      args: { model: "anthropic/explicit", task: "Use explicit executor" },
    });
    expect(controller.status()).toEqual({ state: "idle" });
  });

  it("does not claim when the complete execution had no mutation", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });
    const run = execution();
    run.audits = run.audits.slice(0, 1);

    expect(claimHandoff(controller, run, "session-1", "auto")).toBeUndefined();
    expect(controller.isArmed("session-1")).toBe(true);
  });

  it("re-arms after a trajectory handoff when configured", async () => {
    const controller = new PrewalkController();
    controller.arm({
      mode: "trajectory",
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
      alwaysRearm: true,
    });
    const pending = claimHandoff(controller, execution(), "session-1", "auto");
    expect(pending).toMatchObject({ kind: "prewalk-trajectory" });

    const ctx = context();
    const runner = {
      executeHandoff: vi.fn(async () => ({
        handedOff: true,
        completed: true,
        status: "completed",
        implementation: "implemented",
      })),
    };
    const result = await runFabricHandoffAtBoundary(
      controller,
      runner,
      extension().value,
      pending!,
      outerResult(),
      ctx.value,
    );

    expect(result).toMatchObject({
      prewalk: true,
      mode: "trajectory",
      completed: true,
      implementation: "implemented",
    });
    expect(controller.status()).toMatchObject({
      state: "armed",
      mode: "trajectory",
      model: "anthropic/executor",
      alwaysRearm: true,
    });
    expect(controller.status()).not.toHaveProperty("task");
    expect(ctx.setStatus).toHaveBeenLastCalledWith(
      "fabric-prewalk",
      "armed → anthropic/executor",
    );
    expect(
      withTrajectoryRearmDirective("outer output", pending!, result, controller, "session-1"),
    ).toContain("Prewalk re-armed");
  });
});

describe("prewalkArmedPrompt", () => {
  it("describes the trajectory boundary for Main", () => {
    const text = prewalkArmedPrompt("trajectory", "anthropic/executor");
    expect(text).toContain("anthropic/executor (trajectory)");
    expect(text).toContain("pi.edit / pi.write / schema.commit");
    expect(text).toContain("the executor takes over the requested work there, and a hidden follow-up asks you to verify its work and summarize when it finishes.");
    expect(text).toContain("prewalk.plan(");
  });

  it("describes in-place continuation for Main", () => {
    const text = prewalkArmedPrompt("in-place", "anthropic/executor");
    expect(text).toContain("this session switches to anthropic/executor and keeps working.");
    expect(text).not.toContain("hidden follow-up asks you to verify");
  });
});

describe("hasPrewalkArmedPrompt", () => {
  it("matches persisted armed prompts by content only", () => {
    const armed = prewalkArmedPrompt("trajectory", "anthropic/executor");
    const entries = [
      { type: "message", message: { role: "user" } },
      {
        type: "custom_message",
        customType: PREWALK_ARMED_MESSAGE_TYPE,
        content: [{ type: "text", text: armed }],
      },
      { type: "custom_message", customType: "other-extension", content: armed },
    ];
    expect(hasPrewalkArmedPrompt(entries, armed)).toBe(true);
    expect(hasPrewalkArmedPrompt(entries, prewalkArmedPrompt("in-place", "other/model"))).toBe(false);
    expect(hasPrewalkArmedPrompt([], armed)).toBe(false);
  });

  it("accepts string content and ignores malformed entries", () => {
    const entries = [
      { type: "custom_message", customType: PREWALK_ARMED_MESSAGE_TYPE, content: "plain" },
      null,
      42,
    ];
    expect(hasPrewalkArmedPrompt(entries, "plain")).toBe(true);
    expect(hasPrewalkArmedPrompt(entries, "other")).toBe(false);
  });
});

describe("withTrajectoryRearmDirective", () => {
  const trajectoryPending = (alwaysRearm: boolean) => {
    const controller = new PrewalkController();
    controller.arm({
      mode: "trajectory",
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement",
      alwaysRearm,
    });
    const pending = claimHandoff(controller, execution(), "session-1", "auto")!;
    return { controller, pending };
  };

  it("appends the directive after a completed trajectory handoff when re-armed", () => {
    const { controller, pending } = trajectoryPending(true);
    controller.completeTask(); // boundary finally re-arms
    const text = withTrajectoryRearmDirective("OUTPUT", pending, { completed: true }, controller, "session-1");
    expect(text.startsWith("OUTPUT\n\n")).toBe(true);
    expect(text).toContain("result above is final");
    expect(text).toContain("pi.edit / pi.write or shell file changes in fabric_exec to hand off again");
    expect(text).toContain("keep any fixes scoped to what verification fails.");
  });

  it("omits the directive for in-place pendings", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });
    const pending = claimHandoff(controller, execution(), "session-1", "auto")!;
    expect(pending.kind).toBe("prewalk-in-place");
    controller.completeTask();
    expect(withTrajectoryRearmDirective("OUTPUT", pending, { completed: true }, controller, "session-1")).toBe("OUTPUT");
  });

  it("omits the directive when the handoff failed", () => {
    const { controller, pending } = trajectoryPending(true);
    controller.completeTask();
    expect(withTrajectoryRearmDirective("OUTPUT", pending, { completed: false }, controller, "session-1")).toBe("OUTPUT");
  });

  it("omits the directive when the arm was one-shot", () => {
    const { controller, pending } = trajectoryPending(false);
    controller.completeTask(); // no alwaysRearm -> idle
    expect(controller.status()).toEqual({ state: "idle" });
    expect(withTrajectoryRearmDirective("OUTPUT", pending, { completed: true }, controller, "session-1")).toBe("OUTPUT");
  });

  it("omits the directive when the arm belongs to another session", () => {
    const { controller, pending } = trajectoryPending(true);
    controller.completeTask();
    expect(withTrajectoryRearmDirective("OUTPUT", pending, { completed: true }, controller, "session-2")).toBe("OUTPUT");
  });
});

describe("filesystem-drift prewalk claims", () => {
  it("claims shell-write drift with trigger files for an in-place continuation", async () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      sessionId: "session-1",
      task: "Implement the guard",
    });
    const run = bashExecution();
    const pending = claimDriftHandoff(
      controller,
      run,
      "session-1",
      { files: ["src/guard.ts", "docs/guard.md"], truncated: 3, added: 1, modified: 1, deleted: 0, unchanged: 0 },
      "json",
    );

    expect(run.audits.map((audit) => audit.ref)).toEqual(["pi.bash", "fabric.prewalk"]);
    expect(pending).toMatchObject({
      kind: "prewalk-in-place",
      args: { model: "anthropic/executor", task: "Implement the guard" },
      audit: { args: { seq: 1 } },
      triggerRef: "fs.drift",
      triggerSeq: 1,
      triggerFiles: ["src/guard.ts", "docs/guard.md"],
      triggerFilesTruncated: 3,
    });

    const ctx = context();
    const ext = extension();
    const result = await runFabricHandoffAtBoundary(
      controller,
      unusedRunner(),
      ext.value,
      pending!,
      outerResult(),
      ctx.value,
    );

    expect(result).toMatchObject({
      prewalk: true,
      mode: "in-place",
      continued: true,
      trigger: {
        ref: "fs.drift",
        seq: 1,
        files: ["src/guard.ts", "docs/guard.md"],
        truncated: 3,
      },
    });
    expect(controller.status()).toMatchObject({ state: "continuation_pending" });
  });

  it("claims shell-write drift as a trajectory child in trajectory mode", () => {
    const controller = new PrewalkController();
    controller.arm({
      model: "anthropic/executor",
      mode: "trajectory",
      sessionId: "session-1",
    });
    const run = bashExecution();
    const pending = claimDriftHandoff(
      controller,
      run,
      "session-1",
      { files: ["src/guard.ts"], truncated: 0, added: 0, modified: 1, deleted: 0, unchanged: 0 },
      "json",
    );

    expect(pending).toMatchObject({
      kind: "prewalk-trajectory",
      audit: { ref: "agents.handoff" },
      triggerRef: "fs.drift",
      triggerFiles: ["src/guard.ts"],
    });
    expect(controller.status()).toMatchObject({ state: "handing_off", mode: "trajectory" });
  });

  it("refuses drift claims for a disarmed or foreign session", () => {
    const controller = new PrewalkController();
    expect(
      claimDriftHandoff(
        controller,
        bashExecution(),
        "session-1",
        { files: ["a.ts"], truncated: 0, added: 0, modified: 1, deleted: 0, unchanged: 0 },
        "json",
      ),
    ).toBeUndefined();

    controller.arm({ model: "anthropic/executor", sessionId: "session-1" });
    expect(
      claimDriftHandoff(
        controller,
        bashExecution(),
        "session-2",
        { files: ["a.ts"], truncated: 0, added: 0, modified: 1, deleted: 0, unchanged: 0 },
        "json",
      ),
    ).toBeUndefined();
    expect(controller.status()).toMatchObject({ state: "armed" });
  });
});

describe("prewalk plan checkpoint", () => {
  const plan = {
    outcome: "Review only: do not edit source",
    steps: ["Inspect the parser and report the mismatch EXACT-PLAN-571"],
    verification: ["Read the parser's callers"],
    risks: "Preserve unrelated work",
  };

  it.each(["in-place", "trajectory"] as const)("delivers the plan independently of the nested return (%s)", async (mode) => {
    const controller = new PrewalkController();
    controller.arm({ mode, model: "anthropic/executor", sessionId: "session-1", requirePlan: true, task: "Review only" });
    const ctx = context();
    const ext = extension();
    await new PrewalkProvider(controller).invoke("plan", plan, {
      extensionContext: ctx.value, update() {},
    } as unknown as FabricInvocationContext); // Deliberately discard the return.
    const pending = claimHandoff(controller, execution(), "session-1", "json")!;
    const runner = { executeHandoff: vi.fn().mockResolvedValue({ completed: true, status: "completed" }) };
    await runFabricHandoffAtBoundary(controller, runner, ext.value, pending, outerResult(), ctx.value);
    const delivered = mode === "in-place"
      ? String(ext.sendMessage.mock.calls.find(([message]) => message.customType === "pi-fabric-prewalk-continue")?.[0].content)
      : String(runner.executeHandoff.mock.calls[0]?.[0].task);
    expect(delivered).toContain(prewalkPlanText(plan));
  });

  it.each(["planned", "disabled", "unplanned"] as const)("warns only for a claimed unplanned fallback (%s)", async (kind) => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: kind !== "disabled" });
    if (kind === "planned") controller.submitPlan("session-1", plan);
    if (kind === "unplanned") {
      claimFabricHandoff(controller, execution(), "session-1", "auto");
      claimFabricHandoff(controller, execution(), "session-1", "auto");
    }
    const ctx = context();
    const pending = new Map<string, PendingFabricHandoff>();
    const state = {
      config: normalizeFabricConfig({}), prewalk: controller,
      ensure: async () => {}, execution: { execute: async () => execution() },
      claimHandoff: async (run: FabricExecutionResult) => claimHandoff(controller, run, "session-1", "auto"),
    } as unknown as FabricState;
    const tool = createFabricExecTool(state, defaultCodePreviewSettings(), pending, (value) => value);
    await tool.execute("outer", { code: "return 1" }, undefined, undefined, ctx.value);
    if (kind === "unplanned") {
      expect(ctx.value.ui.notify).toHaveBeenCalledWith(expect.stringContaining("without a recorded plan after 2 reminders"), "warning");
    } else expect(ctx.value.ui.notify).not.toHaveBeenCalled();
    expect(pending.get("outer")?.audit.args).toMatchObject({ readiness: kind });
  });

  it("returns a checkpoint instead of a handoff on the first mutation", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    const run = execution();

    const outcome = claimFabricHandoff(controller, run, "session-1", "auto");
    expect(outcome).toMatchObject({ kind: "prewalk-plan", mutation: { ref: "pi.edit" } });
    // No handoff audit is appended: the boundary stays an ordinary tool result.
    expect(run.audits.map((entry) => entry.ref)).toEqual(["pi.read", "pi.edit", "pi.write"]);
    expect(controller.status()).toMatchObject({ state: "armed" });
    // The gate stays closed until a plan is recorded: the next boundary asks
    // again instead of handing off.
    expect(controller.planCheckpointRequired("session-1")).toBe(true);
    expect(claimFabricHandoff(controller, run, "session-1", "auto")).toMatchObject({
      kind: "prewalk-plan",
    });
  });

  it("delivers the checkpoint as a hidden steer that triggers a turn", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    const outcome = claimFabricHandoff(controller, execution(), "session-1", "auto");
    if (!outcome || outcome.kind !== "prewalk-plan") throw new Error("expected a plan checkpoint");
    const sendMessage = vi.fn();

    expect(
      deliverPrewalkPlanCheckpoint({ sendMessage } as unknown as ExtensionAPI, outcome),
    ).toBe(true);
    const [message, options] = sendMessage.mock.calls[0]!;
    expect(message).toMatchObject({ customType: PREWALK_PLAN_MESSAGE_TYPE, display: false });
    expect(String(message.content)).toContain("anthropic/executor");
    expect(String(message.content)).toContain("Raw reasoning replay is not guaranteed");
    expect(String(message.content)).toContain("bounded advisory digest");
    expect(String(message.content)).toContain("does not replace the explicit plan");
    expect(String(message.content)).not.toContain("your reasoning does not transfer");
    expect(options).toEqual({ deliverAs: "steer", triggerTurn: true });
  });

  it("reports a failed delivery so the caller can reopen the checkpoint", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    const outcome = claimFabricHandoff(controller, execution(), "session-1", "auto");
    if (!outcome || outcome.kind !== "prewalk-plan") throw new Error("expected a plan checkpoint");
    const sendMessage = vi.fn(() => {
      throw new Error("host rejected the message");
    });

    expect(
      deliverPrewalkPlanCheckpoint({ sendMessage } as unknown as ExtensionAPI, outcome),
    ).toBe(false);
  });

  it("gates filesystem drift boundaries through the same checkpoint", () => {
    const controller = new PrewalkController();
    controller.arm({ model: "anthropic/executor", sessionId: "session-1", requirePlan: true });
    const run = bashExecution();

    const outcome = claimFabricFsDriftHandoff(
      controller,
      run,
      "session-1",
      { files: ["src/guard.ts"], truncated: 0, added: 0, modified: 1, deleted: 0, unchanged: 0 },
      "json",
    );
    expect(outcome).toMatchObject({ kind: "prewalk-plan", mutation: { ref: "fs.drift" } });
    expect(run.audits.map((entry) => entry.ref)).toEqual(["pi.bash"]);
  });
});

describe("passive host session compatibility guard", () => {
  it("fails before subscribing when the installed host renamed a required member", async () => {
    const { AgentSession } = await import("@earendil-works/pi-coding-agent");
    const saved = Object.getOwnPropertyDescriptor(AgentSession.prototype, "sendCustomMessage");
    if (!saved) throw new Error("expected sendCustomMessage on the installed host prototype");
    const subscribe = vi.fn();
    try {
      Object.defineProperty(AgentSession.prototype, "sendCustomMessage", {
        value: undefined,
        configurable: true,
        writable: true,
      });
      expect(() =>
        createPassiveHostSession({ subscribe } as unknown as Agent, {} as unknown as SessionManager),
      ).toThrow(/sendCustomMessage/);
      expect(subscribe).not.toHaveBeenCalled();
    } finally {
      Object.defineProperty(AgentSession.prototype, "sendCustomMessage", saved);
    }
  });
});
