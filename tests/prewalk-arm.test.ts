import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricPrewalkMode } from "../src/config.js";
import type { FabricState } from "../src/fabric-state.js";
import { armFabricPrewalkSession, autoArmFabricPrewalk } from "../src/prewalk/arm.js";
import { PrewalkController } from "../src/prewalk/controller.js";
import { PREWALK_ARMED_MESSAGE_TYPE, prewalkArmedPrompt } from "../src/prewalk/messages.js";
import type { FabricThinking } from "../src/thinking.js";

const CWD = "/tmp/fabric-prewalk-arm-test";

beforeEach(() => {
  vi.stubEnv("PI_FABRIC_PARENT_RUN", undefined);
  vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", undefined);
});
afterEach(() => vi.unstubAllEnvs());

interface Harness {
  state: FabricState;
  context: ExtensionContext;
  pi: ExtensionAPI;
  prewalk: PrewalkController;
  captureBaseline: ReturnType<typeof vi.fn>;
  sendMessage: ReturnType<typeof vi.fn>;
  setStatus: ReturnType<typeof vi.fn>;
}

const makeHarness = (
  input: {
    alwaysRearm?: boolean;
    mode?: FabricPrewalkMode;
    model?: string;
    thinking?: FabricThinking;
    detectShellWrites?: boolean;
    requirePlan?: boolean;
    enabled?: boolean;
    fullCodeMode?: boolean;
    schemaMode?: string;
    agentsEnabled?: boolean;
    branch?: unknown[];
  } = {},
): Harness => {
  const prewalk = new PrewalkController();
  const captureBaseline = vi.fn(async () => {});
  const sendMessage = vi.fn();
  const setStatus = vi.fn();
  const branch = input.branch ?? [];
  const state = {
    config: {
      fullCodeMode: input.fullCodeMode ?? true,
      schema: { mode: input.schemaMode ?? "assist" },
      agents: { enabled: input.agentsEnabled ?? true },
      prewalk: {
        ...(input.enabled === false ? { enabled: false } : {}),
        mode: input.mode ?? "in-place",
        ...(input.model !== undefined ? { model: input.model } : {}),
        ...(input.thinking !== undefined ? { thinking: input.thinking } : {}),
        alwaysRearm: input.alwaysRearm ?? true,
        detectShellWrites: input.detectShellWrites ?? true,
        requirePlan: input.requirePlan ?? true,
      },
    },
    prewalk,
    prewalkDrift: { captureBaseline },
  } as unknown as FabricState;
  const context = {
    cwd: CWD,
    hasUI: true,
    sessionManager: {
      getSessionId: () => "session-1",
      getBranch: () => branch,
    },
    ui: { setStatus, notify: vi.fn() },
  } as unknown as ExtensionContext;
  const pi = { sendMessage } as unknown as ExtensionAPI;
  return { state, context, pi, prewalk, captureBaseline, sendMessage, setStatus };
};

describe("armFabricPrewalkSession", () => {
  it.each(["manual", "auto"] as const)("does not demand a rejected plan when the gate is disabled (%s)", async (entry) => {
    const h = makeHarness({ model: "anthropic/executor", requirePlan: false });
    if (entry === "auto") await autoArmFabricPrewalk(h.state, h.context, h.pi);
    else await armFabricPrewalkSession(h.state, h.context, h.pi, { model: "anthropic/executor" });
    expect(h.prewalk.planRequired("session-1")).toBe(false);
    expect(h.sendMessage.mock.calls[0]?.[0].content).not.toContain("prewalk.plan(");
    expect(h.sendMessage.mock.calls[0]?.[0].content).toContain("first successful");
  });

  it("arms from the live config and mirrors arm-time side effects", async () => {
    const h = makeHarness({ model: "anthropic/executor", thinking: "high" });

    await armFabricPrewalkSession(h.state, h.context, h.pi, {
      model: "anthropic/executor",
      task: "  draft the guard  ",
    });

    expect(h.prewalk.status()).toMatchObject({
      state: "armed",
      mode: "in-place",
      model: "anthropic/executor",
      sessionId: "session-1",
      alwaysRearm: true,
      task: "draft the guard",
      thinking: "high",
    });
    expect(h.captureBaseline).toHaveBeenCalledWith("session-1", CWD);
    expect(h.sendMessage).toHaveBeenCalledTimes(1);
    expect(h.sendMessage.mock.calls[0]?.[0]).toMatchObject({
      customType: PREWALK_ARMED_MESSAGE_TYPE,
      display: false,
      content: prewalkArmedPrompt("in-place", "anthropic/executor"),
    });
    expect(h.sendMessage.mock.calls[0]?.[1]).toEqual({ deliverAs: "nextTurn" });
    expect(h.setStatus).toHaveBeenCalledWith(
      "fabric-prewalk",
      "armed (in-place) → anthropic/executor",
    );
  });

  it("queues the armed advisory only once per identical prompt", async () => {
    const h = makeHarness({
      branch: [
        {
          type: "custom_message",
          customType: PREWALK_ARMED_MESSAGE_TYPE,
          content: prewalkArmedPrompt("in-place", "anthropic/executor"),
        },
      ],
    });

    await armFabricPrewalkSession(h.state, h.context, h.pi, { model: "anthropic/executor" });

    expect(h.prewalk.status().state).toBe("armed");
    expect(h.sendMessage).not.toHaveBeenCalled();
  });

  it("skips the drift baseline when shell-write detection is off", async () => {
    const h = makeHarness({ detectShellWrites: false });

    await armFabricPrewalkSession(h.state, h.context, h.pi, { model: "anthropic/executor" });

    expect(h.captureBaseline).not.toHaveBeenCalled();
    expect(h.prewalk.status().state).toBe("armed");
  });
});

describe("autoArmFabricPrewalk", () => {
  it("stays silent when the prewalk master switch is off", async () => {
    const h = makeHarness({ model: "anthropic/executor", enabled: false });

    const skip = await autoArmFabricPrewalk(h.state, h.context, h.pi);

    expect(skip).toBeUndefined();
    expect(h.prewalk.status().state).toBe("idle");
  });

  it.each(["in-place", "trajectory"] as const)("arms new %s Main sessions from prewalk.model", async (mode) => {
    const h = makeHarness({ model: "anthropic/executor", mode });

    const skip = await autoArmFabricPrewalk(h.state, h.context, h.pi);

    expect(skip).toBeUndefined();
    expect(h.prewalk.status()).toMatchObject({
      state: "armed",
      mode,
      model: "anthropic/executor",
      sessionId: "session-1",
      alwaysRearm: true,
    });
    expect(h.captureBaseline).toHaveBeenCalledWith("session-1", CWD);
    expect(h.sendMessage).toHaveBeenCalledTimes(1);
  });

  describe.each(["in-place", "trajectory"] as const)("%s participant isolation", (mode) => {
    it.each([
      { name: "child executor", parentRun: "child-run", actorId: undefined, depth: "1" },
      { name: "nested executor", parentRun: "nested-run", actorId: undefined, depth: "2" },
      { name: "actor", parentRun: "actor-run", actorId: "actor-1", depth: "1" },
      { name: "standalone actor", parentRun: undefined, actorId: "actor-1", depth: "0" },
    ])("never auto-arms a $name or claims its writes", async ({ parentRun, actorId, depth }) => {
      vi.stubEnv("PI_FABRIC_PARENT_RUN", parentRun);
      vi.stubEnv("PI_FABRIC_ACTOR_ID", actorId);
      vi.stubEnv("PI_FABRIC_DEPTH", depth);
      const h = makeHarness({ model: "anthropic/executor", mode });

      // Startup and reload share this path; neither may arm a participant.
      for (let activation = 0; activation < 2; activation++) {
        expect(await autoArmFabricPrewalk(h.state, h.context, h.pi)).toBeUndefined();
        expect(h.prewalk.status().state).toBe("idle");
      }
      expect(h.prewalk.claim([
        { ref: "pi.write", nestedToolCallId: "first-write", startedAt: 1, success: true },
      ], "session-1")).toBeUndefined();
      expect(h.prewalk.claimFsDrift("session-1", ["edited.ts"])).toBeUndefined();
      expect(h.sendMessage).not.toHaveBeenCalled();
      expect(h.captureBaseline).not.toHaveBeenCalled();
      expect(h.setStatus).not.toHaveBeenCalled();
    });
  });

  it("still auto-arms Main without a UI", async () => {
    const h = makeHarness({ model: "anthropic/executor", mode: "trajectory" });

    await autoArmFabricPrewalk(h.state, { ...h.context, hasUI: false }, h.pi);

    expect(h.prewalk.status()).toMatchObject({ state: "armed", mode: "trajectory" });
  });

  it("preserves explicitly armed participants", async () => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "child-run");
    const h = makeHarness({ model: "anthropic/executor", mode: "trajectory" });
    await armFabricPrewalkSession(h.state, h.context, h.pi, { model: "openai/manual" });
    const armed = h.prewalk.status();

    expect(await autoArmFabricPrewalk(h.state, h.context, h.pi)).toBeUndefined();

    expect(h.prewalk.status()).toEqual(armed);
    expect(h.sendMessage).toHaveBeenCalledTimes(1);
    expect(h.captureBaseline).toHaveBeenCalledTimes(1);
    expect(h.setStatus).toHaveBeenCalledTimes(1);
  });

  it("does not warn participants about Main's auto-arm prerequisites", async () => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "child-run");
    const h = makeHarness({ fullCodeMode: false });

    expect(await autoArmFabricPrewalk(h.state, h.context, h.pi)).toBeUndefined();

    expect(h.prewalk.status().state).toBe("idle");
    expect(h.sendMessage).not.toHaveBeenCalled();
  });

  it("skips auto-arm while Main is still on the executor after a failed return, and arms once Main is restored", async () => {
    const h = makeHarness({ model: "anthropic/executor" });
    h.prewalk.arm({ model: "anthropic/executor", sessionId: "session-1", alwaysRearm: true });
    h.prewalk.claim([
      { ref: "pi.edit", nestedToolCallId: "edit-1", startedAt: 1, success: true },
    ], "session-1");
    h.prewalk.beginContinuation("cont-1", "anthropic/frontier");
    h.prewalk.acceptContinuation("session-1", "cont-1");
    // The return to Main failed: the arm is cancelled, the borrowed record survives.
    h.prewalk.cancel();
    expect(h.prewalk.status().state).toBe("idle");
    expect(h.prewalk.borrowedReturn()).toEqual({
      returnModel: "anthropic/frontier",
      executorModel: "anthropic/executor",
    });

    const onExecutor = {
      ...h.context,
      model: { provider: "anthropic", id: "executor" },
    } as unknown as ExtensionContext;
    const skip = await autoArmFabricPrewalk(h.state, onExecutor, h.pi);

    expect(skip).toContain("failed in-place return");
    expect(h.prewalk.status().state).toBe("idle");
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(h.captureBaseline).not.toHaveBeenCalled();

    // Once restoreBorrowedInPlaceMain has put Main back, auto-arm resumes.
    const restored = {
      ...h.context,
      model: { provider: "anthropic", id: "frontier" },
    } as unknown as ExtensionContext;
    expect(await autoArmFabricPrewalk(h.state, restored, h.pi)).toBeUndefined();
    expect(h.prewalk.status()).toMatchObject({
      state: "armed",
      model: "anthropic/executor",
      sessionId: "session-1",
    });
  });

  it("stays silent when always re-arm is off", async () => {
    const h = makeHarness({ alwaysRearm: false, model: "anthropic/executor" });

    const skip = await autoArmFabricPrewalk(h.state, h.context, h.pi);

    expect(skip).toBeUndefined();
    expect(h.prewalk.status().state).toBe("idle");
    expect(h.sendMessage).not.toHaveBeenCalled();
  });

  it("never clobbers an existing arm", async () => {
    const h = makeHarness({ model: "anthropic/executor" });
    h.prewalk.arm({ model: "openai/manual", sessionId: "session-1", alwaysRearm: true });

    const skip = await autoArmFabricPrewalk(h.state, h.context, h.pi);

    expect(skip).toBeUndefined();
    expect(h.prewalk.status().state).toBe("armed");
    expect(h.prewalk.status()).toMatchObject({ model: "openai/manual" });
    expect(h.sendMessage).not.toHaveBeenCalled();
  });

  it.each([
    {
      name: "full code mode disabled",
      harness: { model: "anthropic/executor", fullCodeMode: false },
      reason: "full code mode",
    },
    {
      name: "Schema enforce mode on",
      harness: { model: "anthropic/executor", schemaMode: "enforce" },
      reason: "Schema enforce mode",
    },
    {
      name: "trajectory without agents",
      harness: { model: "anthropic/executor", mode: "trajectory" as const, agentsEnabled: false },
      reason: "agents.enabled",
    },
    {
      name: "prewalk.model unset",
      harness: {},
      reason: "prewalk.model",
    },
    {
      name: "prewalk.model not provider/model",
      harness: { model: "plain-model" },
      reason: "prewalk.model",
    },
  ])("returns a skip reason when $name", async ({ harness, reason }) => {
    const h = makeHarness(harness);

    const skip = await autoArmFabricPrewalk(h.state, h.context, h.pi);

    expect(skip).toContain(reason);
    expect(h.prewalk.status().state).toBe("idle");
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(h.captureBaseline).not.toHaveBeenCalled();
  });
});
