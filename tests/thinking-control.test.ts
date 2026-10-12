import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { removeTree } from "../src/agents/rm.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG, loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { ThinkingProvider } from "../src/providers/thinking-provider.js";
import {
  childThinkingBounds,
  clampThinkingToBounds,
  inheritedThinkingBounds,
  intersectThinkingBounds,
  modelThinkingLevels,
  normalizeThinkingBounds,
  selectThinkingLevel,
  type FabricThinking,
  type FabricThinkingBounds,
} from "../src/thinking.js";
import { FABRIC_THINKING_ENTRY_TYPE, FabricThinkingController } from "../src/thinking-control.js";
import { parseWorkerOptions } from "../src/worker/options.js";

const reasoningModel = { reasoning: true, thinkingLevelMap: { xhigh: "xhigh" } };

interface Entry { type: string; customType?: string; data?: unknown }

const fakeHost = (initial: FabricThinking = "medium", supported?: FabricThinking[]) => {
  const entries: Entry[] = [];
  const host = {
    level: initial as FabricThinking,
    entries,
    getThinkingLevel: vi.fn(() => host.level),
    setThinkingLevel: vi.fn((level: FabricThinking) => {
      // Pi clamps to model capability; emulate with the same fallback.
      host.level = supported ? (selectThinkingLevel(level, supported) ?? "off") : level;
    }),
    appendEntry: vi.fn((customType: string, data?: unknown) => {
      entries.push({ type: "custom", customType, data });
    }),
  };
  return host;
};

const contextFor = (host: ReturnType<typeof fakeHost>, sessionId = "session-a", model: unknown = reasoningModel) => ({
  model,
  sessionManager: {
    getSessionId: () => sessionId,
    getBranch: () => host.entries,
  },
}) as never;

const controller = (host: ReturnType<typeof fakeHost>, bounds: FabricThinkingBounds = {}) =>
  new FabricThinkingController(host as never, () => bounds);

describe("thinking bounds helpers", () => {
  it("validates bounds fail-closed", () => {
    expect(normalizeThinkingBounds(undefined, "b")).toEqual({});
    expect(normalizeThinkingBounds({ min: "low", max: "high" }, "b")).toEqual({ min: "low", max: "high" });
    expect(() => normalizeThinkingBounds({ max: "ultra" }, "b")).toThrow("b.max must be one of");
    expect(() => normalizeThinkingBounds({ min: "high", max: "low" }, "b")).toThrow("must not exceed");
    expect(() => normalizeThinkingBounds({ min: "low", ceiling: "high" }, "b")).toThrow("accepts only min and max");
    expect(() => normalizeThinkingBounds("high", "b")).toThrow("must be an object");
  });

  it("intersects without ever widening; disjoint ranges keep the parent", () => {
    expect(intersectThinkingBounds({ max: "high" }, { min: "low", max: "max" })).toEqual({ min: "low", max: "high" });
    expect(intersectThinkingBounds({ min: "low" }, {})).toEqual({ min: "low" });
    expect(intersectThinkingBounds({ max: "low" }, { min: "high" })).toEqual({ max: "low" });
  });

  it("requires child bounds inside the parent and inherits omitted ends", () => {
    expect(childThinkingBounds({ max: "high" }, undefined)).toEqual({ max: "high" });
    expect(childThinkingBounds({ max: "high" }, { min: "low" })).toEqual({ min: "low", max: "high" });
    expect(() => childThinkingBounds({ max: "high" }, { max: "xhigh" })).toThrow("must lie inside");
    expect(() => childThinkingBounds({ min: "low" }, { min: "off" })).toThrow("must lie inside");
  });

  it("clamps and selects like pi-ai, restricted to allowed levels", () => {
    expect(clampThinkingToBounds("max", { max: "high" })).toBe("high");
    expect(clampThinkingToBounds("off", { min: "low" })).toBe("low");
    expect(selectThinkingLevel("low", ["off", "high"])).toBe("high");
    expect(selectThinkingLevel("max", ["off", "medium"])).toBe("medium");
    expect(selectThinkingLevel("max", [])).toBeUndefined();
    expect(modelThinkingLevels(undefined)).toHaveLength(7);
    expect(modelThinkingLevels({ reasoning: false })).toEqual(["off"]);
    expect(modelThinkingLevels({ reasoning: true, thinkingLevelMap: { minimal: null } }))
      .toEqual(["off", "low", "medium", "high"]);
  });

  it("parses inherited bounds and rejects malformed values", () => {
    expect(inheritedThinkingBounds({})).toBeUndefined();
    expect(inheritedThinkingBounds({ PI_FABRIC_THINKING_BOUNDS: '{"max":"high"}' })).toEqual({ max: "high" });
    expect(() => inheritedThinkingBounds({ PI_FABRIC_THINKING_BOUNDS: "high" })).toThrow("Invalid PI_FABRIC_THINKING_BOUNDS");
    expect(() => inheritedThinkingBounds({ PI_FABRIC_THINKING_BOUNDS: '{"max":"ultra"}' })).toThrow("max must be one of");
  });
});

describe("FabricThinkingController", () => {
  it("reports level, model levels inside bounds, and the baseline", () => {
    const host = fakeHost("medium");
    const status = controller(host, { max: "high" }).status(contextFor(host));
    expect(status).toEqual({
      level: "medium",
      available: ["off", "minimal", "low", "medium", "high"],
      bounds: { min: "off", max: "high" },
      baseline: "medium",
    });
  });

  it("applies a turn override and reverts it at the next agent_end", () => {
    const host = fakeHost("medium");
    const thinking = controller(host);
    const result = thinking.set({ level: "high", reason: "hard proof" }, contextFor(host));
    expect(result).toMatchObject({ level: "high", baseline: "medium", override: { level: "high", scope: "turn", reason: "hard proof" } });
    expect(result.clamped).toBeUndefined();
    expect(host.entries.at(-1)).toMatchObject({ customType: FABRIC_THINKING_ENTRY_TYPE, data: { version: 1, override: { baseline: "medium" } } });
    thinking.agentEnded(contextFor(host));
    expect(host.level).toBe("medium");
    expect(thinking.status(contextFor(host)).override).toBeUndefined();
    expect(host.entries.at(-1)).toMatchObject({ data: { version: 1, override: null } });
  });

  it("counts down turns scope and keeps session scope until reset", () => {
    const host = fakeHost("low");
    const thinking = controller(host);
    thinking.set({ level: "high", scope: "turns", turns: 2 }, contextFor(host));
    thinking.agentEnded(contextFor(host));
    expect(thinking.status(contextFor(host)).override).toMatchObject({ remainingTurns: 1 });
    expect(host.level).toBe("high");
    thinking.agentEnded(contextFor(host));
    expect(host.level).toBe("low");

    thinking.set({ level: "minimal", scope: "session" }, contextFor(host));
    thinking.agentEnded(contextFor(host));
    thinking.agentEnded(contextFor(host));
    expect(host.level).toBe("minimal");
    // Re-setting keeps the original baseline.
    thinking.set({ level: "medium", scope: "session" }, contextFor(host));
    expect(thinking.reset(contextFor(host))).toMatchObject({ level: "low", baseline: "low" });
    expect(thinking.status(contextFor(host)).override).toBeUndefined();
  });

  it("clamps into bounds and model support and reports the request", () => {
    const host = fakeHost("medium", ["off", "low", "medium", "high"]);
    const thinking = controller(host, { min: "low", max: "high" });
    const model = { reasoning: true };
    expect(thinking.set({ level: "max" }, contextFor(host, "s", model))).toMatchObject({ level: "high", clamped: true, requested: "max" });
    expect(thinking.set({ level: "off" }, contextFor(host, "s", model))).toMatchObject({ level: "low", clamped: true, requested: "off" });
    expect(() => thinking.set({ level: "high" }, contextFor(host, "s", { reasoning: false })))
      .toThrow("No thinking level supported by the active model");
  });

  it("validates scope/turns/reason combinations", () => {
    const host = fakeHost();
    const thinking = controller(host);
    expect(() => thinking.set({ level: "high", scope: "turns" }, contextFor(host))).toThrow("requires turns");
    expect(() => thinking.set({ level: "high", scope: "turns", turns: 21 }, contextFor(host))).toThrow("requires turns");
    expect(() => thinking.set({ level: "high", turns: 2 }, contextFor(host))).toThrow('only valid with scope "turns"');
    expect(() => thinking.set({ level: "ultra" as FabricThinking }, contextFor(host))).toThrow("Unknown thinking level");
    expect(() => thinking.set({ level: "high", reason: "x".repeat(257) }, contextFor(host))).toThrow("at most 256");
    expect(host.setThinkingLevel).not.toHaveBeenCalled();
  });

  it("never fights a level someone else changed during the override", () => {
    const host = fakeHost("medium");
    const thinking = controller(host);
    thinking.set({ level: "high" }, contextFor(host));
    host.level = "minimal";
    host.setThinkingLevel.mockClear();
    thinking.agentEnded(contextFor(host));
    expect(host.setThinkingLevel).not.toHaveBeenCalled();
    expect(host.level).toBe("minimal");
    expect(thinking.status(contextFor(host)).override).toBeUndefined();
  });

  it("restores the latest persisted override after reload and per session", () => {
    const host = fakeHost("medium");
    controller(host).set({ level: "xhigh", scope: "turns", turns: 3 }, contextFor(host));
    const reloaded = controller(host);
    expect(reloaded.status(contextFor(host))).toMatchObject({
      level: "xhigh", baseline: "medium", override: { scope: "turns", remainingTurns: 3 },
    });
    reloaded.agentEnded(contextFor(host));
    reloaded.agentEnded(contextFor(host));
    reloaded.agentEnded(contextFor(host));
    expect(host.level).toBe("medium");
    // A malformed latest entry ends the override instead of reviving an older one.
    controller(host).set({ level: "high", scope: "session" }, contextFor(host));
    host.entries.push({ type: "custom", customType: FABRIC_THINKING_ENTRY_TYPE, data: { version: 1, override: { level: "nope" } } });
    expect(controller(host).status(contextFor(host)).override).toBeUndefined();
    // Another session id replays its own branch.
    const other = controller(host);
    other.status(contextFor(host));
    other.invalidate();
    expect(other.status(contextFor(fakeHost(), "session-b")).override).toBeUndefined();
  });

  it("moves a child level back inside inherited bounds", () => {
    const host = fakeHost("xhigh");
    controller(host, { max: "medium" }).enforceBounds(contextFor(host));
    expect(host.level).toBe("medium");
    const unbounded = fakeHost("xhigh");
    controller(unbounded).enforceBounds(contextFor(unbounded));
    expect(unbounded.setThinkingLevel).not.toHaveBeenCalled();
  });
});

describe("ThinkingProvider", () => {
  const invocation = (host: ReturnType<typeof fakeHost>, sessionId = "session-a") => ({
    extensionContext: contextFor(host, sessionId),
  }) as unknown as FabricInvocationContext;

  it("describes read status and write set/reset with typed effects", async () => {
    const provider = new ThinkingProvider(controller(fakeHost()), "session-a");
    const actions = await provider.list({});
    expect(actions.map((action) => [action.name, action.risk, action.effect?.kind])).toEqual([
      ["status", "read", "none"], ["set", "write", "emission"], ["reset", "write", "emission"],
    ]);
  });

  it("validates arguments, enforces session ownership, and applies", async () => {
    const host = fakeHost("low");
    const provider = new ThinkingProvider(controller(host), "session-a");
    await expect(provider.invoke("set", { level: "ultra" }, invocation(host))).rejects.toThrow("Invalid thinking.set arguments");
    await expect(provider.invoke("set", { level: "high", extra: 1 }, invocation(host))).rejects.toThrow("Invalid thinking.set arguments");
    await expect(provider.invoke("status", {}, invocation(host, "session-b"))).rejects.toThrow("different session");
    await expect(provider.invoke("set", provider.prepareArguments("set", { Level: "high" }), invocation(host)))
      .resolves.toMatchObject({ level: "high" });
    await expect(provider.invoke("reset", {}, invocation(host))).resolves.toMatchObject({ level: "low" });
  });
});

describe("thinking configuration", () => {
  const roots: string[] = [];
  afterEach(async () => {
    vi.unstubAllEnvs();
    await Promise.all(roots.splice(0).map((root) => removeTree(root)));
  });

  it("defaults to unbounded and rejects invalid bounds", () => {
    expect(DEFAULT_FABRIC_CONFIG.thinking).toEqual({ bounds: {} });
    expect(normalizeFabricConfig({ thinking: { bounds: { max: "high" } } }).thinking.bounds).toEqual({ max: "high" });
    expect(() => normalizeFabricConfig({ thinking: { bounds: { max: "loud" } } })).toThrow("thinking.bounds.max");
    expect(() => normalizeFabricConfig({ thinking: { bounds: { min: "max", max: "low" } } })).toThrow("must not exceed");
  });

  it("narrows configured bounds by the inherited parent bounds, never widening", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-thinking-config-"));
    roots.push(root);
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(agentDir, { recursive: true });
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ thinking: { bounds: { min: "low", max: "max" } } }));
    const location = { cwd: root, agentDir, projectTrusted: false };
    vi.stubEnv("PI_FABRIC_THINKING_BOUNDS", JSON.stringify({ max: "high" }));
    expect(loadFabricConfig(location).thinking.bounds).toEqual({ min: "low", max: "high" });
    vi.stubEnv("PI_FABRIC_THINKING_BOUNDS", "{bad");
    expect(() => loadFabricConfig(location)).toThrow("Invalid PI_FABRIC_THINKING_BOUNDS");
  });
});

describe("child thinking bounds", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  const createManager = (bounds: FabricThinkingBounds) => {
    const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-thinking-agent-"));
    roots.push(runRoot);
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      runRoot,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      thinkingBounds: () => bounds,
    });
    managers.push(manager);
    return manager;
  };
  beforeEach(() => {
    for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID", "PI_FABRIC_THINKING_BOUNDS"]) {
      vi.stubEnv(key, undefined);
    }
  });
  afterEach(async () => {
    await Promise.all(managers.splice(0).map((manager) => manager.close()));
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    await Promise.all(roots.splice(0).map((root) => removeTree(root)));
  });

  it("normalizes requested bounds from agents.run arguments", () => {
    const defaults = { runner: "pi" as const, timeoutMs: 0 };
    expect(normalizeAgentRunRequest({ task: "t", thinkingBounds: { max: "low" } }, defaults).thinkingBounds).toEqual({ max: "low" });
    expect(() => normalizeAgentRunRequest({ task: "t", thinkingBounds: { max: "loud" } }, defaults)).toThrow("thinkingBounds.max");
  });

  it("forwards effective bounds and clamps the per-run level with a report", async () => {
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = createManager({ max: "high" });
    const handle = await manager.spawn({ task: "bounded", transport: "process", thinking: "max", thinkingBounds: { min: "low" } });
    expect(handle).toMatchObject({ requestedThinking: "max" });
    const options = parseWorkerOptions(["node", "worker.js", ...launch.mock.calls[0]![0].workerArguments]);
    expect(options.thinking).toBe("high");
    expect(JSON.parse(options.thinkingBounds!)).toEqual({ min: "low", max: "high" });
    await manager.wait(handle.id);
  });

  it("rejects child bounds wider than the caller before launch", async () => {
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = createManager({ max: "medium" });
    await expect(manager.spawn({ task: "wide", transport: "process", thinkingBounds: { max: "high" } }))
      .rejects.toThrow("must lie inside");
    expect(launch).not.toHaveBeenCalled();
  });

  it("keeps existing behavior when no bounds are configured", async () => {
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = createManager({});
    const handle = await manager.spawn({ task: "free", transport: "process", thinking: "max" });
    expect(handle.requestedThinking).toBeUndefined();
    const options = parseWorkerOptions(["node", "worker.js", ...launch.mock.calls[0]![0].workerArguments]);
    expect(options.thinking).toBe("max");
    expect(options.thinkingBounds).toBeUndefined();
    await manager.wait(handle.id);
  });
});
