import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { AGENT_WAIT_MAX_MS, MAIN_AGENT_WAIT_MAX_MS, agentWaitBound, mainAgentWaitBound, isInteractiveMain } from "../src/agents/wait-bound.js";

// Isolate the shared live flag reader from real user configuration.
vi.mock("node:os", async original => ({ ...await original<typeof import("node:os")>(), homedir: () => "/fixture-wait-home" }));
vi.mock("node:fs", async original => ({ ...await original<typeof import("node:fs")>(), existsSync: () => false }));
beforeEach(() => vi.stubEnv("SMARTY_ALPHA", "0"));
afterEach(() => { vi.unstubAllEnvs(); vi.restoreAllMocks(); });

// smarty-dev#2119: OFF retains the 60 s Main and 5 min non-Main bounds.
describe("agents.wait bound", () => {
  it("caps a Main wait at 60 s and keeps 5 min for every other caller", () => {
    expect(agentWaitBound(1_800_000, MAIN_AGENT_WAIT_MAX_MS)).toBe(60_000);
    expect(agentWaitBound(undefined, MAIN_AGENT_WAIT_MAX_MS)).toBe(60_000);
    expect(agentWaitBound(5_000, MAIN_AGENT_WAIT_MAX_MS)).toBe(5_000);
    expect(agentWaitBound(1_800_000)).toBe(AGENT_WAIT_MAX_MS);
    expect(AGENT_WAIT_MAX_MS).toBe(300_000);
  });

  it("honors alpha non-Main explicit timeouts without changing Main/default/floor/invalid bounds, warning once", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.stubEnv("SMARTY_ALPHA", "1");
    expect(agentWaitBound(1_800_000)).toBe(1_800_000);
    expect(agentWaitBound(420_000.9)).toBe(420_000);
    expect(agentWaitBound(undefined)).toBe(300_000);
    expect(agentWaitBound(1)).toBe(1_000);
    for (const invalid of [NaN, Infinity, "420000", null]) expect(agentWaitBound(invalid)).toBe(300_000);
    expect(mainAgentWaitBound(1_800_000)).toBe(60_000);
    expect(agentWaitBound(1_800_000, MAIN_AGENT_WAIT_MAX_MS)).toBe(60_000);
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]?.[0]).toMatch(/NON_MAIN_WAIT_BOUND/);
    vi.stubEnv("SMARTY_ALPHA", "0");
    expect(agentWaitBound(1_800_000)).toBe(300_000);
    expect(warn).toHaveBeenCalledOnce();
  });

  it("reserves 2 s of the remaining absolute Main budget with a 1 s observation floor", () => {
    const now = Date.now();
    expect(mainAgentWaitBound(undefined)).toBe(60_000);
    expect(mainAgentWaitBound(86_400_000, now + 600_000)).toBe(60_000);
    expect(mainAgentWaitBound(undefined, now + 60_000)).toBeLessThanOrEqual(58_000);
    expect(mainAgentWaitBound(5_000, now + 60_000)).toBe(5_000);
    expect(mainAgentWaitBound(undefined, now + 3_000)).toBe(1_000);
    expect(mainAgentWaitBound(undefined, now + 500)).toBe(1_000);
    expect(mainAgentWaitBound(undefined, now - 1)).toBe(1_000);
  });

  it.each(["tui", "rpc"])("returns false for incomplete %s session contexts instead of throwing", mode => {
    for (const sessionManager of [undefined, {}, { getSessionId: undefined }, { getSessionId: "not a method" }]) {
      const context = { mode, sessionManager } as unknown as ExtensionContext;
      expect(isInteractiveMain(context, {})).toBe(false);
    }
  });

  it("treats only a TUI or RPC session with no parent run or actor id as an interactive Main", () => {
    const ctx = (mode: string) => ({ mode, sessionManager: { getSessionId: () => "s" } }) as unknown as ExtensionContext;
    expect(isInteractiveMain(ctx("tui"), {})).toBe(true);
    expect(isInteractiveMain(ctx("rpc"), {})).toBe(true);
    expect(isInteractiveMain(ctx("print"), {})).toBe(false);
    expect(isInteractiveMain(ctx("json"), {})).toBe(false);
    expect(isInteractiveMain(ctx("rpc"), { PI_FABRIC_PARENT_RUN: "run-1" })).toBe(false);
    expect(isInteractiveMain(ctx("rpc"), { PI_FABRIC_ACTOR_ID: "actor-1" })).toBe(false);
    expect(isInteractiveMain(undefined, {})).toBe(false);
  });
});
