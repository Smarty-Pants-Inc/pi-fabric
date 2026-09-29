import { describe, expect, it } from "vitest";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { AGENT_WAIT_MAX_MS, MAIN_AGENT_WAIT_MAX_MS, agentWaitBound, isInteractiveMain } from "../src/agents/wait-bound.js";

// smarty-dev#2119: a 30-minute wait in an interactive Main ends at 60 s; elsewhere #854's 5 min stays.
describe("agents.wait bound", () => {
  it("caps a Main wait at 60 s and keeps 5 min for every other caller", () => {
    expect(agentWaitBound(1_800_000, MAIN_AGENT_WAIT_MAX_MS)).toBe(60_000);
    expect(agentWaitBound(undefined, MAIN_AGENT_WAIT_MAX_MS)).toBe(60_000);
    expect(agentWaitBound(5_000, MAIN_AGENT_WAIT_MAX_MS)).toBe(5_000);
    expect(agentWaitBound(1_800_000)).toBe(AGENT_WAIT_MAX_MS);
    expect(AGENT_WAIT_MAX_MS).toBe(300_000);
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
