import { describe, expect, it, vi } from "vitest";

const loads = vi.hoisted(() => vi.fn());
vi.mock("../src/agents/spawn-router.js", async original => {
  loads(); return original<typeof import("../src/agents/spawn-router.js")>();
});

describe("spawn router cold import and idle", () => {
  it("keeps the subprocess adapter out of configuration and provider imports until first use", async () => {
    vi.resetModules(); loads.mockClear();
    const config = await import("../src/config.js");
    config.normalizeFabricConfig({ agents: { router: { command: [process.execPath], mode: "enforce" } } });
    await import("../src/providers/agents-provider.js");
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(loads).not.toHaveBeenCalled();
    const adapter = await import("../src/agents/spawn-router.js");
    expect(typeof adapter.routeAgentCreation).toBe("function");
    expect(loads).toHaveBeenCalledTimes(1);
  });
});
