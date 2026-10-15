import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";
import type { FabricCommandDeps } from "../src/commands/fabric.js";
import { afterEach, describe, expect, it, vi } from "vitest";

type CommandDefinition = Parameters<ExtensionAPI["registerCommand"]>[1];
const expectedStatus = [
  "cwd: cold-project",
  "mode: orchestration-only (native Pi tools)",
  "providers: cold-provider",
  "runner: pi · transport: process · model: inherit",
  "agent limits: concurrency 2, per execution 3, depth 1",
  "prewalk: idle · in-place · model Ask each time · auto-arm & re-arm off",
  "captured tools: disabled (native registry preserved)",
  "actors: 0 · mesh: disabled",
  "MCP: disabled",
  "UI: disabled",
].join("\n");

async function freshRegistry() {
  const { registerFabricCommand } = await import("../src/commands/fabric.js");
  const commands = new Map<string, CommandDefinition>();
  const hooks = new Map<string, Array<() => unknown>>();
  const pi = {
    registerCommand: (name: string, definition: CommandDefinition) => commands.set(name, definition),
    registerShortcut: vi.fn(),
    events: { on: vi.fn(() => vi.fn()) },
    on: (event: string, hook: () => unknown) => hooks.set(event, [...hooks.get(event) ?? [], hook]),
  } as unknown as ExtensionAPI;
  const ensure = vi.fn(async () => {});
  const deps = {
    state: {
      initialized: false, ensure, cwd: "cold-project",
      config: {
        fullCodeMode: false,
        agents: { runner: "pi", transport: "process", model: "", maxConcurrent: 2, maxPerExecution: 3, maxDepth: 1 },
        prewalk: { mode: "in-place", model: "", alwaysRearm: false },
        foreground: { tools: [] }, mesh: { enabled: false }, mcp: { enabled: false }, ui: { enabled: false },
      },
      registry: { providers: () => [{ name: "cold-provider" }] },
      actors: { list: () => [] }, prewalk: { status: () => ({ state: "idle" }) },
    },
    fabricUi: {}, capturedTools: {}, applyFabricMode: vi.fn(), suspendToolCapture: vi.fn(),
  } as unknown as FabricCommandDeps;
  const notify = vi.fn();
  const context = { ui: { notify } } as unknown as ExtensionCommandContext;
  registerFabricCommand(pi, deps);
  return { command: commands.get("fabric")!, hooks, ensure, context, notify };
}

afterEach(() => {
  vi.doUnmock("../src/commands/fabric-handler.js");
  vi.resetModules();
});

// Keep reason: a failed optional-module import cannot be reached cheaply in a
// normal user flow. Both proofs use the real registry and real status handler.
describe("/fabric command first-use loading", () => {
  it("stays unloaded at cold registration and idle, then executes real status text", async () => {
    vi.resetModules();
    const loads = vi.fn();
    vi.doMock("../src/commands/fabric-handler.js", async () => {
      loads();
      return vi.importActual<typeof import("../src/commands/fabric-handler.js")>("../src/commands/fabric-handler.js");
    });
    const { command, hooks, ensure, context, notify } = await freshRegistry();
    expect(command.description).toBe("Open Fabric dashboard, chat or tasks, arm prewalk, reload, or manage agents and actors");
    expect(command.getArgumentCompletions?.("sta")).toEqual([{ value: "status", label: "status" }]);
    for (const event of ["session_start", "resources_discover"]) {
      for (const hook of hooks.get(event) ?? []) await hook();
    }
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(loads).not.toHaveBeenCalled();
    expect(ensure).not.toHaveBeenCalled();

    await command.handler("status", context);
    expect(loads).toHaveBeenCalledOnce();
    expect(notify).toHaveBeenLastCalledWith(expectedStatus, "info");
    await command.handler("status", context);
    expect(loads).toHaveBeenCalledOnce();
    expect(notify).toHaveBeenCalledTimes(2);
  });

  it("notifies an import failure and allows the same registered command to retry", async () => {
    vi.resetModules();
    const failedLoad = vi.fn(() => { throw new Error("handler chunk unavailable"); });
    vi.doMock("../src/commands/fabric-handler.js", failedLoad);
    const { command, ensure, context, notify } = await freshRegistry();
    await command.handler("status", context);
    expect(failedLoad).toHaveBeenCalledOnce();
    // Vitest wraps a rejected mock factory in its own import error; the real
    // wrapper must surface whichever import failure the loader supplies.
    expect(notify).toHaveBeenLastCalledWith(expect.stringMatching(/^Failed to load \/fabric command: .+/), "error");
    expect(ensure).not.toHaveBeenCalled();

    // Simulate the missing chunk becoming available without re-registering.
    vi.doMock("../src/commands/fabric-handler.js", () =>
      vi.importActual<typeof import("../src/commands/fabric-handler.js")>("../src/commands/fabric-handler.js"));
    await command.handler("status", context);
    expect(notify).toHaveBeenLastCalledWith(expectedStatus, "info");
    expect(ensure).toHaveBeenCalledOnce();
  });
});
