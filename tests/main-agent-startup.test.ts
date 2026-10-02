import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";

const mainLoaded = vi.hoisted(() => vi.fn());
const routerLoaded = vi.hoisted(() => vi.fn());
const pruneLoaded = vi.hoisted(() => vi.fn());
vi.mock("../src/actors/prune.js", async original => {
  pruneLoaded();
  return original<typeof import("../src/actors/prune.js")>();
});
vi.mock("../src/main-agent.js", async original => {
  mainLoaded();
  return original<typeof import("../src/main-agent.js")>();
});
vi.mock("../src/providers/agents-message-router.js", async original => {
  routerLoaded();
  return original<typeof import("../src/providers/agents-message-router.js")>();
});
afterEach(() => vi.unstubAllEnvs());

describe("Main replay and route-authority startup boundary", () => {
  it("stays unloaded during cold import, registration and idle hooks, then loads once at runtime first use", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-main-startup-"));
    fs.mkdirSync(path.join(cwd, "agent"));
    fs.mkdirSync(path.join(cwd, ".pi"));
    fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({
      fullCodeMode: false, components: [], prewalk: { enabled: false, alwaysRearm: false },
      mesh: { enabled: false }, agents: { enabled: false }, residency: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    }));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
    const handlers = new Map<string, Handler[]>();
    const pi = {
      events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
      on(name: string, handler: Handler) {
        handlers.set(name, [...(handlers.get(name) ?? []), handler]);
        return () => handlers.set(name, (handlers.get(name) ?? []).filter(fn => fn !== handler));
      },
      getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []), getThinkingLevel: () => "off",
      registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
      setActiveTools: vi.fn(), sendMessage: vi.fn(),
    } as unknown as ExtensionAPI;
    const context = {
      cwd, hasUI: false, mode: "rpc", isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: { getSessionId: () => "startup-session", getBranch: () => [], getEntries: () => [],
        getSessionFile: () => undefined, getLeafId: () => null },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const emit = async (name: string) => {
      for (const handler of [...(handlers.get(name) ?? [])]) await handler({}, context);
    };
    let state: import("../src/fabric-state.js").FabricState | undefined;
    try {
      vi.resetModules();
      mainLoaded.mockClear();
      routerLoaded.mockClear();
      pruneLoaded.mockClear();
      const { default: register } = await import("../src/index.js");
      expect(mainLoaded).not.toHaveBeenCalled();
      expect(routerLoaded).not.toHaveBeenCalled();
      await register(pi);
      for (const name of ["resources_discover", "session_start"]) await emit(name);
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(mainLoaded).not.toHaveBeenCalled();
      expect(routerLoaded).not.toHaveBeenCalled();
      const { FabricState } = await import("../src/fabric-state.js");
      const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
      state = new FabricState(pi, new CapturedToolCatalog());
      await state.bootstrap(context);
      expect(mainLoaded).not.toHaveBeenCalled();
      await state.ensure(context);
      expect(state.initialized).toBe(true);
      expect(mainLoaded).toHaveBeenCalledOnce();
      expect(routerLoaded).toHaveBeenCalledOnce();
      expect(pruneLoaded).not.toHaveBeenCalled();
      await state.ensure(context);
      expect(mainLoaded).toHaveBeenCalledOnce();
      expect(routerLoaded).toHaveBeenCalledOnce();
    } finally {
      await state?.shutdown();
      await emit("session_shutdown");
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });
});
