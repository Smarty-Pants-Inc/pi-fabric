import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";

const loads = vi.hoisted(() => vi.fn());
vi.mock("../src/core/landlock.js", async original => {
  loads(); return await original<typeof import("../src/core/landlock.js")>();
});
afterEach(() => { vi.unstubAllEnvs(); vi.restoreAllMocks(); });

it("keeps Landlock out of import/registration/idle/off calls, then loads at first enforced use", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-landlock-startup-"));
  const cwd = path.join(root, "lane"); const tmp = path.join(root, "private-tmp");
  const agentDir = path.join(root, "profile");
  fs.mkdirSync(cwd); fs.mkdirSync(agentDir); fs.mkdirSync(tmp, { mode: 0o700 });
  vi.stubEnv("TMPDIR", tmp);
  vi.resetModules(); loads.mockClear();
  const { PiToolsProvider } = await import("../src/providers/pi-tools-provider.js");
  const { ActionRegistry } = await import("../src/core/action-registry.js");
  const { SessionManager, createExtensionRuntime, ExtensionRunner } = await import("@earendil-works/pi-coding-agent");
  const { DEFAULT_FABRIC_CONFIG, loadFabricConfig, liveLandlockSettings } = await import("../src/config.js");
  const settings = loadFabricConfig({ cwd, agentDir, projectTrusted: true }).executor.landlock;
  expect(settings).toEqual(DEFAULT_FABRIC_CONFIG.executor.landlock);
  expect(settings).toEqual({ mode: "off", disabled: false });
  const provider = new PiToolsProvider(cwd, undefined, undefined, {
    powerShellToolDefinitionFactory: undefined, getShellHangMs: () => 0,
    getLandlockSettings: () => liveLandlockSettings(settings, agentDir),
  });
  const registry = new ActionRegistry(); registry.register(provider);
  const runtime = createExtensionRuntime(); runtime.getThinkingLevel = () => "off";
  const runner = new ExtensionRunner([], runtime, cwd, SessionManager.inMemory(cwd), {} as never);
  const context = { cwd, extensionContext: runner.createContext(), signal: new AbortController().signal,
    parentToolCallId: "lazy-parent", nestedToolCallId: "lazy-first-use", update: () => {},
    approve: async () => {}, audits: [], maxResultChars: 100_000,
  };
  try {
    expect(loads).not.toHaveBeenCalled();
    await new Promise<void>(resolve => setImmediate(resolve));
    await provider.describe("bash", context);
    expect(loads).not.toHaveBeenCalled();
    await registry.invoke("pi.bash", { command: ":" }, context);
    expect(loads).not.toHaveBeenCalled();
    settings.mode = "enforce";
    await registry.invoke("pi.bash", { command: "printf ok > first-use" }, context);
    if (process.platform === "linux") {
      expect(loads).toHaveBeenCalledOnce();
      expect(fs.readFileSync(path.join(cwd, "first-use"), "utf8")).toBe("ok");
      await registry.invoke("pi.bash", { command: ":" }, context);
      expect(loads).toHaveBeenCalledOnce();
    } else expect(loads).not.toHaveBeenCalled();
  } finally { await registry.close(); fs.rmSync(root, { recursive: true, force: true }); }
});
