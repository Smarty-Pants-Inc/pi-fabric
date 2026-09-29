import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";

// A models.json edit reaches the session registry only after refresh(); a spawn that misses
// must refresh once and retry instead of requiring a fleet reload (smarty-dev#1830).
const withRuntime = async (
  added: string[],
  run: (runtime: FabricRuntimeState, refresh: ReturnType<typeof vi.fn>) => Promise<void>,
) => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-model-refresh-"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  const models = [{ provider: "dest", id: "old", name: "Old", contextWindow: 48_000 }];
  const refresh = vi.fn(async () => {
    await new Promise((resolve) => setTimeout(resolve, 20));
    for (const id of added) models.push({ provider: "dest", id, name: id, contextWindow: 48_000 });
    return {};
  });
  const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn() } as unknown as ExtensionAPI;
  const context = {
    cwd, hasUI: false, isProjectTrusted: () => false, isIdle: () => true, hasPendingMessages: () => false,
    model: { provider: "dest", id: "old", contextWindow: 48_000 },
    modelRegistry: {
      getAvailable: () => [...models],
      find: (provider: string, id: string) => models.find((m) => m.provider === provider && m.id === id),
      getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "test" }),
      refresh,
    },
    sessionManager: { getSessionId: () => "model-refresh-test", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"), residentHost: path.join(cwd, "unused.mjs"), skills: cwd,
  } });
  try {
    await runtime.initialize(context, normalizeFabricConfig({ fullCodeMode: false, agents: { enabled: true, budgetUsd: 0 }, mcp: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, mesh: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } }));
    await run(runtime, refresh);
  } finally {
    await runtime.shutdown();
    vi.unstubAllEnvs();
    fs.rmSync(cwd, { recursive: true, force: true });
  }
};

it("spawns with a model added to models.json after one registry refresh", async () => {
  await withRuntime(["fresh"], async (runtime, refresh) => {
    const handle = await runtime.agents.spawn({ task: "HANG", model: "dest/fresh" });
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(runtime.agents.status(handle.id)).toMatchObject({ model: "dest/fresh" });
    await runtime.agents.stop(handle.id);
  });
});

it("throws the same error after exactly one refresh when the model is still missing", async () => {
  await withRuntime([], async (runtime, refresh) => {
    const error = 'Model "dest/typo-model-zzzz" is not available to this Pi session';
    await expect(runtime.agents.spawn({ task: "HANG", model: "dest/typo-model-zzzz" })).rejects.toThrow(error);
    expect(refresh).toHaveBeenCalledTimes(1);
    // Rate-limited: an immediate retry fails fast without another refresh.
    await expect(runtime.agents.spawn({ task: "HANG", model: "dest/typo-model-zzzz" })).rejects.toThrow(error);
    expect(refresh).toHaveBeenCalledTimes(1);
  });
});

it("shares one refresh between concurrent misses", async () => {
  await withRuntime(["alpha-new", "beta-new"], async (runtime, refresh) => {
    const handles = await Promise.all([
      runtime.agents.spawn({ task: "HANG", model: "dest/alpha-new" }),
      runtime.agents.spawn({ task: "HANG", model: "dest/beta-new" }),
    ]);
    expect(refresh).toHaveBeenCalledTimes(1);
    for (const handle of handles) await runtime.agents.stop(handle.id);
  });
});
