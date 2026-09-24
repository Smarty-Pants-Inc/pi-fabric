import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { SessionManager, SettingsManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { snapshotHandoffSession } from "../src/agents/handoff.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";

it.each([[true, false], [false, false], [true, true]])("resolves handoff settings at first use (trusted=%s, different cwd=%s)", async (trusted, differentCwd) => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-handoff-settings-"));
  const agentDir = path.join(cwd, "agent");
  const other = path.join(cwd, "other");
  fs.mkdirSync(agentDir);
  for (const directory of [cwd, other]) {
    fs.mkdirSync(path.join(directory, ".pi"), { recursive: true });
    fs.writeFileSync(path.join(directory, ".pi", "settings.json"), JSON.stringify({ compaction: { keepRecentTokens: 9000, modelOverrides: { "dest/worker": { keepRecentTokens: 777 } } } }));
  }
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ compaction: { keepRecentTokens: 1500, modelOverrides: { "dest/worker": { reserveTokens: 6000 } } } }));
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  const settingsSpy = vi.spyOn(SettingsManager, "create");
  const target = { provider: "dest", id: "worker", name: "Worker", contextWindow: 48_000 };
  const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn() } as unknown as ExtensionAPI;
  const context = {
    cwd, hasUI: false, isProjectTrusted: () => trusted, isIdle: () => true, hasPendingMessages: () => false,
    model: { provider: "source", id: "parent", contextWindow: 1_000_000 },
    modelRegistry: { getAvailable: () => [target], find: () => target, getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "test" }) },
    sessionManager: { getSessionId: () => "settings-test", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"), residentHost: path.join(cwd, "unused.mjs"), skills: cwd,
  } });
  try {
    await runtime.initialize(context, normalizeFabricConfig({ fullCodeMode: false, agents: { enabled: true, budgetUsd: 0 }, compaction: { targetContextRatio: 0.5 }, mcp: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, mesh: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } }));
    expect(settingsSpy).not.toHaveBeenCalled();
    const source = SessionManager.inMemory(cwd);
    source.appendMessage({ role: "user", content: "x".repeat(80_000), timestamp: 1 });
    source.appendMessage({ role: "assistant", content: [{ type: "toolCall", id: "outer", name: "fabric_exec", arguments: {} }], api: "anthropic-messages", provider: "source", model: "parent", stopReason: "toolUse", timestamp: 2,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } });
    const seed = snapshotHandoffSession(source, undefined, { role: "toolResult", toolCallId: "outer", toolName: "fabric_exec", content: [{ type: "text", text: "done" }], isError: false, timestamp: 3 }, "outer");
    const handle = await runtime.agents.spawn({ task: "HANG", model: "dest/worker", sessionSeed: seed, handoffCompact: {}, ...(differentCwd ? { cwd: other } : {}) });
    const directory = path.join(runtime.agents.runDirectory(handle.id)!, "handoff-session");
    const session = SessionManager.open(path.join(directory, fs.readdirSync(directory)[0]!));
    expect(session.getBranch().find(e => e.type === "compaction")).toMatchObject({ details: { budget: { contextWindow: 48_000, targetContextRatio: 0.5, reserveTokens: 6000, keepRecentTokens: trusted && !differentCwd ? 777 : 1500 } } });
    expect(settingsSpy).toHaveBeenCalledExactlyOnceWith(differentCwd ? fs.realpathSync(other) : cwd, agentDir, { projectTrusted: trusted && !differentCwd });
    await runtime.agents.stop(handle.id);
  } finally {
    await runtime.shutdown();
    settingsSpy.mockRestore();
    vi.unstubAllEnvs();
    fs.rmSync(cwd, { recursive: true, force: true });
  }
});
