import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig, type FabricAgentConfig } from "../src/config.js";
import type { FabricThinking } from "../src/thinking.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";

// A models.json edit reaches the session registry only after refresh(); a spawn that misses
// must refresh once and retry instead of requiring a fleet reload (smarty-dev#1830).
const withRuntime = async (
  added: string[],
  run: (runtime: FabricRuntimeState, refresh: ReturnType<typeof vi.fn>, context: ExtensionContext) => Promise<void>,
  initial: string[] = [],
  aliases: Record<string, string> = {},
  options: { parentKind?: "actor" | "agent"; thinking?: FabricThinking; agents?: Partial<FabricAgentConfig> } = {},
) => {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-model-refresh-"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
  if (options.parentKind) {
    vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", "session:remote-parent");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "parent-run");
    if (options.parentKind === "actor") vi.stubEnv("PI_FABRIC_ACTOR_ID", "parent-actor");
  }
  const models = [{ provider: "dest", id: "old", name: "Old", contextWindow: 48_000 },
    ...initial.map((id) => ({ provider: "dest", id, name: id, contextWindow: 48_000 }))];
  const refresh = vi.fn(async () => {
    await new Promise((resolve) => setTimeout(resolve, 20));
    for (const id of added) models.push({ provider: "dest", id, name: id, contextWindow: 48_000 });
    return {};
  });
  const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => options.thinking ?? "off", sendMessage: vi.fn() } as unknown as ExtensionAPI;
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
    await runtime.initialize(context, normalizeFabricConfig({ fullCodeMode: false, agents: { enabled: true, budgetUsd: 0, ...options.agents }, mcp: { enabled: false }, memory: { enabled: false }, residency: { enabled: false }, models: { aliases }, mesh: { enabled: true }, prewalk: { enabled: false, alwaysRearm: false } }));
    await run(runtime, refresh, context);
  } finally {
    await runtime.shutdown();
    vi.unstubAllEnvs();
    fs.rmSync(cwd, { recursive: true, force: true });
  }
};

it.each(["actor", "agent"] as const)("#2490 inherits the actual %s host binding even when Main is remote and config is denied", async (parentKind) => {
  await withRuntime([], async (runtime, refresh, context) => {
    const handle = await invoke(runtime, context, "agents.spawn", { task: "HANG" }) as { id: string; model: string; thinking: string };
    expect(handle).toMatchObject({ model: "dest/old", thinking: "max" });
    expect(refresh).not.toHaveBeenCalled();
    await runtime.agents.stop(handle.id);
    const actor = await invoke(runtime, context, "agents.create", { name: "nested", instructions: "Review." }) as { id: string };
    expect(runtime.actors.definition(actor.id)).toMatchObject({ model: "dest/old", thinking: "max" });
  }, [], {}, { parentKind, thinking: "max", agents: { model: "dest/denied", thinking: "low", deniedModels: ["dest/denied"], deniedModelReplacement: "dest/old" } });
});

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

// The public entry point: fabric_exec's agents.spawn goes through the registered provider,
// whose own model check must refresh too (review/astra P1 on #138).
const invoke = (runtime: FabricRuntimeState, context: ExtensionContext, ref: string, args: Record<string, unknown>) =>
  runtime.registry.invoke(ref, args, {
    cwd: context.cwd, signal: undefined, parentToolCallId: "fabric_test", nestedToolCallId: "fabric_test-nested",
    extensionContext: context, update: vi.fn(), approve: vi.fn(async () => {}), audits: [], maxResultChars: 100_000,
  } as unknown as Parameters<FabricRuntimeState["registry"]["invoke"]>[2]);

it("agents.spawn through the registered provider resolves a model added by one refresh", async () => {
  await withRuntime(["fresh"], async (runtime, refresh, context) => {
    const handle = await invoke(runtime, context, "agents.spawn", { task: "HANG", model: "dest/fresh" }) as { id: string };
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(runtime.agents.status(handle.id)).toMatchObject({ model: "dest/fresh" });
    await runtime.agents.stop(handle.id);
  });
});

it("agents.spawn through the registered provider keeps the same error when still missing", async () => {
  await withRuntime([], async (runtime, refresh, context) => {
    await expect(invoke(runtime, context, "agents.spawn", { task: "HANG", model: "dest/typo-model-zzzz" }))
      .rejects.toThrow('Model "dest/typo-model-zzzz" is not available to this Pi session');
    expect(refresh).toHaveBeenCalledTimes(1);
  });
});

it("creates an actor through the provider with a model added by one refresh", async () => {
  await withRuntime(["fresh"], async (runtime, refresh, context) => {
    const actor = await invoke(runtime, context, "agents.create", {
      name: "fresh-actor", instructions: "Reply.", runner: "pi", model: "dest/fresh",
    }) as { id: string; model?: string };
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(actor.model).toBe("dest/fresh");
  });
});

it("creates an actor on the manager hook with a model added by one refresh, and keeps the error when missing", async () => {
  await withRuntime(["fresh"], async (runtime, refresh) => {
    const actor = await runtime.actors.create({ name: "hook-actor", instructions: "Reply.", runner: "pi", model: "dest/fresh" });
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(actor.model).toBe("dest/fresh");
  });
  await withRuntime([], async (runtime, refresh) => {
    await expect(runtime.actors.create({ name: "typo-actor", instructions: "Reply.", runner: "pi", model: "dest/typo-model-zzzz" }))
      .rejects.toThrow('Model "dest/typo-model-zzzz" is not available to this Pi session');
    expect(refresh).toHaveBeenCalledTimes(1);
  });
});

it("activates actors with just-added models after one shared refresh", async () => {
  await withRuntime(["alpha-new", "beta-new"], async (runtime, refresh) => {
    const [a, b] = await Promise.all(["act-a", "act-b"].map((name) =>
      runtime.actors.create({ name, instructions: "Reply.", runner: "pi", model: "dest/old" })));
    expect(refresh).not.toHaveBeenCalled();
    const replies = await Promise.all([
      runtime.actors.ask(a!.id, "hi", undefined, undefined, { overrides: { model: "dest/alpha-new" } }),
      runtime.actors.ask(b!.id, "hi", undefined, undefined, { overrides: { model: "dest/beta-new" } }),
    ]);
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(replies.map((reply) => reply.error)).toEqual([undefined, undefined]);
  });
});

it("fails an activation whose model is still missing with the same error", async () => {
  await withRuntime([], async (runtime, refresh) => {
    const actor = await runtime.actors.create({ name: "act-miss", instructions: "Reply.", runner: "pi", model: "dest/old" });
    await expect(runtime.actors.ask(actor.id, "hi", undefined, undefined, { overrides: { model: "dest/typo-model-zzzz" } }))
      .rejects.toThrow('Model "dest/typo-model-zzzz" is not available to this Pi session');
    expect(refresh).toHaveBeenCalledTimes(1);
  });
});

// review/astra round 2 on #138: a similar stale model on the same provider must not win the fuzzy
// fallback before the refresh runs (claude-opus-5-6 requested, only claude-opus-5-5 listed).
const similar = ["claude-opus-5-5"];

it("resolves an exact model added next to a similar one after one refresh, on every path", async () => {
  await withRuntime(["claude-opus-5-6"], async (runtime, refresh) => {
    const handle = await runtime.agents.spawn({ task: "HANG", model: "dest/claude-opus-5-6" });
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(runtime.agents.status(handle.id)).toMatchObject({ model: "dest/claude-opus-5-6" });
    await runtime.agents.stop(handle.id);
  }, similar);
  await withRuntime(["claude-opus-5-6"], async (runtime, refresh, context) => {
    const handle = await invoke(runtime, context, "agents.spawn", { task: "HANG", model: "dest/claude-opus-5-6" }) as { id: string };
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(runtime.agents.status(handle.id)).toMatchObject({ model: "dest/claude-opus-5-6" });
    await runtime.agents.stop(handle.id);
  }, similar);
  await withRuntime(["claude-opus-5-6"], async (runtime, refresh, context) => {
    const actor = await invoke(runtime, context, "agents.create", {
      name: "similar-actor", instructions: "Reply.", runner: "pi", model: "dest/claude-opus-5-6",
    }) as { model?: string };
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(actor.model).toBe("dest/claude-opus-5-6");
  }, similar);
  await withRuntime(["claude-opus-5-6"], async (runtime, refresh) => {
    const actor = await runtime.actors.create({ name: "bare-actor", instructions: "Reply.", runner: "pi", model: "claude-opus-5-6" });
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(actor.model).toBe("dest/claude-opus-5-6");
  }, similar);
});

it("keeps the same error for a truly unknown exact id next to similar models after one refresh", async () => {
  await withRuntime([], async (runtime, refresh) => {
    await expect(runtime.agents.spawn({ task: "HANG", model: "dest/typo-model-zzzz" }))
      .rejects.toThrow('Model "dest/typo-model-zzzz" is not available to this Pi session');
    expect(refresh).toHaveBeenCalledTimes(1);
  }, similar);
});

it("resolves an alias or a fuzzy query that matches today without a refresh", async () => {
  await withRuntime(["claude-opus-5-6"], async (runtime, refresh, context) => {
    const aliased = await invoke(runtime, context, "agents.spawn", { task: "HANG", model: "big" }) as { id: string };
    const fuzzy = await runtime.agents.spawn({ task: "HANG", model: "opus" });
    expect(refresh).not.toHaveBeenCalled();
    expect(runtime.agents.status(aliased.id)).toMatchObject({ model: "dest/claude-opus-5-5" });
    expect(runtime.agents.status(fuzzy.id)).toMatchObject({ model: "dest/claude-opus-5-5" });
    for (const handle of [aliased, fuzzy]) await runtime.agents.stop(handle.id);
  }, similar, { big: "dest/claude-opus-5-5" });
});
