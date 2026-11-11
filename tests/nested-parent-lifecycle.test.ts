import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  ExtensionRunner,
  SessionManager,
  SettingsManager,
  type ExtensionAPI,
  type ToolResultEvent,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { expect, it } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricToolResultProxy } from "../src/core/tool-result-proxy.js";
import { FabricExecutionService, type FabricExecutionResult } from "../src/execution-service.js";
import { FABRIC_NESTED_TOOL_CALL_ID_PREFIX } from "../src/protocol.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { createShellCommandPreflight } from "../src/providers/shell-preflight.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

// Keep reason: smarty-dev#20050, the real guest -> provider -> Pi middleware
// boundary must not let a top-level output cap flatten program-local data.
it("preserves a >64KiB mesh.read array through a registered parent-aware tool_result guard", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nested-parent-"));
  const events: ToolResultEvent[] = [];
  const capped: string[] = [];
  const settingsManager = SettingsManager.inMemory({ defaultProjectTrust: "never" });
  const loader = new DefaultResourceLoader({
    cwd: root, agentDir: root, settingsManager,
    noExtensions: true, noSkills: true, noThemes: true, noPromptTemplates: true,
    agentsFilesOverride: () => ({ agentsFiles: [] }),
    extensionFactories: [pi => {
      pi.on("tool_result", event => {
        events.push(event);
        if (event.parentToolCallId) return;
        const text = event.content.filter(part => part.type === "text").map(part => part.text).join("\n");
        if (Buffer.byteLength(text, "utf8") <= 64 * 1024) return;
        capped.push(event.toolCallId);
        return { content: [{ type: "text", text: "guard capped top-level output" }] };
      });
    }],
  });
  let registry: ActionRegistry | undefined;
  try {
    await loader.reload();
    const loaded = loader.getExtensions();
    expect(loaded.errors).toEqual([]);
    loaded.runtime.getThinkingLevel = () => "off";
    const runner = new ExtensionRunner(loaded.extensions, loaded.runtime, root, SessionManager.inMemory(root), {} as never);
    const context = runner.createContext();
    const identity: MeshIdentity = { id: "session:nested-parent", kind: "main", name: "main", sessionId: "nested-parent" };
    const store = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
    const payload = { label: "preserve-object", body: "nested data! ".repeat(6 * 1024), values: [1, 2, 3] };
    await store.publish({ topic: "nested-parent", from: identity, data: payload });
    registry = new ActionRegistry(new FabricToolResultProxy(() => runner));
    registry.register(new MeshProvider(store, identity, { list: () => [], get: () => undefined } as unknown as FabricParticipantSource));
    const config = normalizeFabricConfig({
      fullCodeMode: false, approvals: { read: "allow" },
      executor: { maxNestedResultChars: 256 * 1024 },
    });
    const result = await new FabricExecutionService(registry, config).execute({
      code: `const rows = await mesh.read({topic: "nested-parent"});
return {isArray: Array.isArray(rows), count: rows.length, data: rows[0]?.data};`,
      context, signal: undefined, parentToolCallId: "call-fabric-parent", onPartial() {},
    });
    expect(result.success, result.error).toBe(true);
    const value = result.value as { isArray: boolean; count: number; data?: typeof payload };
    expect({ isArray: value.isArray, count: value.count, label: value.data?.label,
      bodyChars: value.data?.body.length, values: value.data?.values,
    }).toEqual({ isArray: true, count: 1, label: payload.label,
      bodyChars: payload.body.length, values: payload.values });
    expect(value.data?.body === payload.body).toBe(true);
    expect(capped).toEqual([]);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: "tool_result", toolName: "mesh.read", parentToolCallId: "call-fabric-parent",
      toolCallId: expect.stringMatching(new RegExp(`^${FABRIC_NESTED_TOOL_CALL_ID_PREFIX}`)),
    });
  } finally {
    await registry?.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 30_000);

it("attributes replayed Pi/captured lifecycle events without losing middleware patches or error hooks", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nested-events-"));
  const events: Array<{ type: string; toolCallId: string; parentToolCallId?: string; toolName: string }> = [];
  const executed: string[] = [];
  const register = (pi: ExtensionAPI) => {
    for (const name of ["progress", "fails", "blocked", "bash"]) {
      pi.registerTool({
        name, label: name, description: name,
        parameters: name === "bash" ? Type.Object({ command: Type.String() }) : Type.Object({}),
        async execute(_id, _args, _signal, onUpdate) {
          executed.push(name);
          if (name === "fails") throw new Error("provider failed");
          onUpdate?.({ content: [{ type: "text", text: "working" }], details: {} });
          return { content: [{ type: "text", text: "original" }], details: {} };
        },
      });
    }
    pi.on("tool_execution_start", event => { events.push(event); });
    pi.on("tool_execution_update", event => { events.push(event); });
    pi.on("tool_execution_end", event => { events.push(event); });
    pi.on("tool_call", event => {
      events.push(event);
      if (event.toolName === "blocked") return { block: true, reason: "guard blocked" };
    });
    pi.on("tool_result", event => {
      events.push(event);
      if (event.toolName === "fails") {
        expect(event.isError).toBe(true);
        return { content: [{ type: "text", text: "recovered by hook" }], isError: false };
      }
      if (event.toolName === "progress" || event.toolName === "ls" || event.toolName === "bash") {
        return { content: [{ type: "text", text: "patched by hook" }] };
      }
    });
  };
  const loader = new DefaultResourceLoader({
    cwd: root, agentDir: root, settingsManager: SettingsManager.inMemory({ defaultProjectTrust: "never" }),
    noExtensions: true, noSkills: true, noThemes: true, noPromptTemplates: true,
    agentsFilesOverride: () => ({ agentsFiles: [] }), extensionFactories: [register],
  });
  let registry: ActionRegistry | undefined;
  try {
    await loader.reload();
    const loaded = loader.getExtensions();
    expect(loaded.errors).toEqual([]);
    loaded.runtime.getThinkingLevel = () => "off";
    loaded.runtime.getActiveTools = () => ["progress", "fails", "blocked", "bash"];
    const runner = new ExtensionRunner(loaded.extensions, loaded.runtime, root, SessionManager.inMemory(root), {} as never);
    const catalog = new CapturedToolCatalog();
    const config = normalizeFabricConfig({ fullCodeMode: true });
    catalog.replace(runner.getAllRegisteredTools(), runner, config.capture, "/fabric/index.ts");
    const captured = new CapturedToolsProvider(catalog);
    registry = new ActionRegistry(new FabricToolResultProxy(() => runner));
    registry.register(captured);
    registry.register(new PiToolsProvider(root, catalog, captured));
    const context = {
      cwd: root, extensionContext: runner.createContext(), signal: undefined,
      parentToolCallId: "call-lifecycle-parent", nestedToolCallId: "metadata",
      update() {}, approve: async () => {}, audits: [], maxResultChars: 100_000,
    };
    expect(await registry.invoke("pi.ls", { path: root }, context)).toBe("patched by hook");
    expect(await registry.invoke("extensions.progress", {}, context)).toMatchObject({ text: "patched by hook", isError: false });
    expect(await registry.invoke("pi.bash", { command: "fixture only; not executed in a shell" }, context)).toMatchObject({ output: "patched by hook" });
    expect(await registry.invoke("extensions.fails", {}, context)).toMatchObject({ text: "recovered by hook", isError: false });
    await expect(registry.invoke("extensions.blocked", {}, context)).rejects.toThrow("guard blocked");
    expect(executed).toEqual(["progress", "bash", "fails"]);
    expect(events.every(event => event.parentToolCallId === context.parentToolCallId)).toBe(true);
    for (const name of ["ls", "progress", "bash", "fails", "blocked"]) {
      const calls = events.filter(event => event.toolName === name);
      expect(calls.map(event => event.type)).toEqual([
        "tool_execution_start", "tool_call",
        ...(["progress", "bash"].includes(name) ? ["tool_execution_update"] : []),
        "tool_result", "tool_execution_end",
      ]);
      expect(new Set(calls.map(event => event.toolCallId)).size).toBe(1);
    }
    const before = events.length;
    await createShellCommandPreflight(catalog)("printf preflight-only", context, "state.verify", 0);
    expect(events.slice(before)).toEqual([expect.objectContaining({
      type: "tool_call", toolName: "bash", parentToolCallId: context.parentToolCallId,
    })]);
  } finally {
    await registry?.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 30_000);

// Exercise the model-issued outer call and Pi's native nested-call recorder as
// well as Fabric's replayed events. Native dispatch must keep owning its IDs,
// nestedCalls record, and usage while program-local provider values stay intact.
it("keeps parent attribution and native nestedCalls/usage in a real mixed AgentSession program", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-mixed-parent-"));
  const parent = "call-mixed-parent";
  const events: Array<{ type: string; toolName: string; toolCallId: string; parentToolCallId?: string }> = [];
  const capped: string[] = [];
  let api!: ExtensionAPI;
  let service!: FabricExecutionService;
  let execution: FabricExecutionResult | undefined;
  let requests = 0;
  const settingsManager = SettingsManager.inMemory({
    defaultProjectTrust: "never", compaction: { enabled: false }, retry: { enabled: false },
  });
  const loader = new DefaultResourceLoader({
    cwd: root, agentDir: root, settingsManager,
    noExtensions: true, noSkills: true, noThemes: true, noPromptTemplates: true,
    agentsFilesOverride: () => ({ agentsFiles: [] }),
    extensionFactories: [pi => {
      api = pi;
      pi.registerTool({
        name: "fixture_native", label: "Native fixture", description: "Native nested fixture",
        parameters: Type.Object({ value: Type.String() }),
        outputSchema: Type.Object({ value: Type.String() }),
        async execute(_id, args, _signal, onUpdate) {
          onUpdate?.({ content: [{ type: "text", text: "native progress" }], details: {} });
          return {
            content: [{ type: "text", text: args.value }], structuredContent: { value: args.value }, details: {},
            usage: { input: 7, output: 11, cacheRead: 0, cacheWrite: 0, totalTokens: 18,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
          };
        },
      });
      pi.registerTool({
        name: "fabric_exec", exposure: "model-only", label: "Fabric fixture", description: "Mixed Fabric program",
        parameters: Type.Object({}),
        async execute(id, _args, signal, _onUpdate, context) {
          execution = await service.execute({
            code: `const rows = await mesh.read({topic: "mixed-parent"});
const native = await tools.call({ref: "extensions.fixture_native", args: {value: "native original"}}) as {text: string; structuredContent: {value: string}};
const listing = await pi.ls({path: ${JSON.stringify(root)}});
return {isArray: Array.isArray(rows), count: rows.length, bodyChars: rows[0]?.data?.body.length,
  nativeText: native.text, nativeValue: native.structuredContent.value, listedMarker: listing.includes("marker.txt")};`,
            context, signal, parentToolCallId: id, onPartial() {},
          });
          return { content: [{ type: "text", text: JSON.stringify(execution.value) }], details: { success: execution.success } };
        },
      });
      pi.on("tool_execution_start", event => { events.push(event); });
      pi.on("tool_execution_update", event => { events.push(event); });
      pi.on("tool_execution_end", event => { events.push(event); });
      pi.on("tool_call", event => { events.push(event); });
      pi.on("tool_result", event => {
        events.push(event);
        if (event.toolName === "fixture_native") return {
          content: [{ type: "text", text: "native patched" }], structuredContent: { value: "native patched" },
        };
        if (event.parentToolCallId) return;
        const text = event.content.filter(part => part.type === "text").map(part => part.text).join("\n");
        if (Buffer.byteLength(text, "utf8") > 64 * 1024) {
          capped.push(event.toolCallId);
          return { content: [{ type: "text", text: "guard capped top-level output" }] };
        }
      });
      pi.registerProvider("offline-nested-parent", {
        api: "offline-nested-parent", apiKey: "offline", baseUrl: "http://invalid.local",
        models: [{ id: "fixture", name: "Fixture", reasoning: false, input: ["text"],
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 2048 }],
        streamSimple(model) {
          const call = requests++ === 0;
          const message: AssistantMessage = {
            role: "assistant", content: call
              ? [{ type: "toolCall", id: parent, name: "fabric_exec", arguments: {} }]
              : [{ type: "text", text: "done" }],
            api: model.api, provider: model.provider, model: model.id, timestamp: Date.now(),
            stopReason: call ? "toolUse" : "stop",
            usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
          };
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "start", partial: message });
          stream.push({ type: "done", reason: call ? "toolUse" : "stop", message });
          stream.end();
          return stream;
        },
      });
    }],
  });
  let registry: ActionRegistry | undefined;
  let session: Awaited<ReturnType<typeof createAgentSession>>["session"] | undefined;
  try {
    fs.writeFileSync(path.join(root, "marker.txt"), "mixed program marker");
    await loader.reload();
    expect(loader.getExtensions().errors).toEqual([]);
    const modelRuntime = await ModelRuntime.create({
      authPath: path.join(root, "auth.json"), modelsPath: null,
      modelsStorePath: path.join(root, "models.json"), refreshOnCreate: false,
    });
    ({ session } = await createAgentSession({
      cwd: root, agentDir: root, settingsManager, modelRuntime,
      resourceLoader: loader, sessionManager: SessionManager.inMemory(root),
    }));
    await session.bindExtensions({});
    await session.setModel(modelRuntime.getModel("offline-nested-parent", "fixture")!);
    api.setActiveTools(["fabric_exec", "fixture_native", "ls"]);
    const runner = session.extensionRunner;
    const catalog = new CapturedToolCatalog();
    const config = normalizeFabricConfig({
      fullCodeMode: true, approvals: { read: "allow", execute: "allow" },
      executor: { maxNestedResultChars: 256 * 1024 },
    });
    catalog.replace(runner.getAllRegisteredTools(), runner, config.capture, "/fabric/index.ts");
    const captured = new CapturedToolsProvider(catalog);
    registry = new ActionRegistry(new FabricToolResultProxy(() => runner));
    registry.register(captured);
    registry.register(new PiToolsProvider(root, catalog, captured));
    const identity: MeshIdentity = { id: "session:mixed-parent", kind: "main", name: "main", sessionId: "mixed-parent" };
    const store = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
    const body = "mixed program data! ".repeat(4 * 1024);
    await store.publish({ topic: "mixed-parent", from: identity, data: { body } });
    registry.register(new MeshProvider(store, identity, { list: () => [], get: () => undefined } as unknown as FabricParticipantSource));
    service = new FabricExecutionService(registry, config);
    await session.prompt("Run the mixed Fabric program.");
    expect(requests).toBe(2);
    expect(execution?.success, execution?.error).toBe(true);
    expect(execution?.value).toEqual({
      isArray: true, count: 1, bodyChars: body.length,
      nativeText: "native patched", nativeValue: "native patched", listedMarker: true,
    });
    expect(capped).toEqual([]);
    const nested = events.filter(event => ["mesh.read", "fixture_native", "ls"].includes(event.toolName));
    expect(nested.every(event => event.parentToolCallId === parent)).toBe(true);
    const results = nested.filter(event => event.type === "tool_result");
    expect(results.map(event => event.toolName)).toEqual(["mesh.read", "fixture_native", "ls"]);
    expect(new Set(results.map(event => event.toolCallId)).size).toBe(3);
    for (const name of ["mesh.read", "ls"]) {
      const matching = nested.filter(event => event.toolName === name);
      expect(matching.every(event => event.toolCallId.startsWith(FABRIC_NESTED_TOOL_CALL_ID_PREFIX))).toBe(true);
      expect(new Set(matching.map(event => event.toolCallId)).size).toBe(1);
    }
    const nativeEvents = nested.filter(event => event.toolName === "fixture_native");
    expect(nativeEvents.map(event => event.type)).toEqual([
      "tool_execution_start", "tool_call", "tool_execution_update", "tool_result", "tool_execution_end",
    ]);
    expect(nativeEvents.every(event => event.toolCallId === `${parent}/1`)).toBe(true);
    const outer = session.messages.find(message => message.role === "toolResult" && message.toolCallId === parent);
    expect(outer).toMatchObject({
      isError: false,
      nestedCalls: { calls: [{ id: `${parent}/1`, name: "fixture_native", status: "ok", arguments: { value: "native original" } }], complete: true },
      usage: { input: 7, output: 11, totalTokens: 18 },
    });
  } finally {
    await registry?.close();
    session?.dispose();
    fs.rmSync(root, { recursive: true, force: true });
  }
}, 30_000);
