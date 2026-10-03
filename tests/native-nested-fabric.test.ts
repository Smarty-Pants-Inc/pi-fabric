import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AgentTool, AgentToolCallOutcome } from "@earendil-works/pi-agent-core";
import {
  AgentSession, ExtensionRunner, createSyntheticSourceInfo, defineTool, wrapRegisteredTool,
  type ExtensionContext, type ToolCallEvent, type ToolResultEvent,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricToolLifecycle } from "../src/core/tool-ownership.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { createFabricExecTool } from "../src/fabric-exec-tool.js";
import type { FabricState } from "../src/fabric-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import { PrewalkController } from "../src/prewalk/controller.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import { defaultCodePreviewSettings } from "../src/ui/code-preview.js";

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};

// Keep Pi's actual createToolContext, registered-tool wrapper, AgentSession
// nested runner, runToolCall, and before/after hooks. Only session services and
// the model are fixtures: no reimplementation of the native tool pipeline.
const harness = (root: string) => {
  const lifecycle = new FabricToolLifecycle(() => true, () => undefined);
  const context = { cwd: root, hasUI: false, sessionManager: { getSessionId: () => "native-refusal" } } as ExtensionContext;
  const runner = Object.create(ExtensionRunner.prototype) as ExtensionRunner;
  const events: Array<{ type: string; toolName?: string; parentToolCallId?: string }> = [];
  const handlers = new Map([
    ["tool_call", [(event: ToolCallEvent) => lifecycle.toolCall(event, context)]],
    ["tool_result", [(event: ToolResultEvent) => lifecycle.toolResult(event)]],
  ]);
  Object.assign(runner, {
    extensions: [{ path: "/extensions/pi-fabric/index.ts", handlers }],
    createContext: () => Object.create(context),
    getActiveTools: () => [],
    errorListeners: new Set(),
  });
  const session = Object.create(AgentSession.prototype) as AgentSession;
  // The public native context calls the real private session entrypoint.
  const nativeSession = session as unknown as {
    _executeNestedToolCall: (id: string, name: string, args: unknown, options: { signal?: AbortSignal }) => Promise<AgentToolCallOutcome>;
  };
  let tools: AgentTool[] = [];
  Object.assign(session, {
    _extensionRunner: runner,
    _getCallableTools: () => tools,
    _findLastAssistantMessage: () => ({ role: "assistant", content: [] }),
    _limitsModel: () => undefined,
    settingsManager: { getImageAutoResize: () => false },
    agent: { toolExecution: "parallel", state: { messages: [], tools: [] } },
    _emit: (event: typeof events[number]) => { events.push(event); },
  });
  Object.assign(runner, {
    executeToolFn: (id: string, name: string, args: unknown, options: { signal?: AbortSignal }) =>
      nativeSession._executeNestedToolCall(id, name, args, options),
    getCallableToolsFn: () => tools,
  });
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.fullCodeMode = true;
  config.schema.mode = "off";
  for (const risk of ["read", "write", "execute", "agent"] as const) config.approvals[risk] = "allow";
  const registry = new ActionRegistry();
  const store = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity = { id: "session:native-refusal", name: "main", kind: "main" as const, sessionId: "native-refusal" };
  registry.register(new MeshProvider(store, identity, {} as FabricParticipantSource));
  const state = {
    config, ensure: async () => {}, claimHandoff: async () => undefined,
    prewalk: new PrewalkController(), execution: new FabricExecutionService(registry, config),
  } as unknown as FabricState;
  const fabric = lifecycle.bindExecution(createFabricExecTool(state, defaultCodePreviewSettings(), new Map(), tool => tool));
  const register = (definitions: Parameters<typeof wrapRegisteredTool>[0]["definition"][]) => {
    const registered = [fabric, ...definitions].map(definition => ({
      definition, sourceInfo: createSyntheticSourceInfo(`/extensions/${definition.name}/index.ts`, { source: "test" }),
    }));
    tools = registered.map(tool => wrapRegisteredTool(tool, runner));
    const catalog = new CapturedToolCatalog();
    catalog.replace(registered.slice(1), runner, config.capture, "/extensions/pi-fabric/index.ts");
    registry.register(new CapturedToolsProvider(catalog));
  };
  const execute = async (code: string, signal?: AbortSignal) => {
    const id = "outer";
    const args = { code, resultFormat: "json" as const };
    const admission = await runner.emitToolCall({ type: "tool_call", toolCallId: id, toolName: "fabric_exec", input: args });
    if (admission?.block) throw new Error(admission.reason);
    return fabric.execute(id, args, signal, undefined, runner.createToolContext(id, signal));
  };
  return { lifecycle, runner, registry, store, events, register, execute };
};

describe("Native nested Fabric execution refusal", () => {
  it.each((["settle", "abort", "clear"] as const).flatMap(end =>
    [false, true].map(independentSignal => ({ end, independentSignal })),
  ))("refuses an unjoined child after $end (independentSignal=$independentSignal)", async ({ end, independentSignal }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-refusal-"));
    const h = harness(root);
    const release = deferred();
    const entered = deferred();
    const controller = new AbortController();
    let child: Promise<AgentToolCallOutcome> | undefined;
    const hold = vi.fn(async () => { entered.resolve(); await release.promise; return "released"; });
    h.registry.register({
      name: "hold", description: "read-only test barrier",
      async list() { return [{ name: "wait", description: "wait", inputSchema: { type: "object", properties: {}, additionalProperties: false }, risk: "read" }]; },
      async describe() { return { name: "wait", description: "wait", inputSchema: { type: "object", properties: {}, additionalProperties: false }, risk: "read" }; },
      invoke: hold,
    });
    h.register([defineTool({
      name: "start_unjoined", label: "start", description: "Start an unjoined native child", parameters: Type.Object({}),
      async execute(_id, _args, _signal, _update, ctx) {
        child = ctx.executeTool("fabric_exec", {
          code: 'await tools.call({ref:"hold.wait",args:{}}); await mesh.put({key:"forbidden",value:1}); return await mesh.publish({topic:"forbidden",data:{late:true}});',
          resultFormat: "json",
        }, independentSignal ? { signal: new AbortController().signal } : undefined);
        // On the vulnerable head wait until the genuine child has entered
        // its runtime. On the fixed head the refused child settles instead.
        // Do not join a running child before the outer operation ends.
        await Promise.race([entered.promise, child.then(() => {})]);
        if (end === "abort") controller.abort();
        if (end === "clear") h.lifecycle.clear();
        return { content: [{ type: "text", text: "child launched" }], details: {} };
      },
    })]);
    try {
      const outer = await h.execute('await tools.call({ref:"extensions.start_unjoined",args:{}}); return "outer settled";', controller.signal);
      release.resolve();
      expect(child, JSON.stringify(outer)).toBeDefined();
      const outcome = await child!;
      if (end === "abort") expect(outer).toMatchObject({ isError: true });
      else expect(outer).not.toMatchObject({ isError: true });
      expect({
        state: await h.store.get("forbidden"),
        events: await h.store.read({ topic: "forbidden" }),
      }).toEqual({ state: undefined, events: [] });
      expect(outcome.isError).toBe(true);
      expect(JSON.stringify(outcome.result)).toContain("Native nested fabric_exec is disabled");
      expect(hold).not.toHaveBeenCalled();
      expect(h.events).toContainEqual(expect.objectContaining({ type: "tool_execution_end", toolName: "fabric_exec", parentToolCallId: expect.any(String), isError: true }));
    } finally {
      release.resolve();
      await child;
      h.lifecycle.clear();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("preserves top-level Fabric, Fabric-mediated captured calls, and other native nested tools", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-controls-"));
    const h = harness(root);
    const echo = defineTool({ name: "native_echo", label: "echo", description: "native echo", exposure: "codemode", parameters: Type.Object({}),
      async execute() { return { content: [{ type: "text", text: "native echo ok" }], details: {} }; },
    });
    h.register([echo, defineTool({ name: "captured_echo", label: "captured", description: "captured native caller", parameters: Type.Object({}),
      async execute(_id, _args, _signal, _update, ctx) {
        const nested = await ctx.executeTool("native_echo", {});
        expect(nested.isError).toBe(false);
        return nested.result;
      },
    })]);
    try {
      const result = await h.execute('const echoed = await tools.call({ref:"extensions.captured_echo",args:{}}); await mesh.put({key:"allowed",value:42}); await mesh.publish({topic:"allowed",data:{ok:true}}); return {answer:42,echoed};');
      expect(result, JSON.stringify(result)).not.toMatchObject({ isError: true });
      expect(JSON.stringify(result.content)).toContain("native echo ok");
      expect(await h.store.get("allowed")).toMatchObject({ value: 42 });
      expect(await h.store.read({ topic: "allowed" })).toHaveLength(1);
      expect(h.events).toContainEqual(expect.objectContaining({ type: "tool_execution_end", toolName: "native_echo", parentToolCallId: expect.any(String), isError: false }));
    } finally {
      h.lifecycle.clear();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
