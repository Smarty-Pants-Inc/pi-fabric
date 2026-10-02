import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createBashToolDefinition,
  createReadToolDefinition,
  createSyntheticSourceInfo,
  ExtensionRunner,
  type ExtensionContext,
  type RegisteredTool,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { describe, expect, it, vi } from "vitest";
import { prepareFabricActorHostPayload } from "../src/actors/host-event-payload.js";
import { ActorManager } from "../src/actors/manager.js";
import type { AgentManager } from "../src/agents/manager.js";
import { buildActorContext } from "../src/actors/context.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, type FabricAgentRunner } from "../src/config.js";
import { ActionRegistry, type FabricCallAudit } from "../src/core/action-registry.js";
import { MeshStore } from "../src/mesh/store.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { TranscriptAccumulator } from "../src/ui/transcript-parser.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";

vi.mock("@earendil-works/pi-coding-agent", async importOriginal => {
  const host = await importOriginal<typeof import("@earendil-works/pi-coding-agent")>();
  return { ...host, createReadToolDefinition: vi.fn(host.createReadToolDefinition) };
});

const cwd = process.cwd();
// Deliberately not a credential-shaped token: downstream generic sanitizers
// must not mask a failure at the tool_result redaction boundary.
const secret = "private-review-output";
const clean = "[withheld]";
type ResultEvent = Parameters<ExtensionRunner["emitToolResult"]>[0];
type ResultPatch = Awaited<ReturnType<ExtensionRunner["emitToolResult"]>>;
const context = {
  cwd, signal: new AbortController().signal, parentToolCallId: "parent",
  nestedToolCallId: "fabric_redaction", extensionContext: { cwd, sessionManager: { getSessionId: () => "redaction-test", getSessionFile: () => undefined } } as unknown as ExtensionContext,
  update: vi.fn(), approve: vi.fn(async () => {}), audits: [], maxResultChars: 100_000,
};

function setup(tools: RegisteredTool[], handler: (event: ResultEvent) => ResultPatch) {
  const events: Array<Record<string, any>> = [];
  const postHook: ResultEvent[] = [];
  const middlewareRunner = {
    createContext: () => ({ cwd, sessionManager: { getSessionId: () => "redaction-test", getSessionFile: () => undefined } }),
    extensions: [{ path: "/extensions/redaction.ts", handlers: new Map([["tool_result", [
      handler,
      (event: ResultEvent) => { postHook.push({ ...event }); },
    ]]]) }],
    emitError: (error: unknown) => { throw new Error(JSON.stringify(error)); },
  } as unknown as ExtensionRunner;
  const runner = {
    createContext: () => ({ cwd, sessionManager: { getSessionId: () => "redaction-test", getSessionFile: () => undefined } }), getActiveTools: () => [],
    emit: vi.fn(async (event: Record<string, any>) => { events.push(event); }),
    emitToolCall: vi.fn(async () => undefined),
    emitToolResult: vi.fn((event: ResultEvent) => ExtensionRunner.prototype.emitToolResult.call(middlewareRunner, event)),
  } as unknown as ExtensionRunner;
  const catalog = new CapturedToolCatalog();
  catalog.replace(tools, runner, DEFAULT_FABRIC_CONFIG.capture, "/extensions/fabric.ts");
  return { catalog, events, postHook, runner };
}

const registered = (definition: RegisteredTool["definition"]): RegisteredTool => ({
  definition, sourceInfo: createSyntheticSourceInfo("/extensions/fixture.ts", { source: "test" }),
});
const fixture = (structuredContent: unknown, details: unknown = { output: secret }, text = secret) => registered({
  name: "structured_fixture", label: "Structured fixture", description: "redaction fixture",
  parameters: Type.Object({}), outputSchema: Type.Object({ output: Type.String() }),
  async execute() {
    return { content: [{ type: "text" as const, text }], structuredContent, details } as any;
  },
});

async function assertConsumers(event: Record<string, any>) {
  const message = { ...event.result, role: "toolResult", toolName: event.toolName, toolCallId: event.toolCallId, isError: event.isError };
  const turn = { type: "turn_end", toolResults: [message] };
  const transcript = new TranscriptAccumulator();
  transcript.append([event, { type: "message_end", message }, turn]);
  const actorContext = buildActorContext([{ type: "message", message }], 10, 40_000);
  const prepared = prepareFabricActorHostPayload({ event: event.type, signal: { payload: event }, ...actorContext }, 40_000);
  // Persist the exact payload used for actor mailbox/model input and mesh
  // delivery, not a separately redacted copy of the original execution result.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-redaction-"));
  try {
    const mesh = new MeshStore(root, 100_000, 10);
    await mesh.publish({ topic: "host:tool_execution_end", from: { id: "main", name: "Main", kind: "agent" }, data: prepared.payload });
    for (const consumer of [event, turn, actorContext, prepared.payload, transcript.snapshot(), mesh.tail(0, 10)]) {
      expect(JSON.stringify(consumer)).not.toContain(secret);
    }
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}

describe("thrown non-shell error blank redaction", () => {
  it.each(["native", "captured", "outputSchema"])("%s guest rejection, outer audit/final result and nested consumers never restore the exception", async kind => {
    for (const content of [[], [{ type: "text" as const, text: "" }], [{ type: "text" as const, text: " \t\n " }]]) {
      const name = kind === "native" ? "read" : "throwing_fixture";
      if (kind === "native") {
        // Throw from a real native definition's execute, not a captured override
        // or a hook. Input arguments deliberately do not contain the marker.
        const native = createReadToolDefinition(cwd);
        vi.mocked(createReadToolDefinition).mockReturnValueOnce({ ...native, async execute() { throw new Error(secret); } });
      }
      const definition = registered({
        name, label: "Thrown fixture", description: "Private error fixture", parameters: Type.Object({}),
        ...(kind === "outputSchema" ? { outputSchema: Type.Object({ output: Type.String() }) } : {}),
        async execute() { throw new Error(secret); },
      });
      const s = setup(kind === "native" ? [] : [definition], () => ({ content }));
      const provider = kind === "native"
        ? new PiToolsProvider(cwd, s.catalog, new CapturedToolsProvider(s.catalog))
        : new CapturedToolsProvider(s.catalog);
      const registry = new ActionRegistry(); registry.register(provider);
      const ref = kind === "native" ? "pi.read" : "extensions.throwing_fixture";
      const args = kind === "native" ? { path: "README.md" } : {};
      const audits: FabricCallAudit[] = [];
      const observations: unknown[] = [];
      const final = await new QuickJsRuntime().execute(
        `return await tools.call({ ref: ${JSON.stringify(ref)}, args: ${JSON.stringify(args)} });`,
        (action, input) => registry.invoke(action === "fabric.$call" ? input.ref as string : action, action === "fabric.$call" ? input.args as Record<string, unknown> : input, { ...context, audits, observeInvocation: event => { observations.push(event); } }),
        { timeoutMs: 10_000, memoryLimitBytes: 32 * 1024 * 1024 },
      );
      const generic = kind === "native" ? "Pi tool read failed" : "Captured tool throwing_fixture failed";
      expect(final.terminationReason).toBe("runtime_error");
      expect(final.error).toContain(generic);
      expect(audits).toHaveLength(1);
      expect(audits[0]?.error).toBe(generic);
      expect(vi.mocked(s.runner.emitToolResult).mock.calls[0]?.[0].content).toEqual([{ type: "text", text: secret }]);
      const end = s.events.find(event => event.type === "tool_execution_end")!;
      expect(end.isError).toBe(true);
      expect(end.result.content).toEqual(content);
      for (const publicValue of [final, audits, observations, s.postHook]) expect(JSON.stringify(publicValue)).not.toContain(secret);
      await assertConsumers(end);
      if (provider instanceof PiToolsProvider) await provider.close();
    }
  }, 30_000);
});
describe("nested tool_result structured redaction", () => {
  it.each([false, true])("native/captured=%s shell output never survives content-only redaction", async (captured) => {
    for (const exitCode of [0, 7]) {
      for (const explicit of [false, true]) {
        const s = setup(captured ? [registered(createBashToolDefinition(cwd) as RegisteredTool["definition"])] : [], event => ({
          content: [{ type: "text", text: clean }],
          ...(explicit ? { structuredContent: { output: clean, exitCode } } : {}),
        }));
        const provider = new PiToolsProvider(cwd, s.catalog, new CapturedToolsProvider(s.catalog));
        const value = await provider.invoke("bash", { command: `printf '%s%s' 'private-review-' 'output'; exit ${exitCode}` }, context).catch(error => error);
        expect(value instanceof Error ? value.message : JSON.stringify(value)).not.toContain(secret);
        expect(vi.mocked(s.runner.emitToolResult).mock.calls[0]?.[0]).toMatchObject({ structuredContent: { output: secret, exit_code: exitCode } });
        expect(s.postHook).toHaveLength(1);
        expect(JSON.stringify(s.postHook)).not.toContain(secret);
        const end = s.events.find(event => event.type === "tool_execution_end")!;
        expect(end.isError).toBe(exitCode !== 0);
        await assertConsumers(end);
        if (explicit) expect(end.result.structuredContent).toEqual({ output: clean, exitCode });
        else expect(end.result).not.toHaveProperty("structuredContent");
      }
    }
  });

  it.each([false, true])("captured outputSchema content/details redaction (explicit structured=%s)", async explicit => {
    const s = setup([fixture({ output: secret })], event => ({
      content: [{ type: "text", text: clean }], details: { output: clean, count: 2 },
      ...(explicit ? { structuredContent: { output: clean } } : {}),
    }));
    const value = await new CapturedToolsProvider(s.catalog).invoke("structured_fixture", {}, context);
    expect(value).toMatchObject({ text: clean, details: { output: clean, count: 2 }, isError: false });
    expect(JSON.stringify(s.postHook)).not.toContain(secret);
    const end = s.events.find(event => event.type === "tool_execution_end")!;
    if (explicit) expect(end.result.structuredContent).toEqual({ output: clean });
    else expect(end.result).not.toHaveProperty("structuredContent");
    await assertConsumers(end);
  });

  it("drops inherited details as well as structured data after content-only redaction", async () => {
    const s = setup([fixture({ output: secret })], () => ({ content: [{ type: "text", text: clean }] }));
    const value = await new CapturedToolsProvider(s.catalog).invoke("structured_fixture", {}, context);
    expect(JSON.stringify(value)).not.toContain(secret);
    expect(JSON.stringify(s.postHook)).not.toContain(secret);
    await assertConsumers(s.events.find(event => event.type === "tool_execution_end")!);
  });

  it("replaces retained failed-shell progress in the outer Fabric audit envelope", async () => {
    const s = setup([], () => ({ content: [{ type: "text", text: clean }] }));
    const registry = new ActionRegistry();
    registry.register(new PiToolsProvider(cwd, s.catalog, new CapturedToolsProvider(s.catalog)));
    const audits: FabricCallAudit[] = [];
    await expect(registry.invoke("pi.bash", { command: "printf '%s%s' 'private-review-' 'output'; exit 7" }, { ...context, audits })).rejects.toThrow(clean);
    expect(audits).toHaveLength(1);
    expect(audits[0]?.preview).toMatchObject({ result: clean });
    expect(JSON.stringify(audits)).not.toContain(secret);
  });

  it("preserves useful non-secret structured output and Fabric's captured envelope", async () => {
    const s = setup([fixture({ output: "public", values: [1, true, null] }, { count: 3 }, "public output")], () => undefined);
    const value = await new CapturedToolsProvider(s.catalog).invoke("structured_fixture", {}, context);
    expect(value).toEqual({ content: [{ type: "text", text: "public output" }], text: "public output", details: { count: 3 }, isError: false, source: s.catalog.require("structured_fixture").sourceInfo });
    expect(s.postHook[0]?.structuredContent).toEqual({ output: "public", values: [1, true, null] });
    expect(s.events.at(-1)?.result.structuredContent).toEqual({ output: "public", values: [1, true, null] });
  });

  it("keeps Pi 0.87 content/details/status patch behavior unchanged", async () => {
    const definition = fixture(undefined, { window: 1 }).definition;
    delete definition.outputSchema;
    const s = setup([registered(definition)], () => ({ content: [{ type: "text", text: clean }] }));
    expect(await new CapturedToolsProvider(s.catalog).invoke("structured_fixture", {}, context)).toMatchObject({ text: clean, details: { window: 1 }, isError: false });
  });

  it.each([false, true])("fans out only effective output to transferred actor mesh subscribers (explicit=%s)", async explicit => {
    const s = setup([fixture({ output: secret })], () => ({
      content: [{ type: "text", text: clean }],
      ...(explicit ? { structuredContent: { output: clean }, details: { output: clean } } : {}),
    }));
    await new CapturedToolsProvider(s.catalog).invoke("structured_fixture", {}, context);
    const prepared = prepareFabricActorHostPayload(s.events.at(-1)!, 40_000);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-redaction-fanout-"));
    const mesh = new MeshStore(path.join(root, "mesh"), 100_000, 100);
    let owned = true;
    // No worker or agent is started: transferred subscribers receive the
    // normal durable host-event relay, with a trap on accidental execution.
    const run = vi.fn(async () => { throw new Error("must not start an agent"); });
    const agents = {
      config: DEFAULT_FABRIC_CONFIG.agents,
      // Match AgentManager's configured default selection without starting a worker.
      defaultModel(this: Pick<AgentManager, "config">, runner: FabricAgentRunner = this.config.runner): string | undefined {
        return runner === "claude" ? this.config.claude.model
          : runner === "veda" ? this.config.veda.model : this.config.model;
      },
      resolveKernel: () => undefined,
      run,
    } as unknown as AgentManager;
    const actors = new ActorManager("redaction", { id: "main", name: "Main", kind: "agent" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true }, agents, () => {},
      { actorRoot: path.join(root, "actors"), actorScope: "session", canManageActor: () => owned });
    try {
      const subscribers = await Promise.all(["one", "two"].map(name => actors.create({
        name, instructions: "fixture", events: ["tool_execution_end"], residency: "durable", delivery: "mailbox",
      })));
      owned = false;
      expect(actors.dispatchObservedHostEvent("tool_execution_end", prepared.payload)).toBe(2);
      await vi.waitFor(() => expect(mesh.tail(0, 100).events.filter(event => event.topic === "fabric.actor.host-event")).toHaveLength(2));
      const relays = mesh.tail(0, 100).events.filter(event => event.topic === "fabric.actor.host-event");
      expect(new Set(relays.map(event => event.to))).toEqual(new Set(subscribers.map(actor => actor.id)));
      expect(JSON.stringify(relays)).not.toContain(secret);
      expect(JSON.stringify(relays)).toContain(clean);
      expect(run).not.toHaveBeenCalled();
    } finally {
      owned = true;
      await actors.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("allows explicit structured-only replacement without changing the content envelope", async () => {
    const s = setup([fixture({ output: secret }, { count: 2 })], () => ({ structuredContent: { output: clean } }));
    const value = await new CapturedToolsProvider(s.catalog).invoke("structured_fixture", {}, context);
    expect(value.text).toBe(secret);
    expect(s.events.at(-1)?.result.structuredContent).toEqual({ output: clean });
    expect(s.events.at(-1)?.result.details).toBeUndefined();
  });

  it("drops opaque structured/details replacements supplied by middleware", async () => {
    const opaque = { toJSON: () => ({ output: secret }) };
    const s = setup([fixture({ output: "public" }, { count: 1 })], () => ({
      content: [{ type: "text", text: clean }], structuredContent: opaque as any, details: opaque,
    }));
    const value = await new CapturedToolsProvider(s.catalog).invoke("structured_fixture", {}, context);
    expect(JSON.stringify(value)).not.toContain(secret);
    expect(s.events.at(-1)?.result).not.toHaveProperty("structuredContent");
    await assertConsumers(s.events.at(-1)!);
  });

  it.each(["function", "class", "cycle", "getter", "toJSON", "deep"])("fails closed for unknown structured %s shapes", async shape => {
    const cycle: Record<string, unknown> = {}; cycle.self = cycle;
    let deep: unknown = { output: secret };
    for (let i = 0; i < 100; i++) deep = { nested: deep };
    const shapes: Record<string, unknown> = {
      function: { output: () => secret }, class: new (class { output = secret; })(), cycle,
      getter: { get output() { throw new Error(secret); } }, toJSON: { toJSON: () => ({ output: secret }) }, deep,
    };
    const s = setup([fixture(shapes[shape], shapes[shape])], () => ({ content: [{ type: "text", text: clean }] }));
    const value = await new CapturedToolsProvider(s.catalog).invoke("structured_fixture", {}, context);
    expect(JSON.stringify(value)).not.toContain(secret);
    expect(s.postHook[0]).not.toHaveProperty("structuredContent");
    expect(s.postHook[0]?.details).toBeUndefined();
    const end = s.events.at(-1)!;
    expect(end.result).not.toHaveProperty("structuredContent");
    await assertConsumers(end);
  });
});
