import { createSyntheticSourceInfo, defineTool, type ExtensionContext, type ExtensionRunner, type RegisteredTool } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { FabricActivityStore } from "../src/activity/store.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { isSelectedNativeMcpTool } from "../src/core/native-mcp-identity.js";
import { FabricExecutionTraceRecorder } from "../src/audit/trace.js";
import { clearActiveCompiledSurface, setActiveCompiledSurface } from "../src/entropy/active.js";
import { deriveNormalFormPlan } from "../src/entropy/normal-form.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { McpProvider, mcpToolDescriptor } from "../src/providers/mcp-provider.js";
import { PiNativeMcpTools } from "../src/providers/pi-native-mcp.js";

const ambient = vi.hoisted(() => ({ createRuntime: vi.fn(async () => ({ listServers: () => [] as string[], close: async () => {} })) }));
vi.mock("mcporter", () => ambient);
const inputSchema = Type.Object({ value: Type.String(), count: Type.Optional(Type.Integer({ minimum: 1 })) }, { additionalProperties: false });
const outputSchema = Type.Object({ value: Type.String() });
const rawContent = [{ type: "text" as const, text: "complete result" }];

function fixture(names = ["find.item"], servers = ["docs-api"]) {
  const catalog = new CapturedToolCatalog();
  const direct = vi.fn(async () => { throw new Error("Native execute must go through Pi"); });
  let registered: RegisteredTool[] = names.map((tool, i) => ({
    definition: defineTool({
      name: `mcp__hashed_${i}`, label: `docs-api/${tool}`, description: `Precise description: ${tool}`,
      namespace: { name: "mcp__docs-api", description: "Server instructions" }, exposure: "codemode",
      parameters: inputSchema, outputSchema: Type.Object({ content: Type.Array(Type.Any()), structuredContent: outputSchema }),
      annotations: { readOnlyHint: true, destructiveHint: false }, execute: direct,
    }),
    sourceInfo: createSyntheticSourceInfo("/builtin/mcp.ts", { source: "builtin:mcp" }),
  }));
  let active = registered.map(tool => tool.definition.name);
  const runner = { getActiveTools: () => active, createContext: () => ({ cwd: process.cwd() }) } as unknown as ExtensionRunner;
  const replace = (next = registered, enabled = true) => {
    registered = next;
    catalog.replace(registered, runner, { ...DEFAULT_FABRIC_CONFIG.capture, enabled }, "/fabric/index.ts");
  };
  replace();
  const native = new PiNativeMcpTools(catalog, servers, 500);
  const config = { ...DEFAULT_FABRIC_CONFIG.mcp, nativeServers: servers, cache: { ...DEFAULT_FABRIC_CONFIG.mcp.cache, enabled: false } };
  const provider = new McpProvider(process.cwd(), config, { native });
  const executeTool = vi.fn(async (_name: string, args: unknown, options: { signal?: AbortSignal; onUpdate?: (result: unknown) => void }) => {
    options.onUpdate?.({ content: [{ type: "text", text: "progress" }], details: {} });
    return {
      toolCall: { arguments: args }, isError: false,
      result: { content: [{ type: "text", text: "truncated model text" }], details: {},
        structuredContent: { content: rawContent, structuredContent: { value: "answer" } } },
    };
  });
  const context = {
    cwd: process.cwd(), signal: undefined as AbortSignal | undefined, parentToolCallId: "outer", nestedToolCallId: "inner",
    extensionContext: { tools: registered.map(tool => tool.definition), executeTool } as unknown as ExtensionContext,
    update: vi.fn(), updateArguments: vi.fn(), attachMedia: vi.fn(), approve: vi.fn(async () => {}), audits: [], maxResultChars: 100_000,
  };
  const registry = new ActionRegistry();
  registry.register(provider);
  return { catalog, native, config, provider, context, registry, executeTool, direct, registered, replace,
    setActive: (names: string[]) => { active = names; } };
}

beforeEach(() => { ambient.createRuntime.mockClear(); });
afterEach(() => { clearActiveCompiledSurface(); vi.unstubAllEnvs(); vi.useRealTimers(); });

describe("Pi-owned MCP tools inside Fabric", () => {
  it("preserves raw names, descriptions, schemas, annotations and normalized results without ambient discovery", async () => {
    const f = fixture();
    const listed = await f.provider.list({ namespace: "docs-api" }, f.context);
    expect(listed).toEqual([mcpToolDescriptor("docs-api", {
      name: "find.item", description: "Precise description: find.item",
      inputSchema: inputSchema as unknown as Record<string, unknown>, outputSchema: outputSchema as unknown as Record<string, unknown>,
      annotations: { readOnlyHint: true, destructiveHint: false },
    })]);
    expect(await f.provider.describe("docs-api.find_item", f.context)).toEqual(listed[0]);
    expect(deriveNormalFormPlan("mcp.docs-api.find.item", listed[0]!.inputSchema))
      .toEqual(deriveNormalFormPlan("mcp.docs-api.find.item", inputSchema));
    await expect(f.registry.invoke("mcp.docs-api.find.item", { value: "x" }, f.context)).resolves.toEqual({
      text: "complete result", content: rawContent, structuredContent: { value: "answer" },
    });
    expect(f.executeTool).toHaveBeenCalledExactlyOnceWith("mcp__hashed_0", { value: "x" }, expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(f.context.update).toHaveBeenCalledWith("docs-api.find.item: progress");
    expect(f.direct).not.toHaveBeenCalled();
    expect(ambient.createRuntime).not.toHaveBeenCalled();
  });

  it("preserves the declared execution name and description in the activity UI", async () => {
    const f = fixture();
    const activity = new FabricActivityStore();
    const execution = new FabricExecutionService(f.registry, { ...DEFAULT_FABRIC_CONFIG, mcp: f.config }, activity);
    const result = await execution.execute({
      code: 'return await tools.call({ref: "mcp.docs-api.find.item", args: {value: "x"}});',
      parentToolCallId: "display-test", signal: undefined, onPartial() {},
      display: { name: "Inspect project docs", description: "Verify the documented contract" },
      context: { ...f.context.extensionContext, cwd: process.cwd(), hasUI: false },
    });
    expect(result.success).toBe(true);
    expect(activity.get("display-test")).toMatchObject({
      name: "Inspect project docs", description: "Verify the documented contract", status: "completed",
      calls: [expect.objectContaining({ ref: "mcp.docs-api.find.item", status: "completed" })],
    });
    await f.provider.close();
  });

  it("keeps namespaced discovery query filtering and a conservative network risk", async () => {
    const f = fixture(["find.item", "other"]);
    expect(await f.provider.list({ namespace: "docs_api", query: "Precise description: other" }, f.context))
      .toMatchObject([{ name: "docs-api.other", description: "Precise description: other", risk: "network" }]);
    expect(f.provider.description).toContain("Pi-owned");
    expect((await f.provider.describe("$reload", f.context))?.description).toContain("manage Pi-owned servers with /mcp");
    expect((await f.provider.describe("$servers", f.context))?.name).toBe("$servers");
  });

  it("refuses cross-backend sanitized server collisions without connecting either server", async () => {
    const f = fixture();
    ambient.createRuntime.mockResolvedValueOnce({ listServers: () => ["docs.api"], close: async () => {} });
    await expect(f.provider.describe("docs_api.find_item", f.context)).rejects.toThrow("Ambiguous MCP server alias");
    expect(f.executeTool).not.toHaveBeenCalled();
  });

  it("retains every tool and refuses ambiguous aliases without guessing hashed host names", async () => {
    const f = fixture(["find.item", "find-item", "very/long.".repeat(12)]);
    expect(await f.provider.list({ namespace: "docs-api" }, f.context)).toHaveLength(3);
    await expect(f.provider.describe("docs_api.find_item", f.context)).rejects.toThrow("ambiguous");
    await f.provider.invoke("docs-api.find-item", { value: "x" }, f.context);
    expect(f.executeTool).toHaveBeenCalledWith("mcp__hashed_1", expect.anything(), expect.anything());
    await f.provider.invoke(`docs-api.${"very/long.".repeat(12)}`, { value: "x" }, f.context);
    expect(f.executeTool).toHaveBeenCalledWith("mcp__hashed_2", expect.anything(), expect.anything());
  });

  it("indexes identities once per registration snapshot while keeping descriptions and activation live", async () => {
    const f = fixture(Array.from({ length: 1000 }, (_, i) => `tool-${i}`));
    let identityReads = 0;
    for (const { definition } of f.registered) {
      const label = definition.label;
      Object.defineProperty(definition, "label", { configurable: true, get() { identityReads++; return label; } });
    }
    await f.provider.describe("docs-api.tool-999", f.context);
    const indexedReads = identityReads;
    expect(indexedReads).toBeGreaterThan(0);
    for (let i = 0; i < 10; i++) await f.provider.describe("docs-api.tool-999", f.context);
    expect(identityReads).toBe(indexedReads);
    f.registered[999]!.definition.description = "Updated live description";
    expect(await f.provider.describe("docs-api.tool-999", f.context)).toMatchObject({ description: "Updated live description" });
    f.replace();
    await f.provider.describe("docs-api.tool-999", f.context);
    expect(identityReads).toBeGreaterThan(indexedReads);
  });

  it("never resolves a withdrawn exact name to a different live alias", async () => {
    const f = fixture(["find_item", "find-item"]);
    await f.provider.describe("docs-api.find_item", f.context);
    f.registered[0]!.definition.exposure = "hidden";
    await expect(f.provider.invoke("docs-api.find_item", {}, f.context)).rejects.toThrow("Unknown or ambiguous");
    expect(f.executeTool).not.toHaveBeenCalled();
    expect(await f.provider.describe("docs-api.find-item", f.context)).toMatchObject({ name: "docs-api.find-item" });
  });

  it("keeps extension aliases callable but advertises only the canonical MCP surface", async () => {
    const f = fixture();
    const extensions = new CapturedToolsProvider(f.catalog, entry => isSelectedNativeMcpTool(entry.definition, f.config.nativeServers));
    expect(await extensions.list({}, f.context)).toEqual([]);
    expect(await extensions.describe("mcp__hashed_0", f.context)).toMatchObject({ name: "mcp__hashed_0" });
    await extensions.invoke("mcp__hashed_0", { value: "x" }, f.context);
    expect(f.executeTool).toHaveBeenCalledOnce();
  });

  it("observes withdrawal, late registration, direct activation and capture-disabled mode without a TTL", async () => {
    const f = fixture();
    f.replace(f.registered, false);
    expect(f.catalog.list()).toEqual([]);
    expect(f.native.list()).toHaveLength(1);
    f.replace([{ ...f.registered[0]!, definition: { ...f.registered[0]!.definition, exposure: "hidden" } }]);
    expect(f.native.list()).toEqual([]);
    await expect(f.provider.invoke("docs-api.find.item", {}, f.context)).rejects.toThrow("no mcporter fallback");
    f.replace([{ ...f.registered[0]!, definition: { ...f.registered[0]!.definition, exposure: "direct" } }]);
    f.setActive([]);
    expect(f.native.list()).toEqual([]);
    f.setActive(["mcp__hashed_0"]);
    expect(f.native.list()).toHaveLength(1);
    f.catalog.markSuspended();
    expect(f.native.list()).toEqual([]);
    f.catalog.markResumed();
    f.replace();
    expect(f.native.list()).toHaveLength(1);
    expect(ambient.createRuntime).not.toHaveBeenCalled();
  });

  it("honors child allowlists and cannot fall back when a selected server is unavailable", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", "[]");
    const f = fixture();
    expect(f.native.list()).toEqual([]);
    await expect(f.registry.invoke("mcp.docs-api.find.item", { value: "x" }, f.context)).rejects.toThrow();
    await expect(f.provider.invoke("$register", { name: "docs-api", command: "bad" }, f.context)).rejects.toThrow("owned by Pi");
    expect(f.executeTool).not.toHaveBeenCalled();
    expect(ambient.createRuntime).not.toHaveBeenCalled();
  });

  it("normalizes arguments once before approval and emits one logical trace operation", async () => {
    const f = fixture();
    setActiveCompiledSurface(undefined, true);
    const trace = new FabricExecutionTraceRecorder();
    await f.registry.invoke("mcp.docs-api.find.item", { VALUE: "x", count: "2" }, { ...f.context, trace });
    expect(f.context.approve).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ ref: "mcp.docs-api.find.item" }), { value: "x", count: 2 });
    expect(f.executeTool).toHaveBeenCalledWith("mcp__hashed_0", { value: "x", count: 2 }, expect.anything());
    const operations = trace.seal("succeeded", []).operations;
    expect(operations).toHaveLength(1);
    expect(operations[0]).toMatchObject({ ref: "mcp.docs-api.find.item", outcome: "succeeded", normalization: { rules: expect.any(Array) } });
    const invalid = new FabricExecutionTraceRecorder();
    await expect(f.registry.invoke("mcp.docs-api.find.item", { value: [] }, { ...f.context, trace: invalid })).rejects.toThrow("Invalid arguments");
    expect(invalid.seal("failed", []).operations[0]).toMatchObject({ failureStage: "validate", outcome: "failed" });
    expect(f.executeTool).toHaveBeenCalledOnce();
  });

  it("rechecks schema identity after approval rather than using an obsolete normal-form plan", async () => {
    const f = fixture();
    const approve = async () => {
      f.replace([{ ...f.registered[0]!, definition: { ...f.registered[0]!.definition, parameters: Type.Object({ other: Type.String() }) } }]);
    };
    await expect(f.registry.invoke("mcp.docs-api.find.item", { value: "x" }, { ...f.context, approve })).rejects.toThrow();
    expect(f.executeTool).not.toHaveBeenCalled();
  });

  it("does not bypass Fabric authorization or approval", async () => {
    const f = fixture();
    await expect(f.registry.invoke("mcp.docs-api.find.item", { value: "x" }, { ...f.context, authorize: async () => { throw new Error("guard denied"); } })).rejects.toThrow("guard denied");
    await expect(f.registry.invoke("mcp.docs-api.find.item", { value: "x" }, { ...f.context, approve: async () => { throw new Error("approval denied"); } })).rejects.toThrow("approval denied");
    expect(f.executeTool).not.toHaveBeenCalled();
  });

  it("uses redacted content when native middleware removes structuredContent, and propagates errors without retry", async () => {
    const f = fixture();
    f.executeTool.mockResolvedValueOnce({ toolCall: { arguments: { value: "rewritten" } }, isError: false,
      result: { content: [{ type: "text", text: "redacted" }], details: {} } } as never);
    expect(await f.provider.invoke("docs-api.find.item", { value: "secret" }, f.context))
      .toEqual({ text: "redacted", content: [{ type: "text", text: "redacted" }], structuredContent: null });
    expect(f.context.updateArguments).toHaveBeenCalledWith({ value: "rewritten" });
    f.executeTool.mockResolvedValueOnce({ toolCall: { arguments: {} }, isError: true,
      result: { content: [{ type: "text", text: "native gate denied" }], details: {} } } as never);
    await expect(f.provider.invoke("docs-api.find.item", {}, f.context)).rejects.toThrow("native gate denied");
    expect(f.executeTool).toHaveBeenCalledTimes(2);
    expect(ambient.createRuntime).not.toHaveBeenCalled();
  });

  it("forwards media after native middleware and keeps the MCP structured payload", async () => {
    const f = fixture();
    const image = { type: "image", data: "filtered", mimeType: "image/png" };
    f.executeTool.mockResolvedValueOnce({ toolCall: { arguments: {} }, isError: false,
      result: { content: [image], structuredContent: { content: [image], structuredContent: { value: "safe" } } } } as never);
    expect(await f.provider.invoke("docs-api.find.item", {}, f.context)).toMatchObject({ content: [image], structuredContent: { value: "safe" } });
    expect(f.context.attachMedia).toHaveBeenCalledExactlyOnceWith([image]);
  });

  it("cancels only the borrowed call, never closes the host server, and enforces the Fabric call deadline", async () => {
    const f = fixture();
    const controller = new AbortController();
    controller.abort(new Error("cancelled"));
    await expect(f.provider.invoke("docs-api.find.item", {}, { ...f.context, signal: controller.signal })).rejects.toThrow("cancelled");
    expect(f.executeTool).not.toHaveBeenCalled();
    vi.useFakeTimers();
    let passedSignal: AbortSignal | undefined;
    f.executeTool.mockImplementationOnce(async (_name, _args, options) => {
      passedSignal = options.signal;
      return new Promise(() => {});
    });
    const pending = f.provider.invoke("docs-api.find.item", {}, f.context);
    const rejected = expect(pending).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(501);
    await rejected;
    expect(passedSignal?.aborted).toBe(true);
    await f.provider.close();
    expect(ambient.createRuntime).not.toHaveBeenCalled();
    await expect(f.provider.list({ namespace: "docs-api" }, f.context)).rejects.toThrow("closed");
  });

  it("aborts its in-flight native call on provider shutdown without closing shared transports", async () => {
    const f = fixture();
    let signal: AbortSignal | undefined;
    f.executeTool.mockImplementationOnce(async (_name, _args, options) => { signal = options.signal; return new Promise(() => {}); });
    const pending = f.provider.invoke("docs-api.find.item", {}, f.context);
    const rejected = expect(pending).rejects.toThrow("closed");
    await vi.waitFor(() => expect(f.executeTool).toHaveBeenCalledOnce());
    await f.provider.close();
    await rejected;
    expect(signal?.aborted).toBe(true);
    expect(ambient.createRuntime).not.toHaveBeenCalled();
  });

  it("fails closed when the native execution context or identity metadata is unavailable", async () => {
    const f = fixture();
    await expect(f.provider.invoke("docs-api.find.item", {}, { ...f.context, extensionContext: {} as ExtensionContext })).rejects.toThrow("not callable");
    f.replace([{ ...f.registered[0]!, definition: { ...f.registered[0]!.definition, label: "unknown" } }]);
    expect(() => f.native.list()).toThrow("refusing to guess");
    expect(ambient.createRuntime).not.toHaveBeenCalled();
  });
});
