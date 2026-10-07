import { normalizeScope } from "../src/scope.js";
import { describeNativeNamespace } from "../src/native-discovery.js";
import { nativeMcpIdentity } from "../src/core/native-mcp-identity.js";
import { describe, expect, it, vi } from "vitest";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { NativeCodemodeProvider, STORE_TOTAL_LIMIT, STORE_VALUE_LIMIT, validateScriptStore } from "../src/native-codemode.js";
import { prepareFabricExecArguments } from "../src/fabric-exec-arguments.js";

const classifier = { type: "classifier", provider: "test", id: "bool", name: "Bool", api: "classifier", input: ["text"], headers: { Authorization: "secret" }, apiKey: "secret" };
const usage = { input: 2, output: 3, totalTokens: 5, cost: { total: 0.01 } };
const question = '{ state: { good: true }, questions: { ok: { type: "bool", instructions: "Is it good?", criteria: { true: "good", false: "bad" } } } }';
function fixture(runtime: "quickjs" | "node-process" | "bun-process" = "quickjs", persistence = true) {
  let leaf = "root";
  let session = "session";
  let branch: any[] = [];
  const registry = {
    getAvailable: vi.fn(() => [classifier]),
    getModelsOfType: vi.fn(() => [classifier]),
    getAvailableOfType: vi.fn(async () => [classifier]),
    getModelOfType: vi.fn(() => classifier),
    classify: vi.fn(async (_model: unknown, _context: unknown, _options: unknown) => ({ provider: "test", model: "bool", stopReason: "stop", answers: { ok: { type: "bool", probability: 0.8 } }, usage })),
    generateImages: vi.fn(async () => ({ provider: "test", model: "paint", stopReason: "stop", output: [{ type: "image", mimeType: "image/png", data: "aGVsbG8=" }], usage })),
  };
  const context = { cwd: process.cwd(), hasUI: false, modelRegistry: registry, sessionManager: {
    getBranch: () => branch, getSessionId: () => session, getLeafId: () => leaf,
  } } as unknown as ExtensionContext;
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.executor.runtime = runtime;
  config.approvals.read = "allow"; config.approvals.write = "allow"; config.approvals.network = "allow";
  const actions = new ActionRegistry();
  const service = new FabricExecutionService(actions, config);
  const append = vi.fn((customType: string, data: unknown) => { leaf = `entry-${branch.length}`; branch.push({ id: leaf, type: "custom", customType, data }); });
  if (persistence) service.nativeCodemode.setPersistence(append);
  let n = 0;
  const run = (code: string, signal?: AbortSignal, hardTimeoutMs?: number) => service.execute({ code, signal, context, parentToolCallId: `test-${++n}`, onPartial() {}, ...(hardTimeoutMs ? { hardTimeoutMs } : {}) });
  return { run, service, actions, config, registry, context, append, setLeaf: (value: string) => { leaf = value; }, setSession: (value: string) => { session = value; }, setBranch: (value: any[]) => { branch = value; }, branch: () => branch };
}

describe("native Pi codemode bridge", () => {
  it("retains callable aliases, strips auth and accounts minimal native usage", async () => {
    const f = fixture();
    const result = await f.run(`const all = await models.getAvailableOfType("classifier"); const result = await tools.models.classify(all[0], ${question}); return { same: models === tools.models, old: await tools.models(), all, result };`);
    expect(result.success, result.error ?? JSON.stringify(result.typeErrors)).toBe(true);
    expect(result.value).toMatchObject({ same: true, result: { answers: { ok: { type: "bool", probability: 0.8 } } } });
    expect(JSON.stringify(result.value)).not.toContain("secret");
    expect(result.usage).toEqual({ ...usage, cacheRead: 0, cacheWrite: 0, cost: { total: 0.01, input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } });
    expect(f.registry.classify.mock.calls[0]?.[2]).toMatchObject({ signal: expect.any(AbortSignal) });
  });
  it("preserves native error stopReason without throwing it away", async () => {
    const f = fixture();
    f.registry.classify.mockImplementation(async () => ({ provider: "test", model: "bool", stopReason: "error", answers: {}, usage, errorMessage: "native refusal" }) as any);
    const result = await f.run(`return await models.classify({ provider: "test", id: "bool" }, ${question});`);
    expect(result.value).toMatchObject({ stopReason: "error", errorMessage: "native refusal" });
    expect(result.usage?.totalTokens).toBe(5);
  });
  it("limits mixed model calls to four and cancels outstanding calls", async () => {
    const f = fixture(); let active = 0; let peak = 0;
    f.registry.classify.mockImplementation(async (_m, _c, options: any) => {
      active++; peak = Math.max(peak, active);
      try { await new Promise<void>((resolve, reject) => { const timer = setTimeout(resolve, 15); options.signal.addEventListener("abort", () => { clearTimeout(timer); reject(new Error("cancelled")); }, { once: true }); }); }
      finally { active--; }
      return { provider: "test", model: "bool", stopReason: "stop", answers: {}, usage } as any;
    });
    const result = await f.run(`return await Promise.all(Array.from({length: 9}, () => models.classify({provider:"test",id:"bool"}, ${question})));`);
    expect(result.success, result.error).toBe(true); expect(peak).toBe(4); expect(active).toBe(0);
    const aborted = await f.run(`await models.classify({provider:"test",id:"bool"}, ${question});`, undefined, 5);
    expect(aborted.success).toBe(false); expect(active).toBe(0);
  });
  it("enforces approvals, schema authorizer and argument validation before model calls", async () => {
    const f = fixture(); f.config.approvals.network = "deny";
    const denied = await f.run(`return await models.classify({provider:"test",id:"bool"}, ${question});`);
    expect(denied.success).toBe(false); expect(f.registry.classify).not.toHaveBeenCalled();
    f.config.approvals.network = "allow";
    const invalid = await f.run('return await tools.call({ref:"native.classify",args:{model:{provider:"test",id:"bool"},context:{state:{},questions:{q:{type:"noul"}}}}});');
    expect(invalid.success).toBe(false); expect(f.registry.classify).not.toHaveBeenCalled();
    const registry = new ActionRegistry(); const authorize = vi.fn(async () => { throw new Error("schema denied"); });
    const service = new FabricExecutionService(registry, f.config, undefined, { authorize });
    const schema = await service.execute({ code: `return await models.classify({provider:"test",id:"bool"}, ${question});`, context:f.context, signal:undefined, parentToolCallId:"schema", onPartial(){} });
    expect(schema.success).toBe(false); expect(authorize).toHaveBeenCalledWith("native.classify", "schema"); expect(f.registry.classify).not.toHaveBeenCalled();
  });
  it.each(["quickjs", "node-process", "bun-process"] as const)("%s supports synchronous state aliases, images and exit", async runtime => {
    const f = fixture(runtime);
    const first = await f.run('const save = store; save("cursor", {n:1}); text("before"); image({image_url:{url:"data:image/png;base64,aGVsbG8="}}); exit();');
    expect(first.success, first.error ?? JSON.stringify(first.typeErrors)).toBe(true); expect(first.logs).toContain("before"); expect(first.media).toHaveLength(1); expect(f.append).toHaveBeenCalledTimes(1);
    expect((await f.run('const read = load; return read("cursor");')).value).toEqual({ n: 1 });
    const failed = await f.run('store("cursor", 2); text("partial"); image("data:image/png;base64,aGVsbG8="); throw new Error("boom");');
    expect(failed.success).toBe(false); expect(failed.logs).toContain("partial"); expect(failed.media).toHaveLength(1); expect(f.append).toHaveBeenCalledTimes(1);
    await f.run('store("cursor", undefined);'); expect((await f.run('return load("cursor");')).value).toBeUndefined();
    expect((await f.run('image("https://example.com/a.png");')).success).toBe(false);
  });
  it.each(["quickjs", "node-process", "bun-process"] as const)("%s retains partial output and rolls back state on a hard deadline", async runtime => {
    const f = fixture(runtime);
    const result = await f.run('store("pending", 1); text("partial timeout"); image("data:image/png;base64,aGVsbG8="); await new Promise(() => {});', undefined, 500);
    expect(result.success).toBe(false); expect(result.logs).toContain("partial timeout"); expect(result.media).toHaveLength(1); expect(f.append).not.toHaveBeenCalled();
  });

  it("rolls back aborted, type-error, stale leaf/session and concurrent transactions", async () => {
    const f = fixture();
    await f.run('store("x", 1);');
    const badType = await f.run('store("x", 2); missingPublicSymbol();'); expect(badType.success).toBe(false); expect(f.append).toHaveBeenCalledTimes(1);
    const abort = new AbortController(); abort.abort(); await expect(f.run('store("x", 3);', abort.signal)).rejects.toThrow(); expect(f.append).toHaveBeenCalledTimes(1);
    const provider = f.service.nativeCodemode;
    const ctx = { extensionContext: f.context, parentToolCallId: "a", signal: undefined } as any;
    const a = provider.begin("a", f.context), b = provider.begin("b", f.context);
    await provider.invoke("store", { values: { a: 1 } }, ctx); await provider.invoke("store", { values: { b: 1 } }, { ...ctx, parentToolCallId: "b" });
    a.finish(true); expect(() => b.finish(true)).toThrow(/stale/);
    const leaf = provider.begin("leaf", f.context); await provider.invoke("store", {values:{bad:1}}, {...ctx,parentToolCallId:"leaf"}); f.setLeaf("other-branch"); expect(() => leaf.finish(true)).toThrow(/stale/);
    const session = provider.begin("session", f.context); await provider.invoke("store", {values:{bad:1}}, {...ctx,parentToolCallId:"session"}); f.setSession("new-session"); expect(() => session.finish(true)).toThrow(/stale/);
    expect(f.append).toHaveBeenCalledTimes(2);
    f.setBranch([]); expect((await f.run('return load("a");')).value).toBeUndefined();
  });
  it("rejects exact store bounds and tolerates malformed historical entries", async () => {
    expect(validateScriptStore({ x: "a".repeat(STORE_VALUE_LIMIT - 2) }).x).toBeDefined();
    expect(() => validateScriptStore({ x: "a".repeat(STORE_VALUE_LIMIT - 1) })).toThrow(/262144/);
    const values = Object.fromEntries(Array.from({length:4}, (_,i)=>[i,"a".repeat(STORE_TOTAL_LIMIT / 4 - 2)])); expect(validateScriptStore(values)).toEqual(values);
    expect(() => validateScriptStore({...values, extra:0})).toThrow(/1048576/);
    const f = fixture(); f.setBranch([{type:"custom",customType:"fabric-codemode-store",data:{}}]); expect((await f.run('return 1;')).success).toBe(true);
  });
  it("enforces host-issued model/store scope without leaking snapshots", async () => {
    const f = fixture(); await f.run('store("secret", 1);');
    const key = Symbol.for("pi-fabric:scope:v1"); const host = globalThis as any; const saved = host[key];
    host[key] = { sealed: true, scope: normalizeScope({ principal: { id: "restricted" }, grants: [] }) };
    try {
      expect((await f.run('return load("secret");')).success).toBe(false);
      expect((await f.run(`return await models.classify({provider:"test",id:"bool"}, ${question});`)).success).toBe(false);
      expect(f.registry.classify).not.toHaveBeenCalled();
      expect((await f.run('store("secret", 2);')).success).toBe(false); expect(f.append).toHaveBeenCalledTimes(1);
    } finally { if (saved === undefined) delete host[key]; else host[key] = saved; }
  });
  it("runs images through the native registry and hoists explicit/returned local forms", async () => {
    const f = fixture(); const result = await f.run('const r = await models.generateImages({provider:"test",id:"paint"},{input:[{type:"text",text:"paint"}]}); for (const b of r.output) if(b.type === "image") image(b); return {image_url:"data:image/png;base64,aGVsbG8="};');
    expect(result.success, result.error).toBe(true); expect(result.media).toHaveLength(1); expect(result.usage?.totalTokens).toBe(5); expect(f.registry.generateImages).toHaveBeenCalledTimes(1);
  });
  it("allows only one concurrent successful store commit from a shared snapshot", async () => {
    const f = fixture(); let calls = 0; let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    f.registry.classify.mockImplementation(async () => { if (++calls === 2) release(); await gate; return { provider:"test",model:"bool",stopReason:"stop",answers:{},usage } as any; });
    const source = `store("x", 1); await models.classify({provider:"test",id:"bool"}, ${question});`;
    const results = await Promise.all([f.run(source), f.run(source)]);
    expect(results.filter(result => result.success)).toHaveLength(1); expect(results.find(result=>!result.success)?.error).toMatch(/stale/); expect(f.append).toHaveBeenCalledTimes(1);
  });
  it("keeps native namespace instructions separate and only for visible tools", () => {
    const action = {ref:"extensions.demo",name:"demo",provider:"extensions",description:"tool",inputSchema:{},risk:"read"} as const;
    const catalog = { registeredTools: () => [{definition:{name:"demo",label:"Demo",exposure:"codemode",namespace:{name:"example",description:"short",instructions:"long instructions"}}}] } as any;
    expect(describeNativeNamespace("example", [action], catalog, nativeMcpIdentity)).toMatchObject({description:"short",instructions:"long instructions",tools:[{name:"extensions.demo"}]});
    expect(describeNativeNamespace("example", [], catalog, nativeMcpIdentity)).toBeUndefined();
    expect(describeNativeNamespace("extensions", [action], undefined, nativeMcpIdentity)).not.toHaveProperty("instructions");
  });
  it("initializes compatibility discovery and returns declarations", async () => {
    const f = fixture(); const result = await f.run('return {all:ALL_TOOLS,found:await searchTools("classifier"),one:await describeTool("native.classify"),namespace:await describeNamespace("native")};');
    expect(result.success, result.error ?? JSON.stringify(result.typeErrors)).toBe(true);
    expect(result.value).toMatchObject({all:expect.arrayContaining([{name:"native.classify",description:expect.any(String)}]),one:{name:"native.classify",declaration:expect.stringContaining("args:")},namespace:{name:"native"}});
  });
  it("parses native options and rejects unsupported or conflicting options", () => {
    expect(prepareFabricExecArguments('// @options: {"timeout_ms": 50, "max_output_tokens": 100}\nreturn 1;')).toMatchObject({timeout_ms:50,maxOutputTokens:100});
    expect(() => prepareFabricExecArguments('// @options: {"made_up":true}\nreturn 1;')).toThrow(/Unsupported/);
    expect(() => prepareFabricExecArguments({code:'return 1;',max_output_tokens:2,maxOutputTokens:3})).toThrow(/Conflicting/);
  });
});
