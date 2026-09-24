import { afterEach, describe, expect, it, vi } from "vitest";
import { ActionRegistry, type FabricRegistryInvocationContext } from "../src/core/action-registry.js";
import { CapabilityAuthority } from "../src/verified/authority.js";
import * as kernel from "../src/verified/generated/authority-kernel.js";
import { FabricSpeculationStore } from "../src/speculation/store.js";
import type { FabricActionDescriptor, FabricCapabilityBindingView, FabricInvocationContext, FabricProvider } from "../src/protocol.js";

const context: FabricInvocationContext = { cwd: process.cwd(), signal: undefined, parentToolCallId: "authority", nestedToolCallId: "authority", extensionContext: {} as FabricInvocationContext["extensionContext"], update() {} };
const deferred = () => { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; };
const registries: ActionRegistry[] = [];
const create = () => { const registry = new ActionRegistry(); registries.push(registry); return registry; };
const invocation = (extra: Partial<FabricRegistryInvocationContext> = {}): FabricRegistryInvocationContext => ({ ...context, approve: async () => {}, audits: [], maxResultChars: 10000, ...extra });
const fixture = (scoped = false) => {
  const descriptor: FabricActionDescriptor = { name: "run", description: "Run", inputSchema: { type: "object" }, risk: "read", effect: { kind: scoped ? "scoped" : "none", resources: ["demo"], ordering: "commutative" } };
  const provider: FabricProvider = { name: "demo", description: "Demo", async list() { return [descriptor]; }, async describe(name) { return ["run", "alias"].includes(name) ? descriptor : undefined; }, invoke: vi.fn(async (_name, args) => args), acquire: vi.fn(async (_name, args) => ({ value: args, dispose: vi.fn() })), close: vi.fn() };
  return { provider, descriptor };
};
afterEach(async () => { await Promise.all(registries.splice(0).map(registry => registry.close())); vi.restoreAllMocks(); });
const text = (value: string): kernel.AuthorityText => { let out: kernel.AuthorityText = { $: "Nil" }; for (let i = value.length - 1; i >= 0; i--) out = { $: "Con", head: BigInt(value.charCodeAt(i)), tail: out }; return out; };
const grants = (values: string[]): kernel.AuthorityGrants => values.reduceRight<kernel.AuthorityGrants>((tail, value) => ({ $: "Con", head: text(value), tail }), { $: "Nil" });

// Mutation anchors: A.derive must not issue unconditionally or reverse covered;
// A.allows must not accept Released; nameEqual must not accept a prefix; codec
// must not omit a tuple field. Positive tests kill deny-all mutations. Production
// tests kill removal of prepared/approval/trace clones and shutdown propagation.
describe("compiled authority state transitions", () => {
  it("encodes accepted and refused derivations without re-enumerating subsets", () => {
    // derive_safe/derive_complete quantify over arbitrary grant lists.
    const cases = [
      [[], [], true],
      [["a"], [], true],
      [["a", "b"], ["b"], true],
      [["a", "b"], ["a", "b"], true],
      [["a", "b"], ["a", "a"], true],
      [["a", "a"], ["a"], true],
      [["a"], ["a", "b"], false],
      [["a"], ["b"], false],
    ] as const;
    for (const [parent, candidate, accepted] of cases) {
      const child = kernel.authorityDerive(kernel.authorityIssue(grants([...parent])), grants([...candidate]));
      expect(child).toEqual(accepted ? { $: "Active", grants: grants([...candidate]) } : { $: "Released" });
      expect(kernel.authorityLive(child)).toBe(accepted);
    }
    const root = kernel.authorityIssue(grants(["a", "b"]));
    const child = kernel.authorityDerive(root, grants(["b"]));
    expect(kernel.authorityAllows(child, text("b"))).toBe(true);
    expect(kernel.authorityAllows(child, text("a"))).toBe(false);
    expect(kernel.authorityAllows(child, text("c"))).toBe(false);
    const released = kernel.authorityRelease(root);
    expect(kernel.authorityLive(released)).toBe(false);
    expect(kernel.authorityDerive(released, grants([]))).toEqual({ $: "Released" });
    expect(kernel.authorityAllows(released, text("a"))).toBe(false);
    expect(root).toEqual({ $: "Active", grants: grants(["a", "b"]) });
  });

  it("compares complete UTF-16 text, including long suffixes, NUL, astral and lone surrogates", () => {
    const prefix = "x".repeat(1100);
    for (const value of ["", "a", "a\0", "😀", "\ud800", prefix + "a"]) {
      const root = kernel.authorityIssue(grants([value]));
      expect(kernel.authorityAllows(root, text(value))).toBe(true);
      for (const other of [value + "b", value + "\0", value + "\udfff"]) {
        expect(kernel.authorityAllows(root, text(other))).toBe(false);
        expect(kernel.authorityLive(kernel.authorityDerive(root, grants([other])))).toBe(false);
      }
    }
  });
});

describe("private canonical authority codec", () => {
  const grant: FabricCapabilityBindingView = { ref: "demo.run", provider: "demo", providerBindingId: "generation\0😀\ud800" + "x".repeat(1100), generation: 1, descriptorHash: "descriptor" + "x".repeat(1100) };
  it.each(["ref", "provider", "providerBindingId", "generation", "descriptorHash"] as const)("rejects changing %s without changing the parent's other fields", field => {
    const parent = CapabilityAuthority.issue([grant]);
    const changed = { ...grant, [field]: field === "generation" ? 2 : `${grant[field]}!` };
    expect(parent.derive([changed]).active).toBe(false);
    expect(parent.derive([{ ...grant }]).resolve(grant.ref)).toEqual(grant);
  });
  it("does not trust later mutations to issue inputs or returned binding snapshots", () => {
    const input = { ...grant };
    const parent = CapabilityAuthority.issue([input]);
    input.descriptorHash = "mutated";
    parent.bindings()[0]!.descriptorHash = "mutated";
    parent.resolve(grant.ref)!.generation = 99;
    expect(parent.derive([grant]).active).toBe(true);
    expect(parent.derive([input]).active).toBe(false);
    parent.release();
    expect(parent.resolve(grant.ref)).toBeUndefined();
    expect(parent.derive([]).active).toBe(false);
  });
});

describe("ActionRegistry authority integration", () => {
  it("uses compiled issue/derive/use/release in the real registry, preserving aliases and retained generations", async () => {
    const issue = vi.spyOn(kernel, "authorityIssue"); const derive = vi.spyOn(kernel, "authorityDerive"); const allows = vi.spyOn(kernel, "authorityAllows"); const release = vi.spyOn(kernel, "authorityRelease");
    const registry = create(); const first = fixture(); registry.register(first.provider);
    const parent = await registry.acquireCapabilityView(["demo.run", "demo.alias"], context);
    registry.register(fixture().provider, { overwrite: true });
    const child = await registry.acquireCapabilityView(["demo.alias"], { ...context, capabilityView: parent.view! });
    expect(child.view!.bindings["demo.alias"]!.generation).toBe(parent.view!.bindings["demo.alias"]!.generation);
    expect(await registry.invoke("demo.alias", { value: 1 }, invocation({ capabilityView: child.view! }))).toEqual({ value: 1 });
    expect(first.provider.invoke).toHaveBeenCalledWith("run", { value: 1 }, expect.anything());
    expect(issue).toHaveBeenCalledOnce(); expect(derive).toHaveBeenCalledOnce(); expect(allows).toHaveBeenCalled();
    expect(first.provider.close).not.toHaveBeenCalled();
    await child.release();
    expect(await registry.invoke("demo.run", {}, invocation({ capabilityView: parent.view! }))).toEqual({});
    await parent.release(); expect(release).toHaveBeenCalledTimes(2); expect(first.provider.close).toHaveBeenCalledOnce();
  });

  it("refuses widening, forged/copied/cross-registry views and cascades parent release", async () => {
    const registry = create(); registry.register(fixture().provider);
    const parent = await registry.acquireCapabilityView(["demo.run"], context);
    const ctx = { ...context, capabilityView: parent.view! };
    expect((await registry.acquireCapabilityView(["demo.alias"], ctx)).satisfied).toBe(false);
    const child = await registry.acquireCapabilityView(["demo.run"], ctx);
    const empty = await registry.acquireCapabilityView([], ctx);
    expect(empty.satisfied).toBe(true);
    expect(Object.isFrozen(parent.view!.bindings["demo.run"])).toBe(true);
    const other = create(); other.register(fixture().provider);
    for (const [target, view] of [[registry, structuredClone(parent.view!)], [other, parent.view!]] as const) {
      await expect(target.invoke("demo.run", {}, invocation({ capabilityView: view }))).rejects.toThrow(/unissued/);
    }
    await parent.release();
    for (const view of [parent.view!, child.view!, empty.view!]) {
      await expect(registry.acquireCapabilityView([], { ...context, capabilityView: view })).rejects.toThrow(/released/);
      expect(() => registry.providers({ ...context, capabilityView: view })).toThrow(/released/);
    }
    await child.release(); await empty.release();
    expect(await registry.invoke("demo.run", {}, invocation())).toEqual({});
  });

  it("does not publish a child when its parent is released during asynchronous resolution", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); registry.register(provider);
    const parent = await registry.acquireCapabilityView(["demo.run"], context);
    const entered = deferred(); const gate = deferred();
    provider.describe = async () => { entered.resolve(); await gate.promise; return descriptor; };
    const pending = registry.acquireCapabilityView(["demo.run"], { ...context, capabilityView: parent.view! });
    const rejected = expect(pending).rejects.toThrow(/released/);
    await entered.promise; await parent.release(); gate.resolve(); await rejected;
  });

  it("shutdown releases held root/child authority and revokes inspection views", async () => {
    const release = vi.spyOn(kernel, "authorityRelease");
    const registry = create(); const { provider } = fixture(); registry.register(provider);
    const parent = await registry.acquireCapabilityView(["demo.run"], context);
    const child = await registry.acquireCapabilityView(["demo.run"], { ...context, capabilityView: parent.view! });
    const inspected = await registry.inspectCapabilities(["demo.run"], context);
    await registry.close();
    expect(release).toHaveBeenCalledTimes(3);
    expect(provider.close).toHaveBeenCalledOnce();
    for (const view of [parent.view!, child.view!, inspected.view!]) expect(() => registry.providers({ ...context, capabilityView: view })).toThrow(/released/);
    await parent.release(); await child.release();
    expect(provider.close).toHaveBeenCalledOnce();
  });
});

describe("production validated/approved argument fidelity", () => {
  const original = () => ({ nested: { value: "allowed", items: ["allowed"] }, bytes: new Uint8Array([7]), map: new Map([["key", { value: "allowed" }]]) });
  type Args = ReturnType<typeof original>;
  const mutate = (args: Record<string, unknown>, value: string) => {
    const typed = args as Args;
    typed.nested.value = value; typed.nested.items[0] = value;
    if (typed.bytes instanceof Uint8Array) typed.bytes[0] = 99;
    if (typed.map instanceof Map) typed.map.get("key")!.value = value;
  };

  it.each([false, true])("isolates provider-held and async approval references (scoped=%s)", async scoped => {
    const registry = create(); const { provider, descriptor } = fixture(scoped); registry.register(provider);
    descriptor.inputSchema = { type: "object", properties: { nested: { type: "object", properties: { value: { const: "allowed" } }, required: ["value"] } }, required: ["nested"] };
    const held = original(); provider.prepareArguments = async () => held;
    const entered = deferred(); const gate = deferred(); let approval!: Record<string, unknown>;
    const authorize = vi.fn(async (action: FabricActionDescriptor) => { action.inputSchema = {}; action.effect!.kind = "emission"; });
    const approve = vi.fn(async (_action: FabricActionDescriptor, args: Record<string, unknown>) => {
      expect(args).toEqual(original()); approval = args; mutate(args, "callback-before-await"); entered.resolve(); await gate.promise; mutate(args, "callback-after-await");
    });
    const caller = original();
    const pending = scoped
      ? registry.acquireScoped("demo.alias", caller, { ...context, authorize, approve })
      : registry.invoke("demo.alias", caller, invocation({ authorize, approve }));
    await entered.promise; mutate(held, "provider"); mutate(caller, "caller"); mutate(approval, "external"); gate.resolve();
    const result = await pending;
    if (scoped) { const acquired = result as Awaited<ReturnType<ActionRegistry["acquireScoped"]>>; expect(acquired.value).toEqual(original()); await acquired.dispose(); }
    else expect(result).toEqual(original());
    expect(authorize).toHaveBeenCalledOnce(); expect(approve).toHaveBeenCalledOnce();
    expect(scoped ? provider.acquire : provider.invoke).toHaveBeenCalledWith("run", original(), expect.anything());
  });

  it("isolates synchronous trace and activity observers before approval and dispatch", async () => {
    const registry = create(); registry.register(fixture().provider);
    const trace = { resolved() {}, prepared: vi.fn((args: Record<string, unknown>) => mutate(args, "trace")), normalized() {}, succeed() {}, fail() {} } as unknown as NonNullable<FabricRegistryInvocationContext["traceOperation"]>;
    const approve = vi.fn(async (_action, args) => { expect(args).toEqual(original()); });
    const observeInvocation: FabricRegistryInvocationContext["observeInvocation"] = event => { if (event.type === "call_start") mutate(event.args, "observer"); };
    const recorder = { issueCall(_ref: string, args: Record<string, unknown>) { mutate(args, "issue"); return trace; } } as unknown as FabricRegistryInvocationContext["trace"];
    expect(await registry.invoke("demo.run", original(), invocation({ trace: recorder!, approve, observeInvocation }))).toEqual(original());
    expect(approve).toHaveBeenCalledOnce();
  });

  it("does not let an observer turn invalid prepared input into a validated invocation", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); registry.register(provider);
    descriptor.inputSchema = { type: "object", properties: { value: { const: "allowed" } }, required: ["value"] };
    provider.prepareArguments = async () => ({ value: "forbidden" });
    const approve = vi.fn();
    const trace = { resolved() {}, prepared(args: Record<string, unknown>) { args.value = "allowed"; }, normalized() {}, fail() {} } as unknown as NonNullable<FabricRegistryInvocationContext["traceOperation"]>;
    await expect(registry.invoke("demo.run", {}, invocation({ approve, traceOperation: trace }))).rejects.toThrow(/Invalid arguments/);
    expect(approve).not.toHaveBeenCalled(); expect(provider.invoke).not.toHaveBeenCalled();
  });

  it.each(["authorize", "approve"] as const)("honors scoped %s denial before dispatch", async hook => {
    const registry = create(); const { provider } = fixture(true); registry.register(provider);
    await expect(registry.acquireScoped("demo.run", {}, { ...context, [hook]: async () => { throw new Error("policy denied"); } })).rejects.toThrow("policy denied");
    expect(provider.acquire).not.toHaveBeenCalled();
  });

  it("cancels scoped asynchronous approval on view release", async () => {
    const registry = create(); const { provider } = fixture(true); registry.register(provider);
    const view = await registry.acquireCapabilityView(["demo.run"], context);
    const entered = deferred(); const gate = deferred();
    const pending = registry.acquireScoped("demo.run", {}, { ...context, capabilityView: view.view!, approve: async () => { entered.resolve(); await gate.promise; } });
    const rejected = expect(pending).rejects.toThrow(/released/);
    await entered.promise; await view.release(); gate.resolve(); await rejected; expect(provider.acquire).not.toHaveBeenCalled();
  });

  it.each([false, true])("rejects shared backing memory rather than falsely approving a snapshot (prepared=%s)", async prepared => {
    const registry = create(); const { provider } = fixture(); registry.register(provider);
    const buffer = new SharedArrayBuffer(8);
    const payload = { nested: new Map([["bytes", new Uint8Array(buffer)]]) };
    if (prepared) provider.prepareArguments = async () => payload;
    const approve = vi.fn();
    await expect(registry.invoke("demo.run", prepared ? {} : payload, invocation({ approve }))).rejects.toThrow(/shared memory/);
    expect(approve).not.toHaveBeenCalled(); expect(provider.invoke).not.toHaveBeenCalled();
  });

  it.each([false, true])("rejects shared WebAssembly memory hidden behind native internal slots (prepared=%s)", async prepared => {
    const registry = create(); const { provider } = fixture(); registry.register(provider);
    const memory = new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true });
    const payload = { memory };
    if (prepared) provider.prepareArguments = async () => payload;
    const approve = vi.fn();
    await expect(registry.invoke("demo.run", prepared ? {} : payload, invocation({ approve }))).rejects.toThrow(/shared memory/);
    expect(approve).not.toHaveBeenCalled(); expect(provider.invoke).not.toHaveBeenCalled();
  });

  it("keeps speculative validation snapshots private until execution", async () => {
    const registry = create(); const { provider } = fixture(); registry.register(provider);
    registry.setSpeculation(new FabricSpeculationStore({ maxConcurrent: 2, maxEntries: 4, entryTtlMs: 1000 }), () => true);
    const held = original(); provider.prepareArguments = async () => held;
    const prepared = await registry.speculate("demo.run", {}, context, {});
    mutate(held, "provider"); mutate(prepared!.preparedArgs, "caller");
    expect(await prepared!.execute(undefined)).toEqual(original());
  });
});
