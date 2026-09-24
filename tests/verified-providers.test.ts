import { readFileSync, readdirSync } from "node:fs";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricSpeculationStore } from "../src/speculation/store.js";
import * as kernel from "../src/verified/generated/provider-kernel.js";
import type { StateText } from "../src/verified/generated/provider-kernel.js";
import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider, FabricScopedProviderResult } from "../src/protocol.js";

const context: FabricInvocationContext = { cwd: process.cwd(), signal: undefined, parentToolCallId: "proof", nestedToolCallId: "proof", extensionContext: {} as FabricInvocationContext["extensionContext"], update() {} };
const deferred = <T = void>() => { let resolve!: (value: T) => void; const promise = new Promise<T>(done => { resolve = done; }); return { promise, resolve }; };
const registries: ActionRegistry[] = [];
const create = () => { const registry = new ActionRegistry(); registries.push(registry); return registry; };
const fixture = (name = "demo") => {
  const descriptor: FabricActionDescriptor = { name: "run", description: "Run", inputSchema: { type: "object" }, risk: "read", effect: { kind: "none", ordering: "commutative", resources: [name] } };
  const provider: FabricProvider = { name, description: name, async list() { return [descriptor]; }, async describe(action) { return action === "run" ? descriptor : undefined; }, invoke: vi.fn(async (_action, args) => args), close: vi.fn() };
  return { provider, descriptor };
};
const invoke = (registry: ActionRegistry, ctx: Partial<FabricInvocationContext> = {}, args: Record<string, unknown> = {}, ref = "demo.run", approve = async () => {}) => registry.invoke(ref, args, { ...context, ...ctx, approve, audits: [], maxResultChars: 10000, effectPolicy: "strict" });
const speculative = (registry: ActionRegistry) => { const store = new FabricSpeculationStore({ maxConcurrent: 2, maxEntries: 4, entryTtlMs: 1000 }); registry.setSpeculation(store, () => true); return store; };
afterEach(async () => { await Promise.all(registries.splice(0).map(registry => registry.close())); vi.restoreAllMocks(); });

const encode = (text: string): StateText => { let value: StateText = { $: "Nil" }; for (let i = text.length - 1; i >= 0; i--) value = { $: "Con", head: BigInt(text.charCodeAt(i)), tail: value }; return value; };
describe("compiled provider plans", () => {
  it("preserves ticket fields, exact text and consumed outcome encodings", () => {
    // Safety, completeness and consumption are universal Bend laws. Keep
    // field-distinguishing wire examples rather than crossing every failure.
    const ticket: kernel.ProviderTicket = { $: "Ticket", grant: { $: "Grant", active: true, key: encode("a") }, expected: encode("1"), payload: encode("slot\0😀\udfff") };
    const write = { $: "Write", key: encode("a"), expected: encode("1"), value: ticket.payload };
    const cases = [
      { ticket, key: "a", revision: "1", stopped: false, plan: write },
      { ticket: { ...ticket, grant: { ...ticket.grant, active: false } }, key: "a", revision: "1", stopped: false, plan: { $: "Denied" } },
      { ticket, key: "a", revision: "1", stopped: true, plan: { $: "Denied" } },
      { ticket, key: "ab", revision: "1", stopped: false, plan: { $: "Denied" } },
      { ticket, key: "a\0", revision: "1", stopped: false, plan: { $: "Denied" } },
      { ticket, key: "\ud800", revision: "1", stopped: false, plan: { $: "Denied" } },
      { ticket, key: "", revision: "1", stopped: false, plan: { $: "Denied" } },
      { ticket, key: "a", revision: "10", stopped: false, plan: { $: "Denied" } },
      { ticket, key: "a", revision: "1\0", stopped: false, plan: { $: "Denied" } },
    ];
    for (const row of cases) {
      const before = structuredClone(row.ticket);
      const result = kernel.providerTake(row.ticket, encode(row.key), encode(row.revision), row.stopped);
      expect(result.plan).toEqual(row.plan);
      expect(result.next).toEqual({ ...row.ticket, grant: { ...row.ticket.grant, active: false } });
      expect(kernel.providerTake(result.next, encode("a"), encode("1"), false).plan).toEqual({ $: "Denied" });
      expect(row.ticket).toEqual(before);
    }
    expect(kernel.providerTake(kernel.providerRevoke(ticket), encode("a"), encode("1"), false).plan).toEqual({ $: "Denied" });
  });
});

describe("scoped ownership and lifecycle finalization", () => {
  it("disposes when a trusted scope cannot adopt the newly acquired resource", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); descriptor.effect = { kind: "scoped", resources: ["scope"] };
    const dispose = vi.fn(); provider.acquire = async () => ({ value: 1, dispose }); registry.register(provider);
    await expect(registry.acquireScoped("demo.run", {}, context, () => { throw new Error("scope closed"); })).rejects.toThrow("scope closed");
    expect(dispose).toHaveBeenCalledOnce(); await registry.close(); expect(provider.close).toHaveBeenCalledOnce();
  });

  it("automatic shutdown releases every scoped hold without requiring a second explicit disposal", async () => {
    const registry = create(); const { provider, descriptor } = fixture();
    descriptor.effect = { kind: "scoped", resources: ["scope"] };
    const dispose = vi.fn(); provider.acquire = async () => ({ value: 1, dispose }); registry.register(provider);
    const acquired = await registry.acquireScoped("demo.run", {}, context);
    try {
      await registry.close();
      expect(dispose).toHaveBeenCalledOnce(); expect(provider.close).toHaveBeenCalledOnce();
      expect(registry.providerStatus()).toEqual([]);
    } finally { await acquired.dispose(); }
    expect(dispose).toHaveBeenCalledOnce();
  });

  it("delivers scoped approval cancellation without awaiting a non-cooperative close", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); descriptor.effect = { kind: "scoped", resources: ["scope"] };
    provider.acquire = vi.fn(async () => ({ value: 1, dispose() {} }));
    const approval = deferred(); const approving = deferred(); const closing = deferred(); const closeGate = deferred();
    provider.close = vi.fn(async () => { closing.resolve(); await closeGate.promise; }); registry.register(provider);
    const pending = registry.acquireScoped("demo.run", {}, { ...context, approve: async () => { approving.resolve(); await approval.promise; } });
    const refused = expect(pending).rejects.toThrow(/revoked/);
    await approving.promise; registry.revokeProvider("demo");
    try { await refused; await closing.promise; expect(provider.acquire).not.toHaveBeenCalled(); }
    finally { approval.resolve(); closeGate.resolve(); }
  });

  it("skips closing generations without suppressing the replacement's invocation finalizer", async () => {
    const registry = create(); const first = fixture(); const second = fixture(); const entered = deferred(); const gate = deferred();
    first.provider.close = vi.fn(async () => { entered.resolve(); await gate.promise; });
    first.provider.invocationEnded = vi.fn(); second.provider.invocationEnded = vi.fn();
    registry.register(first.provider); registry.register(second.provider, { overwrite: true }); await entered.promise;
    try {
      await registry.endInvocation("parent");
      expect(first.provider.invocationEnded).not.toHaveBeenCalled();
      expect(second.provider.invocationEnded).toHaveBeenCalledOnce();
    } finally { gate.resolve(); }
  });
});

const families = ["agents", "compact", "components", "extensions", "jev", "mcp", "memory", "mesh", "pi", "prewalk", "schema", "state"];
describe("universal registry migration", () => {
  it("accounts for every built-in family and has no remaining raw dispatch in the registry", () => {
    const directory = new URL("../src/providers/", import.meta.url);
    const names = readdirSync(directory).filter(name => name.endsWith("-provider.ts")).flatMap(name => [...readFileSync(new URL(name, directory), "utf8").matchAll(/readonly name = "([^"]+)"/g)].map(match => match[1])).sort();
    expect(names).toEqual(families);
    const source = readFileSync(new URL("../src/core/action-registry.ts", import.meta.url), "utf8");
    expect(source).not.toMatch(/provider\.(?:invoke|acquire)\s*!?\s*\(/);
    for (const route of ["invoke", "acquire", "speculate", "replay"]) expect(source).toContain(`"${route}"`);
  });

  it.each([...families, "thirdparty"])("gates %s registrations through the actual compiled kernel", async name => {
    const take = vi.spyOn(kernel, "providerTake");
    const registry = create(); const { provider } = fixture(name); registry.register(provider);
    expect(await invoke(registry, {}, { payload: "test" }, `${name}.run`)).toEqual({ payload: "test" });
    expect(take).toHaveBeenCalledOnce(); expect(provider.invoke).toHaveBeenCalledOnce();
    registry.revokeProvider(name);
    await expect(invoke(registry, {}, {}, `${name}.run`)).rejects.toThrow();
    expect(provider.invoke).toHaveBeenCalledOnce();
  });

  it("rejects forged, cross-registry, and released views while leaving root authority compatible", async () => {
    const registry = create(); registry.register(fixture().provider);
    const lease = await registry.acquireCapabilityView(["demo.run"], context);
    expect(Object.isFrozen(lease.view)).toBe(true);
    expect(Object.isFrozen(lease.view!.bindings["demo.run"])).toBe(true);
    await expect(invoke(registry, { capabilityView: structuredClone(lease.view!) })).rejects.toThrow(/unissued/);
    const other = create(); other.register(fixture().provider);
    await expect(invoke(other, { capabilityView: lease.view! })).rejects.toThrow(/unissued/);
    await lease.release();
    await expect(invoke(registry, { capabilityView: lease.view! })).rejects.toThrow(/released/);
    expect(() => registry.providers({ ...context, capabilityView: lease.view! })).toThrow(/released/);
    await expect(invoke(registry)).resolves.toEqual({});
  });

  it("does not widen derived authority and cascades revocation to existing children", async () => {
    const registry = create(); const { provider } = fixture(); registry.register(provider); registry.register(fixture("other").provider);
    const parent = await registry.acquireCapabilityView(["demo.run"], context);
    const scoped = { ...context, capabilityView: parent.view! };
    const refused = await registry.acquireCapabilityView(["other.run"], scoped);
    expect(refused.satisfied).toBe(false);
    const child = await registry.acquireCapabilityView(["demo.run"], scoped);
    await parent.release();
    await expect(invoke(registry, { capabilityView: child.view! })).rejects.toThrow(/revoked/);
    expect(provider.invoke).not.toHaveBeenCalled(); await child.release(); await refused.release();
  });

  it("snapshots caller arguments before awaiting approval", async () => {
    const registry = create(); const { provider } = fixture(); registry.register(provider);
    const entered = deferred(); const gate = deferred(); const args = { nested: { key: "original" } };
    const pending = invoke(registry, {}, args, "demo.run", async () => { entered.resolve(); await gate.promise; });
    await entered.promise; args.nested.key = "changed"; gate.resolve();
    expect(await pending).toEqual({ nested: { key: "original" } });
  });

  it("refuses descriptor drift after approval and does not silently rebind a restricted child", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); registry.register(provider);
    const lease = await registry.acquireCapabilityView(["demo.run"], context);
    const entered = deferred(); const gate = deferred();
    const pending = invoke(registry, { capabilityView: lease.view! }, {}, "demo.run", async () => { entered.resolve(); await gate.promise; });
    await entered.promise; descriptor.description = "changed"; gate.resolve();
    await expect(pending).rejects.toThrow(/stale descriptor/); expect(provider.invoke).not.toHaveBeenCalled();
    const child = await registry.acquireCapabilityView(["demo.run"], { ...context, capabilityView: lease.view! });
    expect(child.satisfied).toBe(false); await child.release(); await lease.release();
  });

  it("revokes an already admitted call waiting for approval", async () => {
    const registry = create(); const { provider } = fixture(); registry.register(provider);
    const entered = deferred(); const gate = deferred();
    const pending = invoke(registry, {}, {}, "demo.run", async () => { entered.resolve(); await gate.promise; });
    const rejected = expect(pending).rejects.toThrow(); await entered.promise;
    registry.revokeProvider("demo"); gate.resolve(); await rejected;
    expect(provider.invoke).not.toHaveBeenCalled();
  });

  it("holds the real invocation and its conflict footprint after caller cancellation", async () => {
    const registry = create(); const { provider, descriptor } = fixture();
    descriptor.effect = { kind: "transactional", resources: ["shared"], ordering: "ordered" };
    const entered = deferred(); const gate = deferred(); provider.invoke = vi.fn(async () => { entered.resolve(); await gate.promise; return "done"; });
    const lease = registry.mount(provider); const abort = new AbortController();
    const pending = invoke(registry, { signal: abort.signal }); const rejected = expect(pending).rejects.toThrow();
    await entered.promise; abort.abort(); await rejected;
    expect(vi.mocked(provider.invoke).mock.calls[0]![2].signal?.aborted).toBe(true);
    await expect(invoke(registry)).rejects.toThrow(/independent|conflict|shared/);
    await lease.release(); expect(provider.close).not.toHaveBeenCalled();
    expect(registry.providerStatus()[0]!.inFlight).toBeGreaterThan(0);
    gate.resolve(); await vi.waitFor(() => expect(provider.close).toHaveBeenCalledOnce());
  });

  it("holds ignored-abort catalog work until the actual descriptor settles", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); const entered = deferred(); const gate = deferred();
    provider.describe = async () => { entered.resolve(); await gate.promise; return descriptor; };
    const lease = registry.mount(provider); const abort = new AbortController();
    const pending = invoke(registry, { signal: abort.signal }); const rejected = expect(pending).rejects.toThrow();
    await entered.promise; abort.abort(); await rejected; await lease.release();
    expect(provider.close).not.toHaveBeenCalled(); gate.resolve();
    await vi.waitFor(() => expect(provider.close).toHaveBeenCalledOnce()); expect(provider.invoke).not.toHaveBeenCalled();
  });

  it("disposes a late canceled acquisition before closing the provider", async () => {
    const registry = create(); const { provider, descriptor } = fixture();
    descriptor.effect = { kind: "scoped", resources: ["scope"], ordering: "ordered" };
    const entered = deferred(); const gate = deferred<FabricScopedProviderResult>(); const dispose = vi.fn();
    provider.acquire = async () => { entered.resolve(); return gate.promise; };
    const lease = registry.mount(provider); const abort = new AbortController();
    const pending = registry.acquireScoped("demo.run", {}, { ...context, signal: abort.signal });
    const rejected = expect(pending).rejects.toThrow(); await entered.promise; abort.abort(); await rejected;
    await lease.release(); expect(provider.close).not.toHaveBeenCalled(); gate.resolve({ value: "late", dispose });
    await vi.waitFor(() => expect(provider.close).toHaveBeenCalledOnce()); expect(dispose).toHaveBeenCalledOnce();
  });

  it("disposes once and quarantines failed cleanup instead of pretending to close", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); descriptor.effect = { kind: "scoped", resources: ["scope"] };
    const dispose = vi.fn(async () => { throw new Error("cleanup failed"); }); provider.acquire = async () => ({ value: "resource", dispose }); registry.register(provider);
    const acquired = await registry.acquireScoped("demo.run", {}, context);
    const first = acquired.dispose(); const second = acquired.dispose(); expect(first).toBe(second);
    await expect(first).rejects.toThrow("cleanup failed"); expect(dispose).toHaveBeenCalledOnce();
    expect(registry.providerStatus()).toEqual([expect.objectContaining({ revoked: true, error: "cleanup failed" })]);
    await expect(invoke(registry)).rejects.toThrow(); expect(provider.close).not.toHaveBeenCalled();
  });

  it("retains close failures as observable quarantines", async () => {
    const registry = create(); const { provider } = fixture(); provider.close = async () => { throw new Error("close failed"); }; registry.register(provider);
    await registry.close(); expect(registry.providerStatus()).toEqual([expect.objectContaining({ revoked: true, error: "close failed" })]);
    await expect(invoke(registry)).rejects.toThrow(/closed/);
  });

  it("consumes speculative tickets exactly once, including a canceled first attempt", async () => {
    const registry = create(); const { provider } = fixture(); registry.register(provider); speculative(registry);
    const prepared = await registry.speculate("demo.run", { value: 1 }, context, {}); expect(prepared).toBeDefined();
    const results = await Promise.allSettled([prepared!.execute(undefined), prepared!.execute(undefined)]);
    expect(results.map(result => result.status).sort()).toEqual(["fulfilled", "rejected"]); expect(provider.invoke).toHaveBeenCalledOnce();
    const canceled = await registry.speculate("demo.run", {}, context, {}); const abort = new AbortController(); abort.abort();
    await expect(canceled!.execute(abort.signal)).rejects.toThrow();
    await expect(canceled!.execute(undefined)).rejects.toThrow(/spent/); expect(provider.invoke).toHaveBeenCalledOnce();
  });

  it("does not expose mutable speculative payload slots and rechecks descriptors at launch", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); registry.register(provider); speculative(registry);
    const prepared = await registry.speculate("demo.run", { nested: { value: "before" } }, context, {});
    (prepared!.preparedArgs.nested as { value: string }).value = "after";
    expect(await prepared!.execute(undefined)).toEqual({ nested: { value: "before" } });
    const stale = await registry.speculate("demo.run", {}, context, {}); descriptor.description = "changed";
    await expect(stale!.execute(undefined)).rejects.toThrow(/stale/); expect(provider.invoke).toHaveBeenCalledOnce();
  });

  it("never reuses speculative results across descriptor revisions or capability views", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); registry.register(provider); const store = speculative(registry);
    const prepared = await registry.speculate("demo.run", {}, context, {});
    expect(store.launch(context.parentToolCallId, "demo.run", {}, prepared!.execute, undefined, {}, prepared!.bindingToken)).toBe(true);
    await vi.waitFor(() => expect(provider.invoke).toHaveBeenCalledOnce());
    descriptor.description = "new semantics";
    await invoke(registry); expect(provider.invoke).toHaveBeenCalledTimes(2);
    const fresh = await registry.speculate("demo.run", { x: 1 }, context, {});
    const lease = await registry.acquireCapabilityView(["demo.run"], context);
    const restricted = await registry.speculate("demo.run", { x: 1 }, { ...context, capabilityView: lease.view! }, {});
    expect(fresh!.bindingToken).not.toBe(restricted!.bindingToken); await lease.release();
  });

  it("rejects mutating speculation even if an eligibility callback opts it in", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); descriptor.risk = "write"; registry.register(provider); speculative(registry);
    expect(await registry.speculate("demo.run", {}, context, {})).toBeUndefined();
    expect(provider.invoke).not.toHaveBeenCalled();
  });

  it("does not wait for a hanging provider close before rejecting a canceled approval", async () => {
    const registry = create(); const { provider } = fixture(); const close = deferred(); const entered = deferred(); const approval = deferred();
    provider.close = () => close.promise; registry.register(provider);
    const pending = invoke(registry, {}, {}, "demo.run", async () => { entered.resolve(); await approval.promise; });
    const rejected = expect(pending).rejects.toThrow(); await entered.promise; registry.revokeProvider("demo");
    try { await rejected; expect(provider.invoke).not.toHaveBeenCalled(); }
    finally { close.resolve(); approval.resolve(); }
  });

  it("cannot revive an unretained generation whose close has already started", async () => {
    const registry = create(); const { provider } = fixture(); const closing = deferred(); const release = deferred();
    provider.close = async () => { closing.resolve(); await release.promise; }; registry.register(provider); speculative(registry);
    const prepared = await registry.speculate("demo.run", {}, context, {});
    registry.unregister("demo"); await closing.promise;
    try { await expect(prepared!.execute(undefined)).rejects.toThrow(/unavailable/); expect(provider.invoke).not.toHaveBeenCalled(); }
    finally { release.resolve(); }
  });

  it("does not close a shared provider instance while a replacement still owns it", async () => {
    const registry = create(); const { provider } = fixture(); const first = registry.mount(provider);
    const pinned = await registry.acquireCapabilityView(["demo.run"], context);
    registry.register(provider, { overwrite: true }); await pinned.release(); await first.release();
    expect(provider.close).not.toHaveBeenCalled(); await invoke(registry); await registry.close();
    expect(provider.close).toHaveBeenCalledOnce();
  });

  it("captures cleanup once and cleans up an undeliverable acquisition", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); descriptor.effect = { kind: "scoped", resources: ["scope"] };
    const original = vi.fn(); const replacement = vi.fn(); const result = { value: 1, dispose: original };
    provider.acquire = async () => result; registry.register(provider);
    const acquired = await registry.acquireScoped("demo.run", {}, context); result.dispose = replacement; await acquired.dispose();
    expect(original).toHaveBeenCalledOnce(); expect(replacement).not.toHaveBeenCalled();
    provider.acquire = async () => ({ get value() { throw new Error("bad value"); }, dispose: original });
    await expect(registry.acquireScoped("demo.run", {}, context)).rejects.toThrow("bad value");
    expect(original).toHaveBeenCalledTimes(2);
  });

  it("races discovery cancellation while retaining ignored-abort catalog work", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); const entered = deferred(); const gate = deferred();
    provider.list = async () => { entered.resolve(); await gate.promise; return [descriptor]; };
    const lease = registry.mount(provider); const abort = new AbortController();
    const pending = registry.list({}, { ...context, signal: abort.signal }); const rejected = expect(pending).rejects.toThrow();
    await entered.promise; abort.abort(); await rejected; await lease.release(); expect(provider.close).not.toHaveBeenCalled();
    gate.resolve(); await vi.waitFor(() => expect(provider.close).toHaveBeenCalledOnce());
  });

  it("revalidates cached result authority before disclosure", async () => {
    const registry = create(); const { provider, descriptor } = fixture(); registry.register(provider);
    registry.setSpeculation({ bumpEpoch() {}, async tryServe() { descriptor.description = "changed after resolution"; return { hit: true, value: "secret", replay: {} }; } }, () => true);
    await expect(invoke(registry)).rejects.toThrow(/stale/); expect(provider.invoke).not.toHaveBeenCalled();
  });
});
