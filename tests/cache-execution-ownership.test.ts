import type { ChildProcess } from "node:child_process";
import { setTimeout as realDelay } from "node:timers/promises";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { CacheProvider } from "../src/providers/cache-provider.js";
import type { FabricCacheStatus, FabricInvocationContext } from "../src/protocol.js";

// Capture executor children so a test can lose one without a pending host call.
const children = vi.hoisted(() => [] as ChildProcess[]);
vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return {
    ...actual,
    spawn: ((...args: Parameters<typeof actual.spawn>) => {
      const child = actual.spawn(...args);
      children.push(child);
      return child;
    }) as typeof actual.spawn,
  };
});

/** A host that implements the optional native warming capability. */
function fixture(timeoutMs = 10_000) {
  const nativeOwners = new Set<symbol>();
  const releaseNative = vi.fn((key: symbol) => { nativeOwners.delete(key); });
  const acquire = vi.fn(() => { const key = Symbol(); nativeOwners.add(key); return () => releaseNative(key); });
  const host = {
    cwd: process.cwd(), hasUI: false,
    model: { provider: "test", id: "model" }, thinkingLevel: "off", getSystemPrompt: () => "stable prompt",
    sessionManager: { getSessionId: () => "session", getLeafEntry: () => undefined, getEntry: () => undefined },
    acquireCacheWarming: acquire,
  } as unknown as ExtensionContext;
  const pi = { getActiveTools: () => ["fabric_exec"], getThinkingLevel: () => "off", on: () => () => undefined };
  const provider = new CacheProvider(pi as unknown as ExtensionAPI, host, true);
  let settled!: () => void;
  const held = new Promise<void>(resolve => { settled = resolve; });
  const registry = new ActionRegistry();
  registry.register(provider);
  const ready = { name: "ready", description: "hold settled", risk: "read" as const, inputSchema: { type: "object", additionalProperties: false } };
  registry.register({
    name: "probe", description: "test probe",
    async list() { return [ready]; },
    async describe(name) { return name === "ready" ? ready : undefined; },
    async invoke() { settled(); return true; },
  });
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.fullCodeMode = false;
  config.executor.runtime = "node-process";
  config.executor.timeoutMs = timeoutMs;
  config.approvals.read = "allow";
  config.approvals.agent = "allow";
  const service = new FabricExecutionService(registry, config);
  const ctx = { cwd: process.cwd(), signal: undefined, parentToolCallId: "probe", nestedToolCallId: "probe", extensionContext: host, update() {} } as FabricInvocationContext;
  const leases = async () => ((await provider.invoke("status", {}, ctx)) as FabricCacheStatus).leases;
  const run = (code: string, parentToolCallId: string, signal?: AbortSignal) => service.execute({
    code, signal, parentToolCallId, context: host, onPartial() {},
  });
  return { provider, ctx, leases, run, held, acquire, releaseNative, nativeOwners };
}

// The hold settles, then the probe settles, then the guest waits with no host call pending.
const holdThenHang = `
const lease = await tools.call({ ref: "cache.hold", args: { durationMs: 600000 } });
if (lease.status !== "held") throw new Error(lease.reason);
await tools.call({ ref: "probe.ready", args: {} });
await new Promise(() => {});
`;
// Let the probe response reach the child so no host task remains.
const quiesce = () => new Promise(resolve => setTimeout(resolve, 200));

describe("cache holds through the Node executor", () => {
  it("releases the hold of an execution that reaches its deadline", async () => {
    const timeoutMs = 1_500;
    // Freeze only host time: the real child must acquire a native hold before
    // we advance the production shared deadline, regardless of startup speed.
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const f = fixture(timeoutMs);
    const abort = new AbortController();
    const readiness = new AbortController();
    const execution = f.run(holdThenHang, "deadline", abort.signal);
    try {
      await Promise.race([
        f.held,
        execution.then(result => { throw new Error(`Execution settled before cache hold: ${result.error}`); }),
        realDelay(10_000, undefined, { signal: readiness.signal }).then(() => { throw new Error("Cache hold readiness timed out"); }),
      ]);
      readiness.abort();
      // Let the probe response reach the real child with no host call pending.
      await realDelay(200);
      expect(f.acquire).toHaveBeenCalledOnce();
      expect(f.nativeOwners.size).toBe(1);
      expect(await f.leases()).toHaveLength(1);
      expect(f.releaseNative).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(timeoutMs - 1);
      expect(f.nativeOwners.size).toBe(1);
      expect(f.releaseNative).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      const result = await execution;
      expect(result.success).toBe(false);
      expect(result.error).toMatch(/timed out/);
      expect(f.acquire).toHaveBeenCalledOnce();
      expect(f.releaseNative).toHaveBeenCalledOnce();
      expect(f.nativeOwners.size).toBe(0);
      expect(await f.leases()).toEqual([]);
    } finally {
      readiness.abort();
      abort.abort();
      vi.useRealTimers();
      await execution;
      await f.provider.close();
    }
  }, 20_000);

  it("releases the hold of a cancelled execution", async () => {
    const f = fixture();
    const abort = new AbortController();
    const execution = f.run(holdThenHang, "cancelled", abort.signal);
    await f.held; await quiesce();
    expect(await f.leases()).toHaveLength(1);
    abort.abort();
    const result = await execution;
    expect(result.success).toBe(false);
    expect(f.releaseNative).toHaveBeenCalledOnce();
    expect(f.nativeOwners.size).toBe(0);
    expect(await f.leases()).toEqual([]);
    await f.provider.close();
  }, 20_000);

  it("releases the hold of an execution whose child exits", async () => {
    const f = fixture();
    const before = children.length;
    const execution = f.run(holdThenHang, "child-exit");
    await f.held; await quiesce();
    const child = children.slice(before).at(-1)!;
    expect(child.kill("SIGKILL")).toBe(true);
    const result = await execution;
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/exited before returning/);
    expect(f.releaseNative).toHaveBeenCalledOnce();
    expect(f.nativeOwners.size).toBe(0);
    expect(await f.leases()).toEqual([]);
    await f.provider.close();
  }, 20_000);

  it("keeps a successful execution's hold until explicit release, and a failed one keeps other holds", async () => {
    const f = fixture();
    const result = await f.run(`return await tools.call({ ref: "cache.hold", args: { durationMs: 600000 } });`, "success");
    expect(result.success, result.error).toBe(true);
    const lease = result.value as { status: string; id: string };
    expect(lease.status).toBe("held");
    expect((await f.leases()).map(held => held.id)).toEqual([lease.id]);
    expect(f.releaseNative).not.toHaveBeenCalled();

    const failed = await f.run(`await tools.call({ ref: "cache.hold", args: { durationMs: 600000 } }); throw new Error("boom");`, "failure");
    expect(failed.success).toBe(false);
    expect((await f.leases()).map(held => held.id)).toEqual([lease.id]);
    expect(f.nativeOwners.size).toBe(1);

    const released = await f.run(`return await tools.call({ ref: "cache.release", args: { id: ${JSON.stringify(lease.id)} } });`, "release");
    expect(released.value).toEqual({ released: true, cleanupError: null });
    expect(f.releaseNative).toHaveBeenCalledOnce();
    expect(f.nativeOwners.size).toBe(0);
    await f.provider.close();
  }, 20_000);
});
