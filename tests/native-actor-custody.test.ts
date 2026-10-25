// Keep: custody/data-safety failure paths are expensive to reach through a broker. These use
// real serial native callbacks and genuine ingress proof, not a fake AgentManager run ID.
// Real-process 11s synchronous stall/observer/lease evidence is intentionally NOT claimed here.
import { afterEach, describe, expect, it, vi } from "vitest";
import { createNativeActorFixture, deferred, nativePosix, type NativeActorFixture } from "./helpers/native-actor-ingress.js";

const fixtures: NativeActorFixture[] = [];
const fixture = async (...args: Parameters<typeof createNativeActorFixture>) => {
  const f = await createNativeActorFixture(...args); fixtures.push(f); return f;
};
afterEach(async () => { vi.restoreAllMocks(); await Promise.all(fixtures.splice(0).map(f => f.close())); });

describe.skipIf(!nativePosix)("native callback custody belongs to actual promise settlement", () => {
  it("stop/abort/failed close retain custody while a callback ignores its signal", async () => {
    const entered = deferred<void>(), release = deferred<void>();
    let signal: AbortSignal | undefined;
    const f = await fixture({ handler: async (_request, context) => {
      signal = context.signal; entered.resolve(); await release.promise; return { data: { committed: true } };
    } });
    const spies = f.modelSpies();
    const pending = f.ask("rpc", { body: "SECRET_CUSTODY" }).catch(error => error as Error);
    try {
      await entered.promise;
      expect(f.actors.inFlightCount()).toBe(1); expect(f.actors.releaseCustodyCount()).toBeGreaterThan(0);
      expect(f.actors.status(f.binding.actorId)).not.toHaveProperty("inFlightRun");
      await f.actors.stop(f.binding.actorId);
      expect(signal?.aborted).toBe(true);
      expect(await pending).toBeInstanceOf(Error);
      expect(f.actors.inFlightCount()).toBe(1);
      f.actors.pauseForRelease({ strict: true });
      await expect(f.actors.checkpointForRelease()).rejects.toThrow(/not quiescent/);
      await expect(f.actors.close()).rejects.toMatchObject({ name: "NativeActorBusyError", code: "FABRIC_NATIVE_ACTOR_BUSY" });
      expect(f.actors.releaseCustodyCount()).toBeGreaterThan(0);
      for (const spy of spies) expect(spy).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      await f.actors.stop(f.binding.actorId, undefined, true);
    }
    expect(f.actors.inFlightCount()).toBe(0);
    await f.actors.close(); // retry only after ACTUAL callback settlement
  });

  it("removal stays pending behind the actual callback instead of treating absence of a run ID as settlement", async () => {
    const entered = deferred<void>(), release = deferred<void>();
    const f = await fixture({ handler: async () => { entered.resolve(); await release.promise; return { data: { committed: true } }; } });
    const pending = f.ask("rpc", { effect: "uncertain" }).catch(error => error as Error);
    let settlement: Promise<void> | undefined;
    try {
      await entered.promise;
      const result = await f.actors.remove(f.binding.actorId, { wait: false });
      expect(result.pending).toMatch(/pending/); expect(result.cleaned).not.toBe(true);
      settlement = f.actors.removalSettled(f.binding.actorId);
      expect(settlement).toBeDefined();
      expect(f.actors.releaseCustodyCount()).toBeGreaterThan(0);
      expect(await pending).toBeInstanceOf(Error);
      expect(f.handler).toHaveBeenCalledTimes(1);
    } finally {
      release.resolve(); await settlement;
    }
    expect(f.actors.inFlightCount()).toBe(0);
  });

  it("queued cancellation removes the invocation, while in-flight cancellation retains the actual promise", async () => {
    const entered = deferred<void>(), release = deferred<void>();
    let calls = 0;
    const f = await fixture({ handler: async () => { ++calls; entered.resolve(); await release.promise; return { data: { ok: true } }; } });
    const firstController = new AbortController(), queuedController = new AbortController();
    const first = f.ask("rpc", { n: 1 }, firstController.signal).catch(error => error as Error);
    try {
      await entered.promise;
      const proof = await f.issue("rpc", { n: 2 });
      const second = f.actors.ask(f.binding.actorId, "rpc", { n: 2 }, queuedController.signal, proof).catch(error => error as Error);
      await Promise.resolve(); await Promise.resolve();
      queuedController.abort();
      expect(await second).toBeInstanceOf(Error);
      firstController.abort();
      expect(await first).toBeInstanceOf(Error);
      expect(calls).toBe(1); expect(f.actors.releaseCustodyCount()).toBeGreaterThan(0);
    } finally {
      release.resolve();
      await f.actors.stop(f.binding.actorId, undefined, true);
    }
    expect(calls).toBe(1);
  });

  it("rechecks deadline at callback entry after queue waiting; expired proof never invokes", async () => {
    const entered = deferred<void>(), release = deferred<void>();
    let calls = 0;
    const f = await fixture({ handler: async () => {
      ++calls; if (calls === 1) { entered.resolve(); await release.promise; } return { data: { ok: true } };
    } });
    try {
      const first = f.ask("rpc", { n: 1 }); await entered.promise;
      const proof = await f.issue("rpc", { n: 2 }, 500);
      const queued = f.actors.ask(f.binding.actorId, "rpc", { n: 2 }, undefined, proof).catch(error => error as Error);
      // Native admission uses its injected monotonic fixture clock; no timer/sleep polling needed.
      await Promise.resolve(); await Promise.resolve();
      f.clock.now += 501;
      release.resolve(); await first;
      expect(await queued).toBeInstanceOf(Error); expect(calls).toBe(1);
    } finally { release.resolve(); }
  });

  it("keeps a session native actor out of idle dormancy without changing any timer", async () => {
    const f = await fixture();
    expect(f.actors.hasDormantIdleActor()).toBe(false);
    expect(await f.actors.dormantIdleActors()).toBe(0);
    expect(f.actors.status(f.binding.actorId).status).toBe("idle");
  });

  it("uses the narrow pre-READY birth option ONLY for ensure, never admitted asks or execution", async () => {
    let ready = false, materializing = false;
    const f = await fixture({ ensure: false, managerOptions: { releasePaused: true,
      canConsumeMesh: () => ready, canMaterializeNative: () => materializing } });
    const spies = f.modelSpies();
    await expect(f.actors.ensureNativeActor(f.binding)).rejects.toThrow(/Native/);
    materializing = true;
    try { await f.actors.ensureNativeActor(f.binding); }
    finally { materializing = false; }
    expect(f.rows()).toHaveLength(1);
    const proof = await f.issue("rpc", { preReady: true });
    await expect(f.actors.ask(f.binding.actorId, "rpc", { preReady: true }, undefined, proof)).rejects.toThrow(/Native/);
    f.actors.resumeQueued();
    expect(f.handler).not.toHaveBeenCalled(); expect(f.actors.releaseCustodyCount()).toBe(0);
    expect(ready).toBe(false); // only the real host, NOT this option/manager, owns READY publication
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
    ready = true; f.actors.resumeAfterRelease();
    expect((await f.ask("rpc", { ready: true })).data).toEqual({ ready: true });
  });

  it("rechecks the birth precondition inside custody after a suspension; no row/callback/model is published", async () => {
    let materializing = true;
    const f = await fixture({ ensure: false, managerOptions: { canConsumeMesh: () => false, canMaterializeNative: () => materializing } });
    const spies = f.modelSpies();
    const registry = new (await import("../src/actors/registry-store.js")).ActorRegistryStore(f.actorRoot);
    const held = deferred<void>(), unlock = deferred<void>();
    const blocker = registry.withLock(async () => { held.resolve(); await unlock.promise; });
    await held.promise;
    const birth = f.actors.ensureNativeActor(f.binding).catch(error => error as Error);
    materializing = false; unlock.resolve(); await blocker;
    expect(await birth).toBeInstanceOf(Error); expect(f.rows()).toEqual([]);
    expect(f.handler).not.toHaveBeenCalled();
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });
});
