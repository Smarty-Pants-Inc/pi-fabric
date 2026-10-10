import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { claimFabricRuntimeInstance, isFabricRuntimeInstanceObsolete, MeshBackgroundRetry } from "../src/core/atomic-write.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { FabricState } from "../src/fabric-state.js";
import { readHostLeases } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { RootInbox } from "../src/topology/root-inbox.js";

const STALE = "This extension ctx is stale after session replacement or reload. Do not use a captured pi or command ctx after ctx.reload().";
const roots: string[] = [];
const cleanup: Array<() => Promise<unknown> | unknown> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const invalidatable = <T extends object>(target: T) => {
  let stale = false;
  return {
    value: new Proxy(target, { get(object, key, receiver) {
      if (stale) throw new Error(STALE);
      return Reflect.get(object, key, receiver);
    } }),
    invalidate: () => { stale = true; },
  };
};

const harness = () => {
  const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-8588-")));
  roots.push(base);
  for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
  const meshRoot = path.join(base, "mesh");
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", base);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(base, "agent"));
  fs.mkdirSync(path.join(base, ".pi"));
  fs.writeFileSync(path.join(base, ".pi", "fabric.json"), JSON.stringify({ fullCodeMode: false,
    mesh: { enabled: true, root: meshRoot, actorPollMs: 20 }, agents: { enabled: false }, residency: { enabled: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false } }));
  const sessionId = "8588aaaa-0000-0000-0000-000000000001";
  const session = () => invalidatable({
    cwd: base, hasUI: true, mode: "rpc", model: { provider: "faux", id: "m1" },
    isIdle: () => true, hasPendingMessages: () => false, isProjectTrusted: () => true, abort: () => {},
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
      getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus: () => {}, notify: () => {} },
  });
  const generation = () => {
    const pi = invalidatable({ on: () => () => {}, events: { emit: () => {}, on: () => () => {} },
      sendMessage: () => {}, appendEntry: () => {}, getThinkingLevel: () => "off", getSessionName: () => "epoch-main" });
    const state = new FabricState(pi.value as unknown as ExtensionAPI, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
      residentHost: path.join(base, "unused.mjs"), skills: base,
    } });
    cleanup.push(() => state.shutdown("exit"));
    return { state, pi, ctx: session() };
  };
  const activate = async (entry: ReturnType<typeof generation>, first = false) => {
    const ctx = entry.ctx.value as unknown as ExtensionContext;
    await entry.state.bootstrap(ctx);
    if (first || entry.state.shouldEagerlyActivate(ctx)) await entry.state.ensure(ctx);
    expect(entry.state.initialized).toBe(true);
  };
  return { meshRoot, sessionId, generation, activate, session,
    lease: () => readHostLeases(meshRoot).get(`session:${sessionId}`)?.updatedAt ?? 0 };
};

/** Census actual scheduled heartbeat handles, not implementation strings or total process timers. */
const timerCensus = () => {
  const scheduled = new Map<ReturnType<typeof setInterval>, () => void>();
  const interval = globalThis.setInterval;
  const clear = globalThis.clearInterval;
  // Other Main maintenance intervals also use 5 s. Count only scheduling performed
  // by the participant directory's executable start/rebind boundaries.
  let participantBoundary = false;
  const start = ParticipantDirectory.prototype.start;
  vi.spyOn(ParticipantDirectory.prototype, "start").mockImplementation(function (this: ParticipantDirectory) {
    participantBoundary = true;
    try { return start.call(this); } finally { participantBoundary = false; }
  });
  const rebind = ParticipantDirectory.prototype.rebindLifecycle;
  vi.spyOn(ParticipantDirectory.prototype, "rebindLifecycle").mockImplementation(function (this: ParticipantDirectory) {
    participantBoundary = true;
    try { return rebind.call(this); } finally { participantBoundary = false; }
  });
  vi.spyOn(globalThis, "setInterval").mockImplementation(((callback: (...args: unknown[]) => void,
    delay?: number, ...args: unknown[]) => {
    const timer = interval(callback, delay, ...args);
    if (participantBoundary && delay === 5_000) scheduled.set(timer, () => callback(...args));
    return timer;
  }) as typeof setInterval);
  vi.spyOn(globalThis, "clearInterval").mockImplementation((timer) => {
    if (typeof timer === "object" && timer) scheduled.delete(timer);
    clear(timer);
  });
  return scheduled;
};

const warningLines = (warn: ReturnType<typeof vi.spyOn>): string[] =>
  warn.mock.calls.map((call: unknown[]) => call.map(String).join(" "));

describe("epoch-owned background timer retirement (smarty-dev#8588)", () => {
  it("only a newer Fabric-owned instance cancels once, even when its Pi binding still looks live", async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const owner = claimFabricRuntimeInstance("obsolete-unit", "first");
    let attempts = 0;
    let timer: ReturnType<typeof setInterval>;
    const current = vi.fn(() => true);
    const retry = new MeshBackgroundRetry("participant heartbeat/change refresh", 100, 5_000, {
      epoch: 2, current,
      obsolete: () => isFabricRuntimeInstanceObsolete("obsolete-unit", owner),
      retire: () => clearInterval(timer),
    });
    timer = setInterval(() => void retry.run(() => { attempts++; }), 100);
    claimFabricRuntimeInstance("obsolete-unit", "second");
    await vi.advanceTimersByTimeAsync(100);
    expect(retry.retired).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    expect(current).not.toHaveBeenCalled();
    expect(attempts).toBe(0);
    expect(warningLines(warn)).toEqual([expect.stringContaining("retired obsolete background timer for epoch 2")]);
    retry.success();
    retry.rebind();
    await retry.run(() => { attempts++; });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(attempts).toBe(0);
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it.each(["probe", "tick"])("an unrelated Error with Pi's stale prefix on the CURRENT %s retries, counts failures, and resets once recovered", async via => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    let failing = true;
    const failures: number[] = [];
    const retire = vi.fn();
    const retry = new MeshBackgroundRetry("current-owner", 100, 400, {
      epoch: 1, obsolete: () => false,
      current: () => { if (failing && via === "probe") throw new Error(`${STALE} unrelated bug`); return true; },
      retire, onFailure: (_error, count) => { failures.push(count); },
    });
    const operation = () => { if (failing && via === "tick") throw new Error(`${STALE} unrelated bug`); };
    for (const wait of [0, 50, 100, 200]) {
      await vi.advanceTimersByTimeAsync(wait);
      await retry.run(operation);
    }
    expect(failures).toEqual([1, 2, 3, 4]);
    expect(retire).not.toHaveBeenCalled();
    expect(retry.retired).toBe(false);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warningLines(warn).filter(line => line.includes(STALE))).toEqual([]);
    failing = false;
    await vi.advanceTimersByTimeAsync(200);
    expect(await retry.run(operation)).toBe("done");
    failing = true;
    await retry.run(operation);
    expect(failures).toEqual([1, 2, 3, 4, 1]);
    expect(warn).toHaveBeenCalledTimes(2);
  });

  it("a transient CURRENT context-probe failure keeps the heartbeat and renews on the next tick", async () => {
    const h = harness();
    const scheduled = timerCensus();
    vi.spyOn(Math, "random").mockReturnValue(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const one = h.generation();
    await h.activate(one, true);
    const tick = [...scheduled.values()][0]!;
    const probe = vi.spyOn(one.ctx.value, "isIdle").mockImplementationOnce(() => { throw new Error("transient current probe"); });
    const before = h.lease();
    tick();
    expect(scheduled.size).toBe(1);
    expect(h.lease()).toBe(before);
    await new Promise(resolve => setTimeout(resolve, 10));
    tick();
    await vi.waitFor(() => expect(h.lease()).toBeGreaterThan(before));
    expect(scheduled.size).toBe(1);
    expect(probe.mock.calls.length).toBeGreaterThan(1);
    expect(warningLines(warn).filter(line => line.includes("retired"))).toEqual([]);
    console.info("8588 transient probe", JSON.stringify({ heartbeatTimers: scheduled.size, leaseRenewed: h.lease() > before }));
  }, 30_000);

  it("an unrelated stale-prefix tick error on the CURRENT runtime reports degraded after three failures, never retires, and recovers", async () => {
    const h = harness();
    const scheduled = timerCensus();
    vi.spyOn(Math, "random").mockReturnValue(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    let directory: ParticipantDirectory | undefined;
    const register = ParticipantDirectory.prototype.registerSource;
    vi.spyOn(ParticipantDirectory.prototype, "registerSource").mockImplementation(function (this: ParticipantDirectory, source) {
      directory = this;
      return register.call(this, source);
    });
    const one = h.generation();
    await h.activate(one, true);
    const tick = [...scheduled.values()][0]!;
    const refresh = vi.spyOn(directory!, "refresh").mockRejectedValue(new Error(`${STALE} unrelated storage error`));
    const before = h.lease();
    for (let i = 0; i < 3; i++) {
      tick();
      await new Promise(resolve => setTimeout(resolve, 10));
      expect(directory!.degraded).toBe(i === 2);
      expect(scheduled.size).toBe(1);
    }
    expect(h.lease()).toBe(before);
    for (let i = 0; i < 3; i++) {
      tick();
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    expect(warningLines(warn).filter(line => line.includes("is degraded"))).toHaveLength(1);
    expect(warningLines(warn).filter(line => line.includes("retired"))).toEqual([]);
    refresh.mockRestore();
    tick();
    await vi.waitFor(() => expect(h.lease()).toBeGreaterThan(before));
    expect(directory!.degraded).toBe(false);
    expect(scheduled.size).toBe(1);
    console.info("8588 unrelated stale-prefix tick", JSON.stringify({ heartbeatTimers: scheduled.size,
      degradedReports: 1, recovered: !directory!.degraded, leaseRenewed: h.lease() > before }));
  }, 30_000);

  it("two interrupted reloads leave exactly one heartbeat, no stale failure spam, and a renewing lease", async () => {
    const runtimes: FabricRuntimeState[] = [];
    const bind = FabricRuntimeState.prototype.bindLifecycle;
    vi.spyOn(FabricRuntimeState.prototype, "bindLifecycle").mockImplementation(function (this: FabricRuntimeState, ctx, current) {
      if (!runtimes.includes(this)) runtimes.push(this);
      return bind.call(this, ctx, current);
    });
    const h = harness();
    const scheduled = timerCensus();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const one = h.generation();
    await h.activate(one, true);
    one.ctx.invalidate(); one.pi.invalidate();
    // Deliberately omit old session_shutdown: /reload during /reload can strand its old instance.
    const two = h.generation();
    await h.activate(two);
    two.ctx.invalidate(); two.pi.invalidate();
    const three = h.generation();
    await h.activate(three); // #660 re-arm on activation, without first tool use
    expect(scheduled.size).toBe(3);
    const scheduledBeforeFirstTick = scheduled.size;
    const callbacks = [...scheduled.values()];
    const before = h.lease();
    await new Promise(resolve => setTimeout(resolve, 10));
    for (const tick of callbacks) tick();
    expect(scheduled.size).toBe(1);
    await vi.waitFor(() => expect(h.lease()).toBeGreaterThan(before));
    const retirements = warningLines(warn).filter(line => line.includes("participant heartbeat/change refresh: retired obsolete background timer"));
    expect(retirements).toHaveLength(2);
    expect(retirements.every(line => line.includes("epoch 1"))).toBe(true); // epoch ids are local to each extension instance
    const afterFirstTick = warn.mock.calls.length;
    // A late callback from either old extension must not take the process-global record back.
    for (const runtime of runtimes.slice(0, 2)) runtime.bindLifecycle(h.session().value as unknown as ExtensionContext, () => true);
    expect(scheduled.size).toBe(1);
    for (let i = 0; i < 4; i++) for (const tick of callbacks.slice(0, 2)) tick();
    expect(warn.mock.calls).toHaveLength(afterFirstTick);
    expect(warningLines(warn).filter(line => line.includes("This extension ctx is stale"))).toEqual([]);
    const renewed = h.lease();
    await new Promise(resolve => setTimeout(resolve, 10));
    for (const tick of scheduled.values()) tick();
    await vi.waitFor(() => expect(h.lease()).toBeGreaterThan(renewed));
    expect(scheduled.size).toBe(1);
    console.info("8588 timer census", JSON.stringify({ generations: 3, scheduledBeforeFirstTick,
      scheduledAfterFirstTick: scheduled.size, retiredHeartbeats: retirements.length,
      repeatedOldTickWarnings: warn.mock.calls.length - afterFirstTick,
      staleFailureLines: warningLines(warn).filter(line => line.includes("This extension ctx is stale")).length,
      leaseRenewed: h.lease() > renewed }));
  }, 30_000);

  it.each(["ctx read", "lease check"])("a stale CURRENT %s keeps its timer, raises #660 after three failures, and recovers on a live rebind", async staleVia => {
    const h = harness();
    const scheduled = timerCensus();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    let inbox: RootInbox | undefined;
    const inboxStart = RootInbox.prototype.start;
    vi.spyOn(RootInbox.prototype, "start").mockImplementation(function (this: RootInbox) {
      inbox = this;
      return inboxStart.call(this);
    });
    const directories: ParticipantDirectory[] = [];
    const register = ParticipantDirectory.prototype.registerSource;
    vi.spyOn(ParticipantDirectory.prototype, "registerSource").mockImplementation(function (this: ParticipantDirectory, source) {
      if (!directories.includes(this)) directories.push(this);
      return register.call(this, source);
    });
    let runtime: FabricRuntimeState | undefined;
    const bind = FabricRuntimeState.prototype.bindLifecycle;
    vi.spyOn(FabricRuntimeState.prototype, "bindLifecycle").mockImplementation(function (this: FabricRuntimeState, ctx, current) {
      runtime = this;
      return bind.call(this, ctx, current);
    });
    const one = h.generation();
    await h.activate(one, true);
    const tick = [...scheduled.values()][0]!;
    vi.spyOn(Math, "random").mockReturnValue(0);
    let leaseChecks = 0;
    if (staleVia === "lease check") {
      runtime!.bindLifecycle(one.ctx.value as unknown as ExtensionContext, () => { leaseChecks++; throw new Error(STALE); });
    } else {
      one.ctx.invalidate(); // lease still says current: only the ctx probe discovers the outage
    }
    for (let i = 0; i < 3; i++) {
      tick();
      expect(scheduled.size).toBe(1);
      expect(directories[0]!.degraded).toBe(i === 2);
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    await vi.waitFor(() => {
      const events = runtime!.mesh.read({ topic: "ops.fabric.presence" }).filter(event => event.kind === "fabric.presence.degraded");
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toMatchObject({ participantId: `session:${h.sessionId}`, hostId: `session:${h.sessionId}`, ticks: 3 });
    });
    const noticesAfterDegraded = warn.mock.calls.length;
    for (let i = 0; i < 4; i++) {
      tick();
      inbox!.names();
      void runtime!.mesh.stateBackendHandle.backgroundReadCacheMs;
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    const checksDuringOutage = leaseChecks;
    if (staleVia === "lease check") expect(checksDuringOutage).toBeGreaterThan(3);
    expect(scheduled.size).toBe(1);
    const degradedWarnings = warningLines(warn).filter(line => line.includes("is degraded"));
    expect(degradedWarnings).toHaveLength(1);
    // Reads have their own deduplicated outage warning, not one warning per stale read.
    expect(warn.mock.calls.length - noticesAfterDegraded).toBeLessThanOrEqual(2);
    expect(warningLines(warn).filter(line => line.includes("This extension ctx is stale"))).toEqual([]);
    const live = h.session();
    runtime!.bindLifecycle(live.value as unknown as ExtensionContext, () => true);
    expect(scheduled.size).toBe(1);
    expect(directories[0]!.degraded).toBe(false);
    const before = h.lease();
    await new Promise(resolve => setTimeout(resolve, 10));
    for (const currentTick of scheduled.values()) currentTick();
    await vi.waitFor(() => expect(h.lease()).toBeGreaterThan(before));
    console.info("8588 current-binding probe", JSON.stringify({ staleVia, scheduledAfterLiveRebind: scheduled.size,
      degradedCleared: !directories[0]!.degraded, leaseChecksWhileRetrying: checksDuringOutage,
      leaseRenewed: h.lease() > before }));
  }, 30_000);
});
