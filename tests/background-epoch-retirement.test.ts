import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { MeshBackgroundRetry } from "../src/core/atomic-write.js";
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
  it.each(["obsolete epoch", "stale tick", "stale lease"])("%s cancels once and never retries", async reason => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    let current = true;
    let attempts = 0;
    let staleReports = 0;
    let timer: ReturnType<typeof setInterval>;
    const retry = new MeshBackgroundRetry("participant heartbeat/change refresh", 100, 5_000, {
      epoch: 2,
      current: () => { if (reason === "stale lease") throw new Error(STALE); return current; },
      retire: () => clearInterval(timer),
      onStale: () => { staleReports++; },
    });
    timer = setInterval(() => void retry.run(async () => { attempts++; throw new Error(STALE); }), 100);
    if (reason === "obsolete epoch") current = false;
    await vi.advanceTimersByTimeAsync(100);
    expect(retry.retired).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    expect(attempts).toBe(reason === "stale tick" ? 1 : 0);
    expect(staleReports).toBe(reason === "obsolete epoch" ? 0 : 1);
    expect(warningLines(warn)).toEqual([expect.stringContaining("participant heartbeat/change refresh: retired background timer for epoch 2")]);
    retry.success(); // success/backoff reset must never revive a terminal owner
    await retry.run(() => { attempts++; });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(attempts).toBe(reason === "stale tick" ? 1 : 0);
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("two interrupted reloads leave exactly one heartbeat, no stale failure spam, and a renewing lease", async () => {
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
    const retirements = warningLines(warn).filter(line => line.includes("participant heartbeat/change refresh: retired background timer"));
    expect(retirements).toHaveLength(2);
    expect(retirements.every(line => line.includes("epoch 1"))).toBe(true); // epoch ids are local to each extension instance
    const afterFirstTick = warn.mock.calls.length;
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

  it.each(["ctx read", "lease check"])("a stale CURRENT %s raises #660's degraded ops report and a live bind re-arms it", async staleVia => {
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
    let leaseChecks = 0;
    if (staleVia === "lease check") {
      runtime!.bindLifecycle(one.ctx.value as unknown as ExtensionContext, () => { leaseChecks++; throw new Error(STALE); });
    } else {
      one.ctx.invalidate(); // lease still says current: only the ctx probe discovers the outage
    }
    tick();
    expect(scheduled.size).toBe(0);
    expect(directories[0]!.degraded).toBe(true);
    await vi.waitFor(() => {
      const events = runtime!.mesh.read({ topic: "ops.fabric.presence" }).filter(event => event.kind === "fabric.presence.degraded");
      expect(events).toHaveLength(1);
      expect(events[0]!.data).toMatchObject({ participantId: `session:${h.sessionId}`, hostId: `session:${h.sessionId}`, ticks: 3 });
    });
    tick(); // even an already-queued tick cannot duplicate the report
    // Public reader interfaces must freeze as well: not even a stale lease getter is retried.
    inbox!.names();
    void runtime!.mesh.stateBackendHandle.backgroundReadCacheMs;
    const checksAfterRetirement = leaseChecks;
    const noticesAfterRetirement = warn.mock.calls.length;
    for (let i = 0; i < 4; i++) {
      inbox!.names();
      void runtime!.mesh.stateBackendHandle.backgroundReadCacheMs;
    }
    expect(leaseChecks).toBe(checksAfterRetirement);
    expect(warn.mock.calls).toHaveLength(noticesAfterRetirement);
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
      degradedCleared: !directories[0]!.degraded, staleLeaseChecksAfterRetirement: leaseChecks - checksAfterRetirement,
      leaseRenewed: h.lease() > before }));
  }, 30_000);
});
