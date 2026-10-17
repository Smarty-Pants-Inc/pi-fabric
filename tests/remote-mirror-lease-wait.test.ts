// smarty-dev#11703: a live origin's remote mirror lease lapses (BRIDGE_LEASE_MS) when the
// bridge is briefly silent. followUp/steer must wait once, notification-driven and bounded by
// 15 s, for the bridge renewal; then deliver exactly once. Without renewal they fail with the
// typed retryable FabricPeerLeaseStaleError, never Unknown and never a publication to the
// stale mirror. A truly absent record stays Unknown; a native local peer never waits.
//
// Decision C (clock):
// | classification | why the clock is mocked | real boundary exercised | missing live proof |
// | mocked clock (constant offset only) | Date.now = real now + offset, so 16 s of bridge silence is
//   simulated without sleeping 16 s; time still flows in real milliseconds, so the 15 s wait bound,
//   fs notifications, timers and control-plane polling are all real | two real MeshStores, a real
//   MeshBridge with StoreBridgeSide on both sides, real ParticipantDirectory, real FabricControlPlane
//   sender/receiver, real AgentsProvider.acceptControl -> MainAgentController receiving handler; the
//   no-renewal case waits the real (<=15 s) bound | no ssh/RemoteBridgeSide transport, no second
//   machine, and no live Pi session on the far host; the 15 s lapse itself is offset, not observed |
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { BRIDGE_LEASE_MS, MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricPeerLeaseStaleError, PEER_LEASE_WAIT_MS } from "../src/agents/peer-lease-stale.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { readHostLease, writeHostLease } from "../src/topology/host-leases.js";

const realSetTimeout = globalThis.setTimeout;
const realNow = Date.now.bind(Date);
const sleep = (ms: number) => new Promise<void>(resolve => realSetTimeout(resolve, ms));
const roots: string[] = [];
const cleanups: Array<() => unknown> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) { try { await cleanup(); } catch { /* fixture teardown */ } }
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const scratch = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-11703-")); roots.push(root); return root; };
const hash = (id: string) => createHash("sha256").update(id).digest("hex");
const sid = (name: string) => `session:${name}-00000000`;
const on = (store: MeshStore, topic: string): MeshEvent[] => store.read({ after: 0, limit: 200 }).filter(e => e.topic === topic);
const waitFor = async (check: () => boolean, ms = 5_000) => {
  const until = performance.now() + ms;
  while (!check() && performance.now() < until) await sleep(10);
  expect(check()).toBe(true);
};

const addRoot = async (store: MeshStore, name: string, expiresIn = 60_000) => {
  const now = Date.now(), hostId = sid(name), ownerIncarnation = `fixture:${hostId}`;
  const identity: MeshIdentity = { id: hostId, name: "main", kind: "main", sessionId: name };
  await store.put({ key: `topology/hosts/${hash(hostId)}`, identity,
    value: { format: 1, id: hostId, rootId: hostId, identity, startedAt: now, updatedAt: now, expiresAt: now + expiresIn } });
  await store.put({ key: `topology/participants/${hash(hostId)}`, identity, value: {
    format: 1, id: hostId, kind: "root", rootId: hostId, ownerHostId: hostId, ownerIdentityId: hostId, ownerIncarnation,
    name, label: name.toUpperCase(), status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
    sessionId: name, startedAt: now, updatedAt: now, controlProtocol: "v1",
  } });
  return { hostId, identity, ownerIncarnation };
};

/** Hub "dev1" (sender) and far "forge" (live origin), joined by a real bridge that then goes silent. */
const world = async (waitMs?: number, heldMain = false) => {
  let offset = 0;
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + offset);
  const hub = new MeshStore(scratch(), 64 * 1024, 100), far = new MeshStore(scratch(), 64 * 1024, 100);
  const lane = await addRoot(hub, "lane");
  const local = await addRoot(hub, "local-peer");
  const target = await addRoot(far, "remote"); // live origin: its own lease stays fresh throughout
  const bridge = new MeshBridge({ localName: "dev1", remoteName: "forge", presenceMs: 0,
    cursorPath: path.join(scratch(), "cursor.json"), local: new StoreBridgeSide(hub, "forge"), remote: new StoreBridgeSide(far, "dev1"),
    log: () => undefined, debug: () => undefined });
  cleanups.push(() => bridge.stop());
  const directory = (store: MeshStore, identity: MeshIdentity) => {
    const d = new ParticipantDirectory(store, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
      ...(waitMs === undefined ? {} : { routingLease: { waitMs } }) });
    cleanups.push(() => d.close()); return d;
  };
  const hubDirectory = directory(hub, lane.identity), farDirectory = directory(far, target.identity);
  const plane = (store: MeshStore, who: { hostId: string; identity: MeshIdentity; ownerIncarnation: string }, mirrored?: ParticipantDirectory) => {
    const p = new FabricControlPlane(store, who.identity, { enabled: true, hostId: who.hostId, ownerIncarnation: who.ownerIncarnation, pollMs: 20,
      ...(mirrored ? { readMirroredOwner: (...args: Parameters<ParticipantDirectory["mirroredControlOwner"]>) => mirrored.mirroredControlOwner(...args) } : {}) });
    cleanups.push(() => p.close()); return p;
  };
  const sender = plane(hub, lane, hubDirectory);
  const receiver = plane(far, target);
  const localReceiver = plane(hub, local);

  // The true receiving path on the origin: control -> AgentsProvider.acceptControl -> Main controller -> pi.sendMessage.
  const sendMessage = vi.fn();
  const pi = { sendMessage, on: () => undefined } as unknown as ExtensionAPI;
  const ctx = { isIdle: () => !heldMain, hasPendingMessages: () => false,
    sessionManager: { getEntries: () => [] } } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, target.identity.id, true, os.tmpdir(), "remote");
  const journal = path.join(scratch(), "followups.json");
  main.attachFollowUpDrain(ctx, 120_000, journal);
  cleanups.push(() => main.closeFollowUpDrain());
  const receiving = new AgentsProvider({ cwd: os.tmpdir() } as any, { identity: target.identity } as any, {} as any, main,
    farDirectory, receiver, {} as any, () => false, undefined, false);
  const localHandler = vi.fn(() => ({ accepted: true, messageId: "local-delivery" }));

  type Ports = ConstructorParameters<typeof AgentMessageRouter>;
  const sending = new AgentMessageRouter(
    { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0],
    { identity: lane.identity, status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); }, owns: () => false,
      resolveBinding: (_id: string, binding: unknown) => binding, validateDirectMessage: () => undefined } as unknown as Ports[1],
    { id: lane.identity.id, local: true, matches: (id: string) => id === lane.identity.id } as unknown as Ports[2],
    hubDirectory, sender, binding => binding);

  sender.start(() => ({ accepted: false }));
  const admissions: Array<{ id: string; message: string; at: number; verification: string | undefined }> = [];
  receiver.start(async (cmd, from, signal, verification) => {
    const result = await receiving.acceptControl(cmd, from, signal, verification);
    if (heldMain && result.accepted) {
      const items = JSON.parse(fs.readFileSync(journal, "utf8")).items as Array<{ deliveryId: string; message: string }>;
      expect(items.some(item => item.deliveryId === cmd.commandId && item.message === cmd.message)).toBe(true);
      admissions.push({ id: cmd.commandId, message: cmd.message!, at: performance.now(), verification });
    }
    return result;
  });
  localReceiver.start(localHandler);
  await bridge.start();
  await bridge.syncPresence();
  // Precondition: the mirror is live, capped at one bridge lease from this sync.
  const mirror = () => hubDirectory.mirroredControlOwner(target.hostId, target.identity.id, target.identity.id);
  expect(mirror()).toMatchObject({ remoteHost: "forge" });
  expect(mirror()!.expiresAt).toBeLessThanOrEqual(Date.now() + BRIDGE_LEASE_MS);
  // The bridge is silent for 16 s: its mirror lapses by ~1 s while the origin stays live.
  offset = BRIDGE_LEASE_MS + 1_000;
  expect(mirror()!.expiresAt).toBeLessThan(Date.now());
  expect(far.get(`topology/hosts/${hash(target.hostId)}`)!.value).toMatchObject({ expiresAt: expect.any(Number) });
  expect((far.get(`topology/hosts/${hash(target.hostId)}`)!.value as { expiresAt: number }).expiresAt).toBeGreaterThan(Date.now());
  return { hub, far, bridge, lane, local, target, hubDirectory, sending, sender, sendMessage, localHandler, mirror, journal, admissions };
};

const settle = <T>(promise: Promise<T>) => {
  const state: { done?: { value?: T; error?: unknown } } = {};
  promise.then(value => { state.done = { value }; }, error => { state.done = { error }; });
  return state;
};

// Keep reason: #1136 cancellation at the pre-publication trust boundary. Observe genuine
// fs watches/timers, not fake notifications or clocks; no extra polling drives these checks.
const observeLeaseWait = (directory: ParticipantDirectory, controller: AbortController, abortDuringArm = false) => {
  const root = directory.mesh.root;
  const realWatch = fs.watch;
  const realResolve = directory.resolveRoutingLease.bind(directory);
  // Existing control-plane watches share this root, and Windows can deliver their
  // mkdir notifications after the lease wait is armed. Track ownership, not the
  // path alone: async context also includes children rebound by this wait's watch.
  const ownership = new AsyncLocalStorage<boolean>();
  vi.spyOn(directory, "resolveRoutingLease").mockImplementation((...args) =>
    ownership.run(true, () => realResolve(...args)));
  const watchers: fs.FSWatcher[] = [];
  const foreignWatchers: fs.FSWatcher[] = [];
  const timers: Array<ReturnType<typeof setTimeout>> = [];
  let armed!: () => void;
  const ready = new Promise<void>(resolve => { armed = resolve; });
  vi.spyOn(fs, "watch").mockImplementation(((...args: unknown[]) => {
    const watcher = Reflect.apply(realWatch, fs, args) as fs.FSWatcher;
    if (String(args[0]) === root || String(args[0]).startsWith(root + path.sep)) {
      vi.spyOn(watcher, "close");
      if (ownership.getStore()) {
        watchers.push(watcher);
        if (abortDuringArm && !controller.signal.aborted) controller.abort("cancel during watch setup");
      } else foreignWatchers.push(watcher);
    }
    return watcher;
  }) as typeof fs.watch);
  vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: any[]) => void, delay?: number, ...args: any[]) => {
    const timer = realSetTimeout(callback, delay, ...args);
    if (delay === PEER_LEASE_WAIT_MS) { timers.push(timer); armed(); }
    return timer;
  }) as typeof setTimeout);
  const cleared = vi.spyOn(globalThis, "clearTimeout");
  const added = vi.spyOn(controller.signal, "addEventListener");
  const removed = vi.spyOn(controller.signal, "removeEventListener");
  const assertClean = () => {
    if (timers.length > 0) expect(watchers.length).toBeGreaterThan(0);
    for (const watcher of watchers) expect(watcher.close).toHaveBeenCalledOnce();
    for (const timer of timers) expect(cleared).toHaveBeenCalledWith(timer);
    for (const [event, listener] of added.mock.calls) {
      if (event === "abort") expect(removed).toHaveBeenCalledWith("abort", listener);
    }
  };
  return { ready, watchers, foreignWatchers, timers, assertClean };
};
const invocation = (signal: AbortSignal) => ({ signal } as FabricInvocationContext);

// Fault seam: mutate only the bridge-written lease/checkpoint, not the directory, wait,
// notifications or receiving path. A real bridge pass below supplies the fresh renewal.
const indeterminate = async (w: Awaited<ReturnType<typeof world>>, sample: "stale" | "absent" | "source-live expired") => {
  const lease = readHostLease(w.hub.root, w.target.hostId)!;
  const now = Date.now(), expiresAt = sample === "source-live expired" ? now - 1 : now + 60_000;
  const { mirror: old, ...rest } = lease;
  writeHostLease(w.hub.root, { ...rest, expiresAt, mirrorLease: 1,
    ...(sample === "absent" ? {} : { mirror: { ...old!,
      observedAt: sample === "stale" ? now - BRIDGE_LEASE_MS - 1 : now,
      // Even an old source-clock lapse is not death without a fresh bridge sample.
      ...(sample === "stale" ? { sourceExpiresAt: old!.sourceObservedAt - 1 } : {}) } }) });
  const key = `topology/hosts/${hash(w.target.hostId)}`, host = w.hub.get(key, { fresh: true })!;
  await w.hub.put({ key, identity: host.updatedBy, value: { ...(host.value as object), expiresAt, mirrorLease: 1 } });
  expect(readHostLease(w.hub.root, w.target.hostId)!.expiresAt).toBe(expiresAt);
  expect(w.hubDirectory.get(w.target.hostId, undefined, { fresh: true }))
    .toMatchObject({ remoteHost: "forge", stale: false, bridgeStale: true });
};

const replacementSample = (w: Awaited<ReturnType<typeof world>>, remoteHost: string) => {
  const lease = readHostLease(w.hub.root, w.target.hostId)!;
  writeHostLease(w.hub.root, { ...lease, expiresAt: Date.now() + 60_000,
    mirror: { ...lease.mirror!, remoteHost, observedAt: Date.now() } });
};

// F-001 enters the real provider API, retaining the real directory/bridge/control ports.
// Only process-local managers are inert: these operations address a foreign Main mirror.
const invokingProvider = (w: Awaited<ReturnType<typeof world>>, mainMatch = false) => {
  type Ports = ConstructorParameters<typeof AgentsProvider>;
  const main = mainMatch
    ? { id: w.target.identity.id, local: false, matches: (id: string) => id === w.target.identity.id }
    : w.sending.mainAgent;
  return new AgentsProvider(w.sending.manager as Ports[0], w.sending.actorManager as Ports[1], {} as Ports[2],
    main as Ports[3], w.hubDirectory, w.sender, {} as Ports[6], () => false, undefined, false);
};

// A5 premise: public API -> real FILE/bridge/receiver -> actual busy Main durable journal.
// Private two-store mirror simulation, constant Date offset; native Pi consumption is inert.
describe("A5 public-entry mirrored-sample N20", () => {
  it("admits all 20 once after same-owner sample renewal, with no caller replay", async () => {
    const w = await world(undefined, true), provider = invokingProvider(w);
    const rows: Array<Record<string, unknown>> = [];
    const samples = ["stale", "absent", "source-live expired"] as const;
    const epoch = w.target.ownerIncarnation;
    for (let i = 0; i < 20; i++) {
      const sample = samples[i % samples.length]!;
      await indeterminate(w, sample);
      if (i > 0) await w.hubDirectory.refreshRoutingView(); // First input uses bootstrap.
      const pin = w.hubDirectory.mirroredControlOwner(w.target.hostId, w.target.identity.id, w.target.hostId)!;
      expect(pin).toMatchObject({ remoteHost: "forge", ownerIncarnation: epoch });
      const message = `a5-input-${i}`, started = performance.now();
      const pending = provider.invoke("followUp", { id: w.target.hostId, message, data: { input: i },
        idempotencyKey: `a5-key-${i}` }, { signal: AbortSignal.timeout(5000), callId: `a5-call-${i}` } as unknown as FabricInvocationContext);
      let settledAt = started;
      void pending.then(() => { settledAt = performance.now(); }, () => { settledAt = performance.now(); });
      const state = settle(pending);
      // The bridge presence pass is the normal event source; delay only this renewal.
      await sleep(100);
      await w.bridge.syncPresence();
      await waitFor(() => on(w.hub, "fabric.control.command").length > i || state.done !== undefined);
      await w.bridge.step();
      if (!state.done) {
        await waitFor(() => on(w.far, "fabric.control.ack").length > i || state.done !== undefined);
        await w.bridge.step();
      }
      let refusal: string | null = null, acknowledged = false;
      try { acknowledged = (await pending as { acknowledged?: boolean }).acknowledged === true; }
      catch (error) { refusal = error instanceof Error ? `${error.name}: ${error.message}` : String(error); }
      const admitted = w.admissions.filter(item => item.message === message);
      rows.push({ input: i, sample, message, acknowledged, refusal, settledMs: settledAt - started,
        admissions: admitted.length, admissionMs: admitted[0] ? admitted[0].at - started : null });
      expect(w.hubDirectory.mirroredControlOwner(w.target.hostId, w.target.identity.id, w.target.hostId))
        .toMatchObject({ remoteHost: "forge", ownerIncarnation: epoch });
    }
    const items = fs.existsSync(w.journal) ? JSON.parse(fs.readFileSync(w.journal, "utf8")).items as Array<{ deliveryId: string; message: string; data: unknown }> : [];
    const times = rows.flatMap(row => typeof row.admissionMs === "number" ? [row.admissionMs] : []).sort((a, b) => a - b);
    const percentile = (p: number) => times.length ? times[Math.ceil(times.length * p) - 1] : null;
    const summary = { inputs: 20, commands: on(w.hub, "fabric.control.command").length,
      carried: on(w.far, "fabric.control.command").length, admissions: w.admissions.length, journalItems: items.length,
      duplicates: w.admissions.length - new Set(w.admissions.map(item => item.id)).size,
      refusals: rows.filter(row => row.refusal !== null).length, nativeSends: w.sendMessage.mock.calls.length,
      p50: percentile(.5), p95: percentile(.95), p99: percentile(.99), rows };
    console.info("A5_PUBLIC_N20", JSON.stringify(summary));
    expect(summary.refusals).toBe(0); expect(summary.commands).toBe(20); expect(summary.carried).toBe(20);
    expect(summary.admissions).toBe(20); expect(summary.duplicates).toBe(0); expect(summary.nativeSends).toBe(0);
    expect(w.admissions.every(item => item.verification === "bridge")).toBe(true);
    expect(items.map(item => [item.message, item.data])).toEqual(Array.from({ length: 20 }, (_, i) => [`a5-input-${i}`, { input: i }]));
    expect(items.map(item => item.deliveryId)).toEqual(w.admissions.map(item => item.id));
  }, 30000);
});

describe("F-001 provider invocation mirror wait", () => {
  it("observes only lease-wait cleanup when an existing control watch binds a child after renewal", async () => {
    const realWatch = fs.watch;
    const delayed: Array<{ root: string; notify: () => void }> = [];
    // Windows may deliver the initial host-leases mkdir notification only during
    // renewal. Hold that genuine watcher callback to reproduce the ordering on Linux.
    const pendingNotices = vi.spyOn(fs, "watch").mockImplementation(((...args: unknown[]) => {
      const callback = args.at(-1) as (event: string, name: string | Buffer | null) => void;
      const root = String(args[0]);
      const held = (event: string, name: string | Buffer | null) => {
        if (String(name) === "host-leases") delayed.push({ root, notify: () => callback(event, name) });
        else callback(event, name);
      };
      return Reflect.apply(realWatch, fs, [...args.slice(0, -1), held]) as fs.FSWatcher;
    }) as typeof fs.watch);
    const w = await world();
    await waitFor(() => delayed.some(notice => notice.root === w.hub.root));
    pendingNotices.mockRestore();
    const controller = new AbortController();
    const resources = observeLeaseWait(w.hubDirectory, controller);
    const pending = invokingProvider(w).invoke("status", { id: w.target.identity.id }, invocation(controller.signal));
    await resources.ready;
    for (const notice of delayed.filter(notice => notice.root === w.hub.root)) notice.notify();
    expect(resources.foreignWatchers.length).toBeGreaterThan(0);
    // Also require ownership to follow a child created AFTER arming, rather than
    // merely snapshotting the initial watchers and missing a real wait-owned leak.
    const initiallyOwned = resources.watchers.length;
    fs.mkdirSync(path.join(w.hub.root, "participants"));
    await waitFor(() => resources.watchers.length > initiallyOwned);
    await w.bridge.syncPresence();
    await expect(pending).resolves.toMatchObject({ id: w.target.identity.id, remoteHost: "forge" });
    resources.assertClean();
    // The lease wait must not close the sender's long-lived control watches.
    for (const watcher of resources.foreignWatchers) expect(watcher.close).not.toHaveBeenCalled();
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
  }, 20_000);

  it.each([
    ["status-main", "status", true, "bootstrap"],
    ["status-main", "status", true, "lookup"],
    ["status-generic", "status", false, "bootstrap"],
    ["status-generic", "status", false, "lookup"],
    ["actor-target-resident", "resetSession", false, "bootstrap"],
    ["actor-message-target", "ask", false, "bootstrap"],
    ["stop-wrapper", "stop", false, "bootstrap"],
  ] as const)("F-001 cancellation %s at %s mainMatch=%s entry=%s releases resources before publication", async (branch, action, mainMatch, entry) => {
    const w = await world();
    if (entry === "lookup") await w.hubDirectory.refreshRoutingView();
    const provider = invokingProvider(w, mainMatch);
    const controller = new AbortController();
    const resources = observeLeaseWait(w.hubDirectory, controller);
    const pending = provider.invoke(action, { id: w.target.identity.id, thinking: "high", message: "never publish" }, invocation(controller.signal));
    const state = settle(pending); // Attach rejection observation before abort, including on old source.
    await resources.ready;
    expect(resources.watchers.length).toBeGreaterThan(0);
    expect(resources.timers).toHaveLength(1);
    expect(state.done).toBeUndefined();
    const reason = { cancelled: branch, entry };
    controller.abort(reason);
    resources.assertClean(); // Synchronous abort cleanup: fail red without waiting the 15 s deadline.
    await expect(pending).rejects.toBe(reason);
    expect(state.done?.error).toBe(reason);
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 20_000);

  it.each([true, false])("F-001 non-cancelled status mainMatch=%s returns the same-owner renewed root", async mainMatch => {
    const w = await world();
    const controller = new AbortController();
    const resources = observeLeaseWait(w.hubDirectory, controller);
    const pending = invokingProvider(w, mainMatch).invoke("status", { id: w.target.identity.id }, invocation(controller.signal));
    const state = settle(pending);
    await resources.ready;
    expect(state.done).toBeUndefined();
    await w.bridge.syncPresence();
    await expect(pending).resolves.toMatchObject({ id: w.target.identity.id, kind: "root",
      ownerHostId: w.target.hostId, ownerIdentityId: w.target.identity.id, rootId: w.target.identity.id, remoteHost: "forge" });
    resources.assertClean();
    expect(controller.signal.aborted).toBe(false);
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 20_000);

  it("F-001 actor mismatch still refuses a Main target after same-owner renewal", async () => {
    const w = await world();
    const controller = new AbortController();
    const resources = observeLeaseWait(w.hubDirectory, controller);
    const pending = invokingProvider(w).invoke("resetSession", { id: w.target.identity.id }, invocation(controller.signal));
    const state = settle(pending);
    await resources.ready;
    expect(state.done).toBeUndefined();
    await w.bridge.syncPresence();
    await expect(pending).rejects.toThrow(`Fabric Main participant ${w.target.identity.id} is not an actor`);
    resources.assertClean();
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 20_000);
});

describe("remote mirror lease wait (smarty-dev#11703)", () => {
  it.each(["stale", "absent", "source-live expired"] as const)("%s modern sample waits once at known status lookup, cancels cleanly, then the same owner renews", async sample => {
    const w = await world();
    await indeterminate(w, sample);
    await w.hubDirectory.refreshRoutingView(); // Exercise known-positive lookup, not bootstrap.
    const controller = new AbortController();
    const resources = observeLeaseWait(w.hubDirectory, controller);
    const pending = invokingProvider(w).invoke("status", { id: w.target.hostId }, invocation(controller.signal));
    const state = settle(pending);
    await resources.ready;
    expect(state.done).toBeUndefined();
    expect(resources.timers).toHaveLength(1);
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    const reason = { sample, cancelled: true };
    controller.abort(reason);
    await expect(pending).rejects.toBe(reason);
    resources.assertClean();

    // A separate attempt renews by the actual two-store bridge, then delivers once.
    const renewed = w.sending.routeMessage(w.target.hostId, `renew ${sample}`, undefined, "followUp");
    const outcome = settle(renewed);
    await sleep(100);
    expect(outcome.done).toBeUndefined();
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    await w.bridge.syncPresence();
    await waitFor(() => on(w.hub, "fabric.control.command").length === 1 || outcome.done !== undefined);
    if (outcome.done?.error) throw outcome.done.error;
    await w.bridge.step();
    await waitFor(() => on(w.far, "fabric.control.ack").length === 1);
    await w.bridge.step();
    await expect(renewed).resolves.toMatchObject({ acknowledged: true, routed: "mesh" });
    expect(on(w.hub, "fabric.control.command")).toHaveLength(1);
    expect(on(w.far, "fabric.control.command")).toHaveLength(1);
    expect(w.sendMessage).toHaveBeenCalledOnce();
  }, 20_000);

  it.each(["stale", "absent", "source-live expired"] as const)("%s modern sample without renewal waits its configured bound, then fails peer-stale without publication", async sample => {
    const w = await world(50); // Real-clock bounded seam; production default is tested above.
    await indeterminate(w, sample);
    await w.hubDirectory.refreshRoutingView();
    const started = performance.now();
    const errors = await Promise.all([
      w.sending.routeMessage(w.target.hostId, "never publish", undefined, "followUp"),
      invokingProvider(w).invoke("status", { id: w.target.hostId }, invocation(new AbortController().signal)),
    ].map(p => p.catch((error: unknown) => error)));
    expect(performance.now() - started).toBeGreaterThanOrEqual(40);
    expect(performance.now() - started).toBeLessThan(2_000);
    for (const error of errors) {
      expect(error).toMatchObject({ code: "FABRIC_PEER_LEASE_STALE", retryable: true, remoteHost: "forge" });
      expect((error as Error).message).not.toContain("lapsed");
      expect((error as Error).message).toContain("this attempt was not published");
    }
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    await w.bridge.step();
    expect(on(w.far, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 20_000);

  it("a modern fresh source-lapsed sample refuses both provider status and messages immediately, never arming or publishing", async () => {
    const w = await world();
    const lease = readHostLease(w.hub.root, w.target.hostId)!;
    const now = Date.now();
    writeHostLease(w.hub.root, { ...lease, expiresAt: now + 60_000, mirror: { ...lease.mirror!,
      observedAt: now, sourceObservedAt: now - 60_000, sourceExpiresAt: now - 60_001 } });
    await w.hubDirectory.refreshRoutingView();
    const controller = new AbortController();
    const resources = observeLeaseWait(w.hubDirectory, controller);
    const started = performance.now();
    for (const kind of ["followUp", "steer"] as const) {
      await expect(w.sending.routeMessage(w.target.hostId, "fresh lapse", undefined, kind))
        .rejects.toThrow(`Unknown Fabric participant: ${w.target.hostId} (its lease mirrored from remote host forge lapsed`);
    }
    await expect(invokingProvider(w).invoke("status", { id: w.target.hostId }, invocation(controller.signal)))
      .rejects.toThrow("its lease mirrored from remote host forge lapsed");
    expect(performance.now() - started).toBeLessThan(2_000);
    expect(resources.timers).toEqual([]);
    expect(resources.watchers).toEqual([]);
    resources.assertClean();
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 20_000);
  it.each([
    ["followUp", "bootstrap"], ["steer", "lookup"], ["followUp", "late-read"],
  ] as const)("%s cancels the %s mirror wait immediately, releasing watches/timer without publication", async (kind, entry) => {
    const w = await world();
    if (entry !== "bootstrap") await w.hubDirectory.refreshRoutingView();
    if (entry === "late-read") vi.spyOn(w.hubDirectory, "get").mockImplementationOnce(() => { throw new Error("late routing read"); });
    const controller = new AbortController();
    const resources = observeLeaseWait(w.hubDirectory, controller);
    const pending = w.sending.routeMessage(w.target.identity.id, "cancel before publication", undefined, kind, invocation(controller.signal));
    const state = settle(pending);
    await resources.ready;
    expect(resources.watchers.length).toBeGreaterThan(0);
    expect(resources.timers).toHaveLength(1);
    expect(state.done).toBeUndefined();
    const reason = { cancelled: entry }; // Identity must survive every wrapper, not just Error.message.
    controller.abort(reason);
    await expect(pending).rejects.toBe(reason);
    resources.assertClean();
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 20_000);

  it.each(["preaborted", "watch-setup"] as const)("%s cancellation never leaves an armed watcher/timer or publishes", async entry => {
    const w = await world();
    const controller = new AbortController();
    if (entry === "preaborted") controller.abort("already cancelled");
    const resources = observeLeaseWait(w.hubDirectory, controller, entry === "watch-setup");
    const pending = w.sending.routeMessage(w.target.identity.id, "never arm after cancellation", undefined, "steer", invocation(controller.signal));
    const error = await pending.catch((reason: unknown) => reason);
    expect(error).toBe(controller.signal.reason);
    resources.assertClean();
    expect(resources.timers).toEqual([]);
    if (entry === "preaborted") expect(resources.watchers).toEqual([]);
    else expect(resources.watchers.length).toBeGreaterThan(0);
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 20_000);

  it.each([
    ["bootstrap", false], ["bootstrap", true],
    ["retained-lastKnown", false], ["retained-lastKnown", true],
    ["diagnostic-lastKnown", false], ["diagnostic-lastKnown", true],
  ] as const)("%s recovers through one canonical probe with resolver=%s, never waiting on an unreadable pin", async (entry, hasResolver) => {
    const w = await world();
    let failed = true, reads = 0;
    const pinFailure = new Error("lastKnown unreadable");
    const resolver = vi.fn(w.hubDirectory.resolveRoutingLease.bind(w.hubDirectory));
    const probe = vi.fn(async () => { await w.hubDirectory.refreshRoutingView(); failed = false; });
    const source = {
      get: (id: string, now?: number, options?: { fresh?: boolean }) => failed ? undefined : w.hubDirectory.get(id, now, options),
      lastKnown: () => {
        if (failed && (entry !== "diagnostic-lastKnown" || ++reads >= 3)) throw pinFailure;
        return undefined;
      },
      routingUnavailable: () => entry === "bootstrap" && failed ? "no confirmed view" : undefined,
      refreshRoutingView: probe, scheduleRefresh: () => undefined,
      ...(hasResolver ? { resolveRoutingLease: resolver } : {}),
    };
    const router = new AgentMessageRouter(w.sending.manager, w.sending.actorManager, w.sending.mainAgent,
      source, w.sender, binding => binding);
    const target = entry === "diagnostic-lastKnown" ? sid("absent") : w.local.identity.id;
    const pending = router.routeMessage(target, "recover canonical view", undefined, "followUp");
    if (entry === "diagnostic-lastKnown") {
      await expect(pending).rejects.toThrow(`Unknown Fabric participant: ${target}`);
      expect(on(w.hub, "fabric.control.command")).toEqual([]);
    } else {
      await expect(pending).resolves.toMatchObject({ acknowledged: true, messageId: "local-delivery" });
      expect(w.localHandler).toHaveBeenCalledOnce();
      expect(on(w.hub, "fabric.control.command")).toHaveLength(1);
    }
    expect(probe).toHaveBeenCalledOnce();
    expect(resolver).not.toHaveBeenCalled(); // Pin failed or no resolver: no selected wait authority.
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 20_000);

  it.each(["followUp", "steer"] as const)("%s waits once for the bridge renewal, then delivers exactly once through the real receiving handler", async kind => {
    const w = await world();
    const started = performance.now();
    const pending = w.sending.routeMessage(w.target.identity.id, `after renewal ${kind}`, undefined, kind, undefined, { idempotencyKey: `renew-${kind}` });
    const state = settle(pending);
    await sleep(300);
    // Waiting, not failing as Unknown, and nothing is published toward the stale mirror.
    if (state.done?.error) throw state.done.error;
    expect(state.done).toBeUndefined();
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    // The bridge renewal arrives (a real presence pass writes the hub's mirror lease/records).
    await w.bridge.syncPresence();
    expect(w.mirror()!.expiresAt).toBeGreaterThan(Date.now());
    await waitFor(() => on(w.hub, "fabric.control.command").length === 1 || state.done !== undefined);
    if (state.done?.error) throw state.done.error;
    expect(performance.now() - started).toBeLessThan(BRIDGE_LEASE_MS);
    await w.bridge.step();
    await waitFor(() => on(w.far, "fabric.control.ack").length === 1);
    await w.bridge.step();
    await expect(pending).resolves.toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
    // Exactly once: one command published, one carried, one handler delivery.
    expect(on(w.hub, "fabric.control.command")).toHaveLength(1);
    expect(on(w.far, "fabric.control.command")).toHaveLength(1);
    expect(on(w.far, "fabric.control.command")[0]!.data).toMatchObject({ targetId: w.target.identity.id, operation: kind });
    expect(w.sendMessage).toHaveBeenCalledOnce();
    expect(JSON.stringify(w.sendMessage.mock.calls[0]![0])).toContain(`after renewal ${kind}`);
    expect(w.sendMessage.mock.calls[0]![1]).toMatchObject({ deliverAs: kind });
    expect(w.localHandler).not.toHaveBeenCalled();
  }, 30_000);

  it("without renewal followUp and steer fail once, within 15 s, with typed retryable FabricPeerLeaseStaleError and never publish", async () => {
    const w = await world();
    const started = performance.now();
    const errors = await Promise.all((["followUp", "steer"] as const).map(kind =>
      w.sending.routeMessage(w.target.identity.id, `never ${kind}`, undefined, kind, undefined, { idempotencyKey: `stale-${kind}` })
        .then(() => { throw new Error(`${kind} unexpectedly delivered to a stale mirror`); }, (error: unknown) => error)));
    const elapsed = performance.now() - started;
    // One bounded wait (two sequential waits would take ~30 s); it did wait, not fail at once.
    expect(elapsed).toBeLessThanOrEqual(BRIDGE_LEASE_MS + 2_000);
    expect(elapsed).toBeGreaterThanOrEqual(1_000);
    for (const error of errors) {
      expect(error).toBeInstanceOf(FabricPeerLeaseStaleError);
      expect(error).toMatchObject({ name: "FabricPeerLeaseStaleError", code: "FABRIC_PEER_LEASE_STALE", retryable: true,
        targetId: w.target.identity.id, remoteHost: "forge" });
      const { lapsedMs, message } = error as FabricPeerLeaseStaleError & { lapsedMs: number; message: string };
      expect(Number.isFinite(lapsedMs)).toBe(true);
      expect(lapsedMs).toBeGreaterThan(0);
      expect(message).toContain(w.target.identity.id);
      expect(message).toContain("forge");
      expect(message).toMatch(/may .*live/i);
      expect(message).not.toMatch(/^Unknown /);
    }
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    // A later bridge pass has nothing queued to carry: the stale attempt was never published.
    await w.bridge.step();
    await sleep(100);
    expect(on(w.far, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 40_000);

  it("never replays a post-publication owner failure after a renewed wait, even one that says Unknown", async () => {
    const w = await world();
    const ownerFailure = new Error(`Unknown Fabric participant: ${w.target.identity.id} after remote admission`);
    const request = vi.spyOn(w.sender, "request").mockRejectedValueOnce(ownerFailure);
    const pending = w.sending.routeMessage(w.target.identity.id, "published once", undefined, "followUp", undefined, { idempotencyKey: "no-replay" });
    const state = settle(pending);
    await sleep(300);
    if (state.done?.error && state.done.error !== ownerFailure) throw state.done.error;
    await w.bridge.syncPresence();
    await waitFor(() => state.done !== undefined, 5_000);
    expect(state.done!.error).toBe(ownerFailure);
    // One request (the stand-in for an uncertain publication) and no second lease wait/replay.
    await sleep(300);
    expect(request).toHaveBeenCalledOnce();
    expect(request.mock.calls[0]!.slice(0, 3)).toEqual([w.target.hostId, w.target.identity.id, "followUp"]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 30_000);

  // R1: a live re-read of a DIFFERENT authority while waiting must never be accepted as the renewal.
  it.each(["remoteHost change", "owner host/identity replacement"] as const)("R1 refuses, never publishing, when a valid live %s replaces the expired mirror while waiting", async variant => {
    const w = await world();
    const pending = w.sending.routeMessage(w.target.identity.id, `no redirect ${variant}`, undefined, "followUp", undefined, { idempotencyKey: `r1-${variant}` });
    const state = settle(pending);
    await sleep(300);
    if (state.done?.error) throw state.done.error;
    expect(state.done).toBeUndefined();
    const now = Date.now();
    const pKey = `topology/participants/${hash(w.target.hostId)}`;
    const mirrored = w.hub.get(pKey, { fresh: true })!;
    const participant = mirrored.value as Record<string, unknown>;
    let owner: string, ownerIdentity: string, remoteHost: string;
    if (variant === "remoteHost change") {
      const hKey = `topology/hosts/${hash(w.target.hostId)}`, host = w.hub.get(hKey, { fresh: true })!;
      await w.hub.writeBatch({ identity: mirrored.updatedBy, ops: [
        { kind: "put", key: hKey, value: { ...(host.value as object), remoteHost: "ryzen2", updatedAt: now, expiresAt: now + 60_000 } },
        { kind: "put", key: pKey, value: { ...participant, remoteHost: "ryzen2", updatedAt: now } },
      ] });
      replacementSample(w, "ryzen2"); // A genuinely live sample must match the replacement link.
      owner = w.target.hostId; ownerIdentity = w.target.identity.id; remoteHost = "ryzen2";
    } else {
      const b = sid("replacement"), identity: MeshIdentity = { id: b, name: "main", kind: "main", sessionId: "replacement" };
      await w.hub.put({ key: `topology/hosts/${hash(b)}`, identity, value: { format: 1, id: b, rootId: w.target.hostId, identity,
        remoteHost: "forge", startedAt: now, updatedAt: now, expiresAt: now + 60_000 } });
      await w.hub.put({ key: pKey, identity, value: { ...participant, ownerHostId: b, ownerIdentityId: b,
        ownerIncarnation: `fixture:${b}`, updatedAt: now } });
      owner = b; ownerIdentity = b; remoteHost = "forge";
    }
    // The replacement is genuinely VALID and LIVE in a fresh directory read.
    const replacement = w.hubDirectory.mirroredControlOwner(owner, ownerIdentity, w.target.identity.id);
    expect(replacement).toMatchObject({ remoteHost });
    expect(replacement!.expiresAt).toBeGreaterThan(Date.now());
    await waitFor(() => state.done !== undefined, 5_000);
    expect(state.done!.error).toMatchObject({ code: "FABRIC_PEER_LEASE_STALE", retryable: true });
    await w.bridge.step();
    await sleep(100);
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    expect(on(w.far, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 30_000);

  // R1 post-decision: the original same-owner renewal resolves the wait, then a VALID LIVE replacement is installed
  // before the router retries. The retry must stay pinned to the pre-wait root authority (never its own recache).
  // Decision C seams: vi.spyOn(directory.resolveRoutingLease) wraps the real wait; vi.spyOn(directory.get) makes the
  // non-fresh cached view re-cache the replacement (fresh reads stay real). Stores, bridge and control are real.
  it.each(["remoteHost change", "owner host/identity replacement"] as const)("R1 refuses a %s installed after the same-owner renewal decision, never publishing", async variant => {
    const w = await world();
    const realResolve = w.hubDirectory.resolveRoutingLease.bind(w.hubDirectory);
    const realGet = w.hubDirectory.get.bind(w.hubDirectory);
    let replaced = false;
    // Let the real renewal pass finish before installing B, so it cannot overwrite the replacement.
    let finishPresence!: () => void;
    const presenceFinished = new Promise<void>(resolve => { finishPresence = resolve; });
    vi.spyOn(w.hubDirectory, "resolveRoutingLease").mockImplementation(async (id: string) => {
      const renewed = await realResolve(id);
      expect(renewed).toBe(true);
      await presenceFinished;
      const now = Date.now();
      const pKey = `topology/participants/${hash(w.target.hostId)}`;
      const mirrored = w.hub.get(pKey, { fresh: true })!;
      const participant = mirrored.value as Record<string, unknown>;
      if (variant === "remoteHost change") {
        const hKey = `topology/hosts/${hash(w.target.hostId)}`, host = w.hub.get(hKey, { fresh: true })!;
        await w.hub.put({ key: hKey, identity: host.updatedBy, value: { ...(host.value as object), remoteHost: "ryzen2", updatedAt: now, expiresAt: now + 60_000 } });
        await w.hub.put({ key: pKey, identity: mirrored.updatedBy, value: { ...participant, remoteHost: "ryzen2", updatedAt: now } });
        replacementSample(w, "ryzen2");
        expect(w.hubDirectory.mirroredControlOwner(w.target.hostId, w.target.identity.id, w.target.identity.id)).toMatchObject({ remoteHost: "ryzen2" });
      } else {
        const b = sid("replacement"), identity: MeshIdentity = { id: b, name: "main", kind: "main", sessionId: "replacement" };
        await w.hub.put({ key: `topology/hosts/${hash(b)}`, identity, value: { format: 1, id: b, rootId: w.target.hostId, identity,
          remoteHost: "forge", startedAt: now, updatedAt: now, expiresAt: now + 60_000 } });
        await w.hub.put({ key: pKey, identity, value: { ...participant, ownerHostId: b, ownerIdentityId: b,
          ownerIncarnation: `fixture:${b}`, updatedAt: now } });
        expect(w.hubDirectory.mirroredControlOwner(b, b, w.target.identity.id)).toMatchObject({ remoteHost: "forge" });
      }
      replaced = true;
      return true;
    });
    // Cache-positive seam: after the decision the cached view re-caches the replacement (as a fresh read would).
    vi.spyOn(w.hubDirectory, "get").mockImplementation(((id: string, kind?: unknown, options?: { fresh?: boolean }) =>
      replaced && !options?.fresh ? realGet(id, kind as never, { fresh: true }) : realGet(id, kind as never, options as never)) as typeof w.hubDirectory.get);
    const pending = w.sending.routeMessage(w.target.identity.id, `no post-decision redirect ${variant}`, undefined, "followUp", undefined, { idempotencyKey: `r1-post-${variant}` });
    const state = settle(pending);
    await sleep(300);
    if (state.done?.error) throw state.done.error;
    expect(state.done).toBeUndefined();
    await w.bridge.syncPresence();
    finishPresence();
    await waitFor(() => state.done !== undefined, 5_000);
    expect(replaced).toBe(true);
    expect(state.done!.error).toMatchObject({ code: "FABRIC_ROUTE_AUTHORITY_CHANGED" });
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
    expect(w.sendMessage).not.toHaveBeenCalled();
  }, 30_000);

  it("a truly absent record stays Unknown without waiting", async () => {
    const w = await world();
    const started = performance.now();
    const error = await w.sending.routeMessage(sid("absent"), "nobody", undefined, "followUp").catch((e: unknown) => e);
    expect(performance.now() - started).toBeLessThan(2_000);
    expect(error).not.toBeInstanceOf(FabricPeerLeaseStaleError);
    expect((error as Error).message).toMatch(new RegExp(`^Unknown Fabric participant: ${sid("absent")}`));
    expect(on(w.hub, "fabric.control.command")).toEqual([]);
  }, 20_000);

  it("a native local participant is unchanged: delivered once with no lease wait, beside a lapsed mirror", async () => {
    const w = await world();
    const started = performance.now();
    await expect(w.sending.routeMessage(w.local.identity.id, "local hello", undefined, "followUp", undefined, { idempotencyKey: "local-once" }))
      .resolves.toMatchObject({ acknowledged: true, messageId: "local-delivery" });
    expect(performance.now() - started).toBeLessThan(2_000);
    expect(w.localHandler).toHaveBeenCalledOnce();
    expect(on(w.hub, "fabric.control.command")).toHaveLength(1);
    expect(on(w.hub, "fabric.control.command")[0]!.data).toMatchObject({ targetId: w.local.identity.id });
    expect(w.sendMessage).not.toHaveBeenCalled();
    // The remote mirror remains lapsed; routing the local peer did not need or fake its renewal.
    expect(w.mirror()!.expiresAt).toBeLessThan(Date.now());
  }, 20_000);
});
