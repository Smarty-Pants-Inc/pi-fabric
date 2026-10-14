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
import { FabricPeerLeaseStaleError } from "../src/agents/peer-lease-stale.js";

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
const world = async () => {
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
    const d = new ParticipantDirectory(store, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
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
  const ctx = { isIdle: () => true, sessionManager: { getEntries: () => [] } } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, target.identity.id, true, os.tmpdir(), "remote");
  main.attachFollowUpDrain(ctx, 120_000, path.join(scratch(), "followups.json"));
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
  receiver.start((cmd, from, signal, verification) => receiving.acceptControl(cmd, from, signal, verification));
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
  return { hub, far, bridge, lane, local, target, hubDirectory, sending, sender, sendMessage, localHandler, mirror };
};

const settle = <T>(promise: Promise<T>) => {
  const state: { done?: { value?: T; error?: unknown } } = {};
  promise.then(value => { state.done = { value }; }, error => { state.done = { error }; });
  return state;
};

describe("remote mirror lease wait (smarty-dev#11703)", () => {
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
