import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { LIVENESS_POLICY_KEY, readHostLeaseCurrent, removeHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { liveControlOwnerIncarnation } from "./helpers/live-control-owner.js";

// Keep: idle optimization must not cache routing authority across the lease/owner trust boundary.
// The fixture follows routing-refresh-removal and mesh-bridge-participants: real directory writers,
// two private meshes, bridge stdio RPC, control admission and Main's durable native journal.
// No fake resolver/lease answer, fake Date/timers, live host, model turn or shared scratch path.
const roots: string[] = [];
const cleanups: Array<() => unknown> = [];
afterEach(async () => {
  try { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); }
  finally {
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = (id: string) => key("topology/hosts/", id);
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const unknownManager = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0];
const actors = (identity: MeshIdentity) => ({ identity, status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
  validateDirectMessage: () => undefined } as unknown as Ports[1]);

const world = async (backend: "file" | "sqlite", idleMain = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-idle-send-")); roots.push(root);
  const store = (name: string) => {
    const mesh = new MeshStore(path.join(root, name), 65_536, 1_000, { stateBackend: backend, readCacheMs: 2_000 });
    cleanups.push(() => mesh.closeState()); return mesh;
  };
  const hub = store("hub"), far = store("far");
  const native = async (mesh: MeshStore, name: string) => {
    const identity: MeshIdentity = { id: `session:${name}-00000000`, name, kind: "main", sessionId: name };
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false,
      routingLease: { graceMs: 0, waitMs: 0, pollMs: 1 } });
    cleanups.push(() => directory.close());
    const record: FabricParticipantRecord = { format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name, sessionId: name,
      status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
      controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now(), livenessLeaseFiles: 1 };
    directory.registerSource(() => [record]);
    await mesh.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, participants: "files" } });
    await directory.start();
    return { identity, directory };
  };
  const source = await native(hub, "sender"), target = await native(far, "target"), local = await native(far, "native-sender");
  const input = new PassThrough(), output = new PassThrough();
  const serving = serveBridgeAgent(new StoreBridgeSide(far, "hub"), input, output);
  const remote = new RemoteBridgeSide(output, input);
  cleanups.push(async () => { remote.close(); input.end(); await serving; output.end(); });
  const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local: new StoreBridgeSide(hub, "far"), remote,
    cursorPath: path.join(root, "cursor.json"), presenceMs: 60_000, log: () => undefined });
  cleanups.push(() => bridge.stop());
  await bridge.start(); await bridge.syncPresence();
  // Warm both structural decisions repeatedly, with all raw candidates advertising modern leases.
  // Subsequent lease-only mutations below leave the shared-state token unchanged.
  const warm = async () => {
    for (const { directory } of [source, local]) { await directory.refresh(); await directory.refresh(); }
  };
  await warm();
  const plane = (mesh: MeshStore, owner: typeof source) => {
    const p = new FabricControlPlane(mesh, owner.identity, { enabled: true, hostId: owner.identity.id,
      ownerIncarnation: owner.directory.ownerIncarnation, pollMs: 20, acknowledgementTimeoutMs: 2_000, leaseGraceMs: 0,
      canConsumeMesh: () => owner.directory.canConsumeMesh(),
      captureOwnerLease: (...args) => owner.directory.captureControlOwnerLease(...args),
      readMirroredOwner: (...args) => owner.directory.mirroredControlOwner(...args) });
    cleanups.push(() => p.close()); return p;
  };
  const sender = plane(hub, source), nativeSender = plane(far, local), receiver = plane(far, target);
  const piSend = vi.fn();
  const main = new MainAgentController({ on: () => () => {}, sendMessage: piSend } as unknown as ExtensionAPI,
    target.identity.id, true, root, "target");
  const journal = path.join(root, "followups.json");
  main.attachFollowUpDrain({ isIdle: () => idleMain, hasPendingMessages: () => false,
    sessionManager: { getEntries: () => [] } } as unknown as ExtensionContext, 60_000, journal);
  cleanups.push(() => main.closeFollowUpDrain());
  const receiving = new AgentMessageRouter(unknownManager, actors(target.identity), main, target.directory, receiver, b => b);
  const accept = vi.fn((...args: Parameters<AgentMessageRouter["acceptControl"]>) => receiving.acceptControl(...args));
  receiver.start(accept); sender.start(() => ({ accepted: false })); nativeSender.start(() => ({ accepted: false }));
  const router = (owner: typeof source, control: FabricControlPlane) => new AgentMessageRouter(unknownManager, actors(owner.identity),
    { id: owner.identity.id, local: true, matches: (id: string) => id === owner.identity.id } as unknown as Ports[2],
    owner.directory, control, b => b);
  const sending = router(source, sender), nativeSending = router(local, nativeSender);
  const commands = (mesh: MeshStore) => mesh.read({ topic: "fabric.control.command", limit: 100 }).filter(e => e.kind !== "cancel");
  const items = () => fs.existsSync(journal) ? (JSON.parse(fs.readFileSync(journal, "utf8")) as { items: Array<Record<string, unknown>> }).items : [];
  const mirror = () => source.directory.mirroredControlOwner(target.identity.id, target.identity.id, target.identity.id);
  expect(mirror()).toMatchObject({ remoteHost: "far", ownerIncarnation: target.directory.ownerIncarnation });
  expect(await liveControlOwnerIncarnation(source.directory, target.identity.id)).toBe(receiver.incarnation);
  const remoteSend = async (message: string, idempotencyKey = message) => {
    const count = commands(hub).length;
    const ackCount = far.read({ topic: "fabric.control.ack", limit: 100 }).length;
    const pending = sending.routeMessage(target.identity.id, message, { proof: "unchanged" }, "followUp", undefined, { idempotencyKey });
    void pending.catch(() => undefined);
    await vi.waitFor(() => expect(commands(hub)).toHaveLength(count + 1));
    await bridge.step();
    await vi.waitFor(() => expect(far.read({ topic: "fabric.control.ack", limit: 100 })).toHaveLength(ackCount + 1));
    await bridge.step();
    return await pending;
  };
  return { root, hub, far, source, target, local, warm, bridge, sender, nativeSender, receiver,
    sending, nativeSending, commands, items, mirror, remoteSend, accept, piSend };
};

const admittedOnce = (f: Awaited<ReturnType<typeof world>>, message: string) => {
  expect(f.accept).toHaveBeenCalledOnce();
  expect(f.items()).toEqual([expect.objectContaining({ message, data: { proof: "unchanged" },
    provenance: expect.objectContaining({ sender: expect.objectContaining({ id: f.source.identity.id, verified: "bridge" }) }) })]);
  expect(f.piSend).not.toHaveBeenCalled(); // durable busy-Main admission, not a claimed Pi/model turn
  expect(f.commands(f.far)).toHaveLength(1);
  expect(f.commands(f.far)[0]!.verification).toBe("bridge");
};

describe.each(["file", "sqlite"] as const)("idle send lease freshness (%s)", backend => {
  it("re-reads lease-only mirror renewal/shortening, then the real remote flow durably admits exactly once", async () => {
    const f = await world(backend), id = f.target.identity.id;
    const original = readHostLeaseCurrent(f.hub.root, id)!;
    const token = f.hub.stateToken({ fresh: true });
    const renewed = Date.now() + 12_000, shortened = Date.now() + 8_000;
    writeHostLease(f.hub.root, { ...original, expiresAt: Date.now() - 1 });
    expect(f.mirror()!.expiresAt).toBeLessThan(Date.now());
    writeHostLease(f.hub.root, { ...original, expiresAt: renewed });
    expect(f.mirror()!.expiresAt).toBe(renewed);
    writeHostLease(f.hub.root, { ...original, expiresAt: shortened });
    expect(f.mirror()!.expiresAt).toBe(shortened);
    expect(f.hub.stateToken({ fresh: true })).toBe(token);
    // Route BEFORE another bridge pass: the existing bridge-created source sample is fresh,
    // but only this owner's real lease file has changed since the structural cache was warmed.
    await expect(f.remoteSend("after idle renewal")).resolves.toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
    admittedOnce(f, "after idle renewal");
    expect(f.commands(f.hub)).toHaveLength(1); // no caller replay or hidden second publication
  });

  it.each(["shortened", "removed", "wrong identity", "new incarnation"] as const)(
    "a %s mirror lease cannot be hidden by warmed structural caches or authorize publication", async change => {
      const f = await world(backend), id = f.target.identity.id;
      const original = readHostLeaseCurrent(f.hub.root, id)!;
      const token = f.hub.stateToken({ fresh: true });
      if (change === "removed") removeHostLease(f.hub.root, id);
      else writeHostLease(f.hub.root, { ...original,
        ...(change === "shortened" ? { expiresAt: Date.now() - 1 } : {}),
        ...(change === "wrong identity" ? { identityId: "session:not-the-owner" } : {}),
        ...(change === "new incarnation" ? { incarnationToken: "replacement-host-token" } : {}) });
      expect(f.hub.stateToken({ fresh: true })).toBe(token);
      const observed = f.mirror()!;
      expect(observed.expiresAt).toBeLessThan(Date.now());
      expect(observed.ownerIncarnation).toBe(f.receiver.incarnation); // never adopt lease bytes as ownership
      await expect(f.sending.routeMessage(id, "must not publish", undefined, "followUp"))
        .rejects.toMatchObject({ code: "FABRIC_PEER_LEASE_STALE", retryable: true });
      expect(f.commands(f.hub)).toEqual([]); expect(f.items()).toEqual([]); expect(f.accept).not.toHaveBeenCalled();
    });

  it("a fresh route naming a replacement activation is fenced by the native receiver, with no Main side effect", async () => {
    const f = await world(backend), id = f.target.identity.id;
    const participantKey = key("topology/participants/", id);
    const entry = f.hub.get(participantKey, { fresh: true })!;
    await f.hub.put({ key: participantKey, identity: entry.updatedBy,
      value: { ...(entry.value as object), ownerIncarnation: "replacement-activation", updatedAt: Date.now() + 1 } });
    expect(f.source.directory.get(id, undefined, { fresh: true })!.ownerIncarnation).toBe("replacement-activation");
    await expect(f.remoteSend("wrong activation")).rejects.toMatchObject({ code: "FABRIC_CONTROL_STALE_INCARNATION" });
    expect(f.commands(f.hub)).toHaveLength(1);
    expect(f.commands(f.hub)[0]!.data).toMatchObject({ ownerIncarnation: "replacement-activation" });
    expect(f.accept).not.toHaveBeenCalled(); expect(f.items()).toEqual([]); expect(f.piSend).not.toHaveBeenCalled();
  });

  it("captured native owner reads observe renewal, shortening, removal and wrong incarnation; fresh sends fail closed", async () => {
    const f = await world(backend), id = f.target.identity.id;
    const entry = f.far.get(hostKey(id), { fresh: true })!;
    const floor = Date.now() - 120_000;
    // Model the normal retained shared checkpoint after file-only renewal has outlived it.
    await f.far.put({ key: hostKey(id), identity: entry.updatedBy, value: { ...(entry.value as object), expiresAt: floor } });
    await f.warm();
    const read = f.local.directory.captureControlOwnerLease(id, id, id)!;
    expect(read).toBeTypeOf("function");
    expect(f.local.directory.captureControlOwnerLease(id, "session:wrong-owner", id)).toBeUndefined();
    const original = readHostLeaseCurrent(f.far.root, id)!;
    const token = f.far.stateToken({ fresh: true });
    const renewed = Date.now() + 240_000, shortened = Date.now() + 180_000;
    writeHostLease(f.far.root, { ...original, expiresAt: renewed }); expect(read()).toBe(renewed);
    writeHostLease(f.far.root, { ...original, expiresAt: shortened }); expect(read()).toBe(shortened);
    expect(f.far.stateToken({ fresh: true })).toBe(token);
    await expect(f.nativeSending.routeMessage(id, "native admission", { proof: "native" }, "followUp"))
      .resolves.toMatchObject({ queued: true, acknowledged: true });
    expect(f.accept).toHaveBeenCalledOnce(); expect(f.items()).toHaveLength(1);
    const admittedToken = f.far.stateToken({ fresh: true });
    for (const change of ["removed", "wrong identity", "new incarnation"] as const) {
      if (change === "removed") removeHostLease(f.far.root, id);
      else writeHostLease(f.far.root, { ...original, expiresAt: renewed,
        ...(change === "wrong identity" ? { identityId: "session:wrong-owner" } : { incarnationToken: "replacement-host-token" }) });
      expect(read()).toBe(floor);
      await expect(f.nativeSending.routeMessage(id, `refuse ${change}`, undefined, "followUp")).rejects.toThrow();
      expect(f.commands(f.far)).toHaveLength(1); expect(f.accept).toHaveBeenCalledOnce(); expect(f.items()).toHaveLength(1);
      expect(f.far.stateToken({ fresh: true })).toBe(admittedToken);
    }
    expect(f.piSend).not.toHaveBeenCalled();
  });

  it("allowed counterexample: losing a native lease file does not erase an unexpired shared checkpoint", async () => {
    const f = await world(backend, true), id = f.target.identity.id;
    const host = f.far.get(hostKey(id), { fresh: true })!.value as { expiresAt: number };
    const read = f.local.directory.captureControlOwnerLease(id, id, id)!;
    removeHostLease(f.far.root, id);
    expect(read()).toBe(host.expiresAt); expect(read()).toBeGreaterThan(Date.now());
    await expect(f.nativeSending.routeMessage(id, "checkpoint remains live", undefined, "followUp"))
      .resolves.toMatchObject({ queued: true, acknowledged: true });
    expect(f.accept).toHaveBeenCalledOnce(); expect(f.commands(f.far)).toHaveLength(1);
    expect(f.piSend).toHaveBeenCalledOnce();
    expect(f.piSend.mock.calls[0]![1]).toEqual({ deliverAs: "followUp", triggerTurn: true });
  });
});
