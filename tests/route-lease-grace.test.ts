import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane, type FabricControlCommand, type FabricControlPlaneOptions } from "../src/topology/control-plane.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricParticipantStaleError, participantLeaseGraceMs, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
const planes: FabricControlPlane[] = [];
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-grace-")); roots.push(root); return root; };
afterEach(async () => {
  await Promise.all(planes.splice(0).map(p => p.close()));
  await Promise.all(directories.splice(0).map(d => d.close()));
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = async (late = 2000, kind: "root" | "actor" = "root") => {
  const root = temp(), mesh = new MeshStore(root, 65536, 1000, { readCacheMs: 60_000 });
  let now = Date.now(), renew = false, polls = 0;
  const target = kind === "actor" ? "actor:target" : "session:target", who = identity(target), reader = identity("session:reader");
  const presence: FabricParticipantRecord = { format: 1, id: target, kind, rootId: target,
    ownerHostId: target, ownerIdentityId: target, name: "lead", status: "running", runner: "pi", transport: "host",
    capabilities: ["followUp", "steer"], controlProtocol: "v1", startedAt: 1, updatedAt: now - 20000 };
  const host = { format: 1, id: target, rootId: target, identity: who, startedAt: 1, updatedAt: now - late - 15000, expiresAt: now - late };
  await mesh.writeBatch({ identity: who, ops: [
    { kind: "put", key: "topology/liveness", value: { version: 1, hostLeases: "files" } },
    { kind: "put", key: key("topology/hosts/", target), value: host },
    { kind: "put", key: key("topology/participants/", target), value: presence },
  ] });
  const lease = { id: target, rootId: target, identityId: target, startedAt: 1, updatedAt: host.updatedAt, expiresAt: host.expiresAt };
  writeHostLease(root, lease);
  const sleep = vi.fn(async (ms: number) => {
    now += ms; polls++;
    if (renew && polls === 3) writeHostLease(root, { ...lease, updatedAt: now, expiresAt: now + 60000 });
  });
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: reader.id, rootId: reader.id, identity: reader,
    reapDeadHosts: false, heartbeatMs: 60000, leaseMs: 120000,
    routingLease: { now: () => now, sleep, lockWaiting: () => true } });
  directories.push(directory); await directory.refresh();
  return { root, mesh, directory, target, presence, host, lease, sleep, now: () => now, renew: () => { renew = true; } };
};

type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const router = (directory: ParticipantDirectory, request = vi.fn(async () => ({ queued: true, messageId: "only-once", routed: "mesh", acknowledged: true }))) => {
  const actors = { identity: identity("session:reader"), status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
    validateDirectMessage: () => undefined } as unknown as Ports[1];
  const manager = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0];
  const main = { id: "session:reader", local: true, matches: (id: string) => id === "session:reader" } as unknown as Ports[2];
  return { value: new AgentMessageRouter(manager, actors, main, directory, { request } as unknown as Ports[4], b => b), request };
};

describe("native route lease grace (#4383)", () => {
  it.each(["root", "actor"] as const)("resolves a %s lease late by 2s after file renewal, with no state reads during the wait", async kind => {
    const f = await fixture(2000, kind); f.renew();
    const reads = vi.spyOn(fs, "readFileSync");
    const original = f.sleep.getMockImplementation()!;
    f.sleep.mockImplementation(async ms => {
      expect(reads.mock.calls.filter(([file]) => String(file) === path.join(f.root, "state.json"))).toHaveLength(0);
      await original(ms);
    });
    expect(await f.directory.resolveRoutingLease(f.target)).toBe(true);
    expect(f.sleep).toHaveBeenCalledTimes(3);
    expect(f.directory.get(f.target, f.now())?.stale).toBe(false);
  });

  it.each(["followUp", "steer", "status", "actor"] as const)("returns retryable STALE, not Unknown, for a recent unrenewed %s target", async action => {
    const f = await fixture(2000, action === "actor" ? "actor" : "root");
    const send = router(f.directory);
    const result = action === "status" ? send.value.resolveParticipantFresh(f.target) : action === "actor"
      ? send.value.resolveActorTargetFresh(f.target) : send.value.routeMessage(f.target, "unchanged", undefined, action);
    const error = await result.catch(e => e);
    expect(error).toMatchObject({ name: "FabricParticipantStaleError", code: "FABRIC_PARTICIPANT_STALE", retryable: true });
    expect(error.message).toContain("lease late by 12 s; retry");
    expect(error.message).not.toContain("Unknown");
    expect(f.sleep).toHaveBeenCalledTimes(100);
    expect(send.request).not.toHaveBeenCalled();
  });

  it("routes the exact renewed root once, with the original idempotency key", async () => {
    const f = await fixture(); f.renew(); const send = router(f.directory);
    await expect(send.value.routeMessage(f.target, "unchanged", undefined, "followUp", undefined, { idempotencyKey: "retry-once" }))
      .resolves.toMatchObject({ messageId: "only-once" });
    expect(send.request).toHaveBeenCalledOnce();
    expect(send.request.mock.calls[0]).toEqual([f.target, f.target, "followUp", expect.objectContaining({ message: "unchanged" }), f.target,
      expect.objectContaining({ idempotencyKey: "retry-once", routedRemoteHost: null })]);
  });

  it("never replays a post-publication owner failure that happens to say Unknown", async () => {
    const f = await fixture(); f.renew(); const send = router(f.directory);
    const ownerFailure = new Error(`Unknown Fabric participant: ${f.target} after remote admission`);
    send.request.mockRejectedValue(ownerFailure);
    await expect(send.value.routeMessage(f.target, "unchanged", undefined, "followUp")).rejects.toBe(ownerFailure);
    expect(send.request).toHaveBeenCalledOnce();
    expect(f.sleep).toHaveBeenCalledTimes(3);
  });

  it("keeps a 120s-lapsed exact target Unknown without polling or redirecting", async () => {
    const f = await fixture(120000), send = router(f.directory);
    await expect(send.value.routeMessage(f.target, "never redirect", undefined, "followUp")).rejects.toThrow(`Unknown Fabric participant: ${f.target}`);
    expect(f.sleep).not.toHaveBeenCalled(); expect(send.request).not.toHaveBeenCalled();
  });

  it("a terminal receipt vetoes renewal even if retained presence and a fresh file remain", async () => {
    const f = await fixture(); f.renew();
    writeHostLease(f.root, { ...f.lease, expiresAt: f.now() + 60000 });
    await f.mesh.put({ key: key("topology/lineage-closures/", f.target), identity: identity(f.target), value:
      { format: 1, rootId: f.target, ownerHostId: f.target, ownerIdentityId: f.target, closedAt: f.now() } });
    expect(await f.directory.resolveRoutingLease(f.target)).toBe(false);
    expect(f.sleep).not.toHaveBeenCalled();
  });

  it("does not revive a replaced owner's incarnation", async () => {
    const f = await fixture();
    const original = f.sleep.getMockImplementation()!;
    f.sleep.mockImplementation(async ms => {
      await original(ms);
      writeHostLease(f.root, { ...f.lease, startedAt: 2, updatedAt: f.now(), expiresAt: f.now() + 60000 });
    });
    await expect(f.directory.resolveRoutingLease(f.target)).rejects.toBeInstanceOf(FabricParticipantStaleError);
  });

  it("recovers when the matching owner's file advanced beyond stored state, without a lock signal", async () => {
    const f = await fixture(); f.renew();
    f.directory.options.routingLease!.lockWaiting = () => false;
    vi.spyOn(f.directory, "writeStalled").mockReturnValue(undefined);
    expect(await f.directory.resolveRoutingLease(f.target)).toBe(true);
    expect(f.sleep).toHaveBeenCalledTimes(3);
  });

  it("rejects a beyond-grace owner before publishing a new command", async () => {
    const root = temp();
    const sender = control(root, "sender", { captureOwnerLease: () => () => Date.now() - 120000 });
    await expect(sender.request("owner", "session:target", "followUp"))
      .rejects.toMatchObject({ code: "FABRIC_PARTICIPANT_STALE", retryable: true, idempotencyKey: expect.any(String) });
    expect(sender.mesh.read({ topic: "fabric.control.command" })).toHaveLength(0);
  });

  it("accepts a configurable grace without changing lease TTLs", () => {
    expect(participantLeaseGraceMs(45000)).toBe(45000);
    expect(participantLeaseGraceMs(1000)).toBe(1000);
    expect(participantLeaseGraceMs(Number.NaN)).toBe(45000);
    expect(participantLeaseGraceMs(-1)).toBe(45000);
  });
});

const control = (root: string, id: string, options: Partial<FabricControlPlaneOptions> = {}) => {
  const p = new FabricControlPlane(new MeshStore(root, 65536, 1000), identity(id), { enabled: true, hostId: id, pollMs: 20, ...options });
  planes.push(p); return p;
};
const command = (sender: FabricControlPlane) => sender.mesh.read({ topic: "fabric.control.command", limit: 20 })
  .filter(e => e.kind !== "cancel").at(-1)!.data as FabricControlCommand;
const drainMicrotasks = async () => { for (let i = 0; i < 30; i++) await Promise.resolve(); };

describe("live-owner ACK window and same-key retry (#4383)", () => {
  it("outlasts the old 15s ACK bound when the owner file stays fresh", async () => {
    vi.useFakeTimers(); const root = temp(); let expiresAt = Date.now() + 120000;
    const sender = control(root, "sender", { now: () => Date.now(), captureOwnerLease: () => () => expiresAt });
    sender.start(() => ({ accepted: false }));
    let settled = false;
    const outcome = sender.request("owner", "session:target", "followUp", { message: "later" }).finally(() => { settled = true; });
    await drainMicrotasks();
    const sent = command(sender);
    expect(sent.deadlineAt! - sent.requestedAt).toBe(60000);
    await vi.advanceTimersByTimeAsync(20000);
    expect(settled).toBe(false);
    expiresAt = Date.now() + 120000;
    await sender.mesh.publish({ topic: "fabric.control.ack", kind: "accepted", from: identity("owner"), to: "sender",
      data: { version: 1, commandId: sent.commandId, targetId: sent.targetId, accepted: true, messageId: "late-ack" } });
    await vi.advanceTimersByTimeAsync(20);
    await expect(outcome).resolves.toMatchObject({ messageId: "late-ack" });
  });

  it("fails fast with STALE once owner freshness exceeds grace, without cancelling/replaying", async () => {
    vi.useFakeTimers(); const root = temp(); let expiresAt = Date.now() + 60000;
    const sender = control(root, "sender", { captureOwnerLease: () => () => expiresAt });
    sender.start(() => ({ accepted: false }));
    const outcome = sender.request("owner", "session:target", "steer", {}, "owner", { idempotencyKey: "stable-key" }).catch(e => e);
    await drainMicrotasks(); expiresAt = Date.now() - 45001;
    await vi.advanceTimersByTimeAsync(20);
    expect(await outcome).toMatchObject({ code: "FABRIC_PARTICIPANT_STALE", retryable: true, idempotencyKey: "stable-key" });
    expect(sender.mesh.read({ topic: "fabric.control.command" })).toHaveLength(1);
  });

  it.each([true, false])("a stale retry reuses the persisted claim and never delivers twice (explicit key=%s)", async explicit => {
    vi.useFakeTimers(); const root = temp(); let expiresAt = Date.now() + 120000;
    const owner = control(root, "owner"), sender = control(root, "sender", { captureOwnerLease: () => () => expiresAt });
    const deliver = vi.fn(() => ({ accepted: true, messageId: "one-delivery" })); owner.start(deliver); sender.start(() => ({ accepted: false }));
    const publish = owner.mesh.publish.bind(owner.mesh);
    let drop = true;
    vi.spyOn(owner.mesh, "publish").mockImplementation(async input => {
      if (drop && input.topic === "fabric.control.ack") return { sequence: 0 } as Awaited<ReturnType<typeof owner.mesh.publish>>;
      return publish(input);
    });
    const first = sender.request("owner", "session:target", "followUp", { message: "unchanged" }, "owner", explicit ? { idempotencyKey: "same-message" } : {}).catch(e => e);
    await drainMicrotasks(); await vi.advanceTimersByTimeAsync(100);
    expect(deliver).toHaveBeenCalledOnce(); const firstId = command(sender).commandId;
    expiresAt = Date.now() - 45001; await vi.advanceTimersByTimeAsync(20);
    const stale = await first;
    expect(stale).toMatchObject({ code: "FABRIC_PARTICIPANT_STALE", idempotencyKey: explicit ? "same-message" : expect.any(String) });
    expiresAt = Date.now() + 120000; drop = false;
    const retry = sender.request("owner", "session:target", "followUp", { message: "unchanged" }, "owner", { idempotencyKey: stale.idempotencyKey });
    await drainMicrotasks(); await vi.advanceTimersByTimeAsync(100);
    await expect(retry).resolves.toMatchObject({ messageId: "one-delivery" });
    expect(command(sender).commandId).toBe(firstId); expect(deliver).toHaveBeenCalledOnce();
  });
});

describe("same-key retry after proven-notRun resend (#4383)", () => {
  it("re-observes the stable second attempt rather than delivering again", async () => {
    vi.useFakeTimers(); const root = temp(); let expiresAt = Date.now() + 120000;
    const owner = control(root, "owner"), sender = control(root, "sender", { captureOwnerLease: () => () => expiresAt });
    const deliver = vi.fn(() => ({ accepted: true, messageId: "one-resend" }));
    sender.start(() => ({ accepted: false }));
    const publish = owner.mesh.publish.bind(owner.mesh); let drop = true;
    vi.spyOn(owner.mesh, "publish").mockImplementation(async input => {
      if (drop && input.topic === "fabric.control.ack" && input.kind === "accepted")
        return { sequence: 0 } as Awaited<ReturnType<typeof owner.mesh.publish>>;
      return publish(input);
    });
    const first = sender.request("owner", "session:target", "followUp", { message: "unchanged" }, "owner", { idempotencyKey: "resend-key" }).catch(e => e);
    await drainMicrotasks(); await vi.advanceTimersByTimeAsync(60001);
    owner.start(deliver); await vi.advanceTimersByTimeAsync(500);
    expect(deliver).toHaveBeenCalledOnce(); const admittedId = command(sender).commandId;
    expect(sender.mesh.read({ topic: "fabric.control.command" })).toHaveLength(2);
    expiresAt = Date.now() - 45001; await vi.advanceTimersByTimeAsync(20);
    expect(await first).toMatchObject({ code: "FABRIC_PARTICIPANT_STALE", idempotencyKey: "resend-key" });
    expiresAt = Date.now() + 120000; drop = false;
    const retry = sender.request("owner", "session:target", "followUp", { message: "unchanged" }, "owner", { idempotencyKey: "resend-key" });
    await drainMicrotasks(); await vi.advanceTimersByTimeAsync(500);
    await expect(retry).resolves.toMatchObject({ messageId: "one-resend" });
    expect(command(sender).commandId).toBe(admittedId); expect(deliver).toHaveBeenCalledOnce();
  });
});

describe("mixed-host idle graph/IO bench (#4383)", () => {
  it("100 mixed file/state owners on a 2.35MB state add zero idle state.json reads", async () => {
    const root = temp(), mesh = new MeshStore(root, 65536, 1000, { readCacheMs: 60000 });
    const now = Date.now();
    const f = await fixture(0); const base = f.presence;
    await mesh.writeBatch({ identity: identity("writer"), ops: [
      { kind: "put", key: "topology/liveness", value: { version: 1, hostLeases: "files" } },
      ...Array.from({ length: 44 }, (_, i) => ({ kind: "put" as const, key: `bench/padding-${i}`, value: "x".repeat(50_000) })),
      ...Array.from({ length: 100 }, (_, i) => {
        const id = `session:mixed-${i}`, who = identity(id);
        if (i % 2 === 0) writeHostLease(root, { id, rootId: id, identityId: id, startedAt: 1, updatedAt: now, expiresAt: now + 60000 });
        return [{ kind: "put" as const, identity: who, key: key("topology/hosts/", id), value: { format: 1, id, rootId: id, identity: who,
          startedAt: 1, updatedAt: now, expiresAt: now + 60000 } },
        { kind: "put" as const, identity: who, key: key("topology/participants/", id), value: { ...base, id, rootId: id, ownerHostId: id, ownerIdentityId: id } }];
      }).flat(),
    ] });
    const reader = identity("reader"), d = new ParticipantDirectory(mesh, { enabled: true, hostId: reader.id, rootId: reader.id, identity: reader }); directories.push(d);
    d.list({ scope: "project" });
    const reads = vi.spyOn(fs, "readFileSync");
    for (let i = 0; i < 100; i++) {
      expect(d.get(`session:mixed-${i}`, now)).toBeDefined();
      expect(readHostLease(root, `session:mixed-${i}`)?.id).toBe(i % 2 === 0 ? `session:mixed-${i}` : undefined);
    }
    expect(reads.mock.calls.filter(([file]) => String(file) === path.join(root, "state.json"))).toHaveLength(0);
    const stateBytes = fs.statSync(path.join(root, "state.json")).size;
    expect(stateBytes).toBeGreaterThan(2_300_000);
    console.info("route-grace mixed-host bench", JSON.stringify({ owners: 100, fileOwners: 50, stateOwners: 50, stateBytes, idleStateReads: 0 }));
  });
});
