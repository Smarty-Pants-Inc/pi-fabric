import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { readHostLeaseCurrent, writeHostLease } from "../src/topology/host-leases.js";
import { processStartTime } from "../src/residency/process-identity.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// Keep reason: #12486's real route/delivery boundary and native-process authority fence.
const roots: string[] = [], directories: ParticipantDirectory[] = [], planes: FabricControlPlane[] = [], stores: MeshStore[] = [];
const children: ChildProcess[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const child of children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) { const exited = once(child, "exit"); child.kill(); await exited; }
  }
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const gate = () => {
  let release!: () => void, enter!: () => void;
  const entered = new Promise<void>(resolve => { enter = resolve; });
  const wait = new Promise<void>(resolve => { release = resolve; });
  return { release, enter, entered, wait };
};
const fixture = async (liveHeartbeat = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "root-liveness-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 1_000, { readCacheMs: 0 }); stores.push(mesh);
  const target = "session:target", senderId = "session:sender";
  const owner = new ParticipantDirectory(mesh, { enabled: true, identity: identity(target), hostId: target, rootId: target,
    heartbeatMs: 100, leaseMs: 200, reapDeadHosts: false });
  directories.push(owner);
  const record: FabricParticipantRecord = { format: 1, id: target, rootId: target, ownerHostId: target,
    ownerIdentityId: target, ownerIncarnation: owner.ownerIncarnation, kind: "root", runner: "pi", transport: "host",
    name: "target", status: "idle", capabilities: ["followUp", "steer"], controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now() };
  owner.registerSource(() => [{ ...record, updatedAt: Date.now() }]);
  if (liveHeartbeat) await owner.start(); else await owner.refresh();
  const senderMesh = new MeshStore(root, 65_536, 1_000, { readCacheMs: 0 }); stores.push(senderMesh);
  const reader = new ParticipantDirectory(senderMesh, { enabled: true, identity: identity(senderId), hostId: senderId,
    rootId: senderId, reapDeadHosts: false, routingLease: { graceMs: 0, waitMs: 0 } });
  directories.push(reader);
  const receiver = new FabricControlPlane(mesh, identity(target), { enabled: true, hostId: target,
    ownerIncarnation: owner.ownerIncarnation, pollMs: 10 });
  const sender = new FabricControlPlane(senderMesh, identity(senderId), { enabled: true, hostId: senderId, pollMs: 10,
    captureOwnerLease: (host, who, id) => reader.captureControlOwnerLease(host, who, id) });
  planes.push(receiver, sender);
  const deliver = vi.fn(() => ({ accepted: true, messageId: "delivered" }));
  receiver.start(deliver); sender.start(() => ({ accepted: false }));
  type Ports = ConstructorParameters<typeof AgentMessageRouter>;
  const router = new AgentMessageRouter(
    { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0],
    { identity: identity(senderId), status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
      owns: () => false } as unknown as Ports[1],
    { id: senderId, local: true, matches: (id: string) => id === senderId } as unknown as Ports[2],
    reader, sender, binding => binding,
  );
  const participantKey = key("topology/participants/", target), hostKey = key("topology/hosts/", target);
  const expire = async () => {
    const entry = mesh.get(hostKey, { fresh: true })!;
    await mesh.put({ key: hostKey, identity: identity(target), value: { ...(entry.value as object), updatedAt: 1, expiresAt: 2 } });
    writeHostLease(root, { ...readHostLeaseCurrent(root, target)!, updatedAt: 1, expiresAt: 2 });
  };
  const publish = async (changes: Partial<FabricParticipantRecord>) => {
    // Newer shared presence wins over the unchanged committed participant file.
    await pause(2);
    const entry = mesh.get(participantKey, { fresh: true })!;
    await mesh.put({ key: participantKey, identity: identity(target), value: { ...(entry.value as object), ...changes } });
  };
  return { root, mesh, target, owner, reader, record, router, deliver, expire, publish, hostKey };
};

describe("Main liveness independent of tool turns (#12486)", () => {
  it.each(["publication", "preparation"] as const)("renews across several leases during awaited %s, delivering and ACKing each period", async phase => {
    const f = await fixture(true), blocked = gate();
    const confirmed = f.owner.confirmedAt();
    let held = false;
    if (phase === "publication") {
      const write = f.mesh.writeBatch.bind(f.mesh);
      vi.spyOn(f.mesh, "writeBatch").mockImplementationOnce(async input => {
        held = true; blocked.enter(); await blocked.wait; held = false; return write(input);
      });
    } else f.owner.options.beforePublication = async () => { blocked.enter(); await blocked.wait; };
    f.record.status = "running";
    const pending = f.owner.refresh();
    try {
      await blocked.entered;
      for (let period = 0; period < 3; period++) {
        await pause(260); // No turn/tool events; the existing unref heartbeat is the only writer.
        expect(readHostLeaseCurrent(f.root, f.target)!.expiresAt).toBeGreaterThan(Date.now());
        expect(f.owner.confirmedAt()).toBe(confirmed);
        expect(f.owner.canConsumeMesh()).toBe(false);
        if (phase === "publication") expect(held).toBe(true); // awaited write retains real lease custody
        await expect(f.router.routeMessage(f.target, `period ${period}`, undefined, "followUp", undefined,
          { idempotencyKey: `period-${period}` })).resolves.toMatchObject({ messageId: "delivered" });
        expect(f.deliver).toHaveBeenCalledTimes(period + 1);
      }
    } finally { blocked.release(); await pending; }
    expect(f.owner.confirmedAt()).toBeGreaterThan(confirmed);
  });
});

describe.skipIf(process.platform !== "linux")("local native Main routing proof", () => {
  it("routes an expired file with exact live PID/birth, then refuses after that actual process exits", async () => {
    const f = await fixture();
    const child = spawn(process.execPath, ["-e", "process.stdout.write('ready');setInterval(()=>{},1000)"], { stdio: ["ignore", "pipe", "pipe"] });
    children.push(child); await once(child.stdout!, "data");
    const birth = processStartTime(child.pid!)!; expect(birth).toMatch(/^\d+$/);
    await f.publish({ mainProcess: { pid: child.pid!, host: os.hostname(), startTime: birth } });
    await f.expire();
    expect(f.reader.get(f.target, Date.now(), { fresh: true })).toMatchObject({ stale: false });
    expect(f.reader.list({ scope: "project", fresh: true }).find(p => p.id === f.target)).toMatchObject({ stale: false });
    expect(f.reader.retainedRouteAllowed(f.target)).toBe(true);
    const captured = f.reader.captureControlOwnerLease(f.target, f.target, f.target)!;
    const before = Date.now();
    expect(captured()).toBeGreaterThanOrEqual(before + 100);
    await expect(f.router.routeMessage(f.target, "live process", undefined, "followUp")).resolves.toMatchObject({ messageId: "delivered" });
    const exited = once(child, "exit"); child.kill(); await exited;
    expect(captured()).toBeLessThan(Date.now() - 60_000);
    expect(await f.reader.resolveRoutingLease(f.target)).toBe(false);
    expect(f.reader.retainedRouteAllowed(f.target)).toBe(false);
    await expect(f.router.routeMessage(f.target, "gone", undefined, "followUp")).rejects.toThrow("Unknown Fabric participant");
    expect(f.deliver).toHaveBeenCalledOnce();
  });

  it.each(["reused", "foreign", "unknown", "unreadable", "zombie", "actor", "resident", "identity", "mirror"] as const)("does not grant native extension to %s metadata", async kind => {
    const f = await fixture();
    const mainProcess = { pid: process.pid, host: os.hostname(), startTime: processStartTime(process.pid)! };
    const changes: Partial<FabricParticipantRecord> = { mainProcess };
    if (kind === "reused") mainProcess.startTime += "0";
    if (kind === "foreign") mainProcess.host += "-foreign";
    if (kind === "unknown") changes.mainProcess = { pid: process.pid, host: os.hostname() };
    if (kind === "unreadable") mainProcess.pid = 2_147_483_647;
    if (kind === "actor") changes.kind = "actor";
    if (kind === "resident") changes.rootId = "session:other-root";
    if (kind === "identity") changes.ownerIdentityId = "session:other-owner";
    if (kind === "mirror") changes.remoteHost = "foreign";
    await f.publish(changes); await f.expire();
    if (kind === "zombie") {
      const read = fs.readFileSync.bind(fs), proc = `/proc/${process.pid}/stat`;
      const stat = read(proc, "utf8"), close = stat.lastIndexOf(")");
      vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) =>
        String(file) === proc ? stat.slice(0, close + 2) + "Z" + stat.slice(close + 3) :
          Reflect.apply(read, fs, [file, ...args])) as typeof fs.readFileSync);
    }
    // Unknown/malformed records retain the ordinary resolver's behavior; only
    // actual routing is authority to deliver, and it must refuse every case.
    if (!["resident", "identity", "mirror"].includes(kind)) expect(await f.reader.resolveRoutingLease(f.target)).toBe(false);
    expect(f.reader.retainedRouteAllowed(f.target)).toBe(false);
    await expect(f.router.routeMessage(f.target, "must not extend", undefined, "followUp")).rejects.toThrow();
    expect(f.deliver).not.toHaveBeenCalled();
  });

  it.each(["unchanged", "stopping", "reloading", "closed", "activation", "process", "owner", "lease successor"] as const)("captured native ACK extension rechecks %s authority", async change => {
    const f = await fixture();
    await f.publish({ mainProcess: { pid: process.pid, host: os.hostname(), startTime: processStartTime(process.pid)! } });
    await f.expire();
    const captured = f.reader.captureControlOwnerLease(f.target, f.target, f.target)!;
    const before = Date.now();
    expect(captured()).toBeGreaterThanOrEqual(before + 100);
    if (change === "stopping" || change === "reloading") await f.publish({ status: change });
    if (change === "activation") await f.publish({ ownerIncarnation: "replacement" });
    if (change === "process") await f.publish({ mainProcess: { pid: process.pid, host: os.hostname(), startTime: "wrong" } });
    if (change === "owner") {
      const entry = f.mesh.get(f.hostKey, { fresh: true })!;
      await f.mesh.put({ key: f.hostKey, identity: identity(f.target), value: { ...(entry.value as object), incarnationToken: "successor" } });
    }
    if (change === "lease successor") writeHostLease(f.root, { ...readHostLeaseCurrent(f.root, f.target)!, incarnationToken: "successor" });
    if (change === "closed") await f.mesh.put({ key: key("topology/lineage-closures/", f.target), identity: identity(f.target),
      value: { format: 1, rootId: f.target, ownerHostId: f.target, ownerIdentityId: f.target, closedAt: Date.now() } });
    if (change === "unchanged") {
      const beforeRecheck = Date.now();
      expect(captured()).toBeGreaterThanOrEqual(beforeRecheck + 100);
    } else expect(captured()).toBeLessThan(Date.now() - 60_000);
  });
});
