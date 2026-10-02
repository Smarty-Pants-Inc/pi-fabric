import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { removeHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { MIRROR_COLLISION_TOPIC, ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readParticipantFiles, writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#2004: the mesh bridge mirrors another host's root records and host lease into this
// mesh, marked remoteHost. They are routing targets, never local owners, and expire with the
// mirrored lease. The "remote" control plane below stands in for the far side of the bridge: it
// answers commands addressed to the remote host id with the remote owner identity.

const cleanup: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const step of cleanup.splice(0).reverse()) await step();
});

const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const identityOf = (name: string): MeshIdentity => ({ id: `session:${name}`, name: "main", kind: "main", sessionId: name });

const rootRecord = (id: string, hostId: string, identityId: string): FabricParticipantRecord => ({
  format: 1,
  id,
  kind: "root",
  rootId: id,
  ownerHostId: hostId,
  ownerIdentityId: identityId,
  name: "main",
  status: "idle",
  runner: "pi",
  transport: "host",
  capabilities: ["steer", "followUp", "fabric"],
  cwd: "/home/forge/project",
  sessionId: id.replace(/^session:/, ""),
  startedAt: 1,
  updatedAt: 2,
  pendingMessages: false,
  controlProtocol: "v1",
});

const agentRecord = (id: string, rootId: string): FabricParticipantRecord => ({
  format: 1, id, kind: "agent", rootId, ownerHostId: rootId, ownerIdentityId: rootId, parentId: rootId,
  name: "child", status: "running", runner: "pi", transport: "process", capabilities: ["steer", "followUp", "stop"],
  startedAt: 3, updatedAt: 4, controlProtocol: "v1",
});

const setup = async (options: { heartbeatMs?: number; children?: string[]; readCacheMs?: number; remoteId?: string } = {}) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-remote-presence-"));
  cleanup.push(() => fs.rmSync(dir, { recursive: true, force: true }));
  const meshRoot = path.join(dir, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
  const local = identityOf("dev1");
  const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000, { readCacheMs: options.readCacheMs ?? 0 }), {
    enabled: true, hostId: local.id, rootId: local.id, identity: local,
    heartbeatMs: options.heartbeatMs ?? 100, leaseMs: Math.max(5_000, 2 * (options.heartbeatMs ?? 100)), reapDeadHosts: false,
  });
  directory.registerSource(() => [
    rootRecord(local.id, local.id, local.id),
    ...(options.children ?? []).map((id) => agentRecord(id, local.id)),
  ]);
  await directory.start();
  cleanup.push(() => directory.close());

  const remote = options.remoteId
    ? { ...identityOf(options.remoteId), id: options.remoteId }
    : identityOf("forge");
  // What the bridge writes: the remote records verbatim, as the remote identity, marked remoteHost.
  const mirror = async (
    options: {
      expiresAt?: number;
      record?: Partial<FabricParticipantRecord> & Record<string, unknown>;
      /** Write the participant without remoteHost, as a local host would. */
      unmarked?: true;
      hostId?: string;
      host?: Record<string, unknown>;
    } = {},
  ) => {
    const hostId = options.hostId ?? remote.id;
    const expiresAt = options.expiresAt ?? Date.now() + 5_000;
    const marked = { ...rootRecord(remote.id, hostId, remote.id), remoteHost: "forge", ...options.record };
    const { remoteHost: _remoteHost, ...unmarked } = marked;
    const record = options.unmarked ? unmarked : marked;
    await mesh.writeBatch({
      identity: remote,
      ops: [
        { kind: "put", key: "topology/participants/" + hash(record.id), value: record },
        {
          kind: "put",
          key: "topology/hosts/" + hash(hostId),
          value: {
            format: 1, id: hostId, rootId: remote.id, identity: remote,
            startedAt: 1, updatedAt: expiresAt - 5_000, expiresAt, remoteHost: "forge", ...options.host,
          },
        },
      ],
    });
    return record;
  };
  return { meshRoot, mesh, local, remote, directory, mirror };
};

const routerFor = (
  directory: ParticipantDirectory,
  control: FabricControlPlane,
  self: MeshIdentity,
  children: string[] = [],
) => {
  type Ports = ConstructorParameters<typeof AgentMessageRouter>;
  const known = (id: string) => {
    if (!children.includes(id)) throw new Error(`Unknown Fabric agent: ${id}`);
    return { id, name: "child" };
  };
  return new AgentMessageRouter(
    {
      status: known,
      steer: vi.fn((id: string) => (known(id), { messageId: "local-steer" })),
      followUp: vi.fn((id: string) => (known(id), { messageId: "local-followUp" })),
      stop: vi.fn(),
    } as unknown as Ports[0],
    {
      identity: self,
      status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
      validateDirectMessage: vi.fn(), tell: vi.fn(), ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn(), resolveBinding: vi.fn(),
    } as unknown as Ports[1],
    { id: self.id, local: true, matches: (id: string) => id === self.id || id === "main", deliverAgent: vi.fn() } as unknown as Ports[2],
    directory,
    control,
    (binding) => binding,
  );
};

describe("mirrored remote roots (smarty-dev#2004)", () => {
  it("lists a mirrored root as a remote session and peer with its host, never local", async () => {
    const { directory, mirror, remote } = await setup();
    await mirror();
    const got = directory.get(remote.id);
    expect(got).toMatchObject({ id: remote.id, remoteHost: "forge", local: false, stale: false });
    expect(directory.sessions().map((session) => [session.id, session.local, session.remoteHost]))
      .toEqual(expect.arrayContaining([["session:dev1", true, undefined], [remote.id, false, "forge"]]));
    expect(directory.peers()).toEqual([expect.objectContaining({ id: remote.id, host: "forge", local: false })]);
    expect(directory.list({ scope: "local" }).map((participant) => participant.id)).toEqual(["session:dev1"]);
  });

  it.each(["steer", "followUp"] as const)("resolves a long-lived remote root from a fresh Main's negative cache within its first minute (%s)", async (kind) => {
    const { mesh, directory, mirror, local, remote } = await setup({ heartbeatMs: 60_000, readCacheMs: 60_000 });
    expect(directory.get(remote.id)).toBeUndefined(); // Main's first read, before presence arrived
    await mirror(); // another process refreshes the already long-lived remote root
    const { received } = await remoteOwner(mesh, local, remote);
    const sender = senderOn(mesh, local, 5_000, directory);
    const request = vi.spyOn(sender, "request");
    const router = routerFor(directory, sender, local);
    await expect(router.routeMessage(remote.id, "first-minute reply", undefined, kind))
      .resolves.toMatchObject({ acknowledged: true, messageId: "m-1" });
    expect(received).toEqual([[kind, remote.id, "first-minute reply", local.id]]);
    expect(directory.peers()).toEqual([expect.objectContaining({ id: remote.id, host: "forge" })]);
    expect(request).toHaveBeenCalledExactlyOnceWith(remote.id, remote.id, kind,
      { message: "first-minute reply", data: undefined, principal: undefined,
        ...(kind === "followUp" ? { triggerTurn: true } : {}) }, remote.id, { routedRemoteHost: "forge" });
  });

  // The far side of the bridge: the owner runs on its own mesh; a relay carries commands there and
  // acknowledgements back, stamped data.bridge = { from: <side> } as the mesh bridge does.
  const remoteOwner = async (
    mesh: MeshStore,
    local: MeshIdentity,
    remote: MeshIdentity,
    stamp: (data: Record<string, unknown>) => Record<string, unknown> = (data) => ({ ...data, bridge: { from: "forge", id: "x" } }),
  ) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-remote-mesh-"));
    cleanup.push(() => fs.rmSync(dir, { recursive: true, force: true }));
    const far = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 1_000);
    const received: Array<[string, string, string | undefined, string]> = [];
    const owner = new FabricControlPlane(far, remote, { enabled: true, hostId: remote.id, pollMs: 20 });
    owner.start((command, from) => {
      received.push([command.operation, command.targetId, command.message, from.id]);
      return { accepted: true, messageId: `m-${received.length}` };
    });
    const cursors = { near: 0, far: 0 };
    const relay = async () => {
      const out = mesh.tail(cursors.near, 100);
      cursors.near = out.nextOffset;
      for (const event of out.events) {
        if (event.topic !== "fabric.control.command" || event.to !== remote.id) continue;
        await far.publish({ topic: event.topic, kind: event.kind, from: event.from, to: event.to, data: { ...(event.data as object), bridge: { from: "dev1", id: event.id } } });
      }
      const back = far.tail(cursors.far, 100);
      cursors.far = back.nextOffset;
      for (const event of back.events) {
        if (event.topic !== "fabric.control.ack" || event.to !== local.id) continue;
        await mesh.publish({ topic: event.topic, kind: event.kind, from: event.from, to: event.to, data: stamp(event.data as Record<string, unknown>) });
      }
    };
    const timer = setInterval(() => void relay().catch(() => undefined), 20);
    cleanup.push(() => clearInterval(timer), () => owner.close());
    return { received };
  };

  const senderOn = (mesh: MeshStore, local: MeshIdentity, acknowledgementTimeoutMs = 300, directory?: ParticipantDirectory) => {
    const sender = new FabricControlPlane(mesh, local, {
      enabled: true, hostId: local.id, pollMs: 20, acknowledgementTimeoutMs,
      ...(directory ? { readMirroredOwner: (host: string, ownerIdentity: string | undefined, target: string) =>
        directory.mirroredControlOwner(host, ownerIdentity, target) } : {}),
    });
    sender.start(() => ({ accepted: false, error: "unused" }));
    cleanup.push(() => sender.close());
    return sender;
  };

  it("routes steer and followUp to the mirrored root through the bridge and returns its acknowledgement", async () => {
    const { mesh, directory, mirror, local, remote } = await setup();
    await mirror();
    const { received } = await remoteOwner(mesh, local, remote);
    const sender = senderOn(mesh, local, 5_000, directory);
    const request = vi.spyOn(sender, "request");
    const router = routerFor(directory, sender, local);
    await expect(router.routeMessage(remote.id, "steer me", undefined, "steer"))
      .resolves.toMatchObject({ routed: "mesh", acknowledged: true, messageId: "m-1" });
    await expect(router.routeMessage(remote.id, "later", undefined, "followUp"))
      .resolves.toMatchObject({ routed: "mesh", acknowledged: true, messageId: "m-2" });
    expect(request).toHaveBeenNthCalledWith(1, remote.id, remote.id, "steer",
      { message: "steer me", data: undefined }, remote.id, { routedRemoteHost: "forge" });
    expect(request).toHaveBeenNthCalledWith(2, remote.id, remote.id, "followUp",
      { message: "later", data: undefined, principal: undefined, triggerTurn: true }, remote.id, { routedRemoteHost: "forge" });
    expect(received).toEqual([
      ["steer", remote.id, "steer me", local.id],
      ["followUp", remote.id, "later", local.id],
    ]);
    const commands = mesh.tail(0, 100).events.filter((event) => event.topic === "fabric.control.command");
    expect(commands.map((event) => event.to)).toEqual([remote.id, remote.id]);
    expect(commands.map((event) => (event.data as { destinationRemoteHost?: string }).destinationRemoteHost))
      .toEqual(["forge", "forge"]);
  });

  it("routes a validated native root with a null destination and preserves request input", async () => {
    const { mesh, directory, mirror, local, remote } = await setup();
    await mirror({ unmarked: true, host: { remoteHost: undefined } });
    const owner = new FabricControlPlane(mesh, remote, { enabled: true, hostId: remote.id, pollMs: 20 });
    const received = vi.fn(() => ({ accepted: true, messageId: "native-root" }));
    owner.start(received);
    cleanup.push(() => owner.close());
    const sender = senderOn(mesh, local, 5_000, directory);
    const request = vi.spyOn(sender, "request");
    const router = routerFor(directory, sender, local);
    const data = { routedRemoteHost: "forge", destinationRemoteHost: "ryzen2", business: "unchanged" };
    await expect(router.routeMessage(remote.id, "native", data, "steer", undefined, { triggerTurn: false }))
      .resolves.toMatchObject({ acknowledged: true, messageId: "native-root" });
    expect(request).toHaveBeenCalledWith(remote.id, remote.id, "steer",
      { message: "native", data, triggerTurn: false }, remote.id, { routedRemoteHost: null });
    expect(received).toHaveBeenCalledWith(expect.objectContaining({
      destinationRemoteHost: null, message: "native", data, triggerTurn: false,
    }), expect.objectContaining({ id: local.id }), expect.any(AbortSignal), "mesh");
  });

  it.each(["steer", "followUp"] as const)("refuses a cached native %s route replaced by a mirror without publication or replacement replay", async (kind) => {
    const { meshRoot, mesh, directory, mirror, local, remote } = await setup({ heartbeatMs: 60_000 });
    await mirror({ unmarked: true, host: { remoteHost: undefined } });
    const native = directory.get(remote.id);
    expect(native).toMatchObject({ id: remote.id });
    expect(native?.remoteHost).toBeUndefined();
    const get = directory.get.bind(directory);
    const cached = vi.spyOn(directory, "get").mockImplementation((id, now, options) =>
      id === remote.id && !options?.fresh ? native : get(id, now, options));
    try {
      await mesh.writeBatch({ identity: remote, ops: [
        { kind: "delete", key: "topology/hosts/" + hash(remote.id) },
        { kind: "delete", key: "topology/participants/" + hash(remote.id) },
      ] });
      removeHostLease(meshRoot, remote.id);
      await mirror({ record: { remoteHost: "ryzen2" }, host: { remoteHost: "ryzen2" } });
      expect(directory.get(remote.id, undefined, { fresh: true })).toMatchObject({ remoteHost: "ryzen2" });
      const sender = senderOn(mesh, local, 300, directory);
      const publish = vi.spyOn(sender.mesh, "publish");
      const request = vi.spyOn(sender, "request");
      const router = routerFor(directory, sender, local);
      const failure = await router.routeMessage(remote.id, "private payload", { private: "native-only" }, kind).catch((error: unknown) => error);
      expect(failure).toMatchObject({ code: "FABRIC_ROUTE_AUTHORITY_CHANGED" });
      expect(failure).toBeInstanceOf(Error);
      expect(request).not.toHaveBeenCalled();
      expect(publish).not.toHaveBeenCalled();
      const writer = new MeshStore(meshRoot, 64 * 1024, 1_000);
      await writer.publish({ topic: "fabric.control.ack", kind: "rejected", from: remote, to: local.id,
        data: { version: 1, commandId: "foreign-native-replacement", targetId: remote.id, accepted: false,
          error: "replacement says not run", notRun: true, bridge: { from: "ryzen2" } } });
      await sender.close();
      expect(mesh.read({ topic: "fabric.control.command", limit: 100 })).toEqual([]);
      expect(publish).not.toHaveBeenCalled();
    } finally {
      cached.mockRestore();
    }
  });

  // Lane A's security pass on pi-fabric#135 (F2): a faulty bridge must not answer for another
  // link's owner, or for a native one.
  it("accepts a mirrored owner's acknowledgement only through its own bridge", { timeout: 30_000 }, async () => {
    for (const forged of [
      (data: Record<string, unknown>) => data,                                                  // no stamp
      (data: Record<string, unknown>) => ({ ...data, bridge: { from: "ryzen2", id: "x" } }),    // another link
    ]) {
      const { mesh, directory, mirror, local, remote } = await setup();
      // Keep the lease live beyond the 45 s bridge/ACK window. The fake clock drives
      // the actual relay, command handler and sender observation without a real 45 s wait.
      await mirror({ expiresAt: Date.now() + 120_000 });
      vi.useFakeTimers();
      const { received } = await remoteOwner(mesh, local, remote, forged);
      const sender = senderOn(mesh, local, 2_000, directory);
      const router = routerFor(directory, sender, local);
      try {
        const observation = router.routeMessage(remote.id, "hi", undefined, "steer");
        void observation.catch(() => undefined);
        await vi.advanceTimersByTimeAsync(0); // publish the actual command before the clock jump
        await vi.advanceTimersByTimeAsync(45_000);
        await expect(observation).rejects.toThrow("Fabric mesh bridge to remote host forge is not responding");
      } finally {
        await sender.close();
        vi.useRealTimers();
      }
      expect(received.length).toBeGreaterThan(0);                                               // it ran; the forged answer was ignored
    }
  });

  it.each([
    ["forge", "withdrawal"], ["forge", "replacement"],
    ["dev1", "withdrawal"], ["dev1", "replacement"],
  ] as const)("refuses cached %s routing before first fresh capture after %s without publication or replacement notRun replay", async (routedHost, change) => {
    const { meshRoot, mesh, directory, mirror, local, remote } = await setup({ heartbeatMs: 60_000, readCacheMs: 2_000, remoteId: "X" });
    // Both link orientations use canonical root X (host = identity = target).
    await mirror({ record: { remoteHost: routedHost }, host: { remoteHost: routedHost } });
    writeHostLease(meshRoot, { id: remote.id, rootId: remote.id, identityId: remote.id,
      updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
    expect(directory.mirroredControlOwner(remote.id, remote.id, remote.id)?.remoteHost).toBe(routedHost);
    expect(directory.get(remote.id)).toMatchObject({ remoteHost: routedHost }); // prime production cache
    const writer = new MeshStore(meshRoot, 64 * 1024, 1_000);
    await writer.writeBatch({ identity: remote, ops: [
      { kind: "delete", key: "topology/hosts/" + hash(remote.id) },
      { kind: "delete", key: "topology/participants/" + hash(remote.id) },
    ] });
    removeHostLease(meshRoot, remote.id);
    if (change === "replacement") await mirror({ record: { remoteHost: "ryzen2" }, host: { remoteHost: "ryzen2" } });
    // Same cached, unexpired shared-state lease still validates the router's old origin.
    expect(directory.get(remote.id)).toMatchObject({ remoteHost: routedHost });
    const sender = senderOn(mesh, local, 5_000, directory);
    const publish = vi.spyOn(sender.mesh, "publish");
    const router = routerFor(directory, sender, local);
    await expect(router.routeMessage(remote.id, "private payload", { destinationRemoteHost: "ryzen2" }, "followUp"))
      .rejects.toThrow(`Fabric mesh bridge routing to remote host ${routedHost} is unavailable for ${remote.id}; the routed owner could not be revalidated; this attempt was not published.`);
    expect(publish).not.toHaveBeenCalled();
    await writer.publish({ topic: "fabric.control.ack", kind: "rejected", from: remote, to: local.id,
      data: { version: 1, commandId: "foreign-unadmitted", targetId: remote.id, accepted: false,
        error: "replacement says not run", notRun: true, bridge: { from: "ryzen2" } } });
    await sender.close(); // consume foreign ACK, prove no resurrection/replay/cancel
    expect(writer.read({ topic: "fabric.control.command", limit: 100 })).toEqual([]);
    expect(publish).not.toHaveBeenCalled();
  });

  it("ignores a bridge-stamped acknowledgement for a native owner, and takes the same one unstamped", async () => {
    const { mesh, local } = await setup();
    const native = identityOf("native");
    const sender = senderOn(mesh, local);
    const answer = async (targetId: string, bridge: boolean) => {
      const command = await vi.waitFor(() => {
        const found = mesh.tail(0, 1_000).events.find((event) =>
          event.topic === "fabric.control.command" && (event.data as { targetId: string }).targetId === targetId);
        if (!found) throw new Error("no command yet");
        return found;
      });
      const { commandId } = command.data as { commandId: string };
      await mesh.publish({
        topic: "fabric.control.ack", kind: "accepted", from: native, to: local.id,
        data: { version: 1, commandId, targetId, accepted: true, messageId: "answered", ...(bridge ? { bridge: { from: "forge", id: "x" } } : {}) },
      });
    };
    const stamped = sender.request(native.id, "session:one", "steer", { message: "x" });
    await answer("session:one", true);
    await expect(stamped).rejects.toThrow("Timed out waiting for the remote Fabric owner to acknowledge");
    const plain = sender.request(native.id, "session:two", "steer", { message: "x" });
    await answer("session:two", false);
    await expect(plain).resolves.toMatchObject({ messageId: "answered" });
  });

  it("expires with the mirrored lease and names the remote host at once, without waiting for an ack", async () => {
    const { directory, mirror, local, remote, meshRoot } = await setup();
    const now = Date.now();
    await mirror({ expiresAt: now + 5_000 });
    writeHostLease(meshRoot, { id: remote.id, rootId: remote.id, identityId: remote.id, updatedAt: now, expiresAt: now + 60_000 });
    // The file lease renews the mirrored host without a shared-state write.
    expect(directory.get(remote.id, now + 30_000)).toMatchObject({ remoteHost: "forge", stale: false });
    expect(directory.get(remote.id, now + 61_000)).toBeUndefined();
    expect(directory.peers(now + 61_000)).toEqual([]);

    await mirror({ expiresAt: Date.now() - 2_000 });
    writeHostLease(meshRoot, { id: remote.id, rootId: remote.id, identityId: remote.id, updatedAt: now, expiresAt: Date.now() - 2_000 });
    expect(directory.lastKnown(remote.id)).toMatchObject({ participant: { remoteHost: "forge" } });
    const request = vi.fn();
    const router = routerFor(directory, { request } as unknown as FabricControlPlane, local);
    await expect(router.routeMessage(remote.id, "hello", undefined, "followUp"))
      .rejects.toThrow(`Unknown Fabric participant: ${remote.id} (its lease mirrored from remote host forge lapsed`);
    expect(request).not.toHaveBeenCalled();
  });

  it.each(["steer", "followUp"] as const)("refuses a non-interactive mirrored auditor with a named error (%s)", async (kind) => {
    const { directory, mirror, local, remote } = await setup();
    await mirror({ record: { interactive: false, capabilities: ["fabric"] } });
    const request = vi.fn();
    const router = routerFor(directory, { request } as unknown as FabricControlPlane, local);
    await expect(router.routeMessage(remote.id, "do not interrupt audit", undefined, kind)).rejects.toMatchObject({
      name: "FabricParticipantNonInteractiveError", code: "FABRIC_PARTICIPANT_NON_INTERACTIVE",
    });
    expect(request).not.toHaveBeenCalled();
  });

  it("never takes a mirrored record as a local owner or as this host's own record", async () => {
    const { mesh, directory, local } = await setup();
    // A mirrored root claiming this host and identity as its owner, written as this host.
    await mesh.put({
      key: "topology/participants/" + hash("session:spoof"),
      value: { ...rootRecord("session:spoof", local.id, local.id), remoteHost: "forge" },
      identity: local,
    });
    expect(directory.get("session:spoof")).toBeUndefined();
    expect(directory.list({ includeStale: true }).map((participant) => participant.id)).not.toContain("session:spoof");
  });

  // review/astra F2 on #132: a valid mirrored owner pair at a local participant's key must not
  // redirect it, neither before this host's next heartbeat nor after.
  it("refuses a mirrored collision with a local child or root before and after a heartbeat, and logs it once", async () => {
    const CHILD = "0123456789abcdef0123456789abcdef";
    const { mesh, directory, mirror, local, remote } = await setup({ heartbeatMs: 60_000, children: [CHILD] });
    const hostOf = (id: string) => ({ id: `forge-host:${id}`, rootId: id, identity: remote });
    // Valid pairs: each mirrored root has a mirrored host of the same remoteHost, rootId and identity.
    await mirror({ record: { id: CHILD, rootId: CHILD, ownerHostId: hostOf(CHILD).id }, hostId: hostOf(CHILD).id, host: hostOf(CHILD) });
    const ownLabel = directory.get(local.id)?.label;
    expect(ownLabel).toMatch(/-\d+$/);
    await mirror({ record: { id: local.id, rootId: local.id, ownerHostId: hostOf(local.id).id, label: "EVIL-1" }, hostId: hostOf(local.id).id, host: hostOf(local.id) });
    const mirrored = (id: string) => (mesh.get("topology/participants/" + hash(id), { fresh: true })?.value as { remoteHost?: string }).remoteHost;
    expect([mirrored(CHILD), mirrored(local.id)]).toEqual(["forge", "forge"]);   // the keys now hold the mirrors

    const check = async () => {
      expect(directory.get(CHILD)).toMatchObject({ id: CHILD, kind: "agent", local: true });
      expect(directory.get(local.id)).toMatchObject({ id: local.id, kind: "root", local: true });
      expect(directory.get(local.id)?.remoteHost).toBeUndefined();
      expect(directory.list().filter((participant) => participant.remoteHost !== undefined)).toEqual([]);
      expect(directory.peers()).toEqual([]);
      const request = vi.fn();
      const router = routerFor(directory, { request } as unknown as FabricControlPlane, local, [CHILD]);
      await expect(router.routeMessage(CHILD, "hi", undefined, "followUp"))
        .resolves.toMatchObject({ routed: "local", messageId: "local-followUp" });
      expect(request).not.toHaveBeenCalled();
    };
    await check();                                                    // before this host's heartbeat
    await directory.refresh();
    expect([mirrored(CHILD), mirrored(local.id)]).toEqual([undefined, undefined]); // heartbeat took the keys back
    await check();                                                    // and after it
    expect(directory.get(local.id)?.label).toBe(ownLabel);                     // the mirror's label is never adopted

    const refusals = mesh.tail(0, 1_000).events.filter((event) => event.topic === MIRROR_COLLISION_TOPIC);
    expect(refusals.map((event) => (event.data as { id: string }).id).sort()).toEqual([CHILD, local.id].sort());
  });

  // pi-fabric#142: a local record also lives in its own file. A mirror at its key is refused and
  // reported whichever copy is newer (CI saw a file and a mirror in the same millisecond).
  it("refuses and reports a mirror at a local key even when the local file is newer than the mirror", async () => {
    const CHILD = "0123456789abcdef0123456789abcdef";
    const { mesh, directory, mirror, remote } = await setup({ heartbeatMs: 60_000, children: [CHILD] });
    const hostOf = (id: string) => ({ id: `forge-host:${id}`, rootId: id, identity: remote });
    await mirror({ record: { id: CHILD, rootId: CHILD, ownerHostId: hostOf(CHILD).id }, hostId: hostOf(CHILD).id, host: hostOf(CHILD) });
    const key = "topology/participants/" + hash(CHILD);
    const file = readParticipantFiles(mesh.root, { maxAgeMs: 0 }).find((entry) => entry.key === key)!;
    writeParticipantFile(mesh.root, { ...file, updatedAt: Date.now() + 60_000 });
    expect(directory.get(CHILD)).toMatchObject({ id: CHILD, kind: "agent", local: true });
    expect(directory.list().filter((participant) => participant.remoteHost !== undefined)).toEqual([]);
    const refusals = mesh.tail(0, 1_000).events.filter((event) => event.topic === MIRROR_COLLISION_TOPIC);
    expect(refusals.map((event) => (event.data as { id: string }).id)).toEqual([CHILD]);
  });

  it("qualifies a mirrored peer's label with its host, so a selector never matches it for a local label", async () => {
    const { directory, mirror, remote } = await setup();
    await mirror({ record: { label: "PF-2" } });
    expect(directory.peers()).toEqual([expect.objectContaining({ id: remote.id, label: "PF-2@forge", name: "PF-2@forge", host: "forge" })]);
  });

  // Security review F2 on #132: one malformed mirror must not break discovery for the mesh.
  it("drops a mirror with malformed optional fields alone, logs it once, and peers still lists the healthy ones", async () => {
    const { mesh, directory, mirror, remote } = await setup();
    await mirror();
    for (const [field, bad] of [["sessionId", 42], ["cwd", {}], ["label", 7], ["role", []], ["project", 1], ["repository", 42], ["interactive", "yes"]] as const) {
      await mirror({ record: { id: `session:bad-${field}`, rootId: remote.id, [field]: bad, ...(field === "label" ? {} : { label: undefined }) } as never });
    }
    expect(() => directory.peers()).not.toThrow();
    expect(directory.peers().map((peer) => peer.id)).toEqual([remote.id]);
    expect(directory.sessions().map((session) => session.id).sort()).toEqual(["session:dev1", remote.id].sort());
    const refused = await vi.waitFor(() => {
      const texts = mesh.tail(0, 1_000).events.filter((event) => event.topic === MIRROR_COLLISION_TOPIC).map((event) => event.text);
      if (texts.length < 7) throw new Error("not yet");
      return texts;
    });
    expect(refused.every((text) => text?.endsWith("it is malformed"))).toBe(true);
  });

  it("rejects mixed ownership: a remote record under a local host, a local record under a mirrored host", async () => {
    const { mesh, directory, mirror, remote } = await setup();
    await mirror({ host: { remoteHost: undefined } });                   // host not marked remote
    expect(directory.get(remote.id)).toBeUndefined();
    await mirror({ unmarked: true });                                    // participant not marked
    expect(directory.get(remote.id)).toBeUndefined();
    await mirror({ record: { remoteHost: "../dev1" } });                 // invalid host name
    expect(directory.get(remote.id)).toBeUndefined();
    await mirror({ record: { kind: "agent", parentId: remote.id } });    // v1 mirrors roots only
    expect(directory.get(remote.id)).toBeUndefined();
    await mirror();
    expect(directory.get(remote.id)).toMatchObject({ remoteHost: "forge" });
    expect(mesh.listAll("topology/participants/").length).toBe(2);
  });

  it("leaves mirrored records to the bridge: this host's heartbeat and close keep them", async () => {
    const { mesh, directory, mirror, remote } = await setup();
    await mirror();
    await directory.refresh();
    await directory.close();
    const ids = mesh.listAll("topology/participants/", { fresh: true }).map((entry) => (entry.value as { id: string }).id);
    const hosts = mesh.listAll("topology/hosts/", { fresh: true }).map((entry) => (entry.value as { id: string }).id);
    expect(ids).toEqual([remote.id]);
    expect(hosts).toEqual([remote.id]);
  });
});
