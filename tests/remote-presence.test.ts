import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
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

const setup = async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-remote-presence-"));
  cleanup.push(() => fs.rmSync(dir, { recursive: true, force: true }));
  const meshRoot = path.join(dir, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
  const local = identityOf("dev1");
  const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000), {
    enabled: true, hostId: local.id, rootId: local.id, identity: local,
    heartbeatMs: 100, leaseMs: 5_000, reapDeadHosts: false,
  });
  directory.registerSource(() => [rootRecord(local.id, local.id, local.id)]);
  await directory.start();
  cleanup.push(() => directory.close());

  const remote = identityOf("forge");
  // What the bridge writes: the remote records verbatim, as the remote identity, marked remoteHost.
  const mirror = async (
    options: { expiresAt?: number; record?: Partial<FabricParticipantRecord> & Record<string, unknown>; hostId?: string; host?: Record<string, unknown> } = {},
  ) => {
    const hostId = options.hostId ?? remote.id;
    const expiresAt = options.expiresAt ?? Date.now() + 5_000;
    const record = { ...rootRecord(remote.id, hostId, remote.id), remoteHost: "forge", ...options.record };
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
) => {
  type Ports = ConstructorParameters<typeof AgentMessageRouter>;
  return new AgentMessageRouter(
    { status: () => { throw new Error("Unknown Fabric agent"); }, steer: vi.fn(), followUp: vi.fn(), stop: vi.fn() } as unknown as Ports[0],
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

  it("routes steer and followUp to the mirrored root's owner host and returns its acknowledgement", async () => {
    const { mesh, directory, mirror, local, remote } = await setup();
    await mirror();
    const sender = new FabricControlPlane(mesh, local, { enabled: true, hostId: local.id, pollMs: 20 });
    const owner = new FabricControlPlane(mesh, remote, { enabled: true, hostId: remote.id, pollMs: 20 });
    const received: Array<[string, string, string | undefined, string]> = [];
    sender.start(() => ({ accepted: false, error: "unused" }));
    owner.start((command, from) => {
      received.push([command.operation, command.targetId, command.message, from.id]);
      return { accepted: true, messageId: `m-${received.length}` };
    });
    cleanup.push(() => sender.close(), () => owner.close());
    const router = routerFor(directory, sender, local);
    await expect(router.routeMessage(remote.id, "steer me", undefined, "steer"))
      .resolves.toMatchObject({ routed: "mesh", acknowledged: true, messageId: "m-1" });
    await expect(router.routeMessage(remote.id, "later", undefined, "followUp"))
      .resolves.toMatchObject({ routed: "mesh", acknowledged: true, messageId: "m-2" });
    expect(received).toEqual([
      ["steer", remote.id, "steer me", local.id],
      ["followUp", remote.id, "later", local.id],
    ]);
    const commands = mesh.tail(0, 100).events.filter((event) => event.topic === "fabric.control.command");
    expect(commands.map((event) => event.to)).toEqual([remote.id, remote.id]);
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

  it("never takes a mirrored record as a local owner or as this host's own record", async () => {
    const { mesh, directory, mirror, local } = await setup();
    // A mirrored root claiming this host and identity as its owner, written as this host.
    await mesh.put({
      key: "topology/participants/" + hash("session:spoof"),
      value: { ...rootRecord("session:spoof", local.id, local.id), remoteHost: "forge" },
      identity: local,
    });
    expect(directory.get("session:spoof")).toBeUndefined();
    expect(directory.list({ includeStale: true }).map((participant) => participant.id)).not.toContain("session:spoof");
    // A mirrored copy of this host's own root never replaces its own record.
    await mirror({ record: { id: local.id, rootId: local.id }, hostId: "session:elsewhere" });
    await directory.refresh();
    expect(directory.self()).toMatchObject({ id: local.id, local: true });
    expect(directory.self().remoteHost).toBeUndefined();
    expect(directory.list({ scope: "local" }).map((participant) => participant.id)).toEqual([local.id]);
  });

  it("rejects mixed ownership: a remote record under a local host, a local record under a mirrored host", async () => {
    const { mesh, directory, mirror, remote } = await setup();
    await mirror({ host: { remoteHost: undefined } });                   // host not marked remote
    expect(directory.get(remote.id)).toBeUndefined();
    await mirror({ record: { remoteHost: undefined } });                 // participant not marked
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
