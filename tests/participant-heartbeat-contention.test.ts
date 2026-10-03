import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { deadHostRecords, reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { LIVENESS_POLICY_KEY, readHostLeases } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const directories: ParticipantDirectory[] = [];
const roots: string[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const setup = async (files: boolean, leaseMs = 15_000) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-heartbeat-contention-"));
  roots.push(root);
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const identity: MeshIdentity = { id: "session:live", name: "main", kind: "main", sessionId: "live" };
  const mesh = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 0 });
  if (files) await mesh.put({ key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files" }, identity });
  const record: FabricParticipantRecord = {
    format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    kind: "root", name: "main", label: "TEST-1", status: "idle", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp", "fabric"], cwd: root, sessionId: "live", startedAt: now,
    updatedAt: now, pendingMessages: false, controlProtocol: "v1",
  };
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, identity, rootId: identity.id, hostId: identity.id,
    heartbeatMs: 5_000, leaseMs, reapDeadHosts: false,
  });
  directories.push(directory);
  directory.registerSource(() => [{ ...record, updatedAt: Date.now() }]);
  await directory.refresh();
  const writes = vi.spyOn(mesh, "writeBatch");
  const hostKey = key("topology/hosts/", identity.id);
  const participantKey = key("topology/participants/", identity.id);
  return { root, mesh, identity, record, directory, writes, hostKey, participantKey,
    advance: (ms: number) => { now += ms; }, now: () => now };
};

describe("#3752 participant heartbeat contention", () => {
  it.each([false, true])("timestamp-only refresh does not write state (file policy: %s)", async files => {
    const { root, directory, writes, identity, advance } = await setup(files);
    const before = fs.readFileSync(path.join(root, "state.json"), "utf8");
    advance(5_000);
    await directory.refresh();
    expect(writes).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(before);
    expect(readHostLeases(root).get(identity.id)?.updatedAt).toBe(Date.now());
    expect(directory.confirmedAt()).toBe(Date.now());
  });

  it.each([false, true])("status and capability changes publish immediately (file policy: %s)", async files => {
    const { directory, record, writes, advance } = await setup(files);
    advance(1_000);
    record.status = "running";
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(1);
    expect(directory.get(record.id)).toMatchObject({ status: "running", stale: false });
    record.capabilities = ["fabric"];
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(2);
    expect(directory.get(record.id)?.capabilities).toEqual(["fabric"]);
  });

  it.each([false, true])("a real change does not republish a timestamp-only sibling (file policy: %s)", async files => {
    const { directory, mesh, record, writes, advance } = await setup(files);
    const sibling: FabricParticipantRecord = {
      ...record, id: "agent:sibling", kind: "agent", parentId: record.id, name: "sibling", transport: "process",
    };
    directory.registerSource(() => [{ ...sibling, updatedAt: Date.now() }]);
    await directory.refresh();
    const siblingKey = key("topology/participants/", sibling.id);
    const before = mesh.get(siblingKey)!;
    writes.mockClear();
    advance(1_000);
    record.status = "running";
    await directory.refresh();
    expect(writes).toHaveBeenCalledOnce();
    expect(directory.get(record.id)?.status).toBe("running");
    expect(mesh.get(siblingKey)!.version).toBe(before.version);
    expect(directory.get(sibling.id)).toMatchObject({ stale: false });
  });

  it("renews state leases at half-life without rewriting timestamp-only participants", async () => {
    const { directory, mesh, hostKey, participantKey, writes, advance } = await setup(false);
    const participant = mesh.get(participantKey)!;
    const host = mesh.get(hostKey)!;
    advance(5_000);
    await directory.refresh();
    advance(5_000);
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(1);
    expect(mesh.get(hostKey)!.version).toBeGreaterThan(host.version);
    expect(mesh.get(participantKey)!.version).toBe(participant.version);
    expect(mesh.get("sessions/live")!.updatedAt).toBe(Date.now());
  });

  it("keeps the legacy session TTL fresh even with a longer host lease", async () => {
    const { directory, mesh, writes, advance } = await setup(false, 120_000);
    advance(10_000);
    await directory.refresh();
    expect(writes).toHaveBeenCalledTimes(1);
    expect(mesh.get("sessions/live")!.updatedAt).toBe(Date.now());
  });

  it("unchanged participants stay live to directory, lineage, reaper and bridge, then expire", async () => {
    const { directory, mesh, hostKey, participantKey, identity, writes, advance, now } = await setup(true);
    const participant = mesh.get(participantKey)!;
    advance(20_000); // state lease has lapsed, but the independent file lease renews
    await directory.refresh();
    expect(writes).not.toHaveBeenCalled();
    expect(mesh.get(participantKey)!.version).toBe(participant.version);
    expect(directory.get(identity.id)).toMatchObject({ stale: false });
    expect(directory.lineageAlive(identity.id)).toBe(true);
    expect(deadHostRecords(mesh, { ownHostId: "observer", now: now(), deadAfterMs: 0 })).toEqual([]);
    const bridge = new StoreBridgeSide(mesh, "other", now);
    const presence = await bridge.presence();
    expect(presence.participants.map(p => p.id)).toEqual([identity.id]);
    expect(presence.hosts[0]!.record.updatedAt).toBe(now());
    expect(presence.hosts[0]!.expiresAt - presence.hosts[0]!.record.updatedAt).toBe(15_000);
    advance(20_000); // no refresh: both liveness sources lapse
    expect(directory.get(identity.id)).toBeUndefined();
    expect((await bridge.presence()).participants).toEqual([]);
    // Expiry is not proof of lineage closure, even when the stale row is reaped.
    expect(directory.lineageAlive(identity.id)).toBe(true);
    expect(await reapDeadHostRecords(mesh, identity, { ownHostId: "observer", now: now(), deadAfterMs: 0 })).toBeGreaterThan(0);
    expect(mesh.get(hostKey)).toBeUndefined();
    expect(mesh.get(participantKey)).toBeUndefined();
    expect(directory.lineageAlive(identity.id)).toBe(true);
  });
});
