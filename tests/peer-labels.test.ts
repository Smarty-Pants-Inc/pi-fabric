import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { publicationGeneration } from "../src/topology/publication-generation.js";
import { readParticipantFiles } from "../src/topology/participant-files.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];

const rootRecord = (id: string, sessionId: string, cwd: string): FabricParticipantRecord => ({
  format: 1,
  id,
  kind: "root",
  rootId: id,
  ownerHostId: id,
  ownerIdentityId: id,
  name: "main",
  status: "idle",
  runner: "pi",
  transport: "host",
  capabilities: ["steer", "followUp", "fabric"],
  cwd,
  sessionId,
  startedAt: 1,
  updatedAt: 2,
  pendingMessages: false,
  controlProtocol: "v1",
});

const agentRecord = (id: string, rootId: string): FabricParticipantRecord => ({
  format: 1,
  id,
  kind: "agent",
  rootId,
  ownerHostId: rootId,
  ownerIdentityId: rootId,
  parentId: rootId,
  name: id,
  status: "running",
  runner: "pi",
  transport: "process",
  capabilities: ["steer", "followUp", "stop"],
  startedAt: 3,
  updatedAt: 4,
  controlProtocol: "v1",
});

const createDirectory = (
  meshRoot: string,
  identity: MeshIdentity,
  rootId: string,
  source: () => FabricParticipantRecord[],
  enabled = true,
): ParticipantDirectory => {
  const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000), {
    enabled,
    hostId: identity.id,
    rootId,
    identity,
    heartbeatMs: 100,
    leaseMs: 300,
  });
  directory.registerSource(source);
  directories.push(directory);
  return directory;
};

const mainIdentity = (name: string): MeshIdentity => ({
  id: `session:${name}`,
  name: "main",
  kind: "main",
  sessionId: name,
});

const tmpRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-peer-labels-"));
  roots.push(root);
  return path.join(root, "mesh");
};

afterEach(async () => {
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("peer labels", () => {
  it.each([false, true])("publishes a fresh root with one sequence claim across generation retry (race=%s)", async race => {
    const meshRoot = tmpRoot(), identity = mainIdentity("fresh");
    const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
    let attempts = 0;
    const prepare = vi.fn(() => {
      // Include state.json, so the label CAS itself would invalidate a late claim.
      const generation = publicationGeneration(meshRoot);
      return () => generation === publicationGeneration(meshRoot);
    });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity,
      heartbeatMs: 60_000, reapDeadHosts: false, preparePublicationFence: prepare,
      withPublicationFence: async publish => {
        if (++attempts === 1 && race) await mesh.put({ key: "test/race", value: true, identity });
        return mesh.withTryLock(publish, 50);
      },
    });
    directories.push(directory);
    directory.registerSource(() => [rootRecord(identity.id, "fresh", "/repo/project")]);
    await directory.start();
    expect(attempts).toBe(race ? 2 : 1);
    expect(prepare).toHaveBeenCalledTimes(attempts);
    expect(mesh.get("topology/peer-seq")).toMatchObject({ version: 1, value: { next: 1 } });
    expect(directory.self().label).toBe("PRO-1");
    expect(directory.canConsumeMesh()).toBe(true);
    expect(mesh.listAll("topology/hosts/")).toHaveLength(1);
    expect(readParticipantFiles(meshRoot).map(entry => entry.value)).toContainEqual(expect.objectContaining({ id: identity.id, label: "PRO-1" }));
    await directory.refresh();
    expect(mesh.get("topology/peer-seq")).toMatchObject({ version: 1, value: { next: 1 } });
  });
  it("mints sequential Linear-style labels from the project basename", async () => {
    const meshRoot = tmpRoot();
    const alpha = createDirectory(meshRoot, mainIdentity("alpha"), "session:alpha", () => [
      rootRecord("session:alpha", "alpha", "/repo/pi-queue-steer"),
    ]);
    const beta = createDirectory(meshRoot, mainIdentity("beta"), "session:beta", () => [
      rootRecord("session:beta", "beta", "/repo/pi-queue-steer"),
    ]);

    await alpha.start();
    await beta.start();

    expect(alpha.self().label).toBe("PQS-1");
    expect(beta.self().label).toBe("PQS-2");
    expect(alpha.peers().map((peer) => [peer.label, peer.name])).toEqual([["PQS-2", "PQS-2"]]);
    expect(beta.peers().map((peer) => [peer.label, peer.name])).toEqual([["PQS-1", "PQS-1"]]);
  });

  it("keeps labels stable across refreshes and never labels agents", async () => {
    const meshRoot = tmpRoot();
    const alpha = createDirectory(meshRoot, mainIdentity("alpha"), "session:alpha", () => [
      rootRecord("session:alpha", "alpha", "/repo/fabric"),
      agentRecord("agent:alpha-child", "session:alpha"),
    ]);
    await alpha.start();
    await alpha.refresh();
    await alpha.refresh();

    expect(alpha.self().label).toBe("FAB-1");
    const child = alpha.get("agent:alpha-child");
    expect(child?.label).toBeUndefined();
    // A second host attaching afterwards sees the already-minted label.
    const beta = createDirectory(meshRoot, mainIdentity("beta"), "session:beta", () => [
      rootRecord("session:beta", "beta", "/repo/fabric"),
    ]);
    await beta.start();
    expect(beta.peers().at(0)?.label).toBe("FAB-1");
  });

  it("never reuses retired sequence numbers", async () => {
    const meshRoot = tmpRoot();
    const alpha = createDirectory(meshRoot, mainIdentity("alpha"), "session:alpha", () => [
      rootRecord("session:alpha", "alpha", "/repo/project"),
    ]);
    await alpha.start();
    expect(alpha.self().label).toBe("PRO-1");
    await alpha.close();

    // The previous record is gone; the counter still moves forward.
    const gamma = createDirectory(meshRoot, mainIdentity("gamma"), "session:gamma", () => [
      rootRecord("session:gamma", "gamma", "/repo/project"),
    ]);
    await gamma.start();
    expect(gamma.self().label).toBe("PRO-2");
  });

  it("survives concurrent first-time registration without duplicate labels", async () => {
    const meshRoot = tmpRoot();
    const alpha = createDirectory(meshRoot, mainIdentity("alpha"), "session:alpha", () => [
      rootRecord("session:alpha", "alpha", "/repo/project"),
    ]);
    const beta = createDirectory(meshRoot, mainIdentity("beta"), "session:beta", () => [
      rootRecord("session:beta", "beta", "/repo/project"),
    ]);
    await Promise.all([alpha.start(), beta.start()]);
    const labels = [alpha.self().label, beta.self().label].sort();
    expect(labels).toEqual(["PRO-1", "PRO-2"]);
  });

  it("skips minting when the mesh is disabled", async () => {
    const meshRoot = tmpRoot();
    const alpha = createDirectory(
      meshRoot,
      mainIdentity("alpha"),
      "session:alpha",
      () => [rootRecord("session:alpha", "alpha", "/repo/project")],
      false,
    );
    await alpha.start();
    expect(alpha.self().label).toBeUndefined();
  });
});
