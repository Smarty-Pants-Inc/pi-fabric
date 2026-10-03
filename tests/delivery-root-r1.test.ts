import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { projectOf } from "../src/topology/project-identity.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentDeliveryPrefix, RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => { vi.restoreAllMocks(); vi.unstubAllEnvs(); for (const close of cleanups.splice(0).reverse()) await close(); });
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const original = "session:creating-root";
const successor = "session:11111111-1111-4111-8111-111111111111";
const identity = (id: string): MeshIdentity => ({ id, name: "main", kind: "main", sessionId: id.slice(8) });
const record = (id: string, cwd: string): FabricParticipantRecord => ({
  format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
  name: "main", status: "idle", runner: "pi", transport: "host", role: "project-agent", project: projectOf(cwd), cwd,
  sessionId: id.slice(8), capabilities: ["steer", "followUp", "fabric"], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1",
});
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r1-"));
  cleanups.push(async () => { fs.rmSync(root, { recursive: true, force: true }); });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: original, rootId: original, identity: identity(original), reapDeadHosts: false });
  directory.registerSource(() => [record(original, root)]);
  cleanups.push(() => directory.close());
  return { root, mesh, directory };
};
const renewalRace = (directory: ParticipantDirectory, now: number) => {
  const get = directory.get.bind(directory);
  const lastKnown = directory.lastKnown.bind(directory);
  const live = vi.spyOn(directory, "get").mockImplementation((id, at, options) => {
    const result = get(id, at, options);
    if (id === original) {
      expect(result).toBeUndefined();
      writeHostLease(directory.mesh.root, { id, rootId: id, identityId: id, updatedAt: now, expiresAt: now + 60_000 });
    }
    return result;
  });
  const stale = vi.spyOn(directory, "lastKnown").mockImplementation((id, at) => {
    const result = lastKnown(id, at);
    if (id === original) expect(result).toBeUndefined(); // live after renewal, not stale
    return result;
  });
  return { live, stale };
};

describe("Astra R1 #3662 positive lineage evidence", () => {
  it("never mistakes renewal between mutually exclusive lookups for death", async () => {
    const { directory } = fixture();
    await directory.refresh();
    const now = Date.now() + 120_000;
    const { live, stale } = renewalRace(directory, now);
    expect(directory.lineageAlive(original, now)).toBe(true);
    // Correct path never consults either lease-filtered predicate.
    expect(live).not.toHaveBeenCalled();
    expect(stale).not.toHaveBeenCalled();
  });

  it("requires a positive root-owned clean-close receipt and invalidates it on resume", async () => {
    const { root, mesh, directory } = fixture();
    expect(directory.lineageAlive("session:never-recorded")).toBe(true);
    await directory.refresh();
    await mesh.delete({ key: key("topology/participants/", original) });
    fs.rmSync(path.join(mesh.root, "participants", createHash("sha256").update(original).digest("hex") + ".json"));
    await mesh.delete({ key: "sessions/creating-root" });
    expect(directory.lineageAlive(original)).toBe(true); // Even total absence is unknown.
    await directory.close();
    expect(directory.lineageAlive(original)).toBe(false);
    const resumed = new ParticipantDirectory(mesh, { enabled: true, hostId: original, rootId: original, identity: identity(original) });
    resumed.registerSource(() => [record(original, root)]);
    cleanups.push(() => resumed.close());
    await resumed.refresh();
    expect(mesh.get(key("topology/lineage-closures/", original), { fresh: true })).toBeUndefined();
    expect(resumed.lineageAlive(original)).toBe(true);
  });

  it.each([undefined, null, {}, { format: 1, rootId: original, ownerHostId: "other", ownerIdentityId: original, closedAt: 1 }])(
    "never treats missing or malformed close proof %j as death", async receipt => {
      const { mesh, directory } = fixture();
      if (receipt !== undefined) await mesh.put({ key: key("topology/lineage-closures/", original), identity: identity(original), value: receipt });
      expect(directory.lineageAlive(original)).toBe(true);
    },
  );

  it("refuses a close receipt attributed to anyone other than the creating Main", async () => {
    const { mesh, directory } = fixture();
    await mesh.put({ key: key("topology/lineage-closures/", original), identity: identity(successor), value: {
      format: 1, rootId: original, ownerHostId: original, ownerIdentityId: original, closedAt: Date.now(),
    } });
    expect(directory.lineageAlive(original)).toBe(true);
  });

  it("a resident/child publisher closing cannot certify Main lineage death", async () => {
    const { root, mesh, directory } = fixture();
    const child = new ParticipantDirectory(mesh, { enabled: true, hostId: "resident:child", rootId: original, identity: identity("resident:child") });
    child.registerSource(() => [record(original, root)]);
    await child.refresh();
    await child.close();
    expect(mesh.get(key("topology/lineage-closures/", original), { fresh: true })).toBeUndefined();
    expect(directory.lineageAlive(original)).toBe(true);
  });

  const resident = async () => {
    const { root, mesh, directory } = fixture();
    await directory.refresh();
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: original, sessionId: "creating-root", cwd: root, projectRoot: root,
      project: projectOf(root), meshRoot: mesh.root, actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
      fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    };
    fs.mkdirSync(config.residencyRoot);
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    const host = new ResidentHost(config, () => {});
    cleanups.push(() => host.close());
    await host.start();
    await mesh.put({ key: key("topology/hosts/", successor), identity: identity(successor), value: {
      format: 1, id: successor, rootId: successor, identity: identity(successor), startedAt: Date.now(), updatedAt: Date.now(), expiresAt: Date.now() + 120_000,
    } });
    await mesh.put({ key: key("topology/participants/", successor), identity: identity(successor), value: record(successor, root) });
    const expired = Date.now() - 60_000;
    await mesh.put({ key: key("topology/hosts/", original), identity: identity(original), value: {
      format: 1, id: original, rootId: original, identity: identity(original), startedAt: 1, updatedAt: expired, expiresAt: expired,
    } });
    writeHostLease(mesh.root, { id: original, rootId: original, identityId: original, updatedAt: expired, expiresAt: expired });
    vi.stubEnv("SMARTY_LEAD_SESSION", successor);
    return { root, mesh, directory, config, host };
  };

  it("renewal cannot move delivery to a newer bound project-agent root", async () => {
    const { mesh, config, host } = await resident();
    expect(host.participants.get(original, Date.now(), { fresh: true })).toBeUndefined();
    renewalRace(host.participants, Date.now());
    host.actors.onDeliver({
      actor: { id: "actor:renewal", name: "renewal", project: config.project } as Parameters<typeof host.actors.onDeliver>[0]["actor"],
      message: { id: "renewal", actorId: "actor:renewal", actorName: "renewal", direction: "out", source: "actor", createdAt: Date.now(), text: "renewal directive" },
      delivery: "steer", triggerTurn: true,
    });
    await vi.waitFor(() => expect(mesh.listAll("residency/deliveries/", { fresh: true })).toHaveLength(1));
    expect(mesh.listAll(residentDeliveryPrefix(original))).toHaveLength(1);
    expect(mesh.listAll(residentDeliveryPrefix(successor))).toHaveLength(0);
  });

  it.each(["session", "durable"] as const)("renewal cannot authorize %s orphan adoption", async residency => {
    const { host, config, mesh } = await resident();
    const actor = await host.actors.create({ name: "renewal", instructions: "Keep root lineage.", residency });
    await host.close();
    const nextConfig = { ...config, rootId: successor, sessionId: successor.slice(8), residencyRoot: path.join(path.dirname(config.residencyRoot), "successor"), role: "project-agent" };
    fs.mkdirSync(nextConfig.residencyRoot);
    fs.writeFileSync(path.join(nextConfig.residencyRoot, "config.json"), JSON.stringify(nextConfig));
    const next = new ResidentHost(nextConfig, () => {});
    cleanups.push(() => next.close());
    await next.start();
    renewalRace(next.participants, Date.now());
    const registry = path.join(config.actorRoot, "actors.json");
    const before = fs.readFileSync(registry, "utf8");
    next.actors.listOwned();
    await new Promise(resolve => setTimeout(resolve, 150));
    expect(next.actors.owns(actor.id)).toBe(false);
    expect(next.actors.status(actor.id).rootId).toBe(original);
    expect(fs.readFileSync(registry, "utf8")).toBe(before);
    expect(mesh.get(key("topology/participants/", original), { fresh: true })).toBeDefined();
  });
});
