import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { residentHostId } from "../src/residency/protocol.js";

const cleanups: (() => Promise<unknown> | void)[] = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); vi.restoreAllMocks(); });
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const oldRoot = "session:absent-main";
const identity = (id: string): MeshIdentity => ({ id, name: "main", kind: "main", sessionId: id.slice(8) });
const wait = async (predicate: () => boolean) => {
  const end = Date.now() + 10_000;
  while (!predicate()) { if (Date.now() > end) throw new Error("Adoption observation timed out"); await new Promise(resolve => setTimeout(resolve, 20)); }
};
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "absent-adoption-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  cleanups.push(() => agents.close());
  const config = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const owner = new ActorManager("absent-main", identity(oldRoot), mesh, config, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true, rootId: oldRoot, project: root, role: "project-agent", claimResidency: "durable",
  });
  cleanups.push(() => owner.close());
  const actor = await owner.create({ name: "retained", instructions: "Handle exactly once.", residency: "durable", topics: ["absent.proof"], responseMode: "text" });
  await owner.close();
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: "observer", rootId: "observer", identity: identity("observer"), reapDeadHosts: false });
  cleanups.push(() => directory.close());
  const old = Date.now() - 20 * 60_000;
  const clock = vi.spyOn(Date, "now").mockReturnValue(old);
  try {
    await mesh.put({ key: "actors/absent-main/" + actor.id, identity: identity(oldRoot), value: actor });
    const record = { format: 1 as const, id: oldRoot, rootId: oldRoot, kind: "root" as const, name: "main", status: "idle", runner: "pi" as const,
      transport: "host" as const, ownerHostId: oldRoot, ownerIdentityId: oldRoot, capabilities: ["fabric"], startedAt: old - 1000, updatedAt: old, controlProtocol: "v1" as const };
    await mesh.put({ key: key("topology/participants/", oldRoot), identity: identity(oldRoot), value: record });
    writeParticipantFile(mesh.root, { key: key("topology/participants/", oldRoot), version: 1, updatedBy: identity(oldRoot), updatedAt: old, value: record });
    await mesh.put({ key: key("topology/hosts/", oldRoot), identity: identity(oldRoot), value: { format: 1, id: oldRoot, rootId: oldRoot, identity: identity(oldRoot), startedAt: old - 1000, updatedAt: old, expiresAt: old + 15_000 } });
  } finally { clock.mockRestore(); }
  // Real directory reaper removes the old raw Main/host, without a clean-close receipt.
  await reapDeadHostRecords(mesh, identity("observer"), { ownHostId: "observer", deadAfterMs: 10 * 60_000 });
  expect(mesh.get(key("topology/participants/", oldRoot))).toBeUndefined();
  expect(mesh.get(key("topology/lineage-closures/", oldRoot))).toBeUndefined();
  const deliveries: string[] = [];
  const candidate = async (name: string, project = root, role = "project-agent") => {
    const rootId = "session:" + name, hostId = residentHostId(rootId);
    const hostIdentity: MeshIdentity = { id: hostId, kind: "agent", name: "resident" };
    const hostDirectory = new ParticipantDirectory(mesh, { enabled: true, hostId, rootId, identity: hostIdentity, reapDeadHosts: false });
    cleanups.push(() => hostDirectory.close());
    await hostDirectory.refresh();
    const manager = new ActorManager(name, hostIdentity, mesh, config, agents, ({ message }) => { if (message.text) deliveries.push(message.text); }, {
      actorRoot: path.join(root, "actors"), persistent: true, rootId, project, role, claimResidency: "durable", adoptionGraceMs: 0,
      canManageActor: id => { const participant = directory.get(id, Date.now(), { fresh: true }); return participant ? participant.ownerHostId === hostId : undefined; },
      lineageAlive: id => directory.lineageAlive(id),
      lineageAdoptable: id => directory.lineageAdoptable(id),
    });
    cleanups.push(() => manager.close());
    return manager;
  };
  return { root, mesh, actor, directory, candidate, deliveries, agents };
};

describe("F3059 aged absent-root adoption", () => {
  it("reaps the root, then two same-project project-agent hosts adopt and run one event exactly once", async () => {
    const f = await fixture();
    expect(f.directory.lineageAlive(oldRoot)).toBe(true);
    expect(f.directory.lineageAdoptable(oldRoot)).toBe(true);
    const writes = vi.spyOn(ActorRegistryStore.prototype, "write");
    const candidates = await Promise.all([f.candidate("candidate-a"), f.candidate("candidate-b")]);
    await wait(() => candidates.filter(manager => manager.owns(f.actor.id)).length === 1);
    const winner = candidates.find(manager => manager.owns(f.actor.id))!;
    await wait(() => candidates.every(manager => manager.status(f.actor.id).rootId === winner.status(f.actor.id).rootId));
    const claims = writes.mock.calls.filter(([rows]) => rows.some(row => row.id === f.actor.id && row.adoptedAt !== undefined));
    expect(claims).toHaveLength(1);
    await f.mesh.publish({ topic: "absent.proof", from: identity("sender"), text: "ADOPTED_EVENT" });
    await wait(() => winner.messages(f.actor.id).some(message => message.direction === "out"));
    const messages = winner.messages(f.actor.id);
    expect(messages.filter(message => message.direction === "in" && JSON.stringify(message.data).includes("ADOPTED_EVENT"))).toHaveLength(1);
    expect(messages.filter(message => message.direction === "out")).toEqual([expect.objectContaining({ text: "fake worker complete", runId: expect.any(String) })]);
    expect(JSON.parse(fs.readFileSync(path.join(f.root, "actors", "actors.json"), "utf8")).actors).toHaveLength(1);
  });

  it.each(["fresh", "20-second-lapse"])("does not adopt with absent Main and %s resident file-only lease", async mode => {
    const f = await fixture();
    const hostId = residentHostId(oldRoot), expiry = Date.now() + (mode === "fresh" ? 15_000 : -20_000);
    writeHostLease(f.mesh.root, { id: hostId, rootId: oldRoot, identityId: hostId, updatedAt: expiry - 15_000, expiresAt: expiry });
    expect(f.directory.lineageAdoptable(oldRoot)).toBe(false);
    const next = await f.candidate("candidate");
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(next.owns(f.actor.id)).toBe(false); expect(next.status(f.actor.id).rootId).toBe(oldRoot);
  });

  it.each([["other-project", "project-agent"], [undefined, "worktree-agent"]] as const)("does not adopt for project %s role %s", async (project, role) => {
    const f = await fixture();
    const next = await f.candidate("ineligible", project ?? f.root, role);
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(next.owns(f.actor.id)).toBe(false); expect(next.status(f.actor.id).rootId).toBe(oldRoot);
  });

  it.each(["unknown", "throwing"] as const)("does not adopt with a %s adoption proof, even if routing reports death", async mode => {
    const f = await fixture();
    vi.spyOn(f.directory, "lineageAlive").mockReturnValue(false);
    vi.spyOn(f.directory, "lineageAdoptable").mockImplementation(() => {
      if (mode === "throwing") throw new Error("unreadable adoption proof");
      return undefined as unknown as boolean;
    });
    const next = await f.candidate("candidate");
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(next.owns(f.actor.id)).toBe(false);
    expect(next.status(f.actor.id).rootId).toBe(oldRoot);
  });

  it("vetoes renewal appearing only inside the registry + mesh locked recheck", async () => {
    const f = await fixture();
    const guard = f.directory.lineageAdoptable.bind(f.directory);
    let calls = 0;
    vi.spyOn(f.directory, "lineageAdoptable").mockImplementation(id => {
      if (id === oldRoot && ++calls === 2) {
        expect(fs.existsSync(path.join(f.root, "actors", "actors.json.lock", "owner"))).toBe(true);
        expect(fs.existsSync(path.join(f.mesh.root, ".lock", "owner"))).toBe(true);
        const hostId = residentHostId(oldRoot);
        writeHostLease(f.mesh.root, { id: hostId, rootId: oldRoot, identityId: hostId, updatedAt: Date.now(), expiresAt: Date.now() + 15_000 });
      }
      return guard(id);
    });
    const next = await f.candidate("locked-candidate");
    await wait(() => calls >= 2);
    expect(next.owns(f.actor.id)).toBe(false); expect(next.status(f.actor.id).rootId).toBe(oldRoot);
  });
});
