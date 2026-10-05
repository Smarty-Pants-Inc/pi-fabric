import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { FabricParticipantStaleError, readHostLease, writeHostLease } from "../src/topology/host-leases.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { residentHostId } from "../src/residency/protocol.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";

const cleanups: (() => Promise<unknown> | void)[] = [];
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); vi.restoreAllMocks(); });
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const oldRoot = "session:absent-main";
const identity = (id: string): MeshIdentity => ({ id, name: "main", kind: "main", sessionId: id.slice(8) });
const wait = async (predicate: () => boolean) => {
  const end = Date.now() + 10_000;
  while (!predicate()) { if (Date.now() > end) throw new Error("Adoption observation timed out"); await new Promise(resolve => setTimeout(resolve, 20)); }
};
const fixture = async (suspendedOwner = false, ownerOpinion?: () => boolean | undefined) => {
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "absent-adoption-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  cleanups.push(() => agents.close());
  const config = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const owner = new ActorManager("absent-main", identity(oldRoot), mesh, config, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true, rootId: oldRoot, project: root, role: "project-agent", claimResidency: "durable",
    ...(ownerOpinion ? { canManageActor: ownerOpinion } : {}),
  });
  cleanups.push(() => owner.close());
  const actor = await owner.create({ name: "retained", instructions: "Handle exactly once.", residency: "durable", topics: ["absent.proof"], responseMode: "text" });
  if (suspendedOwner) owner.pauseForRelease();
  else await owner.close();
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
  const candidateDirectories = new Map<string, ParticipantDirectory>();
  const candidate = async (name: string, project = root, role = "project-agent", beforeManager?: () => void) => {
    const rootId = "session:" + name, hostId = residentHostId(rootId);
    const hostIdentity: MeshIdentity = { id: hostId, kind: "agent", name: "resident" };
    const hostDirectory = new ParticipantDirectory(mesh, { enabled: true, hostId, rootId, identity: hostIdentity, reapDeadHosts: false });
    cleanups.push(() => hostDirectory.close());
    await hostDirectory.refresh();
    beforeManager?.();
    const manager = new ActorManager(name, hostIdentity, mesh, config, agents, ({ message }) => { if (message.text) deliveries.push(message.text); }, {
      actorRoot: path.join(root, "actors"), persistent: true, rootId, project, role, claimResidency: "durable", adoptionGraceMs: 0,
      canManageActor: id => { const participant = hostDirectory.get(id, Date.now(), { fresh: true }); return participant ? participant.ownerHostId === hostId : undefined; },
      lineageAlive: id => directory.lineageAlive(id),
      lineageAdoptable: id => directory.lineageAdoptable(id),
    });
    cleanups.push(() => manager.close());
    hostDirectory.registerSource(() => manager.listOwned().map(actor =>
      actorParticipantRecord(actor, rootId, hostId, hostIdentity.id, rootId)), manager.participantCustody);
    candidateDirectories.set(name, hostDirectory);
    return manager;
  };
  return { root, mesh, actor, directory, candidate, candidateDirectories, deliveries, agents, owner };
};

const retainActorFile = (f: Awaited<ReturnType<typeof fixture>>) => {
  const old = Date.now() - 20 * 60_000;
  writeParticipantFile(f.mesh.root, {
    key: key("topology/participants/", f.actor.id), version: 1, updatedBy: identity(oldRoot), updatedAt: old,
    value: { format: 1, id: f.actor.id, rootId: oldRoot, kind: "actor", name: "retained", status: "idle", runner: "pi",
      transport: "host", ownerHostId: oldRoot, ownerIdentityId: oldRoot, capabilities: ["fabric"], startedAt: old - 1000,
      updatedAt: old, ownershipFence: 1, controlProtocol: "v1" },
  });
};
type AdoptionFixture = Awaited<ReturnType<typeof fixture>>;
const participantPath = (f: AdoptionFixture) => path.join(f.mesh.root, "participants", key("topology/participants/", f.actor.id).slice("topology/participants/".length) + ".json");
const leasePath = (f: AdoptionFixture, id: string) => path.join(f.mesh.root, "host-leases", createHash("sha256").update(id).digest("hex").slice(0, 32) + ".json");
const rawTimestamp = (file: string, fields: string[], literal: string) => {
  const value = JSON.parse(fs.readFileSync(file, "utf8"));
  let cursor = value;
  for (const field of fields.slice(0, -1)) cursor = cursor[field];
  cursor[fields.at(-1)!] = "INVALID_TIME";
  fs.writeFileSync(file, JSON.stringify(value).replace('"INVALID_TIME"', literal));
};
const fileEvidenceVeto = async (stage: "initial" | "locked", damage: (f: AdoptionFixture) => void) => {
  const f = await fixture();
  await f.mesh.delete({ key: "actors/absent-main/" + f.actor.id });
  retainActorFile(f); // The only retained actor history is a file, not shared state.
  expect(f.directory.lineageAdoptable(oldRoot)).toBe(true);
  const writes = vi.spyOn(ActorRegistryStore.prototype, "write");
  const guard = f.directory.lineageAdoptable.bind(f.directory);
  let calls = 0;
  if (stage === "initial") damage(f);
  else vi.spyOn(f.directory, "lineageAdoptable").mockImplementation(id => {
    if (id === oldRoot && ++calls === 2) {
      expect(fs.existsSync(path.join(f.root, "actors", "actors.json.lock", "owner"))).toBe(true);
      expect(fs.existsSync(path.join(f.mesh.root, ".lock", "owner"))).toBe(true);
      damage(f);
    }
    return guard(id);
  });
  const next = await f.candidate("file-evidence-candidate");
  if (stage === "locked") await wait(() => calls >= 2);
  else await new Promise(resolve => setTimeout(resolve, 60));
  expect(f.directory.lineageAdoptable(oldRoot)).toBe(false);
  expect(next.owns(f.actor.id)).toBe(false);
  expect(next.status(f.actor.id).rootId).toBe(oldRoot);
  expect(writes.mock.calls.flatMap(([rows]) => rows).filter(row => row.id === f.actor.id && row.adoptedAt !== undefined)).toHaveLength(0);
  expect(new ActorRegistryStore(path.join(f.root, "actors")).records().find(row => row.id === f.actor.id)?.rootId).toBe(oldRoot);
  await next.close();
};
const damagedStates = ["{truncated", "", " \n\t", "{}", '{"format":1,"entries":{"hidden":{"key":"other"}}}',
  '{"format":1,"entries":{}}\n{"format":1,"entries":{}}'] as const;

describe("F3059 aged absent-root adoption", () => {
  it("keeps an adopted root and its durable actor retryable inside routing lease grace without transferring custody again", async () => {
    const f = await fixture();
    const next = await f.candidate("grace-successor");
    await wait(() => next.owns(f.actor.id));
    next.pauseForRelease();
    const rootId = "session:grace-successor", hostId = residentHostId(rootId);
    const store = new ActorRegistryStore(path.join(f.root, "actors"));
    const committed = store.records().find(row => row.id === f.actor.id)!;
    expect(committed).toMatchObject({ rootId, ownershipToken: expect.any(String), adoptedAt: expect.any(Number) });
    const actorPresence = actorParticipantRecord(next.status(f.actor.id), rootId, hostId, hostId, rootId);
    const main = new ParticipantDirectory(f.mesh, { enabled: true, hostId: rootId, rootId, identity: identity(rootId), reapDeadHosts: false });
    const resident = new ParticipantDirectory(f.mesh, { enabled: true, hostId, rootId, identity: next.identity, reapDeadHosts: false });
    cleanups.push(() => main.close(), () => resident.close());
    main.registerSource(() => [{ ...actorPresence, id: rootId, kind: "root", ownerHostId: rootId, ownerIdentityId: rootId }]);
    resident.registerSource(() => [actorPresence]);
    await main.refresh(); await resident.refresh();
    // Advance beyond both published host/session TTLs; a fabricated older file
    // must not override the fresher shared-state lease.
    let now = Date.now() + 120_000;
    f.directory.options.routingLease = { now: () => now, graceMs: 45_000, waitMs: 30, pollMs: 10,
      lockWaiting: () => true, sleep: async ms => { now += ms; } };
    const leases = [rootId, hostId].map(id => readHostLease(f.mesh.root, id)!);
    for (const lease of leases) writeHostLease(f.mesh.root, { ...lease, updatedAt: now - 17_000, expiresAt: now - 2_000 });
    for (const target of [rootId, f.actor.id]) {
      expect(f.directory.get(target, now, { fresh: true })).toBeUndefined();
      expect(f.directory.retainedRouteAllowed(target)).toBe(true);
      const error = await f.directory.resolveRoutingLease(target).catch(error => error);
      expect(error).toBeInstanceOf(FabricParticipantStaleError);
      expect(error).toMatchObject({ targetId: target, code: "FABRIC_PARTICIPANT_STALE", retryable: true });
      expect(error.message).not.toContain("Unknown");
    }
    expect(f.directory.lineageAdoptable(rootId, now)).toBe(false);
    expect(store.records().find(row => row.id === f.actor.id)).toEqual(committed);
    f.directory.options.routingLease.sleep = async ms => {
      now += ms;
      for (const lease of leases) writeHostLease(f.mesh.root, { ...lease, updatedAt: now, expiresAt: now + 60_000 });
    };
    for (const target of [rootId, f.actor.id]) {
      expect(await f.directory.resolveRoutingLease(target)).toBe(true);
      expect(f.directory.get(target, now, { fresh: true })).toMatchObject({ rootId, stale: false,
        ownerHostId: target === rootId ? rootId : hostId });
    }
    expect(store.records().find(row => row.id === f.actor.id)).toEqual(committed);
    expect(f.directory.get(oldRoot, now, { fresh: true })).toBeUndefined();
  });
  it.each(["missing", "stale-positive"])("fences an existing suspended predecessor with %s directory opinion before the adopter publishes its participant", async mode => {
    // Stop only A's polling, not its manager: its original actor and cached ownership
    // survive the entire absence. B starts normally after the spy is restored.
    let dispatch!: ActorMeshMonitor["callbacks"]["onEvent"];
    const start = vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) {
      dispatch = this.callbacks.onEvent;
    });
    let opinion: boolean | undefined;
    const f = await fixture(true, mode === "stale-positive" ? () => opinion : undefined);
    start.mockRestore();
    expect(f.owner.owns(f.actor.id)).toBe(true);
    const next = await f.candidate("suspended-successor");
    await wait(() => next.owns(f.actor.id));
    const store = new ActorRegistryStore(path.join(f.root, "actors"));
    const committed = store.records().find(row => row.id === f.actor.id)!;
    expect(committed.rootId).toBe("session:suspended-successor");
    expect(committed.ownershipToken).toEqual(expect.any(String));
    if (mode === "stale-positive") opinion = true; // stale A advertisement cannot overrule committed custody
    // No successor actor participant yet: directory fallback must not revive A.
    expect(f.directory.get(f.actor.id, Date.now(), { fresh: true })).toBeUndefined();
    // A locked save before any list/status reload used to restore the stale A row.
    await f.owner.checkpointForRelease();
    expect(store.records().find(row => row.id === f.actor.id)).toEqual(committed);
    f.owner.resumeAfterRelease();
    expect(f.owner.listOwned()).toEqual([]);
    expect(f.owner.owns(f.actor.id)).toBe(false);
    expect(() => f.owner.tell(f.actor.id, "STALE_DIRECT")).toThrow("owned by another host");
    await expect(f.owner.setInstructions(f.actor.id, "STALE_WRITE")).rejects.toThrow("owned by another host");
    const event = await f.mesh.publish({ topic: "absent.proof", from: identity("sender"), text: "RESUMED_EVENT" });
    expect(dispatch(event)).toBe("ignored");
    await wait(() => next.messages(f.actor.id).some(message => message.direction === "out"));
    // Passive message listings may mirror B's shared registry; dispatch's ignored
    // result and the unchanged A presence, not that listing, prove no A consumption.
    expect(next.messages(f.actor.id).filter(message => message.direction === "in" &&
      JSON.stringify(message.data).includes("RESUMED_EVENT"))).toHaveLength(1);
    expect(f.mesh.get("actors/absent-main/" + f.actor.id)?.updatedAt).toBeLessThan(Date.now() - 10 * 60_000);
    await f.owner.close();
    const retained = store.records().find(row => row.id === f.actor.id)!;
    expect(retained).toMatchObject({ rootId: committed.rootId, ownershipToken: committed.ownershipToken, adoptedAt: committed.adoptedAt });
    expect(retained.adoptedFrom).toEqual(committed.adoptedFrom);
  });

  it("rejects a predecessor registry/presence save that resumes custody acquisition after the claim", async () => {
    const start = vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    const f = await fixture(true);
    start.mockRestore();
    const lock = ActorRegistryStore.prototype.withLock;
    let entered = false, release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const custody = vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(async function <T>(this: ActorRegistryStore, operation: () => T | Promise<T>): Promise<T> {
      if (!entered) { entered = true; await gate; }
      return lock.call(this, operation) as Promise<T>;
    });
    // Admitted while A still owns the row; suspend before taking registry custody.
    const staleSave = f.owner.setInstructions(f.actor.id, "STALE_PREDECESSOR_INSTRUCTIONS");
    await wait(() => entered);
    try {
      const next = await f.candidate("custody-successor");
      await wait(() => next.owns(f.actor.id));
      const store = new ActorRegistryStore(path.join(f.root, "actors"));
      const committed = store.records().find(row => row.id === f.actor.id)!;
      expect(committed.instructions).not.toBe("STALE_PREDECESSOR_INSTRUCTIONS");
      release();
      await staleSave;
      expect(store.records().find(row => row.id === f.actor.id)).toEqual(committed);
      expect(f.owner.listOwned()).toEqual([]);
      expect(f.mesh.get("actors/absent-main/" + f.actor.id)?.updatedAt).toBeLessThan(Date.now() - 10 * 60_000);
    } finally { release(); await staleSave; custody.mockRestore(); }
  });

  it.each(["files", "shared", "delayed-copy", "retained-file"] as const)("fences the real captured participant refresh on the %s publication path and delivers to B", async mode => {
    let predecessorDispatch!: ActorMeshMonitor["callbacks"]["onEvent"];
    const start = vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) {
      predecessorDispatch = this.callbacks.onEvent;
    });
    const f = await fixture(true);
    start.mockRestore();
    if (mode === "files") await f.mesh.put({ key: "topology/liveness", identity: identity("observer"),
      value: { version: 1, hostLeases: "files", participants: "files" } });
    if (mode === "retained-file") retainActorFile(f);
    const a = new ParticipantDirectory(f.mesh, { enabled: true, hostId: oldRoot, rootId: oldRoot,
      identity: identity(oldRoot), reapDeadHosts: false });
    cleanups.push(() => a.close());
    let captured = false;
    // Exactly the production ResidentHost/Main source, not the legacy actors/ presence writer.
    a.registerSource(() => {
      const records = f.owner.listOwned().map(actor => actorParticipantRecord(actor, oldRoot, oldRoot, oldRoot, oldRoot));
      captured = records.length === 1;
      return records;
    }, f.owner.participantCustody);
    const lock = f.owner.participantCustody.withLock.bind(f.owner.participantCustody);
    let entered = false, calls = 0, release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const custody = vi.spyOn(f.owner.participantCustody, "withLock").mockImplementation(async operation => {
      if (++calls === (mode === "delayed-copy" ? 2 : 1)) { entered = true; await gate; }
      return lock(operation);
    });
    const refresh = a.refresh();
    try {
      await wait(() => entered);
      expect(captured).toBe(true);
      // A is suspended: reaped advertisements/lease and aged retained actor history.
      fs.rmSync(leasePath(f, oldRoot), { force: true });
      await f.mesh.delete({ key: key("topology/hosts/", oldRoot) });
      await f.mesh.delete({ key: key("topology/participants/", f.actor.id) });
      const next = await f.candidate("real-refresh-successor");
      await wait(() => next.owns(f.actor.id));
      expect(f.directory.get(f.actor.id, Date.now(), { fresh: true })).toBeUndefined();
      release();
      await refresh;
      if (mode === "retained-file") {
        // Its host lease revives the retained old file, not an actor write. Both actual
        // owner directories still reject that obsolete registry generation.
        expect(f.directory.get(f.actor.id, Date.now(), { fresh: true })?.rootId).toBe(oldRoot);
        expect(a.get(f.actor.id, Date.now(), { fresh: true })).toBeUndefined();
      } else expect(f.directory.get(f.actor.id, Date.now(), { fresh: true })).toBeUndefined();
      const event = await f.mesh.publish({ topic: "absent.proof", from: identity("sender"), text: "CAPTURED_REFRESH_EVENT" });
      expect(predecessorDispatch(event)).toBe("ignored");
      await wait(() => next.messages(f.actor.id).some(message => message.direction === "out"));
      expect(next.messages(f.actor.id).filter(message => message.direction === "in" &&
        JSON.stringify(message.data).includes("CAPTURED_REFRESH_EVENT"))).toHaveLength(1);
      expect(next.owns(f.actor.id)).toBe(true);
      expect(f.owner.owns(f.actor.id)).toBe(false);
    } finally { release(); await refresh; custody.mockRestore(); }
  });

  it.each(["retained-file", "retained-shared", "files-only"] as const)("replaces a revived predecessor advertisement on %s and routes passive Main messages exactly once", async mode => {
    const start = vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    const f = await fixture(true);
    start.mockRestore();
    if (mode === "files-only") await f.mesh.put({ key: "topology/liveness", identity: identity("observer"),
      value: { version: 1, hostLeases: "files", participants: "files" } });
    const a = new ParticipantDirectory(f.mesh, { enabled: true, hostId: oldRoot, rootId: oldRoot,
      identity: identity(oldRoot), reapDeadHosts: false });
    cleanups.push(() => a.close());
    const captured = f.owner.listOwned().map(actor => actorParticipantRecord(actor, oldRoot, oldRoot, oldRoot, oldRoot));
    let includeActor = true;
    a.registerSource(() => includeActor ? captured : [], f.owner.participantCustody);
    await a.refresh();
    const participantKey = key("topology/participants/", f.actor.id);
    const old = Date.now() - 20 * 60_000;
    const retained = { ...captured[0], ownershipFence: 1 as const, startedAt: old - 1_000, updatedAt: old };
    const clock = vi.spyOn(Date, "now").mockReturnValue(old);
    try {
      if (mode === "retained-shared") {
        await f.mesh.put({ key: participantKey, identity: identity(oldRoot), value: retained });
        fs.rmSync(participantPath(f), { force: true });
      } else {
        await f.mesh.delete({ key: participantKey });
        writeParticipantFile(f.mesh.root, { key: participantKey, value: retained, version: 1,
          updatedAt: old, updatedBy: identity(oldRoot) });
      }
    } finally { clock.mockRestore(); }
    fs.rmSync(leasePath(f, oldRoot), { force: true });
    await f.mesh.delete({ key: key("topology/hosts/", oldRoot) });
    expect(f.directory.lineageAdoptable(oldRoot)).toBe(true);
    const next = await f.candidate("addressable-successor");
    const b = f.candidateDirectories.get("addressable-successor")!;
    await wait(() => next.owns(f.actor.id));
    const committed = new ActorRegistryStore(path.join(f.root, "actors")).records().find(row => row.id === f.actor.id)!;
    // A resumes first. Its host is live, but the retained advertisement is obsolete.
    await a.refresh();
    expect(readHostLease(f.mesh.root, oldRoot)!.expiresAt).toBeGreaterThan(Date.now());
    expect(b.get(f.actor.id, Date.now(), { fresh: true })).toBeUndefined();
    await b.refresh(); // B's actual listOwned() source must replace A, not merely hide it.
    const successor = { rootId: committed.rootId, ownershipToken: committed.ownershipToken,
      ownerHostId: b.options.hostId, ownershipFence: 1, controlProtocol: "v1", stale: false };
    const published = () => {
      expect(b.get(f.actor.id, Date.now(), { fresh: true })).toMatchObject(successor);
      expect(JSON.parse(fs.readFileSync(participantPath(f), "utf8")).value).toMatchObject({
        rootId: committed.rootId, ownershipToken: committed.ownershipToken, ownerHostId: b.options.hostId,
      });
      if (mode !== "files-only") expect(f.mesh.get(participantKey, { fresh: true })?.value).toMatchObject({
        rootId: committed.rootId, ownershipToken: committed.ownershipToken, ownerHostId: b.options.hostId,
      });
    };
    published();
    for (let heartbeat = 0; heartbeat < 3; heartbeat++) { await a.refresh(); await b.refresh(); published(); }

    const passiveIdentity = identity("session:passive-main");
    const passiveDirectory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: passiveIdentity.id,
      rootId: passiveIdentity.id, identity: passiveIdentity, reapDeadHosts: false });
    cleanups.push(() => passiveDirectory.close());
    const passive = new ActorManager("passive-main", passiveIdentity, f.mesh, DEFAULT_FABRIC_CONFIG.mesh, f.agents, () => {}, {
      actorRoot: path.join(f.root, "actors"), persistent: true, rootId: passiveIdentity.id,
      project: f.root, role: "project-agent", canManageActor: () => false,
    });
    cleanups.push(() => passive.close());
    passiveDirectory.registerSource(() => [], passive.participantCustody);
    await passiveDirectory.refresh();
    expect(passive.owns(f.actor.id)).toBe(false);
    const main = (id: string, local: boolean) => ({ id, local, matches: (target: string) => target === id,
      deliverAgent: () => { throw new Error("Unexpected Main delivery"); } });
    const plane = (who: MeshIdentity, hostId: string) => {
      const control = new FabricControlPlane(new MeshStore(f.mesh.root, 64 * 1024, 100), who,
        { enabled: true, hostId, pollMs: 20, acknowledgementTimeoutMs: 2_000 });
      cleanups.push(() => control.close());
      return control;
    };
    const ownerPlane = plane(next.identity, b.options.hostId);
    const passivePlane = plane(passiveIdentity, passiveIdentity.id);
    const receive = new AgentMessageRouter(f.agents, next, main(b.options.rootId, false), b, ownerPlane, binding => binding);
    const send = new AgentMessageRouter(f.agents, passive, main(passiveIdentity.id, true), passiveDirectory, passivePlane, binding => binding);
    const accept = vi.fn(receive.acceptControl.bind(receive));
    ownerPlane.start(accept);
    passivePlane.start(() => ({ accepted: false }));
    const tell = vi.spyOn(next, "tell");
    const run = vi.spyOn(f.agents, "run");
    for (const kind of ["followUp", "steer"] as const) {
      await a.refresh(); // A continues renewing its host lease during public routing.
      const text = `PASSIVE_${kind}_${mode}`;
      const outputsBefore = next.messages(f.actor.id).filter(message => message.direction === "out" && !message.error).length;
      await expect(send.routeMessage(f.actor.id, text, undefined, kind)).resolves.toMatchObject({ acknowledged: true, routed: "mesh" });
      await wait(() => next.messages(f.actor.id).filter(message => message.direction === "out" && !message.error).length === outputsBefore + 1);
      await wait(() => next.status(f.actor.id).status === "idle" && next.status(f.actor.id).queued === 0);
      expect(next.messages(f.actor.id).filter(message => message.direction === "in" && (message.data as { message?: string })?.message === text)).toHaveLength(1);
      expect(readHostLease(f.mesh.root, oldRoot)!.expiresAt).toBeGreaterThan(Date.now());
    }
    expect(accept).toHaveBeenCalledTimes(2);
    expect(tell).toHaveBeenCalledTimes(2);
    expect(run).toHaveBeenCalledTimes(2);
    expect(f.owner.owns(f.actor.id)).toBe(false);
    expect(() => f.owner.tell(f.actor.id, "STALE_DIRECT")).toThrow("owned by another host");
    includeActor = false;
    await a.refresh(); // A's cleanup and close must not delete B's published record.
    published();
    await a.close();
    published();
    expect(new ActorRegistryStore(path.join(f.root, "actors")).records().find(row => row.id === f.actor.id))
      .toMatchObject({ rootId: committed.rootId, ownershipToken: committed.ownershipToken });
  });

  it("drops a captured legacy presence put that resumes after adoption without blocking registry custody", async () => {
    const start = vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    const f = await fixture(true);
    start.mockRestore();
    const put = f.mesh.put.bind(f.mesh);
    let entered = false, release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    vi.spyOn(f.mesh, "put").mockImplementation(async input => {
      if (!entered && input.key === "actors/absent-main/" + f.actor.id) { entered = true; await gate; }
      return put(input);
    });
    const stale = f.owner.setInstructions(f.actor.id, "CAPTURED_BEFORE_ADOPTION");
    try {
      await wait(() => entered);
      const next = await f.candidate("presence-successor");
      await wait(() => next.owns(f.actor.id));
      const before = f.mesh.get("actors/absent-main/" + f.actor.id, { fresh: true });
      release();
      await stale;
      expect(f.mesh.get("actors/absent-main/" + f.actor.id, { fresh: true })).toEqual(before);
      expect(next.owns(f.actor.id)).toBe(true);
      expect(f.owner.owns(f.actor.id)).toBe(false);
    } finally { release(); await stale; }
  });

  it.each(["refresh", "close"] as const)("fences %s cleanup against a newer same-host ABA ownership token", async operation => {
    const start = vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    const f = await fixture(true);
    start.mockRestore();
    const a = new ParticipantDirectory(f.mesh, { enabled: true, hostId: oldRoot, rootId: oldRoot,
      identity: identity(oldRoot), reapDeadHosts: false });
    cleanups.push(() => a.close());
    const captured = f.owner.listOwned().map(actor => actorParticipantRecord(actor, oldRoot, oldRoot, oldRoot, oldRoot));
    let removed = false;
    a.registerSource(() => removed ? [] : captured, f.owner.participantCustody);
    await a.refresh();
    const store = new ActorRegistryStore(path.join(f.root, "actors"));
    await store.withLock(() => store.write(store.records().map(row => ({ ...row, ownershipToken: "successor-ABA-token" }))));
    const successor = { ...actorParticipantRecord(f.actor, oldRoot, oldRoot, oldRoot, oldRoot),
      ownershipToken: "successor-ABA-token", ownershipFence: 1 as const };
    const participantKey = key("topology/participants/", f.actor.id);
    const committed = await f.mesh.put({ key: participantKey, identity: identity(oldRoot), value: successor });
    writeParticipantFile(f.mesh.root, committed);
    const fileBefore = fs.readFileSync(participantPath(f), "utf8");
    removed = true;
    if (operation === "refresh") await a.refresh();
    else await a.close();
    expect(f.mesh.get(participantKey)).toEqual(committed);
    expect(fs.readFileSync(participantPath(f), "utf8")).toBe(fileBefore);
  });

  it.each(["legacy-presence", "modern-presence", "registry"] as const)("fails closed for unfenced prior-release %s even when all predecessor leases are absent", async mode => {
    const f = await fixture();
    const store = new ActorRegistryStore(path.join(f.root, "actors"));
    if (mode === "registry") await store.withLock(() => store.write(store.records().map(row => {
      const { ownershipFence: _fence, ...old } = row;
      return old;
    })));
    else if (mode === "modern-presence") {
      retainActorFile(f);
      const file = participantPath(f), entry = JSON.parse(fs.readFileSync(file, "utf8"));
      delete entry.value.ownershipFence;
      fs.writeFileSync(file, JSON.stringify(entry));
    } else {
      const entry = f.mesh.get("actors/absent-main/" + f.actor.id)!;
      const { ownershipFence: _fence, ...old } = entry.value as Record<string, unknown>;
      const clock = vi.spyOn(Date, "now").mockReturnValue(entry.updatedAt);
      try { await f.mesh.put({ key: entry.key, identity: identity(oldRoot), value: old }); }
      finally { clock.mockRestore(); }
    }
    const next = await f.candidate("unfenced-successor");
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(next.owns(f.actor.id)).toBe(false);
    expect(store.records().find(row => row.id === f.actor.id)).toMatchObject({ rootId: oldRoot });
    expect(store.records().find(row => row.id === f.actor.id)?.adoptedAt).toBeUndefined();
  });

  it.each(["matching", "mismatched"] as const)("validates a %s native session lease produced by the real Main writer", async mode => {
    const f = await fixture();
    const old = Date.now() - 20 * 60_000;
    const clock = vi.spyOn(Date, "now").mockReturnValue(old);
    const main = new ParticipantDirectory(f.mesh, { enabled: true, hostId: oldRoot, rootId: oldRoot,
      identity: identity(oldRoot), reapDeadHosts: false });
    cleanups.push(() => main.close());
    main.registerSource(() => [{ format: 1, id: oldRoot, rootId: oldRoot, kind: "root", name: "main", status: "idle",
      runner: "pi", transport: "host", ownerHostId: oldRoot, ownerIdentityId: oldRoot, capabilities: ["fabric"],
      sessionId: mode === "matching" ? oldRoot.slice(8) : "different-native-session", startedAt: old - 1000,
      updatedAt: old, controlProtocol: "v1" }]);
    try { await main.refresh(); }
    finally { clock.mockRestore(); }
    const lease = readHostLease(f.mesh.root, oldRoot)!;
    expect(lease.session?.id).toBe(mode === "matching" ? oldRoot.slice(8) : "different-native-session");
    // Reaper interrupted after advertisements, before lease cleanup.
    await f.mesh.delete({ key: key("topology/participants/", oldRoot) });
    await f.mesh.delete({ key: key("topology/hosts/", oldRoot) });
    await f.mesh.delete({ key: "sessions/absent-main" });
    fs.rmSync(path.join(f.mesh.root, "participants", key("topology/participants/", oldRoot).slice("topology/participants/".length) + ".json"), { force: true });
    expect(f.directory.lineageAdoptable(oldRoot)).toBe(mode === "matching");
    const next = await f.candidate("native-lease-successor");
    if (mode === "matching") await wait(() => next.owns(f.actor.id));
    else { await new Promise(resolve => setTimeout(resolve, 100)); expect(next.owns(f.actor.id)).toBe(false); }
  });

  it("preserves adopted custody and checkpoint-only history through a registry checkpoint and reload", async () => {
    const f = await fixture();
    const actorRoot = path.join(f.root, "actors"), store = new ActorRegistryStore(actorRoot);
    const history = [{ id: "before-adoption", source: "direct", direction: "in", createdAt: 1, text: "Retain across adoption." }];
    await store.withLock(() => store.write(store.records().map(row => ({ ...row, messages: history }))));
    const registry = path.join(actorRoot, "actors.json");
    const stripHistoryReference = () => {
      // A legacy owned-row save drops the selecting field, leaving only #486's checkpoint.
      const saved = JSON.parse(fs.readFileSync(registry, "utf8"));
      for (const row of saved.actors) delete row.messageHistory;
      fs.writeFileSync(registry, JSON.stringify(saved));
    };
    stripHistoryReference();
    const archivePath = path.join(actorRoot, f.actor.id, "registry", "messages.jsonl");
    const headPath = path.join(actorRoot, f.actor.id, "registry", "messages-head.json");
    const archive = fs.readFileSync(archivePath, "utf8"), historyHead = fs.readFileSync(headPath, "utf8");
    const next = await f.candidate("checkpoint-successor");
    await wait(() => next.owns(f.actor.id));
    const adopted = store.records().find(row => row.id === f.actor.id)!;
    expect(adopted).toMatchObject({ rootId: "session:checkpoint-successor", ownershipToken: expect.any(String), adoptedAt: expect.any(Number) });
    next.pauseForRelease();
    await next.checkpointForRelease();
    await next.close();
    expect(store.records().find(row => row.id === f.actor.id)).toMatchObject({
      rootId: adopted.rootId, ownershipToken: adopted.ownershipToken, adoptedAt: adopted.adoptedAt,
    });
    expect(store.records().find(row => row.id === f.actor.id)?.adoptedFrom).toEqual(adopted.adoptedFrom);
    expect(fs.readFileSync(headPath, "utf8")).toBe(historyHead);
    stripHistoryReference();
    const reloaded = await f.candidate("checkpoint-successor");
    expect(reloaded.owns(f.actor.id)).toBe(true);
    expect(reloaded.status(f.actor.id).rootId).toBe(adopted.rootId);
    expect(reloaded.messages(f.actor.id)).toEqual(history);
    reloaded.pauseForRelease();
    await reloaded.checkpointForRelease();
    expect(store.records().find(row => row.id === f.actor.id)).toMatchObject({
      rootId: adopted.rootId, ownershipToken: adopted.ownershipToken, adoptedAt: adopted.adoptedAt,
    });
    expect(fs.readFileSync(archivePath, "utf8")).toBe(archive);
    expect(fs.readFileSync(headPath, "utf8")).toBe(historyHead);
  });

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

  describe.each(["initial", "locked"] as const)("strict file evidence in the %s check", stage => {
    it.each([oldRoot, residentHostId(oldRoot)].flatMap(hostId =>
      ["wrong-root", "wrong-identity", "wrong-filename"].map(mode => [hostId, mode] as const)))
      ("vetoes canonical lease %s with %s attribution", async (hostId, mode) => {
        await fileEvidenceVeto(stage, f => {
          const old = Date.now() - 20 * 60_000;
          writeHostLease(f.mesh.root, { id: hostId, rootId: mode === "wrong-root" ? "session:other" : oldRoot,
            identityId: mode === "wrong-identity" ? "session:other" : hostId, updatedAt: old, expiresAt: old + 15_000 });
          if (mode === "wrong-filename") {
            const file = leasePath(f, hostId);
            const value = JSON.parse(fs.readFileSync(file, "utf8"));
            value.id = "session:other";
            fs.writeFileSync(file, JSON.stringify(value));
          }
        });
      });

    it.each(["entry", "value", "startedAt"].flatMap(field =>
      ["-1e999", "1e999", "NaN", "999999999999999"].map(time => [field, time] as const)))
      ("vetoes participant %s timestamp %s independently of its finite sibling", async (field, time) => {
        await fileEvidenceVeto(stage, f => rawTimestamp(participantPath(f),
          field === "entry" ? ["updatedAt"] : ["value", field === "value" ? "updatedAt" : "startedAt"], time));
      });

    it.each(["updatedAt", "expiresAt", "startedAt", "session.updatedAt"].flatMap(field =>
      ["-1e999", "1e999", "NaN", "999999999999999"].map(time => [field, time] as const)))
      ("vetoes host lease %s timestamp %s", async (field, time) => {
        await fileEvidenceVeto(stage, f => {
          const old = Date.now() - 20 * 60_000, hostId = residentHostId(oldRoot);
          writeHostLease(f.mesh.root, { id: hostId, rootId: oldRoot, identityId: hostId, startedAt: old - 1000,
            updatedAt: old, expiresAt: old + 15_000,
            session: { id: oldRoot.slice(8), startedAt: old - 1000, updatedAt: old, expiresAt: old + 15_000 } });
          rawTimestamp(leasePath(f, hostId), field.split("."), time);
        });
      });
  });

  it.each(damagedStates)("vetoes damaged shared state %j despite retained old actor-file evidence", async damaged => {
    const f = await fixture();
    retainActorFile(f);
    expect(f.directory.lineageAdoptable(oldRoot)).toBe(true);
    const state = path.join(f.mesh.root, "state.json");
    let original = "";
    const writes = vi.spyOn(ActorRegistryStore.prototype, "write");
    const next = await f.candidate("damaged-candidate", f.root, "project-agent", () => {
      original = fs.readFileSync(state, "utf8");
      fs.writeFileSync(state, damaged);
    });
    try {
      expect(f.directory.lineageAlive(oldRoot)).toBe(true);
      expect(f.directory.lineageAdoptable(oldRoot)).toBe(false);
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(next.owns(f.actor.id)).toBe(false);
      expect(next.status(f.actor.id).rootId).toBe(oldRoot);
      expect(writes.mock.calls.flatMap(([rows]) => rows).filter(row => row.id === f.actor.id && row.adoptedAt !== undefined)).toHaveLength(0);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "actors", "actors.json"), "utf8")).actors.find((row: { id: string }) => row.id === f.actor.id).rootId).toBe(oldRoot);
    } finally { await next.close(); fs.writeFileSync(state, original); }
  });

  it.each(["{truncated", "", " \n\t"])("vetoes state damage %j first appearing under registry + mesh custody", async damaged => {
    const f = await fixture();
    retainActorFile(f);
    const state = path.join(f.mesh.root, "state.json");
    let original = "", calls = 0;
    const guard = f.directory.lineageAdoptable.bind(f.directory);
    const writes = vi.spyOn(ActorRegistryStore.prototype, "write");
    vi.spyOn(f.directory, "lineageAdoptable").mockImplementation(id => {
      if (id === oldRoot && ++calls === 2) {
        expect(fs.existsSync(path.join(f.root, "actors", "actors.json.lock", "owner"))).toBe(true);
        expect(fs.existsSync(path.join(f.mesh.root, ".lock", "owner"))).toBe(true);
        original = fs.readFileSync(state, "utf8");
        fs.writeFileSync(state, damaged);
      }
      return guard(id);
    });
    const next = await f.candidate("locked-damage-candidate");
    try {
      await wait(() => calls >= 2);
      expect(f.directory.lineageAdoptable(oldRoot)).toBe(false);
      expect(next.owns(f.actor.id)).toBe(false);
      expect(next.status(f.actor.id).rootId).toBe(oldRoot);
      expect(writes.mock.calls.flatMap(([rows]) => rows).filter(row => row.id === f.actor.id && row.adoptedAt !== undefined)).toHaveLength(0);
    } finally { await next.close(); if (original) fs.writeFileSync(state, original); }
  });

  it.each([oldRoot, residentHostId(oldRoot)].flatMap(hostId => ["missing", "wrong-id", "wrong-root"].map(mode => [hostId, mode] as const)))
    ("vetoes %s canonical host with %s attribution before filtering relevance", async (hostId, mode) => {
      const f = await fixture();
      const old = Date.now() - 20 * 60_000;
      const clock = vi.spyOn(Date, "now").mockReturnValue(old);
      try {
        await f.mesh.put({ key: key("topology/hosts/", hostId), identity: identity(hostId), value: mode === "missing" ? { format: 1 } : {
          format: 1, id: mode === "wrong-id" ? "session:other" : hostId, rootId: "session:other", identity: identity(hostId),
          startedAt: old - 1000, updatedAt: old, expiresAt: old + 15_000,
        } });
      } finally { clock.mockRestore(); }
      expect(f.directory.lineageAdoptable(oldRoot)).toBe(false);
      const next = await f.candidate("bad-host-candidate");
      await new Promise(resolve => setTimeout(resolve, 100));
      expect(next.owns(f.actor.id)).toBe(false);
      expect(next.status(f.actor.id).rootId).toBe(oldRoot);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "actors", "actors.json"), "utf8")).actors.find((row: { id: string }) => row.id === f.actor.id).rootId).toBe(oldRoot);
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
