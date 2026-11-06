import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { SessionActorOrphans, SESSION_ACTOR_ORPHAN_GRACE_MS } from "../src/actors/session-orphans.js";
import type { FabricActorStorageScope } from "../src/actors/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { registerMainInbox } from "../src/topology/stall-alarms.js";
import type { FabricState } from "../src/fabric-state.js";
import { createDashboardSnapshot } from "../src/ui/snapshot.js";

// Keep reason: data safety across the foreign-registry/ownership trust boundary.
// Real registries and reconciliation are used; no worker needs to run.
const fixtures: Array<{ root: string; directories: ActorDirectory[]; participants: ParticipantDirectory[]; manager: AgentManager }> = [];
afterEach(async () => {
  for (const { root, directories, participants, manager } of fixtures.splice(0)) {
    for (const participant of participants) await participant.close();
    for (const directory of directories) await directory.close();
    await manager.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
  vi.restoreAllMocks();
});

const open = (canManageActor?: (id: string, fresh?: boolean) => boolean | undefined) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-list-readonly-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = { id: "session:current", name: "Main", kind: "main", sessionId: "current" };
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actorRoots = { project: path.join(mesh.root, "actors"), session: path.join(mesh.root, "actors", "current") };
  const directories: ActorDirectory[] = [], participants: ParticipantDirectory[] = [];
  const newDirectory = () => {
    const directory = new ActorDirectory([
      "current", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, manager, () => {},
      { persistent: true, rootId: identity.id, lineageAlive: () => true, ...(canManageActor ? { canManageActor } : {}) },
    ], actorRoots, "project");
    directories.push(directory);
    return directory;
  };
  const directory = newDirectory();
  fixtures.push({ root, directories, participants, manager });
  return { directory, manager, mesh, actorRoots, identity, participants, newDirectory };
};

// Match the existing orphan-provider fixture: observe ESRCH rather than assuming
// an arbitrary PID is dead, without starting a child or mocking death authority.
const absentPid = (): number => {
  for (let pid = 2_147_483_647; pid > 2_147_483_627; pid--) {
    try { process.kill(pid, 0); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return pid; }
  }
  throw new Error("Could not obtain a provably absent PID for the orphan lease fixture");
};

const row = (id: string, name: string, rootId = "session:current") => ({
  id, name, rootId, residency: "session", status: "idle", runner: "pi", instructions: "Observe only.",
  events: [], topics: [], messages: [], createdAt: Date.now() - 1_000, updatedAt: Date.now(),
});

const seedForeign = (mesh: MeshStore, sessionId = "old", id = "a".repeat(32), stopped = false) => {
  const actorRoot = path.join(mesh.root, "actors", sessionId);
  const store = new ActorRegistryStore(actorRoot);
  const at = Date.now() - SESSION_ACTOR_ORPHAN_GRACE_MS - 10_000;
  const actor = { ...row(id, `${sessionId}-supervisor`, `session:${sessionId}`),
    status: stopped ? "stopped" : "running", createdAt: at - 1_000, updatedAt: at,
    ...(stopped ? { stopReason: "host-exit", sessionOrphan: { oldRoot: `session:${sessionId}`,
      oldHost: os.hostname(), reason: "Main owner process is dead", lastUpdated: at,
      orphanedAt: at + 1_000, alarmPublishedAt: at + 1_000 } } : {}),
  };
  store.write([actor]);
  writeHostLease(mesh.root, { id: actor.rootId, rootId: actor.rootId, identityId: actor.rootId,
    updatedAt: at, expiresAt: at,
    writer: { pid: absentPid(), host: os.hostname(), startedAt: at - 1_000,
      releaseSha: "test", lockProtocol: 1, stateBackend: "file" },
  });
  return { store, actor, actorRoot };
};
const alarms = (mesh: MeshStore) => mesh.read({ topic: "ops.owner", after: 0, limit: 100 })
  .filter(event => event.kind === "actor.session.orphaned");
const turn = () => new Promise<void>(resolve => setImmediate(resolve));

describe("ActorDirectory read-only list snapshots", () => {
  it("never invokes orphan reconciliation on repeated UI reads", async () => {
    const { directory } = open();
    const reconcile = vi.spyOn(directory, "reconcileSessionOrphans");
    const fleetReconcile = vi.spyOn(SessionActorOrphans.prototype, "reconcile");
    for (let read = 0; read < 3; read++) { directory.list(); await turn(); }
    expect.soft(reconcile).not.toHaveBeenCalled();
    expect(fleetReconcile).not.toHaveBeenCalled();
  });

  it("does not enumerate foreign roots, stop their actors, or publish/write their repairs", async () => {
    const { directory, mesh, actorRoots } = open();
    const foreign = seedForeign(mesh);
    const before = foreign.store.snapshot();
    const enumerate = vi.spyOn(fs, "readdirSync");
    for (let read = 0; read < 3; read++) { directory.list(); await turn(); }
    // Join any accidental read-triggered custody before examining durable effects.
    await directory.close();
    const foreignScans = enumerate.mock.calls.filter(([input, options]) => {
      const target = String(input);
      return target === foreign.actorRoot || target.startsWith(`${foreign.actorRoot}${path.sep}`) ||
        (target === actorRoots.project && typeof options === "object" && options?.withFileTypes === true);
    });
    expect.soft(foreignScans).toEqual([]);
    expect.soft(foreign.store.snapshot().generation).toBe(before.generation);
    expect.soft(foreign.store.snapshot().bytes).toBe(before.bytes);
    expect.soft(foreign.store.records()[0]).toMatchObject({ status: "running" });
    expect(alarms(mesh)).toEqual([]);
  });

  it("explicit maintenance discovers and repairs a dead foreign actor for subsequent list display", async () => {
    const { directory, mesh } = open();
    expect(directory.list()).toEqual([]); // Warm before the foreign directory exists.
    await turn();
    const { store, actor } = seedForeign(mesh);
    expect(await directory.reconcileSessionOrphans()).toBe(1);
    expect(store.records()[0]).toMatchObject({ id: actor.id, status: "stopped", stopReason: "host-exit",
      sessionOrphan: { oldRoot: actor.rootId, alarmPublishedAt: expect.any(Number) } });
    expect(directory.list()).toContainEqual(expect.objectContaining({ id: actor.id, status: "stopped",
      scope: "session", sessionOrphan: expect.objectContaining({ oldRoot: actor.rootId }) }));
    expect(alarms(mesh)).toHaveLength(1);
  });

  it("invalidates known registry generations but discovers unknown foreign roots only during maintenance", async () => {
    const { directory, mesh } = open();
    const known = seedForeign(mesh);
    expect(await directory.reconcileSessionOrphans()).toBe(1);
    expect(directory.list().find(actor => actor.id === known.actor.id)?.name).toBe(known.actor.name);
    await turn();
    await known.store.update(rows => ({ actors: rows.map(actor => ({ ...actor, name: "externally-renamed" })), value: true }));
    const unknown = seedForeign(mesh, "newly-visible", "b".repeat(32), true);
    const snapshot = directory.list();
    expect.soft(snapshot.find(actor => actor.id === known.actor.id)?.name).toBe("externally-renamed");
    expect.soft(snapshot.map(actor => actor.id)).not.toContain(unknown.actor.id);
    await directory.reconcileSessionOrphans();
    expect(directory.list().map(actor => actor.id)).toContain(unknown.actor.id);
  });
});

const participantKey = (id: string) => `topology/participants/${createHash("sha256").update(id).digest("hex")}`;

// Same unrelated-service floor as dashboard-snapshot.test.ts, but BOTH actor and
// participant reads are real, including the runtime's exact terminal callback.
const dashboardState = (fixture: ReturnType<typeof open>, actors = fixture.directory): FabricState => {
  const { mesh, identity, manager } = fixture;
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id,
    identity, reapDeadHosts: false, terminalActor: entry => actors.validateSessionActorTombstone(entry) });
  fixture.participants.push(participants);
  return {
    activity: { runs: () => [] }, agents: manager, actors,
    participantInfos: (options: Parameters<FabricState["participantInfos"]>[0]) => participants.list(options),
    peerInfos: (options: Parameters<FabricState["peerInfos"]>[0]) => participants.peers(undefined, options),
    mainAgentInfo: () => ({ ...identity, status: "idle", runner: "pi", transport: "host", cwd: process.cwd(),
      startedAt: 1, updatedAt: 1, pendingMessages: false, local: true }),
    globalActors: { list: () => [] }, config: { mesh: { enabled: false } }, mesh,
  } as unknown as FabricState;
};

const settleDashboardOrphan = async (fixture: ReturnType<typeof open>) => {
  const { mesh, identity, directory } = fixture;
  const foreign = seedForeign(mesh);
  const actor = { ...foreign.actor, project: "dashboard-project" };
  foreign.store.write([actor]);
  const old: MeshIdentity = { id: actor.rootId, sessionId: "old", name: "Old Main", kind: "main" };
  registerMainInbox(mesh.root, old, "old", undefined, "native-lead", actor.project);
  registerMainInbox(mesh.root, identity, "current", undefined, "native-lead", actor.project);
  const ownerPath = path.join(mesh.root, "main-followups", "old.owner.json");
  fs.writeFileSync(ownerPath, JSON.stringify({ ...JSON.parse(fs.readFileSync(ownerPath, "utf8")),
    pid: absentPid(), processStartedAt: "1" }));
  fs.utimesSync(ownerPath, new Date(actor.updatedAt), new Date(actor.updatedAt));
  writeHostLease(mesh.root, { id: identity.id, rootId: identity.id, identityId: identity.id,
    updatedAt: Date.now(), expiresAt: Date.now() + 60_000,
    writer: { pid: process.pid, host: os.hostname(), startedAt: Date.now(), releaseSha: "test", lockProtocol: 1, stateBackend: "file" } });
  const clock = vi.spyOn(Date, "now").mockReturnValue(actor.updatedAt);
  try {
    await mesh.put({ identity: old, key: `actors/old/${actor.id}`, value: actor });
    await mesh.put({ identity: old, key: participantKey(actor.id), value: { format: 1, id: actor.id, kind: "actor",
      name: actor.name, rootId: old.id, ownerHostId: old.id, ownerIdentityId: old.id, residency: "session", runner: "pi",
      transport: "host", capabilities: [], controlProtocol: "legacy", status: "running",
      startedAt: actor.createdAt, updatedAt: actor.updatedAt } });
  } finally { clock.mockRestore(); }
  expect(await directory.reconcileSessionOrphans()).toBe(1);
  const terminal = mesh.get(participantKey(actor.id), { fresh: true })!;
  expect(terminal.value).toMatchObject({ id: actor.id, status: "stopped", stopReason: "owner-root-gone" });
  return { ...foreign, actor, terminal };
};

// Count only the foreign-root discovery boundary, not ordinary mesh/participant
// directory reads. This is the same distinction as the direct-list regression.
const foreignScans = (enumerate: { mock: { calls: unknown[][] } }, actorRoot: string) =>
  enumerate.mock.calls.filter(([input, options]) => String(input).startsWith(`${actorRoot}${path.sep}old`) ||
    (String(input) === actorRoot && options !== null && typeof options === "object" &&
      "withFileTypes" in options && options.withFileTypes === true));

describe("full dashboard settled-orphan reads", () => {
  it("does not discover foreign actor roots when no settled tombstone exists", () => {
    const fixture = open();
    const foreign = seedForeign(fixture.mesh);
    const state = dashboardState(fixture);
    const enumerate = vi.spyOn(fs, "readdirSync");
    for (let read = 0; read < 3; read++) {
      expect((createDashboardSnapshot(state, []).participants ?? []).map(entry => entry.id)).not.toContain(foreign.actor.id);
    }
    expect(foreignScans(enumerate, fixture.actorRoots.project)).toEqual([]);
    expect(foreign.store.records()[0]).toMatchObject({ status: "running" });
  });

  it("retains a settled terminal across repeated full snapshots without rediscovering foreign roots", async () => {
    const fixture = open();
    const state = dashboardState(fixture);
    expect(createDashboardSnapshot(state, []).participants).toEqual([]);
    const enumerate = vi.spyOn(fs, "readdirSync");
    const { actor, store } = await settleDashboardOrphan(fixture);
    // Explicit maintenance DID discover the fixture; dashboard reads must not.
    expect(foreignScans(enumerate, fixture.actorRoots.project).length).toBeGreaterThan(0);
    enumerate.mockClear();
    const before = store.snapshot().bytes;
    const reconcile = vi.spyOn(fixture.directory, "reconcileSessionOrphans");
    const validate = vi.spyOn(fixture.directory, "validateSessionActorTombstone");
    for (let read = 0; read < 3; read++) {
      validate.mockClear();
      expect(createDashboardSnapshot(state, []).main.id).toBe(fixture.identity.id);
      // Dashboard defaults hide stale participants, but still revalidate their
      // terminals before filtering. Observe the real callback, never stub it.
      expect(validate.mock.results.map(result => result.value)).toContainEqual(expect.objectContaining({
        id: actor.id, name: actor.name, status: "stopped", stopReason: "owner-root-gone", capabilities: [],
      }));
    }
    expect.soft(foreignScans(enumerate, fixture.actorRoots.project)).toEqual([]);
    expect.soft(reconcile).not.toHaveBeenCalled();
    expect(store.snapshot().bytes).toBe(before);
    expect(fixture.participants[0]!.list({ includeStale: true })).toContainEqual(expect.objectContaining({
      id: actor.id, name: actor.name, status: "stopped", stopReason: "owner-root-gone",
    }));
    // A cached referenced store must still read a newly committed generation.
    await store.update(rows => ({ actors: rows.map(row => ({ ...row, name: "fresh-terminal-name" })), value: true }));
    enumerate.mockClear();
    validate.mockClear();
    createDashboardSnapshot(state, []);
    expect(validate.mock.results.map(result => result.value)).toContainEqual(expect.objectContaining({
      id: actor.id, name: "fresh-terminal-name", status: "stopped", stopReason: "owner-root-gone",
    }));
    expect(foreignScans(enumerate, fixture.actorRoots.project)).toEqual([]);
  });

  it("validates an exact referenced terminal on a cold directory without catalog warming or discovery", async () => {
    const fixture = open();
    const { actor, terminal } = await settleDashboardOrphan(fixture);
    const cold = fixture.newDirectory(); // No list/reconcile/resolve on this directory.
    const state = dashboardState(fixture, cold);
    const enumerate = vi.spyOn(fs, "readdirSync");
    expect(cold.validateSessionActorTombstone(terminal)).toMatchObject({ id: actor.id, status: "stopped" });
    createDashboardSnapshot(state, []);
    expect.soft(foreignScans(enumerate, fixture.actorRoots.project)).toEqual([]);
    expect(fixture.participants[0]!.list({ includeStale: true })).toContainEqual(expect.objectContaining({
      id: actor.id, status: "stopped", stopReason: "owner-root-gone",
    }));
  });

  it.each(["tampered-terminal", "changed-owner", "missing-private-receipt"] as const)(
    "still rejects a %s rather than treating cached registry knowledge as proof", async mode => {
      const fixture = open();
      const { terminal, store } = await settleDashboardOrphan(fixture);
      expect(fixture.directory.validateSessionActorTombstone(terminal)).toBeDefined();
      if (mode === "tampered-terminal") {
        const value = terminal.value as Record<string, unknown>;
        expect(fixture.directory.validateSessionActorTombstone({ ...terminal, value: { ...value, ownerRootGoneBy: "session:forged" } }))
          .toBeUndefined();
      } else {
        await store.update(rows => ({ actors: rows.map(row => mode === "changed-owner"
          ? { ...row, rootId: "session:successor" }
          : { ...row, sessionOrphan: { ...(row.sessionOrphan as object), directorySettlement: undefined } }), value: true }));
        expect(fixture.directory.validateSessionActorTombstone(terminal)).toBeUndefined();
        createDashboardSnapshot(dashboardState(fixture), []);
        expect(fixture.participants[0]!.list({ includeStale: true }).map(entry => entry.id))
          .not.toContain((terminal.value as Record<string, unknown>).id);
      }
    });
});

const scopes: FabricActorStorageScope[] = ["project", "session"];
describe("mutation authority after a warm list snapshot", () => {
  describe.each(scopes)("%s incumbent registry", incumbentScope => {
    it.each(scopes)("create in %s sees an externally inserted active name", async targetScope => {
      const { directory, actorRoots } = open();
      expect(directory.list()).toEqual([]);
      const store = new ActorRegistryStore(actorRoots[incumbentScope]);
      const incumbent = row("c".repeat(32), "external-incumbent");
      // Another writer commits after the UI snapshot, not through this directory.
      await store.update(() => ({ actors: [incumbent], value: true }));
      const before = store.snapshot().bytes;
      await expect(directory.create({ scope: targetScope, name: incumbent.name, instructions: "Must not replace it." }))
        .rejects.toThrow(/already active/);
      expect(store.snapshot().bytes).toBe(before);
    });

    it.each(["stop", "remove"] as const)("%s refuses canonical ownership moved after a warm list", async mutation => {
      let canonicalOwner = true;
      // The observer deliberately remains stale-positive. Mutations must ask for
      // canonical authority, not reuse the display's cached owner decision.
      const { directory, actorRoots } = open((_id, fresh = true) => fresh ? canonicalOwner : true);
      const actor = await directory.create({ scope: incumbentScope, name: "ownership-moved", instructions: "Observe only." });
      expect(directory.list().map(entry => entry.id)).toContain(actor.id);
      const store = new ActorRegistryStore(actorRoots[incumbentScope]);
      await store.update(rows => ({ actors: rows.map(entry => ({ ...entry, rootId: "session:successor" })), value: true }));
      canonicalOwner = false;
      const before = store.snapshot().bytes;
      await expect(directory[mutation](actor.id)).rejects.toThrow(/owned by another host|owning host/);
      expect(store.snapshot().bytes).toBe(before);
    });

    it("routes a newly committed local actor for stop/remove and still allows legitimate create", async () => {
      const { directory, actorRoots } = open(() => true);
      expect(directory.list()).toEqual([]);
      const store = new ActorRegistryStore(actorRoots[incumbentScope]);
      const actor = row("d".repeat(32), "legitimate-local");
      await store.update(() => ({ actors: [actor], value: true }));
      // Routing must refresh both scopes even though the warm snapshot was empty.
      await expect(directory.stop(actor.id)).resolves.toMatchObject({ id: actor.id, status: "stopped" });
      directory.list();
      await expect(directory.remove(actor.id)).resolves.toMatchObject({ removed: true });
      expect(store.records().map(entry => entry.id)).not.toContain(actor.id);
      directory.list();
      await expect(directory.create({ scope: incumbentScope, name: actor.name, instructions: "Observe only." }))
        .resolves.toMatchObject({ name: actor.name, scope: incumbentScope, status: "idle" });
    });
  });
});
