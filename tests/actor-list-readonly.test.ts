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

// Keep reason: data safety across the foreign-registry/ownership trust boundary.
// Real registries and reconciliation are used; no worker needs to run.
const fixtures: Array<{ root: string; directory: ActorDirectory; manager: AgentManager }> = [];
afterEach(async () => {
  for (const { root, directory, manager } of fixtures.splice(0)) {
    await directory.close();
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
  const directory = new ActorDirectory([
    "current", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, manager, () => {},
    { persistent: true, rootId: identity.id, lineageAlive: () => true, ...(canManageActor ? { canManageActor } : {}) },
  ], actorRoots, "project");
  fixtures.push({ root, directory, manager });
  return { directory, manager, mesh, actorRoots };
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
