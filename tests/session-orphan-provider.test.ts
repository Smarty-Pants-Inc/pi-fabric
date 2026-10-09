import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { FabricActorInfo, FabricActorReadInfo, FabricActorStorageScope } from "../src/actors/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const context: FabricInvocationContext = {
  cwd: process.cwd(), signal: undefined, parentToolCallId: "session-orphan-provider",
  nestedToolCallId: "nested", extensionContext: { modelRegistry: { getAvailable: () => [] } } as unknown as ExtensionContext,
  update() {}, activity() {},
};
const tmp = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-session-orphan-provider-"));
  roots.push(root);
  return root;
};

// This is a real ESRCH observation, not a mocked liveness verdict. No child
// processes are started: the separate process-proof suite covers SIGKILL/grace.
const absentPid = (): number => {
  for (let pid = 2_147_483_647; pid > 2_147_483_627; pid--) {
    try { process.kill(pid, 0); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return pid; }
  }
  throw new Error("Could not obtain a provably absent PID for the orphan lease fixture");
};

const open = (root: string, sessionId: string, members: FabricParticipantInfo[] = []) => {
  const identity: MeshIdentity = { id: `session:${sessionId}`, name: "Main", kind: "main", sessionId };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, `runs-${sessionId}`), hostId: identity.id,
  });
  const mainAgent: FabricMainAgentTarget = {
    id: identity.id, local: true, matches: id => id === "main" || id === identity.id,
    info: () => ({ id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      startedAt: 1, updatedAt: Date.now(), pendingMessages: false, local: true, sessionId }),
    deliverAgent: () => ({ queued: true, messageId: "fixture-main-message", routed: "main" }),
  };
  const actorRoots = { project: path.join(mesh.root, "actors"), session: path.join(mesh.root, "actors", sessionId) };
  const directory = new ActorDirectory([
    sessionId, identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, manager, () => {},
    { persistent: true, claimResidency: "session", rootId: identity.id, mainAgent },
  ], actorRoots, "project");
  const self: FabricParticipantInfo = {
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "Main", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
    startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1", local: true, stale: false,
  };
  const participants: FabricParticipantSource = {
    list: (options = {}) => members.filter(member => (!options.kinds || options.kinds.includes(member.kind)) &&
      (options.scope !== "local" || member.local) && (options.scope !== "lineage" || member.rootId === identity.id)),
    get: id => members.find(member => member.id === id), self: () => self, peers: () => [],
    async refresh() {}, scheduleRefresh() {},
  };
  const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: true, pollMs: 60_000, maxReadEvents: 100 }, () => {});
  const provider = new AgentsProvider(manager, directory, new GlobalActorRegistry(root, 64 * 1024), mainAgent, participants, undefined, lifecycle);
  let closed = false;
  const close = async () => { if (closed) return; closed = true; await provider.close(); };
  closers.push(close);
  return { directory, manager, provider, mesh, actorRoots, participants, close };
};

const oldLease = (meshRoot: string, sessionId: string, changes: Partial<FabricHostLease> = {}, withWriter = true) => {
  const lastUpdated = Date.now() - 180_000;
  const rootId = `session:${sessionId}`;
  const lease: FabricHostLease = {
    id: rootId, rootId, identityId: rootId, startedAt: lastUpdated - 60_000,
    updatedAt: lastUpdated - 15_000, expiresAt: lastUpdated,
    writer: { pid: absentPid(), host: os.hostname(), startedAt: lastUpdated - 60_000,
      releaseSha: "test", lockProtocol: 1, stateBackend: "file" },
    ...changes,
  };
  if (!withWriter) delete lease.writer;
  writeHostLease(meshRoot, lease);
  return lease;
};

const seed = async (root: string, sessionId: string, options: {
  name?: string; residency?: "session" | "durable"; scope?: FabricActorStorageScope;
} = {}) => {
  const owner = open(root, sessionId);
  const scope = options.scope ?? "session";
  const actor = await owner.directory.create({ scope, name: options.name ?? "old-supervisor", instructions: "Observe only.",
    residency: options.residency ?? "session", responseMode: "text", delivery: "mailbox" });
  await owner.close();
  const store = new ActorRegistryStore(owner.actorRoots[scope]);
  const updatedAt = Date.now() - 180_000;
  // Seed a persisted crash snapshot using the production registry protocol.
  // No ActorDirectory.status/owns or provider method is mocked.
  await store.update(rows => ({ actors: rows.map(row => row.id === actor.id
    ? { ...row, status: "running", createdAt: updatedAt - 60_000, updatedAt } : row), value: true }));
  return { actor: { ...actor, createdAt: updatedAt - 60_000, updatedAt }, store, mesh: owner.mesh };
};
const retainedRunning = (actor: FabricActorInfo): FabricParticipantInfo => ({
  ...actorParticipantRecord({ ...actor, status: "running", queued: 3,
    inFlightRun: { id: "f".repeat(32), startedAt: actor.updatedAt, ageS: 180 } }, actor.rootId!, actor.rootId!, actor.rootId!, actor.rootId!),
  local: false, stale: false,
});
const status = (provider: AgentsProvider, id: string) => provider.invoke("actorStatus", { id }, context) as Promise<FabricActorReadInfo>;
const actors = (provider: AgentsProvider) => provider.invoke("actors", {}, context) as Promise<FabricActorReadInfo[]>;
const alarms = (mesh: MeshStore) => mesh.read({ topic: "ops.owner", limit: 100 }).filter(event => event.kind === "actor.session.orphaned");
const expectOrphan = (actor: FabricActorReadInfo, id: string, oldRoot: string) => {
  expect(actor).toMatchObject({ id, rootId: oldRoot, status: "stopped", lastError: expect.stringContaining("root-gone:") });
  expect(actor.lastError).toContain("re-run activation.py in the new Main");
  expect(actor.inFlightRun).toBeUndefined();
};

describe("public session-orphan provider reads (#7227)", () => {
  it.each(["absent", "retained-running"] as const)("actorStatus/actors reconcile an old session with %s participant once across concurrent readers", async visibility => {
    const root = tmp();
    const { actor, store, mesh } = await seed(root, "old");
    oldLease(mesh.root, "old");
    const members = visibility === "retained-running" ? [retainedRunning(actor)] : [];
    const first = open(root, "current", members);
    const second = open(root, "other", members);
    const owned = await first.directory.create({ scope: "session", name: "current-owned", instructions: "Observe only." });
    const before = owned;
    const [read1, list1, read2, list2] = await Promise.all([
      status(first.provider, actor.id), actors(first.provider), status(second.provider, actor.id), actors(second.provider),
    ]);
    expectOrphan(read1, actor.id, "session:old");
    expectOrphan(read2, actor.id, "session:old");
    expectOrphan(list1.find(entry => entry.id === actor.id)!, actor.id, "session:old");
    expectOrphan(list2.find(entry => entry.id === actor.id)!, actor.id, "session:old");
    expect(await status(first.provider, owned.id)).toEqual(before);
    expect(first.directory.owns(actor.id)).toBe(false);
    expect(second.directory.owns(actor.id)).toBe(false);
    expect(first.directory.listOwned().map(entry => entry.id)).not.toContain(actor.id);
    const persisted = store.snapshot().actors.find(row => row.id === actor.id)!;
    expect(persisted).toMatchObject({ status: "stopped", rootId: "session:old", sessionOrphan: {
      oldRoot: "session:old", oldHost: os.hostname(), lastUpdated: actor.updatedAt, orphanedAt: expect.any(Number),
      reason: "Main owner process is dead",
    } });
    expect(persisted.adoptedAt).toBeUndefined();
    expect(alarms(mesh)).toHaveLength(1);
    expect(alarms(mesh)[0]).toMatchObject({ dedupeKey: `session-actor-orphan:${actor.id}:session:old`,
      data: { actorId: actor.id, name: actor.name, oldRoot: "session:old", oldHost: os.hostname(), lastUpdated: actor.updatedAt } });
    await Promise.all([status(first.provider, actor.id), actors(first.provider), status(second.provider, actor.id)]);
    const reopened = open(root, "new-reader", members);
    expectOrphan(await status(reopened.provider, actor.id), actor.id, "session:old");
    expect(alarms(mesh)).toHaveLength(1);
    // The stopped foreign orphan is observable by exact ID, not a removable
    // predecessor of a new actor in the current Main.
    const replacement = await first.provider.invoke("create", { name: actor.name, scope: "session", instructions: "Observe the current Main only." }, context) as FabricActorInfo;
    expect(replacement.id).not.toBe(actor.id);
    expect(await status(first.provider, replacement.id)).toMatchObject({ id: replacement.id, rootId: "session:current", status: "idle" });
    expect(first.directory.owns(replacement.id)).toBe(true);
    expect(store.snapshot().actors.find(row => row.id === actor.id)).toMatchObject({ rootId: "session:old", status: "stopped" });
    expectOrphan(await status(first.provider, actor.id), actor.id, "session:old");
    expect(alarms(mesh)).toHaveLength(1);
    expect(first.manager.list()).toEqual([]);
    expect(second.manager.list()).toEqual([]);
  });

  it("a terminal orphan bypasses retained-owner routing recovery when no participant is available", async () => {
    const root = tmp();
    const { actor, mesh } = await seed(root, "old-route");
    oldLease(mesh.root, "old-route");
    const current = open(root, "current-route");
    current.participants.lastKnown = () => ({ participant: { ...retainedRunning(actor), stale: true }, lapsedMs: 180_000 });
    const recovery = vi.fn(async () => { throw new Error("must not recover routing for a proven orphan"); });
    current.participants.resolveRoutingLease = recovery;
    expectOrphan(await status(current.provider, actor.id), actor.id, "session:old-route");
    expectOrphan((await actors(current.provider)).find(entry => entry.id === actor.id)!, actor.id, "session:old-route");
    expect(recovery).not.toHaveBeenCalled();
    expect(alarms(mesh)).toHaveLength(1);
  });

  it.each(["live-process", "live-lease", "reload", "session-lease", "recent-expiry", "no-witness", "foreign-host", "no-lease"] as const)(
    "%s evidence preserves the same foreign actor and running overlay", async evidence => {
      const root = tmp();
      const { actor, store, mesh } = await seed(root, "protected", { scope: "project" });
      const now = Date.now();
      if (evidence !== "no-lease") {
        const changes: Partial<FabricHostLease> = {};
        if (evidence === "live-lease") changes.expiresAt = now + 60_000;
        if (evidence === "reload") changes.reloadUntil = now + 60_000;
        if (evidence === "session-lease") changes.session = { id: "protected", startedAt: now - 240_000, updatedAt: now, expiresAt: now + 60_000 };
        if (evidence === "recent-expiry") changes.expiresAt = now - 30_000;
        if (evidence === "live-process" || evidence === "foreign-host") changes.writer = {
          pid: evidence === "live-process" ? process.pid : absentPid(), host: evidence === "foreign-host" ? `${os.hostname()}-other-host` : os.hostname(),
          startedAt: Math.floor(Date.now() - process.uptime() * 1000), releaseSha: "test", lockProtocol: 1, stateBackend: "file",
        };
        oldLease(mesh.root, "protected", changes, evidence !== "no-witness");
      }
      const bytes = store.snapshot().bytes;
      const current = open(root, "current", [retainedRunning(actor)]);
      expect(await status(current.provider, actor.id)).toMatchObject({ id: actor.id, rootId: "session:protected", status: "running" });
      expect((await actors(current.provider)).find(entry => entry.id === actor.id)).toMatchObject({ id: actor.id, status: "running" });
      expect(store.snapshot().bytes).toBe(bytes);
      expect(store.snapshot().actors.find(row => row.id === actor.id)!.sessionOrphan).toBeUndefined();
      expect(current.directory.owns(actor.id)).toBe(false);
      expect(alarms(mesh)).toEqual([]);
    });

  it.each(["live", "reload", "unknown"] as const)("%s foreign private sessions remain invisible to public actors", async evidence => {
    const root = tmp();
    const { actor, store, mesh } = await seed(root, "private-owner");
    oldLease(mesh.root, "private-owner", evidence === "live" ? { expiresAt: Date.now() + 60_000 }
      : evidence === "reload" ? { reloadUntil: Date.now() + 60_000 } : {}, evidence !== "unknown");
    const bytes = store.snapshot().bytes;
    const current = open(root, "current");
    expect((await actors(current.provider)).map(entry => entry.id)).not.toContain(actor.id);
    await expect(status(current.provider, actor.id)).rejects.toThrow(/Unknown .*actor/);
    expect(store.snapshot().bytes).toBe(bytes);
    expect(alarms(mesh)).toEqual([]);
  });

  it("an in-process reload keeps the same own-root actor identity without an orphan alarm", async () => {
    const root = tmp();
    const { actor, mesh } = await seed(root, "reload-main");
    oldLease(mesh.root, "reload-main", { reloadUntil: Date.now() + 60_000 });
    const reloaded = open(root, "reload-main");
    expect(await status(reloaded.provider, actor.id)).toMatchObject({ id: actor.id, rootId: "session:reload-main", status: "idle" });
    expect(reloaded.directory.owns(actor.id)).toBe(true);
    expect((await actors(reloaded.provider)).find(entry => entry.id === actor.id)).toMatchObject({ id: actor.id, status: "idle" });
    expect((await status(reloaded.provider, actor.id)).lastError).toBeUndefined();
    expect((await status(reloaded.provider, actor.id)).sessionOrphan).toBeUndefined();
    expect(alarms(mesh)).toEqual([]);
  });

  it("unknown foreign evidence without a participant remains unknown, not stopped", async () => {
    const root = tmp();
    const { actor, store, mesh } = await seed(root, "unknown", { scope: "project" });
    oldLease(mesh.root, "unknown", {}, false);
    const bytes = store.snapshot().bytes;
    const current = open(root, "current");
    expect(await status(current.provider, actor.id)).toMatchObject({ id: actor.id, status: "unknown" });
    expect((await actors(current.provider)).find(entry => entry.id === actor.id)).toMatchObject({ id: actor.id, status: "unknown" });
    expect(store.snapshot().bytes).toBe(bytes);
    expect(alarms(mesh)).toEqual([]);
  });

  it("durable actor execution overlays and registry bytes are unchanged by a dead session root", async () => {
    const root = tmp();
    const { actor, store, mesh } = await seed(root, "durable-owner", { residency: "durable", scope: "project" });
    oldLease(mesh.root, "durable-owner");
    const bytes = store.snapshot().bytes;
    const current = open(root, "current", [retainedRunning(actor)]);
    expect(await status(current.provider, actor.id)).toMatchObject({ id: actor.id, status: "running", residency: "durable" });
    expect((await actors(current.provider)).find(entry => entry.id === actor.id)).toMatchObject({ id: actor.id, status: "running" });
    expect(store.snapshot().bytes).toBe(bytes);
    expect(alarms(mesh)).toEqual([]);
  });

  it("ambiguous foreign same-name rows do not block ordinary current-session create or capture its activation", async () => {
    const root = tmp();
    const left = await seed(root, "left", { name: "supervisor" });
    const right = await seed(root, "right", { name: "supervisor" });
    // Missing witnesses are deliberately ambiguous, not dead. Private foreign
    // session rows must stay hidden and never become local create conflicts.
    oldLease(left.mesh.root, "left", {}, false);
    oldLease(right.mesh.root, "right", {}, false);
    const current = open(root, "current", [retainedRunning(left.actor), retainedRunning(right.actor)]);
    const listed = await actors(current.provider);
    expect(listed.map(entry => entry.id)).not.toContain(left.actor.id);
    expect(listed.map(entry => entry.id)).not.toContain(right.actor.id);
    const created = await current.provider.invoke("create", { name: "supervisor", scope: "session", instructions: "Observe this Main only." }, context) as FabricActorInfo;
    expect(created.id).not.toBe(left.actor.id);
    expect(created.id).not.toBe(right.actor.id);
    expect(await status(current.provider, created.id)).toMatchObject({ id: created.id, rootId: "session:current", status: "idle" });
    expect(current.directory.owns(created.id)).toBe(true);
    expect(current.directory.listOwned().map(entry => entry.id)).toContain(created.id);
    expect(current.directory.owns(left.actor.id)).toBe(false);
    expect(current.directory.owns(right.actor.id)).toBe(false);
    expect(left.store.snapshot().actors.find(row => row.id === left.actor.id)!.rootId).toBe("session:left");
    expect(right.store.snapshot().actors.find(row => row.id === right.actor.id)!.rootId).toBe("session:right");
    expect(alarms(current.mesh)).toEqual([]);
    expect(current.manager.list()).toEqual([]);
  });
});
