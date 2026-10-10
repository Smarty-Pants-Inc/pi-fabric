import { spawn } from "node:child_process";
import { once } from "node:events";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { SessionActorOrphans, SESSION_ACTOR_ORPHAN_GRACE_MS, sessionActorRootGone } from "../src/actors/session-orphans.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { reapDeadHostRecords } from "../src/topology/host-reaper.js";

let deadPid: number;
beforeAll(async () => {
  const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
  deadPid = child.pid!;
  await once(child, "exit");
  expect(() => process.kill(deadPid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
});
const fixtures: Array<{ root: string; repair: SessionActorOrphans }> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const { root, repair } of fixtures.splice(0)) { await repair.close(); fs.rmSync(root, { recursive: true, force: true }); }
});

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "session-actor-orphans-"));
  const mesh = new MeshStore(root, 64 * 1024, 100);
  const identity: MeshIdentity = { id: "session:new", name: "new Main", kind: "main", sessionId: "new" };
  const store = new ActorRegistryStore(path.join(root, "actors", "old"));
  const at = Date.now() - SESSION_ACTOR_ORPHAN_GRACE_MS - 10_000;
  const id = "a".repeat(32);
  const actor = { id, name: "project-supervisor", rootId: "session:old", residency: "session", status: "running",
    project: "project", role: "supervisor", instructions: "Supervise Main", events: ["agent_settled"], topics: [],
    runner: "pi", createdAt: at - 1_000, updatedAt: at, messages: [] };
  store.write([actor]);
  const lease: FabricHostLease = { id: "session:old", rootId: "session:old", identityId: "session:old", updatedAt: at,
    expiresAt: at, writer: { pid: deadPid, host: os.hostname(), startedAt: at - 1_000, releaseSha: "test", lockProtocol: 1, stateBackend: "file" } };
  writeHostLease(root, lease);
  const repair = new SessionActorOrphans(mesh, identity, "new", path.join(root, "actors"), () => true);
  fixtures.push({ root, repair });
  return { root, mesh, identity, store, actor, lease, repair, id };
};
const alarms = (mesh: MeshStore) => mesh.read({ topic: "ops.owner", after: 0, limit: 100 });
const staleInbox = (root: string, lease: FabricHostLease, pid = deadPid, processStartedAt = "1") => {
  const owner = path.join(root, "main-followups", "old.owner.json");
  fs.mkdirSync(path.dirname(owner), { recursive: true });
  fs.writeFileSync(owner, JSON.stringify({ rootId: lease.id, sessionId: "old", ownerIdentityId: lease.id,
    pid, processStartedAt, host: os.hostname(), name: "prior incarnation" }));
  fs.utimesSync(owner, new Date(lease.updatedAt), new Date(lease.updatedAt));
};
const staleRootPresence = (root: string, lease: FabricHostLease, changes = {}) => {
  const key = `topology/participants/${createHash("sha256").update(lease.id).digest("hex")}`;
  writeParticipantFile(root, { key, version: 1, updatedAt: lease.updatedAt,
    updatedBy: { id: lease.id, kind: "main", name: "old Main" },
    value: { id: lease.id, kind: "root", rootId: lease.id, ownerHostId: lease.id, ownerIdentityId: lease.id,
      name: "old Main", status: "idle", ...changes } });
};

describe("foreign session actor registry truth", () => {
  it("discovers the old registry, persists root-gone and emits one factory-compatible alarm across duplicate reads", async () => {
    const { mesh, store, repair, id } = fixture();
    expect(await repair.reconcile()).toBe(1);
    expect(store.records()[0]).toMatchObject({ status: "stopped", lastError: expect.stringContaining("root-gone:"),
      sessionOrphan: { oldRoot: "session:old", oldHost: os.hostname(), alarmPublishedAt: expect.any(Number) } });
    expect(repair.resolve(id)).toMatchObject({ id, scope: "session", status: "stopped", sessionOrphan: { reason: "Main owner process is dead" } });
    await Promise.all([repair.reconcile(), repair.reconcile()]);
    expect(alarms(mesh)).toHaveLength(1);
    expect(alarms(mesh)[0]).toMatchObject({ topic: "ops.owner", kind: "actor.session.orphaned",
      text: "actor project-supervisor orphaned: its Main moved or ended; re-run activation.py in the new Main",
      data: { actorId: id, name: "project-supervisor", oldRoot: "session:old", rootId: "session:old", oldHost: os.hostname(),
        lastUpdated: expect.any(Number), project: "project", role: "supervisor", line: expect.stringContaining("re-run activation.py") } });
  });

  it.each(["grace", "reload", "live-process", "foreign-host", "missing-lease", "invalid-lease", "durable", "current-root"] as const)("preserves %s evidence and private visibility", async mode => {
    const { root, store, repair, lease, actor } = fixture();
    if (mode === "grace") writeHostLease(root, { ...lease, expiresAt: Date.now() - 10_000 });
    if (mode === "reload") writeHostLease(root, { ...lease, reloadUntil: Date.now() + 10_000 });
    if (mode === "live-process") writeHostLease(root, { ...lease, writer: { ...lease.writer!, pid: process.pid } });
    if (mode === "foreign-host") writeHostLease(root, { ...lease, writer: { ...lease.writer!, host: "another-machine" } });
    if (mode === "missing-lease") fs.rmSync(hostLeasePath(root, lease.id));
    if (mode === "invalid-lease") fs.writeFileSync(hostLeasePath(root, lease.id), "not json");
    if (mode === "durable") store.write([{ ...actor, residency: "durable" }]);
    if (mode === "current-root") store.write([{ ...actor, rootId: "session:new" }]);
    const before = store.snapshot().bytes;
    expect(await repair.reconcile()).toBe(0);
    expect(store.snapshot().bytes).toBe(before);
    expect(repair.list()).toEqual([]);
    expect(repair.resolve(actor.id)).toBeUndefined();
  });

  it.each(["live-local-lease", "foreign-lease", "different-dead-lease", "foreign-presence", "different-owner-presence", "shared-foreign-presence"] as const)(
    "a stale dead inbox cannot override present %s evidence", async mode => {
      const { root, mesh, identity, lease, store, repair, actor } = fixture();
      const at = Date.now() - 300_000;
      const current = { ...lease, updatedAt: at, expiresAt: at,
        session: { id: "old", startedAt: at - 60_000, updatedAt: at, expiresAt: at } };
      staleInbox(root, current);
      staleRootPresence(root, current);
      if (mode === "live-local-lease") current.writer = { ...lease.writer!, pid: process.pid };
      if (mode === "foreign-lease") current.writer = { ...lease.writer!, host: "another-machine" };
      if (mode === "different-dead-lease") current.writer = { ...lease.writer!, pid: 2_147_483_647 };
      if (mode === "foreign-presence") staleRootPresence(root, current, { remoteHost: "another-machine" });
      if (mode === "different-owner-presence") {
        const other = { ...current, id: "runtime:current", identityId: "runtime:current",
          writer: { ...lease.writer!, pid: process.pid } };
        writeHostLease(root, other);
        staleRootPresence(root, current, { ownerHostId: other.id, ownerIdentityId: other.identityId });
      }
      if (mode === "shared-foreign-presence") {
        const clock = vi.spyOn(Date, "now").mockReturnValue(at);
        try { await mesh.put({ identity,
          key: `topology/participants/${createHash("sha256").update(lease.id).digest("hex")}`,
          value: { id: lease.id, kind: "root", rootId: lease.id, ownerHostId: lease.id, ownerIdentityId: lease.id,
            remoteHost: "another-machine" } });
        } finally { clock.mockRestore(); }
      }
      writeHostLease(root, current);
      const before = store.snapshot().bytes;
      // No clean-close proof: the prior process's death says nothing about this writer.
      expect(sessionActorRootGone(mesh, lease.id, () => true)).toBeUndefined();
      expect(await repair.reconcile()).toBe(0);
      expect(store.snapshot().bytes).toBe(before);
      expect(store.records()[0]?.sessionOrphan).toBeUndefined();
      expect(repair.list()).toEqual([]);
      expect(repair.resolve(actor.id)).toBeUndefined();
      expect(alarms(mesh)).toEqual([]);
    });

  it("still retires a matched dead lease and inbox with stale root presence", async () => {
    const { root, lease, store, mesh, repair } = fixture();
    staleInbox(root, lease);
    staleRootPresence(root, lease);
    expect(await repair.reconcile()).toBe(1);
    expect(store.records()[0]).toMatchObject({ status: "stopped", sessionOrphan: { oldHost: os.hostname() } });
    expect(alarms(mesh)).toHaveLength(1);
  });

  it.skipIf(process.platform !== "linux")("keeps a present unqualified live lease writer unknown despite a reused inbox PID across two full reconciles", async () => {
    const { root, lease, mesh, store, repair, actor } = fixture();
    const tick = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
    expect(tick.slice(tick.lastIndexOf(")") + 2).trim().split(/\s+/)[19]).not.toBe("1");
    const at = Date.now() - 300_000;
    const current = { ...lease, updatedAt: at, expiresAt: at,
      session: { id: "old", startedAt: at - 60_000, updatedAt: at, expiresAt: at },
      writer: { ...lease.writer!, pid: process.pid, startedAt: Math.floor(Date.now() - process.uptime() * 1000) } };
    staleInbox(root, current, process.pid, "1");
    staleRootPresence(root, current);
    writeHostLease(root, current);
    const before = store.snapshot().bytes;
    for (let pass = 0; pass < 2; pass++) {
      expect(sessionActorRootGone(mesh, lease.id, () => true)).toBeUndefined();
      expect(await repair.reconcile()).toBe(0);
      expect(store.snapshot().bytes).toBe(before);
      expect(store.records()[0]?.sessionOrphan).toBeUndefined();
      expect(repair.list()).toEqual([]);
      expect(repair.resolve(actor.id)).toBeUndefined();
      expect(alarms(mesh)).toEqual([]);
    }
  });

  it.skipIf(process.platform !== "linux")("proves old-incarnation death with a reused PID after the lease is missing", async () => {
    const { root, lease, mesh, repair } = fixture();
    staleInbox(root, lease, process.pid, "1");
    staleRootPresence(root, lease);
    fs.rmSync(hostLeasePath(root, lease.id));
    expect(sessionActorRootGone(mesh, lease.id, () => true)).toMatchObject({ reason: "Main inbox owner incarnation is dead" });
    expect(await repair.reconcile()).toBe(1);
    expect(await repair.reconcile()).toBe(0);
    expect(alarms(mesh)).toHaveLength(1);
  });

  it("lets a durable resident observer repair its own dead Main's private actor without changing durable rows", async () => {
    const { root, mesh, store, actor, repair } = fixture();
    const durable = { ...actor, id: "b".repeat(32), name: "reviewer", residency: "durable" };
    store.write([actor, durable]);
    const main = new SessionActorOrphans(mesh, { id: "session:old", name: "old Main", kind: "main" }, "old", path.join(root, "actors"));
    expect(await main.reconcile()).toBe(0);
    await main.close();
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    });
    const resident = new ActorDirectory(["old", { id: "resident:old", name: "resident", kind: "agent" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {},
      { persistent: true, releasePaused: true, claimResidency: "durable", rootId: "session:old", canManageActor: () => false }],
      { project: path.join(root, "actors"), session: path.join(root, "actors", "old") }, "project");
    try {
      expect(await resident.reconcileSessionOrphans()).toBe(1);
      expect(store.records().find(row => row.id === actor.id)?.status).toBe("stopped");
      expect(store.records().find(row => row.id === durable.id)).toEqual(durable);
      expect(resident.list().filter(row => row.id === actor.id)).toHaveLength(1);
      expect(resident.status(actor.id)).toMatchObject({ status: "stopped", sessionOrphan: { oldRoot: "session:old" } });
      expect(alarms(mesh)).toHaveLength(1);
      expect(await repair.reconcile()).toBe(0);
    } finally { await resident.close(); await agents.close(); }
  });

  it("attributes an orphan to the validated stale Main name and role", async () => {
    const { root, mesh, repair, lease, actor } = fixture();
    const key = `topology/participants/${createHash("sha256").update(lease.id).digest("hex")}`;
    writeParticipantFile(root, { key, version: 1, updatedAt: actor.updatedAt, updatedBy: { id: lease.id, kind: "main", name: "product-lead" },
      value: { id: lease.id, kind: "root", rootId: lease.id, ownerHostId: lease.id, ownerIdentityId: lease.id,
        name: "product-lead", role: "project-agent", status: "idle" } });
    expect(await repair.reconcile()).toBe(1);
    expect(alarms(mesh)[0]?.data).toMatchObject({ leadName: "product-lead", oldHost: os.hostname() });
  });

  it("repairs a long-dead Main after the real host reaper removed its root lease", async () => {
    const { root, mesh, identity, repair, lease, store } = fixture();
    const at = Date.now() - 7 * 60 * 60_000;
    writeHostLease(root, { ...lease, updatedAt: at, expiresAt: at });
    await mesh.put({ identity, key: `topology/hosts/${createHash("sha256").update(lease.id).digest("hex")}`,
      value: { id: lease.id, rootId: lease.id, identity: { id: lease.id }, expiresAt: at } });
    const owner = path.join(root, "main-followups", "old.owner.json");
    fs.mkdirSync(path.dirname(owner), { recursive: true });
    fs.writeFileSync(owner, JSON.stringify({ rootId: lease.id, sessionId: "old", ownerIdentityId: lease.id,
      pid: deadPid, processStartedAt: "1", host: os.hostname(), name: "old lead" }));
    fs.utimesSync(owner, new Date(at), new Date(at));
    expect(await reapDeadHostRecords(mesh, identity, { ownHostId: identity.id })).toBeGreaterThan(0);
    expect(fs.existsSync(hostLeasePath(root, lease.id))).toBe(false);
    staleRootPresence(root, { ...lease, updatedAt: at, expiresAt: at });
    expect(await repair.reconcile()).toBe(1);
    expect(store.records()[0]).toMatchObject({ status: "stopped", sessionOrphan: { oldHost: os.hostname(), reason: "Main inbox owner incarnation is dead" } });
    expect(alarms(mesh)).toHaveLength(1);
  });

  it.each(["live", "invalid", "hostless", "foreign", "fresh", "invalid-lease"] as const)("preserves %s Main inbox evidence after root lease removal", async mode => {
    const { root, repair, lease } = fixture();
    fs.rmSync(hostLeasePath(root, lease.id));
    const at = Date.now() - SESSION_ACTOR_ORPHAN_GRACE_MS - 10_000;
    const owner = path.join(root, "main-followups", "old.owner.json");
    fs.mkdirSync(path.dirname(owner), { recursive: true });
    const value: Record<string, unknown> = { rootId: lease.id, sessionId: "old", ownerIdentityId: lease.id,
      pid: mode === "live" ? process.pid : deadPid, processStartedAt: "1", host: os.hostname() };
    if (mode === "live") delete value.processStartedAt; // live PID without reuse evidence is live
    if (mode === "hostless") delete value.host;
    if (mode === "foreign") value.host = "another-machine";
    fs.writeFileSync(owner, mode === "invalid" ? "not json" : JSON.stringify(value));
    if (mode !== "fresh") fs.utimesSync(owner, new Date(at), new Date(at));
    if (mode === "invalid-lease") fs.writeFileSync(hostLeasePath(root, lease.id), "invalid lease");
    expect(await repair.reconcile()).toBe(0);
  });

  it("uses unknown rather than a root ID as clean-close host attribution without host evidence", async () => {
    const { root, mesh, repair, lease } = fixture();
    await repair.close();
    fs.rmSync(hostLeasePath(root, lease.id));
    const closer: MeshIdentity = { id: lease.id, kind: "main", name: "old Main" };
    await mesh.put({ identity: closer, key: `topology/lineage-closures/${createHash("sha256").update(lease.id).digest("hex")}`,
      value: { format: 1, rootId: lease.id, ownerHostId: lease.id, ownerIdentityId: lease.id, closedAt: Date.now() - SESSION_ACTOR_ORPHAN_GRACE_MS - 10_000 } });
    expect(sessionActorRootGone(mesh, lease.id, () => false)).toMatchObject({ oldHost: "unknown", reason: "Main lineage closed" });
  });

  it("preserves a live actor owner on another host even when Main is dead", async () => {
    const { root, repair, lease, id } = fixture();
    const owner = { ...lease, id: "runtime:owner", identityId: "runtime:owner", expiresAt: Date.now() + 60_000 };
    writeHostLease(root, owner);
    const key = `topology/participants/${createHash("sha256").update(id).digest("hex")}`;
    writeParticipantFile(root, { key, version: 1, updatedAt: Date.now(), updatedBy: { id: owner.id, kind: "main", name: "owner" },
      value: { id, rootId: "session:old", ownerHostId: owner.id, ownerIdentityId: owner.id, status: "running" } });
    expect(await repair.reconcile()).toBe(0);
  });

  it("retries a failed alarm from the durable stopped row, including a crash after publication before acknowledgement", async () => {
    const { mesh, repair, store } = fixture();
    const publish = vi.spyOn(mesh, "publish").mockRejectedValueOnce(new Error("publication unavailable"));
    expect(await repair.reconcile()).toBe(1);
    expect(store.records()[0]?.status).toBe("stopped");
    expect(alarms(mesh)).toHaveLength(0);
    publish.mockRestore();
    const acknowledge = vi.spyOn(ActorRegistryStore.prototype, "update").mockRejectedValueOnce(new Error("ack unavailable"));
    await repair.reconcile();
    expect(alarms(mesh)).toHaveLength(1);
    acknowledge.mockRestore();
    await repair.reconcile();
    expect(alarms(mesh)).toHaveLength(1);
    expect(store.records()[0]?.sessionOrphan).toMatchObject({ alarmPublishedAt: expect.any(Number) });
  });

  it("vetoes a CAS successor generation and a live renewal appearing after selection", async () => {
    const successor = fixture();
    const locks = ActorRegistryStore.withLocks.bind(ActorRegistryStore);
    let injected = false;
    const intercept = vi.spyOn(ActorRegistryStore, "withLocks").mockImplementation((stores, operation) => locks(stores, () => {
      if (!injected) { injected = true; successor.store.write([{ ...successor.actor, rootId: "session:successor", updatedAt: Date.now() }]); }
      return operation();
    }));
    expect(await successor.repair.reconcile()).toBe(0);
    expect(successor.store.records()[0]).toMatchObject({ rootId: "session:successor", status: "running" });
    expect(alarms(successor.mesh)).toHaveLength(0);
    intercept.mockRestore();
    const live = fixture();
    vi.spyOn(ActorRegistryStore, "withLocks").mockImplementation((stores, operation) => locks(stores, () => {
      writeHostLease(live.root, { ...live.lease, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
      return operation();
    }));
    expect(await live.repair.reconcile()).toBe(0);
    expect(live.store.records()[0]?.status).toBe("running");
    expect(alarms(live.mesh)).toHaveLength(0);
  });

  it("rejects a lease renewed between parsing and optional-evidence validation", () => {
    const { root, mesh, lease } = fixture();
    const read = fs.readFileSync.bind(fs);
    let reads = 0;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (file === hostLeasePath(root, lease.id) && ++reads === 2) writeHostLease(root, { ...lease, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 });
      return read(file, options as never);
    }) as typeof fs.readFileSync);
    expect(sessionActorRootGone(mesh, lease.id)).toBeUndefined();
    expect(reads).toBe(2);
  });
});
