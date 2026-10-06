import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorBindingStore } from "../src/actors/binding-store.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { LIVENESS_POLICY_KEY, readHostLease } from "../src/topology/host-leases.js";
import { readParticipantFile } from "../src/topology/participant-files.js";

const roots: string[] = [], hosts: ResidentHost[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(hosts.splice(0).map(host => host.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const participantKey = (id: string) => "topology/participants/" + createHash("sha256").update(id).digest("hex");
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const fixture = async (files = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-presence-batch-")); roots.push(root);
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:absent-main", sessionId: "absent-main", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    sessionActorRoot: path.join(root, "session-actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:absent-main"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const now = Date.now();
  const records = Array.from({ length: 40 }, (_, index) => ({
    id: (index + 1).toString(16).padStart(32, "0"), name: `idle-${index}`, instructions: "wait",
    createdAt: now, updatedAt: now, rootId: config.rootId, residency: "durable", runner: "pi", status: "idle",
    scope: index < 20 ? "project" : "session", events: [], topics: [], messages: [],
  }));
  for (const [scope, actorRoot] of [["project", config.actorRoot], ["session", config.sessionActorRoot!]]) {
    fs.mkdirSync(actorRoot!, { recursive: true });
    fs.writeFileSync(path.join(actorRoot!, "actors.json"), JSON.stringify({ format: 1, actors: records.filter(row => row.scope === scope) }));
  }
  if (files) await new MeshStore(config.meshRoot, 256 * 1024, 500).put({ key: LIVENESS_POLICY_KEY,
    value: { version: 1, hostLeases: "files", participants: "files" }, identity: { id: "policy", name: "policy", kind: "agent" } });
  const host = new ResidentHost(config); hosts.push(host); await host.start();
  await delay(20); await host.participants.refresh();
  return { root, config, host, records };
};

describe("#4383 resident host presence batch", () => {
  it.each([false, true])("renews 40 actors across both scopes in one host batch without a Main (files=%s)", async files => {
    const { host, config, records } = await fixture(files);
    let now = Date.now(); vi.spyOn(Date, "now").mockImplementation(() => now);
    const batch = vi.spyOn(host.mesh, "writeBatch"), put = vi.spyOn(host.mesh, "put"), del = vi.spyOn(host.mesh, "delete");
    const confirm = vi.spyOn(host.mesh, "confirmWritable");
    for (let round = 0; round < 3; round++) {
      now += 5_000; batch.mockClear();
      await host.participants.refresh();
      expect(batch).toHaveBeenCalledOnce(); expect(put).not.toHaveBeenCalled(); expect(del).not.toHaveBeenCalled();
      expect(confirm).not.toHaveBeenCalled();
      const ops = batch.mock.calls[0]![0].ops;
      expect(ops.filter(op => op.key.startsWith("actors/"))).toHaveLength(40);
      expect(ops.filter(op => op.kind === "put" && op.key.startsWith("topology/participants/"))).toHaveLength(files ? 0 : 40);
      for (const row of records) {
        const presence = host.mesh.get(`actors/${config.sessionId}/${row.id}`, { fresh: true })!;
        expect(presence.value).toMatchObject({ id: row.id, name: row.name, scope: row.scope, status: "idle", residency: "durable", updatedAt: row.updatedAt });
        expect(presence.updatedAt).toBe(now);
        const participant = readParticipantFile(config.meshRoot, participantKey(row.id))!;
        expect(participant.updatedAt).toBe(now);
        expect(participant.value).toMatchObject({ id: row.id, ownerHostId: host.hostId, rootId: config.rootId });
      }
      expect(readHostLease(config.meshRoot, host.hostId)).toMatchObject({ updatedAt: now, expiresAt: now + 15_000 });
      expect(host.participants.get(config.rootId)).toBeUndefined();
    }
  });

  it.each([false, true])("flushes only changed presence before the next full renewal (files=%s)", async files => {
    const { host, config, records } = await fixture(files);
    let now = Date.now(); vi.spyOn(Date, "now").mockImplementation(() => now);
    const full = vi.spyOn(host.participants, "refresh");
    const batch = vi.spyOn(host.mesh, "writeBatch");
    const sibling = records[1]!;
    const prior = readParticipantFile(config.meshRoot, participantKey(sibling.id))!.updatedAt;
    // Disable only the scheduled duplicate: the setter's awaited production flush runs.
    vi.spyOn(host.participants, "scheduleRefresh").mockImplementation(() => {});
    now += 5_000;
    await host.actors.setInstructions(records[0]!.id, "new persona");
    expect(full).not.toHaveBeenCalled();
    expect(batch).toHaveBeenCalledOnce();
    expect(batch.mock.calls[0]![0].ops.filter(op => op.key.startsWith("actors/"))).toHaveLength(1);
    expect(readParticipantFile(config.meshRoot, participantKey(sibling.id))!.updatedAt).toBe(prior);
    batch.mockClear(); now += 5_000;
    await host.participants.refresh();
    expect(full).toHaveBeenCalledOnce();
    expect(batch).toHaveBeenCalledOnce();
    expect(batch.mock.calls[0]![0].ops.filter(op => op.key.startsWith("actors/"))).toHaveLength(40);
    for (const row of records) {
      expect(readParticipantFile(config.meshRoot, participantKey(row.id))!.updatedAt).toBe(now);
    }
    expect(readHostLease(config.meshRoot, host.hostId)!.updatedAt).toBe(now);
  });

  it("does not spawn per-actor or change-triggered retries after a timed-out heartbeat", async () => {
    const { host, records } = await fixture();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    let entered!: () => void, release!: () => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { release = resolve; });
    const batch = vi.spyOn(host.mesh, "writeBatch").mockImplementationOnce(async () => {
      entered(); await held; throw new MeshLockTimeoutError(" held by pid 123", 5, 10);
    });
    const refreshing = host.participants.refresh();
    const rejected = expect(refreshing).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    await started;
    for (let actor = 0; actor < 40; actor++) host.participants.scheduleRefresh();
    release(); await rejected;
    // Even fresh actor writes and idle gates do not turn an outage into 40 retries.
    await Promise.all(records.map(row => host.actors.setInstructions(row.id, "changed during outage")));
    for (let poll = 0; poll < 40; poll++) {
      host.participants.scheduleRefresh();
      expect(host.participants.canConsumeMesh()).toBe(false);
    }
    await delay(1_100); // Past change-refresh throttle, before the next 5 s heartbeat.
    expect(batch).toHaveBeenCalledOnce();
    expect(warn.mock.calls.flat().join("\n")).not.toContain("actor presence");
    await host.participants.refresh();
    expect(batch).toHaveBeenCalledTimes(2);
    expect(batch.mock.calls[1]![0].ops.filter(op => op.key.startsWith("actors/"))).toHaveLength(40);
  });

  it("does not turn a failed change round into a second full heartbeat for its waiters", async () => {
    const { host, records } = await fixture();
    const mutationRefresh = vi.spyOn(host.participants, "refreshPresence").mockResolvedValue();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    let entered!: () => void, release!: () => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { release = resolve; });
    const batch = vi.spyOn(host.mesh, "writeBatch").mockImplementationOnce(async () => {
      entered(); await held; throw new MeshLockTimeoutError(" held by pid 123", 5, 10);
    });
    try {
      await host.actors.setInstructions(records[0]!.id, "changed");
      await started; // scheduled change-only publication now owns the registry fences
      const waiting = expect(host.participants.refresh()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      release(); await waiting;
      await delay(1_100);
      expect(batch).toHaveBeenCalledOnce(); expect(warn).toHaveBeenCalledOnce();
      await host.participants.refresh();
      expect(batch).toHaveBeenCalledTimes(2);
    } finally { release(); mutationRefresh.mockRestore(); }
  });

  it("retains newer pending presence when an older batch commits", async () => {
    const { host, records } = await fixture();
    vi.spyOn(host.participants, "refreshPresence").mockResolvedValue();
    vi.spyOn(host.participants, "scheduleRefresh").mockImplementation(() => {});
    const id = records[0]!.id;
    await host.actors.setInstructions(id, "first");
    const old = host.actors.presenceBatch(false);
    await host.actors.setInstructions(id, "latest");
    old.committed();
    const next = host.actors.presenceBatch(false);
    expect(next.ops.some(op => op.key.endsWith(`/${id}`) && op.kind === "put")).toBe(true);
    next.committed();
    expect(host.actors.presenceBatch(false).ops).toEqual([]);
  });

  it("commits removed actor presence and participant deletion before clearing cleanup", async () => {
    const { host, config, records } = await fixture();
    const id = records[0]!.id;
    expect(host.mesh.get(`actors/${config.sessionId}/${id}`, { fresh: true })).toBeDefined();
    expect(await host.actors.remove(id, { wait: true })).toMatchObject({ removed: true });
    expect(host.actors.cleanupObligation(id)).toBeUndefined();
    expect(host.mesh.get(`actors/${config.sessionId}/${id}`, { fresh: true })).toBeUndefined();
    expect(readParticipantFile(config.meshRoot, participantKey(id))).toBeUndefined();
    expect(new ActorRegistryStore(config.actorRoot).records().some(row => row.id === id)).toBe(false);
    expect(host.actors.listOwned()).toHaveLength(39);
  });

  it("uses fresh registry custody before batching legacy actor presence", async () => {
    const { host, config, records } = await fixture();
    const id = records[0]!.id, key = `actors/${config.sessionId}/${id}`;
    const prior = host.mesh.get(key, { fresh: true });
    const registry = new ActorRegistryStore(config.actorRoot);
    await registry.withLock(() => registry.write(registry.records().map(row => row.id === id
      ? { ...row, rootId: "session:successor", adoptedAt: Date.now(), adoptedFrom: [config.rootId] } : row)));
    const batch = vi.spyOn(host.mesh, "writeBatch"); await host.participants.refresh();
    expect(batch.mock.calls[0]![0].ops.some(op => op.kind === "put" && op.key === key)).toBe(false);
    expect(host.mesh.get(key, { fresh: true })).toEqual(prior);
    expect(host.actors.owns(id)).toBe(false);
  });

  it("checks active durable metadata in both scopes without building public records", async () => {
    const { host, records } = await fixture();
    const bindingReads = vi.spyOn(ActorBindingStore.prototype, "get");
    const messageReads = vi.spyOn(ActorRegistryStore.prototype, "messageCount");
    expect(host.actors.hasActiveDurableActor()).toBe(true);
    expect(bindingReads).not.toHaveBeenCalled(); expect(messageReads).not.toHaveBeenCalled();
    for (const row of records.filter(row => row.scope === "project")) await host.actors.cede(row.id);
    bindingReads.mockClear(); messageReads.mockClear();
    expect(host.actors.hasActiveDurableActor()).toBe(true); // secondary session scope
    expect(bindingReads).not.toHaveBeenCalled(); expect(messageReads).not.toHaveBeenCalled();
    for (const row of records.filter(row => row.scope === "session")) await host.actors.cede(row.id);
    expect(host.actors.hasActiveDurableActor()).toBe(false);
  });
});
