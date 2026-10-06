import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

const roots: string[] = [], hosts: ResidentHost[] = [];
const actors: ActorManager[] = [], agents: AgentManager[] = [];
const nativeReads: Promise<unknown>[] = [];
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const root = () => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-identity-deadline-")); roots.push(value); return value;
};
afterEach(async () => {
  // Join even a deliberately non-cooperative mock before closing/removing fixtures.
  await Promise.all(nativeReads.splice(0));
  vi.restoreAllMocks();
  await Promise.all(actors.splice(0).map(manager => manager.close()));
  await Promise.all(agents.splice(0).map(manager => manager.close()));
  await Promise.all(hosts.splice(0).map(host => host.close()));
  for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true });
});
const slowRead = (value: string) => {
  const pending = pause(1200).then(() => value); nativeReads.push(pending); return pending;
};
const holder = (mesh: MeshStore, incarnation: string) => {
  const lock = path.join(mesh.root, ".lock"); fs.mkdirSync(lock);
  const receipt = `external\n${process.pid}\n${Date.now()}\n${incarnation}\n`;
  fs.writeFileSync(path.join(lock, "owner"), receipt);
  return { lock, receipt, inode: fs.statSync(lock).ino };
};
const intact = (mesh: MeshStore, held: ReturnType<typeof holder>) => {
  expect(fs.readFileSync(path.join(held.lock, "owner"), "utf8")).toBe(held.receipt);
  expect(fs.statSync(held.lock).ino).toBe(held.inode);
  expect(fs.readdirSync(mesh.root).filter(name => name.startsWith(".lock.dead."))).toEqual([]);
};

describe("native identity cannot retain registry custody past a bounded mesh try", () => {
  for (const protocol of [1, 2] as const) for (const budget of [0, 50]) {
    it(`four-line holder, protocol ${protocol}, ${budget === 0 ? "adoption zero-ms" : "publication 50-ms"}`, async () => {
      const base = root(), registry = new ActorRegistryStore(path.join(base, "actors"));
      const mesh = new MeshStore(path.join(base, "mesh"), 65536, 100, { lockProtocol: protocol });
      await mesh.exclusive(() => undefined); // own preparation outside registry custody
      const incarnation = (await atomic.processIncarnation(process.pid))!;
      expect(incarnation).toBeDefined();
      const held = holder(mesh, incarnation);
      const read = vi.spyOn(atomic, "processIncarnation").mockImplementation(() => slowRead(incarnation));
      let selected!: () => void;
      const selectedPromise = new Promise<void>(resolve => { selected = resolve; });
      const attempt = registry.withLock(() => {
        selected();
        return budget === 0 ? mesh.exclusive(() => { throw new Error("must not enter"); }, 0)
          : mesh.withTryLock(() => mesh.confirmWritable(), 50);
      }).catch(error => error);
      await selectedPromise;
      const begin = performance.now();
      await registry.withLock(() => registry.write([]));
      const registryBlockedMs = performance.now() - begin;
      expect(await attempt).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(registryBlockedMs).toBeLessThan(150);
      expect(read).not.toHaveBeenCalled(); // no command-backed evidence under either custody try
      intact(mesh, held);
      await Promise.all(nativeReads); // late evidence cannot recover the holder
      intact(mesh, held);
      console.log(JSON.stringify({ protocol, budgetMs: budget, simulatedNativeMs: 1200, registryBlockedMs, lateRecovery: false }));
    });
  }

  it.each([1, 2] as const)("late different-incarnation evidence cannot escape a timed-out attempt (protocol=%s)", async protocol => {
    const mesh = new MeshStore(path.join(root(), "mesh"), 65536, 100, { lockProtocol: protocol });
    await mesh.exclusive(() => undefined);
    const incarnation = (await atomic.processIncarnation(process.pid))!;
    const held = holder(mesh, incarnation);
    const different = process.platform === "linux" ? String(BigInt(incarnation) + 1n)
      : process.platform === "win32" ? "win32:1" : "darwin:Wed Sep 30 12:00:00 2026";
    vi.spyOn(atomic, "processIncarnation").mockImplementation(() => slowRead(different));
    // Ordinary outside-custody evidence still obeys the acquisition deadline.
    await expect(mesh.exclusive(() => undefined, 50)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    await Promise.all(nativeReads);
    intact(mesh, held);
  });

  it.each([1, 2] as const)("actual ActorManager adoption skips a slow identity read and releases the registry immediately (protocol=%s)", async protocol => {
    const base = root(), actorRoot = path.join(base, "actors");
    const registry = new ActorRegistryStore(actorRoot);
    const id = "1".padStart(32, "0");
    registry.write([{ id, name: "orphan", instructions: "Wait", rootId: "session:dead", residency: "session", runner: "pi",
      createdAt: Date.now(), updatedAt: Date.now(), status: "idle", events: [], topics: [], messages: [] }]);
    const mesh = new MeshStore(path.join(base, "mesh"), 65536, 100, { lockProtocol: protocol });
    await mesh.exclusive(() => undefined);
    const incarnation = (await atomic.processIncarnation(process.pid))!;
    const held = holder(mesh, incarnation);
    const read = vi.spyOn(atomic, "processIncarnation").mockImplementation(() => slowRead(incarnation));
    let entered!: () => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    const exclusive = mesh.exclusive.bind(mesh);
    const attempt = vi.spyOn(mesh, "exclusive").mockImplementation((operation, timeout) => {
      expect(fs.existsSync(path.join(actorRoot, "actors.json.lock", "owner"))).toBe(true);
      expect(timeout).toBe(0); entered(); return exclusive(operation, timeout);
    });
    const agentManager = new AgentManager(base, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(base, "runs"),
    }); agents.push(agentManager);
    const identity = { id: "session:successor", name: "successor", kind: "main" as const, sessionId: "successor" };
    const manager = new ActorManager("successor", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agentManager, () => {}, {
      actorRoot, persistent: true, claimResidency: "session", rootId: identity.id, canManageActor: () => undefined, lineageAlive: () => false,
    }); actors.push(manager);
    try {
      await started;
      const begin = performance.now();
      await registry.withLock(() => registry.write(registry.records().map(row => ({ ...row, marker: "concurrent setter" }))));
      const registryBlockedMs = performance.now() - begin;
      expect(registryBlockedMs).toBeLessThan(150);
      expect(read).not.toHaveBeenCalled();
      expect(registry.records()[0]!.rootId).toBe("session:dead");
      intact(mesh, held);
      await pause(200); expect(attempt).toHaveBeenCalledOnce(); // retries remain outside custody
      console.log(JSON.stringify({ entrypoint: "ActorManager adoption", protocol, simulatedNativeMs: 1200, registryBlockedMs, nativeReads: 0 }));
    } finally { attempt.mockRestore(); await manager.close(); fs.rmSync(held.lock, { recursive: true, force: true }); }
  });

  it("cold protocol-2 prepares self once outside custody; bounded tries fail promptly until ready", async () => {
    const base = root(), registry = new ActorRegistryStore(path.join(base, "actors"));
    const incarnation = (await atomic.processIncarnation(process.pid))!;
    const owner = path.join(base, "actors", "actors.json.lock", "owner");
    let ownSawRegistryCustody = false;
    const own = vi.spyOn(atomic, "ownProcessIncarnation").mockImplementation(() => {
      ownSawRegistryCustody ||= fs.existsSync(owner);
      return slowRead(incarnation);
    });
    const mesh = new MeshStore(path.join(base, "mesh"), 65536, 100, { lockProtocol: 2 });
    const begin = performance.now();
    await registry.withLock(async () => {
      await expect(mesh.withTryLock(() => mesh.confirmWritable(), 50)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      await expect(mesh.exclusive(() => undefined, 0)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    });
    const registryHeldMs = performance.now() - begin;
    expect(registryHeldMs).toBeLessThan(150);
    expect(fs.existsSync(path.join(mesh.root, ".lock"))).toBe(false);
    await registry.withLock(() => registry.write([]));
    await Promise.all(nativeReads);
    await registry.withLock(() => mesh.withTryLock(() => mesh.exclusive(() => {
      expect(fs.readFileSync(path.join(mesh.root, ".lock", "owner"), "utf8").split("\n")[3]).toBe(incarnation);
    }), 50));
    expect(own).toHaveBeenCalledOnce();
    expect(ownSawRegistryCustody).toBe(false);
    console.log(JSON.stringify({ acquisition: "cold protocol-2", simulatedNativeMs: 1200, registryHeldMs, ownReads: 1, ownSawRegistryCustody }));
  });

  it.each([1, 2] as const)("real resident setters remain available during slow four-line-holder lookup (protocol=%s)", async protocol => {
    const base = root();
    const config: ResidentHostConfig = {
      format: 1, rootId: "session:identity-absent-main", sessionId: "identity-absent-main", cwd: base, projectRoot: base,
      meshRoot: path.join(base, "mesh"), actorRoot: path.join(base, "actors"), sessionActorRoot: path.join(base, "session-actors"),
      residencyRoot: residentRoot(path.join(base, "mesh"), "session:identity-absent-main"), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, lockProtocol: protocol },
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
    };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    const rows = [config.actorRoot, config.sessionActorRoot!].map((actorRoot, i) => {
      fs.mkdirSync(actorRoot, { recursive: true });
      const row = { id: String(i + 1).padStart(32, "0"), name: `idle-${i}`, instructions: "wait", createdAt: Date.now(), updatedAt: Date.now(),
        rootId: config.rootId, residency: "durable", runner: "pi", status: "idle", scope: i ? "session" : "project", events: [], topics: [], messages: [] };
      fs.writeFileSync(path.join(actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: [row] })); return row;
    });
    const host = new ResidentHost(config); hosts.push(host); await host.start(); await pause(20); await host.participants.refresh();
    expect(host.actors.listOwned()).toHaveLength(2);
    const incarnation = (await atomic.processIncarnation(process.pid))!;
    const held = holder(host.mesh, incarnation);
    let entered!: () => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    const read = vi.spyOn(atomic, "processIncarnation").mockImplementation(() => slowRead(incarnation));
    const writeBatch = host.mesh.writeBatch.bind(host.mesh);
    vi.spyOn(host.mesh, "writeBatch").mockImplementation(input => {
      for (const actorRoot of [config.actorRoot, config.sessionActorRoot!]) {
        expect(fs.existsSync(path.join(actorRoot, "actors.json.lock", "owner"))).toBe(true);
      }
      entered(); return writeBatch(input);
    });
    const refresh = host.participants.refresh().catch(error => error);
    try {
      await started;
      const begin = performance.now();
      await Promise.all(rows.map(row => host.actors.setInstructions(row.id, "durably changed during native lookup")));
      const setterMs = performance.now() - begin;
      expect(await refresh).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(read).not.toHaveBeenCalled();
      for (const actorRoot of [config.actorRoot, config.sessionActorRoot!]) {
        expect(new ActorRegistryStore(actorRoot).records()[0]!.instructions).toBe("durably changed during native lookup");
      }
      expect(setterMs).toBeLessThan(150);
      intact(host.mesh, held); await Promise.all(nativeReads); intact(host.mesh, held);
      console.log(JSON.stringify({ entrypoint: "ResidentHost refresh -> real setters in both scopes", protocol, simulatedNativeMs: 1200, setterMs, mutationDurable: true }));
    } finally { await refresh; fs.rmSync(held.lock, { recursive: true, force: true }); }
  }, 10000);
});
