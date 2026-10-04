import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [], managers: ActorManager[] = [], agents: AgentManager[] = [];
const settle = async () => { for (let i = 0; i < 20; i++) await new Promise<void>(resolve => setImmediate(resolve)); };
afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(agents.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
function setup(records: Record<string, unknown>[] = [], actorQueueLimit = DEFAULT_FABRIC_CONFIG.mesh.actorQueueLimit) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-registry-writes-")); roots.push(root);
  const actorRoot = path.join(root, "actors"), store = new ActorRegistryStore(actorRoot);
  if (records.length) store.write(records);
  const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
  const mesh = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
  vi.spyOn(mesh, "put").mockResolvedValue({ key: "presence", value: {}, version: 1, updatedAt: Date.now(), updatedBy: identity });
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
  vi.spyOn(ActorMeshMonitor.prototype, "schedule").mockImplementation(() => {});
  const agent = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") }); agents.push(agent);
  vi.spyOn(agent, "status").mockReturnValue({ id: "mock-run", queuePosition: 1 } as ReturnType<AgentManager["status"]>);
  const manager = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorQueueLimit }, agent, () => {}, {
    actorRoot, persistent: true, rootId: identity.id, claimResidency: "session", reapDeadSessionPresence: false,
  }); managers.push(manager);
  return { manager, agent, root, actorRoot, store };
}

describe("actor registry lazy/status writes (#3752, #4383)", () => {
  it("honors an explicit clear after a legacy save strips the history reference", async () => {
    const id = "a".repeat(32), messages = [{ id: "m", source: "direct", createdAt: 1, direction: "in", text: "keep until clear" }];
    const f = setup([{ id, name: "mixed-clear", rootId: "session:test", instructions: "Review.", messages, createdAt: 1, status: "idle" }]);
    const record = f.store.records()[0]!;
    delete record.messageHistory;
    fs.writeFileSync(path.join(f.actorRoot, "actors.json"), JSON.stringify({ actors: [record] }));
    expect(f.manager.messages(id, 100)).toEqual(messages);
    await f.manager.clearMessages(id);
    expect(f.manager.messages(id, 100)).toEqual([]);
    expect(f.store.messages(f.store.records()[0]!)).toEqual([]);
    await f.manager.close();
    expect(new ActorRegistryStore(f.actorRoot).messages(f.store.records()[0]!)).toEqual([]);
  });


  it("loads histories only for their first consumer, not list/status/config saves/close", async () => {
    const id = "b".repeat(32), messages = Array.from({ length: 100 }, (_, i) => ({ id: `m-${i}`, source: "direct", createdAt: i, direction: "in", text: "x".repeat(1_100) }));
    const read = vi.spyOn(ActorRegistryStore.prototype, "messages");
    const f = setup([{ id, name: "lazy", rootId: "session:test", instructions: "Review.", messages, createdAt: 1, status: "idle" }]);
    for (let i = 0; i < 50; i++) { f.manager.list(); f.manager.status(id); }
    expect(f.manager.status(id).messages).toBe(100);
    expect(read).not.toHaveBeenCalled();
    await f.manager.setNice(id, 7);
    expect(read).not.toHaveBeenCalled();
    expect(f.manager.messages(id, 5).map(message => message.id)).toEqual(messages.slice(-5).map(message => message.id));
    expect(read).toHaveBeenCalledTimes(1);
    f.manager.messages(id, 5);
    expect(read).toHaveBeenCalledTimes(1);
    await f.manager.close();
    expect(read).toHaveBeenCalledTimes(1);
  });

  it("archives every message in a synchronous burst larger than the active ring", async () => {
    const f = setup([], 500);
    vi.spyOn(f.agent, "run").mockImplementation((_request, signal, onSpawned) => {
      onSpawned?.({ id: "mock-run" } as Parameters<NonNullable<typeof onSpawned>>[0]);
      return new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(new Error("fixture abort")), { once: true }));
    });
    const actor = await f.manager.create({ name: "burst", instructions: "Review." });
    for (let i = 0; i < 250; i++) f.manager.tell(actor.id, `burst-${i}`);
    await settle();
    expect(f.manager.status(actor.id).messages).toBe(100);
    const log = fs.readFileSync(path.join(f.actorRoot, actor.id, "registry", "messages.jsonl"), "utf8");
    const archived = log.split("\n").filter(Boolean).flatMap(line => JSON.parse(line).messages);
    const inputs = new Set(archived.map((message: { data?: { message?: string } }) => message.data?.message).filter((text: string | undefined) => text?.startsWith("burst-")));
    expect(inputs.size).toBe(250);
    const record = f.store.records()[0]!;
    expect(record.registryMessageAppend).toBeUndefined();
    expect(f.store.messages(record)).toHaveLength(100);
  });

  it("coalesces native worker status pulses before acquiring locks and flushes the latest state on close", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(1_800_000_000_000);
    const f = setup();
    let running: (() => void) | undefined, waiting: (() => void) | undefined;
    vi.spyOn(f.agent, "run").mockImplementation((_request, signal, onSpawned, _authorize, _downgrade, onQueued) => {
      const handle = { id: "mock-run" } as Parameters<NonNullable<typeof onSpawned>>[0];
      running = () => onSpawned?.(handle); waiting = () => onQueued?.(handle);
      running();
      return new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(new Error("fixture abort")), { once: true }));
    });
    const actor = await f.manager.create({ name: "pulse", instructions: "Review." });
    f.manager.tell(actor.id, "one activation, many status transitions");
    await settle();
    expect(running).toBeTypeOf("function");
    const write = vi.spyOn(ActorRegistryStore.prototype, "write"), lock = vi.spyOn(ActorRegistryStore.prototype, "withLock");
    for (let second = 1; second <= 12; second++) {
      vi.setSystemTime(1_800_000_000_000 + second * 1_000);
      (second % 2 ? waiting : running)!();
      await settle();
    }
    expect(write).toHaveBeenCalledTimes(2);
    expect(lock).toHaveBeenCalledTimes(2);
    expect(f.manager.status(actor.id).status).toBe("running");
    const writesBeforeConfig = write.mock.calls.length;
    await f.manager.setTools(actor.id, ["read"]);
    expect(write.mock.calls.length).toBe(writesBeforeConfig + 1); // Config bypasses the window.
    waiting!(); await settle();
    await f.manager.close();
    const persisted = f.store.records().find(row => row.id === actor.id)!;
    expect(persisted.status).toBe("idle");
    expect(persisted.tools).toEqual(["read"]);
    expect(write.mock.calls.length).toBeGreaterThan(writesBeforeConfig + 1);
  });

  it("flushes a quiet pending status at the timer boundary, without another callback", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const f = setup();
    let waiting: (() => void) | undefined;
    vi.spyOn(f.agent, "run").mockImplementation((_request, signal, onSpawned, _authorize, _downgrade, onQueued) => {
      const handle = { id: "mock-run" } as Parameters<NonNullable<typeof onSpawned>>[0];
      waiting = () => onQueued?.(handle); onSpawned?.(handle);
      return new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(new Error("fixture abort")), { once: true }));
    });
    const actor = await f.manager.create({ name: "timer", instructions: "Review." });
    f.manager.tell(actor.id, "activate"); await settle();
    const write = vi.spyOn(ActorRegistryStore.prototype, "write");
    waiting!(); await settle();
    expect(write).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(4_999); await settle();
    expect(write).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1); await settle();
    expect(write).toHaveBeenCalledTimes(1);
    expect(f.store.records()[0]!.status).toBe("waiting");
  });

  it("a pending status save does not overwrite a foreign lineage/config update", async () => {
    vi.useFakeTimers({ toFake: ["Date"] }); vi.setSystemTime(1_800_000_000_000);
    const f = setup();
    let waiting: (() => void) | undefined;
    vi.spyOn(f.agent, "run").mockImplementation((_request, signal, onSpawned, _authorize, _downgrade, onQueued) => {
      const handle = { id: "mock-run" } as Parameters<NonNullable<typeof onSpawned>>[0];
      waiting = () => onQueued?.(handle); onSpawned?.(handle);
      return new Promise((_resolve, reject) => signal?.addEventListener("abort", () => reject(new Error("fixture abort")), { once: true }));
    });
    const actor = await f.manager.create({ name: "pulse", instructions: "Review." });
    f.manager.tell(actor.id, "activate"); await settle();
    waiting!(); await settle(); // Deferred, no registry lock.
    const foreign = { id: "c".repeat(32), name: "foreign", rootId: "session:other", instructions: "Keep.", createdAt: 1, status: "stopped", futureField: true };
    await f.store.withLock(() => f.store.write([...f.store.records(), foreign]));
    await f.manager.close();
    expect(f.store.records().find(row => row.id === foreign.id)).toEqual(foreign);
    expect(f.store.records().find(row => row.id === actor.id)?.status).toBe("idle");
  });
});
