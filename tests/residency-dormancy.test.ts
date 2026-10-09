import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { ARCHIVE_PENDING_FILE } from "../src/agents/archive-custody.js";
import { MeshStore } from "../src/mesh/store.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { readWakeJson, residentSleepingPath, residentWakeRequestPath, wakeResidentActors } from "../src/residency/wake.js";
import { superviseWake } from "../src/residency/launcher.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const until = async (done: () => boolean, ms = 8_000) => {
  const started = performance.now();
  while (!done() && performance.now() - started < ms) await sleep(20);
  expect(done()).toBe(true);
};
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-dormant-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:dormancy", sessionId: "dormancy", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
    residencyRoot: residentRoot(path.join(root, "mesh"), "session:dormancy"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh },
    retention: { ...DEFAULT_FABRIC_CONFIG.retention }, workerPath: path.resolve("dist/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const idle = vi.fn();
  const host = new ResidentHost(config, idle);
  return { root, config, host, idle };
};
const fakeRun = (host: ResidentHost, consume: (task: string) => Promise<void> = async () => {}) =>
  vi.spyOn(host.agents, "run").mockImplementation(async (request, _signal, onSpawned) => {
    const id = "1".repeat(32);
    onSpawned?.({ id } as never);
    await consume(request.task);
    return { id, status: "completed", text: "done", toolCalls: 0, startedAt: Date.now(), finishedAt: Date.now() } as never;
  });

// In-process host tests still take the real kernel fence on Linux.
describe("resident dormancy (smarty-dev#6782 / #2264)", () => {
  it("marks an idle actor dormant without losing its subscriptions, then reaches clean host exit", async () => {
    const { root, config, host, idle } = fixture();
    try {
      await host.start();
      const actor = await host.actors.create({ name: "listener", instructions: "wait", residency: "durable", topics: ["test.wake"] });
      await until(() => host.actors.status(actor.id).status === "dormant");
      const record = new ActorRegistryStore(config.actorRoot).records().find(row => row.id === actor.id);
      expect(record?.status).toBe("dormant");
      expect(record?.topics).toEqual(["test.wake"]);
      expect(host.actors.hasActiveDurableActor()).toBe(false);
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 31_000);
      await until(() => idle.mock.calls.length === 1);
      await host.close();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(config.residencyRoot, "config.json"))).toBe(true);
      const archive = readWakeJson<{ version: number; dir: string }>(path.join(config.meshRoot, "event-archive.json"));
      expect(archive?.version).toBe(1);
      expect(fs.statSync(archive!.dir).isDirectory()).toBe(true);
      expect(fs.existsSync(path.join(config.residencyRoot, "wake-routes.json"))).toBe(true);
    } finally { vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("never sleeps an activation with a pending ask/reply", async () => {
    const { root, host, idle } = fixture();
    let reply!: () => void;
    const gate = new Promise<void>(resolve => { reply = resolve; });
    let ask: Promise<unknown> | undefined;
    try {
      await host.start();
      fakeRun(host, async () => gate);
      const actor = await host.actors.create({ name: "asking", instructions: "work", residency: "durable" });
      ask = host.actors.ask(actor.id, "wait for reply");
      await until(() => host.actors.inFlightCount() === 1);
      expect(await host.actors.dormantIdleActors()).toBe(0);
      const now = Date.now();
      vi.spyOn(Date, "now").mockImplementation(() => now + 60_000);
      await sleep(300);
      expect(idle).not.toHaveBeenCalled();
      expect(host.actors.status(actor.id).status).not.toBe("dormant");
      reply(); await ask;
    } finally { reply(); await ask; vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("retains a pending child reply even when the spawning activation has ended", async () => {
    const { root, config, host } = fixture();
    try {
      await host.start();
      const actor = await host.actors.create({ name: "child-reply", instructions: "wait", residency: "durable" });
      const childId = "2".repeat(32);
      const directory = path.join(root, childId);
      fs.mkdirSync(directory);
      fs.writeFileSync(path.join(directory, ARCHIVE_PENDING_FILE), JSON.stringify({ format: 1, awaitingResult: true, ownerPid: process.pid }));
      const store = new ActorChildCompletionStore(path.join(config.actorRoot, actor.id, "session.jsonl"));
      store.trackArchiveSource(childId, directory);
      expect(store.hasPendingReply()).toBe(true);
      expect(await host.actors.dormantIdleActors()).toBe(0);
      await sleep(1_200);
      expect(host.actors.status(actor.id).status).toBe("idle");
      fs.rmSync(path.join(directory, ARCHIVE_PENDING_FILE));
      store.releaseArchiveSource(childId);
      expect(await host.actors.dormantIdleActors()).toBe(1);
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("keeps a live Main's supervisor expected, even between activations", async () => {
    const { root, config, host } = fixture();
    try {
      await host.start();
      const actor = await host.actors.create({ name: "supervisor", instructions: "supervise", events: ["agent_settled"], residency: "durable" });
      const get = host.participants.get.bind(host.participants);
      vi.spyOn(host.participants, "get").mockImplementation((id, ...rest) => id === config.rootId
        ? { id: config.rootId, kind: "root", rootId: config.rootId } as never : get(id, ...rest));
      await sleep(1_300);
      expect(host.actors.status(actor.id).status).toBe("idle");
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("queues deliveries throughout the wake window and drains exactly once, in order, across two sleeps", async () => {
    const { root, config, host } = fixture();
    let resumed: ResidentHost | undefined;
    let again: ResidentHost | undefined;
    const seen: number[] = [];
    try {
      await host.start();
      const actor = await host.actors.create({ name: "wake-order", instructions: "process", residency: "durable", topics: ["test.wake"], coalesce: false });
      await until(() => host.actors.status(actor.id).status === "dormant");
      // Retain both scope cursors exactly as clean idle exit does.
      host.actors.pauseForRelease();
      await host.actors.checkpointForRelease();
      await host.close();
      const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
      const launch = vi.fn(async () => {}); // hold boot: delivery must already be durable
      const events = [];
      // Avoid the real spawn in this unit test; exercise the same post-commit router explicitly.
      const wake = await import("../src/residency/wake.js");
      const dispatch = wake.wakeResidentActors;
      const routed = vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, batch) => dispatch(store, batch, launch));
      for (let n = 1; n <= 3; n++) events.push(await mesh.publish({ topic: "test.wake", from: { id: "publisher", name: "publisher", kind: "main" }, data: { n } }));
      expect(launch).toHaveBeenCalledTimes(3);
      expect(mesh.read().filter(event => event.topic === "test.wake").map(event => (event.data as { n: number }).n)).toEqual([1, 2, 3]);
      expect(readWakeJson<{ sequence: number }>(residentWakeRequestPath(config.residencyRoot))?.sequence).toBe(events[2]!.sequence);
      resumed = new ResidentHost(config, () => {});
      await resumed.start();
      fakeRun(resumed, async task => {
        const match = task.match(/"n"\s*:\s*(\d+)/);
        if (match) seen.push(Number(match[1]));
      });
      await until(() => seen.length === 3);
      expect(seen).toEqual([1, 2, 3]);
      await until(() => resumed!.actors.status(actor.id).status === "dormant");
      resumed.actors.pauseForRelease(); await resumed.actors.checkpointForRelease(); await resumed.close();
      again = new ResidentHost(config, () => {});
      await again.start();
      const runs = fakeRun(again);
      await sleep(400);
      expect(runs).not.toHaveBeenCalled();
      expect(seen).toEqual([1, 2, 3]);
      routed.mockRestore(); mesh.closeState();
    } finally { await host.close(); await resumed?.close(); await again?.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("admits a direct dormant message, commits before wake, then gets a real owner ACK and one actor activation", async () => {
    type Ports = ConstructorParameters<typeof AgentMessageRouter>;
    const { root, config, host } = fixture();
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const identity = { id: "session:sender", name: "sender", kind: "main" as const };
    const sender = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 10, acknowledgementTimeoutMs: 5_000 });
    let woken: ResidentHost | undefined;
    const seen: string[] = [];
    try {
      await host.start();
      const created = await host.actors.create({ name: "direct-listener", instructions: "process", residency: "durable" });
      await until(() => host.actors.status(created.id).status === "dormant");
      const actor = host.actors.status(created.id);
      host.actors.pauseForRelease(); await host.actors.checkpointForRelease(); await host.close();
      const routes = readWakeJson<{ actors: Array<{ participant?: { ownerHostId: string; actorOwnershipToken?: string } }> }>(path.join(config.residencyRoot, "wake-routes.json"));
      expect(routes?.actors[0]?.participant?.actorOwnershipToken).toBe(actor.ownershipToken);
      expect(routes?.actors[0]?.participant?.ownerHostId).toBe(host.hostId);
      sender.start(async () => ({ accepted: false, error: "sender does not own actors" }));
      const wake = await import("../src/residency/wake.js");
      const dispatch = wake.wakeResidentActors;
      let launches = 0;
      vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, events) => dispatch(store, events, async () => {
        launches++;
        expect(mesh.read().some(event => event.topic === "fabric.control.command" && (event.data as { targetId?: string }).targetId === actor.id)).toBe(true);
        const { AgentManager } = await import("../src/agents/manager.js");
        vi.spyOn(AgentManager.prototype, "run").mockImplementation(async (request, _signal, onSpawned) => {
          const id = "1".repeat(32); onSpawned?.({ id } as never); seen.push(request.task);
          return { id, status: "completed", text: "done", toolCalls: 0, startedAt: Date.now(), finishedAt: Date.now() } as never;
        });
        woken = new ResidentHost(config, () => {});
        await woken.start();
      }));
      const actors = { identity, mesh, owns: () => false, status: (id: string) => {
        if (id !== actor.id) throw new Error(`Unknown Fabric actor: ${id}`); return actor;
      }, validateDirectMessage: host.actors.validateDirectMessage.bind(host.actors), resolveBinding: () => ({}),
      resolveActivationBinding: vi.fn(), tell: vi.fn(), ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn() };
      const router = new AgentMessageRouter({ status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0],
        actors as Ports[1], { id: identity.id, local: true, matches: (id: string) => id === identity.id, deliverAgent: vi.fn() } as Ports[2],
        { get: () => undefined, scheduleRefresh: vi.fn(), lastKnown: () => undefined } as Ports[3], sender, binding => binding);
      const result = await router.routeMessage(actor.id, "admitted direct delivery", undefined, "followUp");
      expect(result.acknowledged).toBe(true);
      expect(launches).toBe(1);
      await until(() => seen.length === 1);
      expect(seen[0]).toContain("admitted direct delivery");
      await sleep(100);
      expect(seen).toHaveLength(1);
      expect(actors.tell).not.toHaveBeenCalled();
    } finally { vi.restoreAllMocks(); await sender.close(); await host.close(); await woken?.close(); mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 15_000);

  it.skipIf(process.platform === "win32")("a delivery crossing the final sleep boundary starts exactly one successor generation", async () => {
    const { root, config } = fixture();
    const configPath = path.join(config.residencyRoot, "config.json");
    try {
      fs.writeFileSync(residentWakeRequestPath(config.residencyRoot), JSON.stringify({ id: "first" }));
      let runs = 0;
      await superviseWake(configPath, async () => {
        runs++;
        fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: { id: runs === 1 ? "first" : "racing" } }));
        if (runs === 1) fs.writeFileSync(residentWakeRequestPath(config.residencyRoot), JSON.stringify({ id: "racing" }));
      });
      expect(runs).toBe(2);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform === "win32").each(["final release", "covered startup"] as const)("a commit after equal snapshots at %s but before wake.lock release wakes and drains the actor exactly once", async phase => {
    const { root, config, host } = fixture();
    const configPath = path.join(config.residencyRoot, "config.json");
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    let successor: ResidentHost | undefined;
    let publication: Promise<unknown> | undefined;
    let committed!: () => void;
    const commit = new Promise<void>(resolve => { committed = resolve; });
    let launches = 0;
    const seen: string[] = [];
    const wake = await import("../src/residency/wake.js");
    try {
      await host.start();
      const actor = await host.actors.create({ name: "final-window", instructions: "process", residency: "durable", topics: ["test.final-window"], coalesce: false });
      await until(() => host.actors.status(actor.id).status === "dormant");
      host.actors.pauseForRelease(); await host.actors.checkpointForRelease(); await host.close();
      fs.writeFileSync(residentWakeRequestPath(config.residencyRoot), JSON.stringify({ id: "covered" }));
      fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: { id: "covered" } }));
      const dispatch = wake.wakeResidentActors;
      vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, events) => {
        committed(); // MeshStore calls here only AFTER the event durability barrier.
        return dispatch(store, events, async () => {
          await superviseWake(configPath, async () => {
            launches++;
            successor = new ResidentHost(config, () => {});
            const { AgentManager } = await import("../src/agents/manager.js");
            vi.spyOn(AgentManager.prototype, "run").mockImplementation(async (request, _signal, onSpawned) => {
              const id = "1".repeat(32);
              onSpawned?.({ id } as never);
              seen.push(request.task);
              return { id, status: "completed", text: "done", toolCalls: 0, startedAt: Date.now(), finishedAt: Date.now() } as never;
            });
            await successor.start();
            await until(() => seen.length === 1);
            await until(() => successor!.actors.status(actor.id).status === "dormant");
            successor.actors.pauseForRelease(); await successor.actors.checkpointForRelease(); await successor.close();
            fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: readWakeJson(residentWakeRequestPath(config.residencyRoot)) }));
          }, { wakeOnly: true });
        });
      });
      await superviseWake(configPath, async () => {
        fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: { id: "covered" } }));
      }, { wakeOnly: phase === "covered startup", beforeFinalRelease: async () => {
        // Exact rejected-review window: equality has already been observed; wake.lock is held.
        publication = mesh.publish({ topic: "test.final-window", from: { id: "publisher", name: "publisher", kind: "main" }, data: { n: 7 } });
        await commit;
        expect(mesh.read().filter(event => event.topic === "test.final-window")).toHaveLength(1);
        expect(readWakeJson<{ id: string }>(residentWakeRequestPath(config.residencyRoot))?.id).toBe("covered");
      } });
      await publication;
      expect(launches).toBe(1);
      expect(seen).toHaveLength(1);
      expect(seen[0]).toMatch(/"n"\s*:\s*7/);
      // Another start uses the saved queue/cursor and cannot repeat that actor activation.
      vi.restoreAllMocks();
      await successor?.close();
      successor = new ResidentHost(config, () => {});
      const { AgentManager } = await import("../src/agents/manager.js");
      const repeated = vi.spyOn(AgentManager.prototype, "run").mockResolvedValue({ status: "completed", text: "unexpected replay" } as never);
      await successor.start();
      await sleep(150);
      expect(repeated).not.toHaveBeenCalled();
    } finally {
      await publication; vi.restoreAllMocks(); await host.close(); await successor?.close(); mesh.closeState();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);

  it.skipIf(process.platform === "win32")("serializes wake launchers and never leaves one warm after the sleep boundary", async () => {
    const { root, config } = fixture();
    const configPath = path.join(config.residencyRoot, "config.json");
    let finish!: () => void;
    const gate = new Promise<void>(resolve => { finish = resolve; });
    let active: Promise<void> | undefined;
    try {
      fs.writeFileSync(residentWakeRequestPath(config.residencyRoot), JSON.stringify({ id: "first" }));
      const run = vi.fn(async () => {
        await gate;
        fs.writeFileSync(residentSleepingPath(config.residencyRoot), JSON.stringify({ request: { id: "first" } }));
      });
      active = superviseWake(configPath, run);
      await until(() => run.mock.calls.length === 1);
      await superviseWake(configPath, run);
      expect(run).toHaveBeenCalledTimes(1);
      finish(); await active;
      expect(run).toHaveBeenCalledTimes(1);
      expect(processStartTime(process.pid)).toBeDefined();
    } finally { finish(); await active; fs.rmSync(root, { recursive: true, force: true }); }
  });
});
