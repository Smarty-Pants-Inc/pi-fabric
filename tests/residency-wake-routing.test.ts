import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { residentRoot } from "../src/residency/protocol.js";
import { processStartTime } from "../src/residency/process-identity.js";
import * as wake from "../src/residency/wake.js";
import { canonicalResidentWakeConfig } from "../src/residency/wake-index.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";

describe("resident event wake routing", () => {
  it("enables existing file-archive retention before sleep and never replaces an operator archive", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-archive-"));
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100);
    try {
      await wake.ensureResidentWakeArchive(mesh);
      const file = path.join(mesh.root, "event-archive.json");
      expect(wake.readWakeJson<{ dir: string }>(file)?.dir).toBe(path.join(mesh.root, "wake-archive"));
      const selected = path.join(root, "operator-archive");
      fs.mkdirSync(selected);
      fs.writeFileSync(file, JSON.stringify({ version: 1, dir: selected }));
      const original = fs.readFileSync(file, "utf8");
      await wake.ensureResidentWakeArchive(mesh);
      expect(fs.readFileSync(file, "utf8")).toBe(original);
      await mesh.publish({ topic: "retained", from: { id: "publisher", name: "publisher", kind: "main" } });
      expect(fs.existsSync(path.join(selected, "HEAD.json"))).toBe(true);
    } finally { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); }
  });
  it("wakes on topics, direct steer/followUp and lifecycle subscriptions only after durable commit", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-routing-"));
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100);
    const resident = residentRoot(mesh.root, "session:listener");
    fs.mkdirSync(resident, { recursive: true });
    const actorId = "a".repeat(32);
    const config = { rootId: "session:listener", residencyRoot: resident, cwd: root, fabricExtensionPath: path.resolve("dist/index.js") };
    fs.writeFileSync(path.join(resident, "config.json"), JSON.stringify(config));
    fs.writeFileSync(path.join(resident, "wake-routes.json"), JSON.stringify({ format: 1, rootId: config.rootId, configJson: canonicalResidentWakeConfig(config),
      hostId: "host:listener", actors: [{ id: actorId, name: "listener", topics: ["test.topic"] }] }));
    const launch = vi.fn(async () => {
      const request = wake.readWakeJson<{ sequence: number }>(wake.residentWakeRequestPath(resident));
      expect(mesh.read().some(event => event.sequence === request?.sequence)).toBe(true);
    });
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, events) => dispatch(store, events, launch));
    const from = { id: "publisher", name: "publisher", kind: "main" as const };
    try {
      await mesh.publish({ topic: "unrelated", from });
      expect(launch).not.toHaveBeenCalled();
      await mesh.publish({ topic: "test.topic", from });
      expect(launch).toHaveBeenCalledTimes(1);
      for (const operation of ["steer", "followUp"]) {
        await mesh.publish({ topic: "fabric.control.command", to: "host:listener", from, data: { targetId: actorId, operation } });
      }
      expect(launch).toHaveBeenCalledTimes(3);
      await mesh.put({ key: "topology/subscriptions/one", identity: from,
        value: { from: "child", to: actorId, events: ["run.completed"] } });
      await mesh.publish({ topic: "fabric.participant.lifecycle", from,
        data: { source: { id: "child" }, event: "run.completed" } });
      expect(launch).toHaveBeenCalledTimes(4);
      // A warm host consumes ordinarily; a final-close host needs a nudge even though its PID is live.
      fs.writeFileSync(path.join(resident, "owner.json"), JSON.stringify({ pid: process.pid, processStartTime: processStartTime(process.pid), token: "closing" }));
      await mesh.publish({ topic: "test.topic", from });
      expect(launch).toHaveBeenCalledTimes(4);
      fs.writeFileSync(wake.residentSleepingPath(resident), JSON.stringify({ token: "closing" }));
      await mesh.publish({ topic: "test.topic", from });
      expect(launch).toHaveBeenCalledTimes(5);
    } finally { vi.restoreAllMocks(); mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  const directFixture = () => {
    type Ports = ConstructorParameters<typeof AgentMessageRouter>;
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-admitted-wake-"));
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100);
    const actorId = "b".repeat(32);
    const actor = { id: actorId, rootId: "session:other", ownershipToken: "lineage", residency: "durable", status: "dormant", name: "listener", runner: "pi" };
    const resident = residentRoot(mesh.root, actor.rootId);
    fs.mkdirSync(resident, { recursive: true });
    const participant = { format: 1, id: actorId, rootId: actor.rootId, kind: "actor", name: actor.name, runner: "pi", transport: "host",
      residency: "durable", status: "dormant", actorOwnershipToken: actor.ownershipToken,
      ownerHostId: "resident:listener", ownerIdentityId: "resident:listener", capabilities: ["steer", "followUp", "ask", "actor-bindings"],
      controlProtocol: "v1", startedAt: 1, updatedAt: 1 };
    const config = { rootId: actor.rootId, residencyRoot: resident, cwd: root };
    const routes = { format: 1, rootId: actor.rootId, configJson: canonicalResidentWakeConfig(config), hostId: participant.ownerHostId,
      actors: [{ id: actorId, name: actor.name, topics: [], participant }] };
    const saveRoutes = () => fs.writeFileSync(path.join(resident, "wake-routes.json"), JSON.stringify(routes));
    saveRoutes();
    fs.writeFileSync(path.join(resident, "config.json"), JSON.stringify(config));
    const participants = { get: () => undefined, scheduleRefresh: vi.fn(), lastKnown: () => undefined, writeStalled: () => undefined };
    const actors = { status: (id: string) => { if (id === actorId) return actor; throw new Error(`Unknown Fabric actor: ${id}`); },
      mesh, identity: { id: "publisher", name: "publisher", kind: "main" }, owns: () => false,
      validateDirectMessage: vi.fn(), tell: vi.fn(), resolveBinding: vi.fn(() => ({})), ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn(), resolveActivationBinding: vi.fn() };
    const main = { id: "session:sender", local: true, matches: (id: string) => id === "main" || id === "session:sender", deliverAgent: vi.fn() };
    const manager = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } };
    const launch = vi.fn(async () => {
      const request = wake.readWakeJson<{ sequence: number }>(wake.residentWakeRequestPath(resident));
      expect(mesh.read().some(event => event.sequence === request?.sequence && event.topic === "fabric.control.command")).toBe(true);
    });
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, events) => dispatch(store, events, launch));
    const direct = vi.spyOn(wake, "wakeDormantActor");
    const control = { request: vi.fn(async (host: string, target: string, operation: string) => {
      await mesh.publish({ topic: "fabric.control.command", to: host, from: actors.identity as never, data: { targetId: target, operation } });
      return { queued: true, acknowledged: true, routed: "mesh", messageId: "once" };
    }) };
    const router = new AgentMessageRouter(manager as unknown as Ports[0], actors as unknown as Ports[1], main as Ports[2], participants as Ports[3], control as unknown as Ports[4], binding => binding);
    const close = () => { vi.restoreAllMocks(); mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); };
    return { root, mesh, resident, actorId, actor, actors, participant, participants, control, router, launch, direct, saveRoutes, close };
  };

  it.each([
    ["steer", "dormant"], ["followUp", "dormant"], ["steer", "failed"], ["followUp", "failed"],
  ] as const)("%s admits a %s definition before commit-owned wake and uses the ACK path once", async (kind, status) => {
    const f = directFixture();
    f.actor.status = status;
    try {
      const result = await f.router.routeMessage(f.actorId, "work", undefined, kind);
      expect(f.direct).not.toHaveBeenCalled();
      expect(f.launch).toHaveBeenCalledOnce();
      expect(f.control.request).toHaveBeenCalledOnce();
      expect(f.control.request).toHaveBeenCalledWith("resident:listener", f.actorId, kind,
        expect.objectContaining({ message: "work" }), "resident:listener",
        expect.objectContaining({ timeoutMs: 90_000, idempotencyKey: expect.any(String) }));
      expect(result).toMatchObject({ acknowledged: true, messageId: "once" });
      expect(f.actors.tell).not.toHaveBeenCalled();
    } finally { f.close(); }
  });

  it.each(["directory", "capability", "binding", "owner", "ack", "message", "deadline", "cancelled"] as const)(
    "a %s admission rejection writes no wake request and starts no host", async reason => {
      const f = directFixture();
      let options: Parameters<typeof f.router.routeMessage>[5] = {};
      let context: Parameters<typeof f.router.routeMessage>[4];
      if (reason === "directory") f.participants.writeStalled = () => new Error("directory denied") as never;
      if (reason === "capability") f.participant.capabilities = [];
      if (reason === "binding") {
        f.participant.capabilities = ["steer", "followUp"];
        f.actors.resolveBinding.mockReturnValue({ model: "fixed" } as never);
        options = { binding: { model: "fixed" } };
      }
      if (reason === "owner") f.participant.actorOwnershipToken = "replacement-lineage";
      if (reason === "ack") f.control.request.mockRejectedValueOnce(new Error("Fabric control plane closed"));
      if (reason === "message") f.actors.validateDirectMessage.mockImplementationOnce(() => { throw new Error("invalid message"); });
      if (reason === "deadline") options = { deadlineMs: 100 };
      if (reason === "cancelled") {
        const abort = new AbortController(); abort.abort(new Error("cancelled"));
        context = { signal: abort.signal } as never;
      }
      f.saveRoutes();
      try {
        await expect(f.router.routeMessage(f.actorId, "work", undefined, "followUp", context, options)).rejects.toThrow();
        expect(f.direct).not.toHaveBeenCalled();
        expect(f.launch).not.toHaveBeenCalled();
        expect(fs.existsSync(wake.residentWakeRequestPath(f.resident))).toBe(false);
        expect(fs.existsSync(path.join(f.resident, "owner.json"))).toBe(false);
        expect(f.mesh.read().filter(event => event.topic === "fabric.control.command")).toHaveLength(0);
      } finally { f.close(); }
    },
  );

  it.each(["unavailable", "disabled"] as const)("a real %s ACK channel cannot wake a dormant owner", async state => {
    const f = directFixture();
    const plane = new FabricControlPlane(f.mesh, f.actors.identity as never, { enabled: false, hostId: "publisher" });
    const router = new AgentMessageRouter(f.router.manager, f.router.actorManager, f.router.mainAgent,
      f.router.participants, state === "disabled" ? plane : undefined, f.router.resolvePiRunBinding);
    try {
      await expect(router.routeMessage(f.actorId, "work", undefined, "followUp")).rejects.toThrow();
      expect(f.launch).not.toHaveBeenCalled();
      expect(f.direct).not.toHaveBeenCalled();
      expect(fs.existsSync(wake.residentWakeRequestPath(f.resident))).toBe(false);
      expect(f.mesh.read().filter(event => event.topic === "fabric.control.command")).toHaveLength(0);
    } finally { await plane.close(); f.close(); }
  });

  it.each(["spawn", "write"] as const)("isolates a per-root %s failure and retries it on the next unrelated delivery", async failure => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-retry-"));
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100);
    const roots = ["first", "second"].map(name => {
      const resident = residentRoot(mesh.root, `session:${name}`);
      fs.mkdirSync(resident, { recursive: true });
      const config = { rootId: `session:${name}`, residencyRoot: resident, cwd: root };
      fs.writeFileSync(path.join(resident, "config.json"), JSON.stringify(config));
      fs.writeFileSync(path.join(resident, "wake-routes.json"), JSON.stringify({ format: 1, rootId: `session:${name}`, hostId: `host:${name}`,
        configJson: canonicalResidentWakeConfig(config),
        actors: [{ id: name, name, topics: ["retry.topic"] }] }));
      return resident;
    }).sort();
    let failed = false;
    const launch = vi.fn(async (configPath: string) => {
      if (failure === "spawn" && path.dirname(configPath) === roots[0] && !failed) { failed = true; throw new Error("injected spawn failure"); }
    });
    if (failure === "write") {
      const atomic = await import("../src/core/atomic-write.js");
      const write = atomic.writeJsonAtomic;
      vi.spyOn(atomic, "writeJsonAtomic").mockImplementation((file, ...args) => {
        if (file === wake.residentWakeRequestPath(roots[0]!) && !failed) { failed = true; throw new Error("injected write failure"); }
        return write(file, ...args);
      });
    }
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, events) => dispatch(store, events, launch));
    const from = { id: "publisher", name: "publisher", kind: "main" as const };
    try {
      const committed = await mesh.publish({ topic: "retry.topic", from });
      expect(failed).toBe(true);
      expect(launch.mock.calls.some(([file]) => path.dirname(file) === roots[1])).toBe(true);
      expect(wake.readWakeJson<{ delivery: { id: string } }>(path.join(roots[0]!, "wake-failure.json"))?.delivery.id).toBe(committed.id);
      if (failure === "spawn") expect(wake.readWakeJson<{ id: string }>(wake.residentWakeRequestPath(roots[0]!))?.id).toBe(committed.id);
      // Other root slept having covered its nudge; it must not spuriously relaunch on retry.
      fs.writeFileSync(wake.residentSleepingPath(roots[1]!), JSON.stringify({ request: wake.readWakeJson(wake.residentWakeRequestPath(roots[1]!)) }));
      const before = launch.mock.calls.length;
      await mesh.publish({ topic: "unrelated.retry.trigger", from });
      expect(launch.mock.calls.length).toBe(before + 1);
      expect(path.dirname(launch.mock.calls.at(-1)![0])).toBe(roots[0]);
      expect(wake.readWakeJson<{ id: string }>(wake.residentWakeRequestPath(roots[0]!))?.id).toBe(committed.id);
      expect(fs.existsSync(path.join(roots[0]!, "wake-failure.json"))).toBe(false);
      expect(mesh.read().filter(event => event.topic === "retry.topic")).toHaveLength(1);
    } finally { vi.restoreAllMocks(); mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["deadline", "error", "throw"] as const)("re-reads readiness once after watcher %s and proceeds without polling", async failure => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-reread-"));
    const readyFile = path.join(root, "maintenance-ready.json");
    const close = vi.fn();
    const watcher = Object.assign(new EventEmitter(), { close }) as unknown as fs.FSWatcher;
    const interval = vi.spyOn(globalThis, "setInterval");
    const ready = vi.fn(() => wake.readWakeJson<{ token: string }>(readyFile)?.token === "ready");
    vi.spyOn(fs, "watch").mockImplementation(() => {
      if (failure === "throw") {
        fs.writeFileSync(readyFile, JSON.stringify({ token: "ready" }));
        throw new Error("watch unavailable");
      }
      return watcher;
    });
    try {
      const waiting = wake.waitResidentChange(root, ready, 10, "wake pending");
      fs.writeFileSync(readyFile, JSON.stringify({ token: "ready" }));
      if (failure === "error") watcher.emit("error", new Error("watch failed"));
      await waiting;
      expect(ready).toHaveBeenCalledTimes(failure === "throw" ? 1 : 2);
      expect(interval).not.toHaveBeenCalled();
      expect(close).toHaveBeenCalledTimes(failure === "throw" ? 0 : 1);
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["deadline", "error", "throw"] as const)("returns typed pending after watcher %s when its single re-read is unsatisfied", async failure => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-pending-"));
    const close = vi.fn();
    const watcher = Object.assign(new EventEmitter(), { close }) as unknown as fs.FSWatcher;
    const ready = vi.fn(() => false);
    vi.spyOn(fs, "watch").mockImplementation(() => {
      if (failure === "throw") throw new Error("watch unavailable");
      return watcher;
    });
    try {
      const waiting = wake.waitResidentChange(root, ready, 10, "wake pending");
      const rejected = expect(waiting).rejects.toMatchObject({ name: "ResidentWakePending", code: "RESIDENT_WAKE_PENDING", root });
      if (failure === "error") watcher.emit("error", new Error("watch failed"));
      await rejected;
      expect(ready).toHaveBeenCalledTimes(failure === "throw" ? 1 : 2);
      expect(close).toHaveBeenCalledTimes(failure === "throw" ? 0 : 1);
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("observes atomic ready-file writes without an interval and closes its bounded watch", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-watch-"));
    const interval = vi.spyOn(globalThis, "setInterval");
    const atomic = await import("../src/core/atomic-write.js");
    try {
      const ready = path.join(root, "maintenance-ready.json");
      const waiting = wake.waitResidentChange(root, () => wake.readWakeJson<{ token: string }>(ready)?.token === "ready", 1_000, "deadline");
      atomic.writeJsonAtomic(ready, { token: "ready" });
      await waiting;
      expect(interval).not.toHaveBeenCalled();
      await expect(wake.waitResidentChange(root, () => false, 10, "bounded deadline")).rejects.toThrow("bounded deadline");
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
