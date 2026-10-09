import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { residentRoot } from "../src/residency/protocol.js";
import { processStartTime } from "../src/residency/process-identity.js";
import * as wake from "../src/residency/wake.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";

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
    fs.writeFileSync(path.join(resident, "wake-routes.json"), JSON.stringify({ format: 1, rootId: config.rootId,
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

  it.each([
    ["steer", "dormant"], ["followUp", "dormant"], ["steer", "failed"], ["followUp", "failed"],
  ] as const)("%s wakes a %s definition before lease admission, then uses the ACKed control path once", async (kind, status) => {
    type Ports = ConstructorParameters<typeof AgentMessageRouter>;
    const actorId = "b".repeat(32);
    let live = false;
    const actor = { id: actorId, rootId: "session:other", residency: "durable", status, name: "listener", runner: "pi" };
    const participants = { get: (id: string) => live && id === actorId ? { ...actor, kind: "actor", local: false,
      ownerHostId: "resident:listener", ownerIdentityId: "resident:listener", capabilities: ["steer", "followUp"], controlProtocol: "v1" } : undefined,
      scheduleRefresh: vi.fn(), lastKnown: () => undefined };
    const actors = { status: (id: string) => { if (id === actorId) return actor; throw new Error(`Unknown Fabric actor: ${id}`); },
      mesh: { root: "/test/mesh" }, identity: { id: "publisher", name: "publisher", kind: "main" }, owns: () => false,
      validateDirectMessage: vi.fn(), tell: vi.fn(), resolveBinding: vi.fn(() => ({})), ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn(), resolveActivationBinding: vi.fn() };
    const main = { id: "session:sender", local: true, matches: (id: string) => id === "main" || id === "session:sender", deliverAgent: vi.fn() };
    const manager = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } };
    const control = { request: vi.fn(async () => ({ queued: true, acknowledged: true, routed: "mesh", messageId: "once" })) };
    const trigger = vi.spyOn(wake, "wakeDormantActor").mockImplementation(async () => { live = true; return true; });
    try {
      const router = new AgentMessageRouter(manager as unknown as Ports[0], actors as unknown as Ports[1], main as Ports[2], participants as Ports[3], control as unknown as Ports[4], binding => binding);
      const result = await router.routeMessage(actorId, "work", undefined, kind);
      expect(trigger).toHaveBeenCalledWith("/test/mesh", actorId);
      expect(control.request).toHaveBeenCalledOnce();
      expect(control.request.mock.calls[0]?.slice(0, 3)).toEqual(["resident:listener", actorId, kind]);
      expect(result).toMatchObject({ acknowledged: true, messageId: "once" });
      expect(actors.tell).not.toHaveBeenCalled();
    } finally { vi.restoreAllMocks(); }
  });
});
