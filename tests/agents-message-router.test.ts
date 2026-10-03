import fs from "node:fs";
import { createHash } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { removeParticipantFileIf, writeParticipantFile } from "../src/topology/participant-files.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

const message = "followUp to a running task waits until its current run finishes; use agents.steer for a correction needed before completion.";
const warning = (targetId: string) => ({ code: "FABRIC_FOLLOW_UP_RUNNING_TASK", targetId, kind: "agent", status: "running", message });
const identity: MeshIdentity = { id: "session:owner", name: "Owner", kind: "main" };
const roots: string[] = [];
const managers: AgentManager[] = [];
const planes: FabricControlPlane[] = [];
const directories: ParticipantDirectory[] = [];
const drains: Array<() => void> = [];
type Ports = ConstructorParameters<typeof AgentMessageRouter>;

const router = (manager: Ports[0], entries: FabricParticipantInfo[] = [], control?: Ports[4], source?: Ports[3]) => {
  const actors = {
    identity, validateDirectMessage: vi.fn(),
    status: vi.fn((id: string) => {
      if (id !== "actor:running") throw new Error(`Unknown Fabric actor: ${id}`);
      return { id, status: "running", runner: "pi" };
    }),
    owns: () => true, tell: vi.fn(() => ({ messageId: "mailbox" })),
    ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn(), resolveBinding: vi.fn(),
  } as unknown as Ports[1];
  const main = { id: identity.id, local: true, matches: (id: string) => id === "main" || id === identity.id,
    deliverAgent: vi.fn(() => ({ queued: true, messageId: "main-queue", routed: "main" })) } as unknown as Ports[2];
  const participants = { get: (id: string) => entries.find(p => p.id === id), scheduleRefresh: vi.fn(), lastKnown: () => undefined };
  return { value: new AgentMessageRouter(manager, actors, main, source ?? participants, control, b => b), actors, main };
};
const running = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-followup-advisory-")); roots.push(root);
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false,
  });
  managers.push(manager);
  const handle = await manager.spawn({ task: "HANG", transport: "process" });
  const run = manager.runDirectory(handle.id)!;
  const statusFile = path.join(run, "status.json");
  await vi.waitFor(() => {
    expect(fs.existsSync(statusFile)).toBe(true);
    expect(JSON.parse(fs.readFileSync(statusFile, "utf8")).status).toBe("running");
  });
  const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
  const entries = () => fs.existsSync(path.join(run, "steer.jsonl"))
    ? fs.readFileSync(path.join(run, "steer.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line)) : [];
  return { root, manager, id: handle.id, statusFile, record, entries };
};
const remote = (id: string, status = "running", kind = "agent") => ({ id, kind, status, local: false,
  ownerHostId: "host:owner", ownerIdentityId: "host:owner", capabilities: ["steer", "followUp"] } as FabricParticipantInfo);
const unknown = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0];

afterEach(async () => {
  await Promise.all(planes.splice(0).map(p => p.close()));
  for (const close of drains.splice(0)) close();
  await Promise.all(directories.splice(0).map(d => d.close()));
  await Promise.all(managers.splice(0).map(m => m.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const mainLeaseFixture = async (files: boolean) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-main-lease-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 1000);
  if (files) await mesh.put({ key: LIVENESS_POLICY_KEY, identity, value: { version: 1, participants: "files" } });
  const directory = new ParticipantDirectory(mesh, {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 120_000,
  });
  directories.push(directory);
  await directory.refresh();
  const sessionId = "11111111-1111-4111-8111-111111111111";
  const target: MeshIdentity = { id: `session:${sessionId}`, name: "Main", kind: "main", sessionId };
  const key = (prefix: string) => prefix + createHash("sha256").update(target.id).digest("hex");
  const presence: FabricParticipantRecord = {
    format: 1, id: target.id, rootId: target.id, kind: "root", ownerHostId: target.id, ownerIdentityId: target.id,
    name: "Main", status: "running", runner: "pi", transport: "host", cwd: root, sessionId,
    capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: 1, updatedAt: Date.now(),
  };
  await mesh.put({ key: key("topology/hosts/"), identity: target, value: {
    format: 1, id: target.id, rootId: target.id, identity: target, startedAt: 1,
    updatedAt: Date.now(), expiresAt: Date.now() - 600_000,
  } });
  const participantKey = key("topology/participants/");
  if (files) writeParticipantFile(meshRoot, { key: participantKey, value: presence, version: 1, updatedAt: Date.now(), updatedBy: target });
  else await mesh.put({ key: participantKey, identity: target, value: presence });
  const plane = (who: MeshIdentity) => {
    const value = new FabricControlPlane(new MeshStore(meshRoot, 64 * 1024, 1000), who,
      { enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
    planes.push(value); return value;
  };
  return { root, meshRoot, mesh, directory, target, sessionId, participantKey, key, plane };
};

describe("Main target lineage delivery (#3686)", () => {
  it.each([
    [false, "steer", false], [false, "followUp", false], [true, "steer", false], [true, "followUp", false],
    [false, "steer", true], [false, "followUp", true], [true, "steer", true], [true, "followUp", true],
  ] as const)("queues %s file presence / %s / bare UUID=%s despite a ten-minute lease lapse", async (files, kind, bare) => {
    const f = await mainLeaseFixture(files);
    expect(f.directory.get(f.target.id, undefined, { fresh: true })).toBeUndefined();
    expect(f.directory.lastKnown(f.target.id)?.lapsedMs).toBeGreaterThanOrEqual(600_000);
    expect(f.directory.lineageAlive(f.target.id)).toBe(true);
    const sender = f.plane(identity);
    sender.start(() => ({ accepted: false }));
    const send = router(unknown, [], sender, f.directory);
    let error: unknown;
    const pending = send.value.routeMessage(bare ? f.sessionId : f.target.id, "live Main reply", { proof: "unchanged" }, kind)
      .catch(failure => { error = failure; return undefined; });
    try {
      await vi.waitFor(() => expect(error !== undefined || f.mesh.read({ topic: "fabric.control.command", limit: 10 }).length > 0).toBe(true));
      expect(error).toBeUndefined();
      // No target control plane is running yet. A fresh store sees the durable mailbox command.
      const commands = new MeshStore(f.meshRoot, 64 * 1024, 1000).read({ topic: "fabric.control.command", limit: 10 });
      expect(commands).toHaveLength(1);
      expect(commands[0]!.data).toMatchObject({ targetId: f.target.id, operation: kind, message: "live Main reply", data: { proof: "unchanged" } });
      const sendMessage = vi.fn();
      const pi = { on: () => () => {}, sendMessage, getThinkingLevel: () => "off" } as unknown as ExtensionAPI;
      const main = new MainAgentController(pi, f.target.id, true, f.root, f.sessionId);
      const journal = path.join(f.root, "main-followups.json");
      main.attachFollowUpDrain({ isIdle: () => false, hasPendingMessages: () => false,
        sessionManager: { getEntries: () => [] } } as unknown as ExtensionContext, 60_000, journal);
      drains.push(() => main.closeFollowUpDrain());
      const owner = f.plane(f.target);
      const receive = new AgentMessageRouter(unknown, send.actors, main, f.directory, owner, b => b);
      owner.start((command, from, signal) => receive.acceptControl(command, from, signal));
      await expect(pending).resolves.toMatchObject({ queued: true, acknowledged: true, routed: "mesh" });
      if (kind === "followUp") {
        expect(main.queueDepth().pendingFollowUps).toBe(1);
        expect(JSON.parse(fs.readFileSync(journal, "utf8")).items).toEqual([
          expect.objectContaining({ message: "live Main reply", data: { proof: "unchanged" } }),
        ]);
      } else expect(sendMessage).toHaveBeenCalledOnce();
      expect(f.directory.get(f.target.id, undefined, { fresh: true })).toBeUndefined();
    } finally {
      await sender.close();
      await pending;
    }
  });

  it.each([false, true])("keeps dead roots and unknown ids unknown (files=%s)", async (files) => {
    const f = await mainLeaseFixture(files);
    if (files) await removeParticipantFileIf(f.mesh, f.participantKey, () => true);
    else await f.mesh.delete({ key: f.participantKey });
    await f.mesh.put({ key: f.key("topology/lineage-closures/"), identity: f.target, value: {
      format: 1, rootId: f.target.id, ownerHostId: f.target.id, ownerIdentityId: f.target.id, closedAt: Date.now(),
    } });
    expect(f.directory.lineageAlive(f.target.id)).toBe(false);
    expect(f.directory.lineageAlive("session:unknown")).toBe(true); // Unknown lineage alone is not an address.
    const request = vi.fn();
    const send = router(unknown, [], { request }, f.directory);
    for (const id of [f.target.id, "session:unknown"]) for (const kind of ["steer", "followUp"] as const) {
      await expect(send.value.routeMessage(id, "not deliverable", undefined, kind)).rejects.toThrow(`Unknown Fabric participant: ${id}`);
    }
    expect(request).not.toHaveBeenCalled();
    expect(f.mesh.read({ topic: "fabric.control.command", limit: 10 })).toEqual([]);
  });
});

describe("running-task followUp advisory (#3005)", () => {
  it("A1 local receipt warns after exactly one unchanged follow_up append", async () => {
    const f = await running();
    const data = { private: "unchanged" };
    const receipt = await router(f.manager).value.routeMessage(f.id, "later", data, "followUp");
    expect(f.entries()).toEqual([{ type: "follow_up", message: "later", data, provenance: expect.any(Object), id: receipt.messageId, ts: expect.any(Number) }]);
    expect(f.entries()[0]).not.toHaveProperty("warning");
    expect(receipt).toEqual({ queued: true, messageId: expect.any(String), routed: "local", warning: warning(f.id) });
  });

  it("A2 ordinary remote owner ACK and replay retain the warning without re-enqueue", async () => {
    const f = await running();
    const meshRoot = path.join(f.root, "mesh");
    const make = (id: string) => {
      const p = new FabricControlPlane(new MeshStore(meshRoot, 64 * 1024, 1000), { ...identity, id },
        { enabled: true, hostId: id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
      planes.push(p); return p;
    };
    const owner = make("host:owner"), sender = make("host:sender");
    const accept = vi.fn((...args: Parameters<AgentMessageRouter["acceptControl"]>) => router(f.manager).value.acceptControl(...args));
    owner.start(accept); sender.start(() => ({ accepted: false }));
    const receipt = await router(unknown, [remote(f.id, "idle")], sender).value.routeMessage(f.id, "later", undefined, "followUp");
    const command = sender.mesh.read({ topic: "fabric.control.command", limit: 10 })[0]!;
    await owner.close(); // Replay through a new owner: exercise the persisted seen outcome, not an in-memory map.
    const restarted = make("host:owner"); restarted.start(accept);
    await sender.mesh.publish({ topic: command.topic, kind: command.kind, from: command.from, to: command.to!, data: command.data });
    await vi.waitFor(() => expect(sender.mesh.read({ topic: "fabric.control.ack", limit: 10 }).length).toBeGreaterThanOrEqual(2));
    expect(accept).toHaveBeenCalledOnce(); expect(f.entries()).toHaveLength(1);
    for (const ack of sender.mesh.read({ topic: "fabric.control.ack", limit: 10 })) expect(ack.data).toMatchObject({ accepted: true, warning: warning(f.id) });
    expect(receipt).toEqual({ queued: true, messageId: expect.any(String), routed: "mesh", acknowledged: true, warning: warning(f.id) });
  });

  it("A3 steer, queue-mode and compact keep their entries and never warn", async () => {
    const f = await running();
    const receipt = await router(f.manager).value.routeMessage(f.id, "correct now", undefined, "steer");
    const receipts = [receipt, f.manager.setSteeringMode(f.id, "all"), f.manager.setFollowUpMode(f.id, "one-at-a-time"), f.manager.compact(f.id, "keep facts")];
    for (const result of receipts) expect(result).not.toHaveProperty("warning");
    expect(f.entries().map(e => e.type)).toEqual(["steer", "set_steering_mode", "set_follow_up_mode", "compact"]);
    expect(f.entries()[0].message).toBe("correct now"); expect(f.entries()[3].instructions).toBe("keep facts");
  });

  it("A4 running Main, peer root and actor keep their original routes without task admission", async () => {
    const admission = vi.fn(() => { throw new Error("Unexpected task admission"); });
    const manager = { ...unknown, followUp: admission };
    const request = vi.fn(async () => ({ queued: true as const, messageId: "peer", routed: "mesh" as const, acknowledged: true as const }));
    const r = router(manager, [remote("session:peer", "running", "root")], { request });
    expect(await r.value.routeMessage("main", "later", undefined, "followUp")).toEqual({ queued: true, messageId: "main-queue", routed: "main" });
    expect(await r.value.routeMessage("session:peer", "later", undefined, "followUp")).toEqual({ queued: true, messageId: "peer", routed: "mesh", acknowledged: true });
    expect(await r.value.routeMessage("actor:running", "later", undefined, "followUp")).toEqual({ queued: true, messageId: "mailbox", routed: "local" });
    expect(r.main.deliverAgent).toHaveBeenCalledOnce(); expect(request).toHaveBeenCalledOnce(); expect(r.actors.tell).toHaveBeenCalledOnce(); expect(admission).not.toHaveBeenCalled();
    expect(await r.value.acceptControl({ operation: "followUp", targetId: identity.id, message: "later", commandId: "root" } as never, identity)).not.toHaveProperty("warning");
    expect(await r.value.acceptControl({ operation: "followUp", targetId: "actor:running", message: "later", commandId: "actor" } as never, identity)).not.toHaveProperty("warning");
  });

  it("A5 owner status wins over stale mirrors in both directions", async () => {
    const f = await running();
    const owner = router(f.manager).value;
    const control = { request: vi.fn(async (_host: string, id: string, operation: "steer" | "followUp", input: { message?: string }) => {
      const acceptance = await owner.acceptControl({ targetId: id, operation, message: input.message, commandId: "stale" } as never, identity);
      if (!acceptance.accepted) throw new Error(acceptance.error);
      return { queued: true as const, routed: "mesh" as const, acknowledged: true as const, ...acceptance };
    }) } as unknown as Ports[4];
    fs.writeFileSync(f.statusFile, JSON.stringify({ ...f.record, status: "completed" }));
    await expect(router(unknown, [remote(f.id)], control).value.routeMessage(f.id, "too late", undefined, "followUp")).rejects.toThrow(/already finished/);
    expect(f.entries()).toEqual([]);
    fs.writeFileSync(f.statusFile, JSON.stringify(f.record));
    const receipt = await router(unknown, [remote(f.id, "idle")], control).value.routeMessage(f.id, "later", undefined, "followUp");
    expect(f.entries()).toHaveLength(1); expect(receipt).toHaveProperty("warning", warning(f.id));
  });

  it.each(["missing", "queued"])("A6 %s owner record does not invent running status", async status => {
    const f = await running();
    if (status === "missing") fs.rmSync(f.statusFile);
    else fs.writeFileSync(f.statusFile, JSON.stringify({ ...f.record, status }));
    const receipt = f.manager.followUp(f.id, "later");
    expect(receipt).toEqual({ queued: true, messageId: expect.any(String) });
    expect(f.entries()).toHaveLength(1);
  });
});
