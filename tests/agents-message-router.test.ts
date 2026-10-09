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
import { Agent, type AgentMessage, type QueueMode } from "@earendil-works/pi-agent-core";
import { createFauxCore, fauxAssistantMessage } from "@earendil-works/pi-ai";
import principalDelivery from "../src/worker/principal-delivery.js";
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
    resolveActivationBinding: vi.fn(async () => ({})),
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
    routingLease: { waitMs: 5, pollMs: 1 },
  });
  directories.push(directory);
  await directory.refresh();
  const sessionId = "11111111-1111-4111-8111-111111111111";
  const target: MeshIdentity = { id: `session:${sessionId}`, name: "Main", kind: "main", sessionId };
  const key = (prefix: string) => prefix + createHash("sha256").update(target.id).digest("hex");
  const presence: FabricParticipantRecord = {
    format: 1, id: target.id, rootId: target.id, kind: "root", ownerHostId: target.id, ownerIdentityId: target.id,
    ownerIncarnation: `lease-test:${sessionId}`,
    name: "lead-example", status: "running", runner: "pi", transport: "host", cwd: root, sessionId,
    capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: 1, updatedAt: Date.now(),
  };
  await mesh.put({ key: key("topology/hosts/"), identity: target, value: {
    format: 1, id: target.id, rootId: target.id, identity: target, startedAt: 1,
    updatedAt: Date.now(), expiresAt: Date.now() - 2_000,
  } });
  const participantKey = key("topology/participants/");
  if (files) writeParticipantFile(meshRoot, { key: participantKey, value: presence, version: 1, updatedAt: Date.now(), updatedBy: target });
  else await mesh.put({ key: participantKey, identity: target, value: presence });
  const plane = (who: MeshIdentity) => {
    const value = new FabricControlPlane(new MeshStore(meshRoot, 64 * 1024, 1000), who,
      { enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 2000,
        ...(who.id === target.id ? { ownerIncarnation: presence.ownerIncarnation! } : {}) });
    planes.push(value); return value;
  };
  const publishPresence = async (value: FabricParticipantRecord) => {
    if (files) writeParticipantFile(meshRoot, { key: participantKey, value, version: 2, updatedAt: Date.now(), updatedBy: target });
    else await mesh.put({ key: participantKey, identity: target, value });
  };
  return { root, meshRoot, mesh, directory, target, sessionId, participantKey, key, plane, presence, publishPresence };
};

describe("directory availability for live Mains (#2386)", () => {
  it.each([[false, false], [false, true], [true, false], [true, true]] as const)("reports a retryable lock outage then delivers after recovery (files=%s, fresh lease=%s)", async (files, freshLease) => {
    const f = await mainLeaseFixture(files);
    if (freshLease) {
      const host = f.mesh.get(f.key("topology/hosts/"))!.value as Record<string, unknown>;
      await f.mesh.put({ key: f.key("topology/hosts/"), identity: f.target, value: { ...host, expiresAt: Date.now() + 120_000 } });
      expect(f.directory.get(f.target.id)?.kind).toBe("root");
    }
    const timeout = Object.assign(new Error("Timed out waiting for the Fabric mesh lock (injected)"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
    // smarty-dev#6477 L6: a peer commit since the last receipt confirms an idle heartbeat without
    // the lock. Absorb the fixture's commits so the injected lock timeout below is reached.
    await f.directory.refresh();
    const heartbeat = vi.spyOn(f.mesh, "confirmWritable").mockRejectedValueOnce(timeout);
    await expect(f.directory.refresh()).rejects.toBe(timeout);
    heartbeat.mockRestore();
    expect(f.directory.routingUnavailable()).toContain(timeout.message);
    const request = vi.fn().mockResolvedValue({ queued: true, messageId: "recovered", routed: "mesh", acknowledged: true });
    const send = router(unknown, [], { request }, f.directory);
    const probe = vi.spyOn(f.directory, "refreshRoutingView");
    const lock = path.join(f.meshRoot, ".lock");
    fs.mkdirSync(lock, { mode: 0o700 });
    fs.writeFileSync(path.join(lock, "owner"), `contended\n${process.pid}\n${Date.now()}\n`);
    try {
      const failure = await send.value.routeMessage(f.target.id, "live Main reply", { proof: "unchanged" }, "followUp").catch(error => error);
      expect(failure).toMatchObject(freshLease
        ? { name: "FabricDirectoryUnavailableError", code: "FABRIC_DIRECTORY_UNAVAILABLE", retryable: true }
        : { name: "FabricParticipantStaleError", code: "FABRIC_PARTICIPANT_STALE", retryable: true });
      expect(failure.message).toContain(freshLease ? "Timed out waiting for the Fabric mesh lock" : "lease late by");
      expect(failure.message).not.toContain("Unknown Fabric actor");
      expect(probe).toHaveBeenCalledTimes(freshLease ? 1 : 0);
      expect(request).not.toHaveBeenCalled();
      expect(send.actors.status).not.toHaveBeenCalled();
    } finally {
      fs.rmSync(lock, { recursive: true, force: true });
    }
    // A late target renews before retry; routing still needs its ordinary canonical read.
    if (!freshLease) {
      const host = f.mesh.get(f.key("topology/hosts/"))!.value as Record<string, unknown>;
      await f.mesh.put({ key: f.key("topology/hosts/"), identity: f.target, value: { ...host, expiresAt: Date.now() + 120_000 } });
    }
    // No sender heartbeat success is needed for the canonical routing read to recover.
    await expect(send.value.routeMessage(f.target.id, "live Main reply", { proof: "unchanged" }, "followUp"))
      .resolves.toMatchObject({ queued: true, messageId: "recovered", acknowledged: true });
    expect(probe).toHaveBeenCalledTimes(freshLease ? 2 : 1);
    expect(request).toHaveBeenCalledOnce();
    expect(request.mock.calls[0]?.slice(0, 4)).toEqual([f.target.id, f.target.id, "followUp", expect.objectContaining({ message: "live Main reply", data: { proof: "unchanged" } })]);
    expect(f.directory.canConsumeMesh()).toBe(false); // Routing did not weaken lease admission.
  });

  it.each(["", "  ", "{", "{}", "null"])("never confirms damaged canonical bytes %j as absence of a published Main", async (damaged) => {
    const f = await mainLeaseFixture(false);
    const host = f.mesh.get(f.key("topology/hosts/"))!.value as Record<string, unknown>;
    await f.mesh.put({ key: f.key("topology/hosts/"), identity: f.target, value: { ...host, expiresAt: Date.now() + 120_000 } });
    expect(f.directory.get(f.target.id)?.kind).toBe("root");
    const state = path.join(f.meshRoot, "state.json");
    const healthy = fs.readFileSync(state, "utf8");
    fs.writeFileSync(state, damaged);
    const request = vi.fn();
    const send = router(unknown, [], { request }, f.directory);
    try {
      await expect(f.directory.refresh()).rejects.toThrow();
      await expect(f.directory.refreshRoutingView()).rejects.toThrow();
      expect(f.directory.routingUnavailable()).toBeDefined();
      for (const kind of ["followUp", "steer"] as const) {
        await expect(send.value.routeMessage(f.target.id, "must not publish", undefined, kind))
          .rejects.toMatchObject({ code: "FABRIC_DIRECTORY_UNAVAILABLE", retryable: true });
      }
      expect(request).not.toHaveBeenCalled();
      expect(fs.readFileSync(state, "utf8")).toBe(damaged);
    } finally {
      fs.writeFileSync(state, healthy);
    }
  });

  it.each(["cached-root", "fresh-root", "fresh-negative", "retained-list", "name-list", "retained-lastKnown", "lineage", "diagnostic-lastKnown", "diagnostic-peers"] as const)("classifies the late %s read with one bounded probe", async (readPath) => {
    const target = { ...remote("session:live", "running", "root"), name: "live-name", rootId: "session:live", controlProtocol: "v1" } as FabricParticipantInfo;
    for (const fails of [false, true]) {
      let failed = true;
      let cachedReads = 0;
      let lists = 0;
      let retainedReads = 0;
      const badRead = () => { if (failed) throw new Error(`fresh ${readPath} read failed`); };
      const get = vi.fn((id: string, _scope?: unknown, options?: { fresh?: boolean }) => {
        if (id === target.id) {
          if (options?.fresh && readPath === "fresh-root") badRead();
          if (!options?.fresh && readPath === "cached-root" && ++cachedReads >= 2) badRead();
          if (!["retained-list", "retained-lastKnown", "lineage"].includes(readPath)) return target;
        }
        if (options?.fresh && readPath === "fresh-negative") badRead();
        return undefined;
      });
      const list = vi.fn(() => {
        if (readPath === "retained-list" || (readPath === "name-list" && ++lists >= 2)) badRead();
        return readPath.startsWith("diagnostic") || readPath === "fresh-negative" ? [] : [target];
      });
      const lastKnown = vi.fn(() => {
        if (readPath === "retained-lastKnown" || (readPath === "diagnostic-lastKnown" && ++retainedReads >= 3)) badRead();
        return readPath === "retained-lastKnown" ? { participant: target, lapsedMs: 1 } : undefined;
      });
      const peers = vi.fn(() => { if (readPath === "diagnostic-peers") badRead(); return []; });
      const lineageAlive = vi.fn(() => { if (readPath === "lineage") badRead(); return true; });
      const refreshRoutingView = vi.fn(async () => { if (fails) throw new Error("probe failed"); failed = false; });
      const request = vi.fn().mockResolvedValue({ queued: true, routed: "mesh" });
      const source = { get, ...(["retained-lastKnown", "diagnostic-lastKnown"].includes(readPath) ? {} : { list }),
        lastKnown, peers, lineageAlive, scheduleRefresh: vi.fn(), routingUnavailable: () => undefined, refreshRoutingView };
      const send = router(unknown, [], { request }, source);
      const selector = ["name-list", "fresh-negative"].includes(readPath) ? "live-name"
        : readPath.startsWith("diagnostic") ? "session:absent" : target.id;
      const delivery = send.value.routeMessage(selector, "one bounded resolution", undefined, "followUp");
      if (fails) {
        await expect(delivery).rejects.toMatchObject({ code: "FABRIC_DIRECTORY_UNAVAILABLE", retryable: true });
        expect(request).not.toHaveBeenCalled();
      } else if (readPath.startsWith("diagnostic") || readPath === "fresh-negative") {
        await expect(delivery).rejects.toThrow("Unknown Fabric participant");
        expect(request).not.toHaveBeenCalled();
      } else {
        await expect(delivery).resolves.toMatchObject({ queued: true });
        expect(request).toHaveBeenCalledOnce();
      }
      expect(refreshRoutingView).toHaveBeenCalledOnce();
      if (readPath === "fresh-root") expect(get.mock.calls.some(call => call[0] === target.id && !call[2]?.fresh)).toBe(true);
    }
  });

  it.each(["no confirmed view", "view overdue", "last refresh failed"])("probes once when the %s, for both delivery modes", async (reason) => {
    let unavailable: string | undefined = reason;
    const target = remote("session:live", "running", "root");
    const get = vi.fn(() => unavailable ? undefined : target);
    const refreshRoutingView = vi.fn(async () => { unavailable = undefined; });
    const request = vi.fn().mockResolvedValue({ queued: true, routed: "mesh" });
    const send = router(unknown, [], { request }, { get, scheduleRefresh: vi.fn(), routingUnavailable: () => unavailable, refreshRoutingView });
    for (const kind of ["followUp", "steer"] as const) {
      unavailable = reason;
      await expect(send.value.routeMessage(target.id, "recovered", undefined, kind)).resolves.toMatchObject({ queued: true });
    }
    expect(refreshRoutingView).toHaveBeenCalledTimes(2);
    expect(request).toHaveBeenCalledTimes(2);
    expect(send.actors.status).not.toHaveBeenCalled();
  });

  it.each([false, true])("revalidates actor ownership before lookup, retaining failed probe causes (fails=%s)", async (fails) => {
    let unavailable: string | undefined = "no confirmed view";
    const target = remote("actor:resident", "idle", "actor");
    const get = vi.fn(() => target);
    const lockError = Object.assign(new Error("ownerless mesh lock"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
    const refreshRoutingView = vi.fn(async () => {
      if (fails) throw lockError;
      unavailable = undefined;
    });
    const send = router(unknown, [], undefined, { get, scheduleRefresh: vi.fn(), routingUnavailable: () => unavailable, refreshRoutingView });
    const lookup = send.value.resolveActorTargetFresh(target.id);
    if (fails) {
      await expect(lookup).rejects.toMatchObject({ code: "FABRIC_DIRECTORY_UNAVAILABLE", cause: lockError });
      expect(get).not.toHaveBeenCalled();
    } else {
      await expect(lookup).resolves.toMatchObject({ participant: target });
      expect(get).toHaveBeenCalledOnce();
    }
    expect(refreshRoutingView).toHaveBeenCalledOnce();
  });

  it.each([false, true])("bounds recovery of a read failure after a healthy preflight (probe fails=%s)", async (fails) => {
    let failed = true;
    const target = remote("session:live", "running", "root");
    const get = vi.fn(() => { if (failed) throw new Error("directory read failed"); return target; });
    const refreshRoutingView = vi.fn(async () => {
      if (fails) throw new Error("Timed out waiting for the Fabric mesh lock");
      failed = false;
    });
    const request = vi.fn().mockResolvedValue({ queued: true, routed: "mesh" });
    const send = router(unknown, [], { request }, { get, scheduleRefresh: vi.fn(), routingUnavailable: () => undefined, refreshRoutingView });
    const delivery = send.value.routeMessage(target.id, "retry resolution", undefined, "followUp");
    if (fails) {
      await expect(delivery).rejects.toMatchObject({ code: "FABRIC_DIRECTORY_UNAVAILABLE", retryable: true });
      expect(request).not.toHaveBeenCalled();
    } else {
      await expect(delivery).resolves.toMatchObject({ queued: true });
      expect(request).toHaveBeenCalledOnce();
    }
    expect(refreshRoutingView).toHaveBeenCalledOnce();
  });

  it("never retries a repeated fresh-root read failure or a post-publication ACK failure", async () => {
    const target = remote("session:live", "running", "root");
    const refreshRoutingView = vi.fn().mockResolvedValue(undefined);
    const get = vi.fn((_id: string, _scope?: unknown, options?: { fresh?: boolean }) => {
      if (options?.fresh) throw new Error("persistent fresh read failure");
      return target;
    });
    const request = vi.fn();
    const send = router(unknown, [], { request }, { get, scheduleRefresh: vi.fn(), routingUnavailable: () => undefined, refreshRoutingView });
    await expect(send.value.routeMessage(target.id, "no publication", undefined, "followUp"))
      .rejects.toMatchObject({ code: "FABRIC_DIRECTORY_UNAVAILABLE", retryable: true });
    expect(refreshRoutingView).toHaveBeenCalledOnce();
    expect(request).not.toHaveBeenCalled();
    get.mockImplementation(() => target);
    refreshRoutingView.mockClear();
    const ackFailure = new Error("Control acknowledgement timed out after publication");
    request.mockRejectedValue(ackFailure);
    await expect(send.value.routeMessage(target.id, "already published", undefined, "followUp")).rejects.toBe(ackFailure);
    expect(request).toHaveBeenCalledOnce();
    expect(refreshRoutingView).not.toHaveBeenCalled();
  });

  it.each(["followUp", "steer"] as const)("delivers canonical local Main/tasks without probing an unavailable directory (%s)", async (kind) => {
    const refreshRoutingView = vi.fn(async () => { throw new Error("probe failed"); });
    const get = vi.fn(() => { throw new Error("directory read failed"); });
    const request = vi.fn();
    const manager = {
      status: vi.fn((id: string) => {
        if (id === "child") return { id, name: "Child", runner: "pi" };
        throw new Error(`Unknown Fabric agent: ${id}`);
      }),
      steer: vi.fn(() => ({ messageId: "local-task" })),
      followUp: vi.fn(() => ({ messageId: "local-task" })),
    } as unknown as Ports[0];
    const send = router(manager, [], { request }, {
      get, scheduleRefresh: vi.fn(), routingUnavailable: () => "heartbeat failed", refreshRoutingView,
    });
    await expect(send.value.routeMessage(identity.id, "local Main", undefined, kind)).resolves.toMatchObject({ routed: "main" });
    await expect(send.value.routeMessage("child", "local task", undefined, kind)).resolves.toMatchObject({ routed: "local" });
    expect(send.main.deliverAgent).toHaveBeenCalledOnce();
    expect(kind === "steer" ? manager.steer : manager.followUp).toHaveBeenCalledOnce();
    expect(refreshRoutingView).not.toHaveBeenCalled();
    await expect(send.value.routeMessage("session:remote", "no publication", undefined, kind))
      .rejects.toMatchObject({ code: "FABRIC_DIRECTORY_UNAVAILABLE", retryable: true });
    expect(refreshRoutingView).toHaveBeenCalledOnce();
    expect(request).not.toHaveBeenCalled();
    expect(send.main.deliverAgent).toHaveBeenCalledOnce();
  });

  it("does not publish remotely when a proven-local task disappears during delivery", async () => {
    const status = vi.fn().mockReturnValueOnce({ id: "child", name: "Child" })
      .mockImplementation(() => { throw new Error("Unknown Fabric agent: child"); });
    const get = vi.fn();
    const request = vi.fn();
    const send = router({ status } as unknown as Ports[0], [], { request }, { get, scheduleRefresh: vi.fn() });
    await expect(send.value.routeMessage("child", "no fallback", undefined, "steer")).rejects.toThrow("Unknown Fabric agent: child");
    expect(get).not.toHaveBeenCalled();
    expect(request).not.toHaveBeenCalled();
  });

  it("only a fresh view proves an actor id absent, and healthy actor routes are unchanged", async () => {
    const f = await mainLeaseFixture(false);
    const send = router(unknown, [], undefined, f.directory);
    await expect(send.value.resolveActorMessageTarget("actor:missing")).rejects.toThrow("Unknown Fabric actor: actor:missing");
    for (const kind of ["followUp", "steer"] as const) {
      await expect(send.value.routeMessage("actor:running", "existing route", undefined, kind))
        .resolves.toMatchObject({ queued: true, routed: "local", messageId: "mailbox" });
    }
    expect(send.actors.tell).toHaveBeenCalledTimes(2);
  });

  it.each(["followUp", "steer"] as const)("never tries an actor for an absent session: address (%s)", async (kind) => {
    const send = router(unknown);
    const failure = await send.value.routeMessage("session:missing", "no actor fallback", undefined, kind).catch(error => error);
    expect(failure.message).toContain("Unknown Fabric participant: session:missing");
    expect(failure.message).not.toContain("Unknown Fabric actor");
    await expect(send.value.resolveActorMessageTarget("session:missing")).rejects.toThrow("Unknown Fabric Main participant");
    expect(() => send.value.resolveActorTarget("session:missing")).toThrow("is not an actor");
    expect(send.actors.status).not.toHaveBeenCalled();
    expect(send.actors.validateDirectMessage).not.toHaveBeenCalled();
  });
});

describe("Main target lineage delivery (#3686)", () => {
  it.each([
    [false, "steer", false], [false, "followUp", false], [true, "steer", false], [true, "followUp", false],
    [false, "steer", true], [false, "followUp", true], [true, "steer", true], [true, "followUp", true],
    [false, "steer", "name"], [false, "followUp", "name"], [true, "steer", "name"], [true, "followUp", "name"],
  ] as const)("queues %s file presence / %s / selector=%s during the bounded recent-lease grace", async (files, kind, selector) => {
    const f = await mainLeaseFixture(files);
    expect(f.directory.get(f.target.id, undefined, { fresh: true })).toBeUndefined();
    expect(f.directory.lastKnown(f.target.id)?.lapsedMs).toBeGreaterThanOrEqual(2_000);
    expect(f.directory.lineageAlive(f.target.id)).toBe(true);
    const sender = f.plane(identity);
    sender.start(() => ({ accepted: false }));
    const send = router(unknown, [], sender, f.directory);
    let error: unknown;
    const pending = send.value.routeMessage(selector === "name" ? "lead-example" : selector ? f.sessionId : f.target.id, "live Main reply", { proof: "unchanged" }, kind)
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
    for (const id of [f.target.id, "lead-example", "session:unknown"]) for (const kind of ["steer", "followUp"] as const) {
      await expect(send.value.routeMessage(id, "not deliverable", undefined, kind)).rejects.toThrow(`Unknown Fabric participant: ${id}`);
    }
    expect(request).not.toHaveBeenCalled();
    expect(f.mesh.read({ topic: "fabric.control.command", limit: 10 })).toEqual([]);
  });
});

describe.each([false, true])("stale Main name safeguards (files=%s)", (files) => {
  it.each(["reloading", "stopping", "mirrored"] as const)("does not admit a %s root by stale name", async (state) => {
    const f = await mainLeaseFixture(files);
    await f.publishPresence({ ...f.presence, ...(state === "mirrored" ? { remoteHost: "peer-host" } : { status: state }) });
    if (state === "mirrored") {
      const host = f.mesh.get(f.key("topology/hosts/"))!.value as Record<string, unknown>;
      await f.mesh.put({ key: f.key("topology/hosts/"), identity: f.target, value: { ...host, remoteHost: "peer-host" } });
    }
    expect(f.directory.list({ scope: "project", includeStale: true, fresh: true }))
      .toEqual(expect.arrayContaining([expect.objectContaining({ id: f.target.id, stale: true })]));
    const request = vi.fn();
    const send = router(unknown, [], { request }, f.directory);
    for (const kind of ["followUp", "steer"] as const) {
      await expect(send.value.routeMessage("lead-example", "must not arrive", undefined, kind))
        .rejects.toThrow("Unknown Fabric participant: lead-example");
    }
    expect(request).not.toHaveBeenCalled();
  });

  it.each(["non-interactive", "missing-capability"] as const)("retains %s admission guards for a stale named root", async (state) => {
    const f = await mainLeaseFixture(files);
    await f.publishPresence({ ...f.presence, ...(state === "non-interactive" ? { interactive: false } : { capabilities: ["fabric"] }) });
    const request = vi.fn();
    const send = router(unknown, [], { request }, f.directory);
    for (const kind of ["followUp", "steer"] as const) {
      await expect(send.value.routeMessage("lead-example", "must not arrive", undefined, kind))
        .rejects.toThrow(state === "non-interactive" ? "is non-interactive" : `does not support ${kind}`);
    }
    expect(request).not.toHaveBeenCalled();
  });

  it("retains duplicate-name and actor-name ambiguity before publication", async () => {
    const f = await mainLeaseFixture(files);
    const request = vi.fn();
    const send = router(unknown, [], { request }, f.directory);
    const status = vi.spyOn(send.actors, "status").mockReturnValue({ id: "actor:named", name: "lead-example", runner: "pi" } as ReturnType<Ports[1]["status"]>);
    await expect(send.value.routeMessage("lead-example", "must not arrive", undefined, "followUp"))
      .rejects.toThrow(`Ambiguous Fabric participant: lead-example (actor actor:named, root ${f.target.id})`);
    status.mockRestore();
    const duplicate = { ...f.presence, id: "session:duplicate", rootId: "session:duplicate", ownerHostId: "session:duplicate", ownerIdentityId: "session:duplicate" };
    const duplicateKey = "topology/participants/" + createHash("sha256").update(duplicate.id).digest("hex");
    const duplicateIdentity: MeshIdentity = { id: duplicate.id, name: "Main", kind: "main" };
    if (files) writeParticipantFile(f.meshRoot, { key: duplicateKey, value: duplicate, version: 1, updatedAt: Date.now(), updatedBy: duplicateIdentity });
    else await f.mesh.put({ key: duplicateKey, identity: duplicateIdentity, value: duplicate });
    for (const kind of ["followUp", "steer"] as const) {
      const failure = await send.value.routeMessage("lead-example", "never guess", undefined, kind).catch(error => error);
      expect(failure.message).toContain("Ambiguous Fabric participant: lead-example");
      expect(failure.message).toContain(f.target.id);
      expect(failure.message).toContain(duplicate.id);
    }
    expect(request).not.toHaveBeenCalled();
    expect(f.mesh.read({ topic: "fabric.control.command", limit: 10 })).toEqual([]);
  });

  it.each(["dead-lineage", "write-stalled"] as const)("does not admit a %s root by stale name", async (state) => {
    const f = await mainLeaseFixture(files);
    const stalled = new Error("Fabric mesh write stalled");
    if (state === "dead-lineage") vi.spyOn(f.directory, "lineageAlive").mockReturnValue(false);
    else {
      vi.spyOn(f.directory, "routingUnavailable").mockReturnValue(stalled.message);
      vi.spyOn(f.directory, "refreshRoutingView").mockRejectedValue(stalled);
    }
    const request = vi.fn();
    const send = router(unknown, [], { request }, f.directory);
    for (const kind of ["followUp", "steer"] as const) {
      await expect(send.value.routeMessage("lead-example", "must not arrive", undefined, kind))
        .rejects.toThrow(state === "write-stalled" ? stalled.message : "Unknown Fabric participant: lead-example");
    }
    expect(request).not.toHaveBeenCalled();
  });

  it("reads the current stale name rather than a cached previous name", async () => {
    const f = await mainLeaseFixture(files);
    f.directory.list({ scope: "project", kinds: ["root"], includeStale: true });
    await f.publishPresence({ ...f.presence, name: "renamed-lead" });
    const request = vi.fn().mockResolvedValue({ queued: true, acknowledged: true, routed: "mesh" });
    const send = router(unknown, [], { request }, f.directory);
    await expect(send.value.routeMessage("lead-example", "must not arrive", undefined, "followUp"))
      .rejects.toThrow("Unknown Fabric participant: lead-example");
    await expect(send.value.routeMessage("renamed-lead", "current name", undefined, "followUp"))
      .resolves.toMatchObject({ queued: true, acknowledged: true });
    expect(request).toHaveBeenCalledOnce();
    expect(request.mock.calls[0]?.slice(0, 3)).toEqual([f.target.id, f.target.id, "followUp"]);
  });
});

describe("running-task followUp advisory (#3005)", () => {
  it("A1 local receipt warns after exactly one tracked follow_up append with unchanged payload", async () => {
    const f = await running();
    const data = { private: "unchanged" }, admittedAt = Date.now();
    const receipt = await router(f.manager).value.routeMessage(f.id, "later", data, "followUp");
    expect(f.entries()).toEqual([{ type: "follow_up", message: "later", data, provenance: expect.any(Object),
      followUpId: receipt.messageId, deadlineAt: receipt.deadlineAt, id: receipt.messageId, ts: expect.any(Number) }]);
    expect(f.entries()[0]).not.toHaveProperty("warning");
    expect(receipt).toEqual({ queued: true, messageId: expect.any(String), routed: "local", warning: warning(f.id), deadlineAt: expect.any(Number) });
    expect(receipt.deadlineAt).toBeGreaterThanOrEqual(admittedAt + 600_000);
    expect(receipt.deadlineAt).toBeLessThanOrEqual(Date.now() + 600_000);
    expect(f.manager.status(f.id).followUpDeliveries).toEqual([{ messageId: receipt.messageId, deadlineAt: receipt.deadlineAt, state: "queued" }]);
  });

  it.each([
    { followUpMode: "all", steeringMode: "one-at-a-time", firstBatch: ["FOLLOW_FIRST", "FOLLOW_SECOND"] },
    { followUpMode: "one-at-a-time", steeringMode: "all", firstBatch: ["FOLLOW_FIRST"] },
  ] as const)("public tracked follow-ups honour $followUpMode independently of steering $steeringMode", async ({ followUpMode, steeringMode, firstBatch }) => {
    const f = await running(), r = router(f.manager).value;
    f.manager.setSteeringMode(f.id, steeringMode);
    f.manager.setFollowUpMode(f.id, followUpMode);
    const run = f.manager.runDirectory(f.id)!, directory = path.join(run, "deliveries");
    fs.mkdirSync(directory, { recursive: true });
    const previous = process.env.PI_FABRIC_DELIVERY_DIR;
    const handlers = new Map<string, (...args: any[]) => any>();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const contexts: AgentMessage[][] = [];
    const faux = createFauxCore({ tokensPerSecond: 100_000 });
    faux.setResponses(Array.from({ length: 4 }, () => async (_context, _options, state) => {
      if (state.callCount === 1) await gate;
      return fauxAssistantMessage("done");
    }));
    const modes = f.entries();
    const receiver = new Agent({
      initialState: { model: faux.getModel() }, streamFn: faux.streamSimple,
      steeringMode: modes[0].mode as QueueMode, followUpMode: modes[1].mode as QueueMode,
      transformContext: async messages => {
        const result = await handlers.get("context")?.({ messages }, {});
        const consumed = result?.messages ?? messages;
        contexts.push(consumed);
        return consumed;
      },
    });
    const send = vi.fn((text: string, options: { deliverAs: "steer" | "followUp" }) => {
      const message: AgentMessage = { role: "user", content: [{ type: "text", text }], timestamp: Date.now() };
      if (options.deliverAs === "steer") receiver.steer(message);
      else receiver.followUp(message);
    });
    process.env.PI_FABRIC_DELIVERY_DIR = directory;
    try {
      principalDelivery({
        registerCommand: (_name: string, command: any) => handlers.set("command", command.handler),
        on: (name: string, handler: any) => handlers.set(name, handler), sendUserMessage: send,
      } as unknown as ExtensionAPI);
    } finally {
      if (previous === undefined) delete process.env.PI_FABRIC_DELIVERY_DIR;
      else process.env.PI_FABRIC_DELIVERY_DIR = previous;
    }
    receiver.subscribe(async event => {
      if (event.type === "turn_end") await handlers.get("turn_end")?.(event, { isIdle: () => false, signal: receiver.signal });
    });
    const processing = receiver.prompt("initial request");
    try {
      await vi.waitFor(() => expect(faux.state.callCount).toBe(1));
      const receipts = [];
      for (const marker of ["FOLLOW_FIRST", "FOLLOW_SECOND", "FOLLOW_CANCELLED"]) {
        const receipt = await r.routeMessage(f.id, marker, undefined, "followUp");
        receipts.push(receipt);
        const entry = f.entries().at(-1)!;
        expect(entry).toMatchObject({ followUpId: receipt.messageId, deadlineAt: receipt.deadlineAt });
        fs.writeFileSync(path.join(directory, receipt.messageId + ".json"), JSON.stringify({
          message: entry.message, delivery: "followUp", followUpId: entry.followUpId, provenance: entry.provenance,
        }));
        await handlers.get("command")!(receipt.messageId, { isIdle: () => false });
      }
      expect(send).not.toHaveBeenCalled();
      expect(f.manager.status(f.id).followUpDeliveries?.map(d => d.state)).toEqual(["queued", "queued", "queued"]);
      expect(f.manager.cancelFollowUp(f.id, receipts[2]!.messageId).state).toBe("cancelled");
      release(); await processing;
      const markers = (messages: AgentMessage[]) => messages.flatMap(m => m.role === "user" && Array.isArray(m.content)
        ? m.content.flatMap(c => c.type === "text" && c.text.startsWith("FOLLOW_") ? [c.text] : []) : []);
      expect(markers(contexts[1]!)).toEqual(firstBatch);
      expect(markers(contexts.at(-1)!)).toEqual(["FOLLOW_FIRST", "FOLLOW_SECOND"]);
      expect(send.mock.calls.map(call => call[1].deliverAs)).toEqual(["followUp", "followUp"]);
      expect(f.manager.status(f.id).followUpDeliveries?.map(d => d.state)).toEqual(["delivered", "delivered", "cancelled"]);
      expect(receipts.every(receipt => Number.isSafeInteger(receipt.deadlineAt))).toBe(true);
    } finally {
      release(); receiver.abort(); await processing;
    }
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
