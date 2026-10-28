import { spawn } from "node:child_process";
import { once } from "node:events";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { ActorManager, actorAlarmKey, ACTOR_BLOCK_REALARM_PUBLISH_TIMEOUT_MS } from "../src/actors/manager.js";
import type { FabricActorDeliveryRequest } from "../src/actors/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";
import { RootInbox, rootInboxMessage, type RootInboxSession } from "../src/topology/root-inbox.js";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";

// Keep: the silent durable-backlog incident, owner/recipient trust boundary, and queue data safety.
// Real maintenance -> durable claim -> addressed mesh work -> recipient RootInbox -> Pi injection.
// Workers are paused; the healthy resident is distinct from its dead Main owner.
const MINUTE = 60_000, BOUND = 12 * MINUTE;
const OWNER = "session:00000000-0000-0000-0000-000000000001";
const FALLBACK = "session:00000000-0000-0000-0000-000000000002";
const ROTATED = "session:00000000-0000-0000-0000-000000000003";
const KIND = "actor-owner-orphaned";
const notHeld: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
const closers: Array<() => Promise<void>> = [];
const roots: string[] = [];
let deadPid: number;
beforeAll(async () => {
  const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
  deadPid = child.pid!;
  await once(child, "exit");
  expect(() => process.kill(deadPid, 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
});
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 60_000);
const realNow = Date.now.bind(Date);
const waitFor = async (predicate: () => boolean, timeoutMs = 5_000) => {
  const deadline = realNow() + timeoutMs;
  while (!predicate()) {
    if (realNow() > deadline) throw new Error("Timed out waiting for orphan-owner alarm");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const settle = () => new Promise(resolve => setTimeout(resolve, 200));
const digest = (value: string) => createHash("sha256").update(value).digest("hex");

const setup = async (residency: "durable" | "session" = "durable") => {
  vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", FALLBACK);
  vi.stubEnv("SMARTY_LEAD_SESSION", "");
  const clock = { skew: 0 };
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-orphan-owner-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  closers.push(async () => { mesh.closeState(); });
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  closers.push(() => agents.close());
  const resident: MeshIdentity = { id: "resident:healthy", kind: "agent", name: "resident" };
  const owner: MeshIdentity = { id: OWNER, kind: "main", name: "old Main", sessionId: OWNER.slice(8) };
  const lease: FabricHostLease = { id: OWNER, rootId: OWNER, identityId: OWNER, updatedAt: Date.now(), expiresAt: Date.now(),
    writer: { pid: deadPid, host: os.hostname(), startedAt: Date.now() - MINUTE,
      releaseSha: "test", lockProtocol: 1, stateBackend: "file" } };
  writeHostLease(mesh.root, lease);
  writeHostLease(mesh.root, { ...lease, id: resident.id, identityId: resident.id, expiresAt: Date.now() + 60 * MINUTE,
    writer: { ...lease.writer!, pid: process.pid } });
  const delivered: FabricActorDeliveryRequest[] = [];
  const makeManager = () => {
    const manager = new ActorManager(OWNER.slice(8), resident, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      agents, request => { delivered.push(request); }, { actorRoot: path.join(root, "actors"), persistent: true,
        rootId: OWNER, claimResidency: residency, canManageActor: () => true, releasePaused: true,
        retentionSweepMs: 20, meshRetentionSweepPath: false, reapDeadSessionPresence: false });
    closers.push(() => manager.close());
    return manager;
  };
  const actors = makeManager();
  const actor = await actors.create({ name: "backlogged", instructions: "Retain all work.", residency, coalesce: false });
  const setOwnerAge = (age: number, changes: Partial<FabricHostLease> = {}) => {
    const at = Date.now() - age;
    writeHostLease(mesh.root, { ...lease, updatedAt: at, expiresAt: at, ...changes });
  };
  const inbox = (id = FALLBACK) => {
    const identity: MeshIdentity = { id, kind: "main", name: "configured fallback" };
    const box = new RootInbox(mesh, identity, () => [id], { now: Date.now, steerGraceMs: 0, wakeCooldownMs: 0 });
    closers.push(() => box.close());
    return box;
  };
  const alarms = () => mesh.read({ after: 0, limit: 500 }).filter(event =>
    event.kind === "p0" && (event.data as { actorId?: string } | undefined)?.actorId === actor.id);
  const published = () => (mesh.get(actorAlarmKey(actor.id, KIND), { fresh: true })?.value as { state?: string })?.state === "published";
  const queueFile = () => {
    const directory = path.join(root, "actors", actor.id);
    return path.join(directory, fs.readdirSync(directory).find(name => /^queue-.*\.json$/.test(name))!);
  };
  // Real roots initialize their processing cursor when they become available, before incoming work.
  await Promise.all([FALLBACK, ROTATED, OWNER].map(id => inbox(id).next(notHeld)));
  return { root, mesh, agents, resident, owner, lease, actor, actors, makeManager, clock, setOwnerAge, inbox,
    delivered, alarms, published, queueFile };
};

const receive = async (box: RootInbox) => {
  const batch = await box.wake(notHeld, () => true);
  expect(batch?.events).toHaveLength(1);
  const messages: ReturnType<typeof rootInboxMessage>[] = [];
  const options: Array<Parameters<ExtensionAPI["sendMessage"]>[1]> = [];
  deliverRootInbox({ hostCapabilities: { turnProvenance: 1 },
    sendMessage: (message: unknown, delivery: Parameters<ExtensionAPI["sendMessage"]>[1]) => {
      messages.push(message as ReturnType<typeof rootInboxMessage>);
      options.push(delivery);
    },
  } as unknown as ExtensionAPI, batch!.events);
  expect(messages).toHaveLength(1);
  expect(options).toMatchObject([{ deliverAs: "followUp", triggerTurn: true,
    provenance: { sender: { id: "resident:healthy", kind: "agent", verified: "mesh" } } }]);
  expect(messages[0]!.content).toContain("P0");
  expect(messages[0]!.content).toContain("Inspect");
  await box.consume(messages[0]!.details.ids, messages[0]!.details.receipts);
  expect((await box.next(notHeld)).events).toEqual([]);
  return batch!.events[0]!;
};

describe("durable actor orphan-owner fallback (alarm only)", () => {
  it("delivers queued-work P0 to the configured local inbox, preserves bytes/ownership, and dedupes across passes/restart", async () => {
    const s = await setup();
    await s.actors.tell(s.actor.id, "private queued payload", { keep: [1, 2, 3] });
    const file = s.queueFile();
    const bytes = fs.readFileSync(file);
    const registry = path.join(s.root, "actors", "actors.json");
    const registryBytes = fs.readFileSync(registry);
    s.clock.skew = MINUTE;
    s.setOwnerAge(BOUND + MINUTE);
    await waitFor(s.published);
    const alarm = await receive(s.inbox());
    expect(alarm).toMatchObject({ to: FALLBACK, kind: "p0", from: s.resident,
      data: { actorId: s.actor.id, actorName: "backlogged", ownerRoot: OWNER, queueDepth: 1,
        oldestItemAgeMs: expect.any(Number), ownerAbsentForAtLeastMs: BOUND } });
    expect((alarm.data as { oldestItemAgeMs: number }).oldestItemAgeMs).toBeGreaterThanOrEqual(MINUTE);
    expect(JSON.stringify(alarm)).not.toContain("private queued payload");
    expect((await s.inbox(OWNER).next(notHeld)).events).toEqual([]);
    expect(s.delivered).toEqual([]); // Never enter the owner-pinned actor-output callback.
    await settle();
    expect(s.alarms()).toHaveLength(1);
    expect(fs.readFileSync(file)).toEqual(bytes);
    expect(fs.readFileSync(registry)).toEqual(registryBytes);
    expect(s.actors.status(s.actor.id)).toMatchObject({ rootId: OWNER, residency: "durable", queued: 1 });
    await s.actors.close();
    const restarted = s.makeManager();
    await settle();
    expect(restarted.status(s.actor.id)).toMatchObject({ rootId: OWNER, queued: 1 });
    expect(s.alarms()).toHaveLength(1);
    expect((await s.inbox().next(notHeld)).events).toEqual([]);
  });

  it("detects routed dead-letter-only work held by its existing filter after reload, without rewriting its bytes", async () => {
    const s = await setup();
    await s.actors.setActivationFilter(s.actor.id, [{ id: "wait", topic: ["orphan.routed"], kind: ["wait"] }]);
    const event = await s.mesh.publish({ topic: "orphan.routed", kind: "wait", from: s.owner, text: "private dead-letter payload" });
    await s.actors.close();
    const file = path.join(s.root, "actors", s.actor.id, "dead-letter.jsonl");
    const at = Date.now() - 3 * MINUTE;
    const bytes = `${JSON.stringify({ source: "mesh:orphan.routed", at, event,
      heldBy: { reason: "activationFilter", ruleId: "wait", itemId: event.id, movedAt: at } })}\n`;
    fs.writeFileSync(file, bytes);
    s.setOwnerAge(BOUND + MINUTE);
    const restarted = s.makeManager();
    await waitFor(s.published);
    const alarm = await receive(s.inbox());
    expect(alarm.data).toMatchObject({ queueDepth: 1, counts: { queued: 0, deadLetter: 1 } });
    expect((alarm.data as { oldestItemAgeMs: number }).oldestItemAgeMs).toBeGreaterThanOrEqual(3 * MINUTE);
    expect(fs.readFileSync(file, "utf8")).toBe(bytes);
    expect(restarted.status(s.actor.id)).toMatchObject({ rootId: OWNER, queued: 0 });
    expect(s.delivered).toEqual([]);
  });

  it("waits the full 12 minutes and resolves a rotated fallback after taking the alarm claim", async () => {
    const s = await setup();
    await s.actors.tell(s.actor.id, "waiting");
    s.setOwnerAge(BOUND - 1_000);
    await settle();
    expect(s.alarms()).toEqual([]);
    const writeBatch = s.mesh.writeBatch.bind(s.mesh);
    vi.spyOn(s.mesh, "writeBatch").mockImplementation(async input => {
      const result = await writeBatch(input);
      if (result.some(entry => entry.key === actorAlarmKey(s.actor.id, KIND) && entry.applied)) {
        vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", ROTATED);
      }
      return result;
    });
    s.clock.skew += 2_000;
    await waitFor(s.published);
    expect((await s.inbox(FALLBACK).next(notHeld)).events).toEqual([]);
    expect(await receive(s.inbox(ROTATED))).toMatchObject({ to: ROTATED });
  });

  it("replays a published-but-unmarked alarm through existing pending-claim takeover without a second delivery", async () => {
    const s = await setup();
    await s.actors.tell(s.actor.id, "waiting");
    const key = actorAlarmKey(s.actor.id, KIND);
    const put = s.mesh.put.bind(s.mesh);
    let interrupted = false;
    vi.spyOn(s.mesh, "put").mockImplementation(async input => {
      if (input.key === key && (input.value as { state?: string }).state === "published" && !interrupted) {
        interrupted = true;
        throw new Error("test crash before claim mark");
      }
      return put(input);
    });
    s.setOwnerAge(BOUND + MINUTE);
    await waitFor(() => interrupted);
    const first = await receive(s.inbox());
    expect(s.mesh.get(key)?.value).toMatchObject({ state: "pending" });
    await s.actors.close();
    s.clock.skew += ACTOR_BLOCK_REALARM_PUBLISH_TIMEOUT_MS + 1_000;
    s.makeManager();
    await waitFor(s.published);
    expect(s.alarms().map(event => event.id)).toEqual([first.id]);
    expect((await s.inbox().next(notHeld)).events).toEqual([]);
  });

  it.each(["live", "brief", "reload", "renewed", "foreign", "missing", "invalid", "fresh-inbox", "fresh-presence", "empty", "session"] as const)(
    "does not alarm for %s evidence/backlog", async mode => {
      const s = await setup(mode === "session" ? "session" : "durable");
      if (mode !== "empty") await s.actors.tell(s.actor.id, "preserve me");
      s.setOwnerAge(BOUND + MINUTE);
      if (mode === "live") s.setOwnerAge(BOUND + MINUTE, { writer: { ...s.lease.writer!, pid: process.pid } });
      if (mode === "brief") s.setOwnerAge(3 * MINUTE);
      if (mode === "reload") s.setOwnerAge(BOUND + MINUTE, { reloadUntil: Date.now() + MINUTE });
      if (mode === "renewed") s.setOwnerAge(0, { expiresAt: Date.now() + MINUTE, writer: { ...s.lease.writer!, pid: process.pid } });
      if (mode === "foreign") s.setOwnerAge(BOUND + MINUTE, { writer: { ...s.lease.writer!, host: "foreign-host" } });
      if (mode === "missing") fs.rmSync(hostLeasePath(s.mesh.root, OWNER));
      if (mode === "invalid") fs.writeFileSync(hostLeasePath(s.mesh.root, OWNER), "unreadable evidence");
      if (mode === "fresh-inbox") {
        const file = path.join(s.mesh.root, "main-followups", `${OWNER.slice(8)}.owner.json`);
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, JSON.stringify({ rootId: OWNER, sessionId: OWNER.slice(8), ownerIdentityId: OWNER,
          pid: deadPid, processStartedAt: "1", host: os.hostname() }));
        const at = new Date(Date.now() - 10 * MINUTE);
        fs.utimesSync(file, at, at);
      }
      if (mode === "fresh-presence") writeParticipantFile(s.mesh.root, {
        key: `topology/participants/${digest(OWNER)}`, version: 1, updatedAt: Date.now() - 10 * MINUTE,
        updatedBy: s.owner, value: { id: OWNER, kind: "root", rootId: OWNER, ownerHostId: OWNER, ownerIdentityId: OWNER },
      });
      await settle();
      expect(s.alarms()).toEqual([]);
      expect((await s.inbox().next(notHeld)).events).toEqual([]);
      expect(s.mesh.get(actorAlarmKey(s.actor.id, KIND))).toBeUndefined();
      expect(s.actors.status(s.actor.id)).toMatchObject({ rootId: OWNER, queued: mode === "empty" ? 0 : 1 });
    });

  it.each(["", OWNER])("keeps the slot retryable when fallback is unavailable or the dead owner (%s)", async target => {
    const s = await setup();
    await s.actors.tell(s.actor.id, "waiting");
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", target);
    s.setOwnerAge(BOUND + MINUTE);
    await settle();
    expect(s.alarms()).toEqual([]);
    vi.stubEnv("PI_FABRIC_MESH_ALARM_TO", ROTATED);
    await waitFor(s.published);
    expect(await receive(s.inbox(ROTATED))).toMatchObject({ to: ROTATED });
  });
});
