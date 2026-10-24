import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import * as actorTypes from "../src/actors/types.js";
import type { FabricActorActivationFilter, FabricActorInfo } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

// smarty-dev#16416: stop and setActivationFilter never silently drop queued work. Real manager,
// mesh store, queue checkpoint and dead-letter file; the fake worker holds the active run.
const QUEUED_WORK = "FABRIC_ACTOR_QUEUED_WORK";
// Read through the module namespace so this file loads (and fails per test) where the type does not exist yet.
const queuedWorkError = () => (actorTypes as Record<string, unknown>).FabricActorQueuedWorkError as (new (...args: never[]) => Error) | undefined;

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0)) await close().catch(() => undefined);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };
const setup = (actorQueueLimit = DEFAULT_FABRIC_CONFIG.mesh.actorQueueLimit) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-queued-work-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true,
  });
  let closed = false;
  const close = async () => { if (closed) return; closed = true; await actors.close(); await agents.close(); };
  closers.push(close);
  return { root, mesh, agents, actors, close };
};
const waitFor = async (predicate: () => boolean, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};
// Keep the actor's active run open so later events stay queued; released at teardown.
const holdWorker = async (actors: Pick<ActorManager, "tell" | "status">, root: string, id: string) => {
  const release = path.join(root, `release-${id.slice(0, 8)}`);
  closers.unshift(async () => { fs.writeFileSync(release, "release"); });
  actors.tell(id, "LIVE_WITH_PROGRESS", { fakeWorkerReleasePath: release });
  await waitFor(() => actors.status(id).status === "running");
  return () => fs.writeFileSync(release, "release");
};
const TOPIC = "queued-work.events";
const WAIT_FILTER: FabricActorActivationFilter = [{ id: "wait", topic: [TOPIC], kind: ["wait"] }];
const publish = (mesh: MeshStore, index: number, waiting = false) =>
  mesh.publish({ topic: TOPIC, kind: waiting ? "wait" : "work", from, data: { marker: `event-${index}` } });
type QueueRecord = { id: string; source: string; payload: MeshEvent };
const queueFile = (root: string, id: string) => {
  const directory = path.join(root, "actors", id);
  const name = fs.readdirSync(directory).find(file => file.startsWith("queue-") && file.endsWith(".json"));
  return name ? path.join(directory, name) : undefined;
};
const meshRecords = (root: string, id: string): QueueRecord[] => {
  const file = queueFile(root, id);
  if (!file) return [];
  return (JSON.parse(fs.readFileSync(file, "utf8")) as { items: QueueRecord[] }).items.filter(item => item.source.startsWith("mesh:"));
};
const markers = (records: QueueRecord[]) => records.map(record => (record.payload.data as { marker: string }).marker);
const deadLetterFile = (root: string, id: string) => path.join(root, "actors", id, "dead-letter.jsonl");
const deadLetterLines = (root: string, id: string) => fs.existsSync(deadLetterFile(root, id))
  ? fs.readFileSync(deadLetterFile(root, id), "utf8").split("\n").filter(Boolean) : [];
const filtered = (actors: ActorManager, id: string) => actors.messages(id).filter(message => message.reason?.startsWith("filtered: "));
const stopArchive = (info: FabricActorInfo) => {
  const stopped = info.stoppedQueue;
  if (!stopped) throw new Error("stop returned no stoppedQueue archive");
  return stopped;
};

// Two in the queue, sixteen in its overflow and one in the dead-letter file.
const saturated = async () => {
  const fixture = setup(2);
  const { root, mesh, actors } = fixture;
  const actor = await actors.create({ name: "saturated-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
  const release = await holdWorker(actors, root, actor.id);
  for (let index = 0; index < 18; index++) await publish(mesh, index);
  const dead = await publish(mesh, 18);
  await waitFor(() => actors.status(actor.id).queued === 18 && deadLetterLines(root, actor.id).some(line => line.includes(dead.id)));
  return { ...fixture, actor, release };
};

describe("agents.stop with queued work (smarty-dev#16416)", () => {
  it("refuses a non-empty queue, naming the counts, and changes nothing", async () => {
    const { root, actors, actor } = await saturated();
    const before = meshRecords(root, actor.id);
    const refusal = await actors.stop(actor.id).then(info => info, (error: unknown) => error);
    expect(refusal).toBeInstanceOf(Error);
    expect(refusal).toBeInstanceOf(queuedWorkError() ?? class Missing {});
    expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "stop", actorId: actor.id,
      counts: { queued: 2, parked: 0, overflow: 16, deadLetter: 1 } });
    expect((refusal as Error).message).toContain("queued 2, parked 0, overflow 16, dead-letter 1");
    expect(actors.status(actor.id)).toMatchObject({ status: "running", queued: 18 });
    expect(meshRecords(root, actor.id)).toEqual(before);
    expect(deadLetterLines(root, actor.id)).toHaveLength(1);
    expect(actors.messages(actor.id).filter(message => message.error?.startsWith("Dropped a queued event"))).toEqual([]);
  }, 30_000);

  it("with dropQueued archives the byte-exact queue with a MOVED-TO pointer, and the actor dir keeps it", async () => {
    const { root, actors, actor, close, release } = await saturated();
    const queued = meshRecords(root, actor.id);
    const deadLetters = deadLetterLines(root, actor.id);
    expect(queued).toHaveLength(18);
    const stopped = await actors.stop(actor.id, undefined, false, "actors.stop", { dropQueued: true });
    const { archive, sha256, counts } = stopArchive(stopped);
    expect(stopped.status).toBe("stopped");
    expect(counts).toEqual({ queued: 2, parked: 0, overflow: 16, deadLetter: 1 });
    expect(path.dirname(archive)).toBe(path.join(root, "actors", actor.id, "stopped-queues"));
    expect(path.basename(archive)).toMatch(new RegExp(`^\\d{8}T\\d{9}Z-${sha256.slice(0, 12)}\\.jsonl$`));
    const bytes = fs.readFileSync(archive);
    expect(createHash("sha256").update(bytes).digest("hex")).toBe(sha256);
    const lines = bytes.toString("utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as { kind: string; item?: unknown; entry?: unknown; counts?: unknown });
    expect(lines[0]).toMatchObject({ kind: "header", actorId: actor.id, actorName: actor.name, counts });
    // Each queued record is the queue checkpoint's own record, byte for byte; each dead letter its own line.
    expect(lines.slice(1, 19).map(line => line.kind)).toEqual([...Array(2).fill("queued"), ...Array(16).fill("overflow")]);
    expect(lines.slice(1, 19).map(line => JSON.stringify(line.item))).toEqual(queued.map(record => JSON.stringify(record)));
    expect(lines.slice(19).map(line => line.kind)).toEqual(["dead-letter"]);
    expect(lines.slice(19).map(line => JSON.stringify(line.entry))).toEqual(deadLetters);
    // The pointer, and the work moved: nothing left in the queue or dead-letter file, nothing recorded as dropped.
    expect(actors.messages(actor.id)).toContainEqual(expect.objectContaining({ text: `MOVED-TO ${archive}\nsha256 ${sha256}` }));
    expect(actors.messages(actor.id).filter(message => message.source.startsWith("mesh:") && message.error?.startsWith("Dropped a queued event"))).toEqual([]);
    expect(meshRecords(root, actor.id)).toEqual([]);
    expect(fs.existsSync(deadLetterFile(root, actor.id))).toBe(false);
    release();
    await waitFor(() => actors.inFlightCount() === 0);
    await close();
    expect(fs.readFileSync(archive).equals(bytes)).toBe(true);
  }, 30_000);

  it("requeueArchived delivers each archived item once through the new actor's filter, and a second call adds nothing", async () => {
    const { root, mesh, actors } = setup(10);
    const old = await actors.create({ name: "old-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    await holdWorker(actors, root, old.id);
    for (let index = 0; index < 4; index++) await publish(mesh, index, index % 2 === 1);
    await waitFor(() => actors.status(old.id).queued === 4);
    const { archive } = stopArchive(await actors.stop(old.id, undefined, false, "actors.stop", { dropQueued: true }));
    // The recreated actor listens elsewhere, so only requeueArchived can feed it these events.
    const fresh = await actors.create({ name: "new-supervisor", instructions: "Supervise.", topics: ["queued-work.other"],
      coalesce: false, activationFilter: WAIT_FILTER });
    const release = await holdWorker(actors, root, fresh.id);
    const first = await actors.requeueArchived(fresh.id, archive);
    expect(first).toMatchObject({ id: fresh.id, archive, requeued: 2, filtered: 2, alreadyRequeued: 0, skipped: [] });
    expect(actors.status(fresh.id).queued).toBe(2);
    expect(markers(meshRecords(root, fresh.id))).toEqual(["event-0", "event-2"]);
    expect(filtered(actors, fresh.id).map(message => message.reason)).toEqual(["filtered: wait", "filtered: wait"]);
    const second = await actors.requeueArchived(fresh.id, archive);
    expect(second).toMatchObject({ requeued: 0, filtered: 0, alreadyRequeued: 4, skipped: [] });
    expect(actors.status(fresh.id).queued).toBe(2);
    expect(filtered(actors, fresh.id)).toHaveLength(2);
    expect(fs.readFileSync(path.join(root, "actors", fresh.id, "requeued-archives.jsonl"), "utf8").split("\n").filter(Boolean)).toHaveLength(4);
    release();
    await waitFor(() => actors.status(fresh.id).status === "idle" &&
      actors.messages(fresh.id).filter(message => message.direction === "out" && message.source === `mesh:${TOPIC}`).length === 2);
    expect(actors.messages(fresh.id).filter(message => message.direction === "out" && message.source === `mesh:${TOPIC}`)).toHaveLength(2);
  }, 30_000);

  it("stops an empty, idle actor exactly as before", async () => {
    const { root, actors } = setup();
    const actor = await actors.create({ name: "idle-actor", instructions: "Wait.", topics: [TOPIC] });
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    const stopped = await actors.stop(actor.id);
    expect(stopped).toMatchObject({ id: actor.id, status: "stopped", stopReason: "explicit", queued: 0 });
    expect(stopped).not.toHaveProperty("stoppedQueue");
    expect(fs.existsSync(path.join(root, "actors", actor.id, "stopped-queues"))).toBe(false);
    expect(actors.messages(actor.id).some(message => message.text?.startsWith("MOVED-TO"))).toBe(false);
    await expect(actors.stop(actor.id)).resolves.toMatchObject({ status: "stopped" });
  }, 30_000);
});

describe("setActivationFilter over queued work (smarty-dev#16416)", () => {
  const MATCHES = new Set([0, 2, 3, 5, 7, 9]);
  const queuedTen = async (limit: number) => {
    const fixture = setup(limit);
    const { root, mesh, actors } = fixture;
    const actor = await actors.create({ name: "filtered-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    await holdWorker(actors, root, actor.id);
    for (let index = 0; index < 10; index++) await publish(mesh, index, MATCHES.has(index));
    await waitFor(() => actors.status(actor.id).queued === 10);
    return { ...fixture, actor };
  };

  it("refuses a filter that would skip queued and overflow items, with the count, and changes nothing", async () => {
    const { root, actors, actor } = await queuedTen(2);
    const before = meshRecords(root, actor.id);
    const refusal = await actors.setActivationFilter(actor.id, WAIT_FILTER).then(info => info, (error: unknown) => error);
    expect(refusal).toBeInstanceOf(queuedWorkError() ?? class Missing {});
    // Queue holds events 0 and 1 (one skip); the overflow holds 2..9 (five skips).
    expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "setActivationFilter", actorId: actor.id,
      counts: { skipped: 6, queued: 1, overflow: 5 } });
    expect((refusal as Error).message).toContain("would skip 6 already queued item(s)");
    const status = actors.status(actor.id);
    expect(status.activationFilter).toBeUndefined();
    expect(status).toMatchObject({ queued: 10, filterSkipped: { count: 0 } });
    expect(meshRecords(root, actor.id)).toEqual(before);
    expect(filtered(actors, actor.id)).toEqual([]);
  }, 30_000);

  it("with dropQueued moves the skipped items to the dead-letter queue, returns the count, and never deletes them", async () => {
    const { root, actors, actor } = await queuedTen(10);
    const before = meshRecords(root, actor.id);
    const skipped = before.filter(record => record.payload.kind === "wait");
    const changed = await actors.setActivationFilter(actor.id, WAIT_FILTER, undefined, undefined, undefined, { dropQueued: true });
    expect(changed).toMatchObject({ movedToDeadLetter: 6, queued: 4, status: "running", filterSkipped: { count: 0 } });
    expect(changed.activationFilter).toBeDefined();
    expect(markers(meshRecords(root, actor.id))).toEqual(["event-1", "event-4", "event-6", "event-8"]);
    const held = deadLetterLines(root, actor.id).map(line => JSON.parse(line) as { source: string; event: MeshEvent; heldBy?: { itemId: string; ruleId: string } });
    expect(held.map(entry => entry.event.id)).toEqual(skipped.map(record => record.payload.id));
    expect(held.map(entry => entry.heldBy)).toEqual(skipped.map(record => expect.objectContaining({ itemId: record.id, ruleId: "wait" })));
    expect(filtered(actors, actor.id)).toEqual([]);
    expect(actors.messages(actor.id)).toContainEqual(expect.objectContaining({ source: "actor:activation-filter",
      reason: "moved 6 queued item(s) the new activationFilter skips to the dead-letter queue" }));
    // Re-applying the filter skips nothing queued; replay keeps the held entries while the filter skips them.
    await expect(actors.setActivationFilter(actor.id, WAIT_FILTER)).resolves.toMatchObject({ queued: 4 });
    expect(deadLetterLines(root, actor.id)).toHaveLength(6);
    // Clearing the filter restores them through ordinary dead-letter replay.
    await actors.setActivationFilter(actor.id, null);
    expect(actors.status(actor.id).queued).toBe(10);
    expect(markers(meshRecords(root, actor.id)).sort()).toEqual([...Array(10).keys()].map(index => `event-${index}`).sort());
    expect(fs.existsSync(deadLetterFile(root, actor.id))).toBe(false);
  }, 30_000);

  it("leaves a filter that skips nothing queued, and an empty queue, unchanged", async () => {
    const { root, mesh, actors } = setup(10);
    const idle = await actors.create({ name: "empty-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    const empty = await actors.setActivationFilter(idle.id, WAIT_FILTER);
    expect(empty).not.toHaveProperty("movedToDeadLetter");
    expect(empty.activationFilter).toBeDefined();
    const actor = await actors.create({ name: "working-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    await holdWorker(actors, root, actor.id);
    for (let index = 0; index < 4; index++) await publish(mesh, index);
    await waitFor(() => actors.status(actor.id).queued === 4);
    const changed = await actors.setActivationFilter(actor.id, WAIT_FILTER);
    expect(changed).not.toHaveProperty("movedToDeadLetter");
    expect(changed).toMatchObject({ queued: 4, filterSkipped: { count: 0 } });
    expect(markers(meshRecords(root, actor.id))).toEqual(["event-0", "event-1", "event-2", "event-3"]);
    expect(fs.existsSync(deadLetterFile(root, actor.id))).toBe(false);
  }, 30_000);
});

describe("resident stop forwarding (smarty-dev#16416)", () => {
  it("a stop forwarded to a resident-owned actor honours the same refusal, and dropQueued archives", async () => {
    installInProcessResidentFence();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-queued-work-resident-"));
    roots.push(root);
    const rootIdentity = { id: "session:queued-resident", name: "Main", kind: "main" as const, sessionId: "queued-resident" };
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: rootIdentity.id, sessionId: rootIdentity.sessionId,
      cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
      residencyRoot: residentRoot(path.join(root, "mesh"), rootIdentity.id), fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
    };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    const mesh = new MeshStore(config.meshRoot, 65_536, 1_000);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: rootIdentity.id, rootId: rootIdentity.id, identity: rootIdentity });
    participants.registerSource(() => [{
      format: 1, id: rootIdentity.id, kind: "root", rootId: rootIdentity.id, ownerHostId: rootIdentity.id, ownerIdentityId: rootIdentity.id,
      name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: [],
      sessionId: rootIdentity.sessionId, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
    }]);
    const host = new ResidentHost(config);
    closers.push(async () => { await host.close(); await participants.close(); });
    await participants.start();
    await host.start();
    const client = new ResidentActorClient(config.meshRoot, config.rootId);
    const caller = { identity: rootIdentity, hostId: rootIdentity.id };
    const actor = await host.actors.create({ name: "resident-supervisor", instructions: "Supervise.", residency: "durable", topics: [TOPIC], coalesce: false });
    await holdWorker(host.actors, root, actor.id);
    for (let index = 0; index < 3; index++) await publish(mesh, index);
    await waitFor(() => host.actors.status(actor.id).queued === 3);
    const refusal = await client.setActor({ operation: "stop", id: actor.id }, undefined, caller).then(info => info, (error: unknown) => error);
    expect(refusal).toBeInstanceOf(queuedWorkError() ?? class Missing {});
    expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "stop", actorId: actor.id,
      counts: { queued: 3, parked: 0, overflow: 0, deadLetter: 0 } });
    expect(host.actors.status(actor.id)).toMatchObject({ status: "running", queued: 3 });
    const stopped = await client.setActor({ operation: "stop", id: actor.id, dropQueued: true } as Parameters<ResidentActorClient["setActor"]>[0], undefined, caller);
    expect(stopped.status).toBe("stopped");
    const { archive, counts } = stopArchive(stopped);
    expect(counts).toEqual({ queued: 3, parked: 0, overflow: 0, deadLetter: 0 });
    expect(path.dirname(archive)).toBe(path.join(root, "actors", actor.id, "stopped-queues"));
    expect(fs.readFileSync(archive, "utf8").split("\n").filter(Boolean)).toHaveLength(4);
  }, 30_000);
});

// pi-fabric#1572 round 2: the four P1s of the fabric-store review.
describe("queued-work guard, round 2 (pi-fabric#1572)", () => {
  const sha = (text: string | Buffer) => createHash("sha256").update(text).digest("hex");
  // A file shaped exactly like a stop archive, named by its own sha256: what anyone can write.
  const forge = (directory: string, lines: unknown[]) => {
    const text = lines.map(line => `${JSON.stringify(line)}\n`).join("");
    fs.mkdirSync(directory, { recursive: true });
    const file = path.join(directory, `20261024T000000000Z-${sha(text).slice(0, 12)}.jsonl`);
    fs.writeFileSync(file, text);
    return file;
  };
  const header = (actorId: string, counts = { queued: 2, parked: 0, overflow: 0, deadLetter: 0 }) =>
    ({ format: 1, kind: "header", actorId, actorName: "forged", rootId: identity.id, stoppedAt: Date.now(), counts });
  const forgedItems = [
    { kind: "queued", item: { id: "forged-direct", source: "direct", payload: { message: "FORGED HOST ORDER" }, createdAt: Date.now(),
      sender: { authority: "host" } } },
    { kind: "queued", item: { id: "forged-mesh", source: `mesh:${TOPIC}`, createdAt: Date.now(), payload: {
      id: "forged-event", topic: TOPIC, kind: "work", from, sequence: 999_999, createdAt: Date.now(),
      verification: "mesh", principal: { id: "paul" }, data: { marker: "FORGED" } } } },
  ];

  it("P1: requeueArchived refuses an archive no stop of the actor it names recorded, and feeds the recorded one", async () => {
    const { root, mesh, actors } = setup(10);
    const old = await actors.create({ name: "old-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    await holdWorker(actors, root, old.id);
    for (let index = 0; index < 2; index++) await publish(mesh, index);
    await waitFor(() => actors.status(old.id).queued === 2);
    const { archive, sha256 } = stopArchive(await actors.stop(old.id, undefined, false, "actors.stop", { dropQueued: true }));
    const fresh = await actors.create({ name: "new-supervisor", instructions: "Supervise.", topics: ["queued-work.other"], coalesce: false });
    await holdWorker(actors, root, fresh.id);
    const refusedFor = (file: string) => actors.requeueArchived(fresh.id, file).then(() => "accepted", (error: unknown) => (error as Error).message);
    // A forged file in the stopped actor's own directory, naming it, with a host sender and a verified principal.
    const forgedOld = forge(path.dirname(archive), [header(old.id), ...forgedItems]);
    expect(await refusedFor(forgedOld)).toMatch(/requeueArchived refused .*no stop record/);
    // The recorded archive, edited and renamed to its new sha256.
    const original = fs.readFileSync(archive, "utf8");
    const edited = original.replace("event-0", "event-X");
    const tampered = path.join(path.dirname(archive), `20261024T000000001Z-${sha(edited).slice(0, 12)}.jsonl`);
    fs.writeFileSync(tampered, edited);
    expect(await refusedFor(tampered)).toMatch(/no stop record/);
    // A forged archive naming the target itself, in its own directory: it never stopped.
    const forgedFresh = forge(path.join(root, "actors", fresh.id, "stopped-queues"), [header(fresh.id), ...forgedItems]);
    expect(await refusedFor(forgedFresh)).toMatch(/no stop record/);
    // A copy of the recorded archive elsewhere is not the recorded file.
    const copy = path.join(root, "actors", fresh.id, "stopped-queues", path.basename(archive));
    fs.copyFileSync(archive, copy);
    expect(await refusedFor(copy)).toMatch(/requeueArchived refused/);
    expect(actors.status(fresh.id).queued).toBe(0);
    expect(fs.existsSync(path.join(root, "actors", fresh.id, "requeued-archives.jsonl"))).toBe(false);
    // The archive the stop recorded is fed, callerless, with the provenance the stop archived.
    const fed = await actors.requeueArchived(fresh.id, archive);
    expect(fed).toMatchObject({ id: fresh.id, archive, sha256, requeued: 2, filtered: 0, alreadyRequeued: 0, skipped: [] });
    expect(markers(meshRecords(root, fresh.id))).toEqual(["event-0", "event-1"]);
    expect(meshRecords(root, fresh.id).map(record => record.payload.from.id)).toEqual([from.id, from.id]);
  }, 30_000);

  it("P2: a requeued old item never replaces newer queued work with its coalesce key; the rest append in order", async () => {
    const { root, mesh, actors } = setup(10);
    const state = (subject: string, marker: string) => mesh.publish({ topic: TOPIC, kind: "work", from, data: { subject, marker } });
    const old = await actors.create({ name: "old-state", instructions: "Track.", topics: [TOPIC], coalesce: false, coalesceKey: "subject" });
    await holdWorker(actors, root, old.id);
    await state("s1", "OLD-STATE");
    await state("s2", "B-OLD");
    await waitFor(() => actors.status(old.id).queued === 2);
    const { archive } = stopArchive(await actors.stop(old.id, undefined, false, "actors.stop", { dropQueued: true }));
    const fresh = await actors.create({ name: "new-state", instructions: "Track.", topics: [TOPIC], coalesce: false, coalesceKey: "subject" });
    await holdWorker(actors, root, fresh.id);
    await state("s1", "NEW-STATE");
    await waitFor(() => actors.status(fresh.id).queued === 1);
    expect(markers(meshRecords(root, fresh.id))).toEqual(["NEW-STATE"]);
    const fed = await actors.requeueArchived(fresh.id, archive);
    expect(markers(meshRecords(root, fresh.id))).toEqual(["NEW-STATE", "B-OLD"]);
    expect(fed).toMatchObject({ requeued: 1, superseded: 1, filtered: 0, alreadyRequeued: 0 });
  }, 30_000);

  it("P3: remove refuses while stop archives or held dead letters exist; with dropQueued it keeps them", async () => {
    const { root, mesh, actors } = setup(10);
    const kept = (actorId: string) => {
      const base = path.join(root, "mesh", "session-archive");
      const roots = fs.existsSync(base) ? fs.readdirSync(base) : [];
      return roots.flatMap(entry => fs.readdirSync(path.join(base, entry)).filter(name => name.startsWith(`${actorId}.recoverable-`))
        .map(name => path.join(base, entry, name)));
    };
    // An actor whose stop archived its queue.
    const archived = await actors.create({ name: "archived-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    const releaseArchived = await holdWorker(actors, root, archived.id);
    for (let index = 0; index < 2; index++) await publish(mesh, index);
    await waitFor(() => actors.status(archived.id).queued === 2);
    const { archive } = stopArchive(await actors.stop(archived.id, undefined, false, "actors.stop", { dropQueued: true }));
    const bytes = fs.readFileSync(archive);
    releaseArchived();
    await waitFor(() => actors.inFlightCount() === 0);
    const refusal = await actors.remove(archived.id).then(() => undefined, (error: unknown) => error);
    expect(refusal).toBeInstanceOf(queuedWorkError() ?? class Missing {});
    expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "remove", actorId: archived.id, counts: { archived: 1, heldDeadLetter: 0 } });
    expect(fs.readFileSync(archive).equals(bytes)).toBe(true);
    expect(actors.status(archived.id).status).toBe("stopped");
    await expect(actors.remove(archived.id, { dropQueued: true })).resolves.toMatchObject({ removed: true });
    expect(fs.existsSync(path.join(root, "actors", archived.id))).toBe(false);
    const [keptArchived] = kept(archived.id);
    expect(keptArchived).toBeDefined();
    expect(fs.readFileSync(path.join(keptArchived!, "stopped-queues", path.basename(archive))).equals(bytes)).toBe(true);
    // An actor whose filter change holds dead letters.
    const held = await actors.create({ name: "held-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    const releaseHeld = await holdWorker(actors, root, held.id);
    for (let index = 0; index < 4; index++) await publish(mesh, index, index % 2 === 0);
    await waitFor(() => actors.status(held.id).queued === 4);
    await actors.setActivationFilter(held.id, WAIT_FILTER, undefined, undefined, undefined, { dropQueued: true });
    const heldLines = deadLetterLines(root, held.id);
    expect(heldLines).toHaveLength(2);
    const heldRefusal = await actors.remove(held.id).then(() => undefined, (error: unknown) => error);
    expect(heldRefusal).toMatchObject({ code: QUEUED_WORK, operation: "remove", actorId: held.id, counts: { archived: 0, heldDeadLetter: 2 } });
    expect(deadLetterLines(root, held.id)).toEqual(heldLines);
    expect(actors.status(held.id).status).toBe("running");
    const removing = actors.remove(held.id, { dropQueued: true });
    releaseHeld();
    await expect(removing).resolves.toMatchObject({ removed: true });
    const [keptHeld] = kept(held.id);
    expect(keptHeld).toBeDefined();
    // The removal's stop folded the held dead letters into a stop archive, kept with the tree.
    const keptLines = fs.readdirSync(path.join(keptHeld!, "stopped-queues")).flatMap(name =>
      fs.readFileSync(path.join(keptHeld!, "stopped-queues", name), "utf8").split("\n").filter(Boolean));
    expect(keptLines.filter(line => (JSON.parse(line) as { kind: string }).kind === "dead-letter").map(line =>
      JSON.stringify((JSON.parse(line) as { entry: unknown }).entry))).toEqual(heldLines);
  }, 60_000);

  it("P4: setActivationFilter counts the routed dead-letter backlog it would skip; with dropQueued it holds it", async () => {
    const fixture = setup(2);
    const { root, mesh, actors } = fixture;
    const actor = await actors.create({ name: "backlog-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    const release = await holdWorker(actors, root, actor.id);
    for (let index = 0; index < 18; index++) await publish(mesh, index);
    const waiting = [await publish(mesh, 18, true), await publish(mesh, 19, true)];
    await waitFor(() => actors.status(actor.id).queued === 18 && deadLetterLines(root, actor.id).length === 2);
    const before = deadLetterLines(root, actor.id);
    const refusal = await actors.setActivationFilter(actor.id, WAIT_FILTER).then(info => info, (error: unknown) => error);
    expect(refusal).toBeInstanceOf(queuedWorkError() ?? class Missing {});
    expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "setActivationFilter", actorId: actor.id,
      counts: { skipped: 2, queued: 0, overflow: 0, deadLetter: 2 } });
    expect(actors.status(actor.id).activationFilter).toBeUndefined();
    expect(deadLetterLines(root, actor.id)).toEqual(before);
    const changed = await actors.setActivationFilter(actor.id, WAIT_FILTER, undefined, undefined, undefined, { dropQueued: true });
    expect(changed).toMatchObject({ heldInDeadLetter: 2, queued: 18 });
    expect(changed).not.toHaveProperty("movedToDeadLetter");
    const held = deadLetterLines(root, actor.id).map(line => JSON.parse(line) as { event: MeshEvent; heldBy?: { ruleId: string; itemId: string } });
    expect(held.map(entry => entry.event.id)).toEqual(waiting.map(event => event.id));
    expect(held.map(entry => entry.heldBy)).toEqual(waiting.map(event => expect.objectContaining({ ruleId: "wait", itemId: event.id })));
    // The queue drains; replay keeps the held backlog instead of filtering it away.
    release();
    await waitFor(() => actors.status(actor.id).status === "idle" && actors.status(actor.id).queued === 0, 45_000);
    expect(deadLetterLines(root, actor.id).map(line => (JSON.parse(line) as { event: MeshEvent }).event.id)).toEqual(waiting.map(event => event.id));
    expect(filtered(actors, actor.id)).toEqual([]);
  }, 90_000);
});
