import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import * as actorTypes from "../src/actors/types.js";
import type { FabricActorActivationFilter, FabricActorInfo } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { runFabricCommand } from "../src/commands/fabric-handler.js";

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
// A returned archive path is native: Windows joins it with backslashes (pi-fabric#1572 round 4; CI windows-latest).
const SEP = "[\\\\/]";
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
    // Round 3 (pi-fabric#1572): only an explicit operator stop is guarded (astra F1).
    const refusal = await actors.stop(actor.id, undefined, false, "actors.stop", { explicit: true }).then(info => info, (error: unknown) => error);
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
    // Round 3: the activation in flight (the held worker's item) is archived too (astra F3).
    expect(counts).toEqual({ queued: 2, parked: 0, overflow: 16, deadLetter: 1, inFlight: 1, settled: 0 });
    expect(path.dirname(archive)).toBe(path.join(root, "actors", actor.id, "stopped-queues"));
    expect(path.basename(archive)).toMatch(new RegExp(`^\\d{8}T\\d{9}Z-${sha256.slice(0, 12)}\\.jsonl$`));
    const bytes = fs.readFileSync(archive);
    expect(createHash("sha256").update(bytes).digest("hex")).toBe(sha256);
    const lines = bytes.toString("utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as { kind: string; item?: unknown; entry?: unknown; counts?: unknown });
    expect(lines[0]).toMatchObject({ kind: "header", actorId: actor.id, actorName: actor.name, counts });
    // Each queued record is the queue checkpoint's own record, byte for byte; each dead letter its own line.
    expect(lines[1]).toMatchObject({ kind: "in-flight", item: { source: "direct", payload: { message: "LIVE_WITH_PROGRESS" } } });
    expect(lines.slice(2, 20).map(line => line.kind)).toEqual([...Array(2).fill("queued"), ...Array(16).fill("overflow")]);
    expect(lines.slice(2, 20).map(line => JSON.stringify(line.item))).toEqual(queued.map(record => JSON.stringify(record)));
    expect(lines.slice(20).map(line => line.kind)).toEqual(["dead-letter"]);
    expect(lines.slice(20).map(line => JSON.stringify(line.entry))).toEqual(deadLetters);
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

  // Round 3 (astra P1 c6385171802): the same flow with the SAME name, where create removes the stopped predecessor.
  it.each(["new-supervisor", "old-supervisor"])("requeueArchived delivers each archived item once through the new actor's filter, and a second call adds nothing (recreated as %s)", async (name) => {
    const { root, mesh, actors } = setup(10);
    const old = await actors.create({ name: "old-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    const releaseOld = await holdWorker(actors, root, old.id);
    for (let index = 0; index < 4; index++) await publish(mesh, index, index % 2 === 1);
    await waitFor(() => actors.status(old.id).queued === 4);
    const { archive } = stopArchive(await actors.stop(old.id, undefined, false, "actors.stop", { dropQueued: true }));
    releaseOld();
    await waitFor(() => actors.inFlightCount() === 0);
    // The recreated actor listens elsewhere, so only requeueArchived can feed it these events.
    const fresh = await actors.create({ name, instructions: "Supervise.", topics: ["queued-work.other"],
      coalesce: false, activationFilter: WAIT_FILTER });
    // A same-name create removes the predecessor; its tree goes, its archive moves to session-archive/.
    if (name === "old-supervisor") await waitFor(() => !fs.existsSync(path.join(root, "actors", old.id)));
    expect(fs.existsSync(archive)).toBe(name !== "old-supervisor");
    const release = await holdWorker(actors, root, fresh.id);
    const first = await actors.requeueArchived(fresh.id, archive);
    // The old activation in flight (its released worker item) is requeued too (round 3, astra F3).
    expect(first).toMatchObject({ id: fresh.id, requeued: 3, filtered: 2, alreadyRequeued: 0, skipped: [] });
    expect(first.archive).toEqual(name === "old-supervisor"
      ? expect.stringMatching(new RegExp(`session-archive.*${old.id}\\.recoverable-[0-9TZ]+${SEP}stopped-queues${SEP}${path.basename(archive)}$`)) : archive);
    expect(actors.status(fresh.id).queued).toBe(3);
    expect(markers(meshRecords(root, fresh.id))).toEqual(["event-0", "event-2"]);
    expect(filtered(actors, fresh.id).map(message => message.reason)).toEqual(["filtered: wait", "filtered: wait"]);
    const second = await actors.requeueArchived(fresh.id, archive);
    expect(second).toMatchObject({ requeued: 0, filtered: 0, alreadyRequeued: 5, skipped: [] });
    expect(actors.status(fresh.id).queued).toBe(3);
    expect(filtered(actors, fresh.id)).toHaveLength(2);
    // Round 3: the ledger is per archive, next to it.
    expect(fs.readFileSync(first.archive.replace(/\.jsonl$/, ".requeued.jsonl"), "utf8").split("\n").filter(Boolean)).toHaveLength(5);
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
    expect(counts).toEqual({ queued: 3, parked: 0, overflow: 0, deadLetter: 0, inFlight: 1, settled: 0 });
    expect(path.dirname(archive)).toBe(path.join(root, "actors", actor.id, "stopped-queues"));
    expect(fs.readFileSync(archive, "utf8").split("\n").filter(Boolean)).toHaveLength(5);
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
    expect(fs.existsSync(archive.replace(/\.jsonl$/, ".requeued.jsonl"))).toBe(false);
    // The archive the stop recorded is fed, callerless, with the provenance the stop archived (and, round 3, its in-flight item).
    const fed = await actors.requeueArchived(fresh.id, archive);
    expect(fed).toMatchObject({ id: fresh.id, archive, sha256, requeued: 3, filtered: 0, alreadyRequeued: 0, skipped: [] });
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
    // requeued: B-OLD and (round 3) the old activation's in-flight item.
    expect(fed).toMatchObject({ requeued: 2, superseded: 1, filtered: 0, alreadyRequeued: 0 });
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
    expect((heldRefusal as Error).message).not.toContain("requeue it first");
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

// pi-fabric#1572 round 3: astra F1/F3, the same-name restore (astra P1 c6385171802), fabric-store P2 a-e and the resident removal.
describe("queued-work guard, round 3 (pi-fabric#1572)", () => {
  const archiveLines = (archive: string) => fs.readFileSync(archive, "utf8").split("\n").filter(Boolean)
    .map(line => JSON.parse(line) as { kind: string; item?: { source?: string; payload?: unknown; settledWindow?: unknown }; counts?: unknown });
  const providerFor = (fixture: ReturnType<typeof setup>) => {
    const participants = { scheduleRefresh() {}, list: () => [], get: () => undefined } as unknown as FabricParticipantSource;
    const main = { id: identity.id, local: true, matches: (id: string) => id === identity.id } as FabricMainAgentTarget;
    const lifecycle = new LifecycleBroker(fixture.mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(fixture.agents, fixture.actors, new GlobalActorRegistry(fixture.root, 64 * 1024), main, participants, undefined, lifecycle);
    const context: FabricInvocationContext = {
      cwd: process.cwd(), signal: undefined, parentToolCallId: "test-parent", nestedToolCallId: "test-nested",
      extensionContext: {} as FabricInvocationContext["extensionContext"], activity() {}, update() {},
    };
    return { provider, context };
  };
  const settledPayload = (marker: string) => ({ event: "agent_settled", session: { id: "observed-a", cwd: process.cwd() }, digest: {}, transcript: [],
    signal: { payload: { type: "agent_settled", source: "extension" }, idle: true, observedAt: 123 }, marker });

  it("F1: an internal stop and a non-persistent close with queued work are never refused and settle the waiting asks", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-queued-work-ephemeral-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: false,
    });
    closers.push(async () => { await agents.close(); });
    // An internal (non-explicit) stop of an actor holding a queued ask stops as on e50db08f.
    const first = await actors.create({ name: "internal-stop", instructions: "Work.", responseMode: "text" });
    const hanging = actors.ask(first.id, "HANG").catch((error: unknown) => error);
    await waitFor(() => actors.status(first.id).status === "running");
    const waiting = actors.ask(first.id, "queued behind the hanging run").catch((error: unknown) => error);
    await expect(actors.stop(first.id)).resolves.toMatchObject({ status: "stopped", queued: 0 });
    expect(await waiting).toMatchObject({ message: expect.stringContaining("was stopped while messages were queued") });
    await hanging;
    // The non-persistent close stops every actor (session-reset) despite its queued work, and does not hang.
    const second = await actors.create({ name: "ephemeral", instructions: "Work.", responseMode: "text" });
    const inFlight = actors.ask(second.id, "HANG").catch((error: unknown) => error);
    await waitFor(() => actors.status(second.id).status === "running");
    const queued = actors.ask(second.id, "queued behind the hanging run").catch((error: unknown) => error);
    expect(actors.status(second.id).queued).toBe(1);
    const closed = actors.close().then(() => "closed");
    expect(await Promise.race([closed, new Promise(resolve => setTimeout(() => resolve("close hung"), 15_000))])).toBe("closed");
    expect(await queued).toMatchObject({ message: expect.stringContaining("was stopped while messages were queued") });
    await inFlight;
  }, 60_000);

  it("F3 (round 4): an explicit stop over an activation in flight is not refused; it archives the callerless in-flight item", async () => {
    const { root, actors } = setup();
    const actor = await actors.create({ name: "busy-supervisor", instructions: "Supervise.", topics: [TOPIC] });
    const release = await holdWorker(actors, root, actor.id);
    expect(actors.status(actor.id)).toMatchObject({ status: "running", queued: 0 });
    // Stopping a busy actor is the normal operator action: the run is stopped, and its item is archived, not dropped.
    const stopped = await actors.stop(actor.id, undefined, false, "actors.stop", { explicit: true });
    const { archive, sha256, counts } = stopArchive(stopped);
    expect(stopped.status).toBe("stopped");
    expect(counts).toEqual({ queued: 0, parked: 0, overflow: 0, deadLetter: 0, inFlight: 1, settled: 0 });
    expect(path.dirname(archive)).toBe(path.join(root, "actors", actor.id, "stopped-queues"));
    const lines = archiveLines(archive);
    expect(lines.map(line => line.kind)).toEqual(["header", "in-flight"]);
    expect(lines[1]!.item).toMatchObject({ source: "direct", payload: { message: "LIVE_WITH_PROGRESS" } });
    expect(actors.messages(actor.id)).toContainEqual(expect.objectContaining({ text: `MOVED-TO ${archive}\nsha256 ${sha256}` }));
    release();
    await waitFor(() => !actors.status(actor.id).inFlightRun);
    // A second explicit stop of the stopped actor has nothing in flight of its own left to archive.
    await expect(actors.stop(actor.id, undefined, false, "actors.stop", { explicit: true })).resolves.not.toHaveProperty("stoppedQueue");
    // The archived in-flight item is fed to a successor like a queued one.
    const fresh = await actors.create({ name: "busy-successor", instructions: "Supervise.", topics: ["queued-work.other"] });
    await holdWorker(actors, root, fresh.id);
    await expect(actors.requeueArchived(fresh.id, archive)).resolves.toMatchObject({ requeued: 1, skipped: [] });
  }, 30_000);

  it("F3 (round 4): an explicit stop over a caller's ask in flight stops it as before; the ask is rejected and nothing is archived", async () => {
    const { root, actors } = setup();
    const actor = await actors.create({ name: "asked-supervisor", instructions: "Supervise.", responseMode: "text" });
    const asked = actors.ask(actor.id, "HANG").then(() => "answered", (error: unknown) => error);
    await waitFor(() => Boolean(actors.status(actor.id).inFlightRun));
    const stopped = await actors.stop(actor.id, undefined, false, "actors.stop", { explicit: true });
    expect(stopped).toMatchObject({ status: "stopped" });
    expect(stopped).not.toHaveProperty("stoppedQueue");
    expect(await asked).toBeInstanceOf(Error);
    expect(fs.existsSync(path.join(root, "actors", actor.id, "stopped-queues"))).toBe(false);
    expect(actors.messages(actor.id).some(message => message.text?.startsWith("MOVED-TO"))).toBe(false);
    // Nothing kept, so a plain remove is not refused.
    await expect(actors.remove(actor.id)).resolves.toMatchObject({ removed: true });
  }, 30_000);

  it("same-name restore: stop with dropQueued, create the same name, requeueArchived queues every archived item; forgeries stay refused", async () => {
    const fixture = setup(10);
    const { root, mesh, actors } = fixture;
    const { provider, context } = providerFor(fixture);
    const old = await actors.create({ name: "restored-supervisor", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    const releaseOld = await holdWorker(actors, root, old.id);
    for (let index = 0; index < 3; index++) await publish(mesh, index);
    await waitFor(() => actors.status(old.id).queued === 3);
    // The documented flow, on the agents tool: stop with dropQueued, create the same name, requeueArchived.
    const stopped = await provider.invoke("stop", { id: old.id, dropQueued: true }, context) as FabricActorInfo;
    const { archive, sha256 } = stopArchive(stopped);
    releaseOld();
    await waitFor(() => actors.inFlightCount() === 0);
    const fresh = await provider.invoke("create", { name: "restored-supervisor", instructions: "Supervise.", topics: ["queued-work.other"], coalesce: false },
      context) as FabricActorInfo;
    expect(fresh.id).not.toBe(old.id);
    await waitFor(() => !fs.existsSync(path.join(root, "actors", old.id)));
    const releaseFresh = await holdWorker(actors, root, fresh.id);
    const fed = await provider.invoke("requeueArchived", { id: fresh.id, archive }, context) as actorTypes.FabricActorRequeueResult;
    expect(fed).toMatchObject({ id: fresh.id, sha256, requeued: 4, filtered: 0, superseded: 0, alreadyRequeued: 0, skipped: [] });
    const moved = fed.archive;
    expect(moved).toMatch(new RegExp(`session-archive.*${old.id}\\.recoverable-[0-9TZ]+${SEP}stopped-queues${SEP}${path.basename(archive)}$`));
    expect(markers(meshRecords(root, fresh.id))).toEqual(["event-0", "event-1", "event-2"]);
    expect(actors.status(fresh.id).queued).toBe(4);
    // Each archive is requeued once, through either path.
    await expect(actors.requeueArchived(fresh.id, moved)).resolves.toMatchObject({ requeued: 0, alreadyRequeued: 4 });
    // Another name in the same root and project is refused, as are an edited copy and a forged archive in a recoverable dir.
    const other = await actors.create({ name: "another-supervisor", instructions: "Supervise.", topics: ["queued-work.other"], coalesce: false });
    await expect(actors.requeueArchived(other.id, moved)).rejects.toThrow(/only a successor named restored-supervisor takes it/);
    const edited = fs.readFileSync(moved, "utf8").replace("event-0", "event-X");
    const tampered = path.join(path.dirname(moved), `20261024T000000001Z-${createHash("sha256").update(edited).digest("hex").slice(0, 12)}.jsonl`);
    fs.writeFileSync(tampered, edited);
    await expect(actors.requeueArchived(fresh.id, tampered)).rejects.toThrow(/no stop record carried with it/);
    const forgedDir = path.join(path.dirname(path.dirname(path.dirname(moved))), `${old.id}.recoverable-20261024T000000000Z`, "stopped-queues");
    fs.mkdirSync(forgedDir, { recursive: true });
    const forgedFile = path.join(forgedDir, path.basename(moved));
    fs.copyFileSync(moved, forgedFile);
    await expect(actors.requeueArchived(fresh.id, forgedFile)).rejects.toThrow(/no stop record carried with it/);
    expect(actors.status(fresh.id).queued).toBe(4);
    releaseFresh();
  }, 60_000);

  it("P2 a/e: setActivationFilter counts and holds a queued interval agent_settled item; the same filter again is a no-op; a changed one names dropQueued", async () => {
    const { root, actors } = setup();
    const actor = await actors.create({ name: "settled-observer", instructions: "Observe.", events: ["agent_settled"], activation: { minIntervalMs: 60_000 } });
    const release = await holdWorker(actors, root, actor.id);
    expect(actors.dispatchHostEvent("agent_settled", settledPayload("leading"))).toBe(1);
    await waitFor(() => actors.status(actor.id).queued === 1);
    const SETTLES: FabricActorActivationFilter = [{ id: "settles", source: ["host:agent_settled"] }];
    const refusal = await actors.setActivationFilter(actor.id, SETTLES).then(info => info, (error: unknown) => error);
    expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "setActivationFilter", counts: { skipped: 1, queued: 1 } });
    expect((refusal as Error).message).toMatch(/Re-applying the current filter unchanged is a no-op.*pass dropQueued: true/);
    expect(actors.status(actor.id).activationFilter).toBeUndefined();
    const changed = await actors.setActivationFilter(actor.id, SETTLES, undefined, undefined, undefined, { dropQueued: true });
    expect(changed).toMatchObject({ movedToDeadLetter: 1, queued: 0 });
    // The identical filter again: a no-op, never refused.
    await expect(actors.setActivationFilter(actor.id, SETTLES)).resolves.toMatchObject({ queued: 0 });
    release();
    await waitFor(() => actors.status(actor.id).status === "idle");
    // Held, never filtered away, while the filter skips it; clearing the filter brings it back.
    expect(deadLetterLines(root, actor.id).map(line => JSON.parse(line) as { source: string; heldBy?: unknown }))
      .toEqual([expect.objectContaining({ source: "host:agent_settled", heldBy: expect.objectContaining({ ruleId: "settles", raw: true }) })]);
    expect(filtered(actors, actor.id)).toEqual([]);
    await actors.setActivationFilter(actor.id, null);
    await waitFor(() => deadLetterLines(root, actor.id).length === 0);
  }, 30_000);

  it("P2 b: an explicit stop counts pending settled windows; dropQueued archives them", async () => {
    const { root, actors } = setup();
    const actor = await actors.create({ name: "window-observer", instructions: "Observe.", events: ["agent_settled"], activation: { minIntervalMs: 60_000 } });
    const release = await holdWorker(actors, root, actor.id);
    actors.dispatchHostEvent("agent_settled", settledPayload("leading"));
    await waitFor(() => actors.status(actor.id).queued === 1);
    actors.dispatchHostEvent("agent_settled", settledPayload("trailing"));       // inside the interval: a pending window
    const refusal = await actors.stop(actor.id, undefined, false, "actors.stop", { explicit: true }).then(info => info, (error: unknown) => error);
    expect(refusal).toMatchObject({ code: QUEUED_WORK, counts: { queued: 1, inFlight: 1, settled: 1 } });
    const { archive, counts } = stopArchive(await actors.stop(actor.id, undefined, false, "actors.stop", { explicit: true, dropQueued: true }));
    expect(counts).toMatchObject({ queued: 1, inFlight: 1, settled: 1 });
    const settled = archiveLines(archive).filter(line => line.kind === "settled");
    expect(settled).toEqual([expect.objectContaining({ item: expect.objectContaining({ source: "host:agent_settled",
      payload: expect.objectContaining({ marker: "trailing" }), settledWindow: expect.objectContaining({ sourceId: "session:observed-a" }) }) })]);
    release();
  }, 30_000);

  it("P2 c: the requeue ledger is per archive: a second actor gets nothing already requeued", async () => {
    const { root, mesh, actors } = setup(10);
    const old = await actors.create({ name: "ledger-old", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    const releaseOld = await holdWorker(actors, root, old.id);
    for (let index = 0; index < 2; index++) await publish(mesh, index);
    await waitFor(() => actors.status(old.id).queued === 2);
    const { archive } = stopArchive(await actors.stop(old.id, undefined, false, "actors.stop", { dropQueued: true }));
    releaseOld();
    const first = await actors.create({ name: "ledger-first", instructions: "Supervise.", topics: ["queued-work.other"], coalesce: false });
    const second = await actors.create({ name: "ledger-second", instructions: "Supervise.", topics: ["queued-work.other"], coalesce: false });
    await holdWorker(actors, root, first.id);
    await holdWorker(actors, root, second.id);
    await expect(actors.requeueArchived(first.id, archive)).resolves.toMatchObject({ requeued: 3, alreadyRequeued: 0 });
    await expect(actors.requeueArchived(second.id, archive)).resolves.toMatchObject({ requeued: 0, alreadyRequeued: 3 });
    expect(meshRecords(root, second.id)).toEqual([]);
  }, 30_000);

  it("P2 d: the agents.stop tool and /fabric stop refuse an actor holding work and stop it with dropQueued", async () => {
    const fixture = setup();
    const { root, actors, agents } = fixture;
    const { provider, context } = providerFor(fixture);
    const tool = await actors.create({ name: "tool-stopped", instructions: "Supervise.", topics: [TOPIC] });
    const releaseTool = await holdWorker(actors, root, tool.id);
    // Round 4: the activation in flight alone never refuses a stop; a queued item does.
    actors.tell(tool.id, "queued behind the held run");
    await expect(provider.invoke("stop", { id: tool.id }, context)).rejects.toMatchObject({ code: QUEUED_WORK, counts: { queued: 1, inFlight: 1 } });
    expect(actors.status(tool.id).status).toBe("running");
    await expect(provider.invoke("stop", { id: tool.id, dropQueued: true }, context)).resolves.toMatchObject({ status: "stopped", stoppedQueue: expect.anything() });
    releaseTool();
    const command = await actors.create({ name: "command-stopped", instructions: "Supervise.", topics: [TOPIC] });
    const releaseCommand = await holdWorker(actors, root, command.id);
    actors.tell(command.id, "queued behind the held run");
    const notices: Array<[string, string]> = [];
    const deps = { state: { ensure: async () => {}, actors, agents } } as unknown as Parameters<typeof runFabricCommand>[1];
    const commandContext = { ui: { notify: (text: string, level: string) => { notices.push([text, level]); } } } as unknown as Parameters<typeof runFabricCommand>[3];
    const run = (text: string) => runFabricCommand({} as Parameters<typeof runFabricCommand>[0], deps, text, commandContext, async () => undefined,
      {} as Parameters<typeof runFabricCommand>[5]);
    await run(`stop ${command.id}`);
    expect(notices.at(-1)).toEqual([expect.stringMatching(/stop refused and nothing changed.*dropQueued/), "error"]);
    expect(actors.status(command.id).status).toBe("running");
    await run(`stop ${command.id} --drop-queued`);
    expect(notices.at(-1)).toEqual([expect.stringMatching(/^Stopped Fabric actor .*queued work archived to /), "info"]);
    expect(actors.status(command.id).status).toBe("stopped");
    releaseCommand();
  }, 30_000);
});

describe("resident removal forwarding (pi-fabric#1572 round 3)", () => {
  it("removeActor forwards dropQueued to the owning resident, and an owner without queuedWork support is refused before dispatch", async () => {
    installInProcessResidentFence();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-queued-work-remove-"));
    roots.push(root);
    const rootIdentity = { id: "session:queued-remove", name: "Main", kind: "main" as const, sessionId: "queued-remove" };
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
    const actor = await host.actors.create({ name: "resident-removed", instructions: "Supervise.", residency: "durable", topics: [TOPIC], coalesce: false });
    const release = await holdWorker(host.actors, root, actor.id);
    for (let index = 0; index < 2; index++) await publish(mesh, index);
    await waitFor(() => host.actors.status(actor.id).queued === 2);
    const { archive } = stopArchive(await client.setActor({ operation: "stop", id: actor.id, dropQueued: true }, undefined, caller));
    release();
    await waitFor(() => host.actors.inFlightCount() === 0);
    const bytes = fs.readFileSync(archive);
    // Without dropQueued the owner refuses (typed, over the wire); nothing changes.
    await expect(client.removeActor(actor.id)).rejects.toMatchObject({ code: QUEUED_WORK, operation: "remove", counts: { archived: 1 } });
    expect(host.actors.status(actor.id).status).toBe("stopped");
    // An owner that does not publish queuedWork support would ignore dropQueued: refused before dispatch.
    const ownerPath = path.join(config.residencyRoot, "owner.json");
    const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8")) as Record<string, unknown>;
    const { queuedWork, ...older } = owner;
    expect(queuedWork).toBe(1);
    fs.writeFileSync(ownerPath, JSON.stringify(older));
    await expect(client.removeActor(actor.id, undefined, { dropQueued: true })).rejects.toThrow(/predates dropQueued.*No request was dispatched/);
    expect(host.actors.status(actor.id).status).toBe("stopped");
    fs.writeFileSync(ownerPath, JSON.stringify(owner));
    // With dropQueued the removal goes through and keeps the archive under session-archive.
    await expect(client.removeActor(actor.id, undefined, { dropQueued: true })).resolves.toMatchObject({ removed: true });
    await waitFor(() => !fs.existsSync(path.join(root, "actors", actor.id)));
    const base = path.join(config.meshRoot, "session-archive", encodeURIComponent(rootIdentity.id));
    const kept = fs.readdirSync(base).filter(name => name.startsWith(`${actor.id}.recoverable-`));
    expect(kept).toHaveLength(1);
    expect(fs.readFileSync(path.join(base, kept[0]!, "stopped-queues", path.basename(archive))).equals(bytes)).toBe(true);
  }, 60_000);
});

// pi-fabric#1572 round 4: an explicit stop with a run in flight is not refused for the in-flight item. These reproduce, in a
// fleet-runnable suite, the explicit stops of the hosted-only CI failures on 6600e802 (agents-provider, residency,
// residency-stopped-writer-retention), which a fleet host does not collect (vitest.config.cts hostedOnlyDeletionSuites).
describe("queued-work guard, round 4: explicit stop of a busy actor (pi-fabric#1572)", () => {
  const providerFor = (fixture: ReturnType<typeof setup>) => {
    const participants = { scheduleRefresh() {}, list: () => [], get: () => undefined } as unknown as FabricParticipantSource;
    const main = { id: identity.id, local: true, matches: (id: string) => id === identity.id } as FabricMainAgentTarget;
    const lifecycle = new LifecycleBroker(fixture.mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(fixture.agents, fixture.actors, new GlobalActorRegistry(fixture.root, 64 * 1024), main, participants, undefined, lifecycle);
    const context: FabricInvocationContext = {
      cwd: process.cwd(), signal: undefined, parentToolCallId: "test-parent", nestedToolCallId: "test-nested",
      extensionContext: {} as FabricInvocationContext["extensionContext"], activity() {}, update() {},
    };
    return { provider, context };
  };

  it("the agents.stop tool and a routed control-plane stop over an ask or a callerless item in flight stop it; only the callerless item is archived", async () => {
    const fixture = setup();
    const { root, actors } = fixture;
    const { provider, context } = providerFor(fixture);
    // agents-provider "still stops a zero-progress local actor ASK on explicit stop" / "keeps explicit stop effective after Main's ceiling detaches a local ASK".
    const asked = await actors.create({ name: "tool-asked", instructions: "Advise.", responseMode: "text" });
    const answer = actors.ask(asked.id, "HANG").then(() => "answered", (error: unknown) => error);
    await waitFor(() => Boolean(actors.status(asked.id).inFlightRun));
    const stopped = await provider.invoke("stop", { id: asked.id }, context) as FabricActorInfo;
    expect(stopped).toMatchObject({ status: "stopped" });
    expect(stopped).not.toHaveProperty("stoppedQueue");
    expect(await answer).toBeInstanceOf(Error);
    expect(actors.messages(asked.id).filter(message => message.direction === "out" && message.text)).toEqual([]);
    // agents-provider "preserves accepted native/mesh owner work at the Main ceiling": the owner's side of a routed agents.stop.
    const command = (targetId: string) => ({ version: 1 as const, commandId: randomUUID(), targetId, operation: "stop" as const, replyTo: from.id, requestedAt: Date.now() });
    const remote = await actors.create({ name: "routed-asked", instructions: "Advise.", responseMode: "text" });
    const remoteAnswer = actors.ask(remote.id, "HANG").then(() => "answered", (error: unknown) => error);
    await waitFor(() => Boolean(actors.status(remote.id).inFlightRun));
    await expect(provider.acceptControl(command(remote.id), from)).resolves.toMatchObject({ accepted: true });
    expect(actors.status(remote.id).status).toBe("stopped");
    expect(await remoteAnswer).toBeInstanceOf(Error);
    expect(fs.existsSync(path.join(root, "actors", remote.id, "stopped-queues"))).toBe(false);
    const busy = await actors.create({ name: "routed-busy", instructions: "Supervise.", topics: [TOPIC] });
    const release = await holdWorker(actors, root, busy.id);
    await expect(provider.acceptControl(command(busy.id), from)).resolves.toMatchObject({ accepted: true });
    expect(actors.status(busy.id).status).toBe("stopped");
    // The acknowledgement carries no archive path: the MOVED-TO record in the actor's log names it.
    const moved = actors.messages(busy.id).find(message => message.text?.startsWith("MOVED-TO "));
    expect(moved?.data).toMatchObject({ stoppedQueue: { counts: { queued: 0, inFlight: 1 } } });
    release();
    // A queued item still refuses a routed stop without dropQueued.
    const queued = await actors.create({ name: "routed-queued", instructions: "Supervise.", topics: [TOPIC] });
    const releaseQueued = await holdWorker(actors, root, queued.id);
    actors.tell(queued.id, "queued behind the held run");
    await expect(provider.acceptControl(command(queued.id), from)).resolves.toMatchObject({ accepted: false, error: expect.stringMatching(/stop refused and nothing changed/) });
    expect(actors.status(queued.id).status).toBe("running");
    releaseQueued();
  }, 60_000);

  it("a resident host's control-plane stop over a followUp or an ask in flight stops it; a plain removeActor then works for the ask", async () => {
    installInProcessResidentFence();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-queued-work-r4-"));
    roots.push(root);
    const rootIdentity = { id: "session:queued-r4", name: "Main", kind: "main" as const, sessionId: "queued-r4" };
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
      name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"],
      sessionId: rootIdentity.sessionId, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
    }]);
    const host = new ResidentHost(config);
    const control = new FabricControlPlane(mesh, rootIdentity, { enabled: true, hostId: rootIdentity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    closers.push(async () => {
      for (const run of host.agents?.listForUi() ?? []) await host.agents.stop(run.id).catch(() => undefined);
      await control.close(); await host.close(); await participants.close();
    });
    await participants.start();
    await host.start();
    control.start(() => ({ accepted: false }));
    const hostId = residentHostId(rootIdentity.id);
    // residency-stopped-writer-retention "public actor stop retains its live writer...": a callerless followUp in flight.
    const followed = await host.actors.create({ name: "resident-followed", instructions: "Reply.", residency: "durable", responseMode: "text", delivery: "mailbox" });
    await control.request(hostId, followed.id, "followUp", { message: "HANG_WITH_PROGRESS", ownerIncarnation: host.control.incarnation }, hostId);
    await waitFor(() => Boolean(host.actors.status(followed.id).inFlightRun));
    expect(await control.request(hostId, followed.id, "stop", { ownerIncarnation: host.control.incarnation }, hostId)).toMatchObject({ acknowledged: true });
    expect(host.actors.status(followed.id).status).toBe("stopped");
    const moved = host.actors.messages(followed.id).find(message => message.text?.startsWith("MOVED-TO "));
    const archive = (moved?.data as { stoppedQueue?: { archive?: string } } | undefined)?.stoppedQueue?.archive;
    expect(archive).toBeDefined();
    expect(archiveLinesOf(archive!).map(line => line.kind)).toEqual(["header", "in-flight"]);
    expect(archiveLinesOf(archive!)[1]!.item).toMatchObject({ payload: { message: "HANG_WITH_PROGRESS" } });
    // residency "preserves a resident-host ASK activation at the Main ceiling": an ask in flight, then a plain removeActor.
    const asked = await host.actors.create({ name: "resident-asked", instructions: "Reply.", residency: "durable", responseMode: "text", delivery: "followUp", triggerTurn: false });
    const again = control.requestResult(hostId, asked.id, "ask", { message: "HANG", ownerIncarnation: host.control.incarnation }, hostId, { timeoutMs: 10_000 })
      .then(() => "answered", (error: unknown) => error);
    await waitFor(() => Boolean(host.actors.status(asked.id).inFlightRun));
    expect(await control.request(hostId, asked.id, "stop", { ownerIncarnation: host.control.incarnation }, hostId)).toMatchObject({ acknowledged: true });
    expect(await again).toBeInstanceOf(Error);
    await waitFor(() => !host.actors.status(asked.id).inFlightRun);
    expect(fs.existsSync(path.join(root, "actors", asked.id, "stopped-queues"))).toBe(false);
    await expect(new ResidentActorClient(config.meshRoot, config.rootId).removeActor(asked.id)).resolves.toMatchObject({ removed: true });
  }, 60_000);
});
const archiveLinesOf = (archive: string) => fs.readFileSync(archive, "utf8").split("\n").filter(Boolean)
  .map(line => JSON.parse(line) as { kind: string; item?: { source?: string; payload?: unknown } });

// pi-fabric#1572 round 6 (astra F2): judge the work left in each archive, not just the archive's existence.
describe("queued-work guard, round 6: requeued stop archives (pi-fabric#1572)", () => {
  const stoppedWithSuccessor = async (queuedAsk = false) => {
    const fixture = setup(10);
    const { root, actors } = fixture;
    const old = await actors.create({ name: "requeue-old", instructions: "Supervise." });
    const releaseOld = await holdWorker(actors, root, old.id);
    actors.tell(old.id, "queued work");
    const answer = queuedAsk ? actors.ask(old.id, "queued ask").catch((error: unknown) => error) : undefined;
    const { archive } = stopArchive(await actors.stop(old.id, undefined, false, "actors.stop", { dropQueued: true }));
    if (queuedAsk) expect(await answer).toBeInstanceOf(Error);
    releaseOld();
    await waitFor(() => actors.inFlightCount() === 0);
    const fresh = await actors.create({ name: "requeue-successor", instructions: "Supervise." });
    await holdWorker(actors, root, fresh.id);
    return { ...fixture, old, fresh, archive, ledger: archive.replace(/\.jsonl$/, ".requeued.jsonl") };
  };

  it("plain remove succeeds after every stop-archived item has been requeued", async () => {
    const { root, actors, old, fresh, archive } = await stoppedWithSuccessor();
    await expect(actors.requeueArchived(fresh.id, archive)).resolves.toMatchObject({ requeued: 2, skipped: [] });
    await expect(actors.remove(old.id)).resolves.toMatchObject({ removed: true });
    expect(fs.existsSync(path.join(root, "actors", old.id))).toBe(false);
    expect(actors.status(fresh.id).queued).toBe(2);
  }, 30_000);

  // Round 7 P3 (pi-fabric#1572): a queued caller's ask, which requeueArchived never feeds again, is recorded as skipped and no
  // longer blocks remove for good. A partly requeued archive (an item a full target refused) still blocks: see round 7 below.
  it("a skipped caller's ask is recorded as skipped and no longer blocks remove", async () => {
    const { root, actors, old, fresh, archive, ledger } = await stoppedWithSuccessor(true);
    const skipped = [expect.objectContaining({ reason: "a caller's ask, rejected when the actor stopped" })];
    await expect(actors.requeueArchived(fresh.id, archive)).resolves.toMatchObject({ requeued: 2, skipped, notQueued: [] });
    const outcomes = () => fs.readFileSync(ledger, "utf8").split("\n").filter(Boolean).map(line => (JSON.parse(line) as { outcome: string }).outcome);
    expect(outcomes()).toEqual(["requeued", "requeued", "skipped"]);
    // Again: the ask is still reported skipped, nothing is offered twice and the ledger gains no line.
    await expect(actors.requeueArchived(fresh.id, archive)).resolves.toMatchObject({ requeued: 0, alreadyRequeued: 2, skipped });
    expect(outcomes()).toEqual(["requeued", "requeued", "skipped"]);
    await expect(actors.remove(old.id)).resolves.toMatchObject({ removed: true });
    expect(fs.existsSync(path.join(root, "actors", old.id))).toBe(false);
    expect(actors.status(fresh.id).queued).toBe(2);
  }, 30_000);

  it.each(["corrupt", "invalid item id", "unreadable"])("a %s ledger still blocks remove even after all items were requeued", async (failure) => {
    const { actors, old, fresh, archive, ledger } = await stoppedWithSuccessor();
    await expect(actors.requeueArchived(fresh.id, archive)).resolves.toMatchObject({ requeued: 2, skipped: [] });
    if (failure === "unreadable") {
      fs.renameSync(ledger, `${ledger}.saved`);
      fs.mkdirSync(ledger); // A directory is unreadable as a ledger, without relying on user permissions.
    } else {
      fs.appendFileSync(ledger, failure === "corrupt" ? "{torn\n" : '{"itemId":42}\n');
    }
    const refusal = await actors.remove(old.id).catch((error: unknown) => error);
    expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "remove", counts: { archived: 1 } });
    expect((refusal as Error).message).toContain("requeue it first (agents.requeueArchived");
    expect(actors.status(old.id).status).toBe("stopped");
    expect(fs.existsSync(archive)).toBe(true);
  }, 30_000);
});

// pi-fabric#1572 round 7 (fabric-store F1, P1): a ledger line must prove its item was queued. Queue limit 1, so overflow 8.
describe("queued-work guard, round 7: the requeue ledger records only queued work (pi-fabric#1572)", () => {
  // An archive of 4 (the old actor's callerless item in flight plus 3 queued), and a busy successor holding `load` items.
  const archiveAndSuccessor = async (load: number, mesh = false) => {
    const fixture = setup(1);
    const { root, actors } = fixture;
    const old = await actors.create({ name: "requeue-old", instructions: "Supervise.", ...(mesh ? { topics: [TOPIC], coalesce: false } : {}) });
    const releaseOld = await holdWorker(actors, root, old.id);
    for (let index = 0; index < 3; index++) {
      if (mesh) await publish(fixture.mesh, index);
      else actors.tell(old.id, `archived ${index}`);
    }
    await waitFor(() => actors.status(old.id).queued === 3);
    const { archive, counts } = stopArchive(await actors.stop(old.id, undefined, false, "actors.stop", { dropQueued: true }));
    expect(counts).toMatchObject({ inFlight: 1, queued: 1, overflow: 2 });
    releaseOld();
    await waitFor(() => actors.inFlightCount() === 0);
    const fresh = await actors.create({ name: "requeue-successor", instructions: "Supervise." });
    const release = await holdWorker(actors, root, fresh.id);
    for (let index = 0; index < load; index++) actors.tell(fresh.id, `successor ${index}`);
    expect(actors.status(fresh.id).queued).toBe(load);
    return { ...fixture, old, fresh, archive, release, ledger: archive.replace(/\.jsonl$/, ".requeued.jsonl") };
  };
  const ledgerLines = (ledger: string) => fs.existsSync(ledger)
    ? fs.readFileSync(ledger, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as { itemId: string; outcome: string }) : [];

  it("F1 probe: a full successor takes none of 4; none is recorded, remove stays refused, and after it drains all 4 land once", async () => {
    const { actors, old, fresh, archive, ledger, release } = await archiveAndSuccessor(9);
    const first = await actors.requeueArchived(fresh.id, archive);
    expect(first).toMatchObject({ requeued: 0, filtered: 0, superseded: 0, alreadyRequeued: 0, skipped: [] });
    expect(first.notQueued?.map(item => item.reason)).toEqual([
      expect.stringMatching(/queue limit reached for requeue-successor \(1 and overflow 8\)/),
      ...Array(3).fill(expect.stringMatching(/^behind .+, which could not be queued$/)),
    ]);
    // Nothing was dropped or recorded: the successor holds its own 9, the ledger is empty, and the archive still blocks.
    expect(actors.status(fresh.id).queued).toBe(9);
    expect(actors.messages(fresh.id, 100).filter(message => message.error?.startsWith("Dropped a queued event"))).toEqual([]);
    expect(ledgerLines(ledger)).toEqual([]);
    const refusal = await actors.remove(old.id).catch((error: unknown) => error);
    expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "remove", counts: { archived: 1 } });
    expect((refusal as Error).message).toContain("requeue it first (agents.requeueArchived");
    expect(actors.status(old.id).status).toBe("stopped");
    // Once the successor drains, the same archive lands all 4, in order, exactly once, and the ledger is complete.
    release();
    await waitFor(() => actors.status(fresh.id).status === "idle" && actors.status(fresh.id).queued === 0, 150_000);
    await expect(actors.requeueArchived(fresh.id, archive)).resolves.toMatchObject({ requeued: 4, alreadyRequeued: 0, notQueued: [] });
    await expect(actors.requeueArchived(fresh.id, archive)).resolves.toMatchObject({ requeued: 0, alreadyRequeued: 4, notQueued: [] });
    expect(ledgerLines(ledger).map(line => line.itemId)).toEqual(first.notQueued?.map(item => item.itemId));
    expect(ledgerLines(ledger).map(line => line.outcome)).toEqual(Array(4).fill("requeued"));
    await waitFor(() => actors.status(fresh.id).status === "idle" && actors.status(fresh.id).queued === 0, 150_000);
    const fed = actors.messages(fresh.id, 100).filter(message => message.direction === "in" && message.source === "direct")
      .map(message => message.data as { message: string; data?: { fakeWorkerReleasePath?: string } })
      .filter(data => data.message.startsWith("archived ") || data.data?.fakeWorkerReleasePath?.includes(old.id.slice(0, 8)));
    expect(fed.map(data => data.message)).toEqual(["LIVE_WITH_PROGRESS", "archived 0", "archived 1", "archived 2"]);
    await expect(actors.remove(old.id)).resolves.toMatchObject({ removed: true });
  }, 360_000);

  // fabric-store P2: the queue checkpoint (and a routed event's dead-letter spill) is synced before any ledger line is written.
  it.each([
    ["queue checkpoint", 0, false],
    ["dead-letter file", 8, true],
  ] as const)("P2: the successor's %s is synced before the batch's ledger lines are written and synced", async (_target, load, mesh) => {
    const { root, actors, fresh, archive, ledger } = await archiveAndSuccessor(load, mesh);
    const directory = path.join(root, "actors", fresh.id);
    const realOpen = fs.openSync as (...args: unknown[]) => number;
    const realWrite = fs.writeSync as (...args: unknown[]) => number;
    const realSync = fs.fsyncSync;
    const opened = new Map<number, string>();
    const events: Array<{ op: "write" | "fsync"; file: string; queued?: string[] }> = [];
    const queuedOnDisk = () => (JSON.parse(fs.readFileSync(queueFile(root, fresh.id)!, "utf8")) as { items: Array<{ payload: { message?: string } }> })
      .items.map(item => item.payload.message ?? "mesh");
    vi.spyOn(fs, "openSync").mockImplementation(((...args: unknown[]) => {
      const fd = realOpen(...args);
      opened.set(fd, String(args[0]));
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, ...rest: unknown[]) => {
      const file = opened.get(fd) ?? "";
      events.push({ op: "write", file, ...(file === ledger ? { queued: queuedOnDisk() } : {}) });
      return realWrite(fd, ...rest);
    }) as typeof fs.writeSync);
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd: number) => {
      events.push({ op: "fsync", file: opened.get(fd) ?? "" });
      realSync(fd);
    });
    let result: actorTypes.FabricActorRequeueResult;
    try { result = await actors.requeueArchived(fresh.id, archive); } finally { vi.restoreAllMocks(); }
    expect(result.requeued).toBe(4);
    const synced = events.findIndex(({ op, file }) => op === "fsync" && path.dirname(file) === directory &&
      (mesh ? path.basename(file) === "dead-letter.jsonl" : path.basename(file).startsWith("queue-")));
    const written = events.findIndex(({ op, file }) => op === "write" && file === ledger);
    expect(synced).toBeGreaterThanOrEqual(0);
    expect(written).toBeGreaterThan(synced);
    expect(events.findIndex(({ op, file }, index) => op === "fsync" && file === ledger && index > written)).toBeGreaterThan(written);
    if (!mesh) {
      // The queue file the ledger is about to name already holds the 4 archived items.
      expect(events[written]!.queued).toEqual(["LIVE_WITH_PROGRESS", "LIVE_WITH_PROGRESS", "archived 0", "archived 1", "archived 2"]);
    } else {
      // Past the full queue and overflow, a durable host keeps the 3 routed events in the dead-letter file.
      expect(deadLetterLines(root, fresh.id)).toHaveLength(3);
    }
    expect(ledgerLines(ledger).map(line => line.outcome)).toEqual(Array(4).fill("requeued"));
    expect(result.notQueued).toEqual([]);
  }, 60_000);

  it("P3: a filtered line stays unresolved, so an archive the successor's filter skipped still blocks remove", async () => {
    const { root, mesh, actors } = setup(10);
    const old = await actors.create({ name: "filtered-old", instructions: "Supervise.", topics: [TOPIC], coalesce: false });
    const releaseOld = await holdWorker(actors, root, old.id);
    await publish(mesh, 0, true);
    await waitFor(() => actors.status(old.id).queued === 1);
    const { archive } = stopArchive(await actors.stop(old.id, undefined, false, "actors.stop", { dropQueued: true }));
    releaseOld();
    await waitFor(() => actors.inFlightCount() === 0);
    const fresh = await actors.create({ name: "filtered-successor", instructions: "Supervise.", topics: ["queued-work.other"],
      coalesce: false, activationFilter: WAIT_FILTER });
    await expect(actors.requeueArchived(fresh.id, archive)).resolves.toMatchObject({ requeued: 1, filtered: 1 });
    const refusal = await actors.remove(old.id).catch((error: unknown) => error);
    expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "remove", counts: { archived: 1 } });
    expect(actors.status(old.id).status).toBe("stopped");
    await expect(actors.remove(old.id, { dropQueued: true })).resolves.toMatchObject({ removed: true });
  }, 30_000);
});

// pi-fabric#1572 round 5 (fabric-store N4): a plain remove of a LIVE actor never drops its queue or its callerless item in
// flight. It is refused before any effect; with dropQueued all of it is archived first and travels with the removed tree.
describe("queued-work guard, round 5: remove of a live actor (pi-fabric#1572)", () => {
  const providerFor = (fixture: ReturnType<typeof setup>) => {
    const participants = { scheduleRefresh() {}, list: () => [], get: () => undefined } as unknown as FabricParticipantSource;
    const main = { id: identity.id, local: true, matches: (id: string) => id === identity.id } as FabricMainAgentTarget;
    const lifecycle = new LifecycleBroker(fixture.mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(fixture.agents, fixture.actors, new GlobalActorRegistry(fixture.root, 64 * 1024), main, participants, undefined, lifecycle);
    const context: FabricInvocationContext = {
      cwd: process.cwd(), signal: undefined, parentToolCallId: "test-parent", nestedToolCallId: "test-nested",
      extensionContext: {} as FabricInvocationContext["extensionContext"], activity() {}, update() {},
    };
    return { provider, context };
  };
  const recoverable = (root: string, actorId: string) => {
    const base = path.join(root, "mesh", "session-archive");
    return (fs.existsSync(base) ? fs.readdirSync(base) : []).flatMap(entry => fs.readdirSync(path.join(base, entry))
      .filter(name => name.startsWith(`${actorId}.recoverable-`)).map(name => path.join(base, entry, name)));
  };

  it("N4: remove with 2 queued and 1 in flight is refused with nothing changed (API, tool, /fabric remove); dropQueued archives all 3 and a same-name successor restores them", async () => {
    const fixture = setup(10);
    const { root, actors, agents } = fixture;
    const { provider, context } = providerFor(fixture);
    const old = await actors.create({ name: "live-removed", instructions: "Supervise.", topics: [TOPIC] });
    const releaseOld = await holdWorker(actors, root, old.id);
    actors.tell(old.id, "queued-one");
    actors.tell(old.id, "queued-two");
    await waitFor(() => actors.status(old.id).queued === 2);
    const runId = actors.status(old.id).inFlightRun!.id;
    const queueBefore = fs.readFileSync(queueFile(root, old.id)!);
    const unchanged = () => {
      expect(actors.status(old.id)).toMatchObject({ status: "running", queued: 2 });
      expect(actors.status(old.id).inFlightRun?.id).toBe(runId);
      expect(agents.status(runId)).toMatchObject({ status: "running" });
      expect(fs.readFileSync(queueFile(root, old.id)!).equals(queueBefore)).toBe(true);
      expect(fs.existsSync(path.join(root, "actors", old.id, "stopped-queues"))).toBe(false);
      expect(actors.messages(old.id).filter(message => message.text?.startsWith("MOVED-TO") || message.error?.startsWith("Dropped a queued event"))).toEqual([]);
      expect(actors.list().some(actor => actor.id === old.id)).toBe(true);
    };
    for (const attempt of [() => actors.remove(old.id), () => actors.remove(old.id, { wait: false, operator: true }),
      () => provider.invoke("remove", { id: old.id }, context)]) {
      const refusal = await attempt().then(() => undefined, (error: unknown) => error);
      expect(refusal).toBeInstanceOf(queuedWorkError() ?? class Missing {});
      expect(refusal).toMatchObject({ code: QUEUED_WORK, operation: "remove", actorId: old.id,
        counts: { archived: 0, heldDeadLetter: 0, queued: 2, parked: 0, overflow: 0, inFlight: 1, settled: 0 } });
      expect((refusal as Error).message).toMatch(/queued 2, parked 0, overflow 0, in-flight 1, settled 0\); remove refused and nothing changed.*dropQueued/);
      expect((refusal as Error).message).not.toContain("requeue it first");
      unchanged();
    }
    // /fabric remove answers the refusal like /fabric stop does.
    const notices: Array<[string, string]> = [];
    const deps = { state: { ensure: async () => {}, actors, agents } } as unknown as Parameters<typeof runFabricCommand>[1];
    const commandContext = { ui: { notify: (text: string, level: string) => { notices.push([text, level]); } } } as unknown as Parameters<typeof runFabricCommand>[3];
    await runFabricCommand({} as Parameters<typeof runFabricCommand>[0], deps, `remove ${old.id}`, commandContext, async () => undefined,
      {} as Parameters<typeof runFabricCommand>[5]);
    expect(notices.at(-1)).toEqual([expect.stringMatching(/remove refused and nothing changed/), "error"]);
    unchanged();
    // With dropQueued (the agents tool): all three items are archived before anything is emptied, then kept with the tree.
    const removing = provider.invoke("remove", { id: old.id, dropQueued: true }, context);
    releaseOld();
    await expect(removing).resolves.toMatchObject({ removed: true });
    await waitFor(() => !fs.existsSync(path.join(root, "actors", old.id)));
    const [kept] = recoverable(root, old.id);
    expect(kept).toBeDefined();
    const archives = fs.readdirSync(path.join(kept!, "stopped-queues")).filter(name => /^\d{8}T\d{9}Z-[0-9a-f]{12}\.jsonl$/.test(name));
    expect(archives).toHaveLength(1);
    const moved = path.join(kept!, "stopped-queues", archives[0]!);
    const lines = archiveLinesOf(moved);
    expect(lines.map(line => line.kind)).toEqual(["header", "in-flight", "queued", "queued"]);
    expect(lines.slice(1).map(line => (line.item?.payload as { message?: string } | undefined)?.message))
      .toEqual(["LIVE_WITH_PROGRESS", "queued-one", "queued-two"]);
    const records = fs.readFileSync(path.join(kept!, "stopped-queues", "moved-to.jsonl"), "utf8").split("\n").filter(Boolean)
      .map(line => JSON.parse(line) as { kind: string; sha256: string; counts: Record<string, number> });
    expect(records).toEqual([expect.objectContaining({ kind: "MOVED-TO", sha256: createHash("sha256").update(fs.readFileSync(moved)).digest("hex"),
      counts: expect.objectContaining({ queued: 2, inFlight: 1 }) })]);
    // The same-name successor restores all three, through the path the removal's stop wrote.
    const fresh = await provider.invoke("create", { name: "live-removed", instructions: "Supervise.", topics: ["queued-work.other"] }, context) as FabricActorInfo;
    expect(fresh.id).not.toBe(old.id);
    const releaseFresh = await holdWorker(actors, root, fresh.id);
    const original = path.join(root, "actors", old.id, "stopped-queues", archives[0]!);
    const fed = await provider.invoke("requeueArchived", { id: fresh.id, archive: original }, context) as actorTypes.FabricActorRequeueResult;
    expect(fed).toMatchObject({ id: fresh.id, archive: moved, requeued: 3, filtered: 0, superseded: 0, alreadyRequeued: 0, skipped: [] });
    expect(actors.status(fresh.id).queued).toBe(3);
    releaseFresh();
    // The refusal does not over-reach: only a caller's ask in flight (its caller hears the abort), or an idle actor, is removed as before.
    const asked = await actors.create({ name: "asked-removed", instructions: "Advise.", responseMode: "text" });
    const answer = actors.ask(asked.id, "HANG").then(() => "answered", (error: unknown) => error);
    await waitFor(() => Boolean(actors.status(asked.id).inFlightRun));
    await expect(actors.remove(asked.id)).resolves.toMatchObject({ removed: true });
    expect(await answer).toBeInstanceOf(Error);
    const idle = await actors.create({ name: "idle-removed", instructions: "Wait.", topics: [TOPIC] });
    await expect(actors.remove(idle.id)).resolves.toMatchObject({ removed: true });
  }, 120_000);

  it("N7: the live requeue check reads the stop record in full, not only the bounded in-memory message history", async () => {
    const { root, actors } = setup(10);
    const old = await actors.create({ name: "chatty-supervisor", instructions: "Supervise.", topics: [TOPIC] });
    const release = await holdWorker(actors, root, old.id);
    const { archive } = stopArchive(await actors.stop(old.id, undefined, false, "actors.stop", { explicit: true }));
    release();
    await waitFor(() => !actors.status(old.id).inFlightRun);
    // The in-memory history no longer holds the MOVED-TO message (cleared here; ACTOR_MESSAGE_HISTORY_LIMIT newer messages do the same).
    await actors.clearMessages(old.id);
    expect(actors.messages(old.id).some(message => message.text?.startsWith("MOVED-TO "))).toBe(false);
    const fresh = await actors.create({ name: "chatty-successor", instructions: "Supervise.", topics: ["queued-work.other"] });
    await holdWorker(actors, root, fresh.id);
    await expect(actors.requeueArchived(fresh.id, archive)).resolves.toMatchObject({ requeued: 1, skipped: [] });
    // An edited file no stop recorded is still refused: the stop record names the archive's exact path and sha256.
    const edited = fs.readFileSync(archive, "utf8").replace("LIVE_WITH_PROGRESS", "FORGED");
    const tampered = path.join(path.dirname(archive), `20261024T000000001Z-${createHash("sha256").update(edited).digest("hex").slice(0, 12)}.jsonl`);
    fs.writeFileSync(tampered, edited);
    await expect(actors.requeueArchived(fresh.id, tampered)).rejects.toThrow(/no stop record/);
  }, 60_000);
});
