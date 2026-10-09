import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { evaluateActorValidWhile } from "../src/actors/predicate.js";
import { ActorRecords, normalizeActorRecords } from "../src/actors/records.js";
import type { FabricActorValidityFacts } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [];
const managers: ActorManager[] = [];
const workers: AgentManager[] = [];
const gates: Array<() => void> = [];
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const predicate = { version: 1 as const, source: `({ activation, current }) => {
  if (activation.kind !== "mesh") return true;
  const state = current.records.get(activation.data.key)?.state;
  return !["held", "waited", "answered"].includes(state);
}` };
const facts: FabricActorValidityFacts = {
  activation: { kind: "direct", id: "a", source: "direct", sequence: 1, createdAt: 1 },
  current: { latestActivationSequence: 1, mainRevision: 2, taskRevision: 3, idle: true, now: 4 },
};
const event = (sequence: number, data: unknown, topic = "org.records"): MeshEvent => ({
  id: String(sequence), sequence, data, topic, kind: "record", from: identity, createdAt: 1_000 + sequence,
});
const waitFor = async (predicate: () => boolean) => {
  const end = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > end) throw new Error("Timed out waiting for records actor");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const completed = (request: AgentRunRequest): AgentRunResult => ({
  id: `run-${request.task}`, name: "test", task: request.task, status: "completed", runner: "pi", transport: "process",
  cwd: process.cwd(), startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, text: "done", inferenceStarted: true,
  usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
});
const setup = (persistent = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-records-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 10);
  const worker = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  workers.push(worker);
  const actorsRoot = path.join(root, "actors");
  const deliveries: string[] = [];
  const createManager = () => {
    const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 10 }, worker,
      ({ message }) => { if (message.text) deliveries.push(message.text); }, { actorRoot: actorsRoot, persistent, meshRetentionSweepPath: false });
    managers.push(actors);
    return actors;
  };
  const actors = createManager();
  const publish = (topic: string, data: unknown) => mesh.publish({ topic, kind: "test", from: identity, data });
  return { mesh, worker, actors, createManager, publish, deliveries, actorsRoot };
};
const blockFirstRun = (worker: AgentManager) => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  gates.push(release);
  const run = vi.spyOn(worker, "run").mockImplementation(async request => completed(request));
  run.mockImplementationOnce(async request => { await gate; return completed(request); });
  return { run, release };
};
afterEach(async () => {
  for (const release of gates.splice(0)) release();
  await Promise.all(managers.splice(0).map(m => m.close()));
  await Promise.all(workers.splice(0).map(m => m.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("actor records projection", () => {
  it("defaults to 512, caps at 4096, and validates options", () => {
    expect(normalizeActorRecords({ topic: "org.records" })).toEqual({ topic: "org.records", maxEntries: 512 });
    expect(normalizeActorRecords({ topic: "org.records", maxEntries: 10_000 })?.maxEntries).toBe(4096);
    for (const value of [null, [], { topic: 1 }, { topic: "bad topic" }, { topic: "org.records", maxEntries: 0 },
      { topic: "org.records", maxEntries: 1.5 }, { topic: "org.records", maxEntries: "1" }]) {
      expect(() => normalizeActorRecords(value)).toThrow(/records/);
    }
  });

  it("ignores malformed events and other topics, keeps last sequence, and evicts least-recently-written keys", () => {
    const records = new ActorRecords({ topic: "org.records", maxEntries: 2 });
    records.accept(event(1, { key: "", state: "open" }));
    expect(records.snapshot()[0]?.[0]).toBe("");
    records.accept(event(2, { key: "a", state: "open" }));
    records.accept(event(3, { key: "b", state: "held" }));
    records.accept(event(4, { key: "a", state: "answered" }));
    records.accept(event(2, { key: "a", state: "open" }));
    for (const data of [null, [], "text", {}, { key: 7, state: "held" }, { key: "a", state: 1 }, { key: "a", state: "WAIT" }]) {
      records.accept(event(10, data));
    }
    records.accept(event(5, { key: "c", state: "waited" }, "other.records"));
    records.accept(event(5, { key: "c", state: "waited" }));
    expect(records.snapshot()).toEqual([
      ["a", { state: "answered", at: new Date(1_004).toISOString() }],
      ["c", { state: "waited", at: new Date(1_005).toISOString() }],
    ]);
  });

  it("enforces default and maximum bounds and replays a bounded retained window", async () => {
    const records = new ActorRecords({ topic: "org.records" });
    const capped = new ActorRecords({ topic: "org.records", maxEntries: 50_000 });
    for (let i = 1; i <= 4_100; i++) { records.accept(event(i, { key: String(i), state: "open" })); capped.accept(event(i, { key: String(i), state: "open" })); }
    expect(records.snapshot()).toHaveLength(512);
    expect(capped.snapshot()).toHaveLength(4096);
    const { mesh, publish } = setup();
    for (let i = 0; i < 23; i++) await publish("org.records", { key: String(i), state: "answered" });
    const replayed = new ActorRecords({ topic: "org.records", maxEntries: 2 });
    replayed.replay(mesh);
    expect(replayed.snapshot().map(([key]) => key)).toEqual(["21", "22"]);
    const empty = new ActorRecords({ topic: "org.records" });
    empty.replay({ latestSequence: () => 5, maxReadEvents: 10 });
    expect(empty.snapshot()).toEqual([]);
  });

  it("exposes only a frozen get function and frozen state/ISO values without changing existing facts", async () => {
    const source = { version: 1 as const, source: `({ current }) => {
      const record = current.records.get("__proto__");
      return Object.isFrozen(current) && Object.isFrozen(current.records) && Object.isFrozen(current.records.get)
        && Object.isFrozen(record) && Object.keys(current.records).join() === "get"
        && record.state === "held" && record.at === "1970-01-01T00:00:01.000Z"
        && current.records.get("missing") === undefined && current.records.set === undefined
        && Reflect.set(record, "state", "open") === false && current.mainRevision === 2;
    }` };
    await expect(evaluateActorValidWhile(source, facts, [["__proto__", { state: "held", at: "1970-01-01T00:00:01.000Z" }]]))
      .resolves.toEqual({ valid: true });
  });
});

describe("records-backed actor validity", () => {
  it("does not commit a create receipt when retained-window replay fails", async () => {
    const { actors, mesh, publish } = setup();
    await publish("org.records", { key: "item", state: "answered" });
    const onCommit = vi.fn();
    const read = vi.spyOn(mesh, "read").mockImplementationOnce(() => { throw new Error("retained window unavailable"); });
    await expect(actors.create({ name: "org", instructions: "Work.", records: { topic: "org.records" } }, { onCommit }))
      .rejects.toThrow("retained window unavailable");
    expect(onCommit).not.toHaveBeenCalled();
    expect(actors.list()).toEqual([]);
    read.mockRestore();
  });
  it.each(["held", "waited", "answered"])("drops a queued item marked %s at start, without running its worker", async state => {
    const { actors, worker, publish, deliveries } = setup();
    const { run, release } = blockFirstRun(worker);
    const actor = await actors.create({ name: "org", instructions: "Work.", topics: ["org.work"], delivery: "steer", triggerTurn: false, coalesce: false,
      records: { topic: "org.records" }, validWhile: predicate });
    await publish("org.work", { key: "blocker" });
    await waitFor(() => run.mock.calls.length === 1);
    await publish("org.work", { key: "item" });
    await waitFor(() => actors.status(actor.id).queued === 1);
    await publish("org.records", { key: "item", state });
    // Queueing a later sentinel proves the preceding records event was observed while the first run is held.
    await publish("org.work", { key: "sentinel" });
    await waitFor(() => actors.status(actor.id).queued === 2);
    expect(run).toHaveBeenCalledTimes(1);
    release();
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(run).toHaveBeenCalledTimes(2); // blocker + unrelated sentinel, never item
    const stale = actors.messages(actor.id).filter(message => message.stale);
    expect(stale).toHaveLength(1);
    expect(stale[0]).toMatchObject({ action: "silent", reason: "validWhile returned false" });
    expect(stale[0]).not.toHaveProperty("runId");
    expect(deliveries).toEqual(["done", "done"]);
  });

  it.each(["unrelated", "malformed", "other-topic"])("runs queued work after an %s records event", async kind => {
    const { actors, worker, publish } = setup();
    const { run, release } = blockFirstRun(worker);
    const actor = await actors.create({ name: "org", instructions: "Work.", topics: ["org.work"], delivery: "steer", triggerTurn: false, coalesce: false,
      records: { topic: "org.records" }, validWhile: predicate });
    await publish("org.work", { key: "blocker" });
    await waitFor(() => run.mock.calls.length === 1);
    await publish("org.work", { key: "item" });
    await waitFor(() => actors.status(actor.id).queued === 1);
    await publish(kind === "other-topic" ? "private.records" : "org.records",
      kind === "malformed" ? { key: "item", state: 1 } : { key: kind === "unrelated" ? "other" : "item", state: "answered" });
    await publish("org.work", { key: "sentinel" });
    await waitFor(() => actors.status(actor.id).queued === 2);
    release();
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(run).toHaveBeenCalledTimes(3);
    expect(actors.messages(actor.id).some(message => message.stale)).toBe(false);
  });

  it("also invalidates completed work before delivery when its record changed during the run", async () => {
    const { actors, worker, publish, deliveries } = setup();
    const { run, release } = blockFirstRun(worker);
    const actor = await actors.create({ name: "org", instructions: "Work.", topics: ["org.work"], delivery: "steer", triggerTurn: false, coalesce: false,
      records: { topic: "org.records" }, validWhile: predicate });
    await publish("org.work", { key: "item" });
    await waitFor(() => run.mock.calls.length === 1);
    await publish("org.records", { key: "item", state: "answered" });
    await publish("org.work", { key: "sentinel" });
    await waitFor(() => actors.status(actor.id).queued === 1);
    release();
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(actors.messages(actor.id).find(message => message.stale)).toHaveProperty("runId");
    expect(deliveries).toEqual(["done"]);
  });

  it("leaves actors without the option unchanged and does not activate a model for records alone", async () => {
    const { actors, worker, publish } = setup();
    const run = vi.spyOn(worker, "run").mockImplementation(async request => completed(request));
    const actor = await actors.create({ name: "legacy", instructions: "Work.", topics: ["org.work"],
      validWhile: { version: 1, source: "({ current }) => !Object.hasOwn(current, 'records') && current.records === undefined" } });
    expect(actor).not.toHaveProperty("records");
    await publish("org.records", { key: "item", state: "answered" });
    await publish("org.work", { key: "item" });
    await waitFor(() => run.mock.calls.length === 1 && actors.status(actor.id).status === "idle");
    expect(actors.messages(actor.id).some(message => message.stale)).toBe(false);
  });

  it("rechecks current records before starting a trailing settled activation", async () => {
    const { actors, worker, publish, deliveries } = setup(true);
    const run = vi.spyOn(worker, "run").mockImplementation(async request => completed(request));
    const observed = vi.spyOn(ActorRecords.prototype, "accept");
    const actor = await actors.create({ name: "timed-org", instructions: "Work.", events: ["agent_settled"],
      activation: { minIntervalMs: 1_000 }, delivery: "steer", triggerTurn: false, coalesce: false,
      records: { topic: "org.records" },
      validWhile: { version: 1, source: '({ current }) => current.records.get("item")?.state !== "answered"' } });
    const settled = { event: "agent_settled", session: { id: "source-a" }, signal: { idle: true } };
    expect(actors.dispatchHostEvent("agent_settled", settled)).toBe(1);
    await waitFor(() => run.mock.calls.length === 1 && actors.status(actor.id).status === "idle");
    expect(actors.dispatchHostEvent("agent_settled", settled)).toBe(0);
    await publish("org.records", { key: "item", state: "answered" });
    await waitFor(() => observed.mock.calls.some(([event]) => event.topic === "org.records"));
    await waitFor(() => actors.messages(actor.id).some(message => message.stale));
    expect(run).toHaveBeenCalledTimes(1);
    const stale = actors.messages(actor.id).find(message => message.stale)!;
    expect(stale).toMatchObject({ source: "host:agent_settled", action: "silent", reason: "validWhile returned false" });
    expect(stale).not.toHaveProperty("runId");
    expect(deliveries).toEqual(["done"]);
  });

  it("persists only the actor option and rebuilds records from retained mesh events on restart", async () => {
    const { actors, createManager, worker, publish, actorsRoot } = setup(true);
    const run = vi.spyOn(worker, "run").mockImplementation(async request => completed(request));
    const actor = await actors.create({ name: "org", instructions: "Work.", topics: ["org.work"], delivery: "steer", triggerTurn: false,
      events: ["agent_settled"], activation: { minIntervalMs: 1_000 },
      records: { topic: "org.records", maxEntries: 2 }, validWhile: predicate });
    await publish("org.records", { key: "item", state: "answered" });
    await actors.close();
    const restored = createManager();
    expect(restored.status(actor.id)).toMatchObject({
      records: { topic: "org.records", maxEntries: 2 }, activation: { minIntervalMs: 1_000 }, events: ["agent_settled"],
    });
    await publish("org.work", { key: "item" });
    await waitFor(() => restored.messages(actor.id).some(message => message.stale));
    expect(run).not.toHaveBeenCalled();
    expect(fs.readdirSync(path.join(actorsRoot, actor.id))).not.toContain("records.json");
  });
});
