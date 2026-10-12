import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [];
const closers = new Set<() => Promise<void>>();
afterEach(async () => {
  await Promise.all([...closers].map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "session:processed-key", name: "main", kind: "main", sessionId: "processed-key" };
const from: MeshIdentity = { id: "session:factory", name: "factory", kind: "main", sessionId: "factory" };
const topic = "ops.owner";
const setup = (root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-processed-key-"))) => {
  if (!roots.includes(root)) roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("processed-key", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true,
  });
  const close = async () => { closers.delete(close); await actors.close(); await agents.close(); };
  closers.add(close);
  return { root, mesh, actors, close };
};
const create = (actors: ActorManager, name = "owner-alarm") => actors.create({
  name, instructions: "Handle the owner alarm.", topics: [topic, "ops.other"], coalesce: false, dedupeKey: "data.key", residency: "durable",
});
const alarm = (mesh: MeshStore, key: string | number, eventTopic = topic, text?: string) =>
  mesh.publish({ topic: eventTopic, kind: "stuck.work", from, data: { key }, ...(text ? { text } : {}) });
const outputs = (actors: ActorManager, id: string) => actors.messages(id).filter((message) => message.direction === "out" && message.source.startsWith("mesh:"));
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 15_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("actor did not settle");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};
const settled = async (actors: ActorManager, id: string, count: number) => {
  await waitFor(() => outputs(actors, id).length >= count && actors.status(id).status === "idle" && !actors.inFlightActorIds().includes(id));
};
const queueFile = (root: string, id: string) => {
  const directory = path.join(root, "actors", id);
  const file = fs.readdirSync(directory).find((name) => /^queue-.+\.json$/.test(name));
  expect(file).toBeDefined();
  return path.join(directory, file!);
};

describe("durable actor opt-in occurrence dedupeKey (smarty-dev#7710)", () => {
  it("runs a new head and security event for the same PR when only coalesceKey is configured", async () => {
    const first = setup();
    const { mesh, actors } = first;
    const actor = await actors.create({
      name: "pr-review", instructions: "Review each new head and security event.",
      topics: ["github.demo.pulls"], coalesceKey: "payload.number", residency: "durable",
    });
    const publish = (head: string, kind = "pull_request.synchronize") => mesh.publish({
      topic: "github.demo.pulls", kind, from, data: { payload: { number: 732, head } },
    });
    await publish("head-1");
    await settled(actors, actor.id, 1);
    await publish("head-2");
    await settled(actors, actor.id, 2);
    await publish("head-2", "security.review_requested");
    await settled(actors, actor.id, 3);
    expect(outputs(actors, actor.id)).toHaveLength(3);
    await first.close();
    const second = setup(first.root);
    await second.mesh.publish({
      topic: "github.demo.pulls", kind: "pull_request.synchronize", from,
      data: { payload: { number: 732, head: "head-3" } },
    });
    await settled(second.actors, actor.id, 4);
    expect(outputs(second.actors, actor.id)).toHaveLength(4);
  }, 30_000);

  it.each(["work:7742", 0])("wakes once for a producer retry with a different event id, but runs a different key (%j)", async (key) => {
    const { mesh, actors } = setup();
    const actor = await create(actors);
    const first = await alarm(mesh, key);
    await settled(actors, actor.id, 1);
    // The producer died after publish and before recording 'told', then published again.
    const retry = await alarm(mesh, key);
    expect(retry.id).not.toBe(first.id);
    await alarm(mesh, "work:7722");
    await settled(actors, actor.id, 2);
    expect(outputs(actors, actor.id)).toHaveLength(2);
    expect(actors.messages(actor.id).filter((message) => message.direction === "in")).toHaveLength(2);
  });

  it("drops a repeat queued while the original activation was still running", async () => {
    const { mesh, actors } = setup();
    const actor = await create(actors);
    await alarm(mesh, "slow-work", topic, "LIVE_WITH_PROGRESS");
    await waitFor(() => actors.status(actor.id).status === "running");
    await alarm(mesh, "slow-work");
    await alarm(mesh, "next-work");
    await settled(actors, actor.id, 2);
    expect(outputs(actors, actor.id)).toHaveLength(2);
  }, 30_000);

  it("keeps the completion window in an empty queue snapshot across restart", async () => {
    const first = setup();
    const actor = await create(first.actors);
    await alarm(first.mesh, "already-told");
    await settled(first.actors, actor.id, 1);
    const snapshot = JSON.parse(fs.readFileSync(queueFile(first.root, actor.id), "utf8"));
    expect(snapshot.items).toEqual([]);
    expect(snapshot.processedKeys).toEqual([JSON.stringify(["mesh-dedupe", "data.key", topic, "already-told"])]);
    await first.close();
    const second = setup(first.root);
    expect(second.actors.status(actor.id).dedupeKey).toBe("data.key");
    await alarm(second.mesh, "already-told");
    await alarm(second.mesh, "new-work");
    await settled(second.actors, actor.id, 2);
    expect(outputs(second.actors, actor.id)).toHaveLength(2);
  });

  it("preserves completed occurrences and pending settled delivery together across restart", async () => {
    const first = setup();
    const actor = await first.actors.create({
      name: "combined-alarm", instructions: "Observe alarms and settlements.",
      topics: [topic], events: ["agent_settled"], coalesce: false,
      dedupeKey: "data.key", activation: { minIntervalMs: 2_000 }, residency: "durable",
    });
    await alarm(first.mesh, "already-told");
    await settled(first.actors, actor.id, 1);
    first.actors.pauseForRelease();
    const hostPayload = (marker: string) => ({
      event: "agent_settled", session: { id: "settled-source", cwd: process.cwd() },
      digest: {}, transcript: [], signal: { idle: true, observedAt: Date.now() }, marker,
    });
    expect(first.actors.dispatchHostEvent("agent_settled", hostPayload("leading"))).toBe(1);
    expect(first.actors.dispatchHostEvent("agent_settled", hostPayload("latest"))).toBe(0);
    const file = queueFile(first.root, actor.id);
    const snapshot = JSON.parse(fs.readFileSync(file, "utf8"));
    const completedKey = JSON.stringify(["mesh-dedupe", "data.key", topic, "already-told"]);
    expect(snapshot.processedKeys).toEqual([completedKey]);
    expect(snapshot.settledWindows).toHaveLength(1);
    expect(snapshot.settledWindows[0].pending.payload.marker).toBe("latest");
    await first.close();
    const second = setup(first.root);
    expect(second.actors.status(actor.id)).toMatchObject({ dedupeKey: "data.key", activation: { minIntervalMs: 2_000 } });
    second.actors.resumeQueued();
    await alarm(second.mesh, "already-told");
    await alarm(second.mesh, "new-work");
    await settled(second.actors, actor.id, 2);
    const hostOutputs = () => second.actors.messages(actor.id).filter(message =>
      message.direction === "out" && message.source === "host:agent_settled");
    await waitFor(() => hostOutputs().length === 2 && second.actors.status(actor.id).status === "idle");
    expect(outputs(second.actors, actor.id)).toHaveLength(2);
    expect(second.actors.messages(actor.id).filter(message =>
      message.direction === "in" && message.source === "host:agent_settled")
      .map(message => (message.data as { marker: string }).marker)).toEqual(["leading", "latest"]);
    const restored = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(restored.processedKeys).toContain(completedKey);
    expect(restored.settledWindows ?? []).toEqual([]);
  }, 30_000);

  it("evicts the oldest of the last 256 processed keys, without refreshing a duplicate", async () => {
    const first = setup();
    const actor = await create(first.actors);
    await alarm(first.mesh, "seed");
    await settled(first.actors, actor.id, 1);
    const file = queueFile(first.root, actor.id);
    await first.close();
    // Populate the intentional persisted queue contract at its retention boundary, avoiding
    // 256 unrelated model launches. New completions must update this state through the manager.
    const snapshot = JSON.parse(fs.readFileSync(file, "utf8"));
    snapshot.processedKeys = Array.from({ length: 256 }, (_, index) => JSON.stringify(["mesh-dedupe", "data.key", topic, `old-${index}`]));
    fs.writeFileSync(file, JSON.stringify(snapshot));
    const second = setup(first.root);
    await alarm(second.mesh, "old-0"); // ignored; must not move the oldest to the back
    await alarm(second.mesh, "newest");
    await settled(second.actors, actor.id, 2);
    let saved = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(saved.processedKeys).toHaveLength(256);
    expect(saved.processedKeys[0]).toBe(JSON.stringify(["mesh-dedupe", "data.key", topic, "old-1"]));
    await alarm(second.mesh, "old-2"); // retained: does not run
    await alarm(second.mesh, "old-0"); // evicted: runs again
    await settled(second.actors, actor.id, 3);
    expect(outputs(second.actors, actor.id)).toHaveLength(3);
    saved = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(saved.processedKeys).toHaveLength(256);
    expect(saved.processedKeys.at(-1)).toBe(JSON.stringify(["mesh-dedupe", "data.key", topic, "old-0"]));
  });

  it("keeps actor, topic and scalar type boundaries distinct, independent of coalesce changes", async () => {
    const { mesh, actors } = setup();
    const a = await create(actors, "owner-a");
    await alarm(mesh, 42);
    await settled(actors, a.id, 1);
    const b = await create(actors, "owner-b");
    await alarm(mesh, 42);
    await settled(actors, b.id, 1);
    expect(outputs(actors, a.id)).toHaveLength(1);
    await alarm(mesh, "42");
    await alarm(mesh, 42, "ops.other");
    await settled(actors, a.id, 3);
    await settled(actors, b.id, 3);
    await actors.setCoalesceKey(a.id, "other");
    await mesh.publish({ topic, kind: "stuck.work", from, data: { key: 42, other: 42 } });
    await mesh.publish({ topic, kind: "stuck.work", from, data: { key: "new-occurrence", other: 42 } });
    await settled(actors, a.id, 4);
    expect(outputs(actors, a.id)).toHaveLength(4);
  });

  it.each([
    { label: "empty-string", key: "" },
    { label: "whitespace-only", key: " \t\r\n" },
  ])("runs both $label events and records no occurrence key", async ({ key }) => {
    const { root, mesh, actors } = setup();
    const actor = await create(actors);
    // Keep an intentional completion fence so the persisted queue remains observable.
    await alarm(mesh, "real-occurrence");
    await settled(actors, actor.id, 1);
    const file = queueFile(root, actor.id);
    const expectedKeys = [JSON.stringify(["mesh-dedupe", "data.key", topic, "real-occurrence"])];
    await alarm(mesh, key);
    await settled(actors, actor.id, 2);
    expect(JSON.parse(fs.readFileSync(file, "utf8")).processedKeys).toEqual(expectedKeys);
    await alarm(mesh, key);
    await settled(actors, actor.id, 3);
    expect(outputs(actors, actor.id)).toHaveLength(3);
    expect(outputs(actors, actor.id).every((message) => !message.error)).toBe(true);
    expect(JSON.parse(fs.readFileSync(file, "utf8")).processedKeys).toEqual(expectedKeys);
  });

  it("runs each event when the opted-in occurrence field is missing or non-scalar", async () => {
    const { mesh, actors } = setup();
    const actor = await create(actors);
    const keys = [undefined, null, true, false, { value: "same" }, ["same"]].flatMap((key) => [key, key]);
    for (const key of keys) {
      await mesh.publish({ topic, kind: "stuck.work", from, data: { key } });
    }
    await settled(actors, actor.id, keys.length);
    expect(outputs(actors, actor.id)).toHaveLength(keys.length);
  });

  it("does not interpret legacy resource completion fences as occurrence keys", async () => {
    const first = setup();
    const actor = await create(first.actors);
    await alarm(first.mesh, "seed");
    await settled(first.actors, actor.id, 1);
    const file = queueFile(first.root, actor.id);
    await first.close();
    const snapshot = JSON.parse(fs.readFileSync(file, "utf8"));
    snapshot.processedKeys = [JSON.stringify(["mesh", "data.key", topic, "legacy-resource"])];
    fs.writeFileSync(file, JSON.stringify(snapshot));
    const second = setup(first.root);
    await alarm(second.mesh, "legacy-resource");
    await settled(second.actors, actor.id, 2);
    expect(outputs(second.actors, actor.id)).toHaveLength(2);
  });

  it("reads legacy queue snapshots without completion history", async () => {
    const first = setup();
    const actor = await create(first.actors);
    await alarm(first.mesh, "legacy");
    await settled(first.actors, actor.id, 1);
    const file = queueFile(first.root, actor.id);
    await first.close();
    const snapshot = JSON.parse(fs.readFileSync(file, "utf8"));
    delete snapshot.processedKeys;
    fs.writeFileSync(file, JSON.stringify(snapshot));
    const second = setup(first.root);
    await alarm(second.mesh, "legacy");
    await settled(second.actors, actor.id, 2);
    expect(outputs(second.actors, actor.id)).toHaveLength(2);
  });

  it("drops already processed subjects restored from a predecessor-style pending snapshot", async () => {
    const first = setup();
    const actor = await create(first.actors);
    await alarm(first.mesh, "restored", topic, "LIVE_WITH_PROGRESS");
    await waitFor(() => first.actors.status(actor.id).status === "running");
    const file = queueFile(first.root, actor.id);
    const pending = JSON.parse(fs.readFileSync(file, "utf8")).items;
    expect(pending).toHaveLength(1);
    await settled(first.actors, actor.id, 1);
    await first.close();
    // An older lineage can still hand over the event that this lineage completed.
    const snapshot = JSON.parse(fs.readFileSync(file, "utf8"));
    snapshot.items = pending;
    fs.writeFileSync(file, JSON.stringify(snapshot));
    const second = setup(first.root);
    await alarm(second.mesh, "new-after-restoration");
    await settled(second.actors, actor.id, 2);
    expect(outputs(second.actors, actor.id)).toHaveLength(2);
  }, 30_000);

  it("dedupes a completed occurrence from dead-letter replay but runs a new occurrence after restart", async () => {
    const first = setup();
    const actor = await create(first.actors);
    const completed = await alarm(first.mesh, "dead-lettered-repeat");
    await settled(first.actors, actor.id, 1);
    await first.close();
    const file = path.join(first.root, "actors", actor.id, "dead-letter.jsonl");
    // Exercise the persisted replay interface only, not live mesh ingress: one producer
    // retry has a new ID but the completed key, the other has a new occurrence key.
    const retry = { ...completed, id: "dead-letter-retry" };
    const fresh = { ...completed, id: "dead-letter-fresh", data: { key: "new-dead-letter-occurrence" } };
    fs.writeFileSync(file, [retry, fresh].map(event => JSON.stringify({ at: Date.now(), source: `mesh:${topic}`, event }) + "\n").join(""));
    const second = setup(first.root);
    await settled(second.actors, actor.id, 2);
    expect(outputs(second.actors, actor.id)).toHaveLength(2);
    expect(fs.existsSync(file)).toBe(false);
  });

  it("does not mark failed activations processed", async () => {
    const { mesh, actors } = setup();
    const actor = await create(actors);
    await alarm(mesh, "retry-after-failure", topic, "FAIL_DIRECTIVE");
    await settled(actors, actor.id, 1);
    expect(outputs(actors, actor.id)[0]!.error).toBeTruthy();
    await alarm(mesh, "retry-after-failure");
    await settled(actors, actor.id, 2);
    expect(outputs(actors, actor.id)[1]!.error).toBeUndefined();
  });

  it("leaves session actors' queued-only coalescing unchanged", async () => {
    const { mesh, actors } = setup();
    const actor = await actors.create({ name: "session-review", instructions: "Review.", topics: [topic], coalesceKey: "key" });
    await alarm(mesh, "review-again");
    await settled(actors, actor.id, 1);
    await alarm(mesh, "review-again");
    await settled(actors, actor.id, 2);
    expect(outputs(actors, actor.id)).toHaveLength(2);
  });
});
