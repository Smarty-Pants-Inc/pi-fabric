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
  name, instructions: "Handle the owner alarm.", topics: [topic, "ops.other"], coalesce: false, coalesceKey: "key", residency: "durable",
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

describe("durable actor processed coalesceKey (smarty-dev#7710)", () => {
  it("wakes once for a producer retry with a different event id, but runs a different key", async () => {
    const { mesh, actors } = setup();
    const actor = await create(actors);
    const first = await alarm(mesh, "work:7742");
    await settled(actors, actor.id, 1);
    // The producer died after publish and before recording 'told', then published again.
    const retry = await alarm(mesh, "work:7742");
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
    expect(snapshot.processedKeys).toHaveLength(1);
    await first.close();
    const second = setup(first.root);
    await alarm(second.mesh, "already-told");
    await alarm(second.mesh, "new-work");
    await settled(second.actors, actor.id, 2);
    expect(outputs(second.actors, actor.id)).toHaveLength(2);
  });

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
    snapshot.processedKeys = Array.from({ length: 256 }, (_, index) => JSON.stringify(["mesh", "key", topic, `old-${index}`]));
    fs.writeFileSync(file, JSON.stringify(snapshot));
    const second = setup(first.root);
    await alarm(second.mesh, "old-0"); // ignored; must not move the oldest to the back
    await alarm(second.mesh, "newest");
    await settled(second.actors, actor.id, 2);
    let saved = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(saved.processedKeys).toHaveLength(256);
    expect(saved.processedKeys[0]).toBe(JSON.stringify(["mesh", "key", topic, "old-1"]));
    await alarm(second.mesh, "old-2"); // retained: does not run
    await alarm(second.mesh, "old-0"); // evicted: runs again
    await settled(second.actors, actor.id, 3);
    expect(outputs(second.actors, actor.id)).toHaveLength(3);
    saved = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(saved.processedKeys).toHaveLength(256);
    expect(saved.processedKeys.at(-1)).toBe(JSON.stringify(["mesh", "key", topic, "old-0"]));
  });

  it("keeps actor, topic, scalar type, and configured path boundaries distinct", async () => {
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
    await mesh.publish({ topic, kind: "stuck.work", from, data: { other: 42 } });
    await settled(actors, a.id, 4);
    expect(outputs(actors, a.id)).toHaveLength(4);
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

  it("drops a completed subject from dead-letter replay after restart", async () => {
    const first = setup();
    const actor = await create(first.actors);
    await alarm(first.mesh, "dead-lettered-repeat");
    await settled(first.actors, actor.id, 1);
    await first.close();
    const event = await alarm(first.mesh, "dead-lettered-repeat");
    const file = path.join(first.root, "actors", actor.id, "dead-letter.jsonl");
    fs.writeFileSync(file, JSON.stringify({ at: Date.now(), source: `mesh:${topic}`, event }) + "\n");
    const second = setup(first.root);
    await alarm(second.mesh, "new-after-dead-letter");
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
