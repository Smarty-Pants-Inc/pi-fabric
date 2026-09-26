import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(closers.splice(0).map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };

const setup = (root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-coalesce-key-"))) => {
  if (!roots.includes(root)) roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true,
  });
  closers.push(async () => { await actors.close(); await agents.close(); });
  return { root, mesh, agents, actors };
};
const pull = (mesh: MeshStore, number: number, revision: string, topic = "github.demo.pulls") =>
  mesh.publish({ topic, kind: "github.webhook", from, data: { event: "pull_request", payload: { action: "synchronize", number, revision } } });
const waitFor = async (predicate: () => boolean, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};
// The task of each finished activation, in run order (the actor keeps them under its own runs/).
const runTasks = (root: string, actors: ActorManager, actorId: string) =>
  actors.messages(actorId).flatMap((message) => {
    const runId = (message as { runId?: string }).runId;
    const file = runId ? path.join(root, "actors", actorId, "runs", runId, "task.txt") : undefined;
    return message.direction === "out" && file && fs.existsSync(file) ? [fs.readFileSync(file, "utf8")] : [];
  });

// smarty-dev#705: a review actor queued every pull_request event (12 at once), although its role
// reviews the current head only.
describe("actor coalesceKey for mesh events", () => {
  it("replaces a queued event with the same key value by the newer one, in its place", async () => {
    const { root, mesh, actors } = setup();
    const actor = await actors.create({
      name: "reviewer", instructions: "Review.", topics: ["github.demo.pulls", "github.other.pulls"],
      coalesce: false, coalesceKey: "payload.number",
    });
    expect(actor.coalesceKey).toBe("payload.number");
    await mesh.publish({ topic: "github.demo.pulls", from, text: "LIVE_WITH_PROGRESS" });   // keeps the actor busy
    await waitFor(() => actors.status(actor.id).status === "running");
    await pull(mesh, 1, "rev-a");
    await pull(mesh, 2, "rev-x");
    await pull(mesh, 1, "rev-b");
    await pull(mesh, 1, "rev-c");
    await pull(mesh, 1, "rev-other", "github.other.pulls");                                // another topic
    await waitFor(() => actors.status(actor.id).queued === 3);
    await waitFor(() => actors.messages(actor.id).filter((message) => message.direction === "out").length === 4);
    const tasks = runTasks(root, actors, actor.id).filter((task) => task.includes('"number":'));
    expect(tasks).toHaveLength(3);
    // In run order: PR 1 in the place of its first event, with the newest payload; then PR 2; then
    // the other topic's PR 1, which is not merged with this topic's.
    expect(tasks[0]).toContain("rev-c");
    expect(tasks[0]).not.toMatch(/rev-a|rev-b/);
    expect(tasks[1]).toContain("rev-x");
    expect(tasks[2]).toContain("rev-other");
  }, 30_000);

  // review/astra on #61: a ':'-joined key merged topic "work" with value "review:string:42" and
  // topic "work:string:review" with value "42", and lost the first event.
  it("never merges subjects of different topics or of different value types", async () => {
    const { root, mesh, actors } = setup();
    const topics = ["github.demo.pulls", "work", "work:string:review"];
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics, coalesce: false, coalesceKey: "payload.number" });
    await mesh.publish({ topic: "github.demo.pulls", from, text: "LIVE_WITH_PROGRESS" });
    await waitFor(() => actors.status(actor.id).status === "running");
    const event = (topic: string, number: string | number, revision: string) =>
      mesh.publish({ topic, from, data: { payload: { number, revision } } });
    await event("work", "review:string:42", "rev-joined-a");
    await event("work:string:review", "42", "rev-joined-b");
    await event("work", 42, "rev-number");                                             // number, not "42"
    await event("work", "42", "rev-string");
    await waitFor(() => actors.status(actor.id).queued === 4);
    await waitFor(() => actors.messages(actor.id).filter((message) => message.direction === "out").length === 5);
    const tasks = runTasks(root, actors, actor.id).filter((task) => task.includes("rev-"));
    expect(tasks).toHaveLength(4);
    expect(tasks[0]).toMatch(/"topic": "work",[\s\S]*rev-joined-a/);
    expect(tasks[1]).toMatch(/"topic": "work:string:review",[\s\S]*rev-joined-b/);
    expect(tasks[2]).toContain("rev-number");
    expect(tasks[3]).toContain("rev-string");
    // The inbox records each event under its own topic.
    const sources = actors.messages(actor.id).filter((message) => message.direction === "in").map((message) => message.source);
    expect(sources.slice(1)).toEqual(["mesh:work", "mesh:work:string:review", "mesh:work", "mesh:work"]);
  }, 30_000);

  it("queues every event without a key, and an event without a scalar value at the path", async () => {
    const { root, mesh, actors } = setup();
    const plain = await actors.create({ name: "plain", instructions: "Review.", topics: ["github.demo.pulls"], coalesce: false });
    const keyed = await actors.create({
      name: "keyed", instructions: "Review.", topics: ["github.demo.pulls"], coalesce: false, coalesceKey: "payload.number",
    });
    await mesh.publish({ topic: "github.demo.pulls", from, text: "LIVE_WITH_PROGRESS" });
    await waitFor(() => actors.status(plain.id).status === "running" && actors.status(keyed.id).status === "running");
    await pull(mesh, 1, "rev-a");
    await pull(mesh, 1, "rev-b");
    for (let copy = 0; copy < 2; copy++) {                                                    // not a scalar: never merged
      await mesh.publish({ topic: "github.demo.pulls", from, data: { payload: { number: { nested: true } } } });
    }
    await mesh.publish({ topic: "github.demo.pulls", from, text: "no data at all" });
    await waitFor(() => actors.status(plain.id).queued === 5 && actors.status(keyed.id).queued === 4);
    const done = (id: string, n: number) => actors.messages(id).filter((message) => message.direction === "out").length === n;
    await waitFor(() => done(plain.id, 6) && done(keyed.id, 5));
    expect(runTasks(root, actors, plain.id).filter((task) => task.includes("rev-"))).toHaveLength(2);
    expect(runTasks(root, actors, keyed.id).filter((task) => task.includes("rev-"))).toHaveLength(1);
  }, 30_000);

  // smarty-dev#1065: review actors still queued several events per PR. Three paths bypassed the key.
  it("merges work queued before the key was set, when it is set", async () => {
    const { root, mesh, actors } = setup();
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: ["github.demo.pulls"], coalesce: false });
    await mesh.publish({ topic: "github.demo.pulls", from, text: "LIVE_WITH_PROGRESS" });
    await waitFor(() => actors.status(actor.id).status === "running");
    await pull(mesh, 1, "rev-a");
    await pull(mesh, 2, "rev-x");
    await pull(mesh, 1, "rev-b");
    await waitFor(() => actors.status(actor.id).queued === 3);
    await actors.setCoalesceKey(actor.id, "payload.number");
    expect(actors.status(actor.id).queued).toBe(2);
    await waitFor(() => actors.messages(actor.id).filter((message) => message.direction === "out").length === 3);
    const tasks = runTasks(root, actors, actor.id).filter((task) => task.includes('"number":'));
    expect(tasks.map((task) => task.match(/rev-[a-z]+/)![0])).toEqual(["rev-b", "rev-x"]);   // PR 1 in its first place
  }, 30_000);

  it("merges an event that arrives while the same PR's work is parked", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-coalesce-key-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    let owned = true;
    const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: false, canManageActor: () => owned,
    });
    closers.push(async () => { await actors.close(); await agents.close(); });
    const actor = await actors.create({
      name: "reviewer", instructions: "Review.", topics: ["github.demo.pulls"], coalesce: false, coalesceKey: "payload.number",
    });
    actors.listOwned();
    await mesh.publish({ topic: "github.demo.pulls", from, text: "LIVE_WITH_PROGRESS first" });
    await waitFor(() => actors.status(actor.id).status === "running");
    await pull(mesh, 1, "rev-a");
    await waitFor(() => actors.status(actor.id).queued === 1);
    owned = false;
    actors.listOwned();                                           // PR 1's queued event parks
    expect(actors.status(actor.id).queued).toBe(0);
    owned = true;
    await pull(mesh, 1, "rev-b");                                 // dispatched before the parked work returns
    await pull(mesh, 2, "rev-x");
    await waitFor(() => actors.messages(actor.id).filter((message) => message.direction === "out" && message.runId).length >= 2, 20_000);
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    const ran = runTasks(root, actors, actor.id).filter((task) => task.includes("rev-"));
    expect(ran.filter((task) => /rev-[ab]/.test(task))).toHaveLength(1);               // PR 1 once, newest
    expect(ran.find((task) => /rev-[ab]/.test(task))).toContain("rev-b");
    expect(ran.filter((task) => task.includes("rev-x"))).toHaveLength(1);
  }, 30_000);

  it("merges a restored queue file from before the key", async () => {
    const first = setup();
    const actor = await first.actors.create({ name: "reviewer", instructions: "Review.", topics: ["github.demo.pulls"], coalesce: false });
    await first.mesh.publish({ topic: "github.demo.pulls", from, text: "LIVE_WITH_PROGRESS" });
    await waitFor(() => first.actors.status(actor.id).status === "running");
    await pull(first.mesh, 1, "rev-a");
    await pull(first.mesh, 2, "rev-x");
    await pull(first.mesh, 1, "rev-b");
    await waitFor(() => first.actors.status(actor.id).queued === 3);
    await first.actors.setCoalesceKey(actor.id, null);           // the registry keeps no key
    await first.actors.close();
    closers.length = 0;
    await first.agents.close();
    // The key arrives with the next start (an activation re-applied while it was down).
    const registry = path.join(first.root, "actors", "actors.json");
    const records = JSON.parse(fs.readFileSync(registry, "utf8")) as { actors?: Array<Record<string, unknown>> } | Array<Record<string, unknown>>;
    const list = Array.isArray(records) ? records : records.actors!;
    list.find((record) => record.id === actor.id)!.coalesceKey = "payload.number";
    fs.writeFileSync(registry, JSON.stringify(records));
    const second = setup(first.root);
    await waitFor(() => second.actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error).length >= 3, 20_000);
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    const ran = runTasks(first.root, second.actors, actor.id).filter((task) => task.includes('"number":'));
    expect(ran.filter((task) => /"number":\s*1\b/.test(task))).toHaveLength(1);
    expect(ran.find((task) => /"number":\s*1\b/.test(task))).toContain("rev-b");
    expect(ran.filter((task) => /"number":\s*2\b/.test(task))).toHaveLength(1);
  }, 40_000);

  it("is set and cleared on an existing actor, validated, and kept across a restart", async () => {
    const first = setup();
    const actor = await first.actors.create({ name: "reviewer", instructions: "Review.", topics: ["github.demo.pulls"] });
    expect(actor).not.toHaveProperty("coalesceKey");
    expect((await first.actors.setCoalesceKey(actor.id, "payload.number")).coalesceKey).toBe("payload.number");
    await expect(first.actors.setCoalesceKey(actor.id, "payload..number")).rejects.toThrow("Invalid actor coalesceKey");
    await expect(first.actors.create({ name: "bad", instructions: "x", coalesceKey: "a b" })).rejects.toThrow("Invalid actor coalesceKey");
    await first.actors.close();
    closers.length = 0;
    await first.agents.close();
    const second = setup(first.root);
    expect(second.actors.status(actor.id).coalesceKey).toBe("payload.number");
    expect(second.actors.status(actor.id)).not.toHaveProperty("coalesceKey", undefined);
    expect(await second.actors.setCoalesceKey(actor.id, null)).not.toHaveProperty("coalesceKey");
  }, 30_000);
});
