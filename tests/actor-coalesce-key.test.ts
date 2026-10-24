import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { normalizeScope, senderStamp } from "../src/scope.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(closers.splice(0).map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };

const setup = (root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-coalesce-key-")), canConsumeMesh?: () => boolean) => {
  if (!roots.includes(root)) roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true, ...(canConsumeMesh ? { canConsumeMesh } : {}),
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

// #6087 admission data safety: changing ingress subscriptions must not cancel
// accepted mesh work, replace its owner claim, or resurrect finished work on restart.
describe("actor setTopics", () => {
  it("unsubscribes new mesh work without cancelling its running item or queued backlog, then resubscribes", async () => {
    const { root, mesh, actors, agents } = setup();
    const topic = "topics.work";
    const actor = await actors.create({
      name: "topics-worker", instructions: "Work.", topics: [topic],
      responseMode: "text", coalesce: false, residency: "durable", dedupeKey: "data.key",
    });
    const release = path.join(root, "release-topics-worker");
    const queue = () => fs.readdirSync(path.join(root, "actors", actor.id))
      .filter(name => /^queue-.+\.json$/.test(name))
      .map(name => ({ name, ...JSON.parse(fs.readFileSync(path.join(root, "actors", actor.id, name), "utf8")) }));
    const claim = () => {
      const row = new ActorRegistryStore(path.join(root, "actors")).snapshot().actors.find(row => row.id === actor.id)!;
      return { rootId: row.rootId, residency: row.residency, adoptedAt: row.adoptedAt, adoptedFrom: row.adoptedFrom };
    };
    const completed = () => actors.messages(actor.id).filter(message => message.direction === "out" && !message.error);
    const publish = (key: string) => mesh.publish({ topic, from, text: key, data: { key } });
    try {
      const first = await mesh.publish({ topic, from, text: "topics-first", data: { key: "first", fakeWorkerFinishGate: release } });
      await waitFor(() => fs.existsSync(`${release}.entered`) && actors.status(actor.id).inFlightRun !== undefined);
      const inFlight = actors.status(actor.id).inFlightRun!;
      expect(agents.status(inFlight.id).status).toBe("running");
      const second = await publish("topics-backlog-one");
      const third = await publish("topics-backlog-two");
      await waitFor(() => actors.status(actor.id).queued === 2);
      const accepted = queue();
      expect(accepted).toHaveLength(1);
      expect(accepted[0].items.map((item: { payload: { id: string } }) => item.payload.id)).toEqual([first.id, second.id, third.id]);
      const ownership = claim();

      expect(await actors.setTopics(actor.id, [])).toMatchObject({ topics: [], status: "running", queued: 2 });
      expect(actors.status(actor.id).inFlightRun).toMatchObject({ id: inFlight.id, startedAt: inFlight.startedAt });
      expect(queue()).toEqual(accepted); // Same lineage file and item IDs, not a cancelled/recreated queue.
      expect(claim()).toEqual(ownership);
      const ignored = await publish("topics-ignored");
      // A later addressed event is a positive monitor barrier: it must pass the
      // ignored event before this receipt exists. No arbitrary negative sleep.
      const direct = await mesh.publish({ topic: "topics.direct", to: actor.id, from, text: "topics-direct", data: { key: "direct" } });
      await waitFor(() => queue().some(snapshot => snapshot.items.some((item: { payload: { id: string } }) => item.payload.id === direct.id)));
      expect(actors.status(actor.id)).toMatchObject({ status: "running", queued: 3, topics: [] });
      expect(queue()[0].items.map((item: { payload: { id: string } }) => item.payload.id)).toEqual([first.id, second.id, third.id, direct.id]);
      expect(JSON.stringify(queue())).not.toContain(ignored.id);
      expect(claim()).toEqual(ownership);
      expect(completed()).toEqual([]);

      fs.writeFileSync(release, "finish");
      await waitFor(() => completed().length === 4 && actors.status(actor.id).status === "idle");
      expect(actors.status(actor.id)).toMatchObject({ queued: 0, topics: [] });
      expect(actors.status(actor.id).inFlightRun).toBeUndefined();
      const tasks = runTasks(root, actors, actor.id);
      for (const text of ["topics-first", "topics-backlog-one", "topics-backlog-two", "topics-direct"]) {
        expect(tasks.filter(task => task.includes(text))).toHaveLength(1);
      }
      expect(tasks.some(task => task.includes("topics-ignored"))).toBe(false);
      expect(new Set(completed().map(message => message.runId)).size).toBe(4);
      expect(queue()[0].items).toEqual([]);
      expect(queue()[0].processedKeys).toHaveLength(4);

      expect((await actors.setTopics(actor.id, [topic, topic])).topics).toEqual([topic]);
      await publish("topics-restored");
      await waitFor(() => completed().length === 5 && actors.status(actor.id).status === "idle");
      expect(runTasks(root, actors, actor.id).filter(task => task.includes("topics-restored"))).toHaveLength(1);
      expect(claim()).toEqual(ownership);
      expect(queue()[0].items).toEqual([]);
      expect(queue()[0].processedKeys).toHaveLength(5);
    } finally {
      fs.writeFileSync(release, "finish");
    }
  }, 30_000);

  // #1209 review P2: reconnect/log replacement must retain the admission boundary,
  // not refresh subscriptions by moving the cursor or reconsidering accepted work.
  it("delivers admitted old and new topic events once across a cutover and mesh reconnect", async () => {
    let connected = true;
    const { root, mesh, actors } = setup(undefined, () => connected);
    const oldTopic = "topics.before-reconnect";
    const newTopic = "topics.after-reconnect";
    const actor = await actors.create({
      name: "topics-reconnect", instructions: "Work.", topics: [oldTopic], coalesce: false,
    });
    const release = path.join(root, "release-topics-reconnect");
    const queuedIds = () => fs.readdirSync(path.join(root, "actors", actor.id))
      .filter(name => /^queue-.+\.json$/.test(name))
      .flatMap(name => JSON.parse(fs.readFileSync(path.join(root, "actors", actor.id, name), "utf8"))
        .items.map((item: { payload: { id: string } }) => item.payload.id));
    const completed = () => actors.messages(actor.id).filter(message => message.direction === "out" && !message.error);
    const publish = (topic: string, text: string) => mesh.publish({ topic, from, text });
    try {
      const a = await mesh.publish({ topic: oldTopic, from, text: "reconnect-event-A", data: { fakeWorkerFinishGate: release } });
      await waitFor(() => fs.existsSync(`${release}.entered`) && actors.status(actor.id).inFlightRun !== undefined);
      const inFlight = actors.status(actor.id).inFlightRun!;
      expect(queuedIds()).toEqual([a.id]); // A matched and was durably admitted before the cutover.

      connected = false; // Hold monitor admission while the hub's local projection reconnects.
      expect((await actors.setTopics(actor.id, [newTopic])).topics).toEqual([newTopic]);
      const removedDuringGap = await publish(oldTopic, "reconnect-removed-during-gap");
      const b = await publish(newTopic, "reconnect-event-B");
      expect(queuedIds()).toEqual([a.id]); // B is published, but not yet consumed.

      // Same native-watch reconnect seam as actor-mesh-monitor.test.ts: atomically
      // replace the projected log and generation, without restarting the manager.
      const events = path.join(mesh.root, "events.jsonl");
      fs.writeFileSync(`${events}.tmp`, fs.readFileSync(events));
      fs.renameSync(`${events}.tmp`, events);
      const generation = path.join(mesh.root, "generation");
      fs.writeFileSync(`${generation}.tmp`, String(Number(fs.existsSync(generation) ? fs.readFileSync(generation, "utf8") : 0) + 1));
      connected = true;
      fs.renameSync(`${generation}.tmp`, generation); // A real watch edge wakes the existing monitor.
      await waitFor(() => actors.status(actor.id).queued === 1);
      expect(queuedIds()).toEqual([a.id, b.id]);
      expect(actors.status(actor.id).inFlightRun).toMatchObject({ id: inFlight.id, startedAt: inFlight.startedAt });
      expect(actors.status(actor.id).topics).toEqual([newTopic]);
      expect(new ActorRegistryStore(path.join(root, "actors")).snapshot().actors.find(row => row.id === actor.id)?.topics).toEqual([newTopic]);

      const removedAfterReconnect = await publish(oldTopic, "reconnect-removed-after-reconnect");
      const c = await publish(newTopic, "reconnect-new-topic-still-live");
      // Positive barrier: the monitor must pass both removed-topic events and B
      // before admitting this addressed event; no negative timing assertion.
      const barrier = await mesh.publish({ topic: "topics.reconnect-barrier", to: actor.id, from, text: "reconnect-direct-barrier" });
      await waitFor(() => queuedIds().includes(barrier.id));
      expect(queuedIds()).toEqual([a.id, b.id, c.id, barrier.id]);
      expect(queuedIds()).not.toContain(removedDuringGap.id);
      expect(queuedIds()).not.toContain(removedAfterReconnect.id);
      expect(completed()).toEqual([]);

      fs.writeFileSync(release, "finish");
      await waitFor(() => completed().length >= 4 && actors.status(actor.id).status === "idle");
      const tasks = runTasks(root, actors, actor.id);
      expect(tasks).toHaveLength(4);
      for (const text of ["reconnect-event-A", "reconnect-event-B", "reconnect-new-topic-still-live", "reconnect-direct-barrier"]) {
        expect(tasks.filter(task => task.includes(text))).toHaveLength(1);
      }
      expect(tasks.some(task => task.includes("reconnect-removed-"))).toBe(false);
      expect(new Set(completed().map(message => message.runId)).size).toBe(4);
      expect(actors.status(actor.id)).toMatchObject({ topics: [newTopic], queued: 0 });
      expect(queuedIds()).toEqual([]);
    } finally {
      connected = true;
      fs.writeFileSync(release, "finish");
    }
  }, 30_000);

  it.each([{ topics: [] }, { topics: ["topics.replacement"] }])("persists the replacement subscription $topics across restart without replaying completed work", async ({ topics }) => {
    const first = setup();
    const actor = await first.actors.create({ name: "topics-restart", instructions: "Work.", topics: ["topics.original"], coalesce: false });
    await first.mesh.publish({ topic: "topics.original", from, text: "topics-before-restart" });
    await waitFor(() => first.actors.status(actor.id).status === "idle" && first.actors.messages(actor.id).some(message => message.direction === "out" && !message.error));
    await first.actors.setTopics(actor.id, topics);
    await first.actors.close(); await first.agents.close(); closers.length = 0;
    const second = setup(first.root);
    expect(second.actors.status(actor.id).topics).toEqual(topics);
    await second.mesh.publish({ topic: "topics.original", from, text: "topics-obsolete-after-restart" });
    await second.mesh.publish({ topic: "topics.replacement", from, text: "topics-replacement-after-restart" });
    // Direct addressing still works for an actor with no subscriptions and
    // establishes that both preceding topic events have passed the monitor.
    await second.mesh.publish({ topic: "topics.barrier", to: actor.id, from, text: "topics-restart-barrier" });
    const expected = topics.length ? 3 : 2;
    await waitFor(() => second.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error).length >= expected && second.actors.status(actor.id).status === "idle");
    const tasks = runTasks(first.root, second.actors, actor.id);
    expect(tasks).toHaveLength(expected);
    expect(tasks.filter(task => task.includes("topics-before-restart"))).toHaveLength(1);
    expect(tasks.some(task => task.includes("topics-obsolete-after-restart"))).toBe(false);
    expect(tasks.filter(task => task.includes("topics-replacement-after-restart"))).toHaveLength(topics.length ? 1 : 0);
    expect(tasks.filter(task => task.includes("topics-restart-barrier"))).toHaveLength(1);
    expect(second.actors.status(actor.id)).toMatchObject({ topics, queued: 0 });
  }, 30_000);
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

  it("records targeted publishes only after inference and supersedes replaced event identities", async () => {
    const { root, mesh, actors } = setup();
    const actor = await actors.create({ name: "outcomes", instructions: "Review.", topics: [], coalesce: false, coalesceKey: "key" });
    actors.tell(actor.id, "LIVE_WITH_PROGRESS");
    await waitFor(() => actors.status(actor.id).status === "running");
    const publish = (text: string) => mesh.publish({ topic: "work", to: actor.id, from, text, data: { key: "same" } });
    const first = await publish("old"); await waitFor(() => actors.status(actor.id).queued === 1);
    const second = await publish("new");
    const directory = path.join(mesh.root, "delivery-outcomes");
    const outcomes = () => fs.existsSync(directory) ? fs.readdirSync(directory).flatMap(file =>
      fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").map(line => JSON.parse(line))) : [];
    await waitFor(() => outcomes().some(row => row.eventId === first.id));
    expect(outcomes().map(row => [row.eventId, row.outcome])).toEqual([[first.id, "superseded"]]);
    await waitFor(() => outcomes().some(row => row.eventId === second.id && row.outcome === "delivered"));
    expect(outcomes().map(row => [row.eventId, row.outcome])).toEqual([[first.id, "superseded"], [second.id, "delivered"]]);
    expect(outcomes()[1]).toMatchObject({ to: actor.id, from: from.id, mode: "publish" });
  }, 30_000);

  it("retains a failed final receipt append across restart without turning it back into runnable work", async () => {
    const first = setup();
    const actor = await first.actors.create({ name: "receipt-retry", instructions: "Review.", topics: [], coalesce: false });
    const send = { eventId: "append-retry", to: actor.id, from: from.id, mode: "followUp" as const };
    const open = fs.openSync.bind(fs);
    const blocked = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      if (String(file).includes("delivery-outcomes") && String(file).endsWith(".jsonl")) throw new Error("outcome volume unavailable");
      return open(file, flags, mode);
    });
    try {
      first.actors.tell(actor.id, "work", undefined, { outcomeSend: send });
      await waitFor(() => first.actors.status(actor.id).status === "idle" && first.actors.messages(actor.id).some(message => message.direction === "out"));
      const directory = path.join(first.root, "actors", actor.id);
      const file = fs.readdirSync(directory).find(name => /^queue-.+\.json$/.test(name))!;
      const saved = JSON.parse(fs.readFileSync(path.join(directory, file), "utf8"));
      expect(saved.items).toEqual([]);
      expect(saved.pendingDeliveryOutcomes).toEqual([{ ...send, outcome: "delivered", reason: "consumed by actor inference as itself", at: expect.any(Number) }]);
      await first.actors.close(); await first.agents.close(); closers.length = 0;
    } finally { blocked.mockRestore(); }
    const second = setup(first.root);
    const directory = path.join(second.mesh.root, "delivery-outcomes");
    const outcomes = () => fs.existsSync(directory) ? fs.readdirSync(directory).flatMap(file =>
      fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line))) : [];
    await waitFor(() => outcomes().some(row => row.eventId === send.eventId));
    expect(outcomes().filter(row => row.eventId === send.eventId)).toHaveLength(1);
    expect(second.actors.status(actor.id).queued).toBe(0);
    expect(second.actors.messages(actor.id).filter(message => message.direction === "out")).toHaveLength(1);
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

  // Trust boundary: retroactive coalescing must carry the winning payload's authority,
  // including after bootstrap. The reversed row also rejects an unconditional sender copy.
  it.each([
    { label: "older trusted then newer foreign", olderTrusted: true, reverse: false },
    { label: "older foreign then newer trusted", olderTrusted: false, reverse: false },
    { label: "newer foreign before an older trusted sequence", olderTrusted: true, reverse: true },
  ])("$label retains the winning sender through restore and retroactive coalescing", async ({ olderTrusted, reverse }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-coalesce-trust-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const runs: AgentRunRequest[] = [];
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const run = vi.spyOn(agents, "run").mockImplementation(async (request, signal, onSpawned) => {
      runs.push(request);
      const id = `coalesce-trust-${runs.length}`;
      onSpawned?.({ id, name: "recorded", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
      await new Promise<void>((resolve, reject) => {
        const abort = () => reject(new Error("held coalescing test run aborted"));
        signal?.addEventListener("abort", abort, { once: true });
        if (signal?.aborted) abort();
        void held.then(() => { signal?.removeEventListener("abort", abort); resolve(); });
      });
      return { id, name: "recorded", task: request.task, status: "completed", runner: "pi", transport: "process", cwd: process.cwd(),
        startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: true, text: "",
        value: { action: "silent" }, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } satisfies AgentRunResult;
    });
    let monitor!: ActorMeshMonitor;
    const start = vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
    const open = () => new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 1_000,
      meshRetentionSweepPath: false, canConsumeMesh: () => false,
    });
    let actors = open();
    closers.push(async () => {
      release();
      try { await actors.close(); await agents.close(); }
      finally { run.mockRestore(); start.mockRestore(); }
    });
    const actor = await actors.create({ name: "trust-coalescer", instructions: "Inspect.", topics: ["coalesce.trust"], coalesce: false, batchWindowMs: 0 });
    const dir = path.join(root, "actors", actor.id);
    const queuedMesh = () => fs.readdirSync(dir).filter(name => /^queue-.+\.json$/.test(name)).flatMap(name =>
      (JSON.parse(fs.readFileSync(path.join(dir, name), "utf8")) as {
        items: Array<{ source: string; payload: MeshEvent; sender?: ReturnType<typeof senderStamp> }>;
      }).items.filter(item => item.source === "mesh:coalesce.trust"));
    const trusted = senderStamp(undefined);
    const foreign = senderStamp(normalizeScope({ principal: { id: "foreign-owner" }, grants: [{ resource: "mesh:coalesce.trust", actions: ["write"] }] }));
    actors.tell(actor.id, "coalesce-blocker");
    await waitFor(() => runs.length === 1);
    const older = await mesh.publish({ topic: "coalesce.trust", from, sender: olderTrusted ? trusted : foreign, data: { key: "same", revision: "older" } });
    const newer = await mesh.publish({ topic: "coalesce.trust", from, sender: olderTrusted ? foreign : trusted, data: { key: "same", revision: "newer" } });
    expect(newer.sequence).toBeGreaterThan(older.sequence);
    const admitted = reverse ? [newer, older] : [older, newer];
    // Drive the real monitor admission callback, without a watcher/timing race.
    for (const event of admitted) expect(monitor.callbacks.onEvent(event)).toBe(true);
    expect(actors.status(actor.id).queued).toBe(2);
    expect(queuedMesh().map(item => [item.payload, item.sender])).toEqual(admitted.map(event => [event, event.sender]));

    await actors.close(); // Abort the blocker, leaving both accepted mesh items checkpointed.
    actors = open();
    await waitFor(() => runs.length === 2); // The restored blocker holds the queue again.
    expect(actors.status(actor.id).queued).toBe(2);
    expect(queuedMesh().map(item => [item.payload, item.sender])).toEqual(admitted.map(event => [event, event.sender]));
    await actors.setCoalesceKey(actor.id, "key"); // Actual #mergeCoalesced, not enqueue replacement.
    expect(actors.status(actor.id).queued).toBe(1);
    const selected = queuedMesh();
    expect(selected).toHaveLength(1);
    expect(selected[0]!.payload).toEqual(newer);
    release();
    await waitFor(() => runs.length === 3 && actors.status(actor.id).status === "idle");
    const task = runs[2]!.task;
    const header = task.split("\n\n").find(part => part.startsWith("Fabric actor message from "))!;
    const frame = JSON.parse(task.slice(task.lastIndexOf("\n\n") + 2)) as { source: string; payload: MeshEvent; untrusted?: true };
    expect(frame.source).toBe("mesh:coalesce.trust");
    expect(frame.payload).toEqual(newer);
    expect(frame.payload.sender).toEqual(newer.sender);
    if (olderTrusted) {
      expect(header).toContain("UNTRUSTED input from a different or narrower principal");
      expect(frame.untrusted).toBe(true);
    } else {
      expect(header).toBe("Fabric actor message from mesh:coalesce.trust:");
      expect(frame).not.toHaveProperty("untrusted");
    }
    expect(selected[0]!.sender).toEqual(newer.sender);
  });

  // A HANG run in flight is aborted by the ownership loss and retried first when ownership returns,
  // so PR 1's parked event cannot start, however the polls interleave (review/astra on #86: a
  // running event rightly gets a follow-up, which made run counts racy on Windows).
  it("merges an event that arrives while the same PR's work is parked", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-coalesce-key-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    let owned = true;
    const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, canManageActor: () => owned,
    });
    closers.push(async () => { await actors.close(); await agents.close(); });
    const actor = await actors.create({
      name: "reviewer", instructions: "Review.", topics: ["github.demo.pulls"], coalesce: false, coalesceKey: "payload.number",
    });
    actors.listOwned();
    await mesh.publish({ topic: "github.demo.pulls", from, text: "HANG first" });
    await waitFor(() => actors.status(actor.id).status === "running");
    await pull(mesh, 1, "rev-a");
    await waitFor(() => actors.status(actor.id).queued === 1);
    owned = false;
    actors.listOwned();                                           // rev-a parks; HANG is aborted and parked
    await waitFor(() => actors.status(actor.id).status !== "running");
    owned = true;
    await pull(mesh, 1, "rev-b");                                 // before or after the parked work returns
    await pull(mesh, 2, "rev-x");
    const queued = () => {
      const dir = path.join(root, "actors", actor.id);
      const file = fs.readdirSync(dir).find((name) => /^queue-.+\.json$/.test(name));
      const items = file ? (JSON.parse(fs.readFileSync(path.join(dir, file), "utf8")) as { items: Array<{ payload: { text?: string; data?: { payload?: { number?: number; revision?: string } } } }> }).items : [];
      return items.map((item) => item.payload.data?.payload ?? item.payload.text);
    };
    await waitFor(() => actors.status(actor.id).status === "running" && actors.status(actor.id).queued === 2, 20_000);
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(actors.status(actor.id).queued).toBe(2);
    expect(queued().slice(1)).toEqual([{ action: "synchronize", number: 1, revision: "rev-b" }, { action: "synchronize", number: 2, revision: "rev-x" }]);
  }, 40_000);

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
