import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(closers.splice(0).map((close) => close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };
const waitFor = async (predicate: () => boolean, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

// One test world: a mesh, agents whose runs for a task containing BLOCK wait for release(), and a
// persistent actor manager with a small queue limit that resumes from its cursor on restart.
const world = (actorQueueLimit: number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-queue-overflow-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  closers.push(() => agents.close());
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let unhold!: () => void;                                        // a second gate, for tasks containing HOLD
  const holdGate = new Promise<void>((resolve) => { unhold = resolve; });
  let held = 0;
  const tasks: Array<{ actor: string; task: string }> = [];
  const run = agents.run.bind(agents);
  vi.spyOn(agents, "run").mockImplementation(async (request, signal, ...callbacks) => {
    const wait = request.task.includes("BLOCK") ? gate : request.task.includes("HOLD") ? holdGate : undefined;
    if (wait) {                                                   // until release()/unhold(), or the run is aborted
      if (wait === holdGate) held++;
      await new Promise<void>((resolve) => {
        void wait.then(resolve);
        if (signal?.aborted) resolve();
        signal?.addEventListener("abort", () => resolve(), { once: true });
      });
    }
    // A run aborted while it waited to launch never ran.
    if (!signal?.aborted) tasks.push({ actor: request.actorName ?? "", task: request.task });
    return run(request, signal, ...callbacks);
  });
  const manager = (queueLimit = actorQueueLimit) => {
    const value = new ActorManager("test", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: queueLimit }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: true,
        meshCursorPath: path.join(root, "mesh-cursor.json"), meshReplayAgeMs: 10 * 60_000,
      });
    closers.unshift(() => value.close());
    return value;
  };
  const events = (actor: string) => tasks.filter((entry) => entry.actor === actor)
    .flatMap((entry) => entry.task.match(/ev-\d+/g) ?? []);
  // smarty-dev#816: past the queue and its overflow, an actor's routed events wait in its dead-letter file.
  const deadLetters = (actorId: string) => {
    const file = path.join(root, "actors", actorId, "dead-letter.jsonl");
    return fs.existsSync(file)
      ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map((line) => (JSON.parse(line) as { event: { text?: string } }).event.text ?? "")
      : [];
  };
  return { root, mesh, agents, manager, release: () => release(), unhold: () => unhold(), held: () => held, events, deadLetters };
};
const evs = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, index) => `ev-${from + index}`);

// smarty-dev#1065: after a restart a manager catches up from its cursor, and an event one actor's
// full queue rejected held the cursor for every actor of the manager: one slow supervisor with a
// full queue left all of dev-lead's reviewers 29 minutes behind the mesh.
describe("actor queue overflow", () => {
  it("keeps delivering to other actors while one actor's queue is full during catch-up", async () => {
    const w = world(2);
    const first = w.manager();
    const slow = await first.create({ name: "slow", instructions: "Supervise.", topics: ["team.events"], responseMode: "text", coalesce: false });
    const fast = await first.create({ name: "fast", instructions: "Review.", topics: ["team.events"], responseMode: "text", coalesce: false });
    await first.close();                                          // the cursor stays at the log's end
    await w.mesh.publish({ topic: "team.direct", to: slow.id, from, text: "BLOCK slow's long run" });
    for (let n = 1; n <= 8; n++) await w.mesh.publish({ topic: "team.events", from, text: `ev-${n}` });
    const second = w.manager();                                   // catches up: slow runs BLOCK, then fills up
    await waitFor(() => w.events("fast").length === 8, 20_000);   // every event reached the fast actor
    expect(w.events("fast")).toEqual(["ev-1", "ev-2", "ev-3", "ev-4", "ev-5", "ev-6", "ev-7", "ev-8"]);
    expect(second.status(slow.id).queued).toBe(8);                // 2 queued and 6 in its overflow
    expect(second.messages(slow.id).filter((message) => message.error?.startsWith("Dropped"))).toEqual([]);
    // The overflow survives a restart, in order.
    await second.close();
    const third = w.manager();
    await waitFor(() => third.status(slow.id).queued === 8, 10_000);
    w.release();
    await waitFor(() => w.events("slow").length === 8, 30_000);
    expect(w.events("slow")).toEqual(["ev-1", "ev-2", "ev-3", "ev-4", "ev-5", "ev-6", "ev-7", "ev-8"]);
    expect(w.events("fast").filter((event) => event === "ev-1")).toHaveLength(1);     // nothing ran twice
    void fast;
  }, 60_000);

  // review/astra F1 on #89: cancelling a queued ask frees a slot; the overflow must fill it at once,
  // so the waiting event runs without another arrival and nothing newer overtakes it.
  it.each([
    ["with no further event", false],
    ["ahead of a newer event", true],
  ] as const)("runs an overflow item after a cancelled ask frees its slot, %s", async (_case, newer) => {
    const w = world(1);
    const actors = w.manager();
    const slow = await actors.create({ name: "slow", instructions: "Supervise.", topics: ["team.events"], responseMode: "text", coalesce: false });
    await w.mesh.publish({ topic: "team.direct", to: slow.id, from, text: "BLOCK slow's long run" });
    // BLOCK pauses the injected run before a worker launches.
    await waitFor(() => actors.status(slow.id).status === "preparing");
    const controller = new AbortController();
    const cancelled = actors.ask(slow.id, "a caller's request", undefined, controller.signal).catch((error: Error) => error.message);
    await waitFor(() => actors.status(slow.id).queued === 1);
    await w.mesh.publish({ topic: "team.events", from, text: "ev-1" });            // into the overflow
    await waitFor(() => actors.status(slow.id).queued === 2);
    controller.abort();                                                            // frees the queue slot
    expect(await cancelled).toMatch(/cancelled/);
    if (newer) await w.mesh.publish({ topic: "team.events", from, text: "ev-2" });
    w.release();
    await waitFor(() => w.events("slow").length === (newer ? 2 : 1), 30_000);
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(w.events("slow")).toEqual(newer ? ["ev-1", "ev-2"] : ["ev-1"]);
  }, 60_000);

  // smarty-dev#816: past the queue and its overflow an event is dead-lettered, then processed in order.
  it("records, and never silently loses, an event past the queue and its overflow", async () => {
    const w = world(1);                                           // overflow cap: 8
    const actors = w.manager();
    const slow = await actors.create({ name: "slow", instructions: "Supervise.", topics: ["team.events"], responseMode: "text", coalesce: false });
    await w.mesh.publish({ topic: "team.direct", to: slow.id, from, text: "BLOCK slow's long run" });
    // BLOCK pauses the injected run before a worker launches.
    await waitFor(() => actors.status(slow.id).status === "preparing");
    for (let n = 1; n <= 12; n++) await w.mesh.publish({ topic: "team.events", from, text: `ev-${n}` });
    await waitFor(() => w.deadLetters(slow.id).length === 3, 10_000);
    expect(w.deadLetters(slow.id)).toEqual(evs(10, 12));
    expect(actors.status(slow.id).queued).toBe(9);                // 1 queued and 8 in its overflow
    expect(actors.messages(slow.id, 500).filter((message) => message.error?.startsWith("Dropped"))).toEqual([]);
    w.release();
    await waitFor(() => w.events("slow").length === 12 && w.deadLetters(slow.id).length === 0, 30_000);
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(w.events("slow")).toEqual(evs(1, 12));
    expect(actors.messages(slow.id, 500).filter((message) => message.error?.startsWith("Dropped"))).toEqual([]);
  }, 60_000);

  // PR #554 round 3 P1: restored work waits in #parked after a restart. Dead letters must not replay
  // ahead of it, and work past the cap on restore goes back to the dead-letter head, never dropped.
  it.each([
    ["the same queue limit", 2],
    ["a smaller queue limit, so restored work spills back to the dead-letter head", 1],
  ] as const)("restarts a full queue, full overflow and dead letters with %s: each event runs once, in order", async (_case, restartLimit) => {
    const w = world(2);                                           // queue 2, overflow cap 16
    const first = w.manager();
    const slow = await first.create({ name: "slow", instructions: "Supervise.", topics: ["team.events"], responseMode: "text", coalesce: false });
    await w.mesh.publish({ topic: "team.direct", to: slow.id, from, text: "BLOCK slow's long run" });
    await waitFor(() => first.status(slow.id).status === "preparing");
    for (let n = 1; n <= 22; n++) await w.mesh.publish({ topic: "team.events", from, text: `ev-${n}` });
    await waitFor(() => w.deadLetters(slow.id).length === 4, 10_000);
    expect(first.status(slow.id).queued).toBe(18);
    await first.close();
    expect(w.deadLetters(slow.id)).toEqual(evs(19, 22));
    const second = w.manager(restartLimit);
    await waitFor(() => second.status(slow.id).queued > 0, 10_000);
    await new Promise((resolve) => setTimeout(resolve, 200));
    // Nothing replayed ahead of the restored work; with the smaller limit its tail spilled back.
    expect(w.deadLetters(slow.id)).toEqual(restartLimit === 2 ? evs(19, 22) : evs(10, 22));
    w.release();
    await waitFor(() => w.events("slow").length >= 22 && w.deadLetters(slow.id).length === 0, 30_000);
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(w.events("slow")).toEqual(evs(1, 22));
    expect(second.messages(slow.id, 500).filter((message) => message.error?.startsWith("Dropped"))).toEqual([]);
  }, 60_000);

  // Round 3 P3: a crash before the cursor passed dead-lettered events offers them again on restart.
  it("does not run a dead-lettered event twice when the cursor replays it after a restart", async () => {
    const w = world(1);
    const first = w.manager();
    const slow = await first.create({ name: "slow", instructions: "Supervise.", topics: ["team.events"], responseMode: "text", coalesce: false });
    await w.mesh.publish({ topic: "team.direct", to: slow.id, from, text: "BLOCK slow's long run" });
    await waitFor(() => first.status(slow.id).status === "preparing");
    const cursorFile = path.join(w.root, "mesh-cursor.json");
    const cursor = fs.readFileSync(cursorFile, "utf8");           // before any ev-N
    for (let n = 1; n <= 12; n++) await w.mesh.publish({ topic: "team.events", from, text: `ev-${n}` });
    await waitFor(() => w.deadLetters(slow.id).length === 3, 10_000);
    await first.close();
    fs.writeFileSync(cursorFile, cursor);                         // the crash lost the cursor's advance
    const second = w.manager();
    w.release();
    await waitFor(() => w.events("slow").length >= 12 && w.deadLetters(slow.id).length === 0, 30_000);
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(w.events("slow")).toEqual(evs(1, 12));
    void second;
  }, 60_000);

  // Round 4 P2: replay rewrote the queue file before the drained item was in flight, so a close()
  // while that item waited to launch lost it. After a restart it must run, exactly once, in order.
  it("keeps the drained item that triggered a replay when the manager closes before it launches", async () => {
    const w = world(1);                                           // overflow cap: 8
    const first = w.manager();
    const slow = await first.create({ name: "slow", instructions: "Supervise.", topics: ["team.events"], responseMode: "text", coalesce: false });
    await w.mesh.publish({ topic: "team.direct", to: slow.id, from, text: "BLOCK slow's long run" });
    await waitFor(() => first.status(slow.id).status === "preparing");
    for (let n = 1; n <= 12; n++) await w.mesh.publish({ topic: "team.events", from, text: n === 9 ? "HOLD ev-9" : `ev-${n}` });
    await waitFor(() => w.deadLetters(slow.id).length === 3, 10_000);
    w.release();                                                  // ev-1..ev-8 run; draining ev-9 replays ev-10
    await waitFor(() => w.held() === 1 && w.deadLetters(slow.id).length === 2, 30_000);
    expect(w.events("slow")).toEqual(evs(1, 8));
    await first.close();                                          // ev-9 still waits to launch
    w.unhold();
    const second = w.manager();
    await waitFor(() => w.events("slow").length >= 11 && w.deadLetters(slow.id).length === 0, 30_000);
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(w.events("slow")).toEqual(evs(1, 12));
    void second;
  }, 60_000);
});
