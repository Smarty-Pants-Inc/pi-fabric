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
  const tasks: Array<{ actor: string; task: string }> = [];
  const run = agents.run.bind(agents);
  vi.spyOn(agents, "run").mockImplementation(async (request, signal) => {
    tasks.push({ actor: request.actorName ?? "", task: request.task });
    if (request.task.includes("BLOCK")) {                         // until release(), or the run is aborted
      await new Promise<void>((resolve) => {
        void gate.then(resolve);
        if (signal?.aborted) resolve();
        signal?.addEventListener("abort", () => resolve(), { once: true });
      });
    }
    return run(request, signal);
  });
  const manager = () => {
    const value = new ActorManager("test", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: true,
        meshCursorPath: path.join(root, "mesh-cursor.json"), meshReplayAgeMs: 10 * 60_000,
      });
    closers.unshift(() => value.close());
    return value;
  };
  const events = (actor: string) => tasks.filter((entry) => entry.actor === actor)
    .flatMap((entry) => entry.task.match(/ev-\d+/g) ?? []);
  return { root, mesh, agents, manager, release: () => release(), events };
};

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
    await waitFor(() => actors.status(slow.id).status === "running");
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

  it("records, and never silently loses, an event past the queue and its overflow", async () => {
    const w = world(1);                                           // overflow cap: 8
    const actors = w.manager();
    const slow = await actors.create({ name: "slow", instructions: "Supervise.", topics: ["team.events"], responseMode: "text", coalesce: false });
    await w.mesh.publish({ topic: "team.direct", to: slow.id, from, text: "BLOCK slow's long run" });
    await waitFor(() => actors.status(slow.id).status === "running");
    for (let n = 1; n <= 12; n++) await w.mesh.publish({ topic: "team.events", from, text: `ev-${n}` });
    await waitFor(() => actors.messages(slow.id).filter((message) => message.error?.startsWith("Dropped")).length === 3, 10_000);
    expect(actors.status(slow.id).queued).toBe(9);                // 1 queued and 8 in its overflow
    expect(actors.messages(slow.id).filter((message) => message.error?.startsWith("Dropped"))
      .every((message) => message.error!.includes("overflow"))).toBe(true);
    w.release();
    await waitFor(() => w.events("slow").length === 9, 30_000);
    expect(w.events("slow")).toEqual(["ev-1", "ev-2", "ev-3", "ev-4", "ev-5", "ev-6", "ev-7", "ev-8", "ev-9"]);
  }, 60_000);
});
