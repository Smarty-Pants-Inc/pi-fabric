import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";

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

const setup = (queueLimit = 1, root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-dead-letter-coalesce-"))) => {
  if (!roots.includes(root)) roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const run = agents.run.bind(agents);
  vi.spyOn(agents, "run").mockImplementation(async (request, signal, ...callbacks) => {
    if (request.task.includes("BLOCK")) {
      await new Promise<void>((resolve) => {
        void gate.then(resolve);
        if (signal?.aborted) resolve();
        signal?.addEventListener("abort", () => resolve(), { once: true });
      });
    }
    return run(request, signal, ...callbacks);
  });
  const manager = () => {
    const actors = new ActorManager("test", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: queueLimit }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: true,
        meshCursorPath: path.join(root, "mesh-cursor.json"), meshReplayAgeMs: 10 * 60_000,
      });
    closers.unshift(() => actors.close());
    return actors;
  };
  closers.push(() => agents.close());
  const deadLetters = (actorId: string) => {
    const file = path.join(root, "actors", actorId, "dead-letter.jsonl");
    return fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean)
      .map((line) => JSON.parse(line) as { event: { id: string; sequence: number; topic: string; data?: { payload?: { number?: number; revision?: string } } } }) : [];
  };
  const runTasks = (actors: ActorManager, actorId: string) => actors.messages(actorId).flatMap((message) => {
    const runId = (message as { runId?: string }).runId;
    const file = runId ? path.join(root, "actors", actorId, "runs", runId, "task.txt") : undefined;
    return message.direction === "out" && file && fs.existsSync(file) ? [fs.readFileSync(file, "utf8")] : [];
  });
  return { root, mesh, agents, manager, deadLetters, runTasks, release: () => release() };
};

// The worker gate holds the real actor's in-flight activation while mesh events pass through
// ActorManager's ordinary ingress path. Distinct filler keys occupy the queue and overflow; the
// repeated subject then reaches the persistent dead-letter log.
const fillBeforeDeadLetter = async (world: ReturnType<typeof setup>, actors: ActorManager, actorId: string, count = 9) => {
  await world.mesh.publish({ topic: "team.events", to: actorId, from, text: "BLOCK" });
  await waitFor(() => actors.status(actorId).status === "preparing");
  for (let number = 1; number <= count; number++) {
    await world.mesh.publish({ topic: "team.events", to: actorId, from,
      data: { payload: { number, revision: `filler-${number}` } } });
  }
  await waitFor(() => actors.status(actorId).queued === count);
};
const revision = (number: number, value: string) => ({ payload: { number, revision: value } });
const event = (world: ReturnType<typeof setup>, actorId: string, number: number, value: string) =>
  world.mesh.publish({ topic: "team.events", to: actorId, from, data: revision(number, value) });
const targetTasks = (world: ReturnType<typeof setup>, actors: ActorManager, actorId: string, number: number) =>
  world.runTasks(actors, actorId).filter((task) => task.includes(`"number": ${number}`));
const frame = (task: string) => JSON.parse(task.slice(task.lastIndexOf("\n\n") + 2)) as {
  payload: { data: { payload: { revision: string } } }; coalesced: Array<{ id: string; sequence: number; payload: MeshEvent }>;
};

describe("actor dead-letter coalescing", () => {
  it("coalesces three keyed dead-letter admissions to the newest mesh event", async () => {
    const w = setup();
    const actors = w.manager();
    const actor = await actors.create({ name: "reviewer", instructions: "Review.", topics: [], coalesce: false, coalesceKey: "payload.number" });
    await fillBeforeDeadLetter(w, actors, actor.id);
    const admitted = [await event(w, actor.id, 77, "rev-a"), await event(w, actor.id, 77, "rev-b"), await event(w, actor.id, 77, "rev-c")];
    await waitFor(() => w.deadLetters(actor.id).some(({ event: item }) => item.id === admitted[2]!.id));
    const saved = w.deadLetters(actor.id);
    expect(saved).toHaveLength(1);
    expect(saved[0]!.event.id).toBe(admitted[2]!.id);
    expect(saved[0]!.event.sequence).toBe(admitted[2]!.sequence);
    expect(saved[0]!.event.data?.payload?.revision).toBe("rev-c");
    w.release();
    await waitFor(() => actors.status(actor.id).status === "idle" && w.deadLetters(actor.id).length === 0, 30_000);
    expect(targetTasks(w, actors, actor.id, 77)).toHaveLength(1);
    expect(targetTasks(w, actors, actor.id, 77)[0]).toContain("rev-c");
    const outcomeRoot = path.join(w.mesh.root, "delivery-outcomes");
    const outcomes = fs.readdirSync(outcomeRoot).flatMap((file) => fs.readFileSync(path.join(outcomeRoot, file), "utf8")
      .split("\n").filter(Boolean).map((line) => JSON.parse(line) as { eventId: string; outcome: string }));
    expect(outcomes.filter((row) => admitted.some((item) => item.id === row.eventId)).map((row) => [row.eventId, row.outcome]))
      .toEqual([[admitted[0]!.id, "superseded"], [admitted[1]!.id, "superseded"], [admitted[2]!.id, "delivered"]]);
  }, 30_000);

  it.each([
    { direction: "newer replay replaces older queued work", queuedRevision: "queued-old", replayRevision: "replay-new", replayIsNewer: true },
    { direction: "older replay cannot replace newer queued work", queuedRevision: "queued-new", replayRevision: "replay-old", replayIsNewer: false },
  ])("coalesces a persisted replay against a same-key queued item: $direction", async ({ queuedRevision, replayRevision, replayIsNewer }) => {
    const w = setup(3);
    const first = w.manager();
    const actor = await first.create({ name: "reviewer", instructions: "Review.", topics: [], coalesce: false, coalesceKey: "payload.number" });
    await w.mesh.publish({ topic: "team.events", to: actor.id, from, text: "BLOCK" });
    await waitFor(() => first.status(actor.id).status === "preparing");
    const queued = await event(w, actor.id, 99, queuedRevision);
    await waitFor(() => first.status(actor.id).queued === 1);

    // Save a valid event-shaped dead-letter fixture while the newer/older queued event is persisted.
    // It is deliberately not published: ordinary ingress would coalesce it before replay is exercised.
    await first.close();
    const replayEvent = {
      ...queued,
      id: `replay-${replayRevision}`,
      sequence: queued.sequence + (replayIsNewer ? 1 : -1),
      data: revision(99, replayRevision),
    };
    const deadLetterFile = path.join(w.root, "actors", actor.id, "dead-letter.jsonl");
    fs.writeFileSync(deadLetterFile, `${JSON.stringify({ at: Date.now(), source: "mesh:team.events", event: replayEvent })}\n`);

    const second = w.manager();
    await waitFor(() => second.status(actor.id).queued >= 1);
    expect(w.deadLetters(actor.id)).toHaveLength(0);
    expect(replayEvent.sequence > queued.sequence).toBe(replayIsNewer);
    w.release();
    await waitFor(() => second.status(actor.id).status === "idle" && targetTasks(w, second, actor.id, 99).length >= 1 && w.deadLetters(actor.id).length === 0, 30_000);
    const runs = targetTasks(w, second, actor.id, 99);
    expect(runs).toHaveLength(1);
    const input = frame(runs[0]!);
    expect(input.payload.data.payload.revision).toBe(replayIsNewer ? replayRevision : queuedRevision);
    expect(input.coalesced.map(event => [event.id, event.sequence])).toEqual(
      [queued, replayEvent].sort((a, b) => a.sequence - b.sequence).map(event => [event.id, event.sequence]));
  }, 30_000);

  it("normalizes legacy keyed backlog before a one-slot replay, keeping sequence order over file order", async () => {
    const w = setup();
    const first = w.manager();
    const actor = await first.create({ name: "legacy", instructions: "Review.", topics: [], coalesce: false, coalesceKey: "payload.number" });
    await fillBeforeDeadLetter(w, first, actor.id);
    const old = await event(w, actor.id, 101, "legacy-old");
    const middle = await event(w, actor.id, 101, "legacy-middle");
    const newest = await event(w, actor.id, 101, "legacy-newest");
    await waitFor(() => w.deadLetters(actor.id).some(({ event: item }) => item.id === newest.id));
    await first.close();
    // A pre-fix file, including an older restored spill after its newest event.
    fs.writeFileSync(path.join(w.root, "actors", actor.id, "dead-letter.jsonl"), [newest, old, middle]
      .map((item) => JSON.stringify({ at: Date.now(), source: "mesh:team.events", event: item })).join("\n") + "\n");
    const second = w.manager();
    w.release();
    await waitFor(() => second.status(actor.id).status === "idle" && w.deadLetters(actor.id).length === 0, 30_000);
    const runs = targetTasks(w, second, actor.id, 101);
    expect(runs).toHaveLength(1);
    const input = frame(runs[0]!);
    expect(input.payload.data.payload.revision).toBe("legacy-newest");
    expect(input.coalesced.map(event => [event.id, event.sequence])).toEqual([old, middle, newest].map(event => [event.id, event.sequence]));
  }, 30_000);

  it("retains every unkeyed dead letter for one replay apiece", async () => {
    const w = setup();
    const actors = w.manager();
    const actor = await actors.create({ name: "plain", instructions: "Review.", topics: [], coalesce: false });
    await fillBeforeDeadLetter(w, actors, actor.id);
    const admitted = [
      await w.mesh.publish({ topic: "team.events", to: actor.id, from, text: "unkeyed-a" }),
      await w.mesh.publish({ topic: "team.events", to: actor.id, from, text: "unkeyed-b" }),
      await w.mesh.publish({ topic: "team.events", to: actor.id, from, text: "unkeyed-c" }),
    ];
    await waitFor(() => w.deadLetters(actor.id).length === 3);
    expect(w.deadLetters(actor.id).map(({ event: item }) => item.id)).toEqual(admitted.map((item) => item.id));
    expect(new Set(w.deadLetters(actor.id).map(({ event: item }) => item.id)).size).toBe(3);
    w.release();
    await waitFor(() => w.deadLetters(actor.id).length === 0 && admitted.every((item) => w.runTasks(actors, actor.id).some((task) => task.includes(item.text!))), 30_000);
    const tasks = w.runTasks(actors, actor.id);
    for (const item of admitted) expect(tasks.filter((task) => task.includes(item.text!))).toHaveLength(1);
  }, 30_000);

  it("persists the coalesced newest dead-letter item across manager restart", async () => {
    const w = setup();
    const first = w.manager();
    const actor = await first.create({ name: "reviewer", instructions: "Review.", topics: [], coalesce: false, coalesceKey: "payload.number" });
    await fillBeforeDeadLetter(w, first, actor.id);
    const a = await event(w, actor.id, 123, "restart-a");
    await event(w, actor.id, 123, "restart-b");
    const newest = await event(w, actor.id, 123, "restart-c");
    await waitFor(() => w.deadLetters(actor.id).some(({ event: item }) => item.id === newest.id));
    await first.close();
    const second = w.manager();
    await waitFor(() => second.status(actor.id).queued > 0);
    expect(w.deadLetters(actor.id).map(({ event: item }) => item.id)).toEqual([newest.id]);
    expect(w.deadLetters(actor.id)[0]!.event.sequence).toBeGreaterThan(a.sequence);
    expect(w.deadLetters(actor.id)[0]!.event.data?.payload?.revision).toBe("restart-c");
  }, 30_000);
});
