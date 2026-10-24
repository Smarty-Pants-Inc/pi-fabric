import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import type { FabricActorRequest } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";

// Keep reason: #4605 ADV-3c/3d, a failure path not cheaply reachable in a live flow.
// A crash/duplicate checkpoint may contain history absent from an already launched
// activation. Feed the genuine monitor receiver and inspect actual run requests.
const TOPIC = "github.deferred-replay.pulls";
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0)) await close(); vi.restoreAllMocks(); });
const waitFor = async (predicate: () => boolean, timeout = 10_000) => {
  const deadline = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("deferred replay did not progress without new ingress");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const frame = (task: string): { payload: MeshEvent; coalesced?: Array<{ id: string; untrusted?: true; payload?: MeshEvent }> } => {
  for (const block of task.split("\n\n")) {
    try { const value = JSON.parse(block); if (value.source && value.payload) return value; } catch { /* text */ }
  }
  throw new Error("missing actor run frame");
};
const seenIds = (task: string) => frame(task).coalesced?.map(record => record.id) ?? [frame(task).payload.id];
const skipPadding = { id: "padding", where: [{ path: "data.event", equals: "padding" }] };
const history = (event: MeshEvent) => ({ id: event.id, sequence: event.sequence, topic: event.topic, kind: event.kind,
  from: event.from, sender: event.sender, event: (event.data as { event: string }).event,
  action: "created", payload: event });
const world = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-deferred-replay-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const runs: AgentRunRequest[] = [];
  let release!: () => void;
  let held = new Promise<void>(resolve => { release = resolve; });
  vi.spyOn(agents, "run").mockImplementation(async (request, signal, onSpawned) => {
    const id = `deferred-replay-${runs.push(request)}`;
    onSpawned?.({ id, name: "recorded", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
    await new Promise<void>((resolve, reject) => {
      const abort = () => reject(new Error("deferred replay run aborted"));
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) abort();
      void held.then(() => { signal?.removeEventListener("abort", abort); resolve(); });
    });
    return { id, name: "recorded", task: request.task, status: "completed", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: true,
      text: "", value: { action: "silent" }, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } satisfies AgentRunResult;
  });
  let monitor!: ActorMeshMonitor;
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
  let actors!: ActorManager;
  const open = () => (actors = new ActorManager("test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: 1 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 100,
      meshRetentionSweepPath: false, canConsumeMesh: () => false,
    }));
  open();
  cleanup.push(async () => { release(); await actors.close(); await agents.close(); fs.rmSync(root, { recursive: true, force: true }); });
  const file = (id: string) => path.join(root, "actors", id, "dead-letter.jsonl");
  return {
    mesh, runs, get actors() { return actors; },
    create: (validWhile?: string, extra: Partial<FabricActorRequest> = {}) => actors.create({ name: "reviewer", instructions: "Review.", topics: [TOPIC], coalesce: false,
      coalesceKey: "payload.number", ...(validWhile ? { validWhile: { version: 1, source: validWhile } } : {}), ...extra }),
    pause: () => { held = new Promise<void>(resolve => { release = resolve; }); },
    publish: (event: string, number = 7) => mesh.publish({ topic: TOPIC, kind: "github.webhook", from,
      data: { event, payload: { action: "created", number, body: `human-${number}-${event}` } } }),
    admit: (...events: MeshEvent[]) => { for (const event of events) expect(monitor.callbacks.onEvent(event)).toBe(true); },
    primeBacklog: async () => {
      // Register backlog through genuine overflow admission before replacing its crash
      // checkpoint. A raw post-start file write alone bypasses the receiver's count cache.
      for (let n = 0; n < 10; n++) {
        const event = await mesh.publish({ topic: TOPIC, kind: "github.webhook", from,
          data: { event: "padding", payload: { action: "created", number: 500 + n } } });
        expect(monitor.callbacks.onEvent(event)).toBe(true);
      }
    },
    write: (id: string, entries: unknown[]) => fs.writeFileSync(file(id), entries.map(entry => JSON.stringify(entry)).join("\n") + "\n"),
    deadLetters: (id: string) => fs.existsSync(file(id)) ? fs.readFileSync(file(id), "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)) : [],
    entry: (event: MeshEvent, coalesced?: unknown[]) => ({ at: Date.now(), source: `mesh:${TOPIC}`, event, ...(coalesced ? { coalesced } : {}) }),
    finish: async (id: string, count: number) => { release(); await waitFor(() => runs.length >= count && actors.status(id).status === "idle"); expect(runs).toHaveLength(count); },
  };
};

describe("settlement releases richer deferred coalesced dead letters (#4605 round 2)", () => {
  it("ADV-3c: empty-FIFO settlement replays richer history and unrelated PR300 without another arrival", async () => {
    const w = world();
    const actor = await w.create();
    const older = await w.publish("issue_comment");
    const primary = await w.publish("check_run");
    const unrelated = await w.publish("issue_comment", 300);
    w.admit(primary);
    await waitFor(() => w.runs.length === 1);
    const immutable = w.runs[0]!.task;
    await w.primeBacklog();
    w.write(actor.id, [w.entry(primary, [history(older), history(primary)]), w.entry(unrelated)]);
    await w.actors.setActivationFilter(actor.id, [skipPadding]); // removes filler, NOT a new input event
    expect(w.actors.status(actor.id).queued).toBe(0);
    expect(w.deadLetters(actor.id)).toHaveLength(2);
    expect(w.runs[0]!.task).toBe(immutable);
    await w.finish(actor.id, 3);
    expect(w.runs.slice(1).flatMap(run => seenIds(run.task))).toEqual([older.id, unrelated.id]);
    expect(w.deadLetters(actor.id)).toEqual([]);
  });

  it("ADV-3d: the already captured primary is not delivered twice when only older history was missing", async () => {
    const w = world();
    const actor = await w.create();
    const older = await w.publish("issue_comment");
    const primary = await w.publish("check_run");
    w.admit(primary);
    await waitFor(() => w.runs.length === 1);
    await w.primeBacklog();
    w.write(actor.id, [w.entry(primary, [history(older), history(primary)])]);
    await w.actors.setActivationFilter(actor.id, [skipPadding]);
    await w.finish(actor.id, 2);
    expect(w.runs.flatMap(run => seenIds(run.task)).filter(id => id === primary.id)).toHaveLength(1);
    expect(frame(w.runs[1]!.task).payload).toEqual(older);
    expect(seenIds(w.runs[1]!.task)).toEqual([older.id]);
  });

  it("counterexample: new same-ID authority remains replayable and never mutates the launched prompt", async () => {
    const w = world();
    const actor = await w.create();
    const older = await w.publish("issue_comment");
    const primary = await w.publish("check_run");
    w.admit(primary);
    await waitFor(() => w.runs.length === 1);
    const immutable = w.runs[0]!.task;
    const richer = { ...history(primary), untrusted: true };
    await w.primeBacklog();
    w.write(actor.id, [w.entry(primary, [history(older), richer])]);
    await w.actors.setActivationFilter(actor.id, [skipPadding]);
    expect(w.runs[0]!.task).toBe(immutable);
    await w.finish(actor.id, 2);
    expect(seenIds(w.runs[1]!.task)).toEqual([older.id, primary.id]);
    expect(frame(w.runs[1]!.task).coalesced?.find(record => record.id === primary.id)?.untrusted).toBe(true);
    expect(w.runs[1]!.task).toContain("UNTRUSTED");
  });

  it("replay filtering of a stored collapsed entry drops only the failing primary and keeps the older comment", async () => {
    const w = world();
    const actor = await w.create();
    w.actors.tell(actor.id, "hold while dead letter is restored");
    await waitFor(() => w.runs.length === 1);
    const older = await w.publish("issue_comment");
    const primary = await w.publish("check_run");
    await w.primeBacklog();
    w.write(actor.id, [w.entry(primary, [history(older), history(primary)])]);
    await w.actors.setActivationFilter(actor.id, [skipPadding, { id: "checks", where: [{ path: "data.event", equals: "check_run" }] }]);
    expect(w.actors.status(actor.id).queued).toBe(1);
    await w.finish(actor.id, 2);
    expect(frame(w.runs[1]!.task).payload).toEqual(older);
    expect(seenIds(w.runs[1]!.task)).toEqual([older.id]);
    expect(w.actors.messages(actor.id).find(message => message.reason === "filtered: checks")?.data)
      .toMatchObject({ filteredEventId: primary.id });
  });

  it("replay dedupe prunes only completed records and retires an all-completed entry without losing fresh history", async () => {
    const w = world();
    const actor = await w.create(undefined, { residency: "durable", dedupeKey: "data.event" });
    const completed = await w.publish("check_run");
    w.admit(completed);
    await waitFor(() => w.runs.length === 1);
    await w.finish(actor.id, 1);
    w.pause();
    w.actors.tell(actor.id, "hold while completed occurrences return through dead letters");
    await waitFor(() => w.runs.length === 2);
    const fresh = await w.publish("issue_comment");
    const duplicate = await w.publish("check_run");
    const allCompleted = await w.publish("check_run", 300);
    await w.primeBacklog();
    w.write(actor.id, [w.entry(duplicate, [history(fresh), history(duplicate)]), w.entry(allCompleted)]);
    await w.actors.setActivationFilter(actor.id, [skipPadding]);
    expect(w.deadLetters(actor.id)).toEqual([]);
    expect(w.actors.status(actor.id).queued).toBe(1);
    await w.finish(actor.id, 3);
    expect(seenIds(w.runs[2]!.task)).toEqual([fresh.id]);
    const skipped = w.actors.messages(actor.id).filter(message => message.reason === "deduplicated: completed key")
      .map(message => (message.data as { skippedEventId: string }).skippedEventId);
    expect(skipped).toEqual([duplicate.id, allCompleted.id]);
  });

  it("legacy history without original activation facts is unknown, not judged with the newest record's facts", async () => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || activation.sequence < 0');
    w.actors.tell(actor.id, "hold while legacy history is restored");
    await waitFor(() => w.runs.length === 1);
    const older = await w.publish("issue_comment");
    const primary = await w.publish("check_run");
    // This old checkpoint has no original actor acceptance facts for its historical
    // comment. The replay must not manufacture them from the mesh sequence.
    await w.primeBacklog();
    w.write(actor.id, [w.entry(primary, [history(older), history(primary)])]);
    await w.actors.setActivationFilter(actor.id, [skipPadding]);
    await w.finish(actor.id, 2);
    expect(seenIds(w.runs[1]!.task)).toContain(older.id);
    expect(w.runs[1]!.task).toContain("human-7-issue_comment");
  });
});
