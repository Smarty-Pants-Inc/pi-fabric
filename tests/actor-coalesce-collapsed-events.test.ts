import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#4605: with a coalesceKey, merged queue items kept only the newest event's payload, so a
// human comment or review on a PR was lost behind a later check event for the same PR. A coalesced run
// now receives a record per collapsed event, oldest first, the newest included; the newest event stays
// the run's primary payload.

// =====================================================================================================
// Contract adapter: the ONLY place in this file that names the new surface. The implementation chooses
// the names; map them here once and every test below follows.
//
// The run's JSON frame ({ source, payload, id }) gains a list field `coalesced`, one record per
// collapsed event in sequence order:
//   { sequence, topic, kind, event, action, payload }
// `event` is the event's data.event and `action` its data.payload.action. Each record's `payload` is
// the entire original MeshEvent, so this adapter reads its full data from payload.data. The payload
// is absent on a bounded summary (past the full-data cap).
// =====================================================================================================
interface Collapsed {
  /** The mesh sequence. */
  sequence: number;
  topic: string | undefined;
  kind: string | undefined;
  /** data.event */
  event: string | undefined;
  /** data.payload.action */
  action: string | undefined;
  /** The event's full data; undefined on a bounded summary. */
  data: unknown;
}
/** Full data is bounded: about this many events, and this many bytes, keep it; the rest are summaries. */
const FULL_DATA_MAX_EVENTS = 50;
const FULL_DATA_MAX_BYTES = 64 * 1024;
/** The JSON frame of the rendered run task: the block that carries the activation's source and payload. */
const runFrame = (task: string): Record<string, unknown> & { source: string; payload: unknown } => {
  for (const block of task.split("\n\n")) {
    if (!block.startsWith("{")) continue;
    try {
      const value = JSON.parse(block) as Record<string, unknown>;
      if (typeof value.source === "string" && "payload" in value) return value as Record<string, unknown> & { source: string; payload: unknown };
    } catch { /* not the frame */ }
  }
  throw new Error(`no run frame in task:\n${task.slice(0, 2_000)}`);
};
/** The collapsed-event records the run sees, in rendered order; [] when the run carries no list. */
const collapsedEvents = (task: string): Collapsed[] => {
  const list = runFrame(task).coalesced;
  return Array.isArray(list) ? list.map((raw: Record<string, unknown>) => ({
    sequence: raw.sequence as number,
    topic: raw.topic as string | undefined,
    kind: raw.kind as string | undefined,
    event: raw.event as string | undefined,
    action: raw.action as string | undefined,
    data: (raw.payload as { data?: unknown } | undefined)?.data,
  })) : [];
};
// =====================================================================================================

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(closers.splice(0).map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };
const PULLS = "github.demo.pulls";
const OTHER = "github.other.pulls";

const waitFor = async (predicate: () => boolean, timeoutMs = 15_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

type GithubData = { event: string; payload: { action?: string; number?: unknown } & Record<string, unknown> };
const github = (mesh: MeshStore, topic: string, event: string, payload: GithubData["payload"]) =>
  mesh.publish({ topic, kind: "github.webhook", from, data: { event, payload } });
const dataOf = (event: MeshEvent) => event.data as GithubData;
/** What every record must name, from the published events. */
const expected = (events: MeshEvent[]) => events.map((event) => ({
  sequence: event.sequence, topic: event.topic, event: dataOf(event).event, action: dataOf(event).payload.action,
}));
const named = (records: Collapsed[]) => records.map(({ sequence, topic, event, action }) => ({ sequence, topic, event, action }));
const primary = (task: string) => runFrame(task).payload as MeshEvent;
const prNumber = (task: string) => {
  try { return dataOf(primary(task))?.payload?.number; } catch { return undefined; }
};

// Every activation's AgentRunRequest is recorded, and runs hold until release(), so events queue behind
// a blocker deterministically. The mesh monitor is captured, not started: tests admit events through its
// real onEvent callback, in the order they choose (the harness of actor-coalesce-key.test.ts).
const world = (queueLimit = DEFAULT_FABRIC_CONFIG.mesh.actorQueueLimit) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-coalesce-all-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const runs: AgentRunRequest[] = [];
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  vi.spyOn(agents, "run").mockImplementation(async (request, signal, onSpawned) => {
    runs.push(request);
    const id = `coalesce-all-${runs.length}`;
    onSpawned?.({ id, name: "recorded", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
    await new Promise<void>((resolve, reject) => {
      const abort = () => reject(new Error("held coalesce-all run aborted"));
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) abort();
      void held.then(() => { signal?.removeEventListener("abort", abort); resolve(); });
    });
    return { id, name: "recorded", task: request.task, status: "completed", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: true, text: "",
      value: { action: "silent" }, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } satisfies AgentRunResult;
  });
  let monitor: ActorMeshMonitor | undefined;
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
  let actors: ActorManager | undefined;
  const open = (limit = queueLimit) => (actors = new ActorManager("test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: limit }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 1_000,
      meshRetentionSweepPath: false, canConsumeMesh: () => false,
    }));
  closers.push(async () => { release(); await actors?.close(); await agents.close(); });
  const deadLetterFile = (actorId: string) => path.join(root, "actors", actorId, "dead-letter.jsonl");
  return {
    root, mesh, runs, open, deadLetterFile,
    /** Admit events through the real monitor callback, in this order. */
    admit: (...events: MeshEvent[]) => { for (const event of events) expect(monitor!.callbacks.onEvent(event)).toBe(true); },
    /** Hold the actor busy with a direct message, so the events that follow queue behind it. */
    block: async (actorId: string) => {
      const before = runs.length;
      actors!.tell(actorId, "coalesce-all-blocker");
      await waitFor(() => runs.length === before + 1);
    },
    /** Close the manager (aborting the held run, its queue checkpointed), run between(), and reopen. */
    reopen: async (between?: () => void, limit?: number) => {
      await actors!.close();
      between?.();
      return open(limit);
    },
    /** Release every held run; return the tasks of the runs that started after this call, once idle. */
    finish: async (actorId: string, count: number): Promise<string[]> => {
      const before = runs.length;
      release();
      await waitFor(() => runs.length >= before + count && actors!.status(actorId).status === "idle", 20_000);
      expect(runs.length).toBe(before + count);
      return runs.slice(before).map((run) => run.task);
    },
    deadLetters: (actorId: string) => fs.existsSync(deadLetterFile(actorId))
      ? fs.readFileSync(deadLetterFile(actorId), "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line) as { event: MeshEvent })
      : [],
  };
};
const reviewer = (actors: ActorManager, coalesceKey?: string) => actors.create({
  name: "pr-reviewer", instructions: "Review.", topics: [PULLS, OTHER], coalesce: false, ...(coalesceKey ? { coalesceKey } : {}),
});

describe("a coalesced actor run receives every collapsed event (smarty-dev#4605)", () => {
  it("Paul's case: an issue_comment then a check_run on PR 7 before the run starts; the run gets both, the comment first", async () => {
    const w = world();
    const actors = w.open();
    const actor = await reviewer(actors, "payload.number");
    await w.block(actor.id);
    const comment = await github(w.mesh, PULLS, "issue_comment", {
      action: "created", number: 7, comment: { body: "human-comment-on-pr-7: please rename this helper" },
    });
    const check = await github(w.mesh, PULLS, "check_run", {
      action: "completed", number: 7, check_run: { name: "ci", conclusion: "success" },
    });
    w.admit(comment, check);
    expect(actors.status(actor.id).queued).toBe(1);                       // they still collapse into one run
    const [task] = await w.finish(actor.id, 1);
    expect(primary(task!)).toEqual(check);                                // the newest stays the primary payload
    expect(task).toContain("human-comment-on-pr-7");                      // the human comment reached the run
    const records = collapsedEvents(task!);
    expect(named(records)).toEqual(expected([comment, check]));
    expect(records.map((record) => record.kind)).toEqual(["github.webhook", "github.webhook"]);
    expect(records.map((record) => record.data)).toEqual([comment.data, check.data]);
  }, 30_000);

  it("collapses a review request, a comment and a merge on PR 9 into one run naming all three, in sequence order", async () => {
    const w = world();
    const actors = w.open();
    const actor = await reviewer(actors, "payload.number");
    await w.block(actor.id);
    const requested = await github(w.mesh, PULLS, "pull_request", {
      action: "review_requested", number: 9, requested_reviewer: { login: "reviewer-nine" },
    });
    const comment = await github(w.mesh, PULLS, "issue_comment", {
      action: "created", number: 9, comment: { body: "comment-on-pr-9: looks right" },
    });
    const merged = await github(w.mesh, PULLS, "pull_request", {
      action: "closed", number: 9, pull_request: { merged: true, merge_commit_sha: "merge-sha-pr-9" },
    });
    w.admit(requested, comment, merged);
    expect(actors.status(actor.id).queued).toBe(1);
    const [task] = await w.finish(actor.id, 1);
    expect(primary(task!)).toEqual(merged);
    expect(task).toContain("reviewer-nine");
    expect(task).toContain("comment-on-pr-9");
    const records = collapsedEvents(task!);
    expect(records.map(({ event, action }) => [event, action]))
      .toEqual([["pull_request", "review_requested"], ["issue_comment", "created"], ["pull_request", "closed"]]);
    expect(named(records)).toEqual(expected([requested, comment, merged]));
    expect(records.map((record) => record.data)).toEqual([requested.data, comment.data, merged.data]);
  }, 30_000);

  it("lists an older event replayed after a newer one is queued, in sequence order, with the newest still primary", async () => {
    const w = world();
    let actors = w.open();
    const actor = await reviewer(actors, "payload.number");
    await w.block(actor.id);
    const older = await github(w.mesh, PULLS, "issue_comment", {
      action: "created", number: 11, comment: { body: "replayed-older-comment-on-pr-11" },
    });
    const newer = await github(w.mesh, PULLS, "check_run", { action: "completed", number: 11, check_run: { name: "ci" } });
    expect(newer.sequence).toBeGreaterThan(older.sequence);
    w.admit(newer);                                                       // only the newer one is queued
    expect(actors.status(actor.id).queued).toBe(1);
    // The older event returns through the dead-letter replay (#enqueue with replaying: the
    // older-after-newer branch), against the restored queue that holds the newer one.
    actors = await w.reopen(() => fs.writeFileSync(w.deadLetterFile(actor.id),
      `${JSON.stringify({ at: Date.now(), source: `mesh:${PULLS}`, event: older })}\n`));
    await waitFor(() => w.runs.length === 2);                             // the restored blocker holds the queue again
    await waitFor(() => w.deadLetters(actor.id).length === 0 && actors.status(actor.id).queued === 1);
    const tasks = await w.finish(actor.id, 1);
    const task = tasks[0]!;
    expect(prNumber(task)).toBe(11);
    expect(primary(task)).toEqual(newer);
    expect(task).toContain("replayed-older-comment-on-pr-11");
    expect(named(collapsedEvents(task))).toEqual(expected([older, newer]));
  }, 40_000);

  it("setCoalesceKey on a populated queue: every merged item lists all of its collapsed events", async () => {
    const w = world();
    const actors = w.open();
    const actor = await reviewer(actors);                                 // no key yet: everything queues
    await w.block(actor.id);
    const comment1 = await github(w.mesh, PULLS, "issue_comment", { action: "created", number: 1, comment: { body: "comment-on-pr-1" } });
    const check1 = await github(w.mesh, PULLS, "check_run", { action: "completed", number: 1, check_run: { name: "ci" } });
    const review2 = await github(w.mesh, PULLS, "pull_request", { action: "review_requested", number: 2 });
    const comment2 = await github(w.mesh, PULLS, "issue_comment", { action: "created", number: 2, comment: { body: "comment-on-pr-2" } });
    // PR 1's older comment is admitted after its newer check: #mergeCoalesced's older-item branch.
    w.admit(check1, review2, comment1, comment2);
    expect(actors.status(actor.id).queued).toBe(4);
    await actors.setCoalesceKey(actor.id, "payload.number");
    expect(actors.status(actor.id).queued).toBe(2);
    const [pr1, pr2] = await w.finish(actor.id, 2);
    expect(primary(pr1!)).toEqual(check1);
    expect(primary(pr2!)).toEqual(comment2);
    expect(pr1).toContain("comment-on-pr-1");
    expect(named(collapsedEvents(pr1!))).toEqual(expected([comment1, check1]));
    expect(named(collapsedEvents(pr2!))).toEqual(expected([review2, comment2]));
  }, 30_000);

  it("keeps the list of a merged queued item across a restart", async () => {
    const w = world();
    let actors = w.open();
    const actor = await reviewer(actors, "payload.number");
    await w.block(actor.id);
    const comment = await github(w.mesh, PULLS, "issue_comment", { action: "created", number: 5, comment: { body: "comment-before-restart-pr-5" } });
    const check = await github(w.mesh, PULLS, "check_run", { action: "completed", number: 5, check_run: { name: "ci" } });
    w.admit(comment, check);
    expect(actors.status(actor.id).queued).toBe(1);
    actors = await w.reopen();                                            // serialised, then restored from the queue file
    await waitFor(() => w.runs.length === 2);
    expect(actors.status(actor.id).queued).toBe(1);
    const [task] = await w.finish(actor.id, 1);
    expect(primary(task!)).toEqual(check);
    expect(task).toContain("comment-before-restart-pr-5");
    expect(named(collapsedEvents(task!))).toEqual(expected([comment, check]));
  }, 40_000);

  it("keeps the list when keyed events coalesce in the dead-letter file", async () => {
    const w = world(1);                                                   // queue 1, overflow 8
    const actors = w.open();
    const actor = await reviewer(actors, "payload.number");
    await w.block(actor.id);
    for (let number = 101; number <= 109; number++) {                     // distinct keys fill the queue and overflow
      w.admit(await github(w.mesh, PULLS, "check_run", { action: "completed", number }));
    }
    expect(actors.status(actor.id).queued).toBe(9);
    const requested = await github(w.mesh, PULLS, "pull_request", { action: "review_requested", number: 77 });
    const comment = await github(w.mesh, PULLS, "issue_comment", { action: "created", number: 77, comment: { body: "dead-letter-comment-on-pr-77" } });
    const merged = await github(w.mesh, PULLS, "pull_request", { action: "closed", number: 77, pull_request: { merged: true } });
    w.admit(requested, comment, merged);
    expect(w.deadLetters(actor.id).map(({ event }) => event.id)).toEqual([merged.id]);   // coalesced in the file
    const tasks = await w.finish(actor.id, 10);
    await waitFor(() => w.deadLetters(actor.id).length === 0);
    const pr77 = tasks.filter((task) => prNumber(task) === 77);
    expect(pr77).toHaveLength(1);
    expect(primary(pr77[0]!)).toEqual(merged);
    expect(pr77[0]).toContain("dead-letter-comment-on-pr-77");
    expect(named(collapsedEvents(pr77[0]!))).toEqual(expected([requested, comment, merged]));
  }, 40_000);

  it("keeps the list of a merged item that spills to the dead-letter file on restore", async () => {
    const w = world();
    let actors = w.open();
    const actor = await reviewer(actors, "payload.number");
    await w.block(actor.id);
    for (let number = 201; number <= 210; number++) {
      w.admit(await github(w.mesh, PULLS, "check_run", { action: "completed", number }));
    }
    const comment = await github(w.mesh, PULLS, "issue_comment", { action: "created", number: 78, comment: { body: "spilled-comment-on-pr-78" } });
    const check = await github(w.mesh, PULLS, "check_run", { action: "completed", number: 78, check_run: { name: "ci" } });
    w.admit(comment, check);
    expect(actors.status(actor.id).queued).toBe(11);                      // PR 78's merged item is last
    // Restored under a smaller queue (1, overflow 8): the run-order tail spills to the dead-letter file.
    actors = await w.reopen(undefined, 1);
    await waitFor(() => w.deadLetters(actor.id).some(({ event }) => event.id === check.id));
    const tasks = await w.finish(actor.id, 12);
    await waitFor(() => w.deadLetters(actor.id).length === 0);
    const pr78 = tasks.filter((task) => prNumber(task) === 78);
    expect(pr78).toHaveLength(1);
    expect(primary(pr78[0]!)).toEqual(check);
    expect(pr78[0]).toContain("spilled-comment-on-pr-78");
    expect(named(collapsedEvents(pr78[0]!))).toEqual(expected([comment, check]));
  }, 40_000);

  it("bounds the full data of 60 collapsed events and still names all 60 sequences, types and actions", async () => {
    const w = world();
    const actors = w.open();
    const actor = await reviewer(actors, "payload.number");
    await w.block(actor.id);
    const kinds = [["issue_comment", "created"], ["pull_request_review", "submitted"], ["check_run", "completed"], ["pull_request", "synchronize"]] as const;
    const filler = "x".repeat(4_096);                                    // 60 x 4 KiB: far past the full-data cap
    const events: MeshEvent[] = [];
    for (let index = 0; index < 60; index++) {
      const [event, action] = kinds[index % kinds.length]!;
      events.push(await github(w.mesh, PULLS, event, { action, number: 60, body: `capped-body-${index}-end ${filler}` }));
    }
    w.admit(...events);
    expect(actors.status(actor.id).queued).toBe(1);
    const [task] = await w.finish(actor.id, 1);
    expect(primary(task!)).toEqual(events[59]);
    const records = collapsedEvents(task!);
    // Never silently drop an event's existence: every one is named.
    expect(named(records)).toEqual(expected(events));
    // Full data is bounded, and what is kept is whole.
    const full = records.filter((record) => record.data !== undefined);
    expect(full.length).toBeGreaterThan(0);
    expect(full.length).toBeLessThanOrEqual(FULL_DATA_MAX_EVENTS);
    expect(full.reduce((bytes, record) => bytes + Buffer.byteLength(JSON.stringify(record.data)), 0)).toBeLessThanOrEqual(FULL_DATA_MAX_BYTES);
    for (const record of full) expect(record.data).toEqual(events.find((event) => event.sequence === record.sequence)!.data);
    expect(new Set(task!.match(/capped-body-\d+-end/g)).size).toBeLessThanOrEqual(FULL_DATA_MAX_EVENTS + 1);
    expect(Buffer.byteLength(task!)).toBeLessThan(FULL_DATA_MAX_BYTES + 64 * 1024);   // unbounded would be > 240 KiB
  }, 30_000);

  it("control: distinct keys, distinct topics and check events with no number never merge", async () => {
    const w = world();
    const actors = w.open();
    const actor = await reviewer(actors, "payload.number");
    await w.block(actor.id);
    const comment7 = await github(w.mesh, PULLS, "issue_comment", { action: "created", number: 7, comment: { body: "control-comment-pr-7" } });
    const comment8 = await github(w.mesh, PULLS, "issue_comment", { action: "created", number: 8, comment: { body: "control-comment-pr-8" } });
    const other7 = await github(w.mesh, OTHER, "check_run", { action: "completed", number: 7, check_run: { name: "other-topic-ci" } });
    // GitHub check events name their PRs under check_run.pull_requests, not at payload.number.
    const bare1 = await github(w.mesh, PULLS, "check_run", { action: "completed", check_run: { name: "bare-ci-1", pull_requests: [{ number: 7 }] } });
    const bare2 = await github(w.mesh, PULLS, "check_run", { action: "completed", check_run: { name: "bare-ci-2", pull_requests: [{ number: 7 }] } });
    const check7 = await github(w.mesh, PULLS, "check_run", { action: "completed", number: 7, check_run: { name: "ci" } });
    w.admit(comment7, comment8, other7, bare1, bare2, check7);
    expect(actors.status(actor.id).queued).toBe(5);
    const tasks = await w.finish(actor.id, 5);
    expect(tasks.map((task) => primary(task).id)).toEqual([check7.id, comment8.id, other7.id, bare1.id, bare2.id]);
    // A run that collapsed nothing lists at most its own event, never another subject's.
    for (const [index, own] of [[1, comment8], [2, other7], [3, bare1], [4, bare2]] as const) {
      const records = collapsedEvents(tasks[index]!);
      expect(records.length).toBeLessThanOrEqual(1);
      expect(records.every((record) => record.sequence === own.sequence)).toBe(true);
      expect(tasks[index]).not.toContain("control-comment-pr-7");
    }
    expect(tasks[0]).not.toContain("control-comment-pr-8");
    expect(tasks[0]).not.toContain("other-topic-ci");
    expect(tasks[0]).not.toContain("bare-ci-");
    // The one same-key, same-topic pair collapsed, and its run names exactly those two.
    expect(named(collapsedEvents(tasks[0]!))).toEqual(expected([comment7, check7]));
  }, 30_000);
});
