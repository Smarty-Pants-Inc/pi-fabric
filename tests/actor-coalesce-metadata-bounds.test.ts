import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { mergeCoalescedMeshEvents, restoreCoalescedMeshEvents } from "../src/actors/coalesced-events.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricMessageSender } from "../src/protocol.js";

// Regression/keep reason: smarty-dev#4605 ADV-4a/4b, bounded admission and a trust
// boundary. Exercise the real monitor -> ActorManager -> durable queue -> run
// request, not only the helper or source text. Timings are evidence, not gates.
const TOPIC = "github.meta";
const METADATA_BYTES = 64 * 1024;
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "f", name: "f", kind: "main" };
const principal = { id: "p", binding: "voice-call" as const };
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(closers.splice(0).map(close => close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

interface RecordMetadata {
  id: string; sequence: number; topic: string; kind: string; event?: string; action?: string;
  payload?: MeshEvent; dataOmitted?: true; metadataTruncated?: true; untrusted?: true;
  from?: MeshIdentity; sender?: FabricMessageSender; principal?: MeshEvent["principal"];
  omittedBefore?: { count: number; firstSequence: number; lastSequence: number; countIsLowerBound?: true };
}
interface Frame { source: string; payload: MeshEvent; untrusted?: true; coalesced: RecordMetadata[] }
const frame = (task: string): Frame => {
  for (const block of task.split("\n\n")) {
    if (!block.startsWith("{")) continue;
    try {
      const value = JSON.parse(block) as Frame;
      if (typeof value.source === "string" && value.payload) return value;
    } catch { /* not the activation frame */ }
  }
  throw new Error("missing activation frame");
};
const metadataBytes = (records: RecordMetadata[]) => Buffer.byteLength(JSON.stringify(records.map(record => {
  const { payload: _payload, ...metadata } = record;
  return metadata;
}), null, 2));
const expectStringBounds = (value: unknown): void => {
  if (typeof value === "string") expect(value.length).toBeLessThanOrEqual(256);
  else if (Array.isArray(value)) for (const part of value) expectStringBounds(part);
  else if (value && typeof value === "object") for (const [key, part] of Object.entries(value)) {
    expect(key.length).toBeLessThanOrEqual(256);
    expectStringBounds(part);
  }
};
const expectBounds = (records: RecordMetadata[]) => {
  expect(records.length).toBeLessThanOrEqual(100);
  expect(metadataBytes(records)).toBeLessThanOrEqual(METADATA_BYTES);
  for (const { payload: _payload, ...metadata } of records) expectStringBounds(metadata);
};
const expectFullEventBounds = (input: Frame) => {
  const copies = [input.payload, ...input.coalesced.flatMap(record => record.payload ? [record.payload] : [])];
  expect(copies.length).toBeLessThanOrEqual(50);
  expect(copies.reduce((sum, event) => sum + Buffer.byteLength(JSON.stringify(event)), 0)).toBeLessThanOrEqual(64 * 1024);
};
const waitFor = async (predicate: () => boolean, timeoutMs = 15_000) => {
  const until = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= until) throw new Error("timed out waiting for metadata actor");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};

const world = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-metadata-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 512 * 1024, 1_000);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const runs: AgentRunRequest[] = [];
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  vi.spyOn(agents, "run").mockImplementation(async (request, signal, onSpawned) => {
    runs.push(request);
    const id = `metadata-${runs.length}`;
    onSpawned?.({ id, name: "recorded", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
    await new Promise<void>((resolve, reject) => {
      const abort = () => reject(new Error("held metadata run aborted"));
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) abort();
      void held.then(() => { signal?.removeEventListener("abort", abort); resolve(); });
    });
    return { id, name: "recorded", task: request.task, status: "completed", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: true, text: "",
      value: { action: "silent" }, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } satisfies AgentRunResult;
  });
  let monitor!: ActorMeshMonitor;
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
  let actors: ActorManager;
  const open = () => (actors = new ActorManager("test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 1_000,
      meshRetentionSweepPath: false, canConsumeMesh: () => false,
    }));
  closers.push(async () => { release(); await actors?.close(); await agents.close(); });
  const checkpoint = (actorId: string) => {
    const dir = path.join(root, "actors", actorId);
    const files = fs.readdirSync(dir).filter(name => /^queue-.+\.json$/.test(name));
    const text = files.map(name => fs.readFileSync(path.join(dir, name), "utf8"));
    const items = text.flatMap(json => (JSON.parse(json) as { items: Frame[] }).items).filter(item => item.source === `mesh:${TOPIC}`);
    expect(items).toHaveLength(1);
    return { item: items[0]!, bytes: text.reduce((sum, json) => sum + Buffer.byteLength(json), 0) };
  };
  return {
    mesh, runs, open, checkpoint,
    admit: (...events: MeshEvent[]) => { for (const event of events) expect(monitor.callbacks.onEvent(event)).toBe(true); },
    block: async (actorId: string) => {
      const before = runs.length;
      actors.tell(actorId, "metadata-blocker");
      await waitFor(() => runs.length === before + 1);
    },
    reopen: async (between?: () => void) => {
      const before = runs.length;
      await actors.close();
      between?.();
      open();
      await waitFor(() => runs.length === before + 1);
      return actors;
    },
    replayFile: (actorId: string) => path.join(root, "actors", actorId, "dead-letter.jsonl"),
    finish: async (actorId: string) => {
      const before = runs.length;
      release();
      await waitFor(() => runs.length === before + 1 && actors.status(actorId).status === "idle");
      return runs[before]!;
    },
  };
};
const create = (actors: ActorManager) => actors.create({ name: "metadata-reviewer", instructions: "Review.",
  topics: [TOPIC], coalesce: false, coalesceKey: "payload.number" });
const publish = (mesh: MeshStore, index: number, extra: Partial<Parameters<MeshStore["publish"]>[0]> = {}) =>
  mesh.publish({ topic: TOPIC, kind: "github.webhook", from, sender: { authority: "host" }, principal,
    data: { event: "issue_comment", payload: { number: 7, action: "created", index } }, ...extra });

describe("coalesced metadata admission bounds (smarty-dev#4605 P2-3)", () => {
  it("ADV4a: a 600-event burst checkpoints and prompts only the newest 100 plus an untrusted 500-event range", async () => {
    const w = world();
    const actors = w.open();
    const actor = await create(actors);
    await w.block(actor.id);
    const events: MeshEvent[] = [];
    for (let index = 0; index < 600; index++) events.push(await publish(w.mesh, index));
    const started = performance.now();
    w.admit(...events);
    const admissionMs = performance.now() - started;
    expect(actors.status(actor.id).queued).toBe(1);
    const checkpoint = w.checkpoint(actor.id);
    const run = await w.finish(actor.id);
    const input = frame(run.task);
    // Emit measurements before regression assertions, so the parent's red-base
    // run gets the same admission/queue/prompt evidence as the green candidate.
    console.info("ADV4a600", JSON.stringify({ admissionMs, queueBytes: checkpoint.bytes,
      promptBytes: Buffer.byteLength(run.task), metadataBytes: metadataBytes(input.coalesced), records: input.coalesced.length }));
    expect(checkpoint.item.coalesced.map(record => record.id)).toEqual(events.slice(-100).map(event => event.id));
    const summary = { count: 500, firstSequence: events[0]!.sequence, lastSequence: events[499]!.sequence };
    expect(checkpoint.item.coalesced[0]).toMatchObject({ omittedBefore: summary, untrusted: true });
    expect(checkpoint.item.coalesced[0]!.omittedBefore).not.toHaveProperty("countIsLowerBound");
    expect(checkpoint.item.coalesced[0]).not.toHaveProperty("sender");
    expect(checkpoint.item.coalesced[0]).not.toHaveProperty("principal");
    expectBounds(checkpoint.item.coalesced);
    expect(input.payload).toEqual(events[599]);
    expect(input.coalesced.map(record => record.id)).toEqual(events.slice(-100).map(event => event.id));
    expect(input.coalesced[0]).toMatchObject({ omittedBefore: summary, untrusted: true });
    expect(input.untrusted).toBe(true);
    expect(run.provenance).not.toHaveProperty("principal");
    expect(run.task).toContain("UNTRUSTED");
    expect(run.task).toContain("mesh.read");
    expect(run.task).toContain("500");
    expectBounds(input.coalesced);
    expectFullEventBounds(input);
  }, 60_000);

  it.each(["event", "action"] as const)("ADV4b: twelve 40-KiB %s strings are bounded, marked and cannot carry principal authority", async field => {
    const w = world();
    const actors = w.open();
    const actor = await create(actors);
    await w.block(actor.id);
    const text = "host-trusted-looking-prefix:" + "x".repeat(40 * 1024 - 28);
    const events: MeshEvent[] = [];
    for (let index = 0; index < 12; index++) events.push(await publish(w.mesh, index, {
      data: { event: field === "event" ? text : "issue_comment",
        payload: { number: 7, action: field === "action" ? text : "created", index } },
    }));
    w.admit(...events);
    const checkpoint = w.checkpoint(actor.id);
    const run = await w.finish(actor.id);
    const input = frame(run.task);
    console.info(`ADV4b12x40KiB-${field}`, JSON.stringify({ queueBytes: checkpoint.bytes,
      promptBytes: Buffer.byteLength(run.task), metadataBytes: metadataBytes(input.coalesced) }));
    expect(checkpoint.item.coalesced).toHaveLength(12);
    for (const record of checkpoint.item.coalesced) {
      expect(record[field]!.length).toBeLessThanOrEqual(256);
      expect(record[field]).toContain("[truncated]");
      expect(record).toMatchObject({ metadataTruncated: true, untrusted: true, dataOmitted: true });
      expect(record).not.toHaveProperty("sender");
      expect(record).not.toHaveProperty("principal");
      expect(record).not.toHaveProperty("payload");
    }
    expect(input.payload).toEqual(events[11]);
    expectBounds(input.coalesced);
    expect(run.provenance).not.toHaveProperty("principal");
    expect(input.untrusted).toBe(true);
    expect(Buffer.byteLength(run.task)).toBeLessThan(96 * 1024);
  }, 30_000);

  it("keeps an oversized primary unchanged and gives history no full-event budget", async () => {
    const w = world();
    const actors = w.open();
    const actor = await create(actors);
    await w.block(actor.id);
    const older = await publish(w.mesh, 0);
    const newest = await publish(w.mesh, 1, { data: { event: "issue_comment",
      payload: { number: 7, action: "created", body: "x".repeat(80 * 1024) } } });
    w.admit(older, newest);
    const checkpoint = w.checkpoint(actor.id);
    expect(checkpoint.item.payload).toEqual(newest);
    expect(checkpoint.item.coalesced).toHaveLength(2);
    expect(checkpoint.item.coalesced.every(record => record.dataOmitted && !record.payload)).toBe(true);
    const input = frame((await w.finish(actor.id)).task);
    expect(input.payload).toEqual(newest);
    expect(input.coalesced.every(record => record.dataOmitted && !record.payload)).toBe(true);
    expectBounds(input.coalesced);
  }, 30_000);

  it("caps total rendered metadata even when every individual attribution string fits", async () => {
    const w = world();
    const actors = w.open();
    const actor = await create(actors);
    await w.block(actor.id);
    const sender: FabricMessageSender = { authority: "scope", principalId: "owner", digest: "d".repeat(64),
      grants: Array.from({ length: 8 }, (_, index) => ({ resource: `mesh:${index}:${"r".repeat(180)}`, actions: ["read"] })) };
    const events: MeshEvent[] = [];
    for (let index = 0; index < 100; index++) events.push(await publish(w.mesh, index, { sender,
      from: { ...from, name: "f".repeat(200) } }));
    w.admit(...events);
    const checkpoint = w.checkpoint(actor.id);
    const retained = checkpoint.item.coalesced;
    expect(retained.length).toBeGreaterThan(1);
    expect(retained.length).toBeLessThan(100);
    expect(retained.map(record => record.id)).toEqual(events.slice(-retained.length).map(event => event.id));
    expect(retained[0]!.omittedBefore).toEqual({ count: 100 - retained.length,
      firstSequence: events[0]!.sequence, lastSequence: events[99 - retained.length]!.sequence });
    expectBounds(retained);
    const input = frame((await w.finish(actor.id)).task);
    expectBounds(input.coalesced);
    expect(input.payload).toEqual(events[99]);
    expectFullEventBounds(input);
  }, 30_000);

  it("does not launder oversized nested authority or fabricate id/topic read-back identities", async () => {
    const w = world();
    const actors = w.open();
    const actor = await create(actors);
    await w.block(actor.id);
    const events = [await publish(w.mesh, 0), await publish(w.mesh, 1)];
    // Legacy/bridge envelopes can carry attribution larger than today's producer
    // limits. Drive those genuine envelope fields through real queue admission.
    events[0]!.from = { ...from, name: "f".repeat(10_000) };
    events[0]!.sender = { authority: "scope", principalId: "looks-trusted", digest: "d".repeat(64),
      grants: Array.from({ length: 80 }, () => ({ resource: "mesh:*", actions: ["read"] })) };
    events[0]!.principal = { id: "principal-prefix" + "p".repeat(10_000), binding: "org-agent" };
    events[0]!.id = "valid-looking-id-prefix" + "i".repeat(10_000);
    w.admit(...events);
    const checkpoint = w.checkpoint(actor.id);
    const first = checkpoint.item.coalesced[0]!;
    expect(first).toMatchObject({ metadataTruncated: true, untrusted: true, dataOmitted: true });
    expect(first.id).toContain("event id omitted");
    expect(first.id).not.toBe(events[0]!.id.slice(0, 256));
    expect(first).not.toHaveProperty("sender");
    expect(first).not.toHaveProperty("principal");
    expect(first).not.toHaveProperty("from");
    expectBounds(checkpoint.item.coalesced);
    const run = await w.finish(actor.id);
    expect(frame(run.task).payload).toEqual(events[1]);
    expect(run.provenance).not.toHaveProperty("principal");
    // Topic cannot exceed 256 on the actor's subscribed ordinary route; the
    // supplementary restore probe covers persisted oversized identity fields.
    const restored = restoreCoalescedMeshEvents({ payload: events[1], coalesced: [{
      ...first, topic: "valid-looking-topic-prefix" + "t".repeat(10_000),
    }] })!;
    expect(restored[0]!.topic).toContain("topic omitted");
    expect(restored[0]!.topic).not.toBe("valid-looking-topic-prefix" + "t".repeat(230));
    expectBounds(restored);
  }, 30_000);

  it("persists summary trust across restart, duplicate dead-letter merge and subsequent accumulation", async () => {
    const w = world();
    let actors = w.open();
    const actor = await create(actors);
    await w.block(actor.id);
    const events: MeshEvent[] = [];
    for (let index = 0; index < 120; index++) events.push(await publish(w.mesh, index,
      index === 0 ? { origin: "external", untrusted: true } : {}));
    w.admit(...events);
    const saved = w.checkpoint(actor.id).item;
    expect(saved.coalesced[0]!.omittedBefore?.count).toBe(20);
    actors = await w.reopen(() => fs.writeFileSync(w.replayFile(actor.id), `${JSON.stringify({ at: Date.now(),
      source: `mesh:${TOPIC}`, event: saved.payload, coalesced: saved.coalesced })}\n`));
    await waitFor(() => !fs.existsSync(w.replayFile(actor.id)) || !fs.readFileSync(w.replayFile(actor.id), "utf8").trim());
    expect(w.checkpoint(actor.id).item.coalesced[0]!.omittedBefore).toEqual(saved.coalesced[0]!.omittedBefore);
    const next = await publish(w.mesh, 120);
    w.admit(next);
    expect(w.checkpoint(actor.id).item.coalesced[0]!.omittedBefore).toEqual({ count: 21,
      firstSequence: events[0]!.sequence, lastSequence: events[20]!.sequence });
    actors = await w.reopen();
    expect(actors.status(actor.id).queued).toBe(1);
    const checkpoint = w.checkpoint(actor.id).item;
    expect(checkpoint.coalesced[0]!.omittedBefore?.count).toBe(21);
    expect(checkpoint.coalesced[0]!.untrusted).toBe(true);
    expect(checkpoint.coalesced[0]).not.toHaveProperty("sender");
    expect(checkpoint.coalesced[0]).not.toHaveProperty("principal");
    const run = await w.finish(actor.id);
    const input = frame(run.task);
    expect(input.payload).toEqual(next);
    expect(input.coalesced[0]!.omittedBefore?.count).toBe(21);
    expect(input.untrusted).toBe(true);
    expect(run.task).toContain("UNTRUSTED external input");
    expect(run.provenance).not.toHaveProperty("principal");
    expectBounds(input.coalesced);
  }, 45_000);

  it("supplement: equal sparse ranges are exact only for an identical represented checkpoint", () => {
    const event = (sequence: number): MeshEvent => ({ id: `event-${sequence}`, sequence, topic: TOPIC, kind: "event", from, createdAt: 0 });
    const summary = { count: 80, firstSequence: 1, lastSequence: 100 };
    const a = { payload: event(200), coalesced: [{ ...event(200), omittedBefore: summary }] };
    const copy = structuredClone(a);
    const identical = mergeCoalescedMeshEvents(a.payload, a, copy);
    expect(identical[0]!.omittedBefore).toEqual(summary);
    const divergent = { payload: event(201), coalesced: [{ ...event(201), omittedBefore: summary }] };
    const merged = mergeCoalescedMeshEvents(divergent.payload, a, divergent);
    expect(merged[0]!.omittedBefore).toEqual({ ...summary, countIsLowerBound: true });
  });

  it("supplement: overlapping summarized sparse histories merge idempotently without claiming duplicate identities", () => {
    const event = (sequence: number): MeshEvent => ({ id: `event-${sequence}`, sequence, topic: TOPIC, kind: "event", from, createdAt: 0 });
    const a = { payload: event(200), coalesced: [{ ...event(200), omittedBefore: { count: 80, firstSequence: 1, lastSequence: 100 } }] };
    const b = { payload: event(201), coalesced: [{ ...event(201), omittedBefore: { count: 80, firstSequence: 50, lastSequence: 149 } }] };
    const once = mergeCoalescedMeshEvents(b.payload, a, b);
    const summary = once[0]!.omittedBefore!;
    expect(summary).toEqual({ count: 109, firstSequence: 1, lastSequence: 149, countIsLowerBound: true });
    const twice = mergeCoalescedMeshEvents(b.payload, { payload: b.payload, coalesced: once }, a, b);
    expect(twice[0]!.omittedBefore).toEqual(summary);
    expect(twice[0]!.untrusted).toBe(true);
    expect(twice[0]).not.toHaveProperty("sender");
  });
});
