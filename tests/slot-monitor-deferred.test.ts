import fs, { type FSWatcher } from "node:fs";
import { EventEmitter } from "node:events";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import type { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

// Keep: #7991 R1-1 data safety. Real directory, monitor, cursor, mesh archive,
// registry and queue; only native admission authority is a deterministic double.
// Activation is paused throughout. No worker or product deletion/retention runs.
const native = vi.hoisted(() => ({ authority: undefined as unknown }));
vi.mock("../src/actors/slot-admission.js", () => ({ slotAdmissionFor: () => native.authority }));
const topic = "github.demo.pulls";
const identity = { id: "session:alpha", name: "main", kind: "main" as const, sessionId: "alpha" };
const from = { id: "session:peer", name: "peer", kind: "main" as const };
type Intent = { actorId: string; state: "admitted"; registryRoot: string; spec: Record<string, unknown> };
let intents: Intent[], readable: boolean, recovering: (() => Promise<void>) | undefined;
const roots: string[] = [], directories: ActorDirectory[] = [], meshes: MeshStore[] = [], monitors: ActorMeshMonitor[] = [];
const flush = async () => { for (let n = 0; n < 32; n++) await Promise.resolve(); };
const deferred = () => { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; };

beforeEach(() => {
  vi.useFakeTimers();
  // Delivery is explicitly scheduled; don't let native watch edges split a page
  // while publishing the deterministic prefix/held/suffix fixture.
  vi.spyOn(fs, "watch").mockImplementation(() => {
    const watcher = Object.assign(new EventEmitter(), { close() { watcher.emit("close"); } });
    return watcher as unknown as FSWatcher;
  });
  intents = []; readable = true; recovering = undefined;
  const enforced = (topics: readonly string[]) => topics.includes(topic);
  const current = (id: string, topics: readonly string[], root: string) => !enforced(topics) ||
    (readable && intents.some(intent => intent.actorId === id && intent.registryRoot === root));
  native.authority = {
    requiresEnforcement: enforced, requiresRecovery: () => true,
    admit: async () => undefined, // this suite only creates ordinary actors
    isCurrent: current,
    withCurrent: <T>(id: string, topics: readonly string[], root: string, operation: () => T): T => {
      if (!current(id, topics, root)) throw new Error("native authority unavailable");
      return operation();
    },
    recoverable: async (root: string) => {
      await recovering?.();
      if (!readable) throw new Error("native authority unavailable");
      return intents.filter(intent => intent.registryRoot === root).map(intent => structuredClone(intent));
    },
  };
  const start = ActorMeshMonitor.prototype.start;
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) {
    monitors.push(this); start.call(this);
  });
});
afterEach(async () => {
  // close sets the manager's closing fence BEFORE releasing fake setImmediate:
  // the initial retention slice exits without touching any actor/archive.
  const closing = directories.splice(0).map(directory => directory.close());
  for (const monitor of monitors.splice(0)) monitor.close();
  await vi.advanceTimersByTimeAsync(1);
  await Promise.all(closing);
  for (const mesh of meshes.splice(0)) mesh.closeState();
  vi.restoreAllMocks(); vi.useRealTimers();
  for (const root of roots.splice(0)) {
    expect(root.startsWith(path.resolve(process.env.TMPDIR!) + path.sep)).toBe(true);
    fs.rmSync(root, { recursive: true, force: true });
  }
});

async function fixture(options: { replayAge?: number; archive?: boolean } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "slot-monitor-deferred-")); roots.push(root);
  const meshRoot = path.join(root, "mesh"); fs.mkdirSync(meshRoot);
  if (options.archive) {
    const dir = path.join(root, "archive"); fs.mkdirSync(dir);
    fs.writeFileSync(path.join(meshRoot, "event-archive.json"), JSON.stringify({ version: 1, dir }));
  }
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100); meshes.push(mesh);
  const actorRoots = { project: path.join(root, "actors"), session: path.join(root, "actors", "alpha") };
  const id = "a".repeat(32);
  const intent: Intent = { actorId: id, state: "admitted", registryRoot: actorRoots.project, spec: {
    id, name: "protected-review", rootId: identity.id, instructions: "Review", topics: [topic],
    status: "idle", events: [], delivery: "mailbox", responseMode: "text", triggerTurn: false,
    coalesce: false, residency: "session", runner: "pi", requirements: [], messages: [], createdAt: 1, updatedAt: 1,
    sessionFile: path.join(actorRoots.project, id, "session.jsonl"),
  } };
  intents.push(intent);
  const cursorPath = path.join(root, "cursor.json");
  // Saved Main cursor is already caught up, not a first-start/latest-tail escape.
  const anchor = await mesh.publish({ topic: "ordinary.anchor", from });
  fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, ...mesh.latestCursor() }));
  const run = vi.fn(() => { throw new Error("Worker execution forbidden in this suite"); });
  const agents = { config: DEFAULT_FABRIC_CONFIG.agents, resolveKernel: () => undefined, defaultModel: () => undefined,
    assertModelAllowed: () => {}, run, actorArchiveSources: () => new Map(), close: async () => {} } as unknown as AgentManager;
  const directory = new ActorDirectory(["alpha", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 60_000 }, agents, () => {}, {
    persistent: true, rootId: identity.id, claimResidency: "session", releasePaused: true,
    meshCursorPath: cursorPath, ...(options.replayAge === undefined ? {} : { meshReplayAgeMs: options.replayAge }),
    meshRetentionSweepPath: false, reapDeadSessionPresence: false,
  }], actorRoots, "project");
  directories.push(directory);
  await flush();
  const primary = monitors[monitors.length - 2]!;
  expect(primary.caughtUp()).toBe(true);
  const cursor = () => JSON.parse(fs.readFileSync(`${cursorPath}.project`, "utf8")) as { cursor: number; last: { id: string; sequence: number } };
  const poll = async () => { primary.schedule(); await flush(); };
  const queue = () => fs.readdirSync(path.join(actorRoots.project, id)).filter(name => name.startsWith("queue-"))
    .map(name => fs.readFileSync(path.join(actorRoots.project, id, name), "utf8")).join("\n");
  return { mesh, directory, primary, root, cursor, poll, queue, run, anchor, id };
}

// Native protected-slot support is Linux-bound; the pure monitor counterexample
// below intentionally remains cross-platform.
describe.runIf(process.platform === "linux")("protected mesh input retains its real monitor boundary", () => {
  it.each(["topic", "id", "name"] as const)("holds live non-fleet %s input during pending bootstrap, then wakes and durably accepts", async address => {
    const s = await fixture();
    const gate = deferred(), entered = deferred();
    recovering = async () => { entered.resolve(); await gate.promise; };
    const recovery = s.directory.recoverProtectedActors();
    await entered.promise;
    const before = s.cursor();
    const input = await s.mesh.publish({ topic: address === "topic" ? topic : "ordinary.direct", from,
      ...(address === "topic" ? {} : { to: address === "id" ? s.id : "protected-review" }) });
    await s.poll();
    s.primary.checkpointForRelease();
    expect(s.cursor(), "protected input must not advance the caught-up Main cursor").toEqual(before);
    expect(s.primary.caughtUp()).toBe(false);
    gate.resolve(); await recovery; await flush(); // onReady must wake the actual monitor
    expect(s.queue()).toContain(input.id);
    expect(s.cursor().last.id).toBe(input.id);
    expect(s.run).not.toHaveBeenCalled();
  });

  it("holds expired ledger-only input before enumeration and accepts it on the bootstrap wake", async () => {
    const s = await fixture({ replayAge: -60_000 });
    const before = s.cursor();
    const input = await s.mesh.publish({ topic: "ordinary.direct", to: s.id, from });
    await s.poll(); s.primary.checkpointForRelease();
    expect(s.cursor()).toEqual(before);
    await s.directory.recoverProtectedActors(); await flush();
    expect(s.queue()).toContain(input.id); expect(s.cursor().last.id).toBe(input.id);
    expect(s.run).not.toHaveBeenCalled();
  });

  it("holds live protected input through authority failure and failed reconciliation, then retries without losing queued work", async () => {
    const s = await fixture(); await s.directory.recoverProtectedActors(); await s.poll();
    const first = await s.mesh.publish({ topic, from }); await s.poll();
    expect(s.queue()).toContain(first.id);
    const before = s.cursor(); readable = false;
    const input = await s.mesh.publish({ topic: "ordinary.direct", to: s.id, from }); await s.poll();
    s.primary.checkpointForRelease();
    expect(s.cursor(), "authority loss must retain protected input").toEqual(before);
    await expect(s.directory.recoverProtectedActors()).rejects.toMatchObject({ code: "FABRIC_ACTOR_SLOT_RECOVERY_PARKED" });
    await s.poll(); s.primary.checkpointForRelease(); expect(s.cursor()).toEqual(before);
    readable = true; await s.directory.recoverProtectedActors(); await flush();
    expect(s.queue()).toContain(first.id); expect(s.queue()).toContain(input.id);
    expect(s.cursor().last.id).toBe(input.id); expect(s.run).not.toHaveBeenCalled();
  });

  it("checkpoints only a consumed same-page prefix ahead of deferred protected input", async () => {
    const s = await fixture(); await s.directory.recoverProtectedActors(); await s.poll();
    const ordinary = await s.directory.create({ name: "ordinary", instructions: "Observe", topics: ["ordinary.prefix"] });
    await s.poll(); readable = false;
    const prefix = await s.mesh.publish({ topic: "ordinary.prefix", from });
    const held = await s.mesh.publish({ topic, from });
    const suffix = await s.mesh.publish({ topic: "ordinary.prefix", from }); await s.poll();
    expect(s.cursor().last.id, "partial page must stop at the accepted ordinary prefix").toBe(prefix.id);
    expect(s.directory.messages(ordinary.id).filter(message => message.direction === "in").map(message => (message.data as MeshEvent).id)).toEqual([prefix.id]);
    readable = true; await s.directory.recoverProtectedActors(); await flush();
    expect(s.queue()).toContain(held.id); expect(s.cursor().last.id).toBe(suffix.id);
    expect(s.directory.messages(ordinary.id).filter(message => message.direction === "in").map(message => (message.data as MeshEvent).id)).toEqual([prefix.id, suffix.id]);
    expect(s.run).not.toHaveBeenCalled();
  });

  it.each(["topic", "address"] as const)("replays protected %s input older than the age window, but still skips ordinary input", async address => {
    const s = await fixture({ replayAge: -60_000 });
    await s.directory.recoverProtectedActors(); await flush();
    const ordinary = await s.directory.create({ name: "ordinary", instructions: "Observe", topics: ["ordinary.expired"] });
    const old = await s.mesh.publish({ topic: "ordinary.expired", from });
    const input = await s.mesh.publish({ topic: address === "topic" ? topic : "ordinary.direct", from,
      ...(address === "address" ? { to: s.id } : {}) });
    await s.poll();
    expect(s.queue(), "protected input must bypass replay-age expiry").toContain(input.id);
    expect(s.directory.messages(ordinary.id).some(message => (message.data as MeshEvent)?.id === old.id)).toBe(false);
    expect(s.run).not.toHaveBeenCalled();
  });

  it("recovers protected non-fleet input from archive after rewrite, without replaying ordinary archived input", async () => {
    const s = await fixture({ archive: true });
    await s.directory.recoverProtectedActors(); await s.poll();
    const ordinary = await s.directory.create({ name: "ordinary", instructions: "Observe", topics: ["ordinary.archived"] });
    await s.poll(); s.primary.checkpointForRelease(); const before = s.cursor();
    const input = await s.mesh.publish({ topic: "ordinary.direct", to: s.id, from });
    const ignored = await s.mesh.publish({ topic: "ordinary.archived", from });
    const retained = await s.mesh.publish({ topic: "ordinary.retained", from });
    const live = path.join(s.mesh.root, "events.jsonl");
    const suffix = fs.readFileSync(live, "utf8").split("\n").filter(line => line && JSON.parse(line).sequence >= retained.sequence);
    fs.writeFileSync(live, suffix.join("\n") + "\n"); fs.writeFileSync(path.join(s.mesh.root, "generation"), "1");
    readable = false; await s.poll(); await s.poll();
    s.primary.checkpointForRelease(); expect(s.cursor().last).toEqual(before.last);
    readable = true; await s.directory.recoverProtectedActors(); await flush(); await s.poll();
    expect(s.queue(), "archive selection must include protected addressed non-fleet input").toContain(input.id);
    expect(s.directory.messages(ordinary.id).some(message => (message.data as MeshEvent)?.id === ignored.id)).toBe(false);
    expect(s.run).not.toHaveBeenCalled();
  });
});

// Counterexample: the distinct hold must NOT reinterpret ordinary live false.
describe("ordinary monitor backpressure remains unchanged", () => {
  it.each(["ordinary.full", "fleet.full"])("preserves false handling for %s", async eventTopic => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "slot-monitor-counterexample-")); roots.push(root);
    const cursorPath = path.join(root, "cursor.json");
    const event = { id: "e1", sequence: 1, topic: eventTopic, createdAt: Date.now() } as MeshEvent;
    const monitor = new ActorMeshMonitor({ root, latestOffset: () => 0,
      tail: offset => offset === 0 ? { events: [event], nextOffset: 10, cursors: [10] } : { events: [], nextOffset: 10 } },
    DEFAULT_FABRIC_CONFIG.mesh, { cursorPath, beforePoll: () => true, onEvent: () => false });
    monitors.push(monitor); monitor.start(); await flush(); monitor.checkpointForRelease();
    expect(JSON.parse(fs.readFileSync(cursorPath, "utf8")).cursor).toBe(eventTopic.startsWith("fleet.") ? 0 : 10);
  });
});
