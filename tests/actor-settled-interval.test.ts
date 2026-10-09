import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import * as atomicWrite from "../src/core/atomic-write.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  await Promise.all(closers.splice(0).map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const setup = (root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-settled-interval-")), owns?: () => boolean,
  options: { releasePaused?: boolean; canConsumeMesh?: () => boolean } = {}) => {
  if (!roots.includes(root)) roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true, ...options,
    ...(owns ? { canManageActor: owns } : {}),
  });
  const close = async () => { await actors.close(); await agents.close(); };
  closers.push(close);
  return { root, mesh, agents, actors, close };
};
// Genuine FabricRuntimeState host envelope: signal.payload.source describes an input's origin,
// never the identity of the observed agent. The real identity is session.id.
const payload = (sessionId = "source-a", origin = "extension", event = "agent_settled") => ({
  event, session: { id: sessionId, cwd: process.cwd() }, digest: {}, transcript: [],
  signal: { payload: { type: event, source: origin }, idle: true, observedAt: 123 },
});
const incoming = (actors: ActorManager, id: string, source = "host:agent_settled") =>
  actors.messages(id).filter((message) => message.direction === "in" && message.source === source);
const clock = () => {
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  return { set: (value: number) => { now = value; }, advance: (ms: number) => { now += ms; } };
};
const waitFor = async (predicate: () => boolean) => {
  const deadline = performance.now() + 10_000;
  while (!predicate()) {
    if (performance.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

describe("actor settled round 3 event-driven regressions", () => {
  // Real queue/overflow and atomic queue files; disable only the monitor's autonomous
  // polling so a timer or beforePoll cannot masquerade as a dequeue/ownership wake.
  const fixture = async (owns?: () => boolean, coalesce = false) => {
    let monitor!: ActorMeshMonitor;
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
    vi.spyOn(ActorMeshMonitor.prototype, "schedule").mockImplementation(() => {});
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-settled-r3-"));
    roots.push(root);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const actors = new ActorManager("test", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: 1 }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: true, releasePaused: true, closeGraceMs: 0,
        ...(owns ? { canManageActor: owns } : {}),
      });
    const close = async () => { await actors.close(); await agents.close(); };
    closers.push(close);
    const actor = await actors.create({ name: "regression", instructions: "Observe.", events: ["agent_settled"], coalesce, activation: { minIntervalMs: 1_000 } });
    // Bootstrap readiness is set before any pending work; it is not the recovery event.
    actors.resumeQueued();
    await Promise.resolve();
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    vi.setSystemTime(10_000);
    const directory = path.join(root, "actors", actor.id);
    const queueFile = () => path.join(directory, fs.readdirSync(directory).find(file => file.startsWith("queue-"))!);
    const markers = () => incoming(actors, actor.id).filter(message => !message.reason)
      .map(message => (message.data as { marker?: string }).marker);
    return { root, actors, agents, actor, close, monitor, queueFile, markers };
  };

  it("r3: full queue and overflow defer without a retry timer and actual dequeue delivers latest once", async () => {
    const { actors, actor, monitor, markers } = await fixture();
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    for (let i = 0; i < 8; i++) actors.tell(actor.id, "filler", { index: i });
    expect(actors.status(actor.id).queued).toBe(9); // limit 1 plus overflow 8.
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "superseded" });
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    vi.advanceTimersByTime(900); // The normal original deadline must run first.
    expect(markers()).toEqual(["leading"]);
    expect.soft(scheduled.mock.calls, "full admission must not create a retry timer").toHaveLength(0);
    await actors.setActivationFilter(actor.id, [
      { id: "drain-leading", source: ["host:agent_settled"], where: [{ path: "marker", equals: "leading" }] },
      { id: "drain-fillers", source: ["direct"] },
    ]);
    actors.resumeAfterRelease(); // Real #drain shifts/refills the full FIFO, not a poll.
    expect(actors.status(actor.id).filterSkipped.count).toBeGreaterThanOrEqual(1); // The leading item was actually dequeued.
    actors.pauseForRelease(); // Keep workers gated after that real slot-freeing step.
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(markers()).toEqual(["leading", "latest"]);
    actors.resumeAfterRelease();
    monitor.callbacks.beforePoll(); // Explicit production poll callback only AFTER recovery.
    vi.advanceTimersByTime(20_000);
    actors.pauseForRelease();
    expect(markers()).toEqual(["leading", "latest"]);
  });

  it("r3: ownership-ready refresh admits overdue latest without a timer or poll callback", async () => {
    let owned = true;
    const { actors, actor, markers } = await fixture(() => owned);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    const deadlineCallback = scheduled.mock.calls.find(([, delay]) => delay === 900)![0];
    owned = false;
    vi.advanceTimersByTime(900); // Allow the original deadline and its ownership veto.
    expect(markers()).toEqual(["leading"]);
    scheduled.mockClear();
    owned = true;
    expect(actors.listOwned().map(value => value.id)).toContain(actor.id); // Actual false -> true ownership refresh.
    await Promise.resolve(); // Normal restore microtask, but no resumeQueued or timer execution.
    // Registry reload may schedule an unrelated save debounce. Fingerprint the
    // actual deadline callback captured above, rather than counting all host timers.
    expect.soft(scheduled.mock.calls.filter(([callback]) => String(callback) === String(deadlineCallback)),
      "reacquisition must not schedule a new trailing timer").toHaveLength(0);
    expect(markers()).toEqual(["leading", "latest"]);
    vi.advanceTimersByTime(20_000);
    expect(markers()).toEqual(["leading", "latest"]);
  });

  it("r3: failed pending save is checkpointed by the next settle and restores original latest deadline once", async () => {
    const first = await fixture();
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    const file = first.queueFile();
    const rename = fs.renameSync;
    const failed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) throw new Error("r3 pending checkpoint unavailable");
      return rename(from, to);
    });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "superseded" });
    expect(failed.mock.calls.some(([, to]) => to === file)).toBe(true);
    expect.soft(actors.status(actor.id).lastError, "failed pending checkpoint must be diagnosable").toEqual(expect.stringMatching(/r3 pending checkpoint unavailable/));
    expect(JSON.parse(fs.readFileSync(file, "utf8")).settledWindows).toBeUndefined();
    failed.mockRestore();
    vi.advanceTimersByTime(100);
    const latest = { ...payload(), marker: "latest" };
    actors.dispatchHostEvent("agent_settled", latest, [{ type: "image", data: "r3-image", mimeType: "image/png" }]);
    latest.marker = "mutated";
    const saved = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(saved.settledWindows[0]).toMatchObject({ acceptedAt: 10_000, intervalMs: 1_000, pending: { payload: { marker: "latest" }, images: [{ data: "r3-image" }] } });
    await first.close();
    const second = setup(first.root, undefined, { releasePaused: true });
    second.actors.resumeQueued();
    await Promise.resolve();
    vi.advanceTimersByTime(799);
    expect(incoming(second.actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(incoming(second.actors, actor.id).map(message => (message.data as { marker: string }).marker)).toEqual(["leading", "latest"]);
    expect(JSON.parse(fs.readFileSync(file, "utf8")).items.at(-1).images).toEqual([{ type: "image", data: "r3-image", mimeType: "image/png" }]);
    vi.advanceTimersByTime(20_000);
    expect(incoming(second.actors, actor.id)).toHaveLength(2);
  });

  it("r3: close makes a final save of unsaved pending before suspension and restart delivers it once", async () => {
    const first = await fixture();
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    const file = first.queueFile();
    const rename = fs.renameSync;
    let unavailable = true;
    const candidates: unknown[] = [];
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) {
        candidates.push(JSON.parse(fs.readFileSync(from, "utf8")));
        if (unavailable) throw new Error("r3 pending save failed before close");
      }
      return rename(from, to);
    });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    expect(JSON.parse(fs.readFileSync(file, "utf8")).settledWindows).toBeUndefined();
    candidates.length = 0;
    unavailable = false;
    await first.close();
    expect.soft(candidates, "close must attempt one final pending checkpoint").toHaveLength(1);
    expect.soft(JSON.parse(fs.readFileSync(file, "utf8")).settledWindows).toEqual([
      expect.objectContaining({ acceptedAt: 10_000, intervalMs: 1_000, pending: expect.objectContaining({ payload: expect.objectContaining({ marker: "latest" }) }) }),
    ]);
    const second = setup(first.root, undefined, { releasePaused: true });
    second.actors.resumeQueued();
    await Promise.resolve();
    vi.advanceTimersByTime(899);
    expect(incoming(second.actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(incoming(second.actors, actor.id).map(message => (message.data as { marker: string }).marker)).toEqual(["leading", "latest"]);
    vi.advanceTimersByTime(20_000);
    expect(incoming(second.actors, actor.id)).toHaveLength(2);
  });

  it.each([false, true])("r3-transfer-red: failed pending and deadline transfer saves survive final close and repeated restart (coalesce %s)", async (coalesce) => {
    const first = await fixture(undefined, coalesce);
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    const file = first.queueFile();
    type Snapshot = {
      items: Array<{ source: string; payload: { marker?: string } }>;
      settledWindows?: Array<{ pending?: { payload: { marker?: string } } }>;
    };
    const readQueue = (): Snapshot => JSON.parse(fs.readFileSync(file, "utf8"));
    const markers = (snapshot: Snapshot) => snapshot.items.map(item => [item.source, item.payload.marker]);
    const latestCount = (snapshot: Snapshot) => snapshot.items.filter(item =>
      item.source === "host:agent_settled" && item.payload.marker === "latest").length;
    const totalLatestCount = (snapshot: Snapshot) => latestCount(snapshot) +
      (snapshot.settledWindows?.filter(window => window.pending?.payload.marker === "latest").length ?? 0);
    const rename = fs.renameSync;
    let unavailable = true;
    const attempts: Snapshot[] = [];
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) {
        attempts.push(JSON.parse(fs.readFileSync(from, "utf8")));
        if (unavailable) throw new Error("r3 transfer checkpoint unavailable");
      }
      return rename(from, to);
    });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    expect(attempts.some(snapshot => snapshot.settledWindows?.some(window =>
      window.pending?.payload.marker === "latest"))).toBe(true); // Suppressed checkpoint really failed.
    expect(markers(readQueue())).toEqual([["host:agent_settled", "leading"]]);
    attempts.length = 0;
    vi.advanceTimersByTime(900); // Original 11,000 deadline, no incoming boundary event.
    expect(Date.now()).toBe(11_000);
    expect(attempts.some(snapshot => latestCount(snapshot) === 1 && !snapshot.settledWindows?.length),
      "otherwise available deadline admission must reach the atomic queue transfer").toBe(true);
    expect(markers(readQueue())).toEqual([["host:agent_settled", "leading"]]);
    const failedTransferAttempts = attempts.length;
    attempts.length = 0;
    unavailable = false; // The sink recovers BEFORE suspension, never by editing the queue.
    await first.close();
    const final = readQueue();
    const closeSaveAttempts = attempts.length;
    expect.soft(attempts, "close must make exactly one final save after the failed transfer").toHaveLength(1);
    expect.soft(attempts.filter(snapshot => totalLatestCount(snapshot) === 1),
      "the final atomic save must carry latest exactly once across queue and pending").toHaveLength(1);
    expect.soft(totalLatestCount(final), "final disk snapshot must retain latest after transfer failure").toBe(1);
    const expected = coalesce ? [["host:agent_settled", "latest"]]
      : [["host:agent_settled", "leading"], ["host:agent_settled", "latest"]];
    expect.soft(markers(final), "rollback must preserve leading; committed transfer keeps normal coalescing")
      .toEqual(final.settledWindows?.some(window => window.pending)
        ? [["host:agent_settled", "leading"]] : expected);

    const second = setup(first.root, undefined, { releasePaused: true });
    second.actors.resumeQueued(); // Real bootstrap admission; no private store/method mocks.
    await Promise.resolve();
    const restored = readQueue();
    expect.soft(markers(restored), "bootstrap restores latest exactly once, including its host source").toEqual(expected);
    expect.soft(second.actors.status(actor.id).queued, "restored queue count, not coalescing telemetry").toBe(expected.length);
    vi.advanceTimersByTime(20_000); // Normal postrestart timers must not duplicate transferred work.
    expect.soft(markers(readQueue()), "later timers leave restored queue unchanged").toEqual(expected);
    expect(readQueue().settledWindows?.filter(window => window.pending) ?? []).toHaveLength(0);
    await second.close();
    const third = setup(first.root, undefined, { releasePaused: true });
    third.actors.resumeQueued();
    await Promise.resolve();
    vi.advanceTimersByTime(20_000);
    expect.soft(markers(readQueue()), "repeated restart must neither lose nor duplicate latest").toEqual(expected);
    expect.soft(third.actors.status(actor.id).queued).toBe(expected.length);
    expect(readQueue().settledWindows?.filter(window => window.pending) ?? []).toHaveLength(0);
    console.info("r3-transfer-red evidence", JSON.stringify({ coalesce, failedTransferAttempts,
      closeSaveAttempts, finalLatestCount: totalLatestCount(final),
      finalMarkers: markers(final), restoredMarkers: markers(restored), repeatedRestartMarkers: markers(readQueue()) }));
  });

  it.each([false, true])("r3-transfer: coalesced rollback preserves every leading field and newest overdue payload (leading images %s)", async (withLeadingImages) => {
    const first = await fixture(undefined, true);
    const { actors, actor } = first;
    const image = { type: "image" as const, data: "leading-image", mimeType: "image/png" };
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" }, withLeadingImages ? [image] : []);
    const file = first.queueFile();
    const readQueue = () => JSON.parse(fs.readFileSync(file, "utf8"));
    const leading = readQueue().items[0];
    const rename = fs.renameSync;
    let unavailable = true;
    const attempts: ReturnType<typeof readQueue>[] = [];
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) {
        attempts.push(JSON.parse(fs.readFileSync(from, "utf8")));
        if (unavailable) throw new Error("r3 rollback write failure details");
      }
      return rename(from, to);
    });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "older" });
    vi.advanceTimersByTime(900);
    expect(actors.status(actor.id).lastError).toContain("r3 rollback write failure details");
    expect(readQueue().items).toEqual([leading]);
    attempts.length = 0;
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    const latest = { ...payload(), marker: "newest" };
    const latestImages = withLeadingImages ? [] : [{ ...image, data: "newest-image" }];
    expect(actors.dispatchHostEvent("agent_settled", latest, latestImages)).toBe(0);
    latest.marker = "caller-mutation";
    if (latestImages[0]) latestImages[0].data = "caller-mutation";
    expect(scheduled.mock.calls).toHaveLength(0);
    expect(attempts.at(-1).items[0].payload.marker).toBe("newest");
    expect(attempts.at(-1).settledWindows).toBeUndefined();
    unavailable = false;
    attempts.length = 0;
    await first.close();
    expect(attempts).toHaveLength(1);
    const final = readQueue();
    expect(final.items).toEqual([leading]); // Includes original id, activation, binding and image absence/presence.
    expect(final.latestActivationSequence).toBe(leading.activation.sequence);
    expect(final.settledWindows).toEqual([expect.objectContaining({ acceptedAt: 10_000, intervalMs: 1_000,
      pending: expect.objectContaining({ payload: expect.objectContaining({ marker: "newest" }),
        images: withLeadingImages ? [] : [{ ...image, data: "newest-image" }] }) })]);
    const second = setup(first.root, undefined, { releasePaused: true });
    second.actors.resumeQueued();
    await Promise.resolve();
    expect(readQueue().items).toHaveLength(1);
    expect(readQueue().items[0].payload.marker).toBe("newest");
    expect(readQueue().items[0].id).toBe(leading.id);
    expect(readQueue().items[0].images).toEqual(withLeadingImages ? undefined : [{ ...image, data: "newest-image" }]);
    expect(readQueue().settledWindows).toBeUndefined();
  });

  it("r3-transfer: failed transfer retries on actual dequeue only after the leading item is tracked", async () => {
    const { actors, actor, queueFile, markers } = await fixture();
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    const file = queueFile();
    const rename = fs.renameSync;
    const accepted: Array<{ items: Array<{ payload: { marker: string }; resumed?: boolean }>; settledWindows?: unknown[] }> = [];
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) {
        const snapshot = JSON.parse(fs.readFileSync(from, "utf8"));
        if (!snapshot.items.some((item: { payload: { marker: string }; resumed?: boolean }) =>
          item.payload.marker === "leading" && item.resumed)) throw new Error("r3 save waits for actual in-flight tracking");
        accepted.push(snapshot);
      }
      return rename(from, to);
    });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    vi.advanceTimersByTime(900);
    expect(markers()).toEqual(["leading"]);
    actors.resumeAfterRelease(); // Its pre-drain retry still fails, then the real dequeue frees the slot.
    actors.pauseForRelease();
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(markers()).toEqual(["leading", "latest"]);
    const transfer = accepted.find(snapshot => snapshot.items.some(item => item.payload.marker === "latest"));
    expect(transfer?.items.map(item => item.payload.marker)).toEqual(["leading", "latest"]);
    expect(transfer?.items[0]?.resumed).toBe(true);
    expect(transfer?.settledWindows).toBeUndefined();
    vi.advanceTimersByTime(20_000);
    expect(markers()).toEqual(["leading", "latest"]);
  });

  it("r3: failed final saves expose a diagnostic and retain latest in every attempted snapshot without cancellation", async () => {
    const first = await fixture();
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "saved-older" });
    const file = first.queueFile();
    const committed = fs.readFileSync(file, "utf8");
    const rename = fs.renameSync;
    const candidates: Array<{ settledWindows?: Array<{ pending: { payload: { marker: string } } }> }> = [];
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) {
        candidates.push(JSON.parse(fs.readFileSync(from, "utf8")));
        throw new Error("r3 final pending checkpoint unavailable");
      }
      return rename(from, to);
    });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    candidates.length = 0;
    await first.close();
    expect.soft(candidates.length, "final persistence must be attempted even when the sink remains unavailable").toBeGreaterThan(0);
    for (const candidate of candidates) {
      // The only allowed final snapshot still contains the latest in-memory
      // pending work, never a fabricated cancellation after the failed write.
      expect(candidate.settledWindows?.map(window => window.pending.payload.marker)).toEqual(["latest"]);
    }
    expect.soft(actors.status(actor.id).lastError, "exhausted pending persistence must remain diagnosable")
      .toEqual(expect.stringMatching(/r3 final pending checkpoint unavailable/));
    expect(fs.readFileSync(file, "utf8")).toBe(committed);
    vi.advanceTimersByTime(20_000);
    expect(first.markers()).toEqual(["leading"]); // Closed-manager timers cannot admit the retained work.
  });
});

describe("actor settled round 4 durability and cancellation regressions", () => {
  // Real ActorManager, queue serialization, registry and restart. Only autonomous
  // monitor polling is disabled; all admission/deadline callbacks are production code.
  const fixture = async (root?: string, actorId?: string) => {
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    vi.spyOn(ActorMeshMonitor.prototype, "schedule").mockImplementation(() => {});
    const directoryRoot = root ?? fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-settled-r4-"));
    if (!roots.includes(directoryRoot)) roots.push(directoryRoot);
    const mesh = new MeshStore(path.join(directoryRoot, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(directoryRoot, "runs"),
    });
    const actors = new ActorManager("test", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: 1 }, agents, () => {}, {
        actorRoot: path.join(directoryRoot, "actors"), persistent: true, releasePaused: true, closeGraceMs: 0,
      });
    const close = async () => { await actors.close(); await agents.close(); };
    closers.push(close);
    const actor = actorId ? actors.status(actorId) : await actors.create({
      name: "r4-regression", instructions: "Observe.", events: ["agent_settled"],
      coalesce: false, activation: { minIntervalMs: 1_000 },
    });
    actors.resumeQueued();
    await Promise.resolve();
    if (!root) {
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
      vi.setSystemTime(10_000);
    }
    const directory = path.join(directoryRoot, "actors", actor.id);
    const queueFile = () => path.join(directory, fs.readdirSync(directory).find(file => /^queue-.*\.json$/.test(file))!);
    const markers = () => incoming(actors, actor.id).filter(message => !message.reason)
      .map(message => (message.data as { marker?: string }).marker);
    return { root: directoryRoot, actors, agents, actor, close, queueFile, markers };
  };
  type QueueSnapshot = {
    items: Array<{ source: string; payload: { marker?: string } }>;
    settledWindows?: Array<{ pending?: { payload: { marker?: string } } }>;
  };
  const readQueue = (file: string): QueueSnapshot => JSON.parse(fs.readFileSync(file, "utf8"));
  const queuedMarkers = (file: string) => readQueue(file).items
    .filter(item => item.source === "host:agent_settled").map(item => item.payload.marker);
  const seedPending = (actors: ActorManager, id: string, marker = "latest") => {
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker });
  };

  it("r4: ordinary non-durable pending save is promoted at close and survives power loss exactly once", async () => {
    const first = await fixture();
    first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    const file = first.queueFile();
    // Establish a stable pre-crash baseline through the real core persistence API.
    // The simulated device retains only successful durable core writes thereafter;
    // rename alone makes bytes visible but does NOT make them survive power loss.
    let stable = fs.readFileSync(file, "utf8");
    atomicWrite.writeJsonAtomic(file, JSON.parse(stable), { durable: true });
    const write = atomicWrite.writeJsonAtomic;
    const writes: Array<{ durable: boolean; snapshot: QueueSnapshot }> = [];
    vi.spyOn(atomicWrite, "writeJsonAtomic").mockImplementation((target, value, options) => {
      write(target, value, options); // Real atomic writer, including fsync/rename.
      if (target === file) {
        writes.push({ durable: options?.durable === true, snapshot: JSON.parse(JSON.stringify(value)) });
        if (options?.durable) stable = fs.readFileSync(file, "utf8");
      }
    });
    vi.advanceTimersByTime(100);
    first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "superseded" });
    first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    expect(writes.at(-1)).toMatchObject({ durable: false, snapshot: {
      settledWindows: [{ pending: { payload: { marker: "latest" } } }],
    } });
    expect(readQueue(file).settledWindows?.[0]?.pending?.payload.marker).toBe("latest");
    writes.length = 0;
    await first.close();
    const closeWrites = writes.map(entry => ({
      durable: entry.durable, pending: entry.snapshot.settledWindows?.[0]?.pending?.payload.marker,
    }));
    expect.soft(writes.some(entry => entry.durable &&
      entry.snapshot.settledWindows?.[0]?.pending?.payload.marker === "latest"),
    "close owes a durable receipt even when an ordinary rename already succeeded").toBe(true);
    // Power loss discards unsynced replacements, not manager memory or queue fields.
    // Restoring device bytes is deliberately separate from ActorManager restart.
    fs.writeFileSync(file, stable);
    const second = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(899);
    expect(second.markers()).toEqual(["leading"]);
    vi.advanceTimersByTime(1);
    expect.soft(second.markers(), "latest must survive the simulated device crash").toEqual(["leading", "latest"]);
    expect.soft(queuedMarkers(file)).toEqual(["leading", "latest"]);
    vi.advanceTimersByTime(20_000);
    expect.soft(second.markers()).toEqual(["leading", "latest"]);
    await second.close();
    const third = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(20_000);
    expect.soft(queuedMarkers(file), "repeated restart must not duplicate latest").toEqual(["leading", "latest"]);
    expect.soft(third.actors.status(first.actor.id).queued).toBe(2);
    console.info("r4: power-loss evidence", JSON.stringify({ closeWrites, restoredMarkers: queuedMarkers(file) }));
  });

  it.each([false, true])("r4: failed halt queue rename fences cancelled latest across close/restart (fail close retries %s)", async (failCloseRetries) => {
    const first = await fixture();
    seedPending(first.actors, first.actor.id, "cancelled-latest");
    const file = first.queueFile();
    // A real, durable older snapshot exists before cancellation fails.
    atomicWrite.writeJsonAtomic(file, readQueue(file), { durable: true });
    const rename = fs.renameSync;
    let attempts = 0;
    let rejectWrites = true;
    const failed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) {
        attempts++;
        if (rejectWrites) {
          if (!failCloseRetries) rejectWrites = false;
          throw new Error("r4 halt queue rename unavailable");
        }
      }
      return rename(from, to);
    });
    first.actors.haltAll();
    expect(attempts, "inject the failure during halt, not during an earlier checkpoint").toBeGreaterThan(0);
    expect.soft(first.actors.status(first.actor.id).lastError,
      "failed cancellation must be diagnosable").toEqual(expect.stringMatching(/r4 halt queue rename unavailable/));
    await first.close(); // With failCloseRetries, every queue rename still fails here.
    expect.soft(first.actors.status(first.actor.id).lastError).toEqual(expect.stringMatching(/r4 halt queue rename unavailable/));
    failed.mockRestore();
    const second = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(20_000);
    expect.soft(second.markers(), "cancelled latest must never be admitted by the new manager")
      .not.toContain("cancelled-latest");
    expect.soft(queuedMarkers(file), "stale durable pending must not become runnable work")
      .not.toContain("cancelled-latest");
    // The sink has recovered. Cancellation must not permanently fence legitimate
    // future work, even when the previous manager had no successful queue rewrite.
    second.actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
    expect(second.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "recovered-leading" })).toBe(1);
    vi.advanceTimersByTime(100);
    second.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "recovered-latest" });
    vi.advanceTimersByTime(900);
    expect.soft(queuedMarkers(file).filter(marker => marker?.startsWith("recovered-")))
      .toEqual(["recovered-leading", "recovered-latest"]);
    await second.close();
    const third = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(20_000);
    expect.soft(third.markers(), "cancellation must survive repeated restart").not.toContain("cancelled-latest");
    expect.soft(queuedMarkers(file)).not.toContain("cancelled-latest");
    expect.soft(queuedMarkers(file).filter(marker => marker?.startsWith("recovered-")))
      .toEqual(["recovered-leading", "recovered-latest"]);
    console.info("r4: halt-failure evidence", JSON.stringify({ failCloseRetries, attempts,
      restoredMarkers: queuedMarkers(file) }));
  });

  it("r4: successful durable halt rewrite never replays pending after restart", async () => {
    const first = await fixture();
    seedPending(first.actors, first.actor.id, "cancelled-latest");
    const file = first.queueFile();
    const write = atomicWrite.writeJsonAtomic;
    const rewrites: Array<{ durable: boolean; snapshot: QueueSnapshot }> = [];
    vi.spyOn(atomicWrite, "writeJsonAtomic").mockImplementation((target, value, options) => {
      write(target, value, options);
      if (target === file) rewrites.push({ durable: options?.durable === true,
        snapshot: JSON.parse(JSON.stringify(value)) });
    });
    first.actors.haltAll();
    expect(rewrites.some(entry => entry.durable && !entry.snapshot.settledWindows?.length)).toBe(true);
    expect(readQueue(file).settledWindows).toBeUndefined();
    expect(first.actors.status(first.actor.id).lastError).toBeUndefined();
    await first.close();
    const second = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(20_000);
    expect(second.markers()).not.toContain("cancelled-latest");
    expect(queuedMarkers(file)).toEqual([]);
  });

  it("r4: counterexample without halt delivers successful trailing exactly once", async () => {
    const first = await fixture();
    seedPending(first.actors, first.actor.id);
    const file = first.queueFile();
    vi.advanceTimersByTime(899);
    expect(first.markers()).toEqual(["leading"]);
    vi.advanceTimersByTime(1);
    expect(first.markers()).toEqual(["leading", "latest"]);
    expect(queuedMarkers(file)).toEqual(["leading", "latest"]);
    expect(readQueue(file).settledWindows).toBeUndefined();
    vi.advanceTimersByTime(20_000);
    expect(first.markers()).toEqual(["leading", "latest"]);
    await first.close();
    const second = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(20_000);
    expect(queuedMarkers(file)).toEqual(["leading", "latest"]);
    expect(second.actors.status(first.actor.id).queued).toBe(2);
  });

  it("r4: counterexample input resume allows future new events without resurrecting halted pending", async () => {
    const first = await fixture();
    seedPending(first.actors, first.actor.id, "cancelled-latest");
    first.actors.haltAll();
    first.actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
    expect(first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "new-leading" })).toBe(1);
    vi.advanceTimersByTime(100);
    first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "new-latest" });
    vi.advanceTimersByTime(900);
    expect(first.markers()).toEqual(["leading", "new-leading", "new-latest"]);
    expect(queuedMarkers(first.queueFile())).toEqual(["new-leading", "new-latest"]);
    await first.close();
    const second = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(20_000);
    expect(second.markers()).not.toContain("cancelled-latest");
    expect(queuedMarkers(first.queueFile())).toEqual(["new-leading", "new-latest"]);
  });

  it.each(["persist", "close"] as const)("r4: total queue storage failure cannot acknowledge halt and recovery retries on %s", async (boundary) => {
    const first = await fixture();
    seedPending(first.actors, first.actor.id, "cancelled-latest");
    const file = first.queueFile();
    atomicWrite.writeJsonAtomic(file, readQueue(file), { durable: true });
    const open = fs.openSync;
    let unavailable = true;
    let fallbackAttempts = 0;
    vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) => {
      if (unavailable && (String(target).startsWith(`${file}.`) || (target === file && (flags === "r+" || (typeof flags === "number" && (flags & fs.constants.O_RDWR) !== 0))))) {
        if (target === file) fallbackAttempts++;
        throw new Error("r4 all queue storage unavailable");
      }
      return open(target, flags, mode);
    });
    const write = atomicWrite.writeJsonAtomic;
    const attempts: Array<{ durable: boolean; snapshot: QueueSnapshot }> = [];
    vi.spyOn(atomicWrite, "writeJsonAtomic").mockImplementation((target, value, options) => {
      if (target === file) attempts.push({ durable: options?.durable === true, snapshot: JSON.parse(JSON.stringify(value)) });
      write(target, value, options);
    });
    expect(() => first.actors.haltAll()).toThrow(/not durably acknowledged.*all queue storage unavailable/);
    expect(fallbackAttempts).toBe(1);
    expect(first.actors.halted).toBe(true);
    expect(first.actors.status(first.actor.id).lastError).toMatch(/all queue storage unavailable/);
    expect(first.actors.status(first.actor.id).queued).toBe(0);
    first.actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
    vi.advanceTimersByTime(20_000);
    expect(first.markers()).toEqual(["leading"]); // Window and queue stay cancelled even after resumption.
    expect(attempts.every(entry => entry.durable && !entry.snapshot.items.length && !entry.snapshot.settledWindows?.length)).toBe(true);
    unavailable = false;
    attempts.length = 0;
    if (boundary === "close") await first.close();
    else first.actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
    expect(attempts).toHaveLength(1);
    expect(attempts[0]).toMatchObject({ durable: true, snapshot: { items: [] } });
    expect(readQueue(file).settledWindows).toBeUndefined();
    await first.close();
    const second = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(20_000);
    expect(queuedMarkers(file)).toEqual([]);
    expect(second.markers()).not.toContain("cancelled-latest");
    // Restart safety here follows the RECOVERED durable receipt, not the failed halt.
  });

  it("r4: halt excludes cancelled in-flight work before its deferred finalizer can remove it", async () => {
    const first = await fixture();
    let entered!: () => void;
    const running = new Promise<void>(resolve => { entered = resolve; });
    let settle!: (reason: Error) => void;
    vi.spyOn(first.agents, "run").mockImplementation(() => {
      entered();
      return new Promise<never>((_resolve, reject) => { settle = reject; });
    });
    first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "cancelled-inflight" });
    const file = first.queueFile();
    first.actors.resumeAfterRelease();
    await running;
    first.actors.pauseForRelease();
    first.actors.tell(first.actor.id, "queued", { marker: "cancelled-queued" });
    first.actors.tell(first.actor.id, "overflow", { marker: "cancelled-overflow" });
    const rename = fs.renameSync;
    const failed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) throw new Error("r4 inflight rename unavailable");
      return rename(from, to);
    });
    expect(first.actors.haltAll()).toEqual({ halted: 1 });
    expect(first.actors.inFlightCount()).toBe(1); // Finalizer is deliberately still blocked.
    expect(readQueue(file).items).toEqual([]); // Same-file durable fallback excludes that retained ID.
    expect(first.actors.status(first.actor.id).queued).toBe(0);
    failed.mockRestore();
    // Existing persistence while the old item is still retained must not serialize it again.
    first.actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
    expect(readQueue(file).items).toEqual([]);
    settle(new Error("cancelled worker settled"));
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(first.actors.inFlightCount()).toBe(0);
    expect(readQueue(file).items).toEqual([]);
    await first.close();
    const second = await fixture(first.root, first.actor.id);
    expect(second.actors.status(first.actor.id).queued).toBe(0);
  });

  it("r4: ownership-loss halt excludes a retained execution before finalization and input/regain preserves fresh work", async () => {
    let owned = true;
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    vi.spyOn(ActorMeshMonitor.prototype, "schedule").mockImplementation(() => {});
    const first = setup(undefined, () => owned, { releasePaused: true });
    const actor = await first.actors.create({ name: "lost-execution", instructions: "Observe.",
      events: ["agent_settled"], coalesce: false });
    let entered!: () => void;
    const running = new Promise<void>(resolve => { entered = resolve; });
    let settle!: (reason: Error) => void;
    const run = vi.spyOn(first.agents, "run").mockImplementation(() => {
      entered();
      return new Promise<never>((_resolve, reject) => { settle = reject; });
    });
    first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "cancelled-inflight" });
    first.actors.resumeAfterRelease();
    await running;
    first.actors.pauseForRelease();
    const directory = path.join(first.root, "actors", actor.id);
    const file = path.join(directory, fs.readdirSync(directory).find(name => /^queue-.*\.json$/.test(name))!);
    expect(readQueue(file).items.map(item => item.payload.marker)).toEqual(["cancelled-inflight"]);
    owned = false;
    first.actors.listOwned(); // The current local object remains, with an ownership-aborted controller.
    expect(first.actors.haltAll()).toEqual({ halted: 0 });
    expect(first.actors.inFlightCount()).toBe(1); // The finalizer cannot supply the cancellation receipt.
    expect(readQueue(file).items).toEqual([]);
    first.actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
    expect(readQueue(file).items).toEqual([]);
    owned = true;
    first.actors.listOwned(); // Persistent reload while the cancelled old drain is still retained.
    await Promise.resolve();
    first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "fresh-after-regain" });
    expect(readQueue(file).items.map(item => item.payload.marker)).toEqual(["fresh-after-regain"]);
    settle(new Error("ownership-aborted worker settled"));
    await waitFor(() => first.actors.inFlightCount() === 0);
    expect(first.actors.messages(actor.id).filter(message => message.error?.includes("halted by user interrupt"))).toHaveLength(1);
    expect(run).toHaveBeenCalledTimes(1);
    expect(readQueue(file).items.map(item => item.payload.marker)).toEqual(["fresh-after-regain"]);
    await first.close();
    const second = setup(first.root, undefined, { releasePaused: true });
    second.actors.resumeQueued();
    await Promise.resolve();
    expect(second.actors.status(actor.id).queued).toBe(1);
    expect(readQueue(file).items.map(item => item.payload.marker)).toEqual(["fresh-after-regain"]);
  });

  it("r4: halt cancels local pending and parked work after ownership loss without claiming the foreign actor", async () => {
    let owned = true;
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(() => {});
    const first = setup(undefined, () => owned, { releasePaused: true });
    const actor = await first.actors.create({ name: "authority", instructions: "Observe.", events: ["agent_settled"],
      coalesce: false, activation: { minIntervalMs: 1_000 } });
    // Let the registry catch up, then revoke ownership before halt's refreshed read.
    first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "parked-leading" });
    first.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "cancelled-pending" });
    owned = false;
    first.actors.listOwned(); // Existing ownership path parks the formerly owned leading item.
    expect(first.actors.haltAll()).toEqual({ halted: 0 });
    owned = true;
    first.actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
    await Promise.resolve();
    expect(first.actors.status(actor.id).queued).toBe(0);
    expect(incoming(first.actors, actor.id).some(message => (message.data as { marker?: string }).marker === "cancelled-pending")).toBe(false);
  });

  it.each([false, true])("r4: predecessor restore failure preserves ordinary adoption and fences explicitly cancelled absorbed work (cancel %s)", async (cancel) => {
    const first = await fixture();
    seedPending(first.actors, first.actor.id, "own-pending");
    const file = first.queueFile();
    await first.close();
    const predecessorRoot = "session:r4-predecessor";
    const key = createHash("sha256").update([predecessorRoot, "session"].join("\0")).digest("hex").slice(0, 16);
    const predecessorFile = path.join(path.dirname(file), `queue-${key}.json`);
    const saved = JSON.parse(fs.readFileSync(file, "utf8"));
    saved.settledWindows[0].pending.payload.marker = "predecessor-latest";
    saved.settledWindows[0].pending.observedAt++;
    fs.writeFileSync(predecessorFile, JSON.stringify(saved));
    const registryFile = path.join(first.root, "actors", "actors.json");
    const registry = JSON.parse(fs.readFileSync(registryFile, "utf8"));
    registry.actors.find((row: { id: string }) => row.id === first.actor.id).adoptedFrom = [predecessorRoot];
    fs.writeFileSync(registryFile, JSON.stringify(registry));
    const rename = fs.renameSync;
    const failed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) throw new Error("r4 adopted destination unavailable");
      return rename(from, to);
    });
    const second = await fixture(first.root, first.actor.id);
    expect(fs.existsSync(predecessorFile)).toBe(true);
    expect(second.actors.status(first.actor.id).lastError).toMatch(/adopted destination unavailable/);
    if (cancel) {
      second.actors.haltAll();
      const fence = JSON.parse(fs.readFileSync(file, "utf8"));
      expect(fence.cancelledPredecessors).toEqual([path.basename(predecessorFile)]);
      expect(fence.items).toEqual([]);
      expect(fence.settledWindows).toBeUndefined();
      await second.close(); // Both canonical retries fail; fallback is the durable receipt.
    } else {
      failed.mockRestore();
      await second.close(); // Ordinary adoption must retain, not cancel, latest.
      expect(fs.existsSync(predecessorFile)).toBe(false);
    }
    failed.mockRestore();
    const third = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(900);
    expect(queuedMarkers(file)).toEqual(cancel ? [] : ["leading", "predecessor-latest"]);
    if (cancel) {
      expect(fs.existsSync(predecessorFile)).toBe(true); // The absorbed source still exists; it cannot replay.
      third.actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
      third.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "new-leading" });
      vi.advanceTimersByTime(100);
      third.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "new-latest" });
      vi.advanceTimersByTime(900);
      expect(queuedMarkers(file)).toEqual(["new-leading", "new-latest"]);
    }
    await third.close();
    const fourth = await fixture(first.root, first.actor.id);
    vi.advanceTimersByTime(20_000);
    expect(queuedMarkers(file)).toEqual(cancel ? ["new-leading", "new-latest"] : ["leading", "predecessor-latest"]);
    expect(fourth.actors.status(first.actor.id).queued).toBe(2);
  });

  it.skipIf(process.platform === "win32")("r4: rename fallback refuses a symlink queue receipt without writing its target", async () => {
    const first = await fixture();
    seedPending(first.actors, first.actor.id, "cancelled-latest");
    const file = first.queueFile();
    const unrelated = path.join(first.root, "unrelated.json");
    const bytes = JSON.stringify({ unrelated: "must remain unchanged" });
    fs.writeFileSync(unrelated, bytes);
    fs.rmSync(file);
    fs.symlinkSync(unrelated, file);
    const rename = fs.renameSync;
    const failed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) throw new Error("r4 symlink canonical rename unavailable");
      return rename(from, to);
    });
    expect(() => first.actors.haltAll()).toThrow(/not durably acknowledged.*not a regular file/);
    expect(first.actors.status(first.actor.id).lastError).toMatch(/not a regular file/);
    expect(fs.readFileSync(unrelated, "utf8")).toBe(bytes);
    expect(first.actors.status(first.actor.id).queued).toBe(0);
    vi.advanceTimersByTime(20_000);
    expect(first.markers()).toEqual(["leading"]);
    failed.mockRestore();
    // Canonical recovery replaces the link itself atomically, never follows it.
    await first.close();
    expect(fs.lstatSync(file).isFile()).toBe(true);
    expect(readQueue(file).items).toEqual([]);
    expect(fs.readFileSync(unrelated, "utf8")).toBe(bytes);
  });

  it("r4: own-file restore parse and soft rewrite do not excuse the final durable close barrier", async () => {
    const first = await fixture();
    seedPending(first.actors, first.actor.id);
    const file = first.queueFile();
    await first.close();
    const second = await fixture(first.root, first.actor.id);
    const fsync = fs.fsyncSync;
    const failed = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isFile()) throw new Error("r4 restored queue barrier unavailable");
      return fsync(fd);
    });
    const write = atomicWrite.writeJsonAtomic;
    const attempts: boolean[] = [];
    vi.spyOn(atomicWrite, "writeJsonAtomic").mockImplementation((target, value, options) => {
      if (target === file) attempts.push(options?.durable === true);
      write(target, value, options);
    });
    await second.close();
    expect(attempts).toEqual([true]);
    expect(second.actors.status(first.actor.id).lastError).toMatch(/restored queue barrier unavailable/);
    failed.mockRestore();
    vi.advanceTimersByTime(20_000);
    expect(second.markers()).toEqual(["leading"]);
  });

  it("r4: one failed close save sets lastError even after an ordinary pending save succeeded", async () => {
    const first = await fixture();
    seedPending(first.actors, first.actor.id);
    const file = first.queueFile();
    const rename = fs.renameSync;
    let failures = 0;
    const snapshots: QueueSnapshot[] = [];
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === file) {
        snapshots.push(JSON.parse(fs.readFileSync(from, "utf8")));
        if (failures++ === 0) throw new Error("r4 final close save unavailable");
      }
      return rename(from, to);
    });
    await first.close();
    expect.soft(snapshots, "close must attempt stable storage once after a non-durable pending save").toHaveLength(1);
    for (const snapshot of snapshots) {
      expect(snapshot.settledWindows?.[0]?.pending?.payload.marker).toBe("latest");
    }
    expect.soft(first.actors.status(first.actor.id).lastError).toEqual(expect.stringMatching(/r4 final close save unavailable/));
    vi.advanceTimersByTime(20_000);
    expect(first.markers()).toEqual(["leading"]);
  });
});

describe("actor agent_settled leading + latest trailing minimum interval", () => {
  const timedActor = async (minIntervalMs = 1_000, owns?: () => boolean, coalesce = false) => {
    const fixture = setup(undefined, owns);
    fixture.actors.pauseForRelease(); // Observe real ingress/queueing without launching a worker.
    const actor = await fixture.actors.create({ name: "timed", instructions: "Observe.", events: ["agent_settled"], coalesce, activation: { minIntervalMs } });
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    vi.setSystemTime(10_000);
    return { ...fixture, actor };
  };

  it("delivers a lone in-window settle once at the original window end without another event", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    expect(actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "trailing" })).toBe(0);
    vi.advanceTimersByTime(899);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(incoming(actors, actor.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "trailing"]);
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("delivers a burst as leading plus one trailing latest payload without sliding the boundary", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchObservedHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchObservedHostEvent("agent_settled", { ...payload(), marker: "superseded" });
    vi.advanceTimersByTime(800);
    actors.dispatchObservedHostEvent("agent_settled", { ...payload(), marker: "latest" });
    vi.advanceTimersByTime(99);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(incoming(actors, actor.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "latest"]);
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("cancels the pending trailing timer on close and never delivers after close", async () => {
    const { actors, actor, close } = await timedActor();
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    const cancelled = vi.spyOn(globalThis, "clearTimeout");
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", payload());
    const index = scheduled.mock.calls.findIndex(([, delay]) => delay === 900);
    expect(index).toBeGreaterThanOrEqual(0);
    const timer = scheduled.mock.results[index]!.value;
    await close();
    expect(cancelled).toHaveBeenCalledWith(timer);
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it.each([400, 1_200])("restores one latest pending settle across restart after %i ms without another event", async (downtime) => {
    const first = await timedActor();
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "superseded" });
    const latest = { ...payload(), marker: "latest" };
    const images = [{ type: "image" as const, data: "latest-image", mimeType: "image/png" }];
    actors.dispatchHostEvent("agent_settled", latest, images);
    latest.marker = "mutated"; images[0]!.data = "mutated";
    await first.close();
    vi.advanceTimersByTime(downtime);
    expect(incoming(actors, actor.id)).toHaveLength(1); // No old-manager delivery.
    const second = setup(first.root, undefined, { releasePaused: true });
    second.actors.resumeQueued();
    await Promise.resolve(); // Restore admission uses the host's explicit bootstrap-ready boundary.
    const remaining = Math.max(0, 900 - downtime);
    if (remaining) {
      vi.advanceTimersByTime(remaining - 1);
      expect(incoming(second.actors, actor.id)).toHaveLength(1);
    }
    vi.advanceTimersByTime(remaining ? 1 : 0);
    expect(incoming(second.actors, actor.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "latest"]);
    const directory = path.join(first.root, "actors", actor.id);
    const queued = JSON.parse(fs.readFileSync(path.join(directory, fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!), "utf8"));
    expect(queued.items.find((item: { payload: { marker: string } }) => item.payload.marker === "latest").images).toEqual([{ type: "image", data: "latest-image", mimeType: "image/png" }]);
    await second.close();
    const third = setup(first.root, undefined, { releasePaused: true });
    third.actors.resumeQueued();
    await Promise.resolve();
    vi.advanceTimersByTime(2_000);
    expect(incoming(third.actors, actor.id)).toHaveLength(2); // Successful admission left no pending duplicate.
  });

  it.each(["stop", "remove", "recreate", "halt"] as const)("does not resurrect durable pending after explicit %s and restart", async (cancel) => {
    const first = await timedActor();
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "cancelled" });
    if (cancel === "halt") actors.haltAll();
    else if (cancel === "remove") await actors.remove(actor.id);
    else {
      const definition = actors.definition(actor.id);
      await actors.stop(actor.id);
      if (cancel === "recreate") expect((await actors.create(definition)).id).not.toBe(actor.id);
    }
    await first.close();
    const second = setup(first.root, undefined, { releasePaused: true });
    second.actors.resumeQueued();
    await Promise.resolve();
    vi.advanceTimersByTime(2_000);
    for (const restored of second.actors.list()) {
      expect(incoming(second.actors, restored.id).some(message => (message.data as { marker?: string }).marker === "cancelled")).toBe(false);
      const directory = path.join(first.root, "actors", restored.id);
      for (const file of fs.readdirSync(directory).filter(file => file.startsWith("queue-"))) {
        expect(JSON.parse(fs.readFileSync(path.join(directory, file), "utf8")).settledWindows).toBeUndefined();
      }
    }
    if (cancel === "remove") expect(second.actors.list()).toHaveLength(0);
    if (cancel === "recreate") {
      second.actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "new" });
      expect(incoming(second.actors, second.actors.list()[0]!.id)).toHaveLength(1);
    }
  });

  it("restores independent sources and authenticated root fallback only after publication is ready", async () => {
    let monitor: ActorMeshMonitor | undefined;
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
    const first = await timedActor();
    const { actors, actor, mesh } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload("source-a"), marker: "a-leading" });
    vi.advanceTimersByTime(200);
    actors.dispatchHostEvent("agent_settled", { ...payload("source-b"), marker: "b-leading" });
    actors.dispatchHostEvent("agent_settled", { marker: "root-leading" });
    actors.dispatchHostEvent("agent_settled", { ...payload("source-a"), marker: "a-latest" });
    actors.dispatchHostEvent("agent_settled", { ...payload("source-b"), marker: "b-latest" });
    const relay = await mesh.publish({
      topic: "fabric.actor.host-event", kind: "agent_settled", from: identity, to: actor.id,
      data: { version: 1, actorId: actor.id, event: "agent_settled", payload: { marker: "root-latest" }, mainRevision: 0, taskRevision: 0, idle: true },
    });
    monitor!.callbacks.onEvent(relay);
    await first.close();
    vi.advanceTimersByTime(300);
    let published = false;
    const second = setup(first.root, undefined, { releasePaused: true, canConsumeMesh: () => published });
    await Promise.resolve();
    second.actors.resumeQueued();
    vi.advanceTimersByTime(499);
    expect(incoming(second.actors, actor.id)).toHaveLength(3);
    published = true;
    second.actors.resumeQueued();
    vi.advanceTimersByTime(1); // a's original 11,000 boundary, not 1,000 after restart.
    expect((incoming(second.actors, actor.id)[3]!.data as { marker: string }).marker).toBe("a-latest");
    published = false; // A live timer's publication veto preserves, rather than drops, pending.
    vi.advanceTimersByTime(200);
    expect(incoming(second.actors, actor.id)).toHaveLength(4);
    published = true;
    second.actors.resumeQueued();
    vi.advanceTimersByTime(0);
    expect(incoming(second.actors, actor.id).slice(3).map(message => (message.data as { marker: string }).marker)).toEqual(["a-latest", "b-latest", "root-latest"]);
    expect(second.actors.inFlightCount()).toBe(0); // releasePaused still gates worker activation.
  });

  it("keeps pending work across failed timer admission and another restart", async () => {
    const first = await timedActor();
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    const clone = globalThis.structuredClone;
    const failing = vi.spyOn(globalThis, "structuredClone").mockImplementation(value => {
      if ((value as { marker?: string })?.marker === "latest") throw new Error("temporary admission failure");
      return clone(value);
    });
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    failing.mockRestore();
    await first.close();
    const second = setup(first.root, undefined, { releasePaused: true });
    await Promise.resolve();
    vi.advanceTimersByTime(0);
    expect(incoming(second.actors, actor.id)).toHaveLength(1); // No constructor-time overdue activation.
    second.actors.resumeQueued();
    vi.advanceTimersByTime(0);
    expect(incoming(second.actors, actor.id).map(message => (message.data as { marker: string }).marker)).toEqual(["leading", "latest"]);
  });

  it.each([false, true])("persists boundary admission atomically even if the next checkpoint fails (coalesce %s)", async (coalesce) => {
    const first = await timedActor(1_000, undefined, coalesce);
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "obsolete" });
    const directory = path.join(first.root, "actors", actor.id);
    const queueFile = path.join(directory, fs.readdirSync(directory).find(file => file.startsWith("queue-"))!);
    const rename = fs.renameSync;
    let admittedCheckpoint = false;
    const checkpoint = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === queueFile) {
        if (admittedCheckpoint) throw new Error("no second checkpoint");
        // Revision checkpoints legitimately precede admission. Fence after the
        // first snapshot containing the boundary work, not after those writes.
        const candidate = JSON.parse(fs.readFileSync(from, "utf8"));
        admittedCheckpoint = candidate.items.some((item: { payload: { marker?: string } }) => item.payload.marker === "boundary-latest");
      }
      return rename(from, to);
    });
    vi.setSystemTime(11_000);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "boundary-latest" });
    const saved = JSON.parse(fs.readFileSync(queueFile, "utf8"));
    expect(admittedCheckpoint).toBe(true);
    expect(saved.items).toHaveLength(coalesce ? 1 : 2);
    expect(saved.items.at(-1).payload.marker).toBe("boundary-latest");
    expect(saved.settledWindows).toBeUndefined(); // Already correct in the admission's first atomic write.
    checkpoint.mockRestore();
    await first.close();
    const second = setup(first.root, undefined, { releasePaused: true });
    second.actors.resumeQueued();
    await Promise.resolve();
    vi.advanceTimersByTime(2_000);
    expect(incoming(second.actors, actor.id)).toHaveLength(coalesce ? 1 : 2); // Coalesce admission does not append telemetry.
    expect(second.actors.status(actor.id).queued).toBe(coalesce ? 1 : 2);
  });

  it("retains a pending-only snapshot after the leading queue has completed", async () => {
    const first = await timedActor();
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    await first.close();
    const directory = path.join(first.root, "actors", actor.id);
    const queueFile = path.join(directory, fs.readdirSync(directory).find(file => file.startsWith("queue-"))!);
    const saved = JSON.parse(fs.readFileSync(queueFile, "utf8"));
    saved.items = []; delete saved.cleanHandover; // Completed leading run; only the unadmitted trailing snapshot remains.
    fs.writeFileSync(queueFile, JSON.stringify(saved));
    const second = setup(first.root, undefined, { releasePaused: true });
    await Promise.resolve();
    expect(JSON.parse(fs.readFileSync(queueFile, "utf8")).settledWindows).toHaveLength(1);
    vi.advanceTimersByTime(900);
    expect(incoming(second.actors, actor.id)).toHaveLength(1); // Pending-only restore waits for bootstrap, even when due.
    second.actors.resumeQueued();
    vi.advanceTimersByTime(0);
    expect(incoming(second.actors, actor.id).map(message => (message.data as { marker: string }).marker)).toEqual(["leading", "latest"]);
    expect(JSON.parse(fs.readFileSync(queueFile, "utf8")).items).toHaveLength(1);
  });

  it("imports latest predecessor pending before checkpoint/delete and keeps it when checkpoint fails", async () => {
    const first = await timedActor();
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "own-pending" });
    await first.close();
    const directory = path.join(first.root, "actors", actor.id);
    const queueFile = path.join(directory, fs.readdirSync(directory).find(file => file.startsWith("queue-"))!);
    const predecessorRoot = "session:predecessor";
    const predecessorKey = createHash("sha256").update([predecessorRoot, "session"].join("\0")).digest("hex").slice(0, 16);
    const predecessorFile = path.join(directory, `queue-${predecessorKey}.json`);
    const saved = JSON.parse(fs.readFileSync(queueFile, "utf8"));
    saved.settledWindows[0].pending.payload.marker = "predecessor-latest";
    saved.settledWindows[0].pending.observedAt += 1;
    fs.writeFileSync(predecessorFile, JSON.stringify(saved));
    const registryFile = path.join(first.root, "actors", "actors.json");
    const registry = JSON.parse(fs.readFileSync(registryFile, "utf8"));
    registry.actors.find((record: { id: string }) => record.id === actor.id).adoptedFrom = [predecessorRoot];
    fs.writeFileSync(registryFile, JSON.stringify(registry));
    const rename = fs.renameSync;
    const failed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (to === queueFile) throw new Error("checkpoint unavailable");
      return rename(from, to);
    });
    const second = setup(first.root, undefined, { releasePaused: true });
    second.actors.resumeQueued();
    await Promise.resolve();
    expect(fs.existsSync(predecessorFile)).toBe(true);
    await second.close();
    failed.mockRestore();
    const third = setup(first.root, undefined, { releasePaused: true });
    third.actors.resumeQueued();
    await Promise.resolve();
    expect(fs.existsSync(predecessorFile)).toBe(false); // Only the successfully merged own snapshot permits deletion.
    expect(JSON.parse(fs.readFileSync(queueFile, "utf8")).settledWindows[0].pending.payload.marker).toBe("predecessor-latest");
    vi.advanceTimersByTime(900);
    expect(incoming(third.actors, actor.id).map(message => (message.data as { marker: string }).marker)).toEqual(["leading", "predecessor-latest"]);
    await third.close();
    const fourth = setup(first.root, undefined, { releasePaused: true });
    fourth.actors.resumeQueued();
    await Promise.resolve();
    vi.advanceTimersByTime(2_000);
    expect(incoming(fourth.actors, actor.id)).toHaveLength(2);
  });

  it("retains ownership-deferred pending through restart and admits it once after reacquisition", async () => {
    let owned = true;
    const first = await timedActor(1_000, () => owned);
    const { actors, actor } = first;
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    owned = false;
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    await first.close();
    const second = setup(first.root, () => owned, { releasePaused: true });
    await Promise.resolve();
    vi.advanceTimersByTime(1_000);
    // The old owner's buffered telemetry need not be archived on unowned close.
    expect(incoming(second.actors, actor.id).some(message => (message.data as { marker: string }).marker === "latest")).toBe(false);
    owned = true;
    second.actors.resumeQueued();
    vi.advanceTimersByTime(0);
    expect(incoming(second.actors, actor.id).filter(message => (message.data as { marker: string }).marker === "latest")).toHaveLength(1);
  });

  it("lets a boundary arrival supersede an older still-pending trailing event", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "old-pending" });
    vi.setSystemTime(11_000); // Event-loop ordering: arrival beats the due timer callback.
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "boundary-latest" });
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, actor.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "boundary-latest"]);
  });

  it("chunks intervals above Node's timer maximum without early delivery", async () => {
    const maximumDelay = 2_147_483_647;
    const { actors, actor } = await timedActor(maximumDelay + 1_000);
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    actors.dispatchHostEvent("agent_settled", payload());
    actors.dispatchHostEvent("agent_settled", payload());
    expect(scheduled.mock.calls.some(([, delay]) => delay === maximumDelay)).toBe(true);
    vi.advanceTimersByTime(maximumDelay);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(999);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("retains independent per-source trailing payloads and boundaries", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", payload("source-a"));
    vi.advanceTimersByTime(200);
    actors.dispatchHostEvent("agent_settled", payload("source-b"));
    actors.dispatchHostEvent("agent_settled", { ...payload("source-a"), marker: "a-latest" });
    actors.dispatchHostEvent("agent_settled", { ...payload("source-b"), marker: "b-latest" });
    vi.advanceTimersByTime(800);
    expect(incoming(actors, actor.id)).toHaveLength(3);
    vi.advanceTimersByTime(200);
    expect(incoming(actors, actor.id).slice(2).map((message) => (message.data as { marker: string }).marker)).toEqual(["a-latest", "b-latest"]);
  });

  it("cancels on stop and same-name recreation starts with a fresh immutable ID", async () => {
    const { actors, actor } = await timedActor();
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    const cancelled = vi.spyOn(globalThis, "clearTimeout");
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "old" });
    const timer = scheduled.mock.results[scheduled.mock.calls.findIndex(([, delay]) => delay === 900)]!.value;
    await actors.stop(actor.id);
    expect(cancelled).toHaveBeenCalledWith(timer);
    const replacement = await actors.create({ ...actors.definition(actor.id), name: actor.name });
    expect(replacement.id).not.toBe(actor.id);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "new" });
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, replacement.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["new"]);
    expect(() => actors.status(actor.id)).toThrow();
  });

  it("cancels on removal without rearming a removed actor", async () => {
    const { actors, actor } = await timedActor();
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    const cancelled = vi.spyOn(globalThis, "clearTimeout");
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", payload());
    const timer = scheduled.mock.results[scheduled.mock.calls.findIndex(([, delay]) => delay === 900)]!.value;
    await actors.remove(actor.id);
    expect(cancelled).toHaveBeenCalledWith(timer);
    vi.advanceTimersByTime(2_000);
    expect(actors.list()).toHaveLength(0);
  });

  it("cancels on halt even if user input resumes before the trailing boundary", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "cancelled" });
    actors.haltAll();
    actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "resumed" });
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("delivers trailing on the current registry object after ownership reload", async () => {
    let owned = true;
    const { actors, actor } = await timedActor(1_000, () => owned);
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    owned = false;
    actors.listOwned();
    owned = true; // The timer's fresh ownership check triggers ManagedActor replacement.
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id)).toHaveLength(2);
    expect((incoming(actors, actor.id)[1]!.data as { marker: string }).marker).toBe("latest");
    expect(actors.status(actor.id).queued).toBe(1); // Leading is parked until restore's microtask.
  });

  it("fences trailing delivery when ownership is lost at the boundary", async () => {
    let owned = true;
    const { actors, actor } = await timedActor(1_000, () => owned);
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", payload());
    owned = false;
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it("rechecks current event subscriptions at trailing delivery", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", payload());
    actors.dispatchHostEvent("agent_settled", payload());
    await actors.setEvents(actor.id, []);
    vi.advanceTimersByTime(1_000);
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it("skipped arrivals neither replace a held payload nor arm a window, and trailing rechecks filters", async () => {
    const { actors, actor } = await timedActor();
    const filter = [{ id: "skip-marker", source: ["host:agent_settled"], where: [{ path: "marker", equals: "skip" }] }];
    await actors.setActivationFilter(actor.id, filter);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "skip" });
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "keep" });
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "skip" });
    vi.advanceTimersByTime(1_000);
    const admitted = () => incoming(actors, actor.id).filter((message) => !message.reason);
    expect(admitted().map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "keep"]);
    expect(actors.status(actor.id).filterSkipped.count).toBe(2);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "now-filtered" });
    await actors.setActivationFilter(actor.id, [{ id: "new-filter", source: ["host:agent_settled"] }]);
    vi.advanceTimersByTime(1_000);
    expect(admitted()).toHaveLength(2);
    expect(actors.status(actor.id).filterSkipped.count).toBe(1);
  });

  it("clones latest payload/images and retains the existing queue coalescing contract", async () => {
    const { actors, actor, root } = await timedActor(1_000, undefined, true);
    const directory = path.join(root, "actors", actor.id);
    const queued = () => JSON.parse(fs.readFileSync(path.join(directory, fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!), "utf8")) as {
      items: Array<{ payload: { marker: string }; images?: Array<{ data: string }>; coalesceKey: string }>;
    };
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "superseded" }, [{ type: "image", data: "old", mimeType: "image/png" }]);
    const latest = { ...payload(), marker: "latest" };
    const images = [{ type: "image" as const, data: "latest-image", mimeType: "image/png" }];
    actors.dispatchHostEvent("agent_settled", latest, images);
    latest.marker = "mutated";
    images[0]!.data = "mutated";
    vi.advanceTimersByTime(900);
    expect(queued().items).toHaveLength(1); // Trailing merges with an unlaunched leading item.
    expect(queued().items[0]).toMatchObject({ payload: { marker: "latest" }, images: [{ data: "latest-image" }], coalesceKey: "host:agent_settled" });
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "without-image" });
    vi.advanceTimersByTime(1_000);
    expect(queued().items[0]).toMatchObject({ payload: { marker: "without-image" } });
    expect(queued().items[0]!.images).toBeUndefined();
    expect(actors.status(actor.id).queued).toBe(1);
  });

  it("shares latest trailing state with authenticated root-relay ingress, never a stranger", async () => {
    let monitor: ActorMeshMonitor | undefined;
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
    const { actors, actor, mesh } = await timedActor();
    const relay = async (from: MeshIdentity, marker: string) => {
      const event = await mesh.publish({
        topic: "fabric.actor.host-event", kind: "agent_settled", from, to: actor.id,
        data: { version: 1, actorId: actor.id, event: "agent_settled", payload: { ...payload(), marker }, mainRevision: 0, taskRevision: 0, idle: true },
      });
      monitor!.callbacks.onEvent(event); // Exact production manager reception callback, without watcher timing.
    };
    await relay({ ...identity, id: "session:stranger" }, "stranger-first");
    actors.dispatchObservedHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    await relay(identity, "relay-superseded");
    actors.dispatchObservedHostEvent("agent_settled", { ...payload("source-a", "user"), marker: "local-superseded" });
    await relay(identity, "relay-latest");
    await relay({ ...identity, id: "session:stranger" }, "stranger-last");
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "relay-latest"]);
  });

  it("admits the first immediately, holds same-source repeats, and never slides the boundary", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "limited", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 1_000 } });
    const time = clock();
    time.set(10_000);
    expect(actors.dispatchHostEvent("agent_settled", payload())).toBe(1);
    expect(incoming(actors, actor.id)).toHaveLength(1); // synchronous admission; no delay/timer
    const completed = () => actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error);
    await waitFor(() => completed().length === 1 && actors.status(actor.id).status === "idle");
    time.advance(100);
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(1);
    time.set(10_999);
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(1);
    time.set(11_000); // exactly first + interval, not last dropped + interval
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(2);
    await waitFor(() => completed().length === 2 && actors.status(actor.id).status === "idle");
    expect(completed()).toHaveLength(2); // actual worker wakes, not just queued observations
  });

  it("keys by real source session, independently per actor, not input user/extension origin", async () => {
    const { actors } = setup();
    const first = await actors.create({ name: "first", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 1_000 } });
    const time = clock();
    actors.dispatchObservedHostEvent("agent_settled", payload("source-a", "extension"));
    actors.dispatchObservedHostEvent("agent_settled", payload("source-a", "user"));
    actors.dispatchObservedHostEvent("agent_settled", payload("source-b", "extension"));
    expect(incoming(actors, first.id)).toHaveLength(2);
    const second = await actors.create({ name: "second", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 1_000 } });
    time.advance(1);
    actors.dispatchObservedHostEvent("agent_settled", payload("source-a"));
    expect(incoming(actors, first.id)).toHaveLength(2);
    expect(incoming(actors, second.id)).toHaveLength(1);
  });

  it("leaves other event kinds, direct messages and mesh events unaffected", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({ name: "mixed", instructions: "Observe.", events: ["agent_settled", "turn_end"], topics: ["demo"], coalesce: false, activation: { minIntervalMs: 60_000 } });
    actors.dispatchHostEvent("agent_settled", payload());
    for (let i = 0; i < 2; i++) {
      actors.dispatchHostEvent("turn_end", payload("source-a", "extension", "turn_end"));
      actors.tell(actor.id, "direct");
      await mesh.publish({ topic: "demo", kind: "agent_settled", from: identity, data: payload() });
    }
    await waitFor(() => incoming(actors, actor.id, "mesh:demo").length === 2);
    expect(incoming(actors, actor.id, "host:turn_end")).toHaveLength(2);
    expect(incoming(actors, actor.id, "direct")).toHaveLength(2);
  });

  it.each([undefined, { minIntervalMs: 0 }])("keeps today's every-event behavior when disabled (%j)", async (activation) => {
    const { actors } = setup();
    const actor = await actors.create({ name: "plain", instructions: "Observe.", events: ["agent_settled"], coalesce: false, ...(activation ? { activation } : {}) });
    actors.dispatchHostEvent("agent_settled", payload());
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("uses trusted root identity when an older host envelope has no session.id", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "legacy", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 60_000 } });
    actors.dispatchHostEvent("agent_settled", { signal: { payload: { source: "extension" } } });
    actors.dispatchHostEvent("agent_settled", { signal: { payload: { source: "user" } } });
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it("retains config through definition/global export-import/restart, but resets runtime windows", async () => {
    const first = setup();
    const activation = { minIntervalMs: 60_000 };
    const actor = await first.actors.create({ name: "roundtrip", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation });
    expect(actor.activation).toEqual(activation);
    expect(first.actors.status(actor.id).activation).toEqual(activation);
    const definition = first.actors.definition(actor.id);
    expect(definition.activation).toEqual(activation);
    const globals = new GlobalActorRegistry(first.root, 64 * 1024);
    const template = globals.create(definition);
    expect(template.activation).toEqual(activation);
    expect(globals.update(template.id, { instructions: "Updated." }).activation).toEqual(activation);
    const reloadedGlobals = new GlobalActorRegistry(first.root, 64 * 1024);
    expect(reloadedGlobals.resolve(template.id)?.activation).toEqual(activation);
    const request = reloadedGlobals.toRequest(reloadedGlobals.resolve(template.id)!, "imported");
    expect(request.activation).toEqual(activation);
    expect((await first.actors.create(request)).activation).toEqual(activation);
    first.actors.dispatchHostEvent("agent_settled", payload());
    await waitFor(() => first.actors.status(actor.id).status === "idle");
    await first.close();
    closers.splice(closers.indexOf(first.close), 1);
    const second = setup(first.root);
    expect(second.actors.status(actor.id).activation).toEqual(activation);
    second.actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(second.actors, actor.id)).toHaveLength(2); // persisted inbox + new first wake
    const registry = fs.readFileSync(path.join(first.root, "actors", "actors.json"), "utf8");
    expect(registry).not.toMatch(/settledWindows|lastAccepted|nextAllowed/);
  });

  it("survives same-process ownership/registry object reloads", async () => {
    let owned = true;
    const { actors } = setup(undefined, () => owned);
    const actor = await actors.create({ name: "reload", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 60_000 } });
    actors.dispatchHostEvent("agent_settled", payload());
    await waitFor(() => actors.status(actor.id).status === "idle");
    owned = false;
    actors.listOwned();
    owned = true;
    actors.listOwned(); // #refreshOwnership rebuilds ManagedActor
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it("shares the local gate with authenticated durable-owner relay reception", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({ name: "relay", instructions: "Observe.", residency: "durable", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 60_000 } });
    const relay = (from: MeshIdentity, hostPayload: unknown) => mesh.publish({
      topic: "fabric.actor.host-event", kind: "agent_settled", from, to: actor.id,
      data: { version: 1, actorId: actor.id, event: "agent_settled", payload: hostPayload, mainRevision: 0, taskRevision: 0, idle: true },
    });
    const stranger: MeshIdentity = { ...identity, id: "session:stranger" };
    await relay(stranger, payload("source-a")); // must not poison the source window
    await relay(identity, payload("source-a"));
    await waitFor(() => incoming(actors, actor.id).length >= 1);
    actors.dispatchObservedHostEvent("agent_settled", payload("source-a", "user"));
    await relay(identity, payload("source-a"));
    await relay(identity, payload("source-b"));
    await waitFor(() => incoming(actors, actor.id).some((m) => (m.data as ReturnType<typeof payload>).session?.id === "source-b"));
    expect(incoming(actors, actor.id)).toHaveLength(2);
    // local trusted-root fallback and relay trusted-root fallback are the same source
    actors.dispatchObservedHostEvent("agent_settled", {});
    await relay(identity, {});
    await relay(identity, payload("source-c")); // ordered receipt proves fallback relay consumed
    await waitFor(() => incoming(actors, actor.id).some((m) => (m.data as ReturnType<typeof payload>).session?.id === "source-c"));
    expect(incoming(actors, actor.id)).toHaveLength(4);
  });

  it.each([-1, 1.5, Infinity, NaN, "1000", Number.MAX_SAFE_INTEGER + 1])("rejects invalid minIntervalMs %s instead of silently enabling/disabling", async (minIntervalMs) => {
    const { actors, root } = setup();
    const request = { name: "invalid", instructions: "Observe.", activation: { minIntervalMs } };
    await expect(actors.create(request as Parameters<ActorManager["create"]>[0])).rejects.toThrow(/activation|minIntervalMs/);
    expect(() => new GlobalActorRegistry(root, 64 * 1024).create(request as Parameters<ActorManager["create"]>[0])).toThrow(/activation|minIntervalMs/);
  });

  it("publishes a strict create schema and preserves provider create/export/import config", async () => {
    const { actors, agents, mesh, root } = setup();
    const createSchema = AGENTS_ACTION_DESCRIPTORS.find((action) => action.name === "create")!.inputSchema as { properties: Record<string, unknown> };
    expect(createSchema.properties.activation).toMatchObject({ type: "object", additionalProperties: false, properties: { minIntervalMs: { type: "integer", minimum: 0 } } });
    const globalActors = new GlobalActorRegistry(root, 64 * 1024);
    const participants = { scheduleRefresh() {}, list: () => [], get: () => undefined } as unknown as FabricParticipantSource;
    const main = { id: identity.id, local: true } as FabricMainAgentTarget;
    const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, actors, globalActors, main, participants, undefined, lifecycle);
    const context: FabricInvocationContext = {
      cwd: process.cwd(), signal: undefined, parentToolCallId: "test-parent", nestedToolCallId: "test-nested",
      extensionContext: {} as FabricInvocationContext["extensionContext"], activity() {}, update() {},
    };
    const activation = { minIntervalMs: 1_000 };
    const created = await provider.invoke("create", { name: "provider", instructions: "Observe.", activation }, context) as { id: string; activation?: unknown };
    expect(created.activation).toEqual(activation);
    const exported = await provider.invoke("export", { id: created.id, write: true }, context) as { id: string; activation?: unknown };
    expect(exported.activation).toEqual(activation);
    const imported = await provider.invoke("import", { id: exported.id, as: "provider-import" }, context) as { activation?: unknown };
    expect(imported.activation).toEqual(activation);
    const global = await provider.invoke("create", { name: "global-provider", instructions: "Observe.", scope: "global", activation }, context) as { activation?: unknown };
    expect(global.activation).toEqual(activation);
    await expect(provider.invoke("create", { name: "provider-invalid", instructions: "Observe.", activation: { minIntervalMs: -1 } }, context)).rejects.toThrow(/activation|minIntervalMs/);
  });
});
