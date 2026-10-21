import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_FENCE_RAW_BUFFER_MAX, BridgeFenceObservationBuffer, BridgeFenceStats,
  measureBridgeFence, measureBridgeScan, type BridgeFenceObservation } from "../src/mesh/bridge-fence-stats.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { CommitOutbox } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { readHostLease } from "../src/topology/host-leases.js";

// Keep reason: instrumentation must never change the bridge's admission/append failure path.
const context = { peer: "far", site: "heldFence", scope: "state-write-fence", attempt: 0,
  optimized: true, fallback: false, retry: false } as const;
const sample = (holdMs: number, extra: Partial<BridgeFenceObservation> = {}): BridgeFenceObservation =>
  ({ v: 1, ...context, holdMs, ...extra });

const stores: MeshStore[] = [];
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const store = (): MeshStore => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-fence-stats-"));
  roots.push(root);
  const mesh = new MeshStore(root, 65536, 500, { stateBackend: "sqlite" });
  stores.push(mesh);
  return mesh;
};

describe("bridge fence timing and passive aggregation", () => {
  it("starts inside the callback, excludes acquisition, and includes rollback before observing", () => {
    let now = 0;
    vi.spyOn(performance, "now").mockImplementation(() => now);
    const samples: BridgeFenceObservation[] = [];
    const result = measureBridgeFence(operation => {
      now += 100; // Simulated acquisition/preamble: excluded.
      try { return operation(); }
      finally { now += 7; } // Simulated ROLLBACK: included.
    }, () => { now += 5; return "committed"; }, context, observation => samples.push(observation));
    expect(result).toBe("committed");
    expect(samples).toEqual([sample(12)]);
  });

  it("preserves exact operation failures, observes rollback, and ignores observer failures", () => {
    let now = 0;
    vi.spyOn(performance, "now").mockImplementation(() => now);
    const failure = new Error("original admission failure");
    const samples: BridgeFenceObservation[] = [];
    expect(() => measureBridgeFence(operation => {
      try { return operation(); } finally { now += 3; }
    }, () => { now += 2; throw failure; }, context, observation => {
      samples.push(observation);
      throw new Error("telemetry failed");
    })).toThrow(failure);
    expect(samples).toEqual([sample(5)]);
    expect(measureBridgeFence(operation => operation(), () => 42, context, () => { throw failure; })).toBe(42);
  });

  it("does not turn pre-callback busy failures into hold samples", () => {
    const busy = new Error("busy before callback");
    const observe = vi.fn();
    expect(() => measureBridgeFence(() => { throw busy; }, () => 42, context, observe)).toThrow(busy);
    expect(observe).not.toHaveBeenCalled();
  });

  it("marks writeBatch callback-only duration separately and preserves its result", () => {
    const observe = vi.fn();
    expect(measureBridgeScan(() => 42, { ...context, site: "mirror.prepare", scope: "writeBatch.prepare" }, observe)).toBe(42);
    expect(observe).toHaveBeenCalledWith(expect.objectContaining({ site: "mirror.prepare", scope: "writeBatch.prepare", holdMs: expect.any(Number) }));
  });

  it("measures complete afterCommit callbacks and preserves exact returns and throws despite observer failures", () => {
    let now = 0;
    vi.spyOn(performance, "now").mockImplementation(() => now);
    const samples: BridgeFenceObservation[] = [];
    const afterCommit = { ...context, site: "mirror.afterCommit", scope: "writeBatch.afterCommit",
      attempt: 2, optimized: false, fallback: false, retry: true } as const;
    const observe = (observation: BridgeFenceObservation): void => {
      samples.push(observation);
      throw new Error("observer failed");
    };
    const result = { committed: true };
    expect(measureBridgeScan(() => { now += 13; return result; }, afterCommit, observe)).toBe(result);
    const failure = new Error("original afterCommit failure");
    let caught: unknown;
    try {
      measureBridgeScan(() => { now += 7; throw failure; }, afterCommit, observe);
    } catch (error) { caught = error; }
    expect(caught).toBe(failure);
    expect(samples).toEqual([
      { v: 1, ...afterCommit, holdMs: 13 },
      { v: 1, ...afterCommit, holdMs: 7 },
    ]);
  });

  it("aggregates sites and fallback/retry counts without logging in the measured callback", () => {
    let now = 0;
    const lines: string[] = [];
    const stats = new BridgeFenceStats({ clock: () => now, log: line => lines.push(line),
      observe: () => { throw new Error("raw observer failure"); } });
    stats.record(sample(1));
    stats.record(sample(2, { retry: true, attempt: 1 }));
    stats.record(sample(10, { optimized: false, fallback: true, retry: true, attempt: 2 }));
    stats.record(sample(4, { site: "subscribeChanges.capture" }));
    stats.record(sample(13, { site: "mirror.afterCommit", scope: "writeBatch.afterCommit", optimized: false }));
    now = 60_001;
    stats.record(sample(3)); // No synchronous rollover log in mirror.prepare/any callback.
    expect(lines).toEqual([]);
    stats.flushIfDue();
    expect(lines).toHaveLength(3);
    expect(lines[0]).toBe("bridge write-fence v1 peer=far site=heldFence count=4 holdP50Ms=2 holdP99Ms=10 holdMaxMs=10 fallbackCount=1 optimizedCount=3 retryCount=2");
    expect(lines[1]).toContain("site=subscribeChanges.capture count=1");
    expect(lines[2]).toBe("bridge write-fence v1 peer=far site=mirror.afterCommit count=1 holdP50Ms=13 holdP99Ms=13 holdMaxMs=13 fallbackCount=0 optimizedCount=0 retryCount=0");
    stats.close();
    stats.close();
    expect(lines).toHaveLength(3);
  });

  it("keeps only a rolling 512 percentile samples but exact count and maximum", () => {
    const lines: string[] = [];
    const stats = new BridgeFenceStats({ log: line => lines.push(line) });
    stats.record(sample(99_999));
    for (let index = 0; index < 1_024; index++) stats.record(sample(1));
    stats.close();
    expect(lines[0]).toContain("count=1025 holdP50Ms=1 holdP99Ms=1 holdMaxMs=99999");
    expect(() => new BridgeFenceStats({ log: () => { throw new Error("log sink failed"); } }).close()).not.toThrow();
  });

  it("bounds raw retention and drains remaining samples in call order", () => {
    const buffer = new BridgeFenceObservationBuffer();
    for (let index = 0; index < BRIDGE_FENCE_RAW_BUFFER_MAX + 7; index++) buffer.push(sample(index));
    const drained = buffer.drain();
    expect(drained).toHaveLength(BRIDGE_FENCE_RAW_BUFFER_MAX);
    expect(drained[0]!.holdMs).toBe(7);
    expect(drained.at(-1)!.holdMs).toBe(BRIDGE_FENCE_RAW_BUFFER_MAX + 6);
    expect(buffer.dropped).toBe(7);
    expect(buffer.drain()).toEqual([]);
    buffer.push(sample(8));
    expect(buffer.drain()).toEqual([sample(8)]);
  });
});

describe("StoreBridgeSide instrumented real SQLite paths", () => {
  it("flushes aggregate logs on bridge stop, outside the prepare callback, and exposes raw draining", async () => {
    const local = new StoreBridgeSide(store(), "far"), remote = new StoreBridgeSide(store(), "hub");
    const lines: string[] = [];
    const bridge = new MeshBridge({ localName: "hub", remoteName: "far", local, remote,
      cursorPath: path.join(local.store.root, "cursor.json"), log: line => lines.push(line),
      observeWriteFence: () => { throw new Error("observer failure"); } });
    await local.mirror({ hosts: [], participants: [] });
    expect(lines).toEqual([]);
    expect(bridge.drainWriteFenceObservations()).toEqual([
      expect.objectContaining({ site: "mirror.prepare", fallback: true }),
      expect.objectContaining({ site: "mirror.afterCommit", scope: "writeBatch.afterCommit", optimized: false, fallback: false }),
    ]);
    expect(bridge.drainWriteFenceObservations()).toEqual([]);
    await bridge.stop();
    expect(lines).toHaveLength(2);
    expect(lines[0]).toContain("bridge write-fence v1 peer=far site=mirror.prepare count=1");
    expect(lines[1]).toContain("bridge write-fence v1 peer=far site=mirror.afterCommit count=1");
  });

  it.each([false, true])("observes complete mirror afterCommit effects on changing and write-free SQLite passes (partial=%s)", async partial => {
    const mesh = store();
    const observations: BridgeFenceObservation[] = [];
    const now = Date.now();
    const side = new StoreBridgeSide(mesh, "far", () => now, false, { observeWriteFence: observation => {
      observations.push(observation);
      throw new Error("passive observer failure");
    } });
    let elapsed = 0, completed = 0;
    vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    const planFor = CommitOutbox.prototype.plan;
    vi.spyOn(CommitOutbox.prototype, "plan").mockImplementation(function (this: CommitOutbox) {
      const plan = planFor.call(this);
      return { ...plan, run: (view, halted) => {
        expect(observations.filter(observation => observation.site === "mirror.afterCommit")).toHaveLength(completed);
        const result = plan.run(view, halted); // Keep real lease effects and partial-branch admission.
        elapsed += 13;
        completed += 1;
        return result;
      } };
    });
    const changing: boolean[] = [];
    const writeBatch = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementation(async input => {
      elapsed += 100; // Outside the callback: not part of the sample.
      const results = await writeBatch(input);
      changing.push(results.some(result => result.applied));
      elapsed += 200;
      return results;
    });
    const identity: MeshIdentity = { id: "session:after-commit", name: "source", kind: "main" };
    const record = { format: 1 as const, id: identity.id, rootId: identity.id, identity,
      startedAt: now, updatedAt: now, expiresAt: now + 60_000 };
    const presence = { hosts: [{ record, expiresAt: record.expiresAt }], participants: [], partial };
    await expect(side.mirror(presence)).resolves.toBeUndefined();
    const lease = readHostLease(mesh.root, identity.id);
    expect(lease).toMatchObject({ id: identity.id, identityId: identity.id, startedAt: now });
    await expect(side.mirror(presence)).resolves.toBeUndefined();
    expect(readHostLease(mesh.root, identity.id)).toEqual(lease);
    expect(changing).toEqual([true, false]);
    expect(completed).toBe(2);
    expect(observations.filter(observation => observation.site === "mirror.afterCommit")).toEqual([
      expect.objectContaining({ scope: "writeBatch.afterCommit", holdMs: 13, attempt: 0,
        optimized: false, fallback: false, retry: false }),
      expect.objectContaining({ scope: "writeBatch.afterCommit", holdMs: 13, attempt: 0,
        optimized: false, fallback: false, retry: false }),
    ]);
  });

  it("attributes optimized and fallback notification capture without changing subscription behavior", async () => {
    const mesh = store();
    await mesh.prepareLock();
    const side = new StoreBridgeSide(mesh, "far");
    const close = side.subscribeChanges(() => undefined, error => { throw error; });
    close();
    expect(side.drainWriteFenceObservations()).toEqual([expect.objectContaining({
      site: "subscribeChanges.capture", scope: "state-write-fence", attempt: 0, optimized: true, fallback: false,
    })]);
    const unchanged = vi.spyOn(mesh, "stateUnchangedSince").mockReturnValue(false);
    const closeFallback = side.subscribeChanges(() => undefined, error => { throw error; });
    closeFallback();
    unchanged.mockRestore();
    expect(side.drainWriteFenceObservations().map(({ attempt, optimized, fallback, retry }) => ({ attempt, optimized, fallback, retry }))).toEqual([
      { attempt: 0, optimized: true, fallback: false, retry: false },
      { attempt: 1, optimized: true, fallback: false, retry: true },
      { attempt: 2, optimized: false, fallback: true, retry: true },
    ]);
  });

  it("observes mirror recovery/full-scan and early-plan callbacks without changing publication outcomes", async () => {
    const mesh = store();
    const observations: BridgeFenceObservation[] = [];
    const side = new StoreBridgeSide(mesh, "far", Date.now, false, { observeWriteFence: observation => {
      observations.push(observation);
      throw new Error("raw observer is not an admission step");
    } });
    const now = Date.now();
    const identity: MeshIdentity = { id: "session:fence-source", name: "source", kind: "main" };
    const record = { format: 1 as const, id: identity.id, rootId: identity.id, identity,
      startedAt: now, updatedAt: now, expiresAt: now + 60_000 };
    const presence = { hosts: [{ record, expiresAt: record.expiresAt }], participants: [] };
    await side.mirror(presence);
    await side.mirror(presence);
    expect(observations.filter(observation => observation.site === "mirror.prepare")).toEqual([
      expect.objectContaining({ optimized: false, fallback: true, attempt: 0, scope: "writeBatch.prepare" }),
      expect.objectContaining({ optimized: true, fallback: false, attempt: 0, scope: "writeBatch.prepare" }),
    ]);
    expect(side.holds(identity.id, identity)).toBe(true);
    const event = { topic: "fleet.work.fence", kind: "ask", from: identity, to: "factory",
      data: { bridge: { from: "far", id: "fence-stats-event" } } };
    await side.publish(event, [identity.id]);
    expect(mesh.read({ limit: 10 }).filter(event => event.topic === "fleet.work.fence")).toHaveLength(1);
    expect(observations.at(-1)).toMatchObject({ site: "heldFence", optimized: true, fallback: false });
    const unchanged = vi.spyOn(mesh, "stateUnchangedSince").mockReturnValue(false);
    await side.publish({ ...event, data: { bridge: { from: "far", id: "fence-stats-fallback" } } }, [identity.id]);
    unchanged.mockRestore();
    expect(observations.slice(-3)).toEqual([
      expect.objectContaining({ site: "heldFence", attempt: 0, optimized: true, fallback: false }),
      expect.objectContaining({ site: "heldFence", attempt: 1, optimized: true, fallback: false }),
      expect.objectContaining({ site: "heldFence", attempt: 2, optimized: false, fallback: true }),
    ]);
    expect(mesh.get(`topology/hosts/${createHash("sha256").update(identity.id).digest("hex")}`, { fresh: true })).toBeDefined();
    expect(mesh.read({ limit: 10 }).filter(event => event.topic === "fleet.work.fence")).toHaveLength(2);
  });
});
