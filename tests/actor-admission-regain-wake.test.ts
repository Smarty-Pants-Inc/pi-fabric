import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshEvent, type MeshIdentity, type MeshTailResult } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";

// Admission regain: a mesh event retained while canConsumeMesh is false must be claimed
// promptly, once, on the real committed refresh false -> true edge, with no new publish,
// no explicit test schedule and no slow tick.
const roots: string[] = [];
const closers: Array<() => unknown> = [];
afterEach(async () => {
  for (const close of closers.splice(0)) await close();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const flush = async () => { for (let index = 0; index < 20; index++) await new Promise(resolve => setImmediate(resolve)); };

function setup() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-admission-regain-"));
  roots.push(root);
  const identity: MeshIdentity = { id: "session:regain", name: "Regain", kind: "main" };
  let monitor: ActorMeshMonitor | undefined;
  const onCommittedReady = vi.fn(() => monitor?.schedule());
  const directory = new ParticipantDirectory(new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000), {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 120_000,
    onCommittedReady,
  });
  directory.registerSource(() => []);
  closers.push(() => directory.close());
  const event = { topic: "actor.wake", offset: 1 } as unknown as MeshEvent;
  let delivered = false;
  const mesh = {
    root: path.join(root, "monitor"),
    latestOffset: vi.fn(() => 10),
    tail: vi.fn((): MeshTailResult => delivered ? { events: [], nextOffset: 10 } : { events: [event], nextOffset: 10 }),
  };
  fs.mkdirSync(mesh.root, { recursive: true });
  const onEvent = vi.fn((_event: MeshEvent) => { delivered = true; });
  const beforePoll = vi.fn(() => directory.canConsumeMesh());
  // actorPollMs is huge: no slow tick or retry timer can fire within the test.
  monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 3_600_000, maxReadEvents: 7 },
    { cursorPath: path.join(root, "cursor.json"), beforePoll, onEvent });
  closers.push(() => monitor!.close());
  return { directory, monitor, onEvent, beforePoll, onCommittedReady, event };
}

describe("committed-refresh admission regain wakes the mesh monitor", () => {
  it("retains an event while the gate is false and claims it once on the real committed edge", async () => {
    const s = setup();
    // Original gate-false counterexample: an explicit pass while not admitted claims nothing.
    s.monitor.schedule();
    await flush();
    expect(s.directory.canConsumeMesh()).toBe(false);
    expect(s.beforePoll).toHaveBeenCalled();
    expect(s.onEvent).not.toHaveBeenCalled();
    expect(s.onCommittedReady).not.toHaveBeenCalled();

    // Actual committed refresh: false -> true edge wakes the monitor, no new publish.
    await s.directory.refresh();
    expect(s.directory.canConsumeMesh()).toBe(true);
    expect(s.onCommittedReady).toHaveBeenCalledTimes(1);
    await flush();
    expect(s.onEvent).toHaveBeenCalledTimes(1);
    expect(s.onEvent.mock.calls[0]![0]).toBe(s.event);

    // Steady heartbeat refreshes are not edges.
    await s.directory.refresh();
    await flush();
    expect(s.onCommittedReady).toHaveBeenCalledTimes(1);
    expect(s.onEvent).toHaveBeenCalledTimes(1);
  });

  it("a failed write never invokes the hook; the rightful recovery wakes exactly once", async () => {
    const s = setup();
    await s.directory.refresh();
    expect(s.onCommittedReady).toHaveBeenCalledTimes(1);
    const failure = new Error("heartbeat write failed");
    const heartbeat = vi.spyOn(s.directory.mesh, "confirmWritable").mockRejectedValueOnce(failure);
    await expect(s.directory.refresh()).rejects.toBe(failure);
    heartbeat.mockRestore();
    expect(s.directory.canConsumeMesh()).toBe(false);
    expect(s.onCommittedReady).toHaveBeenCalledTimes(1);
    await s.directory.refresh();
    expect(s.directory.canConsumeMesh()).toBe(true);
    expect(s.onCommittedReady).toHaveBeenCalledTimes(2);
  });

  it("a throwing hook does not falsify the commit receipt", async () => {
    const s = setup();
    s.onCommittedReady.mockImplementationOnce(() => { throw new Error("hook failed"); });
    await expect(s.directory.refresh()).resolves.toBeUndefined();
    expect(s.directory.canConsumeMesh()).toBe(true);
  });

  it("closed directories never fire the hook", async () => {
    const s = setup();
    await s.directory.close();
    await s.directory.refresh().catch(() => undefined);
    expect(s.onCommittedReady).not.toHaveBeenCalled();
  });
});
