import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { MeshStore, MESH_MAX_PENDING_SCHEDULES, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MeshProvider, meshScheduleDueAt } from "../src/providers/mesh-provider.js";
import { residentIdleDecision } from "../src/residency/host.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const roots: string[] = [];
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const context = {} as FabricInvocationContext;
const participants: FabricParticipantSource = {
  list: () => [], get: () => undefined, self: () => undefined as never, peers: () => [],
  async refresh() {}, scheduleRefresh() {},
};

const meshRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-schedule-"));
  roots.push(root);
  return root;
};

afterEach(() => {
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("mesh schedules", () => {
  it("replaces a pending schedule by key and cancels it with unschedule", async () => {
    const store = new MeshStore(meshRoot(), 64 * 1024, 100);
    const now = Date.now();
    const first = await store.schedule({ topic: "jobs", from: identity, text: "one", dueAt: now + 60_000, key: "nightly" });
    const second = await store.schedule({ topic: "jobs", from: identity, text: "two", dueAt: now + 120_000, key: "nightly" });
    await store.schedule({ topic: "other", from: identity, dueAt: now + 30_000 });
    expect(second.id).not.toBe(first.id);
    expect(store.scheduled({ topic: "jobs" })).toEqual([expect.objectContaining({ key: "nightly", text: "two" })]);
    expect(store.scheduled().map((entry) => entry.topic)).toEqual(["other", "jobs"]);
    expect(store.nextScheduleDueAt()).toBe(now + 30_000);
    await expect(store.unschedule("nightly")).resolves.toEqual({ removed: true });
    await expect(store.unschedule("nightly")).resolves.toEqual({ removed: false });
    expect(store.scheduled({ topic: "jobs" })).toEqual([]);
    expect(store.read({ topic: "jobs" })).toEqual([]);
  });

  it("concurrent same-key publishers leave exactly one schedule", async () => {
    const root = meshRoot();
    const stores = [0, 1, 2, 3].map(() => new MeshStore(root, 64 * 1024, 100));
    const dueAt = Date.now() + 60_000;
    await Promise.all(stores.map((store, index) =>
      store.schedule({ topic: "jobs", from: identity, data: { index }, dueAt, key: "same" })));
    expect(stores[0]!.scheduled()).toHaveLength(1);
  });

  it("releases each due schedule exactly once across concurrent releasers", async () => {
    const root = meshRoot();
    const writer = new MeshStore(root, 64 * 1024, 100);
    const now = Date.now();
    // Due times stay in the real future so the scheduling writes release nothing;
    // the releasers pass a clock past them.
    for (let index = 0; index < 5; index++) {
      await writer.schedule({ topic: "wake", from: identity, data: { index }, dueAt: now + 600_000 + index });
    }
    await writer.schedule({ topic: "wake", from: identity, data: { later: true }, dueAt: now + 3_600_000 });
    const releasers = [0, 1, 2, 3].map(() => new MeshStore(root, 64 * 1024, 100));
    const released = await Promise.all(releasers.map((store) => store.releaseDueSchedules(now + 700_000)));
    expect(released.flat()).toHaveLength(5);
    const events = writer.read({ topic: "wake" });
    expect(events.map((event) => (event.data as { index: number }).index)).toEqual([0, 1, 2, 3, 4]);
    // The event id is the schedule id, so a crash-retried release is detectable.
    expect(new Set(events.map((event) => event.id)).size).toBe(5);
    expect(events[0]).toMatchObject({ scheduled: { dueAt: now + 600_000 }, from: identity });
    expect(writer.scheduled()).toEqual([expect.objectContaining({ data: { later: true } })]);
    await expect(writer.releaseDueSchedules(now + 700_000)).resolves.toEqual([]);
  });

  it("releases due schedules before an ordinary publish so due order is kept", async () => {
    const store = new MeshStore(meshRoot(), 64 * 1024, 100);
    await store.schedule({ topic: "t", from: identity, text: "scheduled", dueAt: Date.now() - 1 });
    // A past due time is released by the scheduling write itself.
    expect(store.scheduled()).toEqual([]);
    await store.schedule({ topic: "t", from: identity, text: "soon", dueAt: Date.now() + 20 });
    await new Promise((resolve) => setTimeout(resolve, 30));
    await store.publish({ topic: "t", from: identity, text: "plain" });
    expect(store.read({ topic: "t" }).map((event) => event.text)).toEqual(["scheduled", "soon", "plain"]);
  });

  it("enforces bounds and fails closed on damaged schedule state", async () => {
    const root = meshRoot();
    const store = new MeshStore(root, 4 * 1024, 100);
    const now = Date.now();
    await expect(store.schedule({ topic: "t", from: identity, dueAt: now + 367 * 86_400_000 })).rejects.toThrow("366 days");
    await expect(store.schedule({ topic: "t", from: identity, dueAt: now + 1_000, key: "bad key" })).rejects.toThrow("schedule key");
    await expect(store.schedule({ topic: "t", from: identity, dueAt: now + 1_000, data: "x".repeat(8 * 1024) }))
      .rejects.toThrow("exceeds 4096 bytes");
    const pending = Array.from({ length: MESH_MAX_PENDING_SCHEDULES }, (_, index) => ({
      id: `id-${index}`, topic: "t", kind: "message", from: identity, dueAt: now + 60_000, createdAt: now,
    }));
    fs.writeFileSync(path.join(root, "schedules.json"), JSON.stringify({ format: 1, schedules: pending }));
    await expect(store.schedule({ topic: "t", from: identity, dueAt: now + 1_000 })).rejects.toThrow("At most 1000");
    // Replacing by key does not count the replaced schedule.
    pending[0] = { ...pending[0]!, key: "k" } as typeof pending[number];
    fs.writeFileSync(path.join(root, "schedules.json"), JSON.stringify({ format: 1, schedules: pending }));
    await expect(store.schedule({ topic: "t", from: identity, dueAt: now + 1_000, key: "k" })).resolves.toMatchObject({ key: "k" });

    fs.writeFileSync(path.join(root, "schedules.json"), "{damaged");
    expect(store.scheduled()).toEqual([]);
    await expect(store.schedule({ topic: "t", from: identity, dueAt: now + 1_000 })).rejects.toThrow("Failed to read Fabric mesh schedules");
    await expect(store.unschedule("k")).rejects.toThrow("Failed to read Fabric mesh schedules");
    // Damaged schedules never block ordinary publishing.
    await expect(store.publish({ topic: "t", from: identity, text: "still works" })).resolves.toMatchObject({ text: "still works" });
    expect(fs.readFileSync(path.join(root, "schedules.json"), "utf8")).toBe("{damaged");
  });

  it("schedules through mesh.publish and releases on mesh.read", async () => {
    const store = new MeshStore(meshRoot(), 64 * 1024, 100);
    const provider = new MeshProvider(store, identity, participants);
    const scheduled = await provider.invoke("publish", { topic: "jobs", kind: "tick", afterMs: 300, key: "tick" }, context);
    expect(scheduled).toMatchObject({ scheduled: true, key: "tick", topic: "jobs", kind: "tick" });
    await expect(provider.invoke("scheduled", { topic: "jobs" }, context)).resolves.toHaveLength(1);
    await expect(provider.invoke("publish", { topic: "jobs", key: "tick" }, context)).rejects.toThrow("key requires");
    await expect(provider.invoke("publish", { topic: "fabric.control.x", afterMs: 1 }, context)).rejects.toThrow("reserved");
    await new Promise((resolve) => setTimeout(resolve, 350));
    const events = await provider.invoke("read", { topic: "jobs" }, context) as Array<{ kind: string }>;
    expect(events).toEqual([expect.objectContaining({ kind: "tick" })]);
    await expect(provider.invoke("unschedule", { key: "tick" }, context)).resolves.toEqual({ removed: false });
    expect((await provider.describe("scheduled", context))?.risk).toBe("read");
  });

  it("resolves notBefore as epoch milliseconds or ISO and rejects ambiguity", () => {
    expect(meshScheduleDueAt({ notBefore: 1_000 }, 0)).toBe(1_000);
    expect(meshScheduleDueAt({ notBefore: "2030-01-02T03:04:05Z" })).toBe(Date.parse("2030-01-02T03:04:05Z"));
    expect(meshScheduleDueAt({ afterMs: 250 }, 1_000)).toBe(1_250);
    expect(meshScheduleDueAt({}, 0)).toBeUndefined();
    expect(() => meshScheduleDueAt({ notBefore: "tomorrow" })).toThrow("ISO 8601");
    expect(() => meshScheduleDueAt({ notBefore: 1, afterMs: 1 })).toThrow("not both");
  });

  it("an actor mesh monitor releases due schedules and wakes at the next due time", async () => {
    const store = new MeshStore(meshRoot(), 64 * 1024, 100);
    const events: string[] = [];
    const monitor = new ActorMeshMonitor(store, { enabled: true, actorPollMs: 60_000, maxReadEvents: 100 }, {
      beforePoll: () => true,
      onEvent: (event) => { events.push(String(event.text)); return true; },
    });
    try {
      monitor.start();
      await store.schedule({ topic: "t", from: identity, text: "wake", dueAt: Date.now() + 150 });
      await vi.waitFor(() => expect(events).toEqual(["wake"]), { timeout: 3_000, interval: 20 });
      expect(store.scheduled()).toEqual([]);
    } finally {
      monitor.close();
    }
  });
});

describe("resident host idle rule", () => {
  const base = {
    now: 100_000, idleSince: 0, activeActor: false, activeAgent: false, pendingRequest: false,
    durableParticipants: false, nextScheduleDueAt: undefined, idleExitMs: 30_000,
  };

  it("exits after the grace period with no work", () => {
    expect(residentIdleDecision(base)).toEqual({ busy: false, exit: true });
    expect(residentIdleDecision({ ...base, idleSince: 90_000 })).toEqual({ busy: false, exit: false });
  });

  it("stays alive for pending schedules only with a durable participant, re-armed to the due time", () => {
    expect(residentIdleDecision({ ...base, nextScheduleDueAt: 500_000 })).toEqual({ busy: false, exit: true });
    expect(residentIdleDecision({ ...base, durableParticipants: true, nextScheduleDueAt: 500_000 }))
      .toEqual({ busy: true, exit: false, wakeAt: 500_000 });
    expect(residentIdleDecision({ ...base, activeActor: true, durableParticipants: true, nextScheduleDueAt: 1 }))
      .toMatchObject({ busy: true, wakeAt: 1 });
  });

  it("keeps the existing activity rule", () => {
    for (const key of ["activeActor", "activeAgent", "pendingRequest"] as const) {
      expect(residentIdleDecision({ ...base, [key]: true })).toEqual({ busy: true, exit: false });
    }
  });
});
