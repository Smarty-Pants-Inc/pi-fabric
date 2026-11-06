import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BridgeChange } from "../src/mesh/change-notifier.js";
import { meshStateBusyFrom } from "../src/mesh/state-backend.js";
import type { MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { createHash } from "node:crypto";

// The real watchBridgeStore still runs; the seam only records the bridge's notification callback so a
// burst of separate fs notifications can be delivered deterministically.
const notify: { current?: (kind: BridgeChange) => void } = {};
vi.mock("../src/mesh/change-notifier.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/mesh/change-notifier.js")>();
  return {
    ...actual,
    watchBridgeStore: (root: string, changed: (kind: BridgeChange) => void, failed: (error: Error) => void) => {
      notify.current = changed;
      return actual.watchBridgeStore(root, changed, failed);
    },
  };
});

const { StoreBridgeSide } = await import("../src/mesh/bridge.js");
const { MeshStore } = await import("../src/mesh/store.js");

const roots: string[] = [];
const cleanups: Array<() => void> = [];
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0).reverse()) cleanup();
  for (const root of roots.splice(0)) fs.rmSync(root, { force: true, recursive: true });
  delete notify.current;
});

const immediate = () => new Promise<void>(resolve => setImmediate(resolve));
const macrotasks = async (n = 5) => { for (let i = 0; i < n; i += 1) await immediate(); };

const subscribe = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-coalesce-"));
  roots.push(root);
  const store = new MeshStore(path.join(root, "hub"), 64 * 1024, 2, { stateBackend: "sqlite" });
  cleanups.push(() => store.closeState());
  const side = new StoreBridgeSide(store, "far");
  const changes: BridgeChange[] = [];
  const failures: Error[] = [];
  const unsubscribe = side.subscribeChanges(kind => changes.push(kind), error => failures.push(error));
  cleanups.push(unsubscribe);
  // Unchanged captures still cross the real writer fence; count captures, not projection rebuilds.
  const scans = vi.spyOn(store, "withStateWriteFence");
  const fire = notify.current!;
  expect(fire).toBeTypeOf("function");
  // Let the real watcher deliver the store setup's own notifications before counting.
  await new Promise(resolve => setTimeout(resolve, 350));
  await macrotasks();
  scans.mockClear();
  changes.length = 0;
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  vi.spyOn(performance, "now").mockImplementation(() => Date.now());
  await vi.advanceTimersByTimeAsync(250);
  return { store, side, changes, failures, unsubscribe, scans, fire };
};

// Each fs.watch callback is a separate native callback: microtasks drain between them. `await
// Promise.resolve()` between notifications reproduces that (a microtask flush runs before it continues).
const burst = async (fire: (kind: BridgeChange) => void, n: number) => {
  for (let i = 0; i < n; i += 1) { fire("presence"); await Promise.resolve(); await Promise.resolve(); }
};

// Keep: recorded read-amplification incident smarty-dev#16879 and routed-work authority ordering.
// Only the scheduler clock is fake; SQLite fences and fs.watch remain real.
describe("bounded pre-lock scan coalescing", () => {
  it("fences no-op hints without topology reads and retains one 250 ms trailing scan", async () => {
    const f = await subscribe();
    const reads = vi.spyOn(f.store, "listAll");
    await burst(f.fire, 20);
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(1);
    expect(reads).not.toHaveBeenCalled();
    await burst(f.fire, 20);
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(249);
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(1);
    // Further hints must not restart the deadline (debounce starvation).
    await burst(f.fire, 20);
    await vi.advanceTimersByTimeAsync(1);
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(2);
    expect(reads).not.toHaveBeenCalled();
    expect(f.changes).toEqual([]);
    expect(f.failures).toEqual([]);
  });

  it("coalesces notifications arriving while a scan runs into at most one trailing scan", async () => {
    const f = await subscribe();
    const real = (Object.getPrototypeOf(f.store) as typeof f.store).withStateWriteFence.bind(f.store);
    let inside = 0;
    f.scans.mockImplementation(operation => {
      if (inside++ === 0) for (let i = 0; i < 10; i += 1) f.fire("presence");
      return real(operation);
    });
    await burst(f.fire, 10);
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(250);
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(500);
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(2);
  });

  it("forwards event notifications within one check phase, without a scan", async () => {
    const f = await subscribe();
    f.fire("events");
    f.fire("events");
    await immediate();
    expect(f.changes).toEqual(["events"]);
    expect(f.scans).not.toHaveBeenCalled();
  });

  it("delivers events within the 150 ms watchdog bound while a topology scan is coalesced", async () => {
    const f = await subscribe();
    f.fire("presence");
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(1);
    f.fire("presence");
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(1);

    const startedAt = Date.now();
    await f.store.publish({ topic: "ops.owner", kind: "pr.wake",
      from: { id: "session:source-00000000", name: "source", kind: "main" },
      to: "session:destination-00000000", data: { path: "coalesced-event" } });
    await vi.advanceTimersByTimeAsync(150);
    await macrotasks();
    expect(Date.now() - startedAt).toBeLessThanOrEqual(150);
    expect(f.changes).toEqual(["events"]);
    // The urgent event fence consumes, rather than duplicates, the already-pending scan.
    expect(f.scans).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(100);
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(2);
    expect(f.changes).toEqual(["events"]);
    expect(f.failures).toEqual([]);
  });

  it("classifies a pending lease before an actual routed event within the watchdog bound", async () => {
    const f = await subscribe();
    const id = "session:lane-00000000";
    const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: "lane" };
    const now = Date.now();
    const lease = (expiresAt: number) => writeHostLease(f.store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: Date.now(), expiresAt });
    await f.store.put({ key: `topology/hosts/${createHash("sha256").update(id).digest("hex")}`, identity,
      value: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 10_000 } });
    lease(now + 10_000);
    await vi.advanceTimersByTimeAsync(250);
    await macrotasks();
    f.changes.length = 0;
    f.scans.mockClear();
    // A heartbeat renewal is pending when actual routed work arrives.
    lease(now + 12_000);
    f.fire("presence");
    await Promise.resolve();
    const startedAt = Date.now();
    await f.store.publish({ topic: "ops.owner", kind: "pr.wake", from: identity, to: id,
      data: { path: "lease-before-authority" } });
    await vi.advanceTimersByTimeAsync(150);
    await macrotasks();
    expect(Date.now() - startedAt).toBeLessThanOrEqual(150);
    expect(f.changes).toEqual(["lease", "events"]);
    expect(f.scans).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(100);
    await macrotasks();
    expect(f.changes).toEqual(["lease", "events"]);
    expect(f.scans).toHaveBeenCalledTimes(1);
    // Lease-only burst: classified as a lease change (half-lease batching in MeshBridge), no events wake.
    f.changes.length = 0;
    lease(now + 14_000);
    f.fire("presence");
    await macrotasks();
    await vi.advanceTimersByTimeAsync(250);
    await macrotasks();
    expect(f.changes).toEqual(["lease"]);
    expect(f.failures).toEqual([]);
  });

  it("does not hold events behind a busy scan's backoff", async () => {
    const f = await subscribe();
    const real = (Object.getPrototypeOf(f.store) as typeof f.store).withStateWriteFence.bind(f.store);
    let busy = true;
    f.scans.mockImplementation(operation => {
      if (busy) throw meshStateBusyFrom("state.db", new Error("SQLITE_BUSY"));
      return real(operation);
    });
    f.fire("presence");
    await macrotasks();
    expect(f.scans.mock.calls.length).toBe(1);
    f.fire("events");
    await immediate();
    expect(f.changes).toEqual(["events"]);
    expect(f.scans.mock.calls.length).toBe(1);
    busy = false;
  });

  it("does no work for a queued scan after unsubscribe", async () => {
    const f = await subscribe();
    const presence = vi.spyOn(f.side, "presence");
    f.fire("presence");
    f.unsubscribe();
    await macrotasks();
    expect(f.scans).not.toHaveBeenCalled();
    expect(presence).not.toHaveBeenCalled();
    expect(f.changes).toEqual([]);
  });

  it("cancels a trailing timer and its pending event wake on unsubscribe", async () => {
    const f = await subscribe();
    f.fire("presence");
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(1);
    f.fire("presence");
    f.fire("events");
    f.unsubscribe();
    await vi.advanceTimersByTimeAsync(1_000);
    await macrotasks();
    expect(f.scans).toHaveBeenCalledTimes(1);
    expect(f.changes).toEqual([]);
  });

  it("keeps busy retry to one backoff timer while a burst arrives during the wait", async () => {
    const f = await subscribe();
    let busy = true;
    const real = (Object.getPrototypeOf(f.store) as typeof f.store).withStateWriteFence.bind(f.store);
    f.scans.mockImplementation(operation => {
      if (busy) throw meshStateBusyFrom("state.db", new Error("SQLITE_BUSY"));
      return real(operation);
    });
    const timers = vi.spyOn(globalThis, "setTimeout");
    f.fire("presence");
    await macrotasks();
    expect(f.scans.mock.calls.length).toBe(1);
    const scheduled = timers.mock.calls.length;
    expect(scheduled).toBeGreaterThanOrEqual(1);
    await burst(f.fire, 20);
    await macrotasks();
    // Notifications during the backoff wait neither scan nor add timers.
    expect(f.scans.mock.calls.length).toBe(1);
    expect(timers.mock.calls.length).toBe(scheduled);
    busy = false;
    await vi.advanceTimersByTimeAsync(1_000);
    await macrotasks();
    expect(f.scans.mock.calls.length).toBe(2);
    expect(f.failures).toEqual([]);
  });
});
