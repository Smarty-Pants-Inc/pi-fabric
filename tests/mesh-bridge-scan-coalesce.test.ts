import fs from "node:fs";
import os from "node:os";
import path from "node:path";
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
  // One stateRevision() per fenced capture attempt: each scan that proves its pre-lock snapshot.
  const scans = vi.spyOn(store, "stateRevision");
  const fire = notify.current!;
  expect(fire).toBeTypeOf("function");
  // Let the real watcher deliver the store setup's own notifications before counting.
  await new Promise(resolve => setTimeout(resolve, 60));
  await macrotasks();
  scans.mockClear();
  changes.length = 0;
  return { store, side, changes, failures, unsubscribe, scans, fire };
};

// Each fs.watch callback is a separate native callback: microtasks drain between them. `await
// Promise.resolve()` between notifications reproduces that (a microtask flush runs before it continues).
const burst = async (fire: (kind: BridgeChange) => void, n: number) => {
  for (let i = 0; i < n; i += 1) { fire("presence"); await Promise.resolve(); await Promise.resolve(); }
};

describe("pre-lock scan coalescing per notification burst", () => {
  it("scans once for N separate notifications of one burst, and observes the next burst", async () => {
    const f = await subscribe();
    await burst(f.fire, 20);
    await macrotasks();
    expect(f.scans.mock.calls.length).toBe(1);
    await burst(f.fire, 20);
    await macrotasks();
    expect(f.scans.mock.calls.length).toBe(2);
    expect(f.failures).toEqual([]);
  });

  it("coalesces notifications arriving while a scan runs into at most one trailing scan", async () => {
    const f = await subscribe();
    const revision = f.scans.getMockImplementation();
    let inside = 0;
    f.scans.mockImplementation(function (this: typeof f.store) {
      if (inside++ === 0) for (let i = 0; i < 10; i += 1) f.fire("presence");
      return revision ? revision.call(this) : (Object.getPrototypeOf(f.store) as typeof f.store).stateRevision.call(this);
    });
    await burst(f.fire, 10);
    await macrotasks();
    expect(f.scans.mock.calls.length).toBeLessThanOrEqual(2);
    expect(f.scans.mock.calls.length).toBeGreaterThanOrEqual(1);
  });

  it("forwards event notifications within one check phase, without a scan", async () => {
    const f = await subscribe();
    f.fire("events");
    f.fire("events");
    await immediate();
    expect(f.changes).toEqual(["events"]);
    expect(f.scans).not.toHaveBeenCalled();
  });

  it("classifies a burst's lease renewal before waking its events, so routed work refreshes authority", async () => {
    const f = await subscribe();
    const id = "session:lane-00000000";
    const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: "lane" };
    const now = Date.now();
    const lease = (expiresAt: number) => writeHostLease(f.store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: Date.now(), expiresAt });
    await f.store.put({ key: `topology/hosts/${createHash("sha256").update(id).digest("hex")}`, identity,
      value: { format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 10_000 } });
    lease(now + 10_000);
    await new Promise(resolve => setTimeout(resolve, 60));
    await macrotasks();
    f.changes.length = 0;
    // A heartbeat renewal, then actual work: one poll phase delivers the lease directory and events notifications.
    lease(now + 12_000);
    f.fire("presence");
    await Promise.resolve();
    f.fire("events");
    expect(f.changes).toEqual([]);
    await immediate();
    expect(f.changes).toEqual(["lease", "events"]);
    // Lease-only burst: classified as a lease change (half-lease batching in MeshBridge), no events wake.
    f.changes.length = 0;
    lease(now + 14_000);
    f.fire("presence");
    await macrotasks();
    expect(f.changes.filter(kind => kind !== "lease")).toEqual([]);
    expect(f.failures).toEqual([]);
  });

  it("does not hold events behind a busy scan's backoff", async () => {
    const f = await subscribe();
    const real = (Object.getPrototypeOf(f.store) as typeof f.store).stateRevision;
    let busy = true;
    f.scans.mockImplementation(function (this: typeof f.store) {
      if (busy) throw meshStateBusyFrom("state.db", new Error("SQLITE_BUSY"));
      return real.call(this);
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

  it("keeps busy retry to one backoff timer while a burst arrives during the wait", async () => {
    const f = await subscribe();
    let busy = true;
    const real = (Object.getPrototypeOf(f.store) as typeof f.store).stateRevision;
    f.scans.mockImplementation(function (this: typeof f.store) {
      if (busy) throw meshStateBusyFrom("state.db", new Error("SQLITE_BUSY"));
      return real.call(this);
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
    await new Promise(resolve => setTimeout(resolve, 150));
    await macrotasks();
    expect(f.scans.mock.calls.length).toBe(2);
    expect(f.failures).toEqual([]);
  });
});
