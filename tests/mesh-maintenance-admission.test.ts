import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshStore } from "../src/mesh/store.js";

// KEEP: data safety and contention liveness through the real publication boundary.
const roots: string[] = [];
const from = { id: "session:maintenance", name: "maintenance", kind: "main" as const };
const packet = { topic: "mesh.maintenance", from };
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-maintenance-admission-"));
  roots.push(root);
  return root;
};
const storeAt = (root: string) => new MeshStore(root, 1024, 100, { lockTimeoutMs: 100, maxEventLogBytes: 64 * 1024 });

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("optional receipt maintenance admission", () => {
  it.each(["ordinary", "receipt"] as const)("publishes without stale-scan errors or reacquisition through %s writer interleavings", async contender => {
    const root = scratch(), observer = storeAt(root), writer = storeAt(root);
    const original = await writer.publish({ ...packet, dedupeKey: "keep" });
    const withLock = MeshLock.prototype.withLock;
    let injecting = false, attempts = 0;
    vi.spyOn(MeshLock.prototype, "withLock").mockImplementation((async function(this: MeshLock, ...args: Parameters<typeof withLock>) {
      if (this.root === root && !injecting) {
        attempts++;
        injecting = true;
        try {
          // A real writer commits between the observer's preparation and custody on
          // EVERY attempt. Ordinary appends change only the reservation counter;
          // keyed appends also change the receipt namespace and finish their intent.
          await writer.publish({ ...packet, ...(contender === "receipt" ? { dedupeKey: `racer-${attempts}` } : {}) });
        } finally { injecting = false; }
      }
      return withLock.apply(this, args);
    }) as typeof withLock);
    const admit = vi.fn();
    const outcome = await observer.publish({ ...packet, text: "observer", admit }).then(event => ({ event, error: undefined }), error => ({ event: undefined, error }));
    expect(outcome.error).toBeUndefined();
    expect(attempts).toBe(1);
    expect(admit).toHaveBeenCalledTimes(1);
    vi.restoreAllMocks();
    expect(observer.read().filter(event => event.id === outcome.event!.id)).toHaveLength(1);
    expect(await writer.publish({ ...packet, dedupeKey: "keep" })).toEqual(original);
  });

  it("waits for actual canonical custody once rather than demanding a quiescent preflight", async () => {
    const root = scratch(), store = storeAt(root);
    const lock = new MeshLock(root, { lockTimeoutMs: 100 }, () => undefined);
    let release!: () => void, entered!: () => void;
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const gate = new Promise<void>(resolve => { release = resolve; });
    const holding = lock.withLockAcrossAwait(async () => { entered(); await gate; });
    await ready;
    const acquisitions = vi.spyOn(MeshLock.prototype, "withLock");
    const publishing = store.publish(packet);
    release();
    await holding;
    const event = await publishing;
    expect(acquisitions).toHaveBeenCalledTimes(1);
    expect(store.read()).toEqual([event]);
  });

  it("skips an expiry pass overtaken by an ordinary publisher without rejecting the committed event", async () => {
    const root = scratch(), writer = storeAt(root);
    const first = await writer.publish({ ...packet, dedupeKey: "keep" });
    for (let index = 0; index < 8; index++) await writer.publish({ ...packet, text: "x".repeat(600) });
    const store = new MeshStore(root, 1024, 100, { maxEventLogBytes: 4096, retainedEventLogBytes: 1025 });
    const withLock = MeshLock.prototype.withLock;
    let injecting = false, acquisitions = 0;
    vi.spyOn(MeshLock.prototype, "withLock").mockImplementation((async function(this: MeshLock, ...args: Parameters<typeof withLock>) {
      if (this.root === root && !injecting && ++acquisitions === 2) {
        injecting = true;
        try { await writer.publish({ ...packet, text: "overtakes expiry" }); }
        finally { injecting = false; }
      }
      return withLock.apply(this, args);
    }) as typeof withLock);
    const event = await store.publish({ ...packet, text: "committed before expiry" });
    expect(acquisitions).toBe(2);
    expect(Number(fs.readFileSync(path.join(root, "generation"), "utf8"))).toBeGreaterThan(0);
    vi.restoreAllMocks();
    expect(store.read().filter(current => current.id === event.id)).toEqual([event]);
    expect(await writer.publish({ ...packet, dedupeKey: "keep" })).toEqual(first);
  });

  it("does not compact against a stale pending-intent snapshot; the next fresh pass settles before rewriting", async () => {
    const root = scratch(), writer = storeAt(root);
    for (let index = 0; index < 8; index++) await writer.publish({ ...packet, text: "x".repeat(600) });
    const store = new MeshStore(root, 1024, 100, { maxEventLogBytes: 4096, retainedEventLogBytes: 1025 });
    const withLock = MeshLock.prototype.withLock;
    let injected = false, original!: Awaited<ReturnType<MeshStore["publish"]>>;
    const hash = createHash("sha256").update("anchored").digest("hex");
    const receipt = path.join(root, "event-receipts", `${hash}.json`);
    const pending = path.join(root, "event-receipts", `${hash}.pending.json`);
    vi.spyOn(MeshLock.prototype, "withLock").mockImplementation((async function(this: MeshLock, ...args: Parameters<typeof withLock>) {
      if (this.root === root && !injected) {
        injected = true;
        const liveOffset = fs.statSync(path.join(root, "events.jsonl")).size;
        original = await writer.publish({ ...packet, dedupeKey: "anchored" });
        fs.writeFileSync(pending, JSON.stringify({ dedupeKey: "anchored", eventId: original.id, reservedSequence: original.sequence, liveOffset }));
        fs.rmSync(receipt);
      }
      return withLock.apply(this, args);
    }) as typeof withLock);
    await store.publish(packet);
    expect(fs.existsSync(path.join(root, "generation"))).toBe(false);
    expect(fs.existsSync(pending)).toBe(true);
    vi.restoreAllMocks();
    await store.publish(packet);
    expect(Number(fs.readFileSync(path.join(root, "generation"), "utf8"))).toBeGreaterThan(0);
    expect(fs.existsSync(pending)).toBe(false);
    expect(await writer.publish({ ...packet, dedupeKey: "anchored" })).toEqual(original);
  });
});
