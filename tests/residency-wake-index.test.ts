import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { MeshArchive } from "../src/mesh/archive.js";
import { residentRoot } from "../src/residency/protocol.js";
import * as wake from "../src/residency/wake.js";
import * as index from "../src/residency/wake-index.js";
import * as atomic from "../src/core/atomic-write.js";
import { canonicalResidentWakeConfig } from "../src/residency/wake-index.js";

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-index-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100);
  const resident = residentRoot(mesh.root, "session:index");
  fs.mkdirSync(resident, { recursive: true });
  const config = { rootId: "session:index", residencyRoot: resident, cwd: root };
  fs.writeFileSync(path.join(resident, "config.json"), JSON.stringify(config));
  fs.writeFileSync(path.join(resident, "wake-routes.json"), JSON.stringify({ format: 1, rootId: "session:index", hostId: "host:index",
    configJson: canonicalResidentWakeConfig(config),
    actors: [{ id: "listener", name: "listener", topics: ["indexed.delivery"] }] }));
  return { root, mesh, resident, from: { id: "publisher", name: "publisher", kind: "main" as const } };
};

describe("archive-coupled resident wake retry index", () => {
  it.each(["memory", "disk"] as const)("double write failure retries exactly once on an unrelated commit from the %s index", async source => {
    const f = fixture();
    let fail = true;
    let requestFailures = 0, receiptFailures = 0;
    let beforeFailure = "";
    const write = atomic.writeJsonAtomic;
    const launch = vi.fn(async () => {
      fs.writeFileSync(wake.residentSleepingPath(f.resident), JSON.stringify({ request: wake.readWakeJson(wake.residentWakeRequestPath(f.resident)) }));
    });
    let dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, events) => dispatch(store, events, launch));
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(atomic, "writeJsonAtomic").mockImplementation((file, ...args) => {
      if (fail && [wake.residentWakeRequestPath(f.resident), path.join(f.resident, "wake-failure.json")].includes(file)) {
        // Journal already durably exists on the very first attempted wake write.
        const bytes = fs.readFileSync(index.residentWakeIndexPath(f.mesh.root), "utf8");
        expect(JSON.parse(bytes.trim().split("\n").at(-1)!).op).toBe("pending");
        beforeFailure ||= bytes;
        if (file.endsWith("wake-request.json")) requestFailures++; else receiptFailures++;
        throw new Error("injected root-local storage failure");
      }
      return write(file, ...args);
    });
    try {
      await wake.ensureResidentWakeArchive(f.mesh);
      const delivered = await f.mesh.publish({ topic: "indexed.delivery", from: f.from });
      expect(MeshArchive.fromRoot(f.mesh.root)?.lookupEntry(delivered.sequence)?.event.id).toBe(delivered.id);
      expect(requestFailures).toBe(1); expect(receiptFailures).toBe(1);
      expect(launch).not.toHaveBeenCalled();
      expect(fs.existsSync(wake.residentWakeRequestPath(f.resident))).toBe(false);
      expect(fs.existsSync(path.join(f.resident, "wake-failure.json"))).toBe(false);
      expect(fs.readFileSync(index.residentWakeIndexPath(f.mesh.root), "utf8")).toBe(beforeFailure);
      expect(index.residentUnacknowledgedDeliveries(f.mesh.root).get(f.resident)?.id).toBe(delivered.id);
      fail = false;
      if (source === "disk") {
        vi.resetModules();
        const freshIndex = await import("../src/residency/wake-index.js");
        expect(freshIndex.residentUnacknowledgedDeliveries(f.mesh.root).get(f.resident)?.id).toBe(delivered.id);
        const freshWake = await import("../src/residency/wake.js");
        dispatch = freshWake.wakeResidentActors;
        vi.spyOn(freshWake, "wakeResidentActors").mockImplementation((store, events) => dispatch(store, events, launch));
      }
      // No retry needs historical archive lookup or mesh event reads.
      vi.spyOn(MeshArchive.prototype, "lookupEntry").mockImplementation(() => { throw new Error("archive scan forbidden"); });
      vi.spyOn(f.mesh, "read").mockImplementation(() => { throw new Error("mesh event scan forbidden"); });
      await f.mesh.publish({ topic: "unrelated.retry.trigger", from: f.from });
      expect(launch).toHaveBeenCalledOnce();
      expect(wake.readWakeJson<{ id: string }>(wake.residentWakeRequestPath(f.resident))?.id).toBe(delivered.id);
      expect(index.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      await f.mesh.publish({ topic: "unrelated.after.retry", from: f.from });
      expect(launch).toHaveBeenCalledOnce();
      expect(requestFailures).toBe(1); expect(receiptFailures).toBe(1);
    } finally { vi.restoreAllMocks(); f.mesh.closeState(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });

  it("retains a delivery refused for config mismatch and retries once after config repair", async () => {
    const f = fixture();
    const configFile = path.join(f.resident, "config.json");
    const saved = fs.readFileSync(configFile, "utf8");
    const activeWake = await import("../src/residency/wake.js"); // the disk-reload case resets the module graph
    const launch = vi.fn(async () => {
      fs.writeFileSync(wake.residentSleepingPath(f.resident), JSON.stringify({ request: wake.readWakeJson(wake.residentWakeRequestPath(f.resident)) }));
    });
    const dispatch = activeWake.wakeResidentActors;
    vi.spyOn(activeWake, "wakeResidentActors").mockImplementation((store, events) => dispatch(store, events, launch));
    vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await wake.ensureResidentWakeArchive(f.mesh);
      fs.writeFileSync(configFile, JSON.stringify({ ...JSON.parse(saved), actorRoot: "/foreign-actor-root" }));
      const event = await f.mesh.publish({ topic: "indexed.delivery", from: f.from });
      expect(launch).not.toHaveBeenCalled();
      expect(index.routesAt(f.resident)).toBeUndefined();
      expect(MeshArchive.fromRoot(f.mesh.root)?.lookupEntry(event.sequence)?.event.id).toBe(event.id);
      expect(wake.readWakeJson<{ delivery: { id: string }; error: string }>(path.join(f.resident, "wake-failure.json"))).toMatchObject({
        delivery: { id: event.id }, error: expect.stringContaining("ResidentWakeConfigMismatch"),
      });
      fs.writeFileSync(configFile, saved);
      await f.mesh.publish({ topic: "unrelated.config-repaired", from: f.from });
      expect(launch).toHaveBeenCalledOnce();
      expect(wake.readWakeJson<{ id: string }>(wake.residentWakeRequestPath(f.resident))?.id).toBe(event.id);
      await f.mesh.publish({ topic: "unrelated.after-repair", from: f.from });
      expect(launch).toHaveBeenCalledOnce();
    } finally { vi.restoreAllMocks(); f.mesh.closeState(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });

  it("compacts the append-only journal within its byte bound without losing the outstanding watermark", () => {
    const f = fixture();
    try {
      for (let sequence = 1; sequence <= 700; sequence++) index.indexResidentDeliveries(f.mesh.root, {
        id: `event-${sequence}`, sequence, topic: "indexed.delivery", kind: "event", from: f.from, createdAt: sequence,
      }, []);
      expect(fs.statSync(index.residentWakeIndexPath(f.mesh.root)).size).toBeLessThanOrEqual(index.RESIDENT_WAKE_INDEX_MAX_BYTES);
      expect(index.residentUnacknowledgedDeliveries(f.mesh.root).get(f.resident)).toEqual({ id: "event-700", sequence: 700 });
      index.acknowledgeResidentDelivery(f.mesh.root, f.resident, { id: "older", sequence: 699 });
      expect(index.residentUnacknowledgedDeliveries(f.mesh.root).get(f.resident)?.sequence).toBe(700);
      index.acknowledgeResidentDelivery(f.mesh.root, f.resident, { id: "event-700", sequence: 700 });
      expect(index.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
    } finally { f.mesh.closeState(); fs.rmSync(f.root, { recursive: true, force: true }); }
  }, 60_000); // 700 real namespace/fsync barriers, like the durable fanout cases below.

  it.each([
    [128, "single"], [129, "single"], [257, "single"], [129, "batch"], [129, "matching-retry"],
  ] as const)("publishes to %i roots via %s without partial append and drains overflow on the next delivery", async (count, mode) => {
    const f = fixture();
    const residents = [f.resident];
    for (let n = 1; n < count; n++) {
      const rootId = `session:index-${n}`;
      const resident = residentRoot(f.mesh.root, rootId);
      const config = { rootId, residencyRoot: resident, cwd: f.root };
      fs.mkdirSync(resident, { recursive: true });
      fs.writeFileSync(path.join(resident, "config.json"), JSON.stringify(config));
      fs.writeFileSync(path.join(resident, "wake-routes.json"), JSON.stringify({ format: 1, rootId, hostId: `host:${n}`,
        configJson: canonicalResidentWakeConfig(config),
        actors: [{ id: `listener-${n}`, name: `listener-${n}`, topics: ["indexed.delivery"] }] }));
      residents.push(resident);
    }
    const activeWake = await import("../src/residency/wake.js");
    const launch = vi.fn(async (_file: string, config: { residencyRoot: string }) => {
      fs.writeFileSync(wake.residentSleepingPath(config.residencyRoot), JSON.stringify({
        request: wake.readWakeJson(wake.residentWakeRequestPath(config.residencyRoot)),
      }));
    });
    const dispatch = activeWake.wakeResidentActors;
    vi.spyOn(activeWake, "wakeResidentActors").mockImplementation((store, events) => dispatch(store, events, launch));
    try {
      await wake.ensureResidentWakeArchive(f.mesh);
      // Reproduce the review's full journal with already-outstanding watermarks.
      if (mode !== "matching-retry") index.indexResidentDeliveries(f.mesh.root, { id: "prior", sequence: 0, topic: "indexed.delivery", kind: "event",
        from: f.from, createdAt: 0 }, []);
      const packet = { topic: "indexed.delivery", from: f.from };
      const event = mode === "batch" ? (await f.mesh.publishBatch([packet]))[0]! : await f.mesh.publish(packet);
      expect(launch).toHaveBeenCalledTimes(128);
      expect(f.mesh.read()).toEqual([event]);
      expect(MeshArchive.fromRoot(f.mesh.root)?.lookupEntry(event.sequence)?.event.id).toBe(event.id);
      const remaining = index.residentUnacknowledgedDeliveries(f.mesh.root);
      expect(remaining.size).toBe(count - 128);
      expect([...remaining.values()].every(delivery => delivery.id === event.id)).toBe(true);
      expect(fs.statSync(index.residentWakeIndexPath(f.mesh.root)).size).toBeLessThanOrEqual(index.RESIDENT_WAKE_INDEX_MAX_BYTES);
      if (mode === "matching-retry") {
        const overflowRoot = [...remaining.keys()][0]!;
        const next = await f.mesh.publish(packet); // all 128 earlier roots match AGAIN
        expect(launch).toHaveBeenCalledTimes(256);
        expect(wake.readWakeJson<{ id: string }>(wake.residentWakeRequestPath(overflowRoot))?.id).toBe(next.id);
        expect(index.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(1);
        await f.mesh.publish({ topic: "unrelated.final-matching-drain", from: f.from });
        expect(launch).toHaveBeenCalledTimes(257);
        expect(index.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
        for (const committed of f.mesh.read()) {
          expect(MeshArchive.fromRoot(f.mesh.root)?.lookupEntry(committed.sequence)?.event.id).toBe(committed.id);
        }
        return;
      }
      if (count > 128) {
        // Read the spill from a fresh module, not a process-local overflow cache.
        vi.resetModules();
        const freshIndex = await import("../src/residency/wake-index.js");
        expect(freshIndex.residentUnacknowledgedDeliveries(f.mesh.root)).toEqual(remaining);
        const freshWake = await import("../src/residency/wake.js");
        const drain = freshWake.wakeResidentActors;
        vi.spyOn(freshWake, "wakeResidentActors").mockImplementation((store, events) => drain(store, events, launch));
        await f.mesh.publish({ topic: "unrelated.overflow-drain", from: f.from });
        expect(launch).toHaveBeenCalledTimes(Math.min(count, 256));
        if (count > 256) await f.mesh.publish({ topic: "unrelated.final-drain", from: f.from });
        expect(launch).toHaveBeenCalledTimes(count);
        expect(freshIndex.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      }
      for (const resident of residents) {
        expect(wake.readWakeJson<{ id: string }>(wake.residentWakeRequestPath(resident))?.id).toBe(event.id);
      }
      for (const committed of f.mesh.read()) {
        expect(MeshArchive.fromRoot(f.mesh.root)?.lookupEntry(committed.sequence)?.event.id).toBe(committed.id);
      }
    } finally { vi.restoreAllMocks(); f.mesh.closeState(); fs.rmSync(f.root, { recursive: true, force: true }); }
  }, 60_000); // Durable fanout performs hundreds of real namespace/fsync barriers.

  it.each(["single", "batch"] as const)("refuses a wake-index storage error before any %s live/archive append", async mode => {
    const f = fixture();
    try {
      await wake.ensureResidentWakeArchive(f.mesh);
      fs.writeFileSync(index.residentWakeIndexPath(f.mesh.root), "x".repeat(index.RESIDENT_WAKE_INDEX_MAX_BYTES + 1));
      const packet = { topic: "indexed.delivery", from: f.from };
      await expect(mode === "batch" ? f.mesh.publishBatch([packet]) : f.mesh.publish(packet)).rejects.toThrow("bounded read");
      expect(f.mesh.read()).toEqual([]);
      expect(MeshArchive.fromRoot(f.mesh.root)?.head()).toBeUndefined();
      expect(MeshArchive.fromRoot(f.mesh.root)?.pending()).toBeUndefined();
      expect(fs.existsSync(path.join(f.mesh.root, "wake-archive", "sequence-index"))).toBe(false);
      expect(fs.existsSync(path.join(f.mesh.root, "events.jsonl"))).toBe(false);
    } finally { f.mesh.closeState(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });

  it("refuses an oversized disk index before reading its bytes", () => {
    const f = fixture();
    try {
      fs.writeFileSync(index.residentWakeIndexPath(f.mesh.root), "x".repeat(index.RESIDENT_WAKE_INDEX_MAX_BYTES + 1));
      const read = vi.spyOn(fs, "readFileSync");
      expect(() => index.residentUnacknowledgedDeliveries(f.mesh.root)).toThrow("bounded read");
      expect(read).not.toHaveBeenCalled();
    } finally { vi.restoreAllMocks(); f.mesh.closeState(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });
});
