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
