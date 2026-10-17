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

describe("append-locked parsed resident routes", () => {
  const setup = () => {
    const f = fixture();
    f.mesh.closeState();
    const mesh = new MeshStore(f.mesh.root, 65_536, 100, { canWakeResidents: () => false });
    const file = path.join(f.resident, "wake-routes.json");
    const original = fs.readFileSync(file, "utf8");
    const changed = original.replace("indexed.delivery", "changed.delivery");
    const stat = fs.statSync(file);
    const clock = vi.spyOn(Date, "now").mockReturnValue(Math.ceil(Math.max(stat.mtimeMs, stat.ctimeMs)) + 60_000);
    let parses = 0;
    const parse = JSON.parse;
    vi.spyOn(JSON, "parse").mockImplementation((text, ...args) => {
      if ((text === original || text === changed) && fs.existsSync(path.join(mesh.root, ".lock"))) parses++;
      return parse(text, ...args);
    });
    return { ...f, mesh, file, original, changed, clock, parses: () => parses,
      publish: (topic: string) => mesh.publish({ topic, from: f.from }),
      pending: (event: { id: string; sequence?: number }) => {
        const line = fs.readFileSync(index.residentWakeIndexPath(mesh.root), "utf8").trim().split("\n").at(-1)!;
        expect(JSON.parse(line)).toMatchObject({ op: "pending", root: f.resident, delivery: { id: event.id, sequence: event.sequence } });
        expect(mesh.read().some(saved => saved.id === event.id)).toBe(true);
      },
      close: () => { vi.restoreAllMocks(); mesh.closeState(); fs.rmSync(f.root, { recursive: true, force: true }); },
    };
  };

  const mockRouteMetadata = (file: string, stamp: (stat: fs.Stats | fs.BigIntStats) => object) => {
    const routeFds = new Set<number>();
    const open = fs.openSync, close = fs.closeSync;
    vi.spyOn(fs, "openSync").mockImplementation((target, ...args) => {
      const fd = open(target, ...args);
      if (target === file) routeFds.add(fd);
      return fd;
    });
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { routeFds.delete(fd); close(fd); });
    for (const method of ["statSync", "lstatSync", "fstatSync"] as const) {
      const stat = fs[method] as (...args: unknown[]) => fs.Stats | fs.BigIntStats;
      vi.spyOn(fs, method).mockImplementation(((target: unknown, ...args: unknown[]) => {
        const actual = stat(target, ...args);
        if (target !== file && !(typeof target === "number" && routeFds.has(target))) return actual;
        return Object.assign(Object.create(Object.getPrototypeOf(actual)), actual, stamp(actual));
      }) as never);
    }
    return routeFds;
  };

  // Data safety: the append must retain the latest matching delivery, not a stale route decision.
  it("reuses unchanged aged route parsing only inside the real publish lock", async () => {
    const f = setup();
    let stats = 0, bigintStats = 0;
    mockRouteMetadata(f.file, stat => {
      if (fs.existsSync(path.join(f.mesh.root, ".lock"))) {
        stats++;
        if (typeof stat.ino === "bigint") bigintStats++;
      }
      return {};
    });
    try {
      f.pending(await f.publish("indexed.delivery"));
      expect(f.parses()).toBe(1);
      const coldStats = [stats, bigintStats];
      const hot = await f.publish("indexed.delivery");
      f.pending(hot);
      expect(f.parses()).toBe(1);
      expect([stats - coldStats[0]!, bigintStats - coldStats[1]!]).toEqual([1, 1]);
      // Public/post-publish route readers are not this cache and must still read independently.
      expect(index.retainedRoutesAt(f.resident)?.actors[0]?.topics).toEqual(["indexed.delivery"]);
      expect(f.parses()).toBe(1);
    } finally { f.close(); }
  });

  it.each(["in-place", "atomic replacement"] as const)("refreshes same-size %s routes before the matching append", async mode => {
    const f = setup();
    try {
      fs.utimesSync(f.file, 0, 0);
      const ctime = fs.statSync(f.file, { bigint: true }).ctimeNs / 1_000_000n * 1_000_000n;
      let revision = 0n;
      if (mode === "in-place") mockRouteMetadata(f.file, stat => typeof stat.ino === "bigint"
        ? { ctimeNs: ctime + revision, ctimeMs: ctime / 1_000_000n }
        : { ctimeMs: Number(ctime / 1_000_000n) });
      await f.publish("changed.delivery"); // Warm a positive parsed route that does NOT match this topic.
      expect(index.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      const before = fs.statSync(f.file, { bigint: true });
      if (mode === "in-place") {
        fs.writeFileSync(f.file, f.changed);
        fs.utimesSync(f.file, 0, 0);
        revision = 1n; // Only ctimeNs changes; even ctimeMs stays equal, without timing sleeps.
      } else {
        const replacement = `${f.file}.next`;
        fs.writeFileSync(replacement, f.changed);
        fs.utimesSync(replacement, 0, 0);
        fs.renameSync(replacement, f.file);
      }
      const after = fs.statSync(f.file, { bigint: true });
      expect(after.size).toBe(before.size);
      if (mode === "in-place") {
        expect(after.ino).toBe(before.ino);
        expect(after.mtimeNs).toBe(before.mtimeNs);
        expect(after.ctimeMs).toBe(before.ctimeMs);
        expect(after.ctimeNs).not.toBe(before.ctimeNs);
      } else {
        expect(after.ino).not.toBe(before.ino);
        expect(after.mtimeNs).toBe(before.mtimeNs);
      }
      f.pending(await f.publish("changed.delivery"));
      expect(f.parses()).toBe(2);
    } finally { f.close(); }
  });

  it("does not cache just-written equal-stamp routes, including after the clock ages", async () => {
    const f = setup();
    const now = Date.now();
    f.clock.mockReturnValue(now);
    // Only metadata is synthetic: actual route bytes, lock, append and pending journal stay real.
    mockRouteMetadata(f.file, stat => typeof stat.ino === "bigint"
      ? { mtimeNs: BigInt(now) * 1_000_000n, ctimeNs: BigInt(now) * 1_000_000n,
          mtimeMs: BigInt(now), ctimeMs: BigInt(now), mtime: new Date(now), ctime: new Date(now) }
      : { mtimeMs: now, ctimeMs: now, mtime: new Date(now), ctime: new Date(now) });
    try {
      await f.publish("changed.delivery");
      expect(index.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      fs.writeFileSync(f.file, f.changed); // Same inode, size AND mocked coarse timestamps.
      f.pending(await f.publish("changed.delivery"));
      expect(f.parses()).toBe(2);
      f.clock.mockReturnValue(now + 60_000);
      f.pending(await f.publish("changed.delivery"));
      expect(f.parses()).toBe(3); // A fresh observation must not seed a later stale cache hit.
    } finally { f.close(); }
  });

  it.each(["in-place", "atomic replacement"] as const)("does not seed fresh-at-entry routes after an FD-read %s rewrite ages the clock", async mode => {
    const f = setup(), now = Date.now(), read = fs.readFileSync;
    const replacementBlocked = mode === "atomic replacement" && process.platform === "win32";
    let rewriteAttempted = false, rewriteError: unknown;
    const fds = mockRouteMetadata(f.file, stat => typeof stat.ino === "bigint"
      ? { mtimeNs: BigInt(now) * 1_000_000n, ctimeNs: BigInt(now) * 1_000_000n } : {});
    vi.spyOn(fs, "readFileSync").mockImplementation((target, ...args) => {
      const bytes = read(target, ...args);
      if (typeof target === "number" && fds.has(target) && !rewriteAttempted) {
        try {
          expect(bytes).toBe(f.original);
          expect(fs.existsSync(path.join(f.mesh.root, ".lock"))).toBe(true);
          expect(Buffer.byteLength(f.changed)).toBe(Buffer.byteLength(f.original));
          const before = fs.statSync(f.file, { bigint: true });
          rewriteAttempted = true;
          const next = mode === "in-place" ? f.file : `${f.file}.next`;
          fs.writeFileSync(next, f.changed);
          // libuv shares DELETE, but legacy MoveFileExW replacement still requires a closed destination.
          if (replacementBlocked) expect(() => fs.renameSync(next, f.file)).toThrow(expect.objectContaining({ code: "EPERM" }));
          else if (mode !== "in-place") fs.renameSync(next, f.file);
          const after = fs.statSync(f.file, { bigint: true });
          expect(after.ino === before.ino).toBe(mode === "in-place" || replacementBlocked);
          expect([after.dev, after.size, after.mtimeNs, after.ctimeNs]).toEqual([before.dev, before.size, before.mtimeNs, before.ctimeNs]);
          f.clock.mockReturnValue(now + 11);
        } catch (error) { rewriteError = error; throw error; }
      }
      return bytes;
    });
    try {
      expect((await f.publish("changed.delivery")).sequence).toBe(1);
      // readWakeJson catches every error, including a failing assertion or filesystem operation in the spy.
      expect(rewriteError).toBeUndefined();
      expect(rewriteAttempted).toBe(true);
      expect(fds.size).toBe(0);
      expect(read(f.file, "utf8")).toBe(replacementBlocked ? f.original : f.changed);
      expect(f.parses()).toBe(1);
      expect(index.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      const event = await f.publish("changed.delivery");
      expect(event.sequence).toBe(2);
      expect(f.parses()).toBe(2); // Even an unchanged route must reparse: the first observation was fresh.
      if (replacementBlocked) {
        expect(index.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
        expect(fds.size).toBe(0);
        const before = fs.statSync(f.file, { bigint: true });
        fs.renameSync(`${f.file}.next`, f.file); // The same replacement succeeds after readWakeJson closes its fd.
        expect(fs.statSync(f.file, { bigint: true }).ino).not.toBe(before.ino);
        expect(read(f.file, "utf8")).toBe(f.changed);
        const replaced = await f.publish("changed.delivery");
        expect(replaced.sequence).toBe(3); f.pending(replaced);
        expect(f.parses()).toBe(3);
      } else f.pending(event);
    } finally { f.close(); }
  });

  it.skipIf(process.getuid === undefined)("does not let a hot route cache bypass UID validation", async () => {
    const f = setup();
    const foreign = process.getuid!() + 1;
    try {
      const first = await f.publish("indexed.delivery");
      f.pending(first);
      mockRouteMetadata(f.file, stat => ({ uid: typeof stat.uid === "bigint" ? BigInt(foreign) : foreign }));
      await f.publish("indexed.delivery");
      f.pending(first); // Unsafe current metadata must not advance the original pending watermark.
    } finally { f.close(); }
  });
});

describe("archive-coupled resident wake retry index", () => {
  it.each(["memory", "disk"] as const)("double write failure retries exactly once on the retention sweep from the %s index", async source => {
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
      expect(launch).not.toHaveBeenCalled();
      await (await import("../src/residency/wake.js")).retryResidentWakes(f.mesh, launch, Date.now() + 60_000);
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
      expect(launch).not.toHaveBeenCalled();
      const retryAt = wake.readWakeJson<{ nextRetryAt: number }>(path.join(f.resident, "wake-failure.json"))!.nextRetryAt;
      await activeWake.retryResidentWakes(f.mesh, launch, retryAt);
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
  ] as const)("publishes to %i roots via %s without partial append and drains overflow on the retention sweep", async (count, mode) => {
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
        expect(launch).toHaveBeenCalledTimes(256);
        await activeWake.retryResidentWakes(f.mesh, launch);
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
        expect(launch).toHaveBeenCalledTimes(128);
        await freshWake.retryResidentWakes(f.mesh, launch);
        expect(launch).toHaveBeenCalledTimes(Math.min(count, 256));
        if (count > 256) await freshWake.retryResidentWakes(f.mesh, launch);
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

  it("propagates a non-ENOENT residency directory error so the event is not committed without its wake record", async () => {
    const f = fixture();
    const directory = path.join(f.mesh.root, "residency");
    const readdir = fs.readdirSync;
    try {
      await wake.ensureResidentWakeArchive(f.mesh);
      vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
        if (file === directory) throw Object.assign(new Error(`EACCES: permission denied, scandir '${directory}'`), { code: "EACCES" });
        return (readdir as (...values: unknown[]) => unknown)(file, ...args);
      }) as typeof fs.readdirSync);
      expect(() => index.residentDirectories(f.mesh.root)).toThrow(expect.objectContaining({ code: "EACCES" }));
      await expect(f.mesh.publish({ topic: "indexed.delivery", from: f.from })).rejects.toMatchObject({ code: "EACCES" });
      vi.restoreAllMocks();
      expect(f.mesh.read({ topic: "indexed.delivery", after: 0, limit: 10 })).toHaveLength(0);
      expect(index.residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      // Only a missing residency directory reads as empty.
      expect(index.residentDirectories(path.join(f.root, "absent-mesh"))).toEqual([]);
    } finally { vi.restoreAllMocks(); f.mesh.closeState(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });
});
