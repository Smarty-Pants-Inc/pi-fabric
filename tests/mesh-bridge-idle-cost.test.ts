import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance as monotonicClock } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BRIDGE_CURSOR_CHECKPOINT_MS, MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import * as hostLeases from "../src/topology/host-leases.js";

const roots: string[] = [];
const idleBridges: MeshBridge[] = [];
const idleStores: MeshStore[] = [];
const identity: MeshIdentity = { id: "session:idle0000", name: "idle", kind: "main" };
afterEach(async () => {
  for (const bridge of idleBridges.splice(0)) await bridge.stop();
  for (const store of idleStores.splice(0)) store.closeState();
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-idle-cost-"));
  roots.push(root);
  const hubRoot = path.join(root, "hub");
  fs.mkdirSync(hubRoot);
  // Fleet-sized canonical state, not the tiny state that hid the deployed idle CPU cost.
  const entries: Record<string, MeshStateEntry> = {};
  for (let index = 0; index < 5_000; index++) {
    const key = `control/seen/k${String(index).padStart(5, "0")}`;
    entries[key] = { key, value: "x".repeat(305), version: index + 1, updatedAt: 1, updatedBy: identity };
  }
  const statePath = path.join(hubRoot, "state.json");
  fs.writeFileSync(statePath, JSON.stringify({ readGeneration: randomUUID(), format: 1, revisionFormat: 2, highWater: 5_000, entries }));
  expect(fs.statSync(statePath).size).toBeGreaterThan(2_300_000);
  const hub = new MeshStore(hubRoot, 256 * 1024, 500);
  const writer = new MeshStore(hubRoot, 256 * 1024, 500);
  const far = new MeshStore(path.join(root, "remote"), 256 * 1024, 500);
  const local = new StoreBridgeSide(hub, "quiet");
  const bridge = new MeshBridge({
    localName: "dev1", remoteName: "quiet", local, remote: new StoreBridgeSide(far, "dev1"),
    cursorPath: path.join(root, "cursor.json"),
  });
  await bridge.start();
  await bridge.syncPresence();
  const reads = vi.spyOn(fs, "readFileSync");
  const fullReads = () => reads.mock.calls.filter(([file]) => String(file) === statePath).length;
  return { bridge, hub, writer, local, fullReads };
};

// Requested RED row: real isolated stores, durable cursor files, normal filtered log churn
// and native lease renewals; only the clock is simulated. No live mesh is contacted.
const idlePair = async (backend: "file" | "sqlite") => {
  vi.spyOn(monotonicClock, "now").mockImplementation(() => Date.now());
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-idle-minute-"));
  roots.push(root);
  const hub = new MeshStore(path.join(root, "hub"), 256 * 1024, 500, { stateBackend: backend });
  const far = new MeshStore(path.join(root, "far"), 256 * 1024, 500, { stateBackend: backend });
  idleStores.push(hub, far);
  const local = new StoreBridgeSide(hub, "quiet");
  const remote = new StoreBridgeSide(far, "dev1");
  const cursorPath = path.join(root, "cursor.json");
  const bridge = new MeshBridge({ localName: "dev1", remoteName: "quiet", local, remote, cursorPath });
  idleBridges.push(bridge);
  const native = { id: "session:idle1111", name: "idle remote", kind: "main" as const };
  const startedAt = Date.now();
  const host = { format: 1 as const, id: native.id, rootId: native.id, identity: native,
    startedAt, incarnationToken: "idle-minute-owner", updatedAt: startedAt, expiresAt: startedAt + 15_000 };
  const hostKey = "topology/hosts/" + createHash("sha256").update(native.id).digest("hex");
  await far.put({ key: hostKey, value: host, identity: native });
  const renew = () => hostLeases.writeHostLease(far.root, { id: native.id, rootId: native.id, identityId: native.id,
    startedAt, incarnationToken: host.incarnationToken, updatedAt: Date.now(), expiresAt: Date.now() + 15_000 });
  renew();
  await bridge.start();
  // Settle initial outbox retirement and the mirror's own namespace changes before counting.
  await bridge.syncPresence();
  await bridge.syncPresence();
  return { root, hub, far, local, remote, bridge, cursorPath, renew, native };
};

const cursorIO = (cursorPath: string) => {
  const descriptors = new Set<number>();
  let fsyncs = 0;
  const open = fs.openSync;
  const close = fs.closeSync;
  const sync = fs.fsyncSync;
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: any[]) => {
    const fd = (open as any)(file, ...args);
    if (String(file).startsWith(cursorPath + ".")) descriptors.add(fd);
    return fd;
  }) as typeof fs.openSync);
  vi.spyOn(fs, "closeSync").mockImplementation(fd => { descriptors.delete(fd); return close(fd); });
  vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { if (descriptors.has(fd)) fsyncs++; return sync(fd); });
  const renames = vi.spyOn(fs, "renameSync");
  return { counts: () => ({ cursorFsyncs: fsyncs,
    cursorWrites: renames.mock.calls.filter(([, target]) => String(target) === cursorPath).length }) };
};

describe("idle bridge pair checkpoint and lease-set cost", () => {
  it.each(["file", "sqlite"] as const)("coalesces cursor fsyncs for a simulated idle minute (%s)", async backend => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await idlePair(backend);
    const io = cursorIO(f.cursorPath);
    const scans = vi.spyOn(hostLeases, "readHostLeases");
    const versions = vi.spyOn(hostLeases, "writeHostLeaseVersionIf");
    const cpu = process.cpuUsage();
    for (let tick = 1; tick <= 240; tick++) {
      now += 250;
      if (tick % 20 === 0) f.renew();
      // No addressed traffic: these are the normal log appends a shared hub sees while idle.
      await f.hub.publish({ topic: "probe.unrelated", kind: "tick", from: identity });
      await f.far.publish({ topic: "probe.unrelated", kind: "tick", from: f.native });
      expect(await f.bridge.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
      // Explicit sync represents normal presence-wake churn without changing its scheduling.
      await f.bridge.syncPresence();
    }
    const counts = { backend, simulatedMs: 60_000, passes: 240, ...io.counts(),
      leaseScans: scans.mock.calls.length, leaseVersionWrites: versions.mock.calls.length,
      cpuMicros: process.cpuUsage(cpu) };
    console.log("IDLE_MINUTE " + JSON.stringify(counts));
    expect(counts.cursorWrites).toBeLessThanOrEqual(12);
    expect(counts.cursorFsyncs).toBe(counts.cursorWrites);
    expect(counts.leaseScans).toBeLessThan(counts.passes);
    expect(counts.leaseVersionWrites).toBeLessThanOrEqual(12); // Only the twelve real native renewals.
    expect(versions.mock.results.every(result => result.type === "return" && result.value === true)).toBe(true);
    // Stop flushes the final confirmed prefix even if its interval has not elapsed.
    await f.bridge.stop();
    const saved = JSON.parse(fs.readFileSync(f.cursorPath, "utf8"));
    expect(saved.toRemote.after).toBe(await f.hub.latestSequence());
    expect(saved.toLocal.after).toBe(await f.far.latestSequence());
  }, 30_000);

  it.each(["file", "sqlite"] as const)("does no lease rescan or version write for an unchanged set (%s)", async backend => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await idlePair(backend);
    const scans = vi.spyOn(hostLeases, "readHostLeases");
    const versions = vi.spyOn(hostLeases, "writeHostLeaseVersionIf");
    const currentReads = vi.spyOn(hostLeases, "readHostLeaseCurrent");
    const lists = [vi.spyOn(f.hub, "listAll"), vi.spyOn(f.hub, "listAllShared"),
      vi.spyOn(f.far, "listAll"), vi.spyOn(f.far, "listAllShared")];
    for (let tick = 0; tick < 20; tick++) {
      now += 100;
      await f.bridge.syncPresence();
    }
    console.log("UNCHANGED_LEASE_SET " + JSON.stringify({ backend, syncs: 20,
      scans: scans.mock.calls.length, versionWrites: versions.mock.calls.length,
      currentLeaseReads: currentReads.mock.calls.length, stateLists: lists.reduce((n, spy) => n + spy.mock.calls.length, 0) }));
    expect(scans).not.toHaveBeenCalled();
    expect(versions).not.toHaveBeenCalled();
    expect(currentReads).not.toHaveBeenCalled(); // No #applyLease or file-version CAS on a cache hit.
    for (const list of lists) expect(list).not.toHaveBeenCalled();
    f.renew();
    await f.bridge.syncPresence();
    const mirrored = hostLeases.readHostLease(f.hub.root, f.native.id)!;
    expect(mirrored.mirror?.sourceUpdatedAt).toBe(now);
    expect(mirrored.mirror?.sourceExpiresAt).toBe(now + 15_000);
    now += 16_000;
    await f.bridge.syncPresence();
    expect((await f.local.owned()).hosts).toHaveLength(0); // Time alone still expires unchanged files.
  });

  it("makes a final moved pass durable within the interval with no subsequent bridge work", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await idlePair("sqlite");
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const before = fs.readFileSync(f.cursorPath, "utf8");
    await f.hub.publish({ topic: "probe.unrelated", from: identity });
    await f.bridge.step();
    expect(fs.readFileSync(f.cursorPath, "utf8")).toBe(before);
    now += BRIDGE_CURSOR_CHECKPOINT_MS;
    await vi.advanceTimersByTimeAsync(BRIDGE_CURSOR_CHECKPOINT_MS);
    expect(JSON.parse(fs.readFileSync(f.cursorPath, "utf8")).toRemote.after).toBe(await f.hub.latestSequence());
    const io = cursorIO(f.cursorPath);
    now += BRIDGE_CURSOR_CHECKPOINT_MS * 2;
    await vi.advanceTimersByTimeAsync(BRIDGE_CURSOR_CHECKPOINT_MS * 2);
    expect(io.counts()).toEqual({ cursorFsyncs: 0, cursorWrites: 0 });
  });

  it("reports a timer checkpoint failure to the next bridge pass and retries durability on stop", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await idlePair("sqlite");
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const before = fs.readFileSync(f.cursorPath, "utf8");
    await f.hub.publish({ topic: "probe.unrelated", from: identity });
    await f.bridge.step();
    const rename = fs.renameSync.bind(fs);
    const failure = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(target) === f.cursorPath) throw new Error("cursor timer durability failed");
      return rename(source, target);
    });
    now += BRIDGE_CURSOR_CHECKPOINT_MS;
    await vi.advanceTimersByTimeAsync(BRIDGE_CURSOR_CHECKPOINT_MS);
    expect(fs.readFileSync(f.cursorPath, "utf8")).toBe(before);
    await expect(f.bridge.step()).rejects.toThrow("cursor timer durability failed");
    failure.mockRestore();
    await f.bridge.stop();
    expect(JSON.parse(fs.readFileSync(f.cursorPath, "utf8")).toRemote.after).toBe(await f.hub.latestSequence());
  });

  it.each([false, true])("replays a pre-checkpoint delivery exactly once after crash (lost ACK: %s)", async lostAck => {
    const now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockReturnValue(now);
    const f = await idlePair("sqlite");
    const before = fs.readFileSync(f.cursorPath, "utf8");
    const original = f.remote.publishWithReceipt.bind(f.remote);
    const publish = vi.spyOn(f.remote, "publishWithReceipt");
    if (lostAck) {
      publish.mockImplementationOnce(async (...args) => { await original(...args); throw new Error("lost receipt"); });
    }
    await f.hub.publish({ topic: "fabric.control.command", kind: "followUp", from: identity,
      to: f.native.id, data: { version: 1, commandId: "idle-crash-replay", text: "real store receipt" } });
    if (lostAck) await expect(f.bridge.step()).rejects.toThrow("lost receipt");
    else expect((await f.bridge.step()).toRemote).toBe(1);
    expect(publish).toHaveBeenCalledTimes(1);
    expect(fs.readFileSync(f.cursorPath, "utf8")).toBe(before); // Old mark still covers the whole replay window.
    // Emulate abrupt process loss from the exact OLD durable bytes. Stop only retires
    // fixture timers; restoring the crash-time bytes excludes its shutdown checkpoint.
    await f.bridge.stop();
    fs.writeFileSync(f.cursorPath, before);
    const replayRemote = new StoreBridgeSide(f.far, "dev1");
    const replayPublish = vi.spyOn(replayRemote, "publishWithReceipt");
    const replay = new MeshBridge({ ...f.bridge.options,
      local: new StoreBridgeSide(f.hub, "quiet"), remote: replayRemote });
    idleBridges.push(replay);
    await replay.start();
    expect(await replay.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
    expect(replayPublish).not.toHaveBeenCalled(); // Recovered destination marks deduplicate even an unacknowledged append.
    const delivered = (await f.far.readAsync({ after: 0 })).filter(event => event.topic === "fabric.control.command");
    expect(delivered).toHaveLength(1);
    await replay.stop();
    expect(JSON.parse(fs.readFileSync(f.cursorPath, "utf8")).toRemote.after).toBe(await f.hub.latestSequence());
  });

  it("renews a long-lived source at the mirror cap without rescanning unchanged source leases", async () => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const f = await idlePair("sqlite");
    const lease = hostLeases.readHostLease(f.far.root, f.native.id)!;
    hostLeases.writeHostLease(f.far.root, { ...lease, expiresAt: now + 120_000 });
    await f.bridge.syncPresence();
    await f.bridge.syncPresence();
    const expiry = hostLeases.readHostLease(f.hub.root, f.native.id)!.expiresAt;
    // Prime a no-write repeat at +5 s, then cross the OLD confirmed cap. Caching
    // through the latest sample's hypothetical later deadline would go stale at +15 s.
    now += 5_000;
    await f.bridge.syncPresence();
    now += 10_000;
    const scans = vi.spyOn(hostLeases, "readHostLeases");
    await f.bridge.syncPresence();
    expect(hostLeases.readHostLease(f.hub.root, f.native.id)!.expiresAt).toBe(now + 15_000);
    expect(hostLeases.readHostLease(f.hub.root, f.native.id)!.expiresAt).toBeGreaterThan(expiry);
    expect(scans.mock.calls.filter(([root]) => root === f.far.root)).toHaveLength(0);
  });
});

describe("warm mesh bridge idle state cost (smarty-dev#2854)", () => {
  it.each([false, true])("bounds full-state reads over ten idle seconds (3 Hz commits and unrelated log traffic: %s)", async (traffic) => {
    let now = 1_800_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const { bridge, writer, local, fullReads } = await fixture();
    const presence = vi.spyOn(local, "presence");
    const owned = vi.spyOn(local, "owned");
    let bridgeReads = 0, commits = 0;
    for (let tick = 1; tick <= 40; tick++) {
      now += 250;
      // Exactly three canonical commits per second from a distinct writer/store.
      if (traffic && tick % 4 !== 0) {
        await writer.put({ key: "control/seen/beat", value: ++commits, identity });
      }
      if (traffic && tick % 4 === 0) {
        await writer.publish({ topic: "probe.unrelated", kind: "tick", from: identity });
      }
      const before = fullReads();
      expect(await bridge.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
      const count = fullReads() - before; // excludes writer's own canonical reads
      bridgeReads += count;
      if (tick % 20 !== 0) expect(count).toBe(0);
    }
    // Only the two due presence passes read state: three canonical reads each, not eight
    // full-state parses per tick (plus another eight when filtered pages move the cursor).
    expect(bridgeReads / 10).toBeLessThanOrEqual(1.2);
    expect(presence).toHaveBeenCalledTimes(2); // observation only; mirror preflight uses its locked snapshot
    expect(owned).not.toHaveBeenCalled();
    if (traffic) expect(commits).toBe(30);
  }, 20_000);

  it("still revalidates canonical authority for a nonempty page after idle polls", async () => {
    const { bridge, writer, local, fullReads } = await fixture();
    for (let tick = 0; tick < 4; tick++) await bridge.step();
    const owned = vi.spyOn(local, "owned");
    // Unknown remote recipient is refused, but only after the normal fresh authority read.
    await writer.publish({ topic: "fabric.control.command", kind: "followUp", from: identity,
      to: "session:absent0000", data: { version: 1 } });
    const before = fullReads();
    expect(await bridge.step()).toEqual({ toRemote: 0, toLocal: 0, dropped: 0 });
    expect(owned).toHaveBeenCalledTimes(1);
    // The five authority observations still run (including owned above), but state.json
    // did not change when the event was published. Physical-generation revalidation shares
    // the already-captured canonical snapshot rather than parsing it five more times.
    expect(fullReads() - before).toBe(0);
  });
});
