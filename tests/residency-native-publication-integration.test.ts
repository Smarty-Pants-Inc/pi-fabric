// Keep: #17082 trust/data-safety boundary: cold MeshStore publication must prepare native
// evidence before real locked indexing. Linux filesystem/process execution with simulated
// darwin/win32 identity readers ONLY; this is not native macOS or Windows coverage.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { pidNamespaceTestHooks } from "../src/core/pid-namespace.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";
import { residentHostOwnerAlive } from "../src/residency/process-identity.js";
import type { ResidentHostOwner } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentUnacknowledgedDeliveries, residentWakeIndexPath } from "../src/residency/wake-index.js";

const hostPlatform = process.platform;
const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
const fixtures: Array<{ temp: string; mesh: MeshStore }> = [];
afterEach(() => {
  Object.defineProperty(process, "platform", platformDescriptor);
  vi.restoreAllMocks();
  pidNamespaceTestHooks.resetReader();
  for (const { temp, mesh } of fixtures.splice(0)) {
    mesh.closeState();
    // Only this test's newly created private directory may be removed.
    fs.rmSync(temp, { recursive: true, force: true });
  }
});

const topic = "native.integrated.work";
const from = { id: "publisher", name: "publisher", kind: "main" as const };
type Platform = "darwin" | "win32";
const fixture = (platform: Platform) => {
  // Construct the real lock/state/EventLog on the actual Linux host BEFORE simulation.
  expect(process.platform).toBe("linux");
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), "native-publication-integration-"));
  const mesh = new MeshStore(path.join(temp, "mesh"), 65_536, 100, {
    stateBackend: "file", canWakeResidents: () => false,
  });
  fixtures.push({ temp, mesh });
  const root = path.join(mesh.root, "residency", "native");
  const ownerFile = path.join(root, "owner.json");
  const lock = path.join(mesh.root, ".lock");
  const journal = residentWakeIndexPath(mesh.root);
  fs.mkdirSync(root, { recursive: true });
  const machine = "00000000-0000-0000-0000-000000000001";
  const bootId = platform === "darwin" ? "00000000-0000-0000-0000-000000000002" : "123456";
  const pidNamespace = `${platform}:${machine}:${bootId}:host`;
  const birth = platform === "darwin" ? "darwin:Mon Jan  1 00:00:00 2024" : "win32:123457";
  const staleBirth = platform === "darwin" ? "darwin:Tue Jan  2 00:00:00 2024" : "win32:123458";
  const replacementBirth = platform === "darwin" ? "darwin:Wed Jan  3 00:00:00 2024" : "win32:123459";
  const owner: ResidentHostOwner = { format: 1, hostId: "host:native", pid: process.pid,
    token: "old-generation", startedAt: 1, readyAt: 1, bootId, pidNamespace, processStartTime: birth };
  const saveOwner = (value: ResidentHostOwner) => fs.writeFileSync(ownerFile, JSON.stringify(value));
  const config = { format: 1, rootId: "session:native", residencyRoot: root, meshRoot: mesh.root, cwd: temp };
  saveOwner(owner);
  fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
  fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({ format: 1,
    rootId: config.rootId, hostId: owner.hostId, configJson: canonicalResidentWakeConfig(config),
    actors: [{ id: "listener", name: "listener", topics: [topic] }] }));
  Object.defineProperty(process, "platform", { ...platformDescriptor, value: platform });
  pidNamespaceTestHooks.set(pidNamespace);
  // The real MeshStore lock also prepares its own identity; never invoke nonhost commands.
  const ownRead = vi.spyOn(atomic, "ownProcessIncarnation").mockImplementation(async () => {
    expect(fs.existsSync(lock)).toBe(false);
    return staleBirth;
  });
  const read = vi.spyOn(atomic, "processIncarnation");
  return { mesh, root, ownerFile, lock, journal, owner, birth, staleBirth, replacementBirth, saveOwner, read, ownRead };
};
type Fixture = ReturnType<typeof fixture>;

const assertCommitted = (f: Fixture, events: MeshEvent[], pending: boolean) => {
  expect(f.mesh.read().map(event => event.id)).toEqual(events.map(event => event.id));
  expect(fs.existsSync(f.lock)).toBe(false);
  const deliveries = residentUnacknowledgedDeliveries(f.mesh.root);
  if (pending) {
    expect([...deliveries]).toEqual([[f.root, { id: events.at(-1)!.id, sequence: events.at(-1)!.sequence }]]);
    // Inspect the actual durable journal, not only the process-local index cache.
    const records = fs.readFileSync(f.journal, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(records).toEqual(events.map(event => ({ format: 1, op: "pending", root: f.root,
      delivery: { id: event.id, sequence: event.sequence } })));
  } else {
    expect(deliveries.size).toBe(0);
    expect(fs.existsSync(f.journal)).toBe(false);
  }
  expect(fs.existsSync(path.join(f.root, "wake-request.json"))).toBe(false);
};
const assertSyncPredicate = async (f: Fixture, owner: ResidentHostOwner, alive: boolean) => {
  await f.mesh.exclusive(() => {
    expect(fs.existsSync(f.lock)).toBe(true); // Real custody, not a mocked exclusive callback.
    const reads = f.read.mock.calls.length, ownReads = f.ownRead.mock.calls.length;
    expect(residentHostOwnerAlive(owner, f.ownerFile)).toBe(alive);
    expect(residentHostOwnerAlive(owner, f.ownerFile)).toBe(alive);
    expect(f.read).toHaveBeenCalledTimes(reads);
    expect(f.ownRead).toHaveBeenCalledTimes(ownReads);
  });
};
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};

// Expected skip off Linux: this suite deliberately simulates native identities on Linux.
// Real macOS/Windows filesystem, command and durability behavior require their own runners.
describe.skipIf(hostPlatform !== "linux").each(["darwin", "win32"] as const)("integrated simulated %s native publication", platform => {
  it.each(["stale", "same", "unknown"] as const)("first cold publish uses %s birth before real locked indexing", async verdict => {
    const f = fixture(platform);
    f.read.mockImplementation(async pid => {
      expect(pid).toBe(process.pid);
      expect(fs.existsSync(f.lock)).toBe(false);
      expect(fs.existsSync(f.journal)).toBe(false);
      expect(f.mesh.read()).toEqual([]);
      return verdict === "stale" ? f.staleBirth : verdict === "same" ? f.birth : undefined;
    });
    expect(f.read).not.toHaveBeenCalled();
    // No manual prepareResidentOwnerObservation: MeshStore must cross that real boundary itself.
    const event = await f.mesh.publish({ topic, from });
    expect(f.read).toHaveBeenCalledExactlyOnceWith(process.pid);
    assertCommitted(f, [event], verdict === "stale");
    await assertSyncPredicate(f, f.owner, verdict !== "stale");
    expect(f.read).toHaveBeenCalledOnce(); // The production synchronous index did not initiate a read either.
  });

  it("discards a late stale observation when owner token AND native birth are replaced", async () => {
    const f = fixture(platform);
    const entered = deferred<void>(), result = deferred<string | undefined>();
    f.read.mockImplementation(async pid => {
      expect(pid).toBe(process.pid);
      expect(fs.existsSync(f.lock)).toBe(false);
      entered.resolve();
      return result.promise;
    });
    const publishing = f.mesh.publish({ topic, from });
    const replacement: ResidentHostOwner = { ...f.owner, token: "replacement-generation",
      processStartTime: f.replacementBirth, startedAt: 2, readyAt: 2 };
    try {
      await entered.promise;
      expect(fs.existsSync(f.lock)).toBe(false);
      expect(f.mesh.read()).toEqual([]);
      expect(fs.existsSync(f.journal)).toBe(false);
      f.saveOwner(replacement);
    } finally {
      // The late result differs from BOTH generations: applying it to the replacement would condemn it.
      result.resolve(f.staleBirth);
    }
    const event = await publishing;
    expect(f.read).toHaveBeenCalledExactlyOnceWith(process.pid);
    expect(JSON.parse(fs.readFileSync(f.ownerFile, "utf8"))).toEqual(replacement);
    assertCommitted(f, [event], false);
    await assertSyncPredicate(f, replacement, true);
    // The superseded result must not have been cached even for its original generation.
    await assertSyncPredicate(f, f.owner, true);
    expect(f.read).toHaveBeenCalledOnce();
  });

  it("cold batch indexes both archived deliveries and retains the final durable watermark", async () => {
    const f = fixture(platform);
    f.read.mockImplementation(async () => {
      expect(fs.existsSync(f.lock)).toBe(false);
      expect(f.mesh.read()).toEqual([]);
      return f.staleBirth;
    });
    const events = await f.mesh.publishBatch([{ topic, from, text: "first" }, { topic, from, text: "second" }]);
    expect(events).toHaveLength(2);
    expect(events[1]!.sequence).toBeGreaterThan(events[0]!.sequence!);
    expect(f.read).toHaveBeenCalledExactlyOnceWith(process.pid);
    assertCommitted(f, events, true);
    await assertSyncPredicate(f, f.owner, false);
    expect(f.read).toHaveBeenCalledOnce();
  });
});
