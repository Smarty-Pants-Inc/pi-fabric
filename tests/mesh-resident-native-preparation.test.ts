import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshEvent, type MeshPublishInput } from "../src/mesh/store.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { canonicalResidentWakeConfig, indexResidentDeliveries, residentUnacknowledgedDeliveries } from "../src/residency/wake-index.js";

// Data-safety boundary (#17082): real MeshStore/EventLog/custody/wake journal, but a
// mocked native observation contract. Platform simulation is NOT native Mac/Windows coverage.
// The residency writer independently tests the implementation of this exact-generation cache.
const observation = vi.hoisted(() => ({
  loads: 0,
  verdict: "stale" as "stale" | "live" | "unknown" | "unavailable",
  cache: new Map<string, string>(),
  preparations: [] as Array<{ root: string; underLock: boolean; owner: string }>,
  predicates: [] as Array<{ source: string; owner: string; underLock: boolean; alive: boolean }>,
  afterRead: undefined as undefined | ((root: string) => void | Promise<void>),
  prepare: vi.fn<(root: string) => Promise<void>>(),
  alive: vi.fn<(owner: unknown, source?: string) => boolean>(),
  wake: vi.fn<(...args: unknown[]) => Promise<void>>(),
}));
vi.mock("../src/residency/process-identity.js", () => ({
  // Detect observer export access, rather than claiming this module is otherwise
  // unloaded: wake-index already statically imports its synchronous predicate.
  get prepareResidentOwnerObservation() { observation.loads++; return observation.prepare; },
  residentHostOwnerAlive: observation.alive,
}));
vi.mock("../src/residency/wake.js", () => ({ wakeResidentActors: observation.wake }));

const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
const asPlatform = (platform: string) => Object.defineProperty(process, "platform", { ...platformDescriptor, value: platform });
const sourceKey = (source: string, owner: unknown) => `${source}\n${JSON.stringify(owner)}`;
const from = { id: "publisher", name: "publisher", kind: "main" as const };
const topic = "native.delivery";
const input = (extra: Partial<MeshPublishInput> = {}): MeshPublishInput => ({ topic, from, ...extra });
const cleanups: Array<() => void> = [];
const boundaries = ["publish", "batch", "transact", "schedule", "release"] as const;
type Boundary = typeof boundaries[number];
type Platform = "darwin" | "win32";
const nativeOwner = (platform: Platform, token = "old-generation") => ({
  format: 1, rootId: "session:native", pid: process.pid, token, startedAt: 1,
  bootId: platform === "darwin" ? "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE" : "638640000000000000",
  pidNamespace: platform === "darwin"
    ? "darwin:12345678-1234-1234-1234-123456789ABC:AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE:host"
    : "win32:9f0b7c3e-1d2a-4b5c-8e7f-0a1b2c3d4e5f:638640000000000000:host",
  processStartTime: platform === "darwin" ? "darwin:Wed Sep 30 12:00:00 2026" : "win32:638640000000000001",
});

beforeEach(() => {
  observation.loads = 0;
  observation.verdict = "stale";
  observation.cache.clear();
  observation.preparations.length = 0;
  observation.predicates.length = 0;
  observation.afterRead = undefined;
  observation.prepare.mockReset().mockImplementation(async root => {
    const source = path.join(root, "owner.json");
    const owner = JSON.parse(fs.readFileSync(source, "utf8")) as unknown;
    observation.preparations.push({ root, owner: JSON.stringify(owner),
      underLock: fs.existsSync(path.join(path.dirname(path.dirname(root)), ".lock")) });
    // Snapshot before yielding: replacement during this await cannot inherit its verdict.
    const key = sourceKey(source, owner);
    await observation.afterRead?.(root);
    if (observation.verdict === "unavailable") throw new Error("native observation unavailable");
    observation.cache.set(key, observation.verdict);
  });
  observation.alive.mockReset().mockImplementation((owner, source = "owner.json") => {
    const alive = observation.cache.get(sourceKey(source, owner)) !== "stale";
    observation.predicates.push({ source, owner: JSON.stringify(owner), alive,
      underLock: fs.existsSync(path.join(path.dirname(path.dirname(path.dirname(source))), ".lock")) });
    return alive; // Unprepared, UNKNOWN, and a different owner generation stay live.
  });
  observation.wake.mockReset().mockResolvedValue(undefined);
  vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
});
afterEach(() => {
  Object.defineProperty(process, "platform", platformDescriptor);
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0).reverse()) cleanup();
});

const fixture = (platform: Platform, resident: "present" | "absent" | "empty" = "present", canWakeResidents = () => false) => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-native-preparation-"));
  const mesh = new MeshStore(path.join(temp, "mesh"), 65_536, 100, { canWakeResidents });
  cleanups.push(() => { mesh.closeState(); fs.rmSync(temp, { recursive: true, force: true }); });
  const root = path.join(mesh.root, "residency", "native");
  const ownerFile = path.join(root, "owner.json");
  if (resident !== "absent") fs.mkdirSync(path.dirname(root), { recursive: true });
  if (resident === "present") {
    fs.mkdirSync(root);
    const config = { rootId: "session:native", residencyRoot: root, cwd: temp };
    fs.writeFileSync(ownerFile, JSON.stringify(nativeOwner(platform)));
    fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
    fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({ format: 1,
      rootId: config.rootId, hostId: "host:native", configJson: canonicalResidentWakeConfig(config),
      actors: [{ id: "listener", name: "listener", topics: [topic] }] }));
  }
  return { mesh, root, ownerFile };
};

const outer = async (boundary: Boundary, mesh: MeshStore) => {
  // Seed through the production schedule API before installing the simulated platform.
  const now = Date.now();
  if (boundary === "release") await mesh.schedule({ topic, from, dueAt: now + 10_000 }, now);
  return async (): Promise<MeshEvent[]> => {
    switch (boundary) {
      case "publish": return [await mesh.publish(input())];
      case "batch": return mesh.publishBatch([input({ text: "first" }), input({ text: "second" })]);
      case "transact": return mesh.transact(append => [append(input())]);
      case "schedule": {
        const scheduled = await mesh.schedule({ topic, from, dueAt: now }, now);
        return mesh.read().filter(event => event.id === scheduled.id);
      }
      case "release": return mesh.releaseDueSchedules(now + 10_000);
    }
  };
};
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};

const cases = (["darwin", "win32"] as const).flatMap(platform => boundaries.map(boundary => ({ platform, boundary })));
describe("native preparation at the actual outer append boundaries", () => {
  it("first cold publication indexes a complete stale native birth without receiver priming", async () => {
    const f = fixture("darwin");
    asPlatform("darwin");
    expect(observation.cache.size).toBe(0);
    const event = await f.mesh.publish(input());
    expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)?.id).toBe(event.id);
    expect(f.mesh.read().map(saved => saved.id)).toEqual([event.id]);
    expect(observation.prepare).toHaveBeenCalledOnce();
    expect(observation.wake).not.toHaveBeenCalled();
  });

  it.each(cases)("cold $platform $boundary observes before acquisition, then indexes a stale complete owner", async ({ platform, boundary }) => {
    const f = fixture(platform);
    const invoke = await outer(boundary, f.mesh);
    const acquire = vi.spyOn(MeshLock.prototype, "withLock");
    const entered = deferred(), resume = deferred();
    observation.afterRead = () => { entered.resolve(); return resume.promise; };
    asPlatform(platform);
    expect(observation.cache.size).toBe(0);
    const publishing = invoke();
    await entered.promise;
    expect(acquire).not.toHaveBeenCalled();
    expect(observation.predicates).toEqual([]);
    expect(f.mesh.read()).toEqual([]);
    resume.resolve();
    const events = await publishing;
    expect(events.length).toBeGreaterThan(0);
    expect(observation.prepare).toHaveBeenCalledWith(f.root);
    expect(observation.preparations).toEqual([{ root: f.root, underLock: false,
      owner: JSON.stringify(nativeOwner(platform)) }]);
    expect(observation.predicates.length).toBe(events.length);
    expect(observation.predicates.every(entry => entry.underLock && !entry.alive)).toBe(true);
    expect(observation.prepare.mock.invocationCallOrder[0]).toBeLessThan(acquire.mock.invocationCallOrder[0]!);
    expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)).toMatchObject({ id: events.at(-1)!.id,
      sequence: events.at(-1)!.sequence });
    expect(f.mesh.read().map(event => event.id)).toEqual(events.map(event => event.id));
    expect(f.mesh.scheduled()).toEqual([]);
  });

  it.each(cases.flatMap(entry => (["live", "unknown", "unavailable"] as const).map(verdict => ({ ...entry, verdict }))))(
    "$platform $boundary keeps a $verdict owner live without spurious pending delivery", async ({ platform, boundary, verdict }) => {
      const f = fixture(platform);
      const invoke = await outer(boundary, f.mesh);
      observation.verdict = verdict;
      asPlatform(platform);
      const events = await invoke();
      expect(events.length).toBeGreaterThan(0);
      expect(observation.prepare).toHaveBeenCalledWith(f.root);
      expect(observation.predicates.every(entry => entry.underLock && entry.alive)).toBe(true);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      expect(f.mesh.read().map(event => event.id)).toEqual(events.map(event => event.id));
    });

  it.each(cases)("$platform $boundary rereads an owner replaced while preparation yielded", async ({ platform, boundary }) => {
    const f = fixture(platform);
    const invoke = await outer(boundary, f.mesh);
    const replacement = { ...nativeOwner(platform, "replacement-generation"), startedAt: 2,
      processStartTime: platform === "darwin" ? "darwin:Wed Sep 30 12:00:01 2026" : "win32:638640000000000002" };
    observation.afterRead = () => { fs.writeFileSync(f.ownerFile, JSON.stringify(replacement)); };
    asPlatform(platform);
    const events = await invoke();
    expect(events.length).toBeGreaterThan(0);
    expect(observation.preparations[0]?.owner).toBe(JSON.stringify(nativeOwner(platform)));
    expect(observation.predicates.length).toBe(events.length);
    expect(observation.predicates.every(entry => entry.owner === JSON.stringify(replacement) && entry.alive && entry.underLock)).toBe(true);
    expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
  });
});

describe("cheap paths and synchronous callback", () => {
  it.each(boundaries)("Linux %s never accesses the native observer or enumerates residency before custody", async boundary => {
    const f = fixture("darwin");
    const invoke = await outer(boundary, f.mesh);
    const scans: boolean[] = [];
    const readdir = fs.readdirSync;
    vi.spyOn(fs, "readdirSync").mockImplementation(((directory: fs.PathLike, ...args: unknown[]) => {
      if (directory === path.join(f.mesh.root, "residency")) scans.push(fs.existsSync(path.join(f.mesh.root, ".lock")));
      return (readdir as (...args: unknown[]) => unknown)(directory, ...args);
    }) as typeof fs.readdirSync);
    const acquire = vi.spyOn(MeshLock.prototype, "withLock");
    asPlatform("linux");
    const publishing = invoke();
    expect(acquire).toHaveBeenCalled(); // No new async turn before the existing acquisition boundary.
    await publishing;
    expect(observation.loads).toBe(0);
    expect(observation.prepare).not.toHaveBeenCalled();
    expect(scans.length).toBeGreaterThan(0); // Existing synchronous callback still discovers routes.
    expect(scans.every(underLock => underLock)).toBe(true);
  });

  it.each(cases.flatMap(entry => (["absent", "empty"] as const).map(resident => ({ ...entry, resident }))))(
    "$platform $boundary with $resident residency never accesses the native observer", async ({ platform, boundary, resident }) => {
      const f = fixture(platform, resident);
      const invoke = await outer(boundary, f.mesh);
      const acquire = vi.spyOn(MeshLock.prototype, "withLock");
      asPlatform(platform);
      const publishing = invoke();
      expect(acquire).toHaveBeenCalled(); // No observer promise/await when there are no resident roots.
      const events = await publishing;
      expect(events.length).toBeGreaterThan(0);
      expect(observation.loads).toBe(0);
      expect(observation.prepare).not.toHaveBeenCalled();
      expect(observation.predicates).toEqual([]);
    });

  it("the synchronous indexing callback cannot initiate a native read on a cold owner", async () => {
    const f = fixture("darwin");
    asPlatform("darwin");
    await f.mesh.exclusive(() => indexResidentDeliveries(f.mesh.root, {
      id: "callback-only", sequence: 1, topic, kind: "message", from, createdAt: Date.now(),
    }, []));
    expect(observation.loads).toBe(0);
    expect(observation.prepare).not.toHaveBeenCalled();
    expect(observation.predicates).toMatchObject([{ alive: true, underLock: true }]);
    expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
    expect(f.mesh.read()).toEqual([]);
  });

  it("release with nothing due never prepares or acquires custody", async () => {
    const f = fixture("darwin");
    await f.mesh.schedule({ topic, from, dueAt: Date.now() + 60_000 });
    const acquire = vi.spyOn(MeshLock.prototype, "withLock");
    asPlatform("darwin");
    expect(await f.mesh.releaseDueSchedules()).toEqual([]);
    expect(acquire).not.toHaveBeenCalled();
    expect(observation.loads).toBe(0);
    expect(observation.prepare).not.toHaveBeenCalled();
  });
});

describe("yielding preparation retains publication admission and schedule semantics", () => {
  it.each(["publish", "batch", "schedule"] as const)("%s cancellation during preparation commits nothing", async boundary => {
    const f = fixture("darwin");
    const controller = new AbortController();
    const cancelled = new Error("caller cancelled during native preparation");
    observation.afterRead = () => { controller.abort(cancelled); };
    asPlatform("darwin");
    const publishing = boundary === "publish" ? f.mesh.publish(input({ signal: controller.signal }))
      : boundary === "batch" ? f.mesh.publishBatch([input({ signal: controller.signal })])
        : f.mesh.schedule({ topic, from, dueAt: Date.now(), signal: controller.signal });
    await expect(publishing).rejects.toBe(cancelled);
    expect(observation.prepare).toHaveBeenCalledWith(f.root);
    expect(f.mesh.read()).toEqual([]);
    expect(f.mesh.scheduled()).toEqual([]);
    expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
  });

  it.each(["publish", "batch"] as const)("%s rechecks admission after preparation under the real lock", async boundary => {
    const f = fixture("darwin");
    let admitted = true;
    const refusal = new Error("admission changed while preparing");
    const admit = vi.fn(() => {
      expect(fs.existsSync(path.join(f.mesh.root, ".lock"))).toBe(true);
      if (!admitted) throw refusal;
    });
    observation.afterRead = () => { admitted = false; };
    asPlatform("darwin");
    const publishing = boundary === "publish" ? f.mesh.publish(input({ admit })) : f.mesh.publishBatch([input({ admit })]);
    await expect(publishing).rejects.toBe(refusal);
    expect(admit).toHaveBeenCalledOnce();
    expect(f.mesh.read()).toEqual([]);
    expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
  });

  it("rechecks resident wake admission after preparation without dropping durable indexing", async () => {
    let canWake = true;
    const f = fixture("darwin", "present", () => canWake);
    observation.afterRead = () => { canWake = false; };
    asPlatform("darwin");
    const event = await f.mesh.publish(input());
    expect(observation.wake).not.toHaveBeenCalled();
    expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)?.id).toBe(event.id);
  });

  it("a post-commit wake failure resolves the original publication without appending again", async () => {
    const f = fixture("darwin", "present", () => true);
    observation.wake.mockRejectedValue(new Error("wake unavailable after commit"));
    asPlatform("darwin");
    const event = await f.mesh.publish(input({ dedupeKey: "original-publication" }));
    expect(observation.prepare).toHaveBeenCalledOnce();
    expect(observation.wake).toHaveBeenCalledOnce();
    expect(f.mesh.read().map(saved => saved.id)).toEqual([event.id]);
    expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)?.id).toBe(event.id);
    expect(diagnostics.warn).toHaveBeenCalledWith("src/mesh/store.ts", expect.stringContaining("resident wake deferred"));
  });

  it.each(["publish", "batch", "transact", "schedule"] as const)("%s releases due schedules in order before its ordinary append", async boundary => {
    const f = fixture("darwin");
    const now = Date.now();
    const later = await f.mesh.schedule({ topic, from, text: "later", dueAt: now + 20_000 }, now);
    const earlier = await f.mesh.schedule({ topic, from, text: "earlier", dueAt: now + 10_000 }, now + 1);
    vi.spyOn(Date, "now").mockReturnValue(now + 30_000);
    const invoke = await outer(boundary, f.mesh);
    asPlatform("darwin");
    const ordinary = await invoke();
    expect(f.mesh.read().map(event => event.id)).toEqual([earlier.id, later.id, ...ordinary.map(event => event.id)]);
    expect(observation.preparations.every(entry => !entry.underLock)).toBe(true);
    expect(observation.predicates.every(entry => entry.underLock && !entry.alive)).toBe(true);
    expect(f.mesh.scheduled()).toEqual([]);
    expect(await f.mesh.releaseDueSchedules(now + 40_000)).toEqual([]);
    expect(new Set(f.mesh.read().map(event => event.id)).size).toBe(f.mesh.read().length);
  });

  it.each(["publish", "batch"] as const)("%s releases schedules that became due during native preparation first", async boundary => {
    const f = fixture("darwin");
    const now = Date.now();
    const scheduled = await f.mesh.schedule({ topic, from, dueAt: now + 10_000 }, now);
    const clock = vi.spyOn(Date, "now").mockReturnValue(now);
    observation.afterRead = () => { clock.mockReturnValue(now + 20_000); };
    asPlatform("darwin");
    const events = boundary === "publish" ? [await f.mesh.publish(input())] : await f.mesh.publishBatch([input()]);
    expect(f.mesh.read().map(event => event.id)).toEqual([scheduled.id, ...events.map(event => event.id)]);
    expect(f.mesh.scheduled()).toEqual([]);
    expect(observation.preparations.every(entry => !entry.underLock)).toBe(true);
    expect(observation.predicates.every(entry => entry.underLock && !entry.alive)).toBe(true);
  });

  it("concurrent release requests preserve exactly one scheduled append", async () => {
    const f = fixture("darwin");
    const now = Date.now();
    const scheduled = await f.mesh.schedule({ topic, from, dueAt: now + 10_000 }, now);
    const other = new MeshStore(f.mesh.root, 65_536, 100, { canWakeResidents: () => false });
    cleanups.push(() => other.closeState());
    asPlatform("darwin");
    const releases = await Promise.all([f.mesh.releaseDueSchedules(now + 10_000), other.releaseDueSchedules(now + 10_000)]);
    expect(releases.flat().map(event => event.id)).toEqual([scheduled.id]);
    expect(f.mesh.read().map(event => event.id)).toEqual([scheduled.id]);
    expect(f.mesh.scheduled()).toEqual([]);
    // A releaser that finds the schedule already retired needs no observation or lock.
    expect(observation.preparations.length).toBeGreaterThan(0);
    expect(observation.preparations.every(entry => !entry.underLock)).toBe(true);
    expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)?.id).toBe(scheduled.id);
  });

  it("release revalidates schedules cancelled while observation yielded", async () => {
    const f = fixture("darwin");
    const now = Date.now();
    await f.mesh.schedule({ topic, from, key: "cancelled", dueAt: now + 10_000 }, now);
    const entered = deferred(), resume = deferred();
    observation.afterRead = () => { entered.resolve(); return resume.promise; };
    asPlatform("darwin");
    const releasing = f.mesh.releaseDueSchedules(now + 10_000);
    await entered.promise;
    expect(await f.mesh.unschedule("cancelled")).toEqual({ removed: true });
    resume.resolve();
    expect(await releasing).toEqual([]);
    expect(f.mesh.read()).toEqual([]);
    expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
  });
});
