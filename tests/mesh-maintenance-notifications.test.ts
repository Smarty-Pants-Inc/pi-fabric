import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createCommitStats, createLockStats } from "../src/mesh/commit-stats.js";
import { ShadowStateBackend } from "../src/mesh/state-backend.js";
import { projectorDatabaseRoot, StateProjector } from "../src/mesh/state-projector.js";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore } from "../src/mesh/store.js";

const identity = { id: "maintenance-test", name: "maintenance-test", kind: "agent" as const };
const roots: string[] = [];
const projectors: StateProjector[] = [];
const stores: SqliteStateStore[] = [];
const meshes: MeshStore[] = [];
const cleanup: Array<() => void> = [];
const root = (): string => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-maintenance-notifications-"));
  roots.push(directory);
  return directory;
};
const clock = (): void => {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
};
// Drain async transactions and the projector's coalesced passes without advancing a clock.
const settle = async (): Promise<void> => { for (let step = 0; step < 40; step++) await Promise.resolve(); };
const openStore = async (directory: string, options: Parameters<typeof SqliteStateStore.open>[3] = {}): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(directory, 64 * 1024, 1_000, { initialize: "detached", ...options });
  stores.push(store);
  return store;
};
const openProjector = async (directory: string, options: Partial<Parameters<typeof StateProjector.open>[0]> = {}): Promise<StateProjector> => {
  const projector = await StateProjector.open({ root: directory, verifyMs: 0, statusMs: 0, ...options });
  projectors.push(projector);
  return projector;
};
const fileStore = (directory: string): MeshStore => {
  const mesh = new MeshStore(directory, 64 * 1024, 1_000, { stateBackend: "file" });
  meshes.push(mesh);
  return mesh;
};

// Deterministic notification delivery, including delayed own-write events and null filenames.
const notifications = () => {
  const watches: Array<{ directory: string; closed: boolean; notify: (name: string | null) => void }> = [];
  vi.spyOn(fs, "watch").mockImplementation(((directory: fs.PathLike, _options: unknown, listener: (event: string, name: string | null) => void) => {
    const entry = { directory: String(directory), closed: false, notify: (name: string | null) => listener("change", name) };
    watches.push(entry);
    const watcher = new EventEmitter() as fs.FSWatcher;
    watcher.close = () => { entry.closed = true; };
    return watcher;
  }) as typeof fs.watch);
  return {
    watches,
    emit: (directory: string, name: string | null) => {
      for (const watch of watches) if (!watch.closed && watch.directory === directory) watch.notify(name);
    },
  };
};
const bounded = async <T>(promise: Promise<T>): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error("notification did not arrive within 2 s")), 2_000);
    })]);
  } finally { clearTimeout(timer); }
};

afterEach(async () => {
  for (const projector of projectors.splice(0)) await projector.stop();
  for (const mesh of meshes.splice(0)) mesh.closeState();
  for (const store of stores.splice(0)) store.close();
  for (const dispose of cleanup.splice(0)) dispose();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe("notification-driven mesh maintenance", () => {
  it("checkpoints initial WAL work once, then has zero idle maintainer timers/wakes for 305 s", async () => {
    clock();
    const events = notifications();
    const store = await openStore(root(), { checkpoint: "maintainer", checkpointIntervalMs: 10 });
    await vi.advanceTimersByTimeAsync(0);
    const checkpoints = store.stats().checkpoints.passive;
    expect(checkpoints).toBe(1);
    expect(vi.getTimerCount()).toBe(0);
    events.emit(store.root, "state.db");
    events.emit(store.root, "state.db-wal");
    events.emit(store.root, null);
    await vi.advanceTimersByTimeAsync(305_000);
    expect(store.stats().checkpoints.passive).toBe(checkpoints);
    expect(vi.getTimerCount()).toBe(0);
    store.close();
    expect(events.watches.every(watch => watch.closed)).toBe(true);
  });

  it("coalesces own/store and external WAL commits, including same-size WAL reuse, without checkpoint feedback", async () => {
    clock();
    const events = notifications();
    const directory = root();
    const maintainer = await openStore(directory, { checkpoint: "maintainer", checkpointIntervalMs: 100 });
    const writer = await openStore(directory);
    await vi.advanceTimersByTimeAsync(0);
    await writer.put({ key: "a", value: 1, identity });
    events.emit(directory, "state.db-wal");
    events.emit(directory, "state.db-wal");
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(99);
    expect(maintainer.stats().checkpoints.passive).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(maintainer.stats().checkpoints.passive).toBe(2);
    const size = maintainer.walBytes();
    events.emit(directory, "state.db"); // Own checkpoint event, delivered later.
    expect(vi.getTimerCount()).toBe(0);
    await writer.put({ key: "a", value: 2, identity });
    expect(maintainer.walBytes()).toBe(size); // New frames reused the existing WAL allocation.
    events.emit(directory, null);
    await vi.advanceTimersByTimeAsync(100);
    expect(maintainer.stats().checkpoints.passive).toBe(3);
    await maintainer.put({ key: "a", value: 3, identity }); // Own commits do not move data_version.
    await vi.advanceTimersByTimeAsync(100);
    expect(maintainer.stats().checkpoints.passive).toBe(4);
    events.emit(directory, "state.db-wal");
    await vi.advanceTimersByTimeAsync(305_000);
    expect(maintainer.stats().checkpoints.passive).toBe(4);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("gives a pinned checkpoint only one actual-work retry, then waits for another commit", async () => {
    clock();
    const events = notifications();
    const directory = root();
    const maintainer = await openStore(directory, { checkpoint: "maintainer", checkpointIntervalMs: 100 });
    const writer = await openStore(directory);
    await vi.advanceTimersByTimeAsync(0);
    const reader = openNodeSqlite(maintainer.file);
    try {
      reader.exec("BEGIN");
      reader.prepare("SELECT * FROM kv").all();
      await writer.put({ key: "pinned", value: 1, identity });
      events.emit(directory, "state.db-wal");
      await vi.advanceTimersByTimeAsync(200);
      expect(maintainer.stats().checkpoints.passive).toBe(3);
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(305_000);
      expect(maintainer.stats().checkpoints.passive).toBe(3);
      reader.exec("COMMIT");
      await writer.put({ key: "pinned", value: 2, identity });
      events.emit(directory, "state.db-wal");
      await vi.advanceTimersByTimeAsync(0);
      expect(maintainer.stats().checkpoints.passive).toBe(4);
      expect(vi.getTimerCount()).toBe(0);
    } finally { reader.close(); }
  });

  it("projects notifications immediately, ignores unrelated/own-write events, and keeps only the lease deadline", async () => {
    clock();
    const events = notifications();
    const directory = root();
    const file = fileStore(directory);
    await file.put({ key: "base", value: 0, identity });
    const projector = await openProjector(directory, { leaseMs: 10_000, pollMs: 250, statusMs: 1 });
    const tick = vi.spyOn(projector, "tick");
    projector.run();
    await settle();
    expect(projector.status().role).toBe("active");
    expect(tick).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1); // Lease renewal at half-life, not a 250 ms poll.
    await vi.advanceTimersByTimeAsync(750);
    expect(tick).toHaveBeenCalledTimes(1);
    await file.put({ key: "next", value: 1, identity });
    events.emit(directory, "state.json");
    events.emit(directory, "state.read-journal.jsonl");
    await settle();
    const reader = await openStore(projectorDatabaseRoot(directory));
    expect(reader.get("next")?.value).toBe(1);
    expect(tick).toHaveBeenCalledTimes(2);
    // A single delayed checkpoint deadline belongs to those just-projected WAL frames.
    await vi.advanceTimersByTimeAsync(250);
    expect(projector.status().checkpoints).toBe(2);
    const passes = tick.mock.calls.length;
    for (const name of ["state-projector.status.json", "state.json.tmp", ".lock", "lock-stats"]) events.emit(directory, name);
    events.emit(directory, null); // Filename-less status/unrelated notification: unchanged state.
    events.emit(projectorDatabaseRoot(directory), "state.db-wal");
    events.emit(projectorDatabaseRoot(directory), "state.db");
    events.emit(projectorDatabaseRoot(directory), null);
    await settle();
    await vi.advanceTimersByTimeAsync(3_000);
    expect(tick).toHaveBeenCalledTimes(passes);
    expect(vi.getTimerCount()).toBe(1);
    await projector.stop();
    expect(vi.getTimerCount()).toBe(0);
    expect(events.watches.every(watch => watch.closed)).toBe(true);
  });

  it("renews an active lease at half-life without any filesystem notifications", async () => {
    clock();
    notifications();
    const directory = root();
    const projector = await openProjector(directory, { leaseMs: 1_000 });
    const tick = vi.spyOn(projector, "tick");
    projector.run();
    await settle();
    const raw = openNodeSqlite(projector.database);
    const expiry = () => JSON.parse(String(raw.prepare("SELECT value FROM meta WHERE name = 'projector.lease'").get()?.value)).expiresAt as number;
    try {
      const before = expiry();
      await vi.advanceTimersByTimeAsync(499);
      expect(tick).toHaveBeenCalledTimes(1);
      expect(expiry()).toBe(before);
      await vi.advanceTimersByTimeAsync(1);
      expect(tick).toHaveBeenCalledTimes(2);
      expect(expiry()).toBe(before + 500);
      expect(projector.status().role).toBe("active");
    } finally { raw.close(); }
  });

  it("takes over a crashed holder exactly at lease expiry, with no periodic standby poll", async () => {
    clock();
    notifications();
    const projector = await openProjector(root(), { owner: "standby", leaseMs: 1_000 });
    const raw = openNodeSqlite(projector.database);
    try {
      raw.prepare("INSERT INTO meta(name, value) VALUES ('projector.lease', ?)").run(JSON.stringify({ owner: "crashed", expiresAt: Date.now() + 400 }));
      const tick = vi.spyOn(projector, "tick");
      projector.run();
      await settle();
      expect(projector.status()).toMatchObject({ role: "standby", leaseHolder: "crashed", checkpoints: 0 });
      expect(vi.getTimerCount()).toBe(1);
      await vi.advanceTimersByTimeAsync(399);
      expect(tick).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(tick).toHaveBeenCalledTimes(2);
      expect(projector.status()).toMatchObject({ role: "active", leaseHolder: "standby", checkpoints: 1 });
    } finally { raw.close(); }
  });

  it("checks divergence only after external database notifications, not idle verify deadlines", async () => {
    clock();
    const events = notifications();
    const directory = root();
    const file = fileStore(directory);
    await file.put({ key: "a", value: "authority", identity });
    const projector = await openProjector(directory, { leaseMs: 600_000, verifyMs: 60_000 });
    projector.run();
    await settle();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(projector.status().divergenceChecks).toBe(0);
    expect(vi.getTimerCount()).toBe(1); // Actual lease renewal only.
    const raw = openNodeSqlite(projector.database);
    try { raw.prepare("UPDATE kv SET value = ? WHERE key = ?").run(JSON.stringify("tampered"), "a"); }
    finally { raw.close(); }
    events.emit(projectorDatabaseRoot(directory), "state.db-wal");
    await settle();
    expect(projector.status()).toMatchObject({ divergenceChecks: 1, divergences: 1, lastResync: { reason: "divergence" } });
    events.emit(projectorDatabaseRoot(directory), "state.db");
    events.emit(projectorDatabaseRoot(directory), "state.db-wal");
    await vi.advanceTimersByTimeAsync(60_000);
    expect(projector.status().divergenceChecks).toBe(1);
    expect(vi.getTimerCount()).toBe(1);
  });

  it("native lease release wakes a standby long before its expiry deadline", async () => {
    const directory = root();
    const holder = await openProjector(directory, { owner: "holder", leaseMs: 60_000 });
    await holder.tick();
    let tookOver: (() => void) | undefined;
    const standby = await openProjector(directory, { owner: "standby", leaseMs: 60_000,
      onEvent: event => { if (event.type === "role" && event.role === "active") tookOver?.(); } });
    standby.run();
    await settle();
    expect(standby.status().role).toBe("standby");
    const notified = new Promise<void>(resolve => { tookOver = resolve; });
    await holder.stop();
    await bounded(notified);
    expect(standby.status()).toMatchObject({ role: "active", leaseHolder: "standby" });
  });

  it("native fs.watch follows state.json atomic replacement before any lease/checkpoint deadline", async () => {
    const directory = root();
    const file = fileStore(directory);
    await file.put({ key: "a", value: 1, identity });
    let applied: (() => void) | undefined;
    const projector = await openProjector(directory, { leaseMs: 60_000, checkpointMs: 60_000,
      beforeCommit: () => applied?.() });
    await projector.tick();
    projector.run();
    await settle();
    const notified = new Promise<void>(resolve => { applied = resolve; });
    await file.put({ key: "b", value: 2, identity });
    await bounded(notified);
    const reader = await openStore(projectorDatabaseRoot(directory));
    expect(reader.get("b")?.value).toBe(2);
    expect(projector.status().errors).toBe(0);
  });

  it("native WAL notifications checkpoint another connection promptly and remain quiet afterwards", async () => {
    const directory = root();
    const maintainer = await openStore(directory, { checkpoint: "maintainer", checkpointIntervalMs: 20 });
    const writer = await openStore(directory);
    // Wait for the initial, actual-work checkpoint, then observe the next one.
    const initial = new Promise<void>(resolve => {
      const original = maintainer.checkpoint.bind(maintainer);
      vi.spyOn(maintainer, "checkpoint").mockImplementation((threshold) => { const result = original(threshold); resolve(); return result; });
    });
    await bounded(initial);
    const notified = new Promise<void>(resolve => {
      const original = SqliteStateStore.prototype.checkpoint.bind(maintainer);
      vi.mocked(maintainer.checkpoint).mockImplementation((threshold) => { const result = original(threshold); resolve(); return result; });
    });
    await writer.put({ key: "external", value: 1, identity });
    await bounded(notified);
    expect(maintainer.stats().checkpoints.passive).toBe(2);
    await new Promise(resolve => setTimeout(resolve, 100));
    expect(maintainer.stats().checkpoints.passive).toBe(2);
  });

  it("default file stores and startup lock-stat samples have zero timers for the entire idle proof window", () => {
    clock();
    const key = Symbol.for("pi-fabric.mesh.lock-stats");
    const registry = globalThis as Record<symbol, { dispose(): void } | undefined>;
    const previous = registry[key];
    delete registry[key];
    cleanup.push(() => { registry[key]?.dispose(); registry[key] = previous; });
    const directory = root();
    fileStore(directory);
    const stats = createLockStats("1")!;
    stats.acquired(directory, "bridge", 0, 1);
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(305_000);
    expect(vi.getTimerCount()).toBe(0);
    expect(fs.existsSync(path.join(directory, "lock-stats"))).toBe(false);
    stats.failed(directory, "bridge", 1, true); // Real work flushes the earlier minute.
    expect(fs.readdirSync(path.join(directory, "lock-stats")).filter(name => name.endsWith(".json"))).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("names only opt-in minute diagnostics and shadow safety verification as recurring exceptions", () => {
    clock();
    const key = Symbol.for("pi-fabric.mesh.commit-stats");
    const registry = globalThis as Record<symbol, { dispose(): void } | undefined>;
    const previous = registry[key];
    delete registry[key];
    cleanup.push(() => { registry[key]?.dispose(); registry[key] = previous; });
    expect(createCommitStats("")).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
    delete registry[key];
    const interval = vi.spyOn(globalThis, "setInterval");
    createCommitStats(path.join(root(), "diagnostic.jsonl"));
    const options = { stateBackend: "shadow" as const, shadowVerifyMs: 10 };
    const mesh = new MeshStore(root(), 64 * 1024, 1_000, options);
    meshes.push(mesh);
    expect(mesh.stateBackendHandle).toBeInstanceOf(ShadowStateBackend);
    expect(interval.mock.calls.map(call => call[1])).toEqual([60_000, 60_000]);
    const timers = interval.mock.results.map(result => result.value as NodeJS.Timeout);
    expect(timers.every(timer => !timer.hasRef())).toBe(true);
  });
});
