import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createLockEvents, LOCK_EVENT_MAX_LINE_BYTES, LOCK_EVENTS_FILE, LOCK_EVENTS_ROTATE_BYTES, LOCK_HELD_EVENT_MS, lockEventLine,
  lockStatsHost, type LockEvent, type LockEvents,
} from "../src/mesh/commit-stats.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import type { MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#9290: held-time and wait-timeout events at the source, in <root>/lock-stats/events.jsonl.

const lockKey = Symbol.for("pi-fabric.mesh.lock-stats");
const eventsKey = Symbol.for("pi-fabric.mesh.lock-events");
const registry = globalThis as typeof globalThis & {
  [lockKey]?: { dispose(): void };
  [eventsKey]?: { dispose(): void; events: LockEvents | undefined };
};
const reset = (): void => {
  registry[lockKey]?.dispose();
  delete registry[lockKey];
  registry[eventsKey]?.dispose();
  delete registry[eventsKey];
};
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const temps: string[] = [];
const temp = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lock-events-"));
  temps.push(root);
  return root;
};
const eventsFile = (root: string): string => path.join(root, "lock-stats", LOCK_EVENTS_FILE);
const readEvents = (file: string): LockEvent[] => {
  if (!fs.existsSync(file)) return [];
  const text = fs.readFileSync(file, "utf8");
  expect(text === "" || text.endsWith("\n")).toBe(true);
  return text.split("\n").filter(Boolean).map((line) => {
    expect(Buffer.byteLength(line) + 1).toBeLessThanOrEqual(LOCK_EVENT_MAX_LINE_BYTES);
    return JSON.parse(line) as LockEvent;
  });
};
const spin = (ms: number): void => { const until = performance.now() + ms; while (performance.now() < until) { /* hold */ } };
const ownComm = (): string | undefined => {
  try { return fs.readFileSync(`/proc/${process.pid}/comm`, "utf8").trim(); } catch { return undefined; }
};

// The test environment disables the recorder (PI_FABRIC_LOCK_STATS=0): load modules that captured
// the production default, after an appender with a test warn hook (or none) was registered.
const load = async (options: { warn?: (message: string) => void } = {}) => {
  reset();
  vi.stubEnv("PI_FABRIC_LOCK_STATS", "");
  vi.resetModules();
  const stats = await import("../src/mesh/commit-stats.js");
  const events = stats.createLockEvents(undefined, options)!;
  const lock = await import("../src/mesh/mesh-lock.js");
  const sqlite = await import("../src/mesh/state-sqlite.js");
  const store = await import("../src/mesh/store.js");
  expect(lock.lockEvents).toBe(events);
  return { events, MeshLock: lock.MeshLock, SqliteStateStore: sqlite.SqliteStateStore, MeshStore: store.MeshStore };
};

beforeEach(reset);
afterEach(() => {
  reset();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  for (const root of temps.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const CHILD_PRELUDE = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { SqliteStateStore } = await jiti.import("./src/mesh/state-sqlite.ts");
const identity = { id: "child-" + process.pid, name: "child", kind: "agent" };
const until = async (file) => { while (!fs.existsSync(file)) await new Promise((resolve) => setTimeout(resolve, 5)); };
`;
const child = (code: string, args: string[]) => {
  const handle = spawn(process.execPath, ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", CHILD_PRELUDE + code, ...args],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, PI_FABRIC_LOCK_STATS: "1" } });
  let stdout = "";
  let stderr = "";
  handle.stdout.on("data", (chunk) => { stdout += chunk; });
  handle.stderr.on("data", (chunk) => { stderr += chunk; });
  const done = new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    handle.once("error", reject);
    handle.once("close", (exit) => resolve({ code: exit, stdout, stderr }));
  });
  return { pid: handle.pid!, done };
};

describe("lock events across processes", () => {
  // The SQLite writer lock has no owner record of its own: the waiter reads the kernel's (Linux /proc/locks).
  it.runIf(process.platform === "linux")("a SQLite writer-lock hold and the waiter that gave up: one held, one wait-timeout naming the holder", async () => {
    const root = temp();
    (await SqliteStateStore.open(root, 64 * 1024, 1_000)).close(); // initialised (test fixture), then imported
    const signal = (name: string): string => path.join(root, name);
    const holder = child(`
      const [root, opened, peer, holding] = process.argv.slice(1);
      const store = await SqliteStateStore.open(root, 65536, 1000);
      fs.writeFileSync(opened, "");
      await until(peer);
      await store.exclusive(() => {
        fs.writeFileSync(holding, "");
        const end = Date.now() + 1600; while (Date.now() < end) { /* hold the writer lock */ }
      });
      store.close();`, [root, signal("holder.opened"), signal("waiter.opened"), signal("holding")]);
    const waiter = child(`
      const [root, opened, holding] = process.argv.slice(1);
      const store = await SqliteStateStore.open(root, 65536, 1000, { lockTimeoutMs: 300 });
      fs.writeFileSync(opened, "");
      await until(holding);
      try { await store.put({ key: "w/1", value: 1, identity }); console.log("acquired"); }
      catch (error) { console.log(error.code); }
      store.close();`, [root, signal("waiter.opened"), signal("holding")]);
    const [held, waited] = await Promise.all([holder.done, waiter.done]);
    expect(held, held.stderr).toMatchObject({ code: 0 });
    expect(waited, waited.stderr).toMatchObject({ code: 0, stdout: "FABRIC_MESH_LOCK_TIMEOUT\n" });

    const events = readEvents(eventsFile(root));
    expect(events.map((event) => event.kind).sort()).toEqual(["held", "wait-timeout"]);
    const heldEvent = events.find((event) => event.kind === "held")!;
    const timeout = events.find((event) => event.kind === "wait-timeout")!;
    const host = lockStatsHost();
    expect(Object.keys(heldEvent)).toEqual(["v", "at", "kind", "store", "class", "heldMs", "holder"]);
    expect(heldEvent).toMatchObject({ v: 1, kind: "held", store: "sqlite", class: "custody",
      holder: { pid: holder.pid, op: "custody", host } });
    expect(heldEvent.heldMs).toBeGreaterThanOrEqual(1_500);
    expect(typeof heldEvent.holder.process).toBe("string");
    expect(Object.keys(timeout)).toEqual(["v", "at", "kind", "store", "class", "waitedMs", "holder", "waiter"]);
    expect(timeout).toMatchObject({ v: 1, kind: "wait-timeout", store: "sqlite", class: "put/delete",
      holder: { pid: holder.pid, process: heldEvent.holder.process, op: null, host },
      waiter: { pid: waiter.pid, op: "put/delete" } });
    expect(timeout.waitedMs).toBeGreaterThanOrEqual(290);
    expect(typeof timeout.waiter!.process).toBe("string");
    // The waiter gave up while the holder still held the lock; the holder reported at release.
    expect(timeout.at).toBeLessThanOrEqual(heldEvent.at);
  }, 60_000);
});

describe("lock events in one process", () => {
  it("mesh .lock: held above the threshold, wait-timeout naming the owner record's pid, nothing for a bounded try", async () => {
    const { MeshLock } = await load();
    const root = temp();
    const holder = new MeshLock(root, { lockHeldEventMs: 50 }, () => undefined);
    const waiter = new MeshLock(root, { lockTimeoutMs: 150 }, () => undefined);
    let release!: () => void;
    const released = new Promise<void>((resolve) => { release = resolve; });
    let entered!: () => void;
    const inside = new Promise<void>((resolve) => { entered = resolve; });
    const holding = holder.withLockAcrossAwait(async () => { entered(); await released; spin(60); }, undefined, "custody");
    await inside;
    // A fresh module graph (load): match the typed timeout by code, not by class identity.
    const timedOut = { code: "FABRIC_MESH_LOCK_TIMEOUT" };
    await expect(waiter.withLock(() => 1, 150, "publish")).rejects.toMatchObject(timedOut);
    await expect(waiter.withTryLock(() => waiter.withLock(() => 1, undefined, "publish"), 20)).rejects.toMatchObject(timedOut);
    await expect(waiter.withLock(() => 1, 0, "put/delete", true)).rejects.toMatchObject(timedOut);
    release();
    await holding;
    expect(await holder.withLock(() => 2, undefined, "other")).toBe(2); // quick: no event
    const events = readEvents(eventsFile(root));
    expect(events.map((event) => event.kind)).toEqual(["wait-timeout", "held"]);
    const [timeout, held] = events as [LockEvent, LockEvent];
    const self = ownComm() ?? process.title;
    expect(timeout).toMatchObject({ kind: "wait-timeout", store: "mesh-lock", class: "publish",
      holder: { pid: process.pid, process: self, op: null, host: lockStatsHost() },
      waiter: { pid: process.pid, process: self, op: "publish" } });
    expect(timeout.waitedMs).toBeGreaterThanOrEqual(140);
    expect(held).toMatchObject({ kind: "held", store: "mesh-lock", class: "custody",
      holder: { pid: process.pid, process: self, op: "custody", host: lockStatsHost() } });
    expect(held.heldMs).toBeGreaterThan(50);
    expect(held).not.toHaveProperty("waiter");
  });

  it("threshold 0 switches held events off (mesh .lock and SQLite); the default is 1000 ms", async () => {
    expect(LOCK_HELD_EVENT_MS).toBe(1_000);
    const { MeshLock, SqliteStateStore: Store } = await load();
    const root = temp();
    const off = new MeshLock(root, { lockHeldEventMs: 0 }, () => undefined);
    const byDefault = new MeshLock(root, {}, () => undefined);
    const on = new MeshLock(root, { lockHeldEventMs: 10 }, () => undefined);
    await off.withLock(() => spin(40), undefined, "publish");
    await byDefault.withLock(() => spin(40), undefined, "publish"); // under the 1 s default
    const sqliteOff = await Store.open(root, 64 * 1024, 1_000, { lockHeldEventMs: 0 });
    const sqliteOn = await Store.open(root, 64 * 1024, 1_000, { lockHeldEventMs: 10 });
    try {
      await sqliteOff.exclusive(() => spin(40));
      await sqliteOff.put({ key: "k/off", value: 1, identity });
      expect(readEvents(eventsFile(root))).toEqual([]);
      await on.withLock(() => spin(40), undefined, "writeBatch");
      await sqliteOn.exclusive(() => spin(40));
      await sqliteOn.writeBatch({ identity, ops: [], prepare: () => { spin(40); return [{ kind: "put", key: "k/on", value: 2 }]; } });
    } finally { sqliteOff.close(); sqliteOn.close(); }
    expect(readEvents(eventsFile(root)).map((event) => [event.kind, event.store, event.class])).toEqual([
      ["held", "mesh-lock", "writeBatch"], ["held", "sqlite", "custody"], ["held", "sqlite", "writeBatch"],
    ]);
  });

  it("MeshStore passes lockHeldEventMs to the .lock and to the SQLite writer lock", async () => {
    const { MeshStore } = await load();
    const root = temp();
    const mesh = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite", lockHeldEventMs: 10 });
    try {
      await mesh.writeBatch({ identity, ops: [], lockClass: "bridge", prepare: () => { spin(40); return [{ kind: "put", key: "k/1", value: 1 }]; } });
      await mesh.exclusive(() => spin(40));
    } finally { mesh.closeState(); }
    expect(readEvents(eventsFile(root)).map((event) => [event.kind, event.store, event.class])).toEqual([
      ["held", "sqlite", "bridge"], ["held", "mesh-lock", "custody"],
    ]);
  });

  it("PI_FABRIC_LOCK_STATS=0 disables the appender with the minute files", () => {
    expect(createLockEvents("0")).toBeUndefined();
    expect(createLockEvents("1")).toBeUndefined(); // captured once, including disabled
  });

  it("rotates at 1 MiB to events.jsonl.1, keeping one old file", () => {
    expect(LOCK_EVENTS_ROTATE_BYTES).toBe(1024 * 1024);
    const events = createLockEvents("1")!;
    const root = temp();
    fs.mkdirSync(path.join(root, "lock-stats"), { mode: 0o700 });
    const file = eventsFile(root);
    // Just under the cap: the next line would cross it.
    const filler = `${"x".repeat(1023)}\n`;
    fs.writeFileSync(file, filler.repeat(1023) + `${"y".repeat(1000)}\n`, { mode: 0o600 });
    const before = fs.statSync(file).size;
    expect(before).toBeLessThan(LOCK_EVENTS_ROTATE_BYTES);
    events.held(root, "mesh-lock", "publish", 1_234);
    expect(fs.statSync(`${file}.1`).size).toBe(before);
    expect(readEvents(file).map((event) => event.heldMs)).toEqual([1_234]);
    // Fill again: the second rotation replaces the first old file; no events.jsonl.2.
    fs.appendFileSync(file, filler.repeat(1024));
    events.waitTimeout(root, "sqlite", "put/delete", 10_000, undefined);
    expect(fs.readFileSync(`${file}.1`, "utf8").startsWith(JSON.stringify({ v: 1 }).slice(0, 6))).toBe(true);
    expect(fs.existsSync(`${file}.2`)).toBe(false);
    expect(fs.readdirSync(path.join(root, "lock-stats")).sort()).toEqual([LOCK_EVENTS_FILE, `${LOCK_EVENTS_FILE}.1`]);
    const [timeout] = readEvents(file);
    expect(timeout).toMatchObject({ kind: "wait-timeout", waitedMs: 10_000, holder: { pid: null, process: null, op: null } });
    expect(events.errors).toBe(0);
  });

  it("keeps every line within PIPE_BUF by truncating process names", () => {
    const event: LockEvent = { v: 1, at: 1, kind: "wait-timeout", store: "sqlite", class: "other", waitedMs: 1,
      holder: { pid: 1, process: "h".repeat(5_000), op: null, host: "host" }, waiter: { pid: 2, process: "w".repeat(5_000), op: "other" } };
    const line = lockEventLine(event);
    expect(Buffer.byteLength(line)).toBeLessThanOrEqual(LOCK_EVENT_MAX_LINE_BYTES);
    expect(line.endsWith("\n")).toBe(true);
    expect(JSON.parse(line)).toMatchObject({ holder: { pid: 1 }, waiter: { pid: 2 } });
  });

  it("a failing append never affects the lock: counted, warned at most once a minute", async () => {
    const warnings: string[] = [];
    const { events, MeshLock, SqliteStateStore: Store } = await load({ warn: (message) => warnings.push(message) });
    const root = temp();
    const original = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (String(file).endsWith(LOCK_EVENTS_FILE)) throw Object.assign(new Error("EIO: injected"), { code: "EIO" });
      return (original as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(Date.UTC(2026, 9, 11, 12, 0, 0));
    const lock = new MeshLock(root, { lockHeldEventMs: 5 }, () => undefined);
    expect(await lock.withLock(() => { spin(20); return "held"; }, undefined, "publish")).toBe("held");
    expect(events.errors).toBe(1);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("EIO: injected");
    // The lock was released normally: an immediate acquisition does not wait.
    const started = performance.now();
    expect(await lock.withLock(() => 7, 200, "other")).toBe(7);
    expect(performance.now() - started).toBeLessThan(150);
    const store = await Store.open(root, 64 * 1024, 1_000, { lockHeldEventMs: 5 });
    try {
      expect(await store.exclusive(() => { spin(20); return "sqlite"; })).toBe("sqlite");
      expect((await store.put({ key: "k/1", value: 1, identity })).version).toBe(1);
    } finally { store.close(); }
    expect(events.errors).toBe(2);
    expect(warnings).toHaveLength(1); // within the minute: counted, not warned again
    vi.setSystemTime(Date.UTC(2026, 9, 11, 12, 1, 1));
    await lock.withLock(() => spin(20), undefined, "publish");
    expect(events.errors).toBe(3);
    expect(warnings).toHaveLength(2);
    expect(warnings[1]).toContain("2 append error(s)");
    expect(fs.existsSync(eventsFile(root))).toBe(false);
  });

  it("an unwritable event file (a directory in its place) is an error, not a throw", async () => {
    const { events, MeshLock } = await load({ warn: () => undefined });
    const root = temp();
    fs.mkdirSync(eventsFile(root), { recursive: true });
    const lock = new MeshLock(root, { lockHeldEventMs: 5 }, () => undefined);
    expect(await lock.withLock(() => { spin(20); return 1; }, undefined, "publish")).toBe(1);
    expect(events.errors).toBe(1);
  });
});
