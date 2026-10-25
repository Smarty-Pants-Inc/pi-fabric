import { spawn } from "node:child_process";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createLockEvents, LOCK_EVENT_MAX_LINE_BYTES, LOCK_EVENTS_FILE, LOCK_EVENTS_ROTATE_BYTES, LOCK_EVENTS_ROTATE_LOCK,
  LOCK_EVENTS_ROTATE_LOCK_STALE_MS, LOCK_EVENTS_WIN32_ROTATION_ISSUE, LOCK_HELD_EVENT_MS, lockEventLine, lockStatsHost, type LockEvent, type LockEvents,
} from "../src/mesh/commit-stats.js";
import { openNodeSqlite, SqliteStateStore, type SqliteOpener } from "../src/mesh/state-sqlite.js";
import type { MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#9290: held-time and wait-timeout events at the source, in <root>/lock-stats/events.jsonl.

const lockKey = Symbol.for("pi-fabric.mesh.lock-stats");
const eventsKey = Symbol.for("pi-fabric.mesh.lock-events");
const registry = globalThis as typeof globalThis & {
  [lockKey]?: { dispose(): void; flush(): void };
  [eventsKey]?: { version?: number; dispose(): void; events: LockEvents | undefined };
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
// Handles a test opened on a temp root: closed before the root is removed, also when the test timed out (on
// Windows an open SQLite handle makes the removal fail with EPERM and masks the real failure).
const closers: (() => void)[] = [];
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
// Real store admission and fresh-root creation are setup, not the measured lock operation. A fresh root is created
// under the mesh fence (.lock, class "other"); Windows CI recorded this setup taking over the default 1 s threshold.
// It includes ACL inspection and DB initialisation. Run it with this file's statically imported module graph, whose
// recorder the test environment disabled (PI_FABRIC_LOCK_STATS=0), so the measured store reopens an imported root.
// Keep the setup connection open until afterEach: the last close deletes the WAL, and the measured store's first
// write would then restart it, an open overhead a store that created its root in-process never paid.
const initialisedRoot = async (open?: SqliteOpener): Promise<string> => {
  const root = temp();
  const setup = await SqliteStateStore.open(root, 64 * 1024, 1_000, open ? { open } : {});
  closers.push(() => setup.close());
  expect(readEvents(eventsFile(root))).toEqual([]);
  return root;
};
// Rotation is disabled on Windows (scope cut, smarty-dev#10191): tests of a real rotation run elsewhere.
const itRotates = it.skipIf(process.platform === "win32");
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
  for (const close of closers.splice(0).reverse()) try { close(); } catch { /* already closed by the test */ }
  reset();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
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
        fs.writeFileSync(holding + ".cgroup", fs.readFileSync("/proc/" + process.pid + "/cgroup"));
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
    // Compare the real recorder's output with the holder's proc record captured during custody.
    const cgroup = fs.readFileSync(signal("holding.cgroup"), "utf8").split("\n").find(line => line.startsWith("0::"))?.slice(3);
    if (cgroup && /^[\x20-\x7e]+$/.test(cgroup) && Buffer.byteLength(cgroup) <= 512) {
      expect(timeout.holder).toHaveProperty("cgroup", cgroup);
      const unit = cgroup.split("/").reverse().find(part => /.+\.(?:service|scope)$/.test(part));
      if (unit && Buffer.byteLength(unit) <= 256) expect(timeout.holder).toHaveProperty("unit", unit);
      else expect(timeout.holder).not.toHaveProperty("unit");
    }
    expect(heldEvent.holder).not.toHaveProperty("cgroup");
    expect(timeout.waitedMs).toBeGreaterThanOrEqual(290);
    expect(typeof timeout.waiter!.process).toBe("string");
    // The waiter gave up while the holder still held the lock; the holder reported at release.
    expect(timeout.at).toBeLessThanOrEqual(heldEvent.at);
  }, 60_000);
});

// Trust-boundary/failure-path checks: exercise the actual appender against bounded proc-file fixtures.
describe("lock events: timeout cgroup observation", () => {
  const fixture = () => {
    const root = temp();
    const file = path.join(root, "proc-cgroup");
    const open = fs.openSync;
    const observed: string[] = [];
    vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target).startsWith("/proc/")) {
        observed.push(String(target));
        if (String(target) === `/proc/${process.pid}/cgroup`) target = file;
      }
      return (open as (...args: unknown[]) => number)(target, ...rest);
    }) as typeof fs.openSync);
    const events = createLockEvents("1")!;
    const record = (text: string | Buffer, pid: number | undefined = process.pid) => {
      fs.writeFileSync(file, text);
      events.waitTimeout(root, "sqlite", "bridge", 10_000, pid);
      expect(events.errors).toBe(0);
      return readEvents(eventsFile(root)).at(-1)!.holder;
    };
    return { root, file, events, observed, record };
  };

  it.runIf(process.platform === "linux")("prefers unified membership, safely records printable text, and derives the nearest service/scope (never a slice)", () => {
    const { record } = fixture();
    expect(record("1:name=systemd:/old.service\n0::/user.slice/parent.service/app.slice/worker@a\\x2db.scope/leaf\n"))
      .toMatchObject({ cgroup: "/user.slice/parent.service/app.slice/worker@a\\x2db.scope/leaf", unit: "worker@a\\x2db.scope" });
    expect(record("1:name=systemd:/parent.service/child.scope/leaf\n"))
      .toMatchObject({ cgroup: "/parent.service/child.scope/leaf", unit: "child.scope" });
    expect(record('0::/quoted"\\\\.service/leaf\n'))
      .toMatchObject({ cgroup: '/quoted"\\\\.service/leaf', unit: 'quoted"\\\\.service' });
    expect(record("0::/user.slice/app.slice\n")).not.toHaveProperty("unit");
    expect(record("0::/\n")).toHaveProperty("cgroup", "/");
  });

  it.runIf(process.platform === "linux")("omits deleted cgroup memberships without erasing the sampled PID", () => {
    const { record } = fixture();
    for (const prefix of ["0::", "1:name=systemd:"]) {
      for (const cgroup of ["/gone.service (deleted)", "/parent.service/child.scope (deleted)"]) {
        const holder = record(`${prefix}${cgroup}\n`);
        expect(holder.pid).toBe(process.pid);
        expect(holder).not.toHaveProperty("cgroup");
        expect(holder).not.toHaveProperty("unit");
      }
      expect(record(`${prefix}/parent.service/child.scope\n`)).toMatchObject({
        pid: process.pid, cgroup: "/parent.service/child.scope", unit: "child.scope",
      });
      expect(record(`${prefix}/parent.service (deleted)/child.scope\n`)).toMatchObject({
        pid: process.pid, cgroup: "/parent.service (deleted)/child.scope", unit: "child.scope",
      });
    }
  });

  it.runIf(process.platform === "linux")("omits missing, malformed and oversized observations; caps reads and independently bounds cgroup/unit", () => {
    const { file, events, root, record, observed } = fixture();
    const read = vi.spyOn(fs, "readSync");
    for (const text of ["", "garbage\n", "0::relative\n", "0::/unterminated", "0::/a\n0::/b\n", "0::/a\nmalformed\n",
      "1:cpu:/not-systemd.service\n", "1:name=systemd:/a\n2:name=systemd:/b\n", "0::/ok\n1:cpu:/x\u001b\n",
      "0::/a/../b\n", "0::/x\u001b[31m.service\n", "0::/x\u0000.service\n", "0::/x\t.service\n", "0::/x\u202e.service\n",
      Buffer.from([48, 58, 58, 47, 255, 10]), `0::/${"x".repeat(512)}\n`, `0::/${"é".repeat(256)}\n`]) {
      const holder = record(text);
      expect(holder).not.toHaveProperty("cgroup");
      expect(holder).not.toHaveProperty("unit");
      expect(holder.pid).toBe(process.pid); // observation failure does not erase the sampled kernel PID
    }
    read.mockClear();
    expect(record(`0::/${"x".repeat(5_000)}\n`)).not.toHaveProperty("cgroup");
    expect(read.mock.results.reduce((sum, result) => sum + Number(result.value), 0)).toBe(4097);
    expect(read.mock.calls.every(([, buffer]) => (buffer as Buffer).byteLength <= 4097)).toBe(true);
    expect(record(`0::/${"x".repeat(511)}\n`).cgroup).toHaveLength(512);
    expect(record(`0::/${"x".repeat(248)}.service\n`).unit).toHaveLength(256);
    const longUnit = record(`0::/${"x".repeat(249)}.service\n`);
    expect(longUnit).toHaveProperty("cgroup", `/${"x".repeat(249)}.service`);
    expect(longUnit).not.toHaveProperty("unit");
    read.mockImplementationOnce(() => { throw new Error("proc read failed"); });
    expect(() => events.waitTimeout(root, "sqlite", "bridge", 10_000, process.pid)).not.toThrow();
    expect(readEvents(eventsFile(root)).at(-1)!.holder).not.toHaveProperty("cgroup");
    fs.renameSync(file, `${file}.gone`); // exited PID / unreadable proc entry
    expect(() => events.waitTimeout(root, "sqlite", "bridge", 10_000, process.pid)).not.toThrow();
    expect(readEvents(eventsFile(root)).at(-1)!.holder).not.toHaveProperty("cgroup");
    observed.length = 0;
    for (const pid of [undefined, 0, -1, 1.5, NaN, Number.MAX_SAFE_INTEGER + 1]) {
      events.waitTimeout(root, "sqlite", "bridge", 10_000, pid);
      expect(readEvents(eventsFile(root)).at(-1)!.holder).toMatchObject({ pid: null, process: null });
    }
    expect(observed).toEqual([]);
    expect(events.errors).toBe(0);
  });

  it("non-Linux omits membership without opening procfs", () => {
    vi.stubGlobal("process", Object.create(process, { platform: { value: "darwin" } }));
    const { record, observed } = fixture();
    const holder = record("0::/not-linux.service\n");
    expect(holder).not.toHaveProperty("cgroup");
    expect(holder).not.toHaveProperty("unit");
    expect(observed).toEqual([]);
  });

  it.each([false, true])("canonical foreign/unproven owner (legacy=%s) never receives local PID membership", async (legacy) => {
    const { MeshLock } = await load();
    const { pidNamespaceTestHooks, localFilesystemTestHooks } = await import("../src/core/pid-namespace.js");
    const own = "linux:0123456789abcdef0123456789abcdef:5a5a5a5a-5a5a-4a5a-8a5a-5a5a5a5a5a5a:4026531836";
    pidNamespaceTestHooks.set(own);
    localFilesystemTestHooks.set(0xEF53);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const { root, file, observed } = fixture();
      fs.writeFileSync(file, "0::/local-not-foreign.service\n");
      const record = `foreign\n${process.pid}\n${Date.now()}\n${legacy ? "" : `\n${own.replace(/1836$/, "2999")}\n`}`;
      fs.mkdirSync(path.join(root, ".lock"));
      const owner = path.join(root, ".lock", "owner");
      fs.writeFileSync(owner, record);
      const lock = new MeshLock(root, { lockProtocol: 3, lockTimeoutMs: 150 }, () => undefined);
      await lock.ownIncarnation;
      await expect(lock.withLock(() => "must not enter", undefined, "publish")).rejects.toMatchObject({
        foreignCode: legacy ? "FABRIC_MESH_LOCK_UNPROVEN_HOLDER" : "FABRIC_MESH_LOCK_FOREIGN_HOLDER",
      });
      const [event] = readEvents(eventsFile(root));
      expect(event).toMatchObject({ kind: "wait-timeout", store: "mesh-lock", holder: { pid: process.pid } });
      expect(event!.holder).not.toHaveProperty("cgroup");
      expect(event!.holder).not.toHaveProperty("unit");
      expect(observed).not.toContain(`/proc/${process.pid}/cgroup`);
      expect(fs.readFileSync(owner, "utf8")).toBe(record);
    } finally { pidNamespaceTestHooks.reset(); localFilesystemTestHooks.reset(); }
  });
});

describe("lock events: rotation across processes", () => {
  // Review P2: rotation is serialized by an O_EXCL lock and re-checks the inode, so no appender can
  // rename a newer file over .1 or drop its line on ENOENT. Sized for exactly one rotation, so every
  // line must be in events.jsonl or events.jsonl.1. Windows: rotation is disabled (smarty-dev#10191;
  // CI lost ~46% of the lines here), so every line must be in events.jsonl and no .1 may exist.
  it("N concurrent appenders across a forced rotation lose no lines", async () => {
    const root = temp();
    fs.mkdirSync(path.join(root, "lock-stats"), { mode: 0o700 });
    const writers = 8;
    const lines = 250;
    const sample = lockEventLine({ v: 1, at: Date.now(), kind: "held", store: "mesh-lock", class: "publish", heldMs: 100,
      holder: { pid: 1_000_000, process: "node", op: "publish", host: lockStatsHost() } });
    const total = writers * lines * Buffer.byteLength(sample);
    const rotateBytes = Math.floor(total * 0.6);
    const go = path.join(root, "go");
    const code = `
      import { createJiti } from "jiti";
      import { pathToFileURL } from "node:url";
      import fs from "node:fs";
      const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
      const { createLockEvents } = await jiti.import("./src/mesh/commit-stats.ts");
      const [root, go, lines, rotateBytes] = process.argv.slice(1);
      const events = createLockEvents("1", { rotateBytes: Number(rotateBytes), warn: (message) => console.error(message) });
      process.stdout.write("ready\\n");
      while (!fs.existsSync(go)) await new Promise((resolve) => setTimeout(resolve, 2));
      for (let i = 0; i < Number(lines); i++) events.held(root, "mesh-lock", "publish", 100 + i);
      console.log("errors=" + events.errors);`;
    const children = Array.from({ length: writers }, () => {
      const handle = spawn(process.execPath, ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", code,
        root, go, String(lines), String(rotateBytes)], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
      let stdout = "";
      let stderr = "";
      const ready = new Promise<void>((resolve) => handle.stdout.on("data", (chunk) => {
        stdout += chunk;
        if (stdout.includes("ready")) resolve();
      }));
      handle.stderr.on("data", (chunk) => { stderr += chunk; });
      const done = new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
        handle.once("error", reject);
        handle.once("close", (exit) => resolve({ code: exit, stdout, stderr }));
      });
      return { pid: handle.pid!, ready, done };
    });
    await Promise.all(children.map((entry) => entry.ready));
    fs.writeFileSync(go, "");
    const results = await Promise.all(children.map((entry) => entry.done));
    for (const result of results) {
      expect(result, result.stderr).toMatchObject({ code: 0 });
      expect(result.stdout).toContain("errors=0");
    }
    const file = eventsFile(root);
    if (process.platform === "win32") {
      // The real Windows check: append only, no rename, no rotation lock; nothing is lost.
      expect(fs.existsSync(`${file}.1`)).toBe(false);
      expect(fs.readdirSync(path.join(root, "lock-stats"))).toEqual([LOCK_EVENTS_FILE]);
      expect(results.some((result) => result.stderr.includes(LOCK_EVENTS_WIN32_ROTATION_ISSUE))).toBe(true);
    } else {
      expect(fs.existsSync(`${file}.1`)).toBe(true); // the rotation happened
      expect(fs.readdirSync(path.join(root, "lock-stats")).sort()).toEqual([LOCK_EVENTS_FILE, `${LOCK_EVENTS_FILE}.1`]);
    }
    const seen = [...readEvents(`${file}.1`), ...readEvents(file)].map((event) => `${event.holder.pid}:${event.heldMs}`);
    expect(seen).toHaveLength(writers * lines);
    expect(new Set(seen).size).toBe(writers * lines);
    for (const { pid } of children) for (let i = 0; i < lines; i++) expect(seen).toContain(`${pid}:${100 + i}`);
  }, 60_000);

  itRotates("a busy rotation lock defers rotation without losing the line; a crashed rotator's lock is broken", () => {
    const events = createLockEvents("1", { rotateBytes: 4096 })!;
    const root = temp();
    const directory = path.join(root, "lock-stats");
    fs.mkdirSync(directory, { mode: 0o700 });
    const file = eventsFile(root);
    const lock = path.join(directory, LOCK_EVENTS_ROTATE_LOCK);
    fs.writeFileSync(file, `${"x".repeat(4000)}\n`, { mode: 0o600 });
    fs.writeFileSync(lock, "", { mode: 0o600 }); // another process is rotating
    events.held(root, "mesh-lock", "publish", 1_001);
    expect(fs.existsSync(`${file}.1`)).toBe(false);
    expect(fs.readFileSync(file, "utf8")).toContain('"heldMs":1001');
    const old = (Date.now() - LOCK_EVENTS_ROTATE_LOCK_STALE_MS - 5_000) / 1000;
    fs.utimesSync(lock, old, old); // ... and crashed holding it
    events.held(root, "mesh-lock", "publish", 1_002);
    expect(fs.readFileSync(`${file}.1`, "utf8")).toContain('"heldMs":1001');
    expect(readEvents(file).map((event) => event.heldMs)).toEqual([1_002]);
    expect(fs.existsSync(lock)).toBe(false);
    expect(events.errors).toBe(0);
  });

  // PR #905 Windows CI: an append racing another process's handle on events.jsonl fails with
  // EPERM/EBUSY/EACCES there. Simulated on any OS: rotation is disabled on win32 (smarty-dev#10191),
  // a refused append retries against the current file with a bounded backoff; no line is lost.
  const oversized = (root: string): string => {
    fs.mkdirSync(path.join(root, "lock-stats"), { mode: 0o700 });
    const file = eventsFile(root);
    fs.writeFileSync(file, `${"x".repeat(4000)}\n`, { mode: 0o600 });
    return file;
  };
  /** heldMs of the event lines, skipping the oversized filler. */
  const heldOf = (file: string): unknown[] => fs.readFileSync(file, "utf8").split("\n")
    .filter((line) => line.startsWith("{")).map((line) => (JSON.parse(line) as LockEvent).heldMs);
  const refuse = (code: string): Error => Object.assign(new Error(`${code}: injected`), { code });

  it("win32: rotation is disabled, the oversized file only grows, and one warning names smarty-dev#10191", () => {
    const sleeps: number[] = [];
    const warnings: string[] = [];
    const events = createLockEvents("1", { rotateBytes: 4096, platform: "win32", sleep: (ms) => sleeps.push(ms),
      warn: (message) => warnings.push(message) })!;
    const root = temp();
    const file = oversized(root);
    const renames = vi.spyOn(fs, "renameSync");
    const original = fs.openSync;
    const opened: string[] = [];
    vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      opened.push(path.basename(String(target)));
      return (original as (...args: unknown[]) => number)(target, ...rest);
    }) as typeof fs.openSync);
    const before = fs.statSync(file).size;
    for (const heldMs of [2_001, 2_002, 2_003]) events.held(root, "mesh-lock", "publish", heldMs);
    expect(renames).not.toHaveBeenCalled(); // no rename ...
    expect(opened).toEqual([LOCK_EVENTS_FILE, LOCK_EVENTS_FILE, LOCK_EVENTS_FILE]); // ... and no rotation lock
    expect(fs.existsSync(`${file}.1`)).toBe(false);
    expect(fs.readdirSync(path.dirname(file))).toEqual([LOCK_EVENTS_FILE]);
    expect(heldOf(file)).toEqual([2_001, 2_002, 2_003]); // the file grows, no line lost
    expect(fs.statSync(file).size).toBeGreaterThan(before);
    expect(warnings).toHaveLength(1); // once per process, not per line
    expect(warnings[0]).toContain(LOCK_EVENTS_WIN32_ROTATION_ISSUE);
    expect(LOCK_EVENTS_WIN32_ROTATION_ISSUE).toBe("smarty-dev#10191");
    expect(events.errors).toBe(0);
    expect(sleeps).toEqual([]);
  });

  it("win32: below the rotation size there is no warning", () => {
    const warnings: string[] = [];
    const events = createLockEvents("1", { platform: "win32", warn: (message) => warnings.push(message) })!;
    const root = temp();
    for (const heldMs of [2_101, 2_102]) events.held(root, "mesh-lock", "publish", heldMs);
    expect(readEvents(eventsFile(root)).map((event) => event.heldMs)).toEqual([2_101, 2_102]);
    expect(warnings).toEqual([]);
    expect(events.errors).toBe(0);
  });

  it("win32: an append refused with EPERM/EBUSY/EACCES/ENOENT retries the current file with a bounded backoff", () => {
    const sleeps: number[] = [];
    const warnings: string[] = [];
    const events = createLockEvents("1", { platform: "win32", sleep: (ms) => sleeps.push(ms), warn: (message) => warnings.push(message) })!;
    const root = temp();
    const file = eventsFile(root);
    const original = fs.openSync;
    let refusals: string[] = [];
    let opens = 0;
    vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) === file) {
        opens++;
        const code = refusals.shift();
        if (code) throw refuse(code);
      }
      return (original as (...args: unknown[]) => number)(target, ...rest);
    }) as typeof fs.openSync);
    refusals = ["EPERM", "EBUSY", "EACCES", "ENOENT"]; // four refusals, the fifth try lands
    events.held(root, "mesh-lock", "publish", 3_001);
    expect(opens).toBe(5);
    expect(sleeps).toEqual([10, 20, 30, 40]);
    expect(readEvents(file).map((event) => event.heldMs)).toEqual([3_001]);
    expect(events.errors).toBe(0);
    // Refused for longer than the bound: exactly five tries, then one counted failure.
    opens = 0;
    sleeps.length = 0;
    refusals = Array.from({ length: 50 }, () => "EBUSY");
    events.held(root, "mesh-lock", "publish", 3_002);
    expect(opens).toBe(5);
    expect(sleeps).toEqual([10, 20, 30, 40]);
    expect(events.errors).toBe(1);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("EBUSY: injected");
    refusals = [];
    events.held(root, "mesh-lock", "publish", 3_003);
    expect(readEvents(file).map((event) => event.heldMs)).toEqual([3_001, 3_003]);
  });

  it("linux: unchanged, an EPERM append or rename counts at once; ENOENT is retried once without a delay", () => {
    const sleeps: number[] = [];
    const events = createLockEvents("1", { rotateBytes: 4096, platform: "linux", sleep: (ms) => sleeps.push(ms), warn: () => undefined })!;
    const root = temp();
    const file = oversized(root);
    const originalOpen = fs.openSync;
    const originalRename = fs.renameSync;
    let refusals: string[] = [];
    let opens = 0;
    vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target) === file) {
        opens++;
        const code = refusals.shift();
        if (code) throw refuse(code);
      }
      return (originalOpen as (...args: unknown[]) => number)(target, ...rest);
    }) as typeof fs.openSync);
    let renameRefusals = 1;
    vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
      if (renameRefusals-- > 0) throw refuse("EPERM");
      originalRename(from, to);
    }) as typeof fs.renameSync);
    events.held(root, "mesh-lock", "publish", 4_001); // the rename's EPERM is an append failure (as before)
    expect(events.errors).toBe(1);
    refusals = ["EPERM"];
    opens = 0;
    events.held(root, "mesh-lock", "publish", 4_002);
    expect(opens).toBe(1);
    expect(events.errors).toBe(2);
    refusals = ["ENOENT"];
    opens = 0;
    events.held(root, "mesh-lock", "publish", 4_003); // rotates, then lands in the new file
    expect(opens).toBe(3); // the refused open, then the retry's open and its reopen after the rotation
    expect(events.errors).toBe(2);
    expect(readEvents(file).map((event) => event.heldMs)).toEqual([4_003]);
    expect(sleeps).toEqual([]);
  });

  itRotates("a newer, smaller file whose id matches the rotated one (id reuse or a Windows id collision) is never renamed over .1", () => {
    const events = createLockEvents("1", { rotateBytes: 4096 })!;
    const root = temp();
    const file = oversized(root);
    const rotatedAway = `${"y".repeat(10)}\n`;
    const original = fs.lstatSync;
    let raced = false;
    vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
      if (String(target) !== file || raced) return original(target, options);
      raced = true;
      // Another process rotated the oversized file away and a newer one took the name; its file id
      // compares equal to the one this appender saw.
      const seen = original(file, { bigint: true });
      fs.renameSync(file, `${file}.1`);
      fs.writeFileSync(file, rotatedAway, { mode: 0o600 });
      return Object.assign(original(file, { bigint: true }), { dev: seen.dev, ino: seen.ino });
    }) as typeof fs.lstatSync);
    events.held(root, "mesh-lock", "publish", 5_001);
    expect(raced).toBe(true);
    expect(fs.readFileSync(`${file}.1`, "utf8")).toBe(`${"x".repeat(4000)}\n`); // the rotated file survived
    const [kept, line, ...rest] = fs.readFileSync(file, "utf8").split("\n");
    expect(`${kept}\n`).toBe(rotatedAway); // the newer file was not renamed: the line was appended to it
    expect(JSON.parse(line!)).toMatchObject({ kind: "held", heldMs: 5_001 });
    expect(rest).toEqual([""]);
    expect(events.errors).toBe(0);
  });
});

describe("lock events in one process", () => {
  it.each([false, true])("FIFO expiry (bounded=%s) is counted separately, while real contention still fails the timeout gate", async (bounded) => {
    const { events, MeshLock } = await load();
    const { meshLockQueueDirectory, MeshLockTicket } = await import("../src/mesh/lock-queue.js");
    const { MeshLockTimeoutError } = await import("../src/core/atomic-write.js");
    const { lockStats } = await import("../src/mesh/mesh-lock.js");
    const { main } = await import("../src/mesh-lock-stats-cli.js");
    const root = temp();
    const lock = new MeshLock(root, { lockTimeoutMs: 2_000 }, () => undefined);
    await lock.withLock(() => undefined); // seed the real stats file; no event for a quick acquisition
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const directory = meshLockQueueDirectory(root);
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const head = path.join(directory, `${"0".repeat(24)}-${process.pid}-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa`);
    fs.writeFileSync(head, "");
    const failed = vi.spyOn(lockStats!, "failed");
    const waitTimeout = vi.spyOn(events, "waitTimeout");
    const queueExpired = vi.spyOn(events, "queueExpired");
    const close = vi.spyOn(MeshLockTicket.prototype, "close");
    const operation = vi.fn();
    const pending = Array.from({ length: 3 }, () => lock.withLock(operation, 2_000, "publish", bounded).catch((error: unknown) => error));
    for (let elapsed = 0; elapsed < 2_000; elapsed += 50) {
      const now = Date.now() / 1000;
      fs.utimesSync(head, now, now); // live admission head, but canonical .lock is free
      await vi.advanceTimersByTimeAsync(50);
    }
    for (const error of await Promise.all(pending)) {
      expect(error).toBeInstanceOf(MeshLockTimeoutError); // caller compatibility, not diagnostic attribution
      expect(error).toMatchObject({ name: "MeshLockQueueExpiredError", code: "FABRIC_MESH_LOCK_TIMEOUT", attempts: 0 });
    }
    expect(operation).not.toHaveBeenCalled();
    expect(failed).not.toHaveBeenCalled();
    expect(waitTimeout).not.toHaveBeenCalled();
    expect(queueExpired).toHaveBeenCalledTimes(3);
    expect(close.mock.invocationCallOrder[0]!).toBeLessThan(queueExpired.mock.invocationCallOrder[0]!);
    expect(readEvents(eventsFile(root))).toHaveLength(3);
    for (const event of readEvents(eventsFile(root))) {
      expect(event).toMatchObject({ v: 1, kind: "pre-lock", preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED", store: "mesh-lock", class: "publish",
        waitedMs: 2_000, holder: { pid: null, process: null, op: null }, waiter: { pid: process.pid, op: "publish" } });
      expect(event.waitersAhead).toBeGreaterThanOrEqual(1);
    }
    const meter = () => {
      registry[lockKey]!.flush();
      let json = "";
      const exit = main(["--mesh", root, "--json", "--max-timeouts", "0"],
        { now: Date.now() + 60_000, stdout: text => { json += text; } });
      return { exit, summary: JSON.parse(json) as { n: number; timeouts: number; tries: number } };
    };
    expect(meter()).toMatchObject({ exit: 0, summary: { n: 1, timeouts: 0, tries: 0 } });

    // Counterexample: an eligible head really probes a held canonical lock. It must still count as contention.
    fs.renameSync(head, path.join(root, "fixture-previous-head"));
    fs.mkdirSync(path.join(root, ".lock"));
    fs.writeFileSync(path.join(root, ".lock", "owner"), `held\n${process.pid}\n${Date.now()}\n`);
    const contended = lock.withLock(operation, 100, "publish").catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(150);
    const timeout = await contended;
    expect(timeout).toBeInstanceOf(MeshLockTimeoutError);
    expect(timeout).toMatchObject({ name: "MeshLockTimeoutError" });
    expect((timeout as InstanceType<typeof MeshLockTimeoutError>).attempts).toBeGreaterThan(0);
    expect(failed).toHaveBeenCalledTimes(1);
    expect(waitTimeout).toHaveBeenCalledTimes(1);
    expect(queueExpired).toHaveBeenCalledTimes(3);
    expect(readEvents(eventsFile(root)).filter(event => event.kind === "wait-timeout")).toHaveLength(1);
    expect(meter()).toMatchObject({ exit: 3, summary: { n: 1, timeouts: 1, tries: 0 } });
  });

  it("SQLite native BEGIN timeout emits one wait-timeout event", async () => {
    const { events, SqliteStateStore: Store } = await load();
    const { openNodeSqlite } = await import("../src/mesh/state-sqlite.js");
    const root = await initialisedRoot();
    const store = await Store.open(root, 64 * 1024, 1_000, { lockTimeoutMs: 100 });
    const holder = openNodeSqlite(store.file);
    closers.push(() => store.close(), () => holder.close());
    holder.exec("BEGIN IMMEDIATE");
    const generic = vi.spyOn(events, "waitTimeout");
    try {
      const timeout = await store.put({ key: "native/refused", value: 1, identity }).catch((error: unknown) => error);
      expect(timeout).toMatchObject({ name: "MeshLockTimeoutError", code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect((timeout as { attempts: number }).attempts).toBeGreaterThan(0);
      expect(store.stats()).toMatchObject({ lockTimeouts: 1, writesWaiting: 0, fifoJoins: 0 });
      expect(store.get("native/refused")).toBeUndefined();
      expect(generic).toHaveBeenCalledTimes(1);
      expect(readEvents(eventsFile(root))).toMatchObject([{ kind: "wait-timeout", store: "sqlite", class: "put/delete" }]);
      expect(readEvents(eventsFile(root))).toHaveLength(1);
    } finally {
      if (holder.isTransaction) holder.exec("ROLLBACK");
      holder.close();
      store.close();
    }
  });

  it("MeshStore's SQLite receiver wraps and counts a real BEGIN timeout", async () => {
    const { events, MeshStore, SqliteStateStore: Store } = await load();
    const { MeshStateBusyError } = await import("../src/mesh/state-backend.js");
    const { openNodeSqlite } = await import("../src/mesh/state-sqlite.js");
    const root = temp();
    const mesh = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite", lockTimeoutMs: 100 });
    await mesh.put({ key: "receiver/seed", value: 0, identity });
    const holder = openNodeSqlite(path.join(root, "state.db"));
    holder.exec("BEGIN IMMEDIATE");
    const generic = vi.spyOn(events, "waitTimeout");
    try {
      const timeout = await mesh.put({ key: "receiver/contended", value: 1, identity }).catch((error: unknown) => error);
      expect(timeout).toBeInstanceOf(MeshStateBusyError);
      expect((timeout as { attempts: number }).attempts).toBeGreaterThan(0);
      expect(mesh.stateDiagnostics().busyTimeouts).toBe(1);
      expect(generic).toHaveBeenCalledTimes(1);
      expect(readEvents(eventsFile(root))).toMatchObject([{ kind: "wait-timeout", store: "sqlite", class: "put/delete" }]);
      expect(mesh.get("receiver/contended")).toBeUndefined();
    } finally {
      if (holder.isTransaction) holder.exec("ROLLBACK");
      holder.close();
      mesh.closeState();
    }
  });

  it.each(["kind", "preLockCode"] as const)("the SQLite receiver preserves a caller-compatible refusal identified only by %s", async (field) => {
    const { MeshStore, SqliteStateStore: Store } = await load();
    const { MeshLockTimeoutError } = await import("../src/core/atomic-write.js");
    const root = temp();
    const mesh = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
    try {
      await mesh.put({ key: "marker/seed", value: 0, identity });
      const refusal = Object.assign(new MeshLockTimeoutError("", 0, 0),
        { [field]: field === "kind" ? "pre-lock" : "FABRIC_MESH_LOCK_QUEUE_EXPIRED" });
      refusal.message = "pre-lock compatibility marker probe: acquisition not attempted";
      vi.spyOn(Store.prototype, "put").mockRejectedValue(refusal);
      await expect(mesh.put({ key: "marker/refused", value: 1, identity })).rejects.toBe(refusal);
      expect(mesh.stateDiagnostics().busyTimeouts).toBe(0);
    } finally { mesh.closeState(); }
  });

  it("adds pre-lock events to a captured legacy appender without replacing its existing sink or losing append errors", async () => {
    const original = createLockEvents("1", { warn: () => undefined })!;
    const legacy = { held: original.held, waitTimeout: original.waitTimeout, get errors() { return original.errors; } };
    registry[eventsKey] = { dispose() {}, events: legacy as LockEvents }; // pre-queue-event runtime generation
    const upgraded = createLockEvents("0", { warn: () => undefined })!; // first owner's opt-in stays in force
    expect(upgraded.held).toBe(legacy.held);
    expect(upgraded.waitTimeout).not.toBe(legacy.waitTimeout);
    const root = temp();
    upgraded.queueExpired(root, "mesh-lock", "bridge", 2_000, 1);
    upgraded.waitTimeout(root, "mesh-lock", "publish", 100, process.pid);
    expect(readEvents(eventsFile(root)).map(event => [event.kind, event.preLockCode])).toEqual([
      ["pre-lock", "FABRIC_MESH_LOCK_QUEUE_EXPIRED"], ["wait-timeout", undefined],
    ]);
    vi.resetModules();
    const reloaded = await import("../src/mesh/commit-stats.js");
    expect(reloaded.createLockEvents("0")).toBe(upgraded);
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (String(file) === eventsFile(root)) throw Object.assign(new Error("EIO: injected"), { code: "EIO" });
      return (open as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    upgraded.held(root, "mesh-lock", "publish", 1_001);
    upgraded.queueExpired(root, "mesh-lock", "bridge", 2_000, undefined);
    expect(original.errors).toBe(1);
    expect(upgraded.errors).toBe(2); // both old and new appender failures remain visible
  });

  it("upgrades a captured v2 queue event writer to the full refusal code and current timeout observation", () => {
    const original = createLockEvents("1")!;
    const obsoleteQueueWriter = vi.fn();
    registry[eventsKey] = { version: 2, dispose() {}, events: { held: original.held, waitTimeout: original.waitTimeout,
      queueExpired: obsoleteQueueWriter, get errors() { return original.errors; } } };
    const current = createLockEvents("1")!;
    expect(current.waitTimeout).not.toBe(original.waitTimeout);
    const root = temp();
    current.queueExpired(root, "sqlite", "publish", 2_000, 2);
    expect(obsoleteQueueWriter).not.toHaveBeenCalled();
    expect(readEvents(eventsFile(root))).toMatchObject([{ kind: "pre-lock", preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED",
      store: "sqlite", waitersAhead: 2 }]);
    expect(readEvents(eventsFile(root))).toHaveLength(1);
    expect(createLockEvents("0")).toBe(current);
  });

  it.runIf(process.platform === "linux")("a v3 reload captures the new timeout recorder once without enabling a disabled singleton", async () => {
    const root = temp();
    const oldTimeout = vi.fn();
    const original = createLockEvents("1")!;
    registry[eventsKey] = { version: 3, dispose() {}, events: { held: original.held, waitTimeout: oldTimeout,
      queueExpired: original.queueExpired, get errors() { return original.errors; } } };
    const upgraded = createLockEvents("0")!;
    upgraded.waitTimeout(root, "sqlite", "bridge", 10_000, process.pid);
    expect(oldTimeout).not.toHaveBeenCalled();
    expect(readEvents(eventsFile(root))).toHaveLength(1);
    expect(readEvents(eventsFile(root))[0]!.holder.pid).toBe(process.pid);
    vi.resetModules();
    const reloaded = await import("../src/mesh/commit-stats.js");
    expect(reloaded.createLockEvents("1")).toBe(upgraded);
    reset();
    expect(reloaded.createLockEvents("0")).toBeUndefined();
    expect(createLockEvents("1")).toBeUndefined();
    expect(readEvents(eventsFile(root))).toHaveLength(1);
  });

  // smarty-dev#12036: pending incarnation is not contention. A shorter-budget cold acquisition
  // has its own retry code before it ever tries .lock, with no wait-timeout event.
  it("a cold protocol-2 pending acquisition records no wait-timeout event", async () => {
    const { MeshLock } = await load();
    const root = temp();
    const holder = new MeshLock(root, {}, () => undefined);
    let release!: () => void;
    const released = new Promise<void>((resolve) => { release = resolve; });
    let entered!: () => void;
    const inside = new Promise<void>((resolve) => { entered = resolve; });
    const holding = holder.withLockAcrossAwait(async () => { entered(); await released; }, undefined, "custody");
    await inside;
    const cold = new MeshLock(root, { lockProtocol: 2 }, () => undefined);
    expect(cold.ownIncarnationReady).toBe(false);
    await expect(cold.withLock(() => 1, 50, "publish")).rejects.toMatchObject({ code: "FABRIC_MESH_INCARNATION_PENDING" });
    release();
    await holding;
    expect(readEvents(eventsFile(root))).toEqual([]);
    // Once ready, the same store tries .lock: a full-budget timeout is an event again.
    await cold.ownIncarnation;
    const holdingAgain = holder.withLockAcrossAwait(async () => { await new Promise((resolve) => setTimeout(resolve, 300)); }, undefined, "custody");
    await new Promise((resolve) => setTimeout(resolve, 20));
    await expect(cold.withLock(() => 1, 100, "publish")).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    await holdingAgain;
    expect(readEvents(eventsFile(root)).map((event) => [event.kind, event.class])).toEqual([["wait-timeout", "publish"]]);
  });

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
    const root = await initialisedRoot();
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

  itRotates("rotates at 1 MiB to events.jsonl.1, keeping one old file", () => {
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

  // Review round 3 P2: native holder diagnostics are appended only after holder lookup.
  // pi-fabric Windows CI: a fresh root's creation under the mesh fence (the private-ACL child) is a real held .lock
  // event when it is what is measured; separated as setup, it cannot leak into the measured lock operation's events.
  it("fresh-root creation overhead is a held .lock event only when measured, never in a separately measured operation", async () => {
    const { SqliteStateStore: Store } = await load();
    // A slow initial open stands in for the Windows ACL child: both run inside the fresh-root fence.
    const slowOpen: SqliteOpener = (file) => { spin(LOCK_HELD_EVENT_MS + 100); return openNodeSqlite(file); };
    const measuredCreation = temp();
    (await Store.open(measuredCreation, 64 * 1024, 1_000, { open: slowOpen })).close();
    const creation = readEvents(eventsFile(measuredCreation));
    expect(creation.map((event) => [event.kind, event.store, event.class])).toEqual([["held", "mesh-lock", "other"]]);
    expect(creation[0]).toMatchObject({ holder: { pid: process.pid, op: "other", host: lockStatsHost() } });
    expect(creation[0]!.heldMs).toBeGreaterThan(LOCK_HELD_EVENT_MS);
    // The same slow creation as setup: the measured store's hold and timeout are the only events.
    const root = await initialisedRoot(slowOpen);
    const store = await Store.open(root, 64 * 1024, 1_000, { lockTimeoutMs: 100, lockHeldEventMs: 10 });
    const holder = openNodeSqlite(store.file);
    closers.push(() => store.close(), () => holder.close());
    try {
      await store.exclusive(() => spin(40));
      holder.exec("BEGIN IMMEDIATE");
      await expect(store.put({ key: "w/1", value: 1, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    } finally {
      if (holder.isTransaction) holder.exec("ROLLBACK");
      holder.close();
      store.close();
    }
    expect(readEvents(eventsFile(root)).map((event) => [event.kind, event.store, event.class])).toEqual([
      ["held", "sqlite", "custody"], ["wait-timeout", "sqlite", "put/delete"],
    ]);
  });

  it("SQLite wait-timeout performs native holder lookup before the event append", async () => {
    const { events, SqliteStateStore: Store } = await load();
    const root = await initialisedRoot();
    const order: string[] = [];
    const waitTimeout = events.waitTimeout.bind(events);
    vi.spyOn(events, "waitTimeout").mockImplementation((...args: Parameters<LockEvents["waitTimeout"]>) => {
      order.push("event.append");
      return waitTimeout(...args);
    });
    const store = await Store.open(root, 64 * 1024, 1_000, { lockTimeoutMs: 100 });
    const holder = new (process.getBuiltinModule("node:sqlite").DatabaseSync)(store.file, { timeout: 0 });
    closers.push(() => store.close(), () => holder.close());
    const readFileSync = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      if (file === "/proc/locks") order.push("proc-locks");
      return (readFileSync as (...args: unknown[]) => unknown)(file, ...rest);
    }) as typeof fs.readFileSync);
    syncBuiltinESMExports();
    try {
      holder.exec("BEGIN IMMEDIATE");
      await expect(store.put({ key: "w/1", value: 1, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    } finally {
      vi.mocked(fs.readFileSync).mockRestore();
      syncBuiltinESMExports();
      if (holder.isTransaction) holder.exec("ROLLBACK");
      holder.close();
      store.close();
    }
    expect(order.filter((step) => step === "event.append")).toHaveLength(1);
    if (process.platform === "linux") expect(order.indexOf("proc-locks")).toBeGreaterThanOrEqual(0);
    if (process.platform === "linux") expect(order.indexOf("proc-locks")).toBeLessThan(order.indexOf("event.append"));
    expect(readEvents(eventsFile(root))).toMatchObject([{ kind: "wait-timeout", store: "sqlite", class: "put/delete" }]);
  });

  it("a failing append never affects the lock: counted, warned at most once a minute", async () => {
    const warnings: string[] = [];
    const { events, MeshLock, SqliteStateStore: Store } = await load({ warn: (message) => warnings.push(message) });
    const root = await initialisedRoot();
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
