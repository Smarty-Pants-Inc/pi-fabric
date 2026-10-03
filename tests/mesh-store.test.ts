import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  MeshBatchConflictError,
  MeshStore,
  type MeshIdentity,
  type MeshStateEntry,
  type MeshStoreOptions,
} from "../src/mesh/store.js";

const roots: string[] = [];
const identity: MeshIdentity = {
  id: "session:test",
  name: "main",
  kind: "main",
  sessionId: "test",
};

const mockNativePlatform = (platform: "darwin" | "win32", start: string) => {
  vi.spyOn(process, "platform", "get").mockReturnValue(platform);
  vi.stubEnv("SystemRoot", "C:\\Windows");
  const read = fs.readFileSync.bind(fs);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (String(file).startsWith("/proc/")) throw Object.assign(new Error("no procfs"), { code: "ENOENT" });
    return (read as (...args: unknown[]) => unknown)(file, ...args);
  }) as typeof fs.readFileSync);
  return vi.spyOn(childProcess, "execFile").mockImplementation(((...args: unknown[]) => {
    const done = args[args.length - 1] as (error: Error | null, stdout: string, stderr: string) => void;
    done(null, start + "\n", "");
    return {} as childProcess.ChildProcess;
  }) as typeof childProcess.execFile);
};
const createStore = (options?: MeshStoreOptions, Store: typeof MeshStore = MeshStore): MeshStore => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-"));
  roots.push(root);
  return new Store(root, 64 * 1024, 100, options);
};

// A simulated native platform must not inherit the process-wide own-PID promise
// from earlier real publication tests (notably on native Windows CI). Reload both
// the reader and its importing store; production own-PID memoization is unchanged.
const createSimulatedNativeStore = async (): Promise<MeshStore> => {
  vi.resetModules();
  const { MeshStore: Store } = await import("../src/mesh/store.js");
  return createStore({ lockProtocol: 2, lockTimeoutMs: 100 }, Store);
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("MeshStore", () => {
  it("publishes durable ordered events and reads from a cursor", async () => {
    const store = createStore();
    const initialOffset = store.latestOffset();
    const first = await store.publish({ topic: "team.auth", from: identity, text: "one" });
    const second = await store.publish({
      topic: "team.auth",
      from: identity,
      to: "reviewer",
      text: "two",
      data: { task: 2 },
    });

    expect(first.sequence).toBe(1);
    expect(second.sequence).toBe(2);
    expect(store.read({ after: first.sequence })).toMatchObject([
      { sequence: 2, to: "reviewer", text: "two", data: { task: 2 } },
    ]);
    expect(store.read({ topic: "team.auth", to: "reviewer" })).toHaveLength(1);
    const firstTail = store.tail(initialOffset, 1);
    expect(firstTail.events).toMatchObject([{ sequence: 1, text: "one" }]);
    const secondTail = store.tail(firstTail.nextOffset, 10);
    expect(secondTail.events).toMatchObject([{ sequence: 2, text: "two" }]);
    expect(secondTail.nextOffset).toBe(store.latestOffset());
  });

  it("captures a coherent tail cursor without using reserved or partial sequences", async () => {
    const store = createStore();
    expect(store.latestCursor()).toEqual({ cursor: 0, last: { sequence: 0, id: "" } });
    const event = await store.publish({ topic: "team.auth", from: identity, text: "complete" });
    const cursor = store.latestOffset();
    fs.writeFileSync(path.join(store.root, "sequence"), "999");
    fs.appendFileSync(path.join(store.root, "events.jsonl"), '{"sequence":999,"id":"partial"');
    expect(store.latestCursor()).toEqual({ cursor, last: { sequence: event.sequence, id: event.id } });
    expect(store.tail(cursor).events).toEqual([]);
  });

  it("does not anchor beyond the captured file offset when an append follows the size read", async () => {
    const store = createStore();
    const first = await store.publish({ topic: "team.auth", from: identity, text: "first" });
    const cursor = store.latestOffset();
    const second = await store.publish({ topic: "team.auth", from: identity, text: "unread" });
    const live = path.join(store.root, "events.jsonl");
    const bytes = fs.readFileSync(live);
    const unread = bytes.subarray(cursor);
    fs.writeFileSync(live, bytes.subarray(0, cursor));
    // The sequence reservation (and, with an archive, its durable append) can already
    // name second. Startup must use the first line's same-handle boundary instead.
    const fstat = fs.fstatSync.bind(fs);
    let appended = false;
    const stat = vi.spyOn(fs, "fstatSync").mockImplementation(((...args: Parameters<typeof fs.fstatSync>) => {
      const snapshot = fstat(...args);
      if (!appended) { appended = true; fs.appendFileSync(live, unread); }
      return snapshot;
    }) as typeof fs.fstatSync);
    let boundary: ReturnType<MeshStore["latestCursor"]>;
    try { boundary = store.latestCursor(); } finally { stat.mockRestore(); }
    expect(appended).toBe(true);
    expect(boundary).toEqual({ cursor, last: { sequence: first.sequence, id: first.id } });
    expect(store.tail(boundary.cursor).events.map((event) => event.id)).toEqual([second.id]);
  });

  it("reads only a bounded last line to anchor an existing live tail", () => {
    const store = createStore();
    const live = path.join(store.root, "events.jsonl");
    const prefix = JSON.stringify({ sequence: 1, id: "old", text: "x".repeat(500) }) + "\n";
    const last = JSON.stringify({ sequence: 2, id: "last" }) + "\n";
    fs.writeFileSync(live, prefix.repeat(400) + last);
    const reads = vi.spyOn(fs, "readSync");
    const boundary = store.latestCursor();
    const bytesRead = reads.mock.results.reduce((sum, result) => sum + Number(result.value), 0);
    reads.mockRestore();
    expect(boundary.last).toEqual({ sequence: 2, id: "last" });
    expect(boundary.cursor).toBe(fs.statSync(live).size);
    expect(bytesRead).toBeLessThanOrEqual(store.maxEventBytes + 3);
  });

  it("repairs an interrupted append without reusing sequence numbers", async () => {
    const store = createStore();
    await store.publish({ topic: "team.auth", from: identity, text: "one" });
    fs.writeFileSync(path.join(store.root, "sequence"), "0");
    fs.appendFileSync(path.join(store.root, "events.jsonl"), '{"sequence":999');

    const second = await store.publish({ topic: "team.auth", from: identity, text: "two" });
    expect(second.sequence).toBe(2);
    expect(store.read()).toMatchObject([
      { sequence: 1, text: "one" },
      { sequence: 2, text: "two" },
    ]);
  });

  it("invalidates cached state when another store replaces the file", async () => {
    const writer = createStore();
    const reader = new MeshStore(writer.root, 64 * 1024, 100);
    await writer.put({ key: "shared/value", value: { revision: 1 }, identity });
    expect(reader.get("shared/value")?.value).toEqual({ revision: 1 });

    await writer.put({ key: "shared/value", value: { revision: 2 }, identity });
    expect(reader.get("shared/value")?.value).toEqual({ revision: 2 });
  });

  it("recovers the newest complete state when snapshots are concatenated", async () => {
    const store = createStore();
    await store.put({ key: "shared/value", value: { revision: 1 }, identity });
    const statePath = path.join(store.root, "state.json");
    const first = fs.readFileSync(statePath, "utf8");
    await store.put({ key: "shared/value", value: { revision: 2 }, identity });
    const second = fs.readFileSync(statePath, "utf8");
    fs.writeFileSync(statePath, `${first}\n${second}`);

    expect(store.get("shared/value")?.value).toEqual({ revision: 2 });

    await store.put({ key: "shared/other", value: true, identity });
    const normalized = JSON.parse(fs.readFileSync(statePath, "utf8")) as {
      entries: Record<string, MeshStateEntry>;
    };
    expect(normalized.entries["shared/value"]?.value).toEqual({ revision: 2 });
    expect(normalized.entries["shared/other"]?.value).toBe(true);
  });

  it("recovers state with complete non-state JSON records appended", async () => {
    const store = createStore();
    await store.put({ key: "shared/value", value: { revision: 1 }, identity });
    const statePath = path.join(store.root, "state.json");
    fs.appendFileSync(statePath, '\n{"tick":1}\n{"tick":2}\n');

    expect(store.get("shared/value")?.value).toEqual({ revision: 1 });
  });

  it("treats an empty state file as a missing table", () => {
    const store = createStore();
    const statePath = path.join(store.root, "state.json");
    fs.mkdirSync(store.root, { recursive: true });
    fs.writeFileSync(statePath, "");

    expect(store.listAll()).toEqual([]);
    expect(store.get("shared/value")).toBeUndefined();
    expect(fs.readFileSync(statePath, "utf8")).toBe("");
  });

  // smarty-dev#251 (dev1 load P0): runtime stores reuse a recent parse instead of re-reading
  // the shared state on every change by another process.
  it("reuses a recent parse for reads, sees its own writes at once, and writes against the file", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-cache-"));
    const reader = new MeshStore(root, 64 * 1024, 100, { readCacheMs: 400 });
    const other = new MeshStore(root, 64 * 1024, 100);
    try {
      await reader.put({ key: "cache/own", value: 1, identity });
      expect(reader.get("cache/own")?.value).toBe(1);                   // its own write, at once
      const theirs = await other.put({ key: "cache/theirs", value: 1, identity });
      const parses = vi.spyOn(fs, "readFileSync");
      for (let index = 0; index < 20; index++) reader.listAll("cache/");
      expect(parses.mock.calls.filter(([file]) => String(file).endsWith("state.json"))).toHaveLength(0);
      parses.mockRestore();
      expect(reader.get("cache/theirs")).toBeUndefined();               // within the window: the recent parse
      expect(reader.get("cache/theirs", { fresh: true })?.version).toBe(theirs.version);   // fresh: the file
      expect(reader.listAll("cache/", { fresh: true }).map((entry) => entry.key)).toEqual(["cache/own", "cache/theirs"]);
      // A write still reads the file under the lock: a stale view cannot pass a version check.
      await expect(reader.put({ key: "cache/theirs", value: 2, identity, ifVersion: 0 })).rejects.toThrow("compare-and-swap failed");
      expect(reader.get("cache/theirs")?.version).toBe(theirs.version);  // a failed write drops the cache
      await other.put({ key: "cache/later", value: 1, identity });
      await new Promise((resolve) => setTimeout(resolve, 450));
      expect(reader.get("cache/later")?.value).toBe(1);                 // after the window: re-read
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps re-reading on every change by default", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-cache-"));
    const reader = new MeshStore(root, 64 * 1024, 100);
    const other = new MeshStore(root, 64 * 1024, 100);
    try {
      reader.listAll();
      await other.put({ key: "cache/now", value: 1, identity });
      expect(reader.get("cache/now")?.value).toBe(1);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps at most 1,000 tombstones by default, and an evicted key still conflicts on a stale version", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-tombstones-"));
    const store = new MeshStore(root, 64 * 1024, 100);
    try {
      const first = await store.put({ key: "gone/0", value: 0, identity });
      await store.delete({ key: "gone/0" });
      for (let index = 1; index <= 1_050; index++) {
        await store.writeBatch({ identity, ops: [{ kind: "put", key: `gone/${index}`, value: index }, { kind: "delete", key: `gone/${index}` }] });
      }
      const state = JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8"));
      expect(state.tombstoneOrder).toHaveLength(1_000);
      expect(Object.keys(state.versions)).toHaveLength(1_000);
      expect(state.versions["gone/0"]).toBeUndefined();                  // evicted
      await expect(store.put({ key: "gone/0", value: 1, identity, ifVersion: first.version })).rejects.toThrow("compare-and-swap failed");
      const again = await store.put({ key: "gone/0", value: 2, identity, ifVersion: 0 });
      expect(again.version).toBeGreaterThan(first.version + 1);          // above every earlier revision
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 60_000);

  it("reports the cursor just past each event, so a reader can resume at any event", async () => {
    const store = createStore();
    const from = identity;
    for (const text of ["a", "b", "c"]) await store.publish({ topic: "t", from, text });
    const page = store.tail(0, 10);
    expect(page.events.map((event) => event.text)).toEqual(["a", "b", "c"]);
    expect(page.cursors).toHaveLength(3);
    expect(page.cursors![2]).toBe(page.nextOffset);
    expect(store.tail(page.cursors![0]!, 10).events.map((event) => event.text)).toEqual(["b", "c"]);
    expect(store.tail(page.cursors![1]!, 10).events.map((event) => event.text)).toEqual(["c"]);
  });

  it("keeps unreadable state as a write barrier while serving an empty table", async () => {
    const store = createStore();
    const statePath = path.join(store.root, "state.json");
    fs.mkdirSync(store.root, { recursive: true });
    fs.writeFileSync(statePath, "{");

    expect(store.listAll()).toEqual([]);
    expect(store.get("shared/value")).toBeUndefined();
    expect(fs.readFileSync(statePath, "utf8")).toBe("{");
    await expect(store.put({ key: "shared/value", value: { revision: 1 }, identity })).rejects.toThrow("invalid state format");
    expect(fs.readFileSync(statePath, "utf8")).toBe("{");
  });

  it("supports complete internal prefix scans independently of public read limits", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-scan-"));
    roots.push(root);
    const store = new MeshStore(root, 64 * 1024, 1);
    await store.put({ key: "topology/a", value: 1, identity });
    await store.put({ key: "topology/b", value: 2, identity });

    expect(store.list("topology/", 100)).toHaveLength(1);
    expect(store.listAll("topology/").map((entry) => entry.key)).toEqual([
      "topology/a",
      "topology/b",
    ]);
  });

  it("supports compare-and-swap shared state", async () => {
    const store = createStore();
    const created = await store.put({
      key: "tasks/task-1",
      value: { status: "ready" },
      identity,
      ifVersion: 0,
    });
    expect(created.version).toBe(1);

    await expect(
      store.put({
        key: "tasks/task-1",
        value: { status: "claimed" },
        identity,
        ifVersion: 0,
      }),
    ).rejects.toThrow("compare-and-swap failed");

    const claimed = await store.put({
      key: "tasks/task-1",
      value: { status: "claimed", owner: "worker" },
      identity,
      ifVersion: created.version,
    });
    expect(claimed.version).toBe(2);
    expect(store.get("tasks/task-1")?.value).toEqual({ status: "claimed", owner: "worker" });
    expect(store.list("tasks/")).toHaveLength(1);

    expect(await store.delete({ key: "tasks/task-1", ifVersion: claimed.version })).toEqual({
      deleted: true, version: 3,
    });
    const recreated = await store.put({
      key: "tasks/task-1",
      value: { status: "ready-again" },
      identity,
    });
    expect(recreated.version).toBe(4);
    await expect(
      store.put({
        key: "tasks/task-1",
        value: { status: "stale-owner" },
        identity,
        ifVersion: created.version,
      }),
    ).rejects.toThrow("compare-and-swap failed");
    expect(() => store.get("tasks/__proto__")).toThrow("Invalid Fabric mesh key");
  });

  // smarty-dev#557: every read({ after }) parsed the whole log from its first byte; on the fleet
  // that is 43 MB for each drain of a lifecycle subscription.
  describe("reads after a sequence", () => {
    const bytesRead = <T>(run: () => T): { value: T; bytes: number } => {
      const reads = vi.spyOn(fs, "readSync");
      try {
        const value = run();
        return { value, bytes: reads.mock.results.reduce((sum, result) => sum + (result.value as number), 0) };
      } finally {
        reads.mockRestore();
      }
    };
    const sequences = (events: Array<{ sequence: number }>) => events.map((event) => event.sequence);

    it("start where the last read ended and return what a full scan returns", async () => {
      const store = createStore();
      for (let index = 1; index <= 200; index++) {
        await store.publish({ topic: index % 2 ? "odd" : "even", from: identity, text: "x".repeat(500) });
      }
      const size = fs.statSync(path.join(store.root, "events.jsonl")).size;
      expect(sequences(store.read({ after: 0 }))).toEqual(Array.from({ length: 100 }, (_, i) => i + 1));
      const second = bytesRead(() => store.read({ after: 100 }));
      expect(sequences(second.value)).toEqual(Array.from({ length: 100 }, (_, i) => i + 101));
      expect(second.bytes).toBeLessThan(size * 0.6);                      // from the middle, not the start
      await store.publish({ topic: "odd", from: identity, text: "new" });
      const next = bytesRead(() => store.read({ after: 200 }));
      expect(sequences(next.value)).toEqual([201]);
      expect(next.bytes).toBeLessThan(2_000);                             // only the new line
      const reference = new MeshStore(store.root, 64 * 1024, 100);         // no hint: a full scan
      for (const input of [{ after: 200 }, { after: 50, limit: 3 }, { after: 150, topic: "even", limit: 5 }, { after: 199 }]) {
        expect(sequences(store.read(input))).toEqual(sequences(reference.read(input)));
      }
    });

    // A host's lifecycle subscriptions all sit at one cursor when a new event arrives; with one
    // remembered point, the first read moved it and every other reader scanned the whole log.
    it("serve several readers at one cursor, and a reader a few events behind", async () => {
      const store = createStore();
      for (let index = 1; index <= 200; index++) await store.publish({ topic: "t", from: identity, text: "x".repeat(500) });
      expect(sequences(store.read({ after: 100 })).at(-1)).toBe(200);
      await store.publish({ topic: "t", from: identity, text: "new" });
      for (let reader = 0; reader < 3; reader++) {
        const next = bytesRead(() => store.read({ after: 200 }));
        expect(sequences(next.value)).toEqual([201]);
        expect(next.bytes).toBeLessThan(2_000);
      }
      const behind = bytesRead(() => store.read({ after: 190 }));
      expect(sequences(behind.value)).toEqual(Array.from({ length: 11 }, (_, i) => i + 191));
      expect(behind.bytes).toBeLessThan(15_000);
    });

    it("scan the whole log again after a rotation, even when an old offset falls on a line boundary", async () => {
      const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-rotated-"));
      roots.push(meshRoot);
      const store = new MeshStore(meshRoot, 512, 100, { maxEventLogBytes: 3_000, retainedEventLogBytes: 1_200 });
      // Lines of one length, so the old offset is also a line boundary in the rotated log.
      const publish = (sequence: number) =>
        store.publish({ topic: "t", from: identity, text: "x".repeat(120 - String(sequence).length) });
      for (let sequence = 1; sequence <= 3; sequence++) await publish(sequence);
      expect(sequences(store.read({ after: 0 }))).toEqual([1, 2, 3]);
      for (let sequence = 4; sequence <= 40; sequence++) await publish(sequence);
      expect(fs.readFileSync(path.join(meshRoot, "generation"), "utf8").trim()).not.toBe("0");
      const expected = sequences(new MeshStore(meshRoot, 512, 100).read({ after: 3 }));
      expect(expected.at(-1)).toBe(40);
      expect(sequences(store.read({ after: 3 }))).toEqual(expected);
    });

    it("scan the whole log again when the remembered offset no longer ends a line", async () => {
      const store = createStore();
      await store.publish({ topic: "t", from: identity, text: "a" });
      for (let index = 2; index <= 5; index++) await store.publish({ topic: "t", from: identity, text: "x".repeat(400) });
      expect(sequences(store.read({ after: 0 }))).toEqual([1, 2, 3, 4, 5]);
      await store.publish({ topic: "t", from: identity, text: "x".repeat(400) });
      await store.publish({ topic: "t", from: identity, text: "x".repeat(400) });
      // Rewritten in place without its short first line: the same file, every line moved.
      const log = path.join(store.root, "events.jsonl");
      const content = fs.readFileSync(log);
      fs.writeFileSync(log, content.subarray(content.indexOf(0x0a) + 1));
      expect(sequences(store.read({ after: 5 }))).toEqual([6, 7]);
    });
  });

  it("reports the oldest sequence still in the log", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-oldest-"));
    roots.push(meshRoot);
    const store = new MeshStore(meshRoot, 512, 100, { maxEventLogBytes: 2_000, retainedEventLogBytes: 800 });
    expect(store.oldestSequence()).toBeUndefined();                       // no log yet
    await store.publish({ topic: "t", from: identity, text: "first" });
    expect(store.oldestSequence()).toBe(1);
    for (let index = 0; index < 30; index++) await store.publish({ topic: "t", from: identity, text: `event-${index}` });
    expect(store.oldestSequence()).toBe(store.read({ after: 0, limit: 100 })[0]!.sequence);
    expect(store.oldestSequence()).toBeGreaterThan(1);                     // rotated
  });

  it("prepares multi-owner batches from one locked snapshot, including tombstone recreation", async () => {
    const store = createStore();
    const first = await store.put({ key: "batch/live", value: { kept: true }, identity });
    const gone = await store.put({ key: "batch/gone", value: 1, identity });
    const tombstone = await store.delete({ key: gone.key, ifVersion: gone.version });
    const other: MeshIdentity = { id: "session:other", name: "other", kind: "main" };
    const after = vi.fn();
    const lock = path.join(store.root, ".lock");
    const results = await store.writeBatch({ identity, ops: [], prepare: (view) => {
      expect(fs.existsSync(lock)).toBe(true);
      expect(view.version(gone.key)).toBe(tombstone.version);
      expect(view.get(gone.key)).toBeUndefined();
      expect(view.listAll("batch/")).toEqual([first]);
      // Copies cannot change this transaction's protected snapshot.
      (view.get(first.key)!.value as { kept: boolean }).kept = false;
      view.listAll("batch/")[0]!.updatedBy.id = "tampered";
      return [
        { kind: "put", key: gone.key, value: 2, identity: other, ifVersion: view.version(gone.key) },
        { kind: "put", key: "batch/new", value: 3, ifVersion: view.version("batch/new") },
      ];
    }, afterCommit: (view) => {
      expect(fs.existsSync(lock)).toBe(true);
      expect(view.get(gone.key)!.updatedBy).toEqual(other);
      expect(view.get("batch/new")!.updatedBy).toEqual(identity);
      // The canonical file is already committed when the ownership-bound effect runs.
      expect(new MeshStore(store.root, 64 * 1024, 100).get(gone.key)!.value).toBe(2);
      after();
    } });
    expect(results.map(r => r.applied)).toEqual([true, true]);
    expect(store.get(first.key)).toEqual(first);
    expect(after).toHaveBeenCalledTimes(1);
    expect(fs.existsSync(lock)).toBe(false);
  });

  it("runs no-op effects under one lock/read without a state commit", async () => {
    const store = createStore();
    await store.put({ key: "batch/live", value: 1, identity });
    const canonical = path.join(store.root, "state.json");
    const reads = vi.spyOn(fs, "readFileSync");
    const writes = vi.spyOn(fs, "renameSync");
    const effect = vi.fn(view => {
      expect(fs.existsSync(path.join(store.root, ".lock"))).toBe(true);
      expect(view.get("batch/live")!.value).toBe(1);
    });
    expect(await store.writeBatch({ identity, ops: [], prepare: () => [], afterCommit: effect })).toEqual([]);
    expect(reads.mock.calls.filter(([p]) => String(p) === canonical)).toHaveLength(1);
    expect(writes.mock.calls.filter(([, p]) => String(p) === canonical)).toHaveLength(0);
    expect(effect).toHaveBeenCalledTimes(1);
  });

  it("validates prepared keys and keeps failed batches from committing or running effects", async () => {
    const store = createStore();
    const entry = await store.put({ key: "batch/live", value: 1, identity });
    const effect = vi.fn();
    await expect(store.writeBatch({ identity, ops: [], prepare: () => [
      { kind: "put", key: "batch/new", value: 2 },
      { kind: "delete", key: entry.key, ifVersion: entry.version + 1 },
    ], afterCommit: effect })).rejects.toBeInstanceOf(MeshBatchConflictError);
    expect(store.get("batch/new")).toBeUndefined();
    await expect(store.writeBatch({ identity, ops: [], prepare: () => [
      { kind: "put", key: "", value: 2 },
    ], afterCommit: effect })).rejects.toThrow("Invalid Fabric mesh key");
    expect(store.get(entry.key)).toEqual(entry);
    expect(effect).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(store.root, ".lock"))).toBe(false);
  });

  it("skips a batch delete whose condition fails at commit, seeing earlier ops of the batch", async () => {
    const store = createStore();
    await store.put({ key: "owner/live", value: { alive: true }, identity });
    await store.put({ key: "owner/gone", value: { alive: false }, identity });
    await store.put({ key: "child/of-live", value: 1, identity });
    await store.put({ key: "child/of-gone", value: 1, identity });
    const ownerGone = (key: string) => (current: (key: string) => { value: unknown } | undefined) =>
      !(current(key)?.value as { alive?: boolean } | undefined)?.alive;
    const results = await store.writeBatch({ identity, ops: [
      { kind: "delete", key: "owner/gone", condition: ownerGone("owner/gone") },
      { kind: "delete", key: "child/of-gone", condition: ownerGone("owner/gone") },   // owner deleted above: absent
      { kind: "delete", key: "child/of-live", condition: ownerGone("owner/live") },
    ] });
    expect(results.map((result) => result.applied)).toEqual([true, true, false]);
    expect(store.get("child/of-live")).toBeDefined();
    expect(store.get("owner/gone")).toBeUndefined();
  });

  it("compacts oversized event logs and resets stale tail cursors", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-bounded-"));
    roots.push(meshRoot);
    const maxEventLogBytes = 2_000;
    const store = new MeshStore(meshRoot, 512, 100, {
      maxEventLogBytes,
      retainedEventLogBytes: 800,
    });
    await store.publish({ topic: "team.auth", from: identity, text: "event-0" });
    await store.publish({ topic: "team.auth", from: identity, text: "event-1" });
    const staleCursor = store.latestOffset();
    for (let index = 2; index < 30; index += 1) {
      await store.publish({ topic: "team.auth", from: identity, text: `event-${index}` });
    }

    const tail = store.tail(staleCursor, 100);
    const recent = store.read({ limit: 3 });

    expect(fs.statSync(path.join(meshRoot, "events.jsonl")).size).toBeLessThanOrEqual(
      maxEventLogBytes,
    );
    expect(tail.events.length).toBeGreaterThan(0);
    expect(tail.events.at(-1)?.text).toBe("event-29");
    expect(recent.map((event) => event.text)).toEqual(["event-27", "event-28", "event-29"]);
    expect(tail.nextOffset).toBe(store.latestOffset());
  });

  it("caps deleted-key version tombstones", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-state-"));
    roots.push(meshRoot);
    const store = new MeshStore(meshRoot, 64 * 1024, 100, { maxStateTombstones: 2 });
    for (const key of ["state/a", "state/b", "state/c"]) {
      await store.put({ key, value: { ready: true }, identity });
      await store.delete({ key });
    }

    const state = JSON.parse(fs.readFileSync(path.join(meshRoot, "state.json"), "utf8")) as {
      versions: Record<string, number>;
      tombstoneOrder: string[];
    };
    const recreated = await store.put({ key: "state/a", value: { ready: false }, identity });

    expect(state.tombstoneOrder).toEqual(["state/b", "state/c"]);
    expect(state.versions["state/a"]).toBeUndefined();
    expect(recreated.version).toBe(7);
  });
});

// smarty-dev#1043 (review/astra F1 on #84): the dashboard polls this stamp twice a second, so it
// must see another process's write from the file's metadata alone, without reading the state.
describe("MeshStore.stateStamp", () => {
  it("changes after another store writes the state, without reading or parsing it", async () => {
    const reader = createStore({ readCacheMs: 60_000 });
    const writer = new MeshStore(reader.root, 64 * 1024, 100);
    expect(reader.stateStamp()).toBeUndefined();                        // no state file yet
    await writer.put({ key: "a", value: 1, identity });
    expect(reader.get("a")?.value).toBe(1);                              // the reader's cache is warm
    const before = reader.stateStamp();
    expect(before).toBeDefined();
    await writer.put({ key: "a", value: "a much longer value", identity });
    const read = vi.spyOn(fs, "readFileSync");
    const parse = vi.spyOn(JSON, "parse");
    try {
      const after = reader.stateStamp();
      expect(after).toBeDefined();
      expect(after).not.toBe(before);
      expect(read).not.toHaveBeenCalled();
      expect(parse).not.toHaveBeenCalled();
    } finally {
      read.mockRestore();
      parse.mockRestore();
    }
    expect(reader.stateStamp()).toBe(reader.stateStamp());              // stable while unchanged
  });
});

describe("MeshStore.writeBatch", () => {
  it("applies puts and deletes in one write, with per-operation compare-and-swap", async () => {
    const store = createStore();
    const a = await store.put({ key: "k/a", value: 1, identity });
    const b = await store.put({ key: "k/b", value: 2, identity });
    const renames = vi.spyOn(fs, "renameSync");
    const results = await store.writeBatch({ identity, ops: [
      { kind: "put", key: "k/a", value: 10, ifVersion: a.version },
      { kind: "put", key: "k/b", value: 20, ifVersion: b.version + 5, onConflict: "skip" },
      { kind: "delete", key: "k/missing" },
      { kind: "put", key: "k/lease", value: (now: number) => ({ stampedAt: now }) },
    ] });
    const stateWrites = renames.mock.calls.filter(([, target]) => String(target).endsWith("state.json")).length;
    renames.mockRestore();
    expect(stateWrites).toBe(1);
    expect(results.map((r) => r.applied)).toEqual([true, false, false, true]);
    expect(store.get("k/a")?.value).toBe(10);
    expect(store.get("k/b")?.value).toBe(2);                              // skipped
    expect(typeof (store.get("k/lease")?.value as { stampedAt: number }).stampedAt).toBe("number");
  });

  it("deletes in a batch with a tombstone successor, and recreates only on the current version", async () => {
    const store = createStore();
    const put = await store.put({ identity, key: "batch/k", value: 1 });
    // As delete(): a batch delete consumes the key's successor revision (0.94 clock).
    const [deleted] = await store.writeBatch({ identity, ops: [{ kind: "delete", key: "batch/k", ifVersion: put.version }] });
    expect(deleted).toEqual({ key: "batch/k", applied: true, version: put.version + 1 });
    expect(store.get("batch/k")).toBeUndefined();
    // A stale compare-and-swap against the deleted key conflicts; the tombstone version wins.
    await expect(store.writeBatch({ identity, ops: [{ kind: "put", key: "batch/k", value: 2, ifVersion: put.version }] }))
      .rejects.toThrow(/expected version/);
    const [recreated] = await store.writeBatch({ identity, ops: [{ kind: "put", key: "batch/k", value: 3, ifVersion: deleted!.version }] });
    expect(recreated).toEqual({ key: "batch/k", applied: true, version: deleted!.version + 1 });
    expect(store.get("batch/k")?.value).toBe(3);
  });

  it("keeps state that another writer wrote between two batches", async () => {
    const a = createStore();
    const dir = a.root;
    const b = new MeshStore(dir, 64 * 1024, 100);
    await a.writeBatch({ identity, ops: [{ kind: "put", key: "batch/a", value: "a1" }] });
    await b.put({ identity, key: "batch/b", value: "b1" });
    await a.writeBatch({ identity, ops: [{ kind: "put", key: "batch/a", value: "a2" }, { kind: "delete", key: "batch/none" }] });
    const fresh = new MeshStore(dir, 64 * 1024, 100);
    expect(fresh.get("batch/a")?.value).toBe("a2");
    expect(fresh.get("batch/b")?.value).toBe("b1");
  });

  it("writes nothing when an operation aborts on conflict, or when nothing applies", async () => {
    const store = createStore();
    const a = await store.put({ key: "k/a", value: 1, identity });
    await expect(store.writeBatch({ identity, ops: [
      { kind: "put", key: "k/new", value: 1 },
      { kind: "put", key: "k/a", value: 2, ifVersion: a.version + 1, onConflict: (current) => (current ? "abort" : "skip") },
    ] })).rejects.toBeInstanceOf(MeshBatchConflictError);
    expect(store.get("k/new")).toBeUndefined();                          // all or nothing
    const statePath = path.join(store.root, "state.json");
    const before = fs.statSync(statePath);
    await store.writeBatch({ identity, ops: [{ kind: "delete", key: "k/missing" }] });
    const after = fs.statSync(statePath);
    expect([after.ino, after.mtimeMs]).toEqual([before.ino, before.mtimeMs]);
  });
});

describe("MeshStore lock recovery", () => {

  const holdLock = (store: MeshStore, owner?: string): string => {
    const lockPath = path.join(store.root, ".lock");
    fs.mkdirSync(lockPath, { mode: 0o700 });
    if (owner !== undefined) fs.writeFileSync(path.join(lockPath, "owner"), owner);
    return lockPath;
  };

  it.each(["write", "opened"] as const)("a paused native recoverer cannot detach a default-v1 %s initializer that publishes and enters first", async (phase) => {
    const store = createStore();
    const lock = path.join(store.root, ".lock");
    const ownerPath = path.join(lock, "owner");
    const children: Array<{ closed: Promise<number | null>; output: () => { stdout: string; stderr: string } }> = [];
    const start = (role: "initializer" | "recoverer") => {
      const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-paused-recovery.mjs"), store.root, role, phase], {
        cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"],
      });
      let stdout = "", stderr = "";
      child.stdout.on("data", chunk => { stdout += chunk; });
      child.stderr.on("data", chunk => { stderr += chunk; });
      const closed = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
      closed.catch(() => undefined);
      const captured = { closed, output: () => ({ stdout, stderr }) };
      children.push(captured);
      return captured;
    };
    const signal = (name: string) => fs.writeFileSync(path.join(store.root, name), "");
    const ready = async (name: string) => {
      await vi.waitFor(() => expect(fs.existsSync(path.join(store.root, name))).toBe(true), { timeout: 10_000, interval: 20 });
    };
    const initializer = start("initializer");
    try {
      await ready("initializer.ready");
      expect(fs.existsSync(ownerPath)).toBe(phase === "opened");
      if (phase === "opened") expect(fs.readFileSync(ownerPath, "utf8")).toBe("");
      const past = new Date(Date.now() - 60_000);
      fs.utimesSync(lock, past, past);
      const recoverer = start("recoverer");
      await ready("recoverer.ready");
      signal("initializer.go");
      await ready("initializer.entered"); // actual acquisition validation succeeded; operation remains live
      const held = JSON.parse(fs.readFileSync(path.join(store.root, "initializer.entered"), "utf8")) as { owner: string; ino: number; dev: number };
      expect(held.owner.trim().split("\n")).toHaveLength(3); // default wire did not change
      signal("recoverer.go");
      expect(await recoverer.closed, recoverer.output().stderr).toBe(0);
      // Still inside the initializer's synchronous critical section. Baseline detaches
      // this inode and enters a second operation; neither action is permitted.
      expect(fs.existsSync(lock), recoverer.output().stdout).toBe(true);
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(held.owner);
      expect([fs.lstatSync(lock).dev, fs.lstatSync(lock).ino]).toEqual([held.dev, held.ino]);
      expect(JSON.parse(recoverer.output().stdout.trim())).toMatchObject({ ran: false, timeout: true, boundary: phase === "write" ? "last-comparison-before-detach" : "refused-before-detach" });
      expect(fs.readdirSync(store.root).filter(name => name.startsWith(".lock.dead."))).toEqual([]);
    } finally {
      signal("initializer.go");
      signal("recoverer.go");
      signal("initializer.release");
      // Join EVERY captured native child's actual close before afterEach removes the root,
      // including the deliberately failing baseline ordering.
      const exits = await Promise.all(children.map(async child => ({ code: await child.closed, ...child.output() })));
      for (const exit of exits) expect(exit.code, exit.stderr).toBe(0);
    }
    expect(JSON.parse(initializer.output().stdout.trim())).toMatchObject({ ran: true });
    expect(fs.existsSync(lock)).toBe(false);
  }, 30_000);

  it("a rejected default-v1 initializer cleans its own receipt in a recovery-first empty successor while still alive", async () => {
    const store = createStore({ lockTimeoutMs: 150 });
    const lock = path.join(store.root, ".lock");
    const ownerPath = path.join(lock, "owner");
    const finished = path.join(store.root, "initializer.finished");
    const signal = (name: string) => fs.writeFileSync(path.join(store.root, name), "");
    const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-paused-recovery.mjs"), store.root, "initializer", "write", "recovery-first"], {
      cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "", stderr = "";
    child.stdout.on("data", chunk => { stdout += chunk; });
    child.stderr.on("data", chunk => { stderr += chunk; });
    const closed = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    closed.catch(() => undefined);
    try {
      await vi.waitFor(() => expect(fs.existsSync(path.join(store.root, "initializer.ready"))).toBe(true), { timeout: 10_000, interval: 20 });
      const original = fs.lstatSync(lock);
      expect(fs.existsSync(ownerPath)).toBe(false);
      const past = new Date(Date.now() - 60_000);
      fs.utimesSync(lock, past, past);
      const write = fs.writeFileSync.bind(fs);
      let armed = true;
      let replacement: fs.Stats | undefined;
      vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
        if (armed && String(file) === ownerPath) {
          // Real recovery has removed the empty original and acquired a new directory.
          // Stop the successor's actual owner create and let the original publish first.
          armed = false;
          replacement = fs.lstatSync(lock);
          signal("initializer.go");
          const deadline = Date.now() + 10_000;
          while (!fs.existsSync(finished) && Date.now() < deadline) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
          if (!fs.existsSync(finished)) throw new Error("Timed out awaiting rejected initializer");
        }
        return write(file, data, options);
      });
      const operation = vi.fn();
      await expect(store.exclusive(operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_OWNERSHIP_LOST" });
      expect(operation).not.toHaveBeenCalled();
      expect(replacement).toBeDefined();
      expect([replacement!.dev, replacement!.ino]).not.toEqual([original.dev, original.ino]);
      expect(JSON.parse(fs.readFileSync(finished, "utf8"))).toMatchObject({ ran: false, code: "FABRIC_MESH_LOCK_OWNERSHIP_LOST" });
      expect(fs.existsSync(lock)).toBe(false); // no live rejected receipt can strand this root
      expect(child.exitCode).toBeNull();
      expect(process.kill(child.pid!, 0)).toBe(true);
      const healthy = vi.fn(() => "progress");
      await expect(store.exclusive(healthy)).resolves.toBe("progress");
      expect(healthy).toHaveBeenCalledOnce();
      expect(fs.existsSync(lock)).toBe(false);
      expect(fs.readdirSync(store.root).filter(name => name.startsWith(".lock.released."))).toEqual([]);
    } finally {
      signal("initializer.go");
      signal("initializer.release");
      expect(await closed, stderr).toBe(0); // joined close while root remains owned by this test
    }
    expect(JSON.parse(stdout.trim())).toMatchObject({ ran: false, code: "FABRIC_MESH_LOCK_OWNERSHIP_LOST" });
  }, 30_000);

  it("pre-entry cleanup preserves a changed full receipt even when its token still matches", async () => {
    const store = createStore();
    const lock = path.join(store.root, ".lock");
    const ownerPath = path.join(lock, "owner");
    const write = fs.writeFileSync.bind(fs);
    let receipt: string | undefined;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
      if (String(file) === ownerPath) {
        const fields = String(data).split("\n");
        fields[2] = String(Number(fields[2]) + 1); // same token and live PID, different full receipt
        receipt = fields.join("\n");
        return write(file, receipt, options);
      }
      return write(file, data, options);
    });
    const operation = vi.fn();
    await expect(store.exclusive(operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_OWNERSHIP_LOST" });
    expect(operation).not.toHaveBeenCalled();
    expect(receipt).toBeDefined();
    expect(fs.readFileSync(ownerPath, "utf8")).toBe(receipt);
    expect(fs.readdirSync(store.root).filter(name => name.startsWith(".lock.released."))).toEqual([]);
  });

  it.each([["write", 1], ["opened", 1], ["write", 2], ["rename", 2]] as const)("a paused native %s publication (protocol %s) preserves exclusion against live owners or successors", async (phase, lockProtocol) => {
    const store = createStore({ lockTimeoutMs: 1_000, ...(lockProtocol === 2 ? { lockProtocol } : {}) });
    const lock = path.join(store.root, ".lock");
    const ownerPath = path.join(lock, "owner");
    const ready = path.join(store.root, "paused.ready");
    const go = path.join(store.root, "paused.go");
    const resumed = path.join(store.root, "paused.resumed");
    const entered = path.join(store.root, "paused.entered");
    const release = path.join(store.root, "paused.release");
    const windowsOpened = process.platform === "win32" && phase === "opened";
    const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-paused-publication.mjs"), store.root, phase, String(lockProtocol)], {
      cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "", stderr = "";
    child.stdout.on("data", chunk => { stdout += chunk; });
    child.stderr.on("data", chunk => { stderr += chunk; });
    const closed = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    closed.catch(() => undefined);
    try {
      await vi.waitFor(() => expect(fs.existsSync(ready)).toBe(true), { timeout: 10_000, interval: 20 });
      if (phase !== "rename") {
        if (lockProtocol === 2) fs.mkdirSync(lock);
        fs.writeFileSync(path.join(lock, "legacy-leftover"), "orphan\n");
        const past = new Date(Date.now() - 30_001);
        fs.utimesSync(lock, past, past);
        // This unrecorded directory is nonempty (or has an opened/torn owner).
        // Atomic empty-directory recovery must refuse it in BOTH protocols.
        await expect(store.exclusive(() => { throw new Error("must not recover an unrecorded initializer"); }))
          .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
        expect(fs.existsSync(lock)).toBe(true);
        if (windowsOpened) {
          // Windows cannot replace a directory while this native child's owner fd
          // is open. Only this exact rename's EPERM/EBUSY is the expected outcome.
          const directory = fs.lstatSync(lock);
          expect(fs.readFileSync(ownerPath, "utf8")).toBe("");
          let renameError: unknown;
          try { fs.renameSync(lock, `${lock}.test-detached`); }
          catch (error) { renameError = error; }
          expect(renameError).toBeDefined();
          expect(["EPERM", "EBUSY"]).toContain((renameError as NodeJS.ErrnoException).code);
          expect(fs.existsSync(`${lock}.test-detached`)).toBe(false);
          expect(fs.readFileSync(ownerPath, "utf8")).toBe("");
          expect([fs.lstatSync(lock).dev, fs.lstatSync(lock).ino]).toEqual([directory.dev, directory.ino]);
        } else {
          // Preserve round 4's opposite-order namespace-replacement defense as an
          // adversarial fixture action, NOT a production ownerless-recovery policy.
          fs.renameSync(lock, `${lock}.test-detached`);
        }
      }
      if (windowsOpened) {
        fs.writeFileSync(go, "");
        await vi.waitFor(() => expect(fs.existsSync(entered)).toBe(true), { timeout: 10_000, interval: 20 });
        const held = JSON.parse(fs.readFileSync(entered, "utf8")) as { owner: string; ino: number; dev: number };
        expect(held.owner.trim().split("\n")).toHaveLength(3);
        expect(held.owner.split("\n")[1]).toBe(String(child.pid));
        const operation = vi.fn();
        await expect(store.exclusive(operation)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
        expect(operation).not.toHaveBeenCalled();
        expect(fs.readFileSync(ownerPath, "utf8")).toBe(held.owner);
        expect([fs.lstatSync(lock).dev, fs.lstatSync(lock).ino]).toEqual([held.dev, held.ino]);
        expect(fs.existsSync(`${lock}.test-detached`)).toBe(false);
      } else {
        await store.exclusive(() => {
          const owner = fs.readFileSync(ownerPath, "utf8");
          const inode = fs.statSync(lock).ino;
          fs.writeFileSync(go, "");
          const deadline = Date.now() + 5_000;
          // Only this fixture's child can advance the paused syscall. Keep the actual
          // successor's synchronous critical section live until that resume settles.
          while (!fs.existsSync(resumed) && Date.now() < deadline) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
          expect(fs.existsSync(resumed)).toBe(true);
          expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
          expect(fs.statSync(lock).ino).toBe(inode);
        });
      }
    } finally {
      fs.writeFileSync(go, "");
      fs.writeFileSync(release, "");
      expect(await closed, stderr).toBe(0); // real close, before root teardown
    }
    expect(JSON.parse(stdout.trim())).toMatchObject(windowsOpened
      ? { ran: true, refused: false, timeout: false, ownershipLost: false }
      : lockProtocol === 1
        ? { ran: false, refused: phase === "write", timeout: false, ownershipLost: true }
        : phase === "write"
          ? { ran: true, refused: false, timeout: false, ownershipLost: false }
          : { ran: false, refused: true, timeout: true, ownershipLost: false });
    if (windowsOpened) expect(fs.existsSync(lock)).toBe(false);
    const fences = fs.readdirSync(store.root).filter(name => name.startsWith(".lock.dead."));
    expect(fences).toHaveLength(0);
    expect(fs.readdirSync(store.root).some(name => name.startsWith(".lock.pending."))).toBe(false);
  });

  it.each([1, 2] as const)("interrupted release (protocol %s) never exposes an ownerless canonical or cleans a live successor on resume", async (lockProtocol) => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 100, lockProtocol });
    const other = new MeshStore(store.root, 64 * 1024, 100, { lockTimeoutMs: 100, lockProtocol });
    const lock = path.join(store.root, ".lock");
    const ownerPath = path.join(lock, "owner");
    const remove = fs.rmSync.bind(fs);
    let cleanupPath: string | undefined;
    let ownerlessCanonical = false;
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      const name = String(file);
      if (!cleanupPath && (name === lock || name.startsWith(`${lock}.released.`))) {
        cleanupPath = name;
        fs.unlinkSync(path.join(name, "owner")); // recursive removal interrupted after unlink
        ownerlessCanonical = fs.existsSync(lock) && !fs.existsSync(ownerPath);
        if (fs.existsSync(lock)) {
          const past = new Date(Date.now() - 30_001);
          fs.utimesSync(lock, past, past);
        }
        throw new Error("simulated interrupted cleanup");
      }
      return remove(file, options);
    });
    await store.exclusive(() => "released");
    expect(cleanupPath).toBeDefined();
    let successorUnchanged = false;
    await other.exclusive(() => {
      const successor = fs.readFileSync(ownerPath, "utf8");
      const inode = fs.statSync(lock).ino;
      remove(cleanupPath!, { recursive: true, force: true }); // resume exact old cleanup
      successorUnchanged = fs.existsSync(ownerPath) &&
        fs.readFileSync(ownerPath, "utf8") === successor && fs.statSync(lock).ino === inode;
    });
    expect(ownerlessCanonical).toBe(false);
    expect(successorUnchanged).toBe(true);
    expect(fs.existsSync(cleanupPath!)).toBe(false);
  });

  it("bounded lock backoff keeps the uncontended first attempt immediate", async () => {
    vi.useFakeTimers();
    const store = createStore();
    const timers = vi.spyOn(globalThis, "setTimeout");
    const operation = vi.fn(() => "done");
    const result = store.exclusive(operation);
    expect(operation).toHaveBeenCalledOnce();
    expect(timers).not.toHaveBeenCalled();
    await expect(result).resolves.toBe("done");
  });

  it("bounded lock backoff grows exponentially with jitter and caps contention waits", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 2_000 });
    const lockPath = holdLock(store, `other\n${process.pid}\n${Date.now()}\n`);
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const timers = vi.spyOn(globalThis, "setTimeout");
    const operation = vi.fn(() => "done");
    const result = store.exclusive(operation);
    expect(operation).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1_000);
    const waits = timers.mock.calls.map(([, wait]) => Number(wait));
    expect(waits.slice(0, 6)).toEqual([15, 25, 45, 85, 130, 130]);
    expect(waits.length).toBeLessThan(15); // fixed 10 ms retries took 100 probes here
    expect(waits.every((wait) => wait >= 10 && wait <= 250)).toBe(true);
    expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toContain(`${process.pid}\n`);
    fs.rmSync(lockPath, { recursive: true });
    await vi.advanceTimersByTimeAsync(250);
    await expect(result).resolves.toBe("done");
    expect(operation).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounded lock backoff applies the jitter floor and clamps the deadline, preserving diagnostics", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 100 });
    const lockPath = holdLock(store, `other\n${process.pid}\n${Date.now() - 60_000}\n`);
    vi.spyOn(Math, "random").mockReturnValue(0.999);
    const timers = vi.spyOn(globalThis, "setTimeout");
    const operation = vi.fn();
    const result = store.exclusive(operation).catch((error: unknown) => error as Error & { code: string });
    await vi.advanceTimersByTimeAsync(100);
    const error = await result;
    expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect((error as Error).message).toMatch(/after 4 attempts, largest gap between attempts 42 ms$/);
    expect(timers.mock.calls.map(([, wait]) => Number(wait))).toEqual([19, 39, 42]);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(lockPath)).toBe(true); // a live holder is never swept, even beyond stale age
    expect(vi.getTimerCount()).toBe(0);

    // Minimum randomness still sleeps at least 10 ms rather than spinning on contention.
    vi.mocked(Math.random).mockReturnValue(0);
    timers.mockClear();
    const second = store.exclusive(operation).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    await second;
    expect(timers.mock.calls.map(([, wait]) => Number(wait))).toEqual(Array(10).fill(10));
  });

  it("reclaims a recent dead holder immediately without spending the stale window", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 100 });
    holdLock(store, `recent-dead\n999999999\n${Date.now()}\n`);
    const waits = vi.spyOn(globalThis, "setTimeout");
    const operation = vi.fn(() => "recovered");
    const result = store.exclusive(operation);
    await expect(result).resolves.toBe("recovered");
    expect(operation).toHaveBeenCalledOnce();
    expect(waits).not.toHaveBeenCalled();
    const fences = fs.readdirSync(store.root).filter((name) => name.startsWith(".lock.dead."));
    expect(fences).toHaveLength(1);
    expect(fs.readFileSync(path.join(store.root, fences[0]!, "owner"), "utf8")).toContain("recent-dead\n");
  });

  it("immediately recovers the complete default-v1 receipt of a joined native dead holder", async () => {
    const store = createStore({ lockTimeoutMs: 100 });
    const lock = path.join(store.root, ".lock");
    const child = spawn(process.execPath, ["-e", `
      const fs = require("node:fs"), path = require("node:path");
      fs.mkdirSync(process.argv[1]);
      fs.writeFileSync(path.join(process.argv[1], "owner"), "native-dead\\n" + process.pid + "\\n" + Date.now() + "\\n");
    `, lock], { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    child.stderr.on("data", chunk => { stderr += chunk; });
    const closed = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    expect(await closed, stderr).toBe(0); // actual native holder exit before recovery
    const receipt = fs.readFileSync(path.join(lock, "owner"), "utf8");
    const operation = vi.fn(() => "recovered");
    await expect(store.exclusive(operation)).resolves.toBe("recovered");
    expect(operation).toHaveBeenCalledOnce();
    const fences = fs.readdirSync(store.root).filter(name => name.startsWith(".lock.dead."));
    expect(fences).toHaveLength(1);
    expect(fs.readFileSync(path.join(store.root, fences[0]!, "owner"), "utf8")).toBe(receipt);
  });

  it("a paused stale cleaner cannot remove or rename over a successor held by another cleaner", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 100 });
    const other = new MeshStore(store.root, 64 * 1024, 100, { lockTimeoutMs: 100 });
    const lock = holdLock(store, `old-unique-token\n999999999\n${Date.now() - 60_000}\n`);
    const ownerPath = path.join(lock, "owner");
    const rename = fs.renameSync.bind(fs);
    const remove = fs.rmSync.bind(fs);
    let armed = true;
    let competitor: Promise<void> | undefined;
    let refused: unknown;
    const pause = (resume: () => void) => {
      armed = false;
      competitor = other.exclusive(() => {
        const successor = fs.readFileSync(ownerPath, "utf8");
        const inode = fs.statSync(lock).ino;
        try { resume(); } catch (error) { refused = error; }
        expect(fs.readFileSync(ownerPath, "utf8")).toBe(successor);
        expect(fs.statSync(lock).ino).toBe(inode);
        expect(successor).not.toContain("old-unique-token");
      });
      void competitor.catch(() => undefined);
      if (refused) throw refused;
    };
    // Intercept both the fixed atomic rename and HEAD's unsafe canonical recursive rm.
    // The second cleaner runs synchronously and resumes the paused action while holding
    // its successor, making this an actual filesystem fence test, not a guessed delay.
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (armed && String(from) === lock && String(to).startsWith(`${lock}.dead.`)) return pause(() => rename(from, to));
      return rename(from, to);
    });
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (armed && String(file) === lock) return pause(() => remove(file, options));
      return remove(file, options);
    });
    const operation = vi.fn();
    const result = store.exclusive(operation);
    await vi.advanceTimersByTimeAsync(100);
    expect(armed).toBe(false);
    expect(competitor).toBeDefined();
    await competitor;
    await result;
    expect(operation).toHaveBeenCalledOnce();
    expect(refused).toBeDefined();
    expect(fs.readdirSync(store.root).filter((name) => name.startsWith(".lock.dead."))).toHaveLength(1);
  });

  it.each(["darwin", "win32"] as const)("isolates simulated %s publication after a cached native read", async (platform) => {
    // First use the real host reader, as the earlier protocol-2 cases do in CI.
    vi.resetModules();
    const real = await import("../src/core/atomic-write.js");
    const nativeOwn = real.ownProcessIncarnation();
    await nativeOwn;
    expect(real.ownProcessIncarnation()).toBe(nativeOwn);

    // Prime the exact same platform/SystemRoot key even on Linux, so every host
    // reproduces the native Windows collision rather than relying on CI ordering.
    const previous = platform === "darwin" ? "Wed Sep 30 12:00:00 2026" : "639263664000000000";
    const start = platform === "darwin" ? "Thu Oct  1 12:00:00 2026" : "639264528000000000";
    const command = mockNativePlatform(platform, previous);
    vi.resetModules();
    const cached = await import("../src/core/atomic-write.js");
    const own = cached.ownProcessIncarnation();
    expect(await own).toBe(`${platform}:${previous}`);
    command.mockImplementation(((...args: unknown[]) => {
      const done = args[args.length - 1] as (error: Error | null, stdout: string, stderr: string) => void;
      done(null, start + "\n", "");
      return {} as childProcess.ChildProcess;
    }) as typeof childProcess.execFile);
    expect(cached.ownProcessIncarnation()).toBe(own);
    expect(await cached.ownProcessIncarnation()).toBe(`${platform}:${previous}`);

    const store = await createSimulatedNativeStore();
    for (let publication = 0; publication < 2; publication++) {
      await store.exclusive(() => {
        expect(fs.readFileSync(path.join(store.root, ".lock", "owner"), "utf8").split("\n")[3]).toBe(`${platform}:${start}`);
      });
    }
    expect(command).toHaveBeenCalledTimes(2); // one old read, one memoized isolated read
  });

  it.each(["darwin", "win32"] as const)("publishes native %s incarnation and recovers a reused PID", async (platform) => {
    vi.useFakeTimers({ now: Date.now() });
    const start = platform === "darwin" ? "Thu Oct  1 12:00:00 2026" : "639264528000000000";
    mockNativePlatform(platform, start);
    const store = await createSimulatedNativeStore();
    await store.exclusive(() => {
      expect(fs.readFileSync(path.join(store.root, ".lock", "owner"), "utf8").split("\n")[3]).toBe(`${platform}:${start}`);
    });
    holdLock(store, `reused\n${process.pid}\n${Date.now()}\n${platform === "darwin" ? "darwin:Wed Sep 30 12:00:00 2026" : "win32:639263664000000000"}\n`);
    const operation = vi.fn();
    const pending = store.exclusive(operation).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    await pending;
    expect(operation).toHaveBeenCalledOnce();
  });

  it.each(["darwin", "win32"] as const)("protects old live, unknown and torn %s incarnations and recovers dead holders", async (platform) => {
    vi.useFakeTimers({ now: Date.now() });
    const start = platform === "darwin" ? "Thu Oct  1 12:00:00 2026" : "639264528000000000";
    const native = mockNativePlatform(platform, start);
    const store = await createSimulatedNativeStore();
    const owner = `live\n${process.pid}\n${Date.now() - 60_000}\n${platform}:${start}\n`;
    const lock = holdLock(store, owner);
    for (const scenario of ["live", "unknown", "torn", "legacy"] as const) {
      if (scenario === "unknown") native.mockImplementation(() => { throw new Error("identity unreadable"); });
      if (scenario === "torn") fs.writeFileSync(path.join(lock, "owner"), owner.slice(0, -2));
      if (scenario === "legacy") fs.writeFileSync(path.join(lock, "owner"), `live\n${process.pid}\n${Date.now() - 60_000}\n`);
      const operation = vi.fn();
      const pending = store.exclusive(operation).catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(100);
      expect(await pending).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(operation).not.toHaveBeenCalled();
      expect(fs.existsSync(lock)).toBe(true);
    }
    fs.writeFileSync(path.join(lock, "owner"), `dead\n999999999\n${Date.now()}\n${platform}:${start}\n`);
    const operation = vi.fn();
    await store.exclusive(operation);
    expect(operation).toHaveBeenCalledOnce();
  });

  it.skipIf(process.platform !== "linux" || !fs.existsSync(`/proc/${process.pid}/stat`))("publishes Linux start time and distinguishes a reused PID from its live incarnation", async () => {
    const store = createStore({ lockProtocol: 2 });
    const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
    const startTime = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]!;
    await store.exclusive(() => {
      const owner = fs.readFileSync(path.join(store.root, ".lock", "owner"), "utf8").trim().split("\n");
      expect(owner).toHaveLength(4);
      expect(owner[3]).toBe(startTime);
    });
    holdLock(store, `reused-pid\n${process.pid}\n${Date.now()}\n${BigInt(startTime) + 1n}\n`);
    const operation = vi.fn();
    await store.exclusive(operation);
    expect(operation).toHaveBeenCalledOnce();
  });

  it.skipIf(process.platform !== "linux" || !fs.existsSync(`/proc/${process.pid}/stat`))("keeps a matching live incarnation and fails closed when start time is unavailable", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 100 });
    const procPath = `/proc/${process.pid}/stat`;
    const stat = fs.readFileSync(procPath, "utf8");
    const startTime = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]!;
    const lock = holdLock(store, `live\n${process.pid}\n${Date.now() - 60_000}\n${startTime}\n`);
    const first = store.exclusive(() => { throw new Error("must not acquire"); }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await first).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    fs.writeFileSync(path.join(lock, "owner"), `unknown-incarnation\n${process.pid}\n${Date.now() - 60_000}\n${BigInt(startTime) + 1n}\n`);
    const read = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === procPath) throw Object.assign(new Error("unreadable"), { code: "EACCES" });
      return (read as (...args: unknown[]) => unknown)(file, ...args);
    }) as typeof fs.readFileSync);
    const second = store.exclusive(() => { throw new Error("must not acquire"); }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await second).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.existsSync(lock)).toBe(true);
  });

  it.skipIf(process.platform !== "linux" || !fs.existsSync(`/proc/${process.pid}/stat`))("does not infer PID reuse from a torn fourth owner line", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 100 });
    const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
    const startTime = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]!;
    const partial = startTime.slice(0, -1) || "0";
    const owner = `writing\n${process.pid}\n${Date.now() - 60_000}\n${partial}`;
    const lock = holdLock(store, owner);
    const result = store.exclusive(() => { throw new Error("must not acquire"); }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
  });

  it("does not mistake a permission-denied live holder for a dead one", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 100 });
    const lock = holdLock(store, `protected\n${process.pid}\n${Date.now() - 60_000}\n`);
    vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    const result = store.exclusive(() => { throw new Error("must not acquire"); }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.existsSync(lock)).toBe(true);
  });

  it.each(["EACCES", "EIO", undefined])("fails closed when the PID death probe fails with %s instead of ESRCH", async (code) => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 100 });
    const owner = `unknown\n999999999\n${Date.now() - 60_000}\n`;
    const lock = holdLock(store, owner);
    vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("unknown probe failure"), { code }); });
    const operation = vi.fn();
    const pending = store.exclusive(operation).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await pending).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(operation).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
  });

  it("keeps fresh corrupt records and protects live PIDs even in stale malformed records", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockTimeoutMs: 100 });
    const lock = holdLock(store, "not-a-valid-owner");
    const first = store.exclusive(() => { throw new Error("must not acquire"); }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await first).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    fs.writeFileSync(path.join(lock, "owner"), `malformed\n${process.pid}\ninvalid-time\n`);
    const old = new Date(Date.now() - 60_000);
    fs.utimesSync(lock, old, old);
    const second = store.exclusive(() => { throw new Error("must not acquire"); }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await second).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(fs.existsSync(lock)).toBe(true);
  });

  it("sweeps a stale lock whose owner process is dead", async () => {
    const store = createStore();
    const lockPath = holdLock(store, `crashed\n999999999\n${Date.now() - 60_000}\n`);

    const event = await store.publish({ topic: "team.auth", from: identity, text: "recovered" });

    expect(event.sequence).toBe(1);
    expect(fs.existsSync(lockPath)).toBe(false);
  });

  it.each([1, 2] as const)("fails closed on old torn/corrupt receipts (protocol %s), even with a dead-looking partial PID", async (lockProtocol) => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockProtocol, lockTimeoutMs: 100, staleLockMs: 100 });
    const receipts = ["", "not-a-valid-owner", "writing\n999999999", "writing\n999999999\n",
      "writing\n999999999\n100", "writing\n999999999\ninvalid-time\n", "writing\n999999999\n100\nextra\nextra\n"];
    for (const owner of receipts) {
      const lock = holdLock(store, owner);
      const past = new Date(Date.now() - 60_000);
      fs.utimesSync(lock, past, past);
      const before = fs.lstatSync(lock);
      const operation = vi.fn();
      const pending = store.exclusive(operation).catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(100);
      expect(await pending).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(operation).not.toHaveBeenCalled();
      const after = fs.lstatSync(lock);
      expect([after.dev, after.ino, after.mtimeMs]).toEqual([before.dev, before.ino, before.mtimeMs]);
      expect(fs.readdirSync(lock)).toEqual(["owner"]);
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
      expect(fs.readdirSync(store.root).some(name => name.startsWith(".lock.dead."))).toBe(false);
      fs.rmSync(lock, { recursive: true });
    }
  });

  it.each([1, 2] as const)("recovers an empty ownerless lock strictly after its grace (protocol %s)", async (lockProtocol) => {
    vi.useFakeTimers({ now: 1_000_000 });
    const store = createStore({ lockProtocol, lockTimeoutMs: 100, staleLockMs: 100 });
    const lock = holdLock(store);
    const created = new Date(Date.now());
    fs.utimesSync(lock, created, created);
    const operation = vi.fn();
    const pending = store.exclusive(operation).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await pending).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(operation).not.toHaveBeenCalled();
    expect(fs.readdirSync(lock)).toEqual([]);
    expect(fs.readdirSync(store.root).some(name => name.startsWith(".lock.dead."))).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await store.exclusive(operation);
    expect(operation).toHaveBeenCalledTimes(1);
    expect(fs.existsSync(lock)).toBe(false);
    const fences = fs.readdirSync(store.root).filter(name => name.startsWith(".lock.dead."));
    expect(fences).toHaveLength(1);
    expect(fs.readFileSync(path.join(store.root, fences[0]!, ".recovery-fence"), "utf8")).toBe("1\n");
  });

  it.each([1, 2] as const)("reuses a retained ownerless recovery receipt after inode reuse (protocol %s)", async (lockProtocol) => {
    const store = createStore({ lockProtocol, lockTimeoutMs: 100 });
    const lock = holdLock(store);
    const past = new Date(Date.now() - 60_000);
    fs.utimesSync(lock, past, past);
    const stat = fs.lstatSync(lock);
    const fence = `${lock}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:`).digest("hex")}`;
    fs.mkdirSync(fence);
    fs.writeFileSync(path.join(fence, ".recovery-fence"), "1\n");
    const operation = vi.fn();
    await store.exclusive(operation);
    expect(operation).toHaveBeenCalledTimes(1);
    expect(fs.existsSync(lock)).toBe(false);
    expect(fs.readdirSync(store.root).filter(name => name.startsWith(".lock.dead."))).toHaveLength(1);
    expect(fs.readFileSync(path.join(fence, ".recovery-fence"), "utf8")).toBe("1\n");
  });

  it("never sweeps a lock owned by a live process and times out instead", async () => {
    const store = createStore({ lockTimeoutMs: 300 });
    const lockPath = holdLock(store, `other\n${process.pid}\n${Date.now() - 60_000}\n`);

    await expect(
      store.publish({ topic: "team.auth", from: identity, text: "blocked" }),
    ).rejects.toThrow(`Timed out waiting for the Fabric mesh lock held by pid ${process.pid} (alive`);
    expect(fs.existsSync(lockPath)).toBe(true);
    expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toContain(`${process.pid}\n`);
  });

  // The count and the largest gap tell a starved waiter (large gap) from one that kept
  // losing the race (many attempts, small gaps) (smarty-dev#816).
  it("reports the attempt count and the largest gap between attempts on a lock timeout", async () => {
    const store = createStore({ lockTimeoutMs: 400 });
    holdLock(store, `other\n${process.pid}\n${Date.now()}\n`);
    // Starve the waiter's event loop once for 200 ms while it retries.
    setTimeout(() => {
      const until = Date.now() + 200;
      while (Date.now() < until) { /* busy */ }
    }, 50);

    const error = await store
      .publish({ topic: "team.auth", from: identity, text: "blocked" })
      .then(() => undefined, (caught: unknown) => caught as Error);
    const match = /after (\d+) attempts, largest gap between attempts (\d+) ms$/.exec(error?.message ?? "");
    expect(match, error?.message).not.toBeNull();
    expect(Number(match![1])).toBeGreaterThan(2);
    expect(Number(match![2])).toBeGreaterThanOrEqual(190);
  });

  // A stopped holder keeps the lock (taking it over could let the holder commit stale
  // state on resume); the timeout must name it so it can be restarted (smarty-dev#266).
  it.skipIf(process.platform !== "linux")(
    "names a signal-stopped holder in the timeout and does not take its lock",
    async () => {
      const holder = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
      const closed = new Promise<void>((resolve) => holder.once("close", () => resolve()));
      try {
        await new Promise((resolve) => holder.once("spawn", resolve));
        process.kill(holder.pid!, "SIGSTOP");
        await new Promise((resolve) => setTimeout(resolve, 100));
        const store = createStore({ lockTimeoutMs: 300 });
        const lockPath = holdLock(store, `stopped\n${holder.pid}\n${Date.now() - 60_000}\n`);

        await expect(
          store.publish({ topic: "team.auth", from: identity, text: "blocked" }),
        ).rejects.toThrow(new RegExp(`held by pid ${holder.pid} \\(alive${fs.existsSync(`/proc/${holder.pid}/stat`) ? ", state T stopped" : ""}\\) for \\d+ s`));
        expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toContain(`${holder.pid}\n`);
      } finally {
        try { process.kill(holder.pid!, "SIGCONT"); } catch {}
        holder.kill("SIGKILL");
        await closed;
      }
    },
  );

  it("waits out a fresh ownerless lock instead of sweeping an in-flight acquisition", async () => {
    const store = createStore({ lockTimeoutMs: 300 });
    const lockPath = holdLock(store);

    await expect(
      store.publish({ topic: "team.auth", from: identity, text: "blocked" }),
    ).rejects.toThrow("Timed out waiting for the Fabric mesh lock (lock directory has no owner record)");
    expect(fs.existsSync(lockPath)).toBe(true);
  });
});
