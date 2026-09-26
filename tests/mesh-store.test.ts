import { spawn } from "node:child_process";
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

const createStore = (options?: MeshStoreOptions): MeshStore => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-"));
  roots.push(root);
  return new MeshStore(root, 64 * 1024, 100, options);
};

afterEach(() => {
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

  it("sweeps a stale lock whose owner process is dead", async () => {
    const store = createStore();
    const lockPath = holdLock(store, `crashed\n999999999\n${Date.now() - 60_000}\n`);

    const event = await store.publish({ topic: "team.auth", from: identity, text: "recovered" });

    expect(event.sequence).toBe(1);
    expect(fs.existsSync(lockPath)).toBe(false);
  });

  it("sweeps an ownerless lock left by a crash older than the stale window", async () => {
    const store = createStore();
    const lockPath = holdLock(store);
    const past = new Date(Date.now() - 60_000);
    fs.utimesSync(lockPath, past, past);

    const event = await store.publish({ topic: "team.auth", from: identity, text: "recovered" });

    expect(event.sequence).toBe(1);
    expect(fs.existsSync(lockPath)).toBe(false);
  });

  it("sweeps a lock whose owner file is corrupt", async () => {
    const store = createStore();
    const lockPath = holdLock(store, "not-a-valid-owner");

    const event = await store.publish({ topic: "team.auth", from: identity, text: "recovered" });

    expect(event.sequence).toBe(1);
    expect(fs.existsSync(lockPath)).toBe(false);
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

  // A stopped holder keeps the lock (taking it over could let the holder commit stale
  // state on resume); the timeout must name it so it can be restarted (smarty-dev#266).
  it.skipIf(process.platform !== "linux")(
    "names a signal-stopped holder in the timeout and does not take its lock",
    async () => {
      const holder = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
      try {
        await new Promise((resolve) => holder.once("spawn", resolve));
        process.kill(holder.pid!, "SIGSTOP");
        await new Promise((resolve) => setTimeout(resolve, 100));
        const store = createStore({ lockTimeoutMs: 300 });
        const lockPath = holdLock(store, `stopped\n${holder.pid}\n${Date.now() - 60_000}\n`);

        await expect(
          store.publish({ topic: "team.auth", from: identity, text: "blocked" }),
        ).rejects.toThrow(new RegExp(`held by pid ${holder.pid} \\(alive, state T stopped\\) for \\d+ s`));
        expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toContain(`${holder.pid}\n`);
      } finally {
        try { process.kill(holder.pid!, "SIGCONT"); } catch {}
        holder.kill("SIGKILL");
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
