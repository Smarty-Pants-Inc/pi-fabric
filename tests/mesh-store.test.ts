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
