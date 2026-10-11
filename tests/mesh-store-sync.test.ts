import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { withFabricHostCaller } from "../src/fabric-provenance.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";
import { MeshStateFileNotDurableError } from "../src/mesh/state-file.js";
import { EventLog } from "../src/mesh/event-log.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshEventsNotDurableError, MeshSyncNotDurableError } from "../src/mesh/not-durable.js";
import { openNodeSqlite } from "../src/mesh/state-sqlite.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

// smarty-dev#9313: MeshStore.sync() is the barrier a trusted producer awaits before it fsyncs its
// own cursor or delivered mark. A SIGKILL cannot drop the page cache, so the two-process test also
// checks what the barrier made durable: on Linux, the files it fsynced (events.jsonl, and state.json
// for file state); for SQLite state, that the database file alone, without its WAL, holds the rows.

type Ack = { round: number; published: Array<{ id: string; sequence: number; key: string }>; fsynced: string[] };
const roots: string[] = [];
const tempRoot = (prefix: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  roots.push(root);
  return root;
};
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

const readAcks = (file: string): Ack[] => {
  if (!fs.existsSync(file)) return [];
  // A kill can cut the last line short; only complete lines were acknowledged (each is fsynced whole).
  return fs.readFileSync(file, "utf8").split("\n").flatMap(line => { try { return [JSON.parse(line) as Ack]; } catch { return []; } });
};

const runUntilKilled = async (root: string, backend: "file" | "sqlite", ackPath: string, copyDir: string, acks: number): Promise<void> => {
  const child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-sync-crash.mjs"), root, backend, ackPath, copyDir], {
    cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe"],
  });
  let stderr = "";
  child.stderr!.on("data", chunk => { stderr += String(chunk); });
  const closed = new Promise<void>(resolve => child.once("close", () => resolve()));
  try {
    const deadline = Date.now() + 20_000;
    while (readAcks(ackPath).length < acks) {
      if (child.exitCode !== null) throw new Error(`sync producer exited early: ${stderr}`);
      if (Date.now() > deadline) throw new Error(`timed out waiting for ${acks} synced rounds: ${stderr}`);
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  } finally {
    child.kill("SIGKILL");
    await closed;
  }
  expect(child.signalCode).toBe("SIGKILL");
};

// File state has no namespace barrier on Windows: its sync() refuses there (see the platform-stubbed tests below).
const windowsFile = (backend: "file" | "sqlite"): boolean => backend === "file" && process.platform === "win32";

describe.each(["file", "sqlite"] as const)("MeshStore.sync durability barrier (%s state)", backend => {
  it.skipIf(windowsFile(backend))("a SIGKILLed producer's reopened mesh holds every event and state record its synced cursor covers", async () => {
    const root = tempRoot("mesh-sync-crash-"), out = tempRoot("mesh-sync-ack-");
    const ackPath = path.join(out, "cursor.jsonl");
    await runUntilKilled(root, backend, ackPath, out, 4);
    const acks = readAcks(ackPath);
    expect(acks.length).toBeGreaterThanOrEqual(4);

    const reopened = new MeshStore(root, 64 * 1024, 100, { stateBackend: backend });
    try {
      expect(reopened.stateBackend).toBe(backend);
      const events = new Map(reopened.read({ limit: 100 }).map(event => [event.id, event]));
      for (const { published } of acks) {
        for (const { id, sequence, key } of published) {
          expect(events.get(id)?.sequence, `synced event ${id}`).toBe(sequence);
          expect(reopened.get(key, { fresh: true })?.value, `synced state ${key}`).toEqual({ sequence });
        }
      }
    } finally { reopened.closeState(); }

    const real = fs.realpathSync(root);
    if (process.platform === "linux") {
      for (const { round, fsynced } of acks) {
        expect(fsynced, `round ${round}: the barrier fsyncs the live log`).toContain(path.join(real, "events.jsonl"));
        expect(fsynced, `round ${round}: and the mesh root directory`).toContain(real);
        if (backend === "file") expect(fsynced, `round ${round}: and state.json`).toContain(path.join(real, "state.json"));
      }
    }
    if (backend === "sqlite") {
      for (const { round, published } of acks.slice(0, 3)) {
        const db = openNodeSqlite(path.join(out, `state-${round}.db`));
        try {
          expect(db.prepare("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" });
          const keys = new Set(db.prepare("SELECT key FROM kv").all().map(row => String((row as { key: unknown }).key)));
          // Every round up to this one: the checkpoint backfilled all earlier commits too.
          for (const earlier of acks.slice(0, round + 1)) {
            for (const { key } of earlier.published) expect(keys.has(key), `round ${round}: ${key} in state.db without its WAL`).toBe(true);
          }
          expect(published.length).toBe(3);
        } finally { db.close(); }
      }
    }
  }, 40_000);

  it.skipIf(windowsFile(backend))("syncs an empty root, and its own commits, without creating anything", async () => {
    const root = tempRoot("mesh-sync-empty-");
    const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: backend });
    try {
      await store.sync();
      expect(fs.existsSync(path.join(root, "events.jsonl"))).toBe(false);
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
      expect(fs.existsSync(path.join(root, "state.json"))).toBe(false);
      const identity = { id: "sync-test", name: "sync-test", kind: "agent" as const };
      await store.publish({ topic: "ops.sync", from: identity });
      await store.put({ key: "forwarder/failed/1", value: 1, identity });
      await Promise.all([store.sync(), store.sync(0)]);
      await expect(store.sync(Number.NaN)).rejects.toThrow("budgetMs");
      await expect(store.sync(-1)).rejects.toThrow("budgetMs");
    } finally { store.closeState(); }
  });
});

describe("MeshStore.sync through (pi-fabric#894)", () => {
  // Process A reserves sequence n (the counter) and pauses under .lock before its append; process B
  // (this test) calls mesh.sync. B's through must stay below n until A appends: a producer that
  // fsyncs its cursor through a reserved-but-unappended n would skip n after a crash.
  it.skipIf(process.platform === "win32")("never reports a reserved but unappended sequence across processes", async () => {
    const root = tempRoot("mesh-sync-race-"), out = tempRoot("mesh-sync-race-out-");
    const pause = path.join(out, "pause");
    const identity = { id: "sync-race-b", name: "sync-race-b", kind: "agent" as const };
    const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "file" });
    const participants: FabricParticipantSource = {
      list: () => [], get: () => undefined, self: () => { throw new Error("unused"); },
      peers: () => [], async refresh() {}, scheduleRefresh() {},
    };
    const provider = new MeshProvider(store, identity, participants);
    const host = withFabricHostCaller({
      cwd: process.cwd(), signal: undefined, extensionContext: {} as ExtensionContext,
      parentToolCallId: "sync-race", nestedToolCallId: "sync-race", update() {},
    } satisfies FabricInvocationContext, "fabric-github.forwarder");
    const sync = async (): Promise<number> => ((await provider.invoke("sync", {}, host)) as { through: number }).through;
    let child: ReturnType<typeof spawn> | undefined;
    try {
      await store.publish({ topic: "ops.sync", from: identity });
      const committed = await store.publish({ topic: "ops.sync", from: identity });
      expect(await sync()).toBe(committed.sequence);

      child = spawn(process.execPath, [path.resolve("tests/fixtures/mesh-sync-reserve-pause.mjs"), root], {
        cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"],
        env: { ...process.env, PI_FABRIC_TEST_PAUSE_AFTER_RESERVE: pause },
      });
      let stdout = "", stderr = "";
      child.stdout!.on("data", chunk => { stdout += String(chunk); });
      child.stderr!.on("data", chunk => { stderr += String(chunk); });
      const exited = new Promise<number | null>(resolve => child!.once("close", code => resolve(code)));
      const deadline = Date.now() + 20_000;
      while (!fs.existsSync(`${pause}.reserved`)) {
        if (child.exitCode !== null) throw new Error(`publisher A exited early: ${stderr}`);
        if (Date.now() > deadline) throw new Error(`publisher A never reserved: ${stderr}`);
        await new Promise(resolve => setTimeout(resolve, 5));
      }
      const n = Number(fs.readFileSync(`${pause}.reserved`, "utf8"));
      expect(n).toBe(committed.sequence + 1);
      // The race: the counter already says n, the log does not hold n yet.
      expect(store.latestSequence()).toBe(n);
      expect(store.read({ after: committed.sequence })).toEqual([]);
      for (let round = 0; round < 3; round += 1) {
        const through = await sync();
        expect(through, "B's barrier while A holds a reservation").toBeLessThan(n);
        expect(through).toBe(committed.sequence);
        expect((await store.sync()).through).toBe(committed.sequence);
      }
      expect(child.exitCode, "A is still paused under .lock").toBeNull();

      fs.writeFileSync(`${pause}.go`, "");
      expect(await exited, stderr).toBe(0);
      const appended = JSON.parse(stdout) as MeshEvent;
      expect(appended.sequence).toBe(n);
      const through = await sync();
      expect(through).toBeGreaterThanOrEqual(n);
      expect(store.read({ after: committed.sequence }).map(event => event.id)).toEqual([appended.id]);
    } finally {
      if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      store.closeState();
    }
  }, 30_000);
});

describe("MeshStore.sync failure", () => {
  it.skipIf(process.platform === "win32")("rejects when the event fsync fails, so the caller advances nothing", async () => {
    const root = tempRoot("mesh-sync-fail-");
    const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "file" });
    try {
      await store.publish({ topic: "ops.sync", from: { id: "sync-test", name: "sync-test", kind: "agent" } });
      const real = fs.fsyncSync;
      fs.fsyncSync = () => { throw Object.assign(new Error("EIO: i/o error, fsync"), { code: "EIO" }); };
      try { await expect(store.sync()).rejects.toThrow("EIO"); }
      finally { fs.fsyncSync = real; }
      await store.sync();
    } finally { store.closeState(); }
  });
});

// PR #894 review: on Windows syncPathNamespace skips the directory fsync and renameAtomic is a plain rename,
// so neither StateFile.sync() (state.json's rename) nor EventLog.sync() (events.jsonl's namespace, archive
// segments) can confirm durability. Both use the fail-closed namespace variant, like NATS acceptPeriodicSync:
// one MeshSyncNotDurableError shape, typed per backend, not retryable, and mesh.sync surfaces it.
describe("mesh sync without a namespace barrier (win32)", () => {
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  const onPlatform = async <T>(value: NodeJS.Platform, run: () => Promise<T>): Promise<T> => {
    Object.defineProperty(process, "platform", { ...platform, value });
    try { return await run(); } finally { Object.defineProperty(process, "platform", platform); }
  };
  const identity = { id: "sync-win", name: "sync-win", kind: "agent" as const };

  it("StateFile.sync rejects as not durable on win32 and resolves on linux", async () => {
    const root = tempRoot("mesh-sync-win32-");
    const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "file" });
    try {
      await store.put({ key: "forwarder/cursor/1", value: 1, identity });
      const state = store.stateBackendHandle;
      const error = await onPlatform("win32", () => state.sync(1_000).then(() => undefined, (failure: unknown) => failure));
      expect(error).toBeInstanceOf(MeshStateFileNotDurableError);
      expect(error).toBeInstanceOf(MeshSyncNotDurableError);
      expect(error).toMatchObject({ code: "FABRIC_MESH_STATE_FILE_NOT_DURABLE", retryable: false, platform: "win32" });
      expect((error as Error).message).toMatch(/durability is not confirmed.*win32.*not durable/);
      // Refused before any I/O: an empty root on win32 is refused too (no durability claim at all).
      const empty = new MeshStore(tempRoot("mesh-sync-win32-empty-"), 64 * 1024, 100, { stateBackend: "file" });
      try { await expect(onPlatform("win32", () => empty.stateBackendHandle.sync(0))).rejects.toBeInstanceOf(MeshStateFileNotDurableError); }
      finally { empty.closeState(); }
      if (process.platform !== "win32") {
        await expect(onPlatform("linux", () => state.sync(1_000))).resolves.toBeUndefined();
        // The store stays usable: only the durability claim was refused.
        expect(store.get("forwarder/cursor/1", { fresh: true })?.value).toBe(1);
      }
    } finally { store.closeState(); }
  });

  it("EventLog.sync rejects as not durable on win32, before any I/O, and resolves on linux", async () => {
    const root = tempRoot("mesh-sync-win32-events-");
    const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "sqlite" });
    try {
      const event = await store.publish({ topic: "ops.sync", from: identity });
      const events = new EventLog({ root, maxEventBytes: 64 * 1024, maxReadEvents: 100, lock: new MeshLock(root, {}, () => {}) }, {});
      const fsync = vi.spyOn(fs, "fsyncSync");
      try {
        const error = await onPlatform("win32", () => events.sync().then(() => undefined, (failure: unknown) => failure));
        expect(error).toBeInstanceOf(MeshEventsNotDurableError);
        expect(error).toBeInstanceOf(MeshSyncNotDurableError);
        expect(error).toMatchObject({ code: "FABRIC_MESH_EVENTS_NOT_DURABLE", retryable: false, platform: "win32" });
        expect((error as Error).message).toMatch(/^FABRIC_MESH_EVENTS_NOT_DURABLE: Fabric mesh event durability is not confirmed: .*win32.*not durable$/);
        expect(fsync).not.toHaveBeenCalled(); // no file fsync claimed before the refusal
        // An empty root is refused too: no durability claim at all without a namespace barrier.
        const emptyRoot = tempRoot("mesh-sync-win32-events-empty-");
        const empty = new EventLog({ root: emptyRoot, maxEventBytes: 64 * 1024, maxReadEvents: 100, lock: new MeshLock(emptyRoot, {}, () => {}) }, {});
        await expect(onPlatform("win32", () => empty.sync())).rejects.toBeInstanceOf(MeshEventsNotDurableError);
      } finally { fsync.mockRestore(); }
      if (process.platform !== "win32") {
        await expect(onPlatform("linux", () => events.sync())).resolves.toBe(event.sequence);
        // The log stays usable: only the durability claim was refused.
        expect(store.read({ after: 0 }).map(item => item.id)).toEqual([event.id]);
      }
    } finally { store.closeState(); }
  });

  it("mesh.sync surfaces the not-durable rejection on win32 for the file backend, and synced on linux", async () => {
    const root = tempRoot("mesh-sync-win32-provider-");
    const store = new MeshStore(root, 64 * 1024, 100, { stateBackend: "file" });
    const participants: FabricParticipantSource = {
      list: () => [], get: () => undefined, self: () => { throw new Error("unused"); },
      peers: () => [], async refresh() {}, scheduleRefresh() {},
    };
    const provider = new MeshProvider(store, identity, participants);
    const host = withFabricHostCaller({
      cwd: process.cwd(), signal: undefined, extensionContext: {} as ExtensionContext,
      parentToolCallId: "sync-win32", nestedToolCallId: "sync-win32", update() {},
    } satisfies FabricInvocationContext, "fabric-github.forwarder");
    try {
      await store.put({ key: "forwarder/failed/1", value: 1, identity });
      const event = await store.publish({ topic: "ops.sync", from: identity });
      await onPlatform("win32", async () => {
        // The event barrier runs first and fails closed; the file state barrier would refuse too.
        await expect(store.sync()).rejects.toBeInstanceOf(MeshEventsNotDurableError);
        await expect(store.stateBackendHandle.sync(1_000)).rejects.toBeInstanceOf(MeshStateFileNotDurableError);
        await expect(provider.invoke("sync", {}, host)).rejects.toMatchObject({
          name: "MeshEventsNotDurableError", code: "FABRIC_MESH_EVENTS_NOT_DURABLE", retryable: false,
          message: expect.stringContaining("not durable") });
      });
      if (process.platform !== "win32") {
        await expect(onPlatform("linux", () => provider.invoke("sync", {}, host))).resolves.toEqual({ synced: true, through: event.sequence });
      }
    } finally { store.closeState(); }
  });

  it("shadow state, whose authority is state.json, refuses the same way; SQLite state does not, but its events do", async () => {
    const shadow = new MeshStore(tempRoot("mesh-sync-win32-shadow-"), 64 * 1024, 100, { stateBackend: "shadow" });
    const sqlite = new MeshStore(tempRoot("mesh-sync-win32-sqlite-"), 64 * 1024, 100, { stateBackend: "sqlite" });
    try {
      await shadow.put({ key: "forwarder/a", value: 1, identity });
      await sqlite.put({ key: "forwarder/a", value: 1, identity });
      await onPlatform("win32", async () => {
        await expect(shadow.stateBackendHandle.sync(1_000)).rejects.toMatchObject({ code: "FABRIC_MESH_STATE_FILE_NOT_DURABLE" });
        // SQLite commits never rename: its barrier is a checkpoint flushed into state.db in place.
        await expect(sqlite.stateBackendHandle.sync(1_000)).resolves.toBeUndefined();
        // mesh.sync still refuses on both: the event log's namespace cannot be confirmed on win32.
        await expect(shadow.sync()).rejects.toMatchObject({ code: "FABRIC_MESH_EVENTS_NOT_DURABLE" });
        await expect(sqlite.sync()).rejects.toMatchObject({ code: "FABRIC_MESH_EVENTS_NOT_DURABLE" });
      });
    } finally { shadow.closeState(); sqlite.closeState(); }
  });
});
