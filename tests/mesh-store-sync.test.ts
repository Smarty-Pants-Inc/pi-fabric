import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { openNodeSqlite } from "../src/mesh/state-sqlite.js";

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

describe.each(["file", "sqlite"] as const)("MeshStore.sync durability barrier (%s state)", backend => {
  it("a SIGKILLed producer's reopened mesh holds every event and state record its synced cursor covers", async () => {
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

  it("syncs an empty root, and its own commits, without creating anything", async () => {
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

describe("MeshStore.sync failure", () => {
  it("rejects when the event fsync fails, so the caller advances nothing", async () => {
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
