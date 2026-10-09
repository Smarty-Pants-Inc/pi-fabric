import { createHash } from "node:crypto";
import * as atomic from "../src/core/atomic-write.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshBatchView } from "../src/mesh/store.js";
import { MESH_ARCHIVE_CONFIG, MeshArchive } from "../src/mesh/archive.js";

const identity = { id: "snapshot", name: "snapshot", kind: "main" as const };
const roots: string[] = [];
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-write-snapshot-"));
  roots.push(root);
  return { root, file: path.join(root, "state.json"), lock: path.join(root, ".lock"),
    store: new MeshStore(root, 65536, 100) };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Simulate another lock holder's atomic commit between preparation and acquisition.
const raceAtAcquisition = (root: string, mutate: (state: any) => void) => {
  const mkdir = fs.mkdirSync;
  let raced = false;
  vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
    if (String(target) === path.join(root, ".lock") && !raced) {
      raced = true;
      const file = path.join(root, "state.json");
      const state = JSON.parse(fs.readFileSync(file, "utf8"));
      mutate(state);
      const temporary = path.join(root, "competing.tmp");
      fs.writeFileSync(temporary, JSON.stringify(state));
      fs.renameSync(temporary, file);
    }
    return (mkdir as (...args: unknown[]) => unknown)(target, ...args);
  }) as typeof fs.mkdirSync);
  return () => expect(raced).toBe(true);
};
const assertNoStaging = (root: string) =>
  expect(fs.readdirSync(root).filter(name => name.endsWith(".prepared.tmp"))).toEqual([]);

describe("prepared live archive catch-up", () => {
  it.each([false, true])("scans historical live bytes outside custody and fences a concurrent append (race=%s)", async race => {
    const { root, lock } = setup();
    const file = path.join(root, "events.jsonl");
    const dir = path.join(root, "archive");
    const events = Array.from({ length: 12 }, (_, i) => ({ id: `historical-${i}`, sequence: i + 1,
      topic: "test.history", kind: "message", from: identity, text: "x".repeat(1000), createdAt: Date.now() }));
    fs.writeFileSync(file, events.map(event => JSON.stringify(event) + "\n").join(""));
    fs.writeFileSync(path.join(root, "sequence"), "12");
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
    const stat = fs.statSync(file);
    const read = fs.readSync;
    let historicalReads = 0;
    vi.spyOn(fs, "readSync").mockImplementation(((fd: number, ...args: unknown[]) => {
      const opened = fs.fstatSync(fd);
      if (opened.ino === stat.ino && opened.dev === stat.dev && Number(args[2]) > 4097) {
        historicalReads++; expect(fs.existsSync(lock)).toBe(false);
      }
      return (read as (...args: unknown[]) => unknown)(fd, ...args);
    }) as typeof fs.readSync);
    const mkdir = fs.mkdirSync;
    let raced = false;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
      if (race && String(target) === lock && !raced) {
        raced = true;
        const event = { ...events[0]!, id: "competing-event", sequence: 13, text: "competing" };
        fs.appendFileSync(file, JSON.stringify(event) + "\n");
        fs.writeFileSync(path.join(root, "sequence"), "13");
      }
      return (mkdir as (...args: unknown[]) => unknown)(target, ...args);
    }) as typeof fs.mkdirSync);
    const store = new MeshStore(root, 4096, 100);
    const published = await store.publish({ topic: "test.history", from: identity, text: "new" });
    expect(historicalReads).toBeGreaterThan(0);
    expect(raced).toBe(race);
    expect(published.sequence).toBe(race ? 14 : 13);
    const archive = new MeshArchive(dir, root);
    const historical = archive.readAfter(0, published.sequence, () => true, 100);
    expect(historical.slice(0, 12)).toEqual(events);
    if (race) expect(historical.find(event => event.sequence === 13)?.id).toBe("competing-event");
    expect(archive.lookup(published.sequence)).toEqual(published);
  });

  it("never archives a valid JSON suffix missing its commit newline", async () => {
    const { root } = setup();
    const file = path.join(root, "events.jsonl"), dir = path.join(root, "archive");
    const event = { id: "committed", sequence: 1, topic: "test.history", kind: "message", from: identity, createdAt: Date.now() };
    fs.writeFileSync(file, JSON.stringify(event) + "\n" + JSON.stringify({ ...event, id: "torn", sequence: 2 }));
    fs.writeFileSync(path.join(root, "sequence"), "1");
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
    const published = await new MeshStore(root, 4096, 100).publish({ topic: "test.history", from: identity, text: "new" });
    const archive = new MeshArchive(dir, root);
    expect(archive.readAfter(0, published.sequence, () => true, 100)[0]).toEqual(event);
    expect(published.sequence).toBe(2);
    expect(archive.lookup(2)).toEqual(published);
    expect(fs.readFileSync(file, "utf8")).not.toContain('"id":"torn"');
  });
});

describe("cold optimistic protocol-2 writes", () => {
  it("ordinary writes await constructor identity preparation while bounded tries fail closed", async () => {
    let resolve!: (value: string | undefined) => void;
    const identityReady = new Promise<string | undefined>(done => { resolve = done; });
    vi.spyOn(atomic, "ownProcessIncarnation").mockReturnValue(identityReady);
    // Deterministic preparation time: the remaining optimistic budget is smaller
    // than the configured ordinary budget even on a very fast filesystem.
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now++);
    const { root } = setup();
    const store = new MeshStore(root, 65536, 100, { lockProtocol: 2 });
    try {
      await expect(store.withTryLock(() => store.put({ key: "test/cold/a", value: 0, identity }), 0))
        .rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      const pending = store.put({ key: "test/cold/a", value: 1, identity });
      resolve(undefined);
      await pending;
      expect(store.get("test/cold/a", { fresh: true })?.value).toBe(1);
    } finally { resolve(undefined); }
  });
});

describe("optimistic mesh write snapshots", () => {
  it("reads/parses canonical state and encodes retained entries outside custody", async () => {
    const { root, file, lock, store } = setup();
    const retainedKey = "test/retained/a";
    await store.put({ key: retainedKey, value: { text: "retained雪😀" }, identity });
    const read = fs.readFileSync;
    const stringify = JSON.stringify;
    const parse = JSON.parse;
    let reads = 0, parses = 0, encodings = 0;
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (target === file) { reads++; expect(fs.existsSync(lock)).toBe(false); }
      return (read as (...args: unknown[]) => unknown)(target, ...args);
    }) as typeof fs.readFileSync);
    vi.spyOn(JSON, "parse").mockImplementation(((text: string, ...args: unknown[]) => {
      if (text.includes('"entries":') && text.includes("retained雪😀")) {
        parses++; expect(fs.existsSync(lock)).toBe(false);
      }
      return (parse as (...args: unknown[]) => unknown)(text, ...args);
    }) as typeof JSON.parse);
    vi.spyOn(JSON, "stringify").mockImplementation(((value: any, ...args: unknown[]) => {
      if (value?.key === retainedKey && value?.updatedBy) {
        encodings++; expect(fs.existsSync(lock)).toBe(false);
      }
      return (stringify as (...args: unknown[]) => unknown)(value, ...args);
    }) as typeof JSON.stringify);
    const cold = new MeshStore(root, 65536, 100);
    await cold.put({ key: "other/put/a", value: 1, identity });
    await cold.delete({ key: "other/put/a" });
    await new MeshStore(root, 65536, 100).writeBatch({ identity, ops: [
      { kind: "put", key: "other/heartbeat/a", value: (now: number) => ({ now }) },
    ] });
    // Both cold stores parse; the same-store delete reuses its frozen committed
    // tree only after a fresh exact-byte read. Every observed operation stays
    // outside custody, including the two retained-entry encodings.
    expect(reads).toBe(3); expect(parses).toBe(2); expect(encodings).toBe(2);
    assertNoStaging(root);
  });

  it("retries a copied-marker replacement without losing the competing value", async () => {
    const { root, store } = setup();
    const before = await store.put({ key: "test/retained/a", value: "old", identity });
    const raced = raceAtAcquisition(root, state => { state.entries[before.key].value = "new"; });
    await store.put({ key: "test/other/a", value: 1, identity });
    raced();
    expect(store.get(before.key, { fresh: true })?.value).toBe("new");
    expect(store.get("test/other/a", { fresh: true })?.value).toBe(1);
    assertNoStaging(root);
  });

  it("rechecks CAS after a competing commit and cleans its discarded prepared file", async () => {
    const { root, store } = setup();
    const before = await store.put({ key: "test/cas/a", value: "old", identity });
    const raced = raceAtAcquisition(root, state => {
      state.entries[before.key].value = "winner";
      state.entries[before.key].version++;
      state.versions[before.key] = state.entries[before.key].version;
      state.highWater = state.entries[before.key].version;
    });
    await expect(store.put({ key: before.key, value: "loser", ifVersion: before.version, identity }))
      .rejects.toThrow();
    raced();
    expect(store.get(before.key, { fresh: true })?.value).toBe("winner");
    assertNoStaging(root);
  });

  it("invokes batch callbacks once, under validated custody, including dynamically selected namespaces", async () => {
    const { root, file, lock, store } = setup();
    await store.put({ key: "test/dynamic/a", value: "old", identity });
    const raced = raceAtAcquisition(root, state => { state.entries["test/dynamic/a"].value = "new"; });
    const prepare = vi.fn((view: MeshBatchView) => {
      expect(fs.existsSync(lock)).toBe(true);
      expect(view.get("test/dynamic/a")?.value).toBe("new");
      return [{ kind: "put" as const, key: "test/dynamic/a", value: value }];
    });
    const value = vi.fn(() => { expect(fs.existsSync(lock)).toBe(true); return "final"; });
    const afterCommit = vi.fn((view: MeshBatchView) => {
      expect(fs.existsSync(lock)).toBe(true);
      expect(view.get("test/dynamic/a")?.value).toBe("final");
      expect(JSON.parse(fs.readFileSync(file, "utf8")).entries["test/dynamic/a"].value).toBe("final");
    });
    await store.writeBatch({ identity, ops: [], prepare, afterCommit });
    raced();
    expect(prepare).toHaveBeenCalledOnce(); expect(value).toHaveBeenCalledOnce();
    expect(afterCommit).toHaveBeenCalledOnce();
    const signal = JSON.parse(fs.readFileSync(path.join(root, "state.read-signal.json"), "utf8"));
    const canonical = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(signal.generation).toBe(canonical.readGeneration);
    const hash = createHash("sha256").update(`${JSON.stringify(canonical.entries["test/dynamic/a"])}\n`).digest("base64");
    expect(signal.namespaces["test/dynamic/"]).toBe(hash);
    assertNoStaging(root);
  });

  it("does not overwrite damaged canonical state or leak staging files", async () => {
    const { root, file, store } = setup();
    fs.writeFileSync(file, '{"entries":');
    await expect(store.put({ key: "test/damage/a", value: 1, identity })).rejects.toThrow();
    expect(fs.readFileSync(file, "utf8")).toBe('{"entries":');
    assertNoStaging(root);
  });
});
