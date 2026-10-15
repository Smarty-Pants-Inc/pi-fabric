import { createHash } from "node:crypto";
import * as atomic from "../src/core/atomic-write.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshBatchOperation, type MeshBatchView } from "../src/mesh/store.js";
import type { MeshCommitEffects } from "../src/mesh/state-backend.js";
import { encodeMeshStateNatsMarker, MeshBackendFenceError } from "../src/mesh/backend-fence.js";
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
        .rejects.toMatchObject({ code: "FABRIC_MESH_INCARNATION_PENDING" });
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
    expect(reads).toBe(3); expect(parses).toBe(3); expect(encodings).toBe(2);
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

  it("stages a static batch outside custody and keeps final commit callbacks locked", async () => {
    const { root, file, lock, store } = setup();
    await store.put({ key: "test/batch/delete", value: "old", identity });
    const stringify = JSON.stringify, write = fs.writeFileSync;
    let encodings = 0, staging = 0, encodingFinished = false;
    vi.spyOn(JSON, "stringify").mockImplementation(((value: any, ...args: unknown[]) => {
      if (!encodingFinished && value?.key === "test/batch/new" && value?.updatedBy) {
        encodings++; expect(fs.existsSync(lock)).toBe(false);
      }
      return (stringify as (...args: unknown[]) => unknown)(value, ...args);
    }) as typeof JSON.stringify);
    vi.spyOn(fs, "writeFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(target).includes(".prepared.tmp")) {
        staging++; expect(fs.existsSync(lock)).toBe(false);
      }
      return (write as (...args: unknown[]) => unknown)(target, ...args);
    }) as typeof fs.writeFileSync);
    const beforeCommit = vi.fn(() => {
      expect(fs.existsSync(lock)).toBe(true);
      expect(JSON.parse(fs.readFileSync(file, "utf8")).entries["test/batch/new"]).toBeUndefined();
      encodingFinished = true;
    });
    const afterCommit = vi.fn((view: MeshBatchView) => {
      expect(fs.existsSync(lock)).toBe(true);
      expect(view.get("test/batch/new")?.value).toEqual({ text: "new雪😀" });
      expect(view.get("test/batch/delete")).toBeUndefined();
    });
    const results = await store.writeBatch({ identity, beforeCommit, afterCommit, ops: [
      { kind: "put", key: "test/batch/new", value: { text: "new雪😀" } },
      { kind: "delete", key: "test/batch/delete" },
    ] });
    expect(results.map(result => result.applied)).toEqual([true, true]);
    expect(encodings).toBe(1); expect(staging).toBeGreaterThan(0);
    expect(beforeCommit).toHaveBeenCalledOnce(); expect(afterCommit).toHaveBeenCalledOnce();
    assertNoStaging(root);
  });

  it("rechecks batch CAS atomically after a competing commit and discards all staged operations", async () => {
    const { root, file, store } = setup();
    const before = await store.put({ key: "test/cas/a", value: "old", identity });
    const raced = raceAtAcquisition(root, state => {
      state.entries[before.key].value = "winner";
      state.entries[before.key].version++;
      state.versions[before.key] = state.entries[before.key].version;
      state.highWater = state.entries[before.key].version;
    });
    const afterCommit = vi.fn(), commitOutbox = vi.fn(), beforeCommit = vi.fn();
    await expect(store.writeBatch({ identity, beforeCommit, afterCommit, commitOutbox, ops: [
      { kind: "put", key: "test/cas/prefix", value: "must not commit" },
      { kind: "put", key: before.key, value: "loser", ifVersion: before.version },
    ] })).rejects.toMatchObject({ key: before.key, expected: before.version, found: before.version + 1 });
    raced();
    const state = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(state.entries[before.key].value).toBe("winner");
    expect(state.entries["test/cas/prefix"]).toBeUndefined();
    expect(beforeCommit).not.toHaveBeenCalled(); expect(afterCommit).not.toHaveBeenCalled();
    expect(commitOutbox).not.toHaveBeenCalled();
    assertNoStaging(root);
  });

  it.each([false, true])("bounds static-batch retries without running effects (registry scope=%s)", async scoped => {
    const { root, file, lock, store } = setup();
    await store.put({ key: "test/retry/retained", value: 0, identity });
    const mkdir = fs.mkdirSync;
    let replacements = 0;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
      if (String(target) === lock && replacements < 2) {
        const state = JSON.parse(fs.readFileSync(file, "utf8"));
        state.entries["test/retry/retained"].value = ++replacements;
        const temporary = path.join(root, "competing.tmp");
        fs.writeFileSync(temporary, JSON.stringify(state));
        fs.renameSync(temporary, file);
      }
      return (mkdir as (...args: unknown[]) => unknown)(target, ...args);
    }) as typeof fs.mkdirSync);
    const beforeCommit = vi.fn(), afterCommit = vi.fn(), commitOutbox = vi.fn();
    const write = () => store.writeBatch({ identity, beforeCommit, afterCommit, commitOutbox,
      ops: [{ kind: "put", key: "test/retry/new", value: "must not commit" }],
    });
    const attempts = scoped ? 1 : 2;
    await expect(scoped ? store.withTryLock(write, 0) : write())
      .rejects.toMatchObject({ code: "FABRIC_MESH_SNAPSHOT_RETRIES_EXHAUSTED", attempts });
    expect(replacements).toBe(attempts);
    expect(store.get("test/retry/retained", { fresh: true })?.value).toBe(attempts);
    expect(store.get("test/retry/new", { fresh: true })).toBeUndefined();
    expect(beforeCommit).not.toHaveBeenCalled(); expect(afterCommit).not.toHaveBeenCalled();
    expect(commitOutbox).not.toHaveBeenCalled();
    assertNoStaging(root);
  });

  it.each([false, true])("snapshots a static put's own key and CAS before toJSON can mutate them (valid=%s)", async valid => {
    const { root, store } = setup();
    const a = await store.put({ key: "test/snapshot/a", value: "original A", identity });
    const b = await store.put({ key: "test/snapshot/b", value: "original B", identity });
    expect(a.version).not.toBe(b.version);
    let captures = 0;
    const op: Extract<MeshBatchOperation, { kind: "put" }> = {
      kind: "put", key: a.key, ifVersion: valid ? a.version : b.version, onConflict: "skip",
      value: { toJSON() {
        captures++;
        op.key = b.key;
        op.ifVersion = b.version;
        return "replacement A";
      } },
    };
    const results = await store.writeBatch({ identity, ops: [op] });
    expect(results).toEqual([{ key: a.key, applied: valid, version: a.version + Number(valid) }]);
    expect(captures).toBe(Number(valid));
    if (valid) expect(op).toMatchObject({ key: b.key, ifVersion: b.version });
    expect(store.get(a.key, { fresh: true })).toMatchObject({
      key: a.key, value: valid ? "replacement A" : "original A", version: a.version + Number(valid),
    });
    expect(store.get(b.key, { fresh: true })).toEqual(b);
    assertNoStaging(root);
  });

  it.each([false, true])("serializes a static CAS payload only when the operation applies (valid=%s)", async valid => {
    const { root, store } = setup();
    const before = await store.put({ key: "test/capture/a", value: "old", identity });
    let captures = 0;
    const results = await store.writeBatch({ identity, ops: [{
      kind: "put", key: before.key, ifVersion: before.version + Number(!valid), onConflict: "skip",
      value: { toJSON() { captures++; return "new"; } },
    }] });
    expect(results).toEqual([{ key: before.key, applied: valid, version: before.version + Number(valid) }]);
    expect(captures).toBe(Number(valid));
    expect(store.get(before.key, { fresh: true })).toMatchObject({
      value: valid ? "new" : "old", version: before.version + Number(valid),
    });
    assertNoStaging(root);
  });

  it("snapshots all descriptors and the ops list before an unconditional payload is serialized", async () => {
    const { root, store } = setup();
    const a = await store.put({ key: "test/descriptors/a", value: "original A", identity });
    const b = await store.put({ key: "test/descriptors/b", value: "original B", identity });
    const later: MeshBatchOperation = { kind: "put", key: a.key, ifVersion: a.version, value: "replacement A" };
    const input = { identity, ops: [] as MeshBatchOperation[] };
    let captures = 0;
    input.ops.push({ kind: "put", key: "test/descriptors/trigger", value: { toJSON() {
      captures++;
      later.key = b.key;
      later.ifVersion = b.version;
      input.ops.splice(1, 1, { kind: "put", key: "test/descriptors/injected", value: "must not commit" });
      return "trigger";
    } } }, later);
    const results = await store.writeBatch(input);
    expect(captures).toBe(1);
    expect(later).toMatchObject({ key: b.key, ifVersion: b.version });
    expect(input.ops[1]?.key).toBe("test/descriptors/injected");
    expect(results).toEqual([
      { key: "test/descriptors/trigger", applied: true, version: b.version + 1 },
      { key: a.key, applied: true, version: a.version + 1 },
    ]);
    expect(store.get(a.key, { fresh: true })).toMatchObject({ value: "replacement A", version: a.version + 1 });
    expect(store.get(b.key, { fresh: true })).toEqual(b);
    expect(store.get("test/descriptors/injected", { fresh: true })).toBeUndefined();
    assertNoStaging(root);
  });

  it("pre-captures primary-path static payloads once while mixed callbacks remain authoritative", async () => {
    const { root, lock, store } = setup();
    await store.put({ key: "test/mixed/retained", value: "old", identity });
    await store.put({ key: "test/mixed/expired", value: "expired", identity });
    const raced = raceAtAcquisition(root, state => { state.entries["test/mixed/retained"].value = "new"; });
    let captures = 0, skippedCaptures = 0;
    const ops = Array.from({ length: 190 }, (_, actor) => ({ kind: "put" as const, key: `actors/live/${actor}`,
      value: { toJSON() {
        captures++; expect(fs.existsSync(lock)).toBe(false);
        return { actor, payload: "x".repeat(512) };
      } } }));
    const value = vi.fn((now: number) => { expect(fs.existsSync(lock)).toBe(true); return { now }; });
    const condition = vi.fn((current: MeshBatchView["get"]) => {
      expect(fs.existsSync(lock)).toBe(true);
      return current("test/mixed/retained")?.value === "new";
    });
    const onConflict = vi.fn(() => { expect(fs.existsSync(lock)).toBe(true); return "skip" as const; });
    const prepare = vi.fn((view: MeshBatchView) => {
      expect(fs.existsSync(lock)).toBe(true);
      expect(view.get("test/mixed/retained")?.value).toBe("new");
      return [{ kind: "delete" as const, key: "test/mixed/expired", condition }];
    });
    const results = await store.writeBatch({ identity, prepare, ops: [...ops,
      { kind: "put", key: "hosts/live/self", value },
      { kind: "put", key: "test/mixed/retained", ifVersion: 999, onConflict,
        value: { toJSON() { skippedCaptures++; return "must never serialize"; } } },
    ] });
    raced();
    expect(captures).toBe(190);
    expect(skippedCaptures).toBe(0);
    expect(results[191]).toEqual({ key: "test/mixed/retained", applied: false, version: 1 });
    for (const callback of [prepare, value, condition, onConflict]) expect(callback).toHaveBeenCalledOnce();
    expect(results.filter(result => result.applied)).toHaveLength(192);
    expect(store.get("actors/live/189", { fresh: true })?.value).toEqual({ actor: 189, payload: "x".repeat(512) });
    expect(store.get("test/mixed/retained", { fresh: true })?.value).toBe("new");
    expect(store.get("test/mixed/expired", { fresh: true })).toBeUndefined();
    assertNoStaging(root);
  });

  it.each([false, true])("detaches the exact committed outbox snapshot outside custody (prepare=%s)", async prepared => {
    const { root, lock, store } = setup();
    const stringify = JSON.stringify;
    let clones = 0, committedStamp: string | undefined;
    let competing: Promise<unknown> | undefined;
    vi.spyOn(JSON, "stringify").mockImplementation(((value: any, ...args: unknown[]) => {
      if (value?.entries?.["test/outbox/a"]) {
        clones++; expect(fs.existsSync(lock)).toBe(false);
        // An ordinary competing commit lands synchronously during off-lock cloning.
        competing = store.put({ key: "test/outbox/a", value: { n: 2 }, identity });
        expect(store.get("test/outbox/a", { fresh: true })?.value).toEqual({ n: 2 });
      }
      return (stringify as (...args: unknown[]) => unknown)(value, ...args);
    }) as typeof JSON.stringify);
    let effect!: MeshCommitEffects;
    const results = await store.writeBatch({ identity,
      ops: [{ kind: "put", key: "test/outbox/a", value: { n: 1 } }],
      ...(prepared ? { prepare: () => [] } : {}),
      afterCommit: () => { committedStamp = store.stateStamp(); },
      commitOutbox: committed => { expect(fs.existsSync(lock)).toBe(false); effect = committed; },
    });
    expect(clones).toBe(1);
    await competing;
    expect(effect.stamp).toBe(committedStamp);
    expect(effect.stamp).not.toBe(store.stateStamp());
    expect(effect.view.get("test/outbox/a")?.value).toEqual({ n: 1 });
    (effect.view.get("test/outbox/a")!.value as { n: number }).n = 99;
    expect(effect.view.get("test/outbox/a")?.value).toEqual({ n: 1 });
    expect(effect.results).toEqual(results);
    expect(effect.results[0]).not.toBe(results[0]);
    assertNoStaging(root);
  });

  it("honors final ownership and backend fences after static-batch staging", async () => {
    const { root, file, lock, store } = setup();
    await store.put({ key: "test/fence/a", value: "old", identity });
    const before = fs.readFileSync(file, "utf8");
    const afterCommit = vi.fn(), commitOutbox = vi.fn();
    const beforeCommit = vi.fn(() => { expect(fs.existsSync(lock)).toBe(true); throw new Error("ownership lost"); });
    await expect(store.writeBatch({ identity, beforeCommit, afterCommit, commitOutbox,
      ops: [{ kind: "put", key: "test/fence/a", value: "refused" }] })).rejects.toThrow("ownership lost");
    expect(fs.readFileSync(file, "utf8")).toBe(before);
    expect(beforeCommit).toHaveBeenCalledOnce();
    expect(afterCommit).not.toHaveBeenCalled(); expect(commitOutbox).not.toHaveBeenCalled();
    assertNoStaging(root);
    const marker = encodeMeshStateNatsMarker("cutover", 1);
    const mkdir = fs.mkdirSync;
    let raced = false;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
      if (String(target) === lock && !raced) { raced = true; fs.writeFileSync(file, marker); }
      return (mkdir as (...args: unknown[]) => unknown)(target, ...args);
    }) as typeof fs.mkdirSync);
    await expect(store.writeBatch({ identity, afterCommit, commitOutbox,
      ops: [{ kind: "put", key: "test/fence/a", value: "late" }] })).rejects.toBeInstanceOf(MeshBackendFenceError);
    expect(raced).toBe(true); expect(fs.readFileSync(file, "utf8")).toBe(marker);
    expect(afterCommit).not.toHaveBeenCalled(); expect(commitOutbox).not.toHaveBeenCalled();
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
