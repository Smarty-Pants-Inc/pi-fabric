import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { once } from "node:events";
import { Worker } from "node:worker_threads";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

const roots: string[] = [];
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-registry-cache-"));
  roots.push(root);
  const file = path.join(root, "actors.json");
  const value = { format: 1, actors: [{ id: "actor", extra: { values: [1, 2] } }] };
  fs.writeFileSync(file, JSON.stringify(value));
  return { root, file, value, store: new ActorRegistryStore(root) };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("ActorRegistryStore cached read (#7791)", () => {
  it("bounds decoded generations to 64 paths and promotes hits before LRU eviction", () => {
    const fixtures = Array.from({ length: 65 }, () => setup());
    const views = fixtures.slice(0, 64).map(({ store }) => store.read());
    expect(fixtures[0]!.store.read()).toBe(views[0]); // Most recently used, not oldest inserted.
    fixtures[64]!.store.read();
    const parse = vi.spyOn(JSON, "parse");
    expect(fixtures[0]!.store.read()).toBe(views[0]);
    expect(parse).not.toHaveBeenCalled();
    expect(fixtures[1]!.store.read()).not.toBe(views[1]);
    expect(parse).toHaveBeenCalledTimes(1);
    // Reading the evicted path inserts it and evicts the next least recent path.
    expect(fixtures[2]!.store.read()).not.toBe(views[2]);
    expect(parse).toHaveBeenCalledTimes(2);
  });

  it("explicit release evicts the shared normalized path but preserves other roots", () => {
    const { root, store } = setup();
    const alias = new ActorRegistryStore(path.join(root, "."));
    const other = setup().store;
    const before = store.read(), retained = other.read();
    alias.releaseReadCache();
    const parse = vi.spyOn(JSON, "parse");
    expect(store.read()).not.toBe(before);
    expect(parse).toHaveBeenCalledTimes(1);
    expect(other.read()).toBe(retained);
    expect(parse).toHaveBeenCalledTimes(1);
  });


  it("parses once for 1,000 unchanged reads with one fstat per read, shared across path aliases", () => {
    const { root, value, store } = setup();
    const alias = new ActorRegistryStore(path.join(root, "."));
    const parse = vi.spyOn(JSON, "parse");
    const stat = vi.spyOn(fs, "statSync");
    const fstat = vi.spyOn(fs, "fstatSync");
    const disk = vi.spyOn(fs, "readFileSync");
    const first = store.read();
    let last = first;
    for (let i = 1; i < 1_000; i++) last = (i % 2 ? alias : store).read();
    expect(first).toEqual(value);
    expect(parse).toHaveBeenCalledTimes(1);
    expect(last).toBe(first);
    expect(fstat).toHaveBeenCalledTimes(1_000);
    expect(stat).not.toHaveBeenCalled();
    expect(disk).toHaveBeenCalledTimes(1);
    const view = first as typeof value;
    expect(() => view.actors.push({ id: "bad", extra: { values: [] } })).toThrow(TypeError);
    expect(() => { view.actors[0]!.extra.values[0] = 99; }).toThrow(TypeError);
    expect(store.read()).toEqual(value);
  });

  it("detects a new inode even with identical size and mtime, then in-place mtime and size changes", () => {
    const { file, value, store } = setup();
    expect(store.read()).toEqual(value);
    const before = fs.statSync(file);
    const replacement = { ...value, actors: [{ id: "other", extra: { values: [3, 4] } }] };
    fs.writeFileSync(`${file}.new`, JSON.stringify(replacement));
    fs.utimesSync(`${file}.new`, before.atime, before.mtime);
    fs.renameSync(`${file}.new`, file);
    expect(store.read()).toEqual(replacement);
    const inPlace = { ...value, actors: [{ id: "again", extra: { values: [5, 6] } }] };
    fs.writeFileSync(file, JSON.stringify(inPlace));
    fs.utimesSync(file, before.atime, new Date(before.mtimeMs + 1_000));
    expect(store.read()).toEqual(inPlace);
    const stamp = fs.statSync(file);
    fs.writeFileSync(file, JSON.stringify(value, null, 2));
    fs.utimesSync(file, stamp.atime, stamp.mtime);
    expect(store.read()).toEqual(value);
  });

  it("binds bytes and identity to the same descriptor across an atomic replacement", () => {
    const { file, value, store } = setup();
    const replacement = { format: 1, actors: [{ id: "new" }] };
    const fstat = fs.fstatSync.bind(fs);
    let replaced = false;
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options: unknown) => {
      const stat = Reflect.apply(fstat, fs, [fd, options]);
      if (!replaced) {
        replaced = true;
        fs.writeFileSync(`${file}.new`, JSON.stringify(replacement));
        fs.renameSync(`${file}.new`, file);
      }
      return stat;
    }) as typeof fs.fstatSync);
    expect(store.read()).toEqual(value);
    expect(store.read()).toEqual(replacement);
  });

  it("keeps complete generations under a concurrent atomic writer and reader", async () => {
    const { file, store } = setup();
    fs.writeFileSync(file, JSON.stringify({ format: 1, epoch: 0, actors: Array.from({ length: 8 }, () => ({ id: "actor", epoch: 0 })) }));
    const worker = new Worker(`
      const fs = require('node:fs');
      const { workerData } = require('node:worker_threads');
      for (let epoch = 1; epoch <= 100; epoch++) {
        fs.writeFileSync(workerData + '.new', JSON.stringify({ format: 1, epoch,
          actors: Array.from({ length: 8 }, () => ({ id: 'actor', epoch })) }));
        fs.renameSync(workerData + '.new', workerData);
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1);
      }
    `, { eval: true, workerData: file });
    let done = false;
    const exit = once(worker, "exit");
    worker.on("exit", () => { done = true; });
    let reads = 0;
    try {
      while (!done) {
        const view = store.read() as { epoch: number; actors: { epoch: number }[] };
        expect(view.actors).toHaveLength(8);
        expect(view.actors.every(row => row.epoch === view.epoch)).toBe(true);
        reads++;
        await new Promise(resolve => setImmediate(resolve));
      }
      expect(await exit).toEqual([0]);
      expect(reads).toBeGreaterThan(1);
      expect((store.read() as { epoch: number }).epoch).toBe(100);
    } finally { await worker.terminate(); }
  });

  it.each(["write", "prepared", "downgrade"] as const)("invalidates all stores after %s even if the identity check returns the previous stamp", async kind => {
    const { root, file, store } = setup();
    const alias = new ActorRegistryStore(root);
    const stamp = fs.statSync(file, { bigint: true });
    const fstat = fs.fstatSync.bind(fs);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) =>
      options?.bigint === true ? stamp : Reflect.apply(fstat, fs, [fd, options])) as typeof fs.fstatSync);
    const before = alias.read();
    if (kind === "write") await store.withLock(() => store.write([{ id: "new" }]));
    else if (kind === "prepared") {
      const prepared = store.prepare([{ id: "new" }]);
      try { await store.withLock(() => prepared.commit()); }
      finally { prepared.dispose(); }
    } else await store.restoreInlineForDowngrade();
    const after = alias.read();
    expect(after).not.toBe(before);
    expect(after).toEqual(JSON.parse(fs.readFileSync(file, "utf8")));
    if (kind !== "downgrade") expect((after as { actors: { id: string }[] }).actors[0]!.id).toBe("new");
  });

  it.skipIf(process.platform === "win32").each(["write", "prepared", "downgrade"] as const)(
    "invalidates the cached view after %s rolls back a post-replace durability failure",
    async kind => {
      const { root, file, value, store } = setup();
      const original = { ...value, format: 2 };
      fs.writeFileSync(file, JSON.stringify(original));
      const alias = new ActorRegistryStore(root);
      const stamp = fs.statSync(file, { bigint: true });
      const fstat = fs.fstatSync.bind(fs);
      vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) =>
        options?.bigint === true ? stamp : Reflect.apply(fstat, fs, [fd, options])) as typeof fs.fstatSync);
      const before = alias.read();
      const descriptors = new Map<number, string>();
      const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
      let replaced = false, failed = false;
      vi.spyOn(fs, "openSync").mockImplementation((name, flags, mode) => {
        const fd = open(name, flags, mode); descriptors.set(fd, String(name)); return fd;
      });
      vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        rename(from, to); if (String(to) === file) replaced = true;
      });
      vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
        if (replaced && !failed && descriptors.get(fd) === root) {
          failed = true; throw new Error("post-replace barrier failed");
        }
        sync(fd);
      });
      const operation = async () => {
        if (kind === "write") await store.withLock(() => store.write([{ id: "new" }], { durable: true }));
        else if (kind === "prepared") {
          const prepared = store.prepare([{ id: "new" }], { durable: true });
          try { await store.withLock(() => prepared.commit()); }
          finally { prepared.dispose(); }
        } else await store.restoreInlineForDowngrade();
      };
      await expect(operation()).rejects.toThrow("post-replace barrier failed");
      expect(failed).toBe(true);
      expect(alias.read()).toEqual(original);
      expect(alias.read()).not.toBe(before); // Forced identical identity makes invalidation observable.
    },
  );

  it("does not return cached success for malformed or missing files and recovers on a later replace", () => {
    const { file, value, store } = setup();
    expect(store.read()).toEqual(value);
    fs.writeFileSync(`${file}.new`, "{bad json");
    fs.renameSync(`${file}.new`, file);
    expect(() => store.read()).toThrow(SyntaxError);
    expect(() => store.read()).toThrow(SyntaxError);
    fs.unlinkSync(file);
    expect(() => store.read()).toThrow();
    fs.writeFileSync(file, JSON.stringify(value));
    expect(store.read()).toEqual(value);
  });
});
