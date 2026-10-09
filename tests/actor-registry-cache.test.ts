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
  // Hit/LRU/invalidation assertions need a proven, non-racy generation.
  const old = new Date(Date.now() - 5_000);
  fs.utimesSync(file, old, old);
  const fd = fs.openSync(file, "r");
  let cacheable: boolean;
  try {
    const stat = fs.fstatSync(fd, { bigint: true });
    cacheable = stat.ino > 0n && stat.mtimeNs > 0n && stat.ctimeNs > 0n;
  } finally { fs.closeSync(fd); }
  return { root, file, value, cacheable, store: new ActorRegistryStore(root) };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("ActorRegistryStore cached read (#7791)", () => {
  it("bounds decoded generations to 64 paths and promotes hits before LRU eviction", () => {
    const fixtures = Array.from({ length: 65 }, () => setup());
    const views = fixtures.slice(0, 64).map(({ store }) => store.read());
    const promoted = fixtures[0]!.store.read();
    if (fixtures[0]!.cacheable) expect(promoted).toBe(views[0]); // Most recently used, not oldest inserted.
    else expect(promoted).not.toBe(views[0]); // Unproven identity always re-reads.
    fixtures[64]!.store.read();
    const parse = vi.spyOn(JSON, "parse");
    const retained = fixtures[0]!.store.read();
    if (fixtures[0]!.cacheable) expect(retained).toBe(views[0]);
    else expect(retained).not.toBe(views[0]);
    const misses = fixtures[0]!.cacheable ? 0 : 1;
    expect(parse).toHaveBeenCalledTimes(misses);
    expect(fixtures[1]!.store.read()).not.toBe(views[1]);
    expect(parse).toHaveBeenCalledTimes(misses + 1);
    // Reading the evicted path inserts it and evicts the next least recent path.
    expect(fixtures[2]!.store.read()).not.toBe(views[2]);
    expect(parse).toHaveBeenCalledTimes(misses + 2);
  });

  it("explicit release evicts the shared normalized path but preserves other roots", () => {
    const { root, store } = setup();
    const alias = new ActorRegistryStore(path.join(root, "."));
    const { store: other, cacheable } = setup();
    const before = store.read(), retained = other.read();
    alias.releaseReadCache();
    const parse = vi.spyOn(JSON, "parse");
    expect(store.read()).not.toBe(before);
    expect(parse).toHaveBeenCalledTimes(1);
    const otherView = other.read();
    if (cacheable) expect(otherView).toBe(retained);
    else expect(otherView).not.toBe(retained);
    expect(parse).toHaveBeenCalledTimes(cacheable ? 1 : 2);
  });


  it("shares proven generations across 1,000 alias reads, but re-reads unproven identities", () => {
    const { root, value, cacheable, store } = setup();
    const alias = new ActorRegistryStore(path.join(root, "."));
    const parse = vi.spyOn(JSON, "parse");
    const stat = vi.spyOn(fs, "statSync");
    const fstat = vi.spyOn(fs, "fstatSync");
    const disk = vi.spyOn(fs, "readFileSync");
    const first = store.read();
    let last = first;
    for (let i = 1; i < 1_000; i++) last = (i % 2 ? alias : store).read();
    expect(first).toEqual(value);
    expect(parse).toHaveBeenCalledTimes(cacheable ? 1 : 1_000);
    if (cacheable) expect(last).toBe(first);
    else expect(last).not.toBe(first);
    expect(fstat).toHaveBeenCalledTimes(1_000);
    expect(stat).not.toHaveBeenCalled();
    expect(disk).toHaveBeenCalledTimes(cacheable ? 1 : 1_000);
    const view = first as typeof value;
    expect(() => view.actors.push({ id: "bad", extra: { values: [] } })).toThrow(TypeError);
    expect(() => { view.actors[0]!.extra.values[0] = 99; }).toThrow(TypeError);
    expect(store.read()).toEqual(value);
  });

  it.each([1_000, 2_000])("re-reads same-size in-place updates within a coarse %i ms timestamp quantum", quantumMs => {
    const { file, value, store } = setup();
    const replacement = { ...value, actors: [{ id: "other", extra: { values: [3, 4] } }] };
    const bytes = JSON.stringify(replacement);
    expect(bytes).toHaveLength(JSON.stringify(value).length);
    const quantumStartMs = 1_700_000_000_000;
    let now = quantumStartMs + 100, writtenAt = now;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const nativeFstat = fs.fstatSync.bind(fs);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options: unknown) => {
      const stampNs = BigInt(Math.floor(writtenAt / quantumMs) * quantumMs) * 1_000_000n;
      return Object.assign(Reflect.apply(nativeFstat, fs, [fd, options]), {
        ino: 1n, mtimeNs: stampNs, ctimeNs: stampNs,
      });
    }) as typeof fs.fstatSync);
    const parse = vi.spyOn(JSON, "parse"), disk = vi.spyOn(fs, "readFileSync");
    const before = store.read();
    expect(before).toEqual(value);
    now += 100; writtenAt = now;
    fs.writeFileSync(file, bytes); // Same inode, size, mtime and ctime quantum.
    const after = store.read();
    expect(after).toEqual(replacement);
    expect(after).not.toBe(before);
    expect(Object.isFrozen(after)).toBe(true);
    expect(parse).toHaveBeenCalledTimes(2);
    expect(disk).toHaveBeenCalledTimes(2);
    fs.writeFileSync(file, "!".repeat(bytes.length)); // Same-size malformed collision must re-validate.
    expect(() => store.read()).toThrow(SyntaxError);
    fs.writeFileSync(file, bytes);
    const racy = store.read();
    expect(racy).toEqual(replacement);
    now = quantumStartMs + 3_000;
    const mature = store.read(); // Time alone cannot prove the previously racy cached bytes.
    expect(mature).toEqual(replacement);
    expect(mature).not.toBe(racy);
    expect(parse).toHaveBeenCalledTimes(5);
    expect(disk).toHaveBeenCalledTimes(5);
    expect(store.read()).toBe(mature); // Re-read after the racy window proves this generation.
    expect(parse).toHaveBeenCalledTimes(5);
    expect(disk).toHaveBeenCalledTimes(5);
  });

  it.each([1_000, 2_000])("hits an untouched coarse %i ms generation older than two seconds", quantumMs => {
    const { value, store } = setup();
    const now = 1_700_000_004_001;
    vi.spyOn(Date, "now").mockReturnValue(now);
    const stampNs = BigInt(Math.floor((now - 2_001) / quantumMs) * quantumMs) * 1_000_000n;
    const nativeFstat = fs.fstatSync.bind(fs);
    const fstat = vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options: unknown) =>
      Object.assign(Reflect.apply(nativeFstat, fs, [fd, options]), {
        ino: 1n, mtimeNs: stampNs, ctimeNs: stampNs,
      })) as typeof fs.fstatSync);
    const parse = vi.spyOn(JSON, "parse"), disk = vi.spyOn(fs, "readFileSync");
    const first = store.read();
    expect(first).toEqual(value);
    for (let i = 1; i < 100; i++) expect(store.read()).toBe(first);
    expect(fstat).toHaveBeenCalledTimes(100);
    expect(parse).toHaveBeenCalledTimes(1);
    expect(disk).toHaveBeenCalledTimes(1);
  });

  it.each([-500, 0, 1_999, 2_000])("re-reads a generation whose mtime is %i ms before its read (including future and boundary)", ageMs => {
    const { store } = setup();
    const mtimeMs = 1_700_000_000_000;
    vi.spyOn(Date, "now").mockReturnValue(mtimeMs + ageMs);
    const nativeFstat = fs.fstatSync.bind(fs);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options: unknown) =>
      Object.assign(Reflect.apply(nativeFstat, fs, [fd, options]), {
        ino: 1n, mtimeNs: BigInt(mtimeMs) * 1_000_000n,
      })) as typeof fs.fstatSync);
    const parse = vi.spyOn(JSON, "parse"), disk = vi.spyOn(fs, "readFileSync");
    const first = store.read(), second = store.read();
    expect(second).toEqual(first);
    expect(second).not.toBe(first);
    expect(parse).toHaveBeenCalledTimes(2);
    expect(disk).toHaveBeenCalledTimes(2);
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

  it.each([false, true])("binds bytes and identity to the same descriptor across an atomic replacement (warm=%s)", warm => {
    const { file, value, cacheable, store } = setup();
    const replacement = { ...value, actors: [{ id: "other", extra: { values: [3, 4] } }] };
    expect(JSON.stringify(replacement)).toHaveLength(JSON.stringify(value).length);
    if (warm) store.read(); // An existing cached generation must not mask the replacement.
    const disk = vi.spyOn(fs, "readFileSync");
    const fstat = fs.fstatSync.bind(fs);
    let replaced = false, deferred = false;
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options: unknown) => {
      const stat = Reflect.apply(fstat, fs, [fd, options]);
      if (!replaced) {
        replaced = true;
        fs.writeFileSync(`${file}.new`, JSON.stringify(replacement));
        try { fs.renameSync(`${file}.new`, file); }
        catch (error) {
          // Windows can refuse rename-over-open-file. Publish only after read closes
          // its descriptor; do not retry synchronously while that handle is held.
          expect(process.platform).toBe("win32");
          expect(["EPERM", "EACCES", "EEXIST", "EBUSY"]).toContain((error as NodeJS.ErrnoException).code);
          deferred = true;
        }
      }
      return stat;
    }) as typeof fs.fstatSync);
    const during = store.read();
    if (process.platform === "win32") expect([value, replacement]).toContainEqual(during);
    else expect(during).toEqual(value); // POSIX keeps the old inode readable.
    if (!cacheable) expect(disk).toHaveBeenCalledTimes(1);
    if (deferred) fs.renameSync(`${file}.new`, file);
    expect(store.read()).toEqual(replacement);
    if (!cacheable) expect(disk).toHaveBeenCalledTimes(2);
  });

  it("keeps complete generations under a concurrent atomic writer and reader", async () => {
    const { file, cacheable, store } = setup();
    fs.writeFileSync(file, JSON.stringify({ format: 1, epoch: 0, actors: Array.from({ length: 8 }, () => ({ id: "actor", epoch: 0 })) }));
    const worker = new Worker(`
      const fs = require('node:fs');
      const { workerData } = require('node:worker_threads');
      const wait = new Int32Array(new SharedArrayBuffer(4));
      for (let epoch = 1; epoch <= 100; epoch++) {
        fs.writeFileSync(workerData + '.new', JSON.stringify({ format: 1, epoch,
          actors: Array.from({ length: 8 }, () => ({ id: 'actor', epoch })) }));
        for (let attempt = 0; ; attempt++) {
          try { fs.renameSync(workerData + '.new', workerData); break; }
          catch (error) {
            if (process.platform !== 'win32' || attempt >= 200 ||
                !['EPERM', 'EACCES', 'EEXIST', 'EBUSY'].includes(error.code)) throw error;
            Atomics.wait(wait, 0, 0, 1);
          }
        }
        Atomics.wait(wait, 0, 0, 1);
      }
    `, { eval: true, workerData: file });
    let done = false;
    const exit = once(worker, "exit");
    worker.on("exit", () => { done = true; });
    let reads = 0, previous: { epoch: number; actors: { epoch: number }[] } | undefined;
    const disk = vi.spyOn(fs, "readFileSync");
    try {
      while (!done) {
        let view: typeof previous;
        try { view = store.read() as NonNullable<typeof previous>; }
        catch (error) {
          // A Windows sharing violation is an error, never cached success.
          expect(process.platform).toBe("win32");
          expect(["EPERM", "EACCES", "EBUSY"]).toContain((error as NodeJS.ErrnoException).code);
        }
        if (view) {
          expect(view.actors).toHaveLength(8);
          expect(view.actors.every(row => row.epoch === view.epoch)).toBe(true);
          expect(view.epoch).toBeGreaterThanOrEqual(previous?.epoch ?? 0);
          if (!cacheable && previous) expect(view).not.toBe(previous);
          previous = view;
          reads++;
        }
        await new Promise(resolve => setImmediate(resolve));
      }
      expect(await exit).toEqual([0]);
      expect(reads).toBeGreaterThan(1);
      expect((store.read() as { epoch: number }).epoch).toBe(100);
      if (!cacheable) expect(disk.mock.calls.filter(([name]) => typeof name === "number").length).toBeGreaterThanOrEqual(reads + 1);
    } finally { await worker.terminate(); }
  });

  it("includes descriptor ctime in a generation with unchanged inode, size and mtime", () => {
    const { file, value, store } = setup();
    const replacement = { ...value, actors: [{ id: "other", extra: { values: [3, 4] } }] };
    expect(JSON.stringify(replacement)).toHaveLength(JSON.stringify(value).length);
    const fstat = fs.fstatSync.bind(fs);
    let ctimeNs = 1n;
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options: unknown) =>
      Object.assign(Reflect.apply(fstat, fs, [fd, options]), { ino: 1n, mtimeNs: 1n, ctimeNs })) as typeof fs.fstatSync);
    const before = store.read();
    expect(store.read()).toBe(before);
    fs.writeFileSync(file, JSON.stringify(replacement));
    ctimeNs = 2n;
    expect(store.read()).toEqual(replacement);
    expect(store.read()).not.toBe(before);
  });

  it.each(["native", "win32"] as const)("re-reads zero-ID descriptors with colliding size and timestamps (%s)", platform => {
    const { file, value, store } = setup();
    const replacement = { ...value, actors: [{ id: "other", extra: { values: [3, 4] } }] };
    expect(JSON.stringify(replacement)).toHaveLength(JSON.stringify(value).length);
    const cached = store.read();
    const stamp = fs.statSync(file, { bigint: true });
    const fstat = fs.fstatSync.bind(fs);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options: unknown) =>
      Object.assign(Reflect.apply(fstat, fs, [fd, options]), {
        dev: 0n, ino: 0n, size: stamp.size, mtimeNs: stamp.mtimeNs, ctimeNs: stamp.ctimeNs,
      })) as typeof fs.fstatSync);
    const descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
    try {
      if (platform === "win32") Object.defineProperty(process, "platform", { ...descriptor, value: "win32" });
      const parse = vi.spyOn(JSON, "parse"), disk = vi.spyOn(fs, "readFileSync");
      const before = store.read();
      expect(before).toEqual(value);
      expect(before).not.toBe(cached);
      fs.writeFileSync(`${file}.new`, JSON.stringify(replacement));
      fs.renameSync(`${file}.new`, file);
      const after = store.read();
      expect(after).toEqual(replacement);
      expect(store.read()).toEqual(replacement);
      expect(store.read()).not.toBe(after);
      expect(parse).toHaveBeenCalledTimes(4);
      expect(disk).toHaveBeenCalledTimes(4);
    } finally { Object.defineProperty(process, "platform", descriptor); }
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
