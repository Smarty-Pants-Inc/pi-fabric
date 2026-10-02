// smarty-dev#2355 / #2014 S2: a commit whose optional read hint (state.read-signal.json) fails to
// publish must still be visible. The canonical state.json stat tuple is frozen (equal-size commits,
// coarse tick, inode reuse), so metadata can never reveal the commit; all writes go through the
// locked MeshStore API. A canonical 64-byte header read is cheap; a "full read" is readFileSync of
// state.json or a readSync of more than 256 bytes on a state.json descriptor.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";

const WINDOW_MS = RUNTIME_MESH_READ_CACHE_MS;
const SUBS = "topology/subscriptions/";
const DELIVERIES = "residency/deliveries/0123456789abcdef0123456789abcdef/";
const PARTICIPANTS = "topology/participants/";
const HINT = "state.read-signal.json";
const writerId: MeshIdentity = { id: "session:writer", name: "writer", kind: "main", sessionId: "writer" };
const real = {
  statSync: fs.statSync, readFileSync: fs.readFileSync, writeFileSync: fs.writeFileSync, renameSync: fs.renameSync,
  openSync: fs.openSync, readSync: fs.readSync, closeSync: fs.closeSync,
};
const scratchBase = fileURLToPath(new URL("../.local/test-scratch/", import.meta.url));
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const base = (file: unknown): string => path.basename(String(file));
const disk = (root: string, prefix: string): MeshStateEntry[] =>
  Object.values((JSON.parse(String(real.readFileSync(path.join(root, "state.json"), "utf8"))) as { entries: Record<string, MeshStateEntry> }).entries)
    .filter((entry) => entry.key.startsWith(prefix)).sort((left, right) => left.key.localeCompare(right.key));

type Fault = "eio" | "throw" | "sizecap" | "unwritable";

/** Real stores over .local scratch. Control keys first keep every later version two digits, so sizes stay equal. */
const setup = async (bulk = 0) => {
  const root = fs.mkdtempSync(path.join((fs.mkdirSync(scratchBase, { recursive: true }), scratchBase), "mesh-gen-"));
  roots.push(root);
  const now = Date.now.bind(Date);
  let offset = 0;
  vi.spyOn(Date, "now").mockImplementation(() => now() + offset);
  const advance = (ms: number) => { offset += ms; };
  const writer = new MeshStore(root, 64 * 1024, 100);
  const cached = () => new MeshStore(root, 64 * 1024, 100, { readCacheMs: WINDOW_MS });
  await writer.writeBatch({
    identity: writerId,
    ops: [
      ...Array.from({ length: 9 }, (_, index) => ({ kind: "put" as const, key: `control/seen/x${index}`, value: 1 })),
      { kind: "put", key: `${PARTICIPANTS}a`, value: { beat: 0 } },
      { kind: "put", key: `${SUBS}s1`, value: 0 },
      { kind: "put", key: `${DELIVERIES}d1`, value: 0 },
      ...Array.from({ length: bulk }, (_, index) => ({ kind: "put" as const, key: `bulk/n${index}/k`, value: 0 })),
    ],
  });
  await writer.put({ key: `${PARTICIPANTS}a`, value: { beat: 1 }, identity: writerId });
  const size = () => real.statSync(path.join(root, "state.json")).size;
  let beat = 1;
  const heartbeat = () => writer.put({ key: `${PARTICIPANTS}a`, value: { beat: ++beat % 10 }, identity: writerId });
  /** A locked commit of `value` to the subscription and to the delivery: each put must succeed. */
  const commit = async (value: number) => {
    await expect(writer.put({ key: `${SUBS}s1`, value, identity: writerId })).resolves.toBeDefined();
    await expect(writer.put({ key: `${DELIVERIES}d1`, value, identity: writerId })).resolves.toBeDefined();
  };
  return { root, advance, writer, cached, size, heartbeat, commit };
};

/** Freezes only state.json's fs.statSync tuple, faults only renames onto the hint, counts full canonical reads. */
const instrument = (root: string) => {
  const state = path.join(root, "state.json");
  const frozen = real.statSync(state);
  const control = { fault: undefined as Fault | undefined, fullReads: 0, afterFullRead: undefined as (() => void) | undefined };
  const stateFds = new Set<number>();
  const full = () => { control.fullReads++; const hook = control.afterFullRead; control.afterFullRead = undefined; hook?.(); };
  vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) =>
    path.resolve(String(target)) === state ? frozen : (real.statSync as (...args: unknown[]) => fs.Stats)(target, ...rest)) as typeof fs.statSync);
  vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
    if (base(to) === HINT && (control.fault === "eio" || control.fault === "throw")) {
      throw control.fault === "eio" ? Object.assign(new Error("EIO: i/o error, rename"), { code: "EIO" }) : new Error("crash before hint");
    }
    return real.renameSync(from, to);
  }) as typeof fs.renameSync);
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const fd = (real.openSync as (...args: unknown[]) => number)(file, ...rest);
    if (base(file) === "state.json") stateFds.add(fd);
    return fd;
  }) as typeof fs.openSync);
  vi.spyOn(fs, "closeSync").mockImplementation(((fd: number) => (stateFds.delete(fd), real.closeSync(fd))) as typeof fs.closeSync);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
    const result = (real.readFileSync as (...args: unknown[]) => string | Buffer)(file, ...rest);
    if (typeof file === "number" ? stateFds.has(file) : base(file) === "state.json") full();
    return result;
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "readSync").mockImplementation(((fd: number, buffer: NodeJS.ArrayBufferView, ...rest: unknown[]) => {
    const count = (real.readSync as unknown as (...args: unknown[]) => number)(fd, buffer, ...rest);
    const length = typeof rest[1] === "number" ? rest[1] : (rest[0] as { length?: number } | undefined)?.length ?? buffer.byteLength;
    if (stateFds.has(fd) && length > 256) full();
    return count;
  }) as typeof fs.readSync);
  /** Full canonical reads made by `read` alone (the writer's locked reads are excluded). */
  const cost = <T>(read: () => T): [T, number] => { const before = control.fullReads; const value = read(); return [value, control.fullReads - before]; };
  return { control, cost };
};

const faultOn = (root: string, control: { fault: Fault | undefined }, fault: Fault) => {
  control.fault = fault;
  if (fault === "unwritable") { fs.rmSync(path.join(root, HINT), { force: true }); fs.mkdirSync(path.join(root, HINT)); }
};
const faultOff = (root: string, control: { fault: Fault | undefined }) => {
  if (control.fault === "unwritable") fs.rmSync(path.join(root, HINT), { recursive: true, force: true });
  control.fault = undefined;
};

describe("MeshStore primary generation: a failed optional hint never hides a committed write", () => {
  it.each<Fault>(["eio", "throw", "sizecap", "unwritable"])("hint %s: fresh reads and expired narrow listings see each commit", async (fault) => {
    const { root, advance, cached, size, commit } = await setup(fault === "sizecap" ? 2600 : 0);
    if (fault === "sizecap") expect(fs.existsSync(path.join(root, HINT))).toBe(false);   // the index exceeds its cap
    const fresh = cached(), listing = cached();
    const token = fresh.stateToken({ fresh: true });
    expect(fresh.get(`${SUBS}s1`, { fresh: true })?.value).toBe(0);
    [SUBS, DELIVERIES].forEach((prefix) => expect(listing.listAll(prefix)[0]?.value).toBe(0));
    const bytes = size();
    const { control } = instrument(root);
    faultOn(root, control, fault);
    const hintBefore = fs.existsSync(path.join(root, HINT)) && fs.statSync(path.join(root, HINT)).isFile() ? real.readFileSync(path.join(root, HINT), "utf8") : undefined;
    for (const value of [9, 8]) {
      await commit(value);
      expect(size()).toBe(bytes);                                  // equal sizes: metadata cannot tell
      if (hintBefore !== undefined) expect(real.readFileSync(path.join(root, HINT), "utf8")).toBe(hintBefore);   // old hint left
      expect(disk(root, SUBS)[0]?.value).toBe(value);
      expect(fresh.get(`${SUBS}s1`, { fresh: true })?.value).toBe(value);
      expect(fresh.listAll(SUBS, { fresh: true })).toEqual(disk(root, SUBS));
      expect(fresh.listAll(DELIVERIES, { fresh: true })).toEqual(disk(root, DELIVERIES));
      expect(fresh.stateToken({ fresh: true })).not.toBe(token);
      advance(WINDOW_MS + 1);
      expect(listing.listAll(SUBS)).toEqual(disk(root, SUBS));    // subscription: no lost discovery
      expect(listing.listAll(DELIVERIES)).toEqual(disk(root, DELIVERIES));   // residency: no lost delivery
    }
    faultOff(root, control);
    await commit(7);                                               // counterexample: hint publishes again
    expect(size()).toBe(bytes);
    expect(fresh.get(`${SUBS}s1`, { fresh: true })?.value).toBe(7);
    advance(WINDOW_MS + 1);
    expect(listing.listAll(SUBS)[0]?.value).toBe(7);
    expect(listing.listAll(DELIVERIES)[0]?.value).toBe(7);
  });

  it("counterexample: after a published commit, an unchanged namespace costs 0 full state reads across unrelated writes", async () => {
    const { root, advance, cached, size, heartbeat, commit } = await setup();
    const listing = cached();
    [SUBS, DELIVERIES].forEach((prefix) => listing.listAll(prefix));
    const bytes = size();
    const { cost } = instrument(root);
    await commit(7);
    advance(WINDOW_MS + 1);
    expect(listing.listAll(SUBS)[0]?.value).toBe(7);               // the commit is seen (a full read is fine)
    expect(listing.listAll(DELIVERIES)[0]?.value).toBe(7);
    let reads = 0;
    for (let tick = 0; tick < 5; tick++) {
      await heartbeat();
      expect(size()).toBe(bytes);
      advance(WINDOW_MS + 1);
      for (const prefix of [SUBS, DELIVERIES]) {
        const [entries, count] = cost(() => listing.listAll(prefix));
        expect(entries).toEqual(disk(root, prefix));
        reads += count;
      }
    }
    expect(reads).toBe(0);                                         // a 64-byte header is not a full read
  });

  it("a canonical replace mid-parse with a failed hint never labels the older payload as the new generation", async () => {
    const { root, advance, cached, commit } = await setup();
    const { control } = instrument(root);
    const files = () => new Map(fs.readdirSync(root).filter((name) => name !== ".lock" && real.statSync(path.join(root, name)).isFile())
      .map((name) => [name, real.readFileSync(path.join(root, name))] as const));
    const install = (snapshot: Map<string, Buffer>) => {
      const rank = (name: string) => (name === "state.json" ? 0 : name === HINT ? 2 : 1);   // primary before hint
      for (const name of [...snapshot.keys()].sort((left, right) => rank(left) - rank(right))) {
        real.writeFileSync(path.join(root, `${name}.gen.tmp`), snapshot.get(name)!);
        real.renameSync(path.join(root, `${name}.gen.tmp`), path.join(root, name));
      }
    };
    const earlier = files();
    faultOn(root, control, "eio");
    await commit(9);
    const later = files();
    faultOff(root, control);
    expect(later.get(HINT)).toEqual(earlier.get(HINT));           // the hint still names the older commit
    install(earlier);
    for (const name of later.keys()) if (!earlier.has(name)) fs.rmSync(path.join(root, name));
    const reader = cached();
    let replayed = false;
    control.afterFullRead = () => { replayed = true; install(later); };   // commit 9 lands during the parse
    const token = reader.stateToken({ fresh: true });
    expect(replayed).toBe(true);
    expect(reader.get(`${SUBS}s1`, { fresh: true })?.value).toBe(9);
    expect(reader.listAll(DELIVERIES, { fresh: true })).toEqual(disk(root, DELIVERIES));
    expect(reader.stateToken({ fresh: true })).not.toBe(token);
    advance(WINDOW_MS + 1);
    expect(reader.listAll(SUBS)).toEqual(disk(root, SUBS));
  });
});
