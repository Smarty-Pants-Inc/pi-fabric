// smarty-dev#2014 ABA: the read signal binds namespace digests to state.json's stat stamp
// (dev:ino:size:mtimeMs:ctimeMs). Atomic renames can reuse an inode within one timestamp tick, so
// with equal-size payloads the stamp repeats across real commits. These tests freeze only the
// canonical state.json fs.statSync (never the signal's descriptor stats) across real MeshStore
// commits of equal-size values, and require readers never to stay on the older entries.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";

const WINDOW_MS = RUNTIME_MESH_READ_CACHE_MS;
const SUBS = "topology/subscriptions/";
const DELIVERIES = "residency/deliveries/0123456789abcdef0123456789abcdef/";
const PARTICIPANTS = "topology/participants/";
const writerId: MeshIdentity = { id: "session:writer", name: "writer", kind: "main", sessionId: "writer" };
const real = { statSync: fs.statSync, readFileSync: fs.readFileSync, writeFileSync: fs.writeFileSync, renameSync: fs.renameSync, openSync: fs.openSync, readSync: fs.readSync, closeSync: fs.closeSync };

const scratchBase = fileURLToPath(new URL("../.local/test-scratch/", import.meta.url));
const roots: string[] = [];                             // scratch under .local/, never the system temp
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const base = (file: unknown): string => path.basename(String(file));
const diskEntries = (root: string, prefix: string): MeshStateEntry[] =>
  Object.values((JSON.parse(String(real.readFileSync(path.join(root, "state.json"), "utf8"))) as { entries: Record<string, MeshStateEntry> }).entries)
    .filter((entry) => entry.key.startsWith(prefix)).sort((left, right) => left.key.localeCompare(right.key));
// Full payload reads only: the canonical 64-byte generation peek does not parse state.json.
const stateReads = () => {
  const reads = vi.spyOn(fs, "readFileSync");
  vi.spyOn(fs, "openSync");
  return () => reads.mock.calls.filter(([file]) => base(file) === "state.json").length;
};

/** Seeds control keys first, so every later version has two digits, so every controlled commit keeps state.json's byte size. */
const setup = async (bulkNamespaces = 0) => {
  const root = fs.mkdtempSync(path.join((fs.mkdirSync(scratchBase, { recursive: true }), scratchBase), "mesh-aba-"));
  roots.push(root);
  const now = Date.now.bind(Date);
  let offset = 0;
  vi.spyOn(Date, "now").mockImplementation(() => now() + offset);
  const advance = (milliseconds: number) => { offset += milliseconds; };
  const writer = new MeshStore(root, 64 * 1024, 100, { writeReadJournal: false }); // Repeated-stat signal-only compatibility.
  const reader = new MeshStore(root, 64 * 1024, 100, { readCacheMs: WINDOW_MS });
  await writer.writeBatch({
    identity: writerId,
    ops: [
      ...Array.from({ length: 9 }, (_, index) => ({ kind: "put" as const, key: `control/seen/x${index}`, value: 1 })),
      { kind: "put", key: `${PARTICIPANTS}a`, value: { beat: 0 } },
      { kind: "put", key: `${PARTICIPANTS}b`, value: { beat: 0 } },
      { kind: "put", key: `${SUBS}s1`, value: { source: "one", afterSequence: 3 } },
      { kind: "put", key: `${SUBS}s2`, value: { source: "two", afterSequence: 4 } },
      { kind: "put", key: `${DELIVERIES}d1`, value: { turn: 1 } },
      ...Array.from({ length: bulkNamespaces }, (_, index) => ({ kind: "put" as const, key: `bulk/n${index}/k`, value: 0 })),
    ],
  });
  const size = () => real.statSync(path.join(root, "state.json")).size, beatOf = { beat: 0 };
  /** An unrelated same-size commit by another store (participant heartbeat, one digit), then one 2.2 Hz tick. */
  const heartbeat = async (): Promise<void> => {
    const beat = ++beatOf.beat;
    await writer.put({ key: `${PARTICIPANTS}${beat % 2 ? "b" : "a"}`, value: { beat: beat % 10 }, identity: writerId });
    advance(455);
  };
  /** The subscription namespace commit under test: same size, new version, new field value. */
  const changeSubs = () => writer.put({ key: `${SUBS}s1`, value: { source: "uno", afterSequence: 5 }, identity: writerId });
  return { root, advance, writer, reader, size, heartbeat, changeSubs };
};

/** From now on state.json's stat stamp repeats: the stat taken right now, for every later commit. */
const freezeStateStat = (root: string): void => {
  const file = path.join(root, "state.json");
  const frozen = real.statSync(file);
  vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) =>
    path.resolve(String(target)) === file ? frozen : (real.statSync as (...args: unknown[]) => fs.Stats)(target, ...rest)) as typeof fs.statSync);
};

describe("MeshStore read signal: a repeated state.json stat stamp never pins older entries", () => {
  it("non-fresh narrow listings show a same-size namespace commit after the window", async () => {
    const { root, advance, reader, size, heartbeat, changeSubs } = await setup();
    [SUBS, DELIVERIES].forEach((prefix) => reader.listAll(prefix));
    await heartbeat();
    freezeStateStat(root);
    advance(WINDOW_MS);
    const reads = stateReads();
    expect(reader.listAll(SUBS)).toEqual(diskEntries(root, SUBS));
    expect(reader.listAll(DELIVERIES)).toEqual(diskEntries(root, DELIVERIES));
    expect(reads()).toBe(0);                                  // the signal authorized the cached prefixes
    const bytes = size();
    expect((await changeSubs(), size())).toBe(bytes);
    advance(WINDOW_MS);
    expect(reader.listAll(SUBS)).toEqual(diskEntries(root, SUBS));
    expect(reader.list(SUBS)[0]?.value).toEqual({ source: "uno", afterSequence: 5 });
    expect(reader.listAll(DELIVERIES)).toEqual(diskEntries(root, DELIVERIES));
    for (let tick = 0; tick < 5; tick++) await heartbeat();   // more same-size commits, same stamp
    expect(size()).toBe(bytes);
    advance(WINDOW_MS);
    expect(reader.listAllShared(SUBS)).toEqual(diskEntries(root, SUBS));
  });

  it("fresh narrow listAll, get and stateToken see the current writer's commit immediately", async () => {
    const { root, reader, size, heartbeat, changeSubs } = await setup();
    reader.listAll(SUBS);
    await heartbeat();
    freezeStateStat(root);
    const token = reader.stateToken({ fresh: true });         // parsed and labelled at the repeating stamp
    expect(reader.get(`${SUBS}s1`, { fresh: true })?.value).toEqual({ source: "one", afterSequence: 3 });
    const bytes = size();
    expect((await changeSubs(), size())).toBe(bytes);
    expect(reader.listAll(SUBS, { fresh: true })).toEqual(diskEntries(root, SUBS));
    expect(reader.get(`${SUBS}s1`, { fresh: true })?.value).toEqual({ source: "uno", afterSequence: 5 });
    expect(reader.stateToken({ fresh: true })).not.toBe(token);
    expect(reader.listAll(SUBS)).toEqual(diskEntries(root, SUBS));
  });

  it("a broad stateToken parse that a commit replaces mid-read never labels the older entries as current", async () => {
    const { root, advance, reader, heartbeat, changeSubs } = await setup();
    reader.listAll(SUBS);
    await heartbeat();
    freezeStateStat(root);
    advance(WINDOW_MS);
    // Record the commit's files, roll them back, and replay the commit inside the reader's canonical read.
    const files = () => new Map(fs.readdirSync(root).filter((name) => real.statSync(path.join(root, name)).isFile() && name !== ".lock")
      .map((name) => [name, real.readFileSync(path.join(root, name))] as const));
    const install = (snapshot: Map<string, Buffer>) => {
      const rank = (name: string) => (name === "state.json" ? 0 : name === "state.read-signal.json" ? 2 : 1);
      for (const name of [...snapshot.keys()].sort((left, right) => rank(left) - rank(right))) {
        real.writeFileSync(path.join(root, `${name}.aba.tmp`), snapshot.get(name)!);
        real.renameSync(path.join(root, `${name}.aba.tmp`), path.join(root, name));
      }
    };
    const earlier = files();
    const later = (await changeSubs(), files());
    install(earlier);
    for (const name of later.keys()) if (!earlier.has(name)) fs.rmSync(path.join(root, name));
    let replayed = false;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      const result = (real.readFileSync as (...args: unknown[]) => string | Buffer)(file, ...rest);
      if (!replayed && base(file) === "state.json") { replayed = true; install(later); }
      return result;
    }) as typeof fs.readFileSync);
    const token = reader.stateToken();
    expect(replayed).toBe(true);
    advance(WINDOW_MS);
    expect(reader.listAll(SUBS)).toEqual(diskEntries(root, SUBS));
    expect(reader.get(`${SUBS}s1`)?.value).toEqual({ source: "uno", afterSequence: 5 });
    expect(reader.stateToken()).not.toBe(token);
    expect(reader.listAll(SUBS, { fresh: true })).toEqual(diskEntries(root, SUBS));
  });

  it("costs: equal-size 2.2 Hz unrelated commits read no state.json, and a static store never re-reads the whole signal", async () => {
    const { root, advance, reader, heartbeat } = await setup(1400);
    const signalSize = real.statSync(path.join(root, "state.read-signal.json")).size;
    expect(signalSize > 64 * 1024 && signalSize <= 128 * 1024).toBe(true);   // a large but accepted signal
    reader.listAll(SUBS);
    const reads = stateReads();
    let readerReads = 0;                                      // the writer's own locked reads are not counted
    const readerCall = <T>(read: () => T): T => { const before = reads(); const result = read(); readerReads += reads() - before; return result; };
    for (let tick = 0; tick < 22; tick++) {
      await heartbeat();
      expect(readerCall(() => reader.listAll(SUBS))).toEqual(diskEntries(root, SUBS));
    }
    expect(readerReads).toBe(0);
    const signalDescriptors = new Set<number>();
    let signalBytes = 0;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      const descriptor = (real.openSync as (...args: unknown[]) => number)(file, ...rest);
      if (base(file) === "state.read-signal.json") signalDescriptors.add(descriptor);
      return descriptor;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "readSync").mockImplementation(((descriptor: number, ...rest: unknown[]) => {
      const count = (real.readSync as unknown as (...args: unknown[]) => number)(descriptor, ...rest);
      if (signalDescriptors.has(descriptor)) signalBytes += count;
      return count;
    }) as typeof fs.readSync);
    vi.spyOn(fs, "closeSync").mockImplementation(((descriptor: number) => (signalDescriptors.delete(descriptor), real.closeSync(descriptor))) as typeof fs.closeSync);
    const polls = 40;                                         // 10 s at 250 ms, no writer at all
    for (let poll = 0; poll < polls; poll++) {
      advance(250);
      expect(readerCall(() => reader.listAll(SUBS))).toEqual(diskEntries(root, SUBS));
    }
    expect(readerReads).toBe(0);
    expect(signalBytes).toBeLessThanOrEqual(polls * 4096);    // a cheap header per poll at most
  });

  it("arbitrary empty complete prefixes do not grow an unbounded reuse memo", async () => {
    const { advance, reader, heartbeat } = await setup();
    reader.listAll(SUBS);
    await heartbeat();
    advance(WINDOW_MS);
    const reads = stateReads();
    for (let index = 0; index < 2048; index++) expect(reader.listAll(`empty/p${index}/`)).toEqual([]);
    expect(reads()).toBe(0);
    const opens = vi.spyOn(fs, "openSync");
    const io = () => opens.mock.calls.filter(([file]) => base(file) === "state.read-signal.json").length + reads();
    const before = io();
    expect(reader.listAll("empty/p0/")).toEqual([]); // Evicted selection can reuse the metadata-gated signal index.
    expect(io() - before).toBeLessThanOrEqual(1);
  });
});
