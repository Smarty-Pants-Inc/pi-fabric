// smarty-dev#2014: a small state.read-signal.json sidecar, written after each MeshStore commit, lets
// a non-fresh narrow listing (a prefix with two complete components, such as the lifecycle
// "topology/subscriptions/" or a resident "residency/deliveries/<root>/") skip the canonical
// re-parse after an unrelated write (participant heartbeats at ~2.2 Hz) once the read-cache
// window expires, as long as the signal names the exact canonical file and the namespace
// digest is unchanged. Black-box: these tests observe only public MeshStore methods, the
// filesystem, and fs call counts; they do not depend on the signal's format.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  MeshStore,
  RUNTIME_MESH_READ_CACHE_MS,
  type MeshIdentity,
  type MeshStateEntry,
  type MeshStoreOptions,
} from "../src/mesh/store.js";

const WINDOW_MS = RUNTIME_MESH_READ_CACHE_MS;           // 2 s
const HEARTBEAT_TICK_MS = 455;                          // ~2.2 Hz of other-store writes
const SUBS = "topology/subscriptions/";
const DELIVERIES = "residency/deliveries/0123456789abcdef0123456789abcdef/";
const PARTICIPANTS = "topology/participants/";
const SIGNAL = "state.read-signal.json";

const writerId: MeshIdentity = { id: "session:writer", name: "writer", kind: "main", sessionId: "writer" };
const otherId: MeshIdentity = { id: "session:other", name: "other", kind: "main", sessionId: "other" };

// Scratch under the repo's .local/, never the system temp; each test removes its own root.
const scratchBase = fileURLToPath(new URL("../.local/test-scratch/", import.meta.url));
const roots: string[] = [];
const newRoot = (): string => {
  fs.mkdirSync(scratchBase, { recursive: true });
  const root = fs.mkdtempSync(path.join(scratchBase, "mesh-read-signal-"));
  roots.push(root);
  return root;
};
const storeAt = (root: string, options?: MeshStoreOptions): MeshStore => new MeshStore(root, 64 * 1024, 100, { writeReadJournal: false, ...options }); // Exercise pre-journal/mixed-fleet signal fallback.

// Real fs functions, captured before any spy, for the test's own direct file work.
const real = {
  readFileSync: fs.readFileSync,
  writeFileSync: fs.writeFileSync,
  renameSync: fs.renameSync,
  openSync: fs.openSync,
  readSync: fs.readSync,
  fstatSync: fs.fstatSync,
  closeSync: fs.closeSync,
  truncateSync: fs.truncateSync,
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

/** Moves Date.now forward without sleeping, to expire read-cache windows deterministically. */
const controllableClock = () => {
  const now = Date.now.bind(Date);
  let offset = 0;
  vi.spyOn(Date, "now").mockImplementation(() => now() + offset);
  return (milliseconds: number) => { offset += milliseconds; };
};

const isStateFile = (file: unknown): boolean => path.basename(String(file)) === "state.json";
const isSignalFile = (file: unknown): boolean => path.basename(String(file)) === SIGNAL;

/** Full canonical JSON reads through readState; a bounded generation-header peek is not a parse. */
const spyStateReads = () => {
  const reads = vi.spyOn(fs, "readFileSync");
  vi.spyOn(fs, "openSync"); // Some descriptor-fault cases restore this spy after each prefix.
  return () => reads.mock.calls.filter(([file]) => isStateFile(file)).length;
};

type ReadSync = (descriptor: number, ...rest: unknown[]) => number;
/**
 * The reader takes the signal through a descriptor (fs.openSync, fs.fstatSync, fs.readSync), not
 * by path. This follows the descriptors opened on the signal file (until closed) and lets a test
 * act inside fs.readSync / fs.fstatSync on them: race a canonical replace, or inject a fault.
 */
const onSignalDescriptor = (hooks: {
  read?: (descriptor: number, delegate: () => number, rest: unknown[]) => number;
  fstat?: (descriptor: number, delegate: () => fs.Stats) => fs.Stats;
}) => {
  const open = new Set<number>();
  let reads = 0;
  let fstats = 0;
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const descriptor = (real.openSync as (...args: unknown[]) => number)(file, ...rest);
    if (isSignalFile(file)) open.add(descriptor);
    return descriptor;
  }) as typeof fs.openSync);
  vi.spyOn(fs, "closeSync").mockImplementation(((descriptor: number) => {
    open.delete(descriptor);
    real.closeSync(descriptor);
  }) as typeof fs.closeSync);
  vi.spyOn(fs, "readSync").mockImplementation(((descriptor: number, ...rest: unknown[]) => {
    const delegate = () => (real.readSync as unknown as ReadSync)(descriptor, ...rest);
    if (!open.has(descriptor) || !hooks.read) return delegate();
    reads += 1;
    return hooks.read(descriptor, delegate, rest);
  }) as typeof fs.readSync);
  vi.spyOn(fs, "fstatSync").mockImplementation(((descriptor: number, ...rest: unknown[]) => {
    const delegate = () => (real.fstatSync as (...args: unknown[]) => fs.Stats)(descriptor, ...rest);
    if (!open.has(descriptor) || !hooks.fstat) return delegate();
    fstats += 1;
    return hooks.fstat(descriptor, delegate);
  }) as typeof fs.fstatSync);
  return { reads: () => reads, fstats: () => fstats };
};

/** Reads the canonical file's current entries straight from disk, independent of any store cache. */
const diskEntries = (root: string, prefix: string): MeshStateEntry[] => {
  const state = JSON.parse(String(real.readFileSync(path.join(root, "state.json"), "utf8"))) as {
    entries: Record<string, MeshStateEntry>;
  };
  return Object.values(state.entries)
    .filter((entry) => entry.key.startsWith(prefix))
    .sort((left, right) => left.key.localeCompare(right.key));
};

/**
 * An older Fabric build (no signal support): read state.json, change it, and replace it with a
 * temp-file rename. It never touches the signal, so the signal keeps naming the previous file.
 */
const oldWriterRewrite = (root: string, mutate: (state: {
  entries: Record<string, MeshStateEntry>;
  versions?: Record<string, number>;
  highWater?: number;
}) => void): void => {
  const file = path.join(root, "state.json");
  const state = JSON.parse(String(real.readFileSync(file, "utf8")));
  mutate(state);
  const temporary = `${file}.old-writer.tmp`;
  real.writeFileSync(temporary, JSON.stringify(state));
  real.renameSync(temporary, file);
};
const oldWriterPut = (key: string, value: unknown) => (state: {
  entries: Record<string, MeshStateEntry>;
  versions?: Record<string, number>;
  highWater?: number;
}): void => {
  const clock = Math.max(state.highWater ?? 0, ...Object.values(state.versions ?? {}),
    ...Object.values(state.entries).map((entry) => entry.version));
  const version = clock + 1;
  state.entries[key] = { key, value, version, updatedAt: Date.now(), updatedBy: otherId };
  state.versions = { ...(state.versions ?? {}), [key]: version };
  state.highWater = version;
};

/** The signal's bytes, or "" when there is none (so fallback checks also run where no signal is written). */
const signalText = (root: string): string => {
  try {
    return String(real.readFileSync(path.join(root, SIGNAL), "utf8"));
  } catch {
    return "";
  }
};

const keys = (entries: readonly Readonly<MeshStateEntry>[]): string[] => entries.map((entry) => entry.key);
const plain = (entries: readonly Readonly<MeshStateEntry>[]): MeshStateEntry[] => JSON.parse(JSON.stringify(entries));

/** Every non-fresh narrow read of one prefix, checked against the expected entries; returns them. */
const narrowReads = (store: MeshStore, prefix: string): MeshStateEntry[] => {
  const all = store.listAll(prefix);
  expect(plain(store.listAllShared(prefix))).toEqual(all);
  expect(store.list(prefix, 100)).toEqual(all);
  return all;
};

/** A reader with the runtime window and a writer that stands for other hosts, with seeded state. */
const setup = async (options: { subscriptions: boolean; readCacheMs?: number }) => {
  const root = newRoot();
  const advance = controllableClock();
  const writer = storeAt(root);
  const reader = storeAt(root, { readCacheMs: options.readCacheMs ?? WINDOW_MS });
  await writer.writeBatch({
    identity: writerId,
    ops: [
      { kind: "put", key: `${PARTICIPANTS}a`, value: { beat: 0 } },
      { kind: "put", key: `${PARTICIPANTS}b`, value: { beat: 0 } },
      { kind: "put", key: "control/seen/x", value: 1 },
      ...(options.subscriptions
        ? [
          { kind: "put" as const, key: `${SUBS}s1`, value: { source: "one", afterSequence: 3 } },
          { kind: "put" as const, key: `${SUBS}s2`, value: { source: "two", afterSequence: 4 } },
          { kind: "put" as const, key: `${DELIVERIES}d1`, value: { turn: 1 } },
        ]
        : []),
    ],
  });
  let beat = 0;
  /** One other-store heartbeat write to an unrelated namespace, then one 2.2 Hz tick. */
  const heartbeat = async (): Promise<void> => {
    beat += 1;
    // The payload grows every beat, so no two heartbeat commits share a file size: a stat stamp
    // (dev:ino:size:times) cannot repeat through inode reuse within one timestamp tick (see notes).
    await writer.put({ key: `${PARTICIPANTS}${beat % 2 === 0 ? "a" : "b"}`, value: { beat, pad: "x".repeat(beat) }, identity: writerId });
    advance(HEARTBEAT_TICK_MS);
  };
  return { root, advance, writer, reader, heartbeat };
};

describe("MeshStore read signal: narrow non-fresh listings skip unrelated re-parses", () => {
  for (const subscriptions of [false, true]) {
    it(`serves ${subscriptions ? "non-empty" : "empty"} narrow listings across 2.2 Hz unrelated writes without re-reading state.json`, async () => {
      const { root, writer, reader, heartbeat } = await setup({ subscriptions });
      const expectedSubs = diskEntries(root, SUBS);
      const expectedDeliveries = diskEntries(root, DELIVERIES);
      expect(expectedSubs).toHaveLength(subscriptions ? 2 : 0);
      // Warm: the one canonical parse.
      expect(narrowReads(reader, SUBS)).toEqual(expectedSubs);
      expect(narrowReads(reader, DELIVERIES)).toEqual(expectedDeliveries);
      const reads = spyStateReads();
      let sinceWarm = 0;
      for (let tick = 0; tick < 22; tick++) {                 // 10 s: five read-cache windows expire
        await heartbeat();
        const before = reads();
        expect(narrowReads(reader, SUBS)).toEqual(expectedSubs);
        expect(narrowReads(reader, `${SUBS}s`)).toEqual(expectedSubs);   // deeper prefix, same namespace
        expect(narrowReads(reader, DELIVERIES)).toEqual(expectedDeliveries);
        sinceWarm += reads() - before;
      }
      expect(sinceWarm).toBe(0);
      // The sidecar exists beside the state and stays small.
      const signal = fs.statSync(path.join(root, SIGNAL));
      expect(signal.isFile()).toBe(true);
      expect(signal.size).toBeLessThan(16 * 1024);
      // Unrelated writes were real: a fresh read sees the latest heartbeat.
      expect(reader.listAll(PARTICIPANTS, { fresh: true })).toEqual(diskEntries(root, PARTICIPANTS));
      expect(writer.listAll(SUBS)).toEqual(expectedSubs);
    });
  }

  it("re-reads once the listed namespace changes, then skips again", async () => {
    const { root, writer, reader, advance, heartbeat } = await setup({ subscriptions: true });
    narrowReads(reader, SUBS);
    const reads = spyStateReads();
    await heartbeat();
    advance(WINDOW_MS);
    let before = reads();
    narrowReads(reader, SUBS);
    expect(reads() - before).toBe(0);
    await writer.put({ key: `${SUBS}s3`, value: { source: "three" }, identity: otherId });
    advance(WINDOW_MS);
    before = reads();
    expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
    expect(keys(reader.listAll(SUBS))).toEqual([`${SUBS}s1`, `${SUBS}s2`, `${SUBS}s3`]);
    expect(reads() - before).toBeGreaterThan(0);              // the namespace changed: canonical
    for (let tick = 0; tick < 10; tick++) await heartbeat();
    before = reads();
    expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
    expect(reads() - before).toBe(0);
  });
});

describe("MeshStore read signal: the namespace digest covers every entry field", () => {
  // Counterexamples to a summary digest (count, maximum version, key set): each keeps the
  // namespace's key count and maximum version while changing an entry. Other-namespace writes
  // with higher versions interleave throughout.
  const cases: Array<{ name: string; change: (writer: MeshStore) => Promise<unknown> }> = [
    { name: "overwrite of a low-version key", change: (writer) => writer.put({ key: `${SUBS}lo`, value: { v: "newer" }, identity: writerId }) },
    { name: "same value by another identity", change: (writer) => writer.put({ key: `${SUBS}lo`, value: { v: "old" }, identity: otherId }) },
    { name: "delete of a low-version key", change: (writer) => writer.delete({ key: `${SUBS}lo` }) },
    {
      name: "batch delete of one key and write of another",
      change: (writer) => writer.writeBatch({ identity: writerId, ops: [
        { kind: "delete", key: `${SUBS}lo` },
        { kind: "put", key: `${SUBS}lp`, value: { v: "old" } },
      ] }),
    },
    { name: "rewrite of the high-version key with a lower-looking value", change: (writer) => writer.put({ key: `${SUBS}hi`, value: 0, identity: writerId }) },
  ];
  // An older build rewrites an entry's value, author or time without a new version (a legacy or
  // foreign writer); a current writer's later unrelated commit then signs the whole state. Only a
  // digest over every entry field can tell the reader's cached namespace apart.
  const legacyEdits: Array<{ name: string; edit: (entry: MeshStateEntry) => void }> = [
    { name: "value", edit: (entry) => { entry.value = { source: "one", afterSequence: 99 }; } },
    { name: "updatedBy", edit: (entry) => { entry.updatedBy = otherId; } },
    { name: "updatedAt", edit: (entry) => { entry.updatedAt += 1; } },
  ];
  for (const { name, edit } of legacyEdits) {
    it(`detects a same-version ${name} change signed by a later unrelated commit`, async () => {
      const { root, reader, advance, heartbeat } = await setup({ subscriptions: true });
      const initial = narrowReads(reader, SUBS);
      await heartbeat();
      advance(WINDOW_MS);
      expect(narrowReads(reader, SUBS)).toEqual(initial);
      oldWriterRewrite(root, (state) => edit(state.entries[`${SUBS}s1`]!));
      await heartbeat();                                       // a current writer signs the edited state
      advance(WINDOW_MS);
      const expected = diskEntries(root, SUBS);
      expect(expected.map((entry) => entry.version)).toEqual(initial.map((entry) => entry.version));
      expect(expected).not.toEqual(initial);
      expect(narrowReads(reader, SUBS)).toEqual(expected);
    });
  }

  for (const { name, change } of cases) {
    it(`detects ${name} after the window`, async () => {
      const { root, writer, reader, advance, heartbeat } = await setup({ subscriptions: false });
      await writer.put({ key: `${SUBS}lo`, value: { v: "old" }, identity: writerId });
      for (let round = 0; round < 6; round++) {
        await writer.put({ key: `${SUBS}hi`, value: { round }, identity: writerId });
        await heartbeat();
      }
      const initial = narrowReads(reader, SUBS);
      const lo = initial.find((entry) => entry.key === `${SUBS}lo`)!;
      const hi = initial.find((entry) => entry.key === `${SUBS}hi`)!;
      expect(lo.version).toBeLessThan(hi.version);
      await heartbeat();
      advance(WINDOW_MS);
      expect(narrowReads(reader, SUBS)).toEqual(initial);     // warm across an unrelated change
      await change(writer);
      await heartbeat();
      advance(WINDOW_MS);
      const expected = diskEntries(root, SUBS);
      expect(expected).not.toEqual(initial);
      expect(narrowReads(reader, SUBS)).toEqual(expected);
      expect(reader.get(expected[0]!.key)).toEqual(expected[0]);
    });
  }
});

describe("MeshStore read signal: freshness bounds", () => {
  for (const prefix of [SUBS, DELIVERIES]) {
    it(`detects another store's put and delete under ${prefix.split("/")[0]} within one window`, async () => {
      const { root, writer, reader, advance, heartbeat } = await setup({ subscriptions: true });
      narrowReads(reader, prefix);
      await heartbeat();
      await writer.put({ key: `${prefix}incoming`, value: { at: 1 }, identity: otherId });
      advance(WINDOW_MS);                                     // the change is at most one window old
      expect(narrowReads(reader, prefix)).toEqual(diskEntries(root, prefix));
      expect(keys(reader.listAll(prefix))).toContain(`${prefix}incoming`);
      await heartbeat();
      await writer.delete({ key: `${prefix}incoming` });
      advance(WINDOW_MS);
      expect(keys(reader.listAll(prefix))).not.toContain(`${prefix}incoming`);
      expect(narrowReads(reader, prefix)).toEqual(diskEntries(root, prefix));
      // Past the window, an incoming overwrite amid heartbeats is also seen by the next tick after it.
      await writer.put({ key: `${prefix}incoming`, value: { at: 2 }, identity: otherId });
      for (let tick = 0; tick * HEARTBEAT_TICK_MS < WINDOW_MS; tick++) await heartbeat();
      expect(reader.listAll(prefix).find((entry) => entry.key === `${prefix}incoming`)?.value).toEqual({ at: 2 });
    });
  }

  it("shows its own put, delete and writeBatch at once, without advancing the clock", async () => {
    const { root, reader, heartbeat } = await setup({ subscriptions: true });
    narrowReads(reader, SUBS);
    await heartbeat();
    await reader.put({ key: `${SUBS}mine`, value: 1, identity: writerId });
    expect(keys(narrowReads(reader, SUBS))).toEqual([`${SUBS}mine`, `${SUBS}s1`, `${SUBS}s2`]);
    await reader.delete({ key: `${SUBS}s1` });
    expect(keys(narrowReads(reader, SUBS))).toEqual([`${SUBS}mine`, `${SUBS}s2`]);
    await reader.writeBatch({ identity: writerId, ops: [
      { kind: "delete", key: `${SUBS}s2` },
      { kind: "put", key: `${SUBS}batch`, value: 2 },
      { kind: "put", key: `${DELIVERIES}d2`, value: 3 },
    ] });
    expect(keys(narrowReads(reader, SUBS))).toEqual([`${SUBS}batch`, `${SUBS}mine`]);
    expect(keys(narrowReads(reader, DELIVERIES))).toEqual([`${DELIVERIES}d1`, `${DELIVERIES}d2`]);
    expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
  });

  it("fresh narrow reads re-read the canonical file on any change", async () => {
    const { root, reader, heartbeat } = await setup({ subscriptions: true });
    narrowReads(reader, SUBS);
    const reads = spyStateReads();
    for (let tick = 0; tick < 3; tick++) {
      await heartbeat();
      const before = reads();
      expect(plain(reader.listAllShared(SUBS, { fresh: true }))).toEqual(diskEntries(root, SUBS));
      expect(reads() - before).toBeGreaterThan(0);
      expect(reader.listAll(PARTICIPANTS, { fresh: true })).toEqual(diskEntries(root, PARTICIPANTS));
      await heartbeat();
      const again = reads();
      expect(reader.listAll(SUBS, { fresh: true })).toEqual(diskEntries(root, SUBS));
      expect(reads() - again).toBeGreaterThan(0);
    }
  });

  it("fresh narrow reads past the window re-read the canonical file after an unrelated change", async () => {
    const { root, reader, advance, heartbeat } = await setup({ subscriptions: true });
    narrowReads(reader, SUBS);
    const reads = spyStateReads();
    for (let round = 0; round < 3; round++) {
      await heartbeat();
      advance(WINDOW_MS);
      const before = reads();
      expect(reader.listAll(SUBS, { fresh: true })).toEqual(diskEntries(root, SUBS));
      expect(reads() - before).toBeGreaterThan(0);
      expect(reader.listAll(PARTICIPANTS)).toEqual(diskEntries(root, PARTICIPANTS));
      await heartbeat();
      advance(WINDOW_MS);
      const shared = reads();
      expect(plain(reader.listAllShared(DELIVERIES, { fresh: true }))).toEqual(diskEntries(root, DELIVERIES));
      expect(reads() - shared).toBeGreaterThan(0);
    }
  });

  it("the broad stateToken still reads the canonical file on any change, after a skipped narrow read", async () => {
    const { root, reader, advance, heartbeat } = await setup({ subscriptions: true });
    narrowReads(reader, SUBS);
    const token = reader.stateToken();
    const reads = spyStateReads();
    await heartbeat();
    advance(WINDOW_MS);
    const before = reads();
    narrowReads(reader, SUBS);                                 // may skip the canonical file
    const next = reader.stateToken();                          // must not
    expect(next).not.toBe(token);
    expect(reads() - before).toBeGreaterThan(0);
    expect(reader.stateToken()).toBe(next);
    expect(reader.listAll(PARTICIPANTS)).toEqual(diskEntries(root, PARTICIPANTS));
    // And the fresh form, after another unrelated change.
    await heartbeat();
    advance(WINDOW_MS);
    const fresh = reads();
    narrowReads(reader, SUBS);
    expect(reader.stateToken({ fresh: true })).not.toBe(next);
    expect(reads() - fresh).toBeGreaterThan(0);
  });

  it("broad and partial-component prefixes, and get, still see unrelated changes after the window", async () => {
    const { root, writer, reader, advance, heartbeat } = await setup({ subscriptions: true });
    narrowReads(reader, SUBS);
    await heartbeat();
    advance(WINDOW_MS);
    narrowReads(reader, SUBS);
    await writer.put({ key: "topology/subscriptionsx/a", value: 1, identity: otherId });
    await writer.put({ key: `${PARTICIPANTS}c`, value: { beat: "c" }, identity: otherId });
    advance(WINDOW_MS);
    expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
    expect(reader.listAll("topology/subscriptions")).toEqual(diskEntries(root, "topology/subscriptions"));
    expect(keys(reader.listAll("topology/subscriptions"))).toContain("topology/subscriptionsx/a");
    expect(reader.listAll("topology/")).toEqual(diskEntries(root, "topology/"));
    expect(reader.listAll("")).toEqual(diskEntries(root, ""));
    expect(reader.list("", 100)).toEqual(diskEntries(root, ""));
    expect(reader.get(`${PARTICIPANTS}c`)?.value).toEqual({ beat: "c" });
  });

  it("the default readCacheMs 0 does not relax: every changed file is re-read", async () => {
    const { root, reader, heartbeat } = await setup({ subscriptions: true, readCacheMs: 0 });
    expect(reader.readCacheMs).toBe(0);
    narrowReads(reader, SUBS);
    const reads = spyStateReads();
    for (let tick = 0; tick < 4; tick++) {
      await heartbeat();
      const before = reads();
      expect(reader.listAll(SUBS)).toEqual(diskEntries(root, SUBS));
      expect(reads() - before).toBeGreaterThan(0);
    }
    const plainStore = storeAt(root);
    plainStore.listAll(SUBS);
    await heartbeat();
    const before = reads();
    expect(plainStore.listAll(SUBS)).toEqual(diskEntries(root, SUBS));
    expect(reads() - before).toBeGreaterThan(0);
  });
});

describe("MeshStore read signal: untrusted or stale signals fall back to the canonical file", () => {
  it("an older writer's direct atomic rewrite, keeping the old signal, is seen after the window", async () => {
    const { root, reader, advance, heartbeat } = await setup({ subscriptions: true });
    narrowReads(reader, SUBS);
    await heartbeat();
    advance(WINDOW_MS);
    narrowReads(reader, SUBS);
    const signalBefore = signalText(root);
    oldWriterRewrite(root, oldWriterPut(`${SUBS}legacy`, { from: "old build" }));
    expect(signalText(root)).toBe(signalBefore);
    advance(WINDOW_MS);
    expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
    expect(keys(reader.listAll(SUBS))).toContain(`${SUBS}legacy`);
    // An older writer deleting from the namespace is seen too.
    oldWriterRewrite(root, (state) => { delete state.entries[`${SUBS}s1`]; });
    advance(WINDOW_MS);
    expect(keys(narrowReads(reader, SUBS))).toEqual([`${SUBS}legacy`, `${SUBS}s2`]);
  });

  const corruptions: Array<{ name: string; corrupt: (file: string, previous: string, current: string) => void }> = [
    { name: "missing", corrupt: (file) => fs.rmSync(file, { force: true }) },
    { name: "truncated", corrupt: (file, _previous, current) => real.writeFileSync(file, current.slice(0, Math.floor(current.length / 2))) },
    { name: "empty", corrupt: (file) => real.writeFileSync(file, "") },
    { name: "not JSON", corrupt: (file) => real.writeFileSync(file, "\u0000garbage{") },
    { name: "wrong shape", corrupt: (file) => real.writeFileSync(file, JSON.stringify({ unrelated: [1, 2, 3] })) },
    // The pre-change signal: valid, with the reader's cached digest, but naming the previous file.
    { name: "mismatched (the previous commit's signal)", corrupt: (file, previous) => real.writeFileSync(file, previous) },
    // The previous commit's signal padded far beyond any small sidecar: still valid JSON.
    { name: "oversize", corrupt: (file, previous) => real.writeFileSync(file, previous + " ".repeat(8 * 1024 * 1024)) },
    { name: "a directory", corrupt: (file) => { fs.rmSync(file, { force: true }); fs.mkdirSync(file); } },
  ];
  for (const { name, corrupt } of corruptions) {
    it(`a ${name} signal after a namespace change falls back to the canonical file`, async () => {
      const { root, writer, reader, advance, heartbeat } = await setup({ subscriptions: true });
      const signalPath = path.join(root, SIGNAL);
      narrowReads(reader, SUBS);
      await heartbeat();
      advance(WINDOW_MS);
      narrowReads(reader, SUBS);                              // warm fingerprint of the namespace
      const previous = signalText(root);
      await writer.put({ key: `${SUBS}new`, value: { after: name }, identity: otherId });
      const current = signalText(root);
      corrupt(signalPath, previous, current);
      advance(WINDOW_MS);
      expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
      expect(keys(reader.listAll(SUBS))).toContain(`${SUBS}new`);
      // Unrelated changes with the damaged signal stay correct too.
      await writer.put({ key: `${PARTICIPANTS}a`, value: { beat: "after" }, identity: otherId });
      if (name === "a directory") expect(fs.statSync(signalPath).isDirectory()).toBe(true);
      advance(WINDOW_MS);
      expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
      expect(reader.get(`${PARTICIPANTS}a`)?.value).toEqual({ beat: "after" });
      await writer.delete({ key: `${SUBS}new` });
      advance(WINDOW_MS);
      expect(keys(narrowReads(reader, SUBS))).toEqual([`${SUBS}s1`, `${SUBS}s2`]);
    });
  }

  // After an unrelated commit the namespace is unchanged, so only rejecting the damaged signal
  // itself (not a digest or stamp mismatch) makes the reader go to the canonical file.
  const damages: Array<{ name: string; damage: (file: string, current: string) => void }> = [
    { name: "missing", damage: (file) => fs.rmSync(file, { force: true }) },
    { name: "truncated", damage: (file, current) => real.writeFileSync(file, current.slice(0, current.length - 1)) },
    { name: "oversize (valid JSON padded to 8 MiB)", damage: (file, current) => real.writeFileSync(file, current + " ".repeat(8 * 1024 * 1024)) },
    { name: "not JSON", damage: (file) => real.writeFileSync(file, "not json") },
  ];
  for (const { name, damage } of damages) {
    it(`a ${name} current signal after an unrelated commit forces a canonical read`, async () => {
      const { root, reader, advance, heartbeat } = await setup({ subscriptions: true });
      narrowReads(reader, SUBS);
      await heartbeat();
      damage(path.join(root, SIGNAL), signalText(root));
      advance(WINDOW_MS);
      const reads = spyStateReads();
      expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
      expect(reads()).toBeGreaterThan(0);
      expect(reader.listAll(PARTICIPANTS)).toEqual(diskEntries(root, PARTICIPANTS));
    });
  }

  it("a signal write that fails cannot reject a committed put or leave later listings stale (unwritable path)", async () => {
    const { root, writer, reader, advance, heartbeat } = await setup({ subscriptions: true });
    const signalPath = path.join(root, SIGNAL);
    narrowReads(reader, SUBS);
    await heartbeat();
    advance(WINDOW_MS);
    narrowReads(reader, SUBS);
    fs.rmSync(signalPath, { recursive: true, force: true });
    fs.mkdirSync(signalPath);
    real.writeFileSync(path.join(signalPath, "occupied"), "x");     // a rename onto it must fail
    const entry = await writer.put({ key: `${SUBS}committed`, value: 1, identity: writerId });
    expect(entry.version).toBeGreaterThan(0);
    expect(diskEntries(root, SUBS).map((item) => item.key)).toContain(`${SUBS}committed`);
    expect(keys(writer.listAll(SUBS))).toContain(`${SUBS}committed`);
    expect(await writer.delete({ key: `${SUBS}s1` })).toMatchObject({ deleted: true });
    await writer.writeBatch({ identity: writerId, ops: [{ kind: "put", key: `${SUBS}batched`, value: 2 }] });
    expect(keys(writer.listAll(SUBS))).toEqual([`${SUBS}batched`, `${SUBS}committed`, `${SUBS}s2`]);
    advance(WINDOW_MS);
    expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
    expect(keys(reader.listAll(SUBS))).toEqual([`${SUBS}batched`, `${SUBS}committed`, `${SUBS}s2`]);
  });

  it("a signal write that fails with I/O errors leaves the old signal, and listings stay current", async () => {
    const { root, writer, reader, advance, heartbeat } = await setup({ subscriptions: true });
    narrowReads(reader, SUBS);
    await heartbeat();
    advance(WINDOW_MS);
    narrowReads(reader, SUBS);
    const signalBefore = signalText(root);
    const eio = (): never => { throw Object.assign(new Error("EIO: injected signal write failure"), { code: "EIO" }); };
    const touchesSignal = (...files: unknown[]): boolean => files.some((file) => path.basename(String(file)).startsWith(SIGNAL));
    vi.spyOn(fs, "writeFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) =>
      touchesSignal(file) ? eio() : (real.writeFileSync as (...args: unknown[]) => void)(file, ...rest)) as typeof fs.writeFileSync);
    vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) =>
      touchesSignal(from, to) ? eio() : real.renameSync(from, to)) as typeof fs.renameSync);
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags?: fs.OpenMode, ...rest: unknown[]) =>
      touchesSignal(file) && flags !== undefined && flags !== "r" && flags !== fs.constants.O_RDONLY
        ? eio()
        : (real.openSync as (...args: unknown[]) => number)(file, flags, ...rest)) as typeof fs.openSync);
    await expect(writer.put({ key: `${SUBS}committed`, value: 1, identity: writerId })).resolves.toMatchObject({ key: `${SUBS}committed` });
    await expect(writer.delete({ key: `${SUBS}s1` })).resolves.toMatchObject({ deleted: true });
    await expect(writer.writeBatch({ identity: writerId, ops: [{ kind: "put", key: `${SUBS}batched`, value: 2 }] }))
      .resolves.toEqual([expect.objectContaining({ applied: true })]);
    expect(signalText(root)).toBe(signalBefore);   // injected: unchanged
    expect(keys(writer.listAll(SUBS))).toEqual([`${SUBS}batched`, `${SUBS}committed`, `${SUBS}s2`]);
    advance(WINDOW_MS);
    expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
    expect(keys(reader.listAll(SUBS))).toEqual([`${SUBS}batched`, `${SUBS}committed`, `${SUBS}s2`]);
    await writer.put({ key: `${PARTICIPANTS}a`, value: { beat: "failing" }, identity: writerId });
    advance(WINDOW_MS);
    expect(keys(reader.listAll(SUBS))).toEqual([`${SUBS}batched`, `${SUBS}committed`, `${SUBS}s2`]);
    expect(reader.get(`${PARTICIPANTS}a`)?.value).toEqual({ beat: "failing" });
  });
});

describe("MeshStore read signal: a canonical replace racing the signal read", () => {
  it("never caches the older entries under the newer file past the window", async () => {
    const { root, reader, advance, heartbeat } = await setup({ subscriptions: true });
    const before = narrowReads(reader, SUBS);
    await heartbeat();                                         // the signal now names the current file
    advance(WINDOW_MS);
    // The moment the reader opens the signal, another host's commit replaces state.json (the
    // signal update of that commit has not landed yet).
    let raced = 0;
    const race = (file: unknown): void => {
      if (raced === 0 && isSignalFile(file)) {
        raced += 1;
        oldWriterRewrite(root, oldWriterPut(`${SUBS}raced`, { raced: true }));
      }
    };
    const readSpy = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      race(file);
      return (real.readFileSync as (...args: unknown[]) => string | Buffer)(file, ...rest);
    }) as typeof fs.readFileSync);
    const openSpy = vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      race(file);
      return (real.openSync as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    const during = reader.listAll(SUBS);
    expect(raced).toBe(1);                                     // the race was exercised
    const after = diskEntries(root, SUBS);
    expect([before, after]).toContainEqual(during);            // older (window) or newer, never a mix
    readSpy.mockRestore();
    openSpy.mockRestore();
    // Whatever the racing read returned, the next window must show the replacing commit.
    advance(WINDOW_MS);
    expect(narrowReads(reader, SUBS)).toEqual(after);
    expect(keys(reader.listAll(SUBS))).toContain(`${SUBS}raced`);
    advance(3 * WINDOW_MS);
    expect(narrowReads(reader, SUBS)).toEqual(after);
    expect(reader.listAll(SUBS, { fresh: true })).toEqual(after);
  });

  it("never labels an older canonical payload with the stamp of a file that replaced it mid-read", async () => {
    const { root, writer, reader, advance, heartbeat } = await setup({ subscriptions: true });
    const before = narrowReads(reader, SUBS);
    await writer.put({ key: `${SUBS}s1`, value: { source: "one", changed: true }, identity: writerId });
    await heartbeat();
    advance(WINDOW_MS);
    // The reader's canonical read returns the bytes of the file it opened; right after, another
    // host's commit replaces state.json before the reader stats it again.
    let raced = 0;
    const readSpy = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
      const result = (real.readFileSync as (...args: unknown[]) => string | Buffer)(file, ...rest);
      if (raced === 0 && isStateFile(file)) {
        raced += 1;
        oldWriterRewrite(root, oldWriterPut(`${SUBS}raced`, { raced: true }));
      }
      return result;
    }) as typeof fs.readFileSync);
    const during = reader.listAll(SUBS);
    readSpy.mockRestore();
    expect(raced).toBe(1);
    const after = diskEntries(root, SUBS);
    expect(during).not.toEqual(before);                        // the read did see the first change
    expect(keys(after)).toContain(`${SUBS}raced`);
    advance(WINDOW_MS);
    expect(narrowReads(reader, SUBS)).toEqual(after);
    advance(3 * WINDOW_MS);
    expect(narrowReads(reader, SUBS)).toEqual(after);
    expect(reader.listAll("")).toEqual(diskEntries(root, ""));
  });

  for (const moment of ["before", "after"] as const) {
    it(`a replace ${moment} the signal descriptor's readSync never caches older entries past the window`, async () => {
      const { root, reader, advance, heartbeat } = await setup({ subscriptions: true });
      const before = narrowReads(reader, SUBS);
      await heartbeat();
      advance(WINDOW_MS);
      let raced = 0;
      const replace = (): void => {
        if (raced++ === 0) oldWriterRewrite(root, oldWriterPut(`${SUBS}raced`, { raced: moment }));
      };
      const hooks = onSignalDescriptor({
        read: (_descriptor, delegate) => {
          if (moment === "before") replace();
          const bytes = delegate();
          if (moment === "after") replace();
          return bytes;
        },
      });
      const during = reader.listAll(SUBS);
      expect(hooks.reads()).toBeGreaterThan(0);               // the signal was read by descriptor
      expect(raced).toBeGreaterThan(0);
      const after = diskEntries(root, SUBS);
      expect(keys(after)).toContain(`${SUBS}raced`);
      expect([before, after]).toContainEqual(during);          // older or newer, never a mix
      vi.mocked(fs.readSync).mockRestore();
      advance(WINDOW_MS);
      expect(narrowReads(reader, SUBS)).toEqual(after);
      advance(3 * WINDOW_MS);
      expect(narrowReads(reader, SUBS)).toEqual(after);
      expect(reader.listAll(SUBS, { fresh: true })).toEqual(after);
      // Fresh readers revalidate canonical physical generation. Unchanged payloads keep
      // token identity; a changed or unavailable identity cannot waive fresh authority.
      const cachedToken = reader.stateToken();
      expect(reader.stateToken({ fresh: true })).toBe(cachedToken); // Physical generation unchanged: revalidate, do not reparse.
      expect(reader.listAll(SUBS)).toEqual(after);
    });
  }
});

describe("MeshStore read signal: a lock-bypassing replace racing a commit's signal publication", () => {
  // Another (older) writer replaces state.json after this store's commit renamed its file into
  // place but before the commit finishes: while the signal is hashed (the second stat of the new
  // file) or as the signal is renamed into place. Neither the committing store's cache nor any
  // reader may then keep the commit's payload as the replacing file's contents.
  const moments = [
    { name: "while the signal is hashed", atStat: 2, atSignalRename: false },
    { name: "as the signal is renamed into place", atStat: 0, atSignalRename: true },
  ];
  for (const { name, atStat, atSignalRename } of moments) {
    it(`a replace ${name} is seen by the committing store and by readers`, async () => {
      const { root, writer, advance, heartbeat } = await setup({ subscriptions: true });
      const committer = storeAt(root, { readCacheMs: WINDOW_MS });
      const reader = storeAt(root, { readCacheMs: WINDOW_MS });
      narrowReads(committer, SUBS);
      narrowReads(reader, SUBS);
      await heartbeat();
      let armed = false;
      let stats = 0;
      let raced = 0;
      const replace = (): void => {
        if (raced++ === 0) oldWriterRewrite(root, oldWriterPut(`${SUBS}bypass`, { moment: name }));
      };
      vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
        if (atSignalRename && armed && isSignalFile(to)) replace();
        real.renameSync(from, to);
        if (isStateFile(to) && raced === 0) { armed = true; stats = 0; }
      }) as typeof fs.renameSync);
      const realStat = fs.statSync;
      vi.spyOn(fs, "statSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
        if (armed && atStat > 0 && isStateFile(file) && ++stats === atStat) replace();
        return (realStat as (...args: unknown[]) => fs.Stats)(file, ...rest);
      }) as typeof fs.statSync);
      await committer.put({ key: `${SUBS}committed`, value: 1, identity: writerId });
      vi.mocked(fs.renameSync).mockRestore();
      vi.mocked(fs.statSync).mockRestore();
      expect(raced).toBe(1);                                   // the replace happened inside the commit
      const after = diskEntries(root, SUBS);
      expect(keys(after)).toContain(`${SUBS}bypass`);
      advance(WINDOW_MS);
      expect(narrowReads(committer, SUBS)).toEqual(after);
      expect(narrowReads(reader, SUBS)).toEqual(after);
      // Later unrelated commits keep them current.
      await writer.put({ key: `${PARTICIPANTS}a`, value: { beat: "later" }, identity: writerId });
      advance(2 * WINDOW_MS);
      expect(narrowReads(committer, SUBS)).toEqual(diskEntries(root, SUBS));
      expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
    });
  }
});

describe("MeshStore read signal: descriptor faults on the signal fall back to the canonical file", () => {
  const eio = (): never => { throw Object.assign(new Error("EIO: injected signal read failure"), { code: "EIO" }); };
  const faults: Array<{
    name: string;
    hooks: (signalPath: string) => Parameters<typeof onSignalDescriptor>[0];
  }> = [
    { name: "readSync throws EIO", hooks: () => ({ read: () => eio() }) },
    { name: "fstatSync throws EIO", hooks: () => ({ fstat: () => eio() }) },
    {
      name: "readSync returns a short read",
      hooks: () => ({ read: (descriptor, _delegate, rest) => {
        const [buffer, offset, length, position] = rest as [Buffer, number, number, number | null];
        return real.readSync(descriptor, buffer, offset, Math.max(0, length - 1), position);
      } }),
    },
    {
      name: "the signal is truncated in place between fstatSync and readSync",
      hooks: (signalPath) => ({ read: (_descriptor, delegate) => { real.truncateSync(signalPath, 7); return delegate(); } }),
    },
    {
      name: "fstatSync reports a size beyond the bound",
      hooks: () => ({ fstat: (_descriptor, delegate) => {
        const stat = delegate();
        return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { size: 64 * 1024 * 1024 });
      } }),
    },
  ];
  for (const { name, hooks } of faults) {
    it(`${name}: a namespace change is seen and an unrelated change reads state.json`, async () => {
      const { root, writer, reader, advance, heartbeat } = await setup({ subscriptions: true });
      const signalPath = path.join(root, SIGNAL);
      narrowReads(reader, SUBS);
      await heartbeat();
      advance(WINDOW_MS);
      narrowReads(reader, SUBS);
      const fault = onSignalDescriptor(hooks(signalPath));
      const reads = spyStateReads();
      // An unrelated commit: only rejecting the faulty signal read sends the reader to state.json.
      await heartbeat();
      advance(WINDOW_MS);
      let before = reads();
      expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
      expect(reads() - before).toBeGreaterThan(0);
      expect(fault.reads() + fault.fstats()).toBeGreaterThan(0);   // the fault was injected
      // A namespace change under the same fault.
      await writer.put({ key: `${SUBS}new`, value: { fault: name }, identity: otherId });
      advance(WINDOW_MS);
      before = reads();
      expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
      expect(keys(reader.listAll(SUBS))).toContain(`${SUBS}new`);
      expect(reads() - before).toBeGreaterThan(0);
    });
  }
});

describe("MeshStore read signal: bounds of the signal and of the namespace", () => {
  it("accepts a valid 100 KiB signal and rejects one over 128 KiB", async () => {
    const { root, reader, advance, heartbeat } = await setup({ subscriptions: true });
    const signalPath = path.join(root, SIGNAL);
    narrowReads(reader, SUBS);
    await heartbeat();
    const current = signalText(root);
    expect(current.length).toBeGreaterThan(0);
    real.writeFileSync(signalPath, current + " ".repeat(100 * 1024 - current.length));
    advance(WINDOW_MS);
    const reads = spyStateReads();
    expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
    expect(reads()).toBe(0);                                    // within the bound: reused
    await heartbeat();
    const next = signalText(root);
    real.writeFileSync(signalPath, next + " ".repeat(128 * 1024 + 1 - next.length));
    advance(WINDOW_MS);
    const before = reads();
    expect(narrowReads(reader, SUBS)).toEqual(diskEntries(root, SUBS));
    expect(reads() - before).toBeGreaterThan(0);                // beyond the bound: canonical
  });

  it("prefixes without two complete components always take the canonical file after a change", async () => {
    const { root, reader, advance, heartbeat } = await setup({ subscriptions: true });
    for (const prefix of ["", "topology/", "topology/subscriptions", "topology/sub", "residency/deliveries"]) {
      reader.listAll(prefix);
      await heartbeat();
      advance(WINDOW_MS);
      const reads = spyStateReads();
      expect(reader.listAll(prefix)).toEqual(diskEntries(root, prefix));
      expect(reads()).toBeGreaterThan(0);
      vi.mocked(fs.readFileSync).mockRestore();
      vi.mocked(fs.openSync).mockRestore();
    }
  });
});
