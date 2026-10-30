import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { writeFileAtomic } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import { StateFile, type MeshStateEntry } from "../src/mesh/state-file.js";
import type { PrefixObservation, PrefixWitness } from "../src/mesh/state-backend.js";

// Keep: data safety / opaque-handle trust boundary. These are native FILE backend
// checks, not a replacement for the integrator's real residency/Main drain arms.
const PREFIX = "residency/deliveries/";
const KEY = `${PREFIX}own/000`;
const identity = { id: "session:writer", name: "writer", kind: "main" as const };
const roots: string[] = [];
const stores: MeshStore[] = [];
const fixture = async (options: ConstructorParameters<typeof MeshStore>[3] = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "file-prefix-observation-"));
  roots.push(root);
  const open = () => {
    const mesh = new MeshStore(root, 256 * 1024, 1000, { ...options, stateBackend: "file" });
    stores.push(mesh);
    expect(mesh.stateBackendHandle).toBeInstanceOf(StateFile);
    return mesh;
  };
  const writer = open(), reader = open();
  const backend = reader.stateBackendHandle as StateFile;
  const file = path.join(root, "state.json");
  const journal = path.join(root, "state.read-journal.jsonl");
  const signal = path.join(root, "state.read-signal.json");
  const disk = () => JSON.parse(fs.readFileSync(file, "utf8"));
  const put = (key = KEY, value: unknown = { data: { nested: ["original"] } }) => writer.put({ key, value, identity });
  return { root, open, writer, reader, backend, file, journal, signal, disk, put };
};
const snapshot = (observation: PrefixObservation) => {
  expect(observation.kind).toBe("snapshot");
  if (observation.kind !== "snapshot") throw new Error(`expected snapshot, got ${observation.kind}`);
  expect(Object.isFrozen(observation.witness)).toBe(true);
  return observation;
};
const known = (observation: PrefixObservation) => {
  expect(observation.kind).not.toBe("unknown");
  if (observation.kind === "unknown") throw new Error(observation.reason);
  expect(Object.isFrozen(observation.witness)).toBe(true);
  return observation;
};
// Large native files come from multiple values below maxEventBytes, not an
// oversized put. The default is ~4.5 MiB, enough to exercise byte eviction with
// fewer than 32 prefixes without a near-GiB fixture.
const seedLargeUnrelatedState = async (f: Awaited<ReturnType<typeof fixture>>, count = 24) => {
  const payload = "x".repeat(192 * 1024);
  await f.writer.writeBatch({ identity, ops: Array.from({ length: count }, (_, index) => ({
    kind: "put" as const, key: `config/large/${index}`, value: { payload, index },
  })) });
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const mesh of stores.splice(0)) mesh.closeState();
  // Only roots allocated by THIS test file, never a shared mesh or real user path.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("StateFile owned prefix observations", () => {
  it("returns the complete localeCompare sequence, deeply freezes only its owned graph, and reuses it", async () => {
    const f = await fixture();
    await f.put(`${PREFIX}own/z`);
    await f.put(`${PREFIX}foreign/A`, { from: { id: "foreign", tags: ["sender"] }, principal: { ids: ["principal"] } });
    await f.put(`${PREFIX}foreign/a`);
    await f.put("config/agents/other", { unrelated: true });
    const selection = snapshot(f.backend.observePrefix(PREFIX));
    expect(selection.entries).toEqual(f.reader.listAll(PREFIX));
    expect(selection.entries.map(entry => entry.key)).toEqual(
      [`${PREFIX}own/z`, `${PREFIX}foreign/A`, `${PREFIX}foreign/a`].sort((a, b) => a.localeCompare(b)),
    );
    const original = JSON.stringify(selection.entries);
    const nested = selection.entries.find(entry => entry.key.endsWith("own/z"))!.value as { data: { nested: string[] } };
    const sender = selection.entries.find(entry => entry.key.endsWith("foreign/A"))!.value as { from: { tags: string[] } };
    expect(Object.isFrozen(selection.entries)).toBe(true);
    expect(Object.isFrozen(selection.entries[0]!.updatedBy)).toBe(true);
    expect(Object.isFrozen(nested.data.nested)).toBe(true);
    expect(() => nested.data.nested.push("mutation")).toThrow(TypeError);
    expect(() => { sender.from.tags[0] = "mutation"; }).toThrow(TypeError);
    expect(() => { (selection.entries[0]!.updatedBy as { name: string }).name = "mutation"; }).toThrow(TypeError);
    const legacy = f.reader.listAll(PREFIX);
    expect(Object.isFrozen(legacy)).toBe(false);
    expect(Object.isFrozen(legacy[0]!.updatedBy)).toBe(false);
    legacy[0]!.updatedBy.name = "mutable legacy result";
    const shared = f.reader.listAllShared(PREFIX);
    expect(Object.isFrozen(shared[0])).toBe(false);
    let witness = selection.witness;
    for (let pass = 0; pass < 3; pass++) {
      const hit = known(f.backend.observePrefix(PREFIX, witness));
      expect(hit.kind).toBe("unchanged");
      witness = hit.witness;
      expect(JSON.stringify(selection.entries)).toBe(original);
    }
    expect(f.reader.listAll(PREFIX)).toEqual(selection.entries);
  });

  it("bypasses both warm legacy TTL windows and sees foreign arrivals, deletes, and same-version payload/writer edits", async () => {
    const f = await fixture({ readCacheMs: 60_000, backgroundReadCacheMs: 60_000 });
    await f.put();
    const first = snapshot(f.backend.observePrefix(PREFIX));
    f.reader.listAll(PREFIX); // Warm the independent legacy cache.
    await f.put(`${PREFIX}foreign/late`, { arrived: true });
    const arrival = snapshot(f.backend.observePrefix(PREFIX, first.witness));
    expect(arrival.entries).toHaveLength(2);
    const document = f.disk();
    const version = document.entries[KEY].version;
    document.entries[KEY].value = { data: { nested: ["edited"] } };
    document.entries[KEY].updatedBy.name = "different writer";
    // Copied canonical UUID, chain head, versions and updatedAt; native atomic replacement.
    writeFileAtomic(f.file, JSON.stringify(document));
    const edited = snapshot(f.backend.observePrefix(PREFIX, arrival.witness));
    expect(edited.entries.find(entry => entry.key === KEY)).toMatchObject({
      version, value: { data: { nested: ["edited"] } }, updatedBy: { name: "different writer" },
    });
    await f.writer.delete({ key: `${PREFIX}foreign/late` });
    expect(snapshot(f.backend.observePrefix(PREFIX, edited.witness)).entries).toHaveLength(1);
  });

  it("hashes real in-place edits even when all physical metadata and copied labels repeat", async () => {
    const f = await fixture();
    await f.put(KEY, { value: "AAAA" });
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const normalStat = fs.statSync(f.file), bigStat = fs.statSync(f.file, { bigint: true });
    const document = f.disk();
    document.entries[KEY].value.value = "BBBB";
    document.entries[KEY].updatedBy.name = "editor"; // Same length as writer.
    fs.writeFileSync(f.file, JSON.stringify(document));
    fs.utimesSync(f.file, normalStat.atime, normalStat.mtime);
    const stat = fs.statSync.bind(fs);
    // A supplemental coarse/repeating-ctime adapter over the REAL changed bytes.
    vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) =>
      String(target) === f.file
        ? ((args[0] as { bigint?: boolean } | undefined)?.bigint ? bigStat : normalStat)
        : (stat as (...args: unknown[]) => unknown)(target, ...args)) as typeof fs.statSync);
    const changed = snapshot(f.backend.observePrefix(PREFIX, first.witness));
    expect(changed.entries[0]).toMatchObject({ value: { value: "BBBB" }, updatedBy: { name: "editor" } });
  });

  it("does not hide an unobserved copied-marker edit followed by a normal unrelated commit", async () => {
    const f = await fixture();
    await f.put(KEY, { before: true });
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const document = f.disk();
    document.entries[KEY].value = { rawEdit: true };
    document.entries[KEY].updatedBy.name = "raw editor";
    writeFileAtomic(f.file, JSON.stringify(document));
    await f.put("config/agents/after-raw-edit", { unrelated: true });
    const observed = snapshot(f.backend.observePrefix(PREFIX, first.witness));
    expect(observed.entries[0]).toMatchObject({ value: { rawEdit: true }, updatedBy: { name: "raw editor" } });
  });

  it("does not let a coalesced in-place edit hide in a modern journal even when predecessor tuples repeat", async () => {
    const f = await fixture();
    await f.put(KEY, { state: "AAAA" });
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const normal = fs.statSync(f.file), big = fs.statSync(f.file, { bigint: true });
    const document = f.disk();
    document.entries[KEY].value.state = "BBBB";
    fs.writeFileSync(f.file, JSON.stringify(document));
    fs.utimesSync(f.file, normal.atime, normal.mtime);
    const stat = fs.statSync.bind(fs);
    const spy = vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) =>
      String(target) === f.file ? ((args[0] as { bigint?: boolean } | undefined)?.bigint ? big : normal)
        : (stat as (...args: unknown[]) => unknown)(target, ...args)) as typeof fs.statSync);
    await f.put("config/agents/coalesced", { unrelated: true });
    const observed = f.backend.observePrefix(PREFIX, first.witness);
    spy.mockRestore();
    expect(snapshot(observed).entries[0]!.value).toEqual({ state: "BBBB" });
  });

  it("does not hide a real A→B→A replacement or delete/recreate behind old versions/UUIDs", async () => {
    const f = await fixture();
    await f.put(KEY, { state: "A" });
    const aBytes = fs.readFileSync(f.file, "utf8");
    const a = snapshot(f.backend.observePrefix(PREFIX));
    await f.put(KEY, { state: "B" });
    const b = snapshot(f.backend.observePrefix(PREFIX, a.witness));
    writeFileAtomic(f.file, aBytes);
    const restored = snapshot(f.backend.observePrefix(PREFIX, b.witness));
    expect(restored.entries[0]!.value).toEqual({ state: "A" });
    await f.writer.delete({ key: KEY });
    const empty = snapshot(f.backend.observePrefix(PREFIX, restored.witness));
    expect(empty.entries).toEqual([]);
    await f.put(KEY, { state: "recreated" });
    expect(snapshot(f.backend.observePrefix(PREFIX, empty.witness)).entries[0]!.value).toEqual({ state: "recreated" });
  });

  it("registers exact-prefix/instance handles, rejects forgeries and old handles, and invalidates bounded eviction", async () => {
    const f = await fixture();
    await f.put();
    const initial = snapshot(f.backend.observePrefix(PREFIX));
    snapshot(f.backend.observePrefix(PREFIX, {}));
    snapshot(f.backend.observePrefix(PREFIX, { ...initial.witness }));
    snapshot(f.backend.observePrefix("residency/", initial.witness));
    const peer = f.open().stateBackendHandle as StateFile;
    snapshot(peer.observePrefix(PREFIX, initial.witness));
    const active = snapshot(f.backend.observePrefix(PREFIX, initial.witness));
    const advanced = known(f.backend.observePrefix(PREFIX, active.witness));
    expect(advanced.kind).toBe("unchanged");
    snapshot(f.backend.observePrefix(PREFIX, active.witness)); // Superseded even on a stable hit.
    const toEvict = snapshot(f.backend.observePrefix(PREFIX));
    for (let index = 0; index < 32; index++) snapshot(f.backend.observePrefix(`empty/prefix/${index}`));
    snapshot(f.backend.observePrefix(PREFIX, toEvict.witness));
  });

  it("retires witnesses on dropCache/close/reopen without changing legacy FILE reads after close", async () => {
    const f = await fixture();
    await f.put();
    const first = snapshot(f.backend.observePrefix(PREFIX));
    f.backend.dropCache();
    const reset = snapshot(f.backend.observePrefix(PREFIX, first.witness));
    f.reader.closeState();
    expect(f.backend.observePrefix(PREFIX, reset.witness).kind).toBe("unknown");
    expect(f.reader.listAll(PREFIX)).toHaveLength(1); // Preserve legacy close behavior.
    const reopened = f.open().stateBackendHandle as StateFile;
    snapshot(reopened.observePrefix(PREFIX, reset.witness));
  });

  it("proves nonempty and empty completeness independently of absent or corrupt read signals", async () => {
    const f = await fixture();
    await f.put();
    const first = snapshot(f.backend.observePrefix("empty/namespace/"));
    expect(first.entries).toEqual([]);
    fs.rmSync(f.signal, { force: true });
    const emptyHit = known(f.backend.observePrefix("empty/namespace/", first.witness));
    expect(emptyHit.kind).toBe("unchanged");
    await f.put("config/agents/one", { unrelated: true });
    fs.writeFileSync(f.signal, '{"generation":"copied","namespaces":{}}');
    const unrelated = known(f.backend.observePrefix("empty/namespace/", emptyHit.witness));
    expect(unrelated.kind).toBe("unchanged");
    await f.put("empty/namespace/arrived", { present: true });
    expect(snapshot(f.backend.observePrefix("empty/namespace/", unrelated.witness)).entries).toHaveLength(1);
    const nonempty = snapshot(f.backend.observePrefix(PREFIX));
    const document = f.disk();
    document.entries[KEY].value = "changed while signal lies";
    writeFileAtomic(f.file, JSON.stringify(document));
    expect(snapshot(f.backend.observePrefix(PREFIX, nonempty.witness)).entries[0]!.value).toBe("changed while signal lies");
  });

  it("proves batched unrelated insert/delete changes without mistaking escaped or nested JSON members for root entries", async () => {
    const f = await fixture();
    await f.put(KEY, { data: { text: 'escaped \\" quotes { } [ ]', entries: { "config/agents/one": { nested: true } } } });
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const bytes = JSON.stringify(first.entries);
    await f.writer.writeBatch({ identity, ops: ["one", "two", "three"].map(name => ({
      kind: "put" as const, key: `config/agents/${name}`, value: { name, nested: [{ entries: "not a root member" }] },
    })) });
    const inserted = known(f.backend.observePrefix(PREFIX, first.witness));
    expect(inserted.kind).toBe("unchanged");
    await f.writer.writeBatch({ identity, ops: ["one", "two", "three"].map(name => ({
      kind: "delete" as const, key: `config/agents/${name}`,
    })) });
    expect(known(f.backend.observePrefix(PREFIX, inserted.witness)).kind).toBe("unchanged");
    expect(JSON.stringify(first.entries)).toBe(bytes);
    expect(first.entries[0]!.value).toEqual(f.reader.listAll(PREFIX)[0]!.value);
  });

  it.each(["", "{}", "not JSON"])
    ("never gives a tolerant empty document reusable negative authority (%j)", async bytes => {
      const f = await fixture();
      await f.put();
      const first = snapshot(f.backend.observePrefix(PREFIX));
      writeFileAtomic(f.file, bytes);
      expect(f.backend.observePrefix(PREFIX, first.witness).kind).toBe("unknown");
      expect(f.reader.listAll(PREFIX, { fresh: true })).toEqual([]);
    });

  it("does not disguise an existing full-scan failure on malformed entries as certified absence", async () => {
    const f = await fixture();
    await f.put();
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const document = f.disk();
    document.entries[KEY] = null;
    writeFileAtomic(f.file, JSON.stringify(document));
    expect(f.backend.observePrefix(PREFIX, first.witness).kind).toBe("unknown");
    expect(() => f.reader.listAll(PREFIX, { fresh: true })).toThrow();
  });

  it("falls back for absent/old/malformed headers and cannot reuse the old witness after proof loss", async () => {
    const f = await fixture();
    expect(f.backend.observePrefix(PREFIX).kind).toBe("unknown");
    await f.put();
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const original = fs.readFileSync(f.file, "utf8");
    for (const layout of ["absent", "last", "malformed"] as const) {
      const document = JSON.parse(original), generation = document.readGeneration;
      delete document.readGeneration;
      if (layout === "last") document.readGeneration = generation;
      if (layout === "malformed") document.readGeneration = "not-a-generation";
      writeFileAtomic(f.file, JSON.stringify(document));
      expect(f.backend.observePrefix(PREFIX, first.witness).kind).toBe("unknown");
      expect(f.reader.listAll(PREFIX, { fresh: true })).toHaveLength(1);
    }
    writeFileAtomic(f.file, original);
    snapshot(f.backend.observePrefix(PREFIX, first.witness));
  });

  it("returns UNKNOWN without usable physical identity but preserves moved-marker, oversize and I/O errors", async () => {
    const f = await fixture();
    await f.put();
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const stat = fs.statSync.bind(fs);
    const spy = vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
      const value = (stat as (...args: unknown[]) => unknown)(target, ...args);
      return String(target) === f.file && (args[0] as { bigint?: boolean } | undefined)?.bigint
        ? { ...(value as fs.BigIntStats), mtimeNs: undefined, ctimeNs: undefined } : value;
    }) as typeof fs.statSync);
    expect(f.backend.observePrefix(PREFIX, first.witness).kind).toBe("unknown");
    spy.mockRestore();
    snapshot(f.backend.observePrefix(PREFIX, first.witness));
    for (const natsStream of [undefined, "test-stream"]) {
      writeFileAtomic(f.file, JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3,
        at: new Date().toISOString(), ...(natsStream ? { natsStream } : {}) }));
      expect(() => f.backend.observePrefix(PREFIX)).toThrow(/moved to/);
      expect(() => f.reader.listAll(PREFIX, { fresh: true })).toThrow(/moved to/);
    }
    // Markers with a copied modern header still retain the canonical fatal error.
    writeFileAtomic(f.file, JSON.stringify({ readGeneration: "11111111-1111-1111-1111-111111111111",
      format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3, at: new Date().toISOString() }));
    expect(() => f.backend.observePrefix(PREFIX)).toThrow(/moved to/);
    const small = await fixture({ maxStateBytes: 512 * 1024 });
    fs.writeFileSync(small.file, "x".repeat(512 * 1024 + 1));
    expect(() => small.backend.observePrefix(PREFIX)).toThrow(/state exceeds/);
    const read = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(target) === f.file) throw Object.assign(new Error("injected canonical I/O failure"), { code: "EIO" });
      return (read as (...args: unknown[]) => unknown)(target, ...args);
    }) as typeof fs.readFileSync);
    expect(() => f.backend.observePrefix(PREFIX)).toThrow("Failed to read Fabric mesh state: injected canonical I/O failure");
  });

  it("cannot stamp a read-during-replace, including a copied UUID with repeating lossy metadata", async () => {
    const f = await fixture();
    await f.put(KEY, "before");
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const document = f.disk();
    const read = fs.readFileSync.bind(fs);
    let replaced = false;
    const spy = vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      const value = (read as (...args: unknown[]) => unknown)(target, ...args);
      if (String(target) === f.file && !replaced) {
        replaced = true;
        document.entries[KEY].value = "after";
        writeFileAtomic(f.file, JSON.stringify(document));
      }
      return value;
    }) as typeof fs.readFileSync);
    expect(f.backend.observePrefix(PREFIX, first.witness).kind).toBe("unknown");
    spy.mockRestore();
    expect(replaced).toBe(true);
    const after = snapshot(f.backend.observePrefix(PREFIX, first.witness));
    expect(after.entries[0]!.value).toBe("after");
  });

  it("rejects corrupt chain proofs, snapshots on missing history, and never trusts a self-checksummed terminal payload", async () => {
    const f = await fixture();
    await f.put();
    const first = snapshot(f.backend.observePrefix(PREFIX));
    await f.put("config/agents/one", 1);
    const journal = fs.readFileSync(f.journal, "utf8");
    fs.writeFileSync(f.journal, "broken optional journal\n");
    expect(f.backend.observePrefix(PREFIX, first.witness).kind).toBe("unknown");
    fs.writeFileSync(f.journal, journal);
    const restarted = snapshot(f.backend.observePrefix(PREFIX, first.witness));
    await f.put("config/agents/two", 2);
    fs.rmSync(f.journal);
    const missing = snapshot(f.backend.observePrefix(PREFIX, restarted.witness));
    await f.put("config/agents/three", 3);
    const document = f.disk();
    document.entries[KEY].value = { changed: "without updating journal payload hash" };
    writeFileAtomic(f.file, JSON.stringify(document));
    // Valid journal checksum and canonical copied header cannot bind the new payload.
    expect(f.backend.observePrefix(PREFIX, missing.witness).kind).not.toBe("unchanged");
    expect(f.reader.listAll(PREFIX, { fresh: true })[0]!.value).toEqual(document.entries[KEY].value);
  });
});

// Instrumentation is scoped to one synchronous observation; writer/setup work and
// assertions do not enter these counters. Byte reads remain visible on EVERY hit.
// Framing scan INPUT bytes and slice OUTPUT bytes are deterministic work proxies,
// not timings or claims about V8's rope/sliced-string allocation representation.
const measure = <T>(file: string, read: () => T) => {
  const counts = { canonicalBytes: 0, canonicalParses: 0, metadataParses: 0, deliveryRowsDecoded: 0,
    globalEntriesVisited: 0, selectionSorts: 0, selectionComparisons: 0,
    selectionClones: 0, freezeVisits: 0, journalRows: 0, journalDeliveryRowsDecoded: 0, proofEntriesDecoded: 0,
    proofFramingScans: 0, proofMemberScans: 0, proofScanInputBytes: 0, proofSliceOutputBytes: 0 };
  const parse = JSON.parse, stringify = JSON.stringify, readFile = fs.readFileSync;
  const values = Object.values, entries = Object.entries, sort = Array.prototype.sort, freeze = Object.freeze;
  const startsWith = String.prototype.startsWith, slice = String.prototype.slice;
  let framing = false;
  const spies = [
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      const result = (readFile as (...args: unknown[]) => string | Buffer)(target, ...args);
      if (String(target) === file) counts.canonicalBytes += Buffer.byteLength(result);
      return result;
    }) as typeof fs.readFileSync),
    vi.spyOn(JSON, "parse").mockImplementation(((text: string, ...args: unknown[]) => {
      const result = (parse as (...args: unknown[]) => any)(text, ...args);
      if (text.startsWith('{"readGeneration":')) {
        const decoded = Object.keys(result.entries).filter(key => key.startsWith(PREFIX)).length;
        if (decoded) counts.canonicalParses++;
        else counts.metadataParses++;
        counts.deliveryRowsDecoded += decoded;
      }
      if (result?.key && result?.updatedBy && typeof result?.version === "number") counts.proofEntriesDecoded++;
      if (result?.delta) {
        counts.journalRows++;
        const decoded = Object.keys(result.delta.entries).filter(key => key.startsWith(PREFIX)).length;
        counts.journalDeliveryRowsDecoded += decoded;
        counts.deliveryRowsDecoded += decoded;
      }
      return result;
    }) as typeof JSON.parse),
    vi.spyOn(JSON, "stringify").mockImplementation(((value: unknown, ...args: unknown[]) => {
      if (value && typeof value === "object" && "key" in value && String(value.key).startsWith(PREFIX)) counts.selectionClones++;
      return (stringify as (...args: unknown[]) => string)(value, ...args);
    }) as typeof JSON.stringify),
    vi.spyOn(String.prototype, "startsWith").mockImplementation(function(this: string, search: string, position?: number) {
      // canonicalObjectMember checks the first direct member at offset 1 once
      // per scan, even when the requested key is absent from the entire table.
      if (position === 1 && this[0] === "{" && search[0] === '"' && search[search.length - 1] === ":") {
        if (search === '"entries":') { counts.proofFramingScans++; framing = true; }
        else {
          counts.proofMemberScans++;
          // Fail boundedly on the old quadratic implementation instead of
          // recording thousands of large table strings in the spy itself.
          if (counts.proofMemberScans > 16) throw new Error("FILE proof exceeded 16 member scans");
        }
        counts.proofScanInputBytes += 2 * this.length;
      }
      return startsWith.call(this, search, position);
    }),
    vi.spyOn(String.prototype, "slice").mockImplementation(function(this: string, start?: number, end?: number) {
      const result = slice.call(this, start, end);
      if (framing && this[0] === "{") counts.proofSliceOutputBytes += 2 * result.length;
      return result;
    }),
    vi.spyOn(Object, "values").mockImplementation(((value: Record<string, unknown>) => {
      const result = values(value);
      if (value[KEY] && typeof value[KEY] === "object") counts.globalEntriesVisited += result.length;
      return result;
    }) as typeof Object.values),
    vi.spyOn(Object, "entries").mockImplementation(((value: Record<string, unknown>) => {
      const result = entries(value);
      if (value[KEY] && typeof value[KEY] === "object") counts.globalEntriesVisited += result.length;
      return result;
    }) as typeof Object.entries),
    vi.spyOn(Array.prototype, "sort").mockImplementation((function(this: MeshStateEntry[], compare?: (a: MeshStateEntry, b: MeshStateEntry) => number) {
      const selection = this[0]?.key?.startsWith(PREFIX) === true;
      if (selection) counts.selectionSorts++;
      return sort.call(this, compare && ((left: MeshStateEntry, right: MeshStateEntry) => {
        if (selection) counts.selectionComparisons++;
        return compare(left, right);
      }));
    }) as typeof Array.prototype.sort),
    vi.spyOn(Object, "freeze").mockImplementation(((value: object) => {
      counts.freezeVisits++;
      return freeze(value);
    }) as typeof Object.freeze),
  ];
  try { return { observation: read(), counts }; }
  finally { for (const spy of spies.reverse()) spy.mockRestore(); }
};

describe("FILE optional-proof work and retained-text budgets", () => {
  it("declines thousands of unrelated changed keys before any canonical proof-table scan or copy", async () => {
    const f = await fixture();
    await f.put();
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const initialGeneration = f.disk().readGeneration;
    // Sixteen real batches, not thousands of filesystem commits. Each journal
    // record stays below 256 KiB and the complete chain stays below 2 MiB.
    for (let batch = 0; batch < 16; batch++) {
      await f.writer.writeBatch({ identity, ops: Array.from({ length: 128 }, (_, index) => ({
        kind: "put" as const, key: `config/churn/${batch * 128 + index}`, value: { unrelated: index },
      })) });
    }
    const journal = fs.readFileSync(f.journal, "utf8");
    const lines = journal.trimEnd().split("\n");
    const deltas = lines.map(line => JSON.parse(line).delta);
    expect(Buffer.byteLength(journal)).toBeLessThan(2 * 1024 * 1024);
    expect(lines.every(line => Buffer.byteLength(line) < 256 * 1024)).toBe(true);
    expect(deltas).toHaveLength(16);
    expect(deltas.every(delta => delta.format === 2 && Object.keys(delta.entries).length === 128)).toBe(true);
    expect(deltas[0].previous).toBe(initialGeneration);
    for (let index = 1; index < deltas.length; index++) expect(deltas[index].previous).toBe(deltas[index - 1].generation);
    expect(deltas.at(-1).generation).toBe(f.disk().readGeneration);
    expect(new Set(deltas.flatMap(delta => Object.keys(delta.entries))).size).toBe(2048);
    const arm = measure(f.file, () => f.backend.observePrefix(PREFIX, first.witness));
    const complete = snapshot(arm.observation);
    expect(complete.entries).toEqual(f.reader.listAll(PREFIX));
    expect(arm.counts).toMatchObject({ canonicalParses: 1, deliveryRowsDecoded: 1,
      selectionSorts: 1, selectionClones: 0, metadataParses: 0,
      proofFramingScans: 0, proofMemberScans: 0, proofScanInputBytes: 0,
      proofSliceOutputBytes: 0, proofEntriesDecoded: 0 });
    expect(arm.counts.journalRows).toBeGreaterThan(0); // Not lost/capped history.
    expect(arm.counts.canonicalBytes).toBeGreaterThan(0);
    expect(arm.counts.freezeVisits).toBeGreaterThan(1);
    expect(known(f.backend.observePrefix(PREFIX, complete.witness)).kind).toBe("unchanged");
  }, 30_000);

  it("allows eight small unrelated keys but snapshots before proof framing at nine", async () => {
    const f = await fixture();
    await f.put();
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const batch = (group: string, count: number) => f.writer.writeBatch({ identity,
      ops: Array.from({ length: count }, (_, index) => ({
        kind: "put" as const, key: `config/boundary/${group}/${index}`, value: { index },
      })),
    });
    await batch("small", 8);
    const allowed = measure(f.file, () => f.backend.observePrefix(PREFIX, first.witness));
    const hit = known(allowed.observation);
    expect(hit.kind).toBe("unchanged");
    expect(allowed.counts).toMatchObject({ canonicalParses: 0, deliveryRowsDecoded: 0,
      selectionSorts: 0, selectionClones: 0, freezeVisits: 1,
      proofFramingScans: 2, proofMemberScans: 16 });
    expect(allowed.counts.proofScanInputBytes).toBeGreaterThan(0);
    expect(allowed.counts.proofSliceOutputBytes).toBeGreaterThan(0);
    await batch("large", 9);
    const declined = measure(f.file, () => f.backend.observePrefix(PREFIX, hit.witness));
    expect(snapshot(declined.observation).entries).toEqual(first.entries);
    expect(declined.counts).toMatchObject({ canonicalParses: 1, deliveryRowsDecoded: 1,
      journalRows: 1, metadataParses: 0, proofFramingScans: 0, proofMemberScans: 0,
      proofScanInputBytes: 0, proofSliceOutputBytes: 0 });
  });

  it("declines a three-key proof by text work and still snapshots a raw delivery edit under copied labels and repeating tuples", async () => {
    const f = await fixture();
    await seedLargeUnrelatedState(f, 6); // ~1.1 MiB, with each value under 256 KiB.
    await f.put(KEY, { state: "AAAA" });
    const first = snapshot(f.backend.observePrefix(PREFIX));
    const original = fs.readFileSync(f.file, "utf8");
    const normal = fs.statSync(f.file), big = fs.statSync(f.file, { bigint: true });
    const document = JSON.parse(original);
    document.entries[KEY].value.state = "BBBB";
    fs.writeFileSync(f.file, JSON.stringify(document));
    fs.utimesSync(f.file, normal.atime, normal.mtime);
    const stat = fs.statSync.bind(fs);
    // Same real raw edit + protocol commit path as the existing coarse-ctime
    // case, so history can be complete even though copied physical labels repeat.
    const spy = vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) =>
      String(target) === f.file ? ((args[0] as { bigint?: boolean } | undefined)?.bigint ? big : normal)
        : (stat as (...args: unknown[]) => unknown)(target, ...args)) as typeof fs.statSync);
    try {
      await f.writer.writeBatch({ identity, ops: [0, 1, 2].map(index => ({
        kind: "put" as const, key: `config/work-budget/${index}`, value: { index },
      })) });
      const current = fs.readFileSync(f.file, "utf8");
      const terminal = JSON.parse(fs.readFileSync(f.journal, "utf8").trimEnd().split("\n").at(-1)!).delta;
      expect(terminal.previous).toBe(JSON.parse(original).readGeneration);
      expect(terminal.generation).toBe(JSON.parse(current).readGeneration);
      expect(Object.keys(terminal.entries)).toHaveLength(3);
      expect(2 * (original.length + current.length) * 8 * 4).toBeGreaterThan(64 * 1024 * 1024);
      const arm = measure(f.file, () => f.backend.observePrefix(PREFIX, first.witness));
      const complete = snapshot(arm.observation);
      expect(complete.entries[0]!.value).toEqual({ state: "BBBB" });
      expect(arm.counts).toMatchObject({ canonicalParses: 1, deliveryRowsDecoded: 1,
        journalRows: 1, journalDeliveryRowsDecoded: 0, selectionSorts: 1,
        metadataParses: 0, proofFramingScans: 0, proofMemberScans: 0,
        proofScanInputBytes: 0, proofSliceOutputBytes: 0, proofEntriesDecoded: 0 });
      expect(arm.counts.canonicalBytes).toBeGreaterThan(0);
      expect(arm.counts.freezeVisits).toBeGreaterThan(1);
      expect(first.entries[0]!.value).toEqual({ state: "AAAA" });
    } finally { spy.mockRestore(); }
  });

  it("evicts proof text before 32 slots, replaces without byte drift, and retires authority on loss/reset/close", async () => {
    const f = await fixture();
    await seedLargeUnrelatedState(f);
    await f.put();
    const retainedTextBudget = 64 * 1024 * 1024;
    const textBytes = 2 * fs.readFileSync(f.file, "utf8").length;
    const capacity = Math.floor(retainedTextBudget / textBytes);
    expect(capacity).toBeGreaterThanOrEqual(2);
    expect(capacity).toBeLessThan(32);
    // ASCII makes UTF-8-only accounting observably wrong: it would keep almost
    // twice as many of these separately read canonical strings.
    expect(textBytes).toBe(2 * fs.statSync(f.file).size);
    const prefixes = Array.from({ length: capacity }, (_, index) =>
      index === capacity - 1 ? PREFIX : `empty/proof-budget/${index}/`);
    let handles: PrefixWitness[] = [];
    const fillCold = () => {
      handles = prefixes.map((prefix, index) => snapshot(f.backend.observePrefix(prefix, handles[index])).witness);
    };
    const assertRetained = () => {
      handles = prefixes.map((prefix, index) => {
        const observation = known(f.backend.observePrefix(prefix, handles[index]));
        expect(observation.kind).toBe("unchanged");
        return observation.witness;
      });
    };
    fillCold();
    for (let pass = 0; pass < 3; pass++) {
      const newest = known(f.backend.observePrefix(PREFIX, handles.at(-1)));
      expect(newest.kind).toBe("unchanged");
      handles[capacity - 1] = newest.witness;
    }
    assertRetained(); // Replacements must not consume additional text budget.
    await f.put("config/replacement/one", { unrelated: true });
    // Large-file proofs conservatively snapshot. All replaced read points must
    // still fit, and their new witnesses must all remain registered.
    fillCold();
    assertRetained();
    snapshot(f.backend.observePrefix("empty/proof-budget/overflow/"));
    const recent = known(f.backend.observePrefix(PREFIX, handles.at(-1)));
    expect(recent.kind).toBe("unchanged");
    snapshot(f.backend.observePrefix(prefixes[0]!, handles[0])); // Oldest evicted by bytes.

    f.backend.dropCache();
    fillCold();
    const original = fs.readFileSync(f.file, "utf8");
    writeFileAtomic(f.file, "not JSON");
    expect(f.backend.observePrefix(PREFIX, handles.at(-1)).kind).toBe("unknown");
    writeFileAtomic(f.file, original);
    handles[capacity - 1] = snapshot(f.backend.observePrefix(PREFIX, handles.at(-1))).witness;
    assertRetained(); // UNKNOWN deleted both old authority and its text charge.

    const readFile = fs.readFileSync;
    const readSpy = vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(target) === f.file) throw Object.assign(new Error("budget canonical I/O failure"), { code: "EIO" });
      return (readFile as (...args: unknown[]) => unknown)(target, ...args);
    }) as typeof fs.readFileSync);
    try { expect(() => f.backend.observePrefix(PREFIX, handles.at(-1))).toThrow("budget canonical I/O failure"); }
    finally { readSpy.mockRestore(); }
    handles[capacity - 1] = snapshot(f.backend.observePrefix(PREFIX, handles.at(-1))).witness;
    assertRetained(); // Fatal canonical errors must also retire this prefix.

    writeFileAtomic(f.file, JSON.stringify({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 3,
      at: new Date().toISOString() }));
    expect(() => f.backend.observePrefix(PREFIX, handles.at(-1))).toThrow(/moved to/);
    writeFileAtomic(f.file, original);
    fillCold(); // A moved marker retires every prefix, not only the observed one.
    assertRetained();
    f.backend.dropCache();
    fillCold();
    assertRetained(); // Reset cannot leave a stale aggregate charge.
    f.reader.closeState();
    expect(f.backend.observePrefix(PREFIX, handles.at(-1)).kind).toBe("unknown");
    const legacy = f.reader.listAll(PREFIX);
    expect(legacy).toHaveLength(1);
    expect(Object.isFrozen(legacy[0])).toBe(false);
    snapshot((f.open().stateBackendHandle as StateFile).observePrefix(PREFIX, handles.at(-1)));
  }, 30_000);
});

describe("FILE 387-delivery / 181-config selection work ledger (not whole-drain timing)", () => {
  it("does no selection traversal/sort/clone/freeze on three stable and three unrelated-commit hits", async () => {
    const f = await fixture({ readCacheMs: 60_000 });
    await f.writer.writeBatch({ identity, ops: Array.from({ length: 181 }, (_, index) => ({
      kind: "put" as const, key: `config/agents/${index}`, value: { enabled: true, nested: [index] },
    })) });
    await f.writer.writeBatch({ identity, ops: Array.from({ length: 387 }, (_, index) => ({
      kind: "put" as const,
      key: index < 27 ? `${PREFIX}own/${String(index).padStart(3, "0")}` : `${PREFIX}foreign/${String(index).padStart(3, "0")}`,
      value: { pending: true, from: { id: `sender:${index}` }, principal: { ids: ["principal"] }, data: { nested: [index] } },
    })) });
    const cold = measure(f.file, () => f.backend.observePrefix(PREFIX));
    const selected = snapshot(cold.observation);
    expect(selected.entries).toHaveLength(387);
    expect(selected.entries.filter(entry => entry.key.startsWith(`${PREFIX}own/`))).toHaveLength(27);
    expect(cold.counts).toMatchObject({ canonicalParses: 1, deliveryRowsDecoded: 387, selectionSorts: 1, selectionClones: 0 });
    expect(cold.counts.globalEntriesVisited).toBeGreaterThanOrEqual(568);
    expect(cold.counts.freezeVisits).toBeGreaterThan(387);
    const bytes = JSON.stringify(selected.entries);
    let witness: PrefixWitness = selected.witness;
    const hit = (journalRows: number) => {
      const arm = measure(f.file, () => f.backend.observePrefix(PREFIX, witness));
      const observation = known(arm.observation);
      expect(observation.kind).toBe("unchanged");
      witness = observation.witness;
      expect(arm.counts).toMatchObject({ canonicalParses: 0, deliveryRowsDecoded: 0, globalEntriesVisited: 0,
        selectionSorts: 0, selectionComparisons: 0, selectionClones: 0, freezeVisits: 1, journalRows, journalDeliveryRowsDecoded: 0,
        metadataParses: journalRows ? 2 : 0 });
      // Only the small returned witness is frozen on a hit, never the selection.
      expect(Object.isFrozen(observation.witness)).toBe(true);
      expect(arm.counts.canonicalBytes).toBeGreaterThan(0); // Not O(1), not zero filesystem work.
      expect(JSON.stringify(selected.entries)).toBe(bytes);
    };
    for (let pass = 0; pass < 3; pass++) hit(0);
    for (let commit = 0; commit < 3; commit++) {
      await f.put(`config/agents/${commit}`, { changed: commit });
      hit(1);
    }
    await f.put(KEY, { own: "changed" });
    const change = measure(f.file, () => f.backend.observePrefix(PREFIX, witness));
    const changed = snapshot(change.observation);
    expect(change.counts).toMatchObject({ canonicalParses: 1, deliveryRowsDecoded: 388, journalDeliveryRowsDecoded: 1, selectionSorts: 1 });
    expect(change.counts.freezeVisits).toBeGreaterThan(387);
    expect(changed.entries.find(entry => entry.key === KEY)!.value).toEqual({ own: "changed" });
    expect(JSON.stringify(selected.entries)).toBe(bytes);
    f.backend.dropCache();
    const reset = measure(f.file, () => f.backend.observePrefix(PREFIX, changed.witness));
    const resetSelection = snapshot(reset.observation);
    expect(reset.counts.deliveryRowsDecoded).toBe(387);

    // Unsupported optional proof discards the handle. Today's unstrengthened,
    // mutable listAll remains a separately counted fallback, never stamped here.
    const stat = fs.statSync.bind(fs);
    const statSpy = vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
      const result = (stat as (...args: unknown[]) => unknown)(target, ...args);
      return String(target) === f.file && (args[0] as { bigint?: boolean } | undefined)?.bigint
        ? { ...(result as fs.BigIntStats), mtimeNs: undefined } : result;
    }) as typeof fs.statSync);
    const unsupported = measure(f.file, () => f.backend.observePrefix(PREFIX, resetSelection.witness));
    statSpy.mockRestore();
    expect(unsupported.observation.kind).toBe("unknown");
    expect(unsupported.counts.freezeVisits).toBe(0);
    const fallback = measure(f.file, () => f.reader.listAll(PREFIX));
    expect(fallback.observation).toHaveLength(387);
    expect(fallback.counts.selectionClones).toBe(387);
    expect(fallback.counts.freezeVisits).toBe(0);
    expect(fallback.observation.every(entry => !Object.isFrozen(entry))).toBe(true);
  });
});
