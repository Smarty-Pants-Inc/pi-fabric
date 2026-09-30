// Canonical generation compatibility: a missing legacy marker is UNKNOWN, not an equal
// generation. Non-fresh reuse is bounded; fresh protocol reads must reparse even when a
// legacy writer copies the marker AND repeats the stat stamp (#164 Security S1).
// That authorization regression is covered in mesh-legacy-auth-revalidation.test.ts.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { writeFileAtomic as coreWriteFileAtomic } from "../src/core/atomic-write.js";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";

const PREFIX = "topology/subscriptions/";
const KEY = `${PREFIX}compat`;
const identity: MeshIdentity = { id: "session:compat", name: "compat", kind: "main" };
const IMPORTED_GENERATION = "12345678-1234-4234-8234-123456789abc";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const scratch = fileURLToPath(new URL("../.local/check-temp/", import.meta.url));
const roots: string[] = [];
const real = { readFileSync: fs.readFileSync, statSync: fs.statSync };
type State = { format: number; entries: Record<string, MeshStateEntry>; readGeneration?: string };
const statePath = (root: string) => path.join(root, "state.json");
const text = (root: string) => String(real.readFileSync(statePath(root), "utf8"));
const disk = (root: string): State => JSON.parse(text(root)) as State;
const signalText = (root: string): string | undefined => {
  const file = path.join(root, "state.read-signal.json");
  return fs.existsSync(file) ? String(real.readFileSync(file, "utf8")) : undefined;
};

const setup = async () => {
  fs.mkdirSync(scratch, { recursive: true });
  const root = fs.mkdtempSync(path.join(scratch, "mesh-generation-compat-"));
  roots.push(root);
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const advance = () => { now += RUNTIME_MESH_READ_CACHE_MS + 1; };
  const writer = new MeshStore(root, 64 * 1024, 100);
  await writer.put({ key: KEY, value: "old", identity });
  return { root, writer, advance };
};

// Model a cooperating legacy writer: take the REAL MeshStore lock, atomically replace
// canonical bytes with the core writer, and neither mint a marker nor publish a signal.
const legacyReplace = (root: string, writer: MeshStore, mutate: (state: State) => void) =>
  writer.exclusive(() => {
    const state = disk(root);
    mutate(state);
    coreWriteFileAtomic(statePath(root), JSON.stringify(state));
  });

const freezeCanonicalStat = (root: string) => {
  const file = statePath(root);
  const frozen = real.statSync(file);
  vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) =>
    path.resolve(String(target)) === file ? frozen :
      (real.statSync as (...args: unknown[]) => fs.Stats)(target, ...rest)) as typeof fs.statSync);
};
const fullReads = () => {
  const spy = vi.spyOn(fs, "readFileSync");
  return () => spy.mock.calls.filter(([file]) => String(file).endsWith(`${path.sep}state.json`)).length;
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("MeshStore canonical generation: legacy writers", () => {
  for (const legacy of ["missing marker, frozen stat", "copied marker, changed stat"] as const) {
    for (const mode of ["fresh get", "expired narrow list"] as const) {
      it(`${mode} fully reads a locked legacy replacement with ${legacy}`, async () => {
        const { root, writer, advance } = await setup();
        if (legacy === "missing marker, frozen stat") {
          await legacyReplace(root, writer, (state) => { delete state.readGeneration; });
          freezeCanonicalStat(root);
        } else if (disk(root).readGeneration === undefined) {
          // Baseline builds do not mint a marker; import one to exercise real copied-marker
          // semantics there too, without depending on a baseline fixture in this test.
          await writer.exclusive(() => coreWriteFileAtomic(statePath(root),
            JSON.stringify({ readGeneration: IMPORTED_GENERATION, ...disk(root) })));
        }
        const reader = new MeshStore(root, 64 * 1024, 100, { readCacheMs: RUNTIME_MESH_READ_CACHE_MS });
        expect(reader.listAll(PREFIX).map((entry) => entry.value)).toEqual(["old"]);
        const marker = disk(root).readGeneration;
        const stamp = real.statSync(statePath(root));
        const signal = signalText(root);
        await legacyReplace(root, writer, (state) => {
          // Keep the entry version unchanged: neither version nor stale namespace hint
          // may authorize reuse. A larger value guarantees changed metadata for copied-marker.
          state.entries[KEY]!.value = legacy === "missing marker, frozen stat" ? "new" : "new legacy payload";
        });
        expect(disk(root).readGeneration).toBe(marker);
        expect(signalText(root)).toBe(signal);
        if (legacy === "copied marker, changed stat") expect(real.statSync(statePath(root)).size).not.toBe(stamp.size);
        const expected = disk(root).entries[KEY]!;
        const reads = fullReads();
        if (mode === "fresh get") {
          expect(reader.get(KEY, { fresh: true })).toEqual(expected);
        } else {
          advance();
          expect(reader.listAll(PREFIX)).toEqual([expected]);
        }
        expect(reads()).toBeGreaterThan(0);
      });
    }
  }
});

describe("MeshStore canonical generation: current commits", () => {
  for (const imported of ["no marker", "copied marker"] as const) {
    it(`put, delete and writeBatch mint distinct first-field UUIDs after importing ${imported}`, async () => {
      const { root, writer } = await setup();
      await legacyReplace(root, writer, (state) => {
        delete state.readGeneration;
        // Deliberately last in the imported JSON: current commits must place it FIRST.
        if (imported === "copied marker") state.readGeneration = IMPORTED_GENERATION;
      });
      const generations = new Set([IMPORTED_GENERATION]);
      const checkCommit = () => {
        const serialized = text(root);
        const generation = disk(root).readGeneration;
        expect(generation).toMatch(UUID);
        expect(serialized.startsWith(`{"readGeneration":"${generation}",`)).toBe(true);
        expect(generations.has(generation!)).toBe(false);
        generations.add(generation!);
      };
      await writer.put({ key: KEY, value: "old", identity }); // same value still consumes a revision
      checkCommit();
      await writer.delete({ key: KEY });
      checkCommit();
      await writer.writeBatch({ identity, ops: [
        { kind: "put", key: KEY, value: "batch" },
        { kind: "put", key: `${PREFIX}other`, value: 2 },
      ] });
      checkCommit();
      // A later copied-old marker must not be inherited by ANY of the commit paths.
      for (const operation of [
        () => writer.put({ key: KEY, value: "next", identity }),
        () => writer.delete({ key: KEY }),
        () => writer.writeBatch({ identity, ops: [{ kind: "put", key: KEY, value: "last" }] }),
      ]) {
        await legacyReplace(root, writer, (state) => { state.readGeneration = IMPORTED_GENERATION; });
        await operation();
        checkCommit();
      }
    });
  }

  it("unchanged and rejected operations leave canonical bytes (and marker) untouched", async () => {
    const { root, writer } = await setup();
    const before = text(root);
    const unchanged = () => expect(text(root)).toBe(before);
    expect(await writer.delete({ key: `${PREFIX}absent` })).toEqual({ deleted: false });
    unchanged();
    expect(await writer.writeBatch({ identity, ops: [] })).toEqual([]);
    unchanged();
    await writer.writeBatch({ identity, ops: [
      { kind: "delete", key: `${PREFIX}absent` },
      { kind: "put", key: KEY, value: "skip", ifVersion: 0, onConflict: "skip" },
    ] });
    unchanged();
    await expect(writer.put({ key: KEY, value: "reject", identity, ifVersion: 0 })).rejects.toThrow();
    unchanged();
    await expect(writer.writeBatch({ identity, ops: [
      { kind: "put", key: `${PREFIX}tentative`, value: 1 },
      { kind: "put", key: KEY, value: "abort", ifVersion: 0 },
    ] })).rejects.toThrow();
    unchanged();
    await writer.confirmWritable();
    unchanged();
  });
});
