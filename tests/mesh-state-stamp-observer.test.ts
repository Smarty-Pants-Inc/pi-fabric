// Observer reuse is intentional, not payload authority. These expectations regress if
// UI revalidation parses every time, or if that optimization leaks into fresh auth reads.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, it, vi } from "vitest";
import { writeFileAtomic } from "../src/core/atomic-write.js";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";

const scratch = fileURLToPath(new URL("../.local/check-temp/", import.meta.url));
const roots: string[] = [];
const real = { statSync: fs.statSync, readFileSync: fs.readFileSync };
const key = "topology/participants/observer";
const identity: MeshIdentity = { id: "session:old", name: "owner", kind: "main" };
type State = { readGeneration?: string; entries: Record<string, MeshStateEntry> };
const setup = async () => {
  fs.mkdirSync(scratch, { recursive: true });
  const root = fs.mkdtempSync(path.join(scratch, "stamp-observer-"));
  roots.push(root);
  const file = path.join(root, "state.json");
  const writer = new MeshStore(root, 64 * 1024, 100);
  const reader = new MeshStore(root, 64 * 1024, 100, { readCacheMs: RUNTIME_MESH_READ_CACHE_MS });
  await writer.put({ key, value: { owner: "old" }, identity });
  const disk = (): State => JSON.parse(String(real.readFileSync(file, "utf8")));
  const freeze = () => {
    const stat = real.statSync(file);
    vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) =>
      path.resolve(String(target)) === file ? stat :
        (real.statSync as (...args: unknown[]) => fs.Stats)(target, ...rest)) as typeof fs.statSync);
  };
  const replace = (mutate: (state: State) => void) => writer.exclusive(() => {
    const state = disk();
    mutate(state);
    writeFileAtomic(file, JSON.stringify(state));
  });
  const reads = vi.spyOn(fs, "readFileSync");
  const count = () => reads.mock.calls.filter(([target]) => String(target) === file).length;
  return { writer, reader, disk, freeze, replace, count };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

it("observer reuses an already parsed current get cache without a canonical full read or TTL extension", async () => {
  const { reader, writer, count } = await setup();
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  expect(reader.get(key)?.value).toEqual({ owner: "old" });
  const before = count();
  const stamp = reader.cachedStateStamp();
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(count()).toBe(before);
  now += RUNTIME_MESH_READ_CACHE_MS - 1;
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  await writer.put({ key, value: { owner: "new" }, identity });
  const afterWrite = count(); // The writer itself reads canonical state under the lock.
  now += 2; // Original parse window expired; observer must not have extended it.
  expect(reader.get(key)?.value).toEqual({ owner: "new" });
  expect(count()).toBe(afterWrite + 1);
});

for (const change of ["stat", "generation"] as const) it(`observer refreshes payload on changed ${change} within TTL`, async () => {
  const { reader, writer, freeze, replace, count } = await setup();
  if (change === "generation") freeze();
  reader.get(key);
  const stamp = reader.cachedStateStamp();
  if (change === "stat") await replace((state) => { state.entries[key]!.value = { owner: "larger-new-owner" }; });
  else await writer.put({ key, value: { owner: "new" }, identity });
  const before = count();
  const next = reader.cachedStateStamp(true);
  if (change === "stat") expect(next).not.toBe(stamp);
  else expect(next).toBe(stamp); // Metadata stamp is not a generation or authority token.
  expect(count()).toBe(before + 1);
  expect(reader.get(key)?.value).toEqual({ owner: change === "stat" ? "larger-new-owner" : "new" });
});

for (const method of ["get", "listAll", "listAllShared", "stateToken", "confirmWritable"] as const) {
  it(`legacy same-stat copied-marker observer reuse cannot weaken ${method}`, async () => {
    const { reader, freeze, replace, disk, count } = await setup();
    freeze(); // Only canonical metadata is frozen; bytes, lock and atomic replace are real.
    const old = reader.get(key);
    const stamp = reader.cachedStateStamp();
    const generation = disk().readGeneration;
    await replace((state) => {
      state.entries[key]!.value = { owner: "new" };
      state.entries[key]!.updatedBy = { ...identity, id: "session:new" };
    });
    const expected = disk().entries[key]!;
    expect(disk().readGeneration).toBe(generation);
    const before = count();
    // Baseline legacy ABA limitation is deliberate for this status-only observer.
    expect(reader.cachedStateStamp(true)).toBe(stamp);
    expect(count()).toBe(before);
    expect(reader.get(key)).toEqual(old);
    if (method === "get") expect(reader.get(key, { fresh: true })).toEqual(expected);
    else if (method === "stateToken") {
      reader.stateToken({ fresh: true });
      expect(reader.get(key)).toEqual(expected);
    } else if (method === "confirmWritable") {
      await reader.confirmWritable();
      expect(reader.get(key)).toEqual(expected); // S1 barrier stays intact, even for ordinary reads.
    } else expect(reader[method]("topology/participants/", { fresh: true })).toEqual([expected]);
    expect(count()).toBe(before + 1);
  });
}

it("markerless warm observer reuses unchanged metadata with exactly one 64-byte header read", async () => {
  const { reader, replace, count } = await setup();
  await replace((state) => { delete state.readGeneration; });
  reader.get(key);
  const stamp = reader.cachedStateStamp();
  const before = count();
  const headers = vi.spyOn(fs, "readSync");
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(count()).toBe(before);
  expect(headers.mock.calls).toHaveLength(1);
  expect(headers.mock.calls[0]!.slice(2)).toEqual([0, 64, 0]);
});

for (const barrier of ["fresh", "confirmWritable"] as const) it(`markerless metadata ABA observer reuse preserves ${barrier} authority`, async () => {
  const { reader, replace, freeze, disk, count } = await setup();
  await replace((state) => { delete state.readGeneration; });
  freeze();
  const old = reader.get(key);
  const stamp = reader.cachedStateStamp();
  await replace((state) => {
    state.entries[key]!.value = { owner: "new" };
    state.entries[key]!.updatedBy = { ...identity, id: "session:new" };
  });
  const expected = disk().entries[key]!;
  expect(disk().readGeneration).toBeUndefined();
  const before = count();
  expect(reader.cachedStateStamp(true)).toBe(stamp); // Non-authoritative baseline metadata ABA.
  expect(count()).toBe(before);
  expect(reader.get(key)).toEqual(old);
  if (barrier === "fresh") expect(reader.get(key, { fresh: true })).toEqual(expected);
  else {
    await reader.confirmWritable();
    expect(reader.get(key)).toEqual(expected);
  }
  expect(count()).toBe(before + 1);
});

it("markerless to newly minted current UUID forces same-stat observer parse", async () => {
  const { reader, writer, replace, freeze, disk, count } = await setup();
  await replace((state) => { delete state.readGeneration; });
  freeze();
  reader.get(key);
  const stamp = reader.cachedStateStamp();
  await writer.put({ key, value: { owner: "new" }, identity });
  expect(disk().readGeneration).toMatch(/^[0-9a-f-]{36}$/);
  const before = count();
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(count()).toBe(before + 1);
  expect(reader.get(key)?.value).toEqual({ owner: "new" });
});

it("expired nonfresh unknown generation still parses after markerless observer reuse", async () => {
  const { reader, replace, freeze, count } = await setup();
  await replace((state) => { delete state.readGeneration; });
  freeze();
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  reader.get(key);
  const before = count();
  now += RUNTIME_MESH_READ_CACHE_MS - 1;
  reader.cachedStateStamp(true);
  expect(count()).toBe(before);
  await replace((state) => { state.entries[key]!.value = { owner: "new" }; });
  now += 2;
  expect(reader.get(key)?.value).toEqual({ owner: "new" });
  expect(count()).toBe(before + 1); // Observer neither extends TTL nor waives unknown expiry.
});

it("unchanged public fresh payload calls still perform canonical full reads", async () => {
  const { reader, count } = await setup();
  reader.get(key);
  const before = count();
  reader.get(key, { fresh: true });
  reader.listAll("", { fresh: true });
  reader.listAllShared("", { fresh: true });
  reader.stateToken({ fresh: true });
  expect(count()).toBe(before + 4);
  reader.cachedStateStamp(true);
  expect(count()).toBe(before + 4);
});
