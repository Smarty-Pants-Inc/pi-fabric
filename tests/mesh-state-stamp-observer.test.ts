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
const setup = async (readCacheMs = RUNTIME_MESH_READ_CACHE_MS) => {
  fs.mkdirSync(scratch, { recursive: true });
  const root = fs.mkdtempSync(path.join(scratch, "stamp-observer-"));
  roots.push(root);
  const file = path.join(root, "state.json");
  const writer = new MeshStore(root, 64 * 1024, 100, { writeReadJournal: false });
  const reader = new MeshStore(root, 64 * 1024, 100, { readCacheMs });
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
  return { writer, reader, disk, freeze, replace, count, file };
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

for (const change of ["stat", "generation"] as const) it(`observer defers changed ${change} until the ordinary TTL expires`, async () => {
  const { reader, writer, freeze, replace, count } = await setup();
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  if (change === "generation") freeze();
  const old = reader.get(key);
  const stamp = reader.cachedStateStamp();
  if (change === "stat") await replace((state) => { state.entries[key]!.value = { owner: "larger-new-owner" }; });
  else await writer.put({ key, value: { owner: "new" }, identity });
  const before = count();
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(reader.get(key)).toEqual(old);
  expect(count()).toBe(before);
  now += RUNTIME_MESH_READ_CACHE_MS;
  const next = reader.cachedStateStamp(true);
  if (change === "stat") expect(next).not.toBe(stamp);
  else expect(next).toBe(stamp); // Metadata stamp is not a generation or authority token.
  expect(count()).toBe(before + 1);
  expect(reader.get(key)?.value).toEqual({ owner: change === "stat" ? "larger-new-owner" : "new" });
  expect(reader.cachedStateStamp(true)).toBe(next);
  expect(count()).toBe(before + 1);
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
    if (method !== "confirmWritable") {
      if (method === "get") expect(reader.get(key, { fresh: true })).toEqual(expected);
      else if (method === "stateToken") {
        reader.stateToken({ fresh: true });
        expect(reader.get(key)).toEqual(expected);
      } else expect(reader[method]("topology/participants/", { fresh: true })).toEqual([expected]);
      expect(count()).toBe(before + 2); // Canonical EVERY call, not just on invalidation.
    }
  });
}

it("markerless warm observer uses physical metadata at expiry without re-reading unchanged bytes", async () => {
  const { reader, replace, count } = await setup();
  await replace((state) => { delete state.readGeneration; });
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  reader.get(key);
  const stamp = reader.cachedStateStamp();
  const before = count();
  const headers = vi.spyOn(fs, "readSync");
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(count()).toBe(before);
  expect(headers).not.toHaveBeenCalled();
  now += RUNTIME_MESH_READ_CACHE_MS;
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(count()).toBe(before); // High-resolution physical identity, not an absent UUID, gates reuse.
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(count()).toBe(before);
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

it("markerless to current UUID stays old within TTL and forces same-stat parse at expiry", async () => {
  const { reader, writer, replace, freeze, disk, count } = await setup();
  await replace((state) => { delete state.readGeneration; });
  freeze();
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  reader.get(key);
  const stamp = reader.cachedStateStamp();
  await writer.put({ key, value: { owner: "new" }, identity });
  expect(disk().readGeneration).toMatch(/^[0-9a-f-]{36}$/);
  const before = count();
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(reader.get(key)?.value).toEqual({ owner: "old" });
  expect(count()).toBe(before);
  now += RUNTIME_MESH_READ_CACHE_MS;
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

for (const poll of ["ordinary", "observer"] as const) it(`legacy changed-stat observer coalesces with ${poll} expiry poll`, async () => {
  const { reader, replace, disk, count } = await setup();
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const old = reader.get(key);
  expect(count()).toBe(1); // Warm ordinary parse.
  const parsedAt = now;
  const consumed = reader.cachedStateStamp();
  const generation = disk().readGeneration;
  await replace((state) => { state.entries[key]!.value = { owner: "legacy-larger-new-owner" }; });
  expect(disk().readGeneration).toBe(generation); // Raw legacy replacement copies the marker.
  const onDisk = reader.stateStamp();
  expect(onDisk).not.toBe(consumed);
  const before = count(); // Baseline after raw replacement; disk() reads bypass the spy.
  for (const elapsed of [0, RUNTIME_MESH_READ_CACHE_MS / 2, RUNTIME_MESH_READ_CACHE_MS - 1]) {
    now = parsedAt + elapsed;
    const observed = reader.cachedStateStamp(true);
    expect(count()).toBe(before); // ZERO extra canonical reads, even on changed stat.
    expect(observed).toBe(consumed); // Never label old payload with disk's stamp.
    expect(reader.get(key)).toEqual(old); // UI snapshot in the same window shares the parse.
    expect(count()).toBe(before);
  }
  now += 1; // Exact original TTL boundary, not the last observer call plus TTL.
  if (poll === "ordinary") expect(reader.get(key)?.value).toEqual({ owner: "legacy-larger-new-owner" });
  else expect(reader.cachedStateStamp(true)).toBe(onDisk); // No ordinary poll: bounded observer lag.
  expect(count()).toBe(before + 1);
  expect(reader.cachedStateStamp(true)).toBe(onDisk);
  expect(reader.get(key)?.value).toEqual({ owner: "legacy-larger-new-owner" });
  expect(count()).toBe(before + 1); // Next observer and UI read reuse that ONE expiry parse.
});

it("readCacheMs 0 gates unchanged headers by physical metadata and parses changed stat immediately", async () => {
  const { reader, replace, count } = await setup(0);
  reader.get(key);
  const stamp = reader.cachedStateStamp();
  const before = count();
  const headers = vi.spyOn(fs, "readSync");
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(headers).not.toHaveBeenCalled(); // Same physical file: no repeated 64-byte opens.
  expect(count()).toBe(before);
  await replace((state) => { state.entries[key]!.value = { owner: "larger-new-owner" }; });
  const afterWrite = count();
  expect(reader.cachedStateStamp(true)).not.toBe(stamp);
  expect(count()).toBe(afterWrite + 1);
  expect(reader.get(key)?.value).toEqual({ owner: "larger-new-owner" });
  expect(count()).toBe(afterWrite + 1);
});

it("confirmWritable preserves a snapshot but requires canonical revalidation on its next read", async () => {
  const { reader, freeze, replace, count } = await setup();
  freeze();
  reader.get(key);
  const stamp = reader.cachedStateStamp();
  await replace((state) => { state.entries[key]!.value = { owner: "new" }; });
  const before = count();
  await reader.confirmWritable();
  expect(reader.cachedStateStamp()).toBe(stamp);
  expect(count()).toBe(before);
  expect(reader.cachedStateStamp(true)).toBe(stamp); // Same stat, copied marker: still a full read.
  expect(reader.get(key)?.value).toEqual({ owner: "new" });
  expect(count()).toBe(before + 1);
});

it("header EIO permits within-TTL reuse but requires a canonical parse at expiry", async () => {
  const { reader, freeze, replace, count, file } = await setup();
  freeze();
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const old = reader.get(key);
  const stamp = reader.cachedStateStamp();
  await replace((state) => { state.entries[key]!.value = { owner: "new" }; });
  const before = count();
  // Only reads of state.json count as header reads: the read signal (kernel witness,
  // smarty-dev#4250) may be consulted first and fails closed on the same EIO.
  const stateHeaders: number[] = [];
  const headers = vi.spyOn(fs, "readSync").mockImplementation(((fd: number) => {
    if (fs.fstatSync(fd).ino === real.statSync(file).ino) stateHeaders.push(fd);
    throw Object.assign(new Error("canonical header unavailable"), { code: "EIO" });
  }) as typeof fs.readSync);
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(reader.get(key)).toEqual(old);
  expect(headers).not.toHaveBeenCalled();
  expect(count()).toBe(before);
  now += RUNTIME_MESH_READ_CACHE_MS;
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(stateHeaders).toHaveLength(1);
  expect(count()).toBe(before + 1);
  expect(reader.get(key)?.value).toEqual({ owner: "new" });
  expect(reader.cachedStateStamp(true)).toBe(stamp);
  expect(count()).toBe(before + 1);
});

for (const metadata of ["changed-stat", "same-stat"] as const) it(`explicit remote generation check parses a warm new UUID once with ${metadata}`, async () => {
  const { reader, writer, freeze, count } = await setup();
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  if (metadata === "same-stat") freeze();
  const oldToken = reader.stateToken();
  const oldStamp = reader.cachedStateStamp();
  now += 100; // Well inside the ordinary 2 s reuse window.
  await writer.put({ key, value: { owner: "new" }, identity });
  const before = count(); // Excludes writer's necessary locked read.
  expect(reader.cachedStateStamp(true)).toBe(oldStamp); // True observer semantics do not change.
  expect(reader.get(key)?.value).toEqual({ owner: "old" });
  expect(count()).toBe(before);
  const consumed = reader.cachedStateStamp(true, true);
  if (metadata === "same-stat") expect(consumed).toBe(oldStamp); // Metadata is NOT a generation token.
  else expect(consumed).not.toBe(oldStamp);
  expect(reader.stateToken()).not.toBe(oldToken);
  expect(reader.get(key)?.value).toEqual({ owner: "new" });
  expect(count()).toBe(before + 1);
  expect(reader.cachedStateStamp(true, true)).toBe(consumed);
  expect(reader.cachedStateStamp(true)).toBe(consumed);
  expect(count()).toBe(before + 1); // UI and ordinary readers share the one necessary parse.
});

it("explicit generation checks of stationary state add zero canonical reads after TTL expiry", async () => {
  const { reader, count } = await setup();
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const token = reader.stateToken();
  const stamp = reader.cachedStateStamp();
  const before = count();
  for (const elapsed of [100, 1_999, 2_000, 5_000, 15_000, 30_000]) {
    now += elapsed;
    expect(reader.cachedStateStamp(true, true)).toBe(stamp);
    expect(reader.stateToken()).toBe(token);
    expect(count()).toBe(before);
  }
});

for (const writerMode of ["current", "copied-marker legacy"] as const) it(`explicit generation check coalesces with an ordinary ${writerMode} expiry parse`, async () => {
  const { reader, writer, replace, disk, count } = await setup();
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const oldToken = reader.stateToken();
  const generation = disk().readGeneration;
  if (writerMode === "current") await writer.put({ key, value: { owner: "larger-new-owner" }, identity });
  else {
    await replace((state) => { state.entries[key]!.value = { owner: "legacy-larger-new-owner" }; });
    expect(disk().readGeneration).toBe(generation);
  }
  const before = count();
  now += RUNTIME_MESH_READ_CACHE_MS;
  const currentToken = reader.stateToken(); // Ordinary poll already consumed latest canonical bytes.
  expect(currentToken).not.toBe(oldToken);
  expect(count()).toBe(before + 1);
  const consumed = reader.cachedStateStamp();
  expect(reader.cachedStateStamp(true, true)).toBe(consumed);
  expect(reader.cachedStateStamp(true)).toBe(consumed);
  expect(reader.stateToken()).toBe(currentToken);
  expect(count()).toBe(before + 1); // No forced UI duplicate for either writer kind.
});

for (const marker of ["copied", "unknown", "EIO"] as const) it(`explicit ${marker} generation fallback neither relabels the payload nor extends its TTL`, async () => {
  const { reader, replace, count } = await setup();
  if (marker === "unknown") await replace((state) => { delete state.readGeneration; });
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const oldToken = reader.stateToken();
  const old = reader.get(key);
  const consumed = reader.cachedStateStamp();
  const parsedAt = now;
  await replace((state) => { state.entries[key]!.value = { owner: "legacy-larger-new-owner" }; });
  const onDisk = reader.stateStamp();
  expect(onDisk).not.toBe(consumed);
  const before = count();
  const headers = marker === "EIO" ? vi.spyOn(fs, "readSync").mockImplementation(() => {
    throw Object.assign(new Error("canonical header unavailable"), { code: "EIO" });
  }) : vi.spyOn(fs, "readSync");
  for (const elapsed of [0, RUNTIME_MESH_READ_CACHE_MS / 2, RUNTIME_MESH_READ_CACHE_MS - 1]) {
    now = parsedAt + elapsed;
    expect(reader.cachedStateStamp(true, true)).toBe(consumed);
    expect(reader.stateToken()).toBe(oldToken);
    expect(reader.get(key)).toEqual(old);
    expect(count()).toBe(before); // Changed metadata alone does not force warm-window duplicates.
  }
  expect(headers).toHaveBeenCalled(); // Explicit mode checked the header; default true does not.
  now += 1; // Original TTL boundary, not two seconds after the last explicit observation.
  expect(reader.cachedStateStamp(true, true)).toBe(onDisk);
  expect(reader.get(key)?.value).toEqual({ owner: "legacy-larger-new-owner" });
  expect(reader.stateToken()).not.toBe(oldToken);
  expect(count()).toBe(before + 1);
  expect(reader.cachedStateStamp(true, true)).toBe(onDisk);
  expect(count()).toBe(before + 1);
});

it("unchanged public fresh payload calls share the canonical physical generation", async () => {
  const { reader, count } = await setup();
  reader.get(key);
  const before = count();
  reader.get(key, { fresh: true });
  reader.listAll("", { fresh: true });
  reader.listAllShared("", { fresh: true });
  reader.stateToken({ fresh: true });
  expect(count()).toBe(before);
  reader.cachedStateStamp(true);
  expect(count()).toBe(before);
});
