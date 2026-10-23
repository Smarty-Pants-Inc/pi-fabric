import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Keep: SA-04 lost a retained deletion token on the real FILE deferred-effect path.
const roots: string[] = [];
const stores: MeshStore[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "pinned-reader", name: "pinned-reader", kind: "main" };
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-pinned-versions-"));
  roots.push(root);
  const store = new MeshStore(root, 4 * 1024 * 1024, 100, { stateBackend: "file" });
  stores.push(store);
  return store;
};

it("pins live rows and retained tombstones together across delete, recreate and another delete", async () => {
  const store = fixture(), key = "topology/hosts/pinned";
  const first = await store.put({ key, identity, value: { owner: "first" } });
  const live = store.stateToken({ fresh: true });
  const deleted = await store.delete({ key, ifVersion: first.version });
  const gone = store.stateToken({ fresh: true });
  const recreated = await store.put({ key, identity, value: { owner: "second" }, ifVersion: deleted.version! });
  const replacement = store.stateToken({ fresh: true });
  const deletedAgain = await store.delete({ key, ifVersion: recreated.version });
  const goneAgain = store.stateToken({ fresh: true });
  expect(deleted.version).toBeGreaterThan(first.version);
  expect(recreated.version).toBeGreaterThan(deleted.version!);
  expect(deletedAgain.version).toBeGreaterThan(recreated.version);
  for (const [snapshot, version, owner] of [
    [live, first.version, "first"], [gone, deleted.version!, undefined],
    [replacement, recreated.version, "second"], [goneAgain, deletedAgain.version!, undefined],
  ] as const) {
    expect(store.fileSnapshotVersion(snapshot, key)).toBe(version);
    const rows = store.listAllShared("topology/hosts/", { snapshot });
    expect(rows[0]?.value).toEqual(owner === undefined ? undefined : { owner });
    expect(store.get(key, { snapshot })?.version).toBe(owner === undefined ? undefined : version);
  }
  expect(store.fileSnapshotVersion(gone, "never/existed")).toBe(0);
});

it("reads pinned versions without disk I/O or body copies while public reads remain defensive", async () => {
  const store = fixture(), key = "topology/participants/large";
  await store.put({ key, identity, value: { status: "idle", body: "x".repeat(2_500_000) } });
  const snapshot = store.stateToken({ fresh: true });
  const rows = store.listAllShared("topology/participants/", { snapshot });
  const expected = rows[0]!.version;
  const before = fs.readFileSync(path.join(store.root, "state.json"));
  const read = vi.spyOn(fs, "readFileSync"), stat = vi.spyOn(fs, "statSync"), open = vi.spyOn(fs, "openSync");
  const write = vi.spyOn(fs, "writeFileSync"), rename = vi.spyOn(fs, "renameSync");
  const stringify = vi.spyOn(JSON, "stringify"), parse = vi.spyOn(JSON, "parse");
  for (let index = 0; index < 100; index++) {
    expect(store.fileSnapshotVersion(snapshot, key)).toBe(expected);
    expect(store.listAllShared("topology/participants/", { snapshot })[0]).toBe(rows[0]);
  }
  expect([read, stat, open, write, rename, stringify, parse].map(spy => spy.mock.calls.length)).toEqual([0, 0, 0, 0, 0, 0, 0]);
  vi.restoreAllMocks();
  const exposed = store.get(key, { snapshot })!;
  (exposed.value as { status: string }).status = "caller mutation";
  exposed.updatedBy.name = "caller mutation";
  const listed = store.listAll("topology/participants/", { snapshot });
  (listed[0]!.value as { status: string }).status = "list mutation";
  expect(store.get(key, { snapshot })?.value).toMatchObject({ status: "idle" });
  expect(store.get(key, { snapshot })?.updatedBy).toEqual(identity);
  expect(store.listAllShared("topology/participants/", { snapshot })[0]).toBe(rows[0]);
  expect(fs.readFileSync(path.join(store.root, "state.json"))).toEqual(before);
});
