import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { importMeshState } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { ShadowStateBackend } from "../src/mesh/state-backend.js";
import { StateProjector } from "../src/mesh/state-projector.js";
import { MeshStateUnsupportedError, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity, type MeshStoreOptions } from "../src/mesh/store.js";

// smarty-dev#6477 P0: a sqlite-mode open of a root whose state still lives in state.json must refuse,
// with no side effect, instead of creating an empty authoritative state.db that hides the state and
// fences every file-mode writer at epoch 1.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];
const sqliteStores: SqliteStateStore[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-unimported-${label}-`));
  roots.push(root);
  return root;
};

const open = (root: string, options: MeshStoreOptions = {}): MeshStore => {
  const store = new MeshStore(root, 64 * 1024, 1_000, options);
  stores.push(store);
  return store;
};

afterEach(() => {
  vi.unstubAllEnvs();
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed by the test */ }
  for (const store of sqliteStores.splice(0)) try { store.close(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

/** Every file under root with its content hash: byte identity of the whole tree. */
const snapshot = (root: string): Record<string, string> => {
  const out: Record<string, string> = {};
  const walk = (directory: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) walk(file);
      else out[path.relative(root, file)] = crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
    }
  };
  walk(root);
  return out;
};

const seedFileRoot = async (root: string, count = 25): Promise<void> => {
  const store = open(root, { stateBackend: "file" });
  for (let index = 0; index < count; index += 1) await store.put({ key: `k/${index}`, value: { index }, identity });
  await store.delete({ key: "k/0" });
  store.closeState();
};

const refusal = /this root has file-backend state; run `fabric-mesh-backend import --root .+` first \(smarty-dev#6477\)/;

describe("sqlite mode on an unimported root (smarty-dev#6477)", () => {
  it("refuses a populated file root without creating state.db or touching any file", async () => {
    const root = tempRoot("populated");
    await seedFileRoot(root);
    const before = snapshot(root);
    expect(before["state.json"]).toBeDefined();

    const sqlite = open(root, { stateBackend: "sqlite" });
    expect(() => sqlite.listAll("")).toThrow(MeshStateUnsupportedError);
    expect(() => sqlite.get("k/1")).toThrow(refusal);
    await expect(sqlite.put({ key: "x", value: 1, identity })).rejects.toThrow(refusal);
    await expect(sqlite.writeBatch({ identity, ops: [{ kind: "put", key: "y", value: 1 }] })).rejects.toThrow(MeshStateUnsupportedError);
    sqlite.closeState();
    // The store layer directly, both open paths, and the projector in sqlite mode.
    await expect(SqliteStateStore.open(root, 64 * 1024, 1_000)).rejects.toThrow(refusal);
    expect(() => SqliteStateStore.openSync(root, 64 * 1024, 1_000)).toThrow(refusal);
    await expect(StateProjector.open({ root, mode: "sqlite" })).rejects.toThrow(refusal);

    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    expect(snapshot(root)).toEqual(before);
    // No fence: a file-mode writer still commits, and every entry is still there.
    const file = open(root, { stateBackend: "file" });
    await file.put({ key: "after", value: 1, identity });
    expect(file.listAll("").length).toBe(25);
  });

  it("refuses through the environment override too", async () => {
    const root = tempRoot("env");
    await seedFileRoot(root, 3);
    const before = snapshot(root);
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "sqlite");
    const sqlite = open(root);
    expect(sqlite.stateBackend).toBe("sqlite");
    expect(() => sqlite.listAll("")).toThrow(refusal);
    // The advisory writer-census record (any backend's MeshStore writes one) goes with closeState.
    sqlite.closeState();
    expect(snapshot(root)).toEqual(before);
  });

  it("refuses a tombstone-only state.json and a damaged one", async () => {
    const tombstones = tempRoot("tombstones");
    const store = open(tombstones, { stateBackend: "file" });
    await store.put({ key: "gone", value: 1, identity });
    await store.delete({ key: "gone" });
    store.closeState();
    expect(() => SqliteStateStore.openSync(tombstones, 64 * 1024, 1_000)).toThrow(refusal);
    const damaged = tempRoot("damaged");
    fs.writeFileSync(path.join(damaged, "state.json"), "{\"format\":1,\"entries\":{\"a\"");
    expect(() => SqliteStateStore.openSync(damaged, 64 * 1024, 1_000)).toThrow(refusal);
    expect(fs.existsSync(path.join(tombstones, "state.db"))).toBe(false);
    expect(fs.existsSync(path.join(damaged, "state.db"))).toBe(false);
  });

  it("initialises a fresh root: no state.json, an empty one, or a file store's untouched state", async () => {
    const fresh = tempRoot("fresh");
    const store = open(fresh, { stateBackend: "sqlite" });
    expect(store.listAll("")).toEqual([]);
    expect((await store.put({ key: "a", value: 1, identity })).version).toBe(1);
    expect(fs.existsSync(path.join(fresh, "state.db"))).toBe(true);

    const emptyJson = tempRoot("empty-json");
    fs.writeFileSync(path.join(emptyJson, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }));
    const zeroLength = tempRoot("zero-length");
    fs.writeFileSync(path.join(zeroLength, "state.json"), "");
    const touched = tempRoot("touched");
    const file = open(touched, { stateBackend: "file" });
    expect(file.listAll("")).toEqual([]);
    file.closeState();
    for (const root of [emptyJson, zeroLength, touched]) {
      const sqlite = open(root, { stateBackend: "sqlite" });
      await sqlite.put({ key: "a", value: 1, identity });
      expect(sqlite.listAll("").map((entry) => entry.key)).toEqual(["a"]);
    }
  });

  it("opens an imported root, and the import CLI still imports a populated root", async () => {
    const imported = tempRoot("imported");
    await seedFileRoot(imported);
    await importMeshState(imported);
    const sqlite = open(imported, { stateBackend: "sqlite" });
    expect(sqlite.listAll("").length).toBe(24);
    await sqlite.put({ key: "after", value: 1, identity });
    expect(sqlite.listAll("").length).toBe(25);

    const viaCli = tempRoot("cli");
    await seedFileRoot(viaCli);
    let err = "";
    const code = await main(["import", "--root", viaCli], { stdout: () => {}, stderr: (text) => { err += text; } });
    expect(code, err).toBe(0);
    const after = open(viaCli, { stateBackend: "sqlite" });
    expect(after.listAll("").length).toBe(24);
  });

  it("keeps the shadow backend working on a populated file root", async () => {
    const root = tempRoot("shadow");
    await seedFileRoot(root, 4);
    const store = open(root, { stateBackend: "shadow" });
    const shadow = store.stateBackendHandle;
    if (!(shadow instanceof ShadowStateBackend)) throw new Error(`not a shadow backend: ${shadow.kind}`);
    await store.put({ key: "s", value: 1, identity });
    await shadow.flush();
    expect(await shadow.verify()).toEqual([]);
    expect(shadow.shadow.listAll("").length).toBe(4);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
  });
});
