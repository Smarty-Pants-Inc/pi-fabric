import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { MESH_STATE_BACKEND_KINDS } from "../src/mesh/state-backend-kinds.js";
import { MeshBackendFenceError } from "../src/mesh/backend-fence.js";
import { importMeshState, rollbackMeshState } from "../src/mesh/backend-migration.js";
import { createStateBackend, MESH_STATE_BACKEND_NOT_BUILT_CODE, MeshStateBackendNotBuiltError, resolveMeshStateBackend,
  SqliteStateBackend, STATE_BACKEND_FACTORIES, type StateBackendFactory } from "../src/mesh/state-backend.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshStore, type MeshIdentity, type MeshStoreOptions } from "../src/mesh/store.js";
import { StateFile } from "../src/mesh/state-file.js";

// smarty-dev#7504: the selector is a registry of factories; file/sqlite/shadow pick exactly as before, nats refuses.
const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: MeshStore[] = [];
const natsFactory = STATE_BACKEND_FACTORIES.nats;

const tempRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-selector-"));
  roots.push(root);
  return root;
};
const open = (root: string, options: MeshStoreOptions = {}): MeshStore => {
  const store = new MeshStore(root, 64 * 1024, 1_000, options);
  stores.push(store);
  return store;
};

beforeEach(() => { vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", ""); });
afterEach(() => {
  vi.unstubAllEnvs();
  STATE_BACKEND_FACTORIES.nats = natsFactory;
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("state backend selector registry", () => {
  it("registers exactly one factory per known kind", () => {
    expect(Object.keys(STATE_BACKEND_FACTORIES).sort()).toEqual([...MESH_STATE_BACKEND_KINDS].sort());
  });

  it("resolves file, shadow and sqlite exactly as before", () => {
    const table: [Parameters<typeof resolveMeshStateBackend>[0], string | undefined, string][] = [
      [undefined, undefined, "file"], [undefined, "", "file"], [undefined, "sqlite", "sqlite"], [undefined, " Shadow ", "shadow"],
      [undefined, "FILE", "file"], ["file", "sqlite", "file"], ["sqlite", "file", "sqlite"],
      ["shadow", undefined, "shadow"],
    ];
    for (const [explicit, env, expected] of table) expect(resolveMeshStateBackend(explicit, env)).toBe(expected);
    expect(() => resolveMeshStateBackend("bogus" as "file")).toThrow(/mesh.stateBackend must be file, shadow, sqlite or nats/);
    // pi-fabric#796: an unknown non-empty environment value is refused, no longer ignored.
    expect(() => resolveMeshStateBackend(undefined, "bogus")).toThrow(/PI_FABRIC_MESH_STATE_BACKEND must be/);
  });

  it("opens file by default, sqlite from the environment, and an explicit file over the environment", async () => {
    const root = tempRoot();
    const file = open(path.join(root, "a"));
    expect(file.stateBackend).toBe("file");
    expect(file.stateBackendHandle).toBeInstanceOf(StateFile);
    await file.put({ key: "k", value: 1, identity });
    expect(fs.readdirSync(path.join(root, "a")).filter(name => name.startsWith("state.db"))).toEqual([]);
    expect(fs.existsSync(path.join(root, "b", "state.db"))).toBe(false);

    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "sqlite");
    const sqlite = open(path.join(root, "b"));
    expect(sqlite.stateBackend).toBe("sqlite");
    expect(sqlite.stateBackendHandle).toBeInstanceOf(SqliteStateBackend);
    await sqlite.put({ key: "k", value: 2, identity });
    expect(sqlite.get("k")?.value).toBe(2);
    expect(fs.existsSync(path.join(root, "b", "state.db"))).toBe(true);
    expect(open(path.join(root, "c"), { stateBackend: "file" }).stateBackend).toBe("file");
  });

  it("falls back to file with the reason when a factory reports its kind unavailable", () => {
    const original = STATE_BACKEND_FACTORIES.sqlite;
    STATE_BACKEND_FACTORIES.sqlite = { ...original, unavailable: () => "probe refusal" };
    try {
      const root = tempRoot();
      const backend = createStateBackend({ root, maxEventBytes: 64 * 1024, maxReadEvents: 1_000, lock: undefined as never },
        { stateBackend: "sqlite" });
      expect(backend.kind).toBe("file");
      expect(backend.diagnostics().fallback).toBe("sqlite: probe refusal");
      backend.close();
    } finally { STATE_BACKEND_FACTORIES.sqlite = original; }
  });
});

describe("root-aware backend selection", () => {
  const fixtureDefaultKey = Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures");
  const globals = globalThis as Record<symbol, unknown>;
  let fixtureDefault: unknown;

  // Exercise production selection and initialization, not the suite-wide fresh-SQLite fixture.
  beforeEach(() => {
    fixtureDefault = globals[fixtureDefaultKey];
    delete globals[fixtureDefaultKey];
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", undefined);
  });
  afterEach(() => { globals[fixtureDefaultKey] = fixtureDefault; });

  const seedFileRoot = async (root: string): Promise<void> => {
    const file = open(root, { stateBackend: "file" });
    await file.put({ key: "retained", value: { from: "file" }, identity });
    file.closeState();
  };
  const importedRoot = async (): Promise<string> => {
    const root = tempRoot();
    await seedFileRoot(root);
    await importMeshState(root);
    return root;
  };

  it.skipIf(process.platform === "win32")("defaults to an imported SQLite root, preserving reads and writes through MeshStore and the factory", async () => {
    const root = await importedRoot();
    expect(process.env.PI_FABRIC_MESH_STATE_BACKEND).toBeUndefined();

    const store = open(root);
    expect(store.stateBackend).toBe("sqlite");
    expect(store.stateBackendHandle).toBeInstanceOf(SqliteStateBackend);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
    expect(store.get("retained")?.value).toEqual({ from: "file" });
    await store.put({ key: "after/import", value: { written: "through MeshStore" }, identity });
    store.closeState();

    const backend = createStateBackend({ root, maxEventBytes: 64 * 1024, maxReadEvents: 1_000,
      lock: new MeshLock(root, {}, () => {}) });
    try {
      expect(backend.kind).toBe("sqlite");
      expect(backend.get("retained")?.value).toEqual({ from: "file" });
      expect(backend.get("after/import")?.value).toEqual({ written: "through MeshStore" });
      await backend.put({ key: "after/factory", value: { written: "through factory" }, identity });
      expect(backend.get("after/factory")?.value).toEqual({ written: "through factory" });
    } finally { backend.close(); }
  });

  it.skipIf(process.platform === "win32").each(["intact", "removed"] as const)("refuses an implicit file read with meta.schema %s and an imported root's marker renamed aside", async schemaState => {
    const root = await importedRoot();
    const reader = open(root);
    expect(reader.get("retained", { fresh: true })?.value).toEqual({ from: "file" });
    reader.closeState();
    const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
    let schema: { value: import("node:sqlite").SQLOutputValue } | undefined;
    if (schemaState === "removed") {
      const raw = new sqlite.DatabaseSync(path.join(root, "state.db"));
      try {
        schema = raw.prepare("SELECT value FROM meta WHERE name = 'schema'").get() as { value: import("node:sqlite").SQLOutputValue } | undefined;
        expect(schema).toBeDefined();
        expect(raw.prepare("DELETE FROM meta WHERE name = 'schema'").run().changes).toBe(1);
      } finally { raw.close(); }
    }
    const stateFile = path.join(root, "state.json");
    const markerBackup = path.join(root, "state.json.marker-backup");
    const marker = fs.readFileSync(stateFile);
    const database = fs.readFileSync(path.join(root, "state.db"));
    fs.renameSync(stateFile, markerBackup);

    for (const env of [undefined, " \t "]) {
      vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", env);
      expect(() => resolveMeshStateBackend(undefined, undefined, root)).toThrow(MeshBackendFenceError);
      expect(() => open(root)).toThrow(/state\.json.*state\.db/);
      // This is the reported read path: refusal, never an undefined/empty retained result.
      expect(() => open(root).get("retained", { fresh: true })).toThrow(MeshBackendFenceError);
    }
    // Keep the existing explicit-choice check without opening/reinitializing ambiguous metadata.
    if (schemaState === "intact") {
      const explicit = open(root, { stateBackend: "sqlite" });
      expect(() => explicit.get("retained", { fresh: true })).toThrow(/marker/i);
      explicit.closeState();
    }
    expect(fs.existsSync(stateFile)).toBe(false);
    expect(fs.readFileSync(markerBackup)).toEqual(marker);
    expect(fs.readFileSync(path.join(root, "state.db"))).toEqual(database);

    if (schema) {
      const raw = new sqlite.DatabaseSync(path.join(root, "state.db"));
      try { raw.prepare("INSERT INTO meta(name, value) VALUES ('schema', ?)").run(schema.value); }
      finally { raw.close(); }
    }
    fs.renameSync(markerBackup, stateFile);
    const restored = open(root);
    expect(restored.stateBackend).toBe("sqlite");
    expect(restored.get("retained", { fresh: true })?.value).toEqual({ from: "file" });
  });

  it.each([false, true])("refuses a zero-byte state.db with no state.json (WAL present: %s), independent of metadata", withWal => {
    const root = tempRoot();
    const database = path.join(root, "state.db");
    const wal = path.join(root, "state.db-wal");
    fs.writeFileSync(database, Buffer.alloc(0), { mode: 0o600 });
    if (withWal) fs.writeFileSync(wal, "retained WAL bytes", { mode: 0o600 });
    for (const env of [undefined, " \t "]) {
      vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", env);
      expect(() => resolveMeshStateBackend(undefined, undefined, root)).toThrow(MeshBackendFenceError);
      expect(() => open(root).get("retained", { fresh: true })).toThrow(/state\.json.*state\.db/);
    }
    expect(fs.existsSync(path.join(root, "state.json"))).toBe(false);
    expect(fs.readFileSync(database)).toEqual(Buffer.alloc(0));
    expect(fs.existsSync(wal)).toBe(withWal);
    if (withWal) expect(fs.readFileSync(wal, "utf8")).toBe("retained WAL bytes");
  });

  it("keeps the metadata authority fence when state.json exists and state.db is unreadable", async () => {
    const root = tempRoot();
    await seedFileRoot(root);
    const stateFile = path.join(root, "state.json");
    const envelope = fs.readFileSync(stateFile);
    const database = path.join(root, "state.db");
    const bytes = Buffer.from("unreadable SQLite metadata");
    fs.writeFileSync(database, bytes, { mode: 0o600 });
    expect(() => open(root)).toThrow(MeshBackendFenceError);
    expect(() => open(root)).toThrow(/backend flag is unreadable/);
    expect(() => open(root).get("retained", { fresh: true })).toThrow(/state\.json.*state\.db/);
    expect(fs.readFileSync(stateFile)).toEqual(envelope);
    expect(fs.readFileSync(database)).toEqual(bytes);
  });

  it.each([false, true])("refuses an orphan state.db-wal without changing file state or the WAL (state.json present: %s)", async withFile => {
    const root = tempRoot();
    if (withFile) await seedFileRoot(root);
    const stateFile = path.join(root, "state.json");
    const envelope = withFile ? fs.readFileSync(stateFile) : undefined;
    const wal = path.join(root, "state.db-wal");
    const bytes = Buffer.from("retained WAL bytes");
    fs.writeFileSync(wal, bytes, { mode: 0o600 });
    for (const env of [undefined, " \t "]) {
      vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", env);
      expect(() => open(root)).toThrow(MeshBackendFenceError);
      if (withFile) expect(() => open(root)).toThrow(/state\.db-wal exists without state\.db/);
      expect(() => open(root).get("retained", { fresh: true })).toThrow(/state\.json.*state\.db-wal/);
    }
    expect(fs.existsSync(stateFile)).toBe(withFile);
    if (withFile) expect(fs.readFileSync(stateFile)).toEqual(envelope);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    expect(fs.readFileSync(wal)).toEqual(bytes);
  });

  it.each(["option", "environment"] as const)("allows explicit file from %s on clean and populated file roots", async source => {
    for (const populated of [false, true]) {
      const root = tempRoot();
      if (populated) await seedFileRoot(root);
      if (source === "environment") vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", " FILE ");
      const store = open(root, source === "option" ? { stateBackend: "file" } : {});
      expect(store.stateBackend).toBe("file");
      expect(store.get("retained", { fresh: true })?.value).toEqual(populated ? { from: "file" } : undefined);
      await store.put({ key: "explicit/file", value: true, identity });
      expect(store.get("explicit/file", { fresh: true })?.value).toBe(true);
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    }
  });

  it.skipIf(process.platform === "win32")("allows default and explicit file reads after real rollback with state.db retained", async () => {
    const root = await importedRoot();
    expect((await rollbackMeshState(root)).backend).toBe("file");
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
    for (const source of ["default", "option", "environment"] as const) {
      vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", source === "environment" ? "file" : undefined);
      const store = open(root, source === "option" ? { stateBackend: "file" } : {});
      expect(store.stateBackend).toBe("file");
      expect(store.get("retained", { fresh: true })?.value).toEqual({ from: "file" });
      await store.put({ key: `rollback/${source}`, value: true, identity });
      expect(store.get(`rollback/${source}`, { fresh: true })?.value).toBe(true);
      store.closeState();
    }
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
  });

  it("keeps populated and fresh roots on the default file backend", async () => {
    const populated = tempRoot();
    await seedFileRoot(populated);
    expect(process.env.PI_FABRIC_MESH_STATE_BACKEND).toBeUndefined();
    const file = open(populated);
    expect(file.stateBackend).toBe("file");
    expect(file.stateBackendHandle).toBeInstanceOf(StateFile);
    expect(file.get("retained")?.value).toEqual({ from: "file" });
    await file.put({ key: "still/file", value: true, identity });
    expect(file.get("still/file")?.value).toBe(true);
    expect(fs.existsSync(path.join(populated, "state.db"))).toBe(false);

    const fresh = tempRoot();
    const empty = open(fresh);
    expect(empty.stateBackend).toBe("file");
    expect(empty.stateBackendHandle).toBeInstanceOf(StateFile);
    await empty.put({ key: "fresh/file", value: true, identity });
    expect(fs.existsSync(path.join(fresh, "state.json"))).toBe(true);
    expect(fs.existsSync(path.join(fresh, "state.db"))).toBe(false);
  });

  it("keeps explicit sqlite environment initialization on a fresh root", async () => {
    const root = tempRoot();
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "sqlite");
    const store = open(root);
    expect(store.stateBackend).toBe("sqlite");
    await store.put({ key: "fresh/sqlite", value: true, identity });
    expect(store.get("fresh/sqlite")?.value).toBe(true);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8"))).toMatchObject({
      format: "sqlite", movedTo: "state.db", epoch: 1,
    });
  });

  it.skipIf(process.platform === "win32")("keeps explicit file selection on an imported SQLite root fail-closed", async () => {
    const root = await importedRoot();
    const explicit = open(root, { stateBackend: "file" });
    expect(explicit.stateBackend).toBe("file");
    expect(() => explicit.get("retained")).toThrow(/moved to state\.db/);
    await expect(explicit.put({ key: "must/not/write", value: 1, identity })).rejects.toThrow(/moved to state\.db/);
    explicit.closeState();

    // The environment remains authoritative over root detection when it contradicts the marker.
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "file");
    const fromEnv = open(root);
    expect(fromEnv.stateBackend).toBe("file");
    expect(() => fromEnv.get("retained")).toThrow(/moved to state\.db/);
    await expect(fromEnv.put({ key: "must/not/write/env", value: 1, identity })).rejects.toThrow(/moved to state\.db/);
  });

  it("refuses explicit sqlite selection on populated file roots without creating state.db", async () => {
    const root = tempRoot();
    await seedFileRoot(root);
    const stateFile = fs.readFileSync(path.join(root, "state.json"), "utf8");

    const explicit = open(root, { stateBackend: "sqlite" });
    expect(explicit.stateBackend).toBe("sqlite");
    expect(() => explicit.get("retained")).toThrow(/run `fabric-mesh-backend import --root .+` first/);
    await expect(explicit.put({ key: "must/not/write", value: 1, identity }))
      .rejects.toThrow(/run `fabric-mesh-backend import --root .+` first/);
    explicit.closeState();

    // The environment override must not make populated file state disappear either.
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "sqlite");
    const fromEnv = open(root);
    expect(fromEnv.stateBackend).toBe("sqlite");
    expect(() => fromEnv.get("retained")).toThrow(/run `fabric-mesh-backend import --root .+` first/);
    await expect(fromEnv.put({ key: "must/not/write/env", value: 1, identity }))
      .rejects.toThrow(/run `fabric-mesh-backend import --root .+` first/);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(stateFile);
  });
});

describe("the nats slot", () => {
  it("resolves from the config, the environment and an explicit option", () => {
    expect(resolveMeshStateBackend(undefined, " NATS ")).toBe("nats");
    expect(resolveMeshStateBackend("nats", "file")).toBe("nats");
    expect(normalizeFabricConfig({ mesh: { stateBackend: "nats" } }).mesh.stateBackend).toBe("nats");
    vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "nats");
    expect(normalizeFabricConfig({ mesh: { stateBackend: "sqlite" } }).mesh.stateBackend).toBe("nats");
  });

  it("refuses clearly, without falling back and without writing state", () => {
    const root = tempRoot();
    for (const [options, env] of [[{ stateBackend: "nats" as const }, ""], [{}, "nats"]] as const) {
      vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", env);
      let caught: unknown;
      try { open(path.join(root, `m-${env || "explicit"}`), options); } catch (error) { caught = error; }
      expect(caught).toBeInstanceOf(MeshStateBackendNotBuiltError);
      expect((caught as MeshStateBackendNotBuiltError).code).toBe(MESH_STATE_BACKEND_NOT_BUILT_CODE);
      expect((caught as Error).message).toMatch(/mesh state backend "nats" is not built/);
    }
    const written = fs.readdirSync(root, { recursive: true }).map(String).filter(name => /state\.(json|db)/.test(name));
    expect(written).toEqual([]);
  });

  it("uses a registered nats factory once one exists", () => {
    const registered: StateBackendFactory = { create: (context, options) => Object.assign(new StateFile(context, options), { probe: true }) };
    STATE_BACKEND_FACTORIES.nats = registered;
    const store = open(tempRoot(), { stateBackend: "nats" });
    expect((store.stateBackendHandle as unknown as { probe?: boolean }).probe).toBe(true);
  });
});
