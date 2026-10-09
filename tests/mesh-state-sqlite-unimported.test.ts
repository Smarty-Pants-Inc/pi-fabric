import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { assertFileStateWritable, encodeMeshStateMovedMarker, MeshBackendFenceError, readMeshStateMovedMarker } from "../src/mesh/backend-fence.js";
import { importMeshState } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { ShadowStateBackend } from "../src/mesh/state-backend.js";
import { StateProjector } from "../src/mesh/state-projector.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
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

// The production rule: no suite-wide fixture default (tests/fleet-isolation-setup.ts) in this file.
const FIXTURE_DEFAULT = Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures");
const globals = globalThis as Record<symbol, unknown>;
let fixtureDefault: unknown;
beforeEach(() => { fixtureDefault = globals[FIXTURE_DEFAULT]; delete globals[FIXTURE_DEFAULT]; });

const MARKER_HOOK = Symbol.for("pi-fabric.mesh.sqlite-create.marker-hook");
afterEach(() => {
  globals[FIXTURE_DEFAULT] = fixtureDefault;
  delete globals[MARKER_HOOK];
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
const notImported = /run `fabric-mesh-backend import --root .+` first \(smarty-dev#6477\)/;

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

  /** Fresh roots: none, an empty state file, a zero-length one, a file store's untouched root, a missing directory. */
  const freshRoots = (): [string, string, string, string] => {
    const none = tempRoot("fresh");
    const emptyJson = tempRoot("empty-json");
    fs.writeFileSync(path.join(emptyJson, "state.json"), JSON.stringify({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 }));
    const zeroLength = tempRoot("zero-length");
    fs.writeFileSync(path.join(zeroLength, "state.json"), "");
    const touched = tempRoot("touched");
    const file = open(touched, { stateBackend: "file" });
    expect(file.listAll("")).toEqual([]);
    file.closeState();
    return [none, emptyJson, zeroLength, touched];
  };

  it("refuses a fresh root with no side effects: no state.db, no fence, a file-mode writer still works (part 2)", async () => {
    const missing = path.join(tempRoot("parent"), "not-yet");
    expect(() => SqliteStateStore.openSync(missing, 64 * 1024, 1_000)).toThrow(notImported);
    expect(fs.existsSync(missing)).toBe(false);
    for (const root of freshRoots()) {
      const before = snapshot(root);
      const sqlite = open(root, { stateBackend: "sqlite" });
      expect(() => sqlite.listAll("")).toThrow(notImported);
      await expect(sqlite.put({ key: "a", value: 1, identity })).rejects.toThrow(MeshStateUnsupportedError);
      sqlite.closeState();
      await expect(SqliteStateStore.open(root, 64 * 1024, 1_000)).rejects.toThrow(notImported);
      expect(() => SqliteStateStore.openSync(root, 64 * 1024, 1_000)).toThrow(notImported);
      await expect(StateProjector.open({ root, mode: "sqlite" })).rejects.toThrow(notImported);
      expect(fs.existsSync(path.join(root, "state.db")), root).toBe(false);
      expect(snapshot(root)).toEqual(before);
      // No fence was created: a file-mode writer that starts later commits. (The file store itself reads a
      // zero-length state.json as damage on write, before and after this change: that root is skipped.)
      if (fs.statSync(path.join(root, "state.json"), { throwIfNoEntry: false })?.size === 0) continue;
      const file = open(root, { stateBackend: "file" });
      expect((await file.put({ key: "f", value: 1, identity })).version).toBeGreaterThan(0);
      expect(file.listAll("").map((entry) => entry.key)).toEqual(["f"]);
    }
  });

  it("imports a fresh root (no state.json, empty, zero-length) to an empty db plus the marker at epoch 1; then sqlite works", async () => {
    const [none, emptyJson, zeroLength, touched] = freshRoots();
    for (const root of [none, emptyJson, zeroLength]) {
      const result = await importMeshState(root);
      expect(result).toMatchObject({ epoch: 1, entries: 0 });
      expect(JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8"))).toMatchObject({ format: "sqlite", movedTo: "state.db", epoch: 1 });
      const sqlite = open(root, { stateBackend: "sqlite" });
      expect(sqlite.listAll("")).toEqual([]);
      expect((await sqlite.put({ key: "a", value: 1, identity })).version).toBe(1);
      expect(sqlite.listAll("").map((entry) => entry.key)).toEqual(["a"]);
    }
    let err = "";
    const code = await main(["import", "--root", touched], { stdout: () => {}, stderr: (text) => { err += text; } });
    expect(code, err).toBe(0);
    const viaCli = open(touched, { stateBackend: "sqlite" });
    await viaCli.put({ key: "b", value: 1, identity });
    expect(viaCli.listAll("").map((entry) => entry.key)).toEqual(["b"]);
  }, 60_000); // four fenced imports (fsync at synchronous=FULL): 19 s once on a host at load 21

  it("refuses state.db without the marker, and the marker without state.db", async () => {
    // An old release's fence: backend=sqlite at epoch 1 over a fresh root, without any import.
    const unmarked = tempRoot("unmarked");
    SqliteStateStore.openSync(unmarked, 64 * 1024, 1_000, { initialize: "detached" }).close();
    expect(fs.existsSync(path.join(unmarked, "state.json"))).toBe(false);
    const dbBytes = fs.readFileSync(path.join(unmarked, "state.db"));
    expect(() => SqliteStateStore.openSync(unmarked, 64 * 1024, 1_000)).toThrow(/backend=sqlite but state\.json is not the moved marker/);
    await expect(SqliteStateStore.open(unmarked, 64 * 1024, 1_000)).rejects.toThrow(notImported);
    const sqlite = open(unmarked, { stateBackend: "sqlite" });
    expect(() => sqlite.listAll("")).toThrow(MeshStateUnsupportedError);
    sqlite.closeState();
    expect(fs.existsSync(path.join(unmarked, "state.json"))).toBe(false);
    expect(fs.readFileSync(path.join(unmarked, "state.db")).equals(dbBytes)).toBe(true);

    // The marker with state.db lost.
    const lost = tempRoot("lost");
    fs.writeFileSync(path.join(lost, "state.json"), encodeMeshStateMovedMarker(3));
    const before = snapshot(lost);
    expect(() => SqliteStateStore.openSync(lost, 64 * 1024, 1_000)).toThrow(/moved marker but state\.db is missing or empty/);
    await expect(SqliteStateStore.open(lost, 64 * 1024, 1_000)).rejects.toThrow(/moved marker but state\.db is missing/);
    expect(snapshot(lost)).toEqual(before);
    // A zero-length state.db with the marker: refused the same way, nothing seeded.
    fs.writeFileSync(path.join(lost, "state.db"), "");
    expect(() => SqliteStateStore.openSync(lost, 64 * 1024, 1_000)).toThrow(/moved marker but state\.db is missing or empty/);
    expect(fs.statSync(path.join(lost, "state.db")).size).toBe(0);
  });

  it("lets test fixtures initialise a fresh root explicitly (initialize: \"create\"), never over file state", async () => {
    const fresh = tempRoot("create");
    const store = SqliteStateStore.openSync(fresh, 64 * 1024, 1_000, { initialize: "create" });
    sqliteStores.push(store);
    expect(JSON.parse(fs.readFileSync(path.join(fresh, "state.json"), "utf8"))).toMatchObject({ format: "sqlite", movedTo: "state.db", epoch: 1 });
    // Now it is an imported-shaped root: the default open works.
    const sqlite = open(fresh, { stateBackend: "sqlite" });
    expect((await sqlite.put({ key: "a", value: 1, identity })).version).toBe(1);
    const populated = tempRoot("create-populated");
    await seedFileRoot(populated, 2);
    expect(() => SqliteStateStore.openSync(populated, 64 * 1024, 1_000, { initialize: "create" })).toThrow(refusal);
    expect(fs.existsSync(path.join(populated, "state.db"))).toBe(false);
  });

  describe("initialize: \"create\" only on a genuinely fresh root (review/astra P1)", () => {
    const create = (root: string) => SqliteStateStore.openSync(root, 64 * 1024, 1_000, { initialize: "create" });
    const createAsync = (root: string) => SqliteStateStore.open(root, 64 * 1024, 1_000, { initialize: "create" });
    const unmarkedDb = /state\.db says backend=sqlite but state\.json is not the moved marker, so no import created it/;

    it("(1) refuses a nonempty state.db without the marker, bytes unchanged", async () => {
      const root = tempRoot("create-unmarked-db");
      SqliteStateStore.openSync(root, 64 * 1024, 1_000, { initialize: "detached" }).close();
      const before = snapshot(root);
      expect(() => create(root)).toThrow(unmarkedDb);
      await expect(createAsync(root)).rejects.toThrow(unmarkedDb);
      expect(fs.existsSync(path.join(root, "state.json"))).toBe(false);
      expect(snapshot(root)).toEqual(before);
      // An empty state.json beside it changes nothing: the database is still not an imported one.
      fs.writeFileSync(path.join(root, "state.json"), "");
      expect(() => create(root)).toThrow(unmarkedDb);
      expect(fs.statSync(path.join(root, "state.json")).size).toBe(0);
      const withEmpty = snapshot(root);
      await expect(createAsync(root)).rejects.toThrow(unmarkedDb);
      expect(snapshot(root)).toEqual(withEmpty);
    });

    it("(2) refuses a populated state.json beside an existing state.db, state.json unchanged", async () => {
      const root = tempRoot("create-populated-db");
      await seedFileRoot(root, 3);
      // An old release's fence (or a stray copy) over the populated root.
      const db = SqliteStateStore.openSync(root, 64 * 1024, 1_000, { initialize: "detached" });
      db.close();
      const before = snapshot(root);
      expect(() => create(root)).toThrow(/file-backend state in state\.json and an existing state\.db/);
      await expect(createAsync(root)).rejects.toThrow(MeshStateUnsupportedError);
      expect(snapshot(root)).toEqual(before);
    });

    it("(3) refuses a populated state.json with no db, and the marker without its db", async () => {
      const root = tempRoot("create-populated");
      await seedFileRoot(root, 2);
      const before = snapshot(root);
      expect(() => create(root)).toThrow(refusal);
      await expect(createAsync(root)).rejects.toThrow(refusal);
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
      expect(snapshot(root)).toEqual(before);
      const lost = tempRoot("create-lost");
      fs.writeFileSync(path.join(lost, "state.json"), encodeMeshStateMovedMarker(4));
      const lostBefore = snapshot(lost);
      expect(() => create(lost)).toThrow(/moved marker but state\.db is missing or empty/);
      expect(snapshot(lost)).toEqual(lostBefore);
    });

    it("(4) initialises a fresh root (absent, empty or zero-length state.json, zero-length db); an imported root opens normally", async () => {
      const zeroDb = tempRoot("create-zero-db");
      fs.writeFileSync(path.join(zeroDb, "state.db"), "");
      const [none, emptyJson, zeroLength] = freshRoots();
      for (const root of [none, emptyJson, zeroLength, zeroDb]) {
        sqliteStores.push(create(root));
        expect(JSON.parse(fs.readFileSync(path.join(root, "state.json"), "utf8")), root)
          .toMatchObject({ format: "sqlite", movedTo: "state.db", epoch: 1 });
        expect(fs.readdirSync(root).filter((name) => /\.(tmp|aside)$/.test(name))).toEqual([]);
      }
      // "create" again over the now-imported root is a default open: same database, marker untouched.
      const marker = fs.readFileSync(path.join(none, "state.json"));
      const first = sqliteStores[0];
      if (!first) throw new Error("no store was opened");
      await first.put({ key: "a", value: 1, identity });
      const again = create(none);
      sqliteStores.push(again);
      expect(again.get("a")?.value).toBe(1);
      expect(fs.readFileSync(path.join(none, "state.json")).equals(marker)).toBe(true);
    });

    it("(5) refuses when state.json is populated between the guard and the marker write; state.json unchanged", async () => {
      const populated = JSON.stringify({ format: 1, revisionFormat: 2, entries: { live: { key: "live", value: 1, version: 1 } }, highWater: 1 });
      const replace = (root: string): void => {
        // A file-mode writer's atomic write: temp then rename over the name.
        const temp = path.join(root, "state.json.writer.tmp");
        fs.writeFileSync(temp, populated);
        fs.renameSync(temp, path.join(root, "state.json"));
      };
      const cases = [
        ...["before-marker", "before-claim"].flatMap((phase) => (["absent", "zero-length", "empty"] as const).map((start) => [phase, start] as const)),
        // The claim itself: the inode renamed aside is the populated one, so it is linked back unchanged.
        ...(["zero-length", "empty"] as const).map((start) => ["before-rename", start] as const),
      ];
      for (const [phase, start] of cases) {
        {
          const root = tempRoot(`create-race-${phase}-${start}`);
          if (start === "zero-length") fs.writeFileSync(path.join(root, "state.json"), "");
          if (start === "empty") fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ format: 1, entries: {}, highWater: 0 }));
          let fired = 0;
          globals[MARKER_HOOK] = (at: string, hooked: string) => { if (at === phase && hooked === root) { fired += 1; replace(root); } };
          expect(() => create(root), `${phase}/${start}`).toThrow(/initialize "create" raced/);
          expect(fired).toBe(1);
          expect(fs.readFileSync(path.join(root, "state.json"), "utf8"), `${phase}/${start}`).toBe(populated);
          expect(fs.readdirSync(root).filter((name) => /\.(tmp|aside)$/.test(name))).toEqual([]);
          delete globals[MARKER_HOOK];
        }
      }
    });

    it("(6) is serialized with a file-mode writer that read the fresh state.json first: one authority (review round 2)", async () => {
      const populated = JSON.stringify({ format: 1, revisionFormat: 2, entries: { live: { key: "live", value: 1, version: 1 } }, highWater: 1 });
      const stateJson = (root: string): string => path.join(root, "state.json");
      const seed = (label: string, start: "absent" | "zero-length" | "empty"): string => {
        const root = tempRoot(`create-writer-${label}-${start}`);
        if (start === "zero-length") fs.writeFileSync(stateJson(root), "");
        if (start === "empty") fs.writeFileSync(stateJson(root), JSON.stringify({ format: 1, entries: {}, highWater: 0 }));
        return root;
      };
      // A file-mode writer's commit, as StateFile does it under .lock: the fence check, then temp and rename over state.json.
      const commit = (root: string): void => {
        assertFileStateWritable(root);
        const temp = path.join(root, "state.json.writer.tmp");
        fs.writeFileSync(temp, populated);
        fs.renameSync(temp, stateJson(root));
      };
      const fresh = (root: string): boolean => {
        const text = fs.existsSync(stateJson(root)) ? fs.readFileSync(stateJson(root), "utf8") : "";
        return text === "" || Object.keys((JSON.parse(text) as { entries: object }).entries).length === 0;
      };
      const meshLock = (root: string): MeshLock => new MeshLock(root, { lockProtocol: 1, lockTimeoutMs: 10_000 }, () => undefined);
      for (const start of ["absent", "zero-length", "empty"] as const) {
        // Happen-before: the writer holds .lock from its read through its rename. Create waits on the fence, then
        // its re-checked guard sees the writer's state and refuses: state.json is the only authority, no state.db.
        const first = seed("first", start);
        let creating: Promise<unknown> | undefined;
        await meshLock(first).withLockAcrossAwait(async () => {
          expect(fresh(first)).toBe(true);
          creating = createAsync(first).then((store) => { sqliteStores.push(store); return store; }, (error: unknown) => error);
          await new Promise((resolve) => setTimeout(resolve, 150));
          expect(fs.existsSync(path.join(first, "state.db")), start).toBe(false);
          commit(first);
        });
        const outcome = await creating;
        expect(outcome, start).toBeInstanceOf(MeshStateUnsupportedError);
        expect((outcome as Error).message).toMatch(refusal);
        expect(fs.readFileSync(stateJson(first), "utf8")).toBe(populated);
        expect(fs.existsSync(path.join(first, "state.db"))).toBe(false);

        // The synchronous create cannot enter while the writer holds .lock either: a busy refusal, no side effect.
        const blocked = seed("sync", start);
        await meshLock(blocked).withLockAcrossAwait(async () => {
          expect(() => SqliteStateStore.openSync(blocked, 64 * 1024, 1_000, { initialize: "create" })).toThrow(MeshLockTimeoutError);
          expect(fs.existsSync(path.join(blocked, "state.db"))).toBe(false);
          expect(readMeshStateMovedMarker(blocked)).toBeUndefined();
          commit(blocked);
        });
        expect(() => create(blocked)).toThrow(refusal);
        expect(fs.readFileSync(stateJson(blocked), "utf8")).toBe(populated);

        // Fenced: the writer read the fresh state.json BEFORE the create, which then ran; its later commit is
        // refused by the fence and the marker stays: SQLite is the only authority.
        const late = seed("late", start);
        expect(fresh(late)).toBe(true);
        const store = await createAsync(late);
        sqliteStores.push(store);
        await expect(meshLock(late).withLock(() => commit(late))).rejects.toThrow(MeshBackendFenceError);
        await expect(open(late, { stateBackend: "file" }).put({ key: "f", value: 1, identity })).rejects.toThrow();
        expect(readMeshStateMovedMarker(late)).toMatchObject({ epoch: 1 });
        expect(fs.existsSync(path.join(late, "state.json.writer.tmp"))).toBe(false);
      }
    });
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
