import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { acquireMeshCustodyLock } from "../src/mesh/custody-lock.js";
import { abortMeshRollback, assertFileStateWritable, cutoverMeshState, describeCensusAdvisory, importMeshState, meshBackendStatus, MeshBackendFenceError,
  MeshBackendRefusedError, MeshCutoverFailedError, meshSnapshotDigest, readMeshStateMovedMarker, readStateFileEpoch, resolveMeshStateSource, rollbackMeshState,
  type MeshBackendAlarm, type MeshCensusAdvisory, type MeshStateSnapshot } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { assertMeshStateReadable, decodeMeshStateFile, encodeMeshStateFile, type MeshStateFile } from "../src/mesh/state-file.js";
import { legacyReadState } from "./fixtures/legacy-mesh-state-04930dfd.js";
import { MeshStateRetiredError, openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshBatchOperation, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#6477 L4b: import, cutover and rollback with the R1 fence (docs/mesh-lock-plan.md section 5).

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];
const stores: SqliteStateStore[] = [];

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-backend-${label}-`));
  roots.push(root);
  return root;
};

const children: ChildProcessWithoutNullStreams[] = [];

afterEach(() => {
  for (const child of children.splice(0)) if (child.exitCode === null) child.kill("SIGKILL");
  for (const store of stores.splice(0)) try { store.close(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const openSqlite = async (root: string): Promise<SqliteStateStore> => {
  const store = await SqliteStateStore.open(root, 64 * 1024, 1_000);
  stores.push(store);
  return store;
};

const fileStore = (root: string): MeshStore => new MeshStore(root, 64 * 1024, 1_000);

/** A file-backend root with entries, nested values and CAS tombstones. */
const seedFileRoot = async (root: string, count = 30): Promise<MeshStore> => {
  const store = fileStore(root);
  const ops: MeshBatchOperation[] = Array.from({ length: count }, (_, index) => ({
    kind: "put", key: `seed/ns${index % 3}/${index}`, value: { index, nested: { list: [index, "x"], flag: index % 2 === 0 } },
  }));
  await store.writeBatch({ identity, ops });
  await store.put({ key: "seed/solo", value: "plain", identity });
  for (const index of [3, 7, 11]) await store.delete({ key: `seed/ns${index % 3}/${index}` });
  return store;
};

const decodeFile = (root: string): MeshStateSnapshot & { backendEpoch?: number; backendDigest?: string; readGeneration?: string } =>
  decodeMeshStateFile(path.join(root, "state.json"), 64 * 1024 * 1024, false) as never;

/** The state.json that import/cutover retired at epoch `epoch` (state.json itself is now the moved marker). */
const retiredFile = (root: string, epoch = 1): MeshStateSnapshot & { readGeneration?: string } =>
  decodeMeshStateFile(path.join(root, `state.json.cutover-${epoch}`), 64 * 1024 * 1024, false) as never;

const exportTemps = (root: string): string[] =>
  fs.readdirSync(root).filter(name => name.startsWith("state.json.mesh-backend-") || /^state\.json\.rollback-\d+\.tmp$/.test(name));

const rawMeta = (root: string): Record<string, unknown> => {
  const db = openNodeSqlite(path.join(root, "state.db"));
  try { return Object.fromEntries(db.prepare("SELECT name, value FROM meta").all().map(row => [String(row.name), row.value])); }
  finally { db.close(); }
};

const setRawMeta = (root: string, name: string, value: string | number): void => {
  const db = openNodeSqlite(path.join(root, "state.db"));
  try { db.prepare("UPDATE meta SET value = ? WHERE name = ?").run(value, name); }
  finally { db.close(); }
};

const sorted = <T extends { key: string }>(entries: T[]): T[] => [...entries].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));

describe("mesh backend import", () => {
  it("imports state.json into state.db in one verified transaction and converges on rerun", async () => {
    const root = tempRoot("import");
    const files = await seedFileRoot(root);
    const before = await meshBackendStatus(root);
    expect(before).toMatchObject({ backend: "none", epoch: 0, fileEpoch: 0, reader: { source: "file" }, fenceHolds: true });
    const fileDigest = meshSnapshotDigest(decodeFile(root));
    const fileEntries = sorted(files.listAll("", { fresh: true }));

    // Import commits backend=sqlite, so it is the cutover section (review round 4): fenced on .lock and
    // custody.lock; no census provider is needed (the census is advisory, smarty-dev#6982).
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    const imported = await importMeshState(root);
    expect(imported).not.toHaveProperty("census");
    expect(imported).toMatchObject({ backend: "sqlite", epoch: 1, previousEpoch: 0, entries: 28, tombstones: 3, digest: fileDigest, converged: false,
      stateCopy: path.join(root, "state.json.cutover-1") });
    expect(rawMeta(root)).toMatchObject({ backend: "sqlite", epoch: 1, import_digest: fileDigest });
    expect(readMeshStateMovedMarker(root)).toMatchObject({ epoch: 1 });
    expect(resolveMeshStateSource(root)).toEqual({ source: "sqlite", backend: "sqlite", epoch: 1, fileEpoch: 1 });

    const sqlite = await openSqlite(root);
    expect(sorted(sqlite.listAll(""))).toEqual(fileEntries);
    // CAS tombstones and the clock survived: a stale ifVersion on a deleted key conflicts, a new write is above the clock.
    const tombstoned = retiredFile(root).versions!["seed/ns0/3"]!;
    expect(sqlite.exportState().versions["seed/ns0/3"]).toBe(tombstoned);
    const written = await sqlite.put({ key: "seed/new", value: 1, identity });
    expect(written.version).toBeGreaterThan(retiredFile(root).highWater!);

    const status = await meshBackendStatus(root);
    expect(status).toMatchObject({ backend: "sqlite", epoch: 1, fileEpoch: 1, fileMoved: true, reader: { source: "sqlite" }, fenceHolds: true, importDigest: fileDigest });
    expect(status.dbDigest).not.toBe(fileDigest); // the sqlite write above moved the database on
  });

  it("reruns a committed import as a verification only, and refuses a diverged sqlite root", async () => {
    const root = tempRoot("rerun");
    await seedFileRoot(root);
    const first = await importMeshState(root);
    const again = await importMeshState(root);
    expect(again).toMatchObject({ epoch: first.epoch, digest: first.digest, converged: true });
    const sqlite = await openSqlite(root);
    await sqlite.put({ key: "live/x", value: "sqlite-only", identity });
    await expect(importMeshState(root)).rejects.toThrow(MeshBackendRefusedError);
    expect(rawMeta(root)).toMatchObject({ backend: "sqlite", epoch: 1 });
  });

  it("writes the schema and the byte accounting exactly as state-sqlite.ts does", async () => {
    const source = tempRoot("parity-src");
    await seedFileRoot(source);
    const viaTool = tempRoot("parity-tool");
    fs.copyFileSync(path.join(source, "state.json"), path.join(viaTool, "state.json"));
    await importMeshState(viaTool);
    const viaStore = tempRoot("parity-store");
    const store = await openSqlite(viaStore);
    const file = decodeFile(source);
    await store.importState({ entries: file.entries, versions: file.versions!, tombstoneOrder: file.tombstoneOrder!, highWater: file.highWater! });
    store.close();
    const schema = (root: string): string[] => {
      const db = openNodeSqlite(path.join(root, "state.db"));
      try { return db.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name").all().map(row => String(row.sql).replace(/\s+/g, " ")); }
      finally { db.close(); }
    };
    expect(schema(viaTool)).toEqual(schema(viaStore));
    const tool = rawMeta(viaTool);
    const native = rawMeta(viaStore);
    for (const name of ["schema", "state_bytes", "high_water", "tombstone_ord"]) expect([name, tool[name]]).toEqual([name, native[name]]);
    const reopened = await openSqlite(viaTool);
    expect(meshSnapshotDigest(reopened.exportState())).toBe(meshSnapshotDigest(file));
  });
});

describe("mesh backend rollback fence (R1)", () => {
  it("runs flag, export, verify and switch in order; epochs only grow; old stores fail closed; roll forward re-imports", async () => {
    const root = tempRoot("rollback");
    await seedFileRoot(root);
    expect((await importMeshState(root)).epoch).toBe(1);
    const live = await openSqlite(root);
    await live.put({ key: "live/after-import", value: { from: "sqlite" }, identity });

    // The census never gates a rollback: a reported writer is advisory only (smarty-dev#6982).
    const advised: MeshCensusAdvisory[] = [];
    const steps: string[] = [];
    const rolled = await rollbackMeshState(root, { census: async () => ({ writers: [{ pid: 4242, release: "3.2.0", mode: "sqlite" }] }),
      onAdvisory: advisory => { advised.push(advisory); }, onStep: step => { steps.push(step); } });
    expect(advised).toEqual([{ writers: [{ pid: 4242, release: "3.2.0", mode: "sqlite" }], unknown: [] }]);
    expect(rolled.census).toEqual(advised[0]);
    expect(steps).toEqual(["rollback-flag", "rollback-export-temp", "rollback-verify", "rollback-switch", "rollback-replace"]);
    expect(rolled).toMatchObject({ backend: "file", epoch: 2, steps: [1, 2, 3, 4, 5], converged: false });
    expect(rawMeta(root)).toMatchObject({ backend: "file", epoch: 2, export_digest: rolled.digest, export_generation: rolled.generation });

    // state.json: readGeneration first, then the epoch and the digest of the export.
    const head = fs.readFileSync(path.join(root, "state.json"), "utf8").slice(0, 120);
    expect(head).toMatch(/^\{"readGeneration":"[0-9a-f-]{36}","backendEpoch":2,"backendDigest":"sha256:/);
    const file = decodeFile(root);
    expect(file).toMatchObject({ backendEpoch: 2, backendDigest: rolled.digest, readGeneration: rolled.generation });
    expect(meshSnapshotDigest(file)).toBe(rolled.digest);
    expect(exportTemps(root)).toEqual([]);

    // The sqlite store that was open fails closed; a new one cannot open; the file store has every write.
    await expect(live.put({ key: "live/late", value: 1, identity })).rejects.toThrow(MeshStateRetiredError);
    // Production's default open, without the suite's fixture "create" (tests/fleet-isolation-setup.ts), which
    // refuses this populated root before it reads the flag (smarty-dev#6477).
    const fixtureDefault = Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures");
    const saved = (globalThis as Record<symbol, unknown>)[fixtureDefault];
    delete (globalThis as Record<symbol, unknown>)[fixtureDefault];
    try { await expect(SqliteStateStore.open(root, 64 * 1024, 1_000)).rejects.toThrow(MeshStateRetiredError); }
    finally { (globalThis as Record<symbol, unknown>)[fixtureDefault] = saved; }
    const files = fileStore(root);
    expect(files.get("live/after-import", { fresh: true })?.value).toEqual({ from: "sqlite" });
    expect(resolveMeshStateSource(root)).toEqual({ source: "file", backend: "file", epoch: 2, fileEpoch: 2 });

    // File-mode writers keep the epoch field (with and without the read journal) and the reader rule holds.
    await files.put({ key: "file/after-rollback", value: "file", identity });
    await new MeshStore(root, 64 * 1024, 1_000, { writeReadJournal: false }).put({ key: "file/no-journal", value: 2, identity });
    expect(readStateFileEpoch(root)).toBe(2);
    expect(resolveMeshStateSource(root).source).toBe("file");
    const rerun = await rollbackMeshState(root);
    expect(rerun).toMatchObject({ backend: "file", epoch: 2, steps: [], converged: true });

    // Roll forward is a fresh import at E+1.
    const forward = await importMeshState(root);
    expect(forward).toMatchObject({ epoch: 3, previousEpoch: 2, converged: false });
    const reopened = await openSqlite(root);
    expect(reopened.get("file/after-rollback")?.value).toBe("file");
    expect(reopened.get("live/after-import")?.value).toEqual({ from: "sqlite" });
    expect(resolveMeshStateSource(root)).toMatchObject({ source: "sqlite", epoch: 3, fileEpoch: 3 });
    expect(readMeshStateMovedMarker(root)).toMatchObject({ epoch: 3 });
  });

  it("abort-rollback sets exporting back to sqlite and keeps E+1", async () => {
    const root = tempRoot("abort");
    await seedFileRoot(root);
    await importMeshState(root);
    const crash = new Error("simulated crash after the flag");
    await expect(rollbackMeshState(root, { onStep: (step) => { if (step === "rollback-flag") throw crash; } }))
      .rejects.toBe(crash);
    expect(rawMeta(root)).toMatchObject({ backend: "exporting", epoch: 2, rollback_from_epoch: 1 });
    expect(resolveMeshStateSource(root)).toMatchObject({ source: "sqlite", backend: "exporting", epoch: 2 });
    await expect(SqliteStateStore.open(root, 64 * 1024, 1_000)).rejects.toThrow(MeshStateRetiredError);

    // Like the other mutating commands, abort-rollback runs the advisory census inside its fence
    // (custody.lock held) and returns the report; a reported writer never gates it.
    const advised: MeshCensusAdvisory[] = [];
    let custodyHeld = false;
    const aborted = await abortMeshRollback(root, {
      census: async () => {
        custodyHeld = fs.existsSync(path.join(root, "custody.lock"));
        return { writers: [{ pid: 4242, release: "3.2.0", mode: "sqlite" }] };
      },
      onAdvisory: advisory => { advised.push(advisory); },
    });
    expect(aborted).toMatchObject({ backend: "sqlite", epoch: 2, converged: false });
    expect(custodyHeld).toBe(true);
    expect(advised).toEqual([{ writers: [{ pid: 4242, release: "3.2.0", mode: "sqlite" }], unknown: [] }]);
    expect(aborted.census).toEqual(advised[0]);
    expect(await abortMeshRollback(root, { census: async () => ({ writers: [] }) }))
      .toMatchObject({ backend: "sqlite", epoch: 2, converged: true, census: { writers: [], unknown: [] } });
    expect(await abortMeshRollback(root)).not.toHaveProperty("census");
    const reopened = await openSqlite(root);
    await reopened.put({ key: "after/abort", value: 1, identity });
    expect(resolveMeshStateSource(root)).toMatchObject({ source: "sqlite", epoch: 2 });
    reopened.close();
    // A later rollback takes the next epoch.
    expect(await rollbackMeshState(root)).toMatchObject({ backend: "file", epoch: 3 });
    await expect(abortMeshRollback(root)).rejects.toThrow(MeshBackendRefusedError);
  });

  it("an interrupted export reruns from the stored flag and produces the identical state", async () => {
    const root = tempRoot("rerun-export");
    await seedFileRoot(root);
    await importMeshState(root);
    const marker = fs.readFileSync(path.join(root, "state.json"));
    const before = meshSnapshotDigest((await openSqlite(root)).exportState());
    for (const step of ["rollback-export-temp", "rollback-verify"] as const) {
      await expect(rollbackMeshState(root, { onStep: (at) => { if (at === step) throw new Error(step); } }))
        .rejects.toThrow(step);
      expect(rawMeta(root)).toMatchObject({ backend: "exporting", epoch: 2 });
      // The export goes to its own temp: state.json stays the marker until the very last step.
      expect(exportTemps(root)).toEqual(["state.json.rollback-2.tmp"]);
      expect(fs.readFileSync(path.join(root, "state.json")).equals(marker)).toBe(true);
    }
    const done = await rollbackMeshState(root);
    expect(done).toMatchObject({ backend: "file", epoch: 2, steps: [3, 4, 5], digest: before });
    expect(exportTemps(root)).toEqual([]);
    expect(readMeshStateMovedMarker(root)).toBeUndefined();
  });

  it("a rollback stopped after the switch keeps the marker; the rerun replaces it (from the verified temp, or a re-export)", async () => {
    for (const keepTemp of [true, false]) {
      const root = tempRoot(`after-switch-${String(keepTemp)}`);
      await seedFileRoot(root);
      await importMeshState(root);
      const marker = fs.readFileSync(path.join(root, "state.json"));
      const before = meshSnapshotDigest((await openSqlite(root)).exportState());
      await expect(rollbackMeshState(root, { onStep: (at) => { if (at === "rollback-switch") throw new Error(at); } }))
        .rejects.toThrow("rollback-switch");
      const meta = rawMeta(root);
      expect(meta).toMatchObject({ backend: "file", epoch: 2, export_digest: before });
      // backend=file is committed but state.json is still the marker: readers use SQLite, writers fail closed.
      expect(fs.readFileSync(path.join(root, "state.json")).equals(marker)).toBe(true);
      expect(resolveMeshStateSource(root)).toEqual({ source: "sqlite", backend: "file", epoch: 2, fileEpoch: 1 });
      expect(() => assertFileStateWritable(root)).toThrow(MeshBackendFenceError);
      await expect(fileStore(root).put({ key: "new/refused", value: 1, identity })).rejects.toThrow(MeshBackendFenceError);
      expect(() => legacyReadState(path.join(root, "state.json"), 64 * 1024 * 1024, false)).toThrow("invalid state format");
      expect(await meshBackendStatus(root)).toMatchObject({ fileMoved: true, fenceHolds: true, reader: { source: "sqlite" } });
      await expect(abortMeshRollback(root)).rejects.toThrow(MeshBackendRefusedError);
      if (!keepTemp) fs.rmSync(path.join(root, "state.json.rollback-2.tmp"));
      const rerun = await rollbackMeshState(root);
      expect(rerun).toMatchObject({ backend: "file", epoch: 2, digest: before, generation: meta.export_generation,
        steps: keepTemp ? [5] : [3, 5], converged: false });
      expect(decodeFile(root)).toMatchObject({ backendEpoch: 2, readGeneration: meta.export_generation });
      expect(resolveMeshStateSource(root)).toEqual({ source: "file", backend: "file", epoch: 2, fileEpoch: 2 });
      expect(exportTemps(root)).toEqual([]);
    }
  });
});

describe("mesh backend reader rule", () => {
  it("reads state.json only at backend=file with the same epoch and fails closed with an alarm otherwise", async () => {
    const root = tempRoot("reader");
    await seedFileRoot(root);
    await importMeshState(root);
    await rollbackMeshState(root);
    const alarms: MeshBackendAlarm[] = [];
    const onAlarm = (alarm: MeshBackendAlarm): void => { alarms.push(alarm); };
    expect(resolveMeshStateSource(root, { onAlarm })).toMatchObject({ source: "file", epoch: 2, fileEpoch: 2 });

    setRawMeta(root, "epoch", 3); // backend=file at another epoch
    expect(() => resolveMeshStateSource(root, { onAlarm })).toThrow(MeshBackendFenceError);
    setRawMeta(root, "backend", "exporting"); // exporting: readers stay on SQLite
    expect(resolveMeshStateSource(root, { onAlarm })).toMatchObject({ source: "sqlite", backend: "exporting" });
    // importing (review round 5): SQLite is not authoritative before the marker; state.json below the epoch is.
    setRawMeta(root, "backend", "importing");
    expect(resolveMeshStateSource(root, { onAlarm })).toMatchObject({ source: "file", backend: "importing", epoch: 3, fileEpoch: 2 });
    expect(() => assertFileStateWritable(root)).not.toThrow();
    setRawMeta(root, "epoch", 2);
    expect(() => resolveMeshStateSource(root, { onAlarm })).toThrow(/backend=importing at epoch 2 but state.json carries epoch 2/);
    setRawMeta(root, "backend", "sqlite");
    setRawMeta(root, "epoch", 1); // file epoch above the database epoch
    expect(() => resolveMeshStateSource(root, { onAlarm })).toThrow(/exceeds the database epoch/);
    setRawMeta(root, "epoch", 2);
    setRawMeta(root, "backend", "retired");
    expect(() => resolveMeshStateSource(root, { onAlarm })).toThrow(/not sqlite, importing, exporting or file/);
    expect(alarms.map(alarm => alarm.code)).toEqual(Array(4).fill("FABRIC_MESH_BACKEND_FENCE"));
    const status = await meshBackendStatus(root);
    expect(status.reader).toHaveProperty("error");

    // An epoch-stamped state.json without its database.
    const orphan = tempRoot("orphan");
    fs.copyFileSync(path.join(root, "state.json"), path.join(orphan, "state.json"));
    expect(() => resolveMeshStateSource(orphan)).toThrow(/state.db is missing/);
    expect((await meshBackendStatus(orphan)).fenceHolds).toBe(false);
    // A legacy root without either is a file root.
    expect(resolveMeshStateSource(tempRoot("legacy"))).toEqual({ source: "file", backend: "none", epoch: 0, fileEpoch: 0 });
  });
});

describe("mesh backend cutover fence: .lock and custody.lock; census advisory", () => {
  const sqliteWriter = { pid: 101, release: "3.2.0", mode: "sqlite" };
  const fileWriter = { pid: 103, release: "3.1.57", mode: "file" };

  it("never gates on the census: no provider, file-mode writers or unknown evidence all proceed, reported as advisory", async () => {
    const root = tempRoot("cutover");
    await seedFileRoot(root);
    const g0 = decodeFile(root).readGeneration;
    const advised: MeshCensusAdvisory[] = [];
    const result = await cutoverMeshState(root, {
      census: async () => ({ writers: [sqliteWriter, fileWriter], unknown: [{ pid: 0, release: "unknown", mode: "unknown lock-owner" }] }),
      onAdvisory: (advisory) => { advised.push(advisory); },
    });
    expect(result).toMatchObject({ backend: "sqlite", epoch: 1, writers: [{ pid: 101 }, { pid: 103 }], generation: g0 });
    expect(result.census).toEqual({ writers: [sqliteWriter, fileWriter], unknown: [{ pid: 0, release: "unknown", mode: "unknown lock-owner" }] });
    expect(advised).toEqual([result.census]);
    expect(describeCensusAdvisory(result.census!)).toBe("advisory: 2 writers, 1 unknown");
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(fs.existsSync(path.join(root, "custody.lock"))).toBe(false);
    const status = await meshBackendStatus(root, { census: async () => ({ writers: [sqliteWriter] }) });
    expect(status).toMatchObject({ backend: "sqlite", writers: [{ pid: 101, mode: "sqlite" }], unknownWriters: [], fenceHolds: true });

    // A failing census is reported and changes nothing either.
    const other = tempRoot("census-down");
    await seedFileRoot(other);
    const down = await cutoverMeshState(other, { census: async () => { throw new Error("census down"); } });
    expect(down).toMatchObject({ backend: "sqlite", epoch: 1, writers: [], census: { error: "census down" } });
    expect(describeCensusAdvisory(down.census!)).toBe("advisory: census failed (census down)");
  });

  it("holds .lock and custody.lock across the section: a writer that starts during the import waits, then is refused", async () => {
    const root = tempRoot("blocked");
    await seedFileRoot(root);
    let censusCalls = 0;
    let writer: Promise<string> | undefined;
    let custodyHeld = false;
    const result = await cutoverMeshState(root, {
      census: async () => { censusCalls += 1; await new Promise(resolve => setTimeout(resolve, 150)); return { writers: [] }; },
      onStep: (step) => {
        if (step !== "import-read") return;
        custodyHeld = fs.existsSync(path.join(root, "custody.lock"));
        // A new-release file-mode writer: it takes .lock and checks the flag before it writes.
        writer = new MeshLock(root, { lockTimeoutMs: 20_000 }, () => undefined).withLock(() => {
          const seen = `census ${censusCalls}, backend ${String(rawMeta(root).backend)}`;
          try { assertFileStateWritable(root); return `${seen}: wrote`; }
          catch (error) { return `${seen}: ${(error as Error).name}`; }
        });
      },
    });
    expect(result).toMatchObject({ backend: "sqlite", epoch: 1 });
    expect(custodyHeld).toBe(true);
    // It got .lock only after the section (one advisory census), and the flag refused its write.
    expect(await writer).toBe("census 1, backend sqlite: MeshBackendFenceError");
    expect(fs.existsSync(path.join(root, "custody.lock"))).toBe(false);
  });

  it("waits for custody.lock: a held custody lock fences cutover, rollback and abort-rollback", async () => {
    const root = tempRoot("custody");
    await seedFileRoot(root);
    const release = await acquireMeshCustodyLock(root);
    try {
      await expect(cutoverMeshState(root, { lockTimeoutMs: 200 })).rejects.toThrow(/custody lock/);
      expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
      expect(resolveMeshStateSource(root).source).toBe("file");
    } finally { release(); }
    expect(await cutoverMeshState(root)).toMatchObject({ backend: "sqlite", epoch: 1 });
    const again = await acquireMeshCustodyLock(root);
    try {
      await expect(rollbackMeshState(root, { lockTimeoutMs: 200 })).rejects.toThrow(/custody lock/);
      await expect(abortMeshRollback(root, { lockTimeoutMs: 200 })).rejects.toThrow(/custody lock/);
      expect(rawMeta(root)).toMatchObject({ backend: "sqlite", epoch: 1 });
    } finally { again(); }
    expect(await rollbackMeshState(root)).toMatchObject({ backend: "file", epoch: 2 });
  });

  it("a file-mode writer reported by the census does not fail a cutover; a moved state.json still does", async () => {
    const root = tempRoot("late");
    await seedFileRoot(root);
    const alarms: MeshBackendAlarm[] = [];
    const steps: string[] = [];
    const result = await cutoverMeshState(root, {
      census: async () => ({ writers: [fileWriter] }),
      onAlarm: (alarm) => { alarms.push(alarm); }, onStep: (step) => { steps.push(step); },
    });
    expect(result).toMatchObject({ backend: "sqlite", epoch: 1, writers: [{ pid: 103, mode: "file" }] });
    expect(steps).toEqual(["import-read", "import-commit", "import-verify", "cutover-reconcile", "cutover-copy", "cutover-marker", "cutover-flag"]);
    expect(alarms).toEqual([]);
    // The CLI exits 0 and prints the census as advisory.
    const other = tempRoot("late-cli");
    await seedFileRoot(other);
    let err = "";
    const code = await main(["cutover", "--root", other], {
      census: async () => ({ writers: [fileWriter] }), stdout: () => undefined, stderr: (text) => { err += text; },
    });
    expect(code).toBe(0);
    expect(err).toBe("fabric-mesh-backend: advisory: 1 writer, 0 unknown\n");
    expect(rawMeta(other)).toMatchObject({ backend: "sqlite", epoch: 1 });
  });

  it("aborts with the flag unchanged when state.json moves before the flag commit, and rolls back after it", async () => {
    // A writer that ignored .lock (another lock protocol) commits state.json at the given step.
    const rogueWrite = (root: string): void => {
      const file = decodeFile(root) as MeshStateFile & { highWater: number };
      const version = file.highWater + 1;
      const rogue = { ...file, readGeneration: randomUUID(), highWater: version,
        entries: { ...file.entries, "rogue/key": { key: "rogue/key", value: 1, version, updatedAt: Date.now(), updatedBy: identity } } };
      fs.writeFileSync(path.join(root, "state.json"), encodeMeshStateFile(rogue).serialized);
    };
    const before = tempRoot("moved-before");
    await seedFileRoot(before);
    const aborted = await cutoverMeshState(before, { onStep: (step) => { if (step === "import-read") rogueWrite(before); } })
      .catch((error: unknown) => error);
    expect(aborted).toBeInstanceOf(MeshBackendFenceError);
    expect((aborted as Error).message).toMatch(/changed before the flag commit .*import aborted, the backend flag is unchanged/);
    expect(rawMeta(before)).toMatchObject({ backend: "file", epoch: 0, commit_no: 0 });
    expect(resolveMeshStateSource(before).source).toBe("file");
    expect(decodeFile(before).entries["rogue/key"]?.value).toBe(1);

    const after = tempRoot("moved-after");
    await seedFileRoot(after);
    const imported = meshSnapshotDigest(decodeFile(after));
    const failed = await cutoverMeshState(after, { onStep: (step) => { if (step === "import-verify") rogueWrite(after); } })
      .catch((error: unknown) => error);
    expect(failed).toBeInstanceOf(MeshCutoverFailedError);
    expect((failed as Error).message).toMatch(/changed after the flag commit.*rolled back to backend=file at epoch 2; the conflicting state.json is kept at /);
    expect(rawMeta(after)).toMatchObject({ backend: "file", epoch: 2 });
    expect(meshSnapshotDigest(decodeFile(after))).toBe(imported);
    const conflict = (failed as MeshCutoverFailedError).conflictFile!;
    expect((decodeMeshStateFile(conflict, 64 * 1024 * 1024, false) as MeshStateFile).entries["rogue/key"]?.value).toBe(1);
  });
});

describe("file-mode writer fence (assertFileStateWritable)", () => {
  it("passes only where the reader rule reads state.json", async () => {
    const root = tempRoot("writable");
    await seedFileRoot(root);
    expect(() => assertFileStateWritable(root)).not.toThrow(); // legacy root, no state.db
    await importMeshState(root);
    expect(() => assertFileStateWritable(root)).toThrow(MeshBackendFenceError);
    expect(() => assertFileStateWritable(root)).toThrow(/backend=sqlite at epoch 1/);
    const stop = new Error("stop after the flag");
    await expect(rollbackMeshState(root, { onStep: (step) => { if (step === "rollback-flag") throw stop; } })).rejects.toBe(stop);
    expect(() => assertFileStateWritable(root)).toThrow(/backend=exporting at epoch 2/);
    await rollbackMeshState(root);
    expect(() => assertFileStateWritable(root)).not.toThrow();
    setRawMeta(root, "epoch", 5); // backend=file at another epoch: fail closed
    expect(() => assertFileStateWritable(root)).toThrow(MeshBackendFenceError);
  });
});

// A real file-mode writer in a second process: MeshStore -> StateFile, the release's own commit path.
const WRITER = `
import { createJiti } from "jiti";
import readline from "node:readline";
import { pathToFileURL } from "node:url";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { MeshStore } = await jiti.import("./src/mesh/store.ts");
const root = process.argv.at(-1);
const store = new MeshStore(root, 64 * 1024, 1000);
const identity = { id: "writer", name: "writer", kind: "agent" };
process.stdout.write("ready\\n");
for await (const line of readline.createInterface({ input: process.stdin })) {
  const [op, key] = JSON.parse(line);
  try {
    if (op === "put") await store.put({ key, value: { pid: process.pid }, identity });
    else if (op === "delete") await store.delete({ key });
    else await store.writeBatch({ identity, ops: [{ kind: "put", key, value: 1 }, { kind: "put", key: key + "/2", value: 2 }] });
    process.stdout.write(JSON.stringify({ ok: true }) + "\\n");
  } catch (error) {
    process.stdout.write(JSON.stringify({ ok: false, name: error.name, code: error.code, message: error.message }) + "\\n");
  }
}
`;

interface WriterReply { ok: boolean; name?: string; code?: string; message?: string }

const startWriter = async (root: string, script = WRITER): Promise<{ pid: number; send: (op: "put" | "delete" | "batch", key: string) => Promise<WriterReply>;
  next: () => Promise<string>; stop: () => Promise<void> }> => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", script, root], { cwd: process.cwd(), stdio: ["pipe", "pipe", "pipe"] });
  children.push(child);
  let buffer = "";
  let stderr = "";
  const lines: string[] = [];
  const waiters: Array<{ resolve: (line: string) => void; reject: (error: Error) => void }> = [];
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.stdout.on("data", (chunk) => {
    buffer += chunk;
    for (let index = buffer.indexOf("\n"); index >= 0; index = buffer.indexOf("\n")) {
      const line = buffer.slice(0, index);
      buffer = buffer.slice(index + 1);
      const waiter = waiters.shift();
      if (waiter) waiter.resolve(line); else lines.push(line);
    }
  });
  child.once("exit", (code) => { for (const waiter of waiters.splice(0)) waiter.reject(new Error(`writer exited ${String(code)}: ${stderr}`)); });
  const next = (): Promise<string> => {
    const line = lines.shift();
    return line !== undefined ? Promise.resolve(line) : new Promise((resolve, reject) => { waiters.push({ resolve, reject }); });
  };
  expect(await next()).toBe("ready");
  return {
    pid: child.pid!,
    send: async (op, key) => {
      const reply = next();
      child.stdin.write(`${JSON.stringify([op, key])}\n`);
      return JSON.parse(await reply) as WriterReply;
    },
    next,
    stop: () => new Promise((resolve) => { child.once("exit", () => resolve()); child.stdin.end(); }),
  };
};

describe("file-mode writer fence on the real StateFile commit path (pi-fabric#627 review round 2)", () => {
  it("refuses a live file-mode writer in another process after cutover, leaves state.json byte-identical, admits it after rollback at E+2", async () => {
    const root = tempRoot("real-writer");
    await seedFileRoot(root);
    const statePath = path.join(root, "state.json");
    const writer = await startWriter(root);
    expect(await writer.send("put", "writer/before")).toEqual({ ok: true });
    expect(fileStore(root).get("writer/before", { fresh: true })?.value).toEqual({ pid: writer.pid });

    // The census lane does not see this writer (it started without registering): only the fence stops it.
    const cut = await cutoverMeshState(root, { census: async () => ({ writers: [] }) });
    expect(cut).toMatchObject({ backend: "sqlite", epoch: 1 });
    const frozen = fs.readFileSync(statePath);
    const frozenInode = fs.statSync(statePath).ino;

    // The same writer (warm caches from its first commit): every write path is refused under .lock.
    for (const [op, key] of [["put", "writer/after"], ["delete", "writer/before"], ["batch", "writer/batch"]] as const) {
      const reply = await writer.send(op, key);
      expect(reply).toMatchObject({ ok: false, name: "MeshBackendFenceError", code: "FABRIC_MESH_BACKEND_FENCE" });
      // The strict read meets cutover's moved marker first (review round 3): refused before staging.
      expect(reply.message).toMatch(/state\.json was moved to state\.db \(backend=sqlite at epoch 1/);
    }
    expect(fs.readFileSync(statePath).equals(frozen)).toBe(true);
    expect(fs.statSync(statePath).ino).toBe(frozenInode);
    expect(fs.readdirSync(root).filter((name) => name.endsWith(".prepared.tmp"))).toEqual([]);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    const sqlite = await openSqlite(root);
    expect(sqlite.get("writer/before")?.value).toEqual({ pid: writer.pid });
    expect(sqlite.get("writer/after")).toBeUndefined();
    sqlite.close();

    const rolled = await rollbackMeshState(root);
    expect(rolled).toMatchObject({ backend: "file", epoch: 2 });
    expect(await writer.send("put", "writer/after-rollback")).toEqual({ ok: true });
    expect(await writer.send("batch", "writer/batch-after-rollback")).toEqual({ ok: true });
    expect(resolveMeshStateSource(root)).toEqual({ source: "file", backend: "file", epoch: 2, fileEpoch: 2 });
    expect(readStateFileEpoch(root)).toBe(2);
    expect(fileStore(root).get("writer/after-rollback", { fresh: true })?.value).toEqual({ pid: writer.pid });
    expect(fileStore(root).get("writer/after", { fresh: true })).toBeUndefined();
    await writer.stop();
  }, 60_000);

  it("costs one stat on a root without state.db (SQLite never opened)", async () => {
    const root = tempRoot("fence-cost");
    await fileStore(root).put({ key: "cost/seed", value: 1, identity });
    const open = (): never => { throw new Error("the guard must not open SQLite without state.db"); };
    expect(() => assertFileStateWritable(root, { open })).not.toThrow();
    const measure = (rounds: number, options: Parameters<typeof assertFileStateWritable>[1]): number => {
      for (let index = 0; index < Math.min(200, rounds); index++) assertFileStateWritable(root, options);
      const started = process.hrtime.bigint();
      for (let index = 0; index < rounds; index++) assertFileStateWritable(root, options);
      return Number(process.hrtime.bigint() - started) / 1e6 / rounds;
    };
    const absent = measure(5_000, { open });
    // Reported in the review comment; with state.db the guard opens SQLite (informational only).
    await importMeshState(root);
    await rollbackMeshState(root);
    const present = measure(200, {});
    console.log(`[fence-cost] no state.db: ${(absent * 1000).toFixed(2)} us per write guard (5000 calls); state.db backend=file: ${(present * 1000).toFixed(1)} us per call (200 calls)`);
    expect(absent).toBeLessThan(0.2);
  });
});

// A LEGACY file-mode writer in a second process: the deployed release's decode/write rule
// (tests/fixtures/legacy-mesh-state-04930dfd.ts, a verbatim copy at 04930dfd), no backend guard.
const LEGACY_WRITER = `
import { createJiti } from "jiti";
import readline from "node:readline";
import { pathToFileURL } from "node:url";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { legacyPut } = await jiti.import("./tests/fixtures/legacy-mesh-state-04930dfd.ts");
const root = process.argv.at(-1);
process.stdout.write("ready\\n");
for await (const line of readline.createInterface({ input: process.stdin })) {
  const [, key] = JSON.parse(line);
  process.stdout.write(JSON.stringify({ ok: true, waiting: true }) + "\\n");
  let acquiredAt;
  try {
    await legacyPut(root, key, { legacy: process.pid }, () => { acquiredAt = Date.now(); });
    process.stdout.write(JSON.stringify({ ok: true, acquiredAt }) + "\\n");
  } catch (error) {
    process.stdout.write(JSON.stringify({ ok: false, acquiredAt, name: error.name, message: error.message }) + "\\n");
  }
}
`;

describe("cutover's moved marker fails legacy writers closed (pi-fabric#627 review round 3)", () => {
  it("a legacy writer blocked on .lock during cutover fails with invalid state format and leaves the marker byte-identical; rollback admits both writers", async () => {
    const root = tempRoot("legacy-writer");
    await seedFileRoot(root);
    const statePath = path.join(root, "state.json");
    const legacy = await startWriter(root, LEGACY_WRITER);
    const legacyPut = async (key: string): Promise<WriterReply & { acquiredAt?: number }> => {
      expect(await legacy.send("put", key)).toEqual({ ok: true, waiting: true });
      return JSON.parse(await legacy.next()) as WriterReply & { acquiredAt?: number };
    };
    expect(await legacyPut("legacy/before")).toMatchObject({ ok: true });
    const g0 = decodeFile(root).readGeneration;

    let census = 0;
    let pending: Promise<string> | undefined;
    let markerAt = 0;
    let marker: Buffer | undefined;
    const cut = await cutoverMeshState(root, {
      census: async () => {
        // The advisory census runs once, under the held fence: the legacy writer (started right
        // then, invisible to the census) queues on .lock for the whole section.
        if (census++ === 0) {
          expect(await legacy.send("put", "legacy/after")).toEqual({ ok: true, waiting: true });
          pending = legacy.next();
          await new Promise(resolve => setTimeout(resolve, 300));
        }
        return { writers: [] };
      },
      onStep: (step) => {
        if (step !== "cutover-marker") return;
        markerAt = Date.now();
        marker = fs.readFileSync(statePath);
      },
    });
    expect(cut).toMatchObject({ backend: "sqlite", epoch: 1, generation: g0, stateCopy: path.join(root, "state.json.cutover-1") });
    expect(JSON.parse(marker!.toString("utf8"))).toEqual({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 1, at: expect.any(String) });
    expect(marker!.toString("utf8")).toMatch(/^\{"format":"sqlite","movedTo":"state\.db","backend":"sqlite","epoch":1,"at":"/);

    // It got .lock only after the marker, and the deployed strict read refused before any write.
    const reply = JSON.parse(await pending!) as WriterReply & { acquiredAt?: number };
    expect(reply).toMatchObject({ ok: false, message: "Failed to read Fabric mesh state: invalid state format" });
    expect(reply.acquiredAt).toBeGreaterThanOrEqual(markerAt);
    expect(fs.readFileSync(statePath).equals(marker!)).toBe(true);
    expect(fs.readdirSync(root).filter(name => name.endsWith(".legacy.tmp"))).toEqual([]);
    // And again whenever it retries.
    expect(await legacyPut("legacy/retry")).toMatchObject({ ok: false, message: "Failed to read Fabric mesh state: invalid state format" });
    expect(fs.readFileSync(statePath).equals(marker!)).toBe(true);

    // The retired state.json is kept; SQLite has every write before the cutover and none after.
    expect(decodeMeshStateFile(cut.stateCopy, 64 * 1024 * 1024, false).readGeneration).toBe(g0);
    const sqlite = await openSqlite(root);
    expect(sqlite.get("legacy/before")?.value).toEqual({ legacy: legacy.pid });
    expect(sqlite.get("legacy/after")).toBeUndefined();
    sqlite.close();
    // ponytail: an OLD tolerant reader (recoverDamage=true) sees an empty mesh; the census admits none.
    expect(legacyReadState(statePath, 64 * 1024 * 1024, true).entries).toEqual({});
    expect(resolveMeshStateSource(root)).toEqual({ source: "sqlite", backend: "sqlite", epoch: 1, fileEpoch: 1 });
    expect(await meshBackendStatus(root)).toMatchObject({ fileMoved: true, fileEpoch: 1, fenceHolds: true, reader: { source: "sqlite" } });

    // Rollback step 3 replaces the marker with the export; both writers commit again.
    expect(await rollbackMeshState(root)).toMatchObject({ backend: "file", epoch: 2 });
    expect(readMeshStateMovedMarker(root)).toBeUndefined();
    expect(await legacyPut("legacy/after-rollback")).toMatchObject({ ok: true });
    await fileStore(root).put({ key: "new/after-rollback", value: 2, identity });
    const files = fileStore(root);
    expect(files.get("legacy/after-rollback", { fresh: true })?.value).toEqual({ legacy: legacy.pid });
    expect(files.get("new/after-rollback", { fresh: true })?.value).toBe(2);
    expect(files.get("legacy/before", { fresh: true })?.value).toEqual({ legacy: legacy.pid });
    await legacy.stop();
  }, 60_000);

  it("new code reads the marker as backend=sqlite: file-mode writers and readers fail closed, without state.db it alarms, abort-rollback restores it", async () => {
    const root = tempRoot("marker-new-code");
    await seedFileRoot(root);
    await cutoverMeshState(root);
    const marker = fs.readFileSync(path.join(root, "state.json"));
    expect(readMeshStateMovedMarker(root)).toMatchObject({ format: "sqlite", movedTo: "state.db", backend: "sqlite", epoch: 1 });
    expect(readStateFileEpoch(root)).toBe(1);
    await expect(fileStore(root).put({ key: "new/refused", value: 1, identity })).rejects.toThrow(MeshBackendFenceError);
    await expect(fileStore(root).writeBatch({ identity, ops: [{ kind: "put", key: "new/batch", value: 1 }] })).rejects.toThrow(MeshBackendFenceError);
    await expect(fileStore(root).delete({ key: "seed/solo" })).rejects.toThrow(MeshBackendFenceError);
    expect(() => fileStore(root).get("seed/solo", { fresh: true })).toThrow(MeshBackendFenceError);
    expect(() => assertFileStateWritable(root)).toThrow(/backend=sqlite at epoch 1/);
    expect(fs.readFileSync(path.join(root, "state.json")).equals(marker)).toBe(true);

    // The marker without its database: an alarm, never an empty mesh.
    const orphan = tempRoot("marker-orphan");
    fs.writeFileSync(path.join(orphan, "state.json"), marker);
    const alarms: MeshBackendAlarm[] = [];
    expect(() => resolveMeshStateSource(orphan, { onAlarm: (alarm) => { alarms.push(alarm); } }))
      .toThrow(/moved marker \(state\.db at epoch 1\) but state\.db is missing/);
    expect(alarms.map(alarm => alarm.code)).toEqual(["FABRIC_MESH_BACKEND_FENCE"]);
    await expect(fileStore(orphan).put({ key: "orphan/x", value: 1, identity })).rejects.toThrow(MeshBackendFenceError);
    expect(() => fileStore(orphan).get("seed/solo", { fresh: true })).toThrow(MeshBackendFenceError);
    expect(() => assertMeshStateReadable(orphan)).toThrow(MeshBackendFenceError);
    expect((await meshBackendStatus(orphan)).fenceHolds).toBe(false);
    expect(fs.readFileSync(path.join(orphan, "state.json")).equals(marker)).toBe(true);

    // A rollback stopped after its export, then aborted back to sqlite: the marker returns.
    const stop = new Error("stop after the export");
    await expect(rollbackMeshState(root, { onStep: (step) => { if (step === "rollback-verify") throw stop; } })).rejects.toBe(stop);
    // Review round 4: the export waits in its temp; the marker stays until the last step.
    expect(fs.readFileSync(path.join(root, "state.json")).equals(marker)).toBe(true);
    expect(await abortMeshRollback(root)).toMatchObject({ backend: "sqlite", epoch: 2, converged: false });
    expect(fs.readFileSync(path.join(root, "state.json")).equals(marker)).toBe(true);
    expect(exportTemps(root)).toEqual([]);
    expect(() => legacyReadState(path.join(root, "state.json"), 64 * 1024 * 1024, false)).toThrow("Failed to read Fabric mesh state: invalid state format");
    await expect(fileStore(root).put({ key: "new/refused", value: 1, identity })).rejects.toThrow(MeshBackendFenceError);
  }, 60_000);
});

describe("legacy writers stay fenced through import, rollback and abort (pi-fabric#627 review round 4)", () => {
  const startLegacy = async (root: string) => {
    const legacy = await startWriter(root, LEGACY_WRITER);
    const put = async (key: string): Promise<WriterReply & { acquiredAt?: number }> => {
      expect(await legacy.send("put", key)).toEqual({ ok: true, waiting: true });
      return JSON.parse(await legacy.next()) as WriterReply & { acquiredAt?: number };
    };
    return { legacy, put };
  };
  const refusedByMarker = { ok: false, message: "Failed to read Fabric mesh state: invalid state format" };

  it("a legacy writer (04930dfd) queued on .lock across a standalone import is refused, and never commits afterwards", async () => {
    const root = tempRoot("legacy-import");
    await seedFileRoot(root);
    const statePath = path.join(root, "state.json");
    const { legacy, put } = await startLegacy(root);
    expect(await put("legacy/before")).toMatchObject({ ok: true });
    let census = 0;
    let pending: Promise<string> | undefined;
    let markerAt = 0;
    const imported = await importMeshState(root, {
      census: async () => {
        // The advisory census runs once under the held fence: the legacy writer queues on .lock.
        if (census++ === 0) {
          expect(await legacy.send("put", "legacy/during-import")).toEqual({ ok: true, waiting: true });
          pending = legacy.next();
          await new Promise(resolve => setTimeout(resolve, 300));
        }
        return { writers: [] };
      },
      onStep: (step) => { if (step === "cutover-marker") markerAt = Date.now(); },
    });
    expect(imported).toMatchObject({ backend: "sqlite", epoch: 1, converged: false });
    const marker = fs.readFileSync(statePath);
    const reply = JSON.parse(await pending!) as WriterReply & { acquiredAt?: number };
    expect(reply).toMatchObject(refusedByMarker);
    expect(reply.acquiredAt).toBeGreaterThanOrEqual(markerAt);
    expect(await put("legacy/after-import")).toMatchObject(refusedByMarker);
    expect(fs.readFileSync(statePath).equals(marker)).toBe(true);
    const sqlite = await openSqlite(root);
    expect(sqlite.get("legacy/before")?.value).toEqual({ legacy: legacy.pid });
    expect(sqlite.get("legacy/during-import")).toBeUndefined();
    expect(sqlite.get("legacy/after-import")).toBeUndefined();
    sqlite.close();
    await legacy.stop();
  }, 60_000);

  it("a legacy writer during a rollback stopped after each step before the replace is refused; the marker stands until then", async () => {
    for (const step of ["rollback-flag", "rollback-export-temp", "rollback-verify", "rollback-switch"] as const) {
      const root = tempRoot(`legacy-rollback-${step}`);
      await seedFileRoot(root);
      await importMeshState(root);
      const statePath = path.join(root, "state.json");
      const marker = fs.readFileSync(statePath);
      const { legacy, put } = await startLegacy(root);
      let pending: Promise<string> | undefined;
      await expect(rollbackMeshState(root, { onStep: (at) => {
        if (at !== step) return;
        // Sent while the rollback holds .lock; it gets .lock right after this step stops the run.
        void legacy.send("put", `legacy/${step}`).catch(() => undefined);
        pending = legacy.next();
        throw new Error(at);
      } })).rejects.toThrow(step);
      expect(JSON.parse(await pending!)).toMatchObject(refusedByMarker);
      expect(await put(`legacy/${step}/retry`)).toMatchObject(refusedByMarker);
      expect(fs.readFileSync(statePath).equals(marker)).toBe(true);
      // The rerun completes; only then does the legacy writer commit, to the export at E+1.
      expect(await rollbackMeshState(root)).toMatchObject({ backend: "file", epoch: 2 });
      expect(await put(`legacy/${step}/after`)).toMatchObject({ ok: true });
      expect(fileStore(root).get(`legacy/${step}`, { fresh: true })).toBeUndefined();
      expect(fileStore(root).get(`legacy/${step}/after`, { fresh: true })?.value).toEqual({ legacy: legacy.pid });
      await legacy.stop();
    }
  }, 120_000);

  it("abort-rollback under contention with a legacy writer leaves no legacy commit", async () => {
    const root = tempRoot("legacy-abort");
    await seedFileRoot(root);
    await importMeshState(root);
    const statePath = path.join(root, "state.json");
    const marker = fs.readFileSync(statePath);
    const committed = meshSnapshotDigest((await openSqlite(root)).exportState());
    await expect(rollbackMeshState(root, { onStep: (at) => { if (at === "rollback-verify") throw new Error(at); } }))
      .rejects.toThrow("rollback-verify");
    expect(rawMeta(root)).toMatchObject({ backend: "exporting", epoch: 2 });
    const { legacy, put } = await startLegacy(root);
    // Hold .lock while the legacy writer and abort-rollback both queue on it, then release: whichever
    // gets it first, the legacy writer meets the marker.
    let aborted: ReturnType<typeof abortMeshRollback> | undefined;
    let pending: Promise<string> | undefined;
    await new MeshLock(root, { lockTimeoutMs: 20_000 }, () => undefined).withLockAcrossAwait(async () => {
      expect(await legacy.send("put", "legacy/abort")).toEqual({ ok: true, waiting: true });
      pending = legacy.next();
      aborted = abortMeshRollback(root, { lockTimeoutMs: 20_000 });
      await new Promise(resolve => setTimeout(resolve, 300));
    }, 20_000, "other");
    expect(await aborted!).toMatchObject({ backend: "sqlite", epoch: 2, converged: false });
    expect(JSON.parse(await pending!)).toMatchObject(refusedByMarker);
    expect(await put("legacy/after-abort")).toMatchObject(refusedByMarker);
    expect(fs.readFileSync(statePath).equals(marker)).toBe(true);
    expect(exportTemps(root)).toEqual([]);
    const sqlite = await openSqlite(root);
    expect(meshSnapshotDigest(sqlite.exportState())).toBe(committed);
    expect(sqlite.get("legacy/abort")).toBeUndefined();
    sqlite.close();
    await legacy.stop();
  }, 60_000);
});

describe("mesh backend 5 MB round trip", () => {
  it("imports and exports a seeded 5 MB state with equal digests both ways", async () => {
    const root = tempRoot("five-mb");
    const store = fileStore(root);
    const payload = "x".repeat(2_000);
    for (let batch = 0; batch < 6; batch++) {
      await store.writeBatch({ identity, ops: Array.from({ length: 450 }, (_, index) => ({
        kind: "put" as const, key: `big/b${batch}/${index}`, value: { batch, index, payload },
      })) });
    }
    for (let index = 0; index < 50; index++) await store.delete({ key: `big/b0/${index}` });
    const size = fs.statSync(path.join(root, "state.json")).size;
    expect(size).toBeGreaterThan(5 * 1024 * 1024);
    const original = meshSnapshotDigest(decodeFile(root));

    const started = performance.now();
    const imported = await importMeshState(root);
    const importMs = performance.now() - started;
    expect(imported).toMatchObject({ digest: original, entries: 2_650, tombstones: 50 });
    const status = await meshBackendStatus(root);
    expect(status.dbDigest).toBe(original);
    expect(status.fileMoved).toBe(true);
    expect(meshSnapshotDigest(retiredFile(root))).toBe(original);

    const rolled = await rollbackMeshState(root);
    expect(rolled.digest).toBe(original);
    expect(meshSnapshotDigest(decodeFile(root))).toBe(original);
    expect(sorted(fileStore(root).listAll("big/", { fresh: true })).length).toBe(2_650);
    console.log(`5 MB round trip: state.json ${size} bytes, import ${importMs.toFixed(0)} ms`);
  }, 120_000);
});

describe("fabric-mesh-backend CLI", () => {
  it("runs status, cutover and rollback with exit codes", async () => {
    const root = tempRoot("cli");
    await seedFileRoot(root);
    const run = async (...argv: string[]): Promise<{ code: number; out: string; err: string }> => {
      let out = "";
      let err = "";
      const code = await main(argv, { stdout: (text) => { out += text; }, stderr: (text) => { err += text; } });
      return { code, out, err };
    };
    expect((await run()).code).toBe(0);
    expect((await run("bogus", "--root", root)).code).toBe(2);
    expect((await run("status")).code).toBe(2);
    const status = await run("status", "--root", root, "--json");
    expect(status.code).toBe(0);
    expect(JSON.parse(status.out)).toMatchObject({ backend: "none", reader: { source: "file" } });
    // W1: the CLI runs the L4a writer census as ADVISORY; a record it cannot verify is reported unknown.
    fs.mkdirSync(path.join(root, ".writer-census"), { recursive: true });
    fs.writeFileSync(path.join(root, ".writer-census", "torn.json"), "{");
    const census = await run("census", "--root", root);
    expect(census.code).toBe(0);
    expect(census.out).toMatch(/advisory: 0 writers, 1 unknown/);
    expect(census.out).toMatch(/pid 0 {2}unknown process-record torn\.json/);
    expect(census.out).not.toMatch(/safe|clean/i);
    // An unknown writer never blocks: the cutover is fenced on .lock and custody.lock only.
    expect((await run("cutover", "--root", root, "--assume-no-writers")).code).toBe(2);
    expect(fs.existsSync(path.join(root, "state.db"))).toBe(false);
    const cutover = await run("cutover", "--root", root, "--json");
    expect(cutover.code).toBe(0);
    expect(cutover.err).toMatch(/fabric-mesh-backend: advisory: 0 writers, 1 unknown/);
    expect(JSON.parse(cutover.out)).toMatchObject({ command: "cutover", ok: true, backend: "sqlite", epoch: 1 });
    expect((await run("status", "--root", root)).out).toMatch(/census {8}advisory: 0 writers, 1 unknown/);
    expect((await run("abort-rollback", "--root", root)).code).toBe(3);
    const rollback = await run("rollback", "--root", root);
    expect(rollback.code).toBe(0);
    expect(rollback.out).toMatch(/rollback done: backend=file epoch 2, steps 1,2,3,4,5/);
    expect((await run("status", "--root", root)).out).toMatch(/readers use {3}file/);
    setRawMeta(root, "epoch", 5);
    const broken = await run("status", "--root", root);
    expect(broken.code).toBe(3);
    expect(broken.out).toMatch(/fence {9}VIOLATED/);
  });
});
