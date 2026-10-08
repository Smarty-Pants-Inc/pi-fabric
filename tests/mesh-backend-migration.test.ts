import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { abortMeshRollback, cutoverMeshState, importMeshState, meshBackendStatus, MeshBackendFenceError, MeshBackendRefusedError,
  meshSnapshotDigest, readStateFileEpoch, resolveMeshStateSource, rollbackMeshState, type MeshBackendAlarm,
  type MeshStateSnapshot } from "../src/mesh/backend-migration.js";
import { main } from "../src/mesh/mesh-backend-cli.js";
import { decodeMeshStateFile } from "../src/mesh/state-file.js";
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

afterEach(() => {
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

    const imported = await importMeshState(root);
    expect(imported).toMatchObject({ backend: "sqlite", epoch: 1, previousEpoch: 0, entries: 28, tombstones: 3, digest: fileDigest, converged: false });
    expect(rawMeta(root)).toMatchObject({ backend: "sqlite", epoch: 1, import_digest: fileDigest });
    expect(resolveMeshStateSource(root)).toEqual({ source: "sqlite", backend: "sqlite", epoch: 1, fileEpoch: 0 });

    const sqlite = await openSqlite(root);
    expect(sorted(sqlite.listAll(""))).toEqual(sorted(files.listAll("", { fresh: true })));
    // CAS tombstones and the clock survived: a stale ifVersion on a deleted key conflicts, a new write is above the clock.
    const tombstoned = decodeFile(root).versions!["seed/ns0/3"]!;
    expect(sqlite.exportState().versions["seed/ns0/3"]).toBe(tombstoned);
    const written = await sqlite.put({ key: "seed/new", value: 1, identity });
    expect(written.version).toBeGreaterThan(decodeFile(root).highWater!);

    const status = await meshBackendStatus(root);
    expect(status).toMatchObject({ backend: "sqlite", epoch: 1, fileEpoch: 0, reader: { source: "sqlite" }, fenceHolds: true, importDigest: fileDigest });
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

    await expect(rollbackMeshState(root)).rejects.toThrow(/needs the writer census/);
    await expect(rollbackMeshState(root, { census: async () => ({ writers: [{ pid: 4242, release: "3.2.0", mode: "sqlite" }] }) }))
      .rejects.toThrow(/pid 4242/);
    expect(rawMeta(root)).toMatchObject({ backend: "sqlite", epoch: 1 });

    const steps: string[] = [];
    const rolled = await rollbackMeshState(root, { census: async () => ({ writers: [] }), onStep: step => { steps.push(step); } });
    expect(steps).toEqual(["rollback-flag", "rollback-export-temp", "rollback-export", "rollback-verify", "rollback-switch"]);
    expect(rolled).toMatchObject({ backend: "file", epoch: 2, steps: [1, 2, 3, 4, 5], converged: false });
    expect(rawMeta(root)).toMatchObject({ backend: "file", epoch: 2, export_digest: rolled.digest, export_generation: rolled.generation });

    // state.json: readGeneration first, then the epoch and the digest of the export.
    const head = fs.readFileSync(path.join(root, "state.json"), "utf8").slice(0, 120);
    expect(head).toMatch(/^\{"readGeneration":"[0-9a-f-]{36}","backendEpoch":2,"backendDigest":"sha256:/);
    const file = decodeFile(root);
    expect(file).toMatchObject({ backendEpoch: 2, backendDigest: rolled.digest, readGeneration: rolled.generation });
    expect(meshSnapshotDigest(file)).toBe(rolled.digest);
    expect(fs.readdirSync(root).filter(name => name.startsWith("state.json.mesh-backend-"))).toEqual([]);

    // The sqlite store that was open fails closed; a new one cannot open; the file store has every write.
    await expect(live.put({ key: "live/late", value: 1, identity })).rejects.toThrow(MeshStateRetiredError);
    await expect(SqliteStateStore.open(root, 64 * 1024, 1_000)).rejects.toThrow(MeshStateRetiredError);
    const files = fileStore(root);
    expect(files.get("live/after-import", { fresh: true })?.value).toEqual({ from: "sqlite" });
    expect(resolveMeshStateSource(root)).toEqual({ source: "file", backend: "file", epoch: 2, fileEpoch: 2 });

    // File-mode writers keep the epoch field (with and without the read journal) and the reader rule holds.
    await files.put({ key: "file/after-rollback", value: "file", identity });
    await new MeshStore(root, 64 * 1024, 1_000, { writeReadJournal: false }).put({ key: "file/no-journal", value: 2, identity });
    expect(readStateFileEpoch(root)).toBe(2);
    expect(resolveMeshStateSource(root).source).toBe("file");
    const rerun = await rollbackMeshState(root);
    expect(rerun).toMatchObject({ backend: "file", epoch: 2, steps: [4], converged: true });

    // Roll forward is a fresh import at E+1.
    const forward = await importMeshState(root);
    expect(forward).toMatchObject({ epoch: 3, previousEpoch: 2, converged: false });
    const reopened = await openSqlite(root);
    expect(reopened.get("file/after-rollback")?.value).toBe("file");
    expect(reopened.get("live/after-import")?.value).toEqual({ from: "sqlite" });
    expect(resolveMeshStateSource(root)).toMatchObject({ source: "sqlite", epoch: 3, fileEpoch: 2 });
  });

  it("abort-rollback sets exporting back to sqlite and keeps E+1", async () => {
    const root = tempRoot("abort");
    await seedFileRoot(root);
    await importMeshState(root);
    const crash = new Error("simulated crash after the flag");
    await expect(rollbackMeshState(root, { assumeNoWriters: true, onStep: (step) => { if (step === "rollback-flag") throw crash; } }))
      .rejects.toBe(crash);
    expect(rawMeta(root)).toMatchObject({ backend: "exporting", epoch: 2, rollback_from_epoch: 1 });
    expect(resolveMeshStateSource(root)).toMatchObject({ source: "sqlite", backend: "exporting", epoch: 2 });
    await expect(SqliteStateStore.open(root, 64 * 1024, 1_000)).rejects.toThrow(MeshStateRetiredError);

    expect(await abortMeshRollback(root)).toMatchObject({ backend: "sqlite", epoch: 2, converged: false });
    expect(await abortMeshRollback(root)).toMatchObject({ backend: "sqlite", epoch: 2, converged: true });
    const reopened = await openSqlite(root);
    await reopened.put({ key: "after/abort", value: 1, identity });
    expect(resolveMeshStateSource(root)).toMatchObject({ source: "sqlite", epoch: 2 });
    reopened.close();
    // A later rollback takes the next epoch.
    expect(await rollbackMeshState(root, { assumeNoWriters: true })).toMatchObject({ backend: "file", epoch: 3 });
    await expect(abortMeshRollback(root)).rejects.toThrow(MeshBackendRefusedError);
  });

  it("an interrupted export reruns from the stored flag and produces the identical state", async () => {
    const root = tempRoot("rerun-export");
    await seedFileRoot(root);
    await importMeshState(root);
    const before = meshSnapshotDigest((await openSqlite(root)).exportState());
    for (const step of ["rollback-export-temp", "rollback-export", "rollback-verify"] as const) {
      await expect(rollbackMeshState(root, { assumeNoWriters: true, onStep: (at) => { if (at === step) throw new Error(step); } }))
        .rejects.toThrow(step);
      expect(rawMeta(root)).toMatchObject({ backend: "exporting", epoch: 2 });
    }
    const done = await rollbackMeshState(root);
    expect(done).toMatchObject({ backend: "file", epoch: 2, steps: [3, 4, 5], digest: before });
    expect(fs.readdirSync(root).filter(name => name.startsWith("state.json.mesh-backend-"))).toEqual([]);
  });
});

describe("mesh backend reader rule", () => {
  it("reads state.json only at backend=file with the same epoch and fails closed with an alarm otherwise", async () => {
    const root = tempRoot("reader");
    await seedFileRoot(root);
    await importMeshState(root);
    await rollbackMeshState(root, { assumeNoWriters: true });
    const alarms: MeshBackendAlarm[] = [];
    const onAlarm = (alarm: MeshBackendAlarm): void => { alarms.push(alarm); };
    expect(resolveMeshStateSource(root, { onAlarm })).toMatchObject({ source: "file", epoch: 2, fileEpoch: 2 });

    setRawMeta(root, "epoch", 3); // backend=file at another epoch
    expect(() => resolveMeshStateSource(root, { onAlarm })).toThrow(MeshBackendFenceError);
    setRawMeta(root, "backend", "exporting"); // exporting: readers stay on SQLite
    expect(resolveMeshStateSource(root, { onAlarm })).toMatchObject({ source: "sqlite", backend: "exporting" });
    setRawMeta(root, "backend", "sqlite");
    setRawMeta(root, "epoch", 1); // file epoch above the database epoch
    expect(() => resolveMeshStateSource(root, { onAlarm })).toThrow(/exceeds the database epoch/);
    setRawMeta(root, "epoch", 2);
    setRawMeta(root, "backend", "retired");
    expect(() => resolveMeshStateSource(root, { onAlarm })).toThrow(/not sqlite, exporting or file/);
    expect(alarms.map(alarm => alarm.code)).toEqual(["FABRIC_MESH_BACKEND_FENCE", "FABRIC_MESH_BACKEND_FENCE", "FABRIC_MESH_BACKEND_FENCE"]);
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

describe("mesh backend cutover census", () => {
  it("refuses unless the census shows no file-mode writer, and alarms on a late one", async () => {
    const root = tempRoot("cutover");
    await seedFileRoot(root);
    await expect(cutoverMeshState(root)).rejects.toThrow(/needs the writer census/);
    await expect(cutoverMeshState(root, { census: async () => ({ writers: [
      { pid: 101, release: "3.2.0", mode: "sqlite" }, { pid: 102, release: "3.1.57", mode: "file" },
    ] }) })).rejects.toThrow(/pid 102 \(file, 3\.1\.57\)/);
    expect(resolveMeshStateSource(root).source).toBe("file");

    let calls = 0;
    const alarms: MeshBackendAlarm[] = [];
    const result = await cutoverMeshState(root, {
      census: async () => ({ writers: calls++ === 0 ? [{ pid: 101, release: "3.2.0", mode: "sqlite" }] : [{ pid: 103, release: "3.1.57", mode: "file" }] }),
      onAlarm: (alarm) => { alarms.push(alarm); },
    });
    expect(result).toMatchObject({ backend: "sqlite", epoch: 1, writers: [{ pid: 101 }], lateWriters: [{ pid: 103 }] });
    expect(alarms).toMatchObject([{ code: "FABRIC_MESH_BACKEND_LATE_WRITER" }]);
    const status = await meshBackendStatus(root, { census: async () => ({ writers: [{ pid: 101, release: "3.2.0", mode: "sqlite" }] }) });
    expect(status).toMatchObject({ backend: "sqlite", writers: [{ pid: 101, mode: "sqlite" }] });
  });
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
    expect(status.fileDigest).toBe(original);

    const rolled = await rollbackMeshState(root, { assumeNoWriters: true });
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
    const refused = await run("cutover", "--root", root);
    expect(refused.code).toBe(3);
    expect(refused.err).toMatch(/assume-no-writers/);
    const cutover = await run("cutover", "--root", root, "--assume-no-writers", "--json");
    expect(cutover.code).toBe(0);
    expect(JSON.parse(cutover.out)).toMatchObject({ command: "cutover", ok: true, backend: "sqlite", epoch: 1 });
    expect((await run("abort-rollback", "--root", root)).code).toBe(3);
    const rollback = await run("rollback", "--root", root, "--assume-no-writers");
    expect(rollback.code).toBe(0);
    expect(rollback.out).toMatch(/rollback done: backend=file epoch 2, steps 1,2,3,4,5/);
    expect((await run("status", "--root", root)).out).toMatch(/readers use {3}file/);
    setRawMeta(root, "epoch", 5);
    const broken = await run("status", "--root", root);
    expect(broken.code).toBe(3);
    expect(broken.out).toMatch(/fence {9}VIOLATED/);
  });
});
