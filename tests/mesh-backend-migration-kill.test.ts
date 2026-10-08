import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { abortMeshRollback, cutoverMeshState, importMeshState, meshSnapshotDigest, readMeshStateMovedMarker, readStateFileEpoch, resolveMeshStateSource,
  rollbackMeshState, type MeshBackendStep, type MeshStateSnapshot } from "../src/mesh/backend-migration.js";
import { decodeMeshStateFile } from "../src/mesh/state-file.js";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { legacyPut } from "./fixtures/legacy-mesh-state-04930dfd.js";

// smarty-dev#6477 L4b: the tool is SIGKILLed after each step of its commit order (plan section 5);
// the rerun converges, and the file epoch never exceeds the database epoch at any crash point.

const identity: MeshIdentity = { id: "tester", name: "tester", kind: "agent" };
const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-backend-kill-${label}-`));
  roots.push(root);
  return root;
};

const CHILD = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const migration = await jiti.import("./src/mesh/backend-migration.ts");
const [operation, root, killAt] = process.argv.slice(-3);
const options = { onStep: (step) => {
  if (step !== killAt) return;
  process.kill(process.pid, "SIGKILL");
  for (;;) { /* never return into the next step */ }
} };
const run = operation === "import" ? migration.importMeshState : operation === "cutover" ? migration.cutoverMeshState
  : operation === "rollback" ? migration.rollbackMeshState : migration.abortMeshRollback;
await run(root, options);
process.stdout.write("completed\\n");
`;

const runChild = (operation: string, root: string, killAt: MeshBackendStep): Promise<{ code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }> =>
  new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, operation, root, killAt], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });

type ChildResult = Awaited<ReturnType<typeof runChild>>;

/** The child died by its own SIGKILL. On Windows Node emulates it with TerminateProcess:
 *  signalCode stays null and the exit code is non-zero instead. */
const expectKilled = (child: ChildResult): void => {
  expect(child.stdout, child.stderr).not.toContain("completed");
  if (process.platform === "win32") {
    expect(child.signal, child.stderr).toBeNull();
    expect(child.code, child.stderr).not.toBeNull();
    expect(child.code, child.stderr).not.toBe(0);
  } else {
    expect(child.signal, child.stderr).toBe("SIGKILL");
  }
};

const seed = async (root: string): Promise<void> => {
  const store = new MeshStore(root, 64 * 1024, 1_000);
  await store.writeBatch({ identity, ops: Array.from({ length: 40 }, (_, index) => ({ kind: "put" as const, key: `kill/n${index % 4}/${index}`, value: { index } })) });
  for (const index of [1, 2, 5]) await store.delete({ key: `kill/n${index % 4}/${index}` });
};

const meta = (root: string): { backend: string; epoch: number; values: Record<string, unknown> } => {
  const db = openNodeSqlite(path.join(root, "state.db"));
  try {
    const values = Object.fromEntries(db.prepare("SELECT name, value FROM meta").all().map(row => [String(row.name), row.value]));
    return { backend: String(values.backend), epoch: Number(values.epoch), values };
  } finally { db.close(); }
};

const fileState = (root: string): MeshStateSnapshot & { readGeneration?: string; backendDigest?: string } =>
  decodeMeshStateFile(path.join(root, "state.json"), 64 * 1024 * 1024, false) as never;

const dbDigest = async (root: string): Promise<string> => {
  const db = openNodeSqlite(path.join(root, "state.db"));
  try {
    const entries: MeshStateSnapshot["entries"] = {};
    const versions: Record<string, number> = {};
    for (const row of db.prepare("SELECT key, value, version, updated_at, updated_by FROM kv").all()) {
      entries[String(row.key)] = { key: String(row.key), value: JSON.parse(String(row.value)), version: Number(row.version),
        updatedAt: Number(row.updated_at), updatedBy: JSON.parse(String(row.updated_by)) };
      versions[String(row.key)] = Number(row.version);
    }
    const tombstoneOrder: string[] = [];
    for (const row of db.prepare("SELECT key, version FROM tombstones ORDER BY ord").all()) {
      tombstoneOrder.push(String(row.key));
      versions[String(row.key)] = Number(row.version);
    }
    const highWater = Number(db.prepare("SELECT value FROM meta WHERE name = 'high_water'").get()?.value);
    return meshSnapshotDigest({ entries, versions, tombstoneOrder, highWater });
  } finally { db.close(); }
};

/** The fence at an arbitrary crash point. */
const assertFence = (root: string): void => {
  const fileEpoch = readStateFileEpoch(root);
  if (!fs.existsSync(path.join(root, "state.db"))) { expect(fileEpoch).toBe(0); return; }
  const { backend, epoch, values } = meta(root);
  expect(["sqlite", "importing", "exporting", "file"]).toContain(backend);
  expect(fileEpoch).toBeLessThanOrEqual(epoch);
  const moved = readMeshStateMovedMarker(root) !== undefined;
  // Roll-forward (review round 5): backend=sqlite only with the marker; importing reads the marker or state.json.
  if (backend === "sqlite") expect(moved).toBe(true);
  if (backend === "importing") {
    if (moved) expect(fileEpoch).toBe(epoch);
    else expect(fileEpoch).toBeLessThan(epoch);
    expect(resolveMeshStateSource(root).source).toBe(moved ? "sqlite" : "file");
    return;
  }
  if (backend === "file" && moved) {
    // Between the switch and the replace (review round 4): the marker still stands, readers use SQLite.
    expect(fileEpoch).toBeLessThan(epoch);
    expect(resolveMeshStateSource(root).source).toBe("sqlite");
    return;
  }
  if (backend === "file") {
    // Equal only after a verified export: the switch records it, and the bytes still match it.
    expect(fileEpoch).toBe(epoch);
    // The tool seeds a fresh database at backend=file, epoch 0: the legacy file authority, no export yet.
    if (epoch === 0) { expect(values.export_generation).toBeUndefined(); return; }
    const file = fileState(root);
    expect(file.readGeneration).toBe(values.export_generation);
    expect(meshSnapshotDigest(file)).toBe(values.export_digest);
  }
  // Readers never fail on a crash point of the tool: sqlite until the switch, then the file.
  expect(resolveMeshStateSource(root).source).toBe(backend === "file" ? "file" : "sqlite");
};

const temps = (root: string): string[] =>
  fs.readdirSync(root).filter(name => name.startsWith("state.json.mesh-backend-") || /^state\.json\.rollback-\d+\.tmp$/.test(name));

/** A legacy (04930dfd) put: true when it committed, false when the strict read refused the marker. */
const legacyCommits = async (root: string, key: string): Promise<boolean> => {
  try { await legacyPut(root, key, { legacy: true }); return true; }
  catch (error) {
    expect((error as Error).message).toBe("Failed to read Fabric mesh state: invalid state format");
    return false;
  }
};

const dbHas = (root: string, key: string): boolean => {
  const db = openNodeSqlite(path.join(root, "state.db"));
  try { return db.prepare("SELECT 1 AS present FROM kv WHERE key = ?").get(key) !== undefined; }
  finally { db.close(); }
};

describe("mesh backend tool killed after each step", () => {
  const fencedSteps = ["import-read", "import-commit", "import-verify", "cutover-reconcile", "cutover-copy", "cutover-marker", "cutover-flag"] as const;
  // Import commits backend=sqlite, so it runs the same fenced section as cutover (review round 4).
  // Roll-forward is marker BEFORE flag (review round 5): before the marker the flag is file or importing.
  it.each((["import", "cutover"] as const).flatMap(operation => fencedSteps.map(step => [operation, step] as const)))(
    "%s killed after %s: a legacy writer never commits to a file readers ignore; the rerun converges with no lost write", async (operation, step) => {
      const root = tempRoot(`${operation}-${step}`);
      await seed(root);
      const original = meshSnapshotDigest(fileState(root));
      const killed = await runChild(operation, root, step);
      expectKilled(killed);
      // It died holding .lock: no file-mode writer could have committed meanwhile.
      expect(fs.existsSync(path.join(root, ".lock"))).toBe(true);
      assertFence(root);
      const markerLanded = step === "cutover-marker" || step === "cutover-flag";
      expect(meta(root)).toMatchObject({ backend: step === "import-read" ? "file" : step === "cutover-flag" ? "sqlite" : "importing",
        epoch: step === "import-read" ? 0 : 1 });
      expect(readMeshStateMovedMarker(root)?.epoch).toBe(markerLanded ? 1 : undefined);
      // A legacy (04930dfd) writer takes over the dead tool's .lock. Before the marker state.json is the
      // authority readers use, so its commit is real; from the marker on it is refused, bytes unchanged.
      const before = fs.readFileSync(path.join(root, "state.json"));
      const committed = await legacyCommits(root, "legacy/after-kill");
      expect(committed).toBe(!markerLanded);
      if (committed) {
        expect(resolveMeshStateSource(root).source).toBe("file");
        expect(fileState(root).entries["legacy/after-kill"]?.value).toEqual({ legacy: true });
      } else {
        expect(fs.readFileSync(path.join(root, "state.json")).equals(before)).toBe(true);
        expect(resolveMeshStateSource(root).source).toBe("sqlite");
      }
      assertFence(root);
      const expected = committed ? meshSnapshotDigest(fileState(root)) : original;
      const rerun = await (operation === "import" ? importMeshState : cutoverMeshState)(root, { lockTimeoutMs: 20_000 });
      // Before the marker the rerun redoes the import from state.json; after it only the flag commit.
      expect(rerun).toMatchObject({ backend: "sqlite", epoch: 1, digest: expected, converged: markerLanded });
      assertFence(root);
      expect(meta(root)).toMatchObject({ backend: "sqlite", epoch: 1 });
      expect(await dbDigest(root)).toBe(expected);
      // No lost write: the legacy commit is in SQLite, the authority from now on.
      expect(dbHas(root, "legacy/after-kill")).toBe(committed);
      // Converged: state.json is the moved marker, the retired file is kept beside it.
      expect(readMeshStateMovedMarker(root)?.epoch).toBe(1);
      expect(meshSnapshotDigest(decodeMeshStateFile(path.join(root, "state.json.cutover-1"), 64 * 1024 * 1024, false) as never)).toBe(expected);
      expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
      // And a legacy writer stays refused.
      expect(await legacyCommits(root, "legacy/after-import")).toBe(false);
    }, 120_000);

  it.each(["rollback-flag", "rollback-export-temp", "rollback-verify", "rollback-switch", "rollback-replace"] as const)(
    "rollback killed after %s converges on rerun; a legacy writer never commits before the replace", async (step) => {
      const root = tempRoot(step);
      await seed(root);
      // A real cutover: state.json is the moved marker until the export (step 3) replaces it.
      await cutoverMeshState(root);
      const store = await SqliteStateStore.open(root, 64 * 1024, 1_000);
      await store.put({ key: "kill/live", value: "written on sqlite", identity });
      store.close();
      const committed = await dbDigest(root);
      const killed = await runChild("rollback", root, step);
      expectKilled(killed);
      assertFence(root);
      const switched = step === "rollback-switch" || step === "rollback-replace";
      expect(meta(root)).toMatchObject({ backend: switched ? "file" : "exporting", epoch: 2 });
      expect(temps(root)).toHaveLength(step === "rollback-flag" || step === "rollback-replace" ? 0 : 1);
      // The marker stands until the very last step (the replace).
      expect(readMeshStateMovedMarker(root)?.epoch).toBe(step === "rollback-replace" ? undefined : 1);
      if (step !== "rollback-replace") {
        const marker = fs.readFileSync(path.join(root, "state.json"));
        // A legacy writer takes over the dead tool's .lock and meets the marker: nothing commits.
        expect(await legacyCommits(root, "legacy/after-kill")).toBe(false);
        expect(fs.readFileSync(path.join(root, "state.json")).equals(marker)).toBe(true);
      }
      const rerun = await rollbackMeshState(root, { lockTimeoutMs: 20_000 });
      expect(rerun).toMatchObject({ backend: "file", epoch: 2, converged: step === "rollback-replace",
        steps: step === "rollback-replace" ? [] : step === "rollback-switch" ? [5] : [3, 4, 5] });
      assertFence(root);
      expect(temps(root)).toEqual([]);
      expect(meshSnapshotDigest(fileState(root))).toBe(committed);
      expect(await dbDigest(root)).toBe(committed);
      expect(new MeshStore(root, 64 * 1024, 1_000).get("kill/live", { fresh: true })?.value).toBe("written on sqlite");
    }, 120_000);

  it.each(["rollback-flag", "rollback-verify"] as const)("abort-rollback after a kill at %s returns to sqlite at E+1", async (step) => {
    const root = tempRoot(`abort-${step}`);
    await seed(root);
    await cutoverMeshState(root);
    const committed = await dbDigest(root);
    expectKilled(await runChild("rollback", root, step));
    expectKilled(await runChild("abort", root, "abort-rollback"));
    assertFence(root);
    expect(await abortMeshRollback(root)).toMatchObject({ backend: "sqlite", epoch: 2, converged: true });
    assertFence(root);
    // Back on sqlite: the marker never left (review round 4), the export temp is gone, legacy writers stay fenced.
    expect(readMeshStateMovedMarker(root)?.epoch).toBe(1);
    expect(temps(root)).toEqual([]);
    expect(await legacyCommits(root, "legacy/after-abort")).toBe(false);
    const store = await SqliteStateStore.open(root, 64 * 1024, 1_000);
    try { expect(meshSnapshotDigest(store.exportState())).toBe(committed); }
    finally { store.close(); }
    // The next rollback takes E+2; the file epoch never passes the database epoch.
    expect(await rollbackMeshState(root)).toMatchObject({ backend: "file", epoch: 3 });
    assertFence(root);
  }, 120_000);
});
