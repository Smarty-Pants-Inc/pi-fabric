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
const options = { assumeNoWriters: true, onStep: (step) => {
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
  expect(["sqlite", "exporting", "file"]).toContain(backend);
  expect(fileEpoch).toBeLessThanOrEqual(epoch);
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

const temps = (root: string): string[] => fs.readdirSync(root).filter(name => name.startsWith("state.json.mesh-backend-"));

describe("mesh backend tool killed after each step", () => {
  it.each(["import-read", "import-commit", "import-verify"] as const)("import killed after %s converges on rerun", async (step) => {
    const root = tempRoot(step);
    await seed(root);
    const original = meshSnapshotDigest(fileState(root));
    const killed = await runChild("import", root, step);
    expect(killed.signal, killed.stderr).toBe("SIGKILL");
    assertFence(root);
    expect(meta(root).backend).toBe(step === "import-read" ? "file" : "sqlite");
    // The rerun takes over the dead tool's .lock and converges.
    const rerun = await importMeshState(root, { lockTimeoutMs: 20_000 });
    expect(rerun).toMatchObject({ backend: "sqlite", epoch: 1, digest: original, converged: step !== "import-read" });
    assertFence(root);
    expect(await dbDigest(root)).toBe(original);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  }, 120_000);

  it.each(["import-read", "import-commit", "import-verify", "cutover-reconcile", "cutover-copy", "cutover-marker"] as const)(
    "cutover killed after %s (inside its .lock section) converges on rerun", async (step) => {
      const root = tempRoot(`cutover-${step}`);
      await seed(root);
      const original = meshSnapshotDigest(fileState(root));
      const killed = await runChild("cutover", root, step);
      expect(killed.signal, killed.stderr).toBe("SIGKILL");
      // It died holding .lock: no file-mode writer could have committed meanwhile.
      expect(fs.existsSync(path.join(root, ".lock"))).toBe(true);
      assertFence(root);
      expect(meta(root).backend).toBe(step === "import-read" ? "file" : "sqlite");
      // Before its last step state.json is still the imported file; the marker is written only last.
      expect(readMeshStateMovedMarker(root)?.epoch).toBe(step === "cutover-marker" ? 1 : undefined);
      const rerun = await cutoverMeshState(root, { assumeNoWriters: true, lockTimeoutMs: 20_000 });
      expect(rerun).toMatchObject({ backend: "sqlite", epoch: 1, digest: original, converged: step !== "import-read" });
      assertFence(root);
      expect(await dbDigest(root)).toBe(original);
      // Converged: state.json is the moved marker, the retired file is kept beside it.
      expect(readMeshStateMovedMarker(root)?.epoch).toBe(1);
      expect(meshSnapshotDigest(decodeMeshStateFile(path.join(root, "state.json.cutover-1"), 64 * 1024 * 1024, false) as never)).toBe(original);
      expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    }, 120_000);

  it.each(["rollback-flag", "rollback-export-temp", "rollback-export", "rollback-verify", "rollback-switch"] as const)(
    "rollback killed after %s converges on rerun", async (step) => {
      const root = tempRoot(step);
      await seed(root);
      // A real cutover: state.json is the moved marker until the export (step 3) replaces it.
      await cutoverMeshState(root, { assumeNoWriters: true });
      const store = await SqliteStateStore.open(root, 64 * 1024, 1_000);
      await store.put({ key: "kill/live", value: "written on sqlite", identity });
      store.close();
      const committed = await dbDigest(root);
      const killed = await runChild("rollback", root, step);
      expect(killed.signal, killed.stderr).toBe("SIGKILL");
      assertFence(root);
      expect(meta(root)).toMatchObject({ backend: step === "rollback-switch" ? "file" : "exporting", epoch: 2 });
      if (step === "rollback-export-temp") expect(temps(root)).toHaveLength(1);
      expect(readMeshStateMovedMarker(root)?.epoch).toBe(step === "rollback-flag" || step === "rollback-export-temp" ? 1 : undefined);
      const rerun = await rollbackMeshState(root, { lockTimeoutMs: 20_000 });
      expect(rerun).toMatchObject({ backend: "file", epoch: 2, converged: step === "rollback-switch" });
      assertFence(root);
      expect(temps(root)).toEqual([]);
      expect(meshSnapshotDigest(fileState(root))).toBe(committed);
      expect(await dbDigest(root)).toBe(committed);
      expect(new MeshStore(root, 64 * 1024, 1_000).get("kill/live", { fresh: true })?.value).toBe("written on sqlite");
    }, 120_000);

  it.each(["rollback-flag", "rollback-export"] as const)("abort-rollback after a kill at %s returns to sqlite at E+1", async (step) => {
    const root = tempRoot(`abort-${step}`);
    await seed(root);
    await cutoverMeshState(root, { assumeNoWriters: true });
    const committed = await dbDigest(root);
    expect((await runChild("rollback", root, step)).signal).toBe("SIGKILL");
    const killedAbort = await runChild("abort", root, "abort-rollback");
    expect(killedAbort.signal).toBe("SIGKILL");
    assertFence(root);
    expect(await abortMeshRollback(root)).toMatchObject({ backend: "sqlite", epoch: 2, converged: true });
    assertFence(root);
    // Back on sqlite: an export already written is replaced by the marker again (legacy writers stay fenced).
    expect(readMeshStateMovedMarker(root)?.epoch).toBe(step === "rollback-flag" ? 1 : 2);
    const store = await SqliteStateStore.open(root, 64 * 1024, 1_000);
    try { expect(meshSnapshotDigest(store.exportState())).toBe(committed); }
    finally { store.close(); }
    // The next rollback takes E+2; the file epoch never passes the database epoch.
    expect(await rollbackMeshState(root, { assumeNoWriters: true })).toMatchObject({ backend: "file", epoch: 3 });
    assertFence(root);
  }, 120_000);
});
