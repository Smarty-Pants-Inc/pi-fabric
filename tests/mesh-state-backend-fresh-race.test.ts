import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MeshBackendFenceError } from "../src/mesh/backend-fence.js";
import { MeshStateUnsupportedError } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity, type MeshStoreOptions } from "../src/mesh/store.js";

const identity: MeshIdentity = { id: "fresh-race", name: "fresh-race", kind: "agent" };
const FIXTURE_DEFAULT = Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures");
const globals = globalThis as Record<symbol, unknown>;
const roots: string[] = [];
const stores: MeshStore[] = [];
let savedFixturePresent = false;
let savedFixtureValue: unknown;
let savedBackendEnv: string | undefined;

const CHILD_B = `
import fs from "node:fs";
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
const [root] = process.argv.slice(1);
const fixture = Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures");
if (fixture in globalThis) throw new Error("test fixture global leaked into the child");
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { MeshStore } = await jiti.import("./src/mesh/store.ts");
const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
if (store.stateBackend !== "sqlite") throw new Error("child did not select sqlite");
let entry;
try {
  entry = await store.put({ key: "from-b", value: { owner: "B" }, identity: { id: "child-b", name: "child-b", kind: "agent" } });
} finally {
  store.closeState();
}
process.stdout.write(JSON.stringify({ fixtureAbsent: !(fixture in globalThis), backend: "sqlite", value: entry.value, stateDb: fs.existsSync(root + "/state.db") }) + "\\n");
`;

const tempRoot = (label: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-fresh-race-${label}-`));
  roots.push(root);
  return root;
};

const open = (root: string, options: MeshStoreOptions = {}): MeshStore => {
  const store = new MeshStore(root, 64 * 1024, 1_000, options);
  stores.push(store);
  return store;
};

const runChildB = (root: string): void => {
  const result = spawnSync(process.execPath, ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", CHILD_B, root], {
    cwd: process.cwd(),
    env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" },
    encoding: "utf8",
    timeout: 20_000,
    killSignal: "SIGKILL",
  });
  expect(result.error).toBeUndefined();
  expect(result.signal).toBeNull();
  expect(result.status, result.stderr).toBe(0);
  expect(result.stderr).toBe("");
  const line = result.stdout.trim().split("\n").at(-1);
  expect(line, result.stdout).toBeTruthy();
  expect(JSON.parse(line!)).toEqual({ fixtureAbsent: true, backend: "sqlite", value: { owner: "B" }, stateDb: true });
};

const sqliteArtifacts = (root: string): string[] => ["state.db", "state.db-wal", "state.db-shm"]
  .filter(name => fs.existsSync(path.join(root, name)));

beforeEach(() => {
  savedFixturePresent = Object.prototype.hasOwnProperty.call(globals, FIXTURE_DEFAULT);
  savedFixtureValue = globals[FIXTURE_DEFAULT];
  delete globals[FIXTURE_DEFAULT];
  savedBackendEnv = process.env.PI_FABRIC_MESH_STATE_BACKEND;
  delete process.env.PI_FABRIC_MESH_STATE_BACKEND;
});

afterEach(() => {
  for (const store of stores.splice(0)) try { store.closeState(); } catch { /* closed by the test */ }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  if (savedFixturePresent) globals[FIXTURE_DEFAULT] = savedFixtureValue;
  else delete globals[FIXTURE_DEFAULT];
  if (savedBackendEnv === undefined) delete process.env.PI_FABRIC_MESH_STATE_BACKEND;
  else process.env.PI_FABRIC_MESH_STATE_BACKEND = savedBackendEnv;
});

describe("fresh shared-root backend selection", () => {
  it.each(["in-process", "child-process"] as const)("fences a previously opened file writer after a %s SQLite writer creates state.db", async writer => {
    const root = tempRoot("sqlite-first");
    const file = open(root);
    expect(file.stateBackend).toBe("file");
    expect(file.get("before-b", { fresh: true })).toBeUndefined();

    if (writer === "child-process") runChildB(root);
    else {
      const sqlite = open(root, { stateBackend: "sqlite" });
      expect(sqlite.stateBackend).toBe("sqlite");
      await sqlite.put({ key: "from-b", value: { owner: "B" }, identity });
    }
    const stateFile = path.join(root, "state.json");
    const markerBytes = fs.readFileSync(stateFile);
    expect(sqliteArtifacts(root)).toContain("state.db");

    const lateFileKey = "from-a-after-b";
    await expect(file.put({ key: lateFileKey, value: { owner: "A" }, identity }))
      .rejects.toBeInstanceOf(MeshBackendFenceError);
    expect(fs.readFileSync(stateFile)).toEqual(markerBytes);
    expect(file.stateBackend).toBe("file");

    const markerAside = `${stateFile}.aside`;
    fs.renameSync(stateFile, markerAside);
    const asideBytes = fs.readFileSync(markerAside);
    await expect(file.put({ key: "from-a-without-marker", value: true, identity }))
      .rejects.toBeInstanceOf(MeshBackendFenceError);
    expect(fs.existsSync(stateFile)).toBe(false);
    expect(fs.readFileSync(markerAside)).toEqual(asideBytes);
    expect(() => new MeshStore(root, 64 * 1024, 1_000)).toThrow(MeshBackendFenceError);
    fs.renameSync(markerAside, stateFile);
    file.closeState();

    const fresh = open(root);
    expect(fresh.stateBackend).toBe("sqlite");
    expect(fresh.get("from-b")?.value).toEqual({ owner: "B" });
    expect(fresh.get(lateFileKey)).toBeUndefined();
    expect(fresh.get("from-a-without-marker")).toBeUndefined();
  });

  it("refuses explicit sqlite on first lazy use after file data exists, without importing or creating a database", async () => {
    const root = tempRoot("file-first");
    const file = open(root);
    await file.put({ key: "from-a", value: { owner: "A" }, identity });
    const stateFile = path.join(root, "state.json");
    const fileBytes = fs.readFileSync(stateFile);
    expect(sqliteArtifacts(root)).toEqual([]);

    const sqlite = open(root, { stateBackend: "sqlite" });
    expect(sqlite.stateBackend).toBe("sqlite");
    expect(sqliteArtifacts(root)).toEqual([]);
    expect(() => sqlite.get("from-a")).toThrow(MeshStateUnsupportedError);
    expect(() => sqlite.get("from-a")).toThrow(/this root has file-backend state; run `fabric-mesh-backend import --root .+` first/);
    await expect(sqlite.put({ key: "from-b", value: { owner: "B" }, identity }))
      .rejects.toThrow(/this root has file-backend state; run `fabric-mesh-backend import --root .+` first/);
    expect(fs.readFileSync(stateFile)).toEqual(fileBytes);
    expect(sqliteArtifacts(root)).toEqual([]);
    expect(file.get("from-a", { fresh: true })?.value).toEqual({ owner: "A" });
  });
});
