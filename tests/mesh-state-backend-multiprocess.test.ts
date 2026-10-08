import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { isMeshLockTimeout } from "../src/core/atomic-write.js";
import { MeshStateBusyError } from "../src/mesh/state-backend.js";
import { MeshStore } from "../src/mesh/store.js";

// Four processes write one sqlite-backed mesh root concurrently (smarty-dev#6477 L2a). Each child
// counts every attempt to create the mesh `.lock` (mkdir/mkdtemp of `.lock*` in the root) and the
// parent watches the root too: state writes on sqlite must never take it.
const CHILD = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
import path from "node:path";
const [root, countText] = process.argv.slice(1);
const count = Number(countText);
let lockAttempts = 0;
const isLock = (target) => path.basename(String(target)).startsWith(".lock") && path.dirname(path.resolve(String(target))) === path.resolve(root);
for (const name of ["mkdirSync", "mkdtempSync", "renameSync"]) {
  const original = fs[name];
  fs[name] = function (...args) { if (isLock(args[0]) || (name === "renameSync" && isLock(args[1]))) lockAttempts += 1; return original.apply(this, args); };
}
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { MeshStore } = await jiti.import("./src/mesh/store.ts");
const store = new MeshStore(root, 64 * 1024, 1000, { stateBackend: "sqlite" });
if (store.stateBackend !== "sqlite") throw new Error("not sqlite: " + JSON.stringify(store.stateDiagnostics()));
const identity = { id: "child-" + process.pid, name: "child", kind: "agent" };
let conflicts = 0;
for (let step = 0; step < count; step += 1) {
  // An atomic read-modify-write in one transaction, and a compare-and-swap retry loop.
  await store.writeBatch({ identity, ops: [], prepare: (view) => [{ kind: "put", key: "counter", value: (view.get("counter")?.value ?? 0) + 1 }] });
  for (;;) {
    const current = store.get("cas");
    try { await store.put({ key: "cas", value: (current?.value ?? 0) + 1, identity, ifVersion: current?.version ?? 0 }); break; }
    catch (error) { if (!/compare-and-swap|version/i.test(String(error?.message))) throw error; conflicts += 1; }
  }
  await store.put({ key: "own/" + process.pid, value: step, identity });
}
store.closeState();
process.stdout.write(JSON.stringify({ lockAttempts, conflicts }) + "\\n");
`;

interface ChildResult { code: number | null; stdout: string; stderr: string }

// node:sqlite prints "ExperimentalWarning: SQLite is an experimental feature" on Node 22 (CI's
// runner Node); production does not suppress it, so only these test children silence that one
// warning class. Every other stderr byte still fails the assertion below.
const runChild = (root: string, count: number): Promise<ChildResult> => new Promise((resolve, reject) => {
  const child = spawn(process.execPath, ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", CHILD, root, String(count)],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "" } });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.once("error", reject);
  child.once("close", (code) => resolve({ code, stdout, stderr }));
});

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("sqlite state backend across processes", () => {
  it("four writers commit every update and none takes the mesh .lock", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-state-backend-mp-"));
    roots.push(root);
    const seen: string[] = [];
    const watcher = fs.watch(root, (_event, name) => { if (name) seen.push(String(name)); });
    const count = 40;
    let results: ChildResult[];
    try {
      results = await Promise.all(Array.from({ length: 4 }, () => runChild(root, count)));
      await new Promise(resolve => setTimeout(resolve, 50));
    } finally { watcher.close(); }
    for (const result of results) {
      expect(result.stderr).toBe("");
      expect(result.code).toBe(0);
      expect(JSON.parse(result.stdout.trim())).toMatchObject({ lockAttempts: 0 });
    }
    expect(seen.filter(name => name.startsWith(".lock"))).toEqual([]);
    expect(fs.existsSync(path.join(root, "state.json"))).toBe(false);
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite" });
    try {
      expect(store.get("counter")?.value).toBe(4 * count);
      expect(store.get("cas")?.value).toBe(4 * count);
      expect(store.listAll("own/").map(entry => entry.value)).toEqual([count - 1, count - 1, count - 1, count - 1]);
    } finally { store.closeState(); }
  }, 120_000);
});

// pi-fabric#626 review round 1: another process holds BEGIN IMMEDIATE on a fresh root's state.db,
// so this store's first write cannot initialise the database. The withTryLock budget must bound
// that first open (schema, WAL setup, busy wait), not the store's much longer lockTimeoutMs.
const HOLDER = `
const { DatabaseSync } = await import("node:sqlite");
const db = new DatabaseSync(process.argv[1]);
if (process.argv[2] === "wal") db.exec("PRAGMA journal_mode = WAL");
db.exec("BEGIN IMMEDIATE");
process.stdout.write("held\\n");
setInterval(() => undefined, 1_000);
`;

const holdState = (root: string, mode: "wal" | "delete"): Promise<() => Promise<void>> => new Promise((resolve, reject) => {
  const child = spawn(process.execPath, ["--no-warnings", "--input-type=module", "-e", HOLDER, path.join(root, "state.db"), mode],
    { stdio: ["ignore", "pipe", "pipe"] });
  const exited = new Promise<void>((done) => child.once("close", () => done()));
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.once("error", reject);
  child.once("close", (code) => reject(new Error(`holder exited ${code}: ${stderr}`)));
  child.stdout.on("data", (chunk) => {
    if (String(chunk).includes("held")) resolve(async () => { child.kill("SIGKILL"); await exited; });
  });
});

const firstOpenIdentity = { id: "first-open", name: "first-open", kind: "agent" as const };

describe("sqlite state backend first open under a foreign BEGIN IMMEDIATE", () => {
  const budgetMs = 50;
  it.each(["wal", "delete"] as const)("a bounded try returns the timeout within its budget (holder journal %s)", async (mode) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-state-backend-first-open-"));
    roots.push(root);
    const release = await holdState(root, mode);
    const store = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "sqlite", lockTimeoutMs: 5_000 });
    try {
      // The try starts the first open; an ordinary writer joins it and must keep its own budget.
      const started = performance.now();
      const tried = store.withTryLock(() => store.put({ key: "k", value: "try", identity: firstOpenIdentity }), budgetMs)
        .then(() => undefined, (error: unknown) => ({ error, waited: performance.now() - started }));
      const ordinary = store.put({ key: "k", value: "ordinary", identity: firstOpenIdentity });
      const outcome = await tried;
      expect(outcome?.error).toBeInstanceOf(MeshStateBusyError);
      expect(isMeshLockTimeout(outcome?.error)).toBe(true);
      expect(outcome!.waited).toBeLessThan(budgetMs + 250);
      // Now the ordinary writer's open is in flight: a bounded try that joins it keeps its own budget.
      const joinStarted = performance.now();
      const joined = await store.withTryLock(() => store.put({ key: "j", value: "try", identity: firstOpenIdentity }), budgetMs)
        .then(() => undefined, (error: unknown) => ({ error, waited: performance.now() - joinStarted }));
      expect(joined?.error).toBeInstanceOf(MeshStateBusyError);
      expect(joined!.waited).toBeLessThan(budgetMs + 250);
      expect(store.stateDiagnostics().busyTimeouts).toBe(2);
      await release();
      expect((await ordinary).value).toBe("ordinary");
      expect((await store.put({ key: "k", value: "after", identity: firstOpenIdentity })).value).toBe("after");
    } finally {
      await release();
      store.closeState();
    }
  }, 30_000);
});
