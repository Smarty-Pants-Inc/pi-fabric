import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
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

const runChild = (root: string, count: number): Promise<ChildResult> => new Promise((resolve, reject) => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, root, String(count)],
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
