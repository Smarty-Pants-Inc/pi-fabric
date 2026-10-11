import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";

// smarty-dev#9805: SQLite mesh writers starved for 30-67 s while no hold exceeded 1.6 s and the lock was busy only
// 2-22% of the time. Fairness bound: with N contenders and short holds, no write waits longer than 2 x (N x max hold).
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

const CHILD = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
import path from "node:path";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { SqliteStateStore } = await jiti.import("./src/mesh/state-sqlite.ts");
const [root, go, index, writes, holdMs] = process.argv.slice(1);
const identity = { id: "child-" + process.pid, name: "child", kind: "agent" };
const store = await SqliteStateStore.open(root, 65536, 1000, { lockTimeoutMs: 60_000 });
fs.writeFileSync(path.join(root, "ready-" + index), "");
while (!fs.existsSync(go)) await new Promise(r => setTimeout(r, 2));
const waits = []; let maxHold = 0;
for (let i = 0; i < Number(writes); i++) {
  const asked = performance.now();
  await store.writeBatch({ identity, ops: [], prepare: () => {
    const began = performance.now();
    waits.push(began - asked);
    while (performance.now() - began < Number(holdMs)) { /* a short synchronous hold */ }
    maxHold = Math.max(maxHold, performance.now() - began);
    return [{ kind: "put", key: "k/" + index, value: i }];
  } });
  await new Promise(r => setImmediate(r)); // a writer comes straight back, as busy live paths do
}
console.log(JSON.stringify({ maxWait: Math.max(...waits), maxHold }));
store.close();
`;

const run = (args: string[]) => new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", CHILD, ...args], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
  let stdout = ""; let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.once("error", reject);
  child.once("close", (code) => resolve({ code, stdout, stderr }));
});

describe.skipIf(process.platform === "win32")("SQLite mesh writer fairness (smarty-dev#9805)", () => {
  it("16 contenders with short holds: no write waits longer than 2 x (16 x max hold)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sqlite-fair-"));
    roots.push(root);
    (await SqliteStateStore.open(root, 65536, 1000, { initialize: "create" })).close();
    const go = path.join(root, "go");
    const contenders = 16, writes = 30, holdMs = 10;
    const children = Array.from({ length: contenders }, (_, index) => run([root, go, String(index), String(writes), String(holdMs)]));
    const deadline = Date.now() + 90_000;
    while (fs.readdirSync(root).filter((name) => name.startsWith("ready-")).length < contenders) {
      if (Date.now() > deadline) throw new Error("children not ready");
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    fs.writeFileSync(go, "");
    const results = await Promise.all(children);
    for (const result of results) expect(result.code, result.stderr).toBe(0);
    const reports = results.map((result) => JSON.parse(result.stdout.trim().split("\n").at(-1)!) as { maxWait: number; maxHold: number });
    const maxHold = Math.max(...reports.map((report) => report.maxHold));
    const maxWait = Math.max(...reports.map((report) => report.maxWait));
    console.log(JSON.stringify({ contenders, maxHold: Math.round(maxHold), maxWait: Math.round(maxWait), bound: Math.round(2 * contenders * maxHold) }));
    expect(maxWait).toBeLessThanOrEqual(2 * contenders * maxHold);
  }, 180_000);
});
