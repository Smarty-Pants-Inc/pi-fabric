import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { openNodeSqlite, SqliteStateStore } from "../src/mesh/state-sqlite.js";

// smarty-dev#9805: native SQLite contenders remain asynchronous while short synchronous holds complete.
const roots: string[] = [];
const children: ChildProcess[] = [];
afterEach(() => {
  for (const child of children.splice(0)) if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const PRELUDE = `
import { createJiti } from "jiti";
import { pathToFileURL } from "node:url";
import { monitorEventLoopDelay } from "node:perf_hooks";
import fs from "node:fs";
import path from "node:path";
const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
const { SqliteStateStore } = await jiti.import("./src/mesh/state-sqlite.ts");
const identity = { id: "child-" + process.pid, name: "child", kind: "agent" };
const spin = (ms) => { const began = performance.now(); while (performance.now() - began < ms) { /* a synchronous hold */ } };
`;

// Writes in a tight loop, each holding the lock `holdMs`; reports its max wait, max hold, event-loop p99 and checkpoints.
const WRITER = `
const [root, go, index, writes, holdMs] = process.argv.slice(1);
const store = await SqliteStateStore.open(root, 65536, 1000, { lockTimeoutMs: 60_000 });
fs.writeFileSync(path.join(root, "ready-" + index), "");
while (!fs.existsSync(go)) await new Promise(r => setTimeout(r, 2));
const loop = monitorEventLoopDelay({ resolution: 1 });
loop.enable();
const waits = []; let maxHold = 0;
for (let i = 0; i < Number(writes); i++) {
  const asked = performance.now();
  await store.writeBatch({ identity, ops: [], prepare: () => {
    const began = performance.now();
    waits.push(began - asked);
    spin(Number(holdMs));
    maxHold = Math.max(maxHold, performance.now() - began);
    return [
      { kind: "put", key: "k/" + index, value: i },
      { kind: "put", key: "receipt/" + index + "/" + i, value: { index, i } },
    ];
  } });
  await new Promise(r => setImmediate(r)); // a writer comes straight back, as busy live paths do
}
loop.disable();
const { checkpoints } = store.stats();
console.log(JSON.stringify({ maxWait: Math.max(...waits), maxHold, loopP99Ms: loop.percentile(99) / 1e6, checkpoints }));
store.close();
`;

const run = (code: string, args: string[]) => {
  const child = spawn(process.execPath, ["--input-type=module", "-e", PRELUDE + code, ...args], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
  children.push(child);
  let stdout = ""; let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += chunk; });
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  const done = new Promise<{ code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
  return { child, done };
};
const waitFor = async (predicate: () => boolean, what: string, timeoutMs = 90_000) => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};
const fresh = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sqlite-fair-"));
  roots.push(root);
  (await SqliteStateStore.open(root, 65536, 1000, { initialize: "create" })).close();
  return root;
};
describe.skipIf(process.platform === "win32")("SQLite native writer contention (smarty-dev#9805)", () => {
  it("16 native contenders with short holds: exact receipts and responsive event loops", async () => {
    const root = await fresh();
    const go = path.join(root, "go");
    const contenders = 16, writes = 30, holdMs = 10;
    const writers = Array.from({ length: contenders }, (_, index) => run(WRITER, [root, go, String(index), String(writes), String(holdMs)]));
    await waitFor(() => fs.readdirSync(root).filter((name) => name.startsWith("ready-")).length === contenders, "writers ready");
    fs.writeFileSync(go, "");
    const results = await Promise.all(writers.map((writer) => writer.done));
    for (const result of results) expect(result.code, result.stderr).toBe(0);
    const reports = results.map((result) => JSON.parse(result.stdout.trim().split("\n").at(-1)!) as
      { maxWait: number; maxHold: number; loopP99Ms: number; checkpoints: { passive: number; truncate: number } });
    const maxHold = Math.max(...reports.map((report) => report.maxHold));
    const maxWait = Math.max(...reports.map((report) => report.maxWait));
    const loopP99Ms = Math.max(...reports.map((report) => report.loopP99Ms));
    const checkpoints = reports.reduce((sum, report) => ({ passive: sum.passive + report.checkpoints.passive,
      truncate: sum.truncate + report.checkpoints.truncate }), { passive: 0, truncate: 0 });
    console.log(JSON.stringify({ contenders, maxHold: Math.round(maxHold), maxWait: Math.round(maxWait),
      bound: Math.round(2 * contenders * maxHold), loopP99Ms: Math.round(loopP99Ms * 10) / 10, checkpoints }));
    // The wait measurement is observational: native contention no longer promises FIFO ordering.
    // Keep the existing responsiveness bound: native busy_timeout remains at most 5 ms.
    expect(loopP99Ms).toBeLessThanOrEqual(maxHold + 10);
    const check = await SqliteStateStore.open(root, 65536, 1000);
    try {
      const receipts = check.listAll("receipt/");
      expect(receipts).toHaveLength(contenders * writes);
      expect(new Set(receipts.map((entry) => entry.key)).size).toBe(contenders * writes);
      expect(check.listAll("k/")).toHaveLength(contenders);
      const observer = openNodeSqlite(path.join(root, "state.db"));
      try { expect(observer.prepare("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" }); }
      finally { observer.close(); }
    } finally { check.close(); }
  }, 180_000);

  it("a killed native waiter does not prevent the 15 surviving writers from committing", async () => {
    const root = await fresh();
    // A holder takes the native SQLite writer lock and keeps it for 2 s.
    const holder = run(`
      const [root] = process.argv.slice(1);
      const store = await SqliteStateStore.open(root, 65536, 1000);
      await store.writeBatch({ identity, ops: [], prepare: () => {
        fs.writeFileSync(path.join(root, "held"), "");
        // Held until the test says so (pi-fabric#936 CI): all waiters must be on the native lock before release.
        const until = Date.now() + 150_000;
        while (!fs.existsSync(path.join(root, "release")) && Date.now() < until) spin(5);
        fs.writeFileSync(path.join(root, "released"), String(Date.now()));
        return [{ kind: "put", key: "held", value: 1 }];
      } });
      store.close();`, [root]);
    await waitFor(() => fs.existsSync(path.join(root, "held")), "the holder");
    const single = `
      const [root, index] = process.argv.slice(1);
      const store = await SqliteStateStore.open(root, 65536, 1000, { lockTimeoutMs: 60_000 });
      fs.writeFileSync(path.join(root, "ready-" + index), "");
      let at = 0;
      await store.writeBatch({ identity, ops: [], prepare: () => { at = Date.now(); return [{ kind: "put", key: "w/" + index, value: 1 }]; } });
      console.log(JSON.stringify({ at, writesWaiting: store.stats().writesWaiting }));
      store.close();`;
    // All 16 waiters contend on the native SQLite lock; one is killed while the holder still owns it.
    const victim = run(single, [root, "dead"]);
    const waiters = Array.from({ length: 15 }, (_, index) => run(single, [root, String(index)]));
    await waitFor(() => fs.readdirSync(root).filter((name) => name.startsWith("ready-")).length === 16, "16 native waiters", 120_000);
    await new Promise(resolve => setTimeout(resolve, 100));
    victim.child.kill("SIGKILL");
    expect((await victim.done).signal).toBe("SIGKILL");
    fs.writeFileSync(path.join(root, "release"), ""); // the native holder lets the 15 survivors proceed
    const held = await holder.done;
    expect(held.code, held.stderr).toBe(0);
    const released = Number(fs.readFileSync(path.join(root, "released"), "utf8"));
    const results = await Promise.all(waiters.map((waiter) => waiter.done));
    for (const result of results) expect(result.code, result.stderr).toBe(0);
    const survivors = results.map((result) => JSON.parse(result.stdout.trim().split("\n").at(-1)!) as { at: number; writesWaiting: number });
    const firstWrite = Math.min(...survivors.map((survivor) => survivor.at));
    console.log(JSON.stringify({ nativeReleaseMs: firstWrite - released }));
    // Every remaining writer committed and unwound its waiter count after the killed native waiter.
    expect(survivors.map((survivor) => survivor.writesWaiting)).toEqual(Array(15).fill(0));
    const check = await SqliteStateStore.open(root, 65536, 1000);
    try {
      expect(check.listAll("w/").map((entry) => entry.key).sort()).toEqual(Array.from({ length: 15 }, (_, index) => `w/${index}`).sort());
      expect(check.listAll("w/")).toHaveLength(15);
      const observer = openNodeSqlite(path.join(root, "state.db"));
      try { expect(observer.prepare("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" }); }
      finally { observer.close(); }
    } finally { check.close(); }
  }, 300_000);
});
