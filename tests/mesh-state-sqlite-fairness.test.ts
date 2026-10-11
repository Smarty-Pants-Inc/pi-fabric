import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";

// smarty-dev#9805: SQLite mesh writers starved for 30-67 s while no hold exceeded 1.6 s and the lock was busy only
// 2-22% of the time. Fairness bound: with N contenders and short holds, no write waits longer than 2 x (N x max hold).
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
    return [{ kind: "put", key: "k/" + index, value: i }];
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
const tickets = (root: string) => {
  try { return fs.readdirSync(meshLockQueueDirectory(path.join(root, "state.db"))).filter((name) => /^\d{24}-\d+-/.test(name)); }
  catch { return []; }
};

describe.skipIf(process.platform === "win32" || process.getuid?.() === 0)("SQLite mesh writer queue fails open (pi-fabric#936 round 3)", () => {
  it("an inaccessible queue directory (mode 000) never fails a write, and no waiter is left counted", async () => {
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sqlite-queue-denied-"));
    roots.push(parent);
    const root = path.join(parent, "mesh"); // its queue namespace lives in this private parent, not the shared tmpdir
    fs.mkdirSync(root, { mode: 0o700 });
    const store = await SqliteStateStore.open(root, 65536, 1000, { initialize: "create" });
    const queue = meshLockQueueDirectory(path.join(root, "state.db"));
    const namespace = path.dirname(queue);
    fs.mkdirSync(queue, { recursive: true, mode: 0o700 });
    fs.chmodSync(namespace, 0o000); // as if another user planted it: every stat or listing under it is EACCES
    try {
      const identity = { id: "tester", name: "tester", kind: "agent" as const };
      await Promise.all(Array.from({ length: 8 }, (_, index) => store.put({ key: `d/${index}`, value: index, identity })));
      expect(store.listAll("d/")).toHaveLength(8);
      expect(store.stats().writesWaiting).toBe(0);
    } finally {
      fs.chmodSync(namespace, 0o700);
      store.close();
    }
  });
});

describe.skipIf(process.platform === "win32")("SQLite mesh writer fairness (smarty-dev#9805)", () => {
  it("16 contenders with short holds: no write waits longer than 2 x (16 x max hold), and the event loop stays responsive", async () => {
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
    expect(maxWait).toBeLessThanOrEqual(2 * contenders * maxHold);
    // The writer's own synchronous hold blocks its loop for maxHold; the head's in-SQLite wait adds at most
    // HEAD_BUSY_MS (5 ms) to that, so p99 stays within the hold plus 10 ms.
    expect(loopP99Ms).toBeLessThanOrEqual(maxHold + 10);
    expect(tickets(root)).toEqual([]); // every waiter left the queue
  }, 180_000);

  it("a SIGKILLed FIFO head is taken over within the stall bound while 15 wait", async () => {
    const root = await fresh();
    const go = path.join(root, "go");
    // A holder takes the writer lock with the queue empty and keeps it for 2 s.
    const holder = run(`
      const [root, go] = process.argv.slice(1);
      const store = await SqliteStateStore.open(root, 65536, 1000);
      await store.writeBatch({ identity, ops: [], prepare: () => {
        fs.writeFileSync(path.join(root, "held"), "");
        spin(2000);
        fs.writeFileSync(path.join(root, "released"), String(Date.now()));
        return [{ kind: "put", key: "held", value: 1 }];
      } });
      store.close();`, [root, go]);
    await waitFor(() => fs.existsSync(path.join(root, "held")), "the holder");
    const single = `
      const [root, index] = process.argv.slice(1);
      const store = await SqliteStateStore.open(root, 65536, 1000, { lockTimeoutMs: 60_000 });
      fs.writeFileSync(path.join(root, "ready-" + index), "");
      let at = 0;
      await store.writeBatch({ identity, ops: [], prepare: () => { at = Date.now(); return [{ kind: "put", key: "w/" + index, value: 1 }]; } });
      console.log(JSON.stringify({ at, writesWaiting: store.stats().writesWaiting }));
      store.close();`;
    // The first waiter finds the queue empty, fails once and becomes the FIFO head.
    const head = run(single, [root, "head"]);
    await waitFor(() => tickets(root).length === 1, "the head's ticket", 30_000);
    const waiters = Array.from({ length: 15 }, (_, index) => run(single, [root, String(index)]));
    await waitFor(() => tickets(root).length === 16, "15 queued behind the head", 60_000);
    head.child.kill("SIGKILL");
    expect((await head.done).signal).toBe("SIGKILL");
    const held = await holder.done;
    expect(held.code, held.stderr).toBe(0);
    const released = Number(fs.readFileSync(path.join(root, "released"), "utf8"));
    const results = await Promise.all(waiters.map((waiter) => waiter.done));
    for (const result of results) expect(result.code, result.stderr).toBe(0);
    const survivors = results.map((result) => JSON.parse(result.stdout.trim().split("\n").at(-1)!) as { at: number; writesWaiting: number });
    const firstWrite = Math.min(...survivors.map((survivor) => survivor.at));
    console.log(JSON.stringify({ takeoverMs: firstWrite - released }));
    // The dead head's ticket is reaped by its successor (pid gone); the stall bound is MeshLockTicket's 2 s cap.
    expect(firstWrite - released).toBeLessThanOrEqual(2_000);
    // Every remaining writer committed, and each left nothing behind: no waiter count, no ticket.
    expect(survivors.map((survivor) => survivor.writesWaiting)).toEqual(Array(15).fill(0));
    const check = await SqliteStateStore.open(root, 65536, 1000);
    try { expect(check.listAll("w/").map((entry) => entry.key).sort()).toEqual(Array.from({ length: 15 }, (_, index) => `w/${index}`).sort()); }
    finally { check.close(); }
    expect(tickets(root)).toEqual([]);
  }, 180_000);
});
