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
const store = await SqliteStateStore.open(root, 65536, 1000, { lockTimeoutMs: 60_000, fifoAfterMs: 0 }); // strict FIFO
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
    // Strict FIFO (fifoAfterMs 0): every write consults the queue.
    const store = await SqliteStateStore.open(root, 65536, 1000, { initialize: "create", fifoAfterMs: 0 });
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

describe.skipIf(process.platform === "win32")("SQLite mesh writers next to older releases (smarty-dev#10516)", () => {
  const identity = { id: "tester", name: "tester", kind: "agent" as const };
  // A private parent holds the queue namespace. The receipt is a live head (this process's pid) that heartbeats only
  // when the test refreshes its mtime, as a head whose event loop is blocked never does.
  const queuedRoot = async (): Promise<{ root: string; receipt: string }> => {
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sqlite-mixed-"));
    roots.push(parent);
    const root = path.join(parent, "mesh");
    fs.mkdirSync(root, { mode: 0o700 });
    (await SqliteStateStore.open(root, 65536, 1000, { initialize: "create" })).close();
    const queue = meshLockQueueDirectory(path.join(root, "state.db"));
    fs.mkdirSync(queue, { recursive: true, mode: 0o700 });
    const receipt = path.join(queue, `${"1".padStart(24, "0")}-${process.pid}-00000000-0000-0000-0000-000000000000`);
    fs.writeFileSync(receipt, "");
    return { root, receipt };
  };

  it("by default a writer contends before it queues (8d0af4b0's contest); fifoAfterMs 0 queues first", async () => {
    const { root } = await queuedRoot();
    const contest = await SqliteStateStore.open(root, 65536, 1000);
    const strict = await SqliteStateStore.open(root, 65536, 1000, { lockTimeoutMs: 500, fifoAfterMs: 0 });
    try {
      // The lock is free: the default takes it at its first attempt, with no ticket, whoever is queued.
      await contest.put({ key: "m/contest", value: 1, identity });
      expect(contest.stats().fifoJoins).toBe(0);
      // Strict FIFO queues behind the receipt and waits for it (here: until it is judged stalled).
      const began = Date.now();
      await strict.put({ key: "m/strict", value: 1, identity });
      expect(Date.now() - began).toBeGreaterThanOrEqual(150);
      expect(strict.stats().fifoJoins).toBe(1);
    } finally { contest.close(); strict.close(); }
  });

  it("a waiter passes a stalled head after the stall bound, not at 80% of its budget, and never a head that heartbeats", async () => {
    const { root, receipt } = await queuedRoot();
    // A 10 s budget: the stall bound is 1 s and the 80% fallback 8 s.
    const store = await SqliteStateStore.open(root, 65536, 1000, { lockTimeoutMs: 10_000, fifoAfterMs: 0 });
    const heartbeat = setInterval(() => { const now = Date.now() / 1000; fs.utimesSync(receipt, now, now); }, 50);
    try {
      const began = Date.now();
      setTimeout(() => clearInterval(heartbeat), 2_500); // the head's event loop blocks from here on
      await store.put({ key: "m/passed", value: 1, identity });
      const waited = Date.now() - began;
      // Held back while the head heartbeat (2.5 s), passed one stall bound (1 s) later; before: at 8 s, then the
      // last 2 s of the budget in the plain contest.
      expect(waited).toBeGreaterThanOrEqual(3_300);
      expect(waited).toBeLessThan(6_500);
      expect(store.stats()).toMatchObject({ fifoJoins: 1, lockTimeouts: 0 });
    } finally { clearInterval(heartbeat); store.close(); }
  }, 30_000);
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
        // Held until the test says so (pi-fabric#936 CI): a fixed hold let a slow-starting queue drain before it formed.
        const until = Date.now() + 150_000;
        while (!fs.existsSync(path.join(root, "release")) && Date.now() < until) spin(5);
        fs.writeFileSync(path.join(root, "released"), String(Date.now()));
        return [{ kind: "put", key: "held", value: 1 }];
      } });
      store.close();`, [root, go]);
    await waitFor(() => fs.existsSync(path.join(root, "held")), "the holder");
    const single = `
      const [root, index] = process.argv.slice(1);
      const store = await SqliteStateStore.open(root, 65536, 1000, { lockTimeoutMs: 60_000, fifoAfterMs: 0 });
      fs.writeFileSync(path.join(root, "ready-" + index), "");
      let at = 0;
      await store.writeBatch({ identity, ops: [], prepare: () => { at = Date.now(); return [{ kind: "put", key: "w/" + index, value: 1 }]; } });
      console.log(JSON.stringify({ at, writesWaiting: store.stats().writesWaiting }));
      store.close();`;
    // The first waiter finds the queue empty, fails once and becomes the FIFO head.
    const head = run(single, [root, "head"]);
    // Setup budgets (children start and import under load) are separate from the asserted takeover bound.
    await waitFor(() => tickets(root).length === 1, "the head's ticket", 120_000);
    const waiters = Array.from({ length: 15 }, (_, index) => run(single, [root, String(index)]));
    await waitFor(() => tickets(root).length === 16, "15 queued behind the head", 120_000);
    head.child.kill("SIGKILL");
    expect((await head.done).signal).toBe("SIGKILL");
    fs.writeFileSync(path.join(root, "release"), ""); // the holder lets go only now: 15 queued, head dead
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
  }, 300_000);
});
