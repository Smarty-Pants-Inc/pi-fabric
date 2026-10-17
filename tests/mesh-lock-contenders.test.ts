import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { expect, it } from "vitest";
import { importMeshState } from "../src/mesh/backend-migration.js";
import { MeshStore } from "../src/mesh/store.js";
// Data-safety probe for L01/L02: real CAS claims plus the FILE control/event carrier, not FIFO order.
const CHILD = `
const { MeshStore, isMeshLockTimeout } = await import(process.argv[1]);
const [root, stateBackend, writer] = process.argv.slice(2);
const store = new MeshStore(root, 65536, 1000, { stateBackend, lockTimeoutMs: 9000 });
const from = { id: writer, name: writer, kind: "agent" };
const report = { pid: process.pid, claimed: [], published: [], conflicts: [], waits: [], timeout: null };
let phase, key;
async function timed(operation) {
  const started = performance.now();
  try { return await operation(); } finally { report.waits.push(performance.now() - started); }
}
store.get("warm", { fresh: true });
const go = new Promise(resolve => process.stdin.once("data", resolve));
process.stdout.write("ready\\n");
await go;
process.stdin.pause();
try {
  for (let round = 0; round < 4; round++) for (const suffix of [writer + "/" + round, "race/" + round]) {
    key = "claim/" + suffix;
    const value = { key, writer, payload: "BEGIN:λ:" + key + ":" + "x".repeat(256) + ":END" };
    phase = "claim";
    try { await timed(() => store.put({ key, value, identity: from, ifVersion: 0 })); }
    catch (error) {
      if (suffix.startsWith("race/") && error.message.startsWith("Mesh compare-and-swap failed for " + key + ": expected version 0, found ") && /found [1-9][0-9]*$/.test(error.message)) {
        report.conflicts.push(key); continue;
      }
      throw error;
    }
    report.claimed.push(value);
    phase = "publish";
    const event = await timed(() => store.publish({ topic: "fabric.control", kind: "claim", from, data: value }));
    report.published.push({ key, id: event.id });
  }
} catch (error) {
  if (!isMeshLockTimeout(error) || error.foreignCode) throw error;
  report.timeout = { phase, key, code: error.code, name: error.name };
} finally { store.closeState(); }
process.stdout.write(JSON.stringify(report) + "\\n");
`;
type Claim = { key: string; writer: string; payload: string };
type Report = { pid: number; claimed: Claim[]; published: { key: string; id: string }[];
  conflicts: string[]; waits: number[]; timeout: null | { phase: string; key: string; code: string; name: string } };

it.each(["file", "sqlite"] as const)("16 real %s-state contenders preserve CAS claims and FILE publications", async stateBackend => {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-contenders-"));
  const root = path.join(scratch, "mesh");
  const module = path.join(scratch, "child.mjs");
  const children: ReturnType<typeof spawn>[] = [], exits: Promise<void>[] = [];
  let store: MeshStore | undefined;
  try {
    fs.mkdirSync(root);
    if (stateBackend === "sqlite") await importMeshState(root);
    store = new MeshStore(root, 65536, 1000, { stateBackend });
    await store.put({ key: "warm", value: true, identity: { id: "setup", name: "setup", kind: "agent" } });
    store.closeState(); store = undefined;
    buildSync({ stdin: { contents: 'export { MeshStore } from "./src/mesh/store.ts"; export { isMeshLockTimeout } from "./src/core/atomic-write.ts";',
      resolveDir: process.cwd() }, bundle: true, platform: "node", format: "esm", outfile: module });
    const ready: Promise<void>[] = [];
    const reports = Array.from({ length: 16 }, (_, index) => {
      const child = spawn(process.execPath, ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", CHILD,
        pathToFileURL(module).href, root, stateBackend, "writer-" + index], { stdio: ["pipe", "pipe", "pipe"], timeout: 45000,
        env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: stateBackend, PI_FABRIC_LOCK_STATS: "1", PI_CODING_AGENT_DIR: scratch } });
      children.push(child);
      let stdout = "", stderr = "";
      ready.push(new Promise<void>((resolve, reject) => {
        child.stdout!.on("data", chunk => { stdout += chunk; if (stdout.startsWith("ready\n")) resolve(); });
        child.once("error", reject); child.once("close", () => reject(new Error("exited before barrier: " + stderr)));
      }));
      child.stderr!.on("data", chunk => { stderr += chunk; });
      exits.push(new Promise(resolve => child.once("close", () => resolve())));
      return new Promise<Report>((resolve, reject) => {
        child.once("error", reject);
        child.once("close", code => code === 0 && !stderr ? resolve(JSON.parse(stdout.trim().split("\n").at(-1)!))
          : reject(new Error("child " + index + " exit " + code + ": " + stderr)));
      });
    });
    const completed = Promise.all(reports); void completed.catch(() => undefined);
    await Promise.all(ready); // All sixteen processes are live before any begins its workload.
    children.forEach(child => child.stdin!.end("go\n"));
    const results = await completed;
    expect(new Set(results.map(result => result.pid)).size).toBe(16);
    store = new MeshStore(root, 65536, 1000, { stateBackend });
    const claims = results.flatMap(result => result.claimed), published = results.flatMap(result => result.published);
    expect(claims.length).toBeGreaterThan(0);
    expect(new Set(claims.map(claim => claim.key)).size).toBe(claims.length);
    const entries = store.listAll("claim/", { fresh: true });
    expect(entries.map(entry => entry.value)).toEqual(claims.sort((a, b) => a.key.localeCompare(b.key)));
    for (const entry of entries) expect(entry.version).toBeGreaterThan(0);
    const events = store.read({ topic: "fabric.control", limit: 1000 });
    expect(events.map(event => event.id).sort()).toEqual(published.map(event => event.id).sort());
    expect(new Set(events.map(event => event.id)).size).toBe(events.length);
    for (const event of events) expect(event.data).toEqual(claims.find(claim => claim.key === (event.data as Claim).key));
    for (const result of results) {
      if (!result.timeout) expect(result.claimed.length + result.conflicts.length).toBe(8);
      for (const claim of result.claimed) if (!result.published.some(event => event.key === claim.key))
        expect(result.timeout).toMatchObject({ phase: "publish", key: claim.key });
    }
    const stats = fs.readdirSync(path.join(root, "lock-stats")).filter(name => name.endsWith(".json"))
      .map(name => JSON.parse(fs.readFileSync(path.join(root, "lock-stats", name), "utf8"))).filter(stat => results.some(result => result.pid === stat.pid));
    expect(stats).toHaveLength(16);
    const maxWaitMs = Math.max(...stats.flatMap(stat => stat.minutes.flatMap((minute: { classes: Record<string, { waitMaxMs: number }> }) => Object.values(minute.classes).map(bucket => bucket.waitMaxMs))));
    const maxOperationMs = Math.max(...results.flatMap(result => result.waits));
    console.log(JSON.stringify({ stateBackend, processes: 16, claims: claims.length, publications: events.length, timeouts: results.filter(result => result.timeout).length, maxWaitMs, maxOperationMs, budgetMs: 10000 }));
    expect(maxWaitMs, "FIFO KEEP trigger: acquisition exceeded 10s").toBeLessThanOrEqual(10000);
    expect(maxOperationMs, "FIFO KEEP trigger: operation exceeded 10s").toBeLessThanOrEqual(10000);
  } finally {
    children.forEach(child => { if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); });
    await Promise.all(exits); store?.closeState(); fs.rmSync(scratch, { recursive: true, force: true });
  }
}, 60000);

const FILE_FIFO_REAL_1S_CHILD = `
const fs = await import("node:fs");
const { MeshStore, isMeshLockTimeout } = await import(process.argv[1]);
const [root, writer] = process.argv.slice(2);
const store = new MeshStore(root, 65536, 1000, { stateBackend: "file", lockTimeoutMs: 1000 });
const sleepCell = new Int32Array(new SharedArrayBuffer(4));
const report = { pid: process.pid, writer, completed: 0, typedTimeouts: 0, intervals: [] };
const go = new Promise(resolve => process.stdin.once("data", resolve));
process.stdout.write("ready " + process.pid + "\\n");
await go;
process.stdin.pause();
try {
  for (let i = 0; i < 100; i++) {
    const id = writer + "/" + i;
    await store.exclusive(() => {
      const start = process.hrtime.bigint();
      Atomics.wait(sleepCell, 0, 0, i % 5 + 1);
      fs.appendFileSync(root + "/committed", id + "\\n");
      report.intervals.push({ id, start: start.toString(), end: process.hrtime.bigint().toString() });
    }, 1000);
    report.completed++;
  }
} catch (error) {
  if (isMeshLockTimeout(error)) report.typedTimeouts++;
  throw error; // The first failed acquisition is fatal: no retry, skipped ID, or accepted timeout.
} finally {
  store.closeState();
  process.stdout.write(JSON.stringify(report) + "\\n");
}
`;

it("FILE FIFO real 1s regression: 20 processes each commit 100 exclusive acquisitions without timeouts", async () => {
  type FifoReport = { pid: number; writer: string; completed: number; typedTimeouts: number;
    intervals: { id: string; start: string; end: string }[] };
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-file-fifo-real-1s-"));
  const root = path.join(scratch, "mesh"), module = path.join(scratch, "child.mjs");
  const children: ReturnType<typeof spawn>[] = [], exits: Promise<void>[] = [], ready: Promise<number>[] = [];
  try {
    fs.mkdirSync(root);
    buildSync({ stdin: { contents: 'export { MeshStore } from "./src/mesh/store.ts"; export { isMeshLockTimeout } from "./src/core/atomic-write.ts";',
      resolveDir: process.cwd() }, bundle: true, platform: "node", format: "esm", outfile: module });
    const reports = Array.from({ length: 20 }, (_, index) => {
      const child = spawn(process.execPath, ["--disable-warning=ExperimentalWarning", "--input-type=module", "-e", FILE_FIFO_REAL_1S_CHILD,
        pathToFileURL(module).href, root, "writer-" + index], { stdio: ["pipe", "pipe", "pipe"], timeout: 45000, killSignal: "SIGKILL",
        env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "file", PI_CODING_AGENT_DIR: scratch } });
      children.push(child);
      let stdout = "", stderr = "";
      ready.push(new Promise<number>((resolve, reject) => {
        child.stdout!.on("data", chunk => { stdout += chunk; const match = /^ready (\d+)\n/.exec(stdout); if (match) resolve(Number(match[1])); });
        child.once("error", reject); child.once("close", () => reject(new Error("exited before barrier: " + stderr)));
      }));
      child.stderr!.on("data", chunk => { stderr += chunk; });
      exits.push(new Promise(resolve => child.once("close", () => resolve())));
      return new Promise<FifoReport>((resolve, reject) => {
        child.once("error", reject);
        child.once("close", code => {
          try {
            if (code !== 0 || stderr) throw new Error("child " + index + " exit " + code + ": " + stderr + stdout);
            resolve(JSON.parse(stdout.trim().split("\n").at(-1)!));
          } catch (error) { reject(error); }
        });
      });
    });
    const completed = Promise.all(reports); void completed.catch(() => undefined);
    const pids = await Promise.all(ready);
    expect(new Set(pids).size).toBe(20);
    expect(pids).toEqual(children.map(child => child.pid));
    children.forEach(child => child.stdin!.end("go\n"));
    const results = await completed;
    expect(results.map(result => result.pid)).toEqual(pids);
    expect(results.map(result => result.writer)).toEqual(Array.from({ length: 20 }, (_, i) => "writer-" + i));
    for (const result of results) {
      expect(result.typedTimeouts).toBe(0);
      expect(result.completed).toBe(100);
      expect(result.intervals).toHaveLength(100);
      expect(result.intervals.map(interval => interval.id)).toEqual(Array.from({ length: 100 }, (_, i) => result.writer + "/" + i));
    }
    const expected = results.flatMap(result => Array.from({ length: 100 }, (_, i) => result.writer + "/" + i)).sort();
    const committed = fs.readFileSync(path.join(root, "committed"), "utf8").trim().split("\n");
    expect(committed).toHaveLength(2000);
    expect(new Set(committed).size).toBe(2000);
    expect([...committed].sort()).toEqual(expected);
    const intervals = results.flatMap(result => result.intervals)
      .map(interval => ({ ...interval, start: BigInt(interval.start), end: BigInt(interval.end) }))
      .sort((a, b) => a.start < b.start ? -1 : a.start > b.start ? 1 : 0);
    expect(intervals).toHaveLength(2000);
    expect(intervals.map(interval => interval.id).sort()).toEqual(expected);
    for (let i = 0; i < intervals.length; i++) {
      const interval = intervals[i]!;
      const previous = intervals[i - 1];
      expect(interval.end).toBeGreaterThan(interval.start);
      if (previous) expect(interval.start, "overlapping critical sections: " + previous.id + " / " + interval.id)
        .toBeGreaterThanOrEqual(previous.end);
    }
  } finally {
    children.forEach(child => { if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); });
    await Promise.all(exits); fs.rmSync(scratch, { recursive: true, force: true });
  }
}, 60000);
