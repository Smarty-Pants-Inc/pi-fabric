import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";

// smarty-dev#6100: the bridge's FABRIC_MESH_STATE_BUSY timeouts were SQLite's WRITER lock held for 10 s by a
// checkpoint that copied and fsynced under it. Real path: real child processes write one SQLite mesh root while
// another child maintains it (checkpoint: "maintainer"), and every child's fsync is slow.
//
// Slow-fsync injection: SQLite's fsync is C code inside node:sqlite, so wrapping fs.fsyncSync never reaches it,
// and LD_PRELOAD shims are refused by the fleet's shell guard. Each child therefore runs under strace's syscall
// fault injection: `strace -f --seccomp-bpf -e trace=fsync,fdatasync -e inject=fsync,fdatasync:delay_enter=<us>`
// delays the kernel entry of every fsync/fdatasync the child makes (SQLite's included) by SLOW_FSYNC_MS, and the
// seccomp filter stops the child on those two syscalls only. A checkpoint copies with one WAL fsync and one
// database fsync, so a checkpoint that backfills under the writer lock holds it for ~2 x SLOW_FSYNC_MS = 8 s, and
// the first commit after the WAL reset it causes holds it for another sync (below): a writer that waits through
// both passes the 10 s lock budget. Before smarty-dev#6100 the client WAL reset (a TRUNCATE without a PASSIVE
// first) does exactly that and the other writers time out; after it, the copy and the fsyncs happen in PASSIVE,
// outside the writer lock, and the TRUNCATE is a guarded single attempt that holds it for neither. Skipped where strace cannot inject (not Linux, no
// strace, or ptrace refused); the gate is a real `strace` probe, never an assumption.
//
// Not removable here: with synchronous=NORMAL SQLite fsyncs the WAL inside the FIRST COMMIT after every WAL reset
// (restart in place or TRUNCATE; the WAL-header sync of SQLite ticket ff5be73dee, plus a one-time directory sync per
// connection), under the writer lock. That hold is one or two syncs (<= 2 x SLOW_FSYNC_MS = 8 s, inside the budget),
// so this test asserts no timeout and reports the longest wait; the PR body measures that residual separately.

const SLOW_FSYNC_MS = 4_000;
const WRITERS = 4;
const RUN_MS = 14_000;
const MAINTAINER_DELAY_MS = 1_500; // the client reset reaches the WAL first, as on a hub where commits drive it
// Optional constant short readers (SLOWFSYNC_READERS=n): they hold WAL read marks, so a TRUNCATE rarely completes
// and the before-fix stall is its backfill alone. The default (none) lets the reset complete, as on a quiet hub.
const READERS = Number(process.env.SLOWFSYNC_READERS ?? 0);
const KiB = 1024;
const MiB = 1024 * KiB;
const WAL_RESET_BYTES = 256 * KiB;

const straceArgs = (slowMs: number): string[] => ["-f", "-qq", "-o", "/dev/null", "--seccomp-bpf", "-e", "trace=fsync,fdatasync",
  "-e", `inject=fsync,fdatasync:delay_enter=${slowMs * 1000}`];
const straceWorks = (): boolean => {
  if (process.platform !== "linux") return false;
  // The probe proves the injection itself: a traced fsync of a temp file takes at least the injected 50 ms.
  const script = "const fs = require('node:fs'); const file = require('node:path').join(require('node:os').tmpdir(), 'pf-strace-' + process.pid);"
    + " const fd = fs.openSync(file, 'w'); const started = Date.now(); fs.fsyncSync(fd); const took = Date.now() - started;"
    + " fs.closeSync(fd); fs.rmSync(file, { force: true }); process.exit(took >= 40 ? 0 : 3);";
  try {
    const probe = spawnSync("strace", ["-f", "-qq", "-o", "/dev/null", "--seccomp-bpf", "-e", "trace=fsync,fdatasync",
      "-e", "inject=fsync,fdatasync:delay_enter=50000", process.execPath, "-e", script], { encoding: "utf8", timeout: 30_000 });
    return probe.status === 0;
  } catch { return false; }
};
const STRACE = straceWorks();

const roots: string[] = [];
const children: ChildProcess[] = [];
afterEach(async () => {
  await Promise.all(children.splice(0).map((child) => {
    if (child.exitCode !== null || child.signalCode !== null) return undefined;
    const exited = new Promise((resolve) => child.once("exit", resolve));
    try { child.kill("SIGKILL"); } catch { /* gone */ }
    return exited;
  }));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const PRELUDE = `import { createJiti } from "jiti"; import { pathToFileURL } from "node:url"; import fs from "node:fs";
  const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
  const { SqliteStateStore } = await jiti.import("./src/mesh/state-sqlite.ts");
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const identity = { id: "slow-fsync", name: "slow-fsync", kind: "agent" };`;
// A writer: sequential puts with a short think time (a bridge-like client), counting lock timeouts.
const WRITER = `${PRELUDE}
  const [root, go, index, walResetBytes] = process.argv.slice(1);
  const store = await SqliteStateStore.open(root, 65536, 1000, { walResetBytes: Number(walResetBytes) });
  fs.writeFileSync(root + "/ready-w" + index, "");
  while (!fs.existsSync(go)) await sleep(5);
  const until = Number(fs.readFileSync(go, "utf8"));
  const pad = "p".repeat(2048);
  let ok = 0, timeouts = 0, walCap = 0, maxPutMs = 0; const messages = [];
  while (Date.now() < until) {
    const started = performance.now();
    try { await store.put({ key: "w" + index + "/" + (ok % 16), value: { n: ok, pad }, identity }); ok += 1; }
    catch (error) {
      if (error.code === "FABRIC_MESH_LOCK_TIMEOUT") { timeouts += 1; if (messages.length < 3) messages.push(error.message); }
      else if (error.code === "FABRIC_MESH_STATE_WAL_CAP") walCap += 1;
      else throw error;
    }
    maxPutMs = Math.max(maxPutMs, performance.now() - started);
    await sleep(50);
  }
  process.stdout.write(JSON.stringify({ role: "writer", index: Number(index), ok, timeouts, walCap, maxPutMs: Math.round(maxPutMs),
    maxWaitMs: Math.round(store.stats().maxWaitMs), checkpoints: store.stats().checkpoints, messages }) + "\\n");
  process.exit(0); // no close(): the last close would checkpoint through the slow fsync`;
// A reader: back-to-back single-statement reads (overlapping WAL snapshots), as the hub's readers do.
const READER = `${PRELUDE}
  const [root, go, index] = process.argv.slice(1);
  const store = await SqliteStateStore.open(root, 65536, 1000);
  fs.writeFileSync(root + "/ready-r" + index, "");
  while (!fs.existsSync(go)) await sleep(5);
  const until = Number(fs.readFileSync(go, "utf8"));
  let reads = 0;
  while (Date.now() < until) { store.listAll("w"); reads += 1; if (reads % 200 === 0) await sleep(0); }
  process.stdout.write(JSON.stringify({ role: "reader", index: Number(index), reads, checkpoints: store.stats().checkpoints }) + "\\n");
  process.exit(0);`;
// The maintainer: the projector's role (PASSIVE, then TRUNCATE when the WAL is large and growing).
const MAINTAINER = `${PRELUDE}
  const [root, go, delayMs] = process.argv.slice(1);
  fs.writeFileSync(root + "/ready-m", "");
  while (!fs.existsSync(go)) await sleep(5);
  const until = Number(fs.readFileSync(go, "utf8"));
  await sleep(Number(delayMs));
  const store = await SqliteStateStore.open(root, 65536, 1000, { checkpoint: "maintainer", checkpointBytes: 64 * 1024, checkpointIntervalMs: 250 });
  while (Date.now() < until) await sleep(50);
  process.stdout.write(JSON.stringify({ role: "maintainer", checkpoints: store.stats().checkpoints }) + "\\n");
  process.exit(0);`;

const run = (slowMs: number, root: string, script: string, args: string[]): Promise<{ code: number | null; stdout: string; stderr: string }> => {
  const child = spawn("strace", [...straceArgs(slowMs), process.execPath, "--disable-warning=ExperimentalWarning", "--input-type=module", "-e", script, ...args],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
  children.push(child);
  let stdout = "";
  let stderr = "";
  child.stdout!.on("data", (chunk) => { stdout += String(chunk); });
  child.stderr!.on("data", (chunk) => { stderr += String(chunk); });
  return new Promise((resolve) => child.once("exit", (code) => resolve({ code, stdout, stderr })));
};
const waitFor = async (check: () => boolean, ms: number, what: string): Promise<void> => {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

type Report = { role: string; ok?: number; timeouts?: number; walCap?: number; maxPutMs?: number; maxWaitMs?: number; messages?: string[];
  checkpoints: Record<string, number> };
const scenario = async (slowMs: number, writerCount: number, readerCount: number): Promise<{ ok: number; timeouts: number; walCap: number;
  maxWaitMs: number; maxWal: number; reports: Report[] }> => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-slow-fsync-"));
  roots.push(root);
  // Initialise an imported root (state.db plus the moved marker) untraced; children open it like production.
  (await SqliteStateStore.open(root, 65536, 1000, { initialize: "create" })).close();
  const go = path.join(root, "go");
  const writers = Array.from({ length: writerCount }, (_, index) => run(slowMs, root, WRITER, [root, go, String(index), String(WAL_RESET_BYTES)]));
  const readers = Array.from({ length: readerCount }, (_, index) => run(slowMs, root, READER, [root, go, String(index)]));
  const maintainer = run(slowMs, root, MAINTAINER, [root, go, String(MAINTAINER_DELAY_MS)]);
  await waitFor(() => fs.readdirSync(root).filter((name) => name.startsWith("ready-")).length === writerCount + readerCount + 1, 90_000, "children ready");
  let maxWal = 0;
  const sample = setInterval(() => {
    maxWal = Math.max(maxWal, fs.statSync(path.join(root, "state.db-wal"), { throwIfNoEntry: false })?.size ?? 0);
  }, 25);
  const started = Date.now();
  fs.writeFileSync(go, String(started + RUN_MS));
  const results = await Promise.all([...writers, ...readers, maintainer]);
  clearInterval(sample);
  for (const result of results) expect(result.code, result.stderr.slice(-2_000)).toBe(0);
  const reports = results.map((result) => JSON.parse(result.stdout.trim().split("\n").at(-1)!) as Report);
  const writerReports = reports.filter((report) => report.role === "writer");
  const summary = {
    ok: writerReports.reduce((sum, report) => sum + report.ok!, 0),
    timeouts: writerReports.reduce((sum, report) => sum + report.timeouts!, 0),
    walCap: writerReports.reduce((sum, report) => sum + report.walCap!, 0),
    maxWaitMs: Math.max(...writerReports.map((report) => report.maxWaitMs!)),
    maxWal, reports,
  };
  // Evidence for the PR (smarty-dev#6100): STATE_BUSY timeouts, the longest writer-lock wait and the WAL bound.
  console.info(`SLOWFSYNC ${JSON.stringify({ slowFsyncMs: slowMs, writers: writerCount, readers: readerCount, runMs: RUN_MS,
    elapsedMs: Date.now() - started, ...summary, maxWalMiB: Math.round((maxWal / MiB) * 10) / 10, maxWal: undefined })}`);
  return summary;
};

describe.skipIf(!STRACE)("SQLite mesh under a slow fsync (smarty-dev#6100, real processes, strace fault injection)", () => {
  // N writers: their waits also include SQLite's restart-commit sync (see the file comment), at most two syncs.
  it("[heavy] four writer processes and a maintainer: 0 STATE_BUSY timeouts at the 10 s budget, WAL bounded", async () => {
    const result = await scenario(SLOW_FSYNC_MS, WRITERS, READERS);
    expect(result.ok).toBeGreaterThan(WRITERS * 20);
    expect(result.timeouts).toBe(0);
    expect(result.maxWaitMs).toBeLessThan(10_000);
    // Bounded: far below the WAL hard cap (#835), so no writer was ever deferred by it either.
    expect(result.walCap).toBe(0);
    expect(result.maxWal).toBeLessThan(32 * MiB);
  }, 180_000);
});
