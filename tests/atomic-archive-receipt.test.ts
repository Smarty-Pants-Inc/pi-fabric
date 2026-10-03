import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { afterEach, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

it("#2479 R3 F26 process replacement finishes the native Claude reset before launch", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-claude-receipt-")); roots.push(root);
  const child = spawn("bun", [path.resolve("tests/fixtures/atomic-claude-reset-crash.ts"), root], { stdio: ["ignore", "pipe", "pipe"] });
  const exited = once(child, "exit"); let output = "", errors = "";
  child.stdout.on("data", chunk => { output += chunk.toString(); }); child.stderr.on("data", chunk => { errors += chunk.toString(); });
  let status: unknown;
  try { await until(() => child.exitCode !== null || child.signalCode !== null); status = await exited; }
  finally { if (child.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); await exited; } }
  expect(status, errors).toEqual([0, null]);
  const produced = JSON.parse(output.trim()) as { actorId: string; file: string; archived: string; failures: number };
  expect(produced.failures).toBeGreaterThan(0); expect(fs.existsSync(`${produced.file}.archive-pending.json`)).toBe(true);
  const registry = path.join(root, "actors", "actors.json");
  expect(JSON.parse(fs.readFileSync(registry, "utf8")).actors[0].runnerSessionId).toBe("11111111-1111-4111-8111-111111111111");
  const namespace = atomic.syncPathNamespace; let unavailable = true, failures = 0;
  vi.spyOn(atomic, "syncPathNamespace").mockImplementation((file, inode) => {
    if (file === produced.archived && unavailable) { failures++; throw new Error("native successor receipt unavailable"); }
    namespace(file, inode);
  });
  const previousLog = process.env.FAKE_CLAUDE_LOG, log = path.join(root, "successor-claude.jsonl"); process.env.FAKE_CLAUDE_LOG = log;
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { workerPath: path.resolve("src/worker.ts"), claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"), runRoot: path.join(root, "successor-runs") }); managers.push(agents);
  const run = agents.run.bind(agents);
  const launch = vi.spyOn(agents, "run").mockImplementation(async (request, signal) => {
    expect(JSON.parse(fs.readFileSync(registry, "utf8")).actors[0].runnerSessionId).toBeUndefined();
    expect(fs.existsSync(`${produced.file}.archive-pending.json`)).toBe(false);
    return run(request, signal);
  });
  const actors = new ActorManager("claude-crash", { id: "session:claude-crash", name: "main", kind: "main" },
    new MeshStore(path.join(root, "mesh"), 65536, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
    { actorRoot: path.join(root, "actors"), persistent: true, maxSessionBytes: 64, preparationRetryMs: 25 }); managers.push(actors);
  try {
    await until(() => failures > 0 || launch.mock.calls.length > 0); await delay(150);
    expect(launch).not.toHaveBeenCalled(); expect(fs.existsSync(`${produced.file}.archive-pending.json`)).toBe(true);
    unavailable = false;
    await until(() => launch.mock.calls.length > 0 && actors.inFlightCount() === 0);
    expect(launch).toHaveBeenCalledTimes(1); expect(launch.mock.calls[0]![0].runnerSessionId).toBeUndefined();
    expect(JSON.parse(fs.readFileSync(log, "utf8").trim()).argv).not.toContain("--resume");
  } finally { unavailable = false; if (previousLog === undefined) delete process.env.FAKE_CLAUDE_LOG; else process.env.FAKE_CLAUDE_LOG = previousLog; }
}, 30000);

const roots: string[] = [], managers: Array<{ close(): Promise<void> }> = [];
const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const until = async (predicate: () => boolean) => { for (let n = 0; !predicate(); n++) { if (n > 500) throw new Error("archive receipt probe timed out"); await delay(10); } };
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

it.each(["native", "win32"] as const)("#2479 R2 F6 process replacement cannot use visible archive bytes as a receipt (%s)", async platform => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-archive-receipt-")); roots.push(root);
  const child = spawn("bun", [path.resolve("tests/fixtures/atomic-archive-crash.ts"), root, platform], { stdio: ["ignore", "pipe", "pipe"] });
  const exited = once(child, "exit");
  let output = "", errors = "";
  child.stdout.on("data", chunk => { output += chunk.toString(); }); child.stderr.on("data", chunk => { errors += chunk.toString(); });
  let status: unknown;
  try { await until(() => child.exitCode !== null || child.signalCode !== null); status = await exited; }
  finally { if (child.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); await exited; } }
  expect(status, errors).toEqual([0, null]);
  const produced = JSON.parse(output.trim()) as { actorId: string; file: string; archived: string; contents: string; prior: string[]; failures: number };
  expect(produced.failures).toBeGreaterThan(0);
  expect(fs.existsSync(`${produced.file}.archive-pending.json`)).toBe(true);
  expect(fs.readFileSync(produced.archived, "utf8")).toBe(produced.contents);
  const namespace = atomic.syncPathNamespace, events: string[] = [];
  let unavailable = true, failures = 0;
  vi.spyOn(atomic, "syncPathNamespace").mockImplementation((file, inode) => {
    if (file === produced.archived) {
      if (unavailable) { failures++; throw new Error("replacement archive receipt unavailable"); }
      namespace(file, inode); events.push("confirmed"); return;
    }
    namespace(file, inode);
  });
  const rename = fs.renameSync.bind(fs), rm = fs.rmSync.bind(fs);
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { if (to === produced.file) events.push("replacement"); rename(from, to); });
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => { if (produced.prior.includes(String(file))) events.push("prune"); rm(file, options); });
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { workerPath: path.resolve("tests/fixtures/session-worker.mjs"), runRoot: path.join(root, "successor-runs") }); managers.push(agents);
  const launch = vi.spyOn(agents, "run").mockImplementation(async (...args) => { events.push("launch"); return AgentManager.prototype.run.apply(agents, args); });
  const actors = new ActorManager("archive-crash", { id: "session:archive-crash", name: "main", kind: "main" }, new MeshStore(path.join(root, "mesh"), 65536, 100),
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, { actorRoot: path.join(root, "actors"), persistent: true, preparationRetryMs: 25 }); managers.push(actors);
  let snapshot!: { exists: boolean; prior: boolean; launches: number; events: string[]; failures: number };
  try {
    await until(() => failures > 0 || launch.mock.calls.length > 0);
    await delay(150);
    snapshot = { exists: fs.existsSync(produced.file), prior: produced.prior.every(file => fs.existsSync(file)), launches: launch.mock.calls.length, events: [...events], failures };
  } finally { unavailable = false; }
  await until(() => launch.mock.calls.length > 0 && actors.inFlightCount() === 0);
  expect(snapshot.exists).toBe(false);
  expect(snapshot.prior).toBe(true);
  expect(snapshot.launches).toBe(0);
  expect(snapshot.events).toEqual([]);
  expect(snapshot.failures).toBeGreaterThan(0);
  expect(events[0]).toBe("confirmed");
  expect(events.indexOf("confirmed")).toBeLessThan(events.indexOf("replacement"));
  expect(events.indexOf("confirmed")).toBeLessThan(events.indexOf("launch"));
  expect(launch).toHaveBeenCalledTimes(1);
  expect(fs.readFileSync(produced.archived, "utf8")).toBe(produced.contents);
}, 30000);
