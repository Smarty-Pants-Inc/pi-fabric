import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { scriptSpawnArgs } from "../src/agents/transports/process-utils.js";
import { FABRIC_RUN_ROOT_PREFIX, sweepTempRunRoots, type TempRunSweepRequest } from "../src/storage/retention.js";

const launches = vi.hoisted(() => [] as Array<{ args: readonly string[]; status: number | null; stderr: string }>);
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: (runtime: string, args: readonly string[], options: Parameters<typeof actual.spawn>[2]) => {
    if (!options?.detached) return actual.spawn(runtime, args, options);
    // Exercise the actual detached entry, but join it in the test: no leaked background sweeps.
    const result = actual.spawnSync(runtime, args, { encoding: "utf8", timeout: 20_000 });
    launches.push({ args, status: result.status, stderr: result.stderr });
    return new actual.ChildProcess();
  } };
});
const roots: string[] = [];
const managers: AgentManager[] = [];
const HOUR = 3600000;
const entry = path.resolve("src/storage/sweep-main.ts");
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-temp-sweep-")); roots.push(root); return root; };
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  launches.splice(0); vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const seed = (tempRoot: string, finishedAt: number) => {
  const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "configured");
  const run = path.join(root, "run"); fs.mkdirSync(run, { recursive: true });
  fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: finishedAt, heartbeatAt: finishedAt, closedAt: finishedAt, childrenStopped: true }));
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647", finishedAt }));
  const log = Array.from({ length: 401 }, (_, sequence) => JSON.stringify({ sequence, text: "x".repeat(80) }) + "\n").join("");
  fs.writeFileSync(path.join(run, "events.jsonl"), log);
  fs.utimesSync(run, finishedAt / 1000, finishedAt / 1000);
  return { root, run, log };
};
const snapshot = (run: string) => ({ mtime: fs.statSync(run).mtimeMs, files: fs.readdirSync(run).sort().map(name => {
  const file = path.join(run, name); const stat = fs.statSync(file);
  return [name, stat.ino, stat.mtimeMs, stat.ctimeMs, fs.readFileSync(file, "utf8")];
}) });

describe("configured detached temp sweep", () => {
  it("forwards the manager's configured compaction age and cap to the real sweep entry", async () => {
    const tempRoot = temp(); vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    const { run } = seed(tempRoot, Date.now() - 7 * HOUR); const before = snapshot(run);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
      sweepPath: entry,
      retention: { ...DEFAULT_FABRIC_CONFIG.retention, orphanedTempRunMs: 48 * HOUR, oneShotRunMs: 24 * HOUR,
        terminalRunEventsAgeMs: 24 * HOUR, terminalRunEventsMaxBytes: 1024 },
    }); managers.push(manager);
    await manager.close();
    expect(launches).toHaveLength(1);
    expect(launches[0]).toMatchObject({ status: 0, stderr: "" });
    expect(JSON.parse(launches[0]!.args[1]!)).toMatchObject({ terminalRunEventsAgeMs: 24 * HOUR, terminalRunEventsMaxBytes: 1024 });
    expect(snapshot(run)).toEqual(before);
  });

  it("keeps a seven-hour log at a 24-hour age, then uses the chosen cap without restarting expiry", async () => {
    const tempRoot = temp();
    const { root, run, log } = seed(tempRoot, Date.now() - 7 * HOUR);
    const request = { tempRoot, orphanedTempRunRetentionMs: 48 * HOUR, oneShotRunRetentionMs: 48 * HOUR,
      terminalRunEventsAgeMs: 24 * HOUR, terminalRunEventsMaxBytes: 1024 };
    const invoke = async () => {
      const [runtime, ...args] = await scriptSpawnArgs(entry, [JSON.stringify(request)]);
      const result = childProcess.spawnSync(runtime!, args, { encoding: "utf8", timeout: 20_000 });
      expect(result.status, result.stderr).toBe(0);
    };
    const before = snapshot(run); await invoke(); expect(snapshot(run)).toEqual(before);
    const finishedAt = Date.now() - 24 * HOUR - 1000;
    // Age the root as well: no worker can finish before its root was created.
    fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: finishedAt, heartbeatAt: finishedAt, closedAt: finishedAt, childrenStopped: true }));
    const record = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ ...record, finishedAt }));
    fs.utimesSync(run, finishedAt / 1000, finishedAt / 1000);
    const old = snapshot(run); await invoke();
    const compacted = fs.readFileSync(path.join(run, "events.jsonl"), "utf8");
    expect(compacted).not.toBe(log); expect(Buffer.byteLength(compacted)).toBeLessThanOrEqual(1024);
    expect(JSON.parse(compacted.split("\n")[0]!)).toMatchObject({ fabricTruncated: true });
    expect(JSON.parse(compacted.trim().split("\n").at(-1)!)).toMatchObject({ sequence: 400 });
    expect(snapshot(run).mtime).toBe(old.mtime);
    expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(JSON.stringify({ ...record, finishedAt }));
    expect(sweepTempRunRoots({ ...request, now: finishedAt + 48 * HOUR - 1 }).removedRuns).toEqual([]);
    expect(sweepTempRunRoots({ ...request, now: finishedAt + 48 * HOUR }).removedRuns).toEqual([run]);
    // Keep the serialized request type part of this entry-point regression.
    const typed: TempRunSweepRequest = request; expect(typed.tempRoot).toBe(tempRoot);
  });

  it("uses a configured byte cap in the entry even after the default age has elapsed", async () => {
    const tempRoot = temp(); const { run } = seed(tempRoot, Date.now() - 25 * HOUR);
    const request = { tempRoot, orphanedTempRunRetentionMs: 48 * HOUR, oneShotRunRetentionMs: 48 * HOUR,
      terminalRunEventsAgeMs: 24 * HOUR, terminalRunEventsMaxBytes: 1024 };
    const mtime = fs.statSync(run).mtimeMs;
    const [runtime, ...args] = await scriptSpawnArgs(entry, [JSON.stringify(request)]);
    const result = childProcess.spawnSync(runtime!, args, { encoding: "utf8", timeout: 20_000 });
    expect(result.status, result.stderr).toBe(0);
    expect(fs.statSync(path.join(run, "events.jsonl")).size).toBeLessThanOrEqual(1024);
    expect(fs.statSync(run).mtimeMs).toBe(mtime);
  });

  it("does not skip compaction younger than six hours when a shorter age is configured", async () => {
    const tempRoot = temp(); const finishedAt = Date.now() - 2 * HOUR;
    const { run } = seed(tempRoot, finishedAt);
    const request = { tempRoot, orphanedTempRunRetentionMs: 48 * HOUR, oneShotRunRetentionMs: 48 * HOUR,
      terminalRunEventsAgeMs: HOUR, terminalRunEventsMaxBytes: 1024 };
    const [runtime, ...args] = await scriptSpawnArgs(entry, [JSON.stringify(request)]);
    const result = childProcess.spawnSync(runtime!, args, { encoding: "utf8", timeout: 20_000 });
    expect(result.status, result.stderr).toBe(0);
    expect(fs.statSync(path.join(run, "events.jsonl")).size).toBeLessThanOrEqual(1024);
    expect(fs.statSync(run).mtimeMs).toBe(finishedAt);
  });
});
