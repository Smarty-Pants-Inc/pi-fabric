import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { canRemoveTerminalRun, canRemoveManagedRunRoot, FABRIC_RUN_ROOT_PREFIX, runTreeExitVeto, sweepTempRunRoots } from "../src/storage/retention.js";
import type { AgentRunRecord, AgentTransportLaunch } from "../src/agents/types.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const fixture = (options: { timeoutMs?: number; lost?: boolean; dead?: boolean } = {}) => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-terminal-durability-")); roots.push(temp);
  const root = path.join(temp, FABRIC_RUN_ROOT_PREFIX + "fixture");
  let alive = !options.dead;
  const stop = vi.fn(async () => { alive = false; });
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async (request: AgentTransportLaunch) => {
    const args = request.workerArguments;
    const statusFile = args[args.indexOf("--status-file") + 1]!;
    const record: AgentRunRecord = { id: request.id, name: request.name, task: "fixture", status: "running", runner: "pi", transport: options.lost ? "herdr" : "process", cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, logFile: path.join(path.dirname(statusFile), "events.jsonl") };
    fs.writeFileSync(statusFile, JSON.stringify(record));
    return { kind: options.lost ? "herdr" : "process", relaunchable: false, livenessPollIntervalMs: 1, isAlive: async () => options.lost ? false : alive, stop, ...(options.lost ? { lostContact: () => "Herdr contact lost; pane may still run" } : {}) };
  });
  const consumed = vi.fn();
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: options.timeoutMs ?? 60_000, retainRuns: true, maxConcurrent: 1 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, onResultConsumed: consumed });
  managers.push(manager);
  return { temp, root, manager, launch, stop, consumed };
};
const faults = (reject: (file: string, fd: number) => boolean) => {
  const descriptors = new Map<number, string>();
  const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
  return vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
    if (reject(descriptors.get(fd) ?? "", fd)) throw new Error("injected publication barrier failure");
    sync(fd);
  });
};
const status = (manager: AgentManager, id: string) => path.join(manager.runDirectory(id)!, "status.json");

describe("Astra F15-F17 terminal publication obligations", () => {
  it.skipIf(process.platform === "win32")("F15 stop retries visible post-rename bytes without settling while directory confirmation still fails", async () => {
    const { manager, stop, consumed } = fixture();
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const file = status(manager, handle.id);
    let unavailable = true, barriers = 0;
    const fault = faults((target, fd) => target === path.dirname(file) && fs.fstatSync(fd).isDirectory() && (++barriers, unavailable));
    try {
      await expect(manager.stop(handle.id)).rejects.toThrow("injected publication");
      expect(JSON.parse(fs.readFileSync(file, "utf8")).status).toBe("stopped");
      const beforeRetry = barriers;
      await expect(manager.stop(handle.id)).rejects.toThrow("injected publication");
      expect(barriers).toBeGreaterThan(beforeRetry);
      await expect(manager.wait(handle.id, { timeoutMs: 350 })).rejects.toThrow();
      expect(consumed).not.toHaveBeenCalled();
      unavailable = false;
      expect((await manager.wait(handle.id, { timeoutMs: 3000 })).status).toBe("stopped");
      expect(stop).toHaveBeenCalledOnce();
    } finally { fault.mockRestore(); }
  });

  it.skipIf(process.platform === "win32")("F15 monitor cannot consume a worker paused between rename and directory confirmation", async () => {
    const { manager, consumed } = fixture();
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const file = status(manager, handle.id), gate = path.join(path.dirname(file), "writer-resume");
    const module = pathToFileURL(path.resolve("src/worker/run-record.ts")).href;
    const script = `import fs from 'node:fs'; import {writeRunRecord} from ${JSON.stringify(module)};
      const file = ${JSON.stringify(file)}, gate = ${JSON.stringify(gate)};
      const record = {...JSON.parse(fs.readFileSync(file,'utf8')),status:'completed',finishedAt:Date.now(),text:'durable answer'};
      const sync = fs.fsyncSync; let paused = false;
      fs.fsyncSync = fd => { if (!paused && fs.fstatSync(fd).isDirectory()) { paused = true; fs.writeSync(1,'renamed\\n'); while (!fs.existsSync(gate)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,10); } sync(fd); };
      writeRunRecord(file,record);`;
    let unavailable = true;
    const fault = faults((target, fd) => target === path.dirname(file) && fs.fstatSync(fd).isDirectory() && unavailable);
    const writer = spawn(process.execPath, ["--input-type=module", "--eval", script], { stdio: ["ignore", "pipe", "pipe"] });
    const exited = once(writer, "exit");
    let stderr = ""; writer.stderr.on("data", chunk => { stderr += String(chunk); });
    try {
      await vi.waitFor(() => expect(JSON.parse(fs.readFileSync(file, "utf8")).status).toBe("completed"));
      await expect(manager.wait(handle.id, { timeoutMs: 350 })).rejects.toThrow();
      expect(consumed).not.toHaveBeenCalled();
      expect(writer.exitCode).toBeNull();
      unavailable = false;
      // Reader may discharge the owed barrier itself, even while the writer is paused.
      expect((await manager.wait(handle.id, { timeoutMs: 3000 })).text).toBe("durable answer");
    } finally {
      fault.mockRestore(); fs.writeFileSync(gate, "resume");
      expect(await exited, stderr).toEqual([0, null]);
    }
  });

  it.each(["deadline", "transport-death"] as const)("F16 %s monitor retries one-shot pre-rename fsync failure, settles and releases admission once", async mode => {
    const { manager, launch, stop } = fixture({ timeoutMs: mode === "deadline" ? 1 : 60_000, dead: mode === "transport-death" });
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const file = status(manager, handle.id);
    let failures = 0;
    const fault = faults(target => target.startsWith(file + ".") && target.endsWith(".tmp") && failures++ === 0);
    const unhandled: unknown[] = [];
    const rejection = (error: unknown) => { unhandled.push(error); };
    process.on("unhandledRejection", rejection);
    try {
      const result = await manager.wait(handle.id, { timeoutMs: 4000 });
      expect(result.status).toBe(mode === "deadline" ? "timed_out" : "failed");
      expect(failures).toBeGreaterThanOrEqual(2);
      expect(unhandled).toEqual([]);
      expect(stop).toHaveBeenCalledTimes(mode === "deadline" ? 1 : 0);
      expect(launch).toHaveBeenCalledOnce();
      const next = await manager.spawn({ task: "next admitted", transport: "process" });
      expect(launch).toHaveBeenCalledTimes(2);
      await manager.stop(next.id);
    } finally { fault.mockRestore(); process.off("unhandledRejection", rejection); }
  });

  it("F17 retries a one-shot unresolved-marker publication failure before settlement", async () => {
    const { manager } = fixture({ lost: true });
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const directory = manager.runDirectory(handle.id)!;
    let attempts = 0;
    const fault = faults(target => target.startsWith(path.join(directory, "unresolved-worker.json") + ".") && attempts++ === 0);
    try {
      expect((await manager.wait(handle.id, { timeoutMs: 4000 })).status).toBe("failed");
      expect(attempts).toBeGreaterThanOrEqual(2);
      expect(JSON.parse(fs.readFileSync(path.join(directory, "unresolved-worker.json"), "utf8")).reason).toContain("Herdr contact lost");
    } finally { fault.mockRestore(); }
  });

  it.each(["closed", "orphaned"] as const)("F17 failed veto then successful terminal publication survives manager exit and %s offline sweep", async ownerState => {
    const { temp, root, manager } = fixture({ lost: true });
    const handle = await manager.spawn({ task: "fixture", transport: "process" });
    const directory = manager.runDirectory(handle.id)!;
    let attempts = 0;
    const fault = faults(target => target.startsWith(path.join(directory, "unresolved-worker.json") + ".") && (++attempts, true));
    try {
      expect((await manager.wait(handle.id, { timeoutMs: 4000 })).status).toBe("failed");
      expect(attempts).toBeGreaterThan(0);
      expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(false);
      expect(JSON.parse(fs.readFileSync(status(manager, handle.id), "utf8"))).toMatchObject({ status: "failed", transport: "herdr" });
      await manager.close();
      fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, ...(ownerState === "closed" ? { closedAt: 1, childrenStopped: true } : { orphanedAt: 1 }) }));
      const removable = canRemoveTerminalRun(directory);
      const removableRoot = canRemoveManagedRunRoot(root);
      const veto = runTreeExitVeto(directory);
      const result = sweepTempRunRoots({ tempRoot: temp, now: Date.now() + 10_000, orphanedTempRunRetentionMs: 0, oneShotRunRetentionMs: 0 });
      expect(result).toEqual({ removedRoots: [], removedRuns: [] });
      expect(removable).toBe(false);
      expect(removableRoot).toBe(false);
      expect(veto).toMatch(/no checked worker exit receipt/);
      expect(fs.existsSync(path.join(directory, "task.txt"))).toBe(true);
      // A still-live pane can overwrite the synthesized failure. That cannot
      // remove the persistent transport-based obligation either.
      const record = JSON.parse(fs.readFileSync(status(manager, handle.id), "utf8"));
      fs.writeFileSync(status(manager, handle.id), JSON.stringify({ ...record, status: "completed" }));
      expect(canRemoveTerminalRun(directory)).toBe(false);
      expect(sweepTempRunRoots({ tempRoot: temp, now: Date.now() + 10_000, orphanedTempRunRetentionMs: 0, oneShotRunRetentionMs: 0 })).toEqual({ removedRoots: [], removedRuns: [] });
    } finally { fault.mockRestore(); }
  });
});
