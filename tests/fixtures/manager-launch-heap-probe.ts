import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { setImmediate } from "node:timers/promises";
import { AgentManager } from "../../src/agents/manager.js";
import { ProcessTransport } from "../../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { writeJsonAtomic } from "../../src/core/atomic-write.js";
import type { AgentHandleInfo, AgentTransportHandle, AgentTransportLaunch } from "../../src/agents/types.js";
import { fakeManagerHeapWorker, heapRunResult } from "./manager-heap-probe.js";

const RUNS = 100;
const PARENT_UNITS = 4 * 1024 * 1024;
const RESUME_TASK_UNITS = 64 * 1024;
const digest = (text: string) => createHash("sha256").update(text, "utf16le").digest("hex");
const copy = (text: string) => Buffer.from(text, "utf16le").toString("utf16le");
const argument = (request: AgentTransportLaunch, flag: string) =>
  request.workerArguments[request.workerArguments.indexOf(flag) + 1]!;

const ownedText = (index: number, units: number, prefix: string): string => {
  const bytes = Buffer.alloc(units, 65 + index % 26);
  bytes.write(`${prefix}-${index.toString().padStart(3, "0")}:`);
  // JSON-owned flat parents, distinct even when the repeated body character wraps.
  return (JSON.parse(JSON.stringify({ text: bytes.toString("latin1") })) as { text: string }).text;
};
const collect = async () => {
  if (typeof globalThis.gc !== "function") throw new Error("requires native node --expose-gc");
  for (let pass = 0; pass < 8; pass++) { await setImmediate(); globalThis.gc(); }
  return process.memoryUsage().heapUsed;
};
const latch = () => {
  let release!: () => void;
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
};
const arrivals = (target: number) => {
  const ready = latch();
  let count = 0;
  return { promise: ready.promise, hit() { if (++count === target) ready.release(); }, count: () => count };
};

/** Record every removal, never execute one. Status fencing is modeled by replacement writes. */
export const recordManagerLaunchRemovals = () => {
  const recorded: string[] = [];
  const originals = { rmSync: fs.rmSync, unlinkSync: fs.unlinkSync, rmdirSync: fs.rmdirSync, rm: fs.promises.rm };
  fs.rmSync = ((file: fs.PathLike) => { recorded.push(String(file)); }) as typeof fs.rmSync;
  fs.unlinkSync = ((file: fs.PathLike) => { recorded.push(String(file)); }) as typeof fs.unlinkSync;
  fs.rmdirSync = ((file: fs.PathLike) => { recorded.push(String(file)); }) as typeof fs.rmdirSync;
  fs.promises.rm = (async (file: fs.PathLike) => { recorded.push(String(file)); }) as typeof fs.promises.rm;
  return { recorded, restore() { Object.assign(fs, { rmSync: originals.rmSync, unlinkSync: originals.unlinkSync, rmdirSync: originals.rmdirSync }); fs.promises.rm = originals.rm; } };
};

/** Only public run/status/wait/custody APIs; no helper-only heap measurement. */
export const managerLaunchHeapProbe = async (mode: "name" | "resume", root: string) => {
  const removals = recordManagerLaunchRemovals();
  const originalLaunch = ProcessTransport.prototype.launch;
  const exits = latch(), preparations = latch(), launches = latch();
  const beforeNote = arrivals(RUNS), atPreparation = arrivals(RUNS), atLaunch = arrivals(RUNS);
  const handles: AgentHandleInfo[] = [];
  const checks: string[] = [];
  const expected = new Map<string, { hash: string; units: number; name: string; note?: string }>();
  const attempts = new Map<string, number>();
  const pending: Promise<void>[] = [];
  let initialWorkers = 0, resumedWorkers = 0, holdPreparation = false;
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", budgetUsd: 0, maxConcurrent: RUNS + 1,
    retainRuns: true, sessionExport: false,
  }, { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  const originalPrepare = manager.prepareModelForAdmission.bind(manager);
  manager.prepareModelForAdmission = async (...args: Parameters<AgentManager["prepareModelForAdmission"]>) => {
    if (holdPreparation) { atPreparation.hit(); await preparations.promise; }
    return originalPrepare(...args);
  };
  ProcessTransport.prototype.launch = async request => {
    if (request.name === "warm") return fakeManagerHeapWorker(request);
    const attempt = (attempts.get(request.id) ?? 0) + 1;
    attempts.set(request.id, attempt);
    if (mode === "resume" && attempt === 2) { atLaunch.hit(); await launches.promise; }
    const task = fs.readFileSync(argument(request, "--task-file"), "utf8");
    const key = mode === "name" ? copy(task.slice(0, 60)) : request.name;
    const wanted = expected.get(key);
    const originalTask = mode === "resume" && attempt === 2 ? task.slice(2_002) : task;
    if (!wanted || originalTask.length !== wanted.units || digest(originalTask) !== wanted.hash) checks.push(`worker task ${key}/${attempt}`);
    if (wanted && request.name !== wanted.name) checks.push(`worker name ${key}/${attempt}`);
    const record = heapRunResult(request.id);
    Object.assign(record, { task, name: request.name, cwd: request.cwd, turns: 3, toolCalls: 2 });
    if (mode === "resume" && attempt === 1) {
      initialWorkers++;
      record.status = "stopped";
      record.error = ownedText(initialWorkers - 1, PARENT_UNITS, "tool-result interruption");
      // Keep only the exact bounded expected prefix, never a slice of the parent.
      if (wanted) wanted.note = copy([
        "[Fabric continuation] A previous attempt at this exact task was interrupted before it finished.",
        `It ended with status "stopped" (${record.error}) after 3 turns and 2 tool calls, so the working tree already contains what that attempt completed.`,
        "Inspect the current state first, do not redo work that is already done, and carry the task through to completion.",
      ].join(" ").slice(0, 2_000));
    } else {
      if (mode === "resume") {
        resumedWorkers++;
        if (wanted && task.slice(0, 2_000) !== wanted.note) checks.push(`continuation prefix ${key}`);
        if (task.slice(2_000, 2_002) !== "\n\n") checks.push(`continuation separator ${key}`);
        const carry = JSON.parse(argument(request, "--carry-over")) as { turns: number; toolCalls: number };
        if (carry.turns !== 3 || carry.toolCalls !== 2) checks.push(`carry-over ${key}`);
      } else initialWorkers++;
    }
    writeJsonAtomic(argument(request, "--status-file"), record);
    // Do not close over record/task/error in a retained transport handle.
    return mode === "resume" && attempt === 1 ? stoppedHandle() : completedHandle();
  };
  function completedHandle(): AgentTransportHandle {
    return { kind: "process", sessionId: "2147483647", closed: Promise.resolve(),
      waitForClose: async () => {}, isAlive: async () => false, stop: async () => {} };
  }
  function stoppedHandle(): AgentTransportHandle {
    let first = true;
    return { kind: "process", sessionId: "2147483647", closed: Promise.resolve(), waitForClose: async () => {},
      isAlive: async () => { if (first) { first = false; beforeNote.hit(); await exits.promise; } return false; }, stop: async () => {} };
  }
  // This frame is deliberately gone before GC; a retained final loop variable
  // or full settled run Promise would otherwise supply a false 4-MiB baseline.
  async function runOne(index: number) {
    const task = ownedText(index, mode === "name" ? PARENT_UNITS : RESUME_TASK_UNITS, "distinct full task");
    const name = mode === "name" ? copy(task.slice(0, 60)) : `resume-${index}`;
    expected.set(name, { hash: digest(task), units: task.length, name });
    const result = await manager.run({ task, ...(mode === "resume" ? { name } : {}), transport: "process", extensions: false },
      undefined, handle => { handles.push(handle); });
    const fullTask = mode === "resume" ? result.task.slice(2_002) : result.task;
    if (result.status !== "completed" || digest(fullTask) !== expected.get(name)?.hash || result.name !== name) checks.push(`settled result ${index}`);
  }
  try {
    await manager.run({ name: "warm", task: "warm", transport: "process", extensions: false });
    manager.listForUi();
    const baselineBytes = await collect();
    let stoppedBaselineBytes = baselineBytes, preparationBytes = baselineBytes;
    let custodyAtPreparation = 0, custodyAtLaunch = 0;
    if (mode === "name") {
      for (let index = 0; index < RUNS; index++) await runOne(index);
    } else {
      for (let index = 0; index < RUNS; index++) pending.push(runOne(index));
      // Observe rejections immediately, but do not retain full run results.
      for (const work of pending) void work.catch(() => {});
      await beforeNote.promise;
      stoppedBaselineBytes = await collect();
      holdPreparation = true;
      exits.release();
      await atPreparation.promise;
      preparationBytes = await collect();
      custodyAtPreparation = handles.filter(handle => manager.hasRunCustody(handle.id) && !manager.isSettled(handle.id)).length;
      preparations.release();
      await atLaunch.promise;
      custodyAtLaunch = handles.filter(handle => manager.hasRunCustody(handle.id) && !manager.isSettled(handle.id)).length;
      launches.release();
      await Promise.all(pending);
      pending.length = 0;
    }
    manager.listForUi();
    const settledBytes = await collect();
    const retainedBytes = mode === "name" ? settledBytes - baselineBytes : preparationBytes - stoppedBaselineBytes;
    const receipt = { mode, runs: RUNS, parentUnits: PARENT_UNITS, baselineBytes, stoppedBaselineBytes, preparationBytes,
      settledBytes, retainedBytes, observedStoppedStateBytes: stoppedBaselineBytes - baselineBytes,
      // ASCII/Latin-1 errors: exactly 419,430,400 live error units in BOTH samples.
      unavoidableErrorUnits: mode === "resume" ? RUNS * PARENT_UNITS : 0,
      resumeTaskUnits: mode === "resume" ? RESUME_TASK_UNITS : 0,
      beforeNoteArrivals: beforeNote.count(), preparationArrivals: atPreparation.count(), launchArrivals: atLaunch.count(),
      custodyAtPreparation, custodyAtLaunch, initialWorkers, resumedWorkers,
      settledHandles: handles.filter(handle => manager.hasRunCustody(handle.id) && manager.isSettled(handle.id)).length,
      checks, recordedDeletions: removals.recorded.length };
    // Receipts precede every heap assertion (the parent test owns assertions).
    console.log(JSON.stringify({ heapReceipt: receipt }));
    return receipt;
  } finally {
    exits.release(); preparations.release(); launches.release();
    try {
      await Promise.allSettled(pending);
      await manager.close();
    } finally {
      ProcessTransport.prototype.launch = originalLaunch;
      removals.restore();
    }
  }
};
