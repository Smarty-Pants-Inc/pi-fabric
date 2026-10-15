import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { setImmediate } from "node:timers/promises";
import { AgentManager } from "../../src/agents/manager.js";
import { ProcessTransport } from "../../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { writeJsonAtomic } from "../../src/core/atomic-write.js";
import { saveFinalAnswerReceipt } from "../../src/worker/terminal-answer.js";
import type { AgentRunResult, AgentTransportHandle, AgentTransportLaunch } from "../../src/agents/types.js";

export const heapRunResult = (id: string, payloadBytes = 0): AgentRunResult => {
  // Materialized, distinct flat strings, not repeated ropes. Slices below test
  // the short-input parent-retention failure as well as long text/value previews.
  const payload = Buffer.alloc(payloadBytes, 65 + Number.parseInt(id.slice(-2), 16) % 26 || 65).toString("latin1");
  return {
    id, name: "heap worker", task: "heap result", runner: "pi", transport: "process", cwd: process.cwd(),
    status: "completed", startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
    text: payload, partialText: payload.slice(0, 1_000), lastCompleteText: payload.slice(0, 16_000),
    value: { output: payload }, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    sessionId: "2147483647",
  };
};

/** In-process fake worker: exercises real manager spawn/settlement, never launches execution. */
export const fakeManagerHeapWorker = async (
  request: AgentTransportLaunch,
  payloadBytes = 0,
  receipt = false,
  held?: { closed: Promise<void>; alive(): boolean; stop(): Promise<void> },
): Promise<AgentTransportHandle> => {
  const argument = (name: string): string => request.workerArguments[request.workerArguments.indexOf(name) + 1]!;
  const record = heapRunResult(request.id, payloadBytes);
  record.name = request.name;
  record.task = fs.readFileSync(argument("--task-file"), "utf8");
  record.cwd = request.cwd;
  record.startedAt = Date.now(); record.updatedAt = record.startedAt; record.finishedAt = record.startedAt;
  if (receipt) {
    const answer = saveFinalAnswerReceipt(path.dirname(argument("--status-file")), request.id, record.text);
    record.finalAnswerReceipt = { id: answer.id, recordedAt: answer.recordedAt };
  }
  writeJsonAtomic(argument("--status-file"), record);
  return {
    kind: "process", sessionId: "2147483647",
    closed: held?.closed ?? Promise.resolve(),
    isAlive: async () => held?.alive() ?? false,
    waitForClose: () => held?.closed ?? Promise.resolve(),
    stop: held?.stop ?? (async () => {}),
  };
};

const collect = async (): Promise<number> => {
  assert.equal(typeof globalThis.gc, "function", "heap probe needs node --expose-gc");
  for (let pass = 0; pass < 8; pass++) { await setImmediate(); globalThis.gc!(); }
  return process.memoryUsage().heapUsed;
};

/** Called in a fresh --expose-gc child by the parent-hosted targeted test. */
export const managerHeapProbe = async (mode: "ui" | "receipt" | "restored", root: string) => {
  // No fixture cleanup may delete. The host's private test environment owns
  // eventual cleanup; every manager/atomic-write removal here is only recorded.
  const deletions: string[] = [];
  const originals = { rmSync: fs.rmSync, unlinkSync: fs.unlinkSync, rmdirSync: fs.rmdirSync, rm: fs.promises.rm };
  fs.rmSync = ((file: fs.PathLike) => { deletions.push(String(file)); }) as typeof fs.rmSync;
  fs.unlinkSync = ((file: fs.PathLike) => { deletions.push(String(file)); }) as typeof fs.unlinkSync;
  fs.rmdirSync = ((file: fs.PathLike) => { deletions.push(String(file)); }) as typeof fs.rmdirSync;
  fs.promises.rm = (async (file: fs.PathLike) => { deletions.push(String(file)); }) as typeof fs.promises.rm;
  const originalLaunch = ProcessTransport.prototype.launch;
  let alive = true, releaseHeld!: () => void;
  const closed = new Promise<void>(resolve => { releaseHeld = () => { alive = false; resolve(); }; });
  const held = { closed, alive: () => alive, stop: () => closed };
  // Receipt mode intentionally holds 100 native-close obligations while
  // logical results settle: their shared lexical contexts must not own answers.
  ProcessTransport.prototype.launch = request => fakeManagerHeapWorker(request,
    request.name === "warm" ? 0 : 4 * 1024 * 1024, mode === "receipt",
    mode === "receipt" && request.name !== "warm" ? held : undefined);
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 101, retainRuns: true, sessionExport: false,
  }, { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  try {
    await manager.run({ name: "warm", task: "warm", transport: "process", extensions: false });
    manager.listForUi();
    const baseline = await collect();
    let lastId = "";
    for (let index = 0; index < 100; index++) {
      if (mode === "restored") {
        lastId = `restored-${index.toString(16).padStart(4, "0")}`;
        manager.restorePreviousRuns([heapRunResult(lastId, 4 * 1024 * 1024)]);
      } else {
        lastId = (await manager.run({ task: `result ${index}`, transport: "process", extensions: false })).id;
      }
      // Repeated API/UI rehydration must not repopulate a hidden full cache.
      assert.equal(manager.status(lastId).status, "completed");
      manager.listForUi();
    }
    const retainedBytes = (await collect()) - baseline;
    const uiBytes = JSON.stringify(manager.listForUi()).length * 2;
    assert.ok(retainedBytes < 24 * 1024 * 1024, `${mode}: retained ${retainedBytes} bytes for 100 x 4 MiB results`);
    assert.ok(uiBytes <= 2 * 1024 * 1024, `${mode}: UI retained ${uiBytes} bytes`);
    // Exact results are still available after GC and UI eviction.
    const full = await manager.wait(lastId);
    assert.equal(full.text.length, 4 * 1024 * 1024);
    assert.equal((full.value as { output: string }).output.length, 4 * 1024 * 1024);
    assert.equal(full.partialText?.length, 1_000);
    assert.ok(fs.existsSync(path.join(root, lastId, "status.json")));
    return { mode, runs: 100, retainedBytes, uiBytes, recordedDeletions: deletions.length };
  } finally {
    releaseHeld();
    await manager.close();
    ProcessTransport.prototype.launch = originalLaunch;
    fs.rmSync = originals.rmSync; fs.unlinkSync = originals.unlinkSync; fs.rmdirSync = originals.rmdirSync;
    fs.promises.rm = originals.rm;
  }
};
