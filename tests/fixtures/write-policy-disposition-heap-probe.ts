import fs from "node:fs";
import path from "node:path";
import { setImmediate } from "node:timers/promises";
import { AgentManager } from "../../src/agents/manager.js";
import { ProcessTransport } from "../../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { writeJsonAtomic } from "../../src/core/atomic-write.js";
import type { AgentRunResult, AgentTransportHandle, AgentTransportLaunch } from "../../src/agents/types.js";

const RUNS = 100;
const TASK_UNITS = 4 * 1024 * 1024;
type Disposition = NonNullable<AgentTransportLaunch["onWritePolicyDisposition"]>;
const LOCAL_WARNING = "Shell writes are unconfined by the task write policy (native bash/powershell and nested pi.bash); any Fabric Landlock settings are independent and may grant wider roots.";
const REMOTE_WARNING = "Remote one-shot placement cannot forward task write roots or scratch yet; remote task remains unrestricted.";
const real = (file: string) => fs.realpathSync.native(file);

const collect = async (): Promise<number> => {
  if (typeof globalThis.gc !== "function") throw new Error("requires native node --expose-gc");
  for (let pass = 0; pass < 8; pass++) { await setImmediate(); globalThis.gc(); }
  return process.memoryUsage().heapUsed;
};
// Distinct flat (JSON-owned) 4-MiB task strings, not ropes or shared slices.
const ownedTask = (index: number): string => {
  const bytes = Buffer.alloc(TASK_UNITS, 65 + index % 26);
  bytes.write(`disposition task ${index.toString().padStart(3, "0")}:`);
  return (JSON.parse(JSON.stringify({ text: bytes.toString("latin1") })) as { text: string }).text;
};
const auditRecords = (root: string, id: string) =>
  fs.readFileSync(path.join(root, id, "events.jsonl"), "utf8").trim().split("\n")
    .map(line => JSON.parse(line) as Record<string, unknown> & { type: string; ts?: unknown;
      policy?: { writableRoots?: string[]; shell?: string } })
    .filter(record => record.type.startsWith("write_policy."));

/**
 * Fresh --expose-gc child only. Public manager.run with an in-process fake
 * launch that keeps ONLY each launch's onWritePolicyDisposition callback (as a
 * live transport does). Manager, requests and results are out of scope before
 * measurement, so retained bytes are attributable to the callbacks alone.
 */
export const writePolicyDispositionHeapProbe = async (root: string) => {
  // Record every product removal; never execute one.
  const deletions: string[] = [];
  const originals = { rmSync: fs.rmSync, unlinkSync: fs.unlinkSync, rmdirSync: fs.rmdirSync, rm: fs.promises.rm };
  fs.rmSync = ((file: fs.PathLike) => { deletions.push(String(file)); }) as typeof fs.rmSync;
  fs.unlinkSync = ((file: fs.PathLike) => { deletions.push(String(file)); }) as typeof fs.unlinkSync;
  fs.rmdirSync = ((file: fs.PathLike) => { deletions.push(String(file)); }) as typeof fs.rmdirSync;
  fs.promises.rm = (async (file: fs.PathLike) => { deletions.push(String(file)); }) as typeof fs.promises.rm;
  const originalLaunch = ProcessTransport.prototype.launch;
  const callbacks: Array<{ id: string; cwd: string; disposition: "local" | "remote"; callback: Disposition }> = [];
  const checks: string[] = [];
  ProcessTransport.prototype.launch = async (request: AgentTransportLaunch): Promise<AgentTransportHandle> => {
    const argument = (flag: string) => request.workerArguments[request.workerArguments.indexOf(flag) + 1]!;
    const callback = request.onWritePolicyDisposition;
    // Distinct runs alternate local/remote; each is invoked twice (the repeat must be a no-op).
    const disposition = callbacks.length % 2 === 0 ? "local" as const : "remote" as const;
    if (request.name !== "warm") {
      if (!callback) checks.push(`no disposition callback ${request.name}`);
      else callbacks.push({ id: request.id, cwd: request.cwd, disposition, callback });
    }
    callback?.(disposition);
    callback?.(disposition);
    const startedAt = Date.now();
    const record: AgentRunResult = {
      id: request.id, name: request.name, task: fs.readFileSync(argument("--task-file"), "utf8"), runner: "pi",
      transport: "process", cwd: request.cwd, status: "completed", startedAt, updatedAt: startedAt, finishedAt: startedAt,
      turns: 1, toolCalls: 0, text: "done", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      sessionId: "2147483647",
    };
    writeJsonAtomic(argument("--status-file"), record);
    // The handle closes over nothing from the request/record.
    return { kind: "process", sessionId: "2147483647", closed: Promise.resolve(),
      waitForClose: async () => {}, isAlive: async () => false, stop: async () => {} };
  };
  const newManager = () => new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", budgetUsd: 0, maxConcurrent: RUNS + 1,
    retainRuns: true, sessionExport: false,
  }, { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  // Every manager/request/result reference lives only inside these frames.
  async function warm(): Promise<void> {
    const manager = newManager();
    try { await manager.run({ name: "warm", task: "warm", transport: "process", extensions: false }); }
    finally { await manager.close(); }
  }
  async function runAll(): Promise<void> {
    const manager = newManager();
    try {
      for (let index = 0; index < RUNS; index++) {
        // Explicit short detached names: main's derived-name slice is a separate retainer.
        const result = await manager.run({ name: `disposition-${index}`, task: ownedTask(index),
          transport: "process", extensions: false });
        if (result.status !== "completed" || result.task.length !== TASK_UNITS) checks.push(`settled ${index}`);
      }
    } finally { await manager.close(); }
  }
  try {
    await warm();
    const baselineBytes = await collect();
    await runAll();
    const retainedBytes = (await collect()) - baselineBytes;
    // Callbacks are still live (used below), so the measurement covered them.
    let localRuns = 0, remoteRuns = 0;
    for (const { id, cwd, disposition, callback } of callbacks) {
      // Retained callbacks stay at-most-once after manager close: neither kind appends again.
      callback(disposition);
      callback(disposition === "local" ? "remote" : "local");
      const records = auditRecords(root, id);
      if (records.length !== 1) { checks.push(`audit count ${id}: ${records.map(r => r.type).join(",")}`); continue; }
      const { ts, ...entry } = records[0]!;
      if (typeof ts !== "number") checks.push(`audit ts ${id}`);
      if (disposition === "local") {
        localRuns++;
        const { policy, ...rest } = entry;
        if (JSON.stringify(rest) !== JSON.stringify({ type: "write_policy.default", id, runner: "pi", warning: LOCAL_WARNING }))
          checks.push(`local audit payload ${id}: ${JSON.stringify(rest)}`);
        // Default task policy unchanged: exactly [agent cwd, run tmpdir], shell unconfined.
        const roots = policy?.writableRoots?.map(real);
        if (policy?.shell !== "unconfined" || JSON.stringify(roots?.sort()) !== JSON.stringify([real(cwd), real(path.join(root, id, "tmp"))].sort()))
          checks.push(`local audit policy ${id}: ${JSON.stringify(policy)}`);
      } else {
        remoteRuns++;
        if (JSON.stringify(entry) !== JSON.stringify({ type: "write_policy.remote_unconfined", id, runner: "pi", warning: REMOTE_WARNING }))
          checks.push(`remote audit payload ${id}: ${JSON.stringify(entry)}`);
      }
    }
    const receipt = { runs: RUNS, taskUnits: TASK_UNITS, callbacks: callbacks.length, localRuns, remoteRuns, baselineBytes, retainedBytes,
      checks, recordedDeletions: deletions.length };
    console.log(JSON.stringify({ heapReceipt: receipt }));
    return receipt;
  } finally {
    ProcessTransport.prototype.launch = originalLaunch;
    Object.assign(fs, { rmSync: originals.rmSync, unlinkSync: originals.unlinkSync, rmdirSync: originals.rmdirSync });
    fs.promises.rm = originals.rm;
  }
};
