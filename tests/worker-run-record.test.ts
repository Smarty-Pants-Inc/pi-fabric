import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { applyUsage, createRunningRecord, extractUsageDelta, updateRunRecord, writeCrashRunRecord } from "../src/worker/run-record.js";
import type { AgentRunRecord, AgentWorkerOptions } from "../src/agents/types.js";
import { processStartTime } from "../src/residency/process-identity.js";

const baseRecord = (): AgentRunRecord => ({
  id: "id",
  name: "name",
  task: "task",
  status: "running",
  runner: "pi",
  transport: "process",
  cwd: "/tmp",
  startedAt: 0,
  updatedAt: 0,
  turns: 0,
  toolCalls: 0,
  text: "",
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  logFile: "/tmp/log",
});

describe("worker run-record process identity", () => {
  const options = (transport: AgentWorkerOptions["transport"]): AgentWorkerOptions => ({
    id: "id", name: "worker", runner: "pi", transport, cwd: os.tmpdir(),
    taskFile: "task.txt", statusFile: "status.json", lifecycleFile: "lifecycle.jsonl", logFile: "events.jsonl",
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda", vedaBackend: "", vedaPersona: "",
    timeoutMs: 1000, depth: 0, fullCodeMode: false, extensions: false, tools: [], grantedRisks: [],
  });

  it("persists the publisher's PID and birth identity through terminal and crash publication", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-record-identity-"));
    const file = path.join(root, "status.json");
    try {
      const record = createRunningRecord(options("process"), "task", undefined, 1);
      expect(record.sessionId).toBe(String(process.pid));
      expect(record.processStartTime).toBe(processStartTime(process.pid));
      if (process.platform === "linux") expect(record.processStartTime).toMatch(/^\d+$/);
      record.status = "completed";
      updateRunRecord(file, record);
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ sessionId: String(process.pid), status: "completed" });
      writeCrashRunRecord(file, record, new Error("crash"));
      const saved = JSON.parse(fs.readFileSync(file, "utf8"));
      expect(saved).toMatchObject({ sessionId: String(process.pid), status: "failed" });
      expect(saved.processStartTime).toBe(record.processStartTime);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("keeps a usable PID when the birth identity cannot be read", () => {
    const read = vi.spyOn(fs, "readFileSync").mockImplementation(() => { throw new Error("unreadable"); });
    try {
      const record = createRunningRecord(options("process"), "task", undefined, 1);
      expect(record.sessionId).toBe(String(process.pid));
      expect(record.processStartTime).toBeUndefined();
    } finally { read.mockRestore(); }
  });

  it.each(["tmux", "screen"] as const)("does not claim a process PID for %s", transport => {
    const record = createRunningRecord(options(transport), "task", undefined, 1);
    expect(record.sessionId).toBeUndefined(); expect(record.processStartTime).toBeUndefined();
  });
});

describe("worker run-record usage", () => {
  it("persists the publishing process identity for process workers only", () => {
    const options = { id: "identity", name: "identity", runner: "pi" as const, transport: "process" as const,
      cwd: "/tmp", taskFile: "/tmp/task", statusFile: "/tmp/status", logFile: "/tmp/log", lifecycleFile: "/tmp/lifecycle",
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda", vedaBackend: "", vedaPersona: "",
      timeoutMs: 1000, depth: 0, fullCodeMode: false, extensions: false, tools: [], grantedRisks: [] };
    const record = createRunningRecord(options, "task", undefined, Date.now());
    expect(record.sessionId).toBe(String(process.pid));
    if (process.platform === "linux") expect(record.processStartTime).toMatch(/^\d+$/);
    expect(createRunningRecord({ ...options, transport: "tmux" }, "task", undefined, Date.now()).sessionId).toBeUndefined();
  });
  it("extractUsageDelta returns per-message usage without mutating the record", () => {
    const record = baseRecord();
    const delta = extractUsageDelta({
      usage: { input: 10, output: 5, cacheRead: 3, cacheWrite: 2, cost: { total: 42 } },
    });
    expect(delta).toEqual({ input: 10, output: 5, cacheRead: 3, cacheWrite: 2, cost: 42 });
    expect(record.usage.input).toBe(0);
  });

  it("extractUsageDelta treats a numeric cost as already in total units", () => {
    const delta = extractUsageDelta({ usage: { input: 1, output: 2, cost: 7 } });
    expect(delta).toMatchObject({ input: 1, output: 2, cost: 7 });
  });

  it("extractUsageDelta returns undefined for a usage-free message", () => {
    expect(extractUsageDelta({ content: "text" })).toBeUndefined();
    expect(extractUsageDelta({ usage: null })).toBeUndefined();
  });

  it("applyUsage and extractUsageDelta agree on the same message", () => {
    const record = baseRecord();
    const delta = extractUsageDelta({
      usage: { input: 100, output: 50, cacheRead: 10, cacheWrite: 5, cost: { total: 25 } },
    })!;
    applyUsage(record, {
      usage: { input: 100, output: 50, cacheRead: 10, cacheWrite: 5, cost: { total: 25 } },
    });
    expect(record.usage.input).toBe(delta.input);
    expect(record.usage.output).toBe(delta.output);
    expect(record.usage.cacheRead).toBe(delta.cacheRead);
    expect(record.usage.cacheWrite).toBe(delta.cacheWrite);
    expect(record.usage.cost).toBe(delta.cost);
  });

  it("extractUsageDelta on cumulative Claude frames produces monotonically increasing attribution", () => {
    const first = extractUsageDelta({
      usage: { input: 10, output: 5, cacheRead: 2, cacheWrite: 1 },
    })!;
    const second = extractUsageDelta({
      usage: { input: 25, output: 12, cacheRead: 5, cacheWrite: 3 },
    })!;
    const delta = {
      input: second.input - first.input,
      output: second.output - first.output,
      cacheRead: second.cacheRead - first.cacheRead,
      cacheWrite: second.cacheWrite - first.cacheWrite,
    };
    expect(delta.input).toBe(15);
    expect(delta.output).toBe(7);
    expect(delta.cacheRead).toBe(3);
    expect(delta.cacheWrite).toBe(2);
  });
});
