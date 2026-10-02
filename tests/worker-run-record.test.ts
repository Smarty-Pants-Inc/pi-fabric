import { describe, expect, it } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { applyUsage, extractUsageDelta, createRunRouteMetadata, createRunningRecord, writeRunRecord, updateRunRecord, writeCrashRunRecord } from "../src/worker/run-record.js";
import { parseWorkerOptions } from "../src/worker/options.js";
import type { AgentRunRecord } from "../src/agents/types.js";

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

describe("record-only run classes", () => {
  const argv = (flags: Record<string, string> = {}) => ["node", "worker.js", ...Object.entries({
    id: "run", name: "supervisor", runner: "pi", transport: "process", "task-file": "task.txt",
    "status-file": "status.json", "lifecycle-file": "lifecycle.jsonl", "log-file": "events.jsonl", cwd: "/tmp",
    "pi-binary": "pi", "claude-binary": "claude", "veda-binary": "veda", "veda-backend": "pi", "veda-persona": "worker",
    "timeout-ms": "1000", depth: "1", "full-code-mode": "false", extensions: "false", tools: "[]", "granted-risks": "[]", ...flags,
  }).flatMap(([key, value]) => [`--${key}`, value])];
  it.each(["pi", "claude", "veda"] as const)("derives %s task classes from transport, not task/name text", runner => {
    const options = parseWorkerOptions(argv({ runner, transport: "tmux" }));
    const record = createRunningRecord(options, "actor:review security status-groom", undefined, 1);
    expect(record).toMatchObject({ routeClass: `task:${runner}:tmux`, routeClassSource: "derived" });
    expect(record).not.toHaveProperty("protected");
  });
  it.each(["true", "false"])("persists the host class/source and protection=%s on running, updated and crashed status", protection => {
    const options = parseWorkerOptions(argv({ "route-class": "actor:security", "route-class-source": "derived", protected: protection }));
    const record = createRunningRecord(options, "bounded-lookup", undefined, 1);
    const expected = { routeClass: "actor:security", routeClassSource: "derived", protected: protection === "true" };
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "run-class-")); const file = path.join(dir, "status.json");
    try {
      writeRunRecord(file, record);
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ ...expected, status: "running" });
      updateRunRecord(file, record);
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject(expected);
      writeCrashRunRecord(file, record, new Error("probe"));
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ ...expected, status: "failed" });
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
  it("preserves explicit classes over actor roles and handoff derivation", () => {
    expect(createRunRouteMetadata({ runner: "pi", transport: "process", actorId: "actor", actorName: "factory-review-astra", routeClass: "status-groom", protected: true }))
      .toEqual({ routeClass: "status-groom", routeClassSource: "explicit", protected: true });
    expect(createRunRouteMetadata({ runner: "pi", transport: "process", handoff: true }))
      .toEqual({ routeClass: "handoff", routeClassSource: "derived" });
    expect(createRunRouteMetadata({ runner: "pi", transport: "process", handoff: true, routeClass: "custom-handoff" }))
      .toEqual({ routeClass: "custom-handoff", routeClassSource: "explicit" });
  });
  it("rejects malformed host metadata without inventing a protection value", () => {
    expect(() => parseWorkerOptions(argv({ "route-class-source": "prompt" }))).toThrow("Invalid worker route class source");
    expect(() => parseWorkerOptions(argv({ protected: "unknown" }))).toThrow("Invalid worker protection snapshot");
  });
});

describe("worker run-record usage", () => {
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
