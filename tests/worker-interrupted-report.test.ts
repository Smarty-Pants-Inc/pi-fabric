import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { interruptedModelStreamError } from "../src/worker/provider-error.js";

// A published terminal record is not native process closure. Join every owned
// worker after the manager drains its execution group, before deleting fixtures.
const closed: Promise<void>[] = [];
vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn((...args: Parameters<typeof spawn>) => {
    const child = actual.spawn(...args);
    closed.push(new Promise<void>(resolve => child.once("close", () => resolve())));
    return child;
  }) };
});
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(closed.splice(0));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const previous = "Tool work finished: patched worker and tests.";
const final = "FINAL: completed the patch. 🦄";
const warning = "final report interrupted by a model stream error; showing the last persisted output";
const run = async (mode: string, error = "stream disconnected before completion", request: Partial<AgentRunRequest> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-interrupted-report-")); roots.push(root);
  const launches = path.join(root, "launches.txt");
  const snapshots = path.join(root, "snapshots.jsonl");
  vi.stubEnv("FAKE_PI_REPORT_MODE", mode);
  vi.stubEnv("FAKE_PI_REPORT_ERROR", error);
  vi.stubEnv("FAKE_PI_REPORT_LAUNCHES", launches);
  vi.stubEnv("FAKE_PI_REPORT_SNAPSHOTS", snapshots);
  vi.stubEnv("FAKE_PI_REPORT_RUN_ROOT", path.join(root, "runs"));
  vi.stubEnv("PI_FABRIC_TEST_RECOVERY_TIME_SCALE", "0.001");
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 10000, retainRuns: true }, {
    workerPath: path.resolve(process.env.FABRIC_INTERRUPTED_REPORT_WORKER ?? "src/worker.ts"),
    piBinary: path.resolve("tests/fixtures/fake-pi-interrupted-report.mjs"),
    runRoot: path.join(root, "runs"),
  }); managers.push(manager);
  const actorSession = request.actorId || request.actorName ? path.join(root, "actor-session.jsonl") : undefined;
  if (actorSession) fs.writeFileSync(actorSession, JSON.stringify({ type: "session", version: 3, id: "fake-actor-session", timestamp: new Date().toISOString(), cwd: root }) + "\n");
  const handle = await manager.spawn({ task: "Finish the patch and return only actual output", transport: "process", extensions: false, ...(actorSession ? { sessionFile: actorSession } : {}), ...request });
  if (mode === "stop") {
    const deadline = Date.now() + 5000;
    while (!fs.existsSync(snapshots) || !fs.readFileSync(snapshots, "utf8").includes(JSON.stringify(final))) {
      if (Date.now() >= deadline) throw new Error("Final text did not arrive before stop probe");
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    await manager.stop(handle.id);
  }
  const result = await manager.wait(handle.id);
  const status = manager.status(handle.id);
  const again = await manager.wait(handle.id); // agents.join shares this wait path.
  const record = JSON.parse(fs.readFileSync(path.join(root, "runs", handle.id, "status.json"), "utf8"));
  const streamed = fs.existsSync(snapshots) ? fs.readFileSync(snapshots, "utf8").trim().split("\n").map(line => JSON.parse(line)) : [];
  const evidence = process.env.FABRIC_INTERRUPTED_REPORT_EVIDENCE_DIR;
  if (evidence) {
    fs.mkdirSync(evidence, { recursive: true });
    const variant = request.replyTool ? "-reply-tool" : request.actorId ? "-actor-id" : request.actorName ? "-actor-name" : "";
    fs.writeFileSync(path.join(evidence, `${mode}${variant}-${error.replace(/[^a-z0-9]+/gi, "-")}.json`), JSON.stringify({ result, status, record, streamed, launches: fs.readFileSync(launches, "utf8") }, null, 2));
  }
  for (const returned of [status, again, record]) {
    expect(returned).toMatchObject({ status: result.status, text: result.text });
    expect(returned.error).toBe(result.error);
    expect(returned.partialText).toBe(result.partialText);
    expect(returned.lastCompleteText).toBe(result.lastCompleteText);
    expect(returned.warnings).toEqual(result.warnings);
    expect(returned.exitCode).toBe(result.exitCode);
  }
  expect(fs.readFileSync(launches, "utf8")).toBe("launch\n"); // No regeneration or tool replay.
  return { result, streamed };
};

describe("durable interrupted task reports", () => {
  it.each(["stream disconnected before completion", "Error [ERR_STREAM_PREMATURE_CLOSE]: Premature close", "503 server_is_overloaded"])("returns partial text after completed tools: %s", async error => {
    const { result, streamed } = await run("partial", error);
    expect(result).toMatchObject({ status: "failed", exitCode: 1, lastCompleteText: previous, partialText: final, error });
    expect(result.text).toBe(final);
    expect(result.warnings).toContain(`${warning}: ${error}`);
    expect(streamed.map(record => record.partialText)).toEqual([undefined, "FINAL: completed", final]);
    expect(streamed.every(record => record.lastCompleteText === previous)).toBe(true);
    expect(streamed.every(record => record.status === "running")).toBe(true);
  });
  it.each(["legacy", "raw-cut"])("retains %s final text without a complete message_end", async mode => {
    const { result } = await run(mode);
    expect(result.status).toBe("failed");
    expect(result.partialText).toBe(final);
    expect(result.text).toBe(final);
    expect(result.warnings).toContain(`${warning}: stream disconnected before completion`);
    expect(result.text).not.toContain("not output");
  });
  it("falls back to the preceding complete tool-turn assistant text", async () => {
    const { result } = await run("fallback");
    expect(result.status).toBe("failed");
    expect(result.partialText).toBe(previous);
    expect(result.lastCompleteText).toBe(previous);
    expect(result.text).toBe(previous);
    expect(result.warnings).toContain(`${warning}: stream disconnected before completion`);
  });
  it.each(["no-output", "tool-less", "unfinished-tool", "unfinished-turn", "no-text"])("preserves failure without completed tool work and output: %s", async mode => {
    const { result } = await run(mode);
    expect(result.status).toBe("failed");
    expect(result.error).toBe("stream disconnected before completion");
    expect(result.warnings ?? []).not.toContain(expect.stringContaining(warning));
    if (mode === "no-output") expect(result.text).toBe("");
  });
  it.each(["later-tool-start", "later-tool-start-no-text", "later-tool-delta", "later-tool-end", "later-tool-snapshot", "later-tool-use", "later-tool-execution", "later-parallel-tools"])("does not retain a cut unresolved tool-call turn after completed work: %s", async mode => {
    const { result } = await run(mode);
    expect(result).toMatchObject({ status: "failed", exitCode: 1, error: "stream disconnected before completion" });
    expect(result.lastCompleteText).toBe(mode === "later-tool-use" ? final : previous);
    expect(result.warnings ?? []).not.toContain(expect.stringContaining(warning));
    expect(result.text).not.toContain("not report text");
    if (mode === "later-tool-start-no-text") expect(result.partialText).toBeUndefined();
  });
  it("does not mask a deterministic provider error", async () => {
    const { result } = await run("partial", "400 invalid_request_error");
    expect(result.status).toBe("failed");
    expect(result.error).toBe("400 invalid_request_error");
  });
  it("keeps an interrupted structured request failed without replacing its native error", async () => {
    const { result } = await run("partial", "stream disconnected before completion", { replyTool: true, schema: { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] } });
    expect(result.status).toBe("failed");
    expect(result.error).toBe("stream disconnected before completion");
    expect(result.value).toBeUndefined();
    expect(result.partialText).toBe(final);
  });
  it("does not promote a durable structured reply when the report stream was cut", async () => {
    const { result } = await run("reply", "stream disconnected before completion", { replyTool: true, schema: { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] } });
    expect(result).toMatchObject({ status: "failed", exitCode: 1, partialText: final, error: "stream disconnected before completion" });
    expect(result.value).toBeUndefined();
    expect(result.replyVia).toBeUndefined();
    expect(result.warnings).toContain(`${warning}: stream disconnected before completion`);
  });
  it.each([{ actorId: "interrupted-report-actor" }, { actorName: "legacy-report-actor" }])("does not turn an actor report into success: %j", async request => {
    const { result } = await run("partial", "stream disconnected before completion", request);
    expect(result.status).toBe("failed");
    expect(result.text).toBe(final);
    expect(result.warnings ?? []).not.toContain(expect.stringContaining(warning));
  });
  it.each(["timeout", "stop"])("keeps %s authoritative after persisted text and tools", async mode => {
    const { result } = await run(mode, "stream disconnected before completion", { timeoutMs: 2000 });
    expect(result.status).toBe(mode === "timeout" ? "timed_out" : "stopped");
    expect(result.text).toBe(final);
    expect(result.warnings ?? []).not.toContain(expect.stringContaining(warning));
  });
  it("preserves a native zero exit code without accepting an interrupted report", async () => {
    const { result } = await run("error-exit-zero");
    expect(result).toMatchObject({ status: "failed", exitCode: 0, text: final, partialText: final, error: "stream disconnected before completion" });
    expect(result.warnings).toContain(`${warning}: stream disconnected before completion`);
  });
  it("leaves a normal final report unchanged", async () => {
    const { result } = await run("normal");
    expect(result).toMatchObject({ status: "completed", text: final, lastCompleteText: final, exitCode: 0 });
    expect(result.partialText).toBeUndefined();
    expect(result.error).toBeUndefined();
    expect(result.warnings).toBeUndefined();
  });
});

describe("terminal model stream error classification", () => {
  it.each(["Premature close", "stream disconnected", "stream closed before response.completed", "stream ended before a terminal response event", "server_is_overloaded"])("accepts %s", error => expect(interruptedModelStreamError(error)).toBe(true));
  it.each(["400 stream disconnected", "401 unauthorized", "billing exhausted: stream disconnected", "context exceeded: stream disconnected", "503 internal error", "connection refused", "Worker crashed", "Agent stopped"])("rejects %s", error => expect(interruptedModelStreamError(error)).toBe(false));
});
