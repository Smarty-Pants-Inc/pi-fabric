import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { directiveSchema } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// Source by default for a regression against HEAD before building; the same
// assertions also run against dist via FABRIC_EXIT_GRACE_WORKER.
const workerPath = path.resolve(process.env.FABRIC_EXIT_GRACE_WORKER ?? "src/worker.ts");
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
const run = async (behavior: string, schema?: Record<string, unknown>, replyTool = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-exit-grace-"));
  roots.push(root);
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000 }, {
    workerPath, piBinary: path.resolve("tests/fixtures/fake-pi-exit-grace.mjs"), runRoot: root,
  });
  managers.push(manager);
  // The fake child's signal handler reads the durable record in its cwd.
  const result = await manager.run({ task: behavior, cwd: root, transport: "process", ...(schema ? { schema } : {}), ...(replyTool ? { replyTool } : {}) });
  return { manager, result, root };
};
const readEvents = (logFile: string) => fs.readFileSync(logFile, "utf8").trim().split("\n").map(line => JSON.parse(line));
const isRunning = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    // A killed orphan may await init's reap on Linux; a zombie cannot run.
    if (process.platform === "linux") return !/^\d+ \(.*\) Z /.test(fs.readFileSync(`/proc/${pid}/stat`, "utf8"));
    return true;
  } catch { return false; }
};

describe("settled Pi exit grace", () => {
  it.each(["slow-exit", "never-exit"])("preserves the final result and warns once for %s", async behavior => {
    const { manager, result } = await run(behavior);
    expect(result.status).toBe("completed");
    expect(result.text).toBe("durable final result");
    expect(result.error).toBeUndefined();
    expect(result.warnings).toEqual([expect.stringContaining("did not exit after stdin closed for 5000ms")]);
    const events = readEvents(result.logFile!);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([
      { type: "agent_settled", ...(behavior === "never-exit" ? {} : { outcome: "completed" }) },
    ]);
    expect(events.filter(event => event.type === "worker_warning")).toHaveLength(1);
    expect(events.some(event => event.type === "fabric_recovery_error")).toBe(false);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);
    const snapshot = events.find(event => event.type === "fake_term_snapshot");
    // Do not publish terminal status until pipe draining and validation finish:
    // the manager can immediately retain/remove terminal run artifacts.
    // Windows SIGTERM is unconditional termination; there is no handler probe.
    if (process.platform !== "win32") {
      expect(snapshot).toMatchObject({ status: "running", text: result.text, warnings: result.warnings });
    }
    const durable = JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "status.json"), "utf8"));
    expect(durable).toMatchObject({ status: "completed", text: result.text, warnings: result.warnings });
    expect(manager.listForUi()[0]).toMatchObject({ status: "completed" });
    for (const event of events.filter(event => ["fake_child_pid", "fake_descendant_pid"].includes(event.type))) {
      expect(isRunning(event.pid)).toBe(false);
    }
    if (behavior === "never-exit" && process.platform !== "win32") {
      expect(events.filter(event => event.type === "fake_descendant_pid")).toHaveLength(1);
    }
  }, 45_000);

  it.each([
    ["settle-error", "error", false],
    ["settle-aborted", "aborted", false],
    ["reply-settle-error", "error", true],
    ["reply-settle-aborted", "aborted", true],
  ] as const)("keeps unsuccessful settlement failed after slow exit for %s", async (behavior, outcome, replyTool) => {
    const { manager, result } = await run(behavior, replyTool ? directiveSchema : undefined, replyTool);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("did not exit after stdin closed for 5000ms");
    expect(result.text).toBe(replyTool ? "" : "durable final result");
    expect(result.value).toBeUndefined();
    expect(result.replyVia).toBeUndefined();
    expect(result.warnings ?? []).toEqual([]);
    const events = readEvents(result.logFile!);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([{ type: "agent_settled", outcome }]);
    expect(events.find(event => event.type === "compaction_end")).toMatchObject({
      reason: "threshold", aborted: outcome === "aborted", willRetry: false,
      ...(outcome === "error" ? { errorMessage: "Auto-compaction failed: fixture failure" } : {}),
    });
    expect(events.filter(event => event.type === "message_end" && event.message.role === "assistant")
      .every(event => !["error", "aborted"].includes(event.message.stopReason))).toBe(true);
    if (replyTool) {
      expect(JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "reply.json"), "utf8"))).toEqual({ action: "silent" });
    }
    expect(events.filter(event => event.type === "worker_warning")).toHaveLength(0);
    expect(events.filter(event => event.type === "fabric_recovery_error")).toHaveLength(1);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);
    const durable = JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "status.json"), "utf8"));
    expect(durable.status).toBe("failed");
    expect(manager.listForUi()[0]).toMatchObject({ status: "failed" });
    for (const event of events.filter(event => event.type === "fake_child_pid")) expect(isRunning(event.pid)).toBe(false);
  }, 45_000);

  it.each(["crash", "crash-before-settle"])("keeps %s failed even if text was recorded", async behavior => {
    const { result } = await run(behavior);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Pi exited with code 1");
    expect(result.warnings ?? []).toEqual([]);
  });

  it("keeps a process task that never settles failed despite recorded text and an 8s lifetime", async () => {
    const { manager, result } = await run("never-settles");
    expect(result.status).toBe("failed");
    expect(result.text).toBe("durable final result");
    expect(result.error).toContain("Pi exited with code 1");
    expect(result.warnings ?? []).toEqual([]);
    const events = readEvents(result.logFile!);
    expect(events.some(event => event.type === "agent_settled")).toBe(false);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(false);
    expect(fs.existsSync(path.join(path.dirname(result.logFile!), "settlement.json"))).toBe(false);
    expect(manager.listForUi()[0]).toMatchObject({status: "failed"});
    for (const event of events.filter(event => event.type === "fake_child_pid")) expect(isRunning(event.pid)).toBe(false);
  }, 45_000);

  it("still validates a structured result after slow-exit cleanup", async () => {
    const { result } = await run("slow-exit", { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] });
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(/^Structured agent output was invalid:/);
    expect(result.text).toBe("durable final result");
    expect(result.warnings).toHaveLength(1);
  }, 45_000);

  it.each(["reply-slow-exit", "reply-after-settle"])("preserves a tool-only reply written after assistant consumption for %s", async behavior => {
    const { result } = await run(behavior, directiveSchema, true);
    expect(result).toMatchObject({ status: "completed", value: { action: "silent" }, replyVia: "tool", text: "" });
    expect(result.error).toBeUndefined();
    expect(result.warnings).toEqual([expect.stringContaining("did not exit after stdin closed for 5000ms")]);
    const events = readEvents(result.logFile!);
    expect(events.filter(event => event.type === "fake_assistant_consumed")).toEqual([
      { type: "fake_assistant_consumed", replyExists: false },
    ]);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([{ type: "agent_settled", outcome: "completed" }]);
    expect(events.filter(event => event.type === "worker_warning")).toHaveLength(1);
    expect(events.some(event => event.type === "fabric_recovery_error")).toBe(false);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);
    const durable = JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "status.json"), "utf8"));
    expect(durable).toMatchObject({ status: "completed", value: result.value, warnings: result.warnings });
    for (const event of events.filter(event => event.type === "fake_child_pid")) expect(isRunning(event.pid)).toBe(false);
  }, 45_000);

  it.each(["empty-slow-exit", "empty-never-exit"])("keeps an explicitly successful empty settlement durable through %s", async behavior => {
    const { result } = await run(behavior);
    expect(result).toMatchObject({ status: "completed", text: "" });
    expect(result.error).toBeUndefined();
    expect(result.warnings).toHaveLength(1);
    const events = readEvents(result.logFile!);
    const receipt = { version: 1, runId: result.id, outcome: "completed", text: "" };
    // Native success was fsynced before EOF, not inferred from child exit or text.
    expect(events.find(event => event.type === "fake_stdin_eof")).toMatchObject({ receipt });
    if (process.platform !== "win32") expect(events.find(event => event.type === "fake_term_snapshot"))
      .toMatchObject({ status: "running", receipt });
    expect(JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "settlement.json"), "utf8"))).toEqual(receipt);
    for (const event of events.filter(event => ["fake_child_pid", "fake_descendant_pid"].includes(event.type))) {
      expect(isRunning(event.pid)).toBe(false);
    }
  }, 45_000);

  it("fails closed when the explicit-success receipt cannot be persisted", async () => {
    const { result } = await run("receipt-unwritable");
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Cannot persist Pi settlement receipt");
    expect(result.warnings ?? []).toEqual([]);
  }, 45_000);

  it("keeps a missing tool-only reply failed after slow exit", async () => {
    const { result } = await run("reply-missing", directiveSchema, true);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Directive reply missing");
    expect(result.value).toBeUndefined();
    expect(result.warnings).toHaveLength(1);
  }, 45_000);

  it.each([
    ["reply-invalid", /^Structured agent output was invalid:/],
    ["reply-malformed", /^Directive reply missing:/],
  ] as const)("keeps %s failed during post-drain reply validation", async (behavior, error) => {
    const { result } = await run(behavior, directiveSchema, true);
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(error);
    expect(result.warnings).toHaveLength(1);
  }, 45_000);

  it("does not promote a settled child without a final result", async () => {
    const { result } = await run("settled-no-result");
    expect(result.status).toBe("failed");
    expect(result.error).toContain("did not exit after stdin closed");
    expect(result.warnings ?? []).toEqual([]);
  }, 45_000);
});
