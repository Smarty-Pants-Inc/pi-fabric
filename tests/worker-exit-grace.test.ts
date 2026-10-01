import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
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
const run = async (behavior: string, schema?: Record<string, unknown>) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-exit-grace-"));
  roots.push(root);
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000 }, {
    workerPath, piBinary: path.resolve("tests/fixtures/fake-pi-exit-grace.mjs"), runRoot: root,
  });
  managers.push(manager);
  // The fake child's signal handler reads the durable record in its cwd.
  const result = await manager.run({ task: behavior, cwd: root, transport: "process", ...(schema ? { schema } : {}) });
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

  it.each(["crash", "crash-before-settle"])("keeps %s failed even if text was recorded", async behavior => {
    const { result } = await run(behavior);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Pi exited with code 1");
    expect(result.warnings ?? []).toEqual([]);
  });

  it("still validates a structured result after slow-exit cleanup", async () => {
    const { result } = await run("slow-exit", { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] });
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(/^Structured agent output was invalid:/);
    expect(result.text).toBe("durable final result");
    expect(result.warnings).toHaveLength(1);
  }, 45_000);

  it("does not promote a settled child without a final result", async () => {
    const { result } = await run("settled-no-result");
    expect(result.status).toBe("failed");
    expect(result.error).toContain("did not exit after stdin closed");
    expect(result.warnings ?? []).toEqual([]);
  }, 45_000);
});
