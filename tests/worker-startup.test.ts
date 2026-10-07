import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { assertWorkerRuntime, writeWorkerStartupFailure } from "../src/worker/startup.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("worker bootstrap diagnostics", () => {
  it("rejects old Node without rejecting Bun's compatibility version", () => {
    expect(() => assertWorkerRuntime({ node: "22.22.2" })).toThrow("Node.js 24+");
    expect(() => assertWorkerRuntime({ node: "24.0.0" })).not.toThrow();
    expect(() => assertWorkerRuntime({ node: "26.5.0" })).not.toThrow();
    expect(() => assertWorkerRuntime({ node: "22.0.0", bun: "1.3.0" })).not.toThrow();
  });

  it("writes bounded owner-only diagnostics and retains resumed usage even with invalid options", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-startup-"));
    roots.push(root);
    const file = path.join(root, "status.json");
    const carry = { turns: 3, toolCalls: 2, usage: { input: 10, output: 20, cacheRead: 30, cacheWrite: 40, cost: 0.1 } };
    writeWorkerStartupFailure(["node", "worker.js", "--id", "probe", "--status-file", file, "--task-file", path.join(root, "missing"), "--carry-over", JSON.stringify(carry)], new Error("missing module " + "x".repeat(30000)));
    const record = JSON.parse(fs.readFileSync(file, "utf8"));
    expect(record).toMatchObject({ id: "probe", status: "failed", task: "", ...carry });
    expect(record.error).toContain("missing module");
    expect(record.error.length).toBeLessThanOrEqual(20000);
    expect(record.stderr.length).toBeLessThanOrEqual(20000);
    expect(record.finishedAt).toBeGreaterThanOrEqual(record.startedAt);
    if (process.platform !== "win32") expect(fs.statSync(file).mode & 0o777).toBe(0o600);
  });

  it("does not invent a destination when no status path is available", () => {
    expect(() => writeWorkerStartupFailure(["node", "worker.js"], new Error("startup"))).not.toThrow();
  });
});
