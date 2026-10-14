import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// Every product removal is recorded, never executed (#4664). The detached
// sweep is recorded at admission too, so a child cannot escape the fs spies.
const removalIntents = vi.hoisted(() => [] as string[]);
const sweepClaims = vi.hoisted(() => [] as string[]);
vi.mock("../src/agents/rm.js", () => ({
  removeTree: vi.fn(async (target: string) => { removalIntents.push(target); }),
}));
vi.mock("../src/storage/verified-remove.js", async original => ({
  ...await original<typeof import("../src/storage/verified-remove.js")>(),
  verifiedRemove: vi.fn((target: string) => {
    removalIntents.push(target);
    return { removed: true };
  }),
}));
vi.mock("../src/storage/retention.js", async original => ({
  ...await original<typeof import("../src/storage/retention.js")>(),
  claimTempRunSweep: vi.fn((root: string) => { sweepClaims.push(root); return false; }),
}));

import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

const managers: AgentManager[] = [];
const fsRemovals: string[] = [];

const recordFsRemovals = (): void => {
  vi.spyOn(fs, "rmSync").mockImplementation(file => { fsRemovals.push(String(file)); });
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => { fsRemovals.push(String(file)); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(file => { fsRemovals.push(String(file)); });
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { fsRemovals.push(String(file)); });
};

/** Sweep-owned managed temp root (no explicit runRoot) under the private test TMPDIR. */
const managedFixture = () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agent-retained-result-"));
  vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
  vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  for (const key of ["PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, sessionExport: false, retainRuns: false,
  }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  managers.push(manager);
  const root = path.join(tempRoot, fs.readdirSync(tempRoot).find(name => name.startsWith("pi-fabric-runs-"))!);
  return { manager, root };
};

const readJson = (file: string): AgentRunRecord => JSON.parse(fs.readFileSync(file, "utf8")) as AgentRunRecord;

afterEach(async () => {
  const outcomes = await Promise.allSettled(managers.splice(0).map(async manager => {
    manager.config.retainRuns = true;
    await manager.close();
  }));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  removalIntents.splice(0);
  fsRemovals.splice(0);
  sweepClaims.splice(0);
  expect(outcomes.filter(outcome => outcome.status === "rejected")).toEqual([]);
});

describe("retained manager outcome at close", () => {
  it("retains the exact taskless manager result when host validation failed but the worker later wrote completed", async () => {
    recordFsRemovals();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 32_000, true));
    const { manager, root } = managedFixture();
    const failedHandle = await manager.spawn({ task: "host validation private brief", transport: "process", extensions: false,
      schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
    const validated = await manager.wait(failedHandle.id, { timeoutMs: 2_000 });
    expect(validated.status).toBe("failed");
    expect(validated.error).toMatch(/Structured agent output was invalid/);
    const run = manager.runDirectory(failedHandle.id)!;
    expect(path.dirname(run)).toBe(root);
    const managerResultFile = path.join(run, "manager-result.json");
    const statusFile = path.join(run, "status.json");
    const taskFile = path.join(run, "task.txt");
    const exact = readJson(managerResultFile);
    expect(exact).toMatchObject({ status: "failed", task: "host validation private brief" });
    expect((exact.value as { output: string }).output).toHaveLength(32_000);
    expect(exact.text).toHaveLength(32_000);
    // The worker's late completed status must not govern retention or public reads.
    writeJsonAtomic(statusFile, { ...validated, status: "completed", error: undefined, text: "late success",
      value: { output: true }, exitCode: 0, finishedAt: Date.now() + 1_000, updatedAt: Date.now() + 1_000 });
    expect(manager.status(failedHandle.id)).toMatchObject({ status: "failed" });
    expect(await manager.wait(failedHandle.id)).toMatchObject({ status: "failed" });

    // Success counterexample: whole-tree collection intent still occurs.
    const successHandle = await manager.spawn({ task: "plain success", transport: "process", extensions: false });
    expect(await manager.wait(successHandle.id, { timeoutMs: 2_000 })).toMatchObject({ status: "completed" });
    const successRun = manager.runDirectory(successHandle.id)!;

    await manager.close();
    expect(removalIntents).toContain(successRun);
    const protectedPaths = [run, taskFile, statusFile, managerResultFile];
    for (const target of [...removalIntents, ...fsRemovals]) {
      expect(protectedPaths.some(file => file === target || file.startsWith(target + path.sep))).toBe(false);
    }
    expect(sweepClaims).toContain(path.dirname(root));

    const { task: _task, ...taskless } = exact;
    const retained = readJson(managerResultFile);
    expect(retained).toEqual(taskless);
    expect(retained).not.toHaveProperty("task");
    expect(retained.status).toBe("failed");
    expect(retained.error).toBe(exact.error);
    expect(retained.text).toHaveLength(32_000);
    expect((retained.value as { output: string }).output).toHaveLength(32_000);
    const status = readJson(statusFile);
    expect(status).toMatchObject({ id: failedHandle.id, status: "failed" });
    expect(status).not.toHaveProperty("task");
    expect(status.text.length).toBeLessThan(32_000);
    expect(manager.status(failedHandle.id)).toMatchObject({ status: "failed" });
    expect(await manager.wait(failedHandle.id)).toMatchObject({ status: "failed" });
  });

  it("keeps bounded terminal facts readable after recorded close collection, but rejects an absent result before close", async () => {
    recordFsRemovals();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
    const { manager } = managedFixture();
    const result = await manager.run({ task: "collected result", transport: "process", extensions: false });
    const run = manager.runDirectory(result.id)!;
    const file = path.join(run, "manager-result.json");
    const read = fs.readFileSync;
    const exists = fs.existsSync;
    const hide = () => {
      vi.spyOn(fs, "readFileSync").mockImplementation(((...args: unknown[]) => {
        if (String(args[0]) === file) throw Object.assign(new Error("recorded collection"), { code: "ENOENT" });
        return Reflect.apply(read, fs, args);
      }) as typeof fs.readFileSync);
      vi.spyOn(fs, "existsSync").mockImplementation(target => String(target) === file ? false : exists(target));
    };
    hide();
    expect(() => manager.status(result.id)).toThrow(/no saved terminal result/);
    vi.mocked(fs.readFileSync).mockRestore();
    vi.mocked(fs.existsSync).mockRestore();
    await manager.close();
    expect(removalIntents).toContain(run);
    hide(); // Model the recorded tree removal through reads, never physical deletion.
    expect(manager.status(result.id)).toMatchObject({ status: "completed", task: expect.any(String) });
    const after = await manager.wait(result.id);
    expect(after.status).toBe("completed");
    expect(after.warnings).toContain("Run files were collected at close; this is the manager's bounded terminal summary, not the exact result");
    expect(await manager.stop(result.id)).toMatchObject({ status: "completed" });
    vi.mocked(fs.readFileSync).mockRestore();
    vi.mocked(fs.existsSync).mockRestore();
  });
});
