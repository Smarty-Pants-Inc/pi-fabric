import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager, HOST_STOP_REASON } from "../src/agents/manager.js";
import { readStoppedRuns, STOPPED_AGENTS_ENTRY } from "../src/agents/stopped-runs.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const managers: AgentManager[] = [];
const roots: string[] = [];
const manager = (extra: ConstructorParameters<typeof AgentManager>[2] = {}) => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), "stopped-runs-test-"));
  roots.push(runRoot);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  const created = new AgentManager(
    process.cwd(),
    { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, transport: "process", sessionExport: false },
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot, ...extra },
  );
  managers.push(created);
  return created;
};
afterEach(async () => {
  await Promise.allSettled(managers.splice(0).map((created) => created.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const entry = (data: unknown) => ({ type: "custom", customType: STOPPED_AGENTS_ENTRY, data });

describe("task agents stopped by a reload (smarty-dev#1602)", () => {
  it("reports each run the close stopped, with the reason, last error and last event time", async () => {
    const onStoppedAtClose = vi.fn();
    const first = manager({ onStoppedAtClose });
    const handle = await first.spawn({ task: "HANG", runner: "pi", extensions: false });
    await first.close();
    expect(onStoppedAtClose).toHaveBeenCalledTimes(1);
    const [results] = onStoppedAtClose.mock.calls[0] as [AgentRunResult[]];
    expect(results).toHaveLength(1);
    expect(results[0]).toMatchObject({ id: handle.id, status: "stopped" });
    expect(results[0]!.error).toMatch(
      new RegExp(`^${HOST_STOP_REASON}; last error: Agent stopped; last event: (none|\\d{4}-\\d\\d-\\d\\dT)`),
    );
  });

  it("answers wait, status and steer for a previous runtime's run from its record", async () => {
    const stopped = {
      id: "old-run", name: "worker", task: "", status: "stopped", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, text: "",
      error: `${HOST_STOP_REASON}; last error: Agent stopped; last event: 1970-01-01T00:00:00.002Z`,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    } as AgentRunResult;
    const { runs } = readStoppedRuns([entry({ stopped: [stopped] })]);
    const onResultConsumed = vi.fn();
    const next = manager({ onResultConsumed });
    await expect(next.wait("old-run")).rejects.toThrow("Unknown Fabric agent");
    next.restorePreviousRuns(runs);
    await expect(next.wait("old-run")).resolves.toMatchObject({ status: "stopped", error: stopped.error });
    expect(onResultConsumed).toHaveBeenCalledWith("old-run");
    expect(next.status("old-run")).toMatchObject({ status: "stopped", error: stopped.error });
    expect(() => next.steer("old-run", "hi")).toThrow(/is stopped: stopped by host reload\/shutdown/);
    expect(() => next.steer("old-run", "hi")).not.toThrow(/Unknown Fabric agent/);
    await expect(next.stop("old-run")).resolves.toMatchObject({ status: "stopped" });
    await expect(next.wait("never-seen")).rejects.toThrow("Unknown Fabric agent");
  });

  it("reads back stopped runs from the session and skips delivered ones", () => {
    const run = (id: string) => ({ id, status: "stopped" }) as AgentRunResult;
    const read = readStoppedRuns([
      { type: "custom", customType: "other", data: { stopped: [run("x")] } },
      entry({ stopped: [run("a"), run("b")] }),
      entry({ delivered: ["a"] }),
      entry({ stopped: [run("c")] }),
    ]);
    expect(read.runs.map((r) => r.id)).toEqual(["a", "b", "c"]);
    expect(read.undelivered.map((r) => r.id)).toEqual(["b", "c"]);
  });
});
