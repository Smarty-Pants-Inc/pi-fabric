import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { readProcessIdentity, type ProcessIdentity } from "../src/core/process-identity.js";
import { assertRunProcessesSettled, hasUnsettledRecordedProcesses, readRunProcessEvidence } from "../src/storage/worker-settlement.js";
import { pruneActorRunArchives } from "../src/storage/retention.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  for (const manager of managers.splice(0)) await manager.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-settlement-"));
  roots.push(root);
  const live = readProcessIdentity()!;
  const dead = { ...live, pid: 2147483647, startTime: "1", commandLine: "recorded-dead-worker\0" };
  return { root, live, dead };
};
const journal = (dir: string, worker: ProcessIdentity, runner: ProcessIdentity) => {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ id: path.basename(dir), actorId: "actor", status: "completed", finishedAt: 1 }));
  fs.writeFileSync(path.join(dir, "worker-processes.jsonl"), JSON.stringify({ worker }) + "\n" + JSON.stringify({ worker, runner }) + "\n");
};

describe.skipIf(process.platform !== "linux")("worker settlement evidence preservation", () => {
  it("requires all launch attempts to settle, not just the last worker", () => {
    const { root, live, dead } = setup();
    const dir = path.join(root, "run");
    journal(dir, dead, live);
    fs.appendFileSync(path.join(dir, "worker-processes.jsonl"), JSON.stringify({ worker: { ...dead, pid: 2147483646 }, runner: dead }) + "\n");
    expect(() => assertRunProcessesSettled(readRunProcessEvidence(root))).toThrow("settlement not proven");
    expect(hasUnsettledRecordedProcesses(dir)).toBe(true);
    journal(dir, dead, dead);
    expect(() => assertRunProcessesSettled(readRunProcessEvidence(root))).not.toThrow();
    expect(hasUnsettledRecordedProcesses(dir)).toBe(false);
  });
  it.each(["missing runner", "unknown kernel", "malformed journal", "unresolved", "missing nested journal"])("preserves %s evidence", kind => {
    const { root, dead } = setup();
    const dir = path.join(root, "run");
    journal(dir, dead, dead);
    if (kind === "missing runner") fs.writeFileSync(path.join(dir, "worker-processes.jsonl"), JSON.stringify({ worker: dead }) + "\n");
    if (kind === "unknown kernel") journal(dir, dead, { ...dead, kernelId: "00000000-0000-0000-0000-000000000000/pid:[123]" });
    if (kind === "malformed journal") fs.writeFileSync(path.join(dir, "worker-processes.jsonl"), "broken\n");
    if (kind === "unresolved") fs.writeFileSync(path.join(dir, "unresolved-worker.json"), "{}");
    if (kind === "missing nested journal") {
      const nested = path.join(dir, "nested", "child");
      journal(nested, dead, dead);
      fs.rmSync(path.join(nested, "worker-processes.jsonl"));
    }
    expect(hasUnsettledRecordedProcesses(dir)).toBe(true);
  });
  it("retention keeps a terminal parent archive while its nested runner lives, then expires it", () => {
    const { root, live, dead } = setup();
    const dir = path.join(root, "parent");
    const nested = path.join(dir, "nested", "child");
    journal(dir, dead, dead);
    journal(nested, dead, live);
    const prune = () => pruneActorRunArchives({ runsDirectory: root, retentionMs: 1, now: 1000 });
    expect(prune()).toEqual([]);
    expect(fs.existsSync(nested)).toBe(true);
    journal(nested, dead, dead);
    expect(prune()).toEqual([dir]);
  });
  it("owned cleanup keeps dead-worker/live-runner files, then cleans only after positive settlement", async () => {
    const { root, live, dead } = setup();
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: false, sessionExport: false },
      { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    managers.push(manager);
    const result = await manager.run({ task: "complete", runner: "pi", extensions: false });
    const dir = manager.runDirectory(result.id)!;
    journal(dir, dead, live);
    await expect(manager.cleanup(result.id)).rejects.toThrow("worker/runner");
    expect(fs.existsSync(dir)).toBe(true);
    journal(dir, dead, dead);
    await expect(manager.cleanup(result.id)).resolves.toEqual({ cleaned: true });
    expect(fs.existsSync(dir)).toBe(false);
  });
  it("owned close does not erase a dead worker's live nested runner evidence", async () => {
    const { root, live, dead } = setup();
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: false, sessionExport: false },
      { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    managers.push(manager);
    const result = await manager.run({ task: "complete", runner: "pi", extensions: false });
    const dir = manager.runDirectory(result.id)!;
    journal(dir, dead, dead);
    journal(path.join(dir, "nested", "child"), dead, live);
    await manager.close();
    expect(fs.existsSync(path.join(dir, "nested", "child", "worker-processes.jsonl"))).toBe(true);
  });
});
