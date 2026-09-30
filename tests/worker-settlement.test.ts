import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { readProcessIdentity, type ProcessIdentity } from "../src/core/process-identity.js";
import { assertRunProcessesSettled, hasUnsettledRecordedProcesses, readRunProcessEvidence, recordWorkerLaunchAttempt } from "../src/storage/worker-settlement.js";
import { canRemoveManagedRunRoot, markRunRootClosed, markUnresolvedWorker, pruneActorRunArchives, sweepTempRunRoots } from "../src/storage/retention.js";
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
const journal = (dir: string, worker: ProcessIdentity | null, runner: ProcessIdentity | null) => {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ id: path.basename(dir), actorId: "actor", status: "completed", finishedAt: 1 }));
  const launchFile = path.join(dir, "worker-launches.jsonl");
  const attempts = fs.existsSync(launchFile)
    ? fs.readFileSync(launchFile, "utf8").trim().split("\n").map(line => JSON.parse(line).attempt as string)
    : [undefined];
  fs.writeFileSync(path.join(dir, "worker-processes.jsonl"), attempts.flatMap(attempt => [
    JSON.stringify({ ...(attempt ? { attempt } : {}), worker }),
    JSON.stringify({ ...(attempt ? { attempt } : {}), worker, runner }),
  ]).join("\n") + "\n");
};

describe.skipIf(process.platform !== "linux")("worker settlement evidence preservation", () => {
  it.each(["worker spawn", "runner spawn"])("a crash after %s but before registration cannot reuse earlier settled evidence", kind => {
    const { root, dead } = setup();
    const dir = path.join(root, "run");
    journal(dir, dead, dead);
    recordWorkerLaunchAttempt(dir, "replacement");
    if (kind === "runner spawn") fs.appendFileSync(path.join(dir, "worker-processes.jsonl"),
      JSON.stringify({ attempt: "replacement", worker: dead }) + "\n");
    expect(() => readRunProcessEvidence(root)).toThrow(kind === "worker spawn" ? "Unregistered worker launch attempt" : "Missing runner launch evidence");
    expect(hasUnsettledRecordedProcesses(dir)).toBe(true);
    fs.appendFileSync(path.join(dir, "worker-processes.jsonl"), JSON.stringify({ attempt: "replacement", worker: dead, runner: dead }) + "\n");
    expect(() => assertRunProcessesSettled(readRunProcessEvidence(root))).not.toThrow();
    expect(hasUnsettledRecordedProcesses(dir)).toBe(false);
  });
  it("requires evidence for every pre-spawn attempt rather than just a complete later attempt", () => {
    const { root, dead } = setup();
    const dir = path.join(root, "run");
    journal(dir, dead, dead);
    recordWorkerLaunchAttempt(dir, "unregistered");
    recordWorkerLaunchAttempt(dir, "later");
    fs.appendFileSync(path.join(dir, "worker-processes.jsonl"), JSON.stringify({ attempt: "later", worker: dead, runner: dead }) + "\n");
    expect(() => readRunProcessEvidence(root)).toThrow("Unregistered worker launch attempt unregistered");
  });
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

// Run on every native CI host. Inject only at cleanup time so the owned worker
// uses the real host's spawn/exit transport; its null journal matches a real non-Linux worker.
describe.each(["win32", "darwin"] as const)("%s ordinary owned cleanup without kernel evidence", platformName => {
  const onPlatform = async (check: () => void | Promise<void>) => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: platformName });
    try { await check(); } finally { Object.defineProperty(process, "platform", platform); }
  };
  const ownedRun = async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-settlement-portable-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: false, sessionExport: false },
      { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    managers.push(manager);
    const result = await manager.run({ task: "complete", runner: "pi", extensions: false });
    expect(result.status).toBe("completed");
    const dir = manager.runDirectory(result.id)!;
    fs.writeFileSync(path.join(dir, "worker-processes.jsonl"), '{"worker":null}\n{"worker":null,"runner":null}\n');
    return { root, manager, result, dir };
  };
  it("cleans a completed owned worker with a null process journal", async () => {
    const { manager, result, dir } = await ownedRun();
    await onPlatform(async () => {
      expect(hasUnsettledRecordedProcesses(dir)).toBe(false);
      await expect(manager.cleanup(result.id)).resolves.toEqual({ cleaned: true });
      expect(fs.existsSync(dir)).toBe(false);
    });
  });
  it("close with retainRuns false removes normally settled owned runs", async () => {
    const { root, manager } = await ownedRun();
    await onPlatform(async () => {
      await manager.close();
      expect(fs.existsSync(root)).toBe(false);
    });
  });
  it("expires archives and closed temp roots with null worker and nested runner journals", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "worker-retention-portable-"));
    roots.push(root);
    const archiveRoot = path.join(root, "archives");
    const archive = path.join(archiveRoot, "run");
    journal(archive, null, null);
    journal(path.join(archive, "nested", "child"), null, null);
    const temp = path.join(root, "pi-fabric-runs-portable");
    journal(path.join(temp, "run"), null, null);
    markRunRootClosed(temp, 1, true);
    await onPlatform(() => {
      expect(pruneActorRunArchives({ runsDirectory: archiveRoot, retentionMs: 1, now: 1000 })).toEqual([archive]);
      expect(canRemoveManagedRunRoot(temp)).toBe(true);
      expect(sweepTempRunRoots({ tempRoot: root, orphanedTempRunRetentionMs: 1, oneShotRunRetentionMs: 1, now: 1000 }))
        .toEqual({ removedRoots: [temp], removedRuns: [path.join(temp, "run")] });
    });
  });
  it("still preserves unresolved owned workers on cleanup, close and retention", async () => {
    const { root, manager, result, dir } = await ownedRun();
    markUnresolvedWorker(dir, "unconfirmed transport exit");
    await onPlatform(async () => {
      await expect(manager.cleanup(result.id)).rejects.toThrow("worker/runner");
      await manager.close();
      expect(fs.existsSync(dir)).toBe(true);
      expect(pruneActorRunArchives({ runsDirectory: root, retentionMs: 1, now: Date.now() + 1000 })).toEqual([]);
    });
  });
});
