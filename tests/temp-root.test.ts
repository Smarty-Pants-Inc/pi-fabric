import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { activeBudgetState } from "../src/agents/budget-ledger.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { FabricShellJobStore } from "../src/core/shell-jobs.js";
import { MeshStore } from "../src/mesh/store.js";
import { boundModelOutput, OutputArtifactStore } from "../src/output-budget.js";
import { closeScratch, createScratch, ScratchScope, SCRATCH_OWNER_FILE, sweepScratch } from "../src/storage/scratch.js";
import { fabricDataRoot } from "../src/storage/temp-root.js";
import { sweepTempRunRoots } from "../src/storage/retention.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
const sandbox = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-root-test-"));
  roots.push(root);
  const short = path.join(root, "short");
  const disk = path.join(root, "disk");
  fs.mkdirSync(short);
  if (process.platform === "win32") fs.mkdirSync(disk);
  vi.spyOn(os, "tmpdir").mockReturnValue(process.platform === "win32" ? disk : short);
  vi.stubEnv("PI_FABRIC_TMPDIR", process.platform === "win32" ? undefined : disk);
  for (const name of ["PI_FABRIC_RUN_ROOT", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(name, undefined);
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  return { root, short, disk };
};
const manager = (retainRuns = false) => {
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 1, retainRuns, transport: "process", sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    sweepPath: path.resolve("dist/storage/sweep-main.js"),
  });
  closers.push(() => agents.close());
  return agents;
};

afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(process.platform === "win32")("Fabric file root override", () => {
  it("requires an absolute override, creates it privately, and otherwise uses OS temp", () => {
    const { short, disk } = sandbox();
    expect(fabricDataRoot()).toBe(disk);
    if (process.platform !== "win32") expect(fs.statSync(disk).mode & 0o777).toBe(0o700);
    vi.stubEnv("PI_FABRIC_TMPDIR", "relative-root");
    expect(() => fabricDataRoot()).toThrow("absolute");
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
    expect(fabricDataRoot()).toBe(short);
    vi.stubEnv("PI_FABRIC_TMPDIR", "");
    expect(fabricDataRoot()).toBe(short);
  });

  it("places run, actor, budget, shell and output roots on disk and releases them after shutdown", async () => {
    const { root, short, disk } = sandbox();
    const agents = manager();
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const actors = new ActorManager("test", { id: "session:test", name: "main", kind: "main", sessionId: "test" }, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: false }, agents, () => {});
    closers.push(() => actors.close());
    const jobs = new FabricShellJobStore();
    closers.push(() => jobs.close());
    const outputs = new OutputArtifactStore();
    closers.push(() => outputs.close());
    const job = jobs.begin("bash", "echo test");
    job.append(Buffer.from("shell data"));
    job.spill();
    const log = await job.persistLog();
    await job.finish(0);
    const output = await boundModelOutput("x".repeat(2000), 1000, undefined, outputs.write);
    const run = await agents.run({ task: "hello", runner: "pi", extensions: false });
    expect(run.status).toBe("completed");
    const budget = activeBudgetState()!;
    for (const file of [agents.readLog(run.id).runDirectory, budget.file, log, output.artifactPath!]) expect(file.startsWith(disk + path.sep)).toBe(true);
    for (const prefix of ["runs", "actors", "budget", "shell", "output"]) expect(fs.readdirSync(disk).some(name => name.startsWith(`pi-fabric-${prefix}-`))).toBe(true);
    expect(fs.readdirSync(short)).toEqual([]);
    // Finished logs/artifacts are still readable for the live session.
    expect(fs.readFileSync(log, "utf8")).toContain("shell data");
    expect(fs.readFileSync(output.artifactPath!, "utf8")).toHaveLength(2000);
    await actors.close();
    await agents.close();
    await jobs.close();
    await outputs.close();
    expect(fs.readdirSync(disk).filter(name => name.startsWith("pi-fabric-"))).toEqual([]);
    expect(fs.readdirSync(short)).toEqual([]);
  });

  it("keeps retained runs on close and sweeps an abandoned disk-root owner, never a live owner", async () => {
    const { disk, short } = sandbox();
    const agents = manager(true);
    const run = await agents.run({ task: "retained", extensions: false });
    const runRoot = path.dirname(agents.readLog(run.id).runDirectory);
    await agents.close();
    expect(fs.existsSync(agents.readLog(run.id).runDirectory)).toBe(true);
    const dead = fs.mkdtempSync(path.join(disk, "pi-fabric-runs-"));
    const live = fs.mkdtempSync(path.join(disk, "pi-fabric-runs-"));
    for (const [directory, pid] of [[dead, 2147483647], [live, process.pid]] as const) {
      fs.writeFileSync(path.join(directory, ".fabric-owner.json"), JSON.stringify({ pid, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
      fs.mkdirSync(path.join(directory, "run"));
      fs.writeFileSync(path.join(directory, "run", "status.json"), JSON.stringify({ status: "completed", finishedAt: 1 }));
    }
    const result = sweepTempRunRoots({ tempRoot: disk, currentRoot: runRoot, now: 100_000, orphanedTempRunRetentionMs: 1000, oneShotRunRetentionMs: 1000 });
    expect(result.removedRoots).toEqual([dead]);
    expect(fs.existsSync(live)).toBe(true);
    const scratch = createScratch("output");
    fs.writeFileSync(path.join(scratch, "output.txt"), "abandoned");
    fs.writeFileSync(path.join(scratch, SCRATCH_OWNER_FILE), JSON.stringify({ app: "pi-fabric-scratch", version: 1, kind: "output", pid: 2147483647, createdAt: 1, orphanedAt: 1 }));
    expect((await sweepScratch({ tempRoot: disk, now: 100_000, orphanGraceMs: 1000 })).removed).toEqual([scratch]);
    expect(fs.readdirSync(short)).toEqual([]);
  });

  it("starts its detached sweep in the allocated disk root, even if the override later changes", async () => {
    const { disk, short } = sandbox();
    const agents = manager();
    const dead = fs.mkdtempSync(path.join(disk, "pi-fabric-runs-"));
    fs.writeFileSync(path.join(dead, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    vi.stubEnv("PI_FABRIC_TMPDIR", short);
    await agents.close();
    await vi.waitFor(() => expect(fs.existsSync(dead)).toBe(false), { timeout: 20_000 });
    expect(fs.readdirSync(short)).toEqual([]);
    expect(fs.existsSync(path.join(disk, ".pi-fabric-runs-sweep.json"))).toBe(true);
  });
});

describe("session scratch release", () => {
  it("releases only its own closed roots; protects another session, active data and unknown files", async () => {
    const { disk } = sandbox();
    const scope = new ScratchScope();
    const own = scope.create("output");
    fs.writeFileSync(path.join(own, "output.txt"), "own");
    closeScratch(own);
    const active = scope.create("output");
    const unknown = scope.create("output");
    closeScratch(unknown);
    fs.writeFileSync(path.join(unknown, "mine.txt"), "unrelated");
    const other = createScratch("output");
    closeScratch(other);
    await scope.close();
    await scope.close();
    expect(fs.existsSync(own)).toBe(false);
    for (const directory of [active, unknown, other]) expect(fs.existsSync(directory)).toBe(true);
    expect(() => scope.create("output")).toThrow("closed");
    expect(fs.existsSync(disk)).toBe(true);
  });

  it("never releases a replacement at a previously allocated path", async () => {
    sandbox();
    const scope = new ScratchScope();
    const own = scope.create("output");
    closeScratch(own);
    fs.renameSync(own, `${own}.saved`);
    const replacement = createScratch("output");
    closeScratch(replacement);
    fs.renameSync(replacement, own);
    await scope.close();
    expect(fs.existsSync(own)).toBe(true);
    expect(fs.existsSync(`${own}.saved`)).toBe(true);
  });

  it("waits for a pending artifact write and retains live shell-child files", async () => {
    sandbox();
    const outputs = new OutputArtifactStore();
    const writing = outputs.write("pending data");
    await outputs.close();
    expect(fs.existsSync(path.dirname(await writing))).toBe(false);
    await expect(outputs.write("late")).rejects.toThrow("closed");
    const jobs = new FabricShellJobStore();
    const job = jobs.begin("bash", "live child");
    fs.writeFileSync(job.pidPath, String(process.pid));
    await job.readPid();
    job.spill();
    await job.persistLog();
    await jobs.close();
    expect(fs.existsSync(job.pidPath)).toBe(true);
    expect(fs.existsSync(job.logPath!)).toBe(true);
  });

  it("releases pruned shell logs as well as handles still present at shutdown", async () => {
    sandbox();
    const jobs = new FabricShellJobStore();
    const paths: string[] = [];
    for (let i = 0; i < 258; i++) {
      const job = jobs.begin("bash", "finished");
      job.spill();
      paths.push(path.dirname(await job.persistLog()));
      await job.finish(0);
    }
    expect(jobs.list().length).toBeLessThan(paths.length);
    expect(paths.every(directory => fs.existsSync(directory))).toBe(true);
    await jobs.close();
    expect(paths.some(directory => fs.existsSync(directory))).toBe(false);
  });
});
