import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { WorktreeManager } from "../src/agents/worktree-manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { createRunTmpDirectory, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";
import { markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const sandbox = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-lifecycle-")); roots.push(root); return root; };
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("#369 D1 later descendant cleanup", () => {
  for (const collection of ["cleanup", "close"] as const) {
    for (const scratch of [true, false]) {
      it.each(["live", "unknown"] as const)(`${collection} preserves ${scratch ? "fenced scratch" : "descendant identity veto without scratch"} (%s)`, async identity => {
        const root = sandbox();
        const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, sessionExport: false, budgetUsd: 0 }, {
          workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
        });
        managers.push(manager);
        const result = await manager.run({ task: "cleanup parent", transport: "process" });
        const run = manager.runDirectory(result.id)!;
        // A contained fixture is already disposed. On unsupported hosts,
        // remove only its fixture fence to isolate the later descendant veto.
        fs.rmSync(path.join(run, "tmp"), { recursive: true, force: true });
        fs.rmSync(path.join(run, UNRESOLVED_SCRATCH_FILE), { force: true });
        const child = path.join(run, "nested", "child");
        fs.mkdirSync(child, { recursive: true, mode: 0o700 });
        fs.writeFileSync(path.join(child, "task.txt"), "descendant");
        fs.writeFileSync(path.join(child, "status.json"), JSON.stringify({ status: "completed", transport: "process", ...(identity === "live" ? { sessionId: String(process.pid) } : {}) }));
        if (scratch) fs.writeFileSync(path.join(createRunTmpDirectory(child), "child-file"), "live scratch");
        if (collection === "cleanup") await expect(manager.cleanup(result.id)).rejects.toThrow(/descendant|scratch/);
        else await manager.close();
        expect(fs.existsSync(child)).toBe(true);
        if (scratch) expect(fs.readFileSync(path.join(child, "tmp", "child-file"), "utf8")).toBe("live scratch");
        // Identity-only cases become collectable when exit is known, never on
        // terminal status alone. Fenced scratch continues to fail closed.
        if (!scratch && collection === "cleanup") {
          fs.writeFileSync(path.join(child, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647" }));
          expect((await manager.cleanup(result.id)).cleaned).toBe(true);
        }
      }, 15_000);
    }
  }
});

describe("#369 D2 confirmed pre-worker refusal retention", () => {
  it.skipIf(process.platform === "win32").each(["authorization", "runtime"] as const)("cleans the worktree on checked pre-worker %s refusal after scratch allocation", async failure => {
    const root = sandbox(), worktree = path.join(root, "worktree");
    fs.mkdirSync(worktree, { mode: 0o700 });
    vi.spyOn(WorktreeManager.prototype, "create").mockResolvedValue({ gitRoot: root, path: worktree, cwd: worktree, branch: "fixture" });
    const cleanup = vi.spyOn(WorktreeManager.prototype, "cleanup").mockImplementation(async () => { fs.rmSync(worktree, { recursive: true }); return true; });
    const original = ProcessTransport.prototype.launch;
    let run!: string;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      run = path.dirname(request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!);
      return original.call(this, { ...request, authorize: () => {
        expect(fs.existsSync(path.join(run, "tmp"))).toBe(true);
        return false;
      } });
    });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, sessionExport: false }, {
      runRoot: path.join(root, "runs"), workerPath: path.join(root, failure === "runtime" ? "worker.ts" : "worker.mjs"),
    });
    managers.push(manager);
    if (failure === "runtime") { vi.stubEnv("PATH", ""); vi.stubEnv("PI_FABRIC_NODE_BINARY", ""); }
    await expect(manager.run({ task: "refused worktree", transport: "process", worktree: true })).rejects.toThrow(failure === "runtime" ? /requires.*Bun runtime/ : /no longer authorized/);
    expect(cleanup).toHaveBeenCalledExactlyOnceWith(expect.any(String), true);
    expect(fs.existsSync(worktree)).toBe(false);
    expect(fs.existsSync(path.join(run, "tmp"))).toBe(false);
    expect(fs.existsSync(path.join(run, UNRESOLVED_SCRATCH_FILE))).toBe(false);
    expect(fs.existsSync(path.join(run, "never-started.json"))).toBe(true);
  });
  for (const owner of ["closed", "orphan"] as const) {
    it.skipIf(process.platform === "win32").each(["authorization", "runtime"] as const)(`${owner} retention collects a never-started allocation (%s)`, async failure => {
      const tempRoot = sandbox();
      const root = fs.mkdtempSync(path.join(tempRoot, "pi-fabric-runs-"));
      markRunRootActive(root, 1);
      const run = path.join(root, "refused"); fs.mkdirSync(run, { mode: 0o700 });
      fs.writeFileSync(path.join(run, "task.txt"), "never started");
      let observedAllocated = false;
      if (failure === "runtime") { vi.stubEnv("PATH", ""); vi.stubEnv("PI_FABRIC_NODE_BINARY", ""); }
      await expect(new ProcessTransport().launch({
        id: "refused", name: "refused", cwd: run, workerPath: path.join(tempRoot, failure === "runtime" ? "worker.ts" : "worker.mjs"),
        workerArguments: ["--status-file", path.join(run, "status.json")],
        authorize: () => { observedAllocated = fs.existsSync(path.join(run, "tmp")); return false; },
      })).rejects.toThrow(failure === "runtime" ? /requires.*Bun runtime/ : /no longer authorized/);
      if (failure === "authorization") expect(observedAllocated).toBe(true);
      expect(fs.existsSync(path.join(run, "status.json"))).toBe(false);
      if (owner === "closed") markRunRootClosed(root, 2, false);
      else fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 2 }));
      const sweep = sweepTempRunRoots({ tempRoot, orphanedTempRunRetentionMs: 1, oneShotRunRetentionMs: 1, now: 100 });
      expect(sweep.removedRoots).toContain(root);
      expect(fs.existsSync(root)).toBe(false);
    });
  }

  it("does not turn a reused allocation into a never-started receipt", async () => {
    const tempRoot = sandbox();
    const root = fs.mkdtempSync(path.join(tempRoot, "pi-fabric-runs-")); markRunRootActive(root, 1);
    const run = path.join(root, "retry"); fs.mkdirSync(run, { mode: 0o700 });
    fs.writeFileSync(path.join(run, "task.txt"), "possibly launched before");
    const tmp = createRunTmpDirectory(run); fs.writeFileSync(path.join(tmp, "prior-writer"), "custody unknown");
    await expect(new ProcessTransport().launch({ id: "retry", name: "retry", cwd: run, workerPath: path.join(tempRoot, "worker.mjs"), workerArguments: ["--status-file", path.join(run, "status.json")], authorize: () => false })).rejects.toThrow(/no longer authorized/);
    markRunRootClosed(root, 2, true);
    expect(sweepTempRunRoots({ tempRoot, orphanedTempRunRetentionMs: 1, oneShotRunRetentionMs: 1, now: 100 }).removedRoots).toEqual([]);
    expect(fs.readFileSync(path.join(tmp, "prior-writer"), "utf8")).toBe("custody unknown");
    expect(fs.existsSync(path.join(run, UNRESOLVED_SCRATCH_FILE))).toBe(true);
  });
});
