import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { HOST_POLICY_PATH } from "../src/host-policy.js";

// smarty-dev#8953 / #6829: host-backed E2E of agents.processSlice. Real systemd --user scopes with
// --collect remove an empty scope's cgroup, often before Fabric reads its retained cgroup.events fd
// (ENODEV). No scope, cgroup or systemd mocks: only the root ownership of the test policy file is
// simulated, exactly as tests/host-policy.test.ts does, and only while the policy loads.
const RUNS = 20;
const skipReason = ((): string | undefined => {
  if (process.platform !== "linux") return "not Linux";
  try { if (fs.statfsSync("/sys/fs/cgroup").type !== 0x63677270) return "/sys/fs/cgroup is not cgroup v2"; }
  catch { return "no /sys/fs/cgroup"; }
  const probe = spawnSync("systemd-run", ["--user", "--scope", "--quiet", "--collect", "true"], { encoding: "utf8", timeout: 10_000 });
  if (probe.error || probe.status !== 0) return `no usable systemd-run --user (${probe.error?.message ?? probe.stderr.trim() ?? probe.status})`;
  return undefined;
})();
if (skipReason) console.log(`process-slice-scope-e2e skipped: ${skipReason}`);

const slice = `fabric-e2e-${randomBytes(4).toString("hex")}.slice`;
const temps: string[] = [];
const temp = (label: string): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), `scope-e2e-${label}-`)); temps.push(dir); return dir; };
afterAll(() => {
  if (!skipReason) spawnSync("systemctl", ["--user", "stop", slice], { timeout: 10_000 });
  fs.rmSync(path.dirname(HOST_POLICY_PATH), { recursive: true, force: true });
  for (const dir of temps) fs.rmSync(dir, { recursive: true, force: true });
});

// The policy loader requires a root-owned file and ancestors; simulate ownership of that path only.
const loadRootPolicySlice = async (): Promise<string | undefined> => {
  if (!HOST_POLICY_PATH.startsWith(`${os.tmpdir()}${path.sep}`)) throw new Error("test policy path is not private");
  fs.mkdirSync(path.dirname(HOST_POLICY_PATH), { recursive: true, mode: 0o700 });
  fs.writeFileSync(HOST_POLICY_PATH, JSON.stringify({ agents: { processSlice: slice } }), { mode: 0o600 });
  const ancestors = new Set<string>();
  for (let dir = path.dirname(HOST_POLICY_PATH); ; dir = path.dirname(dir)) { ancestors.add(dir); if (path.dirname(dir) === dir) break; }
  const as = (stat: fs.Stats, changes: Partial<fs.Stats>): fs.Stats => Object.assign(Object.create(Object.getPrototypeOf(stat)) as fs.Stats, stat, changes);
  const lstat = fs.lstatSync, open = fs.openSync, fstat = fs.fstatSync;
  let policyFd: number | undefined;
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const stat = (lstat as (...args: unknown[]) => fs.Stats)(file, ...rest);
    const name = String(file);
    return name === HOST_POLICY_PATH || ancestors.has(name) ? as(stat, { uid: 0, ...(ancestors.has(name) ? { mode: stat.mode & ~0o022 } : {}) }) : stat;
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
    const fd = (open as (...args: unknown[]) => number)(file, ...rest);
    if (String(file) === HOST_POLICY_PATH) policyFd = fd;
    return fd;
  }) as typeof fs.openSync);
  vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, ...rest: unknown[]) => {
    const stat = (fstat as (...args: unknown[]) => fs.Stats)(fd, ...rest);
    return fd === policyFd ? as(stat, { uid: 0 }) : stat;
  }) as typeof fs.fstatSync);
  try {
    const { loadFabricConfig } = await import("../src/config.js");
    const cwd = temp("project");
    fs.mkdirSync(path.join(cwd, ".pi"));
    return loadFabricConfig({ cwd, agentDir: temp("agent"), projectTrusted: true }).agents.processSlice;
  } finally { vi.restoreAllMocks(); }
};

const unresolvedMarkers = (dir: string): string[] => fs.readdirSync(dir, { recursive: true }).map(String)
  .filter(file => path.basename(file) === "unresolved-worker.json");

describe.skipIf(skipReason !== undefined)("agents.processSlice scopes on a real systemd --user (smarty-dev#8953)", () => {
  it(`settles ${RUNS} quick process runs whose empty scopes systemd removes`, async () => {
    const processSlice = await loadRootPolicySlice();
    expect(processSlice).toBe(slice); // from the root policy, not agent/project config (#738)
    const { DEFAULT_FABRIC_CONFIG } = await import("../src/config.js");
    const runRoot = temp("runs");
    const manager = new AgentManager(temp("cwd"), { ...DEFAULT_FABRIC_CONFIG.agents, processSlice: slice, retainRuns: true, budgetUsd: 0,
      maxConcurrent: 4 }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot, preparePiModel: async model => model });
    // Observe (never alter) the retained cgroup.events reads: count the reads the scope removal answered ENODEV.
    let enodev = 0, scopedReads = 0;
    const readSync = fs.readSync;
    vi.spyOn(fs, "readSync").mockImplementation(((...args: unknown[]) => {
      try { return (readSync as (...values: unknown[]) => number)(...args); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === "ENODEV") enodev++; throw error; }
      finally { scopedReads++; }
    }) as typeof fs.readSync);
    try {
      const handles = [];
      for (let index = 0; index < RUNS; index++) handles.push(await manager.spawn({ task: `scope e2e ${index}`, transport: "process" }));
      const results = await Promise.all(handles.map(handle => manager.wait(handle.id, { timeoutMs: 60_000 })));
      // The tree-exit receipt (or its failure, which writes unresolved-worker.json) lands after the logical
      // result, at native close; give the last scope watcher change time to arrive.
      await new Promise(resolve => setTimeout(resolve, 3_000));
      const unresolved = unresolvedMarkers(runRoot);
      const reasons = unresolved.map(file => String(JSON.parse(fs.readFileSync(path.join(runRoot, file), "utf8")).reason));
      console.log(JSON.stringify({ e2e: "process-slice-scope", kernel: os.release(), slice, runs: RUNS, finished: results.length,
        completed: results.filter(result => result.status === "completed").length, enodevReads: enodev, reads: scopedReads,
        unresolved: unresolved.length, firstReason: reasons[0] }));
      expect(results.filter(result => result.status === "completed")).toHaveLength(RUNS); // collected == finished
      // No run left as "execution exit unconfirmed; custody retained".
      expect(reasons).toEqual([]);
      expect(enodev).toBeGreaterThan(0); // the removal race was really exercised
    } finally {
      vi.restoreAllMocks();
      await manager.close();
    }
  }, 180_000);
});
