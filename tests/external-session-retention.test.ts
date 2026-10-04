import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentTransportHandle } from "../src/agents/types.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { TmuxTransport } from "../src/agents/transports/tmux-transport.js";
import { ScreenTransport } from "../src/agents/transports/screen-transport.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { externalSessionHandle } from "../src/agents/transports/external-session.js";
import { fabricWorktreePath } from "../src/agents/worktree-paths.js";
import { canRemoveTerminalRun, hasUnresolvedWorker, sweepTempRunRoots } from "../src/storage/retention.js";

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const until = async (predicate: () => boolean, timeout = 8_000) => {
  const deadline = Date.now() + timeout;
  while (!predicate()) { if (Date.now() >= deadline) throw new Error("External session fixture did not settle"); await sleep(20); }
};
const git = (root: string, ...args: string[]) => execFileSync("git", args, { cwd: root, stdio: "ignore",
  env: { ...process.env, GIT_AUTHOR_NAME: "Fixture", GIT_COMMITTER_NAME: "Fixture", GIT_AUTHOR_EMAIL: "fixture@example.invalid", GIT_COMMITTER_EMAIL: "fixture@example.invalid" } });
const repository = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ext-receipt-"));
  git(root, "init", "-q"); fs.writeFileSync(path.join(root, "input.txt"), "retained working input");
  git(root, "add", "."); git(root, "commit", "-qm", "fixture"); return root;
};
const managerFor = (root: string, kind: "tmux" | "screen", workerPath = path.resolve("tests/fixtures/fake-worker.mjs")) => {
  const adapter = kind === "tmux" ? new TmuxTransport() : new ScreenTransport();
  // Production admission disables these adapters until detached execution
  // custody is checked. Inject real external handles at the supported launch
  // seam to retain coverage of legacy/recovered custody, NOT reopen admission.
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => {
    const workerArguments = [...request.workerArguments];
    workerArguments[workerArguments.indexOf("--transport") + 1] = kind;
    return adapter.launch({ ...request, workerArguments });
  });
  return new AgentManager(root,
    { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: false, nice: 19 },
    { workerPath, runRoot: path.join(root, "runs"), piBinary: process.execPath });
};
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

describe.each(["tmux", "screen"] as const)("%s uncertain launch retention", kind => {
  it("refuses production session admission before invoking the native launch", async () => {
    const root = repository(); const manager = managerFor(root, kind);
    const launch = vi.spyOn(kind === "tmux" ? TmuxTransport.prototype : ScreenTransport.prototype, "launch");
    try {
      await expect(manager.spawn({ task: "must not launch", transport: kind })).rejects.toThrow(/disabled.*detached execution custody/);
      expect(launch).not.toHaveBeenCalled();
      await manager.close();
    } finally {
      await manager.close().catch(() => undefined); vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
    }
  });
  it("records unknown liveness as lost contact, not a transport exit, and retains its files", async () => {
    const root = repository(); const manager = managerFor(root, kind);
    const execute = processUtils.executeFile;
    vi.spyOn(kind === "tmux" ? TmuxTransport.prototype : ScreenTransport.prototype, "available").mockResolvedValue(true);
    vi.spyOn(processUtils, "executeFile").mockImplementation(async (command, args, options) => {
      if (command !== kind) return execute(command, args, options);
      if (args[0] === "new-session" || args[0] === "-dmS") return { stdout: "", stderr: "" };
      throw new Error("session socket unavailable");
    });
    try {
      const info = await manager.spawn({ task: "missing query reply", transport: "process" });
      const result = await manager.wait(info.id);
      expect(result).toMatchObject({ status: "failed", error: expect.stringContaining("Lost track of the worker") });
      expect(result.error).not.toContain("transport exited");
      const run = manager.runDirectory(info.id)!;
      expect(hasUnresolvedWorker(run)).toBe(true);
      await expect(manager.cleanup(info.id)).rejects.toThrow(/lost track/);
      await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved|pending launch/);
      await expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/); expect(fs.existsSync(path.join(run, "task.txt"))).toBe(true);
    } finally {
      await manager.close().catch(() => undefined); vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
    }
  }, 20_000);

  it("retains a cancelled native launch CLI before a handle can be registered", async () => {
    const root = repository(); const manager = managerFor(root, kind); const abort = new AbortController();
    const execute = processUtils.executeFile;
    let entered!: () => void; const creating = new Promise<void>(resolve => { entered = resolve; });
    vi.spyOn(kind === "tmux" ? TmuxTransport.prototype : ScreenTransport.prototype, "available").mockResolvedValue(true);
    vi.spyOn(processUtils, "executeFile").mockImplementation(async (command, args, options) => {
      if (command !== kind) return execute(command, args, options);
      const cli = execute(process.execPath, ["-e", "process.on('SIGTERM',()=>{});setInterval(()=>{},1000)"], options);
      entered(); return cli;
    });
    const launching = manager.spawn({ task: "cancel before registration", transport: "process", worktree: true }, abort.signal);
    // Observe completion before abort, so an uncertain native operation is
    // joined to its queued custody receipt, never an unobserved background job.
    const outcome = launching.then(info => info, error => error);
    try {
      await Promise.race([creating, outcome.then(error => { throw error ?? new Error("Launch completed before CLI creation"); })]); abort.abort();
      expect(await outcome).toMatchObject({ status: "queued" });
      const [id] = fs.readdirSync(path.join(root, "runs"));
      const run = path.join(root, "runs", id!); const worktree = fabricWorktreePath(root, id!);
      expect(hasUnresolvedWorker(run)).toBe(true); expect(fs.existsSync(worktree)).toBe(true);
      await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved|pending launch/);
      await expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/); expect(fs.existsSync(run)).toBe(true); expect(fs.existsSync(worktree)).toBe(true);
    } finally {
      abort.abort(); await outcome; await manager.close().catch(() => undefined); vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
    }
  }, 20_000);
  it.each(["lost reply", "cancelled CLI", "timed out CLI"])("persists an unresolved obligation and retains worktree/run files on %s", async message => {
    const root = repository(); const manager = managerFor(root, kind);
    const execute = processUtils.executeFile;
    vi.spyOn(kind === "tmux" ? TmuxTransport.prototype : ScreenTransport.prototype, "available").mockResolvedValue(true);
    vi.spyOn(processUtils, "executeFile").mockImplementation(async (command, args, options) => {
      if (command === kind) throw new Error(message);
      return execute(command, args, options);
    });
    try {
      const info = await manager.spawn({ task: "unconfirmed launch", transport: "process", worktree: true });
      expect(info.status).toBe("queued");
      await expect(manager.stop(info.id)).rejects.toThrow(/execution exit unconfirmed/);
      const runs = fs.readdirSync(path.join(root, "runs")); expect(runs).toHaveLength(1);
      const id = runs[0]!; const run = path.join(root, "runs", id); const worktree = fabricWorktreePath(root, id);
      expect(JSON.parse(fs.readFileSync(path.join(run, "unresolved-worker.json"), "utf8"))).toMatchObject({ runId: id, transport: kind, sessionId: `pi-fabric-${id.slice(0, 12)}`, worktree, cleanupPending: true });
      expect(fs.existsSync(path.join(run, "task.txt"))).toBe(true); expect(fs.existsSync(worktree)).toBe(true);
      await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved|pending launch/);
      await expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/);
      expect(hasUnresolvedWorker(run)).toBe(true); expect(fs.existsSync(worktree)).toBe(true);
      expect(canRemoveTerminalRun(run)).toBe(false);
    } finally {
      await manager.close().catch(() => undefined); vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
    }
  }, 20_000);
});

// These are real native paths, not a process masquerading as an external pane.
// CI may lack a tool (notably native Windows); skip only with that explicit reason.
for (const kind of ["tmux", "screen"] as const) {
  const installed = await processUtils.commandAvailable(kind);
  it.skipIf(!installed)(`real ${kind}: terminal-live retention and checked inventory (skip only when ${kind} is absent)`, async () => {
    const root = repository(); const screenDir = path.join(root, "s"); fs.mkdirSync(screenDir, { mode: 0o700 });
    const socket = path.join(root, "t.sock");
    vi.stubEnv("SCREENDIR", screenDir);
    const worker = path.join(root, "live-worker.mjs");
    fs.writeFileSync(worker, `import fs from 'node:fs';
const args=new Map();for(let i=2;i<process.argv.length;i+=2)args.set(process.argv[i].slice(2),process.argv[i+1]);
const status=args.get('status-file');if(args.get('pid-file'))fs.writeFileSync(args.get('pid-file'),String(process.pid));
if(status){const now=Date.now();fs.writeFileSync(status,JSON.stringify({id:args.get('id'),name:args.get('name'),task:'live',status:'running',runner:'pi',transport:args.get('transport'),cwd:args.get('cwd'),startedAt:now,updatedAt:now,turns:0,toolCalls:0,text:'',usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}}));fs.writeFileSync(status+'.pid',String(process.pid));}
process.on('SIGHUP',()=>process.exit(0));process.on('SIGTERM',()=>process.exit(0));setInterval(()=>{},1000);
`);
    const actualExecute = processUtils.executeFile;
    const isolatedExecute = (command: string, args: string[], options?: Parameters<typeof processUtils.executeFile>[2]) =>
      actualExecute(command, command === "tmux" ? ["-f", "/dev/null", "-S", socket, ...args] : args, options);
    vi.spyOn(processUtils, "executeFile").mockImplementation(isolatedExecute);
    const adapter = kind === "tmux" ? new TmuxTransport() : new ScreenTransport();
    const prototype = kind === "tmux" ? TmuxTransport.prototype : ScreenTransport.prototype;
    const actualLaunch = prototype.launch;
    let handle: AgentTransportHandle | undefined; let sentinel: AgentTransportHandle | undefined;
    vi.spyOn(prototype, "launch").mockImplementation(async request => { const result = await actualLaunch.call(adapter, request); handle = result; return result; });
    const manager = managerFor(root, kind, worker);
    const pids: number[] = [];
    const sentinelPid = path.join(root, "sentinel.pid");
    try {
      const info = await manager.spawn({ task: "HANG terminal live", transport: "process", worktree: true });
      const run = manager.runDirectory(info.id)!; const status = path.join(run, "status.json");
      await until(() => fs.existsSync(status + ".pid")); pids.push(Number(fs.readFileSync(status + ".pid", "utf8")));
      expect(await handle!.observe!()).toEqual({ state: "alive" });
      // Keep a healthy server/inventory across the manager's stop request. A
      // successful absent-session query still cannot prove detached worker exit.
      const original = handle!;
      sentinel = await actualLaunch.call(adapter, { id: `sentinel-${kind}`, name: "sentinel", cwd: root, workerPath: worker, workerArguments: ["--pid-file", sentinelPid] });
      await until(() => fs.existsSync(sentinelPid)); pids.push(Number(fs.readFileSync(sentinelPid, "utf8")));
      const record = JSON.parse(fs.readFileSync(status, "utf8"));
      fs.writeFileSync(status, JSON.stringify({ ...record, status: "failed", error: "terminal does not mean exit", finishedAt: Date.now() }));
      await expect(manager.wait(info.id, { timeoutMs: 100 })).rejects.toThrow(/still running/);
      await expect(manager.stop(info.id)).rejects.toThrow(/execution exit unconfirmed/);
      expect(hasUnresolvedWorker(run)).toBe(true);
      await expect(manager.checkpointForRelease(Date.now() + 100)).rejects.toThrow(/unresolved/);
      await expect(manager.cleanup(info.id)).rejects.toThrow(/running agent/);
      await expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/);
      await vi.waitFor(async () => expect(await original.observe!()).toEqual({ state: "absent" }), { timeout: 5_000 });
      expect(fs.existsSync(run)).toBe(true); expect(fs.existsSync(info.worktree!)).toBe(true);
      // Recovered/orphaned/nested storage is still vetoed after host death, even
      // without a live manager or an unresolved marker.
      const orphan = path.join(root, "pi-fabric-runs-orphan"); const parent = path.join(orphan, "parent"); const nested = path.join(parent, "nested", "external");
      fs.mkdirSync(nested, { recursive: true });
      fs.writeFileSync(path.join(orphan, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
      fs.writeFileSync(path.join(parent, "status.json"), JSON.stringify({ status: "completed", transport: "process", finishedAt: 1 }));
      fs.writeFileSync(path.join(nested, "status.json"), JSON.stringify({ status: "failed", transport: kind, sessionId: handle!.sessionId, finishedAt: 1 }));
      fs.writeFileSync(path.join(nested, "task.txt"), "live nested worker input");
      expect(canRemoveTerminalRun(parent)).toBe(false);
      const swept = sweepTempRunRoots({ tempRoot: root, now: 100_000, orphanedTempRunRetentionMs: 1, oneShotRunRetentionMs: 1 });
      expect(swept.removedRoots).toEqual([]); expect(swept.removedRuns).toEqual([]); expect(fs.existsSync(nested)).toBe(true);
      expect(await externalSessionHandle(kind, `${original.sessionId}-missing`).observe!()).toEqual({ state: "absent" });
      await original.stop();
      await vi.waitFor(async () => expect(await original.observe!()).toEqual({ state: "absent" }), { timeout: 5_000 });
      expect(canRemoveTerminalRun(run)).toBe(false); // Absence is not a worker receipt.
      expect(fs.existsSync(info.worktree!)).toBe(true);
    } finally {
      await handle?.stop(); await sentinel?.stop(); await manager.close().catch(() => undefined);
      if (kind === "tmux") await isolatedExecute("tmux", ["kill-server"], { timeoutMs: 3_000 }).catch(() => undefined);
      // A pane's child is adopted by init; /proc zombies have exited and are not
      // running work. This is fixture teardown, NOT production exit evidence.
      for (const pid of pids) await until(() => {
        if (!processUtils.processIsAlive(pid)) return true;
        if (process.platform === "linux") { try { return /^\d+ \(.*\) Z /.test(fs.readFileSync(`/proc/${pid}/stat`, "utf8")); } catch { return true; } }
        return false;
      });
      vi.restoreAllMocks(); vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
    }
  }, 30_000);
}
