import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { TmuxTransport } from "../src/agents/transports/tmux-transport.js";
import { ScreenTransport } from "../src/agents/transports/screen-transport.js";
import { LocaltermTransport } from "../src/agents/transports/localterm-transport.js";
import { HerdrTransport } from "../src/agents/transports/herdr-transport.js";
import type { AgentTransportHandle } from "../src/agents/types.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { processStartTime } from "../src/residency/process-identity.js";

const workerPath = path.resolve("dist/worker.js");
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
type Birth = { pid: number; started: string };
const active = (birth: Birth): boolean => {
  if (processStartTime(birth.pid) !== birth.started) return false;
  const stat = fs.readFileSync(`/proc/${birth.pid}/stat`, "utf8");
  return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!);
};
const readBirth = (file: string): Birth => JSON.parse(fs.readFileSync(file, "utf8"));
const birthSource = `const stat = fs.readFileSync('/proc/' + process.pid + '/stat', 'utf8');
fs.writeFileSync(process.argv[2], JSON.stringify({pid: process.pid, started: stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\\s+/)[19]}));`;
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custody-review-"));
  const descendant = path.join(root, "descendant.mjs");
  fs.writeFileSync(descendant, `import fs from 'node:fs'; ${birthSource}
process.on('SIGTERM', () => {}); process.on('SIGHUP', () => {}); setInterval(() => {}, 1000);`);
  return { root, descendant };
};
const dispose = async (manager: AgentManager, root: string, afterClose?: () => Promise<void>) => {
  // Failure-before runs must drain every fixture birth, including a reparented
  // descendant. Never signal a PID merely because it appears in a file.
  for (const name of ["execution.json", "descendant.json", "leader.json", "worker.json"]) {
    const file = path.join(root, name);
    if (!fs.existsSync(file)) continue;
    const birth = readBirth(file);
    if (active(birth)) process.kill(birth.pid, "SIGKILL");
    await vi.waitFor(() => expect(active(birth)).toBe(false), { timeout: 5000 });
  }
  await manager.close().catch(() => undefined);
  await afterClose?.();
  fs.rmSync(root, { recursive: true, force: true });
};

describe.skipIf(process.platform !== "linux" || !fs.existsSync(workerPath))("review execution custody", () => {
  it.each(["wait", "stop", "close", "cleanup"] as const)("F3 crash terminal cannot release live execution via %s", async operation => {
    const { root } = fixture();
    const piBinary = path.join(root, "pi.mjs");
    fs.writeFileSync(piBinary, `import fs from 'node:fs';
const stat = fs.readFileSync('/proc/' + process.pid + '/stat', 'utf8');
fs.writeFileSync(${JSON.stringify(path.join(root, "execution.json"))}, JSON.stringify({pid: process.pid, started: stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\\s+/)[19]}));
process.on('SIGTERM', () => {}); process.stdin.resume();
setTimeout(() => process.stdout.write(JSON.stringify({type:'agent_start'}) + '\\n'), 300);
setInterval(() => {}, 1000);`);
    const before = process.env.PI_FABRIC_INJECT_CRASH;
    process.env.PI_FABRIC_INJECT_CRASH = "stream";
    let releasedWhileLive = false;
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, timeoutMs: 30000, retainRuns: false }, {
      workerPath, piBinary, runRoot: path.join(root, "runs"),
      onSettled: () => { releasedWhileLive ||= active(readBirth(path.join(root, "execution.json"))); },
    });
    try {
      const handle = await manager.spawn({ task: "crash after execution admission", transport: "process" });
      const runDirectory = manager.runDirectory(handle.id)!;
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "execution.json"))).toBe(true));
      // Drive each public branch after the worker's crash publication, rather
      // than racing an ordinary stop before the injection reaches the stream.
      await vi.waitFor(() => {
        const record = JSON.parse(fs.readFileSync(path.join(runDirectory, "status.json"), "utf8"));
        expect(record.status).toBe("failed");
      }, { timeout: 15000 });
      if (operation === "wait") expect((await manager.wait(handle.id)).status).toBe("failed");
      if (operation === "stop") expect((await manager.stop(handle.id)).status).toBe("failed");
      if (operation === "close") await manager.close();
      if (operation === "cleanup") { await manager.wait(handle.id); await manager.cleanup(handle.id); }
      expect(active(readBirth(path.join(root, "execution.json"))), "execution must exit before public completion").toBe(false);
      expect(releasedWhileLive, "terminal result is not an execution-exit receipt").toBe(false);
    } finally {
      if (before === undefined) delete process.env.PI_FABRIC_INJECT_CRASH; else process.env.PI_FABRIC_INJECT_CRASH = before;
      await dispose(manager, root);
    }
  }, 30000);

  it.each(["terminal", "actor-lost-contact"])("F3 inherited/unknown execution custody cannot release admission (%s)", async mode => {
    const { root } = fixture();
    const piBinary = path.join(root, "pi.mjs");
    fs.writeFileSync(piBinary, `import fs from 'node:fs';
const stat = fs.readFileSync('/proc/' + process.pid + '/stat', 'utf8');
fs.writeFileSync(${JSON.stringify(path.join(root, "execution.json"))}, JSON.stringify({pid: process.pid, started: stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\\s+/)[19]}));
process.stdin.resume(); process.on('SIGTERM', () => {}); setInterval(() => {}, 1000);`);
    const realLaunch = ProcessTransport.prototype.launch;
    let launches = 0;
    let notifyLost!: () => void;
    const lostReceipt = new Promise<void>(resolve => { notifyLost = resolve; });
    const spy = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      launches++;
      const handle = await realLaunch.call(this, request);
      const pid = Number(handle.sessionId);
      fs.writeFileSync(path.join(root, "worker.json"), JSON.stringify({ pid, started: processStartTime(pid) }));
      return { ...handle,
        ...(mode === "actor-lost-contact" ? { closed: lostReceipt, isAlive: async () => false, lostContact: () => "injected lost contact" } : {}),
        stop: async () => { throw new Error("injected execution exit unconfirmed"); } };
    });
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, timeoutMs: 30000, retainRuns: false }, {
      workerPath, piBinary, runRoot: path.join(root, "runs"),
    });
    try {
      const handle = await manager.spawn({ task: "admit detached execution", transport: "process",
        ...(mode === "actor-lost-contact" ? { actorId: "custody-actor", actorName: "custody-actor" } : {}) });
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "execution.json"))).toBe(true));
      const runDirectory = manager.runDirectory(handle.id)!;
      const statusFile = path.join(runDirectory, "status.json");
      const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      // A query override cannot wake an event adapter. Publish the simulated
      // contact-loss receipt only after its real native execution is admitted.
      if (mode === "actor-lost-contact") notifyLost();
      if (mode === "terminal") {
        process.kill(readBirth(path.join(root, "worker.json")).pid, "SIGKILL");
        // Replay the reviewed worker's pre-fix crash publication. The parent's
        // retained execution tracker is real, not a synthetic isAlive promise.
        fs.writeFileSync(statusFile, JSON.stringify({ ...record, status: "failed", error: "ordinary crash", finishedAt: Date.now() }));
      }
      await vi.waitFor(() => expect(fs.existsSync(path.join(runDirectory, "unresolved-worker.json"))).toBe(true), { timeout: 5000 });
      let settled = false;
      void manager.wait(handle.id).then(() => { settled = true; });
      await delay(100);
      expect(settled, "unknown actor custody must not resume the same-session drain").toBe(false);
      const queued = await manager.spawn({ task: "must not overlap", transport: "process" });
      await delay(100);
      expect(settled).toBe(false);
      expect(launches, "crash must not release the admission permit").toBe(1);
      expect(queued.status).toBe("queued");
      await expect(manager.stop(handle.id)).rejects.toThrow(/execution exit unconfirmed/);
      await expect(manager.cleanup(handle.id)).rejects.toThrow(/running agent/);
      await expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/);
      expect(manager.runDirectory(handle.id)).toBe(runDirectory);
      expect(fs.existsSync(runDirectory)).toBe(true);
      expect(active(readBirth(path.join(root, "execution.json")))).toBe(true);
    } finally { spy.mockRestore(); await dispose(manager, root); }
  }, 30000);

  it.each(["tmux", "screen", "localterm", "herdr"] as const)("F4 fails closed before %s session transport launch", async transport => {
    const { root } = fixture();
    const adapter = { tmux: TmuxTransport, screen: ScreenTransport, localterm: LocaltermTransport, herdr: HerdrTransport }[transport];
    const available = vi.spyOn(adapter.prototype, "available").mockResolvedValue(true);
    const launch = vi.spyOn(adapter.prototype, "launch").mockRejectedValue(new Error("untracked adapter launch"));
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { workerPath, runRoot: path.join(root, "runs") });
    try {
      await expect(manager.spawn({ task: "no untracked detached execution", transport })).rejects.toThrow(/disabled: detached execution custody is not confirmed; use process/);
      expect(launch).not.toHaveBeenCalled();
    } finally { available.mockRestore(); launch.mockRestore(); await dispose(manager, root); }
  });

  it.each(["native", ...(spawnSync("tmux", ["-V"]).status === 0 ? ["tmux"] : [])])("F4 real worker drains a leaderless pipe-holding group without the ProcessTransport tracker (%s)", async transport => {
    const { root, descendant } = fixture();
    const piBinary = path.join(root, "leader.mjs");
    fs.writeFileSync(piBinary, `import fs from 'node:fs'; import {spawn} from 'node:child_process';
const stat = fs.readFileSync('/proc/' + process.ppid + '/stat', 'utf8');
fs.writeFileSync(${JSON.stringify(path.join(root, "worker.json"))}, JSON.stringify({pid: process.ppid, started: stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\\s+/)[19]}));
const leaderStat = fs.readFileSync('/proc/' + process.pid + '/stat', 'utf8');
fs.writeFileSync(${JSON.stringify(path.join(root, "leader.json"))}, JSON.stringify({pid: process.pid, started: leaderStat.slice(leaderStat.lastIndexOf(')') + 2).trim().split(/\\s+/)[19]}));
const child = spawn(process.execPath, [${JSON.stringify(descendant)}, ${JSON.stringify(path.join(root, "descendant.json"))}], {stdio:['ignore',1,2]});
process.stdin.resume();
setTimeout(() => process.exit(0), 700);`);
    // Launch the real worker with native pipes but NO IPC custody tracker. This
    // is the worker contract used by session transports (tmux/screen/etc.).
    let session: AgentTransportHandle | undefined;
    let sentinel: AgentTransportHandle | undefined;
    const socket = path.join(root, "t.sock");
    const execute = processUtils.executeFile;
    const isolatedExecute = (command: string, args: string[], options?: Parameters<typeof execute>[2]) =>
      execute(command, command === "tmux" ? ["-f", "/dev/null", "-S", socket, ...args] : args, options);
    const query = transport === "tmux" ? vi.spyOn(processUtils, "executeFile").mockImplementation(isolatedExecute) : undefined;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      // Give the worker its real run deadline before the manager's fallback
      // deadline can forcibly remove a session. Public session admission is
      // separately rejected above precisely because that removal is unsafe.
      const args = [...request.workerArguments];
      args[args.indexOf("--timeout-ms") + 1] = "1800";
      request = { ...request, workerArguments: args };
      if (transport === "tmux") {
        session = await new TmuxTransport().launch(request);
        return { ...session, kind: "process" };
      }
      const worker = spawn(process.execPath, [request.workerPath, ...request.workerArguments], { cwd: request.cwd, stdio: "ignore" });
      await new Promise<void>((resolve, reject) => { worker.once("spawn", resolve); worker.once("error", reject); });
      const pid = worker.pid!;
      fs.writeFileSync(path.join(root, "worker.json"), JSON.stringify({ pid, started: processStartTime(pid) }));
      return { kind: "process", isAlive: async () => worker.exitCode === null && worker.signalCode === null,
        stop: async () => { worker.kill("SIGTERM"); } };
    });
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 15000, retainRuns: true }, {
      workerPath, piBinary, runRoot: path.join(root, "runs"),
    });
    try {
      if (transport === "tmux") {
        // Main's checked inventory deliberately treats an unreachable server as
        // unknown, not worker exit. Keep a healthy, isolated server after this
        // worker's last pane exits; the test is about its execution-group drain.
        const sentinelWorker = path.join(root, "sentinel.mjs");
        fs.writeFileSync(sentinelWorker, `import fs from 'node:fs'; ${birthSource} setInterval(() => {}, 1000);`);
        sentinel = await new TmuxTransport().launch({ id: "custody-sentinel", name: "sentinel", cwd: root,
          workerPath: sentinelWorker, workerArguments: [path.join(root, "sentinel.json")] });
        await vi.waitFor(() => expect(fs.existsSync(path.join(root, "sentinel.json"))).toBe(true));
      }
      const handle = await manager.spawn({ task: "cancel leaderless execution", transport: "process" });
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "descendant.json"))).toBe(true));
      const birth = readBirth(path.join(root, "descendant.json"));
      await delay(1000);
      expect(active(birth)).toBe(true);
      const leader = readBirth(path.join(root, "leader.json"));
      expect(active(leader), "native leader exited while the descendant still holds pipes").toBe(false);
      const stat = fs.readFileSync(`/proc/${birth.pid}/stat`, "utf8");
      expect(Number(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[2])).toBe(leader.pid);
      const result = await manager.wait(handle.id);
      expect(result.status).toBe("timed_out");
      expect(active(birth), "same-birth descendant must exit before cancellation settles").toBe(false);
      expect(active(readBirth(path.join(root, "worker.json"))), "native worker birth must also exit").toBe(false);
      if (session) expect(await session.observe!()).toEqual({ state: "absent" });
    } finally {
      launch.mockRestore();
      try {
        await dispose(manager, root, async () => {
          await session?.stop();
          await sentinel?.stop();
          if (transport === "tmux") {
            await isolatedExecute("tmux", ["kill-server"], { timeoutMs: 3000 }).catch(() => undefined);
            const file = path.join(root, "sentinel.json");
            if (fs.existsSync(file)) {
              const birth = readBirth(file);
              if (active(birth)) process.kill(birth.pid, "SIGKILL");
              await vi.waitFor(() => expect(active(birth)).toBe(false), { timeout: 5000 });
            }
          }
        });
      } finally { query?.mockRestore(); }
    }
  }, 30000);
});
