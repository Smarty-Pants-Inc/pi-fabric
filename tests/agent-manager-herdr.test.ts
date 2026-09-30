import { execFileSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { HerdrTransport } from "../src/agents/transports/herdr-transport.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import type { AgentRunResult } from "../src/agents/types.js";

const managers: AgentManager[] = [];
const servers: net.Server[] = [];
const roots: string[] = [];

afterEach(async () => {
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve) => server.close(() => resolve()))));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

const fixture = async (onApply?: (socket: net.Socket, id: string) => void) => {
  const repository = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-repo-"));
  roots.push(repository);
  const git = (...args: string[]) => execFileSync("git", args, {
    cwd: repository, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, GIT_AUTHOR_NAME: "Fabric tests", GIT_AUTHOR_EMAIL: "tests@example.invalid", GIT_COMMITTER_NAME: "Fabric tests", GIT_COMMITTER_EMAIL: "tests@example.invalid" },
  });
  git("init", "-q");
  fs.writeFileSync(path.join(repository, "README.md"), "test\n");
  git("add", "."); git("commit", "-q", "-m", "init");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-manager-"));
  roots.push(root);
  const socketPath = path.join(root, "herdr.sock");
  const requests: string[] = [];
  const server = net.createServer((socket) => {
    let input = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      input += chunk;
      const newline = input.indexOf("\n");
      if (newline < 0) return;
      const request = JSON.parse(input.slice(0, newline)) as { id: string; method: string };
      requests.push(request.method);
      if (request.method === "layout.apply" && onApply) return onApply(socket, request.id);
      socket.end(`${JSON.stringify({ id: request.id, result: { type: "pong" } })}\n`);
    });
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(socketPath, resolve); });
  vi.stubEnv("HERDR_ENV", "1");
  vi.stubEnv("HERDR_SOCKET_PATH", socketPath);
  vi.stubEnv("HERDR_WORKSPACE_ID", "w1");
  // Task-agent sessions can inherit a parent Fabric budget/depth; these managers
  // own isolated repositories and must not consume that parent's admission budget.
  for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID"]) vi.stubEnv(key, undefined);
  const settled = vi.fn();
  const lifecycle = vi.fn();
  const manager = new AgentManager(repository, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, onSettled: settled, onLifecycle: lifecycle,
  });
  managers.push(manager);
  const first = await manager.spawn({ task: "HANG", transport: "process" });
  return { manager, first, root, requests, settled, lifecycle, git };
};

// Review/astra F5: a guard proving no dispatch must not create an F4 obligation.
describe.skipIf(process.platform === "win32")("AgentManager Herdr launch certainty", () => {
  it.each([
    ["command preparation", "stop"],
    ["command preparation", "generation revocation"],
    ["connection establishment", "stop"],
    ["connection establishment", "generation revocation"],
  ] as const)("cleans a known pre-dispatch Herdr cancellation after %s (%s)", async (boundary, cancellation) => {
    const { manager, first, root, requests, settled, lifecycle, git } = await fixture();
    let authorized = true;
    let inLaunch = false;
    let worktree = "";
    let stopped: Promise<AgentRunResult> | undefined;
    let cancel!: () => void;
    const originalLaunch = HerdrTransport.prototype.launch;
    vi.spyOn(HerdrTransport.prototype, "launch").mockImplementation(function (this: HerdrTransport, request) {
      worktree = request.cwd;
      inLaunch = true;
      return originalLaunch.call(this, request).finally(() => { inLaunch = false; });
    });
    const originalArgs = processUtils.scriptSpawnArgs;
    vi.spyOn(processUtils, "scriptSpawnArgs").mockImplementation(async (...args) => {
      const command = await originalArgs(...args);
      // Herdr has claimed its spawn slot and yielded while preparing the command.
      if (inLaunch && boundary === "command preparation") cancel();
      return command;
    });
    const originalConnect = net.createConnection;
    vi.spyOn(net, "createConnection").mockImplementation((...args: Parameters<typeof net.createConnection>) => {
      const socket = originalConnect(...args);
      // Run before Herdr's connect listener checks authority and writes the request.
      if (inLaunch && boundary === "connection establishment") socket.prependOnceListener("connect", () => cancel());
      return socket;
    });
    const owner = new AbortController();
    const queued = await manager.spawn({ task: "cancel before Herdr dispatch", transport: "herdr", worktree: true }, owner.signal, () => authorized);
    expect(queued.status).toBe("queued");
    cancel = () => {
      expect(fs.existsSync(worktree)).toBe(true);
      if (cancellation === "stop") stopped = manager.stop(queued.id);
      else authorized = false;
    };
    await manager.stop(first.id);
    const result = await manager.wait(queued.id);
    if (stopped) expect(await stopped).toEqual(result);
    expect(result).toMatchObject({ status: "stopped", error: cancellation === "stop" ? "Agent launch aborted" : "Agent activation no longer authorized" });
    expect(owner.signal.aborted).toBe(false);
    expect(requests.filter((method) => method === "layout.apply")).toHaveLength(0);
    const runDirectory = path.join(root, queued.id);
    expect(fs.existsSync(path.join(runDirectory, "unresolved-worker.json"))).toBe(false);
    expect(settled.mock.calls.filter(([record]) => record.id === queued.id)).toHaveLength(1);
    expect(lifecycle).toHaveBeenCalledWith(expect.objectContaining({ event: "run.stopped", runId: queued.id }));
    expect(await manager.cleanup(queued.id, true)).toEqual({ cleaned: true });
    expect(fs.existsSync(runDirectory)).toBe(false);
    expect(fs.existsSync(worktree)).toBe(false);
    expect(git("worktree", "list", "--porcelain")).not.toContain(worktree);
    expect(manager.runningCount()).toBe(0);
    expect(manager.list().some((run) => run.id === queued.id)).toBe(false);
  });

  it.each(["definitive rejection", "dropped reply"] as const)("handles cancellation after dispatch with a %s", async (outcome) => {
    let cancel!: () => void;
    const { manager, first, root, requests, git } = await fixture((socket, id) => {
      cancel();
      if (outcome === "dropped reply") socket.destroy();
      else socket.end(`${JSON.stringify({ id, error: { code: "invalid_layout", message: "no worker created" } })}\n`);
    });
    let worktree = "";
    const originalLaunch = HerdrTransport.prototype.launch;
    vi.spyOn(HerdrTransport.prototype, "launch").mockImplementation(function (this: HerdrTransport, request) {
      worktree = request.cwd;
      return originalLaunch.call(this, request);
    });
    const queued = await manager.spawn({ task: "cancel dispatched Herdr launch", transport: "herdr", worktree: true });
    let stopped!: Promise<AgentRunResult>;
    cancel = () => { stopped = manager.stop(queued.id); };
    try {
      await manager.stop(first.id);
      const result = await manager.wait(queued.id);
      expect(await stopped).toEqual(result);
      expect(result.status).toBe("stopped");
      expect(requests.filter((method) => method === "layout.apply")).toHaveLength(1);
      const runDirectory = path.join(root, queued.id);
      const marker = path.join(runDirectory, "unresolved-worker.json");
      if (outcome === "definitive rejection") {
        expect(result.error).toContain("invalid_layout: no worker created");
        expect(fs.existsSync(marker)).toBe(false);
        expect(await manager.cleanup(queued.id, true)).toEqual({ cleaned: true });
        expect(fs.existsSync(runDirectory)).toBe(false);
        expect(fs.existsSync(worktree)).toBe(false);
      } else {
        // F4 is unchanged: a dispatched request with no confirmation retains
        // both run files and worktree, even after stop and manager shutdown.
        expect(result.error).toContain("a worker may still start");
        expect(JSON.parse(fs.readFileSync(marker, "utf8"))).toMatchObject({ runId: queued.id, worktree });
        expect(fs.existsSync(path.join(runDirectory, "task.txt"))).toBe(true);
        expect(fs.existsSync(worktree)).toBe(true);
        await expect(manager.cleanup(queued.id, true)).rejects.toThrow("lost track of its worker");
        await manager.close();
        expect(fs.existsSync(marker)).toBe(true);
        expect(fs.existsSync(worktree)).toBe(true);
      }
    } finally {
      if (worktree && fs.existsSync(worktree)) git("worktree", "remove", "--force", worktree);
    }
  });
});
