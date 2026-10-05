import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import * as scratch from "../src/storage/run-scratch.js";
import * as windowsRoots from "../src/storage/windows-temp-root.js";
import { spawnDetached, WorkerNotStartedError } from "../src/agents/transports/process-utils.js";

vi.mock("../src/agents/transports/process-utils.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/agents/transports/process-utils.js")>();
  return { ...actual, spawnDetached: vi.fn() };
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

describe("Windows scratch disposal and custody scope cut (#4010; allocation remains deferred to #4800)", () => {
  it.each(["absent", "tmp", "unresolved-scratch.json"] as const)("offline scratch inspection never disposes inherited temp and keeps legacy %s custody", artifact => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-win-off-retention-"));
    if (artifact === "tmp") fs.mkdirSync(path.join(root, artifact));
    if (artifact === "unresolved-scratch.json") fs.writeFileSync(path.join(root, artifact), JSON.stringify({ version: 3,
      runDirectory: path.resolve(root), allocatedAt: 1, lastLaunchAt: 1, launchNonce: "legacy" }));
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const read = vi.spyOn(fs, "readFileSync");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const rm = vi.spyOn(fs, "rmSync");
    const acl = vi.spyOn(windowsRoots, "windowsDataRoot");
    try {
      const stat = vi.spyOn(fs, "lstatSync");
      const before = artifact === "absent" ? [] : [artifact];
      expect(scratch.runScratchExitVeto(root)).toBeUndefined();
      expect(scratch.disposeRunTmpDirectory(root)).toBe(false);
      expect(stat).not.toHaveBeenCalled();
      expect(fs.readdirSync(root)).toEqual(before); // no custody sweep, nothing deleted
      expect(read).not.toHaveBeenCalled();
      expect(mkdir).not.toHaveBeenCalled();
      expect(rm).not.toHaveBeenCalled();
      expect(acl).not.toHaveBeenCalled();
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });
  it("keeps manager admission on main's mkdir path without ACL or scratch inspection", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-windows-off-manager-"));
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, sessionExport: false }, {
      runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    });
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const acl = vi.spyOn(windowsRoots, "windowsDataRoot");
    const allocate = vi.spyOn(scratch, "allocateRunTmpDirectory");
    const dispose = vi.spyOn(scratch, "disposeRunTmpDirectory");
    const waitForClose = vi.fn(async () => {});
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
      fs.writeFileSync(statusFile, JSON.stringify({ id: request.id, name: request.name, task: "off", status: "completed",
        runner: "pi", transport: "process", sessionId: "2147483647", cwd: request.cwd,
        startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), text: "done", turns: 1, toolCalls: 0,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
      return { kind: "process", sessionId: "2147483647", isAlive: async () => false, stop: async () => {}, waitForClose };
    });
    try {
      const result = await manager.run({ task: "off", extensions: false });
      expect(result.status).toBe("completed");
      await manager.close();
      expect(waitForClose).toHaveBeenCalled();
      expect(acl).not.toHaveBeenCalled();
      expect(allocate).not.toHaveBeenCalled();
      expect(dispose).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(manager.runDirectory(result.id)!, "tmp"))).toBe(false);
    } finally {
      await manager.close();
      vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each([false, true])("inherits normal user temp without scratch or per-run ACL work (actor=%s)", async actor => {
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    vi.stubEnv("TEMP", "C:\\Users\\runner\\AppData\\Local\\Temp");
    vi.stubEnv("TMP", "C:\\Users\\runner\\AppData\\Local\\Temp");
    vi.stubEnv("TMPDIR", undefined);
    const allocate = vi.spyOn(scratch, "allocateRunTmpDirectory");
    const acl = vi.spyOn(windowsRoots, "windowsDataRoot");
    const closed = Promise.resolve();
    const waitForClose = vi.fn(async () => {}), stop = vi.fn(async () => {});
    vi.mocked(spawnDetached).mockResolvedValue({ pid: 123, isAlive: async () => false,
      lostContact: () => undefined, waitForClose, stop, closed });
    const workerArguments = ["--status-file", path.resolve("run/status.json"), ...(actor ? ["--actor-id", "test-actor"] : [])];
    const handle = await new ProcessTransport().launch({ id: "test", name: "test", cwd: process.cwd(),
      workerPath: path.resolve("custom-worker.mjs"), workerArguments });
    const env = vi.mocked(spawnDetached).mock.calls.at(-1)![4]!;
    expect(env.TEMP).toBe(process.env.TEMP);
    expect(env.TMP).toBe(process.env.TMP);
    expect(env.TMPDIR).toBeUndefined();
    expect(vi.mocked(spawnDetached).mock.calls.at(-1)![8]).toBeUndefined();
    await handle.waitForClose!();
    await handle.stop();
    expect(handle.waitForClose).toBe(waitForClose);
    expect(handle.stop).toBe(stop);
    expect(allocate).not.toHaveBeenCalled();
    expect(acl).not.toHaveBeenCalled();
  });

  it("preserves inherited TMPDIR and propagates pre-spawn refusal without scratch cleanup", async () => {
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    vi.stubEnv("TMPDIR", "caller-selected-temp");
    const allocate = vi.spyOn(scratch, "allocateRunTmpDirectory");
    vi.mocked(spawnDetached).mockRejectedValue(new WorkerNotStartedError("refused"));
    await expect(new ProcessTransport().launch({ id: "test", name: "test", cwd: process.cwd(),
      workerPath: path.resolve("custom-worker.mjs"), workerArguments: ["--status-file", path.resolve("run/status.json")] })).rejects.toThrow("refused");
    expect(vi.mocked(spawnDetached).mock.calls.at(-1)![4]!.TMPDIR).toBe("caller-selected-temp");
    expect(allocate).not.toHaveBeenCalled();
  });
});
