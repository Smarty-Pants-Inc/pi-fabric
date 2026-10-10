import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";

const subprocess = vi.hoisted(() => ({ spawn: vi.fn(), execFile: vi.fn() }));
vi.mock("node:child_process", () => subprocess);
import { routeAgentCreation } from "../src/agents/spawn-router.js";

const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const environment = Object.getOwnPropertyDescriptor(process, "env")!;
const roots: string[] = [];
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  Object.defineProperty(process, "env", environment);
  vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
  subprocess.spawn.mockReset(); subprocess.execFile.mockReset();
});

const fixture = (host: string, inherited: NodeJS.ProcessEnv = { SystemRoot: "C:\\Windows" }) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "router-never-close-")); roots.push(dir);
  const child = Object.assign(new EventEmitter(), { pid: 123456, stdin: new PassThrough(), stdout: new PassThrough(), kill: vi.fn() });
  subprocess.spawn.mockReturnValue(child);
  const kill = vi.spyOn(process, "kill").mockReturnValue(true);
  // Keep native filesystem semantics; select only the environment/kill branches.
  Object.defineProperty(process, "platform", { ...platform, value: host });
  Object.defineProperty(process, "env", { ...environment, value: inherited });
  vi.useFakeTimers();
  const options = {
    config: { command: [process.execPath], mode: "enforce" as const, timeoutMs: 200 }, meshRoot: dir,
    kind: "spawn" as const, role: "task-agent", cwd: dir, project: dir, parentId: "parent", task: "private task",
    defaults: { model: "provider/default", thinking: "medium" as const }, explicit: false, validateModel: (model: string) => model,
  };
  const decision = () => JSON.parse(fs.readFileSync(path.join(dir, "router/decisions.jsonl"), "utf8"));
  return { dir, child, kill, options, decision };
};

const windowsEnv = (root: string) => ({
  SystemRoot: root, PATH: `${path.win32.join(root, "System32")};${root}`,
  COMSPEC: path.win32.join(root, "System32", "cmd.exe"),
});

describe("spawn router native retirement without close", () => {
  it.each(["linux", "win32"])("%s joins retirement even when a descendant holds close forever", async host => {
    const f = fixture(host);
    let helperClosed: (error: Error | null) => void = () => { throw new Error("helper not launched"); };
    subprocess.execFile.mockImplementation((_file, _args, _options, callback) => { helperClosed = callback; });
    let settled = false;
    const pending = routeAgentCreation(f.options).then(result => { settled = true; return result; });
    await vi.advanceTimersByTimeAsync(200);
    expect(settled).toBe(false);
    expect(subprocess.spawn.mock.calls[0]![2]).toMatchObject({ detached: host !== "win32", shell: false });
    if (host === "win32") {
      expect(subprocess.execFile).toHaveBeenCalledWith("C:\\Windows\\System32\\taskkill.exe", ["/T", "/F", "/PID", "123456"],
        expect.objectContaining({ windowsHide: true, timeout: 1000, killSignal: "SIGKILL", env: windowsEnv("C:\\Windows") }), expect.any(Function));
      expect(f.kill).not.toHaveBeenCalled();
      helperClosed(null);
      await vi.advanceTimersByTimeAsync(0);
      expect(settled).toBe(false); // Helper closure alone must not race direct child exit.
    } else expect(f.kill).toHaveBeenCalledWith(-123456, "SIGKILL");
    f.child.emit("exit", null, "SIGKILL");
    await expect(pending).resolves.toBeUndefined();
    expect(f.decision()).toMatchObject({ error: "timeout", pick: null, actual: f.options.defaults });
    expect(f.child.stdin.destroyed).toBe(true); expect(f.child.stdout.destroyed).toBe(true);
    // A late close cannot change the logged fallback or start a second tree kill.
    f.child.emit("close", 0);
    expect(subprocess.execFile).toHaveBeenCalledTimes(host === "win32" ? 1 : 0);
  });

  it.each(["aborted", "output-too-large"])("Windows %s waits for the tree helper before returning fallback", async error => {
    const f = fixture("win32"); const controller = new AbortController();
    let helperClosed: (error: Error | null) => void = () => { throw new Error("helper not launched"); };
    subprocess.execFile.mockImplementation((_file, _args, _options, callback) => { helperClosed = callback; });
    let settled = false;
    const pending = routeAgentCreation({ ...f.options, signal: controller.signal }).then(result => { settled = true; return result; });
    if (error === "aborted") controller.abort();
    else f.child.stdout.write("x".repeat(70000));
    await vi.advanceTimersByTimeAsync(0);
    f.child.emit("exit", null, "SIGKILL");
    expect(settled).toBe(false); // Direct exit alone must not leave a running helper behind.
    expect(fs.existsSync(path.join(f.dir, "router/decisions.jsonl"))).toBe(false);
    helperClosed(null);
    await expect(pending).resolves.toBeUndefined();
    expect(f.decision()).toMatchObject({ error, pick: null, actual: f.options.defaults });
    expect(subprocess.execFile).toHaveBeenCalledTimes(1);
  });

  it("falls back to direct SIGKILL if the Windows tree helper fails", async () => {
    const f = fixture("win32");
    f.child.kill.mockImplementation(() => { f.child.emit("exit", null, "SIGKILL"); return true; });
    subprocess.execFile.mockImplementation((_file, _args, _options, callback) => callback(new Error("private helper error")));
    const pending = routeAgentCreation(f.options);
    await vi.advanceTimersByTimeAsync(200);
    await expect(pending).resolves.toBeUndefined();
    expect(f.child.kill).toHaveBeenCalledWith("SIGKILL");
    expect(f.decision()).toMatchObject({ error: "timeout" });
    expect(JSON.stringify(f.decision())).not.toContain("private helper error");
  });

  it.each(["SystemRoot", "SYSTEMROOT", "systemroot"])("uses a minimal Windows environment with case-insensitive %s", async key => {
    const f = fixture("win32", {
      [key]: "D:\\Windows", Path: "Z:\\workspace", PATH: "Y:\\foreign", COMSPEC: "Z:\\foreign\\cmd.exe",
      GITHUB_TOKEN: "secret", NODE_OPTIONS: "--require=private", node_options: "private", HOME: "router-home", LANG: "C", TZ: "UTC",
    });
    subprocess.execFile.mockImplementation((_file, _args, _options, callback) => callback(null));
    const controller = new AbortController();
    const pending = routeAgentCreation({ ...f.options, signal: controller.signal });
    expect(subprocess.spawn.mock.calls[0]![2].env).toEqual({ ...windowsEnv("D:\\Windows"), HOME: "router-home", LANG: "C", TZ: "UTC" });
    controller.abort(); f.child.emit("exit", null, "SIGKILL");
    await expect(pending).resolves.toBeUndefined();
    expect(subprocess.execFile.mock.calls[0]![0]).toBe("D:\\Windows\\System32\\taskkill.exe");
    expect(subprocess.execFile.mock.calls[0]![2].env).toEqual(subprocess.spawn.mock.calls[0]![2].env);
  });

  it.each([undefined, "", "relative", "C:Windows"])("fails open without spawning for an invalid Windows system root %j", async root => {
    const f = fixture("win32", { SYSTEMROOT: root });
    await expect(routeAgentCreation(f.options)).resolves.toBeUndefined();
    expect(subprocess.spawn).not.toHaveBeenCalled(); expect(subprocess.execFile).not.toHaveBeenCalled();
    expect(f.decision()).toMatchObject({ error: "command-error", actual: f.options.defaults });
  });
});
