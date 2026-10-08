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
const roots: string[] = [];
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
  subprocess.spawn.mockReset(); subprocess.execFile.mockReset();
});

describe("spawn router timeout settlement without close", () => {
  it.each(["linux", "win32"])("%s kills the process tree and settles even when a descendant holds close forever", async host => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "router-never-close-")); roots.push(dir);
    const child = Object.assign(new EventEmitter(), { pid: 123456, stdin: new PassThrough(), stdout: new PassThrough(), kill: vi.fn() });
    subprocess.spawn.mockReturnValue(child);
    const kill = vi.spyOn(process, "kill").mockReturnValue(true);
    // os/path keep native filesystem semantics; only select the adapter's kill branch.
    Object.defineProperty(process, "platform", { ...platform, value: host });
    vi.stubEnv("SystemRoot", "C:\\Windows");
    vi.useFakeTimers();
    const pending = routeAgentCreation({
      config: { command: [process.execPath], mode: "enforce", timeoutMs: 200 }, meshRoot: dir,
      kind: "spawn", role: "task-agent", cwd: dir, project: dir, parentId: "parent", task: "private task",
      defaults: { model: "provider/default", thinking: "medium" }, explicit: false, validateModel: model => model,
    });
    await vi.advanceTimersByTimeAsync(200);
    await expect(pending).resolves.toBeUndefined();
    const decision = JSON.parse(fs.readFileSync(path.join(dir, "router/decisions.jsonl"), "utf8"));
    expect(decision).toMatchObject({ error: "timeout", pick: null, actual: { model: "provider/default", thinking: "medium" } });
    expect(subprocess.spawn.mock.calls[0]![2]).toMatchObject({ detached: host !== "win32", shell: false });
    if (host === "win32") {
      expect(subprocess.execFile).toHaveBeenCalledWith("C:\\Windows\\System32\\taskkill.exe", ["/T", "/F", "/PID", "123456"],
        expect.objectContaining({ windowsHide: true, timeout: 1000, env: expect.objectContaining({ PATH: "/usr/bin:/bin" }) }), expect.any(Function));
      expect(kill).not.toHaveBeenCalled();
    } else expect(kill).toHaveBeenCalledWith(-123456, "SIGKILL");
    expect(child.stdin.destroyed).toBe(true); expect(child.stdout.destroyed).toBe(true);
    // A late close cannot change the logged fallback or start a second tree kill.
    child.emit("close", 0);
    expect(subprocess.execFile).toHaveBeenCalledTimes(host === "win32" ? 1 : 0);
  });
});
