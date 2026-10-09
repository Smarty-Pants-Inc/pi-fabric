import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

const workerPath = path.resolve("dist/worker.js");
const available = process.platform === "linux" && fs.existsSync(workerPath) && fs.existsSync("/sys/fs/cgroup/cgroup.controllers") &&
  spawnSync("systemd-run", ["--user", "--scope", "--quiet", "--collect", "true"], { timeout: 5_000 }).status === 0;

describe.skipIf(!available)("compiled worker cgroup custody", () => {
  it("stop during scope handoff never releases or dispatches to the gated target", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-worker-cgroup-stop-"));
    const target = path.join(root, "target-ran"), piBinary = path.join(root, "pi.mjs");
    fs.writeFileSync(piBinary, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(target)},'executed');setInterval(()=>{},1000);`);
    const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    const stopMessage = vi.fn();
    let atReceipt!: () => void;
    const receipt = new Promise<void>(resolve => { atReceipt = resolve; });
    vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => {
      const child = actual.spawn(...args);
      if (child.send) {
        const send = child.send.bind(child) as (...args: unknown[]) => boolean;
        child.send = ((message: unknown, ...rest: unknown[]) => {
          if (message && typeof message === "object" && "type" in message && message.type === "fabric-execution-started-ack") {
            atReceipt(); // retain the real scope, deliberately withhold release ACK
            const callback = rest.at(-1);
            if (typeof callback === "function") callback(null);
            return true;
          }
          if (message && typeof message === "object" && "type" in message && message.type === "fabric-stop") stopMessage();
          return send(message, ...rest);
        }) as typeof child.send;
      }
      return child;
    });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000, budgetUsd: 0, deniedModels: [] }, {
      workerPath, piBinary, runRoot: path.join(root, "runs"),
    });
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const run = await manager.spawn({ task: "must not execute after stop", transport: "process" });
      await Promise.race([receipt, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("scope handoff was not reached")), 5_000);
      })]);
      clearTimeout(timer);
      expect(fs.existsSync(target)).toBe(false);
      const kill = vi.spyOn(process, "kill");
      const stopped = await manager.stop(run.id);
      expect(stopped.status, stopped.error).toBe("stopped");
      expect(stopMessage).toHaveBeenCalledOnce(); expect(kill).not.toHaveBeenCalled();
      expect(fs.existsSync(target)).toBe(false);
    } finally {
      clearTimeout(timer); vi.restoreAllMocks(); vi.mocked(spawn).mockImplementation(actual.spawn);
      await manager.close(); fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);

  it("ProcessTransport -> real worker -> gated execution transfers distinct scopes before inference and keeps hot checks and stop scan-free", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-worker-cgroup-"));
    const ready = path.join(root, "execution.json"), piBinary = path.join(root, "pi.mjs");
    fs.writeFileSync(piBinary, `import fs from 'node:fs';
      fs.writeFileSync(${JSON.stringify(ready)},JSON.stringify({pid:process.pid,cgroup:fs.readFileSync('/proc/self/cgroup','utf8')}));
      await import(${JSON.stringify(pathToFileURL(path.resolve("tests/fixtures/fake-pi.mjs")).href)});`);
    vi.stubEnv("FAKE_PI_BEHAVIOR", "hang");
    let workerPid = 0;
    const original = ProcessTransport.prototype.launch;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await original.call(this, request); workerPid = Number(handle.sessionId); return handle;
    });
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 30_000, budgetUsd: 0, deniedModels: [] }, {
      workerPath, piBinary, runRoot: path.join(root, "runs"),
    });
    try {
      const run = await manager.spawn({ task: "real cgroup custody", transport: "process" });
      await vi.waitFor(() => expect(fs.existsSync(ready)).toBe(true), { timeout: 5_000 });
      const execution = JSON.parse(fs.readFileSync(ready, "utf8")) as { pid: number; cgroup: string };
      const workerCgroup = fs.readFileSync(`/proc/${workerPid}/cgroup`, "utf8");
      expect(workerCgroup).toMatch(/fabric-worker-[0-9a-f-]+[.]scope/);
      expect(execution.cgroup).toMatch(/fabric-execution-[0-9a-f-]+[.]scope/);
      expect(execution.cgroup).not.toBe(workerCgroup);
      const scan = vi.spyOn(fs, "readdirSync");
      expect(scan.mock.calls.filter(call => String(call[0]) === "/proc")).toEqual([]);
      expect((await manager.stop(run.id)).status).toBe("stopped");
      expect(scan.mock.calls.filter(call => String(call[0]) === "/proc")).toEqual([]);
      await vi.waitFor(() => {
        try {
          const fields = fs.readFileSync(`/proc/${execution.pid}/stat`, "utf8").split(") ").at(-1)!.split(" ");
          expect(["Z", "X"]).toContain(fields[0]);
        } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      }, { timeout: 3_000 });
    } finally {
      launch.mockRestore(); vi.restoreAllMocks(); await manager.close(); vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20_000);
});
