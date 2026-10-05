import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { canRemoveTerminalRun } from "../src/storage/retention.js";
import { createRunTmpDirectory, disposeRunTmpDirectory, runScratchExitVeto, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";

// Linux exposes birth identity and zombie state, so fixture teardown can confirm
// the orphan stopped writing without ever signaling an unowned/reused PID.
const identity = (pid: number): string | undefined => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    return fields[0] === "Z" ? undefined : fields[19];
  } catch (error) {
    if (["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) return undefined;
    throw error;
  }
};

const delegated = (() => {
  if (process.platform !== "linux") return false;
  try { const membership = fs.readFileSync("/proc/self/cgroup", "utf8").match(/^0::(\/.*)$/m)![1]!; fs.accessSync(path.join("/sys/fs/cgroup", membership), fs.constants.W_OK); return true; }
  catch { return false; }
})();

describe("scratch descendant custody (#369 F1)", () => {
  it.skipIf(process.platform === "win32")("keeps the persistent unresolved fence even if a writer removes its temporary directory", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custody-"));
    try {
      const tmp = createRunTmpDirectory(root);
      expect(fs.existsSync(path.join(root, UNRESOLVED_SCRATCH_FILE))).toBe(true);
      fs.rmSync(tmp, { recursive: true });
      expect(runScratchExitVeto(root)).toMatch(/complete descendant scope exit receipt/);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each([...new Set([process.platform, "win32" as const])])("fails closed when scratch custody inspection is unreadable (%s)", platform => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custody-unreadable-"));
    fs.mkdirSync(path.join(root, "tmp"));
    fs.writeFileSync(path.join(root, "tmp", "data"), "possibly live descendant");
    fs.writeFileSync(path.join(root, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647" }));
    vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    const fault = vi.spyOn(fs, "lstatSync").mockImplementation(() => { throw Object.assign(new Error("denied"), {code:"EACCES"}); });
    const removal = vi.spyOn(fs, "rmSync");
    try {
      if (platform === "win32") {
        // Scope cut: no Windows scratch inspection, including on unreadable IO.
        expect(runScratchExitVeto(root)).toBeUndefined();
        expect(disposeRunTmpDirectory(root)).toBe(false);
        expect(fault).not.toHaveBeenCalled();
      } else {
        expect(runScratchExitVeto(root)).toMatch(/inspection failed/);
      }
      expect(removal).not.toHaveBeenCalled();
      fault.mockRestore();
      expect(canRemoveTerminalRun(root)).toBe(false);
      expect(fs.readFileSync(path.join(root, "tmp", "data"), "utf8")).toBe("possibly live descendant");
    } finally {
      vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it("refuses offline collection of legacy scratch without a complete scope exit receipt", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custody-"));
    try {
      fs.mkdirSync(path.join(root, "tmp"));
      fs.writeFileSync(path.join(root, "tmp", "data"), "possibly live descendant");
      fs.writeFileSync(path.join(root, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647" }));
      expect(canRemoveTerminalRun(root)).toBe(false);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(!delegated).each([false, true])("preserves redirected background tool scratch after worker exit (crash=%s)", async crash => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-custody-"));
    const ready = path.join(root, "ready");
    const release = path.join(root, "release");
    const done = path.join(root, "done");
    const writer = path.join(root, "writer.mjs");
    const worker = path.join(root, "worker.mjs");
    fs.writeFileSync(writer, `import fs from "node:fs";
fs.writeFileSync(process.env.TMPDIR + "/live-writer", "ongoing work");
fs.writeFileSync(${JSON.stringify(ready)}, String(process.pid));
const timer = setInterval(() => {
  if (fs.existsSync(${JSON.stringify(release)})) {
    clearInterval(timer); fs.writeFileSync(${JSON.stringify(done)}, "exiting"); process.exit(0);
  }
}, 10);
setTimeout(() => { clearInterval(timer); process.exit(1); }, 15000).unref();
`);
    fs.writeFileSync(worker, `import fs from "node:fs";
import { spawn } from "node:child_process";
const args = new Map();
for (let i=2;i<process.argv.length;i+=2) args.set(process.argv[i],process.argv[i+1]);
const child = spawn(process.execPath, [${JSON.stringify(writer)}], {detached:true, stdio:"ignore", env:process.env});
child.unref();
while (!fs.existsSync(${JSON.stringify(ready)})) await new Promise(resolve => setTimeout(resolve,10));
const now=Date.now();
fs.writeFileSync(args.get("--status-file"), JSON.stringify({id:args.get("--id"),name:args.get("--name"),task:"background tool",status:${JSON.stringify(crash ? "failed" : "completed")},runner:"pi",transport:"process",sessionId:String(process.pid),cwd:args.get("--cwd"),startedAt:now,updatedAt:now,finishedAt:now,turns:1,toolCalls:1,text:"done",usage:{input:1,output:1,cacheRead:0,cacheWrite:0,cost:0}}));
${crash ? 'process.kill(process.pid, "SIGKILL");' : 'process.exit(0);'}
`);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, timeoutMs: 10000, sessionExport: false }, {
      workerPath: worker, runRoot: path.join(root, "runs"), piBinary: process.execPath,
    });
    let pid: number | undefined;
    let birth: string | undefined;
    let directory: string | undefined, scoped = false;
    try {
      const result = await manager.run({ task: "background tool", transport: "process" });
      pid = Number(fs.readFileSync(ready, "utf8")); birth = identity(pid);
      expect(birth).toBeDefined();
      directory = manager.runDirectory(result.id)!;
      scoped = JSON.parse(fs.readFileSync(path.join(directory, UNRESOLVED_SCRATCH_FILE), "utf8")).version === 2;
      expect(fs.existsSync(path.join(directory, "tmp", "live-writer"))).toBe(true);
      expect(canRemoveTerminalRun(directory)).toBe(false);
      await manager.close();
      expect(fs.existsSync(path.join(directory, "tmp", "live-writer"))).toBe(true);
      // This also covers another ordinary descendant with no nested Fabric record.
      expect(fs.existsSync(path.join(directory, "nested"))).toBe(false);
    } finally {
      fs.writeFileSync(release, "finish");
      if (pid === undefined && fs.existsSync(ready)) { pid = Number(fs.readFileSync(ready, "utf8")); birth = identity(pid); }
      if (pid !== undefined) {
        await vi.waitFor(() => expect(identity(pid!)).not.toBe(birth), { timeout: 17000, interval: 20 });
      }
      await manager.close();
      // The survivor's exit, not worker exit or terminal status, completes
      // the kernel scope. Offline cleanup may now reclaim its scratch.
      if (directory && scoped) {
        // Zombie observation can precede the kernel's final cgroup release.
        await vi.waitFor(() => expect(canRemoveTerminalRun(directory!)).toBe(true), { timeout: 2000, interval: 20 });
        expect(fs.existsSync(path.join(directory, "tmp"))).toBe(false);
      }
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 25000);
});
