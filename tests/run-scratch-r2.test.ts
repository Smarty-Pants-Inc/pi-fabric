import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { canRemoveTerminalRun } from "../src/storage/retention.js";

const roots: string[] = [], managers: AgentManager[] = [];
const sandbox = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r2-")); roots.push(root); return root; };
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const delegated = (() => {
  if (process.platform !== "linux") return false;
  try {
    const relative = fs.readFileSync("/proc/self/cgroup", "utf8").trim().match(/^0::(\/.*)$/m)![1]!;
    fs.accessSync(path.join("/sys/fs/cgroup", relative), fs.constants.W_OK);
    return true;
  } catch { return false; }
})();

describe("#369 F3 / D6 POSIX run-root custody", () => {
  it.skipIf(process.platform === "win32").each(
    ["explicit", "environment"].flatMap(source => ["root", "ancestor", "symlink"].map(unsafe => ({ source, unsafe }))),
  )("refuses unsafe $source $unsafe before run files or worker launch, without repair", async ({ source, unsafe }) => {
    const root = sandbox(), foreign = sandbox();
    let selected = path.join(root, "runs");
    fs.mkdirSync(selected, { mode: 0o700 });
    if (unsafe === "root") fs.chmodSync(selected, 0o777);
    if (unsafe === "ancestor") fs.chmodSync(root, 0o777);
    if (unsafe === "symlink") { fs.rmdirSync(selected); fs.symlinkSync(foreign, selected); selected = path.join(selected, "runs"); }
    const beforeRoot = fs.lstatSync(root).mode, beforeTarget = fs.lstatSync(foreign).mode;
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    if (source === "environment") vi.stubEnv("PI_FABRIC_RUN_ROOT", selected);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, sessionExport: false }, { ...(source === "explicit" ? { runRoot: selected } : {}), workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    managers.push(manager);
    await expect(manager.run({ task: "must not launch", transport: "process" })).rejects.toThrow(/unsafe|writable|real directory/i);
    expect(launch).not.toHaveBeenCalled();
    expect(fs.lstatSync(root).mode).toBe(beforeRoot);
    expect(fs.lstatSync(foreign).mode).toBe(beforeTarget);
    expect(fs.readdirSync(foreign)).toEqual([]);
    if (unsafe !== "symlink") expect(fs.readdirSync(selected)).toEqual([]);
  });
  it.skipIf(process.platform === "win32")("accepts a private explicit root beneath the safe sticky OS temp ancestor", async () => {
    const root = sandbox();
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, sessionExport: false }, { runRoot: path.join(root, "new", "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    managers.push(manager);
    expect((await manager.run({ task: "safe temp ancestor", transport: "process" })).status).toBe("completed");
  });
});

describe("#369 D4 joined kernel scratch scope", () => {
  it.skipIf(!delegated)("removes normal completed scratch at settlement, preserving status/logs and Main TMPDIR", async () => {
    const root = sandbox(), before = process.env.TMPDIR;
    const worker = path.join(root, "worker.mjs");
    fs.writeFileSync(worker, `import fs from "node:fs";
const args = new Map(); for (let i=2;i<process.argv.length;i+=2) args.set(process.argv[i],process.argv[i+1]);
const file = process.env.TMPDIR + "/child-file"; fs.writeFileSync(file,"private child scratch");
const now=Date.now(); fs.writeFileSync(args.get("--status-file"),JSON.stringify({id:args.get("--id"),name:args.get("--name"),task:"scratch",status:"completed",runner:"pi",transport:"process",cwd:args.get("--cwd"),startedAt:now,updatedAt:now,finishedAt:now,turns:1,toolCalls:1,text:JSON.stringify({tmpdir:process.env.TMPDIR,file,mode:fs.statSync(process.env.TMPDIR).mode & 0o777}),usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}}));`);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0, sessionExport: false }, { runRoot: path.join(root, "runs"), workerPath: worker });
    managers.push(manager);
    const result = await manager.run({ task: "scratch", transport: "process" });
    expect(result.status).toBe("completed");
    const run = manager.runDirectory(result.id)!, report = JSON.parse(result.text);
    expect(report.tmpdir).toBe(path.join(run,"tmp")); expect(report.mode).toBe(0o700);
    expect(fs.existsSync(report.file)).toBe(false); expect(fs.existsSync(report.tmpdir)).toBe(false);
    expect(fs.existsSync(path.join(run,"unresolved-scratch.json"))).toBe(false);
    expect(fs.existsSync(path.join(run,"status.json"))).toBe(true);
    expect(process.env.TMPDIR).toBe(before);
    expect(canRemoveTerminalRun(run)).toBe(true);
  });
});
