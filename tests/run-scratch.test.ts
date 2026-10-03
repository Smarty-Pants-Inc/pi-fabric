import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { createRunTmpDirectory, safeRunTmpTree } from "../src/storage/run-scratch.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const sandbox = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-run-scratch-"));
  roots.push(root);
  return root;
};
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("runner-owned scratch", () => {
  it("creates an owner-only directory and reuses it for a retry of the same run", () => {
    const root = sandbox();
    const tmp = createRunTmpDirectory(root);
    expect(tmp).toBe(path.join(root, "tmp"));
    if (process.platform !== "win32") expect(fs.statSync(tmp).mode & 0o777).toBe(0o700);
    fs.writeFileSync(path.join(tmp, "keep-through-retry"), "scratch");
    expect(createRunTmpDirectory(root)).toBe(tmp);
    expect(fs.readFileSync(path.join(tmp, "keep-through-retry"), "utf8")).toBe("scratch");
  });

  it.skipIf(process.platform === "win32")("sets exact 0700 on its new allocation despite a restrictive umask", () => {
    const root = sandbox();
    const old = process.umask(0o777);
    try { expect(fs.statSync(createRunTmpDirectory(root)).mode & 0o777).toBe(0o700); }
    finally { process.umask(old); }
  });

  it("rejects symlink redirection without modifying the target", () => {
    const root = sandbox();
    const target = sandbox();
    fs.symlinkSync(target, path.join(root, "tmp"), "junction");
    expect(() => createRunTmpDirectory(root)).toThrow(/Unsafe.*scratch/);
    expect(fs.readdirSync(target)).toEqual([]);
  });

  it.skipIf(process.platform === "win32")("does not chmod or reuse an existing non-private directory", () => {
    const root = sandbox();
    fs.mkdirSync(path.join(root, "tmp"), { mode: 0o755 });
    expect(() => createRunTmpDirectory(root)).toThrow(/owner-only/);
    expect(fs.statSync(path.join(root, "tmp")).mode & 0o777).toBe(0o755);
  });

  it("keeps retention's arbitrary scratch walk bounded and refuses unsafe contents", () => {
    const tmp = createRunTmpDirectory(sandbox());
    fs.mkdirSync(path.join(tmp, "arbitrary"));
    fs.writeFileSync(path.join(tmp, "arbitrary", "suffix.any"), "scratch");
    expect(safeRunTmpTree(tmp, () => false)).toBe(true);
    expect(safeRunTmpTree(tmp, () => true)).toBe(false);
    expect(safeRunTmpTree(tmp, () => false, 33)).toBe(false);
    fs.symlinkSync(sandbox(), path.join(tmp, "linked"), "junction");
    expect(safeRunTmpTree(tmp, () => false)).toBe(false);
  });

  it("joins a replacement launch before disposing a proved-empty scope when stop races its handle", async () => {
    const root = sandbox();
    const worker = path.join(root, "retry-worker.mjs");
    const attempts = path.join(root, "attempts");
    fs.writeFileSync(worker, `import fs from "node:fs";
const args = new Map();
for (let i=2;i<process.argv.length;i+=2) args.set(process.argv[i],process.argv[i+1]);
const marker = ${JSON.stringify(attempts)};
const attempt = fs.existsSync(marker) ? Number(fs.readFileSync(marker,"utf8"))+1 : 1;
fs.writeFileSync(marker,String(attempt));
const failed = attempt === 1, now = Date.now();
fs.writeFileSync(process.env.TMPDIR + "/scratch", "still in use");
fs.writeFileSync(args.get("--status-file"), JSON.stringify({id:args.get("--id"),name:args.get("--name"),task:"retry writer",status:failed?"failed":"completed",runner:"pi",transport:"process",cwd:args.get("--cwd"),startedAt:now,updatedAt:now,finishedAt:now,turns:failed?0:1,toolCalls:0,text:"done",...(failed?{error:"No API key found for fake provider"}:{}),usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}}));
if (!failed) setInterval(() => {}, 1000);
`);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, timeoutMs: 20_000, sessionExport: false }, {
      workerPath: worker, runRoot: path.join(root, "runs"), piBinary: process.execPath,
    });
    managers.push(manager);
    const launch = ProcessTransport.prototype.launch;
    let entered!: () => void;
    const entering = new Promise<void>(resolve => { entered = resolve; });
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    let calls = 0;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const handle = await launch.call(this, request);
      if (++calls === 2) { entered(); await held; }
      return handle;
    });
    let stop: Promise<unknown> | undefined;
    try {
      const handle = await manager.spawn({ task: "retry writer", transport: "process" });
      const directory = manager.runDirectory(handle.id)!;
      await entering;
      await vi.waitFor(() => {
        const record = JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));
        expect(record.status).toBe("completed");
      });
      let stopped = false;
      stop = manager.stop(handle.id).then(result => { stopped = true; return result; });
      await new Promise(resolve => setTimeout(resolve, 150));
      expect(stopped).toBe(false);
      expect(fs.existsSync(path.join(directory, "tmp", "scratch"))).toBe(true);
      release();
      await stop;
      expect(fs.existsSync(path.join(directory, "tmp"))).toBe(false);
      expect(calls).toBe(2);
    } finally {
      release();
      await stop;
      await manager.close();
    }
  }, 20_000);

  it("joins actual worker exit before disposing scratch on scoped and unscoped hosts", async () => {
    const root = sandbox();
    const release = path.join(root, "release");
    const worker = path.join(root, "terminal-writer.mjs");
    fs.writeFileSync(worker, `import fs from "node:fs";
const args = new Map();
for (let i=2;i<process.argv.length;i+=2) args.set(process.argv[i],process.argv[i+1]);
const now = Date.now();
fs.writeFileSync(process.env.TMPDIR + "/scratch", "still in use");
fs.writeFileSync(args.get("--status-file"), JSON.stringify({id:args.get("--id"),name:args.get("--name"),task:"terminal writer",status:"completed",runner:"pi",transport:"process",cwd:args.get("--cwd"),startedAt:now,updatedAt:now,finishedAt:now,turns:1,toolCalls:0,text:"done",usage:{input:1,output:1,cacheRead:0,cacheWrite:0,cost:0}}));
const timer = setInterval(() => { if (fs.existsSync(${JSON.stringify(release)})) { clearInterval(timer); process.exit(0); } }, 20);
`);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, timeoutMs: 10_000, sessionExport: false }, { workerPath: worker, runRoot: path.join(root, "runs"), piBinary: process.execPath });
    managers.push(manager);
    const handle = await manager.spawn({ task: "terminal writer", transport: "process" });
    const directory = manager.runDirectory(handle.id)!;
    await vi.waitFor(() => expect(fs.existsSync(path.join(directory, "status.json"))).toBe(true));
    let completed = false;
    const result = manager.wait(handle.id).then(result => { completed = true; return result; });
    // Logical completion remains observable while the terminal writer is held.
    // It is not authority to dispose scratch or release its native custody.
    expect((await result).status).toBe("completed");
    expect(completed).toBe(true);
    expect(fs.existsSync(release)).toBe(false);
    expect(fs.existsSync(path.join(directory, "tmp", "scratch"))).toBe(true);
    fs.writeFileSync(release, "exit now");
    await vi.waitFor(() => expect(fs.existsSync(path.join(directory, "tmp"))).toBe(false), { timeout: 2000, interval: 20 });
    expect(fs.existsSync(path.join(directory, "status.json"))).toBe(true);
    expect(fs.existsSync(path.join(directory, "unresolved-scratch.json"))).toBe(false);
  });
});
