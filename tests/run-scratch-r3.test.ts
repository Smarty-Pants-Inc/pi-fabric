import childProcess from "node:child_process";
import { scratchEvidence } from "./scratch-evidence.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { allocateRunTmpDirectory, disposeRunTmpDirectory, JOINED_SCRATCH_FILE, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";
import * as scopes from "../src/storage/process-scratch-scope.js";
import { fabricDataRoot } from "../src/storage/temp-root.js";
import { canRemoveTerminalRun, markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";

const roots: string[] = [], managers: AgentManager[] = [];
const sandbox = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-r3-")); roots.push(root); return root; };
const delegated = (() => {
  if (process.platform !== "linux") return false;
  try { const relative = fs.readFileSync("/proc/self/cgroup", "utf8").match(/^0::(\/.*)$/m)![1]!; fs.accessSync(path.join("/sys/fs/cgroup", relative), fs.constants.W_OK); return true; } catch { return false; }
})();
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("#369 D7 OS-selected Darwin aliases", () => {
  it.skipIf(process.platform === "win32").each(["/var/folders/aa/private-user/T", "/private/var/folders/aa/private-user/T", "/tmp", "/private/tmp"])("checks the canonical namespace of %s before allocating a default run root", selected => {
    const original = Object.getOwnPropertyDescriptor(process, "platform")!;
    const real = fs.realpathSync.bind(fs), lstat = fs.lstatSync.bind(fs);
    const root = sandbox();
    const canonical = selected.replace(/^\/var(?=\/|$)/, "/private/var").replace(/^\/tmp$/, "/private/tmp");
    const stats = fs.lstatSync(root);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
    vi.spyOn(os, "tmpdir").mockReturnValue(selected);
    vi.spyOn(fs, "realpathSync").mockImplementation(((file: fs.PathLike) => String(file) === selected ? canonical : real(file)) as typeof fs.realpathSync);
    const checked: string[] = [];
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike) => {
      const name = String(file); checked.push(name);
      if (name.startsWith("/private") || name === "/var" || name === "/tmp") {
        return { ...stats, uid: name === canonical && canonical !== "/private/tmp" ? process.getuid!() : 0,
          mode: name === "/private/tmp" ? 0o41777 : 0o40700,
          isDirectory: () => name !== "/var" && name !== "/tmp", isSymbolicLink: () => name === "/var" || name === "/tmp" } as fs.Stats;
      }
      return lstat(file);
    }) as typeof fs.lstatSync);
    try {
      Object.defineProperty(process, "platform", { value: "darwin" });
      expect(fabricDataRoot()).toBe(canonical);
      expect(checked).toContain(canonical);
      expect(checked).not.toContain("/var");
    } finally { Object.defineProperty(process, "platform", original); }
  });
  it.skipIf(process.platform === "win32")("rejects a nonstandard default symlink and leaves explicit/env policy strict", () => {
    const root = sandbox(), target = path.join(root, "target"), alias = path.join(root, "alias");
    fs.mkdirSync(target, { mode: 0o700 }); fs.symlinkSync(target, alias);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined); vi.spyOn(os, "tmpdir").mockReturnValue(alias);
    expect(() => fabricDataRoot()).toThrow(/unsafe|real directory|alias/i);
    vi.stubEnv("PI_FABRIC_TMPDIR", alias);
    expect(() => fabricDataRoot()).toThrow(/real directory/);
  });
  it("launches with a checked native OS default and does not mutate Main TMPDIR", async () => {
    const before = process.env.TMPDIR;
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0, sessionExport: false }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    managers.push(manager);
    const result = await manager.run({ task: "OS-selected default", transport: "process" });
    expect(result.status).toBe("completed");
    const run = manager.runDirectory(result.id)!; roots.push(path.dirname(run));
    expect(fs.realpathSync(run)).toBe(run);
    expect(process.env.TMPDIR).toBe(before);
    scratchEvidence("d7-native-default", { runDirectory: run, osSelectedDefault: os.tmpdir(),
      canonicalRunDirectory: fs.realpathSync(run), mode: fs.statSync(run).mode & 0o777,
      mainTmpdirBefore: before ?? null, mainTmpdirAfter: process.env.TMPDIR ?? null,
      workerStatus: result.status, nativeDarwin: process.platform === "darwin", factoryLauncher: false });
  });
});

describe("#369 D9 contained retry never-started receipt", () => {
  it.skipIf(!delegated).each(["authorization", "runtime"] as const)("preserves the first actual joined generation after second-attempt %s refusal", async failure => {
    const project = sandbox(), root = path.join(project, "run"), worker = path.join(project, "first.mjs"), status = path.join(root, "status.json");
    fs.mkdirSync(root, { mode: 0o700 });
    fs.writeFileSync(worker, `import fs from "node:fs"; fs.writeFileSync(${JSON.stringify(status)}, JSON.stringify({status:"failed",transport:"process",sessionId:String(process.pid),finishedAt:1}));`);
    const first = allocateRunTmpDirectory(root); expect(first.scope).toBeDefined();
    let handle: Awaited<ReturnType<typeof spawnDetached>> | undefined;
    try {
      handle = await spawnDetached(worker, [], root, undefined, { ...process.env, TMPDIR: first.directory }, first.scope);
      await handle.waitForClose(); expect(handle.lostContact()).toBeUndefined();
      const joined = fs.readFileSync(path.join(root, JOINED_SCRATCH_FILE), "utf8");
      const fence = JSON.parse(fs.readFileSync(path.join(root, UNRESOLVED_SCRATCH_FILE), "utf8"));
      if (failure === "runtime") { vi.stubEnv("PATH", ""); vi.stubEnv("PI_FABRIC_NODE_BINARY", ""); }
      await expect(new ProcessTransport().launch({ id:"retry",name:"retry",cwd:root,workerPath:path.join(root, failure === "runtime" ? "second.ts" : "second.mjs"),workerArguments:["--status-file", status],authorize:()=>false })).rejects.toMatchObject({ name:"WorkerNotStartedError" });
      expect(fs.readFileSync(path.join(root, JOINED_SCRATCH_FILE), "utf8")).toBe(joined);
      expect(JSON.parse(fs.readFileSync(path.join(root, UNRESOLVED_SCRATCH_FILE), "utf8")).launchNonce).toBe(fence.launchNonce);
      expect(canRemoveTerminalRun(root)).toBe(true);
      expect(fs.existsSync(first.directory)).toBe(false);
      scratchEvidence("d9-retry-" + failure, { workerPid: handle.pid, scope: first.scope,
        firstFence: fence, priorJoined: JSON.parse(joined), refusal: failure,
        priorJoinedGenerationRestored: true, scratchCollected: true,
        scopeRemoved: !fs.existsSync(first.scope!.directory) });
    } finally { await handle?.stop(); if (first.scope) scopes.removeEmptyProcessScratchScope(first.scope); }
  });
});

describe("#369 unscoped main-compatible collection", () => {
  it.skipIf(process.platform !== "linux")("collects completed unscoped scratch immediately without querying potential holders", async () => {
    const temp = sandbox(), root = fs.mkdtempSync(path.join(temp, "pi-fabric-runs-")), run = path.join(root, "done");
    fs.mkdirSync(run, { mode: 0o700 }); markRunRootActive(root, Date.now() - 10);
    vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
    const worker = path.join(temp, "unscoped-worker.mjs"), status = path.join(run, "status.json");
    fs.writeFileSync(worker, `import fs from "node:fs";fs.writeFileSync(process.env.TMPDIR+"/data","finished scratch");fs.writeFileSync(${JSON.stringify(status)},JSON.stringify({status:"completed",transport:"process",sessionId:String(process.pid),finishedAt:Date.now()}));`);
    const handle = await new ProcessTransport().launch({ id: "done", name: "done", cwd: temp, workerPath: worker, workerArguments: ["--status-file", status] });
    await handle.waitForClose!(); expect(handle.lostContact?.()).toBeUndefined();
    const census = vi.spyOn(childProcess, "execFileSync");
    expect(disposeRunTmpDirectory(run)).toBe(true);
    expect(census).not.toHaveBeenCalled();
    markRunRootClosed(root, Date.now() - 2, true);
    const sweep = sweepTempRunRoots({ tempRoot: temp, oneShotRunRetentionMs: 1, orphanedTempRunRetentionMs: 1, now: Date.now() + 10, budgetMs: 1000 });
    expect(sweep.removedRoots).toContain(root);
    expect(fs.existsSync(root)).toBe(false);
  });
});

const identity = (pid:number): string | undefined => {
  try { const stat=fs.readFileSync(`/proc/${pid}/stat`,"utf8"), fields=stat.slice(stat.lastIndexOf(")")+2).split(" "); return fields[0] === "Z" ? undefined : fields[19]; }
  catch (error) { if (["ENOENT","ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) return; throw error; }
};

describe("#369 D8 nested scope retirement", () => {
  it.skipIf(!delegated)("offline sweep retires nested scopes bottom-up only after the surviving redirected tool exits", async () => {
    const temp=sandbox(), root=fs.mkdtempSync(path.join(temp,"pi-fabric-runs-")), run=path.join(root,"parent"), child=path.join(run,"nested","child");
    fs.mkdirSync(run,{mode:0o700}); markRunRootActive(root,1);
    const release=path.join(temp,"release"), ready=path.join(temp,"ready"), writer=path.join(temp,"writer.mjs"), nestedWorker=path.join(temp,"nested.mjs"), parentWorker=path.join(temp,"parent.ts");
    fs.writeFileSync(writer,`import fs from "node:fs"; fs.writeFileSync(process.env.TMPDIR+"/live", "nested survivor"); fs.writeFileSync(${JSON.stringify(ready)},String(process.pid)); const timer=setInterval(()=>{if(fs.existsSync(${JSON.stringify(release)})){clearInterval(timer);process.exit(0);}},10);setTimeout(()=>process.exit(1),15000).unref();`);
    fs.writeFileSync(nestedWorker,`import fs from "node:fs";import {spawn} from "node:child_process";const helper=spawn(${JSON.stringify(process.execPath)},[${JSON.stringify(writer)}],{detached:true,stdio:"ignore",env:process.env});helper.unref();while(!fs.existsSync(${JSON.stringify(ready)}))await new Promise(r=>setTimeout(r,10));fs.writeFileSync(${JSON.stringify(path.join(child,"status.json"))},JSON.stringify({status:"completed",transport:"process",sessionId:String(process.pid),finishedAt:1}));`);
    fs.writeFileSync(parentWorker,`import fs from "node:fs";
import {allocateRunTmpDirectory} from ${JSON.stringify(path.resolve("src/storage/run-scratch.ts"))};
import {spawnDetached} from ${JSON.stringify(path.resolve("src/agents/transports/process-utils.ts"))};
fs.mkdirSync(${JSON.stringify(child)},{recursive:true,mode:0o700});const allocation=allocateRunTmpDirectory(${JSON.stringify(child)});if(!allocation.scope)throw new Error("nested delegation unavailable");const handle=await spawnDetached(${JSON.stringify(nestedWorker)},[],${JSON.stringify(temp)},undefined,{...process.env,TMPDIR:allocation.directory},allocation.scope);await handle.waitForClose();fs.writeFileSync(${JSON.stringify(path.join(run,"status.json"))},JSON.stringify({status:"completed",transport:"process",sessionId:String(process.pid),finishedAt:1}));`);
    const allocation=allocateRunTmpDirectory(run);expect(allocation.scope).toBeDefined();
    let handle:Awaited<ReturnType<typeof spawnDetached>>|undefined, childScope:scopes.ProcessScratchScope|undefined, pid:number|undefined, birth:string|undefined;
    const sweep=()=>sweepTempRunRoots({tempRoot:temp,now:Date.now()+100000,oneShotRunRetentionMs:1,orphanedTempRunRetentionMs:1,budgetMs:1000});
    try {
      handle=await spawnDetached(parentWorker,[],temp,undefined,{...process.env,TMPDIR:allocation.directory},allocation.scope);await handle.waitForClose();
      expect(fs.existsSync(path.join(run,"status.json"))).toBe(true);
      childScope=JSON.parse(fs.readFileSync(path.join(child,UNRESOLVED_SCRATCH_FILE),"utf8")).scope;
      expect(childScope!.directory.startsWith(allocation.scope!.directory+"/")).toBe(true);
      pid=Number(fs.readFileSync(ready,"utf8"));birth=identity(pid);expect(birth).toBeDefined();
      markRunRootClosed(root,2,true);expect(sweep().removedRoots).toEqual([]);
      expect(fs.existsSync(path.join(child,"tmp","live"))).toBe(true);
      fs.writeFileSync(release,"finish");await vi.waitFor(()=>expect(identity(pid!)).not.toBe(birth),{timeout:17000,interval:20});
      await vi.waitFor(()=>expect(sweep().removedRoots).toContain(root),{timeout:3000,interval:20});
      expect(fs.existsSync(root)).toBe(false);expect(fs.existsSync(childScope!.directory)).toBe(false);expect(fs.existsSync(allocation.scope!.directory)).toBe(false);
      scratchEvidence("d8-nested-offline", { parentScope: allocation.scope, nestedScope: childScope,
        parentWorkerPid: handle.pid, helperPid: pid, helperBirthAtLiveCheck: birth,
        helperBirthAfterExit: identity(pid!) ?? null, scratchPreservedWhileHelperLive: true,
        helperExitConfirmed: true, parentScopeRemoved: true, nestedScopeRemoved: true, runRootCollected: true });
    } finally {
      await handle?.stop();fs.writeFileSync(release,"finish");
      if(pid===undefined && fs.existsSync(ready)){pid=Number(fs.readFileSync(ready,"utf8"));birth=identity(pid);}
      if(pid!==undefined && birth!==undefined)await vi.waitFor(()=>expect(identity(pid!)).not.toBe(birth),{timeout:17000,interval:20});
      if(childScope)scopes.removeEmptyProcessScratchScope(childScope);if(allocation.scope)scopes.removeEmptyProcessScratchScope(allocation.scope);
    }
  },25000);
});
