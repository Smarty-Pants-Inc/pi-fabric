import childProcess from "node:child_process";
import { scratchEvidence } from "./scratch-evidence.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";
import * as scopes from "../src/storage/process-scratch-scope.js";
import * as census from "../src/storage/scratch-process-census.js";
import { allocateRunTmpDirectory, disposeRunTmpDirectory, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";

const roots:string[]=[];
const sandbox=()=>{const root=fs.mkdtempSync(path.join(os.tmpdir(),"fabric-unscoped-"));roots.push(root);return root;};
afterEach(()=>{vi.restoreAllMocks();vi.unstubAllEnvs();for(const root of roots.splice(0))fs.rmSync(root,{recursive:true,force:true});});
const identity=(pid:number):string|undefined=>{try{const text=fs.readFileSync(`/proc/${pid}/stat`,"utf8"), fields=text.slice(text.lastIndexOf(")")+2).split(" ");return fields[0]==="Z"?undefined:fields[19];}catch(error){if(["ENOENT","ESRCH"].includes((error as NodeJS.ErrnoException).code??""))return;throw error;}};
const emptyCensus=(allocatedAt:number)=>{
  const birth=new Date(allocatedAt-60000).toUTCString().replace(/^(\w+), (\d+) (\w+) (\d+) (.*) GMT$/, "$1 $3 $2 $5 $4");
  return vi.spyOn(childProcess,"execFileSync").mockReturnValue(`${process.pid} ${process.ppid} ${process.getuid?.()??0} S ${birth} node\n` as never);
};

describe.skipIf(process.platform!=="linux")("unscoped generation-safe bounded-age disposal",()=>{
  it.each(["young","live-worker","unknown-worker","no-close-receipt","ambiguous-retry","root-replaced","scratch-replaced","lock-held","census-error","host-changed"])("preserves scratch on %s", fault=>{
    const root=sandbox();vi.spyOn(scopes,"createProcessScratchScope").mockReturnValue(undefined);
    const first=allocateRunTmpDirectory(root), allocatedAt=Date.now();
    fs.writeFileSync(path.join(root,"status.json"),JSON.stringify({status:"completed",transport:"process",sessionId:fault==="live-worker"?String(process.pid):fault==="unknown-worker"?"unknown":"2147483647",finishedAt:Date.now()}));
    if(fault!=="no-close-receipt")first.workerClosed(fault==="live-worker"?process.pid:2147483647);
    fs.writeFileSync(path.join(first.directory,"data"),"do not delete");
    if(fault==="ambiguous-retry")allocateRunTmpDirectory(root);
    if(fault==="root-replaced"){fs.renameSync(root,root+".saved");roots.push(root+".saved");fs.mkdirSync(root,{mode:0o700});fs.cpSync(root+".saved",root,{recursive:true});}
    if(fault==="scratch-replaced"){fs.renameSync(first.directory,first.directory+".saved");fs.mkdirSync(first.directory,{mode:0o700});fs.writeFileSync(path.join(first.directory,"data"),"do not delete");}
    if(fault==="lock-held")fs.mkdirSync(path.join(root,".scratch-custody-lock"));
    emptyCensus(allocatedAt);
    if(fault==="census-error")vi.mocked(childProcess.execFileSync).mockImplementation(()=>{throw new Error("query failed");});
    if(fault==="host-changed")vi.spyOn(census,"scratchHostEpoch").mockReturnValue({platform:"linux",hostname:"other-host",boot:"other-boot"});
    if(fault!=="young")vi.spyOn(Date,"now").mockReturnValue(allocatedAt+census.UNSCOPED_SCRATCH_RETENTION_MS+1000);
    expect(disposeRunTmpDirectory(root)).toBe(false);expect(fs.readFileSync(path.join(first.directory,"data"),"utf8")).toBe("do not delete");expect(fs.existsSync(path.join(root,UNRESOLVED_SCRATCH_FILE))).toBe(true);
  });
  it("joins normal manager success to native close, retaining scratch through shutdown until aged offline proof", async () => {
    const temp = sandbox(), root = fs.mkdtempSync(path.join(temp, "pi-fabric-runs-"));
    markRunRootActive(root);
    vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, budgetUsd: 0, sessionExport: false,
    }, { runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    try {
      const result = await manager.run({ task: "normal unscoped manager completion", transport: "process" });
      expect(result.status).toBe("completed");
      const run = manager.runDirectory(result.id)!;
      const fence = JSON.parse(fs.readFileSync(path.join(run, UNRESOLVED_SCRATCH_FILE), "utf8"));
      expect(fence).toMatchObject({ version: 3, closedPid: Number(result.sessionId) });
      expect(fence.closedAt).toBeGreaterThanOrEqual(fence.lastLaunchAt);
      expect(fs.existsSync(path.join(run, "tmp"))).toBe(true);
      await expect(manager.cleanup(result.id)).rejects.toThrow(/scratch writer exit is unconfirmed/);
      await manager.close();
      expect(fs.existsSync(path.join(run, "tmp"))).toBe(true);
      expect(JSON.parse(fs.readFileSync(path.join(run, UNRESOLVED_SCRATCH_FILE), "utf8"))).toEqual(fence);
      // Explicit roots have no automatic owner marker lifecycle. Model an
      // external owner's checked close; this alone must NOT authorize deletion.
      markRunRootClosed(root, Date.now(), true);
      const options = { tempRoot: temp, oneShotRunRetentionMs: 1, orphanedTempRunRetentionMs: 1, budgetMs: 1000 };
      expect(sweepTempRunRoots(options).removedRoots).toEqual([]);
      emptyCensus(fence.allocatedAt);
      vi.spyOn(Date, "now").mockReturnValue(Date.now() + census.UNSCOPED_SCRATCH_RETENTION_MS + 1000);
      expect(sweepTempRunRoots(options).removedRoots).toContain(root);
      expect(fs.existsSync(root)).toBe(false);
    } finally { await manager.close(); }
  });
  it("restores completed unscoped custody after a checked pre-spawn retry refusal, without resetting its retention age",()=>{
    const root=sandbox();vi.spyOn(scopes,"createProcessScratchScope").mockReturnValue(undefined);
    const first=allocateRunTmpDirectory(root);first.workerClosed(2147483647);
    fs.writeFileSync(path.join(root,"status.json"),JSON.stringify({status:"completed",transport:"process",sessionId:"2147483647",finishedAt:Date.now()}));
    const before=JSON.parse(fs.readFileSync(path.join(root,UNRESOLVED_SCRATCH_FILE),"utf8"));
    const retry=allocateRunTmpDirectory(root);retry.neverStarted();
    expect(JSON.parse(fs.readFileSync(path.join(root,UNRESOLVED_SCRATCH_FILE),"utf8"))).toEqual(before);
    emptyCensus(before.allocatedAt);vi.spyOn(Date,"now").mockReturnValue(Date.now()+census.UNSCOPED_SCRATCH_RETENTION_MS+1000);
    expect(disposeRunTmpDirectory(root)).toBe(true);
  });
  it("keeps an actual redirected scratch writer after the worker closes and age expires, until the writer is proved exited",async()=>{
    const project=sandbox(), run=path.join(project,"run"), ready=path.join(project,"ready"), release=path.join(project,"release"), writer=path.join(project,"writer.mjs"), worker=path.join(project,"worker.mjs"),status=path.join(run,"status.json");
    fs.mkdirSync(run,{mode:0o700});vi.spyOn(scopes,"createProcessScratchScope").mockReturnValue(undefined);
    fs.writeFileSync(writer,`import fs from "node:fs";const scratch=process.env.TMPDIR;delete process.env.TMPDIR;fs.writeFileSync(scratch+"/live","ordinary descendant");fs.writeFileSync(${JSON.stringify(ready)},String(process.pid));const timer=setInterval(()=>{if(fs.existsSync(${JSON.stringify(release)})){clearInterval(timer);process.exit(0);}},10);setTimeout(()=>process.exit(1),15000).unref();`);
    fs.writeFileSync(worker,`import fs from "node:fs";import {spawn} from "node:child_process";const helper=spawn(process.execPath,[${JSON.stringify(writer)}],{detached:true,stdio:"ignore",env:process.env});helper.unref();while(!fs.existsSync(${JSON.stringify(ready)}))await new Promise(r=>setTimeout(r,10));fs.writeFileSync(${JSON.stringify(status)},JSON.stringify({status:"completed",transport:"process",sessionId:String(process.pid),finishedAt:Date.now()}));`);
    let handle:Awaited<ReturnType<ProcessTransport["launch"]>>|undefined,pid:number|undefined,birth:string|undefined;
    try{
      handle=await new ProcessTransport().launch({id:"writer",name:"writer",cwd:project,workerPath:worker,workerArguments:["--status-file",status]});await handle.waitForClose!();expect(handle.lostContact?.()).toBeUndefined();
      const fence=JSON.parse(fs.readFileSync(path.join(run,UNRESOLVED_SCRATCH_FILE),"utf8"));expect(fence.version).toBe(3);expect(fence.closedPid).toBe(Number(handle.sessionId));
      pid=Number(fs.readFileSync(ready,"utf8"));birth=identity(pid);expect(birth).toBeDefined();
      vi.spyOn(Date,"now").mockReturnValue(Date.now()+census.UNSCOPED_SCRATCH_RETENTION_MS+1000);
      // The real complete ps query must see the child, even after it erased TMPDIR.
      expect(disposeRunTmpDirectory(run)).toBe(false);expect(fs.readFileSync(path.join(run,"tmp","live"),"utf8")).toBe("ordinary descendant");
      fs.writeFileSync(release,"finish");await vi.waitFor(()=>expect(identity(pid!)).not.toBe(birth),{timeout:17000,interval:20});
      // Isolate the positive census from unrelated concurrent fleet processes;
      // the writer's actual birth identity was already proved exited above.
      emptyCensus(fence.allocatedAt);expect(disposeRunTmpDirectory(run)).toBe(true);expect(fs.existsSync(path.join(run,"tmp"))).toBe(false);
      scratchEvidence("d4-unscoped-live-writer", { fence, workerPid: Number(handle.sessionId), helperPid: pid,
        helperBirthAtLiveCheck: birth, helperBirthAfterExit: identity(pid!) ?? null,
        negativeCensus: "actual native ps with surviving writer that erased TMPDIR",
        positiveCensus: "isolated mock after actual writer birth identity exited; concurrent fleet processes excluded",
        retentionClock: "injected 24-hour Date.now age, not actual 24-hour native elapsed time",
        scratchPreservedWhileHelperLive: true, helperExitConfirmed: true, scratchCollectedAfterProof: true });
    }finally{
      fs.writeFileSync(release,"finish");await handle?.stop();
      if(pid===undefined&&fs.existsSync(ready)){pid=Number(fs.readFileSync(ready,"utf8"));birth=identity(pid);}
      if(pid!==undefined&&birth!==undefined)await vi.waitFor(()=>expect(identity(pid!)).not.toBe(birth),{timeout:17000,interval:20});
    }
  },25000);
});
