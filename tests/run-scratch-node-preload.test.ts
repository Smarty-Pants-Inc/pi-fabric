import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { allocateRunTmpDirectory, disposeRunTmpDirectory } from "../src/storage/run-scratch.js";
import { removeEmptyProcessScratchScope } from "../src/storage/process-scratch-scope.js";
import { scratchEvidence } from "./scratch-evidence.js";

const delegated = (() => {
  if (process.platform !== "linux") return false;
  try { const membership=fs.readFileSync("/proc/self/cgroup","utf8").match(/^0::(\/.*)$/m)![1]!;fs.accessSync(path.join("/sys/fs/cgroup",membership),fs.constants.W_OK);return true; } catch {return false;}
})();
const identity=(pid:number):string|undefined=>{try{const text=fs.readFileSync(`/proc/${pid}/stat`,"utf8"), fields=text.slice(text.lastIndexOf(")")+2).split(" ");return fields[0]==="Z"?undefined:fields[19];}catch(error){if(["ENOENT","ESRCH"].includes((error as NodeJS.ErrnoException).code??""))return;throw error;}};

describe("runtime environment preloads run after scratch attachment",()=>{
  it.skipIf(!delegated)("contains an ordinary NODE_OPTIONS preload's surviving redirected helper",async()=>{
    const project=fs.mkdtempSync(path.join(os.tmpdir(),"fabric-node-preload-")), run=path.join(project,"run"), ready=path.join(project,"ready"), release=path.join(project,"release"), worker=path.join(project,"worker.mjs"), writer=path.join(project,"writer.mjs"), preload=path.join(project,"preload.mjs");
    fs.mkdirSync(run,{mode:0o700});
    fs.writeFileSync(writer,`import fs from "node:fs";fs.writeFileSync(process.env.TMPDIR+"/live","node preload writer");fs.writeFileSync(${JSON.stringify(ready)},JSON.stringify({pid:process.pid,cgroup:fs.readFileSync("/proc/self/cgroup","utf8").trim(),tmpdir:process.env.TMPDIR}));const timer=setInterval(()=>{if(fs.existsSync(${JSON.stringify(release)})){clearInterval(timer);process.exit(0);}},10);setTimeout(()=>process.exit(1),15000).unref();`);
    fs.writeFileSync(preload,`import {spawn} from "node:child_process";const helper=spawn(process.execPath,[${JSON.stringify(writer)}],{detached:true,stdio:"ignore",env:{...process.env,NODE_OPTIONS:""}});helper.unref();`);
    fs.writeFileSync(worker,`import fs from "node:fs";while(!fs.existsSync(${JSON.stringify(ready)}))await new Promise(r=>setTimeout(r,10));`);
    const prior=process.env.NODE_OPTIONS;
    let allocation:ReturnType<typeof allocateRunTmpDirectory>|undefined,handle:Awaited<ReturnType<typeof spawnDetached>>|undefined,pid:number|undefined,birth:string|undefined;
    try{
      process.env.NODE_OPTIONS=`--import=${preload}`;
      allocation=allocateRunTmpDirectory(run);expect(allocation.scope).toBeDefined();
      handle=await spawnDetached(worker,[],project,undefined,{...process.env,TMPDIR:allocation.directory},allocation.scope);await handle.waitForClose();expect(handle.lostContact()).toBeUndefined();
      const helper=JSON.parse(fs.readFileSync(ready,"utf8"));pid=helper.pid;birth=identity(pid!);expect(birth).toBeDefined();
      expect(helper.cgroup).toBe("0::"+allocation.scope!.directory.slice("/sys/fs/cgroup".length));
      expect(disposeRunTmpDirectory(run)).toBe(false);expect(fs.readFileSync(path.join(allocation.directory,"live"),"utf8")).toBe("node preload writer");
      fs.writeFileSync(release,"finish");await vi.waitFor(()=>expect(disposeRunTmpDirectory(run)).toBe(true),{timeout:3000,interval:20});
      expect(fs.existsSync(allocation.directory)).toBe(false);
      scratchEvidence("f4-node-options-preload",{scope:allocation.scope,workerPid:handle.pid,helper,helperBirthAtLiveCheck:birth,helperBirthAfterExit:identity(pid!)??null,configuredHook:"NODE_OPTIONS --import",scratchPreservedWhileHelperLive:true,scratchCollectedAfterHelperExit:true});
    }finally{
      if(prior===undefined)delete process.env.NODE_OPTIONS;else process.env.NODE_OPTIONS=prior;
      await handle?.stop();fs.writeFileSync(release,"finish");
      if(pid===undefined&&fs.existsSync(ready)){pid=JSON.parse(fs.readFileSync(ready,"utf8")).pid;birth=identity(pid!);}
      if(pid!==undefined&&birth!==undefined)await vi.waitFor(()=>expect(identity(pid!)).not.toBe(birth),{timeout:17000,interval:20});
      if(allocation?.scope)removeEmptyProcessScratchScope(allocation.scope);
      fs.rmSync(project,{recursive:true,force:true});
    }
  },25000);
});
