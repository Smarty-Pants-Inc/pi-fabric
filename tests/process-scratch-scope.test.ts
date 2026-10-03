import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { allocateRunTmpDirectory, disposeRunTmpDirectory, runScratchExitVeto, JOINED_SCRATCH_FILE, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";
import { createProcessScratchScope, checkedProcessScratchScope, removeEmptyProcessScratchScope, type ProcessScratchScope } from "../src/storage/process-scratch-scope.js";
import * as scopes from "../src/storage/process-scratch-scope.js";
import { runTreeExitVeto } from "../src/storage/retention.js";

const roots: string[] = [], groups: ProcessScratchScope[] = [];
const sandbox = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scope-")); roots.push(root); return root; };
const delegated = (() => {
  if (process.platform !== "linux") return false;
  try { const relative = fs.readFileSync("/proc/self/cgroup", "utf8").match(/^0::(\/.*)$/m)![1]!; fs.accessSync(path.join("/sys/fs/cgroup",relative),fs.constants.W_OK); return true; } catch { return false; }
})();
afterEach(() => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const group of groups.splice(0)) removeEmptyProcessScratchScope(group);
  for (const root of roots.splice(0)) fs.rmSync(root,{recursive:true,force:true});
});

describe("identity-bound scratch scopes", () => {
  it.each(["LD_PRELOAD", "LD_AUDIT", "LD_LIBRARY_PATH", "LD_ORIGIN_PATH", "GCONV_PATH"])("does not issue contained custody when %s can execute before attachment", key => {
    vi.stubEnv(key,"synthetic-preload");
    expect(createProcessScratchScope()).toBeUndefined();
  });
  it("retains the legacy/uncontained fence when delegation is unavailable", () => {
    vi.spyOn(scopes,"createProcessScratchScope").mockReturnValue(undefined);
    const root=sandbox(), allocation=allocateRunTmpDirectory(root);
    expect(allocation.scope).toBeUndefined();
    expect(disposeRunTmpDirectory(root)).toBe(false);
    expect(runScratchExitVeto(root)).toMatch(/complete descendant scope/);
    expect(fs.existsSync(path.join(root,"tmp"))).toBe(true);
    // This caller knows it never attempted a spawn, unlike generic cleanup.
    allocation.neverStarted();
    expect(runScratchExitVeto(root)).toBeUndefined();
  });

  it.skipIf(!delegated)("does not treat an empty pre-attachment scope as a run-end receipt", () => {
    const root=sandbox(), allocation=allocateRunTmpDirectory(root);
    expect(allocation.scope).toBeDefined(); groups.push(allocation.scope!);
    expect(disposeRunTmpDirectory(root)).toBe(false);
    expect(disposeRunTmpDirectory(root, () => true)).toBe(false);
    expect(fs.existsSync(path.join(root,UNRESOLVED_SCRATCH_FILE))).toBe(true);
    expect(fs.existsSync(path.join(root,JOINED_SCRATCH_FILE))).toBe(false);
    allocation.neverStarted();
    expect(fs.existsSync(path.join(root,"tmp"))).toBe(false);
  });

  it.skipIf(!delegated).each(["nonce","boot","inode","missing"] as const)("refuses a stale or unproved attachment receipt (%s)", fault => {
    const root=sandbox(), allocation=allocateRunTmpDirectory(root), scope=allocation.scope!;
    expect(scope).toBeDefined(); groups.push(scope);
    const receipt = { ...scope };
    if (fault === "nonce") receipt.launchNonce="old-launch";
    if (fault === "boot") receipt.bootId="previous-boot";
    if (fault === "inode") receipt.ino++;
    if (fault !== "missing") fs.writeFileSync(path.join(root,JOINED_SCRATCH_FILE),JSON.stringify(receipt),{mode:0o600});
    expect(disposeRunTmpDirectory(root)).toBe(false);
    expect(fs.existsSync(path.join(root,"tmp"))).toBe(true);
    expect(fs.existsSync(path.join(root,UNRESOLVED_SCRATCH_FILE))).toBe(true);
  });

  it.skipIf(!delegated).each(["ambiguous", "changed-fence", "changed-joined", "unproved-prior"])("does not restore old custody after an unproved retry (%s)", fault => {
    const root=sandbox(), first=allocateRunTmpDirectory(root);expect(first.scope).toBeDefined();groups.push(first.scope!);
    const joined=path.join(root,JOINED_SCRATCH_FILE), fence=path.join(root,UNRESOLVED_SCRATCH_FILE);
    fs.writeFileSync(joined,JSON.stringify({...first.scope,...(fault==="unproved-prior"?{launchNonce:"unproved"}:{})}),{mode:0o600});
    const second=allocateRunTmpDirectory(root);
    if(fault==="changed-fence"){const text=fs.readFileSync(fence,"utf8");fs.renameSync(fence,fence+".saved");fs.writeFileSync(fence,text,{mode:0o600});}
    if(fault==="changed-joined"){const text=fs.readFileSync(joined,"utf8");fs.renameSync(joined,joined+".saved");fs.writeFileSync(joined,text,{mode:0o600});}
    if(fault!=="ambiguous")second.neverStarted();
    expect(JSON.parse(fs.readFileSync(fence,"utf8")).launchNonce).toBe(second.scope!.launchNonce);
    expect(disposeRunTmpDirectory(root)).toBe(false);expect(fs.existsSync(first.directory)).toBe(true);
  });

  it.skipIf(!delegated)("an unknown nested scope blocks retirement of the checked empty parent", () => {
    const root=sandbox(), first=allocateRunTmpDirectory(root);expect(first.scope).toBeDefined();groups.push(first.scope!);
    fs.writeFileSync(path.join(root,JOINED_SCRATCH_FILE),JSON.stringify(first.scope),{mode:0o600});
    fs.writeFileSync(path.join(root,"status.json"),JSON.stringify({status:"completed",transport:"process",sessionId:"2147483647"}));
    const child=path.join(root,"nested","child");fs.mkdirSync(child,{recursive:true,mode:0o700});
    fs.mkdirSync(path.join(child,"tmp"),{mode:0o700});
    fs.writeFileSync(path.join(child,UNRESOLVED_SCRATCH_FILE),JSON.stringify({version:2,runDirectory:child,scope:{...first.scope!,ino:first.scope!.ino+1},launchNonce:"unknown"}),{mode:0o600});
    // Main's persisted descendant identity fence precedes scratch inspection.
    expect(runTreeExitVeto(root,0,()=>false,true)).toMatch(/unknown descendant identity/);
    expect(fs.existsSync(first.scope!.directory)).toBe(true);expect(fs.existsSync(first.directory)).toBe(true);
    fs.writeFileSync(path.join(child,"status.json"),JSON.stringify({status:"completed",transport:"process",sessionId:"2147483647"}));
    expect(runTreeExitVeto(root,0,()=>false,true)).toMatch(/scratch/);
    expect(fs.existsSync(first.scope!.directory)).toBe(true);expect(fs.existsSync(first.directory)).toBe(true);
  });

  it.skipIf(!delegated)("never accepts a missing cgroup, reboot or replaced inode as emptiness", () => {
    const scope=createProcessScratchScope()!; expect(scope).toBeDefined(); groups.push(scope);
    expect(checkedProcessScratchScope({...scope,bootId:"old-boot"})).toBeUndefined();
    expect(checkedProcessScratchScope({...scope,ino:scope.ino+1})).toBeUndefined();
    expect(removeEmptyProcessScratchScope(scope)).toBe(true);
    expect(checkedProcessScratchScope(scope)).toBeUndefined();
    expect(removeEmptyProcessScratchScope(scope)).toBe(false);
  });
});
