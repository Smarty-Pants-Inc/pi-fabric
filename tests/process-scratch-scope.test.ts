import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { allocateRunTmpDirectory, disposeRunTmpDirectory, runScratchExitVeto, JOINED_SCRATCH_FILE, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";
import { createProcessScratchScope, checkedProcessScratchScope, removeEmptyProcessScratchScope, type ProcessScratchScope } from "../src/storage/process-scratch-scope.js";
import * as scopes from "../src/storage/process-scratch-scope.js";

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
  it.each(["NODE_OPTIONS", "BUN_OPTIONS", "LD_PRELOAD", "LD_AUDIT"])("does not issue contained custody when %s can execute before attachment", key => {
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

  it.skipIf(!delegated)("never accepts a missing cgroup, reboot or replaced inode as emptiness", () => {
    const scope=createProcessScratchScope()!; expect(scope).toBeDefined(); groups.push(scope);
    expect(checkedProcessScratchScope({...scope,bootId:"old-boot"})).toBeUndefined();
    expect(checkedProcessScratchScope({...scope,ino:scope.ino+1})).toBeUndefined();
    expect(removeEmptyProcessScratchScope(scope)).toBe(true);
    expect(checkedProcessScratchScope(scope)).toBeUndefined();
    expect(removeEmptyProcessScratchScope(scope)).toBe(false);
  });
});
