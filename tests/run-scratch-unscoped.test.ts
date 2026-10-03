import childProcess from "node:child_process";
import { scratchEvidence } from "./scratch-evidence.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import * as scopes from "../src/storage/process-scratch-scope.js";
import * as processIdentity from "../src/residency/process-identity.js";
import { allocateRunTmpDirectory, disposeRunTmpDirectory, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";

const roots: string[] = [];
const sandbox = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-unscoped-")); roots.push(root); return root; };
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const terminal = (root: string, sessionId = "2147483647", status = "completed") => fs.writeFileSync(path.join(root, "status.json"), JSON.stringify({ status, transport: "process", sessionId, finishedAt: Date.now() }));
const identity = (pid: number): string | undefined => {
  try { const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8"), fields = text.slice(text.lastIndexOf(")") + 2).split(" "); return fields[0] === "Z" ? undefined : fields[19]; }
  catch (error) { if (["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) return; throw error; }
};

describe.skipIf(process.platform !== "linux")("unscoped main-compatible terminal collection", () => {
  it.each(["live-worker", "unknown-worker", "nonterminal", "unknown-exit", "ambiguous-retry", "root-replaced", "scratch-replaced", "lock-held", "unresolved-worker", "unsafe-content"])("preserves custody on %s", fault => {
    const root = sandbox(); vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
    const first = allocateRunTmpDirectory(root);
    terminal(root, fault === "live-worker" ? String(process.pid) : fault === "unknown-worker" || fault === "unknown-exit" ? "unknown" : "2147483647", fault === "nonterminal" ? "running" : "completed");
    if (fault !== "unknown-exit") first.workerClosed(fault === "live-worker" ? process.pid : 2147483647);
    fs.writeFileSync(path.join(first.directory, "data"), "do not delete");
    if (fault === "ambiguous-retry") allocateRunTmpDirectory(root);
    if (fault === "root-replaced") { fs.renameSync(root, root + ".saved"); roots.push(root + ".saved"); fs.mkdirSync(root, { mode: 0o700 }); fs.cpSync(root + ".saved", root, { recursive: true }); }
    if (fault === "scratch-replaced") { fs.renameSync(first.directory, first.directory + ".saved"); fs.mkdirSync(first.directory, { mode: 0o700 }); fs.writeFileSync(path.join(first.directory, "data"), "do not delete"); }
    if (fault === "lock-held") fs.mkdirSync(path.join(root, ".scratch-custody-lock"));
    if (fault === "unresolved-worker") fs.writeFileSync(path.join(root, "unresolved-worker.json"), "{}");
    if (fault === "unsafe-content") fs.symlinkSync(sandbox(), path.join(first.directory, "foreign"));
    expect(disposeRunTmpDirectory(root)).toBe(false);
    expect(fs.readFileSync(path.join(first.directory, "data"), "utf8")).toBe("do not delete");
    expect(fs.existsSync(path.join(root, UNRESOLVED_SCRATCH_FILE))).toBe(true);
  });

  it.each(["completed", "failed", "stopped", "timed_out"])("collects fresh %s scratch without an age gate or holder census", status => {
    const root = sandbox(); vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
    const allocation = allocateRunTmpDirectory(root); terminal(root, "2147483647", status); allocation.workerClosed(2147483647);
    const census = vi.spyOn(childProcess, "execFileSync").mockImplementation(() => { throw new Error("No census allowed by compatibility policy"); });
    expect(disposeRunTmpDirectory(root)).toBe(true);
    expect(census).not.toHaveBeenCalled(); expect(fs.existsSync(allocation.directory)).toBe(false);
    expect(fs.existsSync(path.join(root, "status.json"))).toBe(true);
  });

  it("joins normal manager success to native close and immediately releases cleanup/root closure", async () => {
    const temp = sandbox(); vi.spyOn(os, "tmpdir").mockReturnValue(temp); vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, budgetUsd: 0, sessionExport: false }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    try {
      const result = await manager.run({ task: "normal unscoped manager completion", transport: "process" }); expect(result.status).toBe("completed");
      const run = manager.runDirectory(result.id)!, root = path.dirname(run);
      await vi.waitFor(() => expect(fs.existsSync(path.join(run, "tmp"))).toBe(false), { timeout: 2000, interval: 20 });
      expect(fs.existsSync(path.join(run, UNRESOLVED_SCRATCH_FILE))).toBe(false);
      expect(await manager.cleanup(result.id)).toMatchObject({ cleaned: true }); expect(fs.existsSync(run)).toBe(false);
      await manager.close(); expect(fs.existsSync(root)).toBe(false);
    } finally { await manager.close(); }
  });

  it("restores completed custody after a checked pre-spawn retry refusal and collects immediately", () => {
    const root = sandbox(); vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
    const first = allocateRunTmpDirectory(root); first.workerClosed(2147483647); terminal(root);
    const before = JSON.parse(fs.readFileSync(path.join(root, UNRESOLVED_SCRATCH_FILE), "utf8"));
    const retry = allocateRunTmpDirectory(root); retry.neverStarted();
    expect(JSON.parse(fs.readFileSync(path.join(root, UNRESOLVED_SCRATCH_FILE), "utf8"))).toEqual(before);
    expect(disposeRunTmpDirectory(root)).toBe(true);
  });

  it("collects a terminal first-generation orphan without an owner-recorded native close, as main does", () => {
    const root = sandbox(); vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
    const allocation = allocateRunTmpDirectory(root); terminal(root);
    expect(JSON.parse(fs.readFileSync(path.join(root, UNRESOLVED_SCRATCH_FILE), "utf8")).closedPid).toBeUndefined();
    expect(disposeRunTmpDirectory(root)).toBe(true); expect(fs.existsSync(allocation.directory)).toBe(false);
  });

  it("uses main's checked PID-birth rule without confusing a reused PID with the worker", () => {
    const root = sandbox(); vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
    const allocation = allocateRunTmpDirectory(root);
    fs.writeFileSync(path.join(root, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: String(process.pid), processStartTime: "1234" }));
    const birth = vi.spyOn(processIdentity, "processStartTime").mockReturnValue("1234");
    expect(disposeRunTmpDirectory(root)).toBe(false);
    birth.mockReturnValue("5678");
    expect(disposeRunTmpDirectory(root)).toBe(true); expect(fs.existsSync(allocation.directory)).toBe(false);
  });

  it("documents the scope cut: native worker close is not unscoped detached-writer exit proof", async () => {
    const project = sandbox(), run = path.join(project, "run"), ready = path.join(project, "ready"), release = path.join(project, "release"), writer = path.join(project, "writer.mjs"), worker = path.join(project, "worker.mjs"), status = path.join(run, "status.json");
    fs.mkdirSync(run, { mode: 0o700 }); vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
    fs.writeFileSync(writer, `import fs from "node:fs";const scratch=process.env.TMPDIR;delete process.env.TMPDIR;fs.writeFileSync(scratch+"/live","ordinary descendant");fs.writeFileSync(${JSON.stringify(ready)},String(process.pid));const timer=setInterval(()=>{if(fs.existsSync(${JSON.stringify(release)})){clearInterval(timer);process.exit(0);}},10);setTimeout(()=>process.exit(1),15000).unref();`);
    fs.writeFileSync(worker, `import fs from "node:fs";import {spawn} from "node:child_process";const helper=spawn(process.execPath,[${JSON.stringify(writer)}],{detached:true,stdio:"ignore",env:process.env});helper.unref();while(!fs.existsSync(${JSON.stringify(ready)}))await new Promise(r=>setTimeout(r,10));fs.writeFileSync(${JSON.stringify(status)},JSON.stringify({status:"completed",transport:"process",sessionId:String(process.pid),finishedAt:Date.now()}));`);
    let handle: Awaited<ReturnType<ProcessTransport["launch"]>> | undefined, pid: number | undefined, birth: string | undefined;
    try {
      handle = await new ProcessTransport().launch({ id: "writer", name: "writer", cwd: project, workerPath: worker, workerArguments: ["--status-file", status] }); await handle.waitForClose!(); expect(handle.lostContact?.()).toBeUndefined();
      const fence = JSON.parse(fs.readFileSync(path.join(run, UNRESOLVED_SCRATCH_FILE), "utf8")); expect(fence.closedPid).toBe(Number(handle.sessionId));
      pid = Number(fs.readFileSync(ready, "utf8")); birth = identity(pid); expect(birth).toBeDefined();
      expect(disposeRunTmpDirectory(run)).toBe(true); expect(fs.existsSync(path.join(run, "tmp"))).toBe(false); expect(identity(pid)).toBe(birth);
      scratchEvidence("unscoped-scope-cut", { fence, workerPid: Number(handle.sessionId), helperPid: pid, helperBirthWhileCollected: birth, immediateCollection: true, descendantExitProved: false, deferredPolicy: "smarty-dev#4010 D4" });
    } finally {
      fs.writeFileSync(release, "finish"); await handle?.stop();
      if (pid === undefined && fs.existsSync(ready)) { pid = Number(fs.readFileSync(ready, "utf8")); birth = identity(pid); }
      if (pid !== undefined && birth !== undefined) await vi.waitFor(() => expect(identity(pid!)).not.toBe(birth), { timeout: 17000, interval: 20 });
    }
  }, 25000);
});
