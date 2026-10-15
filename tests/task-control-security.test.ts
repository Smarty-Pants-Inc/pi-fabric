import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { describe, expect, it, vi } from "vitest";
import { MAX_FINAL_ANSWER_BYTES, MAX_TASK_CONTROL_BYTES, readFinalAnswerReceipt, readTaskControlFile, readTaskControlJson, saveFinalAnswerReceipt, initializeTaskControlDirectory, taskControlDirectory, taskControlReadDirectory, taskSteerFile, withTaskFinalAnswerLock } from "../src/worker/terminal-answer.js";
import { appendTerminalAdmission, FabricSteerJournalError, readTerminalAdmissions, readUndeliveredMessage, TerminalNoticeOutbox } from "../src/agents/terminal-notices.js";
import { canRemoveTerminalRun, removeUnreferencedRun } from "../src/storage/retention.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { queuedTerminalControls, readTerminalControl, settleTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";
import { resolveChildWritePolicy } from "../src/agents/child-env.js";
import { assertTaskControlWriteRoots, createHostTaskRun, ensureTaskControlRoot, FabricTaskControlDirectoryError, legacyTaskControlSibling, withLegacyTaskControlDirectories } from "../src/worker/terminal-answer.js";

// All fixture paths are private and retained. No local deletion is performed.
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "task-control-security-"));
  const run = path.join(root, "run"), workspace = path.join(root, "workspace");
  ensureTaskControlRoot(); // central root lives under the caller-isolated XDG_STATE_HOME, outside TMPDIR
  fs.mkdirSync(workspace, { mode: 0o700 }); // run stays absent: the kernel owns its exclusive mkdir
  // Named setup change (Option 2): mirror the manager's production order — createHostTaskRun
  // exclusively mkdirs the run and publishes current-only (snapshot:null) authority BEFORE
  // any task-written status/raw history. initialize is validation-only and never mints.
  createHostTaskRun(run);
  return { root, run, workspace, control: taskControlDirectory(run) };
};
const confined = (script: string, s: ReturnType<typeof fixture>, policy = resolveChildWritePolicy({ writableRoots: [s.run, s.workspace] }, undefined, s.workspace)!) => {
  assertTaskControlWriteRoots(s.run, s.workspace, policy);
  const roots = [...new Set([s.run, s.workspace, ...policy.writableRoots])];
  const writes = roots.map(directory => {
    const stat = fs.statSync(directory, { bigint: true });
    return `${stat.dev}:${stat.ino}:${directory}`;
  }).join("\n");
  return spawnSync(path.resolve("dist/native/fabric-landlock"), ["-c", `exec ${JSON.stringify(process.execPath)} ${JSON.stringify(script)}`], {
    cwd: s.workspace, encoding: "utf8", timeout: 5000,
    env: { ...process.env, LC_ALL: "C", PI_FABRIC_LANDLOCK_SHELL: "/bin/sh", PI_FABRIC_LANDLOCK_WRITES: writes },
  });
};
// CI=true is not authority to delete on a fleet lane. True removal/replacement
// counterprobes are restricted to GitHub's disposable hosted runner.
const hostedDeletion = process.env.GITHUB_ACTIONS === "true" && process.env.RUNNER_ENVIRONMENT === "github-hosted";
const child = (script: string, cwd: string) => new Promise<{ code: number | null; output: string }>((resolve, reject) => {
  const process = spawn(globalThis.process.execPath, [script], { cwd, env: { ...globalThis.process.env }, stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  const timer = setTimeout(() => { process.kill("SIGTERM"); reject(new Error("Owned fixture process deadline exceeded")); }, 10000);
  process.stdout.on("data", bytes => { output += bytes; }); process.stderr.on("data", bytes => { output += bytes; });
  process.once("error", error => { clearTimeout(timer); reject(error); });
  process.once("close", code => { clearTimeout(timer); resolve({ code, output }); });
});

describe("host-owned task controls", () => {
  it("refuses a FIFO without blocking, while a regular journal still admits controls", () => {
    const s = fixture();
    if (process.platform !== "win32") {
      expect(spawnSync("mkfifo", [taskSteerFile(s.run)]).status).toBe(0);
      const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
      const start = performance.now();
      expect(() => readTerminalAdmissions(s.run)).toThrow(FabricSteerJournalError);
      expect(performance.now() - start).toBeLessThan(1000);
      expect(warning).toHaveBeenCalledWith(expect.stringContaining("FABRIC_STEER_JOURNAL_INVALID"));
      warning.mockRestore();
    }
    const regular = fixture();
    appendTerminalAdmission(regular.run, JSON.stringify({ type: "steer", id: "original", message: "allowed" }) + "\n");
    expect(readTerminalAdmissions(regular.run).byId.get("original")).toMatchObject({ id: "original", delivery: "steer" });
  });

  it.each((process.platform === "win32" ? ["oversized"] : ["fifo", "oversized"]).flatMap(fault =>
    ["registry", "pending notice", "acknowledged notice", "final receipt"].map(kind => ({ fault, kind }))))(
    "bounded $kind reader refuses $fault without blocking or routing invented evidence", ({ fault, kind }) => {
      const s = fixture(), id = "11111111-1111-4111-8111-111111111111";
      const sender = { id: "session:original", kind: "main", verified: "mesh" };
      appendTerminalAdmission(s.run, JSON.stringify({ id, type: "steer", provenance: { v: 1, channel: "fabric", via: "steer", sender } }) + "\n");
      const receipt = kind === "final receipt" ? undefined : saveFinalAnswerReceipt(s.run, "run", "final");
      const notice = { code: "FABRIC_TARGET_TERMINAL", targetId: "run", messageId: id, delivery: "steer", sender, finalAnswerReceiptId: receipt?.id };
      writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", runner: "pi", status: "completed", finalAnswerReceipt: receipt });
      const relative = kind === "registry" ? path.join("terminal-controls", id + ".json") : kind === "final receipt" ? "final-answer.json"
        : path.join("terminal-notices", id + (kind === "acknowledged notice" ? ".json.delivered" : ".json"));
      const file = path.join(s.control, relative); fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
      if (kind === "acknowledged notice") writeJsonAtomic(path.join(s.control, "terminal-notices", id + ".json"), notice);
      if (fault === "fifo") expect(spawnSync("mkfifo", [file]).status).toBe(0);
      else fs.writeFileSync(file, "x".repeat((kind === "final receipt" ? MAX_FINAL_ANSWER_BYTES : MAX_TASK_CONTROL_BYTES) + 1));
      const started = performance.now(), deliveries: unknown[] = [], outbox = new TerminalNoticeOutbox(value => { deliveries.push(value); });
      expect(() => readTaskControlJson(s.run, relative)).toThrow();
      if (kind === "registry") expect(readTerminalControl(s.run, id)).toBeUndefined();
      if (kind === "final receipt") expect(readFinalAnswerReceipt(s.run, "run")).toBeUndefined();
      outbox.recover(s.run); outbox.route(s.run);
      const view = readUndeliveredMessage(s.run, id);
      if (view) expect(view.sender).toEqual(sender); // only the valid original pending row may prove refusal
      expect(deliveries).toEqual([]);
      expect(canRemoveTerminalRun(s.run)).toBe(false);
      expect(performance.now() - started).toBeLessThan(1000);
    });

  it("bounds a file that grows after fstat instead of following its growing EOF", () => {
    const s = fixture(), relative = "metadata.json", file = path.join(s.control, relative);
    fs.writeFileSync(file, "{}");
    const read = fs.readSync; let requested = 0, grew = false;
    const spy = vi.spyOn(fs, "readSync").mockImplementation(((fd: number, buffer: Buffer, offset: number, length: number, position: number) => {
      requested += length;
      if (!grew) { grew = true; fs.appendFileSync(file, "x".repeat(MAX_TASK_CONTROL_BYTES + 1)); }
      return read(fd, buffer, offset, length, position);
    }) as typeof fs.readSync);
    expect(() => readTaskControlFile(s.run, relative)).toThrow(/byte bound/);
    expect(requested).toBeLessThanOrEqual(MAX_TASK_CONTROL_BYTES + 1); spy.mockRestore();
  });

  it("makes accepted admission durable: append, file fsync, then creation directory fsync", () => {
    const s = fixture(), order: string[] = [], append = fs.appendFileSync, sync = fs.fsyncSync;
    const appender = vi.spyOn(fs, "appendFileSync").mockImplementation(((...args: Parameters<typeof fs.appendFileSync>) => {
      order.push("append"); return append(...args);
    }) as typeof fs.appendFileSync);
    const barrier = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { order.push(fs.fstatSync(fd).isDirectory() ? "directory fsync" : "file fsync"); sync(fd); });
    appendTerminalAdmission(s.run, JSON.stringify({ type: "steer", id: "first" }) + "\n");
    expect(order).toEqual(["append", "file fsync", ...(process.platform === "win32" ? [] : ["directory fsync"])]);
    order.length = 0;
    appendTerminalAdmission(s.run, JSON.stringify({ type: "steer", id: "second" }) + "\n");
    expect(order).toEqual(["append", "file fsync"]);
    expect(readTerminalAdmissions(s.run).controls).toBe(2); appender.mockRestore(); barrier.mockRestore();
  });

  it.each(["entry count", "aggregate bytes"] as const)("bounds retention enumeration by %s, even under directory churn", limit => {
    const s = fixture(), id = "11111111-1111-4111-8111-111111111111", controls = path.join(s.control, "terminal-controls");
    appendTerminalAdmission(s.run, JSON.stringify({ id, type: "steer" }) + "\n");
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", runner: "pi", status: "completed", transport: "process", sessionId: "99999999", updatedAt: Date.now() });
    fs.mkdirSync(controls);
    const value = JSON.stringify({ id, delivery: "steer", state: "delivered" });
    fs.writeFileSync(path.join(controls, id + ".json"), limit === "aggregate bytes" ? value + " ".repeat(MAX_TASK_CONTROL_BYTES - Buffer.byteLength(value)) : value);
    expect(canRemoveTerminalRun(s.run)).toBe(true);
    // Retain the private native file; record a churned stream of 5,000 valid
    // entries. A full readdir on the fixture would incorrectly authorize it.
    const readSync = vi.fn(() => ({ name: id + ".json", isFile: () => true })), closeSync = vi.fn(), open = fs.opendirSync;
    const spy = vi.spyOn(fs, "opendirSync").mockImplementation(((directory: fs.PathLike, options?: fs.OpenDirOptions) =>
      fs.realpathSync(String(directory)) === fs.realpathSync(controls) ? { readSync, closeSync } as unknown as fs.Dir : open(directory, options)) as typeof fs.opendirSync);
    expect(canRemoveTerminalRun(s.run)).toBe(false);
    expect(readSync.mock.calls.length).toBe(limit === "entry count" ? 513 : 65);
    expect(readSync.mock.calls.length).toBeLessThan(5000); expect(closeSync).toHaveBeenCalledOnce(); spy.mockRestore();
    expect(canRemoveTerminalRun(s.run)).toBe(true); // real one-entry directory still collects under either budget
  });

  it("bounds the worker's queued-registry directory stream and closes on overflow", () => {
    const s = fixture(), id = "11111111-1111-4111-8111-111111111111", controls = path.join(s.control, "terminal-controls");
    trackTerminalControl(s.run, { id, delivery: "steer" });
    const readSync = vi.fn(() => ({ name: id + ".json", isFile: () => true })), closeSync = vi.fn(), open = fs.opendirSync;
    const spy = vi.spyOn(fs, "opendirSync").mockImplementation(((directory: fs.PathLike, options?: fs.OpenDirOptions) =>
      fs.realpathSync(String(directory)) === controls ? { readSync, closeSync } as unknown as fs.Dir : open(directory, options)) as typeof fs.opendirSync);
    expect(() => queuedTerminalControls(s.run)).toThrow(/entry bound/);
    expect(readSync.mock.calls.length).toBe(257); expect(closeSync).toHaveBeenCalledOnce(); spy.mockRestore();
    expect(queuedTerminalControls(s.run)).toMatchObject([{ id, state: "queued" }]);
  });

  it("ignores a task-written oversized journal and retention fails closed without reading it", () => {
    const s = fixture();
    fs.writeFileSync(path.join(s.run, "steer.jsonl"), "x".repeat(2 * 1024 * 1024));
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "completed" });
    const read = fs.readFileSync;
    const spy = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: any[]) => {
      if (file === path.join(s.run, "steer.jsonl")) throw new Error("untrusted journal must never be read");
      return (read as any)(file, ...args);
    }) as typeof fs.readFileSync);
    expect(readTerminalAdmissions(s.run)).toMatchObject({ controls: 0, bytes: 0, truncated: false });
    expect(canRemoveTerminalRun(s.run)).toBe(false);
    spy.mockRestore();
    // Even corrupted privileged ingress remains bounded and vetoes collection.
    fs.writeFileSync(taskSteerFile(s.run), "x".repeat(2 * 1024 * 1024));
    expect(readTerminalAdmissions(s.run).truncated).toBe(true);
  });

  it("ignores a forged task-local consumption row while the protected registry still settles real controls", () => {
    const s = fixture(), id = "11111111-1111-4111-8111-111111111111";
    writeJsonAtomic(path.join(s.run, "terminal-controls", id + ".json"), { id, delivery: "steer", state: "delivered" });
    expect(readTerminalControl(s.run, id)).toBeUndefined();
    trackTerminalControl(s.run, { id, delivery: "steer" });
    expect(readTerminalControl(s.run, id)?.state).toBe("queued");
    settleTerminalControl(s.run, id, "refused", "run-ended");
    expect(readTerminalControl(s.run, id)).toMatchObject({ state: "refused", reason: "run-ended" });
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "completed" });
    expect(canRemoveTerminalRun(s.run)).toBe(false);
  });

  it.each(["missing", "malformed", ...(process.platform === "win32" ? [] : ["fifo"])] as const)(
    "a task-forged legacy-read status cannot downgrade a new protected run with %s control evidence", fault => {
      const s = fixture(), id = "11111111-1111-4111-8111-111111111111";
      // Establish a NEW host control namespace before any task-written status.
      // Status in the writable run must not authorize legacy fallback.
      initializeTaskControlDirectory(s.run);
      const sender = { id: "session:forged-route", kind: "main", verified: "mesh" };
      const receipt = { version: 1, id: "forged-answer", runId: "run", recordedAt: 1, text: "TASK_FORGED_FINAL" };
      const provenance = { v: 1, channel: "fabric", via: "steer", sender };
      fs.writeFileSync(path.join(s.run, "status.json"), JSON.stringify({
        id: "run", status: "completed", runner: "pi", transport: "process", sessionId: "2147483647", updatedAt: 2,
        taskControlLayout: "legacy-read", finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt },
      }));
      fs.writeFileSync(path.join(s.run, "steer.jsonl"), JSON.stringify({ id, type: "steer", provenance }) + "\n");
      writeJsonAtomic(path.join(s.run, "final-answer.json"), receipt);
      writeJsonAtomic(path.join(s.run, "terminal-controls", id + ".json"), { id, delivery: "steer", provenance, state: "queued" });
      writeJsonAtomic(path.join(s.run, "terminal-notices", id + ".json"), {
        code: "FABRIC_TARGET_TERMINAL", targetId: "run", messageId: id, delivery: "steer", sender, finalAnswerReceiptId: receipt.id,
      });
      if (fault !== "missing") {
        for (const relative of ["final-answer.json", path.join("terminal-controls", id + ".json")]) {
          const file = path.join(s.control, relative); fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
          if (fault === "fifo") expect(spawnSync("mkfifo", [file]).status).toBe(0);
          else fs.writeFileSync(file, "{malformed");
        }
      }
      const deliveries: unknown[] = [], outbox = new TerminalNoticeOutbox(notice => { deliveries.push(notice); });
      expect(taskControlReadDirectory(s.run)).toBe(s.control);
      expect(readTaskControlFile(s.run, "steer.jsonl")).toBeUndefined();
      expect(readTerminalAdmissions(s.run)).toMatchObject({ controls: 0, bytes: 0, truncated: false });
      expect(readFinalAnswerReceipt(s.run, "run")).toBeUndefined();
      expect(readTerminalControl(s.run, id)).toBeUndefined();
      if (fault === "malformed") expect(() => queuedTerminalControls(s.run)).toThrow(SyntaxError);
      else expect(queuedTerminalControls(s.run)).toEqual([]);
      outbox.recover(s.run); outbox.route(s.run);
      expect(deliveries).toEqual([]);
      expect(canRemoveTerminalRun(s.run)).toBe(false);
      expect(fs.readFileSync(path.join(s.run, "final-answer.json"), "utf8")).toContain("TASK_FORGED_FINAL");
    });

  it.each(["sibling-v1", "legacy-read", "unknown-layout"])("a missing sibling plus task-written %s status never authorizes legacy reads or reader repair", layout => {
    const s = fixture(); initializeTaskControlDirectory(s.run);
    fs.renameSync(s.control, s.control + ".retained-evidence");
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", runner: "pi", status: "completed", taskControlLayout: layout });
    writeJsonAtomic(path.join(s.run, "final-answer.json"), { version: 1, id: "forged", runId: "run", recordedAt: 1, text: "untrusted" });
    expect(() => taskControlReadDirectory(s.run)).toThrow();
    expect(() => readFinalAnswerReceipt(s.run, "run")).toThrow();
    expect(() => initializeTaskControlDirectory(s.run)).toThrow();
    expect(fs.existsSync(s.control)).toBe(false);
    expect(canRemoveTerminalRun(s.run)).toBe(false);
    expect(fs.readFileSync(path.join(s.run, "final-answer.json"), "utf8")).toContain("untrusted");
  });

  it("stages a large answer before contending with a real other-process lock, then publishes immutably", async () => {
    const s = fixture(), module = path.join(s.root, "receipt.mjs");
    buildSync({ entryPoints: [path.resolve("src/worker/terminal-answer.ts")], outfile: module, bundle: true, platform: "node", format: "esm" });
    const shared = `import fs from 'node:fs'; import path from 'node:path'; import * as receipt from ${JSON.stringify(pathToFileURL(module).href)}; const run=${JSON.stringify(s.run)}, root=${JSON.stringify(s.root)}, ctl=${JSON.stringify(s.control)}; const wait=condition=>{const deadline=Date.now()+5000;while(!condition()){if(Date.now()>deadline)throw Error('fixture deadline');Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,5);}};`;
    const holder = path.join(s.root, "holder.mjs"), writer = path.join(s.root, "writer.mjs");
    fs.writeFileSync(holder, shared + `receipt.withTaskFinalAnswerLock(run, read=>{fs.writeFileSync(path.join(root,'held'),'');wait(()=>fs.readdirSync(ctl).some(name=>name.startsWith('.final-answer.')&&name.endsWith('.tmp')));if(read('run'))throw Error('published while lock held');process.stdout.write('CONTROL_FIRST stagedBeforeLock=true finalAbsent=true\\n');});`);
    fs.writeFileSync(writer, shared + `wait(()=>fs.existsSync(path.join(root,'held')));const result=receipt.saveFinalAnswerReceipt(run,'run','x'.repeat(2*1024*1024));process.stdout.write('FINAL_PUBLISHED bytes='+result.text.length+'\\n');`);
    const results = await Promise.all([child(holder, s.workspace), child(writer, s.workspace)]);
    expect(results.map(result => result.code)).toEqual([0, 0]);
    expect(results[0]!.output).toContain("CONTROL_FIRST stagedBeforeLock=true finalAbsent=true");
    expect(results[1]!.output).toContain("FINAL_PUBLISHED bytes=2097152");
    expect(readFinalAnswerReceipt(s.run, "run")?.text.length).toBe(2 * 1024 * 1024);
    expect(saveFinalAnswerReceipt(s.run, "run", "late").text.length).toBe(2 * 1024 * 1024);
    expect(() => saveFinalAnswerReceipt(s.run, "run", "x".repeat(MAX_FINAL_ANSWER_BYTES + 1))).toThrow(/8 MiB/);
    console.log(`CONTROL_LOCK: PASS twoRealProcesses=true stagedBeforeLock=true bytes=2097152 root=${s.root}`);
  }, 15000);

  it.skipIf(process.platform !== "linux").each(["explicit request", "inherited parent policy"] as const)(
    "confines every control write/FIFO/rename and live lock change under %s while run/workspace operations remain allowed", mode => {
    const s = fixture(), id = "11111111-1111-4111-8111-111111111111";
    const policy = mode === "explicit request"
      ? resolveChildWritePolicy({ writableRoots: [s.run, s.workspace] }, undefined, s.workspace)
      : resolveChildWritePolicy({}, { readOnly: false, writableRoots: [s.run, s.workspace], shell: "deny" }, s.workspace);
    expect(policy).toEqual({ readOnly: false, writableRoots: [s.run, s.workspace], shell: "deny" });
    const unsafe = path.join(s.run, ".task-final-answer.lock"); fs.mkdirSync(unsafe, { mode: 0o700 });
    fs.writeFileSync(path.join(unsafe, "owner"), "legacy");
    const relative = ["steer.jsonl", "final-answer.json", path.join("terminal-controls", id + ".json"), path.join("terminal-notices", id + ".json")];
    const files = relative.map(name => path.join(s.control, name));
    for (const file of files) {
      fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
      fs.writeFileSync(file, "{\"hostOwned\":true}\n", { mode: 0o600 });
    }
    const probe = path.join(s.workspace, "probe.mjs"), lock = path.join(s.control, ".task-final-answer.lock");
    withTaskFinalAnswerLock(s.run, () => {
      const protectedFiles = [...files, path.join(lock, "owner")];
      const before = protectedFiles.map(file => ({ file, bytes: fs.readFileSync(file), ino: fs.statSync(file).ino }));
      fs.writeFileSync(probe, `
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
const run=${JSON.stringify(s.run)}, workspace=${JSON.stringify(s.workspace)}, control=${JSON.stringify(s.control)}, lock=${JSON.stringify(lock)};
const files=${JSON.stringify(protectedFiles)}, denied=[];
const refuse=(name, operation)=>{
  try { operation(); } catch(error) {
    if (!['EACCES','EPERM','EXDEV'].includes(error.code)) throw error;
    denied.push(name); return;
  }
  throw Error('confinement bypass: '+name);
};
// These attempts do not truncate or unlink. Even a regression preserves bytes:
// the r+ write repeats the original first byte; rename destinations are absent.
for (const [index,file] of files.entries()) {
  refuse('write-open-'+index,()=>{const fd=fs.openSync(file,fs.constants.O_WRONLY);fs.closeSync(fd);});
  const first=fs.readFileSync(file).subarray(0,1);
  refuse('same-byte-write-'+index,()=>{const fd=fs.openSync(file,'r+');try{fs.writeSync(fd,first,0,first.length,0);}finally{fs.closeSync(fd);}});
  const staged=path.join(run,'staged-'+index);fs.writeFileSync(staged,'owned rename evidence');
  refuse('rename-into-controls-'+index,()=>fs.renameSync(staged,path.join(path.dirname(file),'injected-'+index)));
}
refuse('create-control',()=>{const fd=fs.openSync(path.join(control,'injected.json'),'wx');fs.closeSync(fd);});
refuse('hardlink-control',()=>fs.linkSync(files[0],path.join(run,'host-hardlink')));
refuse('rename-lock-owner',()=>fs.renameSync(path.join(lock,'owner'),path.join(run,'lock-owner-evidence')));
refuse('rename-live-lock',()=>fs.renameSync(lock,path.join(run,'lock-evidence')));
refuse('rename-control-directory',()=>fs.renameSync(control,control+'.retained-evidence'));
fs.symlinkSync(control,path.join(run,'control-alias'),'dir');
refuse('write-through-alias',()=>{const fd=fs.openSync(path.join(run,'control-alias','steer.jsonl'),'r+');fs.closeSync(fd);});
const fifo=spawnSync('mkfifo',[path.join(control,'injected.fifo')],{encoding:'utf8',timeout:1000});
if(fifo.error)throw fifo.error;
if(fifo.status!==1||!fifo.stderr.includes('Permission denied'))throw Error('FIFO not refused: '+JSON.stringify(fifo));
denied.push('control-fifo');
// Positive counterexamples use the SAME confined process and syscall paths.
const legacy=fs.openSync(${JSON.stringify(path.join(unsafe, "owner"))},fs.constants.O_WRONLY);fs.closeSync(legacy);
for (const directory of [run,workspace]) {
  const file=path.join(directory,'allowed-write');const fd=fs.openSync(file,'wx');
  try{fs.writeSync(fd,'allowed');}finally{fs.closeSync(fd);}
  const allowedFifo=spawnSync('mkfifo',[path.join(directory,'allowed.fifo')],{encoding:'utf8',timeout:1000});
  if(allowedFifo.error||allowedFifo.status!==0)throw Error('allowed FIFO failed');
}
fs.renameSync(path.join(run,'allowed-write'),path.join(workspace,'allowed-rename'));
console.log(JSON.stringify({type:'LANDLOCK_COUNTERPROBES',denied,allowed:['legacy-write-open','run-write','workspace-write','run-fifo','workspace-fifo','run-to-workspace-rename']}));
`);
      const result = confined(probe, s, policy!);
      expect(result.error).toBeUndefined(); expect(result.signal).toBeNull();
      expect(result.status, result.stderr).toBe(0);
      const evidence = JSON.parse(result.stdout.trim());
      expect(evidence.denied).toHaveLength(protectedFiles.length * 3 + 7);
      expect(evidence.allowed).toHaveLength(6);
      for (const item of before) {
        expect(fs.readFileSync(item.file)).toEqual(item.bytes);
        expect(fs.statSync(item.file).ino).toBe(item.ino);
        expect(fs.statSync(item.file).nlink).toBe(1);
      }
      expect(fs.readFileSync(path.join(s.workspace, "allowed-rename"), "utf8")).toBe("allowed");
      expect(fs.statSync(path.join(s.run, "allowed.fifo")).isFIFO()).toBe(true);
      expect(fs.statSync(path.join(s.workspace, "allowed.fifo")).isFIFO()).toBe(true);
      console.log(`LANDLOCK: PASS policyMode=${mode} denied=${evidence.denied.length} allowed=${evidence.allowed.length} root=${s.root} removal=not-executed`);
    });
  });

  it.skipIf(process.platform !== "linux")("SEC01 #12079: a parent run grant covering descendant R/nested/C refuses both central ctls with EACCES while nested artifacts stay writable", () => {
    const s = fixture(), descendant = path.join(s.run, "nested", "C");
    fs.mkdirSync(path.dirname(descendant), { recursive: true, mode: 0o700 }); // parent only; C stays absent
    createHostTaskRun(descendant); // named setup change: host ctor owns exclusive mkdir; initialize never mints
    const descendantControl = taskControlDirectory(descendant);
    saveFinalAnswerReceipt(s.run, "run", "PARENT_RECEIPT");
    const receipt = path.join(s.control, "final-answer.json"), marker = path.join(descendantControl, "steer.jsonl");
    fs.writeFileSync(marker, "HOST_DESCENDANT_MARKER\n", { mode: 0o600 });
    const policy = resolveChildWritePolicy({ writableRoots: [s.run, s.workspace] }, undefined, s.workspace)!;
    const within = (parent: string, child: string) => { const relative = path.relative(fs.realpathSync(parent), fs.realpathSync(child)); return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative)); };
    expect(policy.writableRoots).toContain(s.run);
    expect(within(s.run, descendant)).toBe(true);
    for (const root of policy.writableRoots) for (const control of [s.control, descendantControl]) {
      expect(within(root, control)).toBe(false); expect(within(control, root)).toBe(false);
    }
    expect(() => assertTaskControlWriteRoots(descendant, s.workspace, policy)).not.toThrow();
    const protectedFiles = [receipt, marker], before = protectedFiles.map(file => fs.readFileSync(file));
    const artifact = path.join(descendant, "artifact.txt"), probe = path.join(s.workspace, "descendant-probe.mjs");
    // Open-only and same-byte r+ writes: no truncation, unlink, rename or replacement.
    fs.writeFileSync(probe, `
import fs from 'node:fs';
const files=${JSON.stringify(protectedFiles)}, codes=[];
const attempt=operation=>{try{operation();codes.push('ALLOWED');}catch(error){codes.push(error.code);}};
for (const file of files) {
  attempt(()=>{const fd=fs.openSync(file,fs.constants.O_WRONLY);fs.closeSync(fd);});
  const first=fs.readFileSync(file).subarray(0,1);
  attempt(()=>{const fd=fs.openSync(file,'r+');try{fs.writeSync(fd,first,0,first.length,0);}finally{fs.closeSync(fd);}});
}
const fd=fs.openSync(${JSON.stringify(artifact)},'wx');try{fs.writeSync(fd,'nested-allowed');}finally{fs.closeSync(fd);}
console.log(JSON.stringify({codes}));
`);
    const result = confined(probe, s, policy);
    expect(result.error).toBeUndefined(); expect(result.signal).toBeNull();
    expect(result.status, result.stderr).toBe(0);
    expect(JSON.parse(result.stdout.trim()).codes).toEqual(["EACCES", "EACCES", "EACCES", "EACCES"]);
    protectedFiles.forEach((file, index) => expect(fs.readFileSync(file)).toEqual(before[index]));
    expect(readFinalAnswerReceipt(s.run, "run")?.text).toBe("PARENT_RECEIPT");
    expect(fs.readFileSync(marker, "utf8")).toBe("HOST_DESCENDANT_MARKER\n");
    expect(fs.readFileSync(artifact, "utf8")).toBe("nested-allowed");
    console.log(`SEC01_DESCENDANT: PASS root=${s.root} removal=not-executed`);
  });

  // FORGE-BEFORE-FIRST-SNAPSHOT: a parent writer (grant R) creates C and raw C.ctl with a
  // well-formed answer and steering row BEFORE any host creation, then exits. No API may
  // adopt it: initialize/read/save/producer refuse; raw bytes, names and inodes are kept.
  it.skipIf(process.platform !== "linux")("FORGE-BEFORE-FIRST-SNAPSHOT: parent-written raw C.ctl before host creation is refused, never adopted", () => {
    const s = fixture(), C = path.join(s.run, "nested", "C"), sibling = legacyTaskControlSibling(C);
    const forgedId = "44444444-4444-4444-8444-444444444444", central = taskControlDirectory(C);
    const forged = JSON.stringify({ version: 1, id: "forged-answer", runId: "C", recordedAt: 1, text: "PRE_SNAPSHOT_FORGED" });
    const artifact = path.join(C, "artifact.txt"), probe = path.join(s.workspace, "forge-before-probe.mjs");
    fs.writeFileSync(probe, `
import fs from 'node:fs';
import path from 'node:path';
const C=${JSON.stringify(C)}, sibling=${JSON.stringify(sibling)};
fs.mkdirSync(C,{recursive:true,mode:0o700});fs.mkdirSync(sibling,{mode:0o700});
fs.writeFileSync(path.join(sibling,'final-answer.json'),${JSON.stringify(forged)},{mode:0o600});
fs.writeFileSync(path.join(sibling,'steer.jsonl'),JSON.stringify({type:'steer',id:${JSON.stringify(forgedId)},message:'forged'})+'\\n',{mode:0o600});
fs.writeFileSync(path.join(C,'status.json'),JSON.stringify({id:'C',runner:'pi',status:'completed',taskControlLayout:'sibling-v1'}));
const fd=fs.openSync(${JSON.stringify(artifact)},'wx');try{fs.writeSync(fd,'nested-allowed');}finally{fs.closeSync(fd);}
console.log('FORGED');
`);
    const result = confined(probe, s);
    expect(result.error).toBeUndefined(); expect(result.signal).toBeNull();
    expect(result.status, result.stderr).toBe(0); expect(result.stdout.trim()).toBe("FORGED");
    const snapshot = () => [C, sibling].flatMap(directory => fs.readdirSync(directory).sort().map(name => {
      const file = path.join(directory, name); return { file, ino: fs.lstatSync(file).ino, bytes: fs.readFileSync(file) };
    }));
    const before = snapshot(), directories = [C, sibling].map(directory => fs.lstatSync(directory).ino);
    expect(() => initializeTaskControlDirectory(C)).toThrow(FabricTaskControlDirectoryError);
    expect(() => readFinalAnswerReceipt(C, "C")).toThrow(FabricTaskControlDirectoryError);
    expect(() => withTaskFinalAnswerLock(C, read => read("C"))).toThrow(FabricTaskControlDirectoryError);
    expect(() => saveFinalAnswerReceipt(C, "C", "HOST")).toThrow(FabricTaskControlDirectoryError);
    expect(() => readTerminalAdmissions(C)).toThrow(); // may wrap the typed refusal
    expect(snapshot()).toEqual(before);
    expect([C, sibling].map(directory => fs.lstatSync(directory).ino)).toEqual(directories);
    expect(fs.existsSync(central)).toBe(false);
    expect(fs.readFileSync(artifact, "utf8")).toBe("nested-allowed");
    console.log(`FORGE_BEFORE_FIRST_SNAPSHOT: PASS root=${s.root} removal=not-executed`);
  });

  it("an old central v1 legacy binding is not auto-upgraded; every consumer refuses", () => {
    const s = fixture(), C = path.join(s.run, "nested", "C"), sibling = legacyTaskControlSibling(C), central = taskControlDirectory(C);
    // Named setup change: reproduce pre-v2 host state (v1 identity binding + raw sibling).
    fs.mkdirSync(C, { recursive: true, mode: 0o700 }); fs.mkdirSync(sibling, { mode: 0o700 });
    fs.writeFileSync(path.join(sibling, "final-answer.json"), JSON.stringify({ version: 1, id: "old", runId: "C", recordedAt: 1, text: "OLD" }), { mode: 0o600 });
    const stat = fs.lstatSync(C, { bigint: true });
    fs.mkdirSync(central, { mode: 0o700 });
    fs.writeFileSync(path.join(central, ".legacy-read.json"), JSON.stringify({ version: 1, runId: "C", dev: String(stat.dev), ino: String(stat.ino) }), { mode: 0o600 });
    const files = [path.join(central, ".legacy-read.json"), path.join(sibling, "final-answer.json")];
    const before = files.map(file => ({ ino: fs.lstatSync(file).ino, bytes: fs.readFileSync(file) })), names = fs.readdirSync(central).sort();
    expect(() => initializeTaskControlDirectory(C)).toThrow(FabricTaskControlDirectoryError);
    expect(() => readFinalAnswerReceipt(C, "C")).toThrow(FabricTaskControlDirectoryError);
    expect(() => saveFinalAnswerReceipt(C, "C", "HOST")).toThrow(FabricTaskControlDirectoryError);
    expect(() => readTerminalAdmissions(C)).toThrow();
    expect(files.map(file => ({ ino: fs.lstatSync(file).ino, bytes: fs.readFileSync(file) }))).toEqual(before);
    expect(fs.readdirSync(central).sort()).toEqual(names);
  });

  // central-v1 keeps the unmarked pre-v2 in-run READ contract, never its WRITE
  // authority. Sibling/unbound layers still refuse; all original bytes survive.
  it.each(["in-run", "sibling", "in-run+sibling", "unbound central-v1"] as const)("CUTOFF: pre-existing %s history is read-only or refused, never upgraded, and byte-preserved", shape => {
    const s = fixture(), C = path.join(s.run, "nested", "C"), sibling = legacyTaskControlSibling(C), central = taskControlDirectory(C);
    const answer = JSON.stringify({ version: 1, id: "old", runId: "C", recordedAt: 1, text: "OLD_UNKNOWN" });
    const steer = JSON.stringify({ type: "steer", id: "55555555-5555-4555-8555-555555555555", message: "old" }) + "\n";
    fs.mkdirSync(C, { recursive: true, mode: 0o700 });
    const layers = shape === "in-run" ? [C] : shape === "sibling" ? [sibling] : shape === "in-run+sibling" ? [C, sibling] : [central];
    for (const directory of layers) {
      if (!fs.existsSync(directory)) fs.mkdirSync(directory, { mode: 0o700 });
      fs.writeFileSync(path.join(directory, "final-answer.json"), answer, { mode: 0o600 });
      fs.writeFileSync(path.join(directory, "steer.jsonl"), steer, { mode: 0o600 });
    }
    if (shape !== "unbound central-v1") writeJsonAtomic(path.join(C, "status.json"), { id: "C", runner: "pi", status: "completed" });
    const snapshot = () => [C, sibling, central].filter(directory => fs.existsSync(directory)).flatMap(directory => fs.readdirSync(directory).sort().map(name => {
      const file = path.join(directory, name); return { file, ino: fs.lstatSync(file).ino, bytes: fs.readFileSync(file) };
    }));
    const before = snapshot(), centralExisted = fs.existsSync(central);
    expect(() => initializeTaskControlDirectory(C)).toThrow(FabricTaskControlDirectoryError);
    if (shape === "in-run") {
      expect(taskControlReadDirectory(C)).toBe(C);
      expect(readFinalAnswerReceipt(C, "C")).toEqual(JSON.parse(answer));
      expect(readTerminalAdmissions(C)).toMatchObject({ controls: 1, bytes: Buffer.byteLength(steer), truncated: false });
    } else {
      expect(() => readFinalAnswerReceipt(C, "C")).toThrow(FabricTaskControlDirectoryError);
      expect(() => readTerminalAdmissions(C)).toThrow(); // may wrap the typed refusal
    }
    expect(() => withTaskFinalAnswerLock(C, read => read("C"))).toThrow(FabricTaskControlDirectoryError);
    expect(() => saveFinalAnswerReceipt(C, "C", "HOST")).toThrow(FabricTaskControlDirectoryError);
    expect(canRemoveTerminalRun(C)).toBe(false);
    expect(snapshot()).toEqual(before);
    expect(fs.existsSync(central)).toBe(centralExisted);
  });

  // Positive after bootstrap, independent of the generic seed: fresh current-only run C
  // (manager order: createHostTaskRun exclusive mkdir + null central) under parent grant R. Accepted
  // evidence comes from REAL host production events (saveFinalAnswerReceipt /
  // appendTerminalAdmission). Raw C.ctl / in-run forgery stays allowed but is ignored.
  it.skipIf(process.platform !== "linux").each(["no host answer", "host answer"] as const)("CURRENT-ONLY: parent-written raw C.ctl and in-run controls after host bootstrap never replace host evidence (%s)", scenario => {
    const s = fixture(), C = path.join(s.run, "nested", "C"), sibling = legacyTaskControlSibling(C), central = taskControlDirectory(C);
    const forgedId = "66666666-6666-4666-8666-666666666666", hostId = "77777777-7777-4777-8777-777777777777";
    fs.mkdirSync(path.dirname(C), { recursive: true, mode: 0o700 }); // parent only; C stays absent
    createHostTaskRun(C);
    expect(() => initializeTaskControlDirectory(C)).not.toThrow(); // validation-only on host-created authority
    expect(fs.existsSync(sibling)).toBe(false);
    expect(withLegacyTaskControlDirectories(C, sources => sources.length)).toBe(0);
    expect(readFinalAnswerReceipt(C, "C")).toBeUndefined();
    appendTerminalAdmission(C, JSON.stringify({ type: "steer", id: hostId, message: "host" }) + "\n");
    const hostAnswer = scenario === "host answer" ? saveFinalAnswerReceipt(C, "C", "HOST_CURRENT_ANSWER") : undefined;
    expect(readTerminalAdmissions(C).byId.has(hostId)).toBe(true);
    const protectedFiles = fs.readdirSync(central).map(name => path.join(central, name)).filter(file => fs.lstatSync(file).isFile());
    const before = protectedFiles.map(file => ({ ino: fs.lstatSync(file).ino, bytes: fs.readFileSync(file) }));
    const forged = JSON.stringify({ version: 1, id: "forged-answer", runId: "C", recordedAt: 2, text: "PARENT_FORGED" });
    const artifact = path.join(C, "artifact.txt"), probe = path.join(s.workspace, "cutoff-probe.mjs");
    fs.writeFileSync(probe, `
import fs from 'node:fs';
import path from 'node:path';
const sibling=${JSON.stringify(sibling)}, C=${JSON.stringify(C)}, central=${JSON.stringify(central)}, forged=${JSON.stringify(forged)};
const files=${JSON.stringify(protectedFiles)}, codes=[];
const attempt=operation=>{try{operation();codes.push('ALLOWED');}catch(error){codes.push(error.code);}};
const row=JSON.stringify({type:'steer',id:${JSON.stringify(forgedId)},message:'forged'})+'\\n';
fs.mkdirSync(sibling,{mode:0o700});
for (const directory of [sibling,C]) { fs.writeFileSync(path.join(directory,'final-answer.json'),forged); fs.writeFileSync(path.join(directory,'steer.jsonl'),row); }
for (const file of files) attempt(()=>{const fd=fs.openSync(file,fs.constants.O_WRONLY);fs.closeSync(fd);});
attempt(()=>{const fd=fs.openSync(path.join(central,'injected.json'),'wx');fs.closeSync(fd);});
const fd=fs.openSync(${JSON.stringify(artifact)},'wx');try{fs.writeSync(fd,'nested-allowed');}finally{fs.closeSync(fd);}
console.log(JSON.stringify({codes}));
`);
    const result = confined(probe, s);
    expect(result.error).toBeUndefined(); expect(result.signal).toBeNull();
    expect(result.status, result.stderr).toBe(0);
    expect(JSON.parse(result.stdout.trim()).codes).toEqual([...protectedFiles, central].map(() => "EACCES"));
    expect(protectedFiles.map(file => ({ ino: fs.lstatSync(file).ino, bytes: fs.readFileSync(file) }))).toEqual(before);
    expect(fs.readFileSync(path.join(sibling, "final-answer.json"), "utf8")).toContain("PARENT_FORGED");
    expect(fs.readFileSync(path.join(C, "final-answer.json"), "utf8")).toContain("PARENT_FORGED");
    const expected = hostAnswer ? { id: hostAnswer.id, text: "HOST_CURRENT_ANSWER" } : undefined;
    if (expected) expect(readFinalAnswerReceipt(C, "C")).toMatchObject(expected); else expect(readFinalAnswerReceipt(C, "C")).toBeUndefined();
    withTaskFinalAnswerLock(C, read => { if (expected) expect(read("C")).toMatchObject(expected); else expect(read("C")).toBeUndefined(); });
    // A late host save never replaces the accepted answer; with none, it is the first answer.
    expect(saveFinalAnswerReceipt(C, "C", "HOST_LATE").text).toBe(expected ? "HOST_CURRENT_ANSWER" : "HOST_LATE");
    expect(withLegacyTaskControlDirectories(C, sources => sources.length)).toBe(0);
    const admissions = readTerminalAdmissions(C);
    expect(admissions.byId.has(hostId)).toBe(true); expect(admissions.byId.has(forgedId)).toBe(false);
    expect(fs.readFileSync(artifact, "utf8")).toBe("nested-allowed");
    console.log(`CURRENT_ONLY_AFTER_BOOTSTRAP: PASS scenario=${scenario} root=${s.root} removal=not-executed`);
  });

  it.skipIf(process.platform !== "linux" || !hostedDeletion)("hosted-only: refuses actual control replacement/removal and live-lock deletion", () => {
    const s = fixture(), controlFile = taskSteerFile(s.run), lock = path.join(s.control, ".task-final-answer.lock");
    fs.writeFileSync(controlFile, "HOST_CONTROL\n");
    fs.mkdirSync(lock); fs.writeFileSync(path.join(lock, "owner"), "HOST_LOCK\n");
    const staged = path.join(s.run, "replacement"); fs.writeFileSync(staged, "ATTACKER\n");
    const probe = path.join(s.workspace, "delete-probe.mjs");
    fs.writeFileSync(probe, `
import fs from 'node:fs';
const refuse=operation=>{try{operation();}catch(error){if(['EACCES','EPERM','EXDEV'].includes(error.code))return;throw error;}throw Error('confinement bypass');};
refuse(()=>fs.renameSync(${JSON.stringify(staged)},${JSON.stringify(controlFile)}));
refuse(()=>fs.unlinkSync(${JSON.stringify(controlFile)}));
refuse(()=>fs.rmSync(${JSON.stringify(lock)},{recursive:true}));
refuse(()=>fs.rmSync(${JSON.stringify(s.control)},{recursive:true}));
console.log('HOSTED_DELETE: PASS actualReplacementAndRemovalRefused=true');
`);
    const result = confined(probe, s);
    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain("HOSTED_DELETE: PASS");
    expect(fs.readFileSync(controlFile, "utf8")).toBe("HOST_CONTROL\n");
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe("HOST_LOCK\n");
    expect(fs.readFileSync(staged, "utf8")).toBe("ATTACKER\n");
  });

  it("refuses and logs a dangling ctl link before retention can remove its run", () => {
    const s = fixture(), retained = s.control + ".retained";
    fs.renameSync(s.control, retained); fs.symlinkSync(path.join(s.root, "missing"), s.control, process.platform === "win32" ? "junction" : "dir");
    expect(fs.existsSync(s.control)).toBe(false); expect(fs.lstatSync(s.control).isSymbolicLink()).toBe(true);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined), remove = vi.spyOn(fs, "rmSync").mockImplementation(() => undefined);
    expect(removeUnreferencedRun(s.run, fs.statSync(s.run), undefined, true)).toBe(false);
    expect(warning).toHaveBeenCalledWith(expect.stringContaining("FABRIC_TASK_CONTROL_UNSAFE cleanup refused"));
    expect(remove).not.toHaveBeenCalled(); expect(fs.existsSync(s.run)).toBe(true);
    // No unlink: keep the offending link as evidence, restore the real directory.
    fs.renameSync(s.control, s.control + ".link-evidence"); fs.renameSync(retained, s.control);
    expect(removeUnreferencedRun(s.run, fs.statSync(s.run), undefined, true)).toBe(true);
    warning.mockRestore(); remove.mockRestore();
  });

  it("refuses a foreign control-directory owner even for an otherwise private directory", () => {
    if (!process.getuid) return;
    const s = fixture(), inode = fs.statSync(s.control).ino, stat = fs.fstatSync;
    const owner = vi.spyOn(fs, "fstatSync").mockImplementation(((...args: Parameters<typeof fs.fstatSync>) => {
      const value = stat(...args);
      if (value.isDirectory() && String(value.ino) === String(inode)) return Object.assign(Object.create(Object.getPrototypeOf(value)), value, { uid: process.getuid!() + 1 });
      return value;
    }) as typeof fs.fstatSync);
    expect(() => readTaskControlFile(s.run, "missing.json")).toThrow(/owned private directory/);
    owner.mockRestore(); expect(readTaskControlFile(s.run, "missing.json")).toBeUndefined();
  });

  it("Windows-mode creation on POSIX still creates private Fabric controls, without weakening existing-directory validation", async () => {
    if (!process.getuid) return; // Windows has no POSIX uid/mode contract.
    const { mkdirPrivate } = await import("../src/storage/private-dir.js");
    const s = fixture(), run = path.join(s.root, "emulated-run"), control = taskControlDirectory(run);
    fs.mkdirSync(run);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    try { Object.defineProperty(process, "platform", { ...platform, value: "win32" }); mkdirPrivate(control); }
    finally { Object.defineProperty(process, "platform", platform); }
    expect(fs.statSync(control).mode & 0o777).toBe(0o700);
    // Strengthened (Option 2): a manually created control dir has no binding; readers refuse it.
    expect(() => readTaskControlFile(run, "missing.json")).toThrow(/unbound/i);
    expect(fs.readdirSync(control)).toEqual([]); // never minted or healed by the reader
    // Legitimate positive: a fresh absent run through the host constructor, Windows-mode creation.
    const fresh = path.join(s.root, "emulated-fresh-run");
    try { Object.defineProperty(process, "platform", { ...platform, value: "win32" }); createHostTaskRun(fresh); }
    finally { Object.defineProperty(process, "platform", platform); }
    expect(fs.statSync(fresh).mode & 0o777).toBe(0o700);
    expect(fs.statSync(taskControlDirectory(fresh)).mode & 0o777).toBe(0o700);
    expect(readTaskControlFile(fresh, "missing.json")).toBeUndefined();
    const bad = path.join(s.root, "replaced-run"); fs.mkdirSync(bad); fs.mkdirSync(taskControlDirectory(bad), { mode: 0o755 });
    expect(() => readTaskControlFile(bad, "missing.json")).toThrow(/owned private directory/);
    expect(fs.statSync(taskControlDirectory(bad)).mode & 0o777).toBe(0o755); // never silently heal replaced controls
  });

  it("pairs ctl with run cleanup in a dry run; neither fixture is removed", () => {
    const s = fixture(); writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "completed" });
    expect(removeUnreferencedRun(s.run, fs.statSync(s.run), undefined, true)).toBe(true);
    expect(fs.existsSync(s.run)).toBe(true); expect(fs.existsSync(s.control)).toBe(true);
  });
});
