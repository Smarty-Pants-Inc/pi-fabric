import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { buildSync } from "esbuild";
import { describe, expect, it, vi } from "vitest";
import { MAX_FINAL_ANSWER_BYTES, MAX_TASK_CONTROL_BYTES, readFinalAnswerReceipt, readTaskControlFile, readTaskControlJson, saveFinalAnswerReceipt, taskControlDirectory, taskSteerFile, withTaskFinalAnswerLock } from "../src/worker/terminal-answer.js";
import { appendTerminalAdmission, FabricSteerJournalError, readTerminalAdmissions, readUndeliveredMessage, TerminalNoticeOutbox } from "../src/agents/terminal-notices.js";
import { canRemoveTerminalRun, removeUnreferencedRun } from "../src/storage/retention.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { queuedTerminalControls, readTerminalControl, settleTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";

// All fixture paths are private and retained. No local deletion is performed.
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "task-control-security-"));
  const run = path.join(root, "run"), workspace = path.join(root, "workspace");
  for (const directory of [run, workspace, taskControlDirectory(run)]) fs.mkdirSync(directory, { mode: 0o700 });
  return { root, run, workspace, control: taskControlDirectory(run) };
};
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
      String(directory) === controls ? { readSync, closeSync } as unknown as fs.Dir : open(directory, options)) as typeof fs.opendirSync);
    expect(canRemoveTerminalRun(s.run)).toBe(false);
    expect(readSync.mock.calls.length).toBe(limit === "entry count" ? 513 : 65);
    expect(readSync.mock.calls.length).toBeLessThan(5000); expect(closeSync).toHaveBeenCalledOnce(); spy.mockRestore();
    if (limit === "entry count") expect(canRemoveTerminalRun(s.run)).toBe(true); // real one-entry directory still collects
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

  it("the task shell cannot write/remove the live host lock; the old run-local lock was writable", () => {
    if (process.platform !== "linux") return;
    const s = fixture(), unsafe = path.join(s.run, ".task-final-answer.lock"); fs.mkdirSync(unsafe, { mode: 0o700 });
    fs.writeFileSync(path.join(unsafe, "owner"), "legacy");
    const probe = path.join(s.workspace, "probe.mjs");
    // O_WRONLY without TRUNC writes no bytes: this is a non-destructive real
    // Landlock access probe. The removal trigger is a recorded-call probe only.
    fs.writeFileSync(probe, `import fs from 'node:fs';const open=p=>{try{const fd=fs.openSync(p,fs.constants.O_WRONLY);fs.closeSync(fd);return true;}catch(e){if(e.code!=='EACCES'&&e.code!=='EPERM')throw e;return false;}};if(!open(${JSON.stringify(path.join(unsafe, "owner"))}))throw Error('counterexample not writable');if(open(${JSON.stringify(path.join(s.control, ".task-final-answer.lock", "owner"))}))throw Error('host lock writable');console.log('LANDLOCK: PASS legacyWritable=true hostLockWritable=false');`);
    const writes = [s.run, s.workspace].map(directory => { const stat = fs.statSync(directory, { bigint: true }); return `${stat.dev}:${stat.ino}:${directory}`; }).join("\n");
    withTaskFinalAnswerLock(s.run, () => {
      const remove = vi.spyOn(fs, "rmSync").mockImplementation(() => undefined);
      fs.rmSync(unsafe, { recursive: true, force: true }); // recorded, never executed locally
      expect(remove).toHaveBeenCalledWith(unsafe, { recursive: true, force: true }); remove.mockRestore();
      const result = spawnSync(path.resolve("dist/native/fabric-landlock"), ["-c", `exec ${JSON.stringify(process.execPath)} ${JSON.stringify(probe)}`], { cwd: s.workspace,
        env: { ...process.env, PI_FABRIC_LANDLOCK_SHELL: "/bin/sh", PI_FABRIC_LANDLOCK_WRITES: writes }, encoding: "utf8", timeout: 5000 });
      expect(result.status, result.stderr).toBe(0);
      expect(result.stdout).toContain("LANDLOCK: PASS");
      console.log(result.stdout.trim());
    });
  });

  it("pairs ctl with run cleanup in a dry run; neither fixture is removed", () => {
    const s = fixture(); writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "completed" });
    expect(removeUnreferencedRun(s.run, fs.statSync(s.run), undefined, true)).toBe(true);
    expect(fs.existsSync(s.run)).toBe(true); expect(fs.existsSync(s.control)).toBe(true);
  });
});
