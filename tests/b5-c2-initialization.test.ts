// Keep: acknowledged-work data safety and trusted NEW/current authority boundary.
// Real fsync/rename/readback and candidate-process cuts; NOT M2/B6/native-effect proof.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { context } from "esbuild";
import { afterEach, describe, expect, it, vi } from "vitest";
import { c2Lifecycle } from "./helpers/b5-c2-lifecycle.js";
import {
  bindInputAdmission, initializeNewRecoverySession, readRecoveryInput, readRecoverySession,
  RecoveryAdmissionError, recoveryCanonical, recoveryHash,
  type NewRecoverySessionInitialization, type RecoveryAdmissionOwner,
} from "../src/recovery/input-admission.js";

const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => { vi.restoreAllMocks(); for (const close of cleanups.splice(0)) await close(); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "b5-c2-new-")), sessionId = path.basename(root);
  const recovery: RecoveryAdmissionOwner = { epoch: { processIncarnationId: "process", bindingId: "binding", epoch: 0 },
    podId: "pod", bootId: "boot", historyComplete: true, assertCurrent() {}, reconsiderInput() {} };
  let authoritative: "NEW" | "REQUIRED" = "NEW";
  const request: NewRecoverySessionInitialization = { mode: "NEW", root, sessionId, recovery,
    assertNew() { if (authoritative !== "NEW") throw new RecoveryAdmissionError("RECOVERY_CONFLICT", "Authoritative identity is REQUIRED, not NEW"); } };
  const binding = { root, sessionId, recovery, mainAgent: { id: `session:${sessionId}`, local: true, sessionId,
    deliverAgent(): never { throw new Error("No native delivery from B5 initialization/read"); } } };
  const bind = () => { const close = bindInputAdmission(binding); cleanups.push(close); return close; };
  return { root, sessionId, recovery, request, binding, bind, require() { authoritative = "REQUIRED"; },
    file: path.join(root, "input-admission", "v1", "session", `${recoveryHash(sessionId)}.json`) };
};
const incomplete = (run: () => unknown) => expect(run).toThrow(expect.objectContaining({ code: "RECOVERY_HISTORY_INCOMPLETE" }));
const conflict = (run: () => unknown) => expect(run).toThrow(expect.objectContaining({ code: "RECOVERY_CONFLICT" }));

describe.skipIf(process.platform === "win32")("C2 explicit NEW seal and REQUIRED read", () => {
  it("durably seals legitimate NEW revision zero including C1 pins, then reopens without any initializer or writes", () => {
    const f = fixture(); const barriers: Array<{ path: string; directory: boolean }> = [], names = new Map<number, string>();
    const open = fs.openSync, close = fs.closeSync, sync = fs.fsyncSync;
    vi.spyOn(fs, "openSync").mockImplementation((...args) => { const fd = open(...args); names.set(fd, String(args[0])); return fd; });
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { names.delete(fd); close(fd); });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { sync(fd); barriers.push({ path: names.get(fd)!, directory: fs.fstatSync(fd).isDirectory() }); });
    initializeNewRecoverySession(f.request);
    expect(barriers.some(b => !b.directory && b.path.startsWith(f.file) && b.path.endsWith(".tmp"))).toBe(true);
    for (let dir = path.dirname(f.file); ; dir = path.dirname(dir)) {
      expect(barriers.some(b => b.directory && b.path === dir), `namespace barrier ${dir}`).toBe(true);
      if (dir === path.dirname(dir)) break;
    }
    const bytes = fs.readFileSync(f.file, "utf8"), envelope = JSON.parse(bytes);
    expect(envelope.sha256).toBe(recoveryHash(recoveryCanonical(envelope.payload)));
    expect(envelope.payload).toEqual({ version: 1, sessionId: f.sessionId, revision: 0, complete: true,
      inputs: [], outcomes: [], maintenance: [], retrySlots: [], repairs: [], recoveryGenerationPins: [], taskAborts: [], driveReceipts: [] });
    const mkdir = vi.spyOn(fs, "mkdirSync"), rename = vi.spyOn(fs, "renameSync");
    f.require(); let unbind = f.bind();
    for (let n = 0; n < 2; n++) {
      expect(readRecoverySession(f.sessionId)).toEqual(envelope.payload);
      unbind(); unbind = f.bind();
    }
    expect(fs.readFileSync(f.file, "utf8")).toBe(bytes);
    expect(mkdir).not.toHaveBeenCalled(); expect(rename).not.toHaveBeenCalled();
  });

  it("pure REQUIRED read of an uninitialized provisioned root never writes or invents a seal", async () => {
    const f = fixture(); f.require(); f.bind();
    const mkdir = vi.spyOn(fs, "mkdirSync"), rename = vi.spyOn(fs, "renameSync");
    incomplete(() => readRecoverySession(f.sessionId));
    expect(await readRecoveryInput(f.sessionId)).toMatchObject({ complete: false });
    expect(fs.readdirSync(f.root)).toEqual([]); expect(mkdir).not.toHaveBeenCalled(); expect(rename).not.toHaveBeenCalled();
  });

  it.each(["valid", "corrupt", "directory", "dangling-symlink"])("never overwrites ANY existing target (%s), including a second identical NEW seal", kind => {
    const f = fixture();
    if (kind === "valid") initializeNewRecoverySession(f.request);
    else {
      fs.mkdirSync(path.dirname(f.file), { recursive: true });
      if (kind === "directory") fs.mkdirSync(f.file);
      else if (kind === "dangling-symlink") fs.symlinkSync(path.join(f.root, "no-target"), f.file);
      else fs.writeFileSync(f.file, "retained corrupt bytes");
    }
    const stat = fs.lstatSync(f.file), bytes = stat.isFile() ? fs.readFileSync(f.file, "utf8") : undefined;
    const rename = vi.spyOn(fs, "renameSync");
    conflict(() => initializeNewRecoverySession(f.request));
    expect(fs.lstatSync(f.file).ino).toBe(stat.ino);
    if (bytes !== undefined) expect(fs.readFileSync(f.file, "utf8")).toBe(bytes);
    expect(rename).not.toHaveBeenCalled();
  });

  it("refuses bound REQUIRED with present/deleted seal, then unbound but authoritatively REQUIRED loss without recreation", () => {
    const f = fixture(); initializeNewRecoverySession(f.request); const bytes = fs.readFileSync(f.file, "utf8");
    const unbind = f.bind(); conflict(() => initializeNewRecoverySession(f.request)); expect(fs.readFileSync(f.file, "utf8")).toBe(bytes);
    fs.unlinkSync(f.file); const mkdir = vi.spyOn(fs, "mkdirSync"), rename = vi.spyOn(fs, "renameSync");
    conflict(() => initializeNewRecoverySession(f.request)); unbind(); f.require();
    conflict(() => initializeNewRecoverySession(f.request));
    expect(fs.existsSync(f.file)).toBe(false); expect(mkdir).not.toHaveBeenCalled(); expect(rename).not.toHaveBeenCalled();
  });

  it.each(["incomplete-native", "legacy-receiver", "async-NEW", "async-current", "ordinary-bound"])("refuses contradictory/non-synchronous NEW evidence (%s) before writes", kind => {
    const f = fixture(); let request = f.request;
    if (kind === "incomplete-native") request = { ...request, recovery: { ...f.recovery, historyComplete: false } };
    if (kind === "legacy-receiver") {
      const dir = path.join(f.root, "input-admission", "v1", "receiver", recoveryHash(f.sessionId));
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "legacy.json"), "retained legacy receiver");
    }
    if (kind === "async-NEW") request = { ...request, assertNew: async () => {} };
    if (kind === "async-current") request = { ...request, recovery: { ...f.recovery, assertCurrent: async () => {} } };
    if (kind === "ordinary-bound") { const { recovery: _owner, ...ordinary } = f.binding; cleanups.push(bindInputAdmission(ordinary)); }
    const before = fs.readdirSync(f.root), mkdir = vi.spyOn(fs, "mkdirSync"), rename = vi.spyOn(fs, "renameSync");
    expect(() => initializeNewRecoverySession(request)).toThrow(RecoveryAdmissionError);
    expect(fs.readdirSync(f.root)).toEqual(before); expect(fs.existsSync(f.file)).toBe(false);
    expect(mkdir).not.toHaveBeenCalled(); expect(rename).not.toHaveBeenCalled();
  });

  it.each(["NEW", "current"])("takes the shared owner BEFORE trusted reentrant %s callbacks", port => {
    const f = fixture(); const nested = () => initializeNewRecoverySession(f.request);
    if (port === "NEW") f.request.assertNew = nested;
    else f.recovery.assertCurrent = nested;
    expect(() => initializeNewRecoverySession(f.request)).toThrow(expect.objectContaining({ code: "RECOVERY_REVISION_CONFLICT" }));
    expect(fs.readdirSync(f.root)).toEqual([]);
  });

  it.each(["target-created", "became-REQUIRED", "binding", "reentrant", "abort"])("rechecks NEW/unbound/absent/current/io immediately before publish (%s)", cut => {
    const f = fixture(); const abort = new AbortController(), write = fs.writeFileSync; let staged = false;
    vi.spyOn(fs, "writeFileSync").mockImplementation((...args) => {
      write(...args);
      if (!staged && typeof args[0] === "number" && fs.fstatSync(args[0]).isFile()) {
        staged = true;
        if (cut === "target-created") write(f.file, "externally created target", { flag: "wx" });
        if (cut === "became-REQUIRED") f.require();
        if (cut === "binding") expect(() => f.bind()).toThrow(expect.objectContaining({ code: "RECOVERY_REVISION_CONFLICT" }));
        if (cut === "reentrant") f.request.assertNew = () => initializeNewRecoverySession(f.request);
        if (cut === "abort") abort.abort();
      }
    });
    if (cut === "binding") {
      initializeNewRecoverySession(f.request); expect(fs.existsSync(f.file)).toBe(true);
    } else {
      expect(() => initializeNewRecoverySession(f.request, { signal: abort.signal })).toThrow(RecoveryAdmissionError);
      if (cut === "target-created") expect(fs.readFileSync(f.file, "utf8")).toBe("externally created target");
      else expect(fs.existsSync(f.file)).toBe(false);
    }
    expect(staged).toBe(true);
    expect(fs.readdirSync(path.dirname(f.file)).filter(p => p.endsWith(".tmp"))).toEqual([]);
  });

  it.each(["missing-root", "root-file", "root-symlink", "ledger-directory", "ledger-symlink", "namespace-file"])("read-only structural validation refuses %s without alias/fresh-root fallback", kind => {
    const f = fixture();
    if (kind.startsWith("ledger")) initializeNewRecoverySession(f.request);
    if (kind === "missing-root") fs.rmdirSync(f.root);
    if (kind === "root-file") { fs.rmdirSync(f.root); fs.writeFileSync(f.root, "root is not a directory"); }
    if (kind === "root-symlink") { const actual = `${f.root}-actual`; fs.renameSync(f.root, actual); fs.symlinkSync(actual, f.root); }
    if (kind === "ledger-directory") { fs.unlinkSync(f.file); fs.mkdirSync(f.file); }
    if (kind === "ledger-symlink") { const actual = `${f.file}.retained`; fs.renameSync(f.file, actual); fs.symlinkSync(actual, f.file); }
    if (kind === "namespace-file") fs.writeFileSync(path.join(f.root, "input-admission"), "not a namespace directory");
    if (!kind.startsWith("ledger")) incomplete(() => initializeNewRecoverySession(f.request));
    f.bind(); const mkdir = vi.spyOn(fs, "mkdirSync"), rename = vi.spyOn(fs, "renameSync");
    incomplete(() => readRecoverySession(f.sessionId));
    expect(mkdir).not.toHaveBeenCalled(); expect(rename).not.toHaveBeenCalled();
  });

  it.each(["EACCES", "EIO"])("other open/barrier I/O errors remain %s, never empty/incomplete success", code => {
    const f = fixture(); initializeNewRecoverySession(f.request); f.bind();
    const error = Object.assign(new Error(code), { code }); const open = fs.openSync;
    const spy = vi.spyOn(fs, "openSync").mockImplementation((...args) => { if (String(args[0]) === f.file) throw error; return open(...args); });
    expect(() => readRecoverySession(f.sessionId)).toThrow(error); spy.mockRestore();
    vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw error; });
    expect(() => readRecoverySession(f.sessionId)).toThrow(error);
  });

  it.each(["after-rename", "after-durable"])("resolves staged seal unknown ACK %s only with exact read + fresh file/namespace barriers", cut => {
    const f = fixture(); const rename = fs.renameSync; let published = false;
    vi.spyOn(fs, "renameSync").mockImplementation((...args) => { rename(...args); if (String(args[1]) === f.file) { published = true; if (cut === "after-rename") throw new Error("seal ACK lost"); } });
    if (cut === "after-durable") f.request.assertNew = () => { if (published) throw new Error("seal ACK lost"); };
    expect(() => initializeNewRecoverySession(f.request)).toThrow(/seal ACK lost/); vi.restoreAllMocks();
    const bytes = fs.readFileSync(f.file, "utf8"), stat = fs.statSync(f.file);
    f.request.assertNew = () => {}; conflict(() => initializeNewRecoverySession(f.request));
    expect(fs.statSync(f.file).ino).toBe(stat.ino); expect(fs.readFileSync(f.file, "utf8")).toBe(bytes);
    f.bind(); const sync = vi.spyOn(fs, "fsyncSync");
    expect(readRecoverySession(f.sessionId)).toEqual(JSON.parse(bytes).payload);
    expect(sync.mock.calls.length).toBeGreaterThan(1); expect(fs.readFileSync(f.file, "utf8")).toBe(bytes);
  });
});

describe.runIf(process.platform === "linux")("C2 candidate-bundled native process seal cuts", () => {
  it.each(["before-publish", "after-rename", "after-durable", "after-required"])("awaits real exit at %s, never initializes REQUIRED reopen or overwrites an unknown seal", async stage => {
    const temp = fs.mkdtempSync(path.join(os.tmpdir(), "b5-c2-process-")), root = path.join(temp, "store"); fs.mkdirSync(root);
    const bundle = path.join(temp, "worker.mjs"), trace = path.join(temp, "trace.jsonl"), fact = path.join(temp, "identity.json");
    fs.writeFileSync(fact, JSON.stringify({ mode: "NEW", sessionId: "seal-session" }), { flag: "wx" });
    // Default output stays in this test's temp. Only an explicit, caller-owned
    // destination may retain evidence beyond Vitest's run-root teardown.
    const destination = process.env.B5_C2_PROCESS_EVIDENCE_DIR;
    if (destination !== undefined && (!path.isAbsolute(destination) || !fs.lstatSync(destination).isDirectory() || fs.lstatSync(destination).isSymbolicLink())) {
      throw new Error("B5_C2_PROCESS_EVIDENCE_DIR must be an existing absolute caller-owned directory");
    }
    const evidence = destination === undefined ? temp : fs.mkdtempSync(path.join(destination, `${stage}-`));
    const persist = () => {
      if (evidence === temp) return;
      // Exact paths allocated by THIS scenario, never a glob/copy of arbitrary traces.
      if (fs.existsSync(trace)) fs.copyFileSync(trace, path.join(evidence, "trace.jsonl"));
      fs.copyFileSync(fact, path.join(evidence, "identity.json"));
    };
    const lifecycle = c2Lifecycle(close => cleanups.push(async () => {
      try { await close(); } finally { persist(); }
    })); // Ownership/cancellation is installed BEFORE the first asynchronous build operation.
    await lifecycle.bundle(() => context({ entryPoints: [fileURLToPath(new URL("./fixtures/b5-c2-seal-worker.ts", import.meta.url))], outfile: bundle, bundle: true, format: "esm", platform: "node", target: "node24" }));
    const run = async (mode: string) => {
      const result = await lifecycle.run(
        () => spawn(process.execPath, [bundle, root, trace, fact, stage, mode], { stdio: ["ignore", "pipe", "pipe"] }),
        terminal => {
          fs.appendFileSync(path.join(evidence, "processes.jsonl"), JSON.stringify({ stage, mode, boundary: "close", ...terminal }) + "\n");
          persist(); // Includes actual exit separately from terminal, drained output, even on cancellation.
        },
      );
      if (result.code !== 0 && result.signal !== "SIGKILL") throw new Error(result.stderr || "C2 worker failed after actual child exit and stdio close");
      return result;
    };
    expect(await run("crash")).toMatchObject({ code: null, signal: "SIGKILL" });
    // Persist the exact cut in the command log before later reopen assertions, even
    // if a deadline fails and Vitest removes its temporary root at global teardown.
    console.info("C2 native exit/cut trace", fs.readFileSync(trace, "utf8").trim());
    const file = path.join(root, "input-admission", "v1", "session", `${recoveryHash("seal-session")}.json`);
    const events = fs.readFileSync(trace, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(events.at(-1)).toMatchObject({ stage, ack: false, required: stage === "after-required" });
    expect(JSON.parse(fs.readFileSync(fact, "utf8"))).toEqual({ mode: stage === "after-required" ? "REQUIRED" : "NEW", sessionId: "seal-session" });
    if (stage === "before-publish") {
      expect(fs.existsSync(file)).toBe(false);
      expect(JSON.parse((await run("read")).stdout)).toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
      expect(fs.existsSync(file)).toBe(false);
      expect((await run("finish-new")).code).toBe(0);
    } else {
      const bytes = fs.readFileSync(file, "utf8"), stat = fs.statSync(file);
      expect(JSON.parse((await run("retry-new")).stdout)).toMatchObject({ code: "RECOVERY_CONFLICT" });
      expect(fs.statSync(file).ino).toBe(stat.ino); expect(fs.readFileSync(file, "utf8")).toBe(bytes);
    }
    const empty = JSON.parse((await run("read")).stdout); expect(empty).toMatchObject({ revision: 0, complete: true, inputs: [], recoveryGenerationPins: [] });
    const admitted = JSON.parse((await run("admit")).stdout); expect(admitted.ack.queued).toBe(true); expect(admitted.state.inputs).toHaveLength(1);
    expect(admitted.nativeDeliveries).toBe(0);
    const bytes = fs.readFileSync(file, "utf8");
    expect(JSON.parse((await run("read")).stdout)).toEqual(admitted.state); expect(fs.readFileSync(file, "utf8")).toBe(bytes);
    expect(fs.readFileSync(trace, "utf8")).not.toContain('"ack":true');
    // Retain process trace outside the stores; a REQUIRED deletion must not bootstrap.
    fs.unlinkSync(file);
    expect(JSON.parse((await run("read")).stdout)).toMatchObject({ code: "RECOVERY_HISTORY_INCOMPLETE" });
    expect(fs.existsSync(file)).toBe(false);
  }, 30_000);
});
