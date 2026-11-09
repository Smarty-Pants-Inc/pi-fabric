// Keep: Astra's P1 at 95e178ed — deployed pre-reservation launcher refusals
// became CUSTODY_UNCONFIRMED, exhausted admission and blocked Main's release.
// Exercise the public receiver -> actual transport/launcher -> manager catch /
// real admission release -> actual release checkpoint, not a parser return mock.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { AgentAdmission } from "../src/agents/lifecycle.js";
import { AgentInputError, RequiredInputMissingError } from "../src/agents/input-validation.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import type { AgentRunRequest } from "../src/agents/types.js";
import { isHostedOnlyAllowed } from "../scripts/hosted-only.mjs";

type Form = "today-host" | "today-admission" | "future" | "legacy";
type Mode = "default-unset" | "strict-zero";
type Receiver = "spawn" | "run";
type Trace = Record<string, unknown>;
const modes: Mode[] = ["default-unset", "strict-zero"];
const forms: Form[] = ["today-host", "today-admission", "future"];
const receivers: Receiver[] = ["spawn", "run"];
// Match temp-run-sweep's inherited Node guard contract without filtering raw stderr.
const knownReporterPrefix = isHostedOnlyAllowed(process.env) ? "" :
  "hosted-only (outside-temp deletion reporting): push and let CI run it\n";
const activeFixtures: Array<{ manager: AgentManager; root: string; trace: Trace[] }> = [];
const errorTrace = (error: unknown): Trace => {
  const value = error as Error & { code?: unknown; field?: unknown; launchOutcome?: unknown; path?: unknown; index?: unknown; stdout?: unknown; stderr?: unknown };
  return { name: value.name, message: value.message, code: value.code, field: value.field,
    launchOutcome: value.launchOutcome, path: value.path, index: value.index, stdout: value.stdout, stderr: value.stderr };
};
const readIfPresent = (file: string): string | undefined => fs.existsSync(file) ? fs.readFileSync(file, "utf8") : undefined;
const treeFiles = (root: string): string[] => !fs.existsSync(root) ? [] : fs.readdirSync(root, { withFileTypes: true }).flatMap(entry => {
  const name = path.join(root, entry.name);
  return entry.isDirectory() ? treeFiles(name) : [name];
});

afterEach(async () => {
  try {
    for (const fixture of activeFixtures.splice(0)) {
      try { await fixture.manager.close(); fixture.trace.push({ stage: "manager.close", outcome: "closed" }); }
      catch (error) { fixture.trace.push({ stage: "manager.close", error: errorTrace(error) }); }
      // Retain actual raw stdout/stderr, argv and custody traces, including on a
      // RED assertion. No teardown deletes; the enclosing test-temp owner owns
      // these private roots. Print the bytes too, before its global teardown.
      const raw = Object.fromEntries(treeFiles(path.join(fixture.root, "wire")).map(file => [path.relative(fixture.root, file), fs.readFileSync(file, "utf8")]));
      const evidence = { root: fixture.root, raw, trace: fixture.trace,
        runFiles: treeFiles(path.join(fixture.root, "runs")).map(file => path.relative(fixture.root, file)),
        heldPermits: fixture.manager.heldPermits(), retainedCustody: fixture.manager.hasRetainedCustody() };
      fs.writeFileSync(path.join(fixture.root, "refusal-custody-evidence.json"), JSON.stringify(evidence, null, 2));
      console.info("placement-launcher-refusal-custody evidence", JSON.stringify(evidence));
    }
  } finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); }
});

function fixture(mode: Mode) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "placement-refusal-custody-")));
  const home = path.join(root, "home"), cwd = path.join(root, "workspace"), callerTmp = path.join(root, "caller-tmp");
  const profile = path.join(root, "profile"), wire = path.join(root, "wire"), bin = path.join(root, "install", "bin");
  for (const dir of [home, cwd, callerTmp, profile, wire, bin]) fs.mkdirSync(dir, { recursive: true });
  vi.stubEnv("HOME", home); vi.stubEnv("USERPROFILE", home); vi.spyOn(os, "homedir").mockReturnValue(home);
  vi.stubEnv("TMPDIR", callerTmp); vi.stubEnv("PI_CODING_AGENT_DIR", profile);
  vi.stubEnv("SMARTY_ALPHA", mode === "strict-zero" ? "0" : undefined);
  expect(process.env.SMARTY_ALPHA).toBe(mode === "strict-zero" ? "0" : undefined);
  const trace: Trace[] = [];
  const wireTrace = path.join(wire, "fixture.jsonl");
  const writeNode = (file: string, source: string) => {
    fs.writeFileSync(file, `#!${process.execPath}\n${source}\n`, { mode: 0o700 });
  };
  const prefix = `import fs from 'node:fs';\nconst trace = data => fs.appendFileSync(${JSON.stringify(wireTrace)}, JSON.stringify(data) + '\\n');\n`;
  const launcher = path.join(bin, "smarty-task-ryzen2");
  const scenarioFile = path.join(root, "launcher-case.json");
  writeNode(launcher, prefix + `
import path from 'node:path';
const argv = process.argv.slice(2), id = argv[0];
const scenario = JSON.parse(fs.readFileSync(${JSON.stringify(scenarioFile)}, 'utf8'));
const stem = path.join(${JSON.stringify(wire)}, id);
fs.writeFileSync(stem + '.argv.json', JSON.stringify(argv));
fs.writeFileSync(stem + '.stdout.raw', scenario.stdout);
fs.writeFileSync(stem + '.stderr.raw', scenario.stderr);
fs.writeFileSync(stem + '.exit.json', JSON.stringify({code: scenario.code}));
trace({stage:'launcher', id, argv, code:scenario.code, stdout:scenario.stdout, stderr:scenario.stderr});
process.stdout.write(scenario.stdout);
process.stderr.write(scenario.stderr);
process.exitCode = scenario.code;
`);
  const picker = path.join(bin, "picker.mjs");
  writeNode(picker, prefix + `trace({stage:'picker', argv:process.argv.slice(2)}); process.stdout.write('worker-a\\n');`);
  const source = path.join(root, "install", "setup", "factory", "task-preflight.py");
  fs.mkdirSync(path.dirname(source), { recursive: true });
  fs.writeFileSync(source, [
    "import json, sys",
    `with open(${JSON.stringify(wireTrace)}, 'a') as out: out.write(json.dumps({'stage': 'probe' if '--probe' in sys.argv else 'admit', 'argv': sys.argv[1:], 'task': '' if '--probe' in sys.argv else sys.stdin.read()}) + '\\n')`,
    "if '--probe' in sys.argv: sys.exit(0)",
    "count_at = sys.argv.index('--admit') + 3",
    "count = int(sys.argv[count_at])",
    "paths = sys.argv[count_at + 1:count_at + 1 + count]",
    "sys.stdout.write('\\0'.join(['', '', 'fixture admission permits original task', '0'] + [p for file in paths for p in [file, file]]) + '\\0')",
  ].join("\n"));
  // Run the real preflight's generated read-only shell, including --probe and
  // test -e, inside our private directory. Strict cannot be short-circuited to
  // an invented 'admitted' transport result. No SSH connection is made.
  const pathBin = path.join(root, "path"); fs.mkdirSync(pathBin);
  writeNode(path.join(pathBin, "ssh"), prefix + `
import {spawnSync} from 'node:child_process';
const argv = process.argv.slice(2);
const result = spawnSync('/bin/sh', ['-c', argv.at(-1)], {encoding:'utf8', env:process.env});
trace({stage:'ssh', argv, code:result.status, stdout:result.stdout, stderr:result.stderr});
if (result.stdout) process.stdout.write(result.stdout);
if (result.stderr) process.stderr.write(result.stderr);
process.exitCode = result.status ?? 1;
`);
  fs.writeFileSync(path.join(pathBin, "python3"), '#!/bin/sh\nexec /usr/bin/python3 "$@"\n', { mode: 0o700 });
  vi.stubEnv("PATH", `${pathBin}${path.delimiter}${process.env.PATH ?? ""}`);
  const requires = [path.join(cwd, "input"), path.join(cwd, "input literal {host} ' ; $(literal)")];
  for (const file of requires) fs.writeFileSync(file, "private fixture: strict preflight admits before launcher refusal\n");
  const task = `Read the declared inputs; retain literal 'quotes', $HOME, {host} and --require.\n${requires[1]}`;
  const localMarker = path.join(root, "local-worker-started");
  const worker = path.join(root, "local.mjs");
  fs.writeFileSync(worker, `import fs from 'node:fs';
const args = new Map(); for (let i=2; i<process.argv.length; i+=2) args.set(process.argv[i], process.argv[i+1]);
fs.writeFileSync(${JSON.stringify(localMarker)}, 'LOCAL FALLBACK');
const now = Date.now(); fs.writeFileSync(args.get('--status-file'), JSON.stringify({
  id:args.get('--id'), name:args.get('--name'), task:fs.readFileSync(args.get('--task-file'),'utf8'),
  status:'completed', runner:'pi', transport:'process', cwd:args.get('--cwd'), startedAt:now, updatedAt:now,
  finishedAt:now, turns:1, toolCalls:0, text:'LOCAL FALLBACK', usage:{input:0,output:0,cacheRead:0,cacheWrite:0,cost:0}
}));`);
  const runRoot = path.join(root, "runs");
  const placement = normalizeFabricConfig({ agents: { placement: {
    default: "remote", sharedMounts: [cwd],
    command: [launcher, "{id}", "--host", "auto", "--cwd", "{cwd}", "--", "{task}"],
    pickerCommand: [process.execPath, picker], sshAliases: { "worker-a": "worker-a-agent" },
    resultDirectory: path.join(root, "results", "{id}"),
    cancelCommand: [process.execPath, "-e", "process.exit(0)"], pollIntervalMs: 10, commandTimeoutMs: 5_000,
  } } }).agents.placement!;
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, placement,
    maxConcurrent: 1, timeoutMs: 30_000, budgetUsd: 0, retainRuns: true, sessionExport: false }, {
    workerPath: worker, runRoot, shortTmpdirSource: () => ({ tmpdir: callerTmp }), fullCodeMode: false,
    // Normal hosts register settlement delivery before preparation; record only,
    // so the manager itself must establish and preserve archive-marker custody.
    onSettled: result => { trace.push({ stage: "manager.onSettled", result }); },
  });
  activeFixtures.push({ manager, root, trace });
  const permits = { acquired: 0, released: 0, active: 0, peak: 0 };
  const acquire = AgentAdmission.prototype.tryAcquire;
  vi.spyOn(AgentAdmission.prototype, "tryAcquire").mockImplementation(function (this: AgentAdmission, ...args) {
    const release = acquire.apply(this, args);
    trace.push({ stage: "admission.tryAcquire", parent: args[0], granted: Boolean(release), active: permits.active });
    if (!release) return release;
    permits.acquired++; permits.active++; permits.peak = Math.max(permits.peak, permits.active);
    let released = false;
    return () => {
      // Observe the real idempotent permit, never grant or release one ourselves.
      release();
      if (!released) { released = true; permits.released++; permits.active--; trace.push({ stage: "admission.release", active: permits.active }); }
    };
  });
  const execute = processUtils.executeFile;
  vi.spyOn(processUtils, "executeFile").mockImplementation(async (...args) => {
    try {
      const result = await execute(...args);
      trace.push({ stage: "executeFile.close", executable: args[0], argv: args[1], code: 0, ...result });
      return result;
    } catch (error) {
      // These are the actual execFile rejection bytes, not a constructed Error.
      trace.push({ stage: "executeFile.close", executable: args[0], argv: args[1], ...errorTrace(error) });
      throw error;
    }
  });
  const launches: Array<{ id: string; task: string; requires: string[] | undefined; runFiles: string[]; preparedBytes: Record<string, string> }> = [];
  const launch = ProcessTransport.prototype.launch;
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
    const taskFile = request.workerArguments[request.workerArguments.indexOf("--task-file") + 1]!;
    const preparedFiles = treeFiles(path.dirname(taskFile));
    const preparedBytes = Object.fromEntries(preparedFiles.flatMap(file => {
      const relative = path.relative(path.dirname(taskFile), file);
      return relative === "task.txt" || relative === "archive-pending.json" || !/session/iu.test(relative) && /\.(?:log|jsonl|ndjson)$/iu.test(relative)
        ? [[relative, fs.readFileSync(file).toString("base64")]] : [];
    }));
    const snapshot = { id: request.id, task: fs.readFileSync(taskFile, "utf8"), requires: request.requires,
      runFiles: preparedFiles.map(file => path.relative(runRoot, file)), preparedBytes };
    launches.push(snapshot); trace.push({ stage: "transport.receiver", ...snapshot });
    expect(snapshot.preparedBytes).toHaveProperty(["archive-pending.json"]);
    expect(JSON.parse(Buffer.from(snapshot.preparedBytes["archive-pending.json"]!, "base64").toString("utf8")))
      .toMatchObject({ format: 1, awaitingResult: true, ownerPid: process.pid });
    const onUnconfirmedExit = request.onUnconfirmedExit;
    try {
      return await launch.call(this, { ...request, onUnconfirmedExit: reason => {
        trace.push({ stage: "transport.onUnconfirmedExit -> manager", id: request.id, reason });
        onUnconfirmedExit?.(reason);
      } });
    } catch (error) {
      // Include any real non-session log prepared/appended by the transport up
      // to its refusal, before the manager takes archival custody.
      for (const file of treeFiles(path.dirname(taskFile))) {
        const relative = path.relative(path.dirname(taskFile), file);
        if (!/session/iu.test(relative) && /\.(?:log|jsonl|ndjson)$/iu.test(relative)) {
          snapshot.preparedBytes[relative] = fs.readFileSync(file).toString("base64");
        }
      }
      trace.push({ stage: "transport.throw -> manager.catch", id: request.id, ...errorTrace(error) });
      throw error;
    }
  });
  const localSpawn = vi.spyOn(processUtils, "spawnDetached"); // Call-through observation, not a local-transport substitute.
  const request: AgentRunRequest = { task, requires, name: "launcher refusal custody", transport: "process" };
  const emit = (stdout: string, stderr: string, code = 3) => fs.writeFileSync(scenarioFile, JSON.stringify({ stdout, stderr, code }));
  return { root, wire, manager, permits, trace, launches, request, requires, task, cwd, runRoot, launcher, localSpawn, localMarker, emit };
}
type Fixture = ReturnType<typeof fixture>;

const signed = (form: Form, reason: string): string => form === "today-host"
  ? `smarty-task-ryzen2: REFUSED host=worker-a: ${reason}; no task started`
  : form === "today-admission" ? `smarty-task-ryzen2: REFUSED: ${reason}; no task started`
    : `PREFLIGHT_REFUSED: ${reason}; no task started`;
const refusalLine = (f: Fixture, form: Form, reason: "missing" | "admission" | "path-collision" | "longer-path" | "decorated-missing" | "substring"): string => {
  const file = f.requires[1]!;
  if (form === "legacy") return `PREFLIGHT_REFUSED: ${file}: does not exist on target`;
  return signed(form, reason === "missing" ? `missing input ${file}`
    : reason === "admission" ? "home-directory inputs are not portable; use needs corpus"
      : reason === "path-collision" ? `${file}: does not exist on target`
        : reason === "longer-path" ? `missing input ${file}-other`
          : reason === "decorated-missing" ? `missing input ${file}: admission denied`
            : `admission denied for missing input ${file}`);
};
async function rejectAtReceiver(f: Fixture, method: Receiver, missing: boolean) {
  f.trace.push({ stage: `receiver.agents.${method}`, task: f.task, requires: f.requires });
  const outcome = await f.manager[method](f.request).then(value => ({ kind: "returned" as const, value }), error => ({ kind: "rejected" as const, error }));
  if (outcome.kind !== "rejected") throw new Error(`Expected typed UNLAUNCHED, receiver returned ${JSON.stringify(outcome.value)}`);
  const error: unknown = outcome.error;
  f.trace.push({ stage: `receiver.agents.${method}.rejected`, ...errorTrace(error) });
  expect(error).toBeInstanceOf(AgentInputError);
  expect((error as Error).constructor).toBe(missing ? RequiredInputMissingError : AgentInputError);
  expect(error).toMatchObject({ code: "FABRIC_AGENT_INPUT_ERROR", launchOutcome: "unlaunched", field: missing ? "requires" : "placement" });
  if (missing) expect(error).toMatchObject({ path: f.requires[1], index: 1 });
  else { expect(error).not.toHaveProperty("path"); expect(error).not.toHaveProperty("index"); }
}
function assertWire(f: Fixture, mode: Mode, count: number, stdout: string, stderr: string, code = 3) {
  const expectedNative = { code, stdout, stderr: knownReporterPrefix + stderr };
  const rows = fs.readFileSync(path.join(f.wire, "fixture.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line) as Trace);
  expect(rows.map(row => row.stage)).toEqual(Array.from({ length: count }, () => mode === "strict-zero"
    ? ["picker", "admit", "probe", "ssh", "launcher"] : ["picker", "launcher"]).flat());
  expect(rows.filter(row => row.stage === "picker")).toHaveLength(count);
  expect(rows.filter(row => row.stage === "launcher")).toHaveLength(count);
  for (const stage of ["admit", "probe", "ssh"]) expect(rows.filter(row => row.stage === stage)).toHaveLength(mode === "strict-zero" ? count : 0);
  for (const row of rows.filter(row => row.stage === "admit")) expect(row.task).toBe(f.task);
  for (const row of rows.filter(row => row.stage === "ssh")) {
    expect(row).toMatchObject({ code: 0, stdout: "", stderr: "" });
    expect(row.argv).toContain("worker-a-agent");
  }
  for (const snapshot of f.launches) {
    expect(snapshot).toMatchObject({ task: f.task, requires: f.requires });
    const stem = path.join(f.wire, snapshot.id);
    expect(JSON.parse(fs.readFileSync(stem + ".argv.json", "utf8"))).toEqual([
      snapshot.id, "--host", "worker-a", "--cwd", f.cwd,
      ...f.requires.flatMap(file => ["--require", file]), "--", f.task,
    ]);
    expect(fs.readFileSync(stem + ".stdout.raw", "utf8")).toBe(stdout);
    expect(fs.readFileSync(stem + ".stderr.raw", "utf8")).toBe(stderr);
    expect(JSON.parse(fs.readFileSync(stem + ".exit.json", "utf8"))).toEqual({ code });
    const nativeClose = f.trace.find(row => row.stage === "executeFile.close" && row.executable === f.launcher && (row.argv as string[])[0] === snapshot.id);
    expect(nativeClose).toMatchObject(expectedNative);
  }
  expect(f.localSpawn).not.toHaveBeenCalled();
  expect(fs.existsSync(f.localMarker)).toBe(false);
  expect(fs.existsSync(path.join(f.root, "results"))).toBe(false);
}
interface CleanupReceipt { source: string; destination: string; id: string; state: string; }
function cleanupReceipts(f: Fixture): Array<{ receiptFile: string; receipt: CleanupReceipt }> {
  const archive = `${f.runRoot}.trace-archive`;
  if (!fs.existsSync(archive)) return [];
  // Existing whole-root custody contract: inspect cleanup containers' receipts,
  // not an invented rollback location or a fabricated worker status/identity.
  return fs.readdirSync(archive).filter(name => name.startsWith("cleanup-")).map(name => {
    const receiptFile = path.join(archive, name, "custody.json");
    return { receiptFile, receipt: JSON.parse(fs.readFileSync(receiptFile, "utf8")) as CleanupReceipt };
  });
}
function assertArchivedPreparation(f: Fixture) {
  const receipts = cleanupReceipts(f);
  for (const snapshot of f.launches) {
    const source = path.join(f.runRoot, snapshot.id);
    const matching = receipts.filter(row => row.receipt.source === source);
    expect(matching, `whole prepared run must be retained for ${snapshot.id}`).toHaveLength(1);
    const { receiptFile, receipt } = matching[0]!;
    expect(receipt).toMatchObject({ source, id: snapshot.id, state: "moved" });
    expect(path.relative(f.root, receipt.destination).startsWith("..")).toBe(false);
    expect(fs.existsSync(source)).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(path.dirname(receiptFile), "custody.prepared.json"), "utf8")))
      .toMatchObject({ source, state: "prepared" });
    expect(fs.readFileSync(path.join(receipt.destination, "task.txt"))).toEqual(Buffer.from(f.task, "utf8"));
    for (const [relative, base64] of Object.entries(snapshot.preparedBytes)) {
      const before = Buffer.from(base64, "base64"), retained = fs.readFileSync(path.join(receipt.destination, relative));
      if (relative === "task.txt" || relative === "archive-pending.json") {
        expect(retained, relative).toEqual(before);
      } else {
        // Logs may gain legitimate transport/rollback entries after preparation;
        // every original prepared byte must survive that append, not be rewritten.
        expect(retained.subarray(0, before.length), relative).toEqual(before);
      }
    }
    f.trace.push({ stage: "manager.preworker-rollback.archive", receiptFile, receipt,
      retainedPreparation: Object.fromEntries(Object.keys(snapshot.preparedBytes).map(relative =>
        [relative, fs.readFileSync(path.join(receipt.destination, relative)).toString("base64")])) });
  }
}
async function assertCleanCheckpoint(f: Fixture) {
  assertArchivedPreparation(f);
  expect(f.manager.heldPermits()).toBe(0);
  expect(f.manager.hasRetainedCustody()).toBe(false);
  expect(f.manager.list()).toEqual([]);
  expect(f.trace.filter(row => row.stage === "transport.onUnconfirmedExit -> manager")).toEqual([]);
  for (const { id } of f.launches) {
    expect(f.manager.hasRunCustody(id)).toBe(false);
    expect(f.manager.runDirectory(id)).toBeUndefined(); // Rejection can legitimately roll registration back.
    expect(fs.existsSync(path.join(f.runRoot, id, "unresolved-worker.json"))).toBe(false);
  }
  expect(treeFiles(f.runRoot).filter(file => path.basename(file) === "unresolved-worker.json")).toEqual([]);
  f.manager.pauseForRelease();
  try {
    await f.manager.checkpointForRelease();
    const receipt = f.manager.releaseCheckpointReceipt();
    expect(receipt).toMatchObject({ admissionHeld: true, workers: [] });
    f.trace.push({ stage: "checkpointForRelease", outcome: "clean", receipt });
  } catch (error) {
    f.trace.push({ stage: "checkpointForRelease", outcome: "unexpectedly blocked", ...errorTrace(error) });
    throw error;
  } finally { f.manager.resumeAfterRelease(); }
  // A proven preworker refusal is not a fabricated worker settlement.
  expect(f.trace.filter(row => row.stage === "manager.onSettled")).toEqual([]);
}

// Installed fleet bootstrap/probe protocol is POSIX-only, as in the existing
// process-placement-affinity suite. This does not change any hosted-only gate.
describe.skipIf(process.platform === "win32")("launcher refusal receiver/bootstrap/attribution custody (Astra P1)", () => {
  it.each(modes.flatMap(mode => forms.flatMap(form => receivers.flatMap(method =>
    (["missing", "admission"] as const).map(reason => ({ mode, form, method, reason })),
  ))))("$mode $form $reason through agents.$method releases real max1 admission and checkpoints", async ({ mode, form, method, reason }) => {
    const f = fixture(mode), line = refusalLine(f, form, reason);
    // TODAY refuses on stderr; FUTURE also accepts stdout transport output.
    const stdout = form === "future" ? line + "\n" : "", stderr = form === "future" ? "" : line + "\n";
    f.emit(stdout, stderr);
    await rejectAtReceiver(f, method, reason === "missing");
    expect(f.permits).toEqual({ acquired: 1, released: 1, active: 0, peak: 1 });
    // Real reacquisition at concurrency=1 proves capacity, not merely an empty
    // heldPermits counter. Use spawn so a regression queues instead of hanging run.
    await rejectAtReceiver(f, "spawn", reason === "missing");
    expect(f.permits).toEqual({ acquired: 2, released: 2, active: 0, peak: 1 });
    assertWire(f, mode, 2, stdout, stderr);
    await assertCleanCheckpoint(f);
  });

  it.each(modes.flatMap(mode => receivers.map(method => ({ mode, method }))))(
    "$mode LEGACY declared path at index1 through agents.$method remains typed and checkpoint-clean", async ({ mode, method }) => {
      const f = fixture(mode), stdout = refusalLine(f, "legacy", "missing") + "\n";
      f.emit(stdout, "");
      await rejectAtReceiver(f, method, true);
      expect(f.permits).toEqual({ acquired: 1, released: 1, active: 0, peak: 1 });
      assertWire(f, mode, 1, stdout, "");
      await assertCleanCheckpoint(f);
    },
  );

  it.each(modes.flatMap(mode => forms.flatMap(form =>
    (["path-collision", "longer-path", "decorated-missing", "substring"] as const).map(reason => ({ mode, form, reason })),
  )))("$mode $form $reason is generic AgentInputError, never prefix/substring-attributed missing input", async ({ mode, form, reason }) => {
    const f = fixture(mode), stderr = refusalLine(f, form, reason) + "\n";
    f.emit("", stderr);
    await rejectAtReceiver(f, "spawn", false);
    expect(f.permits).toEqual({ acquired: 1, released: 1, active: 0, peak: 1 });
    assertWire(f, mode, 1, "", stderr);
    await assertCleanCheckpoint(f);
  });

  it.each(modes.flatMap(mode => [
    "unsigned-exit3", "wrong-prefix-exit3", "missing-suffix-exit3", "today-signed-exit7",
    "future-signed-exit7", "legacy-exit7", "future-signed-exit0", "acceptance-contradiction-exit3",
  ].map(kind => ({ mode, kind }))))("$mode $kind preserves unknown custody, max1 permit, marker and blocked checkpoint", async ({ mode, kind }) => {
    const f = fixture(mode);
    const valid = signed("today-host", `missing input ${f.requires[1]}`);
    const line = kind === "unsigned-exit3" ? "launcher rejected"
      : kind === "wrong-prefix-exit3" ? `untrusted-prefix ${valid}`
        : kind === "missing-suffix-exit3" ? valid.replace(/; no task started$/u, "")
          : kind.startsWith("future-") ? signed("future", `missing input ${f.requires[1]}`)
            : kind === "legacy-exit7" ? refusalLine(f, "legacy", "missing") : valid;
    const code = kind.endsWith("exit7") ? 7 : kind.endsWith("exit0") ? 0 : 3;
    // An accepted receipt contradicts non-launch even when a signed refusal is
    // also present. Keep both actual streams; never discharge that ambiguity.
    const stdout = kind === "acceptance-contradiction-exit3" ? "RYZEN2_TASK_ACCEPTED contradictory on worker-a\n" : "";
    const stderr = line + "\n"; f.emit(stdout, stderr, code);
    f.trace.push({ stage: "receiver.agents.run", task: f.task, requires: f.requires });
    const result = await f.manager.run(f.request);
    f.trace.push({ stage: "receiver.agents.run.returned", result });
    expect(result).toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
    expect(result.error).toContain("Placement launch outcome unknown");
    expect(f.manager.heldPermits()).toBe(1);
    expect(f.manager.hasRetainedCustody()).toBe(true);
    expect(f.permits).toEqual({ acquired: 1, released: 0, active: 1, peak: 1 });
    expect(f.trace.filter(row => row.stage === "transport.onUnconfirmedExit -> manager")).toHaveLength(1);
    const marker = path.join(f.runRoot, result.id, "unresolved-worker.json");
    expect(JSON.parse(fs.readFileSync(marker, "utf8"))).toMatchObject({ runId: result.id, transport: "process" });
    expect(f.manager.runDirectory(result.id)).toBe(path.join(f.runRoot, result.id));
    expect(fs.readFileSync(path.join(f.runRoot, result.id, "task.txt"))).toEqual(Buffer.from(f.task, "utf8"));
    expect(cleanupReceipts(f).filter(row => row.receipt.source === path.join(f.runRoot, result.id))).toEqual([]);
    f.trace.push({ stage: "unknown.active-tree-retained", source: path.join(f.runRoot, result.id), archived: false });
    const queued = await f.manager.spawn({ ...f.request, task: "must remain queued behind unknown custody" });
    expect(queued.status).toBe("queued");
    expect(f.launches).toHaveLength(1);
    expect(f.permits).toEqual({ acquired: 1, released: 0, active: 1, peak: 1 });
    await f.manager.stop(queued.id);
    assertWire(f, mode, 1, stdout, stderr, code);
    f.manager.pauseForRelease();
    try {
      await expect(f.manager.checkpointForRelease().catch(error => {
        f.trace.push({ stage: "checkpointForRelease.rejected", ...errorTrace(error) });
        throw error;
      })).rejects.toThrow(/Agent release quiescence.*(?:unresolved|pending)/u);
      expect(() => f.manager.releaseCheckpointReceipt()).toThrow(/unavailable or unsettled/u);
      f.trace.push({ stage: "checkpointForRelease", outcome: "blocked by actual unknown custody", marker: readIfPresent(marker) });
    } finally { f.manager.resumeAfterRelease(); }
    expect(fs.existsSync(marker)).toBe(true);
    expect(cleanupReceipts(f).filter(row => row.receipt.source === path.join(f.runRoot, result.id))).toEqual([]);
    expect(f.manager.heldPermits()).toBe(1);
    expect(f.permits.released).toBe(0);
  });
});
