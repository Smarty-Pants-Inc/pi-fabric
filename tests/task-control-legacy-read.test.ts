import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readTerminalAdmissions, terminalControlMatchesAdmission, terminalControlRefusalProven, TerminalNoticeOutbox, appendTerminalAdmission } from "../src/agents/terminal-notices.js";
import {
  FabricTaskControlDirectoryError, initializeTaskControlDirectory, legacyTaskControlSibling, readFinalAnswerReceipt, readTaskControlJson,
  removeTaskControlFile, saveFinalAnswerReceipt, taskControlDirectory, taskControlReadDirectory, TASK_CONTROL_LAYOUT, withTaskControlReadDirectory,
  withTaskFinalAnswerLock, writeTaskControlJson, MAX_TASK_CONTROL_BYTES,
} from "../src/worker/terminal-answer.js";
import { markTerminalControlForwarded, readTerminalControl, settleTerminalControl, terminalControls, trackTerminalControl } from "../src/worker/terminal-controls.js";

// Authorized READ-ONLY pre-v2 path (pi-fabric#1150): an owned unmarked run with no central
// entry is read through the pinned no-follow run directory; every write, lock, publication
// and outbox route stays central-only and refuses. Local-eligible: no retention/deletion
// import. XDG_STATE_HOME is a unique, never-created path under the checkout (outside temp).
// Fixtures live under os.tmpdir() and only this suite's own roots are cleaned up.
const posix = process.platform !== "win32";
const roots: string[] = [];
let previousState: string | undefined, hadState = false, state = "";
beforeEach(() => {
  hadState = Object.prototype.hasOwnProperty.call(process.env, "XDG_STATE_HOME");
  previousState = process.env.XDG_STATE_HOME;
  state = path.join(process.cwd(), `.legacy-read-state-${process.pid}-${randomUUID()}`);
  expect(fs.existsSync(state)).toBe(false);
  process.env.XDG_STATE_HOME = state;
});
afterEach(() => {
  // Never created by this suite or by the refusing product helpers.
  expect(fs.existsSync(state)).toBe(false);
  if (hadState) process.env.XDG_STATE_HOME = previousState; else delete process.env.XDG_STATE_HOME;
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const CONTROL = "11111111-1111-4111-8111-111111111111";
const QUEUED = "22222222-2222-4222-8222-222222222222";
const sender = { id: "session:original", kind: "main", verified: "mesh" } as const;
const provenance = { v: 1, channel: "fabric", via: "steer", sender };
const scratch = (): string => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "task-control-legacy-read-"))); roots.push(root); return root;
};
const json = (file: string, value: unknown): void => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
};
/** Minimal pre-v2 run: no central, no sibling, no protected metadata, unmarked status. */
const seed = (options: { status?: Record<string, unknown> | null; id?: string } = {}) => {
  const root = scratch(), id = options.id ?? "run-legacy", run = path.join(root, id);
  fs.mkdirSync(run, { mode: 0o700 });
  const receipt = { version: 1, id: "final-receipt", runId: id, recordedAt: 1_000, text: "final answer" };
  const record = { id, runner: "pi", status: "completed", transport: "process", sessionId: "99999999", finalAnswerReceipt: { id: receipt.id, recordedAt: 1_000 } };
  if (options.status !== null) json(path.join(run, "status.json"), { ...record, ...options.status });
  json(path.join(run, "final-answer.json"), receipt);
  fs.writeFileSync(path.join(run, "steer.jsonl"), JSON.stringify({ id: CONTROL, type: "steer", provenance, message: "retained" }) + "\n", { mode: 0o600 });
  const control = { id: CONTROL, delivery: "steer", state: "delivered", provenance };
  json(path.join(run, "terminal-controls", `${CONTROL}.json`), control);
  return { root, run, id, receipt, record, control };
};
const controlFile = (run: string, id = CONTROL) => path.join(run, "terminal-controls", `${id}.json`);
const snapshot = (target: string): Record<string, string> => {
  const out: Record<string, string> = {};
  const walk = (current: string): void => {
    const stat = fs.lstatSync(current);
    const key = path.relative(target, current) || ".";
    if (stat.isDirectory()) { out[key] = `dir:${stat.mode & 0o777}`; for (const name of fs.readdirSync(current).sort()) walk(path.join(current, name)); }
    else if (stat.isSymbolicLink()) out[key] = `link:${fs.readlinkSync(current)}`;
    else if (stat.isFile()) out[key] = `file:${fs.readFileSync(current, "base64")}`;
    else out[key] = "special";
  };
  walk(target);
  return out;
};
const expectNoCentral = (run: string): void => {
  const central = taskControlDirectory(run);
  expect(fs.existsSync(central)).toBe(false);
  expect(fs.existsSync(`${central}.stage`)).toBe(false);
  expect(fs.existsSync(state)).toBe(false);
};
const silenceWarn = () => vi.spyOn(console, "warn").mockImplementation(() => undefined);

describe.skipIf(!posix)("unmarked pre-v2 run: authorized read-only legacy path", () => {
  it("selects the pinned run itself and reads admission, delivered control and final receipt", () => {
    const { run, id, receipt, control } = seed();
    expect(taskControlReadDirectory(run)).toBe(run);
    expect(withTaskControlReadDirectory(run, directory => fs.realpathSync(directory))).toBe(run);
    expect(readFinalAnswerReceipt(run, id)).toEqual(receipt);
    expect(readFinalAnswerReceipt(run, "other-run")).toBeUndefined();
    const admissions = readTerminalAdmissions(run);
    expect(admissions.truncated).toBe(false);
    const admission = admissions.byId.get(CONTROL);
    expect(admission).toEqual({ id: CONTROL, delivery: "steer", sender });
    expect(readTerminalControl(run, CONTROL)).toMatchObject({ id: CONTROL, delivery: "steer", state: "delivered" });
    expect(terminalControls(run).map(item => [item.id, item.state])).toEqual([[CONTROL, "delivered"]]);
    expect(terminalControlMatchesAdmission(readTaskControlJson(run, path.join("terminal-controls", `${CONTROL}.json`)), admission)).toBe(true);
    expect(readTaskControlJson(run, path.join("terminal-controls", `${CONTROL}.json`))).toEqual(control);
    expect(readTaskControlJson(run, "missing.json")).toBeUndefined();
    expectNoCentral(run);
  });

  it("reads minimal stopped status and queued rows without inventing consumption", () => {
    const { run } = seed({ status: { status: "stopped" } });
    json(controlFile(run, QUEUED), { id: QUEUED, delivery: "followUp", state: "queued", forwarded: false });
    expect(taskControlReadDirectory(run)).toBe(run);
    expect(readTerminalControl(run, QUEUED)?.state).toBe("queued");
    expect(terminalControls(run).map(item => item.state).sort()).toEqual(["delivered", "queued"]);
    expectNoCentral(run);
  });

  it("refuses every write, lock and publication helper without creating central, stage or state", () => {
    const { run, id } = seed();
    json(controlFile(run, QUEUED), { id: QUEUED, delivery: "steer", state: "queued", forwarded: false });
    const before = snapshot(run);
    const operation = vi.fn();
    expect(() => writeTaskControlJson(run, path.join("terminal-controls", `${QUEUED}.json`), { id: QUEUED })).toThrow(FabricTaskControlDirectoryError);
    expect(() => writeTaskControlJson(run, "fresh.json", {})).toThrow(/legacy fallback refused/);
    expect(() => removeTaskControlFile(run, path.join("terminal-controls", `${CONTROL}.json`))).toThrow(FabricTaskControlDirectoryError);
    expect(() => saveFinalAnswerReceipt(run, id, "replacement")).toThrow(FabricTaskControlDirectoryError);
    expect(() => withTaskFinalAnswerLock(run, operation)).toThrow(FabricTaskControlDirectoryError);
    expect(operation).not.toHaveBeenCalled();
    expect(() => initializeTaskControlDirectory(run)).toThrow(/refusing recreation/);
    expect(() => trackTerminalControl(run, { id: "33333333-3333-4333-8333-333333333333", delivery: "steer" })).toThrow(FabricTaskControlDirectoryError);
    expect(() => markTerminalControlForwarded(run, QUEUED)).toThrow(FabricTaskControlDirectoryError);
    expect(() => settleTerminalControl(run, QUEUED, "refused")).toThrow(FabricTaskControlDirectoryError);
    expect(() => appendTerminalAdmission(run, JSON.stringify({ id: "late", type: "steer" }) + "\n")).toThrow();
    expect(snapshot(run)).toEqual(before);
    expect(readTerminalControl(run, QUEUED)?.state).toBe("queued");
    expectNoCentral(run);
  });

  it("sender outbox never routes, retains or proves refusal from raw legacy evidence", async () => {
    const { run, id, receipt } = seed();
    json(controlFile(run, QUEUED), { id: QUEUED, delivery: "steer", state: "queued", forwarded: false, provenance });
    fs.appendFileSync(path.join(run, "steer.jsonl"), JSON.stringify({ id: QUEUED, type: "steer", provenance, message: "late" }) + "\n");
    const notice = { code: "FABRIC_TARGET_TERMINAL", targetId: id, finalAnswerReceiptId: receipt.id, messageId: QUEUED, delivery: "steer", sender } as const;
    json(path.join(run, "terminal-notices", `${QUEUED}.json`), notice);
    const before = snapshot(run);
    const admission = readTerminalAdmissions(run).byId.get(QUEUED)!;
    expect(admission).toBeDefined();
    expect(() => terminalControlRefusalProven(run, admission, id)).toThrow(FabricTaskControlDirectoryError);
    const deliver = vi.fn(), outbox = new TerminalNoticeOutbox(deliver);
    expect(() => outbox.retain(run, notice)).toThrow(FabricTaskControlDirectoryError);
    expect(() => outbox.recover(run)).not.toThrow();
    expect(() => outbox.route(run)).not.toThrow();
    await new Promise<void>(resolve => setImmediate(resolve)); // drain any (absent) route microtask chain
    expect(deliver).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(run, "lifecycle.jsonl"))).toBe(false);
    expect(fs.existsSync(path.join(run, "terminal-notices", `${QUEUED}.json.delivered`))).toBe(false);
    expect(snapshot(run)).toEqual(before);
    expectNoCentral(run);
  });
});

describe.skipIf(!posix)("marked, protected or aliased runs strictly refuse the legacy path", () => {
  it.each([TASK_CONTROL_LAYOUT, "sibling-v1", "unknown-layout", null])("taskControlLayout %j without central refuses", layout => {
    const { run, id } = seed({ status: { taskControlLayout: layout } });
    const before = snapshot(run);
    silenceWarn();
    expect(() => taskControlReadDirectory(run)).toThrow(/legacy fallback refused/);
    expect(() => readFinalAnswerReceipt(run, id)).toThrow(FabricTaskControlDirectoryError);
    expect(readTerminalControl(run, CONTROL)).toBeUndefined();
    expect(() => readTerminalAdmissions(run)).toThrow();
    expect(() => terminalControls(run)).toThrow(FabricTaskControlDirectoryError);
    expect(snapshot(run)).toEqual(before);
    expectNoCentral(run);
  });

  it.each(["binding", "snapshot", "sibling"] as const)("raw protected metadata or .ctl sibling (%s) refuses", kind => {
    const { run, id } = seed();
    if (kind === "binding") json(path.join(run, ".legacy-read.json"), { version: 2, runId: id, dev: "1", ino: "1", snapshot: null });
    if (kind === "snapshot") fs.mkdirSync(path.join(run, ".legacy-snapshot"), { mode: 0o700 });
    if (kind === "sibling") fs.mkdirSync(legacyTaskControlSibling(run), { mode: 0o700 });
    silenceWarn();
    expect(() => taskControlReadDirectory(run)).toThrow(/legacy fallback refused/);
    expect(readTerminalControl(run, CONTROL)).toBeUndefined();
    expect(() => readTerminalAdmissions(run)).toThrow();
    expectNoCentral(run);
  });

  it.each(["missing", "malformed", "array", "linked", "oversized", "group-writable-run"] as const)("unsafe status (%s) never selects the run", fault => {
    const { run } = seed();
    const status = path.join(run, "status.json");
    if (fault === "missing") fs.rmSync(status);
    if (fault === "malformed") fs.writeFileSync(status, "{not json");
    if (fault === "array") fs.writeFileSync(status, "[]");
    if (fault === "linked") { const real = path.join(path.dirname(run), "real-status.json"); fs.renameSync(status, real); fs.symlinkSync(real, status); }
    if (fault === "oversized") fs.writeFileSync(status, JSON.stringify({ status: "completed", pad: "x".repeat(1024 * 1024) }));
    if (fault === "group-writable-run") fs.chmodSync(run, 0o770);
    expect(() => taskControlReadDirectory(run)).toThrow(/legacy fallback refused/);
    expect(readTerminalControl(run, CONTROL)).toBeUndefined();
    expectNoCentral(run);
  });

  it("an aliased run path refuses before any read", () => {
    const { root, run } = seed();
    const alias = path.join(root, "alias");
    fs.symlinkSync(run, alias);
    expect(() => taskControlReadDirectory(alias)).toThrow(FabricTaskControlDirectoryError);
    expect(readTerminalControl(alias, CONTROL)).toBeUndefined();
    expectNoCentral(run);
  });
});

describe.skipIf(!posix)("unmarked run artifacts fail closed as the read APIs promise", () => {
  const replace = (file: string, fault: "symlink" | "hardlink" | "fifo"): void => {
    const real = file + ".real";
    fs.renameSync(file, real);
    if (fault === "symlink") fs.symlinkSync(real, file);
    if (fault === "hardlink") fs.linkSync(real, file);
    if (fault === "fifo") { fs.rmSync(real); expect(spawnSync("mkfifo", [file]).status).toBe(0); }
  };

  it.each(["symlink", "hardlink", "fifo"] as const)("%s final receipt never establishes finality", fault => {
    const { run, id } = seed();
    replace(path.join(run, "final-answer.json"), fault);
    expect(readFinalAnswerReceipt(run, id)).toBeUndefined();
  });

  it.each([
    ["malformed", "{"],
    ["wrong-version", JSON.stringify({ version: 2, id: "x", runId: "run-legacy", recordedAt: 1, text: "t" })],
  ] as const)("%s final receipt never establishes finality", (_name, bytes) => {
    const { run, id } = seed();
    fs.writeFileSync(path.join(run, "final-answer.json"), bytes);
    expect(readFinalAnswerReceipt(run, id)).toBeUndefined();
  });

  it.each(["symlink", "hardlink", "fifo"] as const)("%s control row reads as uncertain, never consumed", fault => {
    const { run } = seed();
    replace(controlFile(run), fault);
    expect(readTerminalControl(run, CONTROL)).toBeUndefined();
    // Enumeration skips non-regular rows and never reports them as delivered.
    expect(terminalControls(run).some(item => item.id === CONTROL)).toBe(false);
  });

  it("oversized or malformed control rows fail closed", () => {
    const { run } = seed();
    fs.writeFileSync(controlFile(run), JSON.stringify({ id: CONTROL, delivery: "steer", state: "delivered", pad: "x".repeat(MAX_TASK_CONTROL_BYTES) }));
    expect(readTerminalControl(run, CONTROL)).toBeUndefined();
    fs.writeFileSync(controlFile(run), "{");
    expect(readTerminalControl(run, CONTROL)).toBeUndefined();
    expect(() => terminalControls(run)).toThrow(SyntaxError);
    fs.writeFileSync(controlFile(run), JSON.stringify({ id: CONTROL, delivery: "steer", state: "consumed" }));
    expect(readTerminalControl(run, CONTROL)).toBeUndefined();
  });

  it("a linked terminal-controls directory is never followed", () => {
    const { root, run } = seed();
    const elsewhere = path.join(root, "elsewhere");
    fs.renameSync(path.join(run, "terminal-controls"), elsewhere);
    fs.symlinkSync(elsewhere, path.join(run, "terminal-controls"));
    expect(readTerminalControl(run, CONTROL)).toBeUndefined();
    expect(() => terminalControls(run)).toThrow();
  });

  it.each(["symlink", "hardlink", "fifo"] as const)("%s steering journal refuses admission reads", fault => {
    const { run } = seed();
    replace(path.join(run, "steer.jsonl"), fault);
    silenceWarn();
    expect(() => readTerminalAdmissions(run)).toThrow();
  });

  it("malformed steering rows grant nothing and an incomplete row truncates", () => {
    const { run } = seed();
    fs.writeFileSync(path.join(run, "steer.jsonl"), "{bad\n" + JSON.stringify({ id: CONTROL, type: "steer", provenance: { v: 99 } }) + "\n" +
      JSON.stringify({ id: QUEUED, type: "steer" }));
    const index = readTerminalAdmissions(run);
    expect(index.byId.size).toBe(0);
    expect(index.truncated).toBe(true);
  });
});
