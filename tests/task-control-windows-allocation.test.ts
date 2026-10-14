import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { prepareRouteDispatch } from "../src/agents/model-route.js";
import { privateDirTestHooks } from "../src/storage/private-dir.js";
import { createHostTaskRun, ensureTaskControlRoot, readFinalAnswerReceipt, readTaskControlJson, routeControlFile, ROUTE_CONTROL_FILES, saveFinalAnswerReceipt, taskControlDirectory, taskControlRoot, writeTaskControlJson } from "../src/worker/terminal-answer.js";

// pi-fabric#1150 Astra P1: Windows cannot open a directory, so ordinary task control
// allocation, publication, reads and writes must use the checked no-follow path there.
// Windows is simulated by private-dir's existing platform seam plus a recorded fs.openSync
// that truthfully refuses every directory open (EISDIR). Central state lives outside temp
// (under the caller's isolated XDG_STATE_HOME, or a private temp sibling; never real user state
// or a shared temp root); the test deletion guard refuses rename/remove there, so those calls are RECORDED
// and emulated (copy-only, never a real central rename or deletion).
const inside = (root: string, value: string) => value === root || value.startsWith(root + path.sep);
const temp = fs.realpathSync(os.tmpdir());
const shared = ["/tmp", "/var/tmp", "/dev/shm", "/private/tmp"].flatMap(root => { try { return [fs.realpathSync(root)]; } catch { return []; } });
const configuredState = process.env.XDG_STATE_HOME;
const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
const tempRoots = [temp, ...(process.env.TMPDIR ? [path.resolve(process.env.TMPDIR)] : []), ...shared];
const usable = process.platform !== "win32" && !!configuredState && path.isAbsolute(configuredState) && !tempRoots.some(root => inside(root, path.resolve(configuredState)));
const state = usable ? path.join(configuredState!, `task-control-windows-state-${process.pid}-${Date.now()}`) : undefined;
beforeEach(() => { if (state) vi.stubEnv("XDG_STATE_HOME", state); });
afterEach(() => {
  Object.defineProperty(process, "platform", platformDescriptor);
  delete privateDirTestHooks.platform;
  vi.unstubAllEnvs(); vi.restoreAllMocks();
});

const simulateWindows = () => {
  privateDirTestHooks.platform = "win32";
  const realOpen = fs.openSync.bind(fs), realRename = fs.renameSync.bind(fs), realRemove = fs.rmSync.bind(fs), realUnlink = fs.unlinkSync.bind(fs);
  const refusedDirectoryOpens: string[] = [], renames: [string, string][] = [], removals: string[] = [], unlinks: string[] = [];
  const writtenSessionIdentities: Array<{ source: string; dev: string; ino: string }> = [];
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags?: fs.OpenMode, mode?: fs.Mode) => {
    if (fs.statSync(file, { throwIfNoEntry: false })?.isDirectory()) {
      refusedDirectoryOpens.push(String(file));
      throw Object.assign(new Error(`EISDIR: illegal operation on a directory, open '${String(file)}'`), { code: "EISDIR" });
    }
    return realOpen(file, flags ?? "r", mode);
  }) as typeof fs.openSync);
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
    if (inside(temp, String(from)) && inside(temp, String(to))) return realRename(from, to);
    renames.push([String(from), String(to)]);
    if (String(to).endsWith(`${path.sep}route${path.sep}route-session.jsonl`)) {
      const stat = fs.lstatSync(from, { bigint: true });
      writtenSessionIdentities.push({ source: String(from), dev: String(stat.dev), ino: String(stat.ino) });
    }
    // Copy-only emulation of the recorded rename: the source stays (no deletion outside temp).
    if (fs.lstatSync(from).isDirectory()) {
      fs.mkdirSync(to, { mode: 0o700 });
      for (const name of fs.readdirSync(from)) {
        if (fs.lstatSync(path.join(String(from), name)).isFile()) fs.writeFileSync(path.join(String(to), name), fs.readFileSync(path.join(String(from), name)), { mode: 0o600, flag: "wx" });
      }
    } else fs.writeFileSync(to, fs.readFileSync(from), { mode: 0o600, flag: "wx" });
  });
  vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
    if (inside(temp, String(target))) return realRemove(target, options);
    removals.push(String(target));
  });
  vi.spyOn(fs, "unlinkSync").mockImplementation(target => {
    if (inside(temp, String(target))) return realUnlink(target);
    unlinks.push(String(target));
  });
  return { refusedDirectoryOpens, renames, removals, unlinks, writtenSessionIdentities };
};
const freshRun = () => path.join(fs.realpathSync(fs.mkdtempSync(path.join(temp, "win-alloc-"))), "nested", "run-1");

describe.skipIf(!usable)("Windows task control allocation uses the checked path, not directory fds", () => {
  it("allocates, publishes central v2 controls, writes, reads and saves the final answer without opening a directory", () => {
    ensureTaskControlRoot();
    const run = freshRun(), seen = simulateWindows();
    createHostTaskRun(run);
    const central = taskControlDirectory(run);
    // Publication reached the central final save: exactly one recorded stage -> central rename.
    expect(seen.renames).toEqual([[`${central}.stage`, central]]);
    const binding = JSON.parse(fs.readFileSync(path.join(central, ".legacy-read.json"), "utf8"));
    const identity = fs.lstatSync(run, { bigint: true });
    expect(binding).toEqual({ version: 2, runId: "run-1", dev: String(identity.dev), ino: String(identity.ino), snapshot: null });

    writeTaskControlJson(run, path.join("terminal-controls", "c.json"), { ok: 1 });
    expect(readTaskControlJson(run, path.join("terminal-controls", "c.json"))).toEqual({ ok: 1 });
    expect(saveFinalAnswerReceipt(run, "run-1", "done").text).toBe("done");
    expect(readFinalAnswerReceipt(run, "run-1")?.text).toBe("done");
    expect(seen.renames.at(-1)?.[1]).toBe(path.join(central, "final-answer.json"));
    // Truthful refusal stayed armed and nothing in the product path relied on a directory fd.
    expect(seen.refusedDirectoryOpens).toEqual([]);
    expect(seen.removals.every(entry => inside(taskControlRoot(), entry))).toBe(true);
  });

  it("keeps exclusive allocation, non-directory and alias refusals on the checked path", () => {
    ensureTaskControlRoot();
    const run = freshRun();
    simulateWindows();
    createHostTaskRun(run);
    // A second exclusive allocation of the same run refuses (bytes preserved).
    expect(() => createHostTaskRun(run)).toThrow(/already exists/);
    // A non-directory control component refuses on the lstat-checked walk (the guard forbids
    // staging a link outside temp; lstat reports a link as a non-directory the same way).
    const central = taskControlDirectory(run);
    fs.writeFileSync(path.join(central, "terminal-notices"), "", { mode: 0o600 });
    expect(() => writeTaskControlJson(run, path.join("terminal-notices", "n.json"), {})).toThrow(/Unsafe task control subdirectory|ENOTDIR|ELOOP/);
    // A run aliased through a link is refused before any allocation.
    const alias = path.join(path.dirname(path.dirname(run)), "alias");
    fs.symlinkSync(path.dirname(run), alias);
    expect(() => createHostTaskRun(path.join(alias, "run-2"))).toThrow();
  });

  it("refuses a reparse alias that lstats as a directory", () => {
    ensureTaskControlRoot();
    const run = freshRun();
    simulateWindows();
    createHostTaskRun(run);
    const realRealpath = fs.realpathSync;
    // A junction/non-link reparse point: lstat says directory, realpath resolves elsewhere.
    const realpath = vi.spyOn(fs, "realpathSync").mockImplementation(((file: fs.PathLike) =>
      path.basename(String(file)) === "terminal-controls" ? path.join(temp, "elsewhere", "terminal-controls") : realRealpath(file)) as typeof fs.realpathSync);
    expect(() => writeTaskControlJson(run, path.join("terminal-controls", "c.json"), {})).toThrow(/must not traverse a link/);
    realpath.mockRestore();
  });

  it("refuses a parent swapped during allocation instead of recapturing it", () => {
    ensureTaskControlRoot();
    simulateWindows();
    // The run parent is replaced between its capture and the exclusive leaf mkdir.
    const swapped = freshRun(), parent = path.dirname(swapped), realMkdir = fs.mkdirSync, realRename = fs.renameSync;
    let swaps = 0;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
      if (String(target) === swapped && !swaps++) { realRename(parent, `${parent}-old`); realMkdir(parent, { mode: 0o700 }); }
      return realMkdir(target, options);
    }) as typeof fs.mkdirSync);
    expect(() => createHostTaskRun(swapped)).toThrow(/Run parent changed/);
    expect(swaps).toBe(1);
  });

  it("refuses a foreign central route/ without repairing it or writing any route file", () => {
    ensureTaskControlRoot();
    simulateWindows();
    const run = freshRun();
    createHostTaskRun(run);
    const route = path.dirname(routeControlFile(run, "route-session.jsonl"));
    fs.mkdirSync(route, { mode: 0o700 });
    const before = fs.lstatSync(route, { bigint: true });
    const original = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: fs.StatOptions) => {
      const stat = original(file, options as never);
      if (String(file) !== route || !stat) return stat;
      return Object.assign(Object.create(Object.getPrototypeOf(stat)), stat, { uid: options?.bigint ? BigInt(process.getuid!() + 1) : process.getuid!() + 1 });
    }) as typeof fs.lstatSync);
    expect(() => routeControlFile(run, "route-session.jsonl", { legacyReadOnly: true })).toThrow(/owned private/);
    const pin = { model: "test/fake", effort: "low" };
    const decision = { decisionId: "foreign-route", mode: "shadow", routeClass: "bounded-lookup", pin, candidates: [pin], shadowChoice: pin, ...pin };
    prepareRouteDispatch(decision as never, undefined, run, "child-id", { ledger: path.join(path.dirname(run), "ledger.jsonl"), decisionRecorded: true });
    expect(decision).toMatchObject({ reasonCode: "record-failed" });
    expect(fs.readdirSync(route)).toEqual([]);
    expect(original(route, { bigint: true }).ino).toBe(before.ino);
    expect(original(route, { bigint: true }).mode).toBe(before.mode);
    for (const name of ROUTE_CONTROL_FILES) expect(fs.existsSync(path.join(run, name))).toBe(false);
  });

  it("publishes all route controls centrally and records pending recovery unlink without touching the run", () => {
    ensureTaskControlRoot();
    const run = freshRun(), base = path.dirname(path.dirname(run));
    const seen = simulateWindows();
    // Routing's ledger durability branch uses the real platform, not the private-dir seam.
    Object.defineProperty(process, "platform", { ...platformDescriptor, value: "win32" });
    createHostTaskRun(run);
    const decisionPin = { model: "test/fake", effort: "low" };
    const decision = { decisionId: `d${Date.now()}-${Math.random()}`, mode: "shadow", routeClass: "bounded-lookup", parentSessionId: "parent", pin: decisionPin,
      candidates: [decisionPin], shadowChoice: decisionPin, confidence: null, probability: null, reasonCode: "shadow-choice", latencyMs: 0, ...decisionPin };
    const ledger = path.join(base, "ledger.jsonl");
    const dispatch = prepareRouteDispatch(decision as never, undefined, run, "child-id", { ledger, decisionRecorded: true });
    const binding = dispatch.bindSession(path.join(base, "worktree"));
    const receiptFile = routeControlFile(run, "route-dispatch-receipt.json");
    const sessionFile = routeControlFile(run, "route-session.jsonl");
    const pendingFile = routeControlFile(run, "pending-route-outcome.json");
    const central = taskControlDirectory(run);
    expect(ROUTE_CONTROL_FILES).toEqual(["route-dispatch-receipt.json", "pending-route-outcome.json", "route-session.jsonl"]);
    expect(receiptFile).toBe(path.join(central, "route", "route-dispatch-receipt.json"));
    expect(sessionFile).toBe(path.join(central, "route", "route-session.jsonl"));
    expect(pendingFile).toBe(path.join(central, "route", "pending-route-outcome.json"));
    expect(JSON.parse(fs.readFileSync(receiptFile, "utf8"))).toMatchObject({ runId: "child-id", decision: { decisionId: decision.decisionId } });
    expect(binding.path).toBe(sessionFile);
    const writtenIdentity = seen.writtenSessionIdentities[0]!;
    const sourceStat = fs.lstatSync(writtenIdentity.source, { bigint: true });
    expect(binding.identity).toEqual({ dev: writtenIdentity.dev, ino: writtenIdentity.ino });
    expect({ dev: String(sourceStat.dev), ino: String(sourceStat.ino) }).toEqual({ dev: writtenIdentity.dev, ino: writtenIdentity.ino });
    // Copy-only emulation cannot prove destination inode retention; hosted worker tests prove that.
    expect(fs.lstatSync(sessionFile, { bigint: true }).ino).not.toBe(sourceStat.ino);

    const open = vi.mocked(fs.openSync).getMockImplementation()!;
    vi.mocked(fs.openSync).mockImplementation(((file: fs.PathLike, flags?: fs.OpenMode, mode?: fs.Mode) => {
      if (String(file) === ledger) throw new Error("injected ledger append failure");
      return open(file, flags ?? "r", mode);
    }) as typeof fs.openSync);
    expect(() => dispatch.outcome({ status: "failed" })).toThrow("injected ledger append failure");
    vi.mocked(fs.openSync).mockImplementation(open);
    expect(fs.existsSync(pendingFile)).toBe(true);
    expect(JSON.parse(fs.readFileSync(pendingFile, "utf8"))).toMatchObject({ ledger, record: { type: "outcome", runId: "child-id", decisionId: decision.decisionId } });
    for (const name of ROUTE_CONTROL_FILES) expect(fs.existsSync(path.join(run, name))).toBe(false);

    dispatch.outcome({ status: "failed" });
    expect(fs.readFileSync(ledger, "utf8")).toContain('"runId":"child-id"');
    expect(seen.unlinks).toContain(pendingFile);
    // Unlink is recorded, never executed outside private temp.
    expect(fs.existsSync(pendingFile)).toBe(true);
    expect(seen.refusedDirectoryOpens).toEqual([]);
    expect(seen.removals.every(entry => inside(taskControlRoot(), entry))).toBe(true);
  });
});
