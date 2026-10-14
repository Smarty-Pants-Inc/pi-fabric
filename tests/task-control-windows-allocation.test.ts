import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager, managerRunTestHooks } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { privateDirTestHooks } from "../src/storage/private-dir.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { createHostTaskRun, ensureTaskControlRoot, readFinalAnswerReceipt, readTaskControlJson, saveFinalAnswerReceipt, taskControlDirectory, taskControlRoot, writeTaskControlJson } from "../src/worker/terminal-answer.js";

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
const stateParent = configuredState && path.isAbsolute(configuredState) ? configuredState : path.dirname(temp);
const state = path.join(stateParent, `task-control-windows-state-${process.pid}-${Date.now()}`);
const usable = process.platform !== "win32" && !inside(temp, state) && !shared.some(root => inside(root, state));
const managers: AgentManager[] = [];
beforeEach(() => { vi.stubEnv("XDG_STATE_HOME", state); });
afterEach(async () => {
  delete privateDirTestHooks.platform;
  delete managerRunTestHooks.platform;
  delete managerRunTestHooks.afterRunAllocation;
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs(); vi.restoreAllMocks();
});

const simulateWindows = () => {
  privateDirTestHooks.platform = "win32";
  const realOpen = fs.openSync.bind(fs), realRename = fs.renameSync.bind(fs), realRemove = fs.rmSync.bind(fs);
  const refusedDirectoryOpens: string[] = [], renames: [string, string][] = [], removals: string[] = [];
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
  return { refusedDirectoryOpens, renames, removals };
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

  it("routed manager dispatch still fails closed on Windows before route publication", async () => {
    ensureTaskControlRoot();
    const base = fs.realpathSync(fs.mkdtempSync(path.join(temp, "win-route-")));
    fs.mkdirSync(path.join(base, "granted"), { mode: 0o700 });
    simulateWindows();
    managerRunTestHooks.platform = "win32";
    const allocated: string[] = [];
    managerRunTestHooks.afterRunAllocation = directory => { allocated.push(directory); };
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(base, "granted", "nested"));
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [] },
      { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), preparePiModel: async model => model, fullCodeMode: false });
    managers.push(manager);
    const pin = { model: "test/fake", effort: "low" };
    const routeDecision = { decisionId: `d${Date.now()}`, mode: "shadow", routeClass: "bounded-lookup", parentSessionId: "parent", pin, candidates: [pin],
      shadowChoice: pin, confidence: null, probability: null, reasonCode: "shadow-choice", latencyMs: 0, ...pin };
    await expect(manager.run({ task: "must not launch", transport: "process", routeDecision,
      routeRecord: { ledger: path.join(base, "ledger.jsonl"), decisionRecorded: true } } as never))
      .rejects.toMatchObject({ code: "FABRIC_CONTROL_PUBLISH_UNSUPPORTED_PLATFORM" });
    expect(allocated).toEqual([]);
    expect(launch).not.toHaveBeenCalled();
    // No route control bytes anywhere in central state (ordinary allocation may precede the pin).
    const names = fs.readdirSync(taskControlRoot(), { recursive: true }).map(name => path.basename(String(name)));
    expect(names.filter(name => /route-dispatch-receipt|pending-route-outcome|route-session/.test(name))).toEqual([]);
  });
});
