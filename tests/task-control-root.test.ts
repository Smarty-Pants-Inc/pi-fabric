import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  TASK_CONTROL_LAYOUT, assertTaskControlWriteRoots, createArchiveTaskControlDirectory, ensureTaskControlRoot, initializeTaskControlDirectory,
  isTaskControlDirectoryEntry, legacyTaskControlSibling, readFinalAnswerReceipt, saveFinalAnswerReceipt, taskControlDirectory,
  taskControlReadDirectory, taskControlRoot,
} from "../src/worker/terminal-answer.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { resolveChildWritePolicy } from "../src/agents/child-env.js";

// The parent supplies an isolated XDG_STATE_HOME outside TMPDIR. Fixtures are
// retained; nothing is deleted. Without that isolation, never touch real state.
const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
const usable = process.platform !== "win32" && !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
const original = process.env.XDG_STATE_HOME;
afterEach(() => { process.env.XDG_STATE_HOME = original; });

const stateCase = () => fs.mkdtempSync(path.join(isolatedState!, "control-root-case-"));
const runFixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "task-control-root-"));
  const run = path.join(root, "run"), workspace = path.join(root, "workspace");
  fs.mkdirSync(run, { mode: 0o700 }); fs.mkdirSync(workspace, { mode: 0o700 });
  return { root, run, workspace };
};

describe.skipIf(!usable)("central task control root", () => {
  it("lives under the validated state home, outside temp, and refuses temp placement", () => {
    process.env.XDG_STATE_HOME = stateCase();
    expect(taskControlRoot()).toBe(path.join(process.env.XDG_STATE_HOME, "pi-fabric", "task-control"));
    expect(insideTemp(ensureTaskControlRoot())).toBe(false);
    expect(fs.statSync(taskControlRoot()).mode & 0o777).toBe(0o700);
    process.env.XDG_STATE_HOME = fs.mkdtempSync(path.join(os.tmpdir(), "state-in-temp-"));
    expect(() => taskControlRoot()).toThrow(/temporary directory/);
  });

  it("refuses a symlink alias and a group-writable state home", () => {
    const base = stateCase(), real = path.join(base, "real"), alias = path.join(base, "alias");
    fs.mkdirSync(real, { mode: 0o700 }); fs.symlinkSync(real, alias, "dir");
    process.env.XDG_STATE_HOME = alias;
    expect(() => taskControlRoot()).toThrow(/link alias/);
    const open = path.join(base, "open"); fs.mkdirSync(open, { mode: 0o700 }); fs.chmodSync(open, 0o770);
    process.env.XDG_STATE_HOME = open;
    expect(() => taskControlRoot()).toThrow(/writable by other users/);
    const loose = path.join(base, "loose"); fs.mkdirSync(path.join(loose, "pi-fabric"), { recursive: true, mode: 0o700 }); fs.chmodSync(path.join(loose, "pi-fabric"), 0o755);
    process.env.XDG_STATE_HOME = loose;
    expect(() => taskControlRoot()).toThrow(/owned private directory/);
  });

  it("derives a deterministic run-id plus path identity, so reused archive ids never collide", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const a = runFixture(), b = runFixture();
    expect(taskControlDirectory(a.run)).toBe(taskControlDirectory(a.run + path.sep));
    expect(path.dirname(taskControlDirectory(a.run))).toBe(taskControlRoot());
    expect(path.basename(taskControlDirectory(a.run))).toMatch(/^run--[0-9a-f]{32}$/);
    expect(taskControlDirectory(a.run)).not.toBe(taskControlDirectory(b.run));
    expect(taskControlDirectory(a.run).startsWith(a.root)).toBe(false);
  });

  it("refuses grants on the root, its rename-capable ancestors and parent controls (SEC01); admits normal run/workspace grants", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const s = runFixture(), parentRun = runFixture();
    initializeTaskControlDirectory(s.run); initializeTaskControlDirectory(parentRun.run);
    const grant = (...roots: string[]) => resolveChildWritePolicy({ writableRoots: roots }, undefined, s.workspace)!;
    expect(() => assertTaskControlWriteRoots(s.run, s.workspace, grant(s.run, s.workspace))).not.toThrow();
    for (const root of [process.env.XDG_STATE_HOME, taskControlRoot(), path.dirname(taskControlRoot()), taskControlDirectory(s.run), taskControlDirectory(parentRun.run)]) {
      expect(() => assertTaskControlWriteRoots(s.run, s.workspace, grant(s.run, s.workspace, root))).toThrow(/overlaps writable root/);
    }
    expect(() => assertTaskControlWriteRoots(s.run, s.workspace, grant(s.run, path.join(taskControlDirectory(parentRun.run), "x")))).toThrow(/overlaps writable root/);
  });

  it("never recreates missing controls of a central-v1 run, while archive creation admits a fresh destination", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const s = runFixture();
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "completed", taskControlLayout: TASK_CONTROL_LAYOUT });
    writeJsonAtomic(path.join(s.run, "final-answer.json"), { version: 1, id: "forged", runId: "run", recordedAt: 1, text: "untrusted" });
    expect(() => initializeTaskControlDirectory(s.run)).toThrow(/refusing recreation/);
    expect(() => taskControlReadDirectory(s.run)).toThrow(/legacy fallback refused/);
    expect(fs.existsSync(taskControlDirectory(s.run))).toBe(false);
    const archive = path.join(s.root, "archive"); fs.mkdirSync(path.join(archive, "run"), { recursive: true, mode: 0o700 });
    const dest = path.join(archive, "run");
    writeJsonAtomic(path.join(dest, "status.json"), { id: "run", status: "completed", taskControlLayout: TASK_CONTROL_LAYOUT });
    createArchiveTaskControlDirectory(dest); createArchiveTaskControlDirectory(dest); // create, then validate existing
    expect(fs.statSync(taskControlDirectory(dest)).mode & 0o777).toBe(0o700);
    expect(fs.existsSync(taskControlDirectory(s.run))).toBe(false);
  });

  it("keeps old sibling-v1 controls readable read-only and writes new controls only centrally", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const s = runFixture(), sibling = legacyTaskControlSibling(s.run);
    fs.mkdirSync(sibling, { mode: 0o700 });
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "running", taskControlLayout: "sibling-v1" });
    writeJsonAtomic(path.join(sibling, "final-answer.json"), { version: 1, id: "old", runId: "run", recordedAt: 1, text: "OLD_SIBLING" });
    expect(taskControlReadDirectory(s.run)).toBe(sibling);
    expect(readFinalAnswerReceipt(s.run, "run")?.text).toBe("OLD_SIBLING");
    expect(isTaskControlDirectoryEntry(s.root, "run.ctl")).toBe(true);
    initializeTaskControlDirectory(s.run);
    expect(taskControlReadDirectory(s.run)).toBe(taskControlDirectory(s.run));
    expect(readFinalAnswerReceipt(s.run, "run")?.text).toBe("OLD_SIBLING");
    expect(saveFinalAnswerReceipt(s.run, "run", "NEW").text).toBe("OLD_SIBLING"); // never replaces an old result
    expect(fs.existsSync(path.join(taskControlDirectory(s.run), "final-answer.json"))).toBe(false);
    expect(fs.readdirSync(s.root).sort()).toEqual(["run", "run.ctl", "workspace"]);
  });

  it("missing old sibling for a sibling-v1 run refuses fallback and recreation", () => {
    process.env.XDG_STATE_HOME = stateCase();
    const s = runFixture();
    writeJsonAtomic(path.join(s.run, "status.json"), { id: "run", status: "completed", taskControlLayout: "sibling-v1" });
    expect(() => taskControlReadDirectory(s.run)).toThrow();
    expect(() => initializeTaskControlDirectory(s.run)).toThrow();
    expect(fs.existsSync(taskControlDirectory(s.run))).toBe(false);
  });
});
