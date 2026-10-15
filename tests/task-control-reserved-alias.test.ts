import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  FabricTaskControlDirectoryError, createHostTaskRun, initializeTaskControlDirectory, readFinalAnswerReceipt,
  readTaskControlJson, removeTaskControlFile, saveFinalAnswerReceipt, taskControlDirectory,
  withLegacyTaskControlDirectories, writeTaskControlJson,
} from "../src/worker/terminal-answer.js";

// DELTA-SEC-03: reserved protected metadata names must be refused under every
// case-insensitive / trailing-dot / trailing-space alias (Windows and macOS name
// folding), as the first component of nested paths with either slash style, by BOTH
// public write and public remove. Refusal must happen before any filesystem mutation,
// so these real-filesystem tests call the refused helpers directly. Env-parity
// convention: the parent supplies an isolated XDG_STATE_HOME outside TMPDIR. Fixtures
// are retained; nothing is deleted.
const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
const usable = !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
const original = process.env.XDG_STATE_HOME;
afterEach(() => { process.env.XDG_STATE_HOME = original; });
const stateCase = () => { process.env.XDG_STATE_HOME = fs.mkdtempSync(path.join(isolatedState!, "reserved-alias-case-")); };
const freshRun = () => path.join(fs.mkdtempSync(path.join(os.tmpdir(), "task-control-reserved-alias-")), "run");
const tree = (target: string): unknown[] => {
  if (!fs.existsSync(target)) return [[target, "absent"]];
  const stat = fs.lstatSync(target, { bigint: true });
  if (!stat.isDirectory()) return [[target, String(stat.ino), Number(stat.mode), fs.readFileSync(target, "base64")]];
  return [[target, String(stat.ino), Number(stat.mode)], ...fs.readdirSync(target).sort().flatMap(name => tree(path.join(target, name)))];
};

const bindingAliases = [
  ".legacy-read.json", // exact (regression floor)
  ".LEGACY-READ.JSON", ".Legacy-Read.Json", ".legacy-READ.json",
  ".legacy-read.json.", ".legacy-read.json ", ".legacy-read.json..", ".legacy-read.json  ",
  ".legacy-read.json. ", ".legacy-read.json .", ".LEGACY-READ.JSON. .", ".Legacy-Read.JSON...  ",
];
const snapshotAliases = [
  ".legacy-snapshot", // exact (regression floor)
  ".LEGACY-SNAPSHOT", ".Legacy-Snapshot", ".legacy-SNAPshot",
  ".legacy-snapshot.", ".legacy-snapshot ", ".legacy-snapshot..", ".legacy-snapshot  ",
  ".legacy-snapshot. ", ".legacy-snapshot .", ".LEGACY-SNAPSHOT. .", ".Legacy-Snapshot...  ",
];
const nestedAliases = [...snapshotAliases, ...bindingAliases].flatMap(first => [
  `${first}/in-run/final-answer.json`, `${first}\\in-run\\final-answer.json`,
  `${first}/x.json`, `${first}\\x.json`,
]);
const reserved = [...bindingAliases, ...snapshotAliases, ...nestedAliases];

type Operation = { name: string; call: (run: string, relative: string) => void };
const operations: Operation[] = [
  { name: "writeTaskControlJson", call: (run, relative) => writeTaskControlJson(run, relative, { forged: true }) },
  { name: "removeTaskControlFile", call: (run, relative) => removeTaskControlFile(run, relative) },
];

/** Host-allocated run with valid current-only binding; returns its protected state. */
const hostRun = () => {
  stateCase();
  const run = freshRun();
  createHostTaskRun(run);
  const central = taskControlDirectory(run), bindingFile = path.join(central, ".legacy-read.json");
  const binding = { bytes: fs.readFileSync(bindingFile), ino: fs.lstatSync(bindingFile, { bigint: true }).ino };
  return { run, central, bindingFile, binding, listing: fs.readdirSync(central).sort(), before: tree(central) };
};
const expectUnchanged = (state: ReturnType<typeof hostRun>, context: string) => {
  expect(fs.readFileSync(state.bindingFile).equals(state.binding.bytes), `${context}: binding bytes`).toBe(true);
  expect(fs.lstatSync(state.bindingFile, { bigint: true }).ino, `${context}: binding inode`).toBe(state.binding.ino);
  expect(fs.readdirSync(state.central).sort(), `${context}: protected namespace listing`).toEqual(state.listing);
  expect(tree(state.central), `${context}: protected namespace tree`).toEqual(state.before);
};
const expectStillUsable = (run: string) => {
  initializeTaskControlDirectory(run); // validation only
  expect(withLegacyTaskControlDirectories(run, sources => sources)).toEqual([]);
  expect(saveFinalAnswerReceipt(run, "run", "done").text).toBe("done");
  expect(readFinalAnswerReceipt(run, "run")?.text).toBe("done");
};

describe.skipIf(!usable)("reserved protected metadata aliases (DELTA-SEC-03)", () => {
  for (const operation of operations) {
    it(`${operation.name} refuses every reserved alias before mutation; binding stays valid`, () => {
      const state = hostRun();
      for (const relative of reserved) {
        const context = `${operation.name}(${JSON.stringify(relative)})`;
        let thrown: unknown;
        try { operation.call(state.run, relative); } catch (error) { thrown = error; }
        expect(thrown, `${context}: must refuse`).toBeInstanceOf(FabricTaskControlDirectoryError);
        expectUnchanged(state, context);
      }
      expectStillUsable(state.run);
    });
  }

  it("write refusal does not depend on a pre-existing alias target (fresh run per alias kind)", () => {
    for (const relative of [".LEGACY-READ.JSON", ".legacy-read.json.", ".legacy-read.json ", ".LEGACY-SNAPSHOT/in-run/final-answer.json", ".legacy-snapshot. \\in-run\\final-answer.json"]) {
      const state = hostRun();
      expect(() => writeTaskControlJson(state.run, relative, { forged: true })).toThrow(FabricTaskControlDirectoryError);
      expect(() => removeTaskControlFile(state.run, relative)).toThrow(FabricTaskControlDirectoryError);
      expectUnchanged(state, relative);
      expectStillUsable(state.run);
    }
  });

  it("ordinary public control JSON is still writable/removable and the current binding stays usable", () => {
    const state = hostRun();
    writeTaskControlJson(state.run, "notes.json", { ok: 1 });
    writeTaskControlJson(state.run, "terminal-controls/a.json", { ok: 2 });
    writeTaskControlJson(state.run, "terminal-notices/b.json", { ok: 3 });
    writeTaskControlJson(state.run, "legacy-read.json", { ok: 4 }); // no leading dot: not reserved
    expect(readTaskControlJson(state.run, "notes.json")).toEqual({ ok: 1 });
    expect(readTaskControlJson(state.run, "terminal-controls/a.json")).toEqual({ ok: 2 });
    expect(readTaskControlJson(state.run, "terminal-notices/b.json")).toEqual({ ok: 3 });
    expect(readTaskControlJson(state.run, "legacy-read.json")).toEqual({ ok: 4 });
    removeTaskControlFile(state.run, "notes.json");
    expect(readTaskControlJson(state.run, "notes.json")).toBeUndefined();
    expect(fs.readFileSync(state.bindingFile).equals(state.binding.bytes)).toBe(true);
    expect(fs.lstatSync(state.bindingFile, { bigint: true }).ino).toBe(state.binding.ino);
    expect(fs.readdirSync(state.central)).not.toContain(".legacy-snapshot");
    expectStillUsable(state.run);
  });
});
