import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  FabricTaskControlDirectoryError, createArchiveTaskControlDirectory, createHostTaskRun,
  initializeTaskControlDirectory, legacyTaskControlSibling, readFinalAnswerReceipt, readTaskControlFile, removeTaskControlFile,
  saveFinalAnswerReceipt, taskControlDirectory, withLegacyTaskControlDirectories, withTaskFinalAnswerLock, writeTaskControlJson,
} from "../src/worker/terminal-answer.js";

// The parent supplies an isolated XDG_STATE_HOME outside TMPDIR. Fixtures are retained; nothing is deleted.
const isolatedState = process.env.XDG_STATE_HOME;
const insideTemp = (value: string) => [os.tmpdir(), process.env.TMPDIR].filter(Boolean)
  .some(temp => path.resolve(value) === path.resolve(temp!) || path.resolve(value).startsWith(path.resolve(temp!) + path.sep));
const usable = process.platform !== "win32" && !!isolatedState && path.isAbsolute(isolatedState) && !insideTemp(isolatedState);
const original = process.env.XDG_STATE_HOME;
afterEach(() => { process.env.XDG_STATE_HOME = original; });
const stateCase = () => { process.env.XDG_STATE_HOME = fs.mkdtempSync(path.join(isolatedState!, "snapshot-case-")); };
const freshRoot = () => fs.mkdtempSync(path.join(os.tmpdir(), "task-control-snapshot-"));
const receipt = (runId: string, text: string) => Buffer.from(JSON.stringify({ version: 1, id: `id-${text}`, runId, recordedAt: 1, text }));
const tree = (target: string): unknown[] => {
  if (!fs.existsSync(target)) return [[target, "absent"]];
  const stat = fs.lstatSync(target);
  if (!stat.isDirectory()) return [[target, stat.ino, stat.mode, fs.readFileSync(target, "base64")]];
  return [[target, stat.ino, stat.mode], ...fs.readdirSync(target).sort().flatMap(name => tree(path.join(target, name)))];
};
const openFds = (): number => process.platform === "linux" ? fs.readdirSync("/proc/self/fd").length : 0;
const unsafe = (operation: () => unknown) => expect(operation).toThrow(FabricTaskControlDirectoryError);

describe.skipIf(!usable)("protected v2 current-only authority (named host producer)", () => {
  it("exclusive host allocation publishes snapshot:null; reads/saves/finality use central only", () => {
    stateCase();
    const run = path.join(freshRoot(), "run");
    createHostTaskRun(run);
    fs.writeFileSync(path.join(run, "route-dispatch-receipt.json"), "{}"); // route receipt follows allocation
    expect(withLegacyTaskControlDirectories(run, sources => sources)).toEqual([]);
    const binding = JSON.parse(fs.readFileSync(path.join(taskControlDirectory(run), ".legacy-read.json"), "utf8"));
    expect(binding).toMatchObject({ version: 2, runId: "run", snapshot: null });
    expect(saveFinalAnswerReceipt(run, "run", "done").text).toBe("done");
    expect(readFinalAnswerReceipt(run, "run")?.text).toBe("done");
    unsafe(() => createHostTaskRun(run)); // existing run refused (EEXIST is never success)
    initializeTaskControlDirectory(run); // validation only
  });

  it("post-bootstrap raw forgery is ignored for finality/read/save", () => {
    stateCase();
    const run = path.join(freshRoot(), "run");
    createHostTaskRun(run);
    fs.writeFileSync(path.join(run, "final-answer.json"), receipt("run", "forged"));
    fs.mkdirSync(legacyTaskControlSibling(run), { mode: 0o700 });
    fs.writeFileSync(path.join(legacyTaskControlSibling(run), "final-answer.json"), receipt("run", "forged2"));
    expect(readFinalAnswerReceipt(run, "run")).toBeUndefined();
    expect(readTaskControlFile(run, "final-answer.json")).toBeUndefined();
    expect(saveFinalAnswerReceipt(run, "run", "genuine").text).toBe("genuine");
    expect(withTaskFinalAnswerLock(run, read => read("run"))?.text).toBe("genuine");
  });

  it("public initialize never mints; unknown raw/v1/unbound central refuses preserving bytes", () => {
    stateCase();
    const root = freshRoot();
    const blank = path.join(root, "blank"); fs.mkdirSync(blank, { mode: 0o700 });
    unsafe(() => initializeTaskControlDirectory(blank));
    unsafe(() => createHostTaskRun(blank)); // blank-looking but not exclusively allocated by us
    expect(fs.existsSync(taskControlDirectory(blank))).toBe(false);
    const raw = path.join(root, "raw"); fs.mkdirSync(raw, { mode: 0o700 });
    fs.writeFileSync(path.join(raw, "status.json"), JSON.stringify({ status: "completed" }));
    fs.writeFileSync(path.join(raw, "final-answer.json"), receipt("raw", "old"));
    const before = tree(root);
    unsafe(() => createHostTaskRun(raw));
    unsafe(() => initializeTaskControlDirectory(raw));
    unsafe(() => readFinalAnswerReceipt(raw, "raw"));
    expect(tree(root)).toEqual(before);
    // Unbound central / v1 binding refuse.
    const v1 = path.join(root, "v1"); fs.mkdirSync(v1, { mode: 0o700 });
    const central = taskControlDirectory(v1); fs.mkdirSync(central, { recursive: true, mode: 0o700 });
    unsafe(() => initializeTaskControlDirectory(v1));
    const stat = fs.lstatSync(v1, { bigint: true });
    fs.writeFileSync(path.join(central, ".legacy-read.json"), JSON.stringify({ version: 1, runId: "v1", dev: String(stat.dev), ino: String(stat.ino) }), { mode: 0o600 });
    const centralBefore = tree(central);
    unsafe(() => initializeTaskControlDirectory(v1));
    unsafe(() => saveFinalAnswerReceipt(v1, "v1", "x"));
    expect(tree(central)).toEqual(centralBefore);
  });

  it("reserves protected metadata names against public write/remove", () => {
    stateCase();
    const run = path.join(freshRoot(), "run");
    createHostTaskRun(run);
    unsafe(() => writeTaskControlJson(run, ".legacy-read.json", {}));
    unsafe(() => writeTaskControlJson(run, ".legacy-snapshot/in-run/final-answer.json", {}));
    unsafe(() => removeTaskControlFile(run, ".legacy-read.json"));
  });

  it("archive ctor writes its own null binding despite copied status", () => {
    stateCase();
    const archive = path.join(freshRoot(), "archive"); fs.mkdirSync(archive, { mode: 0o700 });
    fs.writeFileSync(path.join(archive, "status.json"), JSON.stringify({ status: "completed", taskControlLayout: "central-v1" }));
    createArchiveTaskControlDirectory(archive);
    expect(withLegacyTaskControlDirectories(archive, sources => sources)).toEqual([]);
  });

  it("immutable final name: a second save never replaces the first receipt", () => {
    stateCase();
    const run = path.join(freshRoot(), "run");
    createHostTaskRun(run);
    expect(saveFinalAnswerReceipt(run, "run", "first").text).toBe("first");
    expect(saveFinalAnswerReceipt(run, "run", "second").text).toBe("first");
    expect(readFinalAnswerReceipt(run, "run")?.text).toBe("first");
  });

  it("non-null snapshot binding and stray .legacy-snapshot refuse, bytes preserved", () => {
    stateCase();
    const run = path.join(freshRoot(), "run");
    createHostTaskRun(run);
    const central = taskControlDirectory(run), bindingFile = path.join(central, ".legacy-read.json");
    const binding = JSON.parse(fs.readFileSync(bindingFile, "utf8"));
    expect(Object.keys(binding).sort()).toEqual(["dev", "ino", "runId", "snapshot", "version"]);
    // Stray snapshot directory beside a valid null binding.
    fs.mkdirSync(path.join(central, ".legacy-snapshot", "in-run"), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(central, ".legacy-snapshot", "in-run", "final-answer.json"), receipt("run", "forged"));
    let before = tree(central);
    unsafe(() => withLegacyTaskControlDirectories(run, () => undefined));
    unsafe(() => readFinalAnswerReceipt(run, "run"));
    unsafe(() => saveFinalAnswerReceipt(run, "run", "x"));
    unsafe(() => initializeTaskControlDirectory(run));
    expect(tree(central)).toEqual(before);
    // Forged non-null snapshot binding (even well-formed v2 shape) refuses.
    const other = path.join(freshRoot(), "other");
    createHostTaskRun(other);
    const otherCentral = taskControlDirectory(other), otherFile = path.join(otherCentral, ".legacy-read.json");
    const forged = { ...JSON.parse(fs.readFileSync(otherFile, "utf8")), snapshot: { name: ".legacy-snapshot", dev: "1", ino: "1", layers: [] } };
    fs.chmodSync(otherFile, 0o600); fs.writeFileSync(otherFile, JSON.stringify(forged));
    before = tree(otherCentral);
    unsafe(() => withLegacyTaskControlDirectories(other, () => undefined));
    unsafe(() => readFinalAnswerReceipt(other, "other"));
    unsafe(() => writeTaskControlJson(other, "terminal-controls/a.json", {}));
    expect(tree(otherCentral)).toEqual(before);
    // Extra key refuses.
    const extra = path.join(freshRoot(), "extra");
    createHostTaskRun(extra);
    const extraFile = path.join(taskControlDirectory(extra), ".legacy-read.json");
    fs.chmodSync(extraFile, 0o600);
    fs.writeFileSync(extraFile, JSON.stringify({ ...JSON.parse(fs.readFileSync(extraFile, "utf8")), extra: 1 }));
    unsafe(() => initializeTaskControlDirectory(extra));
  });

  it("current-only producer: no legacy sources ever, sibling/in-run raw bytes never read", () => {
    stateCase();
    const run = path.join(freshRoot(), "run");
    createHostTaskRun(run);
    expect(fs.readdirSync(run)).toEqual([]);
    expect(fs.existsSync(legacyTaskControlSibling(run))).toBe(false);
    fs.writeFileSync(path.join(run, "steer.jsonl"), "{\"forged\":1}\n");
    expect(readTaskControlFile(run, "steer.jsonl")).toBeUndefined();
    expect(withLegacyTaskControlDirectories(run, sources => sources.length)).toBe(0);
  });

  it("stale stage is refused, preserved, never adopted", () => {
    stateCase();
    const run = path.join(freshRoot(), "run");
    const stage = `${taskControlDirectory(run)}.stage`;
    fs.mkdirSync(stage, { recursive: true, mode: 0o700 });
    unsafe(() => createHostTaskRun(run));
    expect(fs.existsSync(stage)).toBe(true);
    expect(fs.existsSync(taskControlDirectory(run))).toBe(false);
  });
});
