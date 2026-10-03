import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as processIdentity from "../src/residency/process-identity.js";
import {
  canRemoveManagedRunRoot,
  canRemoveTerminalRun,
  FABRIC_RUN_ROOT_PREFIX,
  markRunRootActive,
  markRunRootClosed,
  hasUnresolvedWorker,
  runTreeExitVeto,
  markUnresolvedWorker,
  pruneActorRunArchives,
  compactTerminalRunEvents,
  RUN_ROOT_SWEEP_MARKER,
  sweepTempRunRoots,
} from "../src/storage/retention.js";

const roots: string[] = [];
const HOUR = 60 * 60 * 1_000;
const DAY = 24 * HOUR;

const temporaryDirectory = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-retention-test-"));
  roots.push(root);
  return root;
};

const writeStatus = (
  directory: string,
  record: Record<string, unknown>,
): void => {
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify(record));
};

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("shared run-tree exit veto", () => {
  it.each(["running", "queued", "unknown", undefined])("vetoes a nested nonterminal process record (%s) even with a dead saved PID", status => {
    const run = temporaryDirectory();
    writeStatus(run, { status: "completed" });
    writeStatus(path.join(run, "nested", "child"), { status, transport: "process", sessionId: "2147483647" });
    const probe = vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("gone"), { code: "ESRCH" }); });
    try {
      expect(runTreeExitVeto(run)).toMatch(/nonterminal process/);
      expect(canRemoveTerminalRun(run)).toBe(false);
    } finally { probe.mockRestore(); }
  });

  it.each([undefined, "", "not-a-pid", "0", "-1", "1.5", "9007199254740992", 123, null])("vetoes a nested terminal process record with an unusable saved identity (%s)", sessionId => {
    const run = temporaryDirectory();
    writeStatus(run, { status: "completed" });
    writeStatus(path.join(run, "nested", "child"), { status: "completed", transport: "process", sessionId });
    expect(runTreeExitVeto(run)).toMatch(/saved process identity is live or unknown/);
    expect(canRemoveTerminalRun(run)).toBe(false);
  });

  it.each(["EPERM", "EACCES", "unexpected"])("vetoes a nested terminal process record on an unknown liveness result (%s)", code => {
    const run = temporaryDirectory();
    writeStatus(run, { status: "completed" });
    writeStatus(path.join(run, "nested", "child"), { status: "completed", transport: "process", sessionId: "2147483647" });
    const probe = vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("unknown"), { code }); });
    try {
      expect(runTreeExitVeto(run)).toMatch(/saved process identity is live or unknown/);
      expect(canRemoveTerminalRun(run)).toBe(false);
    } finally { probe.mockRestore(); }
  });

  it.each(["completed", "failed", "stopped", "timed_out"])("permits a nested %s process record only after a usable saved PID is confirmed absent", status => {
    const run = temporaryDirectory();
    writeStatus(run, { status: "completed" });
    writeStatus(path.join(run, "nested", "child"), { status, transport: "process", sessionId: "2147483647" });
    const probe = vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("gone"), { code: "ESRCH" }); });
    try {
      expect(runTreeExitVeto(run)).toBeUndefined();
      expect(canRemoveTerminalRun(run)).toBe(true);
      expect(probe).toHaveBeenCalledWith(2147483647, 0);
    } finally { probe.mockRestore(); }
  });

  it.each(["123", "456", undefined])("checks a live saved PID against its birth identity (%s)", current => {
    const run = temporaryDirectory();
    writeStatus(run, { status: "completed" });
    writeStatus(path.join(run, "nested", "child"), { status: "completed", transport: "process", sessionId: "2147483647", processStartTime: "123" });
    const probe = vi.spyOn(process, "kill").mockReturnValue(true);
    const birth = vi.spyOn(processIdentity, "processStartTime").mockReturnValue(current);
    try {
      expect(!!runTreeExitVeto(run)).toBe(current !== "456");
      // Birth mismatch can clear explicit cleanup's exit veto, but automatic
      // retention must still preserve a run whose saved PID is live or unknown.
      expect(canRemoveTerminalRun(run)).toBe(false);
    } finally { probe.mockRestore(); birth.mockRestore(); }
  });

  it("vetoes a failed PID probe even if a birth probe would differ", () => {
    const run = temporaryDirectory();
    writeStatus(run, { status: "completed", transport: "process", sessionId: "2147483647", processStartTime: "123" });
    const probe = vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    const birth = vi.spyOn(processIdentity, "processStartTime").mockReturnValue("456");
    try {
      expect(runTreeExitVeto(run)).toMatch(/exit is unconfirmed/);
      expect(birth).not.toHaveBeenCalled();
    } finally { probe.mockRestore(); birth.mockRestore(); }
  });

  it("does not turn recordless pre-launch rollback into an admitted-worker obligation", () => {
    const run = temporaryDirectory();
    fs.writeFileSync(path.join(run, "task.txt"), "not launched");
    expect(runTreeExitVeto(run)).toBeUndefined();
    expect(canRemoveTerminalRun(run)).toBe(false);
    expect(runTreeExitVeto(path.join(run, "already-removed"))).toBeUndefined();
  });


  it("refuses malformed records and failed nested inspection rather than inferring exit", () => {
    const run = temporaryDirectory();
    const status = path.join(run, "status.json"); fs.writeFileSync(status, "{broken");
    expect(runTreeExitVeto(run)).toMatch(/exit is unconfirmed/);
    fs.writeFileSync(status, JSON.stringify({ status: "completed" }));
    const nested = path.join(run, "nested"); fs.mkdirSync(nested);
    const readdir = fs.readdirSync;
    const fault = vi.spyOn(fs, "readdirSync").mockImplementation((...args: Parameters<typeof readdir>) => {
      if (String(args[0]) === nested) throw Object.assign(new Error("denied"), { code: "EACCES" });
      return readdir(...args);
    });
    try { expect(runTreeExitVeto(run)).toMatch(/inspection failed/); }
    finally { fault.mockRestore(); }
    expect(runTreeExitVeto(run)).toBeUndefined();
  });

  it("ownership retention requires checked exit identities for every descendant, including terminal live writers", () => {
    const run = temporaryDirectory();
    const child = path.join(run, "nested", "child");
    const grandchild = path.join(child, "nested", "grandchild");
    const veto = () => runTreeExitVeto(run, 0, undefined, true);
    writeStatus(child, { status: "completed", transport: "process", sessionId: String(process.pid) });
    expect(veto()).toMatch(/exit is unconfirmed:.*descendant worker may still be running/);
    for (const sessionId of [undefined, "", "0", "-1", "not-a-pid", "9007199254740993"]) {
      writeStatus(child, { status: "completed", transport: "process", sessionId });
      expect(veto()).toMatch(/unknown descendant identity/);
    }
    writeStatus(child, { status: "running", transport: "process", sessionId: "2147483647" });
    // PID absence clears the ownership check, not the independent nonterminal veto.
    expect(veto()).toMatch(/nonterminal process/);
    writeStatus(child, { status: "completed", transport: "process", sessionId: "2147483647" });
    expect(veto()).toBeUndefined();
    writeStatus(grandchild, { status: "completed", transport: "process", sessionId: String(process.pid) });
    expect(veto()).toMatch(/descendant worker may still be running/);
    writeStatus(grandchild, { status: "completed", transport: "process", sessionId: "2147483647" });
    expect(veto()).toBeUndefined();
    fs.rmSync(path.join(grandchild, "status.json"));
    expect(veto()).toMatch(/unknown descendant identity/);
    expect(runTreeExitVeto(path.join(run, "gone"), 1, undefined, true)).toMatch(/inspection failed/);
    expect(runTreeExitVeto(path.join(run, "gone"), 0, undefined, true)).toMatch(/inspection failed/);
    expect(runTreeExitVeto(run, 0, () => true, true)).toMatch(/incomplete/);
  });

  it("retains tracked descendant ownership even when PID reuse passes the cleanup birth proof", () => {
    const run = temporaryDirectory();
    writeStatus(run, { status: "completed" });
    writeStatus(path.join(run, "nested", "child"), { status: "completed", transport: "process", sessionId: "2147483647", processStartTime: "123" });
    const probe = vi.spyOn(process, "kill").mockReturnValue(true);
    const birth = vi.spyOn(processIdentity, "processStartTime").mockReturnValue("456");
    try {
      expect(runTreeExitVeto(run)).toBeUndefined();
      expect(runTreeExitVeto(run, 0, undefined, true)).toMatch(/descendant worker may still be running/);
    } finally { probe.mockRestore(); birth.mockRestore(); }
  });

  it("refuses depth/deadline truncation and nested symlinks", () => {
    const run = temporaryDirectory();
    expect(runTreeExitVeto(run, 33)).toMatch(/incomplete/);
    expect(runTreeExitVeto(run, 0, () => true)).toMatch(/incomplete/);
    const target = temporaryDirectory(); fs.symlinkSync(target, path.join(run, "nested"), "junction");
    expect(runTreeExitVeto(run)).toMatch(/unsafe nested/);
    expect(fs.existsSync(target)).toBe(true);
  });
});
describe("safe run roots", () => {
  const sweep = (tempRoot: string, now = 100 * DAY) => sweepTempRunRoots({ tempRoot, now, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY });

  it.each(["closed", "orphan"])("R3 collects owned route sessions in expired %s roots without weakening fences", kind => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + kind);
    for (const name of ["done", "pending", "live", "unresolved"]) {
      const directory = path.join(root, name);
      writeStatus(directory, { status: "completed", finishedAt: 1, transport: "process", sessionId: name === "live" ? String(process.pid) : "2147483647" });
      fs.writeFileSync(path.join(directory, "route-session.jsonl"), '{"type":"session"}\n');
      if (name === "pending") fs.writeFileSync(path.join(directory, "pending-route-outcome.json"), "{}");
      if (name === "unresolved") markUnresolvedWorker(directory, "not joined");
    }
    fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, ...(kind === "closed" ? { closedAt: 1, childrenStopped: true } : { orphanedAt: 1 }) }));
    const result = sweep(tempRoot);
    if (kind === "closed") expect(result.removedRuns).toContain(path.join(root, "done"));
    expect(fs.existsSync(path.join(root, "done"))).toBe(false);
    for (const name of ["pending", "live", "unresolved"]) expect(fs.existsSync(path.join(root, name))).toBe(true);
  });
  it.each([
    ["closed", "root"], ["closed", "descendant"],
    ["orphan", "root"], ["orphan", "descendant"],
  ])("keeps a live %s-root %s writer even with a mismatched saved birth identity", (kind, location) => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + kind);
    const run = path.join(root, "run");
    writeStatus(run, { status: "completed", finishedAt: 1 });
    const writer = location === "root" ? run : path.join(run, "nested", "child");
    writeStatus(writer, { status: "completed", finishedAt: 1, transport: "process", sessionId: String(process.pid), processStartTime: "123" });
    fs.writeFileSync(path.join(run, "task.txt.provenance.json"), "{}");
    fs.mkdirSync(path.join(run, "deliveries"));
    markRunRootActive(root, 1);
    if (kind === "closed") markRunRootClosed(root, 1, true);
    else fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    const birth = vi.spyOn(processIdentity, "processStartTime").mockReturnValue("456");
    try {
      // Explicit cleanup's PID-reuse proof does not replace retention's
      // independent live-writer fence, for roots or nested descendants.
      expect(runTreeExitVeto(run)).toBeUndefined();
      expect(canRemoveTerminalRun(run)).toBe(false);
      if (kind === "closed") expect(canRemoveManagedRunRoot(root)).toBe(false);
      expect(pruneActorRunArchives({ runsDirectory: root, retentionMs: DAY, now: 100 * DAY })).toEqual([]);
      expect(sweep(tempRoot)).toEqual({ removedRuns: [], removedRoots: [] });
      expect(fs.existsSync(writer)).toBe(true);
    } finally { birth.mockRestore(); }
  });

  it("preserves malformed/unmarked ownership and unknown root contents", () => {
    const tempRoot = temporaryDirectory();
    for (const [suffix, owner] of [["bad", {}], ["pid", { pid: "gone", startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }], ["time", { pid: 2147483647, startedAt: 1, heartbeatAt: "old", orphanedAt: 1 }]] as const) {
      const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + suffix);
      fs.mkdirSync(root);
      fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify(owner));
    }
    const unknown = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "unknown");
    markRunRootActive(unknown, 1);
    fs.writeFileSync(path.join(unknown, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    fs.writeFileSync(path.join(unknown, "mine"), "do not delete");
    const unmarked = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "unmarked");
    fs.mkdirSync(unmarked);
    expect(sweep(tempRoot).removedRoots).toEqual([]);
    expect(fs.readdirSync(tempRoot)).toHaveLength(5);
  });

  it("rejects symlink roots and status markers without touching targets", () => {
    const tempRoot = temporaryDirectory();
    const target = temporaryDirectory();
    markRunRootActive(target, 1);
    const run = path.join(target, "run");
    writeStatus(run, { status: "completed", finishedAt: 1 });
    markRunRootClosed(target, 1);
    fs.symlinkSync(target, path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "link"), "junction");
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "status-link");
    markRunRootActive(root, 1);
    fs.mkdirSync(path.join(root, "run"));
    fs.symlinkSync(path.join(run, "status.json"), path.join(root, "run", "status.json"));
    markRunRootClosed(root, 1, true);
    expect(sweep(tempRoot).removedRuns).toEqual([]);
    expect(fs.existsSync(run)).toBe(true);
  });

  it("expires shutdown-confirmed incomplete runs, but never a live descendant", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed-incomplete");
    markRunRootActive(root, 1);
    const incomplete = path.join(root, "incomplete");
    fs.mkdirSync(incomplete);
    fs.writeFileSync(path.join(incomplete, "task.txt"), "incomplete launch");
    const active = path.join(root, "active");
    writeStatus(active, { status: "running", transport: "process", sessionId: String(process.pid) });
    fs.writeFileSync(path.join(active, "task.txt"), "still live");
    markRunRootClosed(root, 1, true);
    expect(sweep(tempRoot, 5 * HOUR).removedRuns).toEqual([]);
    expect(sweep(tempRoot, 6 * HOUR + 1).removedRuns).toEqual([incomplete]);
    expect(fs.existsSync(active)).toBe(true);
  });

  it("keeps unknown incomplete runs and live nested work under dead owners", () => {
    const tempRoot = temporaryDirectory();
    const root = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "dead-nested");
    markRunRootActive(root, 1);
    fs.writeFileSync(path.join(root, ".fabric-owner.json"), JSON.stringify({ pid: 2147483647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    const run = path.join(root, "outer");
    writeStatus(run, { status: "completed", finishedAt: 1 });
    const nested = path.join(run, "nested", "live");
    writeStatus(nested, { status: "running", transport: "process", sessionId: String(process.pid) });
    expect(sweep(tempRoot).removedRoots).toEqual([]);
    expect(fs.existsSync(nested)).toBe(true);
  });
});

describe("terminal run event log retention", () => {
  const log = Buffer.from(Array.from({ length: 12000 }, (_, sequence) =>
    JSON.stringify({ sequence, text: "🙂".repeat(24) }) + "\n").join(""));
  const make = (runs: string, id: string, record: Record<string, unknown>) => {
    const dir = path.join(runs, id);
    writeStatus(dir, record);
    fs.writeFileSync(path.join(dir, "events.jsonl"), log);
    fs.writeFileSync(path.join(dir, "reply.json"), '{"text":"keep this result"}');
    return dir;
  };

  it.each(["completed", "failed", "stopped", "timed_out"])("bounds an old %s archive to the default tail, keeps status/reply and never rewrites it twice", (status) => {
    const runsDirectory = path.join(temporaryDirectory(), "actors", "project", "a", "runs");
    const dir = make(runsDirectory, "old", { status, finishedAt: DAY });
    const statusBefore = fs.readFileSync(path.join(dir, "status.json"));
    const replyBefore = fs.readFileSync(path.join(dir, "reply.json"));
    const options = { runsDirectory, retentionMs: 7 * DAY, now: 3 * DAY };
    const rename = vi.spyOn(fs, "renameSync");
    const read = vi.spyOn(fs, "readSync");
    try {
      expect(pruneActorRunArchives(options)).toEqual([]);
      const compacted = fs.readFileSync(path.join(dir, "events.jsonl"));
      expect(compacted.length).toBeLessThanOrEqual(256 * 1024);
      expect(compacted.toString().trim().split("\n")).toHaveLength(201);
      const newline = compacted.indexOf(0x0a);
      expect(JSON.parse(compacted.subarray(0, newline).toString())).toMatchObject({ fabricTruncated: true });
      const tail = compacted.subarray(newline + 1);
      expect(tail.equals(log.subarray(log.length - tail.length))).toBe(true);
      const lines = tail.toString().trim().split("\n").map(line => JSON.parse(line));
      expect(lines.at(-1).sequence).toBe(11999);
      expect(lines).toHaveLength(200);
      expect(lines[0].sequence).toBe(11800);
      expect(fs.readFileSync(path.join(dir, "status.json"))).toEqual(statusBefore);
      expect(fs.readFileSync(path.join(dir, "reply.json"))).toEqual(replyBefore);
      expect(read.mock.calls.length).toBeGreaterThan(0);
      expect(read.mock.calls.every(call => call[1].byteLength <= 256 * 1024)).toBe(true);
      expect(rename).toHaveBeenCalledTimes(1);
      const stat = fs.statSync(path.join(dir, "events.jsonl"));
      expect(pruneActorRunArchives(options)).toEqual([]);
      expect(fs.statSync(path.join(dir, "events.jsonl")).ino).toBe(stat.ino);
      expect(fs.statSync(path.join(dir, "events.jsonl")).mtimeMs).toBe(stat.mtimeMs);
      expect(rename).toHaveBeenCalledTimes(1);
      expect(fs.readFileSync(path.join(dir, "events.jsonl")).equals(compacted)).toBe(true);
    } finally { rename.mockRestore(); read.mockRestore(); }
  });

  it("leaves large live, queued, unknown, young and lastRunId-retained archives byte-for-byte", () => {
    const runsDirectory = temporaryDirectory();
    const dirs = [
      make(runsDirectory, "live", { status: "running", finishedAt: DAY }),
      make(runsDirectory, "queued", { status: "queued", finishedAt: DAY }),
      make(runsDirectory, "unknown", { status: "unknown", finishedAt: DAY }),
      make(runsDirectory, "young", { status: "completed", finishedAt: 3 * DAY - HOUR }),
      make(runsDirectory, "latest", { status: "failed", finishedAt: DAY }),
    ];
    expect(pruneActorRunArchives({ runsDirectory, latestRunId: "latest", retentionMs: 7 * DAY, now: 3 * DAY })).toEqual([]);
    for (const dir of dirs) expect(fs.readFileSync(path.join(dir, "events.jsonl")).equals(log)).toBe(true);
  });

  it.each(["unresolved", "live pid", "external pane", "unknown contents", "nested live", "symlink", "hardlink"])("reuses the %s safety veto before compaction", (kind) => {
    const runs = temporaryDirectory();
    const dir = make(runs, "old", { status: "completed", finishedAt: DAY,
      ...(kind === "live pid" ? { transport: "process", sessionId: String(process.pid) } : {}),
      ...(kind === "external pane" ? { transport: "tmux" } : {}),
    });
    if (kind === "unresolved") markUnresolvedWorker(dir, "unknown exit");
    if (kind === "unknown contents") fs.writeFileSync(path.join(dir, "unknown.txt"), "unknown");
    if (kind === "nested live") writeStatus(path.join(dir, "nested", "child"), { status: "running" });
    const file = path.join(dir, "events.jsonl");
    if (kind === "symlink") { const target = path.join(runs, "target"); fs.renameSync(file, target); fs.symlinkSync(target, file); }
    if (kind === "hardlink") fs.linkSync(file, path.join(runs, "alias"));
    expect(compactTerminalRunEvents(dir, { now: 3 * DAY })).toBe(false);
    expect(fs.readFileSync(file).equals(log)).toBe(true);
  });

  it("rechecks descendant exit immediately before atomic replacement, not just before reading the tail", () => {
    const dir = make(temporaryDirectory(), "old", { status: "completed", finishedAt: DAY });
    const child = path.join(dir, "nested", "child");
    writeStatus(child, { status: "completed", transport: "process", sessionId: "2147483647" });
    const read = fs.readSync;
    const changed = vi.spyOn(fs, "readSync").mockImplementation((...args: Parameters<typeof read>) => {
      // The first safety walk succeeds; a legacy/unknown writer appears during the read.
      writeStatus(child, { status: "completed", transport: "process" });
      return read(...args);
    });
    try {
      expect(compactTerminalRunEvents(dir, { now: 3 * DAY })).toBe(false);
      expect(changed).toHaveBeenCalled();
      expect(fs.readFileSync(path.join(dir, "events.jsonl"))).toEqual(log);
    } finally { changed.mockRestore(); }
  });

  it("honors custom age/cap and a zero budget, and drops an oversized final line without corrupt JSON", () => {
    const dir = make(temporaryDirectory(), "old", { status: "completed", finishedAt: DAY });
    expect(compactTerminalRunEvents(dir, { now: 3 * DAY, terminalRunEventsAgeMs: 3 * DAY })).toBe(false);
    expect(compactTerminalRunEvents(dir, { now: 3 * DAY, expired: () => true })).toBe(false);
    expect(fs.readFileSync(path.join(dir, "events.jsonl")).equals(log)).toBe(true);
    expect(compactTerminalRunEvents(dir, { now: 3 * DAY, terminalRunEventsMaxBytes: 1024 })).toBe(true);
    expect(fs.statSync(path.join(dir, "events.jsonl")).size).toBeLessThanOrEqual(1024);
    fs.writeFileSync(path.join(dir, "events.jsonl"), JSON.stringify({ huge: "x".repeat(10000) }) + "\n");
    expect(compactTerminalRunEvents(dir, { now: 3 * DAY, terminalRunEventsMaxBytes: 1024 })).toBe(true);
    const lines = fs.readFileSync(path.join(dir, "events.jsonl"), "utf8").trim().split("\n");
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0]!)).toMatchObject({ fabricTruncated: true });
  });

  it("leaves the original log intact and removes its temporary file when atomic rename fails", () => {
    const dir = make(temporaryDirectory(), "old", { status: "completed", finishedAt: DAY });
    const rename = vi.spyOn(fs, "renameSync").mockImplementation(() => { throw Object.assign(new Error("disk full"), { code: "ENOSPC" }); });
    try {
      expect(compactTerminalRunEvents(dir, { now: 3 * DAY })).toBe(false);
      expect(fs.readFileSync(path.join(dir, "events.jsonl")).equals(log)).toBe(true);
      expect(fs.readdirSync(dir).sort()).toEqual(["events.jsonl", "reply.json", "status.json"]);
    } finally { rename.mockRestore(); }
  });
});

describe("temporal retention", () => {
  // review/astra on 3257dba, D1: a lost worker's run survives its manager, in both sweeps.
  it("never removes a run marked with an unresolved worker, after its owner is gone", () => {
    const tempRoot = temporaryDirectory();
    const orphaned = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "orphaned");
    const closed = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed");
    for (const runRoot of [orphaned, closed]) {
      const run = path.join(runRoot, "lost");
      writeStatus(run, { status: "failed", transport: "herdr", sessionId: "pane-1", finishedAt: 1, updatedAt: 1 });
      fs.writeFileSync(path.join(run, "task.txt"), "work");
      markUnresolvedWorker(run, "the Herdr server has been unreachable for 300 s");
    }
    fs.writeFileSync(path.join(orphaned, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    fs.writeFileSync(path.join(closed, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, closedAt: 1, childrenStopped: true }));
    const sweep = (at: string) => sweepTempRunRoots({ tempRoot: at, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY });
    const result = sweep(tempRoot);
    expect(result).toEqual({ removedRoots: [], removedRuns: [] });
    expect(fs.existsSync(path.join(orphaned, "lost"))).toBe(true);
    expect(fs.existsSync(path.join(closed, "lost"))).toBe(true);
    // Without the marker the same runs are swept.
    for (const runRoot of [orphaned, closed]) fs.rmSync(path.join(runRoot, "lost", "unresolved-worker.json"));
    const unmarked = sweep(tempRoot);
    expect(unmarked.removedRoots).toContain(orphaned);         // the closed root goes too once empty
    expect(unmarked.removedRuns).toEqual([path.join(closed, "lost")]);
  });

  it("never removes a completed parent run whose nested child is marked unresolved", () => {
    const tempRoot = temporaryDirectory();
    const orphaned = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "orphaned-parent");
    const closed = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed-parent");
    for (const runRoot of [orphaned, closed]) {
      const parent = path.join(runRoot, "parent");
      writeStatus(parent, { status: "completed", transport: "process", sessionId: "2147483647", finishedAt: 1, updatedAt: 1 });
      fs.writeFileSync(path.join(parent, "task.txt"), "work");
      const child = path.join(parent, "nested", "child");
      writeStatus(child, { status: "failed", transport: "herdr", sessionId: "pane-1", finishedAt: 1, updatedAt: 1 });
      fs.writeFileSync(path.join(child, "task.txt"), "work");
      markUnresolvedWorker(child, "the Herdr server has been unreachable for 300 s");
    }
    fs.writeFileSync(path.join(orphaned, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
    fs.writeFileSync(path.join(closed, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, closedAt: 1, childrenStopped: true }));
    const result = sweepTempRunRoots({ tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY });
    expect(result).toEqual({ removedRoots: [], removedRuns: [] });
    expect(hasUnresolvedWorker(path.join(orphaned, "parent"))).toBe(true);
  });

  it("removes dead temporary run roots after six hours", () => {
    const tempRoot = temporaryDirectory();
    const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "dead");
    fs.mkdirSync(runRoot);
    fs.writeFileSync(
      path.join(runRoot, ".fabric-owner.json"),
      JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1 }),
    );

    const detected = sweepTempRunRoots({
      tempRoot,
      orphanedTempRunRetentionMs: 6 * HOUR,
      oneShotRunRetentionMs: DAY,
      now: 2,
    });
    expect(detected.removedRoots).toEqual([]);

    const result = sweepTempRunRoots({
      tempRoot,
      orphanedTempRunRetentionMs: 6 * HOUR,
      oneShotRunRetentionMs: DAY,
      now: 6 * HOUR + 2,
    });

    expect(result.removedRoots).toEqual([runRoot]);
    expect(fs.existsSync(runRoot)).toBe(false);
  });

  it("sweeps a host at most once per interval, and again once the marker is stale (smarty-dev#2010)", () => {
    const tempRoot = temporaryDirectory();
    const deadRoot = (name: string): string => {
      const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + name);
      fs.mkdirSync(runRoot);
      fs.writeFileSync(
        path.join(runRoot, ".fabric-owner.json"),
        JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }),
      );
      return runRoot;
    };
    const sweepAt = (now: number) => sweepTempRunRoots({
      tempRoot, now, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY, minIntervalMs: HOUR,
    });
    const first = deadRoot("first");
    expect(sweepAt(DAY).removedRoots).toEqual([first]);
    const second = deadRoot("second");
    // Another process swept within the hour: this one skips the walk.
    expect(sweepAt(DAY + HOUR - 1).removedRoots).toEqual([]);
    expect(fs.existsSync(second)).toBe(true);
    expect(sweepAt(DAY + HOUR).removedRoots).toEqual([second]);
    // A marker from the future (clock skew, tampering) does not suppress collection.
    fs.writeFileSync(path.join(tempRoot, RUN_ROOT_SWEEP_MARKER), JSON.stringify({ sweptAt: 10 * DAY }));
    const third = deadRoot("third");
    expect(sweepAt(2 * DAY).removedRoots).toEqual([third]);
  });

  it("collects expired actor runs with worker reply and overflow files, still vetoed by unknown files (smarty-dev#2010)", () => {
    const tempRoot = temporaryDirectory();
    const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "actor");
    markRunRootActive(runRoot, 1);
    const actorRun = path.join(runRoot, "actor-run");
    writeStatus(actorRun, { status: "completed", actorId: "actor-1", finishedAt: DAY });
    for (const name of ["task.txt", "events.jsonl", "reply.json", "relaunches.jsonl", "oversized-event-prefix.txt", "oversized-event-prefix-2.txt"]) {
      fs.writeFileSync(path.join(actorRun, name), "x");
    }
    const unknown = path.join(runRoot, "unknown");
    writeStatus(unknown, { status: "completed", actorId: "actor-1", finishedAt: DAY });
    fs.writeFileSync(path.join(unknown, "reply.json.bak"), "not ours");
    markRunRootClosed(runRoot, DAY + 1);
    const result = sweepTempRunRoots({
      tempRoot, now: 2 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
    });
    expect(result.removedRuns).toEqual([actorRun]);
    expect(fs.existsSync(unknown)).toBe(true);
  });

  it("skips a closed root younger than the shortest retention, and stops at the budget", () => {
    const tempRoot = temporaryDirectory();
    const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "young");
    markRunRootActive(runRoot, DAY);
    const run = path.join(runRoot, "run");
    writeStatus(run, { status: "completed", finishedAt: DAY });
    markRunRootClosed(runRoot, DAY + 1);
    const read = vi.spyOn(fs, "readdirSync");
    const sweepAt = (now: number, budgetMs?: number) => sweepTempRunRoots({
      tempRoot, now, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
      ...(budgetMs !== undefined ? { budgetMs } : {}),
    });
    expect(sweepAt(DAY + 6 * HOUR - 1).removedRuns).toEqual([]);
    // Only the temp root and the empty-root check were listed: the young root's runs were not walked.
    expect(read.mock.calls.filter(([dir]) => String(dir) === runRoot).length).toBeLessThanOrEqual(1);
    read.mockRestore();
    // A spent budget visits nothing; an unbounded sweep collects the run once it is due.
    expect(sweepAt(3 * DAY, 0).removedRuns).toEqual([]);
    expect(fs.existsSync(run)).toBe(true);
    expect(sweepAt(3 * DAY).removedRuns).toEqual([run]);
  });

  describe("a deadline crossed mid-walk (smarty-dev#2010 review)", () => {
    const budgetSweep = (tempRoot: string, budgetMs?: number) => sweepTempRunRoots({
      tempRoot, now: 100 * DAY, orphanedTempRunRetentionMs: 6 * HOUR, oneShotRunRetentionMs: DAY,
      ...(budgetMs !== undefined ? { budgetMs } : {}),
    });
    const completedRun = (directory: string): void => {
      writeStatus(directory, { status: "completed", finishedAt: 1 });
      fs.writeFileSync(path.join(directory, "task.txt"), "work");
    };

    it("stops removing an expired orphan root's runs at the budget and keeps the root", () => {
      const tempRoot = temporaryDirectory();
      const orphan = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "orphan");
      for (let index = 0; index < 40; index++) completedRun(path.join(orphan, `run-${index}`));
      fs.writeFileSync(path.join(orphan, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, orphanedAt: 1 }));
      let removed = 0;
      const rm = fs.rmSync;
      vi.spyOn(fs, "rmSync").mockImplementation((...args: Parameters<typeof fs.rmSync>) => { removed++; return rm(...args); });
      // The clock passes the budget right after the fifth run is removed.
      vi.spyOn(performance, "now").mockImplementation(() => (removed >= 5 ? 10_000 : 0));
      expect(budgetSweep(tempRoot, 1_000)).toEqual({ removedRoots: [], removedRuns: [] });
      expect(removed).toBe(5);
      expect(fs.readdirSync(orphan).filter((name) => name.startsWith("run-"))).toHaveLength(35);
      vi.restoreAllMocks();
      expect(budgetSweep(tempRoot).removedRoots).toEqual([orphan]);
    });

    it("abandons a nested-run walk at the budget without removing the run", () => {
      const tempRoot = temporaryDirectory();
      const closed = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed");
      const parent = path.join(closed, "parent");
      let deepest = parent;
      completedRun(parent);
      for (let depth = 0; depth < 20; depth++) {
        deepest = path.join(deepest, "nested", `child-${depth}`);
        completedRun(deepest);
      }
      fs.writeFileSync(path.join(closed, ".fabric-owner.json"), JSON.stringify({ pid: 2_147_483_647, startedAt: 1, heartbeatAt: 1, closedAt: 1, childrenStopped: true }));
      let deepVisits = 0;
      const exists = fs.existsSync;
      vi.spyOn(fs, "existsSync").mockImplementation((file) => {
        // Native separators: count "nested" path components below the parent run.
        const relative = path.relative(parent, String(file));
        if (!relative.startsWith("..") && relative.split(path.sep).filter((part) => part === "nested").length >= 10) deepVisits++;
        return exists(file);
      });
      // The clock passes the budget once the walk reaches nesting depth 10.
      vi.spyOn(performance, "now").mockImplementation(() => (deepVisits > 0 ? 10_000 : 0));
      expect(budgetSweep(tempRoot, 1_000)).toEqual({ removedRoots: [], removedRuns: [] });
      // The walk stopped at the crossing: it never went deeper, and the run was kept.
      expect(deepVisits).toBe(1);
      expect(fs.existsSync(deepest)).toBe(true);
      vi.restoreAllMocks();
      expect(budgetSweep(tempRoot).removedRuns).toEqual([parent]);
    });
  });

  it("keeps live roots and the current root out of orphan cleanup", () => {
    const tempRoot = temporaryDirectory();
    const liveRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "live");
    markRunRootActive(liveRoot, 1);

    const result = sweepTempRunRoots({
      tempRoot,
      currentRoot: liveRoot,
      orphanedTempRunRetentionMs: 6 * HOUR,
      oneShotRunRetentionMs: DAY,
      now: 30 * DAY,
    });

    expect(result.removedRoots).toEqual([]);
    expect(fs.existsSync(liveRoot)).toBe(true);
  });

  it("expires terminal one-shot runs from gracefully retained roots after 24 hours", () => {
    const tempRoot = temporaryDirectory();
    const runRoot = path.join(tempRoot, FABRIC_RUN_ROOT_PREFIX + "closed");
    markRunRootActive(runRoot, 1);
    const expired = path.join(runRoot, "expired");
    const fresh = path.join(runRoot, "fresh");
    const actorTemp = path.join(runRoot, "actor-temp");
    writeStatus(expired, { status: "completed", finishedAt: DAY });
    writeStatus(fresh, { status: "completed", finishedAt: 2 * DAY });
    writeStatus(actorTemp, { status: "failed", actorId: "actor-1", finishedAt: DAY });
    markRunRootClosed(runRoot, 2 * DAY);

    const result = sweepTempRunRoots({
      tempRoot,
      orphanedTempRunRetentionMs: 6 * HOUR,
      oneShotRunRetentionMs: DAY,
      now: 2 * DAY + 1,
    });

    expect(result.removedRuns.sort()).toEqual([actorTemp, expired].sort());
    expect(fs.existsSync(expired)).toBe(false);
    expect(fs.existsSync(actorTemp)).toBe(false);
    expect(fs.existsSync(fresh)).toBe(true);
  });

  it("expires actor archives after seven days while preserving the latest run", () => {
    const root = temporaryDirectory();
    const runsDirectory = path.join(root, "runs");
    const expired = path.join(runsDirectory, "expired");
    const latest = path.join(runsDirectory, "latest");
    const fresh = path.join(runsDirectory, "fresh");
    writeStatus(expired, { status: "completed", finishedAt: DAY });
    writeStatus(latest, { status: "completed", finishedAt: DAY });
    writeStatus(fresh, { status: "completed", finishedAt: 8 * DAY });

    const removed = pruneActorRunArchives({
      runsDirectory,
      latestRunId: "latest",
      retentionMs: 7 * DAY,
      now: 8 * DAY + 1,
    });

    expect(removed).toEqual([expired]);
    expect(fs.existsSync(expired)).toBe(false);
    expect(fs.existsSync(latest)).toBe(true);
    expect(fs.existsSync(fresh)).toBe(true);
  });
});
