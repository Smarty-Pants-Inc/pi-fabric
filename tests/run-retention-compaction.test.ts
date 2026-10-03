import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { compactTerminalRunEvents, pruneActorRunArchives, markRunRootActive, markRunRootClosed, sweepTempRunRoots, FABRIC_RUN_ROOT_PREFIX } from "../src/storage/retention.js";
import { ActorLogStore } from "../src/actors/log-store.js";

const roots: string[] = [];
const HOUR = 3600000;
const now = 10 * HOUR;
const log = Array.from({ length: 401 }, (_, sequence) => JSON.stringify({ sequence }) + "\n").join("");
const make = (record: Record<string, unknown> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-tail-")); roots.push(root);
  const run = path.join(root, "runs", "old"); fs.mkdirSync(run, { recursive: true });
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", finishedAt: 4 * HOUR, transport: "process", sessionId: "2147483647", ...record }));
  fs.writeFileSync(path.join(run, "events.jsonl"), log);
  fs.writeFileSync(path.join(run, "reply.json"), '{"text":"retained result"}');
  return { root, run };
};
const snapshot = (run: string) => fs.readdirSync(run).sort().map(name => {
  const file = path.join(run, name); const stat = fs.statSync(file);
  return [name, fs.readFileSync(file, "utf8"), stat.ino, stat.mtimeMs, stat.ctimeMs];
});
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("six-hour terminal run compaction", () => {
  it("atomically keeps the final 200 events at six hours even below the byte cap, with status/result and TTL unchanged", () => {
    const { root, run } = make();
    fs.utimesSync(run, 4 * HOUR / 1000, 4 * HOUR / 1000);
    const directoryMtime = fs.statSync(run).mtimeMs;
    const status = fs.readFileSync(path.join(run, "status.json"), "utf8");
    const inode = fs.statSync(path.join(run, "events.jsonl")).ino;
    expect(compactTerminalRunEvents(run, { now: now - 1 })).toBe(false);
    expect(compactTerminalRunEvents(run, { now })).toBe(true);
    const lines = fs.readFileSync(path.join(run, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(lines[0]).toMatchObject({ fabricTruncated: true });
    expect(lines.slice(1)).toEqual(Array.from({ length: 200 }, (_, i) => ({ sequence: i + 201 })));
    expect(fs.statSync(path.join(run, "events.jsonl")).ino).not.toBe(inode);
    expect(fs.statSync(run).mtimeMs).toBe(directoryMtime);
    expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(status);
    expect(fs.readFileSync(path.join(run, "reply.json"), "utf8")).toBe('{"text":"retained result"}');
    const before = snapshot(run);
    expect(compactTerminalRunEvents(run, { now })).toBe(false);
    expect(snapshot(run)).toEqual(before);
    expect(pruneActorRunArchives({ runsDirectory: path.join(root, "runs"), retentionMs: 7 * 24 * HOUR, now })).toEqual([]);
    expect(pruneActorRunArchives({ runsDirectory: path.join(root, "runs"), retentionMs: 7 * 24 * HOUR, now: 4 * HOUR + 7 * 24 * HOUR })).toEqual([run]);
  });

  it("also compacts a closed managed one-shot root before its unchanged deletion age", () => {
    const { root, run } = make();
    const managed = path.join(root, FABRIC_RUN_ROOT_PREFIX + "closed");
    fs.mkdirSync(managed);
    fs.renameSync(run, path.join(managed, "old"));
    markRunRootActive(managed, 4 * HOUR); markRunRootClosed(managed, 4 * HOUR, true);
    const sweep = (at: number) => sweepTempRunRoots({ tempRoot: root, now: at, orphanedTempRunRetentionMs: 24 * HOUR, oneShotRunRetentionMs: 24 * HOUR });
    expect(sweep(now).removedRuns).toEqual([]);
    expect(fs.readFileSync(path.join(managed, "old", "events.jsonl"), "utf8").trim().split("\n")).toHaveLength(201);
    expect(sweep(28 * HOUR).removedRuns).toEqual([path.join(managed, "old")]);
  });

  it.each([{ status: "running" }, { status: "queued" }, { status: "unknown" }, { sessionId: undefined }, { sessionId: String(process.pid) }, { cleanupPending: true }])("leaves nonterminal/unknown/uncertain custody untouched: %j", record => {
    const { run } = make(record); const before = snapshot(run);
    expect(compactTerminalRunEvents(run, { now })).toBe(false);
    expect(snapshot(run)).toEqual(before);
  });

  it("dry-run reports eligibility without changing bytes, inode or timestamps", () => {
    const { run } = make(); const before = snapshot(run);
    expect(compactTerminalRunEvents(run, { now, dryRun: true } as Parameters<typeof compactTerminalRunEvents>[1] & { dryRun: boolean })).toBe(true);
    expect(snapshot(run)).toEqual(before);
  });

  it("existing owned actor maintenance keeps the newest ordinary backup, including numeric suffixes, and preserves malformed orphans", () => {
    const { root } = make(); const sessionFile = path.join(root, "session.jsonl");
    const names = ["session.jsonl.20260927T150000000Z.bak", "session.jsonl.20260927T150000000Z-2.bak", "session.jsonl.20260927T150000000Z-10.bak", "session.jsonl.20260926T150000000Z.bak", "session.jsonl.20260925T150000000Z.id.orphan-noheader.bak"];
    for (const name of names) fs.writeFileSync(path.join(root, name), name);
    fs.writeFileSync(sessionFile, "live session");
    const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 7 * 24 * HOUR });
    store.pruneRuns({ sessionFile }, now);
    expect(fs.readdirSync(root).filter(name => name.endsWith(".bak")).sort()).toEqual([names[2], names[4]].sort());
    expect(fs.readFileSync(sessionFile, "utf8")).toBe("live session");
  });
});
