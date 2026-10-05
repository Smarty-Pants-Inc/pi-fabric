import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { pruneActorRunArchivesAsync } from "../src/storage/retention.js";
import { disposeRunTmpDirectory, JOINED_SCRATCH_FILE, NEVER_STARTED_FILE, RUN_TMP_DIRECTORY, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "retention-async-")); roots.push(root);
  const run = path.join(root, "run"); fs.mkdirSync(run);
  const status = { status: "completed", transport: "process", sessionId: "2147483646", finishedAt: 1 };
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(status));
  fs.writeFileSync(path.join(run, "task.txt"), "task");
  vi.spyOn(process, "platform", "get").mockReturnValue("win32");
  return { root, run, status, options: { runsDirectory: root, retentionMs: 0 } };
}
const deferred = () => { let resolve!: () => void; const promise = new Promise<void>(done => { resolve = done; }); return { promise, resolve }; };

describe("Windows asynchronous archive queue", () => {
  it("awaits all per-run fs crossings, including an 800ms native deletion, without blocking the 250ms heartbeat", async () => {
    const { root, run, options } = fixture();
    const sync = [vi.spyOn(fs, "lstatSync"), vi.spyOn(fs, "existsSync"), vi.spyOn(fs, "readFileSync"), vi.spyOn(fs, "readdirSync"), vi.spyOn(fs, "rmSync")];
    const stat = fs.promises.lstat, remove = fs.promises.rm;
    let scratchProbes = 0, activeDeletes = 0, largestActive = 0, beats = 0, longest = 0, previous = performance.now();
    vi.spyOn(fs.promises, "lstat").mockImplementation(async (...args) => {
      if ([RUN_TMP_DIRECTORY, UNRESOLVED_SCRATCH_FILE].includes(path.basename(String(args[0])))) scratchProbes++;
      return stat(...args);
    });
    vi.spyOn(fs.promises, "rm").mockImplementation(async (...args) => {
      activeDeletes++; largestActive = Math.max(largestActive, activeDeletes);
      try { await new Promise<void>(done => setTimeout(done, 800)); return await remove(...args); }
      finally { activeDeletes--; }
    });
    const heartbeat = setInterval(() => { const now = performance.now(); longest = Math.max(longest, now - previous); previous = now; beats++; }, 5);
    try {
      expect(await pruneActorRunArchivesAsync(options, () => true)).toEqual([run]);
      for (const spy of sync) expect(spy).not.toHaveBeenCalled();
      expect(scratchProbes).toBe(0); expect(largestActive).toBe(1); expect(activeDeletes).toBe(0);
      expect(beats).toBeGreaterThan(5); expect(longest).toBeLessThan(250);
      process.stdout.write(JSON.stringify({ probe: "async-800ms-Windows-delete", beats, longestSliceMs: longest, scratchProbes, largestActive }) + "\n");
    } finally { clearInterval(heartbeat); }
    vi.restoreAllMocks(); expect(fs.readdirSync(root)).toEqual([]);
  });

  it.each([RUN_TMP_DIRECTORY, UNRESOLVED_SCRATCH_FILE, JOINED_SCRATCH_FILE, NEVER_STARTED_FILE])("retains root and nested hostile scratch artifact %s", async name => {
    const { root, run, options, status } = fixture();
    for (const nested of [false, true]) {
      const holder = nested ? path.join(run, "nested", "child") : run;
      if (nested) { fs.mkdirSync(holder, { recursive: true }); fs.writeFileSync(path.join(holder, "status.json"), JSON.stringify(status)); }
      const artifact = path.join(holder, name);
      if (name === RUN_TMP_DIRECTORY) fs.mkdirSync(artifact); else fs.writeFileSync(artifact, "{}");
      expect(await pruneActorRunArchivesAsync(options, () => true)).toEqual([]);
      expect(fs.existsSync(artifact)).toBe(true); expect(fs.existsSync(run)).toBe(true);
      // Remove only this fixture artifact; no worker/scope was launched.
      fs.rmSync(artifact, { recursive: true });
    }
    expect(fs.existsSync(root)).toBe(true);
  });

  it.each(["authority", "latest", "status", "scratch"])("refreshes the %s fence after asynchronous inspection and before deletion", async fence => {
    const { run, options, status } = fixture();
    let owned = true, latest = false;
    const entered = deferred(), release = deferred(), stat = fs.promises.lstat;
    let rootStats = 0;
    vi.spyOn(fs.promises, "lstat").mockImplementation(async (...args) => {
      if (String(args[0]) === run && ++rootStats === 2) { entered.resolve(); await release.promise; }
      return stat(...args);
    });
    const queued = pruneActorRunArchivesAsync({ ...options, retainRun: () => latest }, () => owned);
    try {
      await entered.promise;
      if (fence === "authority") owned = false;
      if (fence === "latest") latest = true;
      if (fence === "status") fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ ...status, status: "running" }));
      if (fence === "scratch") fs.mkdirSync(path.join(run, RUN_TMP_DIRECTORY));
      release.resolve(); expect(await queued).toEqual([]); expect(fs.existsSync(run)).toBe(true);
    } finally { release.resolve(); await queued; }
  });

  it("fails closed on unreadable file and directory metadata", async () => {
    const { run, options } = fixture(), stat = fs.promises.lstat;
    const denied = vi.spyOn(fs.promises, "lstat").mockImplementation(async (...args) => {
      if (String(args[0]) === path.join(run, "task.txt")) throw Object.assign(new Error("denied"), { code: "EACCES" });
      return stat(...args);
    });
    expect(await pruneActorRunArchivesAsync(options, () => true)).toEqual([]); denied.mockRestore();
    const list = fs.promises.readdir;
    vi.spyOn(fs.promises, "readdir").mockImplementation(async (...args) => {
      if (String(args[0]) === run) throw Object.assign(new Error("denied"), { code: "EACCES" });
      return Reflect.apply(list, fs.promises, args);
    });
    expect(await pruneActorRunArchivesAsync(options, () => true)).toEqual([]); expect(fs.existsSync(run)).toBe(true);
  });

  it.each(["completed", "failed", "stopped", "timed_out"])("preserves bounded asynchronous compaction for recent %s archives", async status => {
    const { run, options, status: record } = fixture();
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ ...record, status }));
    fs.writeFileSync(path.join(run, "reply.json"), "unchanged reply");
    const file = path.join(run, "events.jsonl"), log = Array.from({ length: 12000 }, (_, sequence) => JSON.stringify({ sequence, text: "x".repeat(60) }) + "\n").join("");
    fs.writeFileSync(file, log);
    const statusBefore = fs.readFileSync(path.join(run, "status.json")), rootBefore = fs.statSync(run);
    const settings = { ...options, retentionMs: 7 * 86400000, now: 3 * 86400000 };
    const sync = [vi.spyOn(fs, "lstatSync"), vi.spyOn(fs, "readFileSync"), vi.spyOn(fs, "openSync"),
      vi.spyOn(fs, "readSync"), vi.spyOn(fs, "writeFileSync"), vi.spyOn(fs, "renameSync"), vi.spyOn(fs, "utimesSync")];
    expect(await pruneActorRunArchivesAsync(settings, () => true)).toEqual([]);
    for (const spy of sync) { expect(spy).not.toHaveBeenCalled(); spy.mockRestore(); }
    const compacted = fs.readFileSync(file), after = fs.statSync(file);
    expect(compacted.length).toBeLessThanOrEqual(256 * 1024);
    const lines = compacted.toString().trim().split("\n").map(line => JSON.parse(line));
    expect(lines).toHaveLength(201); expect(lines[0]).toMatchObject({ fabricTruncated: true });
    expect(lines[1].sequence).toBe(11800); expect(lines.at(-1).sequence).toBe(11999);
    expect(fs.readFileSync(path.join(run, "status.json"))).toEqual(statusBefore);
    expect(fs.readFileSync(path.join(run, "reply.json"), "utf8")).toBe("unchanged reply");
    expect(Math.abs(fs.statSync(run).mtimeMs - rootBefore.mtimeMs)).toBeLessThan(1);
    expect(await pruneActorRunArchivesAsync(settings, () => true)).toEqual([]);
    expect(fs.statSync(file).ino).toBe(after.ino); expect(fs.statSync(file).mtimeMs).toBe(after.mtimeMs);
  });

  it("rechecks descendant exit before async atomic compaction without disposing scratch", async () => {
    const { run, options, status } = fixture(), child = path.join(run, "nested", "child");
    fs.mkdirSync(child, { recursive: true }); fs.writeFileSync(path.join(child, "status.json"), JSON.stringify(status));
    const file = path.join(run, "events.jsonl"), log = ('{"text":"' + "x".repeat(100) + '"}\n').repeat(400);
    fs.writeFileSync(file, log);
    const open = fs.promises.open;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      if (String(args[0]) === file) fs.writeFileSync(path.join(child, "status.json"), JSON.stringify({ status: "completed", transport: "process" }));
      return open(...args);
    });
    expect(await pruneActorRunArchivesAsync({ ...options, retentionMs: 7 * 86400000, now: 3 * 86400000, terminalRunEventsMaxBytes: 1024 }, () => true)).toEqual([]);
    expect(fs.readFileSync(file, "utf8")).toBe(log); expect(fs.readdirSync(run).sort()).toEqual(["events.jsonl", "nested", "status.json", "task.txt"]);
  });
  it("keeps direct scratch disposal a no-op with zero fs crossings on win32", () => {
    const { run } = fixture();
    const sync = [vi.spyOn(fs, "lstatSync"), vi.spyOn(fs, "readFileSync"), vi.spyOn(fs, "mkdirSync"), vi.spyOn(fs, "rmSync")];
    expect(disposeRunTmpDirectory(run)).toBe(false);
    for (const spy of sync) expect(spy).not.toHaveBeenCalled();
  });
});
