import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LEGACY_ARCHIVE_DEADLINE_MS, ResidentLegacyRunArchive } from "../src/residency/legacy-run-archive.js";

// B5 (#7978): no idle 250ms sweep interval. Events (root/runs entries and
// per-run status.json replacement) drive slices; one slow unref'd one-shot
// deadline covers age eligibility/re-enable. Archive is rename-only; no test
// here deletes product bytes.
const roots: string[] = [];
const temporary = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-archive-wakes-"));
  roots.push(root); fs.mkdirSync(path.join(root, "runs"), { mode: 0o700 });
  return root;
};
const old = () => Date.now() - 49 * 60 * 60 * 1000;
const tree = (dir: string, record: Record<string, unknown>) => {
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ status: "completed", finishedAt: old(), transport: "process", ...record }), { mode: 0o600 });
  fs.writeFileSync(path.join(dir, "events.jsonl"), "x\n", { mode: 0o600 });
};
const retired = (root: string): string[] => {
  const base = path.join(root, "runs-retired");
  if (!fs.existsSync(base)) return [];
  return fs.readdirSync(base).flatMap(day => fs.readdirSync(path.join(base, day))
    .flatMap(slice => fs.readdirSync(path.join(base, day, slice))));
};
/** Real I/O turns (setImmediate is never faked here), bounded. */
const settle = async (predicate: () => boolean, turns = 5000) => {
  for (let i = 0; i < turns && !predicate(); i++) await new Promise(resolve => setImmediate(resolve));
  expect(predicate()).toBe(true);
};
const fakeTimers = () => vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] });

afterEach(() => {
  vi.useRealTimers(); vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(process.platform === "win32")("legacy run archive wakes (B5)", () => {
  it("idle: no interval and no 250ms wake; one slow unref'd one-shot deadline", async () => {
    fakeTimers();
    const interval = vi.spyOn(globalThis, "setInterval"), timeout = vi.spyOn(globalThis, "setTimeout");
    const archive = new ResidentLegacyRunArchive(temporary(), {}, { isRetained: () => false });
    const sweep = vi.spyOn(archive, "sweep");
    try {
      archive.start();
      await settle(() => vi.getTimerCount() === 1);
      expect(interval).not.toHaveBeenCalled();
      expect(timeout.mock.calls.map(call => call[1])).toEqual([LEGACY_ARCHIVE_DEADLINE_MS]);
      expect(LEGACY_ARCHIVE_DEADLINE_MS).toBeGreaterThanOrEqual(60_000);
      expect((timeout.mock.results[0]!.value as NodeJS.Timeout).hasRef()).toBe(false);
      const calls = sweep.mock.calls.length;
      for (let i = 0; i < 40; i++) { vi.advanceTimersByTime(250); await new Promise(resolve => setImmediate(resolve)); }
      expect(sweep.mock.calls.length).toBe(calls);
    } finally { await archive.close(); }
    expect(vi.getTimerCount()).toBe(0);
  });

  it("a run moved into root/runs is archived by its watch event, without any timer firing", async () => {
    fakeTimers();
    const root = temporary();
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    try {
      archive.start();
      await settle(() => vi.getTimerCount() === 1);
      tree(path.join(root, "staging", "moved"), {});
      fs.renameSync(path.join(root, "staging", "moved"), path.join(root, "runs", "moved"));
      await settle(() => retired(root).includes("moved"));
      expect(archive.health.error).toBe("");
      expect(fs.existsSync(path.join(root, "runs", "moved"))).toBe(false);
    } finally { await archive.close(); }
  });

  it("a per-run status.json close replacement causes a sweep of that run", async () => {
    fakeTimers();
    const root = temporary();
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    try {
      archive.start();
      await settle(() => vi.getTimerCount() === 1);
      tree(path.join(root, "runs", "live"), { status: "running", finishedAt: undefined });
      await settle(() => archive.health.skipped >= 1);
      await settle(() => vi.getTimerCount() === 1);
      expect(retired(root)).toEqual([]);
      // Atomic close: replace status.json by rename (the writer's real shape).
      const staged = path.join(root, "status.next");
      fs.writeFileSync(staged, JSON.stringify({ status: "completed", finishedAt: old(), transport: "process" }), { mode: 0o600 });
      fs.renameSync(staged, path.join(root, "runs", "live", "status.json"));
      await settle(() => retired(root).includes("live"));
      expect(archive.health.error).toBe("");
    } finally { await archive.close(); }
  });

  const closeRun = (root: string, id: string) => {
    const staged = path.join(root, `status.${id}.next`);
    fs.writeFileSync(staged, JSON.stringify({ status: "completed", finishedAt: old(), transport: "process" }), { mode: 0o600 });
    fs.renameSync(staged, path.join(root, "runs", id, "status.json"));
  };
  const recordWatches = (root: string) => {
    const original = fs.watch.bind(fs);
    const byRun = new Map<string, fs.FSWatcher[]>(), closed = new Set<fs.FSWatcher>();
    const spy = vi.spyOn(fs, "watch").mockImplementation(((...args: Parameters<typeof fs.watch>) => {
      const watcher = original(...args), close = watcher.close.bind(watcher);
      watcher.close = () => { closed.add(watcher); close(); };
      const id = path.relative(path.join(root, "runs"), String(args[0]));
      if (id && !id.includes(path.sep)) byRun.set(id, [...byRun.get(id) ?? [], watcher]);
      return watcher;
    }) as typeof fs.watch);
    return { byRun, closed, spy };
  };

  it("A1: a run open before start() is status-watched by the initial scan; its atomic close wakes without the deadline", async () => {
    fakeTimers();
    const root = temporary();
    tree(path.join(root, "runs", "early"), { status: "running", finishedAt: undefined });
    const { byRun } = recordWatches(root);
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    try {
      archive.start();
      // Initial full pass finished: only the slow deadline is armed.
      await settle(() => vi.getTimerCount() === 1 && archive.health.checked === 1);
      expect(archive.health.skipped).toBe(1);
      expect(byRun.get("early")).toHaveLength(1);
      closeRun(root, "early");
      // No timer is advanced: the 60s deadline cannot be the carrier.
      await settle(() => retired(root).includes("early"));
      expect(archive.health.checked).toBe(2);
      expect(archive.health.error).toBe("");
    } finally { await archive.close(); }
  });

  it("A1 bound: >64 open runs at start keep at most 64 status watchers; the evicted run is recovered by the slow deadline", async () => {
    fakeTimers();
    const root = temporary();
    for (let i = 0; i < 65; i++) tree(path.join(root, "runs", `r${i}`), { status: "running", finishedAt: undefined });
    const { byRun, closed } = recordWatches(root);
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    try {
      archive.start();
      await settle(() => vi.getTimerCount() === 1 && archive.health.checked === 65, 50_000);
      expect(byRun.size).toBe(65);
      const evicted = [...byRun].filter(([, watchers]) => closed.has(watchers[0]!)).map(([id]) => id);
      expect(evicted).toHaveLength(1);
      closeRun(root, evicted[0]!);
      for (let i = 0; i < 300; i++) await new Promise(resolve => setImmediate(resolve));
      expect(retired(root)).toEqual([]);
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => retired(root).includes(evicted[0]!), 50_000);
      expect(archive.health.error).toBe("");
    } finally { await archive.close(); }
  });

  it("per-run watcher error re-arms once via the queued check; a repeat error does not spin", async () => {
    fakeTimers();
    const root = temporary();
    tree(path.join(root, "runs", "flaky"), { status: "running", finishedAt: undefined });
    const { byRun } = recordWatches(root);
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    const sweep = vi.spyOn(archive, "sweep");
    try {
      archive.start();
      await settle(() => vi.getTimerCount() === 1 && archive.health.checked === 1);
      byRun.get("flaky")![0]!.emit("error", new Error("watch failed"));
      await settle(() => byRun.get("flaky")!.length === 2 && vi.getTimerCount() === 1);
      expect(archive.health.checked).toBe(2);
      // Repeat error in the same pass: unwatched until the next full pass, no immediates.
      byRun.get("flaky")![1]!.emit("error", new Error("watch failed again"));
      const calls = sweep.mock.calls.length;
      for (let i = 0; i < 300; i++) await new Promise(resolve => setImmediate(resolve));
      expect(sweep.mock.calls.length).toBe(calls);
      expect(byRun.get("flaky")).toHaveLength(2);
      expect(vi.getTimerCount()).toBe(1);
      // The next full pass re-arms it, and the re-armed watcher carries the close.
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => byRun.get("flaky")!.length === 3 && vi.getTimerCount() === 1);
      closeRun(root, "flaky");
      await settle(() => retired(root).includes("flaky"));
    } finally { await archive.close(); }
  });

  it("the slow deadline covers changes with no event carrier (custody release)", async () => {
    fakeTimers();
    const root = temporary();
    tree(path.join(root, "runs", "held"), {});
    let held = true;
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => held });
    try {
      archive.start();
      await settle(() => vi.getTimerCount() === 1);
      held = false;
      for (let i = 0; i < 20; i++) { vi.advanceTimersByTime(250); await new Promise(resolve => setImmediate(resolve)); }
      expect(retired(root)).toEqual([]);
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => retired(root).includes("held"));
    } finally { await archive.close(); }
  });

  it("watcher error: one rescan plus bounded one-shot re-arm backoff, logged once, never an interval", async () => {
    fakeTimers();
    const root = temporary();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const interval = vi.spyOn(globalThis, "setInterval"), timeout = vi.spyOn(globalThis, "setTimeout");
    const watchers: fs.FSWatcher[] = [];
    const original = fs.watch.bind(fs);
    const watch = vi.spyOn(fs, "watch").mockImplementation(((...args: Parameters<typeof fs.watch>) => {
      const watcher = original(...args);
      if (args[0] === path.join(root, "runs")) watchers.push(watcher);
      return watcher;
    }) as typeof fs.watch);
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    const sweep = vi.spyOn(archive, "sweep");
    try {
      archive.start();
      await settle(() => vi.getTimerCount() === 1);
      expect(watchers).toHaveLength(1);
      const before = sweep.mock.calls.length;
      watchers[0]!.emit("error", Object.assign(new Error("ENOSPC: System limit for number of file watchers reached"), { code: "ENOSPC", syscall: "watch" }));
      await settle(() => sweep.mock.calls.length === before + 1);
      expect(warn).toHaveBeenCalledTimes(1);
      const message = String(warn.mock.calls[0]![0]);
      expect(message).toMatch(/inotify watch limit/);
      expect(message).toMatch(/rescanning once and re-arming with bounded backoff/);
      expect(message).not.toMatch(/poll|no events lost/i);
      expect(timeout.mock.calls.map(call => call[1])).toContain(1_000);
      vi.advanceTimersByTime(1_000);
      expect(watchers).toHaveLength(2);
      // Re-armed: an event on the new watcher still wakes the archive.
      tree(path.join(root, "staging", "after"), {});
      fs.renameSync(path.join(root, "staging", "after"), path.join(root, "runs", "after"));
      await settle(() => retired(root).includes("after"));
      // Second failure: no second log; backoff restarts at the base after a successful re-arm.
      watchers[1]!.emit("error", new Error("again"));
      expect(warn).toHaveBeenCalledTimes(1);
      expect(interval).not.toHaveBeenCalled();
      // Permanent failure: bounded attempts only, then silence.
      watch.mockImplementation(() => { throw Object.assign(new Error("EMFILE"), { code: "EMFILE" }); });
      for (let i = 0; i < 10; i++) vi.advanceTimersByTime(64_000);
      await settle(() => !sweep.mock.results.some(result => result.type === "throw"));
      const rearms = timeout.mock.calls.map(call => call[1]).filter(delay => delay !== LEGACY_ARCHIVE_DEADLINE_MS);
      expect(rearms.length).toBeLessThanOrEqual(7);
      expect(Math.max(...rearms.map(Number))).toBeLessThanOrEqual(32_000);
      expect(interval).not.toHaveBeenCalled();
    } finally { await archive.close(); }
    expect(vi.getTimerCount()).toBe(0);
  });

  it("close disposes timers and watchers; later events do nothing", async () => {
    fakeTimers();
    const root = temporary();
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    archive.start();
    await settle(() => vi.getTimerCount() === 1);
    await archive.close();
    expect(vi.getTimerCount()).toBe(0);
    tree(path.join(root, "staging", "late"), {});
    fs.renameSync(path.join(root, "staging", "late"), path.join(root, "runs", "late"));
    for (let i = 0; i < 200; i++) await new Promise(resolve => setImmediate(resolve));
    expect(retired(root)).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });
});
