import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LEGACY_ARCHIVE_DEADLINE_MS, ResidentLegacyRunArchive } from "../src/residency/legacy-run-archive.js";

// B5 (#7978): no idle 250ms sweep interval. Events (root/runs entries and
// per-run status.json replacement) drive slices. A healthy idle archive arms no
// timer; one unref'd one-shot is armed only for the earliest real due item (age
// eligibility, held custody, evicted/failed run watch) or the degraded/disabled
// slow fallback. Archive is rename-only; no test here deletes product bytes.
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
/** Counts completed sweeps (including their scheduling turn). */
const track = (archive: ResidentLegacyRunArchive) => {
  let done = 0;
  const original = archive.sweep.bind(archive);
  const spy = vi.spyOn(archive, "sweep").mockImplementation(async (...args) => { try { return await original(...args); } finally { done++; } });
  return { done: () => done, calls: () => spy.mock.calls.length };
};
const turns = async (n = 50) => { for (let i = 0; i < n; i++) await new Promise(resolve => setImmediate(resolve)); };
/** Advance fake time in steps, letting real I/O turns run between them. */
const advance = async (ms: number, steps = 10) => {
  const step = Math.floor(ms / steps);
  for (let i = 0; i < steps; i++) { vi.advanceTimersByTime(i === steps - 1 ? ms - step * (steps - 1) : step); await turns(20); }
};
const fakeTimers = () => vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] });

afterEach(() => {
  vi.useRealTimers(); vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe.skipIf(process.platform === "win32")("legacy run archive wakes (B5)", () => {
  it("healthy idle: no interval, no timer, and zero sweeps beyond the initial pass across many deadlines", async () => {
    fakeTimers();
    const interval = vi.spyOn(globalThis, "setInterval"), timeout = vi.spyOn(globalThis, "setTimeout");
    const root = temporary();
    // Modern, permanently vetoed and watched-open runs: none is a pending deadline.
    tree(path.join(root, "runs", "modern"), { sessionId: "123" });
    tree(path.join(root, "runs", "open"), { status: "running", finishedAt: undefined });
    tree(path.join(root, "runs", "mystery"), {});
    fs.writeFileSync(path.join(root, "runs", "mystery", "unknown.bin"), "x", { mode: 0o600 });
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && archive.health.checked === 3);
      await turns();
      expect(vi.getTimerCount()).toBe(0);
      expect(interval).not.toHaveBeenCalled();
      expect(timeout).not.toHaveBeenCalled();
      const calls = sweeps.calls();
      for (let i = 0; i < 40; i++) { vi.advanceTimersByTime(250); await new Promise(resolve => setImmediate(resolve)); }
      await advance(5 * LEGACY_ARCHIVE_DEADLINE_MS + 1, 25);
      expect(sweeps.calls()).toBe(calls);
      expect(archive.health.checked).toBe(3);
      expect(vi.getTimerCount()).toBe(0);
      expect(timeout).not.toHaveBeenCalled();
    } finally { await archive.close(); }
    expect(vi.getTimerCount()).toBe(0);
  });

  it("pending age eligibility arms one unref'd one-shot at the exact due time and fires exactly once", async () => {
    fakeTimers();
    const timeout = vi.spyOn(globalThis, "setTimeout");
    const root = temporary(), ageMs = 10 * LEGACY_ARCHIVE_DEADLINE_MS, finishedAt = Date.now() - 3 * LEGACY_ARCHIVE_DEADLINE_MS;
    tree(path.join(root, "runs", "young"), { finishedAt });
    const archive = new ResidentLegacyRunArchive(root, { legacyRunArchiveAgeMs: ageMs }, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && vi.getTimerCount() === 1);
      expect(archive.health.skipped).toBe(1);
      expect(timeout).toHaveBeenCalledTimes(1);
      const delay = Number(timeout.mock.calls[0]![1]), start = Date.now();
      expect(start + delay).toBe(finishedAt + ageMs);
      expect((timeout.mock.results[0]!.value as NodeJS.Timeout).hasRef()).toBe(false);
      const calls = sweeps.calls();
      // Several slow deadlines elapse before eligibility: no sweep, no re-arm.
      await advance(finishedAt + ageMs - start - 1, 7);
      expect(sweeps.calls()).toBe(calls);
      expect(timeout).toHaveBeenCalledTimes(1);
      expect(retired(root)).toEqual([]);
      vi.advanceTimersByTime(1);
      await settle(() => retired(root).includes("young"));
      expect(sweeps.calls()).toBe(calls + 1);
      expect(archive.health.error).toBe("");
      // The rename's own runs-watch event may queue one event-driven check.
      await turns(500);
      expect(vi.getTimerCount()).toBe(0);
      const after = sweeps.calls();
      expect(after).toBeLessThanOrEqual(calls + 2);
      await advance(3 * LEGACY_ARCHIVE_DEADLINE_MS);
      expect(sweeps.calls()).toBe(after);
    } finally { await archive.close(); }
  });

  it("held in-tree custody rechecks only that run at the slow cadence; unknown artifacts do not poll", async () => {
    fakeTimers();
    const root = temporary();
    tree(path.join(root, "runs", "unresolved"), {});
    fs.writeFileSync(path.join(root, "runs", "unresolved", "unresolved-worker.json"), "{}", { mode: 0o600 });
    tree(path.join(root, "runs", "mystery"), {});
    fs.writeFileSync(path.join(root, "runs", "mystery", "unknown.bin"), "x", { mode: 0o600 });
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && archive.health.checked === 2 && vi.getTimerCount() === 1);
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      // Targeted: only the held run is rechecked, not a full pass.
      await settle(() => archive.health.checked === 3 && vi.getTimerCount() === 1);
      fs.renameSync(path.join(root, "runs", "unresolved", "unresolved-worker.json"), path.join(root, "released.json"));
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => retired(root).includes("unresolved"));
      expect(archive.health.checked).toBe(4);
      await turns();
      expect(vi.getTimerCount()).toBe(0);
      expect(fs.existsSync(path.join(root, "runs", "mystery"))).toBe(true);
    } finally { await archive.close(); }
  });

  it("degraded (absent runs namespace) keeps the bounded slow full-pass fallback, then goes idle once watched", async () => {
    fakeTimers();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-archive-wakes-")); roots.push(root);
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && vi.getTimerCount() === 1);
      for (let i = 1; i <= 3; i++) {
        vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
        await settle(() => sweeps.done() >= 1 + i && vi.getTimerCount() === 1);
      }
      expect(sweeps.calls()).toBe(4);
      fs.mkdirSync(path.join(root, "runs"), { mode: 0o700 });
      tree(path.join(root, "runs", "late"), {});
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => retired(root).includes("late"));
      await turns(500);
      expect(vi.getTimerCount()).toBe(0);
      const calls = sweeps.calls();
      await advance(3 * LEGACY_ARCHIVE_DEADLINE_MS);
      expect(sweeps.calls()).toBe(calls);
    } finally { await archive.close(); }
  });

  it("disabled policy: empty namespace arms nothing; a held run keeps the slow re-enable check", async () => {
    fakeTimers();
    const root = temporary();
    const policy = { legacyRunArchiveEnabled: false };
    const archive = new ResidentLegacyRunArchive(root, policy, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1);
      await turns();
      expect(vi.getTimerCount()).toBe(0);
      const idle = sweeps.calls();
      await advance(3 * LEGACY_ARCHIVE_DEADLINE_MS);
      expect(sweeps.calls()).toBe(idle);
      // A run arriving while disabled is a policy-held run: its event arms the slow check.
      tree(path.join(root, "staging", "kept"), {});
      fs.renameSync(path.join(root, "staging", "kept"), path.join(root, "runs", "kept"));
      await settle(() => sweeps.done() > idle && vi.getTimerCount() === 1);
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => sweeps.done() >= idle + 2 && vi.getTimerCount() === 1);
      expect(retired(root)).toEqual([]);
      policy.legacyRunArchiveEnabled = true;
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => retired(root).includes("kept"));
      await turns();
      expect(vi.getTimerCount()).toBe(0);
    } finally { await archive.close(); }
  });

  it("a run moved into root/runs is archived by its watch event, without any timer firing", async () => {
    fakeTimers();
    const root = temporary();
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1);
      await turns();
      expect(vi.getTimerCount()).toBe(0);
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
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1);
      tree(path.join(root, "runs", "live"), { status: "running", finishedAt: undefined });
      await settle(() => archive.health.skipped >= 1);
      await turns();
      // Watched open run: no deadline.
      expect(vi.getTimerCount()).toBe(0);
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
    const sweeps = track(archive);
    try {
      archive.start();
      // Initial full pass finished: the watched open run arms no deadline.
      await settle(() => sweeps.done() >= 1 && archive.health.checked === 1);
      await turns();
      expect(vi.getTimerCount()).toBe(0);
      expect(archive.health.skipped).toBe(1);
      expect(byRun.get("early")).toHaveLength(1);
      closeRun(root, "early");
      // No timer exists: a deadline cannot be the carrier.
      await settle(() => retired(root).includes("early"));
      expect(archive.health.checked).toBe(2);
      expect(archive.health.error).toBe("");
    } finally { await archive.close(); }
  });

  it("A1 bound: >64 open runs at start keep at most 64 status watchers; the evicted run is a known slow due item", async () => {
    fakeTimers();
    const root = temporary();
    for (let i = 0; i < 65; i++) tree(path.join(root, "runs", `r${i}`), { status: "running", finishedAt: undefined });
    const { byRun, closed } = recordWatches(root);
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    try {
      archive.start();
      // Exactly one due item (the evicted run) arms the only timer.
      await settle(() => vi.getTimerCount() === 1 && archive.health.checked === 65, 50_000);
      expect(byRun.size).toBe(65);
      const evicted = [...byRun].filter(([, watchers]) => closed.has(watchers[0]!)).map(([id]) => id);
      expect(evicted).toHaveLength(1);
      closeRun(root, evicted[0]!);
      for (let i = 0; i < 300; i++) await new Promise(resolve => setImmediate(resolve));
      expect(retired(root)).toEqual([]);
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => retired(root).includes(evicted[0]!), 50_000);
      // A targeted recheck, not a full pass over the other 64 runs.
      expect(archive.health.checked).toBe(66);
      expect(archive.health.error).toBe("");
    } finally { await archive.close(); }
  });

  it("per-run watcher error re-arms once via the queued check; a repeat error does not spin", async () => {
    fakeTimers();
    const root = temporary();
    tree(path.join(root, "runs", "flaky"), { status: "running", finishedAt: undefined });
    const { byRun } = recordWatches(root);
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && archive.health.checked === 1);
      await turns();
      expect(vi.getTimerCount()).toBe(0);
      byRun.get("flaky")![0]!.emit("error", new Error("watch failed"));
      await settle(() => byRun.get("flaky")!.length === 2 && sweeps.done() >= 2 && archive.health.checked === 2);
      await turns();
      expect(vi.getTimerCount()).toBe(0);
      // Repeat error: unwatched until one slow targeted recheck, no immediates.
      byRun.get("flaky")![1]!.emit("error", new Error("watch failed again"));
      const calls = sweeps.calls();
      for (let i = 0; i < 300; i++) await new Promise(resolve => setImmediate(resolve));
      expect(sweeps.calls()).toBe(calls);
      expect(byRun.get("flaky")).toHaveLength(2);
      expect(vi.getTimerCount()).toBe(1);
      // The due recheck re-arms it, and the re-armed watcher carries the close.
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => byRun.get("flaky")!.length === 3 && sweeps.done() >= 3 && archive.health.checked === 3);
      await turns();
      expect(vi.getTimerCount()).toBe(0);
      closeRun(root, "flaky");
      await settle(() => retired(root).includes("flaky"));
    } finally { await archive.close(); }
  });

  it("held path custody is a slow due item for that run only; release archives it, then idle", async () => {
    fakeTimers();
    const root = temporary();
    tree(path.join(root, "runs", "held"), {});
    let held = true;
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: id => held && id === "held" });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && vi.getTimerCount() === 1);
      // Still held at the deadline: re-armed, still the only timer.
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => sweeps.done() >= 2 && vi.getTimerCount() === 1);
      held = false;
      for (let i = 0; i < 20; i++) { vi.advanceTimersByTime(250); await new Promise(resolve => setImmediate(resolve)); }
      expect(retired(root)).toEqual([]);
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => retired(root).includes("held"));
      await turns();
      expect(vi.getTimerCount()).toBe(0);
    } finally { await archive.close(); }
  });

  it("a fence veto (pending request custody) holds eligible candidates at the slow cadence", async () => {
    fakeTimers();
    const root = temporary();
    tree(path.join(root, "runs", "fenced"), {});
    fs.mkdirSync(path.join(root, "requests"), { mode: 0o700 });
    fs.writeFileSync(path.join(root, "requests", "pending.json"), "{}", { mode: 0o600 });
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && vi.getTimerCount() === 1);
      expect(retired(root)).toEqual([]);
      fs.renameSync(path.join(root, "requests", "pending.json"), path.join(root, "handled.json"));
      vi.advanceTimersByTime(LEGACY_ARCHIVE_DEADLINE_MS);
      await settle(() => retired(root).includes("fenced"));
      await turns();
      expect(vi.getTimerCount()).toBe(0);
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
      await settle(() => sweep.mock.calls.length >= 1);
      await sweep.mock.results[0]!.value;
      await turns();
      expect(vi.getTimerCount()).toBe(0);
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

  // Round 5 P2: an external owner removing a known due run cancels its due
  // obligation. Removal is a rename OUT of root/runs (no product delete).
  const removeOutside = (root: string, id: string) => {
    fs.mkdirSync(path.join(root, "elsewhere"), { recursive: true, mode: 0o700 });
    fs.renameSync(path.join(root, "runs", id), path.join(root, "elsewhere", id));
  };

  it("a young run removed before its due time cancels the timer via the watch event; no sweep at the old due", async () => {
    fakeTimers();
    const root = temporary(), ageMs = 10 * LEGACY_ARCHIVE_DEADLINE_MS, finishedAt = Date.now() - 3 * LEGACY_ARCHIVE_DEADLINE_MS;
    tree(path.join(root, "runs", "gone"), { finishedAt });
    const archive = new ResidentLegacyRunArchive(root, { legacyRunArchiveAgeMs: ageMs }, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && vi.getTimerCount() === 1);
      const base = sweeps.done();
      removeOutside(root, "gone");
      await settle(() => sweeps.done() > base && vi.getTimerCount() === 0);
      await turns();
      expect(vi.getTimerCount()).toBe(0);
      const calls = sweeps.calls();
      await advance(finishedAt + ageMs - Date.now() + 2 * LEGACY_ARCHIVE_DEADLINE_MS);
      expect(sweeps.calls()).toBe(calls);
      expect(vi.getTimerCount()).toBe(0);
      expect(retired(root)).toEqual([]);
      expect(archive.health.error).toBe("");
    } finally { await archive.close(); }
  });

  it("counterexample: of two young runs, removing the earlier keeps the survivor's own timer and archives it at its deadline", async () => {
    fakeTimers();
    const timeout = vi.spyOn(globalThis, "setTimeout");
    const root = temporary(), ageMs = 10 * LEGACY_ARCHIVE_DEADLINE_MS, start = Date.now();
    const goneAt = start - 3 * LEGACY_ARCHIVE_DEADLINE_MS + ageMs, keptAt = start - LEGACY_ARCHIVE_DEADLINE_MS + ageMs;
    tree(path.join(root, "runs", "gone"), { finishedAt: goneAt - ageMs });
    tree(path.join(root, "runs", "kept"), { finishedAt: keptAt - ageMs });
    const archive = new ResidentLegacyRunArchive(root, { legacyRunArchiveAgeMs: ageMs }, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && vi.getTimerCount() === 1);
      expect(Date.now() + Number(timeout.mock.calls.at(-1)![1])).toBe(goneAt);
      const base = sweeps.done();
      removeOutside(root, "gone");
      await settle(() => sweeps.done() > base && vi.getTimerCount() === 1 && Date.now() + Number(timeout.mock.calls.at(-1)![1]) === keptAt);
      const calls = sweeps.calls();
      // Past the removed run's original due: no sweep; the survivor's timer remains.
      await advance(goneAt - Date.now() + 1, 5);
      expect(sweeps.calls()).toBe(calls);
      expect(vi.getTimerCount()).toBe(1);
      expect(retired(root)).toEqual([]);
      await advance(keptAt - Date.now() - 1, 5);
      expect(sweeps.calls()).toBe(calls);
      vi.advanceTimersByTime(1);
      await settle(() => retired(root).includes("kept"));
      expect(sweeps.calls()).toBe(calls + 1);
      expect(archive.health.error).toBe("");
      await turns(500);
      expect(vi.getTimerCount()).toBe(0);
    } finally { await archive.close(); }
  });

  it("a removal missed while degraded is pruned by the completed recovery full pass; the survivor keeps its due", async () => {
    fakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const root = temporary(), ageMs = 10 * LEGACY_ARCHIVE_DEADLINE_MS, start = Date.now();
    const goneAt = start - 3 * LEGACY_ARCHIVE_DEADLINE_MS + ageMs, keptAt = start - LEGACY_ARCHIVE_DEADLINE_MS + ageMs;
    tree(path.join(root, "runs", "gone"), { finishedAt: goneAt - ageMs });
    tree(path.join(root, "runs", "kept"), { finishedAt: keptAt - ageMs });
    // The first runs watch observes a decoy, so the removal event is missed.
    const decoy = path.join(root, "decoy"); fs.mkdirSync(decoy, { mode: 0o700 });
    const original = fs.watch.bind(fs), runsWatchers: fs.FSWatcher[] = [];
    vi.spyOn(fs, "watch").mockImplementation(((...args: Parameters<typeof fs.watch>) => {
      if (args[0] !== path.join(root, "runs")) return original(...args);
      const watcher = runsWatchers.length ? original(...args) : (original as (...a: unknown[]) => fs.FSWatcher)(decoy, ...args.slice(1));
      runsWatchers.push(watcher);
      return watcher;
    }) as typeof fs.watch);
    const timeout = vi.spyOn(globalThis, "setTimeout");
    const archive = new ResidentLegacyRunArchive(root, { legacyRunArchiveAgeMs: ageMs }, { isRetained: () => false });
    const sweeps = track(archive);
    try {
      archive.start();
      await settle(() => sweeps.done() >= 1 && vi.getTimerCount() === 1);
      removeOutside(root, "gone");
      await turns(200);
      // Missed: the stale due still drives the only timer.
      expect(Date.now() + Number(timeout.mock.calls.at(-1)![1])).toBe(goneAt);
      runsWatchers[0]!.emit("error", new Error("watch failed"));
      vi.advanceTimersByTime(1_000); // bounded re-arm: real watch + rescan
      expect(runsWatchers).toHaveLength(2);
      await settle(() => vi.getTimerCount() === 1 && Date.now() + Number(timeout.mock.calls.at(-1)![1]) === keptAt, 20_000);
      await turns(200);
      expect(vi.getTimerCount()).toBe(1);
      const calls = sweeps.calls();
      await advance(goneAt - Date.now() + 1, 5);
      expect(sweeps.calls()).toBe(calls);
      vi.advanceTimersByTime(keptAt - Date.now());
      await settle(() => retired(root).includes("kept"));
      expect(archive.health.error).toBe("");
    } finally { await archive.close(); }
  });

  it("close disposes timers and watchers; later events do nothing", async () => {
    fakeTimers();
    const root = temporary();
    const archive = new ResidentLegacyRunArchive(root, {}, { isRetained: () => false });
    const sweeps = track(archive);
    archive.start();
    await settle(() => sweeps.done() >= 1);
    await archive.close();
    expect(vi.getTimerCount()).toBe(0);
    tree(path.join(root, "staging", "late"), {});
    fs.renameSync(path.join(root, "staging", "late"), path.join(root, "runs", "late"));
    for (let i = 0; i < 200; i++) await new Promise(resolve => setImmediate(resolve));
    expect(retired(root)).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });
});
