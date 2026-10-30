import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// Keep reason: pi-fabric#1122's Windows stopped-writer catch. After >24h,
// liveIds is empty but the manager still owns the old handle; collecting its
// run breaks manager.stop with FABRIC_MISSING_RESULT. Exercise the real legacy
// collector on every host, without changing process.platform or result auth.
vi.mock("../src/storage/retention-platform.js", () => ({ retentionV2Enabled: () => false }));
const recorded = vi.hoisted(() => ({
  mutations: [] as Array<{ target: string; deleter: string; judged: unknown }>,
  compactions: [] as string[],
}));
vi.mock("../src/storage/verified-remove.js", async (importOriginal) => ({
  ...await importOriginal<typeof import("../src/storage/verified-remove.js")>(),
  // Never call proceed, fallback or the real remover: record run removal
  // at its final mutation boundary; event compaction must never reach it.
  verifiedRemove: (target: string, judged: unknown, options: { deleter: string }) => {
    recorded.mutations.push({ target, judged, deleter: options.deleter });
    return { removed: false, reason: "recorded only (test)" };
  },
}));
vi.mock("../src/storage/retention.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/storage/retention.js")>();
  return {
    ...actual,
    compactTerminalRunEvents: (...args: Parameters<typeof actual.compactTerminalRunEvents>) => {
      recorded.compactions.push(args[0]);
      return actual.compactTerminalRunEvents(...args);
    },
  };
});

const { ResidentRequestRetention } = await import("../src/residency/retention.js");
const DAY = 86_400_000;
const EXITED_PID = "2147483647";
afterEach(() => {
  vi.restoreAllMocks();
  recorded.mutations.length = 0;
  recorded.compactions.length = 0;
  // No fixture teardown: the invoking owner retains/removes its private roots.
});

const writeJson = (file: string, value: unknown) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
};
const fixture = (now: number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "residency-main-sweep-custody-"));
  const actorRoot = path.join(root, "actors");
  // A real, readable empty registry: no wildcard or latest-run reference hides
  // the missing custody check, and unrelated runs remain eligible.
  writeJson(path.join(actorRoot, "actors.json"), { actors: [] });
  const make = (id: string) => {
    const run = path.join(root, "runs", id);
    const result = {
      id, name: id, task: "stopped writer", cwd: root, runner: "pi", transport: "process",
      status: "stopped", sessionId: EXITED_PID, text: "exact saved outcome",
      startedAt: now - 4 * DAY, finishedAt: now - 3 * DAY, updatedAt: now - 3 * DAY,
      turns: 0, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
    writeJson(path.join(run, "status.json"), result);
    writeJson(path.join(run, "reply.json"), { text: result.text });
    fs.writeFileSync(path.join(run, "events.jsonl"), Array.from({ length: 401 },
      (_, sequence) => JSON.stringify({ sequence }) + "\n").join(""), { mode: 0o600 });
    writeJson(path.join(root, "agents", `${id}.json`), {
      format: 1, id, rootId: "resident-root", runDirectory: run,
      createdAt: result.startedAt, updatedAt: result.updatedAt,
      handle: { id, name: id, cwd: root, runner: "pi", transport: "process", status: "stopped", residency: "durable" },
    });
    writeJson(path.join(root, "results", `${id}.json`), result);
    fs.utimesSync(run, (now - 2 * DAY) / 1000, (now - 2 * DAY) / 1000);
    return run;
  };
  return { root, actorRoot, owned: make("owned"), unrelated: make("unrelated") };
};
const snapshot = (root: string): unknown => fs.readdirSync(root).sort().map(name => {
  const file = path.join(root, name);
  return [name, fs.lstatSync(file).isDirectory() ? snapshot(file) : fs.readFileSync(file, "utf8")];
});
const recordDirectRemovals = () => ({
  // Atomic health/expiry writers also attempt cleanup of already-renamed temps.
  // Record those too, so even an unexpected fallback never deletes anything.
  rm: vi.spyOn(fs, "rmSync").mockImplementation(() => {}),
  unlink: vi.spyOn(fs, "unlinkSync").mockImplementation(() => {}),
  rmdir: vi.spyOn(fs, "rmdirSync").mockImplementation(() => {}),
});

describe("off-Linux main sweep manager custody (recorded mutations only)", () => {
  it.each([false, true])("keeps owned storage but progresses unrelated storage with retainRuns=%s", retainRuns => {
    const now = Date.now();
    const { root, actorRoot, owned, unrelated } = fixture(now);
    const before = snapshot(path.join(root, "runs"));
    const resultsBefore = snapshot(path.join(root, "results"));
    const direct = recordDirectRemovals();
    const nativeExitChecks = vi.spyOn(process, "kill");
    const recover = vi.fn();
    const custody = { run: vi.fn((id: string) => id === "owned"), reference: vi.fn(() => false) };
    const collector = new ResidentRequestRetention(root, [actorRoot], { retainRuns }, recover, custody);
    try {
      collector.sweep(now, new Set(), 10_000);
      expect(collector.due(now)).toBe(false);
    } finally { collector.close(); }

    expect(custody.run.mock.calls.map(([id]) => id).sort()).toEqual(["owned", "unrelated"]);
    expect(recover.mock.calls.map(([run]) => run)).toEqual([unrelated]);
    expect(recorded.compactions).toEqual(retainRuns ? [unrelated] : []);
    // retainRuns=false still proves the custody veto against a real eligible removal.
    // retainRuns=true visits/recoveries progress, but preserving traces has no mutation.
    expect(recorded.mutations).toEqual(retainRuns ? [] : [{
      target: unrelated, deleter: "residentRunRetention",
      judged: expect.objectContaining({ ino: expect.any(Number), dev: expect.any(Number) }),
    }]);
    // The removal case must still pass fresh native absence and owned
    // tree/saved-result checks. No exit proof is needed for an inert compaction.
    if (retainRuns) expect(nativeExitChecks).not.toHaveBeenCalled();
    else expect(nativeExitChecks).toHaveBeenCalledWith(Number(EXITED_PID), 0);
    expect(recorded.mutations.some(({ target }) => target === owned || target.startsWith(owned + path.sep))).toBe(false);
    for (const spy of [direct.rm, direct.unlink, direct.rmdir]) {
      expect(spy.mock.calls.filter(([target]) => String(target).startsWith(path.join(root, "runs") + path.sep))).toEqual([]);
    }
    expect(snapshot(path.join(root, "runs"))).toEqual(before);
    expect(snapshot(path.join(root, "results"))).toEqual(resultsBefore);
  });

  it.each([false, true])("fresh live-writer proof still vetoes unrelated mutation with retainRuns=%s", retainRuns => {
    const now = Date.now();
    const { root, actorRoot, owned, unrelated } = fixture(now);
    recordDirectRemovals();
    const recover = vi.fn((run: string) => {
      if (run !== unrelated) throw new Error("manager-owned run reached archive recovery");
      const statusFile = path.join(run, "status.json");
      const status = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      // An exit snapshot cannot authorize mutation after a writer reappears.
      writeJson(statusFile, { ...status, sessionId: String(process.pid) });
    });
    const ownedBefore = snapshot(owned);
    const collector = new ResidentRequestRetention(root, [actorRoot], { retainRuns }, recover,
      { run: id => id === "owned", reference: () => false });
    try { collector.sweep(now, new Set(), 10_000); } finally { collector.close(); }
    expect(recover.mock.calls.map(([run]) => run)).toEqual([unrelated]);
    expect(recorded.mutations).toEqual([]);
    expect(recorded.compactions).toEqual([unrelated]);
    expect(snapshot(owned)).toEqual(ownedBefore);
  });
});
