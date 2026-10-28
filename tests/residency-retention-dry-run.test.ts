import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

// Recorded-call proof (fv2-11107). verifiedRemove is replaced by a recorder:
// it captures the target and never delegates to the real removal or to the
// non-Linux fallback. All selection logic (age, references, tree/exit and
// saved-result fences) is the real product source.
const recorded = vi.hoisted(() => [] as Array<{ target: string; deleter: string; dryRun: boolean }>);
vi.mock("../src/storage/verified-remove.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/storage/verified-remove.js")>();
  return {
    ...actual,
    verifiedRemove: (target: string, _judged: unknown, options: { deleter: string; dryRun?: boolean }) => {
      recorded.push({ target, deleter: options.deleter, dryRun: options.dryRun === true });
      return options.dryRun ? { removed: true, bytes: 0 } : { removed: false, reason: "recorded only (test)" };
    },
  };
});

const { RESIDENT_RUN_RETENTION_MS, ResidentRequestRetention, sweepResidentRuns } = await import("../src/residency/retention.js");

const DAY = 24 * 60 * 60 * 1_000;
const EXITED_PID = "2147483647";
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  recorded.length = 0;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const writeJson = (file: string, value: unknown) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, JSON.stringify(value), { mode: 0o600 });
};
const age = (run: string, mtimeMs: number) => fs.utimesSync(run, mtimeMs / 1000, mtimeMs / 1000);

/** A residency root with one collectable run and one of each keep reason. */
const fixture = (now: number) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "residency-retention-dry-run-"));
  roots.push(dir);
  const runs = path.join(dir, "runs"), actorRoot = path.join(dir, "actor-registry");
  writeJson(path.join(actorRoot, "actors.json"), { actors: [{ id: "actor", lastRunId: "latest" }] });
  const make = (id: string, status: Record<string, unknown>) => {
    const run = path.join(runs, id);
    writeJson(path.join(run, "status.json"), { finishedAt: now - 3 * DAY, ...status });
    fs.writeFileSync(path.join(run, "reply.json"), '{"text":"keep"}');
    return run;
  };
  const exited = { status: "completed", transport: "process", sessionId: EXITED_PID };
  const run = {
    old: make("old", exited),
    young: make("young", exited),
    latest: make("latest", { ...exited, status: "failed" }),
    live: make("live", { status: "running", transport: "process", sessionId: String(process.pid) }),
    liveExitedRecord: make("live-pid-completed", { ...exited, sessionId: String(process.pid) }),
    unknown: make("unknown", { status: "completed", transport: "process" }),
    nonterminal: make("nonterminal", { status: "running", transport: "process", sessionId: EXITED_PID }),
    missingResult: make("missing-result", exited),
  };
  // Resident metadata exists but no saved result: deletion would lose the outcome.
  writeJson(path.join(dir, "agents", "missing-result.json"), { format: 1, id: "missing-result" });
  const aged = now - 2 * DAY;
  for (const [name, directory] of Object.entries(run)) age(directory, name === "young" ? now - 60_000 : aged);
  const snapshot = () => Object.fromEntries(Object.values(run).map(directory =>
    [directory, fs.readdirSync(directory).sort().map(name => [name, fs.readFileSync(path.join(directory, name), "utf8")])]));
  return { dir, runs, actorRoot, run, aged, snapshot };
};

describe.skipIf(process.platform === "win32")("resident run age pruning (no real deletion)", () => {
  it("RESIDENT_RUN_RETENTION_MS is exactly 24 hours", () => {
    expect(RESIDENT_RUN_RETENTION_MS).toBe(86_400_000);
  });

  it("sweepResidentRuns({dryRun:true}) selects only the aged unreferenced exited terminal run", () => {
    const now = Date.now();
    const { runs, actorRoot, run, snapshot } = fixture(now);
    const before = snapshot();
    const rm = vi.spyOn(fs, "rmSync"), rename = vi.spyOn(fs, "renameSync");
    expect(sweepResidentRuns(runs, now, 10_000, { actorRoots: [actorRoot], dryRun: true })).toEqual([run.old]);
    expect(recorded.map(call => call.target).sort()).toEqual([
      path.join(run.old, "reply.json"), path.join(run.old, "status.json"),
    ].sort());
    expect(recorded.every(call => call.deleter === "sweepResidentRuns" && call.dryRun)).toBe(true);
    expect(rm).not.toHaveBeenCalled(); expect(rename).not.toHaveBeenCalled();
    expect(snapshot()).toEqual(before);

    // retainRuns is an explicit policy: no run is selected for deletion.
    recorded.length = 0;
    expect(sweepResidentRuns(runs, now, 10_000, { actorRoots: [actorRoot], dryRun: true, retainRuns: true })).toEqual([]);
    expect(recorded).toEqual([]);
    // Age is the 24 h constant: just inside the window keeps the run.
    expect(sweepResidentRuns(runs, fs.statSync(run.old).mtimeMs + RESIDENT_RUN_RETENTION_MS - 1_000, 10_000,
      { actorRoots: [actorRoot], dryRun: true })).toEqual([]);
    // A caller retention veto (current reference) keeps the aged candidate too.
    expect(sweepResidentRuns(runs, now, 10_000, { actorRoots: [actorRoot], dryRun: true, isRetained: id => id === "old" })).toEqual([]);
    expect(recorded).toEqual([]);
    expect(snapshot()).toEqual(before);
  });

  it("the streaming ResidentRequestRetention selects the same old run (recorded verifiedRemove, no fallback)", () => {
    const now = Date.now();
    const { dir, actorRoot, run, snapshot } = fixture(now);
    const before = snapshot();
    const rm = vi.spyOn(fs, "rmSync");
    const collector = new ResidentRequestRetention(dir, [actorRoot], { retainRuns: false });
    try {
      for (let slice = 0; slice < 50 && collector.due(now); slice++) collector.sweep(now, new Set(), 10_000);
    } finally { collector.close(); }
    const fileOps = recorded.filter(call => call.deleter === "residentRunRetention");
    expect(fileOps).toHaveLength(1);
    expect([path.join(run.old, "status.json"), path.join(run.old, "reply.json")]).toContain(fileOps[0]!.target);
    expect(fileOps[0]).toMatchObject({ deleter: "residentRunRetention", dryRun: false });
    // Only atomic-write temp files of the collector's own health/expiry records.
    expect(rm.mock.calls.filter(([target]) => String(target).startsWith(path.join(dir, "runs")))).toEqual([]);
    expect(snapshot()).toEqual(before);
  });

  it("the streaming collector keeps the old run under retainRuns:true and unset retainRuns", () => {
    const now = Date.now();
    for (const retention of [{ retainRuns: true }, {}]) {
      const { dir, actorRoot } = fixture(now);
      const collector = new ResidentRequestRetention(dir, [actorRoot], retention);
      try {
        for (let slice = 0; slice < 50 && collector.due(now); slice++) collector.sweep(now, new Set(), 10_000);
      } finally { collector.close(); }
      expect(recorded.filter(call => call.deleter === "residentRunRetention")).toEqual([]);
    }
  });

  it.each(["live set", "manager custody", "in-place live status", "nested live descendant", "new latest reference"])(
    "the streaming collector vetoes the old run on a current %s change after reference preparation", (change) => {
      const now = Date.now();
      const { dir, actorRoot, run, aged } = fixture(now);
      // Applied just before the run's fresh checks, after the run-reference
      // snapshot for the scan was prepared (same hook as archive recovery).
      const recover = (directory: string) => {
        if (directory !== run.old) return;
        if (change === "in-place live status") {
          fs.writeFileSync(path.join(run.old, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: String(process.pid) }));
        } else if (change === "nested live descendant") {
          writeJson(path.join(run.old, "nested", "child", "status.json"), { status: "running", transport: "process", sessionId: String(process.pid) });
        } else if (change === "new latest reference") {
          writeJson(path.join(actorRoot, "actors.json"), { actors: [{ id: "actor", lastRunId: "latest" }, { id: "other", lastRunId: "old" }] });
        }
        // Keep the age gate satisfied so only the changed reference/tree vetoes.
        age(run.old, aged);
      };
      const custody = { run: (id: string) => change === "manager custody" && id === "old", reference: () => false };
      const collector = new ResidentRequestRetention(dir, [actorRoot], { retainRuns: false }, recover, custody);
      try {
        for (let slice = 0; slice < 50 && collector.due(now); slice++) collector.sweep(now, new Set(change === "live set" ? ["old"] : []), 10_000);
      } finally { collector.close(); }
      expect(recorded.filter(call => call.deleter === "residentRunRetention")).toEqual([]);
      expect(fs.existsSync(path.join(run.old, "status.json"))).toBe(true);
    });
});
