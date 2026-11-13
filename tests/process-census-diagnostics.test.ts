import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi, type OnTestFinishedHandler, type TestContext } from "vitest";
import { PROCESS_CENSUS_TEST_EVENT, emitProcessCensusTestEvent } from "../src/agents/transports/linux-process-census.js";
import { withProcessCensusDiagnostics, type ProcessCensusDiagnostics } from "./helpers/process-census-diagnostics.js";

// Keep reason: timeout/retry isolation and diagnostic I/O failures cannot be reached cheaply by a live process flow.
type Line = {
  kind: string; attemptID: string; pid: number; testName: string; retryCount?: number;
  elapsedMs: number; currentPhase: string; lastPhase?: string; bodyOutcome?: string;
  artifactErrorCode?: string; details?: Record<string, unknown>;
  availability?: { platform: string; realBusPresent: boolean; systemdRunPath: string | null;
    delegatedUserCgroupAvailable: { outcome: string } };
  counters: { counts: Record<string, number>; byReason: Record<string, Record<string, number>>;
    byErrorCode: Record<string, number>; entries: number; pidEntries: number; dirs: number;
    scopedAttempts: number; unscopedAttempts: number; lastError?: { code: string } };
};
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};

let root: string;
let finishers: (() => Promise<void>)[];
const runnerContext = (name: string, retryCount = 0) => {
  const hooks: OnTestFinishedHandler[] = [];
  const task = { name, result: { state: "run", retryCount } };
  const context = { task, onTestFinished: (hook: OnTestFinishedHandler) => { hooks.push(hook); } } as unknown as TestContext;
  const finish = async (state = "pass") => {
    task.result.state = state;
    for (const hook of hooks.splice(0).reverse()) await hook(context);
  };
  finishers.push(finish);
  return { context, finish };
};
const files = (): string[] => {
  const directory = path.join(root, "process-census-diagnostics");
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory).map(entry => path.join(directory, entry, "events.jsonl"));
};
const lines = (file = files()[0]!): Line[] => fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line) as Line);
const last = (records: Line[]): Line => records[records.length - 1]!;

beforeEach(context => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "process-census-diagnostics-test-"));
  finishers = [];
  vi.stubEnv("PI_TEST_REPORT_DIR", root);
  vi.stubEnv("PI_TEST_PROCESS_CENSUS_DIAGNOSTICS", "previous-value");
  // Registered first, so genuine wrapper finish hooks and their assertions precede temp cleanup.
  context.onTestFinished(async () => {
    try { for (const finish of finishers) await finish(); }
    finally {
      vi.restoreAllMocks();
      vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("process census diagnostics", () => {
  it("uses a genuine Vitest finish hook and flushes independent JSONL snapshots immediately", async context => {
    const initialListeners = process.listenerCount(PROCESS_CENSUS_TEST_EVENT);
    context.onTestFinished(() => {
      const records = lines();
      expect(last(records)).toMatchObject({ kind: "test-finished", lastPhase: "pressure-pending", bodyOutcome: "passed" });
      expect(process.env.PI_TEST_PROCESS_CENSUS_DIAGNOSTICS).toBe("previous-value");
      expect(process.listenerCount(PROCESS_CENSUS_TEST_EVENT)).toBe(initialListeners);
    });
    await withProcessCensusDiagnostics(context, true, async diagnostic => {
      expect(process.env.PI_TEST_PROCESS_CENSUS_DIAGNOSTICS).toBe("1");
      emitProcessCensusTestEvent({ kind: "attempt", scoped: true, dirs: 2 });
      emitProcessCensusTestEvent({ kind: "full-scan", reason: "empty-scoped-snapshot", entries: 12, pidEntries: 9 });
      diagnostic.mark("pressure-pending", { frontier: 100000, ready: true, rawTestData: "not-for-output", pids: [1, 2] });
      const records = lines();
      expect(records.filter(record => record.kind !== "census-checkpoint").map(record => record.kind))
        .toEqual(["test-start", "phase-start", "phase-end", "phase-start"]);
      expect(records[0]?.availability).toMatchObject({ platform: process.platform, realBusPresent: expect.any(Boolean),
        delegatedUserCgroupAvailable: { outcome: expect.stringMatching(/^(available|unavailable|error)$/) } });
      expect(last(records)).toMatchObject({ currentPhase: "pressure-pending", details: { frontier: 100000, ready: true },
        counters: { counts: { attempt: 1, "full-scan": 1 }, entries: 12, pidEntries: 9, dirs: 2, scopedAttempts: 1 } });
      emitProcessCensusTestEvent({ kind: "full-scan", reason: "known-live-escape", entries: 4, pidEntries: 3 });
      // The event receipt is flushed before another phase, while prior JSONL snapshots stay unchanged.
      expect(last(lines())).toMatchObject({ kind: "census-checkpoint", counters: { entries: 16 } });
      expect(last(records).counters.entries).toBe(12);
    });
    const records = lines();
    expect(last(records).counters.entries).toBe(16);
    expect(new Set(records.map(record => record.attemptID)).size).toBe(1);
    expect(records.every(record => record.attemptID.startsWith(`${process.pid}-`) && record.pid === process.pid)).toBe(true);
    expect(records.every((record, index) => record.elapsedMs >= (records[index - 1]?.elapsedMs ?? 0))).toBe(true);
  });

  it("does absolutely nothing except call run when disabled", async () => {
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const append = vi.spyOn(fs, "appendFileSync");
    const output = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const listeners = process.listenerCount(PROCESS_CENSUS_TEST_EVENT);
    const context = new Proxy({}, { get() { throw new Error("disabled must not inspect context"); } }) as TestContext;
    let called = false;
    await withProcessCensusDiagnostics(context, false, async diagnostic => {
      called = true;
      diagnostic.mark("ignored", { value: 1 });
    });
    expect(called).toBe(true);
    expect(process.env.PI_TEST_PROCESS_CENSUS_DIAGNOSTICS).toBe("previous-value");
    expect(process.listenerCount(PROCESS_CENSUS_TEST_EVENT)).toBe(listeners);
    expect(mkdir).not.toHaveBeenCalled();
    expect(append).not.toHaveBeenCalled();
    expect(output).not.toHaveBeenCalled();
  });

  it("aggregates exact event/reason/error totals without retaining raw payloads", async () => {
    const runner = runnerContext("all census events", 2);
    await withProcessCensusDiagnostics(runner.context, true, async diagnostic => {
      for (const scoped of [true, false]) emitProcessCensusTestEvent({ kind: "attempt", scoped, dirs: 3 });
      for (const reason of ["primary-exited", "explicit-full", "unverified-scope", "primary-exited"]) {
        emitProcessCensusTestEvent({ kind: "full-view-request", reason });
      }
      for (const reason of ["no-verified-scope", "no-verified-scope", "cgroup-unreadable"]) {
        emitProcessCensusTestEvent({ kind: "full-scan", reason, entries: 10, pidEntries: 7 });
      }
      for (const code of ["ENOENT", "EACCES", "ENOENT"]) {
        emitProcessCensusTestEvent({ kind: "cgroup-procs-read" });
        emitProcessCensusTestEvent({ kind: "cgroup-procs-read-error", code, error: "RAW-CREDENTIAL-SENTINEL", pids: [100, 200] });
      }
      emitProcessCensusTestEvent({ kind: "attribution-empty" });
      emitProcessCensusTestEvent({ kind: "scope-armed" });
      diagnostic.mark("assert-totals");
    });
    await runner.finish();
    expect(last(lines())).toMatchObject({ retryCount: 2, testName: "all census events", counters: {
      counts: { attempt: 2, "full-scan": 3, "full-view-request": 4, "cgroup-procs-read": 3,
        "cgroup-procs-read-error": 3, "attribution-empty": 1, "scope-armed": 1 },
      byReason: { "full-scan": { "no-verified-scope": 2, "cgroup-unreadable": 1 },
        "full-view-request": { "primary-exited": 2, "explicit-full": 1, "unverified-scope": 1 } },
      byErrorCode: { ENOENT: 2, EACCES: 1 }, entries: 30, pidEntries: 21, dirs: 6,
      scopedAttempts: 1, unscopedAttempts: 1, lastError: { code: "ENOENT" },
    } });
    expect(fs.readFileSync(files()[0]!, "utf8")).not.toContain("RAW-CREDENTIAL-SENTINEL");
  });

  it("fences a timed-out body and old marks from a retry while preserving environment ownership", async () => {
    const initialListeners = process.listenerCount(PROCESS_CENSUS_TEST_EVENT);
    const old = runnerContext("retry isolation", 0);
    const resumeOld = deferred();
    let oldDiagnostic!: ProcessCensusDiagnostics;
    const oldBody = withProcessCensusDiagnostics(old.context, true, async diagnostic => {
      oldDiagnostic = diagnostic;
      diagnostic.mark("scan-pending");
      emitProcessCensusTestEvent({ kind: "full-scan", reason: "no-verified-scope", entries: 5, pidEntries: 3 });
      await resumeOld.promise;
      emitProcessCensusTestEvent({ kind: "full-scan", reason: "late-old", entries: 999, pidEntries: 999 });
      diagnostic.mark("late-old-mark");
    });
    const oldFile = files()[0]!;
    expect(last(lines(oldFile))).toMatchObject({ kind: "census-checkpoint", currentPhase: "scan-pending",
      counters: { entries: 5, pidEntries: 3 } });
    const retry = runnerContext("retry isolation", 1);
    await withProcessCensusDiagnostics(retry.context, true, async diagnostic => {
      await old.finish("fail");
      expect(process.env.PI_TEST_PROCESS_CENSUS_DIAGNOSTICS).toBe("1");
      const frozen = fs.readFileSync(oldFile, "utf8");
      expect(last(lines(oldFile))).toMatchObject({ kind: "test-finished", bodyOutcome: "pending", lastPhase: "scan-pending",
        counters: { entries: 5, pidEntries: 3 } });
      resumeOld.resolve();
      await oldBody;
      // Even an escaped mark called from the NEW attempt's context must be inert.
      oldDiagnostic.mark("borrowed-retry-context");
      emitProcessCensusTestEvent({ kind: "attempt", scoped: true });
      diagnostic.mark("retry-current");
      expect(fs.readFileSync(oldFile, "utf8")).toBe(frozen);
    });
    // Out-of-scope events cannot be borrowed by any still-installed listener.
    emitProcessCensusTestEvent({ kind: "scope-armed" });
    await retry.finish();
    const retryFile = files().find(file => file !== oldFile)!;
    expect(last(lines(retryFile))).toMatchObject({ retryCount: 1, counters: { counts: { attempt: 1, "full-scan": 0, "scope-armed": 0 }, entries: 0 } });
    expect(last(lines(retryFile)).attemptID).not.toBe(last(lines(oldFile)).attemptID);
    expect(process.env.PI_TEST_PROCESS_CENSUS_DIAGNOSTICS).toBe("previous-value");
    expect(process.listenerCount(PROCESS_CENSUS_TEST_EVENT)).toBe(initialListeners);
  });

  it("preserves the exact assertion failure and its failed phase artifact", async () => {
    delete process.env.PI_TEST_PROCESS_CENSUS_DIAGNOSTICS;
    const runner = runnerContext("assertion preservation");
    const original = new Error("original assertion");
    await expect(withProcessCensusDiagnostics(runner.context, true, async diagnostic => {
      diagnostic.mark("failing-assertion");
      throw original;
    })).rejects.toBe(original);
    await runner.finish("fail");
    expect(last(lines())).toMatchObject({ kind: "test-finished", lastPhase: "failing-assertion", bodyOutcome: "failed" });
    expect(process.env.PI_TEST_PROCESS_CENSUS_DIAGNOSTICS).toBeUndefined();
  });

  it("preserves the original rejection even when stdout and mark metadata fail", async () => {
    delete process.env.PI_TEST_REPORT_DIR;
    vi.spyOn(process.stdout, "write").mockImplementation(() => { throw new Error("closed stdout"); });
    const runner = runnerContext("diagnostic failure");
    const original = new Error("real test failure");
    await expect(withProcessCensusDiagnostics(runner.context, true, async diagnostic => {
      diagnostic.mark("metadata-failure", new Proxy({}, { ownKeys() { throw new Error("bad metadata"); } }));
      throw original;
    })).rejects.toBe(original);
    await expect(runner.finish("fail")).resolves.toBeUndefined();
    expect(process.env.PI_TEST_PROCESS_CENSUS_DIAGNOSTICS).toBe("previous-value");
  });

  it.each(["absent", "mkdir-error", "append-error"] as const)("falls back to prefixed JSON stdout (%s), without masking a failure", async mode => {
    if (mode === "absent") delete process.env.PI_TEST_REPORT_DIR;
    if (mode === "mkdir-error") {
      const notDirectory = path.join(root, "not-a-directory");
      fs.writeFileSync(notDirectory, "fixture");
      process.env.PI_TEST_REPORT_DIR = notDirectory;
    }
    if (mode === "append-error") vi.spyOn(fs, "appendFileSync").mockImplementation(() => { throw Object.assign(new Error("private-path"), { code: "EACCES" }); });
    const output = vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const runner = runnerContext("stdout fallback");
    const original = new Error("assertion remains authoritative");
    await expect(withProcessCensusDiagnostics(runner.context, true, async diagnostic => {
      diagnostic.mark("fallback-phase");
      throw original;
    })).rejects.toBe(original);
    await runner.finish("fail");
    const records = output.mock.calls.map(call => {
      const text = String(call[0]);
      expect(text.startsWith("PROCESS_CENSUS_DIAGNOSTIC ")).toBe(true);
      expect(text).not.toContain("private-path");
      return JSON.parse(text.slice("PROCESS_CENSUS_DIAGNOSTIC ".length)) as Line;
    });
    expect(last(records)).toMatchObject({ kind: "test-finished", lastPhase: "fallback-phase", bodyOutcome: "failed" });
    if (mode === "absent") expect(records.every(record => record.artifactErrorCode === undefined)).toBe(true);
    else expect(records.every(record => typeof record.artifactErrorCode === "string")).toBe(true);
    if (mode === "append-error") expect(records[0]?.artifactErrorCode).toBe("EACCES");
  });
});
