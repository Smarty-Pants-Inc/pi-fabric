import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import {
  archiveRaw,
  buildComparison,
  compareBaselineVsClean,
  compareDrift,
  compareQueue,
  compatibilityReport,
  groupRows,
  readRunFile,
  roundStats,
  sha256,
  stats,
  writeExclusive,
} from "../lib/prewalk-bench-lib.mjs";
import { summarizeStage } from "../probe-prewalk-drift.mjs";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const tempRoots: string[] = [];
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "prewalk-bench-tools-"));
  tempRoots.push(root);
  return root;
};

afterAll(() => {
  for (const root of tempRoots) fs.rmSync(root, { recursive: true, force: true });
});

const queueRow = (overrides: Record<string, unknown> = {}) => ({
  prewalk: false,
  followUpMode: "one-at-a-time",
  steers: 0,
  history: 0,
  profile: "compact",
  totalMs: 1,
  totalContextBytes: 1000,
  requests: [{ model: "synthetic-executor" }],
  ...overrides,
});

const driftSample = (overrides: Record<string, unknown> = {}) => ({
  git: true,
  count: 100,
  name: "baseline",
  wallMs: 5,
  cpuMs: 3,
  reported: null,
  ...overrides,
});

const makeRun = (options: {
  sourceFingerprint?: string;
  node?: string;
  settings?: unknown;
  workers?: Array<{ workerId: number; queue: Array<Record<string, unknown>>; drift?: { samples: Array<Record<string, unknown>> } }>;
} = {}) => ({
  schemaVersion: 1,
  status: "completed",
  startedAt: "2026-09-19T00:00:00.000Z",
  finishedAt: "2026-09-19T00:01:00.000Z",
  elapsedMs: 60_000,
  settings: options.settings ?? { processes: 2, samples: 3 },
  environment: {
    node: options.node ?? "v26.7.0",
    platform: "linux",
    arch: "x64",
    cpus: 32,
    hostPackages: { "pi-coding-agent": "0.85.1" },
  },
  provenance: { sourceFingerprint: options.sourceFingerprint ?? "fp-1", runnerSha256: "runner-1", sourceHashes: { "a.ts": "aa" } },
  summary: { findings: [] },
  workers: options.workers ?? [{ workerId: 0, queue: [queueRow()], drift: { samples: [driftSample()] } }],
});

const readFixtureRun = (run: unknown, name: string) => {
  const file = path.join(tempRoot(), name);
  fs.writeFileSync(file, JSON.stringify(run));
  return readRunFile(file);
};

describe("prewalk bench lib stats", () => {
  it("uses nearest-rank medians and rounds to four digits without changing n", () => {
    expect(stats([4, 1, 3, 2])).toEqual({ n: 4, min: 1, median: 2, p95: 4, max: 4 });
    expect(roundStats(stats([1.23456789]))).toEqual({ n: 1, min: 1.2346, median: 1.2346, p95: 1.2346, max: 1.2346 });
    expect(roundStats(null)).toBeNull();
    expect(() => stats([])).toThrow(/at least one value/);
    expect(() => stats([1, Number.NaN])).toThrow(/finite numbers/);
  });

  it("groups rows deterministically", () => {
    const grouped = groupRows([{ k: "b", v: 1 }, { k: "a", v: 2 }, { k: "b", v: 3 }], (row) => row.k, (rows) => ({ sum: rows.reduce((total, row) => total + row.v, 0) }));
    expect(grouped).toEqual([{ id: "b", sum: 4 }, { id: "a", sum: 2 }]);
  });
});

describe("prewalk bench run parsing and archiving", () => {
  it("reads raw and gzip inputs and reports the raw SHA-256", () => {
    const root = tempRoot();
    const rawFile = path.join(root, "run.json");
    fs.writeFileSync(rawFile, JSON.stringify(makeRun()));
    const raw = readRunFile(rawFile);
    expect(raw.gzSha256).toBeNull();
    expect(raw.rawSha256).toBe(sha256(fs.readFileSync(rawFile)));
    const gzFile = path.join(root, "run.json.gz");
    fs.writeFileSync(gzFile, zlib.gzipSync(fs.readFileSync(rawFile)));
    const gzipped = readRunFile(gzFile);
    expect(gzipped.rawSha256).toBe(raw.rawSha256);
    expect(gzipped.rawBytes).toBe(raw.rawBytes);
    expect(gzipped.gzSha256).toBe(sha256(fs.readFileSync(gzFile)));
  });

  it("gzips, verifies decompressed bytes, refuses overwrite, and removes raw only after verification", () => {
    const root = tempRoot();
    const rawFile = path.join(root, "run.json");
    fs.writeFileSync(rawFile, "{\"hello\":\"world\"}\n");
    const gzFile = path.join(root, "run.json.gz");
    const archived = archiveRaw(rawFile, gzFile);
    expect(archived.verified).toBe(true);
    expect(archived.rawSha256).toBe(sha256(fs.readFileSync(rawFile)));
    expect(zlib.gunzipSync(fs.readFileSync(gzFile)).toString("utf8")).toBe(fs.readFileSync(rawFile, "utf8"));
    expect(archived.checksums).toHaveLength(2);
    expect(() => archiveRaw(rawFile, gzFile)).toThrow(/Refusing to overwrite/);
    const rawSecond = path.join(root, "run2.json");
    fs.writeFileSync(rawSecond, "{\"a\":1}\n");
    const removed = archiveRaw(rawSecond, undefined, { removeRaw: true });
    expect(removed.verified).toBe(true);
    expect(removed.removedRaw).toBe(true);
    expect(fs.existsSync(rawSecond)).toBe(false);
  });

  it("writes outputs exclusively", () => {
    const out = path.join(tempRoot(), "out.json");
    writeExclusive(out, "{}\n");
    expect(fs.readFileSync(out, "utf8")).toBe("{}\n");
    expect(() => writeExclusive(out, "{}\n")).toThrow(/Refusing to overwrite/);
  });
});

describe("prewalk comparison", () => {
  it("labels source, runner, settings and host compatibility", () => {
    const a = readFixtureRun(makeRun(), "a.json");
    const b = readFixtureRun(makeRun({ node: "v25.0.0" }), "b.json");
    const report = compatibilityReport([a, b]);
    expect(report.checks.node).toBe("differ");
    expect(report.checks.sourceFingerprint).toBe("match");
    expect(report.comparable).toBe(true);
    expect(report.hostComparable).toBe(false);
    const c = readFixtureRun(makeRun({ settings: { processes: 1 } }), "c.json");
    expect(compatibilityReport([a, c]).comparable).toBe(false);
  });

  it("reports queue cells with real request/context counts and per-worker spread", () => {
    const runA = makeRun({
      workers: [
        { workerId: 0, queue: [queueRow({ totalMs: 2, totalContextBytes: 2000 }), queueRow({ prewalk: true, totalMs: 4, totalContextBytes: 4000, requests: [{}, {}] })], drift: { samples: [driftSample()] } },
        { workerId: 1, queue: [queueRow({ totalMs: 4 }), queueRow({ prewalk: true, totalMs: 8, totalContextBytes: 4000, requests: [{}, {}] })], drift: { samples: [driftSample()] } },
      ],
    });
    const runB = makeRun({
      workers: [
        { workerId: 0, queue: [queueRow({ totalMs: 3 }), queueRow({ prewalk: true, totalMs: 6, requests: [{}, {}] })], drift: { samples: [driftSample()] } },
        { workerId: 1, queue: [queueRow({ totalMs: 5 }), queueRow({ prewalk: true, totalMs: 10, requests: [{}, {}] })], drift: { samples: [driftSample()] } },
      ],
    });
    const cells = compareQueue([readFixtureRun(runA, "qa.json"), readFixtureRun(runB, "qb.json")], ["a", "b"]);
    const prewalkCell = cells.find((cell) => cell.id.startsWith("prewalk/"));
    expect(prewalkCell?.requestsMedian.a?.median).toBe(2);
    expect(prewalkCell?.contextBytesMedian.a?.median).toBe(4000);
    expect(prewalkCell?.samples.a).toBe(2);
    expect(prewalkCell?.perWorker.a?.workers).toBe(2);
    expect(prewalkCell?.perWorker.a?.spreadMs).toBe(4);
    expect(prewalkCell?.deltaMedianTotalMs).toBe(2);
    const comparison = buildComparison([readFixtureRun(runA, "qc.json"), readFixtureRun(runB, "qd.json")], ["a", "b"]);
    expect(comparison.units.time).toContain("milliseconds");
    expect(comparison.queue.cells).toHaveLength(2);
  });

  it("pairs drift clean against baseline", () => {
    const run = makeRun({
      workers: [{
        workerId: 0,
        queue: [queueRow()],
        drift: {
          samples: [
            driftSample({ name: "baseline", wallMs: 10 }),
            driftSample({ name: "baseline", wallMs: 12 }),
            driftSample({ name: "baseline", wallMs: 14 }),
            driftSample({ name: "clean", wallMs: 6 }),
            driftSample({ name: "clean", wallMs: 8 }),
            driftSample({ name: "clean", wallMs: 10 }),
          ],
        },
      }],
    });
    const inputs = [readFixtureRun(run, "da.json"), readFixtureRun(run, "db.json")];
    const groups = compareDrift(inputs, ["a", "b"]);
    expect(groups.find((group) => group.id === "git/100/clean")?.wallMs.a?.median).toBe(8);
    expect(groups.find((group) => group.id === "git/100/baseline")?.wallMs.a?.median).toBe(12);
    const pairs = compareBaselineVsClean(groups, ["a", "b"]);
    expect(pairs).toHaveLength(1);
    expect(pairs[0]?.deltaCleanMinusBaselineMs.a).toBe(-4);
  });
});

describe("prewalk orchestration benchmark", () => {
  it("runs the quick entrypoint with passive delivery and exact request counts", () => {
    const out = path.join(tempRoot(), "queue-benchmark.json");
    const result = spawnSync(process.execPath, [
      path.join(projectRoot, "bench", "prewalk", "benchmark-prewalk.mjs"),
      "--quick", "--out", out,
    ], { cwd: projectRoot, encoding: "utf8", timeout: 120_000 });
    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr).toBe(0);
    const run = JSON.parse(fs.readFileSync(out, "utf8"));
    expect(run.status).toBe("completed");
    expect(run.summary.queueTrials).toBeGreaterThan(0);
    expect(run.summary.findings).toEqual([]);
    expect(run.checks).toMatchObject({
      missingPlanCanaryCaught: true,
      sourceUnchanged: true,
      lq1FirstRequestContract: true,
      b2CarryHashVerified: true,
    });
    for (const worker of run.workers) {
      for (const row of worker.queue) {
        // One boundary continuation, or exactly the legitimate steering turns.
        expect(row.requests).toHaveLength(Math.max(1, row.steers));
        for (const request of row.requests) {
          expect(request.continuationCount).toBe(row.prewalk ? 1 : 0);
          if (row.prewalk) {
            expect(request.plan).toBe(true);
            expect(request.digest).toBe(true);
          }
        }
      }
    }
  }, 120_000);
});

describe("probe-prewalk-drift integration", () => {
  it("attributes listing, stat and hash stages per git fixture category", () => {
    const out = path.join(tempRoot(), "probe.json");
    const result = spawnSync(process.execPath, [
      path.join(projectRoot, "bench", "prewalk", "probe-prewalk-drift.mjs"),
      "--quick", "--files", "4", "--scenarios", "mixed,ignored", "--out", out,
    ], { cwd: projectRoot, encoding: "utf8", timeout: 120_000 });
    expect(result.status, result.stderr).toBe(0);
    const probe = JSON.parse(fs.readFileSync(out, "utf8"));
    expect(probe.checks.failures).toEqual([]);
    const mixed = probe.scenarios.find((scenario: { id: string }) => scenario.id === "mixed");
    expect(mixed.expectedListedFiles).toBe(17);
    expect(mixed.stages.baseline.kinds.subprocess.count).toBe(2);
    expect(mixed.stages.baseline.kinds.stat.count).toBe(17);
    expect(mixed.stages.clean.kinds.hash?.count ?? 0).toBe(0);
    expect(mixed.stages.changed.kinds.hash.count).toBe(1);
    const ignored = probe.scenarios.find((scenario: { id: string }) => scenario.id === "ignored");
    expect(ignored.expectedListedFiles).toBe(1);
    expect(ignored.stages.baseline.kinds.stat.count).toBe(1);
  }, 120_000);
});

describe("drift stage aggregation", () => {
  const sample = (kinds: Record<string, unknown>) => ({ wallMs: 10, totalServiceMs: 5, kinds });

  it("keeps the union of kinds when a sample never observed one of them", () => {
    const statOnly = sample({
      stat: { count: 2, serviceMs: 4, wallMs: 6, labels: { ".": { count: 2, serviceMs: 4 } } },
    });
    const withSubprocess = sample({
      stat: { count: 4, serviceMs: 8, wallMs: 9, labels: { ".": { count: 4, serviceMs: 8 } } },
      subprocess: {
        count: 1,
        serviceMs: 3,
        wallMs: 3,
        labels: { "git ls-files": { count: 1, serviceMs: 3, listedFiles: 7 } },
      },
    });
    const summary = summarizeStage([statOnly, withSubprocess]);
    expect(Object.keys(summary.kinds).sort()).toEqual(["stat", "subprocess"]);
    expect(summary.kinds.subprocess?.count).toBe(1);
    expect(summary.kinds.subprocess?.labels["git ls-files"]?.count).toBe(1);
    expect(summary.kinds.subprocess?.labels["git ls-files"]?.listedFiles).toBe(7);
    // Statistics come from the samples that recorded the kind, not from a hole.
    expect(summary.kinds.stat?.labels["."]?.count).toBe(2);
    expect(summary.samples).toBe(2);
  });

  it("keeps a kind that only the first sample observed", () => {
    const withSubprocess = sample({ subprocess: { count: 1, serviceMs: 3, wallMs: 3, labels: {} } });
    const statOnly = sample({ stat: { count: 2, serviceMs: 4, wallMs: 6, labels: {} } });
    const summary = summarizeStage([withSubprocess, statOnly]);
    expect(Object.keys(summary.kinds).sort()).toEqual(["stat", "subprocess"]);
    expect(summary.kinds.subprocess?.count).toBe(1);
    expect(summary.kinds.stat?.count).toBe(2);
  });
});
