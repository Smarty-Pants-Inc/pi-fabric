import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { rotatePerCallLedger } from "../src/agents/per-call-route.js";
import { appendResidentLog, pruneResidentChildLogs, pruneWedgeEvidence, RESIDENT_LOG_MAX_BYTES } from "../src/residency/launcher.js";
import { FabricShellJobStore, formatShellHangNotice } from "../src/core/shell-jobs.js";
import { ResidentRequestRetention, sweepResidentRuns } from "../src/residency/retention.js";
import * as platform from "../src/storage/retention-platform.js";
import { pruneActorRunArchives, sweepTempRunRoots } from "../src/storage/retention.js";
import { sweepMeshRetention } from "../src/storage/retention-cli.js";
import { closeScratch, createScratch, ScratchScope, SCRATCH_OWNER_FILE, sweepScratch } from "../src/storage/scratch.js";

// Keep reason: data safety. Exercise real collectors on complete raw traces, not numeric default assertions.
// Each default/explicit-policy row is an independent test with a fresh fixture; no finite age props up a default test.
const DAY = 86_400_000;
const NOW = Date.now();
const OLD = NOW - 2 * 366 * DAY;
const TRACE = '{"type":"full-original-output","text":"must survive without a policy"}\n';
const POLICIES = [{ policy: "empty config", explicit: false }, { policy: "explicit short policy", explicit: true }] as const;
const roots: string[] = [];
const temporary = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-deletion-defaults-"));
  roots.push(root); return root;
};
const write = (file: string, value: unknown) => {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, typeof value === "string" ? value : JSON.stringify(value), { mode: 0o600 });
};
const age = (file: string) => fs.utimesSync(file, OLD / 1000, OLD / 1000);
const run = (directory: string, actor = false) => {
  write(path.join(directory, "status.json"), { status: "completed", transport: "process", sessionId: "2147483647",
    startedAt: OLD - DAY, finishedAt: OLD, ...(actor ? { actorId: "actor" } : {}) });
  write(path.join(directory, "events.jsonl"), TRACE);
  write(path.join(directory, "reply.json"), '{"text":"complete outcome"}');
  // #1800 checks last filesystem activity across the whole tree, not only the status age.
  for (const name of ["status.json", "events.jsonl", "reply.json"]) age(path.join(directory, name));
  age(directory); return directory;
};
const kept = (directory: string) => expect(fs.readFileSync(path.join(directory, "events.jsonl"), "utf8")).toBe(TRACE);
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("unset trace deletion policies", () => {
  it.each([DEFAULT_FABRIC_CONFIG, normalizeFabricConfig({})])("does not install finite deletion ages for empty configuration", ({ retention }) => {
    for (const key of ["orphanedTempRunMs", "oneShotRunMs", "actorRunArchiveMs", "terminalRunEventsAgeMs", "legacyRunArchiveAgeMs",
      "residentRunMs", "residentChildLogMs", "perCallLedgerDays"] as const) expect(retention[key]).toBeUndefined();
  });

  it.each(POLICIES.flatMap(policy => ["orphan", "one-shot", "closed actor"].map(kind => ({ ...policy, kind }))))(
    "$policy: two-year-old $kind temporary raw tree", ({ explicit, kind }) => {
      const tempRoot = temporary(), root = path.join(tempRoot, "pi-fabric-runs-old");
      const directory = run(path.join(root, "terminal"), kind === "closed actor");
      write(path.join(root, ".fabric-owner.json"), { pid: 2147483647, startedAt: OLD - DAY, heartbeatAt: OLD,
        ...(kind === "orphan" ? { orphanedAt: OLD } : { closedAt: OLD, childrenStopped: true }) });
      age(path.join(root, ".fabric-owner.json")); age(root);
      const { retention } = normalizeFabricConfig({});
      const result = sweepTempRunRoots({ tempRoot, now: NOW,
        orphanedTempRunRetentionMs: explicit ? DAY : retention.orphanedTempRunMs,
        oneShotRunRetentionMs: explicit ? DAY : retention.oneShotRunMs });
      // Match #1800's Linux-only orphan collection expectations; Windows retains this fixture.
      if (explicit && (kind !== "orphan" || process.platform === "linux")) expect(fs.existsSync(directory)).toBe(false);
      else { expect(result).toEqual({ removedRoots: [], removedRuns: [] }); kept(directory); }
    });

  it.skipIf(process.platform !== "linux").each(["dead", "live", "unknown", "dry-run", "unknown content"])(
    "empty config: metadata-only orphan cleanup keeps the trace fence (%s)", kind => {
      const tempRoot = temporary(), root = path.join(tempRoot, "pi-fabric-runs-empty");
      write(path.join(root, ".fabric-owner.json"), { pid: kind === "live" ? process.pid : 2147483647,
        startedAt: OLD - DAY, heartbeatAt: OLD, orphanedAt: OLD });
      if (kind === "unknown content") write(path.join(root, "output.log"), TRACE);
      if (kind === "unknown") vi.spyOn(process, "kill").mockImplementation(() => {
        throw Object.assign(new Error("owner exit unconfirmed"), { code: "EPERM" });
      });
      const ownerBefore = fs.readFileSync(path.join(root, ".fabric-owner.json"), "utf8");
      const result = sweepTempRunRoots({ tempRoot, now: NOW, dryRun: kind === "dry-run" });
      expect(result.removedRoots).toEqual(kind === "dead" ? [root] : []);
      expect(result.removedRuns).toEqual([]);
      if (kind === "dead") expect(fs.existsSync(root)).toBe(false);
      else expect(fs.readFileSync(path.join(root, ".fabric-owner.json"), "utf8")).toBe(ownerBefore);
      if (kind === "unknown content") expect(fs.readFileSync(path.join(root, "output.log"), "utf8")).toBe(TRACE);
      if (kind === "dry-run") expect(result.candidates).toEqual([]);
    });

  it.each(POLICIES)("$policy: two-year-old actor raw archive", ({ explicit }) => {
    const runsDirectory = temporary(), directory = run(path.join(runsDirectory, "old"), true);
    const retentionMs = explicit ? DAY : normalizeFabricConfig({}).retention.actorRunArchiveMs;
    // #1800 contract: actor archives are never deletion candidates, including explicit short policies.
    expect(pruneActorRunArchives({ runsDirectory, now: NOW, retentionMs, dryRun: true })).toEqual([]);
    kept(directory);
    expect(pruneActorRunArchives({ runsDirectory, now: NOW, retentionMs })).toEqual([]);
    kept(directory);
  });

  it.each(POLICIES)("$policy: two-year-old offline resident raw run with retainRuns:false", ({ explicit }) => {
    const runs = temporary(), directory = run(path.join(runs, "old"));
    const removed = sweepResidentRuns(runs, NOW, 10_000, { retainRuns: false, ...(explicit ? { retentionMs: DAY } : {}) });
    expect(removed).toEqual(explicit ? [directory] : []);
    if (explicit) expect(fs.existsSync(directory)).toBe(false); else kept(directory);
  });

  it.each(POLICIES.flatMap(policy => [true, false].map(v2 => ({ ...policy, v2 }))))(
    "$policy: two-year-old streaming resident raw run (v2=$v2)", ({ explicit, v2 }) => {
      vi.spyOn(platform, "retentionV2Enabled").mockReturnValue(v2);
      const root = temporary(), directory = run(path.join(root, "runs", "old"));
      const collector = new ResidentRequestRetention(root, [], { ...normalizeFabricConfig({}).retention,
        retainRuns: false, ...(explicit ? { residentRunMs: DAY } : {}) });
      try { for (let slice = 0; slice < 20 && collector.due(NOW); slice++) collector.sweep(NOW, new Set(), 10_000); }
      finally { collector.close(); }
      if (explicit) expect(fs.existsSync(directory)).toBe(false); else kept(directory);
    });

  it.skipIf(process.platform !== "linux").each(POLICIES)("$policy: retention CLI dry-run plan for two-year-old raw traces", async ({ explicit }) => {
    const root = temporary(), host = path.join(root, "residency", "host"), actor = path.join(root, "actors", "project", "actor");
    write(path.join(host, "host.lock"), { pid: 2147483647 });
    write(path.join(host, "config.json"), { format: 1, rootId: "session:proof", residencyRoot: host, meshRoot: root });
    write(path.join(root, "actors", "project", "actors.json"), { actors: [{ id: "actor", rootId: "session:proof", status: "idle",
      sessionFile: path.join(actor, "session.jsonl"), lastRunId: "latest" }] });
    write(path.join(actor, "session.jsonl"), "session retained");
    const archived = run(path.join(actor, "runs", "old"), true), resident = run(path.join(host, "runs", "old"));
    const plan = await sweepMeshRetention(root, { now: NOW, dryRun: true,
      ...(explicit ? { runRetentionMs: DAY, residentRunRetentionMs: DAY } : {}) });
    expect(plan.removedRuns).toEqual(explicit ? [resident] : []);
    if (!explicit) expect(plan.changes).toEqual([]);
    kept(archived); kept(resident);
  });

  it.each(["closed", "orphan"])("empty config: two-year-old %s full output ignores age and pressure", async kind => {
    const tempRoot = temporary(), output = createScratch("output", tempRoot);
    write(path.join(output, "output.txt"), TRACE); closeScratch(output);
    const marker = path.join(output, SCRATCH_OWNER_FILE), owner = JSON.parse(fs.readFileSync(marker, "utf8"));
    write(marker, { ...owner, createdAt: OLD - DAY, ...(kind === "closed" ? { closedAt: OLD }
      : { pid: 2147483647, closedAt: undefined, orphanedAt: OLD }) });
    expect((await sweepScratch({ tempRoot, now: NOW, maxBytes: 0, maxItems: 0 })).eligible).toEqual([]);
    expect(fs.readFileSync(path.join(output, "output.txt"), "utf8")).toBe(TRACE);
  });

  it("explicit short policy expires full-output scratch", async () => {
    const tempRoot = temporary(), output = createScratch("output", tempRoot);
    write(path.join(output, "output.txt"), TRACE); closeScratch(output);
    const marker = path.join(output, SCRATCH_OWNER_FILE), owner = JSON.parse(fs.readFileSync(marker, "utf8"));
    write(marker, { ...owner, createdAt: OLD - DAY, closedAt: OLD });
    expect((await sweepScratch({ tempRoot, now: NOW, maxAgeMs: DAY, dryRun: true })).eligible).toEqual([output]);
    expect((await sweepScratch({ tempRoot, now: NOW, maxAgeMs: DAY })).removed).toEqual([output]);
  });

  it("empty config keeps full output through normal session-scope disposal", async () => {
    const scope = new ScratchScope(), output = scope.create("output", temporary());
    write(path.join(output, "output.txt"), TRACE); closeScratch(output);
    await scope.close();
    expect(fs.readFileSync(path.join(output, "output.txt"), "utf8")).toBe(TRACE);
  });

  it.each(POLICIES)("$policy: wedge evidence beyond the former five-directory cap", ({ explicit }) => {
    const wedges = temporary(), directories = Array.from({ length: 7 }, (_, index) => path.join(wedges, `2020-01-0${index + 1}`));
    for (const directory of directories) { write(path.join(directory, "child-log-tail"), TRACE); age(directory); }
    write(path.join(wedges, "reports", "report.txt"), "keep reports");
    pruneWedgeEvidence(wedges, explicit ? 5 : undefined);
    expect(fs.readdirSync(wedges).sort()).toEqual([...directories.slice(explicit ? 2 : 0).map(directory => path.basename(directory)), "reports"]);
    for (const directory of directories.slice(explicit ? 2 : 0)) expect(fs.readFileSync(path.join(directory, "child-log-tail"), "utf8")).toBe(TRACE);
  });

  it.each(["expiry", "pressure", "orphan", "first-observed orphan"])(
    "empty config keeps transcript-linked shell output through %s", async kind => {
      const tempRoot = temporary(), jobs = new FabricShellJobStore(tempRoot);
      try {
        const job = jobs.begin("bash", "trace-bearing command");
        job.append(Buffer.from(TRACE));
        const logPath = await job.persistLog();
        const notice = formatShellHangNotice({ elapsedMs: 1000, logPath });
        expect(notice).toContain(logPath);
        job.spill(); await job.finish(0);
        const before = fs.readFileSync(logPath, "utf8"), marker = path.join(path.dirname(logPath), SCRATCH_OWNER_FILE);
        const owner = JSON.parse(fs.readFileSync(marker, "utf8"));
        write(marker, { ...owner, createdAt: OLD - DAY, closedAt: kind.includes("orphan") ? undefined : kind === "pressure" ? NOW - 2 * 3_600_000 : OLD,
          ...(kind.includes("orphan") ? { pid: 2147483647, ...(kind === "orphan" ? { orphanedAt: OLD } : {}) } : {}) });
        const markerBefore = fs.readFileSync(marker, "utf8");
        expect(await sweepScratch({ tempRoot, now: NOW, ...(kind === "pressure" ? { maxBytes: 0, maxItems: 0 } : {}) }))
          .toEqual({ eligible: [], removed: [], orphaned: [] });
        expect(before).toContain(TRACE);
        expect(fs.readFileSync(logPath, "utf8")).toBe(before);
        expect(fs.readFileSync(marker, "utf8")).toBe(markerBefore);
      } finally { await jobs.close(); }
    });

  it("empty config keeps transcript-linked shell output through shell-store scope release", async () => {
    const jobs = new FabricShellJobStore(temporary());
    try {
      const job = jobs.begin("bash", "trace-bearing command"); job.append(Buffer.from(TRACE));
      const logPath = await job.persistLog(), notice = formatShellHangNotice({ elapsedMs: 1000, logPath });
      job.spill(); await job.finish(0);
      const before = fs.readFileSync(logPath, "utf8");
      await jobs.close();
      expect(notice).toContain(logPath);
      expect(before).toContain(TRACE);
      expect(fs.readFileSync(logPath, "utf8")).toBe(before);
    } finally { await jobs.close(); }
  });

  it.each(["expiry", "pressure", "orphan", "scope release"])("explicit short policy still prunes shell output through %s", async kind => {
    const tempRoot = temporary(), directory = createScratch("shell", tempRoot);
    write(path.join(directory, "output.log"), TRACE); closeScratch(directory);
    const marker = path.join(directory, SCRATCH_OWNER_FILE), owner = JSON.parse(fs.readFileSync(marker, "utf8"));
    write(marker, { ...owner, createdAt: OLD - DAY, closedAt: kind === "orphan" ? undefined : kind === "expiry" ? OLD : NOW - 2 * 3_600_000,
      ...(kind === "orphan" ? { pid: 2147483647, orphanedAt: OLD } : {}) });
    const options = { tempRoot, now: NOW, maxAgeMs: DAY, ...(kind === "pressure" ? { maxBytes: 0, maxItems: 0 } : {}),
      ...(kind === "scope release" ? { released: new Set([directory]) } : {}) };
    expect((await sweepScratch({ ...options, dryRun: true })).eligible).toEqual([directory]);
    expect(fs.readFileSync(path.join(directory, "output.log"), "utf8")).toBe(TRACE);
    expect((await sweepScratch(options)).removed).toEqual([directory]);
    expect(fs.existsSync(directory)).toBe(false);
  });

  it.each(POLICIES)("$policy: two-year-old resident child log", ({ explicit }) => {
    const root = temporary(), file = path.join(root, "child-old.log"); write(file, TRACE); age(file);
    pruneResidentChildLogs(root, path.join(root, "child-current.log"), NOW, explicit ? DAY : undefined);
    if (explicit) expect(fs.existsSync(file)).toBe(false); else expect(fs.readFileSync(file, "utf8")).toBe(TRACE);
  });

  it("empty config does not overwrite resident child logs at the old default size cap", () => {
    const root = temporary(), file = path.join(root, "child.log"), bytes = Buffer.alloc(RESIDENT_LOG_MAX_BYTES + 1, 120);
    appendResidentLog(file, bytes); appendResidentLog(file, TRACE);
    expect(fs.statSync(file).size).toBe(bytes.length + Buffer.byteLength(TRACE));
    expect(fs.existsSync(`${file}.1`)).toBe(false);
  });

  it("an explicit resident log byte cap still rotates", () => {
    const root = temporary(), file = path.join(root, "child.log");
    appendResidentLog(file, "first payload"); appendResidentLog(file, "next", 5);
    expect(fs.readFileSync(`${file}.1`, "utf8")).toBe("first payload");
    expect(fs.readFileSync(file, "utf8")).toBe("next");
  });

  it.each(POLICIES)("$policy: daily per-call rotation of a two-year-old ledger", ({ explicit }) => {
    const root = temporary(), file = path.join(root, "per-call-routing.jsonl"); write(file, TRACE); age(file);
    rotatePerCallLedger(file, new Date(NOW), explicit ? 1 : undefined);
    const history = fs.readdirSync(root);
    if (explicit) expect(history).toEqual([]);
    else { expect(history).toHaveLength(1); expect(fs.readFileSync(path.join(root, history[0]!), "utf8")).toBe(TRACE); }
  });
});
