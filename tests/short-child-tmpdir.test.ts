import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentAdmissionAlarm, AgentRunRequest } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import { canRemoveTerminalRun, markRunRootActive, markRunRootClosed, sweepTempRunRoots } from "../src/storage/retention.js";

// smarty-dev#15878: the fleet bash guard binds `<canonical TMPDIR>/p.XXXXXX` and blocks the whole session's Bash when
// that needs more than 60 UTF-8 bytes (setup/pi/bash-guard.ts:476,526-537). A native-pi Main's TMPDIR is
// /tmp/p.XXXXXX, so the old child TMPDIR `<parent>/pi-fabric-runs-XXXXXX/<32 hex>/tmp` needed 81 bytes.
// Fixtures stay inside this test's private temp: each real parent prefix (/tmp, /srv/scratch/paul) is played by a
// directory here, and the guard budget moves by exactly the byte difference, so lengths match the real paths.
const GUARD_BYTES = 60;
const guardBytes = (directory: string): number => Buffer.byteLength(`${fs.realpathSync(directory)}/p.XXXXXX`, "utf8");
const shortName = (id: string): string => `r.${createHash("sha256").update(id).digest("hex").slice(0, 10)}`;
const inside = (parent: string, child: string): boolean => {
  const relative = path.relative(parent, child);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
};

const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});

type Layout = "native-pi /tmp/p.XXXXXX" | "role-pi /srv/scratch/paul/smarty-pi-tmp/pi-<12hex>/p.XXXXXX";
const LAYOUTS: Layout[] = ["native-pi /tmp/p.XXXXXX", "role-pi /srv/scratch/paul/smarty-pi-tmp/pi-<12hex>/p.XXXXXX"];

const fixture = (layout: Layout, { nothingFits = false, retainRuns = false, sourceFails = false } = {}) => {
  const alarms: AgentAdmissionAlarm[] = [];
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-short-tmpdir-")));
  const cwd = path.join(root, "cwd");
  fs.mkdirSync(cwd);
  const role = layout.startsWith("role-pi");
  const realPrefix = role ? "/srv/scratch/paul" : "/tmp";
  const prefix = path.join(root, role ? "s" : "t");
  fs.mkdirSync(prefix, { mode: 0o700 });
  const storeRoot = role ? path.join(prefix, "smarty-pi-tmp") : undefined;
  const sessionStore = storeRoot ? path.join(storeRoot, "pi-0123456789ab") : undefined;
  const parent = sessionStore ? path.join(sessionStore, "p.AbCdEf") : path.join(prefix, "p.mN2CJ9");
  for (const directory of [storeRoot, sessionStore, parent]) if (directory) fs.mkdirSync(directory, { mode: 0o700 });
  // Same nesting as production: the old child TMPDIR was <parent>/pi-fabric-runs-XXXXXX/<run id>/tmp.
  const runRoot = path.join(parent, "pi-fabric-runs-dVm8g4");
  const offset = Buffer.byteLength(prefix, "utf8") - Buffer.byteLength(realPrefix, "utf8");
  // nothingFits: one byte short of the shortest candidate, `<storeRoot or parent>/r.<10 hex>/p.XXXXXX`.
  const budget = nothingFits ? Buffer.byteLength(storeRoot ?? parent, "utf8") + 1 + 12 + 9 - 1 : GUARD_BYTES + offset;
  const worker = path.join(root, "worker.mjs");
  fs.writeFileSync(worker, `import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
// Mirror worker.ts exporting --tmpdir to its execution child's environment.
if (args.has('--tmpdir')) for (const key of ['TMPDIR', 'TMP', 'TEMP']) process.env[key] = args.get('--tmpdir');
const tmp = os.tmpdir();
const stat = fs.statSync(tmp);
const probe = path.join(tmp, 'probe.txt');
fs.writeFileSync(probe, 'probe');
const now = Date.now();
fs.writeFileSync(args.get('--status-file'), JSON.stringify({
  id: args.get('--id'), name: args.get('--name'), task: 'probe', status: 'completed',
  runner: args.get('--runner'), transport: 'process', sessionId: String(process.pid),
  cwd: args.get('--cwd'), startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0,
  text: JSON.stringify({ tmp, env: process.env.TMPDIR, argTmpdir: args.get('--tmpdir') ?? null,
    policy: JSON.parse(args.get('--write-policy') ?? 'null'), mode: stat.mode & 0o777, uid: stat.uid, probe }),
  exitCode: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }
}));`);
  const options = {
    workerPath: worker, runRoot, fullCodeMode: false,
    shortTmpdirSource: sourceFails ? () => { throw new Error("source unavailable"); }
      : () => ({ tmpdir: parent, ...(sessionStore ? { sessionStore } : {}), budgetBytes: budget }),
    onAdmissionAlarm: (alarm: AgentAdmissionAlarm) => { alarms.push(alarm); },
  };
  const manager = new AgentManager(cwd, {
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 8_000, retainRuns,
  }, options);
  managers.push(manager);
  return { root, cwd, parent, storeRoot, runRoot, offset, budget, manager, alarms };
};
type Fixture = ReturnType<typeof fixture>;

const report = async (f: Fixture, request: Partial<AgentRunRequest> = {}) => {
  const result = await f.manager.run({ task: "probe", transport: "process", ...request });
  expect(result.status, result.error).toBe("completed");
  const run = f.manager.runDirectory(result.id)!;
  const value = JSON.parse(result.text) as { tmp: string; env: string; argTmpdir: string | null;
    policy: { readOnly: boolean; writableRoots: string[]; shell: string } | null; mode: number; uid: number; probe: string };
  return { result, run, value };
};
const events = (run: string): { type: string; warning?: string; code?: string; level?: string }[] => fs.readFileSync(path.join(run, "events.jsonl"), "utf8")
  .trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
// pi-fabric#1554 P2-4: the named warning event of a child launched without a short TMPDIR.
const UNAVAILABLE = "tmpdir.short_unavailable";

describe.skipIf(process.platform === "win32")("short private child TMPDIR (smarty-dev#15878)", () => {
  describe.each(LAYOUTS)("under a %s Main", layout => {
    const expectShort = (f: Fixture, id: string, run: string, tmp: string): void => {
      // (a) The guard's p.XXXXXX child fits 60 bytes at the real parent's length.
      expect(guardBytes(tmp) - f.offset).toBeLessThanOrEqual(GUARD_BYTES);
      expect(guardBytes(tmp)).toBeLessThanOrEqual(f.budget);
      expect(path.basename(tmp)).toBe(shortName(id));
      expect(path.dirname(tmp)).toBe(f.storeRoot ?? f.parent);
      expect(inside(run, tmp)).toBe(false);
    };

    it("gives a process task a short 0700 TMPDIR inside its write grant and removes it with the run", async () => {
      const f = fixture(layout);
      const { result, run, value } = await report(f);
      expectShort(f, result.id, run, value.tmp);
      expect(value.env).toBe(value.tmp);
      expect(value.argTmpdir).toBe(value.tmp);
      // (b) Exact 0700, ours.
      expect(value.mode).toBe(0o700);
      if (typeof process.getuid === "function") expect(value.uid).toBe(process.getuid());
      // (c) Granted: the default task policy is exactly [cwd, its TMPDIR], and the child wrote there.
      expect(value.policy).toEqual({ readOnly: false, writableRoots: [f.cwd, value.tmp].sort(), shell: "unconfined" });
      expect(fs.readFileSync(value.probe, "utf8")).toBe("probe");
      expect(result.temporaryDirectory).toBe(value.tmp);
      expect(events(run).filter(event => event.type === UNAVAILABLE)).toEqual([]);
      expect(f.alarms).toEqual([]);
      // (b) Removed with the run.
      await f.manager.cleanup(result.id);
      expect(fs.existsSync(run)).toBe(false);
      expect(fs.existsSync(value.tmp)).toBe(false);
    });

    it("gives an actor run a short 0700 TMPDIR as its only write grant and removes it with the run", async () => {
      const f = fixture(layout);
      const { result, run, value } = await report(f, { runner: "pi", actorId: "actor" });
      expectShort(f, result.id, run, value.tmp);
      expect(value.env).toBe(value.tmp);
      expect(value.mode).toBe(0o700);
      // (c) Actor writes are private scratch only.
      expect(value.policy).toEqual({ readOnly: false, writableRoots: [value.tmp], shell: "unconfined" });
      expect(fs.readFileSync(value.probe, "utf8")).toBe("probe");
      await f.manager.cleanup(result.id);
      expect(fs.existsSync(run)).toBe(false);
      expect(fs.existsSync(value.tmp)).toBe(false);
    });

    it("falls back to the old run-tree TMPDIR when nothing fits (no lockout), with a named warning and one operator alarm", async () => {
      const f = fixture(layout, { nothingFits: true });
      const warn = vi.spyOn(diagnostics, "warn");
      const ids: string[] = [];
      for (const request of [{}, { runner: "pi" as const, actorId: "actor" }]) {
        // Launched and completed, exactly as before this fix: never refused.
        const { result, run, value } = await report(f, request);
        ids.push(result.id);
        expect(value.tmp).toBe(path.join(run, "tmp"));
        expect(value.mode).toBe(0o700);
        expect(value.policy?.writableRoots).toContain(value.tmp);
        expect(result.temporaryDirectory).toBeUndefined();
        // Every such run carries one named warning event in its own log.
        const warnings = events(run).filter(event => event.type === UNAVAILABLE);
        expect(warnings).toHaveLength(1);
        expect(warnings[0]).toMatchObject({ level: "warning", code: "FABRIC_SHORT_TMPDIR_UNAVAILABLE", budgetBytes: f.budget });
        expect(warnings[0]!.warning).toContain(`run ${result.id}`);
        expect(warnings[0]!.warning).toContain(`over its ${f.budget}`);
        expect(warnings[0]!.warning).toContain("smarty-dev#15878");
        expect(fs.readdirSync(f.storeRoot ?? f.parent).filter(name => name.startsWith("r."))).toEqual([]);
        await f.manager.cleanup(result.id);
        expect(fs.existsSync(run)).toBe(false);
      }
      // The operator alarm: one stderr diagnostics line and one manager alarm naming the first run, not one per spawn.
      expect(f.alarms).toEqual([expect.objectContaining({ kind: "short-tmpdir-unavailable", runId: ids[0],
        message: expect.stringContaining("FABRIC_SHORT_TMPDIR_UNAVAILABLE") })]);
      expect(warn.mock.calls.filter(([, line]) => String(line).includes("FABRIC_SHORT_TMPDIR_UNAVAILABLE"))).toHaveLength(1);
      // Main publishes it to its owner on the manager's alarm topic, beside the held-permit alarm, under its own kind.
      const { admissionAlarmEnvelope } = await import("../src/topology/stall-alarms.js");
      expect(admissionAlarmEnvelope(f.alarms[0]!)).toEqual({ topic: "fabric.alarm.agent-admission", kind: "short-tmpdir-unavailable" });
      expect(admissionAlarmEnvelope({})).toEqual({ topic: "fabric.alarm.agent-admission", kind: "agent-admission-held" });
      expect(admissionAlarmEnvelope({ kind: "recovered-answer-save-failed" })).toEqual({ topic: "ops.owner", kind: "actor.alarm" });
    });

    it("returns the no-fit TMPDIR warning to the caller and status while the child launches", async () => {
      const f = fixture(layout, { nothingFits: true });
      const { result } = await report(f, { runner: "pi", actorId: "actor" });
      const tmpdirWarning = expect.stringContaining("tmpdir: short private TMPDIR unavailable (");
      expect(result.status).toBe("completed");
      expect(result.warnings).toEqual(expect.arrayContaining([tmpdirWarning]));
      expect(f.manager.status(result.id)).toMatchObject({ status: "completed", warnings: expect.arrayContaining([tmpdirWarning]) });
      await f.manager.cleanup(result.id);
    });

    it("falls back the same way when the TMPDIR source itself fails", async () => {
      const f = fixture(layout, { sourceFails: true });
      const { result, run, value } = await report(f);
      expect(value.tmp).toBe(path.join(run, "tmp"));
      const warnings = events(run).filter(event => event.type === UNAVAILABLE);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]!.warning).toContain("its TMPDIR source failed: source unavailable");
      expect(f.alarms).toEqual([expect.objectContaining({ kind: "short-tmpdir-unavailable", runId: result.id })]);
      await f.manager.cleanup(result.id);
    });
  });

  it("retention removes a retained run's short TMPDIR only with its run tree", async () => {
    const f = fixture("native-pi /tmp/p.XXXXXX", { retainRuns: true });
    const { result, run, value } = await report(f);
    expect(path.basename(value.tmp)).toBe(shortName(result.id));
    await f.manager.close();
    // Retained runs keep their short TMPDIR; the run's status records it for the sweep.
    expect(fs.existsSync(value.tmp)).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8")).temporaryDirectory).toBe(value.tmp);
    markRunRootActive(f.runRoot, 1);
    markRunRootClosed(f.runRoot, 1, true);
    const swept = sweepTempRunRoots({ tempRoot: f.parent, now: Date.now() + 1_000, orphanedTempRunRetentionMs: 0, oneShotRunRetentionMs: 0 });
    expect(swept).toEqual({ removedRuns: [run], removedRoots: [f.runRoot] });
    expect(fs.existsSync(value.tmp)).toBe(false);
  });

  // Custody unchanged: unsafe or unbounded scratch vetoes collection exactly as the in-tree tmp did.
  it.each(["root-link", "content-link", "hardlink", "over-limit"] as const)("retention retains a run whose short TMPDIR has a %s", async unsafe => {
    const f = fixture("native-pi /tmp/p.XXXXXX", { retainRuns: true });
    const { result, run, value } = await report(f);
    expect(path.basename(value.tmp)).toBe(shortName(result.id));
    await f.manager.close();
    markRunRootActive(f.runRoot, 1);
    markRunRootClosed(f.runRoot, 1, true);
    const target = path.join(f.root, "outside-owned.txt");
    fs.writeFileSync(target, "must survive");
    if (unsafe === "root-link") {
      fs.renameSync(value.tmp, path.join(f.root, "parked-scratch"));
      fs.symlinkSync(f.root, value.tmp);
    } else if (unsafe === "content-link") fs.symlinkSync(target, path.join(value.tmp, "escape"));
    else if (unsafe === "hardlink") fs.linkSync(target, path.join(value.tmp, "alias"));
    else for (let index = 0; index < 65; index++) fs.writeFileSync(path.join(value.tmp, `scratch-${index}`), "bounded");
    expect(canRemoveTerminalRun(run)).toBe(false);
    expect(sweepTempRunRoots({ tempRoot: f.parent, now: Date.now() + 1_000, orphanedTempRunRetentionMs: 0, oneShotRunRetentionMs: 0 }))
      .toEqual({ removedRuns: [], removedRoots: [] });
    expect(fs.existsSync(run)).toBe(true);
    expect(fs.lstatSync(value.tmp).isSymbolicLink()).toBe(unsafe === "root-link");
    expect(fs.readFileSync(target, "utf8")).toBe("must survive");
  });

  // pi-fabric#1554 P1: a failed, stopped or timed-out run in a managed (sweep-owned) temp root keeps only its prompt
  // record at close. Before the short TMPDIR, that pruning removed <run>/tmp; on fd533e62 the short TMPDIR outside the
  // run tree stayed, and with more than 64 entries every later sweep refused the run (and its root) forever.
  it("prunes a retained failed run's short TMPDIR of 70 entries at close, so the sweep collects the run and its root", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-short-retained-")));
    const cwd = path.join(root, "cwd");
    fs.mkdirSync(cwd);
    const parent = path.join(root, "p.mN2CJ9");
    fs.mkdirSync(parent, { mode: 0o700 });
    const worker = path.join(root, "worker.mjs");
    fs.writeFileSync(worker, `import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
if (args.has('--tmpdir')) for (const key of ['TMPDIR', 'TMP', 'TEMP']) process.env[key] = args.get('--tmpdir');
const tmp = os.tmpdir();
for (let i = 0; i < 70; i++) fs.writeFileSync(path.join(tmp, 'scratch-' + i), 'x');
const now = Date.now();
fs.writeFileSync(args.get('--status-file'), JSON.stringify({
  id: args.get('--id'), name: args.get('--name'), task: 'probe', status: 'failed', error: 'probe failure',
  runner: args.get('--runner'), transport: 'process', sessionId: String(process.pid),
  cwd: args.get('--cwd'), startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0,
  text: JSON.stringify({ tmp }), exitCode: 1, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }
}));`);
    // A managed temp root under the caller's TMPDIR: no explicit run root.
    vi.stubEnv("TMPDIR", parent);
    vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined);
    const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 8_000 },
      { workerPath: worker, fullCodeMode: false, shortTmpdirSource: () => ({ tmpdir: parent, budgetBytes: 4096 }) });
    const result = await manager.run({ task: "probe", transport: "process" });
    expect(result.status).toBe("failed");
    const tmp = (JSON.parse(result.text) as { tmp: string }).tmp;
    const run = manager.runDirectory(result.id)!;
    expect(tmp).toBe(path.join(parent, shortName(result.id)));
    expect(fs.readdirSync(tmp)).toHaveLength(70);
    await manager.close();
    vi.unstubAllEnvs();
    // The retained prompt record stays; the short TMPDIR went with the pruned artifacts.
    expect(fs.readdirSync(run).sort()).toEqual(["manager-result.json", "status.json", "task.txt"]);
    expect(fs.existsSync(tmp)).toBe(false);
    // So retention no longer refuses the run: it collects the record and then the empty root.
    const swept = sweepTempRunRoots({ tempRoot: parent, now: Date.now() + 365 * 86_400_000, orphanedTempRunRetentionMs: 0, oneShotRunRetentionMs: 0 });
    expect(swept.removedRuns).toEqual([run]);
    expect(swept.removedRoots).toEqual([path.dirname(run)]);
    expect(fs.existsSync(run)).toBe(false);
  });

  it("measures the real native-pi and role-pi parents against the guard's 60 bytes", async () => {
    const { shortTmpdirCandidates } = await import("../src/agents/short-tmpdir.js");
    const id = "c19c74d740c74655ab0d972e38683c4e";
    const native = shortTmpdirCandidates(id, { tmpdir: "/tmp/p.mN2CJ9" });
    expect(native[0]).toEqual({ directory: `/tmp/p.mN2CJ9/${shortName(id)}`, bytes: 35, fits: true });
    const store = "/srv/scratch/paul/smarty-pi-tmp/pi-0123456789ab";
    const role = shortTmpdirCandidates(id, { tmpdir: `${store}/p.AbCdEf`, sessionStore: store });
    expect(role.map(candidate => [candidate.bytes, candidate.fits])).toEqual([[78, false], [53, true]]);
    expect(role[1]!.directory).toBe(`/srv/scratch/paul/smarty-pi-tmp/${shortName(id)}`);
    // A session store that does not hold the caller's TMPDIR is never used.
    expect(shortTmpdirCandidates(id, { tmpdir: "/tmp/p.mN2CJ9", sessionStore: store })).toHaveLength(1);
  });
});
