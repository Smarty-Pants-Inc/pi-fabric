import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { readJsonlPage } from "../src/log-tail.js";
import { ResidentRequestRetention, sweepResidentRuns } from "../src/residency/retention.js";
import { MeshRetentionApplyDisabledError, sweepMeshRetention } from "../src/storage/retention-cli.js";
import {
  FABRIC_RUN_ROOT_PREFIX, markRunRootActive, markRunRootClosed,
  pruneActorRunArchives, sweepTempRunRoots,
} from "../src/storage/retention.js";

const roots: string[] = [];
const HOUR = 3_600_000;
const now = 10 * HOUR;
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Data safety: exercise real retention entry points and the same paged file reader
// used by agents.logs, not an internal compaction mock or a source-text assertion.
describe("raw terminal run trace preservation", () => {
  it.each(["status.json.attempt-2-startup-retry", "lifecycle.jsonl.attempt-2-startup-retry-3",
    "status.json.attempt-1-resume-1", "lifecycle.jsonl.attempt-1-resume"])(
    "keeps a tree containing only %s as its permanent trace fence through resident and apply collection", async trace => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-attempt-retention-"));
      roots.push(root);
      const aged = Date.now() - 3 * 24 * HOUR, now = Date.now();
      const bytes = Buffer.from([0, 255, 13, 10, 123, 0]);
      const runs = path.join(root, "runs"), retained = path.join(runs, "retained"), control = path.join(runs, "control");
      for (const run of [retained, control]) {
        fs.mkdirSync(run, { recursive: true });
        fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", finishedAt: aged,
          transport: "process", sessionId: "2147483647" }));
      }
      fs.writeFileSync(path.join(retained, trace), bytes);
      const assertRetained = () => expect(fs.readFileSync(path.join(retained, trace))).toEqual(bytes);
      const ageRuns = () => { for (const run of [retained, control]) fs.utimesSync(run, aged / 1000, aged / 1000); };
      ageRuns();
      const collector = new ResidentRequestRetention(root, [], { retainRuns: false });
      try { for (let slice = 0; slice < 20 && collector.due(now); slice++) collector.sweep(now, new Set(), 10_000); }
      finally { collector.close(); }
      expect(fs.existsSync(control), "resident sweep must actually collect the unfenced control").toBe(false);
      assertRetained();

      // Exercise both collection entry points used by the operator's apply path.
      fs.mkdirSync(control);
      fs.copyFileSync(path.join(retained, "status.json"), path.join(control, "status.json"));
      ageRuns();
      expect(sweepResidentRuns(runs, now, 10_000, { retainRuns: false })).toEqual([control]);
      assertRetained();
      fs.mkdirSync(control);
      fs.copyFileSync(path.join(retained, "status.json"), path.join(control, "status.json"));
      expect(pruneActorRunArchives({ runsDirectory: runs, retentionMs: HOUR, now })).toEqual([control]);
      assertRetained();
      // Public --apply remains disabled; do not bypass its independent safety boundary.
      await expect(sweepMeshRetention(root, { dryRun: false })).rejects.toBeInstanceOf(MeshRetentionApplyDisabledError);
      assertRetained();
    },
  );

  // Judge prototype: the fence must also hold when the only attempt trace is in a nested child run.
  it("keeps a run whose only attempt trace is in a nested child run through resident and apply collection", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-attempt-retention-nested-"));
    roots.push(root);
    const aged = Date.now() - 3 * 24 * HOUR, now = Date.now();
    const bytes = Buffer.from([0, 255, 13, 10, 123, 0]);
    const trace = "lifecycle.jsonl.attempt-1-resume";
    const runs = path.join(root, "runs"), retained = path.join(runs, "retained"), control = path.join(runs, "control");
    const record = JSON.stringify({ status: "completed", finishedAt: aged, transport: "process", sessionId: "2147483647" });
    const build = (run: string, fenced: boolean) => {
      const child = path.join(run, "nested", "child");
      fs.mkdirSync(child, { recursive: true });
      fs.writeFileSync(path.join(child, "status.json"), record);
      if (fenced) fs.writeFileSync(path.join(child, trace), bytes);
      fs.writeFileSync(path.join(run, "status.json"), record);
      for (const directory of [child, path.join(run, "nested"), run]) fs.utimesSync(directory, aged / 1000, aged / 1000);
    };
    const assertRetained = () => expect(fs.readFileSync(path.join(retained, "nested", "child", trace))).toEqual(bytes);
    build(retained, true); build(control, false);
    const collector = new ResidentRequestRetention(root, [], { retainRuns: false });
    try { for (let slice = 0; slice < 20 && collector.due(now); slice++) collector.sweep(now, new Set(), 10_000); }
    finally { collector.close(); }
    expect(fs.existsSync(control), "resident sweep must actually collect the unfenced nested control").toBe(false);
    assertRetained();
    build(control, false);
    expect(sweepResidentRuns(runs, now, 10_000, { retainRuns: false })).toEqual([control]);
    assertRetained();
    build(control, false);
    expect(pruneActorRunArchives({ runsDirectory: runs, retentionMs: HOUR, now })).toEqual([control]);
    assertRetained();
  });

  it.each(["actor", "closed root"] as const)("keeps every head byte and the paging generation through %s maintenance", kind => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-raw-trace-"));
    roots.push(root);
    const runs = path.join(root, kind === "actor" ? "runs" : FABRIC_RUN_ROOT_PREFIX + "closed");
    const directory = path.join(runs, "old");
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ status: "completed",
      finishedAt: 4 * HOUR, transport: "process", sessionId: "2147483647" }));
    const file = path.join(directory, "events.jsonl");
    const original = Buffer.from(Array.from({ length: 401 }, (_, sequence) =>
      JSON.stringify({ sequence, text: sequence === 0 ? "RAW-TRACE-HEAD-🙂" : "event" }) + "\n").join(""));
    fs.writeFileSync(file, original);
    const lastPage = readJsonlPage(file, 100);
    if (kind === "closed root") {
      markRunRootActive(runs, 4 * HOUR);
      markRunRootClosed(runs, 4 * HOUR, true);
    }
    const maintain = () => kind === "actor"
      ? pruneActorRunArchives({ runsDirectory: runs, retentionMs: 24 * HOUR, now })
      : sweepTempRunRoots({ tempRoot: root, orphanedTempRunRetentionMs: 24 * HOUR,
        oneShotRunRetentionMs: 24 * HOUR, now });
    maintain();
    const after = fs.readFileSync(file);
    expect(after.includes(Buffer.from("RAW-TRACE-HEAD-🙂")), "retention dropped raw trace head bytes").toBe(true);
    expect(after.equals(original), "retention changed raw trace bytes").toBe(true);
    let page = readJsonlPage(file, 100, lastPage.before, undefined, lastPage.generation);
    const lines = [...lastPage.lines];
    for (let remaining = 4; remaining > 0; remaining--) {
      lines.unshift(...page.lines);
      if (!page.hasMore) break;
      page = readJsonlPage(file, 100, page.before, undefined, page.generation);
    }
    expect(lines.map(line => (line.parsed as { sequence: number }).sequence))
      .toEqual(Array.from({ length: 401 }, (_, sequence) => sequence));
    maintain();
    expect(fs.readFileSync(file)).toEqual(original);
  });
});
