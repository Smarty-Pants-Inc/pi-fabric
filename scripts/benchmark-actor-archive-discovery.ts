#!/usr/bin/env bun
/** Real AgentManager discovery, with retained, private fixtures. Never calls cleanup/close or deletes traces.
 * Run before/after against the SAME --root; stdout is the raw per-sample timing receipt.
 * bun scripts/benchmark-actor-archive-discovery.ts --root "$TMPDIR/archive-bench" --label before
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const args = process.argv.slice(2);
const option = (name: string, fallback: string) => {
  const index = args.indexOf(`--${name}`);
  return index < 0 ? fallback : args[index + 1]!;
};
const entries = Number(option("entries", "36427")), samples = Number(option("samples", "3"));
assert.ok(Number.isSafeInteger(entries) && entries >= 16);
assert.ok(Number.isSafeInteger(samples) && samples > 0);
const root = path.resolve(option("root", "") || fs.mkdtempSync(path.join(os.tmpdir(), "actor-archive-discovery-")));
const label = option("label", "candidate"), runRoot = path.join(root, "runs");
const actorId = "archive-discovery-target", sessionFile = path.join(root, "actors", actorId, "session.jsonl");
const manifestFile = path.join(root, "fixture.json");
const expected = Array.from({ length: 16 }, (_, i) => (i + 1).toString(16).padStart(32, "0"));
if (!fs.existsSync(manifestFile)) {
  assert.ok(!fs.existsSync(runRoot), "refuse to seed an existing run root without its manifest");
  fs.mkdirSync(runRoot, { recursive: true, mode: 0o700 });
  for (let i = 1; i <= entries; i++) {
    const runId = i.toString(16).padStart(32, "0"), directory = path.join(runRoot, runId);
    fs.mkdirSync(directory, { mode: 0o700 });
    // Every entry has a custody marker: both discovery and bounded marker reads are exercised.
    fs.writeFileSync(path.join(directory, "actor-run-archive-pending.json"), JSON.stringify({ format: 1, runId,
      actorId: i <= expected.length ? actorId : "another-actor", sessionFile }), { mode: 0o600, flag: "wx" });
    if (i <= expected.length) fs.writeFileSync(path.join(directory, "events.jsonl"), `{"benchmarkSentinel":${i}}\n`, { mode: 0o600, flag: "wx" });
  }
  fs.writeFileSync(manifestFile, JSON.stringify({ format: 1, entries, actorId, sessionFile, expected }), { mode: 0o600, flag: "wx" });
}
assert.deepEqual(JSON.parse(fs.readFileSync(manifestFile, "utf8")), { format: 1, entries, actorId, sessionFile, expected });
assert.equal(fs.readdirSync(runRoot).length, entries);
const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, budgetUsd: 0 }, { runRoot });
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
console.log(JSON.stringify({ type: "fixture", label, root, runRoot, entries, markerEntries: entries,
  matchingEntries: expected.length, samples, pid: process.pid, runtime: process.version, bun: process.versions.bun,
  entryPoint: "AgentManager.actorArchiveSources", retained: true, at: new Date().toISOString() }));
for (let sample = 0; sample < samples; sample++) {
  const histogram = monitorEventLoopDelay({ resolution: 5 });
  histogram.enable();
  await pause(30);
  const ticks: { atMs: number; delayMs: number }[] = [];
  let previous = performance.now();
  const timer = setInterval(() => {
    const now = performance.now(); ticks.push({ atMs: now - started, delayMs: Math.max(0, now - previous - 5) }); previous = now;
  }, 5);
  const cpu = process.cpuUsage(), started = performance.now();
  let firstTurnMs = 0;
  const firstTurn = new Promise<void>(resolve => setImmediate(() => { firstTurnMs = performance.now() - started; resolve(); }));
  // Await supports both the baseline synchronous API and its asynchronous replacement; the actual method is never copied.
  const discovery = manager.actorArchiveSources(actorId, sessionFile);
  const callReturnedMs = performance.now() - started;
  const sources = await discovery;
  const completedMs = performance.now() - started, cpuMicros = process.cpuUsage(cpu);
  await firstTurn;
  await pause(20);
  clearInterval(timer); histogram.disable();
  assert.deepEqual([...sources.keys()].sort(), [...expected].sort());
  for (const [runId, directory] of sources) {
    assert.equal(directory, path.join(runRoot, runId));
    assert.ok(fs.existsSync(path.join(directory, "events.jsonl")));
    assert.ok(fs.existsSync(path.join(directory, "actor-run-archive-pending.json")));
  }
  console.log(JSON.stringify({ type: "sample", label, sample, callReturnedMs, completedMs, firstTurnMs,
    maxTickDelayMs: Math.max(0, ...ticks.map(tick => tick.delayMs)), eventLoopMaxMs: histogram.max / 1e6,
    eventLoopP99Ms: histogram.percentile(99) / 1e6, cpuMicros, found: sources.size, ticks }));
}
// No close: it is a custody/collection entry point, not part of this read-only benchmark.
