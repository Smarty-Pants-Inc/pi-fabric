import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";

// Keep: data safety at the real host/incremental-reference/collector boundary,
// from the resident startup/retention incident (smarty-dev#4796). Product
// mutation and fixture cleanup stay inside directories created by these tests.
const roots: string[] = [];
const hosts: ResidentHost[] = [];
afterEach(async () => {
  for (const host of hosts.splice(0)) await host.close();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const DAY = 86_400_000;
const root = () => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "runsboot-retention-"));
  roots.push(value);
  return value;
};
const seed = (runs: string, id: string) => {
  const run = path.join(runs, id), old = Date.now() - 4 * DAY;
  fs.mkdirSync(run, { recursive: true });
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647", startedAt: old - 1, finishedAt: old }));
  fs.writeFileSync(path.join(run, "events.jsonl"), (JSON.stringify({ text: "x".repeat(1000) }) + "\n").repeat(300));
  fs.utimesSync(run, old / 1000, old / 1000);
  return run;
};

// POSIX uses the progressing cursor; Windows keeps its existing legacy path.
describe.skipIf(process.platform === "win32")("incremental resident retention admission", () => {
  it("the real host prunes and compacts nothing during partial preparation, then admits a completed snapshot", async () => {
    const base = root(), residencyRoot = path.join(base, "resident"), runs = path.join(residencyRoot, "runs");
    for (let index = 0; index < 200; index++) seed(runs, index.toString(16).padStart(32, "0"));
    const config: ResidentHostConfig = {
      format: 1, rootId: "session:runsboot-retention", sessionId: "runsboot-retention", cwd: base, projectRoot: base,
      meshRoot: path.join(base, "mesh"), actorRoot: path.join(base, "actors"), residencyRoot, fullCodeMode: false,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, residentIdleExitMs: 60_000 },
      retention: { ...DEFAULT_FABRIC_CONFIG.retention, legacyRunArchiveEnabled: false },
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("src/index.ts"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    };
    fs.writeFileSync(path.join(residencyRoot, "config.json"), JSON.stringify(config));
    const references = AgentManager.prototype.retentionReferences;
    let slices = 0;
    // Force a deterministic suspended real cursor, rather than relying on how
    // many filesystem calls happen to fit into 2 ms on a particular machine.
    vi.spyOn(AgentManager.prototype, "retentionReferences").mockImplementation(function (this: AgentManager, options) {
      slices++;
      return references.call(this, { ...options, maxEntries: 1, budgetMs: 10_000 });
    });
    const sweep = vi.spyOn(ResidentRequestRetention.prototype, "sweep");
    const sample = path.join(runs, "0".repeat(32), "events.jsonl"), before = fs.readFileSync(sample);
    const host = new ResidentHost(config);
    hosts.push(host);
    await host.start();
    await vi.waitFor(() => expect(slices).toBeGreaterThan(0));
    expect(host.agents.retentionReferencesReady()).toBe(false);
    expect(sweep).not.toHaveBeenCalled();
    expect(fs.readdirSync(runs)).toHaveLength(200);
    expect(fs.readFileSync(sample).equals(before)).toBe(true);
    // Finish the SAME cursor, then observe the actual next maintenance tick.
    for (let slice = 0; slice < 10 && !host.agents.retentionReferencesReady(); slice++) {
      references.call(host.agents, { budgetMs: 10_000, maxEntries: 1_000 });
    }
    expect(host.agents.retentionReferencesReady()).toBe(true);
    await vi.waitFor(() => expect(sweep).toHaveBeenCalled());
    expect(sweep.mock.calls[0]![1].has("*")).toBe(false);
  });

  it("a complete scan still keeps an actor-referenced run while an unrelated eligible run is pruned", () => {
    const base = root(), runs = path.join(base, "runs"), actorRoot = path.join(base, "actors");
    const eligible = seed(runs, "eligible"), held = seed(runs, "held");
    fs.mkdirSync(actorRoot);
    fs.writeFileSync(path.join(actorRoot, "actors.json"), JSON.stringify({ actors: [{ id: "actor", lastRunId: "held" }] }));
    const before = fs.readFileSync(path.join(held, "events.jsonl"));
    const collector = new ResidentRequestRetention(base, [actorRoot], { retainRuns: false });
    try {
      collector.sweep(Date.now(), new Set(), 10_000);
      expect(fs.existsSync(eligible)).toBe(false);
      expect(fs.readFileSync(path.join(held, "events.jsonl")).equals(before)).toBe(true);
    } finally { collector.close(); }
  });
});
