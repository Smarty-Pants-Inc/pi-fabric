import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { stageRunArchive } from "../src/agents/archive-custody.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { compactTerminalRunEvents } from "../src/storage/retention.js";

// This exact file also runs unchanged against main 9387af87. These are main's
// Windows tails/counts, NOT V2 expectations weakened to make Windows pass.
// Windows V2 acceptance moves to Smarty-Pants-Inc/smarty-dev#5132.
const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
const roots: string[] = [];
const now = 200_000_000;
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "retention-main-parity-"));
  roots.push(root); return root;
};
const makeRun = (root: string, id: string, lines = 401) => {
  const run = path.join(root, "runs", id);
  fs.mkdirSync(run, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ id, status: "completed", finishedAt: 1,
    transport: "process", sessionId: "2147483647" }), { mode: 0o600 });
  const events = Array.from({ length: lines }, (_, sequence) => JSON.stringify({ sequence }) + "\n").join("");
  fs.writeFileSync(path.join(run, "events.jsonl"), events, { mode: 0o600 });
  return run;
};
beforeEach(() => {
  Object.defineProperty(process, "platform", { ...nativePlatform, value: "win32" });
  vi.spyOn(performance, "now").mockReturnValue(0); // counts, not filesystem speed
});
afterEach(() => {
  vi.restoreAllMocks(); Object.defineProperty(process, "platform", nativePlatform);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("forced-win32 parity with main 9387af87 (Windows V2: smarty-dev#5132)", () => {
  it("keeps all five no-op status proof reads and the original small tail", () => {
    const root = fixture(), run = makeRun(root, "small", 20);
    const original = fs.readFileSync;
    const events = original(path.join(run, "events.jsonl"));
    let reads = 0;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof original>) => {
      if (String(args[0]) === path.join(run, "status.json")) reads++;
      return original(...args);
    });
    const rename = vi.spyOn(fs, "renameSync");
    expect(compactTerminalRunEvents(run, { now })).toBe(false);
    expect(reads).toBe(5); expect(rename).not.toHaveBeenCalled();
    expect(original(path.join(run, "events.jsonl"))).toEqual(events);
    console.info("main-parity bounded: statusReads=5, lines=20, replacements=0");
  });

  it("compacts all 130 runs in one main sweep, including >64-entry trees", () => {
    const root = fixture();
    const runs = Array.from({ length: 130 }, (_, index) => makeRun(root, `run-${index}`));
    for (let i = 0; i < 65; i++) fs.writeFileSync(path.join(runs[0]!, `oversized-event-prefix-${i}.txt`), "keep");
    let recovered = 0;
    const collector = new ResidentRequestRetention(root, [], {}, () => { recovered++; });
    try {
      collector.sweep(now, new Set(), 5);
      expect(collector.due(now)).toBe(false); expect(recovered).toBe(130);
      for (const run of runs) {
        const lines = fs.readFileSync(path.join(run, "events.jsonl"), "utf8").trim().split("\n");
        expect(lines).toHaveLength(201);
        expect(JSON.parse(lines[0]!)).toMatchObject({ fabricTruncated: true });
        expect(JSON.parse(lines[1]!)).toEqual({ sequence: 201 });
        expect(JSON.parse(lines[200]!)).toEqual({ sequence: 400 });
      }
      console.info("main-parity sweep: recovered=130, compacted=130, lines=201, first=201, last=400");
    } finally { collector.close(); }
  });

  it("rereads all 130 ownership proofs on each call instead of applying V2 count/cache bounds", async () => {
    const root = fixture();
    for (let i = 0; i < 130; i++) makeRun(root, `run-${i}`, 20);
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs") });
    const original = fs.readFileSync; let reads = 0;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof original>) => {
      if (String(args[0]).endsWith("status.json")) reads++;
      return original(...args);
    });
    try {
      expect(manager.retentionReferences()).toEqual(new Set()); expect(reads).toBe(650);
      expect(manager.retentionReferences()).toEqual(new Set()); expect(reads).toBe(1300);
      console.info("main-parity ownership: refs=0, firstStatusReads=650, secondTotalStatusReads=1300");
    } finally { await manager.close(); }
  });

  it("recovers all 140 exact outcomes in one main call despite a V2 deadline veto", async () => {
    const root = fixture(), delivered: AgentRunResult[] = [];
    for (let i = 0; i < 140; i++) {
      const id = `run-${i}`, directory = makeRun(root, id, 20);
      const result = { id, status: "completed", startedAt: 1, text: i === 139 ? "x".repeat(1_200_000) : id } as AgentRunResult;
      stageRunArchive(directory, { format: 1, kind: "settlement", result });
    }
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
      runRoot: path.join(root, "runs"), onSettled: result => { delivered.push(result); },
    });
    try {
      const recover = manager.recoverPendingArchives.bind(manager) as (directory?: string, expired?: () => boolean) => number;
      expect(recover(undefined, () => true)).toBe(140); expect(delivered).toHaveLength(140);
      expect(delivered.find(result => result.id === "run-139")?.text).toHaveLength(1_200_000);
      expect(recover()).toBe(0);
      console.info("main-parity recovery: first=140, second=0, fullTextBytes=1200000");
    } finally { await manager.close(); }
  });

  it("keeps main's dual-sink failure ordering rather than activating V2 independent retries", async () => {
    const root = fixture(), directory = makeRun(root, "dual", 20);
    const result = { id: "dual", status: "completed", startedAt: 1, text: "exact" } as AgentRunResult;
    stageRunArchive(directory, { format: 1, kind: "settlement", result });
    stageRunArchive(directory, { format: 1, kind: "shutdown", result });
    const stopped: AgentRunResult[] = [];
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
      runRoot: path.join(root, "runs"), onSettled: () => { throw new Error("sink offline"); }, onStoppedAtClose: values => { stopped.push(...values); },
    });
    try {
      expect(manager.recoverPendingArchives(directory)).toBe(0); expect(stopped).toEqual([]);
      expect(fs.existsSync(path.join(directory, "archive-pending.json"))).toBe(true);
      console.info("main-parity dual-sink: recovered=0, stopped=0, custody=retained");
    } finally { await manager.close(); }
  });
});
