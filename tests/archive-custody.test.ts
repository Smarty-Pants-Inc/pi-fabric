import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ARCHIVE_PENDING_FILE, commitRunArchive, readPendingRunArchives, stageRunArchive } from "../src/agents/archive-custody.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { runTreeExitVeto } from "../src/storage/retention.js";
const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
describe.skipIf(process.platform === "win32")("restart archive recovery (POSIX-only; Windows follow-up smarty-dev#5132)", () => {
  it("changed oversized ancestor input retains its own custody without starving a nested exact result", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "archive-changing-")); roots.push(root);
    const parent = path.join(root, "runs", "parent"), child = path.join(parent, "nested", "child");
    fs.mkdirSync(child, { recursive: true, mode: 0o700 });
    const ancestor = { id: "parent", status: "completed", text: "p".repeat(1_200_000), startedAt: 1 } as AgentRunResult;
    const outcome = { id: "child", status: "completed", text: "exact descendant", startedAt: 1 } as AgentRunResult;
    stageRunArchive(parent, { format: 1, kind: "settlement", result: ancestor });
    stageRunArchive(child, { format: 1, kind: "settlement", result: outcome });
    const delivered: AgentRunResult[] = [];
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs"), onSettled: result => { delivered.push(result); } });
    try {
      let checks = 0; manager.recoverPendingArchives(parent, () => ++checks > 2);
      // Mutate the file while its chunk cursor is suspended. No stale ancestor
      // result may publish, but the nested walk must retain fair progress.
      fs.appendFileSync(path.join(parent, ARCHIVE_PENDING_FILE), " ");
      for (let slice = 0; slice < 8 && !delivered.length; slice++) manager.recoverPendingArchives(parent);
      expect(delivered).toEqual([outcome]);
      expect(fs.existsSync(path.join(parent, ARCHIVE_PENDING_FILE))).toBe(true);
      expect(fs.existsSync(path.join(child, ARCHIVE_PENDING_FILE))).toBe(false);
    } finally { await manager.close(); }
  });

  it.each([false, true])("reaches a deep pending result beyond 128 stable nested directories (time interruption=%s) within K slices", async interrupted => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "archive-recovery-")); roots.push(root);
    const parent = path.join(root, "runs", "parent");
    for (let i = 0; i < 300; i++) fs.mkdirSync(path.join(parent, "nested", `child-${i}`), { recursive: true, mode: 0o700 });
    const cursor = fs.opendirSync(path.join(parent, "nested")); const order: string[] = [];
    try { let entry: fs.Dirent | null; while ((entry = cursor.readSync())) order.push(entry.name); } finally { cursor.closeSync(); }
    const directory = path.join(parent, "nested", order.at(-1)!, "nested", "deep", "nested", "outcome");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const result = { id: "outcome", name: "exact nested source", status: "completed", text: "original full outcome", startedAt: 1 } as AgentRunResult;
    stageRunArchive(directory, { format: 1, kind: "settlement", result });
    const delivered: AgentRunResult[] = [];
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, { runRoot: path.join(root, "runs"), onSettled: value => { delivered.push(value); } });
    try {
      const K = interrupted ? 32 : 4;
      for (let slice = 0; slice < K && !delivered.length; slice++) {
        let checks = 0;
        manager.recoverPendingArchives(parent, () => interrupted && ++checks > 16);
      }
      expect(delivered).toEqual([result]);
      expect(fs.existsSync(path.join(directory, ARCHIVE_PENDING_FILE))).toBe(false);
      expect(fs.readdirSync(path.join(parent, "nested"))).toHaveLength(300);
    } finally { await manager.close(); }
  });

  it.each(["settlement", "shutdown"] as const)("chunk-recovers a >1 MiB dual-sink record; failed %s custody survives independent success and restart", async failed => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "archive-large-")); roots.push(root);
    const directory = path.join(root, "runs", "large"); fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const settlement = { id: "large", name: "large exact outcome", status: "completed", text: "🙂".repeat(150_000), startedAt: 1 } as AgentRunResult;
    const shutdown = { ...settlement, status: "stopped", text: "s".repeat(600_000), error: "Host stopped" } as AgentRunResult;
    stageRunArchive(directory, { format: 1, kind: "settlement", result: settlement });
    stageRunArchive(directory, { format: 1, kind: "shutdown", result: shutdown });
    expect(fs.statSync(path.join(directory, ARCHIVE_PENDING_FILE)).size).toBeGreaterThan(1024 * 1024);
    const settled: AgentRunResult[] = [], stopped: AgentRunResult[] = [];
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
      runRoot: path.join(root, "runs"),
      onSettled: value => { if (failed === "settlement") throw new Error("sink offline"); settled.push(value); },
      onStoppedAtClose: values => { if (failed === "shutdown") throw new Error("sink offline"); stopped.push(...values); },
    });
    try {
      // Budget interruption during input preserves the byte offset, including
      // UTF-8 sequences split at 64-KiB boundaries.
      let checks = 0; expect(manager.recoverPendingArchives(directory, () => ++checks > 4)).toBe(0);
      for (let i = 0; i < 16 && !(settled.length + stopped.length); i++) { checks = 0; manager.recoverPendingArchives(directory, () => ++checks > 4); }
      expect(settled).toEqual(failed === "settlement" ? [] : [settlement]);
      expect(stopped).toEqual(failed === "shutdown" ? [] : [shutdown]);
      expect(readPendingRunArchives(directory)).toEqual([{ format: 1, kind: failed, result: failed === "settlement" ? settlement : shutdown }]);
      expect(runTreeExitVeto(directory)).toMatch(/archive is pending/);
    } finally { await manager.close(); }
    const restarted = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
      runRoot: path.join(root, "runs"), onSettled: value => { settled.push(value); }, onStoppedAtClose: values => { stopped.push(...values); },
    });
    try {
      for (let i = 0; i < 8 && fs.existsSync(path.join(directory, ARCHIVE_PENDING_FILE)); i++) restarted.recoverPendingArchives(directory);
      expect(settled).toEqual([settlement]); expect(stopped).toEqual([shutdown]);
      expect(fs.existsSync(path.join(directory, ARCHIVE_PENDING_FILE))).toBe(false);
    } finally { await restarted.close(); }
  });
});

describe("independent archive custody sinks", () => {
  it.each(["settlement", "shutdown"] as const)("a committed %s sink cannot release the other exact full outcome", kind => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "archive-custody-")); roots.push(directory);
    const result = { id: path.basename(directory), name: "full source", status: "stopped", text: "full result".repeat(10000), startedAt: 1 } as AgentRunResult;
    stageRunArchive(directory, { format: 1, kind: "settlement", result });
    stageRunArchive(directory, { format: 1, kind: "shutdown", result: { ...result, error: "Host stopped" } });
    commitRunArchive(directory, kind);
    const remaining = kind === "shutdown" ? "settlement" : "shutdown";
    expect(readPendingRunArchives(directory)).toEqual([{ format: 1, kind: remaining, result: { ...result, ...(remaining === "shutdown" ? { error: "Host stopped" } : {}) } }]);
    expect(runTreeExitVeto(directory)).toMatch(/archive is pending/);
    commitRunArchive(directory, remaining);
    expect(fs.existsSync(path.join(directory, ARCHIVE_PENDING_FILE))).toBe(false);
  });
});
