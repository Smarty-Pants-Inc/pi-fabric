import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { saveFinalAnswerReceipt, taskControlDirectory, writeTaskControlJson } from "../src/worker/terminal-answer.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import type { AgentTerminalNotice } from "../src/agents/terminal-target.js";
import { TerminalNoticeOutbox, readTerminalAdmission } from "../src/agents/terminal-notices.js";
import { canRemoveTerminalRun, pruneActorRunArchives } from "../src/storage/retention.js";

// Decision C: the worker that wrote the legacy (pre-`forwarded`) row no longer
// exists in this tree, so no E2E can produce the record; the regression drives
// the real manager, restart outbox and collector over its exact persisted bytes.
const roots: string[] = [];
const managers: AgentManager[] = [];
const exits: (() => void)[] = [];
afterEach(async () => {
  for (const exit of exits.splice(0)) exit();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const harness = (onTerminalNotice: (notice: AgentTerminalNotice) => void) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "legacy-refusal-")); roots.push(root);
  const records = new Map<string, AgentRunRecord>();
  const closures = new Map<string, () => void>();
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const args = new Map<string, string>();
    for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
    const now = Date.now();
    const record: AgentRunRecord = { id: request.id, name: request.name, task: "task", status: "running", runner: "pi", transport: "process",
      cwd: request.cwd, startedAt: now, updatedAt: now, turns: 1, toolCalls: 0, text: "intermediate prose",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, sessionId: "99999999" };
    records.set(request.id, record);
    writeJsonAtomic(args.get("--status-file")!, record);
    let alive = true;
    let close!: () => void;
    const closed = new Promise<void>(resolve => { close = () => { alive = false; resolve(); }; });
    closures.set(request.id, close); exits.push(close);
    return { kind: "process", sessionId: "99999999", closed, stopDebt: () => undefined,
      waitForClose: () => closed, isAlive: async () => alive, stop: async () => { await closed; } };
  });
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, retainRuns: true },
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, onSettled: vi.fn(), onTerminalNotice });
  managers.push(manager);
  const finish = (id: string) => {
    const receipt = saveFinalAnswerReceipt(path.join(root, id), id, "done");
    writeJsonAtomic(path.join(root, id, "status.json"), { ...records.get(id)!, status: "completed", text: "done",
      finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
    return receipt;
  };
  return { manager, root, launch, finish, close: (id: string) => closures.get(id)!() };
};
const waitUntil = async (condition: () => boolean) => {
  for (let i = 0; i < 100 && !condition(); i++) await new Promise(resolve => setTimeout(resolve, 10));
  expect(condition()).toBe(true);
};
const settle = () => new Promise(resolve => setTimeout(resolve, 20));

const sender = { id: "session:original", kind: "main" as const, verified: "mesh" as const };
const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const, sender };
const wrong = { ...provenance, sender: { ...sender, id: "session:other" } };

// Exact bytes of 0aabbd5fe's settleTerminalControl: `{ ...readTerminalControl(), state }`,
// i.e. { id, delivery, state, provenance } with no `forwarded` key.
type Variant = "legacy-refused" | "legacy-queued" | "legacy-delivered" | "mismatched-sender" | "refused-unforwarded" | "refused-discard-ack";
const row = (variant: Variant, id: string): Record<string, unknown> => {
  switch (variant) {
    case "legacy-refused": return { id, delivery: "steer", state: "refused", provenance };
    case "legacy-queued": return { id, delivery: "steer", provenance, state: "queued" };
    case "legacy-delivered": return { id, delivery: "steer", state: "delivered", provenance };
    case "mismatched-sender": return { id, delivery: "steer", provenance: wrong, forwarded: false, state: "refused" };
    case "refused-unforwarded": return { id, delivery: "steer", provenance, forwarded: false, state: "refused" };
    case "refused-discard-ack": return { id, delivery: "steer", provenance, forwarded: true, state: "refused" };
  }
};

describe("legacy terminal refusal rows across manager, restart recovery and collector", () => {
  it.each([
    // [variant, routed to the original sender, collectible]
    ["legacy-refused", false, false],
    ["legacy-queued", false, false],
    ["legacy-delivered", false, true],
    ["mismatched-sender", false, false],
    ["refused-unforwarded", true, true],
    ["refused-discard-ack", true, true],
  ] as const)("%s: routed=%s collectible=%s", async (variant, routed, collectible) => {
    const notices: AgentTerminalNotice[] = [];
    const { manager, root, launch, finish, close } = harness(notice => { notices.push(notice); });
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const directory = path.join(root, handle.id);
    const sent = manager.steer(handle.id, "late steer", undefined, provenance);
    expect(sent.undelivered).toBeUndefined();
    // The row format is intentionally pre-forwarded; its current host-owned
    // storage is central-v1. An in-run overlay would never replace the real row.
    const registry = path.join(taskControlDirectory(directory), "terminal-controls", `${sent.messageId}.json`);
    writeTaskControlJson(directory, path.join("terminal-controls", `${sent.messageId}.json`), row(variant, sent.messageId));
    const persisted = fs.readFileSync(registry, "utf8");
    const receipt = finish(handle.id);
    await manager.wait(handle.id, { timeoutMs: 1000 });
    // Native-close barrier: the next launch proves this execution drained.
    const next = await manager.spawn({ task: "native-close barrier", transport: "process" });
    close(handle.id);
    await waitUntil(() => launch.mock.calls.length === 2);
    close(next.id);
    manager.status(handle.id);
    const expected: AgentTerminalNotice = { code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: receipt.id, messageId: sent.messageId, delivery: "steer", sender };
    const pending = path.join(taskControlDirectory(directory), "terminal-notices", `${sent.messageId}.json`);
    if (routed) await waitUntil(() => fs.existsSync(`${pending}.delivered`));
    else await settle();
    expect(notices).toEqual(routed ? [expected] : []);

    // Restart: a fresh outbox recovers the same tree; attribution comes only from steer.jsonl.
    const recovered: AgentTerminalNotice[] = [];
    const outbox = new TerminalNoticeOutbox(notice => { recovered.push(notice); });
    outbox.recover(directory);
    outbox.recover(directory);
    await settle();
    expect(recovered).toEqual([]); // Delivered once already, or never provable.
    expect(fs.existsSync(pending)).toBe(false);
    expect(fs.existsSync(`${pending}.delivered`)).toBe(routed);
    expect(fs.readFileSync(registry, "utf8")).toBe(persisted); // Evidence is never rewritten.
    expect(readTerminalAdmission(directory, sent.messageId)).toEqual({ id: sent.messageId, delivery: "steer", sender });
    expect(canRemoveTerminalRun(directory)).toBe(collectible);
    const swept = pruneActorRunArchives({ runsDirectory: root, retentionMs: 0, now: Date.now() + 1_000, dryRun: true });
    expect(swept.includes(directory)).toBe(collectible);
    expect(fs.existsSync(directory)).toBe(true);
  });

  it("a stale pending notice for a legacy refused row is never routed, and an old route receipt cannot retire it", async () => {
    const notices: AgentTerminalNotice[] = [];
    const { manager, root, launch, finish, close } = harness(notice => { notices.push(notice); });
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const directory = path.join(root, handle.id);
    const sent = manager.steer(handle.id, "late steer", undefined, provenance);
    writeTaskControlJson(directory, path.join("terminal-controls", `${sent.messageId}.json`), row("legacy-refused", sent.messageId));
    const receipt = finish(handle.id);
    await manager.wait(handle.id, { timeoutMs: 1000 });
    const next = await manager.spawn({ task: "native-close barrier", transport: "process" });
    close(handle.id);
    await waitUntil(() => launch.mock.calls.length === 2);
    close(next.id);
    await settle();
    // An earlier host may have retained the outbox entry before restart.
    const notice: AgentTerminalNotice = { code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: receipt.id, messageId: sent.messageId, delivery: "steer", sender };
    const pending = path.join(taskControlDirectory(directory), "terminal-notices", `${sent.messageId}.json`);
    writeTaskControlJson(directory, path.join("terminal-notices", `${sent.messageId}.json`), notice);
    const recovered: AgentTerminalNotice[] = [];
    const outbox = new TerminalNoticeOutbox(value => { recovered.push(value); });
    outbox.recover(directory);
    manager.status(handle.id);
    await settle();
    expect(recovered).toEqual([]);
    expect(notices).toEqual([]);
    expect(fs.existsSync(pending)).toBe(true);
    expect(fs.existsSync(`${pending}.delivered`)).toBe(false);
    expect(canRemoveTerminalRun(directory)).toBe(false);
    // Even an old route receipt (sender already told) does not discharge the uncertain row.
    writeTaskControlJson(directory, path.join("terminal-notices", `${sent.messageId}.json.delivered`), notice);
    expect(canRemoveTerminalRun(directory)).toBe(false);
    expect(pruneActorRunArchives({ runsDirectory: root, retentionMs: 0, now: Date.now() + 1_000, dryRun: true })).not.toContain(directory);
  });
});
