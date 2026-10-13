import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { FabricSteerAdmissionLimitError, type AgentTerminalNotice } from "../src/agents/terminal-target.js";
import { MAX_STEER_BYTES, MAX_STEER_CONTROLS, MAX_PUBLIC_REFUSALS, readTerminalAdmissions } from "../src/agents/terminal-notices.js";

// Private scratch roots are retained as evidence; this regression never deletes fixtures.
const managers: AgentManager[] = [];
const exits: (() => void)[] = [];
afterEach(async () => {
  for (const exit of exits.splice(0)) exit();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
});

const harness = () => {
  const notices: AgentTerminalNotice[] = [];
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "steer-ended-"));
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
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, onTerminalNotice: notice => { notices.push(notice); } });
  managers.push(manager);
  const finish = (id: string, text: string) => {
    const receipt = saveFinalAnswerReceipt(path.join(root, id), id, text);
    writeJsonAtomic(path.join(root, id, "status.json"), { ...records.get(id)!, status: "completed", text,
      finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
    return receipt;
  };
  return { manager, root, launch, records, notices, finish, close: (id: string) => closures.get(id)!() };
};
const waitUntil = async (condition: () => boolean) => {
  for (let i = 0; i < 200 && !condition(); i++) await new Promise(resolve => setTimeout(resolve, 10));
  expect(condition()).toBe(true);
};
const sender = { id: "session:original-sender", name: "Original", kind: "main" as const, verified: "mesh" as const };
const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const, sender };

describe("steer/followUp after an ordinary Pi task's final answer", () => {
  it.each(["steer", "followUp"] as const)("%s: final recorded between ingress pre-check and append is run-ended in the receipt and status, before native exit", async kind => {
    const { manager, root, launch, records, notices, finish, close } = harness();
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const directory = path.join(root, handle.id);
    const steerFile = path.join(directory, "steer.jsonl");
    const append = fs.appendFileSync;
    let receipt: ReturnType<typeof finish> | undefined;
    vi.spyOn(fs, "appendFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: string | Uint8Array, options?: fs.WriteFileOptions) => {
      // The pre-checks passed; the worker records its immutable final answer while this input is being appended.
      if (file === steerFile && !receipt) receipt = finish(handle.id, "done");
      return append(file, data, options);
    }) as typeof fs.appendFileSync);
    const sent = kind === "steer"
      ? manager.steer(handle.id, "racing input", undefined, provenance)
      : manager.followUp(handle.id, "racing input", undefined, { ...provenance, via: "followUp" }, { deadlineMs: 60_000 });
    expect(receipt).toBeDefined();
    const expected = { code: "FABRIC_TARGET_TERMINAL", reason: "run-ended", messageId: sent.messageId, delivery: kind,
      finalAnswerReceiptId: receipt!.id, sender };
    expect(sent).toMatchObject({ queued: true, messageId: sent.messageId, undelivered: expected });
    // Visible through status while the native process is still alive.
    expect(manager.hasRunCustody(handle.id)).toBe(true);
    expect(manager.status(handle.id).undeliveredMessages).toEqual([expected]);
    // The parent consumes the result; it reports the same refusal, still without native exit.
    const result = await manager.wait(handle.id, { timeoutMs: 1000 });
    expect(result).toMatchObject({ status: "completed", text: "done", undeliveredMessages: [expected] });
    await waitUntil(() => fs.existsSync(path.join(directory, "terminal-notices", `${sent.messageId}.json.delivered`)));
    expect(notices).toEqual([{ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id, finalAnswerReceiptId: receipt!.id,
      messageId: sent.messageId, delivery: kind, sender }]);
    // No new turn, no relaunch, and the final answer source is untouched.
    expect(launch).toHaveBeenCalledTimes(1);
    expect(records.get(handle.id)!.turns).toBe(1);
    expect(fs.readFileSync(path.join(directory, "final-answer.json"), "utf8")).toContain("done");
    close(handle.id);
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(notices).toHaveLength(1);
    expect(manager.status(handle.id).undeliveredMessages).toEqual([expected]);
  });

  it("an unread admission appended just before the final answer is run-ended in status without waiting on native exit", async () => {
    const { manager, root, notices, finish } = harness();
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const sent = manager.steer(handle.id, "never read", undefined, provenance);
    expect(sent.undelivered).toBeUndefined();
    const receipt = finish(handle.id, "done");
    expect(manager.hasRunCustody(handle.id)).toBe(true);
    expect(manager.status(handle.id).undeliveredMessages).toEqual([{ code: "FABRIC_TARGET_TERMINAL", reason: "run-ended",
      messageId: sent.messageId, delivery: "steer", finalAnswerReceiptId: receipt.id, sender }]);
    await waitUntil(() => fs.existsSync(path.join(root, handle.id, "terminal-notices", `${sent.messageId}.json.delivered`)));
    expect(notices.map(notice => notice.sender)).toEqual([sender]);
  });

  it("counterexample: input delivered before the final answer stays delivered, with no refusal", async () => {
    const { manager, root, notices, finish, close } = harness();
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const sent = manager.steer(handle.id, "consumed before final", undefined, provenance);
    expect(sent.undelivered).toBeUndefined();
    writeJsonAtomic(path.join(root, handle.id, "terminal-controls", `${sent.messageId}.json`),
      { id: sent.messageId, delivery: "steer", state: "delivered", provenance });
    finish(handle.id, "done");
    expect(manager.status(handle.id).undeliveredMessages).toBeUndefined();
    expect((await manager.wait(handle.id, { timeoutMs: 1000 })).undeliveredMessages).toBeUndefined();
    close(handle.id);
    await new Promise(resolve => setTimeout(resolve, 30));
    expect(notices).toEqual([]);
    expect(fs.existsSync(path.join(root, handle.id, "terminal-notices"))).toBe(false);
  });

  it("5,000 small requests stay bounded: every excess request is typed, every admitted refusal reaches its original sender", async () => {
    const { manager, root, notices, finish } = harness();
    const handle = await manager.spawn({ task: "bounded admission", transport: "process" });
    const directory = path.join(root, handle.id);
    const started = performance.now(), heap = process.memoryUsage().heapUsed;
    const admitted: string[] = [];
    let refused = 0;
    for (let i = 0; i < 5000; i++) {
      try { admitted.push(manager.steer(handle.id, `small ${i}`, undefined, provenance).messageId); }
      catch (error) {
        expect(error).toBeInstanceOf(FabricSteerAdmissionLimitError);
        expect(error).toMatchObject({ code: "FABRIC_STEER_ADMISSION_LIMIT", targetId: handle.id, controlLimit: 256, byteLimit: MAX_STEER_BYTES });
        refused++;
      }
    }
    expect(admitted).toHaveLength(MAX_STEER_CONTROLS);
    expect(refused).toBe(5000 - MAX_STEER_CONTROLS);
    expect(fs.statSync(path.join(directory, "steer.jsonl")).size).toBeLessThan(MAX_STEER_BYTES);
    // Deadline refusal happens before any follow-up payload is created.
    expect(() => manager.followUp(handle.id, "overflow", undefined, provenance, { deadlineMs: 60000 })).toThrow(FabricSteerAdmissionLimitError);
    expect(fs.existsSync(path.join(directory, "follow-ups"))).toBe(false);
    finish(handle.id, "done");
    const read = fs.readFileSync;
    const journalReads = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: any[]) => {
      if (file === path.join(directory, "steer.jsonl")) throw new Error("unbounded admission read");
      return (read as any)(file, ...args);
    }) as typeof fs.readFileSync);
    const streaming = vi.spyOn(fs, "readSync");
    const status = manager.status(handle.id);
    expect(status.undeliveredMessages).toHaveLength(MAX_PUBLIC_REFUSALS);
    expect(status).toMatchObject({ undeliveredMessagesTotal: 256, undeliveredMessagesTruncated: true });
    await waitUntil(() => notices.length === MAX_STEER_CONTROLS);
    expect(new Set(notices.map(notice => notice.messageId))).toEqual(new Set(admitted));
    expect(notices.every(notice => notice.sender?.id === sender.id)).toBe(true);
    // One <=1MiB stream pass shared by scan, view and routing, not N full reads.
    expect(streaming.mock.calls.length).toBeLessThanOrEqual(Math.ceil(fs.statSync(path.join(directory, "steer.jsonl")).size / (16 * 1024)) + 1);
    journalReads.mockRestore(); streaming.mockRestore();
    const elapsedMs = performance.now() - started, heapGrowth = process.memoryUsage().heapUsed - heap;
    expect(elapsedMs).toBeLessThan(20000);
    expect(heapGrowth).toBeLessThan(128 * 1024 * 1024);
    console.log(JSON.stringify({ proof: "5000 admission attempts", admitted: admitted.length, refused, routed: notices.length, publicEntries: status.undeliveredMessages?.length,
      total: status.undeliveredMessagesTotal, truncated: status.undeliveredMessagesTruncated, elapsedMs, heapGrowth }));
  }, 30000);

  it("enforces the durable byte bound and fails closed on an oversized retained journal", async () => {
    const { manager, root } = harness();
    const handle = await manager.spawn({ task: "byte admission", transport: "process" });
    const directory = path.join(root, handle.id);
    for (let i = 0; i < 16; i++) manager.steer(handle.id, "x".repeat(63000), undefined, provenance);
    expect(() => manager.steer(handle.id, "x".repeat(63000), undefined, provenance)).toThrow(FabricSteerAdmissionLimitError);
    const file = path.join(directory, "steer.jsonl");
    expect(fs.statSync(file).size).toBeLessThanOrEqual(MAX_STEER_BYTES);
    // Retained data, not a volatile counter, establishes the bound on resume.
    fs.appendFileSync(file, "x".repeat(MAX_STEER_BYTES));
    expect(readTerminalAdmissions(directory)).toMatchObject({ truncated: true });
    expect(() => manager.steer(handle.id, "tiny", undefined, provenance)).toThrow(FabricSteerAdmissionLimitError);
  });

  it("refuses later input with a typed error naming the run state, keeping FABRIC_TARGET_TERMINAL", async () => {
    const { manager, root, finish } = harness();
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const receipt = saveFinalAnswerReceipt(path.join(root, handle.id), handle.id, "pending");
    expect(() => manager.steer(handle.id, "late")).toThrow(expect.objectContaining({ name: "FabricTargetTerminalError",
      code: "FABRIC_TARGET_TERMINAL", reason: "run-ended", runState: "final-answer-pending", finalAnswerReceiptId: receipt.id }));
    finish(handle.id, "pending");
    await manager.wait(handle.id, { timeoutMs: 1000 });
    expect(() => manager.followUp(handle.id, "late")).toThrow(expect.objectContaining({ code: "FABRIC_TARGET_TERMINAL",
      reason: "run-ended", runState: "completed", message: expect.stringMatching(/run ended \(completed\)/) }));
    expect(fs.existsSync(path.join(root, handle.id, "steer.jsonl"))).toBe(false); // nothing was admitted
  });
});
