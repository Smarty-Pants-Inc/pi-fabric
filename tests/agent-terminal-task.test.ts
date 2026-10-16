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
import type { AgentTerminalNotice } from "../src/agents/terminal-target.js";
import { TerminalNoticeOutbox, readTerminalAdmission } from "../src/agents/terminal-notices.js";
import { canRemoveTerminalRun } from "../src/storage/retention.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const exits: (() => void)[] = [];
afterEach(async () => {
  for (const exit of exits.splice(0)) exit();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const harness = (onTerminalNotice?: (notice: AgentTerminalNotice) => void | Promise<void>, sessionId = "99999999") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-manager-")); roots.push(root);
  const records = new Map<string, AgentRunRecord>();
  const closures = new Map<string, () => void>();
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const args = new Map<string, string>();
    for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
    const now = Date.now();
    const record: AgentRunRecord = { id: request.id, name: request.name, task: "task", status: "running", runner: "pi", transport: "process",
      cwd: request.cwd, startedAt: now, updatedAt: now, turns: 1, toolCalls: 0, text: "intermediate prose",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }, sessionId };
    records.set(request.id, record);
    writeJsonAtomic(args.get("--status-file")!, record);
    let alive = true;
    let close!: () => void;
    const closed = new Promise<void>(resolve => { close = () => { alive = false; resolve(); }; });
    closures.set(request.id, close); exits.push(close);
    return { kind: "process", sessionId, closed, stopDebt: () => undefined,
      waitForClose: () => closed, isAlive: async () => alive, stop: async () => { await closed; } };
  });
  const settled = vi.fn();
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, retainRuns: true },
    { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, onSettled: settled, ...(onTerminalNotice ? { onTerminalNotice } : {}) });
  managers.push(manager);
  const finish = (id: string, text: string) => {
    const receipt = saveFinalAnswerReceipt(path.join(root, id), id, text);
    writeJsonAtomic(path.join(root, id, "status.json"), { ...records.get(id)!, status: "completed", text,
      finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
    return receipt;
  };
  return { manager, root, launch, settled, records, finish, close: (id: string) => closures.get(id)!() };
};
const waitUntil = async (condition: () => boolean) => {
  for (let i = 0; i < 100 && !condition(); i++) await new Promise(resolve => setTimeout(resolve, 10));
  expect(condition()).toBe(true);
};

describe("ordinary process-task final answer boundary", () => {
  it("keeps wait pending on receipt and settles exactly once after owned execution closes", async () => {
    const { manager, settled, launch, close, finish } = harness();
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const receipt = finish(handle.id, "final answer");
    expect(manager.status(handle.id)).toMatchObject({ status: "finishing", text: "final answer" });
    await expect(manager.wait(handle.id, { timeoutMs: 20 })).rejects.toThrow(/still running|timed out/i);
    expect(settled).not.toHaveBeenCalled();
    expect(manager.hasRunCustody(handle.id)).toBe(true);
    const next = await manager.spawn({ task: "next", transport: "process" });
    expect(next.status).toBe("queued"); expect(launch).toHaveBeenCalledTimes(1);
    close(handle.id);
    const result = await manager.wait(handle.id, { timeoutMs: 1000 });
    expect(result).toMatchObject({ status: "completed", text: "final answer", finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt }, sessionId: "99999999" });
    expect(await manager.wait(handle.id)).toMatchObject({ finalAnswerReceipt: { id: receipt.id } });
    expect(settled).toHaveBeenCalledOnce();
    await waitUntil(() => launch.mock.calls.length === 2);
    close(next.id);
  });

  it("retains a terminal answer whose saved PID is still live despite mocked native close", async () => {
    // Main's persisted live-PID fence is independent of the PR's finishing join.
    // The transport is mocked; it must never certify collection of this process.
    const { manager, root, finish, close } = harness(undefined, String(process.pid));
    const handle = await manager.spawn({ task: "live saved PID", transport: "process" });
    const directory = path.join(root, handle.id);
    finish(handle.id, "answer is not an exit receipt");
    expect(manager.status(handle.id).status).toBe("finishing");
    expect(canRemoveTerminalRun(directory)).toBe(false);
    close(handle.id);
    expect(await manager.wait(handle.id, { timeoutMs: 1000 })).toMatchObject({ status: "completed" });
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")).sessionId).toBe(String(process.pid));
    expect(canRemoveTerminalRun(directory)).toBe(false);
  });

  it("run returns only after the receipt and owned execution close", async () => {
    const { manager, launch, settled, finish, close } = harness();
    const run = manager.run({ task: "foreground", transport: "process" });
    await waitUntil(() => launch.mock.calls.length === 1);
    const id = launch.mock.calls[0]![0].id;
    finish(id, "foreground result");
    expect(manager.status(id).status).toBe("finishing");
    expect(settled).not.toHaveBeenCalled();
    close(id);
    expect(await run).toMatchObject({ status: "completed", text: "foreground result" });
    expect(settled).toHaveBeenCalledOnce();
    expect(manager.isSettled(id)).toBe(true);
  });

  it("does not kill a plain task before its validated receipt frame/status lands", async () => {
    const { manager, root, finish, close } = harness();
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const receipt = saveFinalAnswerReceipt(path.join(root, handle.id), handle.id, "plain final");
    expect(manager.status(handle.id)).toMatchObject({ status: "running", finalAnswerReceipt: { id: receipt.id } });
    expect(manager.isSettled(handle.id)).toBe(false);
    await expect(manager.wait(handle.id, { timeoutMs: 20 })).rejects.toThrow(/still running|timed out/i);
    finish(handle.id, "plain final");
    close(handle.id);
    expect(await manager.wait(handle.id, { timeoutMs: 1000 })).toMatchObject({ status: "completed", finalAnswerReceipt: { id: receipt.id } });
  });

  it("fences late steer/followUp even before monitor observes the receipt", async () => {
    const { manager, root, finish } = harness();
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const before = manager.steer(handle.id, "non-final tools remain steerable");
    expect(before.queued).toBe(true);
    const receipt = finish(handle.id, "done");
    for (const send of [() => manager.steer(handle.id, "stale steer"), () => manager.followUp(handle.id, "stale follow-up")]) {
      expect(send).toThrow(expect.objectContaining({ code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: receipt.id }));
    }
    expect(fs.readFileSync(path.join(root, handle.id, "steer.jsonl"), "utf8")).not.toContain("stale");
  });

  it("retains schema failures, waits for the associated validated snapshot, and preserves replyVia/value", async () => {
    const { manager, root, records, close } = harness();
    const handle = await manager.spawn({ task: "structured", schema: { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] }, replyTool: true, transport: "process" });
    const receipt = saveFinalAnswerReceipt(path.join(root, handle.id), handle.id, "invalid report");
    manager.status(handle.id);
    expect(manager.isSettled(handle.id)).toBe(false);
    const record = { ...records.get(handle.id)!, status: "failed", error: "Structured agent output was invalid", replyVia: "tool", value: { ok: 42 },
      finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } };
    writeJsonAtomic(path.join(root, handle.id, "status.json"), record);
    close(handle.id);
    expect(await manager.wait(handle.id, { timeoutMs: 1000 })).toMatchObject({ status: "failed", error: record.error, replyVia: "tool", value: { ok: 42 }, finalAnswerReceipt: { id: receipt.id } });
  });

  it("routes each queued sender refusal once, including unread tail, and leaves consumed non-final input alone", async () => {
    const notices: AgentTerminalNotice[] = [];
    const { manager, root, close, finish } = harness(notice => { notices.push(notice); });
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const provenance = (id: string) => ({ v: 1 as const, channel: "fabric" as const, via: "steer" as const, sender: { id, name: id, kind: "main" as const, verified: "mesh" as const } });
    const consumed = manager.steer(handle.id, "consumed before final", undefined, provenance("sender-consumed"));
    const queued = manager.steer(handle.id, "queued", undefined, provenance("sender-local"));
    const unread = manager.followUp(handle.id, "unread", undefined, { ...provenance("sender-remote"), sender: { id: "sender-remote", kind: "remote", verified: "bridge" } }, { deadlineMs: 60_000 });
    writeJsonAtomic(path.join(root, handle.id, "terminal-controls", `${consumed.messageId}.json`), { id: consumed.messageId, delivery: "steer", state: "delivered", provenance: provenance("sender-consumed") });
    const receipt = finish(handle.id, "done");
    for (const [messageId, sender] of [[queued.messageId, provenance("sender-local").sender], [consumed.messageId, provenance("sender-consumed").sender]] as const) {
      fs.appendFileSync(path.join(root, handle.id, "lifecycle.jsonl"), JSON.stringify({ version: 1, event: "message.refused", occurredAt: Date.now(), data: {
        code: "FABRIC_TARGET_TERMINAL", targetId: handle.id, messageId, delivery: "steer", finalAnswerReceiptId: receipt.id, sender,
      } }) + "\n");
    }
    close(handle.id); await manager.wait(handle.id, { timeoutMs: 1000 });
    await waitUntil(() => notices.length === 2);
    expect(notices.map(notice => notice.sender?.id).sort()).toEqual(["sender-local", "sender-remote"]);
    expect(notices.every(notice => notice.finalAnswerReceiptId === receipt.id)).toBe(true);
    expect(notices.map(notice => notice.messageId).sort()).toEqual([queued.messageId, unread.messageId].sort());
    expect(manager.status(handle.id).followUpDeliveries).toContainEqual(expect.objectContaining({ messageId: unread.messageId, state: "cancelled" }));
    expect(canRemoveTerminalRun(path.join(root, handle.id))).toBe(true);
  });

  it.each([
    ["delivery", "lifecycle"], ["sender", "lifecycle"], ["id", "lifecycle"],
    ["delivery", "unread"], ["sender", "unread"], ["id", "unread"],
  ])("retains uncertainty without refusing a mismatched delivered %s row through %s", async (mismatch, source) => {
    const notices: AgentTerminalNotice[] = [];
    const { manager, root, launch, close, finish } = harness(notice => { notices.push(notice); });
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const sender = { id: "session:senderA", name: "Sender A", kind: "main" as const, verified: "mesh" as const };
    const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const, sender };
    const queued = manager.steer(handle.id, "original admission", undefined, provenance);
    const directory = path.join(root, handle.id);
    writeJsonAtomic(path.join(directory, "terminal-controls", `${queued.messageId}.json`), {
      id: mismatch === "id" ? "other-control" : queued.messageId,
      delivery: mismatch === "delivery" ? "followUp" : "steer", state: "delivered",
      provenance: mismatch === "sender" ? { ...provenance, sender: { ...sender, id: "session:senderB" } } : provenance,
    });
    const receipt = finish(handle.id, "done");
    expect(canRemoveTerminalRun(directory)).toBe(false);
    if (source === "lifecycle") {
      fs.appendFileSync(path.join(directory, "lifecycle.jsonl"), JSON.stringify({ version: 1, event: "message.refused", occurredAt: Date.now(), data: {
        code: "FABRIC_TARGET_TERMINAL", targetId: handle.id, messageId: queued.messageId, delivery: "steer", finalAnswerReceiptId: receipt.id, sender,
      } }) + "\n");
      // The lifecycle claim cannot override an unattributed consumption ACK.
      manager.status(handle.id);
      expect(notices).toEqual([]);
      expect(manager.hasRunCustody(handle.id)).toBe(true);
    }
    expect(manager.status(handle.id).status).toBe("finishing");
    await expect(manager.wait(handle.id, { timeoutMs: 20 })).rejects.toThrow(/still running|timed out/i);
    const next = await manager.spawn({ task: "native-close barrier", transport: "process" });
    expect(next.status).toBe("queued");
    close(handle.id);
    await manager.wait(handle.id, { timeoutMs: 1000 });
    await waitUntil(() => launch.mock.calls.length === 2);
    close(next.id);
    manager.status(handle.id);
    const recovery = new TerminalNoticeOutbox(notice => { notices.push(notice); });
    recovery.recover(directory);
    recovery.recover(directory);
    await new Promise(resolve => setTimeout(resolve, 10));
    const pending = path.join(directory, "terminal-notices", `${queued.messageId}.json`);
    expect(notices).toEqual([]);
    expect(fs.existsSync(pending)).toBe(false);
    expect(fs.existsSync(`${pending}.delivered`)).toBe(false);
    expect(fs.existsSync(path.join(directory, "terminal-controls", `${queued.messageId}.json`))).toBe(true);
    expect(readTerminalAdmission(directory, queued.messageId)).toEqual({ id: queued.messageId, delivery: "steer", sender });
    expect(canRemoveTerminalRun(directory)).toBe(false);
  });

  it("retains the original followUp without cancellation when the delivered row wrongly names steer", async () => {
    const notices: AgentTerminalNotice[] = [];
    const { manager, root, launch, close, finish } = harness(notice => { notices.push(notice); });
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const sender = { id: "session:followUp-sender", kind: "main" as const, verified: "mesh" as const };
    const provenance = { v: 1 as const, channel: "fabric" as const, via: "followUp" as const, sender };
    const queued = manager.followUp(handle.id, "original followUp", undefined, provenance, { deadlineMs: 60_000 });
    const directory = path.join(root, handle.id);
    writeJsonAtomic(path.join(directory, "terminal-controls", `${queued.messageId}.json`), {
      id: queued.messageId, delivery: "steer", state: "delivered", provenance,
    });
    finish(handle.id, "done");
    expect(manager.status(handle.id).status).toBe("finishing");
    await expect(manager.wait(handle.id, { timeoutMs: 20 })).rejects.toThrow(/still running|timed out/i);
    const next = await manager.spawn({ task: "native-close barrier", transport: "process" });
    expect(next.status).toBe("queued");
    close(handle.id);
    await manager.wait(handle.id, { timeoutMs: 1000 });
    await waitUntil(() => launch.mock.calls.length === 2);
    close(next.id);
    const recovery = new TerminalNoticeOutbox(notice => { notices.push(notice); });
    recovery.recover(directory);
    recovery.recover(directory);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(notices).toEqual([]);
    const pending = path.join(directory, "terminal-notices", `${queued.messageId}.json`);
    expect(fs.existsSync(pending)).toBe(false);
    expect(fs.existsSync(`${pending}.delivered`)).toBe(false);
    expect(manager.status(handle.id).followUpDeliveries).toContainEqual(expect.objectContaining({ messageId: queued.messageId, state: "queued" }));
    expect(fs.existsSync(path.join(directory, "terminal-controls", `${queued.messageId}.json`))).toBe(true);
    expect(readTerminalAdmission(directory, queued.messageId)).toEqual({ id: queued.messageId, delivery: "followUp", sender });
    expect(canRemoveTerminalRun(directory)).toBe(false);
  });

  it("cannot use a forged worker refusal to notify a different sender", async () => {
    const notices: AgentTerminalNotice[] = [];
    const { manager, root, close, finish } = harness(notice => { notices.push(notice); });
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const admittedSender = { id: "session:admitted", name: "Admitted", kind: "main" as const, verified: "mesh" as const };
    const queued = manager.steer(handle.id, "queued", undefined, { v: 1, channel: "fabric", via: "steer", sender: admittedSender });
    const receipt = finish(handle.id, "done");
    fs.appendFileSync(path.join(root, handle.id, "lifecycle.jsonl"), JSON.stringify({ version: 1, event: "message.refused", occurredAt: Date.now(), data: {
      code: "FABRIC_TARGET_TERMINAL", targetId: handle.id, messageId: queued.messageId, delivery: "steer", finalAnswerReceiptId: receipt.id,
      sender: { ...admittedSender, id: "session:spoofed-recipient" },
    } }) + "\n");
    close(handle.id); await manager.wait(handle.id, { timeoutMs: 1000 });
    await waitUntil(() => notices.length === 1);
    expect(notices[0]!.sender).toEqual(admittedSender);
    expect(notices.some(notice => notice.sender?.id === "session:spoofed-recipient")).toBe(false);
  });

  it("retries a failed sender route on status and retains an acknowledged notice for collection", async () => {
    let attempts = 0, acknowledged = 0;
    const routed: AgentTerminalNotice[] = [];
    const { manager, root, launch, close, finish } = harness(notice => {
      routed.push(notice);
      attempts++;
      if (attempts === 1) throw new Error("sender temporarily unavailable");
      acknowledged++;
    });
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const,
      sender: { id: "session:sender", kind: "main" as const, verified: "mesh" as const } };
    const queued = manager.steer(handle.id, "queued", undefined, provenance);
    writeJsonAtomic(path.join(root, handle.id, "terminal-controls", `${queued.messageId}.json`), {
      id: queued.messageId, delivery: "steer", state: "refused", forwarded: true, provenance,
    });
    const receipt = finish(handle.id, "done");
    const next = await manager.spawn({ task: "native-close barrier", transport: "process" });
    expect(next.status).toBe("queued");
    const outbox = path.join(root, handle.id, "terminal-notices", `${queued.messageId}.json`);
    manager.status(handle.id);
    await waitUntil(() => attempts === 1 && fs.existsSync(outbox));
    expect(manager.status(handle.id).status).toBe("finishing");
    expect(canRemoveTerminalRun(path.join(root, handle.id))).toBe(false);
    await waitUntil(() => fs.existsSync(`${outbox}.delivered`));
    close(handle.id); await manager.wait(handle.id, { timeoutMs: 1000 });
    expect(acknowledged).toBe(1); expect(attempts).toBe(2);
    const expectedNotice = { code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: receipt.id, messageId: queued.messageId, delivery: "steer", sender: provenance.sender };
    expect(routed).toEqual([expectedNotice, expectedNotice]);
    manager.status(handle.id);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(attempts).toBe(2);
    expect(canRemoveTerminalRun(path.join(root, handle.id))).toBe(true);
    // Sender routing can complete before the independently owned native drain.
    // The queued next launch proves the first run released native admission.
    await waitUntil(() => launch.mock.calls.length === 2);
    close(next.id);
    expect((await manager.cleanup(handle.id)).cleaned).toBe(false); // retained run, no worktree
  });

  it("does not change non-Pi process terminal semantics", async () => {
    const { manager, root, close } = harness();
    const handle = await manager.spawn({ task: "Claude", runner: "claude", transport: "process" });
    saveFinalAnswerReceipt(path.join(root, handle.id), handle.id, "not a native Pi receipt");
    expect(manager.status(handle.id).status).toBe("running");
    expect(manager.isSettled(handle.id)).toBe(false);
    expect(manager.steer(handle.id, "ordinary Claude control").queued).toBe(true);
    close(handle.id);
  });

  it("never settles actor continuations from a raw task receipt", async () => {
    const { manager, root, close } = harness();
    const handle = await manager.spawn({ task: "activation", actorId: "actor-one", transport: "process" });
    saveFinalAnswerReceipt(path.join(root, handle.id), handle.id, "actor turn answer");
    manager.status(handle.id);
    expect(manager.isSettled(handle.id)).toBe(false);
    expect(manager.steer(handle.id, "actor continuation").queued).toBe(true);
    close(handle.id);
  });
});
