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

const harness = (onTerminalNotice?: (notice: AgentTerminalNotice) => void | Promise<void>) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-manager-")); roots.push(root);
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
  it("releases wait and run exactly once on receipt while native admission and custody remain held", async () => {
    const { manager, settled, launch, close, finish } = harness();
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const receipt = finish(handle.id, "final answer");
    const result = await manager.wait(handle.id, { timeoutMs: 1000 });
    expect(result).toMatchObject({ status: "completed", text: "final answer", finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt }, sessionId: "99999999" });
    expect(await manager.wait(handle.id)).toMatchObject({ finalAnswerReceipt: { id: receipt.id } });
    expect(settled).toHaveBeenCalledOnce();
    expect(manager.hasRunCustody(handle.id)).toBe(true);
    await expect(manager.cleanup(handle.id)).rejects.toThrow(/teardown is pending/);
    const next = await manager.spawn({ task: "next", transport: "process" });
    expect(next.status).toBe("queued"); expect(launch).toHaveBeenCalledTimes(1);
    close(handle.id);
    await waitUntil(() => launch.mock.calls.length === 2);
    close(next.id);
  });

  it("run returns on receipt, not the owned execution close", async () => {
    const { manager, launch, settled, finish } = harness();
    const run = manager.run({ task: "foreground", transport: "process" });
    await waitUntil(() => launch.mock.calls.length === 1);
    const id = launch.mock.calls[0]![0].id;
    finish(id, "foreground result");
    expect(await run).toMatchObject({ status: "completed", text: "foreground result" });
    expect(settled).toHaveBeenCalledOnce();
    expect(manager.hasRunCustody(id)).toBe(true);
  });

  it("does not kill a plain task before its validated receipt frame/status lands", async () => {
    const { manager, root, finish } = harness();
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const receipt = saveFinalAnswerReceipt(path.join(root, handle.id), handle.id, "plain final");
    expect(manager.status(handle.id)).toMatchObject({ status: "running", finalAnswerReceipt: { id: receipt.id } });
    expect(manager.isSettled(handle.id)).toBe(false);
    await expect(manager.wait(handle.id, { timeoutMs: 20 })).rejects.toThrow(/still running|timed out/i);
    finish(handle.id, "plain final");
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
    const { manager, root, records } = harness();
    const handle = await manager.spawn({ task: "structured", schema: { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] }, replyTool: true, transport: "process" });
    const receipt = saveFinalAnswerReceipt(path.join(root, handle.id), handle.id, "invalid report");
    manager.status(handle.id);
    expect(manager.isSettled(handle.id)).toBe(false);
    const record = { ...records.get(handle.id)!, status: "failed", error: "Structured agent output was invalid", replyVia: "tool", value: { ok: 42 },
      finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } };
    writeJsonAtomic(path.join(root, handle.id, "status.json"), record);
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
    await manager.wait(handle.id, { timeoutMs: 1000 }); close(handle.id);
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
  ])("refuses a mismatched delivered %s row through %s exactly once to the original sender", async (mismatch, source) => {
    const notices: AgentTerminalNotice[] = [];
    const { manager, root, close, finish } = harness(notice => { notices.push(notice); });
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
      // Prove the lifecycle skip site independently, before the unread exit scan.
      await waitUntil(() => notices.length === 1);
      expect(manager.hasRunCustody(handle.id)).toBe(true);
    }
    await manager.wait(handle.id, { timeoutMs: 1000 });
    close(handle.id);
    const delivered = path.join(directory, "terminal-notices", `${queued.messageId}.json.delivered`);
    await waitUntil(() => fs.existsSync(delivered));
    manager.status(handle.id);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(notices).toEqual([{ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: receipt.id, messageId: queued.messageId, delivery: "steer", sender }]);
    expect(canRemoveTerminalRun(directory)).toBe(true);
  });

  it("preserves the original followUp kind when the delivered row wrongly names steer", async () => {
    const notices: AgentTerminalNotice[] = [];
    const { manager, root, close, finish } = harness(notice => { notices.push(notice); });
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const sender = { id: "session:followUp-sender", kind: "main" as const, verified: "mesh" as const };
    const provenance = { v: 1 as const, channel: "fabric" as const, via: "followUp" as const, sender };
    const queued = manager.followUp(handle.id, "original followUp", undefined, provenance, { deadlineMs: 60_000 });
    const directory = path.join(root, handle.id);
    writeJsonAtomic(path.join(directory, "terminal-controls", `${queued.messageId}.json`), {
      id: queued.messageId, delivery: "steer", state: "delivered", provenance,
    });
    const receipt = finish(handle.id, "done");
    await manager.wait(handle.id, { timeoutMs: 1000 }); close(handle.id);
    await waitUntil(() => fs.existsSync(path.join(directory, "terminal-notices", `${queued.messageId}.json.delivered`)));
    expect(notices).toEqual([{ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: receipt.id, messageId: queued.messageId, delivery: "followUp", sender }]);
    expect(manager.status(handle.id).followUpDeliveries).toContainEqual(expect.objectContaining({ messageId: queued.messageId, state: "cancelled" }));
    expect(canRemoveTerminalRun(directory)).toBe(true);
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
    await manager.wait(handle.id, { timeoutMs: 1000 }); close(handle.id);
    await waitUntil(() => notices.length === 1);
    expect(notices[0]!.sender).toEqual(admittedSender);
    expect(notices.some(notice => notice.sender?.id === "session:spoofed-recipient")).toBe(false);
  });

  it("retries a failed sender route on status and retains an acknowledged notice for collection", async () => {
    let attempts = 0, acknowledged = 0;
    const { manager, root, close, finish } = harness(() => {
      attempts++;
      if (attempts === 1) throw new Error("sender temporarily unavailable");
      acknowledged++;
    });
    const handle = await manager.spawn({ task: "ordinary", transport: "process" });
    const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const,
      sender: { id: "session:sender", kind: "main" as const, verified: "mesh" as const } };
    const queued = manager.steer(handle.id, "queued", undefined, provenance);
    writeJsonAtomic(path.join(root, handle.id, "terminal-controls", `${queued.messageId}.json`), {
      id: queued.messageId, delivery: "followUp", state: "delivered",
      provenance: { ...provenance, sender: { ...provenance.sender, id: "session:wrong-sender" } },
    });
    finish(handle.id, "done");
    await manager.wait(handle.id, { timeoutMs: 1000 }); close(handle.id);
    const outbox = path.join(root, handle.id, "terminal-notices", `${queued.messageId}.json`);
    await waitUntil(() => attempts === 1 && fs.existsSync(outbox));
    expect(canRemoveTerminalRun(path.join(root, handle.id))).toBe(false);
    manager.status(handle.id);
    await waitUntil(() => fs.existsSync(`${outbox}.delivered`));
    expect(acknowledged).toBe(1); expect(attempts).toBe(2);
    manager.status(handle.id);
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(attempts).toBe(2);
    expect(canRemoveTerminalRun(path.join(root, handle.id))).toBe(true);
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
