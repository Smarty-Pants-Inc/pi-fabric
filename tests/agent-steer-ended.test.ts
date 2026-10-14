import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";
import { readTerminalControl, settleTerminalControl } from "../src/worker/terminal-controls.js";
import { TerminalNoticeOutbox } from "../src/agents/terminal-notices.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import type { AgentTerminalNotice } from "../src/agents/terminal-target.js";

// Decision C: only the exact manager pre-check/append instruction interleaving
// is mocked. E-steer-final exercises real process transport and dist worker.
const managers: AgentManager[] = [];
afterEach(async () => { await Promise.all(managers.splice(0).map(manager => manager.close())); vi.restoreAllMocks(); });
const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const,
  sender: { id: "session:original-sender", name: "Original", kind: "main" as const, verified: "mesh" as const } };
const harness = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "steer-ended-manager-"));
  const notices: AgentTerminalNotice[] = [];
  let record: AgentRunRecord;
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    record = { id: request.id, name: request.name, task: "task", runner: "pi", transport: "process", status: "running",
      cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, text: "",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
    writeJsonAtomic(path.join(root, request.id, "status.json"), record);
    let alive = true;
    return { kind: "process", isAlive: async () => alive, stop: async () => { alive = false; } };
  });
  const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: root, onTerminalNotice: notice => { notices.push(notice); },
  });
  managers.push(manager);
  const finish = (id: string) => {
    const receipt = saveFinalAnswerReceipt(path.join(root, id), id, "done");
    writeJsonAtomic(path.join(root, id, "status.json"), { ...record, status: "completed", text: "done",
      finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } });
    return receipt;
  };
  return { root, notices, launch, manager, finish };
};

describe("manager final-answer admission race", () => {
  it.each(["steer", "followUp"] as const)("%s reports an undelivered receipt when final is published during append", async delivery => {
    const h = harness();
    const handle = await h.manager.spawn({ task: "ordinary", transport: "process" });
    const journal = path.join(h.root, handle.id, "steer.jsonl");
    const append = fs.appendFileSync;
    let final: ReturnType<typeof h.finish> | undefined;
    vi.spyOn(fs, "appendFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: string | Uint8Array, options?: fs.WriteFileOptions) => {
      if (file === journal && !final) final = h.finish(handle.id);
      return append(file, data, options);
    }) as typeof fs.appendFileSync);
    const sent = delivery === "steer" ? h.manager.steer(handle.id, "racing input", undefined, provenance)
      : h.manager.followUp(handle.id, "racing input", undefined, { ...provenance, via: "followUp" }, { deadlineMs: 60_000 });
    expect(sent.undelivered).toEqual({ code: "FABRIC_TARGET_TERMINAL", reason: "run-ended", deliveryStatus: "undelivered",
      messageId: sent.messageId, delivery, finalAnswerReceiptId: final!.id, sender: provenance.sender });
    if (delivery === "followUp") expect(h.manager.status(handle.id).followUpDeliveries).toMatchObject([{ state: "cancelled" }]);
    expect(await h.manager.wait(handle.id)).toMatchObject({ status: "completed", text: "done", turns: 1 });
    await h.manager.close();
    expect(h.notices).toEqual([{ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id, finalAnswerReceiptId: final!.id,
      messageId: sent.messageId, delivery, sender: provenance.sender }]);
    expect(h.launch).toHaveBeenCalledTimes(1);
  });

  it("counterexample: an exactly attributed control consumed before final is not refused", async () => {
    const h = harness();
    const handle = await h.manager.spawn({ task: "ordinary", transport: "process" });
    const sent = h.manager.steer(handle.id, "consumed", undefined, provenance);
    expect(sent.undelivered).toBeUndefined();
    writeJsonAtomic(path.join(h.root, handle.id, "terminal-controls", `${sent.messageId}.json`),
      { id: sent.messageId, delivery: "steer", state: "delivered", provenance });
    h.finish(handle.id);
    expect(await h.manager.wait(handle.id)).toMatchObject({ status: "completed", turns: 1 });
    await h.manager.close();
    expect(h.notices).toEqual([]);
  });

  // Keep reason: data-safety/trust boundary. Native close cannot prove that a
  // forwarded frame was discarded; recovery must not invent sender receipts.
  it.each(["queued", "refused"] as const)("forwarded control with %s native ACK preserves its exact evidence through close and outbox recovery", async state => {
    const h = harness();
    const handle = await h.manager.spawn({ task: "ordinary", transport: "process" });
    const directory = path.join(h.root, handle.id);
    const sent = h.manager.steer(handle.id, "possibly consumed native input", undefined, provenance);
    expect(sent.undelivered).toBeUndefined();
    writeJsonAtomic(path.join(directory, "terminal-controls", `${sent.messageId}.json`),
      { id: sent.messageId, delivery: "steer", state: "queued", forwarded: true, provenance });
    if (state === "refused") settleTerminalControl(directory, sent.messageId, "refused"); // Confirmed native discard ACK.
    const final = h.finish(handle.id);
    expect(await h.manager.wait(handle.id)).toMatchObject({ status: "completed", turns: 1 });
    await h.manager.close();
    const notice: AgentTerminalNotice = { code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
      finalAnswerReceiptId: final.id, messageId: sent.messageId, delivery: "steer", sender: provenance.sender };
    expect(h.notices).toEqual(state === "refused" ? [notice] : []);
    expect(readTerminalControl(directory, sent.messageId)).toMatchObject({ state, forwarded: true });

    // Independent restart fixture has NO outbox or delivery receipt. Preserve
    // the original admission and terminal registry exactly; no deletes needed.
    const restartRoot = fs.mkdtempSync(path.join(os.tmpdir(), "steer-ended-outbox-recovery-"));
    const restartDirectory = path.join(restartRoot, handle.id);
    fs.mkdirSync(restartDirectory);
    fs.copyFileSync(path.join(directory, "steer.jsonl"), path.join(restartDirectory, "steer.jsonl"));
    fs.mkdirSync(path.join(restartDirectory, "terminal-controls"));
    fs.copyFileSync(path.join(directory, "terminal-controls", `${sent.messageId}.json`),
      path.join(restartDirectory, "terminal-controls", `${sent.messageId}.json`));
    fs.copyFileSync(path.join(directory, "final-answer.json"), path.join(restartDirectory, "final-answer.json"));
    fs.copyFileSync(path.join(directory, "status.json"), path.join(restartDirectory, "status.json"));
    const recovered: AgentTerminalNotice[] = [];
    const outbox = new TerminalNoticeOutbox(replayed => { recovered.push(replayed); });
    outbox.recover(restartDirectory);
    // Outbox delivery is a Promise microtask chain, not an event/timer poll.
    for (let i = 0; i < 8; i++) await Promise.resolve();
    expect(recovered).toEqual(state === "refused" ? [notice] : []);
    expect(readTerminalControl(restartDirectory, sent.messageId)).toMatchObject({ state, forwarded: true });
    expect(fs.existsSync(path.join(restartDirectory, "terminal-notices", `${sent.messageId}.json.delivered`))).toBe(state === "refused");
    outbox.recover(restartDirectory);
    for (let i = 0; i < 8; i++) await Promise.resolve();
    expect(recovered).toEqual(state === "refused" ? [notice] : []); // No invented refusal or restart duplicate.
  });
});
