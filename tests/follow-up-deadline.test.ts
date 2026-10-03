import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import principalDelivery from "../src/worker/principal-delivery.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  for (const manager of managers.splice(0)) await manager.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.unstubAllEnvs();
});
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (check: () => boolean) => {
  const until = Date.now() + 4000;
  while (!check()) { if (Date.now() > until) throw new Error("deadline probe timed out"); await sleep(20); }
};
async function setup(deadlineMs = 80) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-follow-up-deadline-")); roots.push(root);
  const alarm = vi.fn();
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    onFollowUpAlarm: alarm,
  }); managers.push(manager);
  const handle = await manager.spawn({ task: "HANG", name: "busy-receiver", transport: "process" });
  const run = manager.runDirectory(handle.id)!;
  await wait(() => fs.existsSync(path.join(run, "status.json")));
  const record = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
  record.currentTool = "bash"; record.currentToolStartedAt = Date.now() - 5000;
  fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record));
  const receipt = manager.followUp(handle.id, "unique follow-up", undefined, undefined, { deadlineMs });
  const directory = path.join(run, "deliveries"); fs.mkdirSync(directory, { recursive: true });
  vi.stubEnv("PI_FABRIC_DELIVERY_DIR", directory);
  const handlers = new Map<string, (...args: any[]) => any>();
  const sendUserMessage = vi.fn();
  principalDelivery({
    registerCommand: (_name: string, command: any) => handlers.set("command", command.handler),
    on: (name: string, handler: any) => handlers.set(name, handler), sendUserMessage,
  } as unknown as ExtensionAPI);
  const ingest = async () => {
    fs.writeFileSync(path.join(directory, receipt.messageId + ".json"), JSON.stringify({
      message: "unique follow-up", delivery: "followUp", followUpId: receipt.messageId,
      provenance: { v: 1, channel: "fabric", via: "followUp", sender: { id: "sender", kind: "main", verified: "mesh" } },
    }));
    await handlers.get("command")!(receipt.messageId, { isIdle: () => false });
  };
  const boundary = async (aborted = false) => {
    await handlers.get("turn_end")?.({ outcome: aborted ? "aborted" : "completed" },
      { isIdle: () => false, signal: aborted ? AbortSignal.abort() : undefined });
  };
  const deliveries = () => (manager.status(handle.id) as any).followUpDeliveries;
  return { manager, handle, run, receipt, alarm, ingest, boundary, sendUserMessage, deliveries };
}

describe("task follow-up delivery deadlines", () => {
  it("delivered before deadline: no alarm, no duplicate at later boundaries", async () => {
    const p = await setup(500); await p.ingest();
    expect(p.sendUserMessage).not.toHaveBeenCalled();
    await p.boundary(); await p.boundary(); await sleep(600);
    expect(p.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(p.deliveries()[0]).toMatchObject({ messageId: p.receipt.messageId, state: "delivered" });
    expect(p.alarm).not.toHaveBeenCalled();
  });
  it("late: one sender alarm, retained queue, delivered exactly once", async () => {
    const p = await setup(); await p.ingest();
    await wait(() => p.alarm.mock.calls.length > 0);
    expect(p.alarm).toHaveBeenCalledTimes(1);
    expect(p.alarm.mock.calls[0]![0]).toMatchObject({
      code: "FABRIC_FOLLOW_UP_DEADLINE", targetId: p.handle.id, targetName: "busy-receiver",
      status: "running", currentTool: "bash", currentToolStartedAt: expect.any(Number),
      options: ["wait", "steer", "cancel"], messageId: p.receipt.messageId,
    });
    expect(p.deliveries()[0]).toMatchObject({ state: "queued", alarm: { code: "FABRIC_FOLLOW_UP_DEADLINE" } });
    await sleep(400); p.deliveries(); expect(p.alarm).toHaveBeenCalledTimes(1);
    await p.boundary(); await p.boundary();
    expect(p.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(p.deliveries()[0]).toMatchObject({ state: "delivered", alarm: { code: "FABRIC_FOLLOW_UP_DEADLINE" } });
  });
  it("cancelled after alarm: never delivered, repeated cancellation is idempotent", async () => {
    const p = await setup(); await p.ingest(); await wait(() => p.alarm.mock.calls.length > 0);
    expect(p.manager.cancelFollowUp(p.handle.id, p.receipt.messageId)).toMatchObject({ state: "cancelled" });
    expect(p.manager.cancelFollowUp(p.handle.id, p.receipt.messageId)).toMatchObject({ state: "cancelled" });
    await p.boundary(); await p.boundary();
    expect(p.sendUserMessage).not.toHaveBeenCalled();
    expect(p.deliveries()[0]).toMatchObject({ state: "cancelled" });
  });
  it("cancellation before worker ingestion fences delivery", async () => {
    const p = await setup(); p.manager.cancelFollowUp(p.handle.id, p.receipt.messageId);
    await p.ingest(); await p.boundary();
    expect(p.sendUserMessage).not.toHaveBeenCalled();
    await sleep(300); expect(p.alarm).not.toHaveBeenCalled();
  });
  it("cannot cancel an already delivered message or a foreign id", async () => {
    const p = await setup(500); await p.ingest(); await p.boundary();
    expect(p.manager.cancelFollowUp(p.handle.id, p.receipt.messageId)).toMatchObject({ state: "delivered" });
    expect(() => p.manager.cancelFollowUp(p.handle.id, "foreign")).toThrow(/Unknown follow-up/);
  });
  it("rejects invalid delivery deadlines before queue mutation", async () => {
    const p = await setup(); const file = path.join(p.run, "steer.jsonl");
    const before = fs.readFileSync(file, "utf8");
    for (const deadlineMs of [0, -1, NaN, Infinity, 0.5]) {
      expect(() => p.manager.followUp(p.handle.id, "bad", undefined, undefined, { deadlineMs })).toThrow(/deadlineMs/);
    }
    expect(fs.readFileSync(file, "utf8")).toBe(before);
  });
  it("alarms an undelivered message even when the receiver settles before the deadline", async () => {
    const p = await setup(500);
    await p.manager.stop(p.handle.id);
    await wait(() => p.alarm.mock.calls.length > 0);
    expect(p.alarm).toHaveBeenCalledTimes(1);
    expect(p.alarm.mock.calls[0]![0]).toMatchObject({ status: "stopped", messageId: p.receipt.messageId });
  });
  it("does not report delivery at an aborted boundary", async () => {
    const p = await setup(); await p.ingest(); await p.boundary(true);
    expect(p.sendUserMessage).not.toHaveBeenCalled();
    await wait(() => p.alarm.mock.calls.length > 0);
    expect(p.deliveries()[0]).toMatchObject({ state: "queued" });
    await p.boundary(); expect(p.sendUserMessage).toHaveBeenCalledTimes(1);
  });
  it("registers public deadline and cancellation schemas", () => {
    const follow = AGENTS_ACTION_DESCRIPTORS.find(d => d.name === "followUp")!;
    expect((follow.inputSchema as any).properties.deadlineMs).toMatchObject({ type: "integer", minimum: 1 });
    expect(AGENTS_ACTION_DESCRIPTORS.find(d => d.name === "cancelFollowUp")?.inputSchema).toMatchObject({ required: ["id", "messageId"], additionalProperties: false });
  });
});
