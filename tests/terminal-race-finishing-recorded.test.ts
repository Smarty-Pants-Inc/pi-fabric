import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import type { AgentSession, ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { installTerminalAnswerBoundary } from "../src/worker/terminal-boundary.js";
import principalDelivery from "../src/worker/principal-delivery.js";
import { readFinalAnswerReceipt } from "../src/worker/terminal-answer.js";
import { markTerminalControlForwarded, readTerminalControl, trackTerminalControl } from "../src/worker/terminal-controls.js";
import type { AgentTerminalNotice } from "../src/agents/terminal-target.js";

// Keep: #722 recorded catch, data safety and exact sender attribution. The real
// manager and production CHILD discard gate run; native close is a recorded
// receipt. Product deletions are recorded, not executed; only this fixture's
// ephemeral final-answer lock releases remain real. This is not collector proof.
vi.mock("node:child_process", async () => {
  const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
  return { ...actual, spawn: vi.fn(() => { throw new Error("Unexpected real child launch in terminal-race recorded probe"); }) };
});

const provenance = { v: 1 as const, channel: "fabric" as const, via: "steer" as const,
  sender: { id: "session:recorded-terminal-sender", name: "Original sender", kind: "main" as const, verified: "mesh" as const } };

const setup = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-race-finishing-recorded-"));
    const terminalListeners = new Set(process.listeners("fabric-terminal-answer"));
    const deletions: string[] = [];
    const remove = fs.rmSync.bind(fs);
    vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (typeof target === "string" && target.startsWith(root + path.sep) && path.basename(target) === ".task-final-answer.lock") {
        remove(target, options); // Real release of this fixture's ephemeral lock.
      } else deletions.push(String(target));
    });
    vi.spyOn(fs, "unlinkSync").mockImplementation(target => { deletions.push(String(target)); });
    vi.spyOn(fs.promises, "rm").mockImplementation(async target => { deletions.push(String(target)); });
    vi.spyOn(process, "kill").mockImplementation(() => { throw new Error("Unexpected real signal in terminal-race recorded probe"); });
    let alive = true;
    let nativeClose!: () => void;
    const closed = new Promise<void>(resolve => { nativeClose = () => { alive = false; resolve(); }; });
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const args = new Map<string, string>();
      for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
      writeJsonAtomic(args.get("--status-file")!, { id: request.id, name: request.name, task: "recorded final race",
        status: "running", runner: "pi", transport: "process", cwd: request.cwd,
        startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, text: "progress",
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
      return { kind: "process", closed, waitForClose: () => closed, isAlive: async () => alive,
        stop: async () => { await closed; } };
    });
    const notices: AgentTerminalNotice[] = [];
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
      onTerminalNotice: notice => { notices.push(notice); },
    });
    return { root, deletions, manager, notices, nativeClose, close: async () => {
      nativeClose();
      try { await manager.close(); } finally {
        for (const listener of process.listeners("fabric-terminal-answer")) {
          if (!terminalListeners.has(listener)) process.removeListener("fabric-terminal-answer", listener);
        }
        vi.unstubAllEnvs(); vi.restoreAllMocks();
      }
      // Retain the fixture. No product cleanup/deletion is authorized by this probe.
    } };
};

describe("recorded terminal race finishing custody (#722)", () => {
  it("keeps final-answer wait pending through child discard ACK until exact captured close", async () => {
    const { root, deletions, manager, notices, nativeClose, close } = setup();
    let waiting: ReturnType<AgentManager["wait"]> | undefined;
    try {
      const handle = await manager.spawn({ task: "recorded final race", transport: "process" });
      const directory = manager.runDirectory(handle.id)!;
      const admitted = manager.steer(handle.id, "held prefinal input", undefined, provenance);
      // Model only the worker's write-ahead stdin evidence, never a discard ACK.
      trackTerminalControl(directory, { id: admitted.messageId, delivery: "steer", provenance });
      expect(markTerminalControlForwarded(directory, admitted.messageId)).toBe(true);
      const listeners: Array<(event: any) => void> = [];
      const session = {
        clearQueue: () => ({ steering: [], followUp: [] }),
        prompt: async () => {}, steer: async () => {}, followUp: async () => {},
        agent: { state: { messages: [] }, subscribe: (listener: (event: any) => void) => { listeners.push(listener); },
          hasQueuedMessages: () => false, prompt: async () => {}, continue: async () => {},
          steer: () => {}, followUp: () => {}, finishTurn: async () => ({ action: "end" }) },
      } as unknown as AgentSession;
      installTerminalAnswerBoundary(session, directory, handle.id, () => {});
      for (const listener of listeners) listener({ type: "message_end", message: {
        role: "assistant", content: [{ type: "text", text: "recorded authorized final" }], stopReason: "stop",
      } });
      const receipt = readFinalAnswerReceipt(directory, handle.id)!;
      expect(receipt).toBeDefined();
      const statusFile = path.join(directory, "status.json");
      const snapshot = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      writeJsonAtomic(statusFile, { ...snapshot, status: "completed", text: receipt.text,
        finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt }, finishedAt: receipt.recordedAt });
      waiting = manager.wait(handle.id, { timeoutMs: 3000 });
      // Observe failures immediately, while preserving the original result join.
      void waiting.catch(() => undefined);
      expect(manager.status(handle.id)).toMatchObject({ status: "finishing", finalAnswerReceipt: { id: receipt.id } });
      expect(manager.isSettled(handle.id)).toBe(false);
      expect(readTerminalControl(directory, admitted.messageId)).toMatchObject({ state: "queued", forwarded: true, provenance });
      expect(notices).toEqual([]);
      expect(() => manager.steer(handle.id, "late input", undefined, provenance)).toThrow(expect.objectContaining({
        code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: receipt.id,
      }));
      // Release the held frame through the PRODUCTION child boundary. A thrown
      // terminal error is acceptable only with this exact durable discard ACK.
      await expect(session.prompt(`/fabric-delivery ${admitted.messageId}`)).rejects.toMatchObject({ code: "FABRIC_TARGET_TERMINAL" });
      expect(readTerminalControl(directory, admitted.messageId)).toMatchObject({ state: "refused", forwarded: true, provenance });
      expect(manager.isSettled(handle.id)).toBe(false); // ACK alone is not exit.
      nativeClose();
      expect(await waiting).toMatchObject({ status: "completed", text: receipt.text, finalAnswerReceipt: { id: receipt.id } });
      await vi.waitFor(() => expect(notices).toEqual([{ code: "FABRIC_TARGET_TERMINAL", targetId: handle.id,
        finalAnswerReceiptId: receipt.id, messageId: admitted.messageId, delivery: "steer", sender: provenance.sender }]));
      expect(manager.isSettled(handle.id)).toBe(true);
      console.log(`terminal-race-finishing recorded: root=${root} pendingBeforeClose=true childDiscardAck=true exactSender=true recordedDeletions=${deletions.length}`);
    } finally { await close(); await waiting?.catch(() => undefined); }
  });

  it("preserves production prefinal consumption and a fake native action through a held RPC response", async () => {
    const { root, deletions, manager, notices, nativeClose, close } = setup();
    let waiting: ReturnType<AgentManager["wait"]> | undefined;
    try {
      const handle = await manager.spawn({ task: "recorded prefinal action", transport: "process" });
      const directory = manager.runDirectory(handle.id)!;
      const message = "WRITE PREFINAL_WRITE_SENTINEL";
      const admitted = manager.steer(handle.id, message, undefined, provenance);
      expect(admitted).toMatchObject({ queued: true });
      trackTerminalControl(directory, { id: admitted.messageId, delivery: "steer", provenance });
      expect(markTerminalControlForwarded(directory, admitted.messageId)).toBe(true);
      const deliveries = path.join(directory, "deliveries");
      const envelope = path.join(deliveries, admitted.messageId + ".json");
      writeJsonAtomic(envelope, { message, provenance, delivery: "steer", controlId: admitted.messageId });
      vi.stubEnv("PI_FABRIC_DELIVERY_DIR", deliveries); vi.stubEnv("PI_FABRIC_TERMINAL_TASK", "1"); vi.stubEnv("PI_FABRIC_PARENT_RUN", handle.id);
      const hooks = new Map<string, Array<(event: any, ctx: any) => any>>();
      const commands = new Map<string, { handler: (id: string, ctx: any) => Promise<void> }>();
      const listeners: Array<(event: any) => void> = [];
      const nativeQueue: any[] = [];
      const actions: Array<{ message: string; afterReceipt: boolean }> = [];
      const ctx = { isIdle: () => false };
      const session = {
        clearQueue: () => ({ steering: nativeQueue.splice(0), followUp: [] }),
        prompt: async (text: string) => { await commands.get("fabric-delivery")!.handler(text.slice("/fabric-delivery ".length), ctx); },
        steer: async () => {}, followUp: async () => {},
        agent: { state: { messages: [] }, subscribe: (listener: (event: any) => void) => { listeners.push(listener); },
          hasQueuedMessages: () => nativeQueue.length > 0, prompt: async () => {}, continue: async () => {},
          steer: (message: any) => { nativeQueue.push(message); }, followUp: () => {}, finishTurn: async () => ({ action: "end" }) },
      } as unknown as AgentSession;
      principalDelivery({ hostCapabilities: { turnProvenance: 1 },
        on: (name: string, hook: (event: any, ctx: any) => any) => { hooks.set(name, [...(hooks.get(name) ?? []), hook]); },
        registerCommand: (name: string, command: any) => { commands.set(name, command); },
        sendUserMessage: (content: any, options: any) => {
          expect(options).toMatchObject({ deliverAs: "steer", provenance });
          session.agent.steer({ role: "user", content: typeof content === "string" ? [{ type: "text", text: content }] : content, timestamp: Date.now() });
        },
      } as unknown as ExtensionAPI);
      const emit = async (event: any) => {
        for (const hook of hooks.get(event.type) ?? []) { const result = await hook(event, ctx); if (result?.message) event = { ...event, message: result.message }; }
        for (const listener of listeners) await listener(event);
        return event;
      };
      await emit({ type: "session_start" });
      installTerminalAnswerBoundary(session, directory, handle.id, () => {});
      await session.prompt(`/fabric-delivery ${admitted.messageId}`);
      expect(deletions).toContain(envelope); expect(fs.existsSync(envelope)).toBe(true);
      expect(readTerminalControl(directory, admitted.messageId)).toMatchObject({ state: "queued", forwarded: true, provenance });
      const consumed = await emit({ type: "message_end", message: nativeQueue.shift() });
      session.agent.state.messages.push(consumed.message);
      let messages = [consumed.message];
      for (const hook of hooks.get("context") ?? []) messages = (await hook({ messages }, ctx))?.messages ?? messages;
      for (const item of messages) actions.push({ message: item.content[0].text, afterReceipt: Boolean(readFinalAnswerReceipt(directory, handle.id)) });
      expect(actions).toEqual([{ message, afterReceipt: false }]);
      expect(readTerminalControl(directory, admitted.messageId)).toMatchObject({ state: "delivered", forwarded: true, provenance });
      // Like fake-pi-steer-ended, hold the RPC response AFTER consumption/action.
      const heldResponse = { type: "response", id: admitted.messageId, command: "prompt", success: true };
      let releaseResponse!: (response: typeof heldResponse) => void;
      const response = new Promise<typeof heldResponse>(resolve => { releaseResponse = resolve; });
      let responseReleased = false; void response.then(() => { responseReleased = true; });
      await emit({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "recorded authorized final" }], stopReason: "stop" } });
      const receipt = readFinalAnswerReceipt(directory, handle.id)!;
      const statusFile = path.join(directory, "status.json");
      writeJsonAtomic(statusFile, { ...JSON.parse(fs.readFileSync(statusFile, "utf8")), status: "completed", text: receipt.text,
        finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt }, finishedAt: receipt.recordedAt });
      waiting = manager.wait(handle.id, { timeoutMs: 3000 }); void waiting.catch(() => undefined);
      expect(manager.status(handle.id)).toMatchObject({ status: "finishing", finalAnswerReceipt: { id: receipt.id } });
      expect(manager.isSettled(handle.id)).toBe(false); expect(responseReleased).toBe(false); expect(notices).toEqual([]);
      expect(readTerminalControl(directory, admitted.messageId)).toMatchObject({ state: "delivered", provenance });
      const journal = path.join(directory, "steer.jsonl"), before = fs.readFileSync(journal, "utf8");
      expect(() => manager.steer(handle.id, "WRITE SOURCE_WRITE_SENTINEL", undefined, provenance)).toThrow(expect.objectContaining({
        code: "FABRIC_TARGET_TERMINAL", deliveryStatus: "undelivered", finalAnswerReceiptId: receipt.id,
      }));
      expect(fs.readFileSync(journal, "utf8")).toBe(before);
      releaseResponse(heldResponse); expect(await response).toEqual(heldResponse); expect(responseReleased).toBe(true);
      expect(readTerminalControl(directory, admitted.messageId)).toMatchObject({ state: "delivered", provenance });
      expect(manager.isSettled(handle.id)).toBe(false); expect(notices).toEqual([]); // Response is not native exit.
      nativeClose();
      expect(await waiting).toMatchObject({ status: "completed", text: receipt.text, finalAnswerReceipt: { id: receipt.id } });
      await manager.close();
      expect(manager.isSettled(handle.id)).toBe(true); expect(notices).toEqual([]);
      expect(actions).toEqual([{ message, afterReceipt: false }]);
      expect(readTerminalControl(directory, admitted.messageId)).toMatchObject({ state: "delivered", forwarded: true, provenance });
      const lifecycle = path.join(directory, "lifecycle.jsonl");
      expect(fs.existsSync(lifecycle) ? fs.readFileSync(lifecycle, "utf8") : "").not.toContain('"event":"message.refused"');
      console.log(`terminal-race-finishing prefinal: root=${root} productionConsumed=true fakeActionBeforeReceipt=true heldResponseNotRefusal=true exactClose=true undeliveredNotice=false`);
    } finally { await close(); await waiting?.catch(() => undefined); }
  });
});
