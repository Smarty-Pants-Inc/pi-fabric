import fs from "node:fs";
import { StringDecoder } from "node:string_decoder";
import type { Readable } from "node:stream";
import { writeFileAtomic } from "../core/atomic-write.js";
import { REPLY_TOOL_NAME } from "../core/reply-tool-identity.js";
import { followUpFile, followUpState } from "../agents/follow-up-delivery.js";
import { queuedTerminalControls, settleTerminalControl, terminalControlInputId } from "./terminal-controls.js";
import type { AgentSession } from "@earendil-works/pi-coding-agent";
import { assistantAnswerEvidence, finalAssistantText, readFinalAnswerReceipt, saveFinalAnswerReceipt, type FinalAnswerReceipt } from "./terminal-answer.js";

/** Task-only ingress fence, installed before native RPC attaches its JSONL reader.
 * Session wrappers alone cannot fence compact/queue-mode/UI RPC handlers. Filter
 * complete frames at the child read boundary, including buffered bytes after EOF.
 * The host's stdin.write is never a consumption ACK. */
export const installTerminalControlFrameFence = (input: Readable, runDirectory: string, runId: string): void => {
  const emit = input.emit.bind(input);
  const decoder = new StringDecoder("utf8");
  let pending = "";
  const frame = (line: string): void => {
    if (!readFinalAnswerReceipt(runDirectory, runId)) { emit("data", Buffer.from(line + "\n")); return; }
    // Every post-receipt frame is discarded, not just identified user input.
    // IDs ACK only the existing steer/follow-up registry, never an invented row.
    try {
      const command = JSON.parse(line) as { type?: string; id?: unknown; message?: unknown } | null;
      if (!command || typeof command !== "object" || Array.isArray(command)) return;
      const id = terminalControlInputId(command.message) ??
        (typeof command.id === "string" && /^[0-9a-f-]{36}$/.test(command.id) ? command.id : undefined);
      if (id) settleTerminalControl(runDirectory, id, "refused");
    } catch (error) {
      if (!(error instanceof SyntaxError)) throw error; // A failed ACK is not a successful discard receipt.
    }
  };
  input.emit = ((event: string | symbol, ...args: unknown[]): boolean => {
    if (event === "data") {
      const chunk = args[0];
      pending += typeof chunk === "string" ? chunk : decoder.write(chunk as Buffer);
      let newline: number;
      while ((newline = pending.indexOf("\n")) >= 0) {
        const line = pending.slice(0, newline);
        pending = pending.slice(newline + 1);
        frame(line);
      }
      return true;
    }
    if (event === "end") {
      pending += decoder.end();
      if (pending) frame(pending);
      pending = "";
    }
    return emit(event, ...args);
  }) as typeof input.emit;
};

/** Task-only native boundary. AgentSession's awaited agent listener runs first:
 * extension message replacements and SessionManager.appendMessage complete before
 * this listener, while finishTurn and both queue drains have not started yet.
 * Actors deliberately never install this fence.
 */
export const installTerminalAnswerBoundary = (session: AgentSession, runDirectory: string, runId: string,
  recorded: (receipt: FinalAnswerReceipt) => void, replyFile?: string, resuming = false): void => {
  let receipt = readFinalAnswerReceipt(runDirectory, runId);
  const agent = session.agent;
  // Only the worker's same-run replacement signal or an error observed in this
  // native attempt establishes recovery. Inherited parent transcript errors do
  // not authorize continuation of an ordinary task after its final answer.
  let recovering = resuming;
  let latestAssistant: unknown;
  const unresolvedToolCalls = new Set<string>();
  const hasPendingFollowUpAdmission = (): boolean => queuedTerminalControls(runDirectory).some(control => {
    if (control.delivery !== "followUp") return false;
    const file = followUpFile(runDirectory, control.id);
    return fs.existsSync(file) && ["queued", "settling"].includes(followUpState(file));
  });
  const terminalError = (): Error => Object.assign(new Error(`FABRIC_TARGET_TERMINAL: ${runId} has final answer ${receipt!.id}`),
    { code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: receipt!.id });
  // Fence both ingress and its final low-level enqueue: asynchronous input hooks
  // may have begun before the receipt, then complete after it. No abort here:
  // abort waits for this same awaited listener and would deadlock persistence.
  // Only IDs actually read by this child are discardable here. Never sweep the
  // host registry: it also contains frames still buffered in the transport.
  const nativeQueued = new Set<string>();
  const discard = (input: unknown): void => {
    if (Array.isArray(input)) { for (const item of input) discard(item); return; }
    const id = terminalControlInputId(input);
    if (receipt && id) settleTerminalControl(runDirectory, id, "refused");
  };
  const rememberQueued = (input: unknown): void => {
    const id = terminalControlInputId(input);
    if (id) nativeQueued.add(id);
  };
  const clearQueue = session.clearQueue.bind(session);
  session.clearQueue = (...args) => {
    const cleared = clearQueue(...args);
    if (receipt) {
      discard(cleared?.steering);
      discard(cleared?.followUp);
      for (const id of nativeQueued) settleTerminalControl(runDirectory, id, "refused");
      nativeQueued.clear();
    }
    return cleared;
  };
  const prompt = session.prompt.bind(session);
  session.prompt = async (...args) => {
    if (receipt) { discard(args[0]); throw terminalError(); }
    try { return await prompt(...args); } finally {
      // A private command submits asynchronous input; returning from its handler
      // is NOT proof that that input reached its discard/consumption gate.
      if (receipt && !args[0].startsWith("/fabric-delivery ")) discard(args[0]);
    }
  };
  const steer = session.steer.bind(session);
  session.steer = async (...args) => {
    if (receipt) { discard(args[0]); throw terminalError(); }
    try { return await steer(...args); } finally { if (receipt) discard(args[0]); }
  };
  const followUp = session.followUp.bind(session);
  session.followUp = async (...args) => {
    if (receipt) { discard(args[0]); throw terminalError(); }
    try { return await followUp(...args); } finally { if (receipt) discard(args[0]); }
  };
  const agentPrompt = agent.prompt.bind(agent);
  agent.prompt = (async (...args: unknown[]) => { if (receipt) { discard(args[0]); throw terminalError(); } return Reflect.apply(agentPrompt, agent, args); }) as typeof agent.prompt;
  const continueRun = agent.continue.bind(agent);
  agent.continue = async (...args) => { if (receipt) throw terminalError(); return continueRun(...args); };
  const queueSteer = agent.steer.bind(agent);
  agent.steer = (...args) => { if (receipt) { discard(args[0]); session.clearQueue(); return; } rememberQueued(args[0]); queueSteer(...args); };
  const queueFollowUp = agent.followUp.bind(agent);
  agent.followUp = (...args) => { if (receipt) { discard(args[0]); session.clearQueue(); return; } rememberQueued(args[0]); queueFollowUp(...args); };
  const finishTurn = agent.finishTurn;
  const record = (text: string, structuredReply = false): void => {
    receipt = saveFinalAnswerReceipt(runDirectory, runId, text, {
      assistant: assistantAnswerEvidence(latestAssistant, unresolvedToolCalls.size),
      ...(structuredReply ? { structuredReply: true as const } : {}),
    });
    session.clearQueue();
    // Synchronous child-local notification discards extension-held inputs before
    // IPC/EOF can start teardown. It is not a transport ACK or a registry sweep.
    process.emit("fabric-terminal-answer", runDirectory);
    recorded(receipt);
  };
  agent.finishTurn = async (...args) => {
    // A successful fabric_reply is the structured task's final answer, even
    // though its assistant message contains a tool call. Core's terminate:true
    // skips tool continuation but does NOT skip queue draining. Seal the reply
    // file first, then apply the same task-only native fence before that drain.
    for (const result of args[0].toolResults) unresolvedToolCalls.delete(result.toolCallId);
    if (!receipt && replyFile && unresolvedToolCalls.size === 0 &&
        args[0].toolResults.some(result => result.toolName === REPLY_TOOL_NAME && !result.isError)) {
      const reply = fs.readFileSync(replyFile, "utf8");
      JSON.parse(reply); // Never seal a partial/malformed tool artifact.
      writeFileAtomic(replyFile, reply, { durable: true });
      record("", true);
    }
    const decision = await finishTurn?.(...args);
    if (receipt) { session.clearQueue(); return { action: "end" }; }
    return decision || undefined;
  };
  agent.subscribe(event => {
    if (receipt) { session.clearQueue(); return; }
    if (event.type === "message_start" && event.message.role === "assistant") latestAssistant = undefined;
    if (event.type === "tool_execution_start") unresolvedToolCalls.add(event.toolCallId);
    if (event.type === "tool_execution_end") unresolvedToolCalls.delete(event.toolCallId);
    if (event.type !== "message_end") return;
    if (event.message.role === "toolResult") unresolvedToolCalls.delete(event.message.toolCallId);
    if (event.message.role !== "assistant") return;
    latestAssistant = event.message;
    if (event.message.stopReason === "error" || event.message.stopReason === "aborted") recovering = true;
    // Core returns before tool execution on error/aborted assistants. Do not
    // retain their partial calls, or erase obligations from earlier real starts.
    else for (const block of event.message.content) if (block.type === "toolCall") unresolvedToolCalls.add(block.id);
    const text = finalAssistantText(event.message);
    if (text === undefined || unresolvedToolCalls.size > 0) return;
    const nativeQueued = agent.hasQueuedMessages();
    // A held tracked input has not reached native admission yet. With no native
    // queue owning it, let turn_end/before_settle finish that admission; the
    // worker already retains custody until consumption/cancellation. Ordinary
    // late native queues remain fenced, but recovery must drain its inherited
    // queues before its last successful response can become the task final.
    if ((recovering && nativeQueued) || (hasPendingFollowUpAdmission() && (recovering || !nativeQueued))) return;
    // Persist before making the native loop terminal or notifying the worker.
    // A failed stable-storage barrier propagates as a run failure, not success.
    record(text);
  });
  if (receipt) { session.clearQueue(); recorded(receipt); }
};
