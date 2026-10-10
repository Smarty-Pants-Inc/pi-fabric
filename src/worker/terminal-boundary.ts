import fs from "node:fs";
import { writeFileAtomic } from "../core/atomic-write.js";
import { REPLY_TOOL_NAME } from "../core/reply-tool-identity.js";
import type { AgentSession } from "@earendil-works/pi-coding-agent";
import { finalAssistantText, readFinalAnswerReceipt, saveFinalAnswerReceipt, type FinalAnswerReceipt } from "./terminal-answer.js";

/** Task-only native boundary. AgentSession's awaited agent listener runs first:
 * extension message replacements and SessionManager.appendMessage complete before
 * this listener, while finishTurn and both queue drains have not started yet.
 * Actors deliberately never install this fence.
 */
export const installTerminalAnswerBoundary = (session: AgentSession, runDirectory: string, runId: string,
  recorded: (receipt: FinalAnswerReceipt) => void, replyFile?: string): void => {
  let receipt = readFinalAnswerReceipt(runDirectory, runId);
  const agent = session.agent;
  const terminalError = (): Error => Object.assign(new Error(`FABRIC_TARGET_TERMINAL: ${runId} has final answer ${receipt!.id}`),
    { code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: receipt!.id });
  // Fence both ingress and its final low-level enqueue: asynchronous input hooks
  // may have begun before the receipt, then complete after it. No abort here:
  // abort waits for this same awaited listener and would deadlock persistence.
  const prompt = session.prompt.bind(session);
  session.prompt = async (...args) => { if (receipt) throw terminalError(); return prompt(...args); };
  const steer = session.steer.bind(session);
  session.steer = async (...args) => { if (receipt) throw terminalError(); return steer(...args); };
  const followUp = session.followUp.bind(session);
  session.followUp = async (...args) => { if (receipt) throw terminalError(); return followUp(...args); };
  const agentPrompt = agent.prompt.bind(agent);
  agent.prompt = (async (...args: unknown[]) => { if (receipt) throw terminalError(); return Reflect.apply(agentPrompt, agent, args); }) as typeof agent.prompt;
  const continueRun = agent.continue.bind(agent);
  agent.continue = async (...args) => { if (receipt) throw terminalError(); return continueRun(...args); };
  const queueSteer = agent.steer.bind(agent);
  agent.steer = (...args) => { if (receipt) { session.clearQueue(); return; } queueSteer(...args); };
  const queueFollowUp = agent.followUp.bind(agent);
  agent.followUp = (...args) => { if (receipt) { session.clearQueue(); return; } queueFollowUp(...args); };
  const finishTurn = agent.finishTurn;
  const record = (text: string): void => {
    receipt = saveFinalAnswerReceipt(runDirectory, runId, text);
    session.clearQueue();
    recorded(receipt);
  };
  agent.finishTurn = async (...args) => {
    // A successful fabric_reply is the structured task's final answer, even
    // though its assistant message contains a tool call. Core's terminate:true
    // skips tool continuation but does NOT skip queue draining. Seal the reply
    // file first, then apply the same task-only native fence before that drain.
    if (!receipt && replyFile && args[0].toolResults.some(result => result.toolName === REPLY_TOOL_NAME && !result.isError)) {
      const reply = fs.readFileSync(replyFile, "utf8");
      JSON.parse(reply); // Never seal a partial/malformed tool artifact.
      writeFileAtomic(replyFile, reply, { durable: true });
      record("");
    }
    const decision = await finishTurn?.(...args);
    if (receipt) { session.clearQueue(); return { action: "end" }; }
    return decision || undefined;
  };
  agent.subscribe(event => {
    if (receipt) { session.clearQueue(); return; }
    if (event.type !== "message_end") return;
    const text = finalAssistantText(event.message);
    if (text === undefined) return;
    // Persist before making the native loop terminal or notifying the worker.
    // A failed stable-storage barrier propagates as a run failure, not success.
    record(text);
  });
  if (receipt) { session.clearQueue(); recorded(receipt); }
};
