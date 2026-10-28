import type { ContextEvent } from "@earendil-works/pi-coding-agent";
import { repairToolResultPairing } from "../compaction/orphan-repair.js";

type Messages = ContextEvent["messages"];
type Message = Messages[number];
export type SettledNativeToolResult = Extract<Message, { role: "toolResult" }>;
type Assistant = Extract<Message, { role: "assistant" }>;
type ToolCall = Extract<Assistant["content"][number], { type: "toolCall" }>;

export type RecoveryContextRepairErrorCode =
  | "missing-original-call"
  | "ambiguous-original-call"
  | "tool-name-mismatch"
  | "conflicting-settled-results"
  | "conflicting-context-result"
  | "unsettled-aborted-call";

/** Uncertainty blocks continuation; callers must not turn this into a success. */
export class RecoveryContextRepairError extends Error {
  constructor(readonly code: RecoveryContextRepairErrorCode, readonly toolCallId: string) {
    super(`Recovery context is uncertain (${code}) for original tool call ${JSON.stringify(toolCallId)}`);
    this.name = "RecoveryContextRepairError";
  }
}

interface CallOwner { index: number; call: ToolCall }
const interrupted = (message: Assistant): boolean => message.stopReason === "aborted" || message.stopReason === "error";
// Pi's convertToLlm turns these native entries into user messages before the
// provider sees them. System messages and excluded bash entries are transparent.
const userBoundary = (message: Message): boolean => message.role === "user" || message.role === "custom" ||
  message.role === "branchSummary" || message.role === "compactionSummary" ||
  (message.role === "bashExecution" && !message.excludeFromContext);
const sameResult = (a: SettledNativeToolResult, b: SettledNativeToolResult): boolean => a === b || JSON.stringify(a) === JSON.stringify(b);

/**
 * Request-local ContextEvent projection, AFTER the adapter has settled effects.
 * `settledResults` must be the saved ORIGINAL native result messages, not nested
 * effect receipts or reconstructed fabric_exec output. This function performs no
 * effects, persistence, argument regeneration, alias normalization, or replay.
 *
 * Raw messages and durable result objects are never changed. Results are used by
 * reference. A missing original call (including one elided by compaction) or an
 * ambiguous binding throws instead of inserting an orphan or choosing a winner.
 * Distinct successful result aliases must agree in their complete serialized
 * native message; only historical errors/synthetic failures may be overwritten.
 */
export const repairRecoveryContext = (messages: Messages, settledResults: readonly SettledNativeToolResult[]): Messages => {
  const callsByAssistant = new Map<number, ToolCall[]>();
  const owners = new Map<string, CallOwner[]>();
  for (let index = 0; index < messages.length; index++) {
    const message = messages[index]!;
    if (message.role !== "assistant") continue;
    const calls = message.content.filter((part): part is ToolCall => part.type === "toolCall");
    callsByAssistant.set(index, calls);
    for (const call of calls) {
      const locations = owners.get(call.id) ?? [];
      locations.push({ index, call });
      owners.set(call.id, locations);
    }
  }

  const settled = new Map<string, SettledNativeToolResult>();
  const settledOwners = new Map<string, CallOwner>();
  for (const result of settledResults) {
    const id = result.toolCallId;
    const previous = settled.get(id);
    if (previous) {
      if (!sameResult(previous, result)) throw new RecoveryContextRepairError("conflicting-settled-results", id);
      continue;
    }
    const locations = owners.get(id);
    if (!locations?.length) throw new RecoveryContextRepairError("missing-original-call", id);
    if (locations.length !== 1) throw new RecoveryContextRepairError("ambiguous-original-call", id);
    const owner = locations[0]!;
    if (owner.call.name !== result.toolName) throw new RecoveryContextRepairError("tool-name-mismatch", id);
    settled.set(id, result);
    settledOwners.set(id, owner);
  }

  const promoted = new Map<number, Assistant>();
  for (const { index } of settledOwners.values()) {
    if (promoted.has(index)) continue;
    const message = messages[index]! as Assistant;
    if (!interrupted(message)) continue;
    // Pi filters interrupted assistants. Only a wholly settled native call set
    // may survive in the outgoing view; interrupted text/thinking (including
    // incomplete provider signatures) must never be promoted with it.
    const calls = callsByAssistant.get(index)!;
    for (const call of calls) {
      if (!settled.has(call.id)) throw new RecoveryContextRepairError("unsettled-aborted-call", call.id);
    }
    // Keep the original call blocks/arguments by reference, but construct only
    // the required assistant envelope. Do not carry response/deferred/error or
    // provider reasoning metadata from an incomplete response into its repaired
    // view (pi-messages can forward the whole envelope). Raw history is intact.
    promoted.set(index, {
      role: "assistant", content: calls,
      api: message.api, provider: message.provider, model: message.model,
      usage: message.usage, stopReason: "toolUse", timestamp: message.timestamp,
    });
  }

  const keepAt = new Map<string, number>();
  const replaceAt = new Map<number, SettledNativeToolResult>();
  const drop = new Set<number>();
  let precedingAssistant = -1;
  for (let index = 0; index < messages.length; index++) {
    const message = messages[index]!;
    if (message.role === "assistant") precedingAssistant = index;
    else if (userBoundary(message)) precedingAssistant = -1;
    if (message.role !== "toolResult") continue;
    const id = message.toolCallId;
    const saved = settled.get(id);
    if (saved) {
      if (message.toolName !== saved.toolName) throw new RecoveryContextRepairError("tool-name-mismatch", id);
      if (!message.isError && !sameResult(message, saved)) throw new RecoveryContextRepairError("conflicting-context-result", id);
      // Retain the first position only within the original call's window.
      // Results across a user/assistant boundary are dangling, not valid pairs.
      if (precedingAssistant === settledOwners.get(id)!.index && !keepAt.has(id)) {
        keepAt.set(id, index);
        replaceAt.set(index, saved);
      } else {
        drop.add(index);
      }
    } else {
      const locations = owners.get(id);
      const originalInterrupted = locations?.length === 1 && interrupted(messages[locations[0]!.index]! as Assistant);
      if (originalInterrupted || (precedingAssistant >= 0 && interrupted(messages[precedingAssistant]! as Assistant))) {
        // Do not revive an uncommitted interrupted assistant, or leave its raw
        // error result orphaned when Pi filters that assistant at conversion.
        drop.add(index);
      }
    }
  }

  const projected: Messages = [];
  for (let index = 0; index < messages.length; index++) {
    if (!drop.has(index)) projected.push(replaceAt.get(index) ?? promoted.get(index) ?? messages[index]!);
    for (const call of callsByAssistant.get(index) ?? []) {
      const saved = settled.get(call.id);
      if (saved && !keepAt.has(call.id)) projected.push(saved);
    }
  }
  // Keep existing compaction behavior for unrelated orphans and missing calls:
  // its synthetic results are explicit errors, never invented effect outputs.
  const paired = repairToolResultPairing(projected).messages;
  return paired.length === messages.length && paired.every((message, index) => message === messages[index]) ? messages : paired;
};
