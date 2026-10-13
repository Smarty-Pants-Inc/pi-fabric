import type { ContextEvent } from "@earendil-works/pi-coding-agent";

type AgentMessage = ContextEvent["messages"][number];

// Tool-call pairing repair for the outgoing `context` message list.
//
// A compaction cut, an external summarizer, or a crashed turn can leave a
// tool result whose call is gone, or a call whose result never landed.
// Providers reject both. The repair is deliberately narrow:
//
// - A toolResult is kept only when the nearest preceding assistant message
//   issued its toolCallId and no earlier result answered it. Otherwise it is
//   dropped (orphan or duplicate).
// - A non-final assistant message whose calls have no result before the next
//   user/assistant message gets a synthetic error result per missing call,
//   placed right after the results that did arrive.
//
// Assistant messages that ended in error/abort are skipped for synthesis:
// pi-ai drops those messages before the provider sees them, so a synthetic
// result would itself become an orphan. Their real results are still kept
// only when they match, exactly as for any other assistant.
//
// When nothing is wrong the input array is returned as-is so the prompt
// cache prefix and Pi's identity checks are untouched.

export const MISSING_TOOL_RESULT_TEXT = "Tool result missing (repaired by Fabric)";

interface ToolCallView {
  id: string;
  name: string;
}

type MessageView = { role?: unknown; content?: unknown; toolCallId?: unknown; stopReason?: unknown };

const toolCallsOf = (message: MessageView): ToolCallView[] => {
  if (!Array.isArray(message.content)) return [];
  const calls: ToolCallView[] = [];
  for (const part of message.content) {
    if (!part || typeof part !== "object") continue;
    const candidate = part as { type?: unknown; id?: unknown; name?: unknown };
    if (candidate.type !== "toolCall" || typeof candidate.id !== "string") continue;
    calls.push({ id: candidate.id, name: typeof candidate.name === "string" ? candidate.name : "" });
  }
  return calls;
};

const isBoundary = (role: unknown): boolean => role === "user" || role === "assistant";

const replaysAssistant = (message: MessageView): boolean =>
  message.stopReason !== "error" && message.stopReason !== "aborted";

export interface OrphanRepairResult {
  messages: AgentMessage[];
  dropped: number;
  inserted: number;
}

export const repairToolResultPairing = (messages: AgentMessage[]): OrphanRepairResult => {
  // Detection pass: no allocation beyond the open-call map on a clean list.
  const drop = new Set<number>();
  const insertAfter = new Map<number, ToolCallView[]>();
  let open: Map<string, ToolCallView> | undefined;
  let ownerIndex = -1;
  let synthesize = false;
  // Last index inside the open window: the assistant itself or a matched result.
  let windowEnd = -1;
  const closeWindow = (): void => {
    if (open && open.size > 0 && synthesize && ownerIndex < messages.length - 1) {
      insertAfter.set(windowEnd, [...open.values()]);
    }
    open = undefined;
  };

  for (let index = 0; index < messages.length; index++) {
    const view = messages[index] as MessageView;
    if (isBoundary(view.role)) {
      closeWindow();
      if (view.role === "assistant") {
        open = new Map(toolCallsOf(view).map((call) => [call.id, call]));
        ownerIndex = index;
        synthesize = replaysAssistant(view);
        windowEnd = index;
      }
      continue;
    }
    if (view.role !== "toolResult") continue;
    const id = typeof view.toolCallId === "string" ? view.toolCallId : undefined;
    if (id !== undefined && open?.delete(id)) {
      windowEnd = index;
    } else {
      drop.add(index);
    }
  }
  // The last window closes at the end of the list; its owner is non-final
  // whenever any message (results or otherwise) follows it.
  closeWindow();
  if (drop.size === 0 && insertAfter.size === 0) return { messages, dropped: 0, inserted: 0 };

  const repaired: AgentMessage[] = [];
  let inserted = 0;
  for (let index = 0; index < messages.length; index++) {
    if (!drop.has(index)) repaired.push(messages[index]!);
    const missing = insertAfter.get(index);
    if (!missing) continue;
    for (const call of missing) {
      repaired.push({
        role: "toolResult",
        toolCallId: call.id,
        toolName: call.name,
        content: [{ type: "text", text: MISSING_TOOL_RESULT_TEXT }],
        isError: true,
        timestamp: Date.now(),
      } as unknown as AgentMessage);
      inserted += 1;
    }
  }
  return { messages: repaired, dropped: drop.size, inserted };
};
