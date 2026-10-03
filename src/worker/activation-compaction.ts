import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type { SessionBoundaryDraft, TurnEndEvent } from "@earendil-works/pi-coding-agent";

const EXCERPT_CHARS = 1024;
const SUMMARY = "Earlier tool outputs were compacted to bounded excerpts. The full outputs remain in the actor's audit journal at the indicated entry IDs. User directions, assistant decisions, and the latest tool exchange are unchanged.";

const OVERFLOW_SUMMARY = "Tool outputs, including the latest oversized batch, were compacted to bounded excerpts before dispatch to fit the model window. Full outputs remain in the actor audit journal at the indicated entry IDs. User directions, assistant decisions and tool-call/result identities are unchanged.";

/** Native boundary drafts: never rewrite raw journal entries. Latest results are protected unless they cannot fit. */
export function compactActivationTools(event: TurnEndEvent, options: { includeLatest?: boolean } = {}): {
  entries: SessionBoundaryDraft[];
  messages: AgentMessage[];
  summary: string;
} | undefined {
  const protectedIds = new Set(options.includeLatest ? [] : [event.messageEntryId, ...event.toolResultEntryIds]);
  const edits: SessionBoundaryDraft[] = [];
  const messages: AgentMessage[] = [];
  let firstKeptEntryId: string | undefined;
  for (const projected of event.context.contextEntries) {
    const entry = projected.sourceEntry;
    // Retain earlier edits and all raw messages; only the newest marker renders.
    if (entry.type !== "compaction") firstKeptEntryId ??= entry.id;
    for (const message of projected.messages) {
      if (message.role === "system" || message.role === "compactionSummary") continue;
      if (message.role !== "toolResult" || protectedIds.has(entry.id)) {
        messages.push(message);
        continue;
      }
      const text = message.content.map(part => part.type === "text" ? part.text : `[${part.type}]`).join("\n");
      const marker = `[Compacted tool output; full result in audit journal entry ${entry.id}]`;
      // An already edited output is not repeatedly shortened or annotated.
      if (text.length <= EXCERPT_CHARS + marker.length + 2 && message.content.every(part => part.type === "text")) {
        messages.push(message);
        continue;
      }
      let excerpt = text.slice(0, EXCERPT_CHARS);
      if (/[\uD800-\uDBFF]$/.test(excerpt)) excerpt = excerpt.slice(0, -1);
      const content = [{ type: "text" as const, text: `${excerpt}\n${marker}` }];
      edits.push({ type: "context_edit", targetId: entry.id, replacement: { content } });
      messages.push({ ...message, content });
    }
  }
  if (!edits.length || !firstKeptEntryId) return undefined;
  return {
    entries: [{ type: "compaction", summary: options.includeLatest ? OVERFLOW_SUMMARY : SUMMARY, firstKeptEntryId,
      details: { compactor: "fabric-activation-tools", version: 1, editedEntryIds: edits.map(edit => edit.type === "context_edit" ? edit.targetId : ""), protectedEntryIds: [...protectedIds] } }, ...edits],
    messages,
    summary: options.includeLatest ? OVERFLOW_SUMMARY : SUMMARY,
  };
}
