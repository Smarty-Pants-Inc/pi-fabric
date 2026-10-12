import type { SessionEntry } from "@earendil-works/pi-coding-agent";

export type AgentSeedMode = "task" | "branch" | "snippet";

export const DEFAULT_SEED_MESSAGES = 12;
const MAX_SEED_MESSAGES = 50;
const MAX_SNIPPET_MESSAGE_CHARS = 4_000;

export const checkedSeed = (
  seed: unknown,
  seedMessages: unknown,
): { seed: AgentSeedMode; seedMessages: number } => {
  if (seed !== undefined && seed !== "task" && seed !== "branch" && seed !== "snippet") {
    throw new Error('seed must be "task", "branch", or "snippet"');
  }
  if (seedMessages !== undefined) {
    if (seed !== "snippet") throw new Error('seedMessages applies only to seed: "snippet"');
    if (
      typeof seedMessages !== "number" ||
      !Number.isInteger(seedMessages) ||
      seedMessages < 1 ||
      seedMessages > MAX_SEED_MESSAGES
    ) {
      throw new Error(`seedMessages must be an integer from 1 to ${MAX_SEED_MESSAGES}`);
    }
  }
  return {
    seed: seed ?? "task",
    seedMessages: (seedMessages as number | undefined) ?? DEFAULT_SEED_MESSAGES,
  };
};

type MessageEntry = Extract<SessionEntry, { type: "message" }>;

const isMessage = (entry: SessionEntry): entry is MessageEntry => entry.type === "message";

const toolCallIds = (entry: MessageEntry): string[] => {
  const message = entry.message as { role: string; content?: unknown };
  if (message.role !== "assistant" || !Array.isArray(message.content)) return [];
  return message.content
    .filter((part): part is { type: "toolCall"; id: string } =>
      typeof part === "object" && part !== null &&
      (part as { type?: unknown }).type === "toolCall" &&
      typeof (part as { id?: unknown }).id === "string")
    .map((part) => part.id);
};

/**
 * The caller branch up to its last completed turn. The in-flight fabric_exec
 * assistant turn (and any later partial results) is the newest assistant entry
 * with an unresolved tool call after the latest user message; the prefix ends
 * just before it. Older orphans belong to history and are kept as-is.
 */
export const completedBranchPrefix = (branch: readonly SessionEntry[]): SessionEntry[] => {
  const resolved = new Set<string>();
  for (let index = branch.length - 1; index >= 0; index--) {
    const entry = branch[index]!;
    if (!isMessage(entry)) continue;
    const message = entry.message as { role: string; toolCallId?: unknown };
    if (message.role === "user") break;
    if (message.role === "toolResult" && typeof message.toolCallId === "string") {
      resolved.add(message.toolCallId);
      continue;
    }
    if (toolCallIds(entry).some((id) => !resolved.has(id))) return branch.slice(0, index);
  }
  return [...branch];
};

const messageText = (content: unknown): string => {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((part): part is { type: "text"; text: string } =>
      typeof part === "object" && part !== null &&
      (part as { type?: unknown }).type === "text" &&
      typeof (part as { text?: unknown }).text === "string")
    .map((part) => part.text)
    .join("\n");
};

/**
 * Prefix the task with the last `count` user/assistant text messages. Tool
 * calls, tool results, thinking, and images are dropped; each message is
 * truncated deterministically.
 */
export const snippetTask = (
  branch: readonly SessionEntry[],
  count: number,
  task: string,
): string => {
  const messages: string[] = [];
  for (const entry of completedBranchPrefix(branch)) {
    if (!isMessage(entry)) continue;
    const message = entry.message as { role: string; content?: unknown };
    if (message.role !== "user" && message.role !== "assistant") continue;
    let text = messageText(message.content).trim();
    if (!text) continue;
    if (text.length > MAX_SNIPPET_MESSAGE_CHARS) text = `${text.slice(0, MAX_SNIPPET_MESSAGE_CHARS)}…`;
    messages.push(`[${message.role}]\n${text}`);
  }
  const recent = messages.slice(-count);
  if (recent.length === 0) return task;
  return [
    `<inherited-conversation messages="${recent.length}">`,
    recent.join("\n\n"),
    "</inherited-conversation>",
    "",
    "Task:",
    task,
  ].join("\n");
};
