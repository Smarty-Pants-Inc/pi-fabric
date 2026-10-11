import type { SessionEntry } from "@earendil-works/pi-coding-agent";

// Local mirror of the context-projection helpers in pi 1.1.0
// core/session-manager.js and core/messages.js. Kept identical so Fabric's
// compaction projections match the host's without importing the host package
// during extension load.

export type ContextMessage = { role: string } & Record<string, unknown>;

type PathEntry = SessionEntry & { parentId?: string };

const buildEntryIndex = (
  entries: readonly SessionEntry[],
  byId?: Map<string, SessionEntry>,
): Map<string, SessionEntry> => {
  if (byId) return byId;
  const index = new Map<string, SessionEntry>();
  for (const entry of entries) {
    index.set(entry.id, entry);
  }
  return index;
};

const buildSessionPath = (
  entries: readonly SessionEntry[],
  leafId: string | undefined,
  byId?: Map<string, SessionEntry>,
): SessionEntry[] => {
  const index = buildEntryIndex(entries, byId);
  let leaf: SessionEntry | undefined = undefined;
  if (leafId !== undefined) {
    leaf = index.get(leafId);
  }
  leaf = leaf ?? entries[entries.length - 1];
  if (!leaf) {
    return [];
  }
  const path: SessionEntry[] = [];
  let current: SessionEntry | undefined = leaf;
  while (current) {
    path.push(current);
    const parent = current as PathEntry;
    current = parent.parentId ? index.get(parent.parentId) : undefined;
  }
  path.reverse();
  return path;
};

const getSessionContextSettings = (path: readonly SessionEntry[]): {
  thinkingLevel: string;
  model: { provider: string; modelId: string } | null;
} => {
  let thinkingLevel = "off";
  let model: { provider: string; modelId: string } | null = null;
  for (const entry of path) {
    if (entry.type === "thinking_level_change") {
      thinkingLevel = entry.thinkingLevel;
    } else if (entry.type === "model_change") {
      model = { provider: entry.provider, modelId: entry.modelId };
    } else if (entry.type === "message" && entry.message.role === "assistant") {
      model = { provider: entry.message.provider, modelId: entry.message.model };
    }
  }
  return { thinkingLevel, model };
};

const createBranchSummaryMessage = (
  summary: string,
  fromId: string,
  timestamp: number | string,
): ContextMessage => ({
  role: "branchSummary",
  summary,
  fromId,
  timestamp: new Date(timestamp).getTime(),
});

const createCompactionSummaryMessage = (
  summary: string,
  tokensBefore: number,
  timestamp: number | string,
): ContextMessage => ({
  role: "compactionSummary",
  summary,
  tokensBefore,
  timestamp: new Date(timestamp).getTime(),
});

const createCustomMessage = (
  customType: string,
  content: unknown,
  display: unknown,
  details: unknown,
  timestamp: number | string,
): ContextMessage => ({
  role: "custom",
  customType,
  content,
  display,
  details,
  timestamp: new Date(timestamp).getTime(),
});

export const sessionEntryToContextMessages = (entry: SessionEntry): ContextMessage[] => {
  if (entry.type === "message") {
    const message = entry.message;
    if (message.role === "system" && message.content == null) return [{ ...message, content: "" }];
    if (
      (message.role === "user" || message.role === "assistant" || message.role === "toolResult")
      && message.content == null
    ) {
      return [{ ...message, content: [] }];
    }
    return [message] as unknown as ContextMessage[];
  }
  if (entry.type === "custom_message") {
    return [
      createCustomMessage(entry.customType, entry.content ?? [], entry.display, entry.details, entry.timestamp),
    ];
  }
  if (entry.type === "branch_summary" && entry.summary) {
    return [createBranchSummaryMessage(entry.summary, entry.fromId, entry.timestamp)];
  }
  if (entry.type === "compaction") {
    const summary = createCompactionSummaryMessage(entry.summary, entry.tokensBefore, entry.timestamp);
    return entry.systemMessage ? [entry.systemMessage as unknown as ContextMessage, summary] : [summary];
  }
  return [];
};

const buildContextEntries = (
  entries: readonly SessionEntry[],
  leafId?: string,
  byId?: Map<string, SessionEntry>,
): SessionEntry[] => {
  const path = buildSessionPath(entries, leafId, byId);
  let compaction: SessionEntry | undefined = undefined;
  for (const entry of path) {
    if (entry.type === "compaction") {
      compaction = entry;
    }
  }
  if (!compaction) {
    return path;
  }
  const compactionIdx = [...path].findIndex(
    (entry) => entry.id === compaction?.id,
  );
  if (compactionIdx < 0) {
    return path;
  }
  const contextEntries: SessionEntry[] = [compaction];
  let foundFirstKept = false;
  for (let i = 0; i < compactionIdx; i++) {
    const entry = path[i];
    if (!entry) continue;
    if (entry.id === compaction.firstKeptEntryId) {
      foundFirstKept = true;
    }
    if (foundFirstKept && !(entry.type === "message" && entry.message.role === "system")) {
      contextEntries.push(entry);
    }
  }
  contextEntries.push(...path.slice(compactionIdx + 1));
  return contextEntries;
};

/** Content-only view; preserve IDs/parent links even for omissions so cuts and
 * tree traversal still address the original branch. Never mutate raw history.
 * Also used over cumulative raw history by deterministic compaction: an omitted
 * recovery attempt must not be resurrected by a later summary. */
export const applyContextEdits = (entries: readonly SessionEntry[]): SessionEntry[] => {
  const edits = new Map<string, Extract<SessionEntry, { type: "context_edit" }>>();
  for (const entry of entries) if (entry.type === "context_edit") edits.set(entry.targetId, entry);
  return entries.map((entry) => {
    const edit = edits.get(entry.id);
    if (!edit) return entry;
    const replacement = edit.replacement;
    if (replacement === null) return {
      type: "custom", id: entry.id, parentId: entry.parentId, timestamp: entry.timestamp,
      customType: "fabric.context-omission",
    };
    if (entry.type === "custom_message") return { ...entry, content: replacement.content as typeof entry.content };
    if (entry.type !== "message") return entry;
    const message = entry.message;
    if (!["user", "assistant", "toolResult", "custom"].includes(message.role)) return entry;
    const content = (message.role === "assistant" || message.role === "toolResult") && typeof replacement.content === "string"
      ? [{ type: "text" as const, text: replacement.content }] : replacement.content;
    return { ...entry, message: { ...message, content } } as SessionEntry;
  });
};

export const buildSessionContext = (
  entries: readonly SessionEntry[],
  leafId?: string,
  byId?: Map<string, SessionEntry>,
): {
  messages: ContextMessage[];
  thinkingLevel: string;
  model: { provider: string; modelId: string } | null;
} => {
  const path = buildSessionPath(entries, leafId, byId);
  const { thinkingLevel, model } = getSessionContextSettings(path);
  const messages = applyContextEdits(buildContextEntries(entries, leafId, byId))
    .flatMap((entry, index) => entry.type === "compaction" && index > 0 ? [] : sessionEntryToContextMessages(entry));
  return { messages, thinkingLevel, model };
};
