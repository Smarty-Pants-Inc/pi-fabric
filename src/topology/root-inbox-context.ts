import { ROOT_INBOX_CUSTOM_TYPE } from "./root-inbox-shared.js";

/**
 * The model sees each root inbox event once (smarty-dev#8962). The inbox acks an event as soon as
 * its message enters a turn, but copies Pi had already queued before that ack (a second followUp
 * after a lease expiry, a nextTurn plus a followUp, a stale writer's read) are still injected and
 * kept in the session. Only the request changes here; the session keeps every message.
 * - The first copy of an event id in the context is kept: that is where the model saw it. Every
 *   later copy is dropped.
 * - An acked event (acked id, or at or below the high-water mark) is dropped from every copy when the
 *   context no longer holds the copy that consumed it: compaction summarized a copy of it away.
 * - A mixed batch keeps its other events: only the dropped events are cut from it.
 * - A message with nothing left is dropped. When it ended the request after an assistant reply, one
 *   short line takes its place, so the request still ends on the user's side.
 */
export interface RootInboxContextOptions {
  /** The root inbox acked this event: its id is in the acked set or its sequence at or below the mark. */
  acked: (id: string, sequence?: number) => boolean;
  /** Inbox copies per event id that compaction cut from the context; read only for an acked event. */
  compactedCopies: () => ReadonlyMap<string, number>;
}

// Event text is XML-escaped (root-inbox.ts rootInboxMessage), so it cannot close an element early.
const EVENT = /\n?<event id="([^"]*)" sequence="(\d+)"[\s\S]*?<\/event>/g;
const OVERFLOW = /\n?<overflow count="\d+">[\s\S]*?<\/overflow>/;
const OVERFLOW_RANGE = /<overflow count="\d+">[^<]*?\(sequence (\d+)-(\d+)/;
const HEADER = /<fabric-inbox count="\d+"(?: shown="\d+")?>/;
const NOTHING_NEW = `<fabric-inbox count="0">Nothing new: this delivery repeated work events you already received.</fabric-inbox>`;

const record = (value: unknown): Record<string, unknown> | undefined =>
  value && typeof value === "object" ? value as Record<string, unknown> : undefined;

/** The event ids of a root inbox message, or undefined for any other message. */
const inboxIds = (message: unknown): string[] | undefined => {
  const fields = record(message);
  if (fields?.role !== "custom" && fields?.type !== "custom_message") return undefined;
  if (fields.customType !== ROOT_INBOX_CUSTOM_TYPE) return undefined;
  const ids = record(fields.details)?.ids;
  return Array.isArray(ids) ? [...new Set(ids.filter((id): id is string => typeof id === "string"))] : undefined;
};

const texts = (content: unknown): string[] => typeof content === "string" ? [content]
  : Array.isArray(content) ? content.flatMap((part) => {
    const text = record(part)?.type === "text" ? record(part)!.text : undefined;
    return typeof text === "string" ? [text] : [];
  }) : [];

/** Shown events carry their sequence; summarized ones get the overflow range's upper bound. */
const eventSequences = (content: unknown): { shown: Map<string, number>; overflow?: number } => {
  const shown = new Map<string, number>();
  let overflow: number | undefined;
  for (const text of texts(content)) {
    for (const match of text.matchAll(EVENT)) shown.set(match[1]!, Number(match[2]));
    const range = OVERFLOW_RANGE.exec(text);
    if (range) overflow = Number(range[2]);
  }
  return { shown, ...(overflow === undefined ? {} : { overflow }) };
};

/** How many root inbox copies of each event id these session entries (or messages) hold. */
export const rootInboxCopies = (entries: readonly unknown[]): Map<string, number> => {
  const copies = new Map<string, number>();
  for (const entry of entries) for (const id of inboxIds(entry) ?? []) copies.set(id, (copies.get(id) ?? 0) + 1);
  return copies;
};

/**
 * Inbox copies per event id that the latest compaction on this branch summarized out of the context:
 * every entry before its first kept entry, as Pi's buildContextEntries projects it.
 */
export const rootInboxCompactedCopies = (branch: readonly unknown[]): Map<string, number> => {
  let compaction = -1;
  branch.forEach((entry, index) => { if (record(entry)?.type === "compaction") compaction = index; });
  if (compaction < 0) return new Map();
  const firstKept = record(branch[compaction])?.firstKeptEntryId;
  const kept = branch.findIndex((entry, index) => index < compaction && record(entry)?.id === firstKept);
  return rootInboxCopies(branch.slice(0, kept < 0 ? compaction : kept));
};

const cutEvents = (text: string, drop: ReadonlySet<string>, shown: number, overflow: number, dropOverflow: boolean): string => {
  let next = text.replace(EVENT, (element, id: string) => drop.has(id) ? "" : element);
  if (dropOverflow) next = next.replace(OVERFLOW, "");
  else next = next.replace(/<overflow count="\d+">\+\d+ more/, `<overflow count="${overflow}">+${overflow} more`);
  return next.replace(HEADER, `<fabric-inbox count="${shown + overflow}"${overflow ? ` shown="${shown}"` : ""}>`);
};

export const filterRootInboxContext = <Message>(
  messages: Message[],
  options: RootInboxContextOptions,
): { messages: Message[]; changed: boolean } => {
  const copies = rootInboxCopies(messages);
  if (!copies.size) return { messages, changed: false };
  let compacted: ReadonlyMap<string, number> | undefined;
  // Acked, and the copy that consumed it was compacted away: the model saw it before this context.
  const consumedEarlier = (id: string, sequence: number | undefined): boolean => {
    if (!options.acked(id, sequence)) return false;
    compacted ??= options.compactedCopies();
    return (compacted.get(id) ?? 0) > 0;
  };
  const seen = new Set<string>();
  const filtered: Message[] = [];
  let changed = false;
  let droppedLast: Message | undefined;
  messages.forEach((message, index) => {
    const ids = inboxIds(message);
    if (!ids?.length) {
      filtered.push(message);
      return;
    }
    const fields = message as Record<string, unknown>;
    const sequences = eventSequences(fields.content);
    const drop = new Set<string>();
    for (const id of ids) {
      if (seen.has(id) || consumedEarlier(id, sequences.shown.get(id) ?? sequences.overflow)) drop.add(id);
      else seen.add(id);
    }
    if (!drop.size) {
      filtered.push(message);
      return;
    }
    changed = true;
    if (drop.size === ids.length) {
      if (index === messages.length - 1) droppedLast = message;
      return;
    }
    const kept = ids.filter((id) => !drop.has(id));
    const shown = kept.filter((id) => sequences.shown.has(id)).length;
    const overflow = kept.length - shown;
    const edit = (text: string) => cutEvents(text, drop, shown, overflow, overflow === 0);
    const content = typeof fields.content === "string" ? edit(fields.content)
      : Array.isArray(fields.content) ? fields.content.map((part) => {
        const text = record(part)?.type === "text" ? record(part)!.text : undefined;
        return typeof text === "string" ? { ...record(part), text: edit(text) } : part;
      }) : fields.content;
    filtered.push({ ...fields, content, details: { ...record(fields.details), ids: kept } } as Message);
  });
  if (!changed) return { messages, changed: false };
  // Never end a request on the assistant's own reply: that would read as a prefill.
  const last = record(filtered.at(-1));
  if (droppedLast && (!last || last.role === "assistant")) {
    const fields = droppedLast as Record<string, unknown>;
    filtered.push({ ...fields, content: NOTHING_NEW, details: { ...record(fields.details), ids: [] } } as Message);
  }
  return { messages: filtered, changed };
};
