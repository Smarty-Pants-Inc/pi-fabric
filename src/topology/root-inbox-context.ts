import { ROOT_INBOX_CUSTOM_TYPE, type RootInboxReceiptVerifier } from "./root-inbox-shared.js";

/**
 * The model sees each root inbox event once (smarty-dev#8962). The inbox acks an event as soon as
 * its message enters a turn, but copies Pi had already queued before that ack (a second followUp
 * after a lease expiry, a nextTurn plus a followUp, a stale writer's read) are still injected and
 * kept in the session. Only the request changes here; the session keeps every message.
 * - The first issued copy with a shown row is kept; later issued copies are dropped. Unverifiable
 *   history cannot suppress work: possible redelivery is safer than loss (smarty-dev#12528).
 * - An acked event (acked id, or at or below the high-water mark) is dropped from every copy when the
 *   context no longer holds the copy that consumed it: compaction summarized a copy of it away.
 * - A mixed batch keeps its other events: only the dropped events are cut from it.
 * - A message with nothing left is dropped. When it ended the request after an assistant reply, one
 *   short line takes its place, so the request still ends on the user's side.
 */
export interface RootInboxContextOptions {
  /** Only the private issuer's shown-only snapshot may suppress another carrier. */
  verifier: RootInboxReceiptVerifier;
  /** The root inbox acked this event: its id is in the acked set or its sequence at or below the mark. */
  acked: (id: string, sequence?: number) => boolean;
  /** Inbox copies per event id that compaction cut from the context; read only for an acked event. */
  compactedCopies: () => ReadonlyMap<string, number>;
}

// Event text is XML-escaped (root-inbox.ts rootInboxMessage), so it cannot close an element early.
const EVENT = /\n?<event id="([^"]*)" sequence="(\d+)"[\s\S]*?<\/event>/g;
const OVERFLOW = /\n?<overflow count="\d+">[\s\S]*?<\/overflow>/;
const OVERFLOW_COUNT = /<overflow count="(\d+)">/;
const OVERFLOW_RANGE = /<overflow count="\d+">[^<]*?\(sequence (\d+)-(\d+)/;
const HEADER = /<fabric-inbox count="\d+"(?: shown="\d+")?>/;
const NOTHING_NEW = `<fabric-inbox count="0">Nothing new: this delivery repeated work events you already received.</fabric-inbox>`;

const record = (value: unknown): Record<string, unknown> | undefined =>
  value && typeof value === "object" ? value as Record<string, unknown> : undefined;

/** Unissued history and ids without a shown row cannot hide genuine work (smarty-dev#12356). */
const inboxIds = (message: unknown, verifier: RootInboxReceiptVerifier): string[] | undefined => {
  const fields = record(message);
  if (fields?.role !== "custom" && fields?.type !== "custom_message") return undefined;
  if (fields.customType !== ROOT_INBOX_CUSTOM_TYPE) return undefined;
  const ids = verifier(fields.details)?.ids;
  if (!ids?.length) return undefined;
  const shown = eventSequences(fields.content).shown;
  return [...new Set(ids.filter((id) => shown.has(id)))];
};

const texts = (content: unknown): string[] => typeof content === "string" ? [content]
  : Array.isArray(content) ? content.flatMap((part) => {
    const text = record(part)?.type === "text" ? record(part)!.text : undefined;
    return typeof text === "string" ? [text] : [];
  }) : [];

/** Pending summary counts are independent of shown-only ids and legacy sequence bounds. */
const eventSequences = (content: unknown): { shown: Map<string, number>; overflowCount: number; overflowSequence?: number } => {
  const shown = new Map<string, number>();
  let overflowCount = 0;
  let overflowSequence: number | undefined;
  for (const text of texts(content)) {
    for (const match of text.matchAll(EVENT)) shown.set(match[1]!, Number(match[2]));
    const count = OVERFLOW_COUNT.exec(text);
    if (count) overflowCount = Number(count[1]);
    const range = OVERFLOW_RANGE.exec(text);
    if (range) overflowSequence = Number(range[2]);
  }
  return { shown, overflowCount, ...(overflowSequence === undefined ? {} : { overflowSequence }) };
};

/** How many root inbox copies of each event id these session entries (or messages) hold. */
export const rootInboxCopies = (entries: readonly unknown[], verifier: RootInboxReceiptVerifier): Map<string, number> => {
  const copies = new Map<string, number>();
  for (const entry of entries) for (const id of inboxIds(entry, verifier) ?? []) copies.set(id, (copies.get(id) ?? 0) + 1);
  return copies;
};

/**
 * Inbox copies per event id that the latest compaction on this branch summarized out of the context:
 * every entry before its first kept entry, as Pi's buildContextEntries projects it.
 */
export const rootInboxCompactedCopies = (branch: readonly unknown[], verifier: RootInboxReceiptVerifier): Map<string, number> => {
  let compaction = -1;
  branch.forEach((entry, index) => { if (record(entry)?.type === "compaction") compaction = index; });
  if (compaction < 0) return new Map();
  const firstKept = record(branch[compaction])?.firstKeptEntryId;
  const kept = branch.findIndex((entry, index) => index < compaction && record(entry)?.id === firstKept);
  return rootInboxCopies(branch.slice(0, kept < 0 ? compaction : kept), verifier);
};

const cutEvents = (text: string, drop: ReadonlySet<string>, shown: number, overflow: number): string => {
  let next = text.replace(EVENT, (element, id: string) => drop.has(id) ? "" : element);
  // Removing shown duplicates cannot deliver or reduce the original pending summary.
  if (overflow === 0) next = next.replace(OVERFLOW, "");
  return next.replace(HEADER, `<fabric-inbox count="${shown + overflow}"${overflow ? ` shown="${shown}"` : ""}>`);
};

export const filterRootInboxContext = <Message>(
  messages: Message[],
  options: RootInboxContextOptions,
): { messages: Message[]; changed: boolean } => {
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
    const ids = inboxIds(message, options.verifier);
    if (!ids?.length) {
      filtered.push(message);
      return;
    }
    const fields = message as Record<string, unknown>;
    const sequences = eventSequences(fields.content);
    const drop = new Set<string>();
    for (const id of ids) {
      if (seen.has(id) || consumedEarlier(id, sequences.shown.get(id) ?? sequences.overflowSequence)) drop.add(id);
      else seen.add(id);
    }
    if (!drop.size) {
      filtered.push(message);
      return;
    }
    changed = true;
    if (drop.size === ids.length && sequences.overflowCount === 0) {
      if (index === messages.length - 1) droppedLast = message;
      return;
    }
    const kept = ids.filter((id) => !drop.has(id));
    const shown = kept.filter((id) => sequences.shown.has(id)).length;
    const overflow = sequences.overflowCount;
    const edit = (text: string) => cutEvents(text, drop, shown, overflow);
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
