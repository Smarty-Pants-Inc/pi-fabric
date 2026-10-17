// Type-only: Pi loads this startup graph as native ESM, where a runtime SDK
// import evaluates a second, unbundled SDK (assert-build-artifacts.mjs).
import type { ExtensionAPI, ExtensionContext, MessageRenderer } from "@earendil-works/pi-coding-agent";
import { Box, Markdown, Spacer, Text, truncateToWidth, type MarkdownTheme } from "@earendil-works/pi-tui";
import { createHash } from "node:crypto";
import { SessionEntryCursor, type SessionEntryCursorManager } from "../core/session-entry-cursor.js";
import type { FabricIncomingMessageMode } from "../config.js";
import { safeText } from "./format.js";
import { highlightCode } from "./highlight.js";
import { deliveryIdentity } from "../topology/root-inbox-shared.js";

export const INCOMING_MESSAGE_TYPES = [
  "pi-fabric-agent-message", // Includes mail.inbound follow-up notices.
  "pi-fabric-actor",
  "pi-fabric-inbox",
  "pi-fabric-inbox-summary",
] as const;

type IncomingMessage = Parameters<MessageRenderer>[0];
type Row = { sender: string; body: string; kind: string; full: boolean; attributes: Record<string, string>; span?: readonly [number, number] };
const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
const decodeXml = (text: string): string => text.replace(/&(amp|lt|gt|quot|apos);/g, (_match, name: string) =>
  ({ amp: "&", lt: "<", gt: ">", quot: '"', apos: "'" })[name] ?? _match);
const attributes = (text: string): Record<string, string> => Object.fromEntries(
  [...text.matchAll(/([\w_]+)="((?:\\.|[^"\\])*)"/g)].map((match) => {
    // Inbox/actor attributes use JSON strings; agent attributes use XML escaping.
    let value = match[2]!;
    try { value = JSON.parse(`"${value}"`) as string; } catch { /* XML attribute, not JSON. */ }
    return [match[1]!, decodeXml(value)];
  }),
);
const contentText = (message: IncomingMessage): string => typeof message.content === "string" ? message.content :
  message.content.filter(part => part.type === "text").map(part => part.text).join("\n");

/** Mirrors participantRole without importing its project/git discovery closure. */
export const incomingMessagesCollapsed = (
  mode: FabricIncomingMessageMode,
  environment: NodeJS.ProcessEnv = process.env,
): boolean => mode === "collapsed" || (mode === "auto" &&
  ["org", "org-agent"].includes((environment.PI_FABRIC_ROLE ?? environment.SMARTY_ROLE ?? "").split("@")[0]!.trim()));

// These are display labels, not authority checks. Do not infer a destination
// from an originating `principal` receipt or from a mention in the body.
const orgSender = (name: string): boolean => ["org", "org-agent"].includes(name.split("@")[0]!);
const rows = (message: IncomingMessage): Row[] => {
  const text = contentText(message);
  const details = record(message.details);
  const items = Array.isArray(details.items) ? details.items : [details];
  // A carrier-level recipient applies to each item that has no marker of its own.
  const carrierTo = Array.isArray(details.items) ? details.to ?? record(details.data).to : undefined;
  const row = (body: string, attrs: Record<string, string>, index: number, span?: readonly [number, number]): Row => {
    const item = record(items[index]);
    const from = record(item.from);
    const actor = record(item.actor);
    const data = record(item.data);
    const sender = String(attrs.from_name ?? attrs.name ?? from.name ?? actor.name ?? "Fabric");
    const delivery = attrs.delivery ?? (typeof item.delivery === "string" ? item.delivery : record(item.delivery).mode);
    const kind = /^mail\.inbound\b/.test(body) ? "mail.inbound" : String(attrs.kind ?? delivery ??
      (message.customType === "pi-fabric-actor" ? "actor" : message.customType === "pi-fabric-inbox-summary" ? "inbox" : "agent"));
    return { sender, body, kind, attributes: attrs, ...(span ? { span } : {}),
      full: orgSender(sender) || (attrs.to ?? item.to ?? data.to ?? carrierTo) === "principal" };
  };
  const matches = [...text.matchAll(/<(fabric-agent-message|fabric-actor|event)\b([^>]*)>([\s\S]*?)<\/\1>/g)];
  return matches.length ? matches.map((match, index) => row(
    decodeXml(match[3]!.replace(/\n<data>[\s\S]*?<\/data>\s*$/, "").trim()), attributes(match[2]!), index,
    [match.index!, match.index! + match[0].length],
  )) : [row(text, {}, 0)];
};
const receipt = (from: string, kind: string, value: string): string =>
  createHash("sha256").update(JSON.stringify([from, kind, value])).digest("hex");

type SessionSource = SessionEntryCursorManager & { getHeader?: () => unknown };
type DisplayMessage = { role?: unknown; customType?: unknown; details?: unknown; content?: unknown; timestamp?: unknown };

/** Display-only delivery receipts are updated at lifecycle/message boundaries, never at render time. */
class DeliveredDisplayIndex {
  #cursor = new SessionEntryCursor();
  #receipts = new Set<string>();

  reset(session: SessionSource): void {
    this.#receipts.clear();
    for (const entry of this.#cursor.reset(session).entries) this.#remember(entry);
  }

  addMessage(message: DisplayMessage): void {
    if (message.role === "custom") this.#remember({ ...message, type: "custom_message" });
  }

  get receipts(): ReadonlySet<string> { return this.#receipts; }

  #remember(raw: unknown): void {
    const entry = record(raw);
    if (entry.type !== "custom_message" || entry.customType !== "pi-fabric-agent-message") return;
    const details = record(entry.details);
    const items = Array.isArray(details.items) ? details.items : [details];
    for (const rawItem of items) {
      const item = record(rawItem); const from = record(item.from).id;
      if (typeof from !== "string") continue;
      const add = (kind: string, value: unknown): void => {
        if (typeof value === "string" && value) this.#receipts.add(receipt(from, kind, value));
      };
      add("id", item.id); add("delivery", item.deliveryId);
      const data = record(item.data);
      // Work keys/refs claim work on the carrier's named topic only, as in root-inbox.ts
      // (#852 security P1); a carrier without data.topic hides no shadow by key or ref.
      const topic = typeof data.topic === "string" && data.topic.trim() ? data.topic.trim() : undefined;
      const identity = deliveryIdentity(data);
      if (identity) add(identity.kind, identity.value);
      else if (typeof data.key === "string" && data.key.trim()) { if (topic) add("topic-key", JSON.stringify([topic, data.key.trim()])); }
      else if (typeof data.ref === "string" && data.ref.trim() && topic) add("topic-ref", JSON.stringify([topic, data.ref.trim()]));
    }
  }
}

/** Index prior inbox ids once on session_start/tree change and incrementally on message_end. */
class PriorInboxReceiptIndex {
  #cursor = new SessionEntryCursor();
  #receipts = new Set<string>();
  #byDetails = new WeakMap<object, ReadonlySet<string>>();
  #byContent = new WeakMap<object, Map<number, ReadonlySet<string>>>();
  #byString = new Map<string, ReadonlySet<string>>();

  reset(session: SessionSource): void {
    this.#receipts.clear();
    this.#byDetails = new WeakMap();
    this.#byContent = new WeakMap();
    this.#byString.clear();
    for (const entry of this.#cursor.reset(session).entries) this.#remember(entry);
  }

  addMessage(message: DisplayMessage): void {
    if (message.role === "custom") this.#remember({ ...message, type: "custom_message" });
  }

  prior(message: IncomingMessage): ReadonlySet<string> {
    const details = message.details;
    if (details !== null && typeof details === "object") {
      const found = this.#byDetails.get(details);
      if (found) return found;
    }
    const at = typeof message.timestamp === "number" ? message.timestamp : Number.NaN;
    if (typeof message.content === "string") {
      const found = this.#byString.get(this.#stringKey(message.content, at));
      if (found) return found;
    } else if (message.content !== null && typeof message.content === "object") {
      const found = this.#byContent.get(message.content)?.get(at);
      if (found) return found;
    }
    // A live/custom message that is not yet represented in the branch historically saw every
    // earlier inbox carrier. Its current receipt index is that exact prior-history result.
    return this.#receipts;
  }

  #remember(raw: unknown): void {
    const entry = record(raw);
    if (entry.type !== "custom_message" || entry.customType !== "pi-fabric-inbox") return;
    const at = typeof entry.timestamp === "string" ? Date.parse(entry.timestamp) : entry.timestamp;
    const before = new Set(this.#receipts);
    const details = entry.details;
    if (details !== null && typeof details === "object" && !this.#byDetails.has(details)) this.#byDetails.set(details, before);
    if (typeof at === "number" && typeof entry.content === "string") {
      const key = this.#stringKey(entry.content, at);
      if (!this.#byString.has(key)) this.#byString.set(key, before);
    } else if (typeof at === "number" && entry.content !== null && typeof entry.content === "object") {
      let byTime = this.#byContent.get(entry.content);
      if (!byTime) this.#byContent.set(entry.content, byTime = new Map());
      if (!byTime.has(at)) byTime.set(at, before);
    }
    const ids = record(details).ids;
    if (Array.isArray(ids)) for (const id of ids) if (typeof id === "string") this.#receipts.add(receipt("", "event", id));
  }

  #stringKey(content: string, at: number): string {
    // Inbox carrier bodies are not retained as index keys; retain only a digest and timestamp.
    return `${at}:${createHash("sha256").update(content).digest("hex")}`;
  }
}

const EMPTY_RECEIPTS: ReadonlySet<string> = new Set();

/** Inbox rows whose event id repeats within this carrier or an earlier one (smarty-dev#5518). */
const repeatedInboxRows = (all: readonly Row[], prior: ReadonlySet<string>): Row[] => {
  const seen = new Set<string>();
  return all.filter(row => {
    const id = row.attributes.id;
    if (!id) return false;
    if (seen.has(id) || prior.has(receipt("", "event", id))) return true;
    seen.add(id);
    return false;
  });
};

const unseenRows = (message: IncomingMessage, all: readonly Row[], delivered: ReadonlySet<string>): Row[] => {
  if (message.customType !== "pi-fabric-inbox") return [...all];
  const carried = record(message.details).receipts;
  const receipts = Array.isArray(carried) ? carried.filter((value): value is string => typeof value === "string") : [];
  // Native inbox receipts group each event's identity/work receipts behind its event hash.
  // This lets mixed batches hide only duplicates, including deliveryId-only shadows.
  const boundaries = all.map(row => row.attributes.id ? receipts.indexOf(receipt("", "event", row.attributes.id)) : -1);
  const seen = new Set<string>();
  return all.filter((row, index) => {
    const eventId = row.attributes.id;
    if (eventId && (seen.has(eventId) || delivered.has(receipt("", "event", eventId)))) return false;
    if (eventId) seen.add(eventId);
    const start = boundaries[index]!;
    if (start >= 0) {
      const next = boundaries.slice(index + 1).find(value => value > start) ?? receipts.length;
      // Native identity receipts are definitive: do not fall back to a shared
      // work key when a distinct deliveryId/messageId says this is unseen work.
      return !receipts.slice(start + 1, next).some(value => delivered.has(value));
    }
    const { from_id: from, id, key, ref, topic } = row.attributes;
    const scoped = (value: string | undefined) => topic && value ? JSON.stringify([topic, value]) : undefined;
    return !from || ![["id", id], ["topic-key", scoped(key)], ["topic-ref", scoped(ref)]].some(([kind, value]) =>
      value && delivered.has(receipt(from, kind!, value)));
  });
};

/** The carrier text without the given rows' source spans (and the line break before each). */
const withoutRows = (text: string, hidden: readonly Row[]): string => hidden
  .flatMap(row => row.span ? [row.span] : [])
  .sort((left, right) => right[0] - left[0])
  .reduce((current, [start, end]) => {
    const from = start > 0 && current[start - 1] === "\n" ? start - 1 : start;
    return current.slice(0, from) + current.slice(end);
  }, text);

/**
 * Pi's markdown theme built from the host-supplied renderer Theme (as in
 * dashboard-detail.ts), not the SDK's runtime getMarkdownTheme().
 */
const incomingMarkdownTheme = (theme: Parameters<MessageRenderer>[2], invalidate: () => void): MarkdownTheme => ({
  heading: text => theme.fg("mdHeading", text),
  link: text => theme.fg("mdLink", text),
  linkUrl: text => theme.fg("mdLinkUrl", text),
  code: text => theme.fg("mdCode", text),
  codeBlock: text => theme.fg("mdCodeBlock", text),
  codeBlockBorder: text => theme.fg("mdCodeBlockBorder", text),
  quote: text => theme.fg("mdQuote", text),
  quoteBorder: text => theme.fg("mdQuoteBorder", text),
  hr: text => theme.fg("mdHr", text),
  listBullet: text => theme.fg("mdListBullet", text),
  bold: text => theme.bold(text),
  italic: text => theme.italic(text),
  underline: text => theme.underline(text),
  strikethrough: text => theme.strikethrough(text),
  highlightCode: (code, lang) => highlightCode(code, lang ?? "", invalidate) ??
    code.split("\n").map(line => theme.fg("mdCodeBlock", line)),
});

/**
 * Mirrors Pi's native custom-message rendering (label + full markdown body) for a
 * carrier from which already-delivered rows were removed. Nothing is collapsed or clipped.
 */
const fullCarrier = (message: IncomingMessage, hidden: readonly Row[], theme: Parameters<MessageRenderer>[2]) => {
  const box = new Box(1, 1, text => theme.bg("customMessageBg", text));
  box.addChild(new Text(theme.fg("customMessageLabel", `\x1b[1m[${message.customType}]\x1b[22m`), 0, 0));
  box.addChild(new Spacer(1));
  // The first fenced block with a language starts the shared highlighter; once
  // it is ready, drop the cached lines so the next host render colors the code.
  const markdown: Markdown = new Markdown(withoutRows(contentText(message), hidden), 0, 0,
    incomingMarkdownTheme(theme, () => markdown.invalidate()), { color: text => theme.fg("customMessageText", text) });
  box.addChild(markdown);
  return box;
};

/** Native expanded fallback keeps the entire current rendering; indexes update off the render path. */
export const registerIncomingMessageRenderers = (
  pi: ExtensionAPI,
  mode: () => FabricIncomingMessageMode,
): void => {
  const delivered = new DeliveredDisplayIndex();
  const priorInbox = new PriorInboxReceiptIndex();
  const rebuild = (context: ExtensionContext): void => {
    const session = context.sessionManager as unknown as SessionSource;
    delivered.reset(session);
    priorInbox.reset(session);
  };
  pi.on("session_start", (_event, context) => rebuild(context));
  pi.on("session_tree", (_event, context) => rebuild(context));
  pi.on("message_end", (event) => {
    const message = event.message as unknown as DisplayMessage;
    delivered.addMessage(message);
    priorInbox.addMessage(message);
  });
  for (const type of INCOMING_MESSAGE_TYPES) {
    pi.registerMessageRenderer(type, (message, options, theme) => {
      const prior = type === "pi-fabric-inbox" ? priorInbox.prior(message) : EMPTY_RECEIPTS;
      const all = rows(message);
      if (options.expanded || !incomingMessagesCollapsed(mode())) {
        if (type !== "pi-fabric-inbox") return undefined;
        // A repeated event id renders once; otherwise the native expanded rendering is unchanged.
        const repeated = repeatedInboxRows(all, prior);
        return repeated.length ? fullCarrier(message, repeated, theme) : undefined;
      }
      const receipts = new Set(type === "pi-fabric-inbox" ? delivered.receipts : []);
      for (const id of prior) receipts.add(id);
      const visible = unseenRows(message, all, receipts);
      if (!visible.length) return { render: () => [], invalidate() {} };
      // In the org's own pane, principal replies are the local assistant's, not
      // incoming carriers. Other panes keep full org/principal-addressed bursts,
      // but never let that fallback restore a delivered shadow filtered above.
      if (!incomingMessagesCollapsed("auto") && visible.some(row => row.full)) {
        return visible.length === all.length ? undefined : fullCarrier(message, all.filter(row => !visible.includes(row)), theme);
      }
      const first = visible[0]!;
      const body = Array.from(safeText(first.body));
      const preview = body.length > 80 ? `${body.slice(0, 80).join("")}…` : body.join("");
      const senders = new Set(visible.map(row => row.attributes.from_id ?? safeText(row.sender))).size;
      const kinds = [...new Set(visible.map(row => safeText(row.kind)))].join("/");
      const sender = `${safeText(first.sender)}${senders > 1 ? ` +${senders - 1} sender${senders > 2 ? "s" : ""}` : ""}`;
      const summary = `↳ ${sender}: [${visible.length} ${kinds}] ${preview}`;
      return {
        render: width => [truncateToWidth(theme.fg("dim", `${" ".repeat(Math.min(options.outputPad, Math.max(0, width - 1)))}${summary}`), width)],
        invalidate() {},
      };
    });
  }
};
