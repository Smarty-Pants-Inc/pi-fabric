import type { ExtensionAPI, MessageRenderer } from "@earendil-works/pi-coding-agent";
import { truncateToWidth } from "@earendil-works/pi-tui";
import { createHash } from "node:crypto";
import type { FabricIncomingMessageMode } from "../config.js";
import { safeText } from "./format.js";

export const INCOMING_MESSAGE_TYPES = [
  "pi-fabric-agent-message", // Includes mail.inbound follow-up notices.
  "pi-fabric-actor",
  "pi-fabric-inbox",
  "pi-fabric-inbox-summary",
] as const;

type IncomingMessage = Parameters<MessageRenderer>[0];
type Row = { sender: string; body: string; attributes: Record<string, string> };
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

const rows = (message: IncomingMessage): Row[] => {
  const text = contentText(message);
  const matches = [...text.matchAll(/<(fabric-agent-message|fabric-actor|event)\b([^>]*)>([\s\S]*?)<\/\1>/g)];
  if (matches.length) return matches.map(match => {
    const attrs = attributes(match[2]!);
    return { sender: attrs.from_name ?? attrs.name ?? "Fabric", attributes: attrs,
      body: decodeXml(match[3]!.replace(/\n<data>[\s\S]*?<\/data>\s*$/, "").trim()) };
  });
  const details = record(message.details);
  const from = record(details.from);
  const actor = record(details.actor);
  return [{ sender: String(from.name ?? actor.name ?? "Fabric"), body: text, attributes: {} }];
};
const receipt = (from: string, kind: string, value: string): string =>
  createHash("sha256").update(JSON.stringify([from, kind, value])).digest("hex");

/** A display-only index, separate from the authoritative delivery/inbox state. */
class DeliveredDisplayIndex {
  #seen: readonly unknown[] = [];
  #receipts = new Set<string>();
  update(entries: readonly unknown[]): ReadonlySet<string> {
    const previous = this.#seen;
    if (entries.length < previous.length || (previous.length && entries[previous.length - 1] !== previous.at(-1))) {
      this.#receipts.clear();
      this.#seen = [];
    }
    for (let index = this.#seen.length; index < entries.length; index++) {
      const entry = record(entries[index]);
      if (entry.type !== "custom_message" || entry.customType !== "pi-fabric-agent-message") continue;
      const details = record(entry.details);
      const items = Array.isArray(details.items) ? details.items : [details];
      for (const raw of items) {
        const item = record(raw); const from = record(item.from).id;
        if (typeof from !== "string") continue;
        const add = (kind: string, value: unknown): void => {
          if (typeof value === "string" && value) this.#receipts.add(receipt(from, kind, value));
        };
        add("id", item.id); add("delivery", item.deliveryId);
        const data = record(item.data);
        if (typeof data.deliveryId === "string" && data.deliveryId) add("delivery", data.deliveryId);
        else if (typeof data.messageId === "string" && data.messageId) add("id", data.messageId);
        else if (typeof data.key === "string" && data.key.trim()) add("key", data.key.trim());
        else if (typeof data.ref === "string" && data.ref.trim()) add("ref", data.ref.trim());
      }
    }
    this.#seen = entries;
    return this.#receipts;
  }
}

/** Only earlier inbox carriers count; rendering the first carrier must not hide itself. */
const priorInboxReceipts = (entries: readonly unknown[], message: IncomingMessage): Set<string> => {
  const seen = new Set<string>();
  for (const raw of entries) {
    const entry = record(raw);
    if (entry.type !== "custom_message" || entry.customType !== "pi-fabric-inbox") continue;
    const at = typeof entry.timestamp === "string" ? Date.parse(entry.timestamp) : entry.timestamp;
    if (entry.details === message.details || (entry.content === message.content && at === message.timestamp)) break;
    const ids = record(entry.details).ids;
    if (Array.isArray(ids)) for (const id of ids) if (typeof id === "string") seen.add(receipt("", "event", id));
  }
  return seen;
};

const unseenRows = (message: IncomingMessage, delivered: ReadonlySet<string>): Row[] => {
  const all = rows(message);
  if (message.customType !== "pi-fabric-inbox") return all;
  const carried = record(message.details).receipts;
  const receipts = Array.isArray(carried) ? carried.filter((value): value is string => typeof value === "string") : [];
  // Native inbox receipts group each event's identity/work receipts behind its event hash.
  // This lets mixed batches hide only duplicates, including deliveryId-only shadows.
  const boundaries = all.map(row => row.attributes.id ? receipts.indexOf(receipt("", "event", row.attributes.id)) : -1);
  const seen = new Set<string>();
  return all.filter((row, index) => {
    const id = row.attributes.id;
    if (id && (seen.has(id) || delivered.has(receipt("", "event", id)))) return false;
    if (id) seen.add(id);
    const start = boundaries[index]!;
    if (start >= 0) {
      const next = boundaries.slice(index + 1).find(value => value > start) ?? receipts.length;
      // Native identity receipts are definitive: do not fall back to a shared
      // work key when a distinct deliveryId/messageId says this is unseen work.
      return !receipts.slice(start + 1, next).some(value => delivered.has(value));
    }
    const { from_id: from, id: rowId, key, ref } = row.attributes;
    return !from || ![["id", rowId], ["key", key], ["ref", ref]].some(([kind, value]) =>
      value && delivered.has(receipt(from, kind!, value)));
  });
};

/** No context/message hooks: native expanded fallback keeps the entire current rendering. */
export const registerIncomingMessageRenderers = (
  pi: ExtensionAPI,
  mode: () => FabricIncomingMessageMode,
): void => {
  let entries: () => readonly unknown[] = () => [];
  const delivered = new DeliveredDisplayIndex();
  pi.on("session_start", (_event, context) => {
    entries = () => context.sessionManager.getBranch();
  });
  for (const type of INCOMING_MESSAGE_TYPES) {
    pi.registerMessageRenderer(type, (message, options, theme) => {
      if (options.expanded || !incomingMessagesCollapsed(mode())) return undefined;
      const history = type === "pi-fabric-inbox" ? entries() : [];
      const receipts = new Set(type === "pi-fabric-inbox" ? delivered.update(history) : []);
      for (const id of priorInboxReceipts(history, message)) receipts.add(id);
      const visible = unseenRows(message, receipts);
      if (!visible.length) return { render: () => [], invalidate() {} };
      const first = visible[0]!;
      const body = Array.from(safeText(first.body));
      const preview = body.length > 80 ? `${body.slice(0, 80).join("")}…` : body.join("");
      const summary = `↳ ${safeText(first.sender)}: ${preview}${visible.length > 1 ? ` (+${visible.length - 1} more)` : ""}`;
      return {
        render: width => [truncateToWidth(theme.fg("dim", `${" ".repeat(Math.min(options.outputPad, Math.max(0, width - 1)))}${summary}`), width)],
        invalidate() {},
      };
    });
  }
};
