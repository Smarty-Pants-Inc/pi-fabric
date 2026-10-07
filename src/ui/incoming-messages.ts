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
type Row = { sender: string; body: string; kind: string; full: boolean; attributes: Record<string, string> };
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
  const row = (body: string, attrs: Record<string, string>, index: number): Row => {
    const item = record(items[index]);
    const from = record(item.from);
    const actor = record(item.actor);
    const data = record(item.data);
    const sender = String(attrs.from_name ?? attrs.name ?? from.name ?? actor.name ?? "Fabric");
    const delivery = attrs.delivery ?? (typeof item.delivery === "string" ? item.delivery : record(item.delivery).mode);
    const kind = /^mail\.inbound\b/.test(body) ? "mail.inbound" : String(attrs.kind ?? delivery ??
      (message.customType === "pi-fabric-actor" ? "actor" : message.customType === "pi-fabric-inbox-summary" ? "inbox" : "agent"));
    return { sender, body, kind, attributes: attrs,
      full: orgSender(sender) || (attrs.to ?? item.to ?? data.to ?? carrierTo) === "principal" };
  };
  const matches = [...text.matchAll(/<(fabric-agent-message|fabric-actor|event)\b([^>]*)>([\s\S]*?)<\/\1>/g)];
  return matches.length ? matches.map((match, index) => row(
    decodeXml(match[3]!.replace(/\n<data>[\s\S]*?<\/data>\s*$/, "").trim()), attributes(match[2]!), index,
  )) : [row(text, {}, 0)];
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

const unseenRows = (message: IncomingMessage, delivered: ReadonlySet<string>): Row[] => {
  const all = rows(message);
  if (message.customType !== "pi-fabric-inbox") return all;
  const carried = record(message.details).receipts;
  const receipts = Array.isArray(carried) ? carried.filter((value): value is string => typeof value === "string") : [];
  // Native inbox receipts group each event's identity/work receipts behind its event hash.
  // This lets mixed batches hide only duplicates, including deliveryId-only shadows.
  const boundaries = all.map(row => row.attributes.id ? receipts.indexOf(receipt("", "event", row.attributes.id)) : -1);
  return all.filter((row, index) => {
    const start = boundaries[index]!;
    if (start >= 0) {
      const next = boundaries.slice(index + 1).find(value => value > start) ?? receipts.length;
      // Native identity receipts are definitive: do not fall back to a shared
      // work key when a distinct deliveryId/messageId says this is unseen work.
      return !receipts.slice(start + 1, next).some(value => delivered.has(value));
    }
    const { from_id: from, id, key, ref } = row.attributes;
    return !from || ![["id", id], ["key", key], ["ref", ref]].some(([kind, value]) =>
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
      const visible = unseenRows(message, type === "pi-fabric-inbox" ? delivered.update(entries()) : new Set());
      if (!visible.length) return { render: () => [], invalidate() {} };
      // A native carrier is the burst boundary. If it mixes a principal reply
      // with chatter, preserve the entire native rendering rather than clip it.
      if (visible.some(row => row.full)) return undefined;
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
