import type { ExtensionAPI, ExtensionContext, MessageRenderer, Theme } from "@earendil-works/pi-coding-agent";
import { CustomMessageComponent, initTheme } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";
import { registerIncomingMessageRenderers } from "../src/ui/incoming-messages.js";
import { rootInboxMessage } from "../src/topology/root-inbox.js";
import type { MeshEvent, MeshIdentity } from "../src/mesh/store.js";

type CustomMessage = Parameters<MessageRenderer>[0];

const theme = {
  fg: (_name: string, text: string) => text,
  bg: (_name: string, text: string) => text,
} as Theme;
const plain = (text: string): string => text.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "");
const peer: MeshIdentity = { id: "session:peer", name: "Build & Test", kind: "main" };
const other: MeshIdentity = { id: "session:other", name: "Other", kind: "main" };

const native = (from: MeshIdentity, details: Record<string, unknown>, body = "already delivered"): CustomMessage => ({
  role: "custom", customType: "pi-fabric-agent-message", display: true, timestamp: 123,
  content: `<fabric-agent-message from_name="${from.name}" from_id="${from.id}">${body}</fabric-agent-message>`,
  details: { from, ...details },
});
const event = (id: string, from: MeshIdentity, data: unknown, text = "shadow duplicate"): MeshEvent => ({
  id, sequence: 1, topic: "fleet.work", kind: "work", from, to: "session:root", text, data, createdAt: 123,
});
const inbox = (events: MeshEvent[]): CustomMessage => ({ ...rootInboxMessage(events), role: "custom", timestamp: 123 });
const entry = (value: CustomMessage): unknown => ({ ...value, type: "custom_message" });

const harness = () => {
  let branch: unknown[] = [];
  const renderers = new Map<string, MessageRenderer>();
  let start: ((event: unknown, context: ExtensionContext) => unknown) | undefined;
  const on = (_name: string, callback: (event: unknown, context: ExtensionContext) => unknown): void => { start = callback; };
  const registerMessageRenderer = (type: string, render: MessageRenderer): void => { renderers.set(type, render); };
  registerIncomingMessageRenderers({ on, registerMessageRenderer } as unknown as ExtensionAPI, () => "collapsed");
  start!({}, { sessionManager: { getBranch: () => branch } } as unknown as ExtensionContext);
  const render = (value: CustomMessage): readonly string[] | undefined =>
    renderers.get(value.customType)!(value, { expanded: false, outputPad: 1 }, theme)?.render(200);
  return {
    render,
    renderer: renderers.get("pi-fabric-inbox")!,
    setBranch: (entries: unknown[]) => { branch = entries; },
  };
};

describe("root inbox display identity projection (smarty-dev#12140)", () => {
  it.each([
    ["cross-kind overflow", { deliveryId: "overflow-id" }, { messageId: "overflow-id" }],
    ["same-kind explicit overflow", { deliveryId: "overflow-id" }, { deliveryId: "overflow-id" }],
    ["key-only overflow", { data: { topic: "fleet.work", key: "overflow-key" } }, { key: "overflow-key" }],
    ["unrelated overflow", { deliveryId: "overflow-id" }, { messageId: "unrelated-id" }],
    ["no overflow", { deliveryId: "overflow-id" }, undefined],
  ])("preserves eight unseen shown rows with %s", (_name, delivered, overflowData) => {
    const h = harness();
    h.setBranch([entry(native(peer, delivered))]);
    const shown = Array.from({ length: 8 }, (_, index) =>
      event(`ROW_${index}`, peer, { key: `unseen-${index}` }, `ROW_${index}`));
    const events = overflowData === undefined ? shown : [...shown, event("overflow", peer, overflowData)];
    const rendered = h.render(inbox(events));
    expect(rendered).toHaveLength(1);
    expect(plain(rendered!.join("\n"))).toContain("[8 work]");
  });

  it.each([
    ["principal", (content: string) => content.replace('id="ROW_7"', 'id="ROW_7" to="principal"'), peer],
    ["org", (content: string) => content, { id: "session:org", name: "org", kind: "main" as const }],
  ])("preserves a full %s last shown row beside delivered overflow", (_name, mark, fullSender) => {
    initTheme("dark", false);
    const h = harness();
    h.setBranch([entry(native(peer, { deliveryId: "overflow-id" }))]);
    const shown = Array.from({ length: 8 }, (_, index) => event(
      `ROW_${index}`, index === 7 ? fullSender : peer, { key: `unseen-${index}` },
      index === 7 ? "FULL_LAST_REPLY" : `ROW_${index}`,
    ));
    const carrier = inbox([...shown, event("overflow", peer, { messageId: "overflow-id" })]);
    const marked = { ...carrier, content: mark(String(carrier.content)) };
    const rendered = new CustomMessageComponent(marked, h.renderer).render(120).map(plain).join("\n");
    expect(rendered).toContain("FULL_LAST_REPLY");
    expect(rendered).toContain("ROW_6");
    expect(rendered).not.toContain("↳");
  });

  it.each([
    ["native deliveryId to shadow messageId", { deliveryId: "cross-delivery" }, { messageId: "cross-delivery", key: "same-work" }],
    ["native item id to shadow deliveryId", { id: "cross-item" }, { deliveryId: "cross-item", key: "same-work" }],
  ])("hides explicit cross-kind shadow: %s", (_name, nativeDetails, shadowData) => {
    const h = harness();
    h.setBranch([entry(native(peer, nativeDetails))]);
    expect(h.render(inbox([event("shadow", peer, shadowData)]))).toEqual([]);
  });

  it.each([
    ["principal", (content: string) => content.replace('id="fresh"', 'id="fresh" to="principal"'), peer],
    ["org", (content: string) => content, { id: "session:org", name: "org", kind: "main" as const }],
  ])("removes a delivered shadow while preserving a full %s row", (_name, mark, fullSender) => {
    initTheme("dark", false);
    const h = harness();
    h.setBranch([entry(native(peer, { deliveryId: "mixed-cross" }))]);
    const duplicate = event("duplicate", peer, { messageId: "mixed-cross" }, "SHADOW_DUPLICATE");
    const full = event("fresh", fullSender, { key: "fresh-work" }, "FULL_REPLY");
    const carrier = inbox([duplicate, full]);
    const marked = { ...carrier, content: mark(String(carrier.content)) };
    const rendered = new CustomMessageComponent(marked, h.renderer).render(120).map(plain).join("\n");
    expect(rendered).toContain("FULL_REPLY");
    expect(rendered).not.toContain("SHADOW_DUPLICATE");
    expect(rendered).not.toContain("↳");
  });

  it.each([
    ["key", { key: "same-work" }],
    ["ref", { ref: "same-work" }],
  ])("shows a distinct %s-only event after a native carrier with the same topic and work claim", (_name, claim) => {
    const h = harness();
    h.setBranch([entry(native(peer, { id: "earlier-native-notification", data: { topic: "fleet.work", ...claim } }))]);
    const rendered = h.render(inbox([event("distinct-work-event", peer, claim, "NEW_NOTIFICATION")]));
    expect(rendered).toHaveLength(1);
    expect(plain(rendered!.join("\n"))).toContain("NEW_NOTIFICATION");
  });

  it("hides a shadow sharing an explicit delivery id with the delivered native carrier", () => {
    const h = harness();
    h.setBranch([entry(native(peer, { deliveryId: "shared-delivery", data: { topic: "fleet.work", key: "same-work" } }))]);
    expect(h.render(inbox([event("shadow", peer, { deliveryId: "shared-delivery", key: "same-work" })]))).toEqual([]);
  });

  it("hides a production command-id shadow matching the native delivery id", () => {
    const h = harness();
    h.setBranch([entry(native(peer, { deliveryId: "native-command", data: { topic: "fleet.work", key: "same-work" } }))]);
    expect(h.render(inbox([event("command-shadow", peer, { commandId: "native-command", key: "same-work" })]))).toEqual([]);
  });

  it("hides a command-id shadow matching native details.data.commandId", () => {
    const h = harness();
    h.setBranch([entry(native(peer, { data: { commandId: "native-command", topic: "fleet.work", key: "same-work" } }))]);
    expect(h.render(inbox([event("command-shadow", peer, { commandId: "native-command", key: "same-work" })]))).toEqual([]);
  });

  it("shows a different command-id event sharing the native topic and key", () => {
    const h = harness();
    h.setBranch([entry(native(peer, { data: { commandId: "native-command", topic: "fleet.work", key: "same-work" } }))]);
    const rendered = h.render(inbox([event("distinct-command", peer, { commandId: "different-command", key: "same-work" })]));
    expect(rendered).toHaveLength(1);
    expect(plain(rendered!.join("\n"))).toContain("shadow duplicate");
  });

  it.each([
    ["different commandId with the same sender, topic and key", native(peer, { deliveryId: "native-command", data: { topic: "fleet.work", key: "same-work" } }), event("distinct-command", peer, { commandId: "new-command", key: "same-work" })],
    ["commandId sender mismatch", native(peer, { deliveryId: "native-command" }), event("command-sender", other, { commandId: "native-command" })],
    ["deliveryId outranks a same commandId", native(peer, { deliveryId: "native-command" }), event("command-precedence", peer, { deliveryId: "distinct-delivery", commandId: "native-command", key: "same-work" })],
    ["native commandId yields to a distinct deliveryId", native(peer, { data: { commandId: "native-command", topic: "fleet.work", key: "same-work" } }), event("command-delivery-precedence", peer, { deliveryId: "distinct-delivery", commandId: "native-command", key: "same-work" })],
    ["native commandId yields to a distinct messageId", native(peer, { data: { commandId: "native-command", topic: "fleet.work", key: "same-work" } }), event("command-message-precedence", peer, { messageId: "distinct-message", commandId: "native-command", key: "same-work" })],
    ["native commandId sender mismatch", native(peer, { data: { commandId: "native-command", topic: "fleet.work", key: "same-work" } }), event("command-sender-mismatch", other, { commandId: "native-command", key: "same-work" })],
    ["malformed commandId with the same key", native(peer, { data: { topic: "fleet.work", key: "same-work" } }), event("invalid-command", peer, { commandId: 1, key: "same-work" })],
    ["distinct explicit identity with the same key", native(peer, { data: { messageId: "native-id", key: "same-work" } }), event("distinct", peer, { messageId: "shadow-id", key: "same-work" })],
    ["sender mismatch", native(peer, { deliveryId: "cross-sender" }), event("sender", other, { messageId: "cross-sender" })],
    ["deliveryId outranks a same messageId", native(peer, { data: { messageId: "message-id", key: "same-work" } }), event("precedence", peer, { deliveryId: "delivery-id", messageId: "message-id", key: "same-work" })],
  ])("renders counterexample: %s", (_name, delivered, shadow) => {
    const h = harness();
    h.setBranch([entry(delivered)]);
    const rendered = h.render(inbox([shadow]));
    expect(rendered).toHaveLength(1);
    expect(plain(rendered!.join("\n"))).toContain("shadow duplicate");
  });
});
