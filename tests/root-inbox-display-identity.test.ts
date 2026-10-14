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
