import type { ExtensionAPI, ExtensionContext, MessageRenderer, Theme } from "@earendil-works/pi-coding-agent";
import { convertToLlm, CustomMessageComponent, initTheme } from "@earendil-works/pi-coding-agent";
import { visibleWidth } from "@earendil-works/pi-tui";
import { afterEach, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig, type FabricIncomingMessageMode } from "../src/config.js";
import { INCOMING_MESSAGE_TYPES, incomingMessagesCollapsed, registerIncomingMessageRenderers } from "../src/ui/incoming-messages.js";
import { rootInboxMessage } from "../src/topology/root-inbox.js";
import type { MeshEvent } from "../src/mesh/store.js";

type CustomMessage = Parameters<MessageRenderer>[0];

const theme = { fg: (name: string, text: string) => name === "dim" ? `\x1b[2m${text}\x1b[22m` : text } as Theme;
const plain = (text: string) => text.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "");
const sender = { id: "session:peer", name: "Build & Test", kind: "main" as const };
const message = (body = "first\n  second\tthird & <safe>"): CustomMessage => ({
  role: "custom", customType: "pi-fabric-agent-message", display: true, timestamp: 123,
  content: `<fabric-agent-message from_name="Build &amp; Test" from_id="session:peer">\n${body.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")}\n<data>{"internal":"not a preview"}</data>\n</fabric-agent-message>`,
  details: { id: "native-1", from: sender, data: { key: "same-work", topic: "fleet.work" } },
});
const event = (id: string, text: string, data: unknown = { key: "same-work" }): MeshEvent => ({
  id, sequence: 1, topic: "fleet.work", kind: "work", from: sender, text, data, createdAt: 123,
});
const inbox = (events: MeshEvent[]): CustomMessage => ({ ...rootInboxMessage(events), role: "custom", timestamp: 123 });
const entry = (value: CustomMessage) => ({ ...value, type: "custom_message" });
const harness = (initial: FabricIncomingMessageMode = "collapsed") => {
  let mode = initial; let branch: unknown[] = [];
  const renderers = new Map<string, MessageRenderer>();
  const handlers = new Map<string, Array<(event: any, context: ExtensionContext) => unknown>>();
  const on = vi.fn((name: string, fn: (event: any, context: ExtensionContext) => unknown) => {
    const list = handlers.get(name) ?? []; list.push(fn); handlers.set(name, list);
  });
  const registerMessageRenderer = vi.fn((type: string, render: MessageRenderer) => renderers.set(type, render));
  registerIncomingMessageRenderers({ on, registerMessageRenderer } as unknown as ExtensionAPI, () => mode);
  const getBranch = vi.fn(() => branch);
  const getEntries = vi.fn(() => [...branch]);
  const getLeafEntry = vi.fn(() => branch.at(-1));
  const byId = new Map<string, unknown>();
  const getEntry = vi.fn((id: string) => byId.get(id));
  const sessionManager = {
    getLeafEntry,
    getEntry,
    getSessionId: () => "renderer-test-session",
    getHeader: () => ({ version: 3 }),
    getEntries,
    getBranch,
  };
  const context = { sessionManager } as unknown as ExtensionContext;
  for (const handler of handlers.get("session_start") ?? []) handler({}, context);
  const render = (value: CustomMessage, expanded = false, width = 200) =>
    renderers.get(value.customType)!(value, { expanded, outputPad: 1 }, theme)?.render(width);
  const setBranch = (entries: unknown[]) => {
    byId.clear();
    let parentId: string | null = null;
    branch = entries.map((raw, index) => {
      const value = raw && typeof raw === "object" ? raw as Record<string, unknown> : {};
      const id = typeof value.id === "string" ? value.id : `renderer-entry-${index}`;
      const next = { ...value, id, parentId };
      byId.set(id, next); parentId = id;
      return next;
    });
    for (const handler of handlers.get("session_tree") ?? []) handler({}, context);
  };
  const messageEnd = (value: CustomMessage) => {
    for (const handler of handlers.get("message_end") ?? []) handler({ message: value }, context);
  };
  return { render, renderers, on, getBranch, getEntries, getLeafEntry, getEntry,
    setMode: (value: FabricIncomingMessageMode) => { mode = value; },
    setBranch, messageEnd };
};
afterEach(() => vi.unstubAllEnvs());

describe("incoming Fabric display projection", () => {
  it("registers only incoming types, no context transformation or reply renderer", () => {
    const h = harness();
    expect([...h.renderers.keys()]).toEqual([...INCOMING_MESSAGE_TYPES]);
    expect(h.on.mock.calls.map(call => call[0])).toEqual(["session_start", "session_tree", "message_end"]);
  });

  it("does not read the session branch on each inbox render", () => {
    const h = harness();
    const carrier = inbox([event("rendered", "visible")]);
    const leafLookups = h.getLeafEntry.mock.calls.length;
    const pointLookups = h.getEntry.mock.calls.length;
    for (let index = 0; index < 8; index++) h.render(carrier);
    expect(h.getBranch).not.toHaveBeenCalled();
    expect(h.getEntries).not.toHaveBeenCalled();
    expect(h.getLeafEntry).toHaveBeenCalledTimes(leafLookups);
    expect(h.getEntry).toHaveBeenCalledTimes(pointLookups);
  });

  it("keeps prior-inbox rendering constant-time after indexing a large branch", () => {
    const h = harness();
    const prior = inbox([event("large-history-event", "prior body")]);
    const rows = Array.from({ length: 2_000 }, () => ({ type: "message", message: { role: "user", content: "history" } }));
    h.setBranch([...rows, entry(prior)]);
    const pointLookups = h.getEntry.mock.calls.length;
    const leafLookups = h.getLeafEntry.mock.calls.length;
    expect(plain(h.render(prior)![0]!)).toContain("prior body");
    const replay = { ...inbox([event("large-history-event", "replayed body")]), timestamp: prior.timestamp + 60_000 };
    for (let index = 0; index < 8; index++) expect(h.render(replay)).toEqual([]);
    expect(h.getBranch).not.toHaveBeenCalled();
    expect(h.getEntries).not.toHaveBeenCalled();
    expect(h.getEntry).toHaveBeenCalledTimes(pointLookups);
    expect(h.getLeafEntry).toHaveBeenCalledTimes(leafLookups);
  });

  it("renders exactly one dim decoded whitespace-collapsed body line", () => {
    const lines = harness().render(message())!;
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("\x1b[2m");
    expect(plain(lines[0]!)).toBe(" ↳ Build & Test: [1 agent] first second third & <safe>");
    expect(lines[0]).not.toContain("internal");
  });

  it("bounds body previews and terminal width without wrapping or control injection", () => {
    const h = harness(); const value = message("😀".repeat(90) + "\x1b[31m\nnew line");
    const full = plain(h.render(value)![0]!);
    expect(full).toBe(" ↳ Build & Test: [1 agent] " + "😀".repeat(80) + "…");
    for (const width of [0, 1, 5, 35, 80]) {
      const lines = h.render(value, false, width)!;
      expect(lines).toHaveLength(1);
      expect(visibleWidth(lines[0]!)).toBeLessThanOrEqual(width);
    }
  });

  it("previews batched agent messages once and reports the other messages", () => {
    const first = message(); const second = message("another body");
    const value = { ...first, content: `2 follow-ups released at a boundary\n\n${first.content}\n\n${second.content}` };
    expect(plain(harness().render(value)![0]!)).toBe(" ↳ Build & Test: [2 agent] first second third & <safe>");
  });

  it("collapses mail notices, actor steers, summaries, and content block carriers", () => {
    const h = harness();
    expect(plain(h.render(message("mail.inbound: new mail for you on the mesh"))![0]!))
      .toContain("↳ Build & Test: [1 mail.inbound]");
    const actor = { ...message(), customType: "pi-fabric-actor", content: '<fabric-actor name="actor\\\"name" id="a">\nActor reply\n</fabric-actor>', details: {} };
    expect(plain(h.render(actor)![0]!)).toBe(' ↳ actor"name: [1 actor] Actor reply');
    expect(plain(h.render({ ...message(), customType: "pi-fabric-inbox-summary", content: "Fabric inbox: skipped 45" })![0]!))
      .toContain("skipped 45");
    expect(plain(h.render({ ...message(), content: [{ type: "text", text: "plain body" }] })![0]!))
      .toBe(" ↳ Build & Test: [1 agent] plain body");
  });

  it("identifies a mixed-sender burst and its delivery kinds without changing the carrier", () => {
    const first = message("Build is ready");
    const second = { ...message("Tests are green"),
      content: '<fabric-agent-message from_name="Reviewer" delivery="followUp">Tests are green</fabric-agent-message>' };
    const value = { ...first,
      content: `${String(first.content).replace('from_id="session:peer"', 'from_id="session:peer" delivery="steer"')}\n${second.content}` };
    const bytes = JSON.stringify(value);
    expect(plain(harness().render(value)![0]!)).toBe(" ↳ Build & Test +1 sender: [2 steer/followUp] Build is ready");
    expect(JSON.stringify(value)).toBe(bytes);
  });

  it.each(["org", "org-agent", "org-agent@revision"])("keeps %s replies full even when collapsed", name => {
    initTheme("dark", false);
    const h = harness();
    const value = { ...message(), content: `<fabric-agent-message from_name="${name}" delivery="followUp">Reply to Paul\nFULL_ORG_REPLY</fabric-agent-message>` };
    expect(h.render(value)).toBeUndefined();
    expect(new CustomMessageComponent(value, h.renderers.get(value.customType)).render(120))
      .toEqual(new CustomMessageComponent(value).render(120));
  });

  it("keeps explicitly principal-addressed messages full, including a mixed burst", () => {
    const h = harness();
    const value = { ...message("Principal reply\nFULL_PRINCIPAL_REPLY"),
      details: { from: sender, delivery: "followUp", data: { to: "principal" } } };
    expect(h.render(value)).toBeUndefined();
    const mixed = { ...value, content: `${message("Peer chatter").content}\n${value.content}`,
      details: { items: [message().details, value.details] } };
    expect(h.render(mixed)).toBeUndefined(); // Never clip the principal's reply in a shared carrier.
    expect(h.render({ ...message(), content: '<fabric-agent-message from_name="Builder" to="principal">Full reply</fabric-agent-message>' }))
      .toBeUndefined();
    // Originating authority and a mention in the body are NOT a recipient or org reply.
    expect(h.render({ ...message("org: status for the principal"), details: { principal: { binding: "org-agent" } } }))
      .toHaveLength(1);
  });

  it.each([
    ["details.to", { to: "principal" }],
    ["details.data.to", { data: { to: "principal" } }],
  ])("applies a carrier-level principal marker (%s) to unmarked burst items; an item's own marker wins", (_name, carrier) => {
    initTheme("dark", false);
    const h = harness();
    const peer = { id: "peer-1", from: sender, to: "session:other" };
    const unmarked = { id: "reply-1", from: sender, delivery: "followUp" };
    const mixed = { ...message(), content: `${message("Peer chatter").content}\n${message("Reply for Paul\nFULL_PRINCIPAL_REPLY").content}`,
      details: { ...carrier, items: [peer, unmarked] } };
    expect(h.render(mixed)).toBeUndefined(); // The unmarked item inherits the carrier's principal destination.
    expect(new CustomMessageComponent(mixed, h.renderers.get(mixed.customType)).render(120))
      .toEqual(new CustomMessageComponent(mixed).render(120));
    // Only unmarked items inherit: an item's own non-principal marker (details, data, or XML attribute) wins.
    for (const own of [peer, { id: "peer-2", from: sender, data: { to: "session:other" } }]) {
      expect(h.render({ ...message("Peer chatter"), details: { ...carrier, items: [own] } })).toHaveLength(1);
    }
    expect(h.render({ ...message(), details: { ...carrier, items: [unmarked] },
      content: '<fabric-agent-message from_name="Builder" to="session:other">Peer chatter</fabric-agent-message>' })).toHaveLength(1);
    expect(h.render({ ...message("Reply for Paul"), details: { ...carrier, items: [unmarked] } })).toBeUndefined();
  });

  it.each(["org", "org-agent", "org-agent@revision"])("collapses all incoming labels in the %s pane, with exact native expansion", role => {
    vi.stubEnv("PI_FABRIC_ROLE", undefined); vi.stubEnv("SMARTY_ROLE", role);
    initTheme("dark", false);
    const h = harness("auto");
    const carriers: CustomMessage[] = [
      { ...message("Peer status\nFULL_PEER_DETAIL"), details: { from: sender, delivery: "followUp", data: { to: "principal" } } },
      { ...message(), content: '<fabric-agent-message from_name="org-agent@revision" to="principal">Org-labeled status\nFULL_REMOTE_ORG_DETAIL</fabric-agent-message>' },
      { ...message(), customType: "pi-fabric-actor", content: '<fabric-actor name="org" to="principal">Actor status\nFULL_ACTOR_DETAIL</fabric-actor>' },
      { ...message("mail.inbound: Mail status\nFULL_MAIL_DETAIL"), details: { to: "principal", from: sender } },
      { ...inbox([event("unseen", "Inbox status\nFULL_INBOX_DETAIL", { key: "unseen" })]), details: { to: "principal" } },
      { ...message("Inbox summary"), customType: "pi-fabric-inbox-summary", details: { to: "principal" } },
    ];
    const bytes = JSON.stringify(carriers); const llmBytes = JSON.stringify(convertToLlm(carriers));
    for (const value of carriers) {
      const lines = h.render(value)!;
      expect(lines).toHaveLength(1); expect(lines[0]).toContain("\x1b[2m");
      const actual = new CustomMessageComponent(value, h.renderers.get(value.customType));
      actual.setExpanded(true);
      expect(actual.render(120)).toEqual(new CustomMessageComponent(value).render(120));
      actual.setExpanded(false); expect(plain(actual.render(120).join("\n"))).toContain("↳");
      h.setMode("expanded"); expect(h.render(value)).toBeUndefined(); h.setMode("auto");
    }
    expect(JSON.stringify(carriers)).toBe(bytes); expect(JSON.stringify(convertToLlm(carriers))).toBe(llmBytes);
  });

  it("collapses a principal-addressed mixed org burst once and preserves inbox dedup", () => {
    vi.stubEnv("PI_FABRIC_ROLE", "org"); vi.stubEnv("SMARTY_ROLE", "task-agent");
    const h = harness("collapsed");
    const first = message("Peer status");
    const burst = { ...first, content: `${first.content}\n<fabric-agent-message from_name="org" delivery="followUp">Remote org status</fabric-agent-message>`,
      details: { to: "principal", items: [first.details, { from: { id: "session:org", name: "org" }, delivery: "followUp" }] } };
    expect(plain(h.render(burst)![0]!)).toBe(" ↳ Build & Test +1 sender: [2 agent/followUp] Peer status");
    h.setBranch([entry(message())]);
    // A real shadow keeps the delivered source message id, not merely its work key.
    const shadow = inbox([event("shadow", "SHADOW_DUPLICATE", { key: "same-work", messageId: "native-1" })]);
    expect(h.render(shadow)).toEqual([]);
    expect(h.render(shadow, true)).toBeUndefined();
    // Non-org sessions retain the existing recipient/sender exceptions, even with principal view on.
    vi.stubEnv("PI_FABRIC_ROLE", "project"); vi.stubEnv("SMARTY_ROLE", "org-agent");
    expect(h.render(burst)).toBeUndefined();
  });

  it("preserves distinct durable events sharing sender, topic and work key, but hides event-id replays (smarty-dev#12140)", () => {
    vi.stubEnv("PI_FABRIC_ROLE", "org"); vi.stubEnv("SMARTY_ROLE", "task-agent");
    const h = harness("collapsed");
    const first = event("durable-first", "First durable notification");
    const second = { ...event("durable-second", "Second durable notification"), sequence: 2 };
    // Both source events share the helper's sender, topic and same-work key.
    expect(plain(h.render(inbox([first, second]))![0]!))
      .toBe(" ↳ Build & Test: [2 work] First durable notification");
    const firstInbox = inbox([first]);
    const secondInbox = { ...inbox([second]), timestamp: firstInbox.timestamp + 1 };
    h.setBranch([entry(firstInbox)]);
    expect(plain(h.render(secondInbox)![0]!))
      .toBe(" ↳ Build & Test: [1 work] Second durable notification");
    h.setBranch([entry(firstInbox), entry(secondInbox)]);
    expect(plain(h.render(secondInbox)![0]!))
      .toBe(" ↳ Build & Test: [1 work] Second durable notification"); // Never hide the original carrier itself.
    const replay = { ...inbox([second]), timestamp: secondInbox.timestamp + 60_000 };
    expect(h.render(replay)).toEqual([]);
    // A different mesh sequence does not turn the same source event id into new work.
    expect(h.render({ ...inbox([{ ...second, sequence: 3 }]), timestamp: replay.timestamp + 1 })).toEqual([]);
  });

  it("uses the exact native current rendering when expanded and can collapse again", () => {
    initTheme("dark", false);
    const h = harness(); const value = message("first line\nsecond line\nfinal detail");
    expect(h.render(value, true)).toBeUndefined(); // Pi's documented native fallback.
    const native = new CustomMessageComponent(value);
    const actual = new CustomMessageComponent(value, h.renderers.get(value.customType));
    actual.setExpanded(true);
    expect(actual.render(100)).toEqual(native.render(100));
    expect(plain(actual.render(100).join("\n"))).toContain("final detail");
    actual.setExpanded(false);
    expect(plain(actual.render(100).join("\n"))).toContain("↳ Build & Test:");
    expect(plain(actual.render(100).join("\n"))).not.toContain("[pi-fabric-agent-message]");
  });

  it("snapshots the native transcript before collapse, after collapse, and after expansion", () => {
    initTheme("dark", false);
    const h = harness();
    const value = { ...message(), content: [
      '<fabric-agent-message from_name="Builder" delivery="followUp">Build ready\nCompiler log stays in model context.</fabric-agent-message>',
      '<fabric-agent-message from_name="Reviewer" delivery="followUp">Tests green\nReview detail stays in model context.</fabric-agent-message>',
    ].join("\n\n") };
    const bytes = JSON.stringify(convertToLlm([value]));
    const native = new CustomMessageComponent(value);
    const actual = new CustomMessageComponent(value, h.renderers.get(value.customType));
    const lines = (component: CustomMessageComponent) => component.render(120).map(plain).map(line => line.trim()).filter(Boolean);
    const before = lines(native);
    const collapsed = lines(actual);
    expect({ before, collapsed }).toMatchInlineSnapshot(`
      {
        "before": [
          "[pi-fabric-agent-message]",
          "<fabric-agent-message from_name="Builder" delivery="followUp">Build ready",
          "Compiler log stays in model context.</fabric-agent-message>",
          "<fabric-agent-message from_name="Reviewer" delivery="followUp">Tests green",
          "Review detail stays in model context.</fabric-agent-message>",
        ],
        "collapsed": [
          "↳ Builder +1 sender: [2 followUp] Build ready Compiler log stays in model context.",
        ],
      }
    `);
    expect(collapsed).toHaveLength(1);
    actual.setExpanded(true); expect(lines(actual)).toEqual(before);
    actual.setExpanded(false); expect(lines(actual)).toEqual(collapsed);
    expect(JSON.stringify(convertToLlm([value]))).toBe(bytes);
  });

  it.each(INCOMING_MESSAGE_TYPES)("keeps exact native expanded rendering for %s", customType => {
    initTheme("dark", false);
    const h = harness(); const value = { ...message("full original body\nsecond detail"), customType };
    const native = new CustomMessageComponent(value);
    const actual = new CustomMessageComponent(value, h.renderers.get(customType));
    actual.setExpanded(true);
    expect(actual.render(100)).toEqual(native.render(100));
  });

  it("hides only already-delivered shadows, including mixed and delivery-id-only batches", () => {
    const delivered = { ...message(), details: { id: "native-1", deliveryId: "native-command-1", from: sender,
      data: { key: "same-work", topic: "fleet.work" } } };
    const h = harness(); h.setBranch([entry(delivered)]);
    expect(h.render(inbox([event("shadow-1", "duplicate", { key: "same-work", commandId: "native-command-1" })]))).toEqual([]);
    const mixed = inbox([event("shadow-1", "duplicate", { key: "same-work", commandId: "native-command-1" }), event("unseen", "new work", { key: "different" })]);
    expect(plain(h.render(mixed)![0]!)).toBe(" ↳ Build & Test: [1 work] new work");
    // Same sender and key on another topic is unrelated work (#852 security P1): shown.
    expect(plain(h.render(inbox([{ ...event("other-topic", "other topic work"), topic: "fleet.alarm" }]))![0]!)).toContain("other topic work");
    expect(plain(h.render(inbox([event("same-event", "first", {}), event("same-event", "duplicate second", {})]))![0]!))
      .toBe(" ↳ Build & Test: [1 work] first");
    const delivery = { ...message(), details: { id: "native-2", from: sender, data: { deliveryId: "D1", key: "same-work" } } };
    h.setBranch([entry(message()), entry(delivery)]);
    expect(h.render(inbox([event("d1", "duplicate D1", { deliveryId: "D1" })]))).toEqual([]);
    expect(plain(h.render(inbox([event("d2", "new D2", { deliveryId: "D2", key: "same-work" })]))![0]!)).toContain("new D2");
    expect(h.render(mixed, true)).toBeUndefined();
  });

  it("dedupes an inbox event id repeated a minute later, without hiding the original or new rows", () => {
    const h = harness();
    const first = inbox([event("ca71cd8c", "first copy", {})]);
    const second = { ...inbox([event("ca71cd8c", "REPEATED_COPY", {}), event("new", "new work", {})]), timestamp: first.timestamp + 60_000 };
    h.setBranch([entry(first), entry(second)]);
    expect(plain(h.render(first)![0]!)).toContain("first copy");
    expect(plain(h.render(second)![0]!)).toBe(" ↳ Build & Test: [1 work] new work");
    // Expanded: the native carrier minus the repeated row (upstream fullCarrier rendering).
    initTheme("dark", false);
    const shown = (expanded: boolean) => {
      const component = new CustomMessageComponent(second, h.renderers.get(second.customType));
      component.setExpanded(expanded);
      return component.render(120).map(plain).join("\n");
    };
    expect(shown(true)).toContain("new work");
    expect(shown(true)).not.toContain("REPEATED_COPY");
    h.setMode("expanded");
    expect(shown(false)).toContain("new work");
    expect(shown(false)).not.toContain("REPEATED_COPY");
    h.setMode("collapsed");
    h.setBranch([entry(second)]);
    expect(plain(h.render(second)![0]!)).toContain("REPEATED_COPY");
  });

  it.each([
    ["principal-addressed", (text: string) => text.replace('id="unseen"', 'id="unseen" to="principal"'), { ...sender }],
    ["org-labeled", (text: string) => text, { id: "session:org", name: "org", kind: "main" as const }],
  ])("keeps a delivered shadow suppressed when an unseen %s row renders in full", (_name, mark, from) => {
    initTheme("dark", false);
    const delivered = { ...message(), details: { id: "native-1", deliveryId: "native-command-1", from: sender,
      data: { key: "same-work", topic: "fleet.work" } } };
    const h = harness(); h.setBranch([entry(delivered)]);
    const exempt = { ...event("unseen", "Reply for Paul\nFULL_EXEMPT_REPLY", { key: "different" }), from };
    const base = inbox([event("shadow-1", "SHADOW_DUPLICATE", { key: "same-work", commandId: "native-command-1" }), exempt]);
    const burst = { ...base, content: mark(String(base.content)) };
    const bytes = JSON.stringify(burst);
    const lines = new CustomMessageComponent(burst, h.renderers.get(burst.customType)).render(120).map(plain);
    const text = lines.join("\n");
    expect(text).toContain("Reply for Paul");
    expect(text).toContain("FULL_EXEMPT_REPLY"); // In full: not collapsed to a one-line preview.
    expect(text).not.toContain("SHADOW_DUPLICATE");
    expect(text).not.toContain("↳");
    // Exactly the native rendering of the carrier minus the delivered shadow's row.
    const filtered = { ...burst, content: String(burst.content).replace(/\n<event id="shadow-1"[^\n]*<\/event>/, "") };
    expect(String(filtered.content)).not.toContain("SHADOW_DUPLICATE");
    expect(lines).toEqual(new CustomMessageComponent(filtered).render(120).map(plain));
    expect(JSON.stringify(burst)).toBe(bytes); // The carrier itself is untouched.
    // Expanding still shows the exact native carrier, shadow included.
    expect(h.render(burst, true)).toBeUndefined();
  });

  it("keeps the native fallback unchanged for an exempt burst with no filtered rows", () => {
    initTheme("dark", false);
    const h = harness(); h.setBranch([]);
    const org = { ...event("unseen", "Org reply\nFULL_ORG_REPLY", { key: "different" }), from: { id: "session:org", name: "org", kind: "main" as const } };
    const burst = inbox([event("peer", "Peer chatter"), org]);
    expect(h.render(burst)).toBeUndefined();
    expect(new CustomMessageComponent(burst, h.renderers.get(burst.customType)).render(120))
      .toEqual(new CustomMessageComponent(burst).render(120));
  });

  it("reindexes branch switches and supports older inboxes without receipt metadata", () => {
    const h = harness(); h.setBranch([entry(message())]);
    const value = inbox([event("legacy", "same old work")]); value.details = { ids: ["legacy"] };
    expect(h.render(value)).toEqual([]);
    h.setBranch([]);
    expect(plain(h.render(value)![0]!)).toContain("same old work");
  });

  it("updates delivery and prior-inbox indexes as messages arrive without a branch read", () => {
    const h = harness();
    const delivered = message(); h.messageEnd(delivered);
    const shadow = inbox([event("shadow", "native duplicate", { key: "same-work", messageId: "native-1" })]);
    expect(h.render(shadow)).toEqual([]);
    const firstInbox = inbox([event("event-once", "first copy", {})]);
    h.messageEnd(firstInbox);
    expect(h.render(firstInbox)![0]).toContain("first copy"); // The carrier does not hide itself.
    expect(h.render({ ...inbox([event("event-once", "replay", {})]), timestamp: firstInbox.timestamp + 60_000 })).toEqual([]);
    expect(h.getBranch).not.toHaveBeenCalled();
  });

  it("keeps message and LLM-facing content byte-identical through render and toggles", () => {
    const h = harness(); const all = [message(), inbox([event("shadow", "unchanged & body")]),
      { ...message(), customType: "pi-fabric-actor" }];
    h.setBranch([entry(all[0]!)]);
    const carrierBytes = JSON.stringify(all);
    const llmBytes = JSON.stringify(convertToLlm(all));
    for (const mode of ["auto", "collapsed", "expanded"] as const) {
      h.setMode(mode);
      for (const value of all) { h.render(value, false); h.render(value, true); }
      expect(JSON.stringify(all)).toBe(carrierBytes);
      expect(JSON.stringify(convertToLlm(all))).toBe(llmBytes);
    }
    expect(llmBytes).toContain("unchanged &amp; body");
  });

  it.each([
    [{}, false], [{ SMARTY_ROLE: "org" }, true], [{ SMARTY_ROLE: "org@abc123456789" }, true],
    [{ SMARTY_ROLE: "org-agent" }, true], [{ SMARTY_ROLE: "org-agent@abc123456789" }, true],
    [{ SMARTY_ROLE: "project-agent@abc123456789" }, false],
    [{ PI_FABRIC_ROLE: "task-agent", SMARTY_ROLE: "org-agent@abc123456789" }, false],
    [{ PI_FABRIC_ROLE: "org", SMARTY_ROLE: "task-agent" }, true],
    [{ PI_FABRIC_ROLE: "project", SMARTY_ROLE: "org" }, false],
    [{ SMARTY_ROLE: "task-agent" }, false], [{ SMARTY_ROLE: "org-kate" }, false],
    [{ PI_SMARTY_ROLE: "org" }, false],
  ])("uses exact fleet role precedence for auto: %j", (environment, collapsed) => {
    expect(incomingMessagesCollapsed("auto", environment)).toBe(collapsed);
    expect(incomingMessagesCollapsed("collapsed", environment)).toBe(true);
    expect(incomingMessagesCollapsed("expanded", environment)).toBe(false);
  });

  it("lets the persisted setting override either role default", () => {
    const h = harness("auto");
    vi.stubEnv("PI_FABRIC_ROLE", undefined); vi.stubEnv("SMARTY_ROLE", "org@abc123456789");
    expect(h.render(message())).toHaveLength(1);
    h.setMode("expanded"); expect(h.render(message())).toBeUndefined();
    vi.stubEnv("SMARTY_ROLE", "task-agent"); h.setMode("auto");
    expect(h.render(message())).toBeUndefined();
    h.setMode("collapsed"); expect(h.render(message())).toHaveLength(1);
    expect(h.render(message(), true)).toBeUndefined();
    for (const mode of ["auto", "collapsed", "expanded"] as const) {
      expect(normalizeFabricConfig({ ui: { incomingMessages: mode } }).ui.incomingMessages).toBe(mode);
    }
    expect(normalizeFabricConfig({ ui: { incomingMessages: "invalid" } }).ui.incomingMessages).toBe("auto");
    expect(normalizeFabricConfig({}).ui.incomingMessages).toBe("auto");
  });
});

describe("incoming full-carrier markdown theme (cold import, first use)", () => {
  const sdkLoaded = vi.fn();
  const highlight = vi.fn<(code: string, lang: string, invalidate?: () => void) => string[] | null>();
  afterEach(() => {
    vi.doUnmock("@earendil-works/pi-coding-agent");
    vi.doUnmock("../src/ui/highlight.js");
    vi.resetModules();
  });
  const load = async () => {
    vi.resetModules(); sdkLoaded.mockClear(); highlight.mockReset();
    // Pi loads this module as native ESM: any runtime SDK edge evaluates a second SDK.
    vi.doMock("@earendil-works/pi-coding-agent", () => { sdkLoaded(); throw new Error("runtime Pi SDK import"); });
    vi.doMock("../src/ui/highlight.js", () => ({ highlightCode: highlight }));
    return import("../src/ui/incoming-messages.js");
  };

  it("imports and idles without the SDK or highlighter, then colors code with the host Theme on first use", async () => {
    const { registerIncomingMessageRenderers: register } = await load();
    const renderers = new Map<string, MessageRenderer>();
    const handlers = new Map<string, (event: any, ctx: ExtensionContext) => unknown>();
    register({ on: (name: string, fn: any) => { handlers.set(name, fn); },
      registerMessageRenderer: (type: string, fn: MessageRenderer) => renderers.set(type, fn) } as unknown as ExtensionAPI,
    () => "collapsed");
    let branch: unknown[] = [];
    const byId = new Map<string, unknown>();
    const sessionManager = { getLeafEntry: () => branch.at(-1), getEntry: (id: string) => byId.get(id), getSessionId: () => "cold-test" };
    const context = { sessionManager } as unknown as ExtensionContext;
    const used = new Set<string>();
    const mark = (name: string) => (text: string) => { used.add(name); return text; };
    const hostTheme = { fg: (name: string, text: string) => mark(name)(text), bg: (name: string, text: string) => mark(name)(text),
      bold: mark("bold"), italic: mark("italic"), underline: mark("underline"), strikethrough: mark("strikethrough") } as unknown as Theme;
    const render = (value: CustomMessage, expanded: boolean) =>
      renderers.get(value.customType)!(value, { expanded, outputPad: 1 }, hostTheme);
    const code = "Intro\n\n# Incoming Heading\n\n**Bold** and `inline`\n\n```ts\nconst x = 1;\n```\nOutro";
    // Native inbox construction dedupes within a carrier. A prior carrier is
    // the real path that makes fullCarrier remove a repeat while retaining code.
    const prior = { ...entry(inbox([event("repeated", "already delivered", {})])), id: "prior", parentId: null };
    branch = [prior]; byId.set("prior", prior);
    handlers.get("session_start")!({}, context);
    const value = inbox([event("repeated", "REPEATED_COPY", {}), event("work", code, {})]);
    // Idle: collapsed previews and native fallbacks never touch the highlighter.
    expect(render(value, false)!.render(120)).toHaveLength(1);
    expect(render(inbox([event("plain", "no code here", {})]), true)).toBeUndefined();
    expect(sdkLoaded).not.toHaveBeenCalled();
    expect(highlight).not.toHaveBeenCalled();

    // First use: the not-yet-ready highlighter falls back to the host Theme's code-block color.
    highlight.mockReturnValue(null);
    const box = render(value, true)!;
    const cold = box.render(120).map(plain).join("\n");
    expect(cold).toContain("Incoming Heading");
    expect(cold).toContain("const x = 1;");
    expect(cold).not.toContain("REPEATED_COPY");
    expect(highlight).toHaveBeenCalled();
    expect(highlight.mock.calls[0]!.slice(0, 2)).toEqual(["const x = 1;", "ts"]);
    expect([...used]).toEqual(expect.arrayContaining(["customMessageBg", "customMessageLabel", "customMessageText",
      "mdHeading", "mdCode", "mdCodeBlock", "mdCodeBlockBorder", "bold"]));

    // Ready: the highlighter's invalidate drops cached lines; the next render shows syntax colors.
    const colored = "\x1b[38;2;1;2;3mconst x = 1;\x1b[39m";
    highlight.mockReturnValue([colored]);
    expect(box.render(120).join("\n")).not.toContain(colored); // Cached until invalidated.
    highlight.mock.calls[0]![2]!();
    expect(box.render(120).join("\n")).toContain(colored);
    expect(sdkLoaded).not.toHaveBeenCalled();
  });
});
