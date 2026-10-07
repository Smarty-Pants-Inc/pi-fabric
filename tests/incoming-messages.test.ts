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
  details: { id: "native-1", from: sender, data: { key: "same-work" } },
});
const event = (id: string, text: string, data: unknown = { key: "same-work" }): MeshEvent => ({
  id, sequence: 1, topic: "fleet.work", kind: "work", from: sender, text, data, createdAt: 123,
});
const inbox = (events: MeshEvent[]): CustomMessage => ({ ...rootInboxMessage(events), role: "custom", timestamp: 123 });
const entry = (value: CustomMessage) => ({ ...value, type: "custom_message" });
const harness = (initial: FabricIncomingMessageMode = "collapsed") => {
  let mode = initial; let branch: unknown[] = [];
  const renderers = new Map<string, MessageRenderer>();
  let start: ((event: unknown, ctx: ExtensionContext) => unknown) | undefined;
  const on = vi.fn((name: string, fn: any) => { expect(name).toBe("session_start"); start = fn; });
  const registerMessageRenderer = vi.fn((type: string, render: MessageRenderer) => renderers.set(type, render));
  registerIncomingMessageRenderers({ on, registerMessageRenderer } as unknown as ExtensionAPI, () => mode);
  start!({}, { sessionManager: { getBranch: () => branch } } as unknown as ExtensionContext);
  const render = (value: CustomMessage, expanded = false, width = 200) =>
    renderers.get(value.customType)!(value, { expanded, outputPad: 1 }, theme)?.render(width);
  return { render, renderers, on, setMode: (value: FabricIncomingMessageMode) => { mode = value; },
    setBranch: (entries: unknown[]) => { branch = entries; } };
};
afterEach(() => vi.unstubAllEnvs());

describe("incoming Fabric display projection", () => {
  it("registers only incoming types, no context transformation or reply renderer", () => {
    const h = harness();
    expect([...h.renderers.keys()]).toEqual([...INCOMING_MESSAGE_TYPES]);
    expect(h.on.mock.calls.map(call => call[0])).toEqual(["session_start"]);
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
    const h = harness(); h.setBranch([entry(message())]);
    expect(h.render(inbox([event("shadow-1", "duplicate")]))).toEqual([]);
    const mixed = inbox([event("shadow-1", "duplicate"), event("unseen", "new work", { key: "different" })]);
    expect(plain(h.render(mixed)![0]!)).toBe(" ↳ Build & Test: [1 work] new work");
    const delivery = { ...message(), details: { id: "native-2", from: sender, data: { deliveryId: "D1", key: "same-work" } } };
    h.setBranch([entry(message()), entry(delivery)]);
    expect(h.render(inbox([event("d1", "duplicate D1", { deliveryId: "D1" })]))).toEqual([]);
    expect(plain(h.render(inbox([event("d2", "new D2", { deliveryId: "D2", key: "same-work" })]))![0]!)).toContain("new D2");
    expect(h.render(mixed, true)).toBeUndefined();
  });

  it.each([
    ["principal-addressed", (text: string) => text.replace('id="unseen"', 'id="unseen" to="principal"'), { ...sender }],
    ["org-labeled", (text: string) => text, { id: "session:org", name: "org", kind: "main" as const }],
  ])("keeps a delivered shadow suppressed when an unseen %s row renders in full", (_name, mark, from) => {
    initTheme("dark", false);
    const h = harness(); h.setBranch([entry(message())]);
    const exempt = { ...event("unseen", "Reply for Paul\nFULL_EXEMPT_REPLY", { key: "different" }), from };
    const base = inbox([event("shadow-1", "SHADOW_DUPLICATE"), exempt]);
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
