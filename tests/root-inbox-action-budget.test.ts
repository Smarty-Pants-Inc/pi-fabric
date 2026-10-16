import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { expect, it } from "vitest";
import type { MeshEvent } from "../src/mesh/store.js";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";
import {
  eventReceipts, INBOX_INJECT_MAX_ACTION_RENDERED_BYTES, rootInboxBudget, rootInboxMessage,
} from "../src/topology/root-inbox-shared.js";

// Keep: pi-fabric#1155, a real input-burst failure at the model-input trust boundary. Exercise
// actual carrier bytes, not a second estimate of XML expansion, metadata or truncation notices.
it("caps the global rendered action prefix without issuing overflow or capping tiny action bursts", () => {
  const cap = INBOX_INJECT_MAX_ACTION_RENDERED_BYTES;
  expect(cap).toBe(128 * 1024);
  const event = (sequence: number, kind: string, text: string): MeshEvent => ({
    id: `action-${sequence}`, sequence, kind, text, topic: "work.<&>",
    from: { id: `session:sender-${sequence}`, name: `sender \"${sequence}\" é`, kind: "main" },
    createdAt: 1_700_000_000_000, verification: "mesh",
    data: { ref: "ref\\\"".repeat(80), key: "key<&>".repeat(60), deliveryId: `delivery-${sequence}` },
  });
  type Message = ReturnType<typeof rootInboxMessage>;
  // Each match includes the real joining newline, including after a multiline cut notice.
  const rows = (message: Message) => message.content.match(/<event [\s\S]*?<\/event>\n/g) ?? [];
  const renderedBytes = (message: Message) => rows(message).reduce((sum, row) => sum + Buffer.byteLength(row), 0);
  // Explicit overflow bypasses budgeting, giving an independent observation of actual rendering.
  const unbudgeted = (events: readonly MeshEvent[]) => rootInboxMessage(events, []);
  const kinds = ["blocked", "blocked", "blocker", "blocker", "ask", "ask"];
  const prefix = kinds.map((kind, index) => event(index + 1, kind, "&".repeat(4096) + "cut this tail"));
  const rendered = unbudgeted(prefix);
  expect(rendered.content).toContain("&amp;".repeat(4096));
  expect(rendered.content).toContain("[cut at 4096 bytes;");
  expect(rendered.content).toContain('topic="work.&lt;&amp;&gt;"');
  expect(rendered.content).toContain("ref=");
  expect(rendered.content).toContain("key=");
  expect(renderedBytes(rendered)).toBeGreaterThan(6 * 4096 * 5);

  const boundary = event(7, "handoff", "boundary body");
  const padding = cap - renderedBytes(unbudgeted([...prefix, boundary]));
  expect(padding).toBeGreaterThan(0);
  boundary.from.name += "n".repeat(padding);
  const exact = [...prefix, boundary];
  expect(renderedBytes(unbudgeted(exact))).toBe(cap);
  // Reverse input proves kind ordering and sequence FIFO, rather than incidental input order.
  expect(rootInboxBudget([...exact].reverse())).toEqual({ shown: exact, overflow: [] });
  expect(renderedBytes(rootInboxMessage([...exact].reverse()))).toBe(cap);

  // A single metadata byte tips the actual rendered total over the boundary. Without counting
  // the joining newlines this would fit. A tiny later action must NOT jump the withheld row.
  const tooLarge = { ...boundary, from: { ...boundary.from, name: boundary.from.name + "n" } };
  const tiny = event(8, "handoff", "tiny later action");
  expect(renderedBytes(unbudgeted([...prefix, tooLarge]))).toBe(cap + 1);
  expect(renderedBytes(unbudgeted([...prefix, tiny]))).toBeLessThan(cap);
  const input = [...prefix, tooLarge, tiny].reverse();
  expect(rootInboxBudget(input)).toEqual({ shown: prefix, overflow: [tooLarge, tiny] });
  const shaped = rootInboxMessage(input);
  expect(renderedBytes(shaped)).toBeLessThanOrEqual(cap);
  expect(shaped.details.ids).toEqual(prefix.map(row => row.id));
  expect(shaped.details.receipts).toEqual(prefix.flatMap(eventReceipts));
  expect(shaped.content).toContain('<overflow count="2">');
  expect(shaped.content).not.toContain("boundary body");
  expect(shaped.content).not.toContain("tiny later action");
  for (const withheld of [tooLarge, tiny]) {
    for (const receipt of eventReceipts(withheld)) expect(shaped.details.receipts).not.toContain(receipt);
  }

  const sent: Message[] = [];
  const issuedIds: string[] = [];
  const pi = {
    hostCapabilities: { turnProvenance: 1 },
    sendMessage: (message: Message) => sent.push(message),
  } as unknown as ExtensionAPI;
  expect(deliverRootInbox(pi, input, undefined, details => {
    issuedIds.push(...(details as Message["details"]).ids);
  })).toEqual(prefix.map(row => row.id));
  // Distinct verified senders force provenance splits; the action cap must not reset per carrier.
  expect(sent.length).toBeGreaterThan(1);
  expect(sent.reduce((sum, message) => sum + renderedBytes(message), 0)).toBe(renderedBytes(shaped));
  expect(sent.flatMap(message => message.details.ids)).toEqual(prefix.map(row => row.id));
  expect(issuedIds).toEqual(prefix.map(row => row.id));
  expect(sent.flatMap(message => message.details.receipts)).toEqual(prefix.flatMap(eventReceipts));
  const fullInjectedBytes = sent.reduce((sum, message) => sum + Buffer.byteLength(message.content), 0);
  const expectedCarriers = prefix.map((row, index) => rootInboxMessage(
    [row], index === prefix.length - 1 ? [tooLarge, tiny] : [],
  ));
  expect(fullInjectedBytes).toBe(expectedCarriers.reduce((sum, message) => sum + Buffer.byteLength(message.content), 0));
  expect(fullInjectedBytes).toBeGreaterThan(renderedBytes(shaped));

  // Precisely expose the scope limitation: at the row boundary, split carrier wrappers add
  // bytes beyond 128 KiB. The cap bounds action rows, not total carriers or existing history.
  sent.length = 0;
  deliverRootInbox(pi, [...exact].reverse());
  expect(sent.reduce((sum, message) => sum + renderedBytes(message), 0)).toBe(cap);
  expect(sent.reduce((sum, message) => sum + Buffer.byteLength(message.content), 0)).toBeGreaterThan(cap);

  // The scanner's full 256-row offer is unsafe with XML-heavy 4 KiB bodies, but a row cap would
  // regress the existing 230/236-short-action behavior. Keep the byte-only distinction explicit.
  const heavy = Array.from({ length: 256 }, (_, index) => event(index + 1, "blocked", "&".repeat(4096)));
  expect(renderedBytes(unbudgeted(heavy))).toBeGreaterThan(cap);
  const limited = rootInboxBudget(heavy);
  expect(limited.shown.length).toBeGreaterThan(0);
  expect(limited.overflow.length).toBeGreaterThan(0);
  expect(limited.shown).toEqual(heavy.slice(0, limited.shown.length));
  expect(limited.overflow).toEqual(heavy.slice(limited.shown.length));
  expect(renderedBytes(rootInboxMessage(heavy))).toBeLessThanOrEqual(cap);
  const short = heavy.slice(0, 236).map(row => ({ ...row, text: "short", data: undefined }));
  expect(renderedBytes(unbudgeted(short))).toBeLessThan(cap);
  expect(rootInboxBudget(short)).toEqual({ shown: short, overflow: [] });
});
