import { describe, expect, it } from "vitest";
import { CompactResumeDelivery, compactResumeMessage, COMPACT_RESUME_ENTRY_TYPE } from "../src/compaction/resume-delivery.js";
const id = "12345678-1234-1234-1234-123456789abc";
const pending = (state = "pending") => ({ type: "entry_appended", entry: {
  type: "custom", customType: COMPACT_RESUME_ENTRY_TYPE, data: { id, resume: "Start X", state },
} });

describe("compact resume RPC delivery fence", () => {
  it("releases all bundled startup receipts in one admitted user message", () => {
    const delivery = new CompactResumeDelivery();
    const other = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    delivery.observe(pending());
    delivery.observe({ type: "entry_appended", entry: { type: "custom", customType: COMPACT_RESUME_ENTRY_TYPE,
      data: { id: other, resume: "Start Y", state: "pending" } } });
    delivery.observe({ type: "message_start", message: { role: "user", content:
      compactResumeMessage({ id, resume: "Start X" }) + "\n\n" + compactResumeMessage({ id: other, resume: "Start Y" }) } });
    expect(delivery.pending).toBe(false);
  });

  it("holds duplicate journal notifications until the matching user admission", () => {
    const delivery = new CompactResumeDelivery();
    expect(delivery.pending).toBe(false);
    delivery.observe(pending()); delivery.observe(pending());
    expect(delivery.pending).toBe(true);
    delivery.observe({ type: "agent_settled" });
    delivery.observe({ type: "message_end", message: { role: "assistant", content: compactResumeMessage({ id, resume: "Start X" }) } });
    delivery.observe({ type: "message_start", message: { role: "user", content: compactResumeMessage({ id: "00000000-0000-0000-0000-000000000000", resume: "Start X" }) } });
    expect(delivery.pending).toBe(true);
    delivery.observe({ type: "message_start", message: { role: "user", content: [{ type: "text", text: compactResumeMessage({ id, resume: "Start X" }) }] } });
    expect(delivery.pending).toBe(false);
  });

  it("releases cancellation and resets stale state before worker child replacement", () => {
    const delivery = new CompactResumeDelivery();
    delivery.observe(pending()); delivery.observe(pending("cancelled"));
    expect(delivery.pending).toBe(false);
    delivery.observe(pending()); delivery.reset();
    expect(delivery.pending).toBe(false);
  });

  it("accepts a durable user entry as admission and ignores unrelated/malformed custom entries", () => {
    const delivery = new CompactResumeDelivery();
    for (const entry of [null, [], { type: "custom", customType: "other", data: { id, state: "pending" } },
      { type: "custom", customType: COMPACT_RESUME_ENTRY_TYPE, data: { id, state: "pending" } }]) {
      delivery.observe({ type: "entry_appended", entry });
    }
    expect(delivery.pending).toBe(false);
    delivery.observe(pending());
    delivery.observe({ type: "entry_appended", entry: { type: "message", message: {
      role: "user", content: compactResumeMessage({ id, resume: "Start X" }),
    } } });
    expect(delivery.pending).toBe(false);
  });
});
