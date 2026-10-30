import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  SessionManager,
  type AgentSession,
  type ExtensionAPI,
  type ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FOLLOW_UP_LIMITS, MainAgentController, type HeldAgentMessage } from "../src/main-agent.js";
import type { FabricInterruptAuthority } from "../src/interrupt-authority.js";
import { nativeRecoveryCarrierCustody, nativeRecoveryCarrierIdentities, readNativeRecoveryCustody } from "../src/core/native-recovery.js";
import type { NativeRecoveryCustodyReader, NativeRecoveryCustodyResult, NativeRecoveryTransfer,
  NativeRecoverySessionRef } from "../src/core/session-receipts.js";
import * as deliveryOutcomes from "../src/mesh/delivery-outcomes.js";
import * as atomicWrite from "../src/core/atomic-write.js";
import {
  adoptRootInboxReceiptIssuer, retainRootInboxReceiptIssuer, rootInboxReceiptIssuer,
} from "../src/topology/root-inbox-shared.js";

// Review round 2 on pi-fabric#160 (finding 1, security S1): Pi's sendMessage returns void and may
// only queue the message (streaming, prompt preflight, a settle), or fail asynchronously. A
// delivery with a deliveryId is journalled before deliverAgent returns, stays there until the
// session holds it, and is admitted once by that id, across restarts and reloads.
type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
type Sent = { message: { content: string; details: Record<string, any> }; options: { deliverAs: string; triggerTurn: boolean } };
const fakePi = () => {
  const handlers = new Map<string, Handler[]>();
  const sent: Sent[] = [];
  const pi = {
    on: (name: string, fn: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), fn]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter((h) => h !== fn));
    },
    // Void, as Pi's: the message only reaches a volatile queue here.
    sendMessage: vi.fn((message, options) => { sent.push({ message, options }); }),
    sendUserMessage: vi.fn(),
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const emit = (name: string, event: unknown, ctx: ExtensionContext) => {
    for (const handler of handlers.get(name) ?? []) handler(event, ctx);
  };
  return { pi, sent, emit };
};
const busy = (entries: unknown[], idle = false) => ({
  isIdle: () => idle,
  hasPendingMessages: () => false,
  signal: { aborted: false },
  sessionManager: { getEntries: () => entries },
}) as unknown as ExtensionContext;
const boundary = (pending: unknown[]) => ({ outcome: "completed", context: { pendingMessages: pending }, message: { stopReason: "toolUse" } });
const persisted = (sent: Sent) => ({ type: "custom_message", customType: "pi-fabric-agent-message", details: sent.message.details });
const actor = { id: "actor-1", name: "lucky", kind: "actor" as const };

const roots: string[] = [];
const sessions: AgentSession[] = [];
afterEach(() => {
  for (const session of sessions.splice(0)) session.dispose();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const waitFor = async (predicate: () => boolean, timeoutMs = 5_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};
const journalPath = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-delivery-id-"));
  roots.push(dir);
  return path.join(dir, "main-followups", "root.json");
};

// Keep: process-object receipt authority and manager/session isolation are trust boundaries.
describe("native receipt issuer registry isolation", () => {
  it("rejects copied issuers/capabilities and preserves historical identity across exact claims", () => {
    const source = {}, target = {}, next = {};
    const issuer = rootInboxReceiptIssuer(source, "source");
    expect(() => retainRootInboxReceiptIssuer({ ...issuer })).toThrow("Unknown root inbox receipt issuer");
    const authority = retainRootInboxReceiptIssuer(issuer);
    for (const copy of [{ ...authority }, structuredClone(authority)]) {
      expect(() => adoptRootInboxReceiptIssuer(target, "target", copy)).toThrow();
    }
    expect(() => adoptRootInboxReceiptIssuer(target, "", authority)).toThrow();
    expect(adoptRootInboxReceiptIssuer(target, "target", authority)).toBe(issuer);
    expect(adoptRootInboxReceiptIssuer(target, "target", authority)).toBe(issuer);
    expect(() => adoptRootInboxReceiptIssuer(target, "other", authority)).toThrow();
    expect(() => adoptRootInboxReceiptIssuer({}, "target", authority)).toThrow();
    expect(rootInboxReceiptIssuer(target, "target")).toBe(issuer);
    expect(rootInboxReceiptIssuer(target, "target", true)).toBe(issuer);
    expect(adoptRootInboxReceiptIssuer(next, "next", retainRootInboxReceiptIssuer(issuer))).toBe(issuer);
    expect(issuer.sessionId).toBe("source");
    // An issuer used as a manager key must not become another issuer's provenance.
    const separate = rootInboxReceiptIssuer(issuer, "separate");
    expect(separate).not.toBe(issuer);
    expect(rootInboxReceiptIssuer(next, "next")).toBe(issuer);
  });

  it("keeps ordinary new sessions, reloads and managers independent of native bindings", () => {
    const manager = {}, target = {};
    const issuer = rootInboxReceiptIssuer(manager, "source");
    issuer.queued.set("pending", 3); issuer.turn = 4;
    expect(rootInboxReceiptIssuer(manager, "source", true)).toBe(issuer);
    adoptRootInboxReceiptIssuer(target, "target", retainRootInboxReceiptIssuer(issuer));
    expect(rootInboxReceiptIssuer({}, "target", true)).not.toBe(issuer);
    const ordinary = rootInboxReceiptIssuer(target, "ordinary", true);
    expect(ordinary).not.toBe(issuer);
    expect(ordinary.queued.size).toBe(0);
    expect(rootInboxReceiptIssuer(target, "target")).not.toBe(issuer);
    expect(rootInboxReceiptIssuer(manager, "source")).not.toBe(issuer);
    expect(issuer.queued.get("pending")).toBe(3);
    expect(issuer.turn).toBe(4);
    expect(issuer.sessionId).toBe("source");
  });
});

describe("Main delivery outcome evidence", () => {
  const outcomes = (journal: string) => {
    const directory = path.join(path.dirname(path.dirname(journal)), "delivery-outcomes");
    return fs.existsSync(directory) ? fs.readdirSync(directory).flatMap(file =>
      fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").map(line => JSON.parse(line))) : [];
  };
  it.each(["steer", "followUp"] as const)("records %s only after receiver consumption, never on ACK", delivery => {
    const journal = journalPath(); const h = fakePi(); const entries: unknown[] = []; const context = busy(entries, true);
    const main = new MainAgentController(h.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(context, 0, journal);
    const send = { eventId: "send-1", to: main.id, from: actor.id, mode: delivery };
    main.deliverAgent({ from: actor, message: "consume me", delivery, outcomeSend: send });
    expect(outcomes(journal)).toEqual([]);
    entries.push(persisted(h.sent[0]!));
    h.emit("agent_settled", { outcome: "completed" }, context);
    main.confirmInbox();
    expect(outcomes(journal)).toEqual([{ ...send, outcome: "delivered", reason: "consumed by Main as itself", at: expect.any(Number) }]);
    main.closeFollowUpDrain();
  });
  const haltedReason = "delivered, not triggered: recipient is halted by its operator";
  it.each(["immediate-halt", "held-then-halt", "triggered-then-halt"] as const)(
    "retains per-message handoff evidence through delayed confirmation/reload (%s)", scenario => {
      const journal = journalPath();
      const h = fakePi(); const entries: unknown[] = [];
      const context = busy(entries, scenario !== "held-then-halt");
      const main = new MainAgentController(h.pi, "session:root", true, process.cwd(), "root");
      main.attachFollowUpDrain(context, 60_000, journal);
      if (scenario === "immediate-halt") main.halt();
      const send = { eventId: scenario, to: main.id, from: actor.id, mode: "followUp" as const };
      const receipt = main.deliverAgent({ from: actor, message: "handoff evidence", delivery: "followUp", outcomeSend: send });
      if (scenario === "immediate-halt") expect(receipt).toMatchObject({ triggered: false, halted: true, reason: "operator-halt" });
      if (scenario === "triggered-then-halt") expect(receipt.triggered).toBe(true);
      main.halt();
      if (scenario === "held-then-halt") h.emit("agent_settled", { outcome: "aborted" }, context);
      expect(h.sent).toHaveLength(1);
      const reason = scenario === "triggered-then-halt" ? "consumed by Main as itself" : haltedReason;
      expect(h.sent[0]!.message.details.deliveryReason).toBe(scenario === "triggered-then-halt" ? undefined : "operator-halt");
      // Pi has consumed it but confirmation is delayed until the new controller.
      // The current owner halt must not relabel the earlier triggering handoff.
      entries.push(persisted(h.sent[0]!));
      expect(outcomes(journal)).toEqual([]);
      main.closeFollowUpDrain();
      const next = fakePi();
      const reloaded = new MainAgentController(next.pi, "session:root", true, process.cwd(), "root");
      reloaded.attachFollowUpDrain(busy(entries, true), 60_000, journal);
      reloaded.confirmInbox();
      expect(outcomes(journal)).toEqual([{ ...send, outcome: "delivered", reason, at: expect.any(Number) }]);
      expect(next.sent).toHaveLength(0);
      reloaded.closeFollowUpDrain();
    },
  );
  it("persists an owner-halt downgrade when a held message is replayed before native receipt", () => {
    const journal = journalPath(); const h = fakePi(); const entries: unknown[] = [];
    const main = new MainAgentController(h.pi, "session:root", true, process.cwd(), "root");
    const context = busy(entries);
    main.attachFollowUpDrain(context, 60_000, journal);
    const send = { eventId: "replayed-halt", to: main.id, from: actor.id, mode: "followUp" as const };
    main.deliverAgent({ from: actor, message: "held then stopped", delivery: "followUp", outcomeSend: send });
    main.halt();
    main.closeFollowUpDrain();
    const next = fakePi();
    const replayed = new MainAgentController(next.pi, "session:root", true, process.cwd(), "root");
    replayed.attachFollowUpDrain(busy(entries, true), 60_000, journal);
    expect(next.sent[0]!.options.triggerTurn).toBe(false);
    expect(JSON.parse(fs.readFileSync(journal, "utf8")).items[0].deliveryReason).toBe("operator-halt");
    entries.push(persisted(next.sent[0]!));
    replayed.confirmInbox();
    expect(outcomes(journal)).toEqual([{ ...send, outcome: "delivered", reason: haltedReason, at: expect.any(Number) }]);
    replayed.closeFollowUpDrain();
  });
  it("retries a failed handoff-evidence durability barrier before native send", () => {
    const journal = journalPath(); const h = fakePi(); const entries: unknown[] = []; const context = busy(entries);
    const main = new MainAgentController(h.pi, "session:root", true, process.cwd(), "root");
    main.attachFollowUpDrain(context, 60_000, journal);
    const send = { eventId: "barrier-retry", to: main.id, from: actor.id, mode: "followUp" as const };
    main.deliverAgent({ from: actor, message: "do not lose the evidence", delivery: "followUp", outcomeSend: send });
    main.halt();
    const rename = fs.renameSync;
    const fault = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === journal) throw new Error("handoff evidence unavailable");
      return rename(from, to);
    });
    try {
      for (let retry = 0; retry < 2; retry++) {
        h.emit("agent_settled", { outcome: "aborted" }, context);
        expect(h.sent).toHaveLength(0);
        expect(main.queueDepth().pendingFollowUps).toBe(1);
      }
    } finally { fault.mockRestore(); }
    h.emit("agent_settled", { outcome: "aborted" }, context);
    expect(h.sent).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(journal, "utf8")).items[0].deliveryReason).toBe("operator-halt");
    entries.push(persisted(h.sent[0]!));
    main.confirmInbox();
    expect(outcomes(journal)).toEqual([{ ...send, outcome: "delivered", reason: haltedReason, at: expect.any(Number) }]);
    main.closeFollowUpDrain();
  });
  it("records every same-key original as delivered only after its own consumption", () => {
    const journal = journalPath(); const h = fakePi(); const entries: unknown[] = []; const context = busy(entries);
    const main = new MainAgentController(h.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(context, 60_000, journal);
    for (const eventId of ["old", "new"]) main.deliverAgent({ from: actor, message: eventId, delivery: "followUp", data: { coalesceKey: "work" },
      outcomeSend: { eventId, to: main.id, from: actor.id, mode: "followUp" } });
    expect(outcomes(journal)).toEqual([]);
    h.emit("agent_before_settle", { outcome: "completed" }, context);
    entries.push(persisted(h.sent[0]!));
    main.confirmInbox();
    expect(h.sent[0]!.message.content.match(/\n(?:old|new)\n/g)).toEqual(["\nold\n", "\nnew\n"]);
    expect(outcomes(journal).map(row => [row.eventId, row.outcome])).toEqual([["old", "delivered"], ["new", "delivered"]]);
    main.closeFollowUpDrain();
  });
  it("persists partial legacy superseded-outcome progress and recovers on MAIN without replaying its consumed carrier", () => {
    const journal = journalPath(); const h = fakePi(); const entries: unknown[] = []; const context = busy(entries);
    const main = new MainAgentController(h.pi, "session:root", true, "/tmp/project", "root");
    const send = (eventId: string) => ({ eventId, to: main.id, from: actor.id, mode: "followUp" });
    fs.mkdirSync(path.dirname(journal), { recursive: true });
    fs.writeFileSync(journal, JSON.stringify({ version: 1, items: [{ id: "legacy-carrier", from: actor,
      message: "new", sentAt: Date.now(), outcomeSend: send("new"), supersededSends: [send("old-1"), send("old-2")] }] }));
    const original = deliveryOutcomes.appendDeliveryOutcome;
    let fail = true;
    const append = vi.spyOn(deliveryOutcomes, "appendDeliveryOutcome").mockImplementation((...args) => {
      if (fail && args[1].eventId === "old-2") throw new Error("outcome unavailable");
      return original(...args);
    });
    try {
      main.attachFollowUpDrain(context, 60_000, journal);
      expect(JSON.parse(fs.readFileSync(journal, "utf8")).items[0].supersededSends.map((item: { eventId: string }) => item.eventId)).toEqual(["old-2"]);
      h.emit("agent_before_settle", { outcome: "completed" }, context);
      entries.push(persisted(h.sent[0]!));
      main.confirmInbox();
      // MAIN retains the consumed payload while an outcome append is pending; no O1 retry engine.
      expect(JSON.parse(fs.readFileSync(journal, "utf8")).items[0].supersededSends.map((item: { eventId: string }) => item.eventId)).toEqual(["old-2"]);
      expect(outcomes(journal).map(row => [row.eventId, row.outcome])).toEqual([["old-1", "superseded"]]);
      main.closeFollowUpDrain();
      fail = false;
      const next = fakePi();
      const restarted = new MainAgentController(next.pi, "session:root", true, "/tmp/project", "root");
      try {
        restarted.attachFollowUpDrain(context, 60_000, journal);
        expect(next.sent).toHaveLength(0);
        expect(fs.existsSync(journal)).toBe(false);
        expect(outcomes(journal).map(row => [row.eventId, row.outcome])).toEqual([
          ["old-1", "superseded"], ["old-2", "superseded"], ["new", "delivered"],
        ]);
      } finally { restarted.closeFollowUpDrain(); }
    } finally { append.mockRestore(); main.closeFollowUpDrain(); }
  });
});

describe("#169 round 3 receiver receipt durability", () => {
  it("keeps the journal until the session receipt file can be synced", () => {
    const journal = journalPath();
    const file = path.join(path.dirname(journal), "session.jsonl");
    const first = fakePi();
    const context = busy([], true);
    context.sessionManager.getSessionFile = () => file;
    const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(context, 60_000, journal);
    main.deliverAgent({ from: actor, message: "reply", delivery: "steer", deliveryId: "sync-receipt" });
    fs.writeFileSync(file, `${JSON.stringify(persisted(first.sent[0]!))}\n`);
    const sync = fs.fsyncSync.bind(fs);
    let failed = false;
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      failed = true;
      throw new Error("session barrier unavailable");
    });
    try {
      first.emit("agent_settled", { outcome: "completed" }, context);
      expect(failed).toBe(true);
      expect(fs.readFileSync(journal, "utf8")).toContain("sync-receipt");
    } finally { synced.mockRestore(); }
    const events: string[] = [];
    const open = fs.openSync.bind(fs);
    const descriptors = new Map<number, string>();
    const opened = vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) => {
      const fd = open(target, flags, mode); descriptors.set(fd, String(target)); return fd;
    });
    const syncOrder = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      events.push(descriptors.get(fd) === file ? "session" : fs.fstatSync(fd).isDirectory() ? descriptors.get(fd)! : "consumed");
      sync(fd);
    });
    try {
      first.emit("agent_settled", { outcome: "completed" }, context);
      const directoryBarriers: string[] = [];
      if (process.platform !== "win32") {
        for (let directory = path.dirname(journal); ; directory = path.dirname(directory)) {
          directoryBarriers.push(directory);
          if (path.dirname(directory) === directory) break;
        }
      }
      expect(events).toEqual(["session", ...directoryBarriers, "consumed", ...directoryBarriers]);
      expect(fs.existsSync(journal)).toBe(false);
      expect(fs.readFileSync(`${journal}.delivered`, "utf8")).toContain("sync-receipt");
    } finally { syncOrder.mockRestore(); opened.mockRestore(); main.closeFollowUpDrain(); }
  });
});

describe("Main admits a delivery id durably, once", () => {
  for (const [label, delivery, triggerTurn] of [["a steer", "steer", true], ["a non-triggering followUp", "followUp", false]] as const) {
    it(`${label} to a streaming Main is journalled before the return and replayed once after a restart`, () => {
      const journal = journalPath();
      const entries: unknown[] = [];
      const first = fakePi();
      const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
      main.attachFollowUpDrain(busy(entries), 60_000, journal);
      const result = main.deliverAgent({ from: actor, message: "reply", delivery, triggerTurn, deliveryId: "rec-1" });
      expect(first.sent).toHaveLength(1);                                        // handed to Pi's volatile queue
      expect(fs.readFileSync(journal, "utf8")).toContain("rec-1");               // durable before the return
      // A second drain of the same record (a failed mesh delete) sends nothing.
      expect(main.deliverAgent({ from: actor, message: "reply", delivery, triggerTurn, deliveryId: "rec-1" }))
        .toMatchObject({ duplicate: true, messageId: result.messageId, triggered: false });
      expect(first.sent).toHaveLength(1);
      // Main dies before Pi appends it to the session (no close). A new controller, same journal.
      const second = fakePi();
      const restarted = new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root");
      restarted.attachFollowUpDrain(busy(entries), 60_000, journal);
      expect(restarted.deliverAgent({ from: actor, message: "reply", delivery, triggerTurn, deliveryId: "rec-1" }))
        .toMatchObject({ duplicate: true, triggered: false });                   // admitted, still pending
      second.emit("agent_before_settle", boundary([]), busy(entries));           // Pi's queue is empty: lost
      expect(second.sent.map((sent) => sent.message.details.id)).toEqual([result.messageId]);
      expect(second.sent[0]!.message.details.deliveryId).toBe("rec-1");
      expect(second.sent[0]!.options).toEqual({ deliverAs: delivery, triggerTurn });  // its own policy
      entries.push(persisted(second.sent[0]!));
      second.emit("agent_settled", { outcome: "completed" }, busy(entries, true));
      second.emit("agent_before_settle", boundary([]), busy(entries));
      expect(second.sent).toHaveLength(1);                                       // exactly once
      expect(fs.existsSync(journal)).toBe(false);
      expect(fs.readFileSync(`${journal}.delivered`, "utf8")).toContain("rec-1");
    });
  }

  // Pi reports an asynchronous admission failure to its own error handler, not to the caller.
  it("keeps a message whose asynchronous send failed in the journal, and a restart delivers it", () => {
    const journal = journalPath();
    const entries: unknown[] = [];
    const first = fakePi();
    const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(busy(entries, true), 60_000, journal);
    main.deliverAgent({ from: actor, message: "reply", delivery: "followUp", triggerTurn: true, deliveryId: "rec-async" });
    expect(first.sent).toHaveLength(1);                                          // the prompt later failed in Pi
    first.emit("agent_settled", { outcome: "completed" }, busy(entries, true));
    expect(fs.readFileSync(journal, "utf8")).toContain("rec-async");            // never confirmed, kept
    const second = fakePi();
    new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root").attachFollowUpDrain(busy(entries), 60_000, journal);
    second.emit("agent_before_settle", boundary([]), busy(entries));
    expect(second.sent.map((sent) => sent.message.details.deliveryId)).toEqual(["rec-async"]);
  });

  // Security round 3 on pi-fabric#160 (S2): replay must not widen an actor's delivery policy. A
  // passive message journalled while Main streams keeps its own mode and triggerTurn false on replay.
  for (const [label, delivery] of [["a non-triggering followUp", "followUp"], ["a nextTurn message", "nextTurn"]] as const) {
    it(`replays ${label} with its original policy, never as a triggering release`, () => {
      const journal = journalPath();
      const entries: unknown[] = [];
      const first = fakePi();
      const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
      main.attachFollowUpDrain(busy(entries), 60_000, journal);
      main.deliverAgent({ from: actor, message: "context only", delivery, triggerTurn: false, deliveryId: "rec-passive" });
      expect(first.sent[0]!.options).toEqual({ deliverAs: delivery, triggerTurn: false });
      // A held, triggering followUp from another sender in the same journal must not carry it along.
      main.deliverAgent({ from: { ...actor, id: "actor-2" }, message: "act on this", delivery: "followUp", triggerTurn: true, deliveryId: "rec-active" });
      expect(first.sent).toHaveLength(1);                                        // held for the busy Main
      // Main dies before Pi appends either. A new controller replays the journal.
      const second = fakePi();
      new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root").attachFollowUpDrain(busy(entries), 60_000, journal);
      second.emit("agent_before_settle", boundary([]), busy(entries));
      const passive = second.sent.filter((sent) => sent.message.details.deliveryId === "rec-passive" ||
        (sent.message.details.items ?? []).some((item: { deliveryId?: string }) => item.deliveryId === "rec-passive"));
      expect(passive).toHaveLength(1);
      expect(passive[0]!.options).toEqual({ deliverAs: delivery, triggerTurn: false });
      expect(passive[0]!.message.details.items).toBeUndefined();                // never batched
      expect(second.sent.filter((sent) => sent.message.details.deliveryId === "rec-active").map((sent) => sent.options))
        .toEqual([{ deliverAs: "followUp", triggerTurn: true }]);
      for (const sent of second.sent) entries.push(persisted(sent));
      second.emit("agent_settled", { outcome: "completed" }, busy(entries, true));
      second.emit("agent_before_settle", boundary([]), busy(entries));
      expect(second.sent).toHaveLength(2);                                       // each exactly once
      expect(fs.existsSync(journal)).toBe(false);
    });
  }

  // A failed mesh delete must not duplicate any separately retained same-key original.
  const coalesced = (deliveryId: string, message: string) =>
    ({ from: actor, message, delivery: "followUp" as const, triggerTurn: true, data: { coalesceKey: "state" }, deliveryId });
  it("deduplicates both same-key delivery ids after abrupt restart and keeps both originals", () => {
    const journal = journalPath();
    const entries: unknown[] = [];
    const first = fakePi();
    const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(busy(entries), 60_000, journal);
    main.deliverAgent(coalesced("rec-A", "old state"));                        // A's mesh delete then fails
    expect(main.deliverAgent(coalesced("rec-B", "new state"))).toMatchObject({ pendingFollowUps: 2 });
    // Killed: no boundary, no close. A new controller on the same journal; A's record is drained again.
    const second = fakePi();
    const restarted = new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root");
    restarted.attachFollowUpDrain(busy(entries), 60_000, journal);
    expect(restarted.deliverAgent(coalesced("rec-A", "old state"))).toMatchObject({ duplicate: true });
    expect(restarted.deliverAgent(coalesced("rec-B", "new state"))).toMatchObject({ duplicate: true });
    expect(restarted.queueDepth()).toMatchObject({ pendingFollowUps: 2 });
    second.emit("agent_before_settle", boundary([]), busy(entries));
    expect(second.sent.flatMap(sent => sent.message.details.items.map((item: { deliveryId: string }) => item.deliveryId))).toEqual(["rec-A", "rec-B"]);
  });

  it("KEEP: a pre-supersedes coalesced D1 tombstone prevents stale resident retry after native D2", () => {
    // Data-safety boundary: 45f466fe..872978d86^ replaced same-sender/same-key D1 with D2,
    // consumed D1 before ACK, then wrote only D2's deliveryId plus internal chain/replaces.
    // See 872978d86^:src/main-agent.ts (#consume(replaced), #save, #send); its native
    // details have no supersedes. The old residency/client.ts deletes only AFTER admission:
    // a failed delete retains D1's source even after D2 is durably consumed in this session.
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-legacy-coalesced-"));
    roots.push(root);
    const manager = SessionManager.create(root, path.join(root, "sessions"));
    const sessionId = manager.getSessionId();
    const journal = path.join(root, "main-followups", `${sessionId}.json`);
    const d1 = `resident:${sessionId}:source-D1`;
    const d2 = `resident:${sessionId}:source-D2`;
    const internalD1 = "167d261e-d4bc-4c98-829a-cae76d086462";
    const internalD2 = "543fb841-107a-4de8-bc91-66849acdc520";
    const sentAt = "2026-01-01T00:00:00.000Z";
    const replacedAt = "2026-01-01T00:00:01.000Z";
    const retainedSource = coalesced(d1, "D1 stale state");
    const current = coalesced(d2, "D2 current state");
    // Use the SDK's real native entry serializer, in the SAME session/journal namespace.
    manager.appendMessage({ role: "user", content: "continue", timestamp: Date.parse(sentAt) });
    manager.appendCustomMessageEntry("pi-fabric-agent-message", [
      `<fabric-agent-message from_name="${actor.name}" from_id="${actor.id}" from_kind="${actor.kind}" delivery="followUp" sent_at="${sentAt}">`,
      current.message,
      `<data>${JSON.stringify(current.data)}</data>`,
      "</fabric-agent-message>",
    ].join("\n"), true, {
      id: internalD2, from: actor, delivery: "followUp", sentAt, replacedAt,
      chain: internalD1, generation: 1, replaces: internalD1,
      data: current.data, deliveryId: d2, triggerTurn: true,
    });
    // Exact old consumed-index shape, including D1 (retired unsent) and D2 (consumed).
    // Keep the real atomic writer and its durability barriers, not an fsync mock.
    atomicWrite.writeFileAtomic(`${journal}.delivered`, JSON.stringify({ version: 1, ids: [d1, d2] }), { durable: true });
    expect(fs.existsSync(journal)).toBe(false);
    const nativeFile = manager.getSessionFile()!;
    const nativeBytes = fs.readFileSync(nativeFile, "utf8");
    const nativeEntries = manager.getEntries().filter(entry => entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message");
    expect(nativeEntries).toHaveLength(1);
    expect(nativeBytes).toContain(`"deliveryId":"${d2}"`);
    expect(nativeBytes).not.toContain(d1);
    expect(nativeBytes).not.toContain("supersedes");
    const h = fakePi();
    const context = { ...busy(manager.getEntries(), true), sessionManager: manager } as unknown as ExtensionContext;
    const main = new MainAgentController(h.pi, `session:${sessionId}`, true, root, sessionId);
    try {
      main.attachFollowUpDrain(context, 60_000, journal);
      // Public retry of the retained source: ACK is passive, never a second admission.
      for (let retry = 0; retry < 2; retry++) {
        expect.soft(main.deliverAgent(retainedSource)).toMatchObject({ queued: true, duplicate: true, triggered: false });
      }
      h.emit("agent_before_settle", boundary([]), context);
      main.confirmInbox();
      // Inspect actual send contents too: the rejected J2 source sends stale D1 here.
      expect.soft(h.sent.map(sent => sent.message.content)).toEqual([]);
      expect.soft(h.sent.some(sent => sent.message.content.includes(retainedSource.message))).toBe(false);
      expect.soft(nativeEntries.length + h.sent.length).toBe(1);
      expect.soft(main.queueDepth().pendingFollowUps).toBe(0);
      expect(fs.readFileSync(nativeFile, "utf8")).toBe(nativeBytes); // native D2 untouched
      expect(manager.getEntries().filter(entry => entry.type === "custom_message")).toEqual(nativeEntries);
      expect(fs.existsSync(journal)).toBe(false);
    } finally { main.closeFollowUpDrain(); }
  });

  // smarty-dev#2339 F2: a failed mesh delete can outlive the bounded consumed-id history.
  it.each([2, 3])("keeps a legacy %i-member replacement chain across tombstone eviction and restart", (length) => {
    // Barrier order and failures are covered by tests/atomic-write-durable.test.ts and the barrier regressions.
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation(() => {});
    try {
      const journal = journalPath();
      const entries: unknown[] = [];
      const first = fakePi();
      const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
      const chain = ["rec-A", "rec-B", "rec-C"].slice(0, length);
      fs.mkdirSync(path.dirname(journal), { recursive: true });
      fs.writeFileSync(journal, JSON.stringify({ version: 1, items: chain.map((deliveryId, generation) => ({
        id: `legacy-${deliveryId}`, from: actor, message: `state ${deliveryId}`, sentAt: Date.now(),
        deliveryId, chain: "legacy-rec-A", generation, supersedes: chain.slice(0, generation),
      })) }));
      main.attachFollowUpDrain(busy(entries), 1_000_000_000, journal);
      // Retain A's mesh record; confirm enough unrelated messages to evict all replaced ids.
      for (let index = 0; index <= 2000; index++) {
        main.deliverAgent({ from: actor, message: `m${index}`, delivery: "steer", deliveryId: `other-${index}` });
        entries.push(persisted(first.sent.at(-1)!));
        if (index % 100 === 0 || index === 2000) first.emit("turn_end", boundary([]), busy(entries));
      }
      const consumed = JSON.parse(fs.readFileSync(`${journal}.delivered`, "utf8")) as { ids: string[] };
      expect(consumed.ids).toHaveLength(2000);
      for (const id of chain) expect(consumed.ids).not.toContain(id);
      const held = JSON.parse(fs.readFileSync(journal, "utf8")) as { items: Array<{ deliveryId: string }> };
      expect(held.items.map((item) => item.deliveryId)).toEqual([chain.at(-1)]);
      expect(entries.some((entry) => JSON.stringify(entry).includes("rec-A"))).toBe(false);
      // Abrupt restart, then the retained mesh record redrains before the newer item is released.
      const second = fakePi();
      const restarted = new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root");
      restarted.attachFollowUpDrain(busy(entries), 1_000_000_000, journal);
      for (const id of chain.slice(0, -1)) {
        expect(restarted.deliverAgent(coalesced(id, `stale ${id}`))).toMatchObject({ duplicate: true });
      }
      expect(restarted.queueDepth().pendingFollowUps).toBe(1);
      second.emit("agent_before_settle", boundary([]), busy(entries));
      expect(second.sent.map((sent) => sent.message.details.deliveryId)).toEqual([chain.at(-1)]);
      entries.push(persisted(second.sent[0]!));
      second.emit("agent_settled", { outcome: "completed" }, busy(entries, true));
      expect(fs.existsSync(journal)).toBe(false);
      const confirmed = JSON.parse(fs.readFileSync(`${journal}.delivered`, "utf8")) as { ids: string[] };
      expect(confirmed.ids.slice(-length)).toEqual(chain);
      // The consumed carrier leaves its whole chain in the durable receipt, even without entries.
      const third = fakePi();
      const confirmedRestart = new MainAgentController(third.pi, "session:root", true, "/tmp/project", "root");
      confirmedRestart.attachFollowUpDrain(busy([]), 60_000, journal);
      for (const id of chain) expect(confirmedRestart.deliverAgent(coalesced(id, "stale"))).toMatchObject({ duplicate: true });
      expect(third.sent).toHaveLength(0);
      // Session receipts also remember the chain when the bounded sidecar no longer does.
      fs.rmSync(`${journal}.delivered`);
      const fourth = fakePi();
      const sessionRestart = new MainAgentController(fourth.pi, "session:root", true, "/tmp/project", "root");
      sessionRestart.attachFollowUpDrain(busy(entries), 60_000, journal);
      for (const id of chain) expect(sessionRestart.deliverAgent(coalesced(id, "stale"))).toMatchObject({ duplicate: true });
      expect(fourth.sent).toHaveLength(0);
    } finally { synced.mockRestore(); }
  });

  it("#169 bounds a 5000-message same-key burst without losing originals across restart and release reload", { timeout: 30_000 }, async () => {
    // Barrier order and failures are covered by tests/atomic-write-durable.test.ts and the barrier regressions.
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation(() => {});
    try {
      const journal = journalPath();
      const entries: unknown[] = [];
      const first = fakePi();
      const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
      main.attachFollowUpDrain(busy(entries), 1_000_000_000, journal);
      let accepted = 0;
      let refused = 0;
      let beforeRefusal = "";
      for (let index = 0; index < 5000; index++) {
        try { main.deliverAgent(coalesced(`burst-${index}`, "tiny")); accepted++; }
        catch (error) {
          expect(String(error)).toMatch(/queue is full/);
          refused++;
          const saved = fs.readFileSync(journal, "utf8");
          if (!beforeRefusal) beforeRefusal = saved;
          expect(saved).toBe(beforeRefusal); // Refusal never truncates or replaces prior originals.
        }
      }
      expect(refused).toBeGreaterThan(0);
      expect(accepted).toBe(FOLLOW_UP_LIMITS.senderItems);
      const saved = JSON.parse(fs.readFileSync(journal, "utf8"));
      expect(saved.items).toHaveLength(accepted);
      const originalIds = Array.from({ length: accepted }, (_, index) => `burst-${index}`);
      expect(saved.items.map((item: { deliveryId: string }) => item.deliveryId)).toEqual(originalIds);
      expect(saved.items.every((item: { supersedes?: string[] }) => item.supersedes === undefined)).toBe(true);
      expect(Buffer.byteLength(JSON.stringify(saved))).toBeLessThan(FOLLOW_UP_LIMITS.senderBytes);
      // Unrelated consumed ids cannot hide any pending original from duplicate detection.
      for (let index = 0; index <= 2000; index++) {
        main.deliverAgent({ from: actor, message: "unrelated", delivery: "steer", deliveryId: `unrelated-${index}` });
        entries.push(persisted(first.sent.at(-1)!));
        if (index % 100 === 0 || index === 2000) first.emit("turn_end", boundary([]), busy(entries));
      }
      expect(JSON.parse(fs.readFileSync(`${journal}.delivered`, "utf8")).ids).not.toContain("burst-0");
      const second = fakePi();
      const restarted = new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root");
      restarted.attachFollowUpDrain(busy(entries), 1_000_000_000, journal);
      expect(restarted.deliverAgent(coalesced("burst-0", "stale"))).toMatchObject({ duplicate: true });
      expect(() => restarted.deliverAgent(coalesced("refused-source", "tiny"))).toThrow(/queue is full/);
      restarted.closeFollowUpDrain();
      vi.resetModules();
      const { MainAgentController: Reloaded } = await import("../src/main-agent.js");
      const third = fakePi();
      const reloaded = new Reloaded(third.pi, "session:root", true, "/tmp/project", "root");
      reloaded.attachFollowUpDrain(busy(entries), 1_000_000_000, journal);
      expect(reloaded.deliverAgent(coalesced("burst-0", "stale"))).toMatchObject({ duplicate: true });
      third.emit("agent_before_settle", boundary([]), busy(entries));
      expect(third.sent).toHaveLength(1);
      expect(third.sent[0]!.message.details.items.map((item: { deliveryId: string }) => item.deliveryId)).toEqual(originalIds);
      entries.push(persisted(third.sent[0]!));
      third.emit("agent_settled", { outcome: "completed" }, busy(entries, true));
      fs.rmSync(`${journal}.delivered`);
      reloaded.closeFollowUpDrain();
      const fourth = fakePi();
      const receiptRestart = new Reloaded(fourth.pi, "session:root", true, "/tmp/project", "root");
      receiptRestart.attachFollowUpDrain(busy(entries), 1_000_000_000, journal);
      for (const id of originalIds) expect(receiptRestart.deliverAgent(coalesced(id, "stale"))).toMatchObject({ duplicate: true });
      expect(fourth.sent).toHaveLength(0);
    } finally { synced.mockRestore(); }
  });

  it.each(["sender", "total"] as const)("#169 charges all original delivery-id bytes to the %s quota before admission", (quota) => {
    const journal = journalPath();
    const first = fakePi();
    const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(busy([]), 1_000_000_000, journal);
    const request = (sender: string, id: string) => ({ ...coalesced(id, "tiny"), from: { ...actor, id: sender } });
    // Each sender holds ~240 KiB across thirteen original delivery ids.
    for (let sender = 0; sender < (quota === "total" ? 4 : 1); sender++) {
      for (let generation = 0; generation <= 12; generation++) {
        main.deliverAgent(request(`sender-${sender}`, `${sender}:${generation}:` + "x".repeat(18_000)));
      }
    }
    const before = fs.readFileSync(journal, "utf8");
    if (quota === "sender") {
      expect(() => main.deliverAgent(request("sender-0", "oversized-ancestry:" + "x".repeat(30_000)))).toThrow(/per sender/);
    } else {
      expect(() => main.deliverAgent(request("sender-4", "over-total:" + "x".repeat(120_000)))).toThrow(/in total/);
    }
    expect(fs.readFileSync(journal, "utf8")).toBe(before);
  });
  it("rolls a failed same-key append back without consuming the earlier original", () => {
    const journal = journalPath();
    const first = fakePi();
    const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(busy([]), 60_000, journal);
    main.deliverAgent(coalesced("rec-A", "old state"));
    const rename = fs.renameSync;
    const failing = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === journal) throw new Error("ENOSPC");
      rename(from, to);
    });
    try {
      expect(() => main.deliverAgent(coalesced("rec-B", "new state"))).toThrow(/could not record/);
    } finally {
      failing.mockRestore();
    }
    expect(fs.existsSync(`${journal}.delivered`)).toBe(false);
    const second = fakePi();
    const restarted = new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root");
    restarted.attachFollowUpDrain(busy([]), 60_000, journal);
    second.emit("agent_before_settle", boundary([]), busy([]));
    expect(second.sent.map((sent) => sent.message.details.deliveryId)).toEqual(["rec-A"]);
    expect(restarted.deliverAgent(coalesced("rec-B", "new state"))).not.toMatchObject({ duplicate: true });
  });

  // Astra round 3 finding 2 on pi-fabric#160: a closed journal refuses a durable id.
  it("refuses a durable delivery once its journal is closed, and sends nothing", () => {
    const journal = journalPath();
    const first = fakePi();
    const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(busy([]), 60_000, journal);
    main.closeFollowUpDrain();
    expect(() => main.deliverAgent({ from: actor, message: "reply", delivery: "steer", deliveryId: "rec-late" })).toThrow(/journal/);
    expect(first.sent).toHaveLength(0);
    main.deliverAgent({ from: actor, message: "no durable id", delivery: "steer" });
    expect(first.sent).toHaveLength(1);                                          // others still go
  });

  it("refuses a consumed id from its persisted set, even without the session entries, and keeps the last 2000", () => {
    // Barrier order and failures are covered by tests/atomic-write-durable.test.ts and the barrier regressions.
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation(() => {});
    try {
      const journal = journalPath();
      const entries: unknown[] = [];
      const first = fakePi();
      const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
      main.attachFollowUpDrain(busy(entries), 60_000, journal);
      for (let index = 0; index <= 2000; index++) {
        main.deliverAgent({ from: actor, message: `m${index}`, delivery: "steer", deliveryId: `rec-${index}` });
        entries.push(persisted(first.sent.at(-1)!));                              // Pi appended it
        if (index % 100 === 0 || index === 2000) first.emit("turn_end", boundary([]), busy(entries));
      }
      expect(fs.existsSync(journal)).toBe(false);
      const ids = (JSON.parse(fs.readFileSync(`${journal}.delivered`, "utf8")) as { ids: string[] }).ids;
      expect(ids).toHaveLength(2000);
      expect(ids[0]).toBe("rec-1");
      const second = fakePi();
      const fresh = new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root");
      fresh.attachFollowUpDrain(busy([]), 60_000, journal);                        // no session entries at all
      expect(fresh.deliverAgent({ from: actor, message: "m2000", delivery: "steer", deliveryId: "rec-2000" })).toMatchObject({ duplicate: true });
      expect(second.sent).toHaveLength(0);
    } finally { synced.mockRestore(); }
  });
});

// Data-safety / trust-boundary regressions for C4 (#17478). These inject the N-validated
// projection, NOT a made-up native file format; real native pair validation is N/I's check.
describe("Main native recovery custody is not a delivery receipt", () => {
  const sourceId = "11111111-1111-4111-8111-111111111111";
  const targetId = "22222222-2222-4222-8222-222222222222";
  const fixture = () => {
    const sourceJournal = journalPath();
    const directory = path.dirname(sourceJournal);
    const source: NativeRecoverySessionRef = { sessionId: sourceId, sessionFile: path.join(directory, "source.jsonl") };
    const target: NativeRecoverySessionRef = { sessionId: targetId, sessionFile: path.join(directory, "target.jsonl") };
    fs.mkdirSync(directory, { recursive: true });
    const header = (ref: NativeRecoverySessionRef) => `${JSON.stringify({ type: "session", version: 3, id: ref.sessionId })}\n`;
    fs.writeFileSync(source.sessionFile, header(source));
    fs.writeFileSync(target.sessionFile, header(target));
    let transfer: NativeRecoveryTransfer = { version: 1, operationId: "operation-1", preparedDigest: "a".repeat(64),
      preparedEntryId: "prepared-1", source, target, phase: "prepared", occurrences: [] };
    let view: NativeRecoveryCustodyResult = { status: "validated", transfers: [transfer] };
    let failed = false;
    const reader: NativeRecoveryCustodyReader = vi.fn(() => { if (failed) throw new Error("native barrier failed"); return view; });
    const context = (ref = target, idle = true) => ({ ...busy([], idle), sessionManager: {
      getEntries: () => [], getSessionId: () => ref.sessionId, getSessionFile: () => ref.sessionFile, isPersisted: () => true,
    } }) as unknown as ExtensionContext;
    const received = () => {
      const prepared = transfer;
      transfer = { ...transfer, phase: "received", receivedEntryId: "received-1" };
      if (view.status === "validated") view = { status: "validated", transfers: view.transfers.map(edge => edge === prepared ? transfer : edge) };
    };
    const original = (id = "original-1", deliveryId = "delivery-1"): HeldAgentMessage & { deliveryId: string } => ({
      id, from: actor, message: "identical content", sentAt: 1_000, deliveryId, handed: true,
      deliverAs: "nextTurn", triggerTurn: false, deliveryReason: "operator-halt",
      outcomeSend: { eventId: `event:${id}`, to: `session:${sourceId}`, from: actor.id, mode: "followUp" },
    });
    const native = (items: HeldAgentMessage[]) => ({ customType: "pi-fabric-agent-message", details: items.length === 1 ? items[0] : {
      ...items[0], id: "batch-carrier", items,
    } });
    const seed = (journal: string, items: HeldAgentMessage[]) => fs.writeFileSync(journal, JSON.stringify({ version: 1, items }));
    const rows = (journal = sourceJournal): HeldAgentMessage[] => JSON.parse(fs.readFileSync(journal, "utf8")).items;
    const outcomes = () => {
      const directory = path.join(path.dirname(path.dirname(sourceJournal)), "delivery-outcomes");
      return fs.existsSync(directory) ? fs.readdirSync(directory).flatMap(file =>
        fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").map(line => JSON.parse(line))) : [];
    };
    const attach = (ref = target, journal = path.join(directory, "target.json"), idle = true, flushMs = 60_000,
      authority: FabricInterruptAuthority = {}) => {
      const h = fakePi(); const ctx = context(ref, idle);
      const main = new MainAgentController(h.pi, `session:${ref.sessionId}`, true, "/tmp/project", ref.sessionId, true, undefined, authority);
      main.attachFollowUpDrain(ctx, flushMs, journal, 600, undefined, reader);
      return { ...h, ctx, main, journal };
    };
    return { sourceJournal, source, target, get transfer() { return transfer; }, reader, context, header, original, native, seed, rows, outcomes, received, attach,
      setView: (next: NativeRecoveryCustodyResult) => { view = next; }, fail: (value: boolean) => { failed = value; } };
  };

  it("preserves original-root interrupt authority and actual abort while T owns delivery and its journal", () => {
    const f = fixture(); f.received();
    const target = f.attach(f.target, undefined, false); const abort = vi.fn(); target.ctx.abort = abort;
    const originalId = `session:${f.source.sessionId}`;
    const from = { id: originalId, name: "original-root", kind: "main" as const };
    const request = { from, verification: "mesh" as const, message: "HOLD", delivery: "steer" as const, priority: "interrupt" as const,
      deliveryId: "future-to-original", outcomeSend: { eventId: "future-event", to: originalId, from: from.id, mode: "steer" as const } };
    try {
      target.emit("tool_execution_start", { toolCallId: "running-tool" }, target.ctx);
      expect(() => target.main.deliverAgent(request)).toThrow(/not authorized to interrupt/);
      expect(abort).not.toHaveBeenCalled();
      const receipt = target.main.deliverNativeRecoveryAgent(originalId, request);
      expect(receipt).toMatchObject({ routed: "main", reason: "interrupt pending settlement", triggered: false });
      expect(target.main.id).toBe(`session:${f.target.sessionId}`);
      expect(abort).toHaveBeenCalledTimes(1);
      expect(target.sent).toEqual([]);
      expect(f.rows(target.journal)).toMatchObject([{ id: receipt.messageId, from, deliveryId: request.deliveryId, outcomeSend: request.outcomeSend }]);
      expect(fs.existsSync(f.sourceJournal)).toBe(false);
      target.emit("agent_settled", { outcome: "aborted" }, target.ctx);
      expect(target.sent).toHaveLength(1);
      expect(target.sent[0]!.options).toMatchObject({ deliverAs: "steer", triggerTurn: true });
      expect(target.sent[0]!.message.details).toMatchObject({ id: receipt.messageId, from, deliveryId: request.deliveryId });
    } finally { target.main.closeFollowUpDrain(); }
  });

  it("asks the retained supervisor check about original S, never substitutes T recipient authority", () => {
    const f = fixture(); f.received(); const originalId = `session:${f.source.sessionId}`;
    const isSupervisor = vi.fn((_sender: unknown, recipientId: string) => recipientId === originalId);
    const target = f.attach(f.target, undefined, false, 60_000, { isSupervisor });
    const abort = vi.fn(); target.ctx.abort = abort;
    const request = { from: actor, verification: "mesh" as const, message: "HOLD", delivery: "steer" as const, priority: "interrupt" as const };
    try {
      target.emit("tool_execution_start", { toolCallId: "running-tool" }, target.ctx);
      expect(() => target.main.deliverAgent(request)).toThrow(/not authorized to interrupt/);
      isSupervisor.mockClear();
      target.main.deliverNativeRecoveryAgent(originalId, request);
      expect(isSupervisor).toHaveBeenCalledExactlyOnceWith(actor, originalId);
      expect(abort).toHaveBeenCalledTimes(1);
    } finally { target.main.closeFollowUpDrain(); }
  });

  it.each(["prepared", "unrelated", "sealed-source", "reader-error"] as const)("rejects native recipient routing with %s custody before interrupt effects", failure => {
    const f = fixture(); if (failure !== "prepared") f.received();
    const target = f.attach(f.target, undefined, false); const abort = vi.fn(); target.ctx.abort = abort;
    let originalId = `session:${f.source.sessionId}`;
    if (failure === "unrelated") originalId = "session:00000000-0000-4000-8000-000000000099";
    if (failure === "reader-error") f.fail(true);
    if (failure === "sealed-source") {
      const { receivedEntryId: _received, ...binding } = f.transfer;
      const outgoing: NativeRecoveryTransfer = { ...binding, phase: "prepared", operationId: "operation-2", preparedEntryId: "prepared-2",
        preparedDigest: "b".repeat(64), source: f.target, target: { sessionId: "next", sessionFile: path.join(path.dirname(f.target.sessionFile), "next.jsonl") }, occurrences: [] };
      f.setView({ status: "validated", transfers: [f.transfer, outgoing] });
    }
    try {
      target.emit("tool_execution_start", { toolCallId: "running-tool" }, target.ctx);
      expect(() => target.main.deliverNativeRecoveryAgent(originalId, { from: { id: originalId, name: "source", kind: "main" },
        verification: "mesh", delivery: "steer", priority: "interrupt", message: "not admitted", deliveryId: "not-admitted" })).toThrow();
      expect(abort).not.toHaveBeenCalled(); expect(target.sent).toEqual([]);
      expect(fs.existsSync(target.journal) ? f.rows(target.journal) : []).toEqual([]);
    } finally { target.main.closeFollowUpDrain(); }
  });

  it("keeps current T inbox gates authoritative after original S's received chain validates", () => {
    const f = fixture(); f.received(); const target = f.attach(f.target, undefined, false);
    const abort = vi.fn(); target.ctx.abort = abort;
    target.main.attachFollowUpDrain(target.ctx, 60_000, target.journal, 600, { owns: () => true, active: () => false }, f.reader);
    try {
      const originalId = `session:${f.source.sessionId}`;
      target.emit("tool_execution_start", { toolCallId: "running-tool" }, target.ctx);
      expect(() => target.main.deliverNativeRecoveryAgent(originalId, { from: { id: originalId, name: "source", kind: "main" },
        verification: "mesh", delivery: "steer", priority: "interrupt", message: "HOLD" })).toThrow(/rotated/);
      expect(abort).not.toHaveBeenCalled(); expect(target.sent).toEqual([]);
    } finally { target.main.closeFollowUpDrain(); }
  });

  it.each([0, 60_000])("installs custody before drain (%i), reserving PREPARED, adopting RECEIVED, retiring only appended", flushMs => {
    const f = fixture(); const item = f.original();
    f.transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: f.native([item]) }];
    f.seed(f.sourceJournal, [item]);
    const source = f.attach(f.source, f.sourceJournal, true, flushMs);
    const target = f.attach(f.target, undefined, true, flushMs);
    try {
      expect(source.sent).toEqual([]);
      expect(f.rows()).toEqual([item]);
      expect(() => target.main.receiveInboxItem(item)).toThrow(/PREPARED/);
      const retry = { from: actor, message: item.message, delivery: "followUp" as const, deliveryId: item.deliveryId };
      expect(() => target.main.deliverAgent(retry)).toThrow(/PREPARED/);
      expect(fs.existsSync(`${target.journal}.delivered`)).toBe(false);
      expect(f.outcomes()).toEqual([]);
      f.received();
      target.main.receiveInboxItem(item);
      target.main.receiveInboxItem(item);
      expect(f.rows(target.journal)).toEqual([item]);
      for (let i = 0; i < 2; i++) {
        expect(target.main.deliverAgent(retry)).toMatchObject({ duplicate: true, triggered: false, messageId: item.id });
        target.emit("agent_before_settle", boundary([]), target.ctx);
        target.main.confirmInbox();
      }
      expect(target.sent).toEqual([]);
      expect(f.rows(target.journal)).toEqual([item]);
      expect(f.outcomes()).toEqual([]);
      expect(fs.existsSync(`${target.journal}.delivered`)).toBe(false);
      f.transfer.occurrences = [{ ...f.transfer.occurrences[0]!, state: "appended" }];
      target.main.confirmInbox(); source.main.confirmInbox();
      expect(fs.existsSync(target.journal)).toBe(false);
      expect(fs.existsSync(f.sourceJournal)).toBe(false);
      expect(JSON.parse(fs.readFileSync(`${target.journal}.delivered`, "utf8")).ids).toEqual([item.deliveryId]);
      expect(f.outcomes()).toEqual([{ ...item.outcomeSend, outcome: "delivered",
        reason: "delivered, not triggered: recipient is halted by its operator", at: expect.any(Number) }]);
      expect(target.sent).toEqual([]);
      expect(source.sent).toEqual([]);
    } finally { source.main.closeFollowUpDrain(); target.main.closeFollowUpDrain(); }
  });

  it("revalidates a live held batch before handing or rendering it, without inventing handed evidence", () => {
    const f = fixture(); f.setView({ status: "none" });
    const source = f.attach(f.source, f.sourceJournal, false);
    try {
      source.main.deliverAgent({ from: actor, message: "held before native settlement", delivery: "followUp", deliveryId: "live-held" });
      const original = f.rows()[0]!;
      expect(original.handed).toBeUndefined();
      f.transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: f.native([original]) }];
      f.setView({ status: "validated", transfers: [f.transfer] });
      source.emit("agent_before_settle", boundary([]), source.ctx);
      expect(source.sent).toEqual([]);
      expect(f.rows()).toEqual([original]);
      expect(source.main.drainSnapshot()).toMatchObject({ held: 0, handed: 1 }); // Awaiting native, not a new Pi send.
      expect(f.outcomes()).toEqual([]);
      expect(fs.existsSync(`${f.sourceJournal}.delivered`)).toBe(false);
    } finally { source.main.closeFollowUpDrain(); }
  });

  it("dedupes a restored original absent from the Main journal, but preserves a distinct ID with identical text", () => {
    const f = fixture(); const item = f.original(); f.received();
    f.transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: f.native([item]) }];
    const target = f.attach();
    try {
      expect(target.main.deliverAgent({ from: actor, message: item.message, delivery: "followUp", deliveryId: item.deliveryId }))
        .toMatchObject({ duplicate: true, triggered: false, messageId: item.id });
      expect(fs.existsSync(target.journal)).toBe(false); // Native custody, not a fabricated C payload/receipt.
      expect(target.sent).toEqual([]);
      target.main.deliverAgent({ from: actor, message: item.message, delivery: "nextTurn", triggerTurn: false, deliveryId: "distinct-delivery" });
      expect(target.sent).toHaveLength(1);
      expect(target.sent[0]!.message.details.deliveryId).toBe("distinct-delivery");
      expect(target.sent[0]!.options).toEqual({ deliverAs: "nextTurn", triggerTurn: false });
      expect(f.outcomes()).toEqual([]);
    } finally { target.main.closeFollowUpDrain(); }
  });

  it("adopts a restored child with no delivery ID and no Main row by exact original carrier identity", () => {
    const f = fixture(); f.received(); const { deliveryId: _deliveryId, ...item } = f.original("unindexed-child");
    f.transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: f.native([item]) }];
    const target = f.attach();
    try {
      target.main.receiveInboxItem(item); target.main.receiveInboxItem(item);
      expect(f.rows(target.journal)).toEqual([JSON.parse(JSON.stringify(item))]);
      expect(f.rows(target.journal)[0]!.deliveryId).toBeUndefined(); // Do not invent inbox:<id> in the native original.
      expect(target.sent).toEqual([]); expect(f.outcomes()).toEqual([]);
      f.transfer.occurrences = [{ ...f.transfer.occurrences[0]!, state: "handled" }];
      target.main.confirmInbox();
      expect(fs.existsSync(target.journal)).toBe(false);
      target.main.receiveInboxItem(item); // Repeated route delivery stays consumed, without sending.
      expect(target.sent).toEqual([]);
      expect(f.outcomes().map(row => row.outcome)).toEqual(["delivered"]);
    } finally { target.main.closeFollowUpDrain(); }
  });

  it("retains mixed batch child, chain and delivery identities without resending native children or suppressing Fabric-only neighbors", () => {
    const f = fixture(); f.received();
    const a = { ...f.original("child-A", "delivery-A"), chain: "chain-A", generation: 2, supersedes: ["delivery-old-A"] };
    const { deliveryId: _deliveryId, ...bOriginal } = f.original("child-B");
    const b = { ...bOriginal, inboxReceipts: ["opaque-event-receipt"], inboxEventId: "root-event-B" };
    const { handed: _handed, deliverAs: _deliverAs, triggerTurn: _triggerTurn, ...c } = f.original("child-C", "delivery-C");
    f.transfer.occurrences = [{ ordinal: 7, state: "pending", customMessage: f.native([a, b]) }];
    const journal = path.join(path.dirname(f.sourceJournal), "mixed.json"); f.seed(journal, [a, b, c]);
    const target = f.attach(f.target, journal);
    try {
      expect(target.sent.map(sent => sent.message.details.id)).toEqual([c.id]);
      expect(f.rows(journal).filter(item => item.id !== c.id)).toEqual([a, JSON.parse(JSON.stringify(b))]);
      for (const deliveryId of ["delivery-A", "delivery-old-A"]) {
        expect(target.main.deliverAgent({ from: actor, message: "same", delivery: "steer", deliveryId }))
          .toMatchObject({ duplicate: true, triggered: false, messageId: a.id });
      }
      target.main.receiveInboxItem(b); // No deliveryId: exact original carrier prevents fallback admission.
      expect(target.sent).toHaveLength(1);
      expect(nativeRecoveryCarrierIdentities(f.transfer.occurrences[0]!.customMessage)).toEqual({
        carrierIds: ["batch-carrier", "chain-A", "child-A", "child-B"],
        deliveryIds: ["delivery-A", "delivery-old-A"], inboxIds: ["root-event-B", "opaque-event-receipt"],
      });
      f.transfer.occurrences = [{ ...f.transfer.occurrences[0]!, state: "handled" }];
      target.main.confirmInbox();
      expect(f.rows(journal).map(item => item.id)).toEqual([c.id]);
      expect(f.outcomes().map(row => row.eventId)).toEqual(["event:child-A", "event:child-B"]);
      expect(target.sent).toHaveLength(1);
    } finally { target.main.closeFollowUpDrain(); }
  });

  it("keeps two distinct native occurrence IDs even when text and work key are equal", () => {
    const f = fixture(); f.received();
    const a = { ...f.original("same-A", "D-A"), data: { coalesceKey: "same-work" } };
    const b = { ...f.original("same-B", "D-B"), data: { coalesceKey: "same-work" } };
    f.transfer.occurrences = [a, b].map((item, ordinal) => ({ ordinal, state: "pending", customMessage: f.native([item]) }));
    const target = f.attach();
    try {
      for (const item of [a, b]) target.main.receiveInboxItem(item);
      expect(f.rows(target.journal).map(item => item.id)).toEqual([a.id, b.id]);
      f.transfer.occurrences = [{ ...f.transfer.occurrences[0]!, state: "appended" }, f.transfer.occurrences[1]!];
      target.main.confirmInbox();
      expect(f.rows(target.journal).map(item => item.id)).toEqual([b.id]);
      expect(f.outcomes().map(row => row.eventId)).toEqual(["event:same-A"]);
      expect(target.sent).toEqual([]);
    } finally { target.main.closeFollowUpDrain(); }
  });

  it("SAFE: excludes source-durable history even with a stale source Main journal", () => {
    const f = fixture(); f.received(); const item = f.original();
    f.seed(f.sourceJournal, [item]);
    fs.appendFileSync(f.source.sessionFile, `${JSON.stringify({ type: "custom_message", ...f.native([item]) })}\n`);
    expect(f.transfer.occurrences).toEqual([]); // N's settled pending projection excludes history.
    const source = f.attach(f.source, f.sourceJournal); const target = f.attach();
    try {
      expect(fs.existsSync(f.sourceJournal)).toBe(false);
      expect(source.sent).toEqual([]); expect(target.sent).toEqual([]);
      expect(fs.existsSync(target.journal)).toBe(false);
      expect(f.outcomes().map(row => row.outcome)).toEqual(["delivered"]);
      expect(() => source.main.deliverAgent({ from: actor, message: "new", delivery: "steer" })).toThrow(/sealed/);
    } finally { source.main.closeFollowUpDrain(); target.main.closeFollowUpDrain(); }
  });

  it.each(["reader-error", "reader-hold", "ambiguous", "duplicate-ordinal", "wrong-scope"] as const)(
    "holds original payload and outcome through restart with %s", failure => {
      const f = fixture(); f.received(); const item = f.original();
      f.transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: f.native([item]) }];
      const journal = path.join(path.dirname(f.sourceJournal), "held.json"); f.seed(journal, [item]);
      if (failure === "reader-error") f.fail(true);
      if (failure === "reader-hold") f.setView({ status: "hold", reason: "uncertain canonical header" });
      if (failure === "ambiguous") f.transfer.occurrences = [{ ...f.transfer.occurrences[0]!, state: "ambiguous" }];
      if (failure === "duplicate-ordinal") f.transfer.occurrences = [f.transfer.occurrences[0]!, f.transfer.occurrences[0]!];
      if (failure === "wrong-scope") f.transfer.target = { ...f.target, sessionFile: path.join(path.dirname(journal), "other.jsonl") };
      // A stale plain custom-message is not allowed to override uncertain native custody.
      fs.appendFileSync(f.target.sessionFile, `${JSON.stringify({ type: "custom_message", ...f.native([item]) })}\n`);
      for (let restart = 0; restart < 2; restart++) {
        const target = f.attach(f.target, journal);
        try {
          target.emit("turn_end", boundary([]), target.ctx);
          target.emit("agent_before_settle", boundary([]), target.ctx);
          target.main.confirmInbox();
          expect(() => target.main.deliverAgent({ from: actor, message: item.message, delivery: "steer", deliveryId: item.deliveryId })).toThrow();
          expect(f.rows(journal)).toEqual([item]);
          expect(target.sent).toEqual([]); expect(f.outcomes()).toEqual([]);
          expect(fs.existsSync(`${journal}.delivered`)).toBe(false);
        } finally { target.main.closeFollowUpDrain(); }
      }
    },
  );

  it.each(["wrong-header", "changed-session", "changed-path"] as const)("does not consume on %s native-original receipts", wrong => {
    const f = fixture(); f.received(); const item = f.original();
    f.transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: f.native([item]) }];
    const journal = path.join(path.dirname(f.sourceJournal), "stale.json"); f.seed(journal, [item]);
    const target = f.attach(f.target, journal);
    try {
      if (wrong === "wrong-header") fs.writeFileSync(f.target.sessionFile, `${f.header(f.source)}${JSON.stringify({ type: "custom_message", ...f.native([item]) })}\n`);
      if (wrong === "changed-session") target.ctx.sessionManager.getSessionId = () => f.source.sessionId;
      if (wrong === "changed-path") target.ctx.sessionManager.getSessionFile = () => f.source.sessionFile;
      target.main.confirmInbox(); target.emit("turn_end", boundary([]), target.ctx);
      expect(f.rows(journal)).toEqual([item]);
      expect(target.sent).toEqual([]); expect(f.outcomes()).toEqual([]);
    } finally { target.main.closeFollowUpDrain(); }
  });

  it("does not use a wrong source header as the SAFE consumed-history proof", () => {
    const f = fixture(); f.received(); const item = f.original();
    f.seed(f.sourceJournal, [item]);
    fs.writeFileSync(f.source.sessionFile, `${f.header(f.target)}${JSON.stringify({ type: "custom_message", ...f.native([item]) })}\n`);
    const source = f.attach(f.source, f.sourceJournal);
    try {
      source.main.confirmInbox(); source.emit("agent_before_settle", boundary([]), source.ctx);
      expect(f.rows()).toEqual([item]); expect(f.outcomes()).toEqual([]); expect(source.sent).toEqual([]);
    } finally { source.main.closeFollowUpDrain(); }
  });

  it("keeps native-consumed payload when outcome persistence fails, then retries bookkeeping without native/Fabric replay", () => {
    const f = fixture(); f.received(); const item = f.original();
    f.transfer.occurrences = [{ ordinal: 0, state: "handled", customMessage: f.native([item]) }];
    const journal = path.join(path.dirname(f.sourceJournal), "outcome-failure.json"); f.seed(journal, [item]);
    const append = vi.spyOn(deliveryOutcomes, "appendDeliveryOutcome").mockImplementation(() => { throw new Error("outcome storage unavailable"); });
    const target = f.attach(f.target, journal);
    try {
      target.main.confirmInbox(); target.emit("agent_before_settle", boundary([]), target.ctx);
      expect(f.rows(journal)).toEqual([item]); expect(target.sent).toEqual([]);
      expect(fs.existsSync(`${journal}.delivered`)).toBe(false);
    } finally { target.main.closeFollowUpDrain(); append.mockRestore(); }
    const restarted = f.attach(f.target, journal);
    try {
      restarted.main.confirmInbox();
      expect(fs.existsSync(journal)).toBe(false); expect(restarted.sent).toEqual([]);
      expect(f.outcomes().map(row => row.outcome)).toEqual(["delivered"]);
    } finally { restarted.main.closeFollowUpDrain(); }
  });

  it.each(["torn-json", "unknown-version", "malformed-carrier"] as const)("preserves %s Main bookkeeping instead of overwriting it during recovery", corruption => {
    const f = fixture(); f.received(); const item = f.original();
    f.transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: f.native([item]) }];
    const journal = path.join(path.dirname(f.sourceJournal), "unreadable.json");
    const bytes = corruption === "torn-json" ? '{"version":1,"items":[' : corruption === "unknown-version"
      ? JSON.stringify({ version: 9, items: [item] }) : JSON.stringify({ version: 1, items: [{ ...item, sentAt: null }] });
    fs.writeFileSync(journal, bytes);
    const target = f.attach(f.target, journal);
    try {
      target.main.confirmInbox(); target.emit("agent_before_settle", boundary([]), target.ctx);
      expect(() => target.main.deliverAgent({ from: actor, message: item.message, delivery: "steer", deliveryId: item.deliveryId })).toThrow(/journal/);
      expect(() => target.main.receiveInboxItem(item)).toThrow(/journal/);
      expect(fs.readFileSync(journal, "utf8")).toBe(bytes);
      expect(target.sent).toEqual([]); expect(f.outcomes()).toEqual([]);
    } finally { target.main.closeFollowUpDrain(); }
    expect(fs.readFileSync(journal, "utf8")).toBe(bytes);
  });

  it("retains the original journal across consumed-index failure and recovers without another send", () => {
    const f = fixture(); f.received(); const item = f.original();
    f.transfer.occurrences = [{ ordinal: 0, state: "handled", customMessage: f.native([item]) }];
    const journal = path.join(path.dirname(f.sourceJournal), "index-failure.json"); f.seed(journal, [item]);
    const rename = fs.renameSync;
    const fault = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === `${journal}.delivered`) throw new Error("index commit failed");
      return rename(from, to);
    });
    const target = f.attach(f.target, journal);
    try {
      target.main.confirmInbox();
      expect(f.rows(journal)).toEqual([item]); expect(target.sent).toEqual([]);
    } finally { target.main.closeFollowUpDrain(); fault.mockRestore(); }
    const restarted = f.attach(f.target, journal);
    try {
      restarted.main.confirmInbox();
      expect(fs.existsSync(journal)).toBe(false); expect(restarted.sent).toEqual([]);
      expect(f.outcomes().map(row => row.outcome)).toEqual(["delivered"]);
    } finally { restarted.main.closeFollowUpDrain(); }
  });

  it("checks complete predecessor chains, child identity conflicts and opaque root inbox membership", () => {
    const f = fixture(); f.received(); const item = f.original();
    f.transfer.occurrences = [{ ordinal: 1, state: "pending", customMessage: f.native([item]) }];
    const next = { sessionId: "33333333-3333-4333-8333-333333333333", sessionFile: path.join(path.dirname(f.sourceJournal), "next.jsonl") };
    const second: NativeRecoveryTransfer = { ...f.transfer, operationId: "operation-2", source: f.target, target: next,
      phase: "received", preparedEntryId: "prepared-2", receivedEntryId: "received-2", preparedDigest: "b".repeat(64),
      occurrences: [{ ordinal: 0, state: "handled", customMessage: f.native([item]) },
        { ordinal: 1, state: "pending", customMessage: { customType: "pi-fabric-inbox", details: {
          ids: ["root-event"], receipts: ["opaque-receipt"], nonce: "not-authority",
        } } }] };
    f.setView({ status: "validated", transfers: [f.transfer, second] });
    const view = readNativeRecoveryCustody(next, f.reader);
    expect(nativeRecoveryCarrierCustody(view, { deliveryId: item.deliveryId })).toMatchObject({ status: "consumed", transfer: second });
    expect(nativeRecoveryCarrierCustody(view, { inboxId: "root-event" }).status).toBe("received");
    expect(nativeRecoveryCarrierCustody(view, { inboxId: "not-authority" }).status).toBe("none");
    second.occurrences = [...second.occurrences, { ...second.occurrences[0]!, ordinal: 2 }];
    expect(nativeRecoveryCarrierCustody(readNativeRecoveryCustody(next, f.reader), { carrierId: item.id }).status).toBe("hold");
    second.source = f.source;
    expect(readNativeRecoveryCustody(next, f.reader).status).toBe("hold");
    second.source = f.target; second.target = f.source;
    expect(readNativeRecoveryCustody(f.source, f.reader).status).toBe("hold");
  });
});

// The same boundary in real Pi sessions: a steer to a streaming Main sits in Pi's steer queue when
// deliverAgent returns; the process dies before the session writes it; the restart delivers it once.
describe("a delivery id across a kill, in real Pi sessions", () => {
  const start = async (root: string, journal: string, sessionManager: SessionManager = SessionManager.inMemory(root)) => {
    const faux = fauxProvider();
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    const state: { main?: MainAgentController; releaseTool?: () => void } = {};
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir: path.join(root, "agent"), noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      extensionFactories: [{
        name: "delivery",
        factory: (pi: ExtensionAPI) => {
          pi.registerTool({
            name: "work", label: "work", description: "blocks until released", parameters: Type.Object({}),
            execute: async () => {
              await new Promise<void>((resolve) => { state.releaseTool = resolve; });
              return { content: [{ type: "text", text: "done" }], details: {} };
            },
          });
          pi.on("session_start", (_event, ctx) => {
            state.main = new MainAgentController(pi, "session:root", true, root, "root");
            state.main.attachFollowUpDrain(ctx, 60_000, journal);
          });
        },
      }],
    });
    await loader.reload();
    const { session } = await createAgentSession({
      cwd: root, agentDir: path.join(root, "agent"), modelRuntime, model: faux.getModel(), resourceLoader: loader,
      sessionManager, tools: ["work"],
    });
    sessions.push(session);
    await session.bindExtensions({});
    await waitFor(() => state.main !== undefined);
    return { session, faux, state };
  };
  const delivered = (session: AgentSession) => session.messages.filter((message) =>
    message.role === "custom" && (message as { customType?: string }).customType === "pi-fabric-agent-message")
    .map((message) => (message as { details?: { deliveryId?: string } }).details?.deliveryId);

  it("delivers a steer once after a kill before the session wrote it", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-delivery-kill-"));
    roots.push(root);
    const journal = path.join(root, "journal", "root.json");
    const one = await start(root, journal);
    one.faux.setResponses([fauxAssistantMessage(fauxToolCall("work", {}), { stopReason: "toolUse" }), fauxAssistantMessage("done")]);
    const prompted = one.session.prompt("go");
    await waitFor(() => one.state.releaseTool !== undefined);
    one.state.main!.deliverAgent({ from: actor, message: "exactly once", delivery: "steer", deliveryId: "rec-kill" });
    expect(delivered(one.session)).toEqual([]);                                   // only in Pi's steer queue
    expect(fs.readFileSync(journal, "utf8")).toContain("rec-kill");
    // SIGKILL here: the disk holds this journal and no session entry. Process 2 starts from that
    // disk state (a copy, so process 1's teardown cannot touch it).
    const killed = path.join(root, "killed", "root.json");
    fs.mkdirSync(path.dirname(killed), { recursive: true });
    fs.copyFileSync(journal, killed);
    one.session.abort();
    one.state.releaseTool!();
    await prompted.catch(() => undefined);
    const two = await start(root, killed);
    two.faux.setResponses([fauxAssistantMessage("first"), fauxAssistantMessage("read it"), fauxAssistantMessage("more")]);
    await two.session.prompt("next task");
    await waitFor(() => two.session.isIdle);
    expect(delivered(two.session)).toEqual(["rec-kill"]);
    expect(fs.existsSync(killed)).toBe(false);
    expect(two.state.main!.deliverAgent({ from: actor, message: "exactly once", delivery: "steer", deliveryId: "rec-kill" }))
      .toMatchObject({ duplicate: true });
    expect(delivered(two.session)).toEqual(["rec-kill"]);
  });

  // Security round 3 on pi-fabric#160 (S1): Pi inserts an entry in memory before it writes it, with
  // no rollback on a write error. An entry in getEntries() whose write failed is not delivered.
  it("keeps a message whose session write failed after the in-memory insert, and a restart replays it once", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-delivery-persist-"));
    roots.push(root);
    const journal = path.join(root, "journal", "root.json");
    const manager = SessionManager.create(root, path.join(root, "sessions"));
    const persist = (manager as unknown as { _persist(entry: unknown): void })._persist.bind(manager);
    let failures = 0;
    (manager as unknown as { _persist(entry: unknown): void })._persist = (entry) => {
      const value = entry as { type?: string; customType?: string };
      if (failures === 0 && value.type === "custom_message" && value.customType === "pi-fabric-agent-message") {
        failures++;
        throw new Error("EIO: session file unwritable");
      }
      persist(entry);
    };
    const one = await start(root, journal, manager);
    one.faux.setResponses([fauxAssistantMessage(fauxToolCall("work", {}), { stopReason: "toolUse" }), fauxAssistantMessage("done")]);
    const prompted = one.session.prompt("go");
    await waitFor(() => one.state.releaseTool !== undefined);
    one.state.main!.deliverAgent({ from: actor, message: "exactly once", delivery: "steer", deliveryId: "rec-eio" });
    one.state.releaseTool!();
    await prompted.catch(() => undefined);
    await waitFor(() => one.session.isIdle);
    expect(failures).toBe(1);
    const file = manager.getSessionFile()!;
    const onDisk = () => fs.readFileSync(file, "utf8").split("\n").filter((line) => line.includes('"deliveryId":"rec-eio"')).length;
    expect(manager.getEntries().some((entry) => JSON.stringify(entry).includes("rec-eio"))).toBe(true); // in memory only
    expect(onDisk()).toBe(0);
    // Boundaries passed (turn_end, agent_settled); a later entry was written after the failed one.
    expect(fs.readFileSync(journal, "utf8")).toContain("rec-eio");
    expect(fs.existsSync(`${journal}.delivered`) && fs.readFileSync(`${journal}.delivered`, "utf8").includes("rec-eio")).toBe(false);
    // Restart on the persisted session file and the same journal.
    const two = await start(root, journal, SessionManager.open(file));
    two.faux.setResponses([fauxAssistantMessage("first"), fauxAssistantMessage("read it"), fauxAssistantMessage("more")]);
    await two.session.prompt("next task");
    await waitFor(() => two.session.isIdle);
    expect(delivered(two.session)).toEqual(["rec-eio"]);
    expect(onDisk()).toBe(1);
    expect(fs.existsSync(journal)).toBe(false);
    expect(fs.readFileSync(`${journal}.delivered`, "utf8")).toContain("rec-eio");
  });
});
