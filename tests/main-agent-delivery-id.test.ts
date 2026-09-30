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
import { FOLLOW_UP_LIMITS, MainAgentController } from "../src/main-agent.js";

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
      events.push(descriptors.get(fd) === file ? "session" : fs.fstatSync(fd).isDirectory() ? "directory" : "consumed");
      sync(fd);
    });
    try {
      first.emit("agent_settled", { outcome: "completed" }, context);
      expect(events).toEqual(process.platform === "win32" ? ["session", "consumed"] : ["session", "consumed", "directory"]);
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
        .toMatchObject({ duplicate: true, messageId: result.messageId });
      expect(first.sent).toHaveLength(1);
      // Main dies before Pi appends it to the session (no close). A new controller, same journal.
      const second = fakePi();
      const restarted = new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root");
      restarted.attachFollowUpDrain(busy(entries), 60_000, journal);
      expect(restarted.deliverAgent({ from: actor, message: "reply", delivery, triggerTurn, deliveryId: "rec-1" }))
        .toMatchObject({ duplicate: true });                                     // admitted, still pending
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

  // Astra round 3 finding 4 on pi-fabric#160: a replaced id is durably consumed before the return.
  const coalesced = (deliveryId: string, message: string) =>
    ({ from: actor, message, delivery: "followUp" as const, triggerTurn: true, data: { coalesceKey: "state" }, deliveryId });
  it("refuses a replaced delivery id after an abrupt restart, and keeps its replacement", () => {
    const journal = journalPath();
    const entries: unknown[] = [];
    const first = fakePi();
    const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(busy(entries), 60_000, journal);
    main.deliverAgent(coalesced("rec-A", "old state"));                        // A's mesh delete then fails
    expect(main.deliverAgent(coalesced("rec-B", "new state"))).toMatchObject({ coalesced: true });
    // Killed: no boundary, no close. A new controller on the same journal; A's record is drained again.
    const second = fakePi();
    const restarted = new MainAgentController(second.pi, "session:root", true, "/tmp/project", "root");
    restarted.attachFollowUpDrain(busy(entries), 60_000, journal);
    expect(restarted.deliverAgent(coalesced("rec-A", "old state"))).toMatchObject({ duplicate: true });
    expect(restarted.queueDepth()).toMatchObject({ pendingFollowUps: 1 });
    second.emit("agent_before_settle", boundary([]), busy(entries));
    expect(second.sent.map((sent) => sent.message.details.deliveryId)).toEqual(["rec-B"]);
  });

  // smarty-dev#2339 F2: a failed mesh delete can outlive the bounded consumed-id history.
  it.each([2, 3])("keeps a %i-member replacement chain across tombstone eviction and restart", (length) => {
    // Barrier order and failures are covered by tests/atomic-write-durable.test.ts and the barrier regressions.
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation(() => {});
    try {
      const journal = journalPath();
      const entries: unknown[] = [];
      const first = fakePi();
      const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
      main.attachFollowUpDrain(busy(entries), 1_000_000_000, journal);
      const chain = ["rec-A", "rec-B", "rec-C"].slice(0, length);
      for (const id of chain) main.deliverAgent(coalesced(id, `state ${id}`));
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

  it("#169 round 1 bounds a 5000-replacement burst without losing F2 tombstones across restart and release reload", { timeout: 30_000 }, async () => {
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
          expect(saved).toBe(beforeRefusal); // Refusal never truncates or replaces the prior carrier.
        }
      }
      expect(refused).toBeGreaterThan(0);
      expect(accepted).toBeLessThanOrEqual(FOLLOW_UP_LIMITS.ancestryIds + 1);
      const saved = JSON.parse(fs.readFileSync(journal, "utf8"));
      expect(saved.items).toHaveLength(1);
      expect(saved.items[0].supersedes).toHaveLength(accepted - 1);
      expect(Buffer.byteLength(JSON.stringify(saved))).toBeLessThan(FOLLOW_UP_LIMITS.senderBytes);
      // Evict A from the bounded sidecar while its source may still be redrained.
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
      expect(third.sent[0]!.message.details.deliveryId).toBe(`burst-${accepted - 1}`);
      expect(third.sent[0]!.message.details.supersedes).toHaveLength(accepted - 1);
      entries.push(persisted(third.sent[0]!));
      third.emit("agent_settled", { outcome: "completed" }, busy(entries, true));
      fs.rmSync(`${journal}.delivered`);
      reloaded.closeFollowUpDrain();
      const fourth = fakePi();
      const receiptRestart = new Reloaded(fourth.pi, "session:root", true, "/tmp/project", "root");
      receiptRestart.attachFollowUpDrain(busy(entries), 1_000_000_000, journal);
      expect(receiptRestart.deliverAgent(coalesced("burst-0", "stale"))).toMatchObject({ duplicate: true });
      expect(fourth.sent).toHaveLength(0);
    } finally { synced.mockRestore(); }
  });

  it.each(["sender", "total"] as const)("#169 round 1 charges ancestry bytes to the %s quota before admission", (quota) => {
    const journal = journalPath();
    const first = fakePi();
    const main = new MainAgentController(first.pi, "session:root", true, "/tmp/project", "root");
    main.attachFollowUpDrain(busy([]), 1_000_000_000, journal);
    const request = (sender: string, id: string) => ({ ...coalesced(id, "tiny"), from: { ...actor, id: sender } });
    // Each carrier holds ~240 KiB of delivery-id ancestry, still fewer than 1024 ids.
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
  it("rolls a replacement back when its journal write fails, and the replaced id stays admissible", () => {
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
    expect(fs.readFileSync(`${journal}.delivered`, "utf8")).not.toContain("rec-A");
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
