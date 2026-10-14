import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane, type FabricControlHandler } from "../src/topology/control-plane.js";

// Expired durable followUps are admitted into Main's journal (deferred), while expired live-only
// steers stay refused notRun. A bridge-verified command's deadline is measured from the LOCAL
// envelope createdAt plus its bounded TTL; an unverified bridge marker never changes the clock.

const MAIN = "session:main";
const EXPIRED = "Fabric control command expired";
const roots: string[] = [];
const planes: FabricControlPlane[] = [];
const mains: MainAgentController[] = [];
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const identity = (id: string, kind: MeshIdentity["kind"] = "main"): MeshIdentity => ({ id, name: id, kind, sessionId: id });
const bridged = (id: string): MeshIdentity => ({ ...identity(id), verified: "bridge" } as MeshIdentity);

afterEach(async () => {
  for (const main of mains.splice(0)) { try { main.closeFollowUpDrain(); } catch { /* closed */ } }
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tempRoot = (prefix: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); roots.push(root);
  return root;
};

const acks = (mesh: MeshStore, commandId: string) => mesh.read({ topic: "fabric.control.ack", limit: 1_000 })
  .map(event => event.data as Record<string, unknown> | undefined)
  .filter((data): data is Record<string, unknown> => data?.commandId === commandId);
const awaitAck = async (mesh: MeshStore, commandId: string, count = 1) => {
  await vi.waitFor(() => expect(acks(mesh, commandId).length).toBeGreaterThanOrEqual(count), { timeout: 8_000, interval: 20 });
  return acks(mesh, commandId).at(-1)!;
};

const plane = (mesh: MeshStore, acknowledgementTimeoutMs = 1_000) => {
  const value = new FabricControlPlane(mesh, identity(MAIN), {
    enabled: true, hostId: MAIN, pollMs: 20, acknowledgementTimeoutMs, onUnfencedSenderWarning: () => undefined,
  });
  planes.push(value);
  return value;
};

const command = (operation: "steer" | "followUp" | "ask" | "cancel", commandId: string, replyTo: string, requestedAt: number, deadlineAt: number,
  extra: Record<string, unknown> = {}) => ({
  version: 1, commandId, targetId: MAIN, operation, replyTo, requestedAt, deadlineAt, message: `${operation} ${commandId}`, ...extra,
});

// Main fixture: the real MainAgentController with a journal, behind the real router handler.
type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
const fakePi = () => {
  const handlers = new Map<string, Handler[]>();
  const sent: Array<{ message: { content: string; details: Record<string, any> }; options: { deliverAs: string; triggerTurn: boolean } }> = [];
  const pi = {
    on: (name: string, fn: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), fn]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(h => h !== fn));
    },
    sendMessage: vi.fn((message, options) => { sent.push({ message, options }); }),
    sendUserMessage: vi.fn(),
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const emit = (name: string, event: unknown, ctx: ExtensionContext) => {
    for (const handler of handlers.get(name) ?? []) handler(event, ctx);
  };
  return { pi, sent, emit };
};
const mainContext = (entries: unknown[], state: { idle: boolean }) => ({
  isIdle: () => state.idle, hasPendingMessages: () => false,
  signal: { aborted: false },
  sessionManager: { getEntries: () => entries },
}) as unknown as ExtensionContext;
const deliveryIds = (sent: ReturnType<typeof fakePi>["sent"]) => sent.flatMap(entry => {
  const details = entry.message.details;
  const items = Array.isArray(details.items) ? details.items : [details];
  return items.map((item: { deliveryId?: string }) => item.deliveryId).filter((id: unknown): id is string => typeof id === "string");
});

// What Pi's native queue holds: handed messages the session has not yet recorded.
const queued = (sent: ReturnType<typeof fakePi>["sent"], entries: unknown[]) => sent
  .filter(entry => !entries.some(held => (held as { details?: { id?: string } }).details?.id === entry.message.details.id))
  .map(entry => ({ role: "custom", customType: "pi-fabric-agent-message", details: entry.message.details }));

const mainFixture = (root: string, journal: string, entries: unknown[], state: { idle: boolean }) => {
  const fake = fakePi();
  const ctx = mainContext(entries, state);
  const main = new MainAgentController(fake.pi, MAIN, true, root, "main");
  mains.push(main);
  main.attachFollowUpDrain(ctx, 120_000, journal);
  const participants = {
    get: () => undefined, scheduleRefresh: () => undefined, writeStalled: () => false, lastKnown: () => undefined,
  };
  const unused = () => { throw new Error("Unknown Fabric agent"); };
  const router = new AgentMessageRouter(
    { status: unused, steer: unused, followUp: unused, stop: unused } as never,
    { status: () => { throw new Error("Unknown Fabric actor"); } } as never,
    main, participants as never, undefined, binding => binding,
  );
  const handler = vi.fn<FabricControlHandler>((cmd, from, signal, verification) =>
    router.acceptControl(cmd, from, signal, verification));
  return { ...fake, ctx, main, handler };
};

describe("expired durable followUp to Main", () => {
  it("is admitted deferred into Main's journal, delivered once at the next native boundary, never twice across a duplicate or restart", async () => {
    const root = tempRoot("fabric-expired-followup-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const journal = path.join(root, "main-followups", "main.json");
    const entries: unknown[] = [];
    const state = { idle: false };                          // Main is busy in a run
    const first = mainFixture(root, journal, entries, state);
    const owner = plane(mesh); owner.start(first.handler);

    const now = Date.now();
    const sender = identity("session:sender");
    const publish = () => mesh.publish({ topic: "fabric.control.command", kind: "followUp", from: sender, to: MAIN,
      data: command("followUp", "fu:expired:1", sender.id, now - 10_000, now - 1_000) });
    await publish();
    const ack = await awaitAck(mesh, "fu:expired:1");
    expect(ack).toMatchObject({ accepted: true, deferred: true });
    expect(ack.notRun).toBeUndefined();
    expect(first.handler).toHaveBeenCalledOnce();
    // Durable: in Main's journal under its commandId. It is either still held by Fabric or handed
    // to Pi's native queue passively (next turn); it never wakes or steers the busy run.
    expect(fs.readFileSync(journal, "utf8")).toContain("fu:expired:1");
    expect(first.sent.length).toBeLessThanOrEqual(1);
    for (const entry of first.sent) {
      expect(entry.options.triggerTurn === false || entry.options.deliverAs === "nextTurn").toBe(true);
    }

    // By the next native boundary it has been read exactly once, and the journal still keeps it
    // until the session holds it.
    first.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: queued(first.sent, entries) } }, first.ctx);
    expect(deliveryIds(first.sent)).toEqual(["fu:expired:1"]);
    expect(fs.readFileSync(journal, "utf8")).toContain("fu:expired:1");
    expect(first.sent[0]!.message.content).toContain("followUp fu:expired:1");

    // Same commandId again: answered from the claim; Main never sees it twice.
    await publish();
    await awaitAck(mesh, "fu:expired:1", 2);
    first.emit("turn_end", { message: { role: "assistant", stopReason: "toolUse" } }, first.ctx);
    first.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: queued(first.sent, entries) } }, first.ctx);
    expect(deliveryIds(first.sent)).toEqual(["fu:expired:1"]);
    expect(first.handler).toHaveBeenCalledOnce();

    // Session holds it; restart both the owner plane and Main over the same journal and mesh.
    entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: first.sent[0]!.message.details });
    first.emit("agent_settled", { outcome: "completed" }, first.ctx);
    await owner.close();
    first.main.closeFollowUpDrain();
    const second = mainFixture(root, journal, entries, state);
    const restarted = plane(mesh); restarted.start(second.handler);
    const before = acks(mesh, "fu:expired:1").length;
    await publish();
    await sleep(400);                                     // replay of the log plus the duplicate
    second.emit("turn_end", { message: { role: "assistant", stopReason: "toolUse" } }, second.ctx);
    second.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } }, second.ctx);
    expect(deliveryIds(second.sent)).toEqual([]);
    expect(second.handler).not.toHaveBeenCalled();
    for (const data of acks(mesh, "fu:expired:1").slice(before)) expect(data.accepted).toBe(true);
  }, 30_000);

  it("to an idle Main is admitted deferred without starting a turn", async () => {
    const root = tempRoot("fabric-expired-followup-idle-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const journal = path.join(root, "main-followups", "main.json");
    const state = { idle: true };
    const main = mainFixture(root, journal, [], state);
    plane(mesh).start(main.handler);
    const now = Date.now();
    const sender = identity("session:sender");
    await mesh.publish({ topic: "fabric.control.command", kind: "followUp", from: sender, to: MAIN,
      data: command("followUp", "fu:idle:1", sender.id, now - 10_000, now - 1_000) });
    const ack = await awaitAck(mesh, "fu:idle:1");
    expect(ack).toMatchObject({ accepted: true, deferred: true });
    expect(ack.triggered).not.toBe(true);
    expect(main.sent.length).toBeLessThanOrEqual(1);
    for (const entry of main.sent) {
      expect(entry.options.triggerTurn === false || entry.options.deliverAs === "nextTurn").toBe(true);
    }
    expect(deliveryIds(main.sent).filter(id => id === "fu:idle:1").length).toBeLessThanOrEqual(1);
  }, 30_000);
});

describe("expiry at the claim and sender-observation fences", () => {
  it.each(["followUp", "steer"] as const)("rechecks %s after a claim waits past the deadline", async operation => {
    const root = tempRoot("fabric-expired-claim-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const at = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(at);
    const put = MeshStore.prototype.put;
    vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
      const result = await put.call(this, input);
      // Deterministic process/lock pause between durable local admission and handler execution.
      if (this.root.includes("control-seen") && input.ifVersion === 0) clock.mockReturnValue(at + 6_000);
      return result;
    });
    const handler = vi.fn<FabricControlHandler>((cmd, _from, signal) => {
      expect(signal.aborted).toBe(false);
      if (operation === "followUp") expect(cmd.deferred).toBe(true);
      return { accepted: true, messageId: cmd.commandId };
    });
    plane(mesh).start(handler);
    await mesh.publish({ topic: "fabric.control.command", kind: operation, from: identity("session:sender"), to: MAIN,
      data: command(operation, "claim:late", "session:sender", at, at + 5_000) });
    const ack = await awaitAck(mesh, "claim:late");
    if (operation === "followUp") {
      expect(ack).toMatchObject({ accepted: true, deferred: true });
      expect(handler).toHaveBeenCalledOnce();
    } else {
      expect(ack).toMatchObject({ accepted: false, error: EXPIRED, notRun: true });
      expect(handler).not.toHaveBeenCalled();
    }
  });

  it("leaves an unclaimed followUp durable after the sender stops waiting and admits it on owner resume", async () => {
    const root = tempRoot("fabric-followup-observation-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const main = mainFixture(root, path.join(root, "main-followups", "main.json"), [], { idle: false });
    const owner = plane(mesh, 100);
    const senderId = "session:sender";
    const sender = new FabricControlPlane(mesh, identity(senderId), { enabled: true, hostId: senderId,
      pollMs: 20, acknowledgementTimeoutMs: 100 });
    planes.push(sender); sender.start(() => ({ accepted: false }));
    const failure = await sender.request(MAIN, MAIN, "followUp", { message: "kept after timeout", ownerIncarnation: owner.incarnation }, MAIN)
      .then(() => undefined, (error: Error) => error);
    expect(failure?.message).toContain("the outcome is unknown and it may still be delivered");
    expect(failure).not.toHaveProperty("notDelivered");
    const event = mesh.read({ topic: "fabric.control.command", limit: 100 }).find(event => event.kind === "followUp")!;
    const commandId = (event.data as { commandId: string }).commandId;
    const key = "topology/control-seen/" + createHash("sha256").update(`${MAIN}\0${commandId}`).digest("hex");
    expect(mesh.get(key, { fresh: true })).toBeUndefined(); // no sender-created terminal tombstone
    owner.start(main.handler);
    expect(await awaitAck(mesh, commandId)).toMatchObject({ accepted: true, deferred: true });
    expect(deliveryIds(main.sent)).toEqual([commandId]);
    expect(main.sent[0]!.options).toEqual({ deliverAs: "nextTurn", triggerTurn: false });
    await mesh.publish({ topic: event.topic, kind: event.kind, from: event.from, to: MAIN, data: event.data });
    await awaitAck(mesh, commandId, 2);
    expect(main.handler).toHaveBeenCalledOnce();
    expect(deliveryIds(main.sent)).toEqual([commandId]);
  });

  it("does not admit a payload-forged deferred flag on a fresh followUp", async () => {
    const root = tempRoot("fabric-followup-forged-deferral-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const main = mainFixture(root, path.join(root, "main-followups", "main.json"), [], { idle: true });
    plane(mesh).start(main.handler);
    const at = Date.now();
    await mesh.publish({ topic: "fabric.control.command", kind: "followUp", from: identity("session:sender"), to: MAIN,
      data: command("followUp", "deferral:forged", "session:sender", at, at + 5_000, { deferred: true }) });
    const ack = await awaitAck(mesh, "deferral:forged");
    expect(ack).toMatchObject({ accepted: true, triggered: true });
    expect(ack).not.toHaveProperty("deferred");
    expect(main.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
  });
});

describe("expired live-only steer stays refused", () => {
  it.each([
    ["ordinary", {}],
    ["interrupt priority", { priority: "interrupt" }],
  ] as const)("%s steer past its deadline is notRun and its handler never runs", async (_label, extra) => {
    const root = tempRoot("fabric-expired-steer-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const handler = vi.fn<FabricControlHandler>(() => ({ accepted: true }));
    plane(mesh).start(handler);
    const now = Date.now();
    const sender = identity("session:sender");
    const commandId = `steer:expired:${_label}`;
    await mesh.publish({ topic: "fabric.control.command", kind: "steer", from: sender, to: MAIN,
      data: command("steer", commandId, sender.id, now - 10_000, now - 1_000, extra) });
    const ack = await awaitAck(mesh, commandId);
    expect(ack).toMatchObject({ accepted: false, error: EXPIRED, notRun: true });
    expect(ack.deferred).toBeUndefined();
    expect(handler).not.toHaveBeenCalled();
  }, 30_000);
});

it("refuses an expired cancellation but still applies an in-window cancellation from the same sender", async () => {
  const root = tempRoot("fabric-cancel-expiry-");
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
  let active: AbortSignal | undefined;
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const handler = vi.fn<FabricControlHandler>(async (_cmd, _from, signal) => {
    active = signal;
    await gate;
    return { accepted: true };
  });
  plane(mesh, 5_000).start(handler);
  const at = Date.now();
  const sender = identity("session:sender");
  try {
    await mesh.publish({ topic: "fabric.control.command", kind: "ask", from: sender, to: MAIN,
      data: command("ask", "ask:pending", sender.id, at, at + 60_000) });
    await vi.waitFor(() => expect(active).toBeDefined());
    await mesh.publish({ topic: "fabric.control.command", kind: "cancel", from: sender, to: MAIN,
      data: command("cancel", "cancel:expired", sender.id, at - 10_000, at - 1_000, { cancelCommandId: "ask:pending" }) });
    expect(await awaitAck(mesh, "cancel:expired")).toMatchObject({ accepted: false, error: EXPIRED, notRun: true });
    expect(active!.aborted).toBe(false);
    await mesh.publish({ topic: "fabric.control.command", kind: "cancel", from: sender, to: MAIN,
      data: command("cancel", "cancel:live", sender.id, at, at + 5_000, { cancelCommandId: "ask:pending" }) });
    await vi.waitFor(() => expect(active!.aborted).toBe(true));
    expect(handler).toHaveBeenCalledOnce();
  } finally { release(); }
});

describe("bridge-verified admission clock", () => {
  const remote = "session:remote-sender";
  const bridgeData = { bridge: { from: "remote-hub", id: "remote-event" } };

  it.each([
    ["an old", -60 * 60 * 1_000],
    ["a future", 60 * 60 * 1_000],
  ] as const)("accepts a steer with %s remote requestedAt within its TTL since the local createdAt", async (_label, skew) => {
    const root = tempRoot("fabric-bridge-clock-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const handler = vi.fn<FabricControlHandler>(() => ({ accepted: true, messageId: "ok" }));
    plane(mesh, 5_000).start(handler);
    const requestedAt = Date.now() + skew;
    const commandId = `bridge:skew:${skew}`;
    const event = await mesh.publish({ topic: "fabric.control.command", kind: "steer", from: bridged(remote), to: MAIN,
      data: command("steer", commandId, remote, requestedAt, requestedAt + 5_000, bridgeData) });
    expect(event.verification).toBe("bridge");
    const ack = await awaitAck(mesh, commandId);
    expect(ack).toMatchObject({ accepted: true, messageId: "ok" });
    expect(handler).toHaveBeenCalledOnce();
    expect(handler.mock.calls[0]![3]).toBe("bridge");
  }, 30_000);

  it("refuses a bridged steer that waited locally past its TTL, though the remote clock says it is live", async () => {
    const root = tempRoot("fabric-bridge-waited-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const handler = vi.fn<FabricControlHandler>(() => ({ accepted: true }));
    const owner = plane(mesh, 5_000);
    const requestedAt = Date.now() + 2_000;                // the remote clock runs 2 s ahead
    const event = await mesh.publish({ topic: "fabric.control.command", kind: "steer", from: bridged(remote), to: MAIN,
      data: command("steer", "bridge:waited", remote, requestedAt, requestedAt + 300, bridgeData) });
    expect(event.verification).toBe("bridge");
    await sleep(1_000);                                    // local wait: 1 s > 300 ms TTL
    owner.start(handler);
    const ack = await awaitAck(mesh, "bridge:waited");
    expect(ack).toMatchObject({ accepted: false, error: EXPIRED, notRun: true });
    expect(handler).not.toHaveBeenCalled();
  }, 30_000);

  it("ignores a forged or unverified bridge marker: the remote requestedAt clock still governs", async () => {
    const root = tempRoot("fabric-bridge-forged-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const handler = vi.fn<FabricControlHandler>(() => ({ accepted: true }));
    plane(mesh, 5_000).start(handler);
    const sender = identity("session:forger");
    const old = Date.now() - 60 * 60 * 1_000;
    const forged = await mesh.publish({ topic: "fabric.control.command", kind: "steer", from: sender, to: MAIN,
      data: command("steer", "forged:old", sender.id, old, old + 5_000, { ...bridgeData, verification: "bridge" }) });
    expect(forged.verification).not.toBe("bridge");
    const ack = await awaitAck(mesh, "forged:old");
    expect(ack).toMatchObject({ accepted: false, error: EXPIRED, notRun: true });

    // A native mesh steer whose own clock expired is refused too, never re-clocked locally.
    const recent = Date.now() - 10_000;
    const native = await mesh.publish({ topic: "fabric.control.command", kind: "steer", from: sender, to: MAIN,
      data: command("steer", "native:expired", sender.id, recent, recent + 5_000) });
    expect(native.verification).toBe("mesh");
    expect(await awaitAck(mesh, "native:expired")).toMatchObject({ accepted: false, error: EXPIRED, notRun: true });
    expect(handler).not.toHaveBeenCalled();
  }, 30_000);
});
