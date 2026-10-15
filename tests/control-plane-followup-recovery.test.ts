import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import {
  CONTROL_STALE_INCARNATION, FabricControlPlane, type FabricControlHandler, type FabricControlPlaneOptions,
} from "../src/topology/control-plane.js";

// Crash recovery of an expired Main followUp between the control claim and Main's journal.
// A first owner runs a REAL claim sequence; a MeshStore write hook plus the consumption gate
// "kill" it at an exact durable boundary (no write after the boundary commits). A second owner
// with a NEW epoch and the SAME Main session (journal + session entries) restarts over the mesh.
// Only an explicit durable Main opt-in, the original event id and the original owner epoch may
// replay an outcome-less claim; everything else stays indeterminate and never reruns a handler.

const MAIN = "session:main";
const SENDER = "session:sender";
const EPOCH_1 = "owner-epoch-1";
const EPOCH_2 = "owner-epoch-2";
const SEEN_PREFIX = "topology/control-seen/";
const INDETERMINATE = "Fabric control outcome is indeterminate after owner restart";
const roots: string[] = [];
const planes: FabricControlPlane[] = [];
const mains: MainAgentController[] = [];
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const identity = (id: string, kind: MeshIdentity["kind"] = "main"): MeshIdentity => ({ id, name: id, kind, sessionId: id });

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

const seenKey = (commandId: string) => SEEN_PREFIX + createHash("sha256").update(`${MAIN}\0${commandId}`).digest("hex");
const localSeen = (mesh: MeshStore) => new MeshStore(
  path.join(mesh.root, "control-seen", createHash("sha256").update(MAIN).digest("hex").slice(0, 32)),
  mesh.maxEventBytes, mesh.maxReadEvents, { lockProtocol: mesh.lockProtocol, stateBackend: "file" },
);

const acks = (mesh: MeshStore, commandId: string) => mesh.read({ topic: "fabric.control.ack", limit: 1_000 })
  .map(event => event.data as Record<string, unknown> | undefined)
  .filter((data): data is Record<string, unknown> => data?.commandId === commandId);
const awaitAck = async (mesh: MeshStore, commandId: string, count = 1) => {
  await vi.waitFor(() => expect(acks(mesh, commandId).length).toBeGreaterThanOrEqual(count), { timeout: 8_000, interval: 20 });
  return acks(mesh, commandId).at(-1)!;
};

// durableFollowUpTargetId is the parent's explicit opt-in (absent from the baseline type).
type RecoveryOptions = Partial<FabricControlPlaneOptions> & { durableFollowUpTargetId?: string };
const plane = (mesh: MeshStore, options: RecoveryOptions = {}) => {
  const value = new FabricControlPlane(mesh, identity(MAIN), {
    enabled: true, hostId: MAIN, pollMs: 20, acknowledgementTimeoutMs: 1_000, onUnfencedSenderWarning: () => undefined,
    ...options,
  } as FabricControlPlaneOptions);
  planes.push(value);
  return value;
};

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
const queued = (sent: ReturnType<typeof fakePi>["sent"], entries: unknown[]) => sent
  .filter(entry => !entries.some(held => (held as { details?: { id?: string } }).details?.id === entry.message.details.id))
  .map(entry => ({ role: "custom", customType: "pi-fabric-agent-message", details: entry.message.details }));
const journalItems = (journal: string, commandId: string): Array<Record<string, unknown>> => {
  if (!fs.existsSync(journal)) return [];
  const parsed = JSON.parse(fs.readFileSync(journal, "utf8")) as { items?: Array<Record<string, unknown>> };
  return (parsed.items ?? []).filter(item => item.deliveryId === commandId);
};

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
  const crashAfter = { armed: false, crash: () => undefined as void };
  const handler = vi.fn<FabricControlHandler>(async (cmd, from, signal, verification) => {
    const result = await router.acceptControl(cmd, from, signal, verification);
    if (crashAfter.armed) { crashAfter.armed = false; crashAfter.crash(); }
    return result;
  });
  return { ...fake, ctx, main, handler, crashAfter };
};

type Boundary = "shared" | "local" | "journal";

// A native crash: after the boundary commits, no later write on this mesh/claim store lands.
const installCrashHook = (mesh: MeshStore) => {
  const state = { armed: undefined as Exclude<Boundary, "journal"> | undefined, crashed: false, alive: true, onCrash: () => undefined as void };
  const crash = () => { state.armed = undefined; state.crashed = true; state.alive = false; state.onCrash(); };
  const blocked = (store: MeshStore) => state.crashed && store.root.startsWith(mesh.root);
  const check = (store: MeshStore) => {
    if (!state.armed) return;
    const claims = () => store.listAll(SEEN_PREFIX).length > 0;
    if (state.armed === "shared" && store.root === mesh.root && claims()) crash();
    else if (state.armed === "local" && store.root.includes(`${path.sep}control-seen${path.sep}`) && claims()) crash();
  };
  const put = MeshStore.prototype.put;
  const writeBatch = MeshStore.prototype.writeBatch;
  vi.spyOn(MeshStore.prototype, "put").mockImplementation(async function (this: MeshStore, input) {
    if (blocked(this)) throw new Error("simulated owner process crash: write never happened");
    const result = await put.call(this, input);
    check(this);
    return result;
  });
  vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function (this: MeshStore, input) {
    if (blocked(this)) throw new Error("simulated owner process crash: write never happened");
    const result = await writeBatch.call(this, input);
    check(this);
    return result;
  });
  return { state, crash };
};

interface CrashScenario {
  root: string; mesh: MeshStore; journal: string; entries: unknown[]; state: { idle: boolean };
  first: ReturnType<typeof mainFixture>; commandId: string; eventId: string; deadlineAt: number;
  request: Promise<unknown>; sender: FabricControlPlane;
}

/** Real sender request to the first owner (epoch 1); the owner dies at `boundary`. */
const crashFirstOwner = async (prefix: string, boundary: Boundary, targetId = MAIN, waitForExpiry = true,
  verification?: "bridge"): Promise<CrashScenario> => {
  const root = tempRoot(prefix);
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
  const journal = path.join(root, "main-followups", "main.json");
  const entries: unknown[] = [];
  const state = { idle: false };                            // Main is busy in a run
  const first = mainFixture(root, journal, entries, state);
  const hook = installCrashHook(mesh);
  const owner = plane(mesh, { ownerIncarnation: EPOCH_1, durableFollowUpTargetId: MAIN, canConsumeMesh: () => hook.state.alive });
  hook.state.onCrash = () => owner.pause();
  if (boundary === "journal") { first.crashAfter.armed = true; first.crashAfter.crash = hook.crash; }
  else hook.state.armed = boundary;
  owner.start(first.handler);
  const sender = new FabricControlPlane(mesh, { ...identity(SENDER), ...(verification ? { verified: verification } : {}) },
    { enabled: true, hostId: SENDER, pollMs: 20, acknowledgementTimeoutMs: 1_000 });
  planes.push(sender); sender.start(() => ({ accepted: false }));
  const request = sender.request(MAIN, targetId, "followUp", { message: `recover ${boundary}`, ownerIncarnation: EPOCH_1 }, MAIN,
    { timeoutMs: 1_500 });
  void request.catch(() => undefined);
  await vi.waitFor(() => expect(hook.state.crashed).toBe(true), { timeout: 8_000, interval: 10 });
  await owner.close();                                     // the dead process: nothing more is written
  first.main.closeFollowUpDrain();
  hook.state.crashed = false;
  const event = mesh.read({ topic: "fabric.control.command", limit: 100 }).find(e => e.kind === "followUp")!;
  const data = event.data as { commandId: string; deadlineAt: number };
  expect(acks(mesh, data.commandId)).toEqual([]);          // no control outcome reached the sender
  // Exact durable boundary.
  const shared = mesh.get(seenKey(data.commandId), { fresh: true })?.value as Record<string, unknown> | undefined;
  const local = localSeen(mesh).get(seenKey(data.commandId), { fresh: true })?.value as Record<string, unknown> | undefined;
  expect(shared?.ownerIncarnation).toBe(EPOCH_1);
  if (boundary === "shared") expect(local).toBeUndefined();
  else { expect(local?.ownerIncarnation).toBe(EPOCH_1); expect(local?.acceptance).toBeUndefined(); }
  if (targetId === MAIN) expect(journalItems(journal, data.commandId)).toHaveLength(boundary === "journal" ? 1 : 0);
  expect(first.handler).toHaveBeenCalledTimes(boundary === "journal" ? 1 : 0);
  // Restart only once the followUp has expired; the sender is still inside its ACK grace.
  if (waitForExpiry) await vi.waitFor(() => expect(Date.now()).toBeGreaterThan(data.deadlineAt + 50), { timeout: 4_000, interval: 20 });
  return { root, mesh, journal, entries, state, first, commandId: data.commandId, eventId: event.id, deadlineAt: data.deadlineAt, request, sender };
};

describe("expired Main followUp recovery after an owner crash between claim and Main journal", () => {
  it.each([
    ["after the shared claim, before the local claim and Main journal", "shared"],
    ["after the local claim, before the Main journal", "local"],
    ["after the Main journal, before the control outcome", "journal"],
  ] as const)("recovers %s: deferred ACK under the original epoch, one Main admission and one native read", async (_label, boundary) => {
    const s = await crashFirstOwner(`fabric-fu-recovery-${boundary}-`, boundary);
    const second = mainFixture(s.root, s.journal, s.entries, s.state);   // same Main session and journal
    const restarted = plane(s.mesh, { ownerIncarnation: EPOCH_2, durableFollowUpTargetId: MAIN });
    restarted.start(second.handler);

    // The waiting sender (fenced on epoch 1) accepts the recovered outcome.
    await expect(s.request).resolves.toMatchObject({ acknowledged: true, deferred: true, routed: "mesh" });
    const ack = await awaitAck(s.mesh, s.commandId);
    expect(ack).toMatchObject({ accepted: true, deferred: true, ownerIncarnation: EPOCH_1 });
    expect(ack.error).toBeUndefined();

    // The real router/Main handler ran for the ORIGINAL event; Main dedups by the same commandId.
    expect(second.handler).toHaveBeenCalledOnce();
    const recovered = second.handler.mock.calls[0]![0];
    expect(recovered).toMatchObject({ commandId: s.commandId, eventId: s.eventId, deferred: true, operation: "followUp" });
    expect(second.handler.mock.calls[0]![1].id).toBe(SENDER);
    expect(journalItems(s.journal, s.commandId)).toHaveLength(1);
    expect(fs.readFileSync(s.journal, "utf8")).toContain(s.commandId);

    // By the next native boundary Main reads it exactly once, attributed to the original sender.
    second.emit("turn_end", { message: { role: "assistant", stopReason: "toolUse" } }, second.ctx);
    second.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: queued(second.sent, s.entries) } }, second.ctx);
    expect(deliveryIds(second.sent)).toEqual([s.commandId]);
    const delivered = second.sent[0]!;
    expect(delivered.message.details.from).toMatchObject({ id: SENDER });
    expect(delivered.message.content).toContain(`recover ${boundary}`);
    if (boundary !== "journal") expect(delivered.options).toEqual({ deliverAs: "nextTurn", triggerTurn: false });
    expect(deliveryIds(s.first.sent).filter(id => id === s.commandId).length).toBeLessThanOrEqual(1);

    // A same-commandId duplicate is answered from the saved outcome: no second handler/Main read.
    const original = s.mesh.read({ topic: "fabric.control.command", limit: 100 }).find(e => e.id === s.eventId)!;
    await s.mesh.publish({ topic: original.topic, kind: original.kind, from: original.from, to: MAIN, data: original.data });
    const again = await awaitAck(s.mesh, s.commandId, 2);
    expect(again).toMatchObject({ accepted: true, ownerIncarnation: EPOCH_1 });
    second.emit("turn_end", { message: { role: "assistant", stopReason: "toolUse" } }, second.ctx);
    second.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: queued(second.sent, s.entries) } }, second.ctx);
    expect(second.handler).toHaveBeenCalledOnce();
    expect(deliveryIds(second.sent)).toEqual([s.commandId]);
  }, 30_000);
});

it.each([undefined, "bridge"] as const)("recovers an admitted Main followUp before expiry too, without consuming its only replay opportunity (%s)", async verification => {
  const s = await crashFirstOwner("fabric-fu-early-recovery-", "local", MAIN, false, verification);
  const second = mainFixture(s.root, s.journal, s.entries, s.state);
  expect(Date.now()).toBeLessThan(s.deadlineAt);
  plane(s.mesh, { ownerIncarnation: EPOCH_2, durableFollowUpTargetId: MAIN }).start(second.handler);
  await expect(s.request).resolves.toMatchObject({ acknowledged: true, deferred: true });
  expect(await awaitAck(s.mesh, s.commandId)).toMatchObject({ accepted: true, ownerIncarnation: EPOCH_1, deferred: true });
  expect(second.handler).toHaveBeenCalledOnce();
  expect(second.handler.mock.calls[0]![3]).toBe(verification ?? "mesh");
  expect(second.sent[0]!.options).toEqual({ deliverAs: "nextTurn", triggerTurn: false });
  expect(second.sent[0]!.message.details.from).toMatchObject({ id: SENDER });
  expect(journalItems(s.journal, s.commandId)).toHaveLength(1);
  expect(deliveryIds(second.sent)).toEqual([s.commandId]);
}, 30_000);

describe("counterexamples: outcome-less claims stay indeterminate and never rerun", () => {
  it.each([
    ["without the explicit durable Main opt-in", MAIN, undefined],
    ["for a generic non-idempotent target even when Main is opted in", "actor:worker", MAIN],
  ] as const)("%s", async (_label, targetId, durableFollowUpTargetId) => {
    const s = await crashFirstOwner("fabric-fu-recovery-optout-", "local", targetId);
    const second = mainFixture(s.root, s.journal, s.entries, s.state);
    const generic = vi.fn<FabricControlHandler>(() => ({ accepted: true, messageId: "side-effect" }));
    const handler = vi.fn<FabricControlHandler>((cmd, from, signal, verification) =>
      cmd.targetId === MAIN ? second.handler(cmd, from, signal, verification) : generic(cmd, from, signal, verification));
    plane(s.mesh, { ownerIncarnation: EPOCH_2, ...(durableFollowUpTargetId ? { durableFollowUpTargetId } : {}) }).start(handler);
    await expect(s.request).rejects.toThrow(/indeterminate/);
    expect(await awaitAck(s.mesh, s.commandId)).toMatchObject({ accepted: false, error: INDETERMINATE, ownerIncarnation: EPOCH_1 });
    await sleep(200);
    expect(handler).not.toHaveBeenCalled();
    expect(generic).not.toHaveBeenCalled();
    expect(second.handler).not.toHaveBeenCalled();
    expect(deliveryIds(second.sent)).toEqual([]);
    expect(journalItems(s.journal, s.commandId)).toEqual([]);
  }, 30_000);

  it("a stale-owner expired followUp without a prior claim stays STALE_INCARNATION and never runs", async () => {
    const root = tempRoot("fabric-fu-recovery-stale-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const journal = path.join(root, "main-followups", "main.json");
    const main = mainFixture(root, journal, [], { idle: false });
    plane(mesh, { ownerIncarnation: EPOCH_2, durableFollowUpTargetId: MAIN }).start(main.handler);
    const now = Date.now();
    await mesh.publish({ topic: "fabric.control.command", kind: "followUp", from: identity(SENDER), to: MAIN, data: {
      version: 1, commandId: "fu:stale", targetId: MAIN, operation: "followUp", replyTo: SENDER,
      requestedAt: now - 10_000, deadlineAt: now - 1_000, message: "stale", ownerIncarnation: EPOCH_1,
    } });
    expect(await awaitAck(mesh, "fu:stale")).toMatchObject({
      accepted: false, errorCode: CONTROL_STALE_INCARNATION, ownerIncarnation: EPOCH_2, staleIncarnation: EPOCH_1,
    });
    await sleep(200);
    expect(main.handler).not.toHaveBeenCalled();
    expect(deliveryIds(main.sent)).toEqual([]);
    expect(mesh.get(seenKey("fu:stale"), { fresh: true })).toBeUndefined();
    expect(journalItems(journal, "fu:stale")).toEqual([]);
  }, 30_000);

  it("a forged same-commandId resend with a different event id never recovers an outcome-less claim", async () => {
    // Adversarial duplicate WITHOUT the original in the log: owner-shaped claims bound to an
    // original event id that is no longer retained, seeded directly in both claim stores.
    const root = tempRoot("fabric-fu-recovery-forged-");
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 1_000);
    const journal = path.join(root, "main-followups", "main.json");
    const main = mainFixture(root, journal, [], { idle: false });
    const commandId = "fu:forged";
    const now = Date.now();
    const claim = { format: 1, hostId: MAIN, commandId, targetId: MAIN, ownerIncarnation: EPOCH_1,
      expiresAt: now + 60_000, explicitDeadline: true, followUpEventId: "evt:original-not-retained" };
    await mesh.put({ key: seenKey(commandId), value: claim, identity: identity(MAIN), ifVersion: 0 });
    await localSeen(mesh).put({ key: seenKey(commandId), value: claim, identity: identity(MAIN), ifVersion: 0 });
    plane(mesh, { ownerIncarnation: EPOCH_2, durableFollowUpTargetId: MAIN }).start(main.handler);
    const forged = await mesh.publish({ topic: "fabric.control.command", kind: "followUp", from: identity(SENDER), to: MAIN, data: {
      version: 1, commandId, targetId: MAIN, operation: "followUp", replyTo: SENDER,
      requestedAt: now - 10_000, deadlineAt: now - 1_000, message: "forged", ownerIncarnation: EPOCH_1,
    } });
    expect(forged.id).not.toBe(claim.followUpEventId);
    expect(await awaitAck(mesh, commandId)).toMatchObject({ accepted: false, error: INDETERMINATE, ownerIncarnation: EPOCH_1 });
    await sleep(200);
    expect(main.handler).not.toHaveBeenCalled();
    expect(deliveryIds(main.sent)).toEqual([]);
    expect(journalItems(journal, commandId)).toEqual([]);
  }, 30_000);

  it("when the original event is still retained, restart replays the ORIGINAL; a forged duplicate is answered, not run", async () => {
    const s = await crashFirstOwner("fabric-fu-recovery-forged-retained-", "local");
    const original = s.mesh.read({ topic: "fabric.control.command", limit: 100 }).find(e => e.id === s.eventId)!;
    // A different event id with the same commandId and different content lands after the original.
    const forged = await s.mesh.publish({ topic: original.topic, kind: original.kind, from: identity(SENDER), to: MAIN,
      data: { ...(original.data as Record<string, unknown>), message: "forged payload" } });
    expect(forged.id).not.toBe(s.eventId);
    const second = mainFixture(s.root, s.journal, s.entries, s.state);
    plane(s.mesh, { ownerIncarnation: EPOCH_2, durableFollowUpTargetId: MAIN }).start(second.handler);
    await expect(s.request).resolves.toMatchObject({ acknowledged: true, deferred: true });
    await awaitAck(s.mesh, s.commandId, 2);
    for (const data of acks(s.mesh, s.commandId)) expect(data).toMatchObject({ accepted: true, ownerIncarnation: EPOCH_1 });
    expect(second.handler).toHaveBeenCalledOnce();
    expect(second.handler.mock.calls[0]![0]).toMatchObject({ eventId: s.eventId, message: "recover local" });
    expect(deliveryIds(second.sent)).toEqual([s.commandId]);
    expect(second.sent[0]!.message.content).not.toContain("forged payload");
    expect(journalItems(s.journal, s.commandId)).toHaveLength(1);
  }, 30_000);
});
