import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MainAgentController, type FabricMainAgentDeliveryRequest } from "../src/main-agent.js";
import { closeMainInbox, reopenMainInbox } from "../src/main-drain/gate.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { registerMainInbox, reconcileNativeMainInboxCustody, recordMainSuccessor, resolveNativeMainSuccessor } from "../src/topology/stall-alarms.js";
import { FileControlReader } from "../src/transport/file.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import type { NativeRecoveryBinding, NativeRecoveryCustodyReader, NativeRecoveryParkIntent, NativeRecoveryTransfer } from "../src/core/session-receipts.js";
import { RootInbox } from "../src/topology/root-inbox.js";
import { adoptRootInboxReceiptIssuer, confirmedRootInboxSession, eventReceipt, eventReceipts,
  registerRootInboxReceipt, retainRootInboxReceiptIssuer, rootInboxMessage, rootInboxReceiptIssuer,
  rootInboxSession, verifyRootInboxReceipt } from "../src/topology/root-inbox-shared.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentHostId, type ResidentHostConfig } from "../src/residency/protocol.js";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const rootDirectory = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-session-namespace-"));
  roots.push(root);
  return root;
};
const actor = { id: "actor:reply", name: "reply", kind: "actor" as const };
const directoryChain = (directory: string): string[] => {
  const chain: string[] = [];
  for (let current = path.resolve(directory); ; current = path.dirname(current)) {
    chain.push(current);
    if (path.dirname(current) === current) return chain;
  }
};
const persist = (manager: SessionManager, details: unknown) => {
  manager.appendCustomMessageEntry("pi-fabric-agent-message", "reply", true, details);
  // Real Pi defers a new session's first write until the first assistant message.
  manager.appendMessage(fauxAssistantMessage("persisted"));
};
const replace = (root: string, manager: SessionManager, details: unknown) => {
  // The test dependency's _rewriteFile is in-place; newer Pi installs a replacement by
  // rename. Have a real SessionManager persist that new inode, then install it identically.
  const replacement = SessionManager.create(root, manager.getSessionDir());
  persist(replacement, details);
  fs.renameSync(replacement.getSessionFile()!, fs.realpathSync(manager.getSessionFile()!));
};
const controller = (root: string, manager: SessionManager) => {
  const handlers = new Map<string, Array<(event: unknown, context: ExtensionContext) => void>>();
  const sent: Array<{ details: Record<string, unknown> }> = [];
  const pi = {
    on: (name: string, handler: (event: unknown, context: ExtensionContext) => void) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
    },
    sendMessage: (message: { details: Record<string, unknown> }) => sent.push(message),
  } as unknown as ExtensionAPI;
  const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: manager } as unknown as ExtensionContext;
  const journal = path.join(root, "mesh", "main-followups", "root.json");
  const main = new MainAgentController(pi, "session:root", true, root, "root");
  main.attachFollowUpDrain(context, 60_000, journal);
  const settle = () => { for (const handler of handlers.get("agent_settled") ?? []) handler({ outcome: "completed" }, context); };
  return { main, sent, journal, settle };
};
const barrierProbe = (file: string, failingPath: string) => {
  const descriptors = new Map<number, string>();
  const events: string[] = [];
  const open = fs.openSync.bind(fs);
  const sync = fs.fsyncSync.bind(fs);
  let failing = true;
  const opened = vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) => {
    const fd = open(target, flags, mode);
    descriptors.set(fd, String(target));
    return fd;
  });
  const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
    const target = descriptors.get(fd)!;
    events.push(target === file ? "session-file" : target);
    if (failing && target === failingPath) throw new Error("session directory barrier unavailable");
    sync(fd); // Session-file sync succeeds; ONLY the required directory barrier fails.
  });
  return { events, recover: () => { failing = false; events.length = 0; }, restore: () => { synced.mockRestore(); opened.mockRestore(); } };
};
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("session durability test timed out");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};
const cases = ["creation", "replacement"].flatMap((operation) => ["leaf", "ancestor"].flatMap((barrier) =>
  ([false, true, "file-chain", "directory-chain"] as const).map((symlink) => ({
    operation, barrier, symlink, namespace: typeof symlink === "string" ? symlink : symlink ? "symlinked" : "ordinary",
  })),
));
const session = (root: string, symlink: boolean | "file-chain" | "directory-chain") => {
  const directory = path.join(root, "physical", "sessions");
  if (!symlink) return { manager: SessionManager.create(root, directory), directory };
  const aliases = path.join(root, "aliases", "sessions");
  fs.mkdirSync(directory, { recursive: true });
  fs.mkdirSync(aliases, { recursive: true });
  const file = path.join(aliases, "session.jsonl");
  // A freshly created physical file is initialized through the supported explicit
  // symlink path by the real SessionManager. Its namespace has not been confirmed.
  const target = path.join(directory, "session.jsonl");
  fs.writeFileSync(target, "");
  if (typeof symlink === "string") {
    const intermediate = path.join(root, "intermediate-tree", "links");
    fs.mkdirSync(intermediate, { recursive: true });
    const link = path.join(intermediate, "current");
    if (symlink === "file-chain") {
      fs.symlinkSync(path.relative(intermediate, target), link);
      fs.symlinkSync(path.relative(aliases, link), file);
    } else {
      fs.symlinkSync(path.relative(intermediate, directory), link, "dir");
      const alias = path.join(aliases, "current");
      fs.symlinkSync(path.relative(aliases, link), alias, "dir");
      return { manager: SessionManager.open(path.join(alias, "session.jsonl"), aliases, root), directory: intermediate, intermediate };
    }
    return { manager: SessionManager.open(file, aliases, root), directory: intermediate, intermediate };
  }
  fs.symlinkSync(target, file);
  return { manager: SessionManager.open(file, aliases, root), directory, intermediate: undefined };
};
const receiptChain = (manager: SessionManager, intermediate?: string) => {
  const file = manager.getSessionFile()!;
  const physical = fs.realpathSync(file);
  const aliases = intermediate ? path.join(path.dirname(path.dirname(intermediate)), "aliases", "sessions") : path.dirname(file);
  return [...new Set([...directoryChain(path.dirname(physical)), ...(intermediate ? directoryChain(intermediate) : []),
    ...(physical === file ? [] : directoryChain(aliases))])];
};

// Keep: native/mesh custody trust boundary and no-drop/no-duplicate data safety (#17478).
describe("native recovery recipient-scoped root inbox handoff", () => {
  const setup = () => {
    const root = rootDirectory();
    const mesh = new MeshStore(path.join(root, "mesh"), 1024 * 1024, 500);
    const identity = (id: string) => ({ id: `session:${id}`, name: id, kind: "main" as const });
    const inbox = (id: string) => new RootInbox(mesh, identity(id), () => [identity(id).id, `${id}-alias`], { steerGraceMs: 0, wakeCooldownMs: 0 });
    const binding = (source = "source", target = "target"): NativeRecoveryBinding => ({
      version: 1, operationId: `${source}-${target}`, preparedDigest: "a".repeat(64), preparedEntryId: `prepared-${source}`,
      source: { sessionId: source, sessionFile: path.join(root, `${source}.jsonl`) },
      target: { sessionId: target, sessionFile: path.join(root, `${target}.jsonl`) }, phase: "prepared",
    });
    const committed = (edge: NativeRecoveryBinding): NativeRecoveryTransfer => ({
      ...edge, phase: "received", receivedEntryId: `received-${edge.target.sessionId}`, occurrences: [],
    });
    const publish = (to = "source", text = "work", data?: unknown) => mesh.publish({
      topic: "fleet.work.done", kind: "done", from: actor, to: `session:${to}`, text, data,
    });
    const intent = (edge: NativeRecoveryBinding): NativeRecoveryParkIntent => ({
      version: 1, operationId: edge.operationId, source: edge.source, target: edge.target,
    });
    return { mesh, inbox, binding, intent, committed, publish };
  };

  it("captures the locator without a digest or PREPARED, grants no successor, and leaves an aborted S ordinary", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    source.start();
    const event = await f.publish();
    const intent = f.intent(f.binding()); // No digest, entry ID or phase exists before the snapshot.
    const handoff = await source.parkForNativeRecovery(intent);
    expect(handoff).toEqual({ version: 1, rootId: source.identity.id, key: source.key, after: 0 });
    expect(f.mesh.get(source.key)!.value).toMatchObject({ nativeRecoveryPark: intent });
    expect((f.mesh.get(source.key)!.value as { nativeRecoveryPark: object }).nativeRecoveryPark).not.toHaveProperty("preparedDigest");
    await expect(target.adoptNativeRecoveryInbox(handoff, () => ({ status: "none" }))).rejects.toThrow();
    // R/N own suspension. On a pre-PREPARED abort they can reattach this same, unsealed reader.
    expect((await source.next(rootInboxSession([]))).events.map(item => item.id)).toEqual([event.id]);
    await source.consume([event.id], eventReceipts(event));
    const future = await f.publish();
    expect((await source.wake(rootInboxSession([]), () => true))?.events.map(item => item.id)).toEqual([future.id]);
    const replacement = { ...intent, operationId: "after-native-abort", target: { ...intent.target, sessionId: "another", sessionFile: path.join(path.dirname(intent.target.sessionFile), "another.jsonl") } };
    await expect(source.parkForNativeRecovery(replacement)).resolves.toMatchObject({ rootId: source.identity.id, key: source.key });
    await Promise.all([source.close(), target.close()]);
  });

  it("keeps S pending/backlog/future events addressed to S, with independent S/T cursors and ACKs", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    source.start();
    const pending = await f.publish();
    expect((await source.next(rootInboxSession([]))).events.map(event => event.id)).toEqual([pending.id]);
    const backlog = await f.publish("source", "backlog");
    const edge = f.binding();
    const handoff = await source.parkForNativeRecovery(edge);
    expect(handoff).toEqual({ version: 1, rootId: "session:source", key: source.key, after: 0 });
    const during = await f.publish("source", "during switch");
    target.start();
    const own = await f.publish("target", "own work");
    await f.publish("unrelated", "not ours");
    await f.mesh.publish({ topic: "fleet.work.done", kind: "done", from: actor, to: "source-alias", text: "not a predecessor alias" });
    const read: NativeRecoveryCustodyReader = scope => {
      expect(scope).toEqual(edge.target);
      return { status: "validated", transfers: [f.committed(edge)] };
    };
    await target.adoptNativeRecoveryInbox(handoff, read);
    const batch = await target.next(rootInboxSession([]));
    expect(batch.events.map(event => event.id)).toEqual([pending.id, backlog.id, during.id, own.id]);
    expect(batch.events.slice(0, 3).every(event => event.to === "session:source" && event.from.id === actor.id)).toBe(true);
    const targetBefore = f.mesh.get(target.key)!.value;
    await target.consume([pending.id, backlog.id, during.id], batch.events.slice(0, 3).flatMap(eventReceipts));
    expect(f.mesh.get(target.key)!.value).toEqual(targetBefore);
    expect((f.mesh.get(source.key)!.value as { pending?: unknown }).pending).toBeUndefined();
    expect(target.acked(own.id, own.sequence)).toBe(false);
    await target.consume([own.id], eventReceipts(own));
    // After PREPARED, R/N keep S suspended; only T reconciles the adopted ledgers.
    expect((await target.next(rootInboxSession([]))).events).toEqual([]);
    const future = await f.publish("source", "future watcher");
    expect((await target.wake(rootInboxSession([]), () => true))?.events.map(event => event.id)).toEqual([future.id]);
    await target.consume([future.id], eventReceipts(future));
    expect((await target.next(rootInboxSession([]))).events).toEqual([]);
    await Promise.all([source.close(), target.close()]);
  });

  it("withholds native IDs and child inbox receipt hashes without ACK/cursor advancement, then retires only consumed outcomes", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    source.start();
    const nativeInbox = await f.publish(), nativeChild = await f.publish(), ordinary = await f.publish();
    await source.next(rootInboxSession([]));
    const edge = f.binding(), transfer = f.committed(edge);
    transfer.occurrences = [
      { ordinal: 0, state: "pending", customMessage: { customType: "pi-fabric-inbox", details: { ids: [nativeInbox.id] } } },
      { ordinal: 1, state: "pending", customMessage: { customType: "pi-fabric-agent-message", details: {
        id: "outer", items: [{ id: "child", inboxReceipts: [eventReceipt(nativeChild.id)] }],
      } } },
    ];
    const handoff = await source.parkForNativeRecovery(edge);
    target.start();
    await target.adoptNativeRecoveryInbox(handoff, () => ({ status: "validated", transfers: [transfer] }));
    expect((await target.next(rootInboxSession([]))).events.map(event => event.id)).toEqual([ordinary.id]);
    await target.consume([ordinary.id], eventReceipts(ordinary));
    expect((await target.next(rootInboxSession([]))).events).toEqual([]);
    const retained = f.mesh.get(source.key)!.value as { after: number; pending: { ids: string[] }; delivered: string[] };
    expect(retained.after).toBe(handoff.after);
    expect(retained.pending.ids).toEqual([nativeInbox.id, nativeChild.id]);
    expect(retained.delivered).not.toContain(eventReceipt(nativeInbox.id));
    expect(retained.delivered).not.toContain(eventReceipt(nativeChild.id));
    expect(await target.wake(rootInboxSession([]), () => true)).toBeUndefined();
    expect((await target.next(rootInboxSession([]), { commitOnly: true })).events).toEqual([]);
    expect((f.mesh.get(source.key)!.value as { after: number }).after).toBe(handoff.after);
    transfer.occurrences = transfer.occurrences.map((occurrence, index) => ({ ...occurrence, state: index ? "handled" as const : "appended" as const }));
    expect((await target.next(rootInboxSession([]))).events).toEqual([]);
    const consumed = f.mesh.get(source.key)!.value as { after: number; pending?: unknown; delivered: string[] };
    expect(consumed.after).toBeGreaterThanOrEqual(ordinary.sequence);
    expect(consumed.pending).toBeUndefined();
    expect(consumed.delivered).toContain(eventReceipt(nativeInbox.id));
    expect(consumed.delivered).toContain(eventReceipt(nativeChild.id));
    await Promise.all([source.close(), target.close()]);
  });

  it("a pending native-only backlog event pins its original cursor until a real canonical receipt exists", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    source.start();
    const event = await f.publish(); // Not admitted to RootInbox.pending before parking.
    const edge = f.binding(), transfer = f.committed(edge);
    transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: { customType: "pi-fabric-inbox", details: { ids: [event.id] } } }];
    const handoff = await source.parkForNativeRecovery(edge);
    target.start();
    await target.adoptNativeRecoveryInbox(handoff, () => ({ status: "validated", transfers: [transfer] }));
    expect((await target.next(rootInboxSession([]))).events).toEqual([]);
    expect((f.mesh.get(source.key)!.value as { after: number }).after).toBe(handoff.after);
    expect(target.acked(event.id, event.sequence)).toBe(false); // T's own later cursor is not S's consumption.
    const manager = SessionManager.create(rootDirectory());
    const issuer = rootInboxReceiptIssuer(manager, manager.getSessionId());
    const message = rootInboxMessage([event]);
    registerRootInboxReceipt(issuer, message.details, message.content);
    manager.appendCustomMessageEntry(message.customType, message.content, true, message.details);
    manager.appendMessage(fauxAssistantMessage("durable"));
    const canonical = confirmedRootInboxSession(manager, (details, carrier, accepted) => verifyRootInboxReceipt(issuer, details, carrier, accepted));
    expect(canonical.delivered?.has(eventReceipt(event.id))).toBe(true);
    expect((await target.next(canonical)).events).toEqual([]);
    expect((f.mesh.get(source.key)!.value as { after: number }).after).toBeGreaterThanOrEqual(event.sequence);
    await Promise.all([source.close(), target.close()]);
  });

  it("lets a real canonical Main delivery receipt retire an already-pending native root-inbox shadow", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    source.start();
    const event = await f.publish("source", "canonical steer", { deliveryId: "same-native-work" });
    await source.next(rootInboxSession([]));
    const edge = f.binding(), handoff = await source.parkForNativeRecovery(edge), transfer = f.committed(edge);
    transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: { customType: "pi-fabric-inbox", details: { ids: [event.id] } } }];
    target.start();
    await target.adoptNativeRecoveryInbox(handoff, () => ({ status: "validated", transfers: [transfer] }));
    const manager = SessionManager.create(rootDirectory());
    persist(manager, { id: "canonical-carrier", from: actor, deliveryId: "same-native-work" });
    const canonical = confirmedRootInboxSession(manager);
    expect(canonical.delivered?.has(eventReceipt(event.id))).toBe(false);
    expect((await target.next(canonical)).events).toEqual([]);
    expect(f.mesh.get(source.key)!.value).toMatchObject({ after: event.sequence, delivered: expect.arrayContaining([eventReceipt(event.id)]) });
    expect((f.mesh.get(source.key)!.value as { pending?: unknown }).pending).toBeUndefined();
    await Promise.all([source.close(), target.close()]);
  });

  it("routes a native restored receipt to its original ledger even before T ever offered that event", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    source.start();
    const event = await f.publish();
    const edge = f.binding(), handoff = await source.parkForNativeRecovery(edge);
    target.start();
    const transfer = f.committed(edge);
    transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: { customType: "pi-fabric-inbox", details: { ids: [event.id] } } }];
    await target.adoptNativeRecoveryInbox(handoff, () => ({ status: "validated", transfers: [transfer] }));
    expect(target.acked(event.id, event.sequence)).toBe(false);
    await target.consume([event.id], eventReceipts(event));
    expect(target.acked(event.id, event.sequence)).toBe(true);
    expect((f.mesh.get(source.key)!.value as { delivered: string[] }).delivered).toContain(eventReceipt(event.id));
    expect((f.mesh.get(target.key)!.value as { delivered: string[] }).delivered).not.toContain(eventReceipt(event.id));
    expect((await target.next(rootInboxSession([]))).events).toEqual([]);
    await Promise.all([source.close(), target.close()]);
  });

  it("rejects a failed park save but leaves a pre-PREPARED source ordinary with its original boundary", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    await f.publish("unrelated", "before source started");
    source.start();
    const event = await f.publish();
    const edge = f.binding(), put = f.mesh.put.bind(f.mesh);
    const failing = vi.spyOn(f.mesh, "put").mockImplementation(async input => {
      if (input.key === source.key) throw new Error("cursor barrier unavailable");
      return put(input);
    });
    await expect(source.parkForNativeRecovery(f.intent(edge))).rejects.toThrow("cursor barrier unavailable");
    failing.mockRestore();
    expect((await source.next(rootInboxSession([]))).events.map(item => item.id)).toEqual([event.id]);
    const handoff = await source.parkForNativeRecovery(f.intent(edge));
    expect(handoff.after).toBe(event.sequence - 1);
    target.start();
    await target.adoptNativeRecoveryInbox(handoff, () => ({ status: "validated", transfers: [f.committed(edge)] }));
    expect((await target.next(rootInboxSession([]))).events.map(item => item.id)).toEqual([event.id]);
    await Promise.all([source.close(), target.close()]);
  });

  it("joins an in-flight pending save before handoff without pretending to own R/N's source suspension", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    source.start();
    const event = await f.publish();
    const put = f.mesh.put.bind(f.mesh);
    let release!: () => void, entered!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const saved = new Promise<void>(resolve => { entered = resolve; });
    let first = true;
    const delayed = vi.spyOn(f.mesh, "put").mockImplementation(async input => {
      if (input.key === source.key && first) { first = false; entered(); await gate; }
      return put(input);
    });
    const reading = source.next(rootInboxSession([]));
    await saved;
    const edge = f.binding(), parking = source.parkForNativeRecovery(f.intent(edge));
    release();
    const [oldBatch, handoff] = await Promise.all([reading, parking]);
    delayed.mockRestore();
    expect(oldBatch.events.map(item => item.id)).toEqual([event.id]);
    expect(f.mesh.get(source.key)!.value).toMatchObject({ nativeRecoveryPark: f.intent(edge), pending: { ids: [event.id] } });
    target.start();
    await target.adoptNativeRecoveryInbox(handoff, () => ({ status: "validated", transfers: [f.committed(edge)] }));
    expect((await target.next(rootInboxSession([]))).events.map(item => item.id)).toEqual([event.id]);
    await Promise.all([source.close(), target.close()]);
  });

  it("requires RECEIVED and freshly holds on read errors, ambiguity and disappearing custody without ACKing", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    source.start();
    const event = await f.publish();
    await source.next(rootInboxSession([]));
    const edge = f.binding(), transfer = f.committed(edge);
    const handoff = await source.parkForNativeRecovery(edge);
    let mode = "prepared";
    const read: NativeRecoveryCustodyReader = () => {
      if (mode === "throw") throw new Error("native barrier failed");
      if (mode === "none") return { status: "none" };
      if (mode === "prepared") return { status: "validated", transfers: [{ ...edge, occurrences: [] }] };
      if (mode === "ambiguous") return { status: "validated", transfers: [{ ...transfer, occurrences: [{ ordinal: 0, state: "ambiguous" }] }] };
      return { status: "validated", transfers: [transfer] };
    };
    await expect(target.adoptNativeRecoveryInbox(handoff, read)).rejects.toThrow(/RECEIVED/);
    await expect(target.next(rootInboxSession([]))).rejects.toThrow(/incomplete/);
    mode = "received";
    await target.adoptNativeRecoveryInbox(handoff, read);
    target.start();
    const before = f.mesh.get(source.key)!.value;
    for (mode of ["throw", "ambiguous", "none"]) {
      await expect(target.next(rootInboxSession([]))).rejects.toThrow();
      await expect(target.wake(rootInboxSession([]), () => true)).rejects.toThrow();
      await expect(target.consume([event.id], eventReceipts(event))).rejects.toThrow();
      expect(f.mesh.get(source.key)!.value).toEqual(before);
    }
    mode = "received";
    expect((await target.next(rootInboxSession([]))).events.map(item => item.id)).toEqual([event.id]);
    await Promise.all([source.close(), target.close()]);
  });

  it("enumerates every committed predecessor over multiple hops and survives a new target reader", async () => {
    const f = setup(), source = f.inbox("source"), middle = f.inbox("middle"), target = f.inbox("target");
    source.start();
    const first = f.binding("source", "middle"), second = f.binding("middle", "target");
    const handoffS = await source.parkForNativeRecovery(first);
    middle.start();
    await middle.adoptNativeRecoveryInbox(handoffS, () => ({ status: "validated", transfers: [f.committed(first)] }));
    const older = await f.publish("source", "unread through two switches");
    const handoffM = await middle.parkForNativeRecovery(second);
    target.start();
    const oldFuture = await f.publish("source", "future S"), midFuture = await f.publish("middle", "future M"), own = await f.publish("target", "own");
    const read: NativeRecoveryCustodyReader = () => ({ status: "validated", transfers: [f.committed(first), f.committed(second)] });
    await target.adoptNativeRecoveryInbox(handoffM, read);
    const batch = await target.next(rootInboxSession([]));
    expect(batch.events.map(event => event.id)).toEqual([older.id, oldFuture.id, midFuture.id, own.id]);
    expect(batch.events.map(event => event.to)).toEqual(["session:source", "session:source", "session:middle", "session:target"]);
    await target.consume(batch.events.map(event => event.id), batch.events.flatMap(eventReceipts));
    const resumed = f.inbox("target");
    await resumed.adoptNativeRecoveryInbox(handoffM, read);
    resumed.start();
    expect((await resumed.next(rootInboxSession([]))).events).toEqual([]);
    const future = await f.publish("source", "still reaches newest target");
    expect((await resumed.next(rootInboxSession([]))).events.map(event => event.id)).toEqual([future.id]);
    await Promise.all([source.close(), middle.close(), target.close(), resumed.close()]);
  });

  it("preserves dated receipt, spent fallback and out-of-order ACK ledgers rather than merging T marks", async () => {
    const f = setup(), source = f.inbox("source"), target = f.inbox("target");
    const now = Date.now(), delivered = "d".repeat(64), fallback = "f".repeat(64);
    await f.mesh.put({ key: source.key, identity: source.identity, value: {
      after: 0, delivered: [delivered], deliveredAt: [now - 10], fallbacks: [fallback], fallbackAt: [now - 5],
      fallbackThrough: now - 100, acked: [["previous-lookahead", 30]],
    } });
    source.start();
    const edge = f.binding(), handoff = await source.parkForNativeRecovery(edge);
    target.start();
    await target.adoptNativeRecoveryInbox(handoff, () => ({ status: "validated", transfers: [f.committed(edge)] }));
    await target.next(rootInboxSession([]));
    expect(f.mesh.get(source.key)!.value).toMatchObject({
      delivered: [delivered], deliveredAt: [now - 10], fallbacks: [fallback], fallbackAt: [now - 5],
      fallbackThrough: now - 100, acked: [["previous-lookahead", 30]],
    });
    expect(f.mesh.get(target.key)?.value).not.toMatchObject({ delivered: [delivered] });
    await Promise.all([source.close(), target.close()]);
  });

  it.each(["pending", "backlog", "future"])("keeps %s native reservations distinct from consumption across next/priority/commit/wake/reload", async placement => {
    const f = setup(), source = f.inbox("source");
    let target = f.inbox("target");
    source.start();
    const edge = f.binding(), transfer = f.committed(edge);
    let event = placement === "future" ? undefined : await f.publish("source", "native work", { deliveryId: "native-reservation" });
    if (placement === "pending") await source.next(rootInboxSession([]));
    const handoff = await source.parkForNativeRecovery(f.intent(edge));
    target.start();
    event ??= await f.publish("source", "native work", { deliveryId: "native-reservation" });
    const reserved = event;
    transfer.occurrences = [{ ordinal: 0, state: "pending", customMessage: {
      customType: "pi-fabric-inbox", details: { ids: [reserved.id], receipts: eventReceipts(reserved) },
    } }];
    const read: NativeRecoveryCustodyReader = () => ({ status: "validated", transfers: [transfer] });
    await target.adoptNativeRecoveryInbox(handoff, read);
    const unsigned = rootInboxMessage([reserved]);
    const unconfirmed = rootInboxSession([{ type: "custom_message", customType: unsigned.customType,
      details: unsigned.details, content: unsigned.content, timestamp: Date.now() }]);
    expect(unconfirmed.holdsBatch([reserved.id])).toBe(false);
    for (let reload = 0; reload < 2; reload++) {
      for (const options of [{}, { priority: true }, { turnEnd: true }, { commitOnly: true }]) {
        expect((await target.next(unconfirmed, options)).events).toEqual([]);
        expect(await target.wake(unconfirmed, () => true)).toBeUndefined();
        expect(target.acked(reserved.id, reserved.sequence)).toBe(false);
        const state = f.mesh.get(source.key)!.value as { after: number; delivered?: string[]; pending?: { ids: string[] } };
        expect(state.after).toBe(handoff.after);
        expect(state.delivered ?? []).not.toContain(eventReceipt(reserved.id));
        if (placement === "pending") expect(state.pending?.ids).toContain(reserved.id);
      }
      if (!reload) {
        await target.close(); target = f.inbox("target");
        await target.adoptNativeRecoveryInbox(handoff, read); target.start();
      }
    }
    transfer.occurrences = transfer.occurrences.map(item => ({ ...item, state: "handled" }));
    expect((await target.next(rootInboxSession([]))).events).toEqual([]);
    expect(target.acked(reserved.id, reserved.sequence)).toBe(true);
    expect(f.mesh.get(source.key)!.value).toMatchObject({ delivered: expect.arrayContaining([eventReceipt(reserved.id)]) });
    expect((f.mesh.get(source.key)!.value as { after: number }).after).toBeGreaterThanOrEqual(reserved.sequence);
    await Promise.all([source.close(), target.close()]);
  });

  it("retains the exact private issuer via an opaque process object, not copied nonce/details", async () => {
    const f = setup();
    const event = await f.publish();
    const sourceManager = {}, targetManager = {}, issuer = rootInboxReceiptIssuer(sourceManager, "source");
    const message = rootInboxMessage([event]);
    registerRootInboxReceipt(issuer, message.details, message.content);
    const original = verifyRootInboxReceipt(issuer, message.details, message, new Set())!;
    issuer.queued.set(event.id, 3); issuer.turn = 4; issuer.consumed.add(original);
    const authority = retainRootInboxReceiptIssuer(issuer);
    expect(() => adoptRootInboxReceiptIssuer(targetManager, "target", JSON.parse(JSON.stringify(authority)))).toThrow();
    const unrelated = rootInboxReceiptIssuer({}, "unrelated");
    expect(verifyRootInboxReceipt(unrelated, message.details, message, new Set())).toBeUndefined();
    const adopted = adoptRootInboxReceiptIssuer(targetManager, "target", authority);
    expect(adopted).toBe(issuer);
    expect(rootInboxReceiptIssuer(targetManager, "target")).toBe(issuer);
    expect(rootInboxReceiptIssuer(targetManager, "target", true)).toBe(issuer);
    expect(adoptRootInboxReceiptIssuer(targetManager, "target", authority)).toBe(issuer);
    expect(adopted.consumed.has(original)).toBe(true);
    expect(adopted.queued.get(event.id)).toBe(3);
    expect(adopted.turn).toBe(4);
    expect(verifyRootInboxReceipt(adopted, structuredClone(message.details), message, new Set())).toBe(original);
    expect(verifyRootInboxReceipt(adopted, message.details, { ...message, content: "not shown" }, new Set())).toBeUndefined();
    expect(() => adoptRootInboxReceiptIssuer({}, "other", authority)).toThrow();
    const nextAuthority = retainRootInboxReceiptIssuer(adopted);
    expect(adoptRootInboxReceiptIssuer({}, "next", nextAuthority)).toBe(issuer);
    expect(rootInboxReceiptIssuer(sourceManager, "ordinary-new-session").receipts.size).toBe(0);
  });
});

// Keep: original-recipient authorization and ACK trust boundaries; real FILE/router/Main
// admission and no-loss cursor behavior (#17478). Custody is injected, not native/broker proof.
describe("native recovery actual-source message routing", () => {
  const setup = async (multiHop = false) => {
    const root = rootDirectory(), mesh = new MeshStore(path.join(root, "mesh"), 65536, 1000);
    const sourceManager = SessionManager.create(root, path.join(root, "source"));
    const targetManager = SessionManager.create(root, path.join(root, "target"));
    sourceManager.appendMessage(fauxAssistantMessage("source persisted"));
    targetManager.appendMessage(fauxAssistantMessage("target persisted"));
    const source = { sessionId: sourceManager.getSessionId(), sessionFile: sourceManager.getSessionFile()! };
    const target = { sessionId: targetManager.getSessionId(), sessionFile: targetManager.getSessionFile()! };
    const S = `session:${source.sessionId}`, T = `session:${target.sessionId}`, A = "session:sender";
    const identity = (id: string) => ({ id, name: id, kind: "main" as const });
    const transfer: NativeRecoveryTransfer = { version: 1, operationId: "native-route", preparedDigest: "a".repeat(64),
      preparedEntryId: "prepared", receivedEntryId: "received", source, target, phase: "received", occurrences: [] };
    let transfers: readonly NativeRecoveryTransfer[] = [transfer];
    if (multiHop) {
      const middle = SessionManager.create(root, path.join(root, "middle"));
      middle.appendMessage(fauxAssistantMessage("middle persisted"));
      const scope = { sessionId: middle.getSessionId(), sessionFile: middle.getSessionFile()! };
      registerMainInbox(mesh.root, identity(`session:${scope.sessionId}`), scope.sessionId, scope.sessionFile);
      transfers = [{ ...transfer, target: scope }, { ...transfer, operationId: "native-route-second",
        preparedDigest: "b".repeat(64), preparedEntryId: "prepared-second", receivedEntryId: "received-second", source: scope }];
    }
    let mode = "received", reads = 0, active = true;
    const order: string[] = [];
    const reader: NativeRecoveryCustodyReader = scope => {
      reads++; order.push("read");
      const outgoing = transfers.findIndex(edge => edge.source.sessionId === scope.sessionId && edge.source.sessionFile === scope.sessionFile);
      const incoming = transfers.findIndex(edge => edge.target.sessionId === scope.sessionId && edge.target.sessionFile === scope.sessionFile);
      if (outgoing < 0 && incoming < 0) return { status: "none" }; // Unrelated ordinary roots have no native custody.
      const scoped = transfers.slice(0, (outgoing < 0 ? incoming : outgoing) + 1);
      if (mode === "throw") throw new Error("native readback unavailable");
      if (mode === "hold") return { status: "hold", reason: "native HOLD" };
      if (mode === "none") return { status: "none" };
      if (mode === "wrong-scope") return { status: "validated", transfers: [{ ...transfer,
        target: { ...target, sessionFile: path.join(root, "unknown-scope.jsonl") } }] };
      if (mode === "prepared") {
        const { receivedEntryId: _received, ...prepared } = transfer;
        return { status: "validated", transfers: [{ ...prepared, phase: "prepared" }] };
      }
      return { status: "validated", transfers: scoped };
    };
    registerMainInbox(mesh.root, identity(S), source.sessionId, source.sessionFile);
    registerMainInbox(mesh.root, identity(T), target.sessionId, target.sessionFile);
    await reconcileNativeMainInboxCustody(mesh, target, reader);
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: T, rootId: T, identity: identity(T),
      reapDeadHosts: false, heartbeatMs: 60_000, leaseMs: 120_000 });
    directory.registerSource(() => [{ format: 1, id: T, rootId: T, kind: "root", ownerHostId: T, ownerIdentityId: T,
      name: "same-display-name", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
      sessionId: target.sessionId, cwd: root, startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1", interactive: true }]);
    await directory.refresh();
    directory.setNativeRecoveryReader(reader);
    const planes: FabricControlPlane[] = [];
    const sent: Array<{ details: Record<string, unknown> }> = [];
    const delivered: Array<{ originalTargetId: string; request: FabricMainAgentDeliveryRequest }> = [];
    const supervisorChecks: string[] = [];
    const pi = { on() {}, sendMessage(message: { details: Record<string, unknown> }) { sent.push(message); } } as unknown as ExtensionAPI;
    const main = new MainAgentController(pi, T, true, root, target.sessionId, true, undefined, {
      isSupervisor(from, recipient) { supervisorChecks.push(recipient); return (from.id === "supervisor-of-S" && recipient === S) ||
        (from.id === "supervisor-of-T-only" && recipient === T); },
    });
    const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: targetManager } as unknown as ExtensionContext;
    const journal = path.join(mesh.root, "main-followups", "target-routing.json");
    main.attachFollowUpDrain(context, 60_000, journal, 600, { owns: () => true, active: () => active }, reader);
    const unknownManager: any = { status(id: string) { throw new Error(`Unknown Fabric agent: ${id}`); },
      stop(id: string) { throw new Error(`Unknown Fabric agent: ${id}`); } };
    const actors = (id: string): any => ({ identity: identity(id), mesh: { root: mesh.root }, owns: () => false,
      status(id: string) { throw new Error(`Unknown Fabric actor: ${id}`); }, validateDirectMessage() {},
      tell() { throw new Error("unexpected actor tell"); }, ask() { throw new Error("unexpected ask"); }, stop() {},
      steerRemote() { throw new Error("unfenced fallback forbidden"); }, resolveBinding: (b: any) => b,
      resolveActivationBinding: async () => ({}) });
    const sourcePort: any = { get: directory.get.bind(directory), list: directory.list.bind(directory), scheduleRefresh() {},
      lastKnown: () => undefined, routingUnavailable: () => undefined };
    const localMain = (id: string): any => ({ id, local: true, interactive: true, matches: (value: string) => value === id,
      deliverAgent() { throw new Error("ordinary Main delivery must not replace native routing"); } });
    const taskRouter = (allowed: string, id: string, control?: FabricControlPlane, host: any = localMain(id)) => {
      const before = { ...process.env };
      Object.assign(process.env, { PI_FABRIC_TASK_PROCESS_CHILD: "1", PI_FABRIC_PARENT_RUN: "local-routing-fixture",
        PI_FABRIC_SPAWNER_ID: allowed, PI_FABRIC_MAIN_AGENT_ID: allowed,
        PI_FABRIC_SPAWNER_CHAIN: "[]", PI_FABRIC_TASK_ESCALATION_TARGETS: "[]" });
      delete process.env.PI_FABRIC_ACTOR_ID;
      try { return new AgentMessageRouter(unknownManager, actors(id), host, sourcePort, control, b => b); }
      finally { for (const key of Object.keys(process.env)) if (!(key in before)) delete process.env[key]; Object.assign(process.env, before); }
    };
    const install = (router: AgentMessageRouter, side: string, recipients: readonly string[] = [S]) => router.setNativeRecoveryRouting({ readNativeCustody: reader,
      authorizeOriginalRecipient(request) {
        order.push(`${side}:authorize`);
        assert.ok(recipients.includes(request.targetId), "policy must authorize the exact requested recipient");
        const supervisor = request.targetId === S ? "supervisor-of-S" : "supervisor-of-T-only";
        if (request.from.id === "bad" ||
            (request.priority === "interrupt" && ![request.targetId, supervisor].includes(request.from.id))) throw new Error("recipient policy denied");
      },
      deliverAgent(originalTargetId, request) {
        const result = main.deliverNativeRecoveryAgent(originalTargetId, request);
        delivered.push({ originalTargetId, request });
        return result;
      },
    });
    const plane = (id: string, options: Partial<ConstructorParameters<typeof FabricControlPlane>[2]> = {}) => {
      const result = new FabricControlPlane(new MeshStore(mesh.root, 65536, 1000), identity(id), {
        enabled: true, hostId: id, readNativeCustody: reader, acknowledgementTimeoutMs: 1000, pollMs: 20, ...options });
      planes.push(result); return result;
    };
    const receiver = plane(T, { ownerIncarnation: directory.ownerIncarnation, controlIncarnationFence: "enforce" });
    const sender = plane(A, { captureOwnerLease: directory.captureControlOwnerLease.bind(directory) });
    const receiveRouter = taskRouter("session:unrelated-spawner", T, receiver, main), sendRouter = taskRouter(S, A, sender);
    install(receiveRouter, "receiver"); install(sendRouter, "sender");
    const baseCommand = { version: 1 as const, commandId: "received-direct", targetId: S, operation: "steer" as const,
      priority: "interrupt" as const, replyTo: A, requestedAt: Date.now(), ownerIncarnation: receiver.incarnation,
      message: "interrupt S", data: { exact: true } };
    return { root, mesh, S, T, A, source, target, transfer, reader, directory, main, targetManager, sent, delivered, supervisorChecks,
      order, sender, receiver, sendRouter, receiveRouter, taskRouter, install, plane, identity, baseCommand, journal,
      reads: () => reads, mode: (value: string) => { mode = value; }, active: (value: boolean) => { active = value; },
      transfers: () => transfers, chain: (value: readonly NativeRecoveryTransfer[]) => { transfers = value; },
      async close() { reopenMainInbox(S); reopenMainInbox(T); await Promise.all(planes.map(p => p.close()));
        main.closeFollowUpDrain(); await directory.close(); },
    };
  };

  it("routes original S through real FILE control, receiving router and Main with exact command/event/delivery/ACK identities", async () => {
    const f = await setup();
    try {
      f.receiver.start((...args) => f.receiveRouter.acceptControl(...args)); f.sender.start(() => ({ accepted: false }));
      f.order.length = 0;
      const first = await f.sendRouter.routeMessage(f.S, "future followUp", { exact: "payload" }, "followUp", undefined, { idempotencyKey: "stable" });
      expect(first.acknowledged).toBe(true); expect(f.delivered).toHaveLength(1); expect(f.sent).toHaveLength(1);
      expect(f.order[0]).toBe("sender:authorize");
      const receivedAt = f.order.indexOf("receiver:authorize");
      expect(receivedAt).toBeGreaterThan(0); expect(f.order[receivedAt + 1]).toBe("read");
      const command: any = f.mesh.read({ topic: "fabric.control.command", limit: 1000 }).find((e: any) => e.data.operation === "followUp");
      expect(command.to).toBe(f.T); expect(command.data.targetId).toBe(f.S);
      expect(command.data.ownerIncarnation).toBe(f.receiver.incarnation);
      expect(f.delivered[0]!.originalTargetId).toBe(f.S);
      expect(f.delivered[0]!.request).toMatchObject({ deliveryId: command.data.commandId, inboxEventId: command.id,
        outcomeSend: { to: f.S, eventId: command.id }, from: { id: f.A }, data: { exact: "payload" } });
      expect(f.sent[0]!.details).toMatchObject({ deliveryId: command.data.commandId, inboxEventId: command.id });
      const ack: any = f.mesh.read({ topic: "fabric.control.ack", limit: 1000 }).find((e: any) => e.data.commandId === command.data.commandId);
      expect(ack.from.id).toBe(f.T); expect(ack.data.targetId).toBe(f.S); expect(ack.data.commandId).toBe(command.data.commandId);
      await f.sendRouter.routeMessage(f.S, "future followUp", { exact: "payload" }, "followUp", undefined, { idempotencyKey: "stable" });
      expect(f.delivered).toHaveLength(1); expect(f.sent).toHaveLength(1);
      await expect(f.sendRouter.routeMessage(f.T, "not the return address", undefined, "followUp")).rejects.toThrow(/allowed targets/);
      const denied = f.taskRouter(f.T, f.A, f.sender); f.install(denied, "sender");
      const before = f.reads();
      await expect(denied.routeMessage(f.S, "not allowed", undefined, "followUp")).rejects.toThrow(/allowed targets/);
      expect(f.reads()).toBe(before);
      const asyncPolicy = f.taskRouter(f.S, f.A, f.sender);
      asyncPolicy.setNativeRecoveryRouting({ readNativeCustody: f.reader,
        authorizeOriginalRecipient: (() => Promise.resolve()) as any, deliverAgent() { throw new Error("must not dispatch"); } });
      await expect(asyncPolicy.routeMessage(f.S, "not synchronous authority", undefined, "followUp")).rejects.toThrow(/synchronously/);
      expect(f.reads()).toBe(before);
    } finally { await f.close(); }
  });

  it.each(["steer", "followUp"] as const)("delivers direct current T %s over sender router -> FILE/control -> receiver -> real Main alongside old S", async operation => {
    const f = await setup();
    try {
      const direct = f.taskRouter(f.T, f.A, f.sender);
      f.install(direct, "direct-sender", [f.T]); f.install(f.receiveRouter, "receiver", [f.S, f.T]);
      f.receiver.start((...args) => f.receiveRouter.acceptControl(...args)); f.sender.start(() => ({ accepted: false }));
      f.order.length = 0;
      const result = await direct.routeMessage(f.T, `direct current T ${operation}`, { current: true }, operation,
        undefined, { idempotencyKey: `direct-T:${operation}` });
      expect(result.acknowledged).toBe(true); expect(f.sent).toHaveLength(1); expect(f.delivered).toHaveLength(0);
      expect(resolveNativeMainSuccessor(f.mesh.root, f.T, f.reader)).toBeUndefined();
      expect(f.directory.resolveNativeMessageTarget(f.T, operation)).toBeUndefined();
      expect(f.directory.get(f.T)?.id).toBe(f.T);
      expect(f.order[0]).toBe("direct-sender:authorize");
      const admittedAt = f.order.indexOf("receiver:authorize");
      expect(admittedAt).toBeGreaterThan(0); expect(f.order[admittedAt + 1]).toBe("read");
      const command: any = f.mesh.read({ topic: "fabric.control.command", limit: 1000 }).find((e: any) => e.data.targetId === f.T);
      expect(command.to).toBe(f.T); expect(command.data.ownerIncarnation).toBe(f.receiver.incarnation);
      expect(f.sent[0]!.details).toMatchObject({ deliveryId: command.data.commandId, inboxEventId: command.id });
      const ack: any = f.mesh.read({ topic: "fabric.control.ack", limit: 1000 }).find((e: any) => e.data.commandId === command.data.commandId);
      expect(ack.from.id).toBe(f.T); expect(ack.data.targetId).toBe(f.T);
      await direct.routeMessage(f.T, `direct current T ${operation}`, { current: true }, operation,
        undefined, { idempotencyKey: `direct-T:${operation}` });
      expect(f.sent).toHaveLength(1);
      expect((await f.sendRouter.routeMessage(f.S, "old S still routes", undefined, operation)).acknowledged).toBe(true);
      expect(f.delivered).toHaveLength(1); expect(f.delivered[0]!.originalTargetId).toBe(f.S); expect(f.sent).toHaveLength(2);
      // A different FILE owner must ignore incoming-only T, not invent an outgoing
      // alias or HOLD just because it observed validated incoming custody.
      const foreign = new FileControlReader(f.mesh, "session:foreign-owner");
      foreign.setNativeCommandRoute({ readNativeCustody: f.reader, ownerIdentityId: "session:foreign-owner",
        ownerIncarnation: f.receiver.incarnation });
      const selected: string[] = [];
      await foreign.drain(event => { selected.push(event.id); }, () => false);
      expect(selected).toEqual([]);
    } finally { await f.close(); }
  });

  it("direct current T never downgrades sealed, PREPARED, unknown or malformed native evidence into ordinary delivery", async () => {
    const f = await setup();
    try {
      const direct = f.taskRouter(f.T, f.A, f.sender); f.install(direct, "direct-sender", [f.T]);
      f.install(f.receiveRouter, "receiver", [f.S, f.T]);
      const chain = f.transfers();
      const { receivedEntryId: _received, ...prepared } = chain[0]!;
      const outgoing: NativeRecoveryTransfer = { ...prepared, operationId: "T-to-U", preparedDigest: "c".repeat(64),
        preparedEntryId: "prepared-T", source: f.target,
        target: { sessionId: "44444444-4444-4444-8444-444444444444", sessionFile: path.join(f.root, "U.jsonl") }, phase: "prepared" };
      const attempts: Array<() => void> = [
        () => f.mode("hold"), () => f.mode("throw"), () => f.mode("prepared"),
        () => f.chain([{ ...chain[0]!, preparedDigest: "malformed" }]),
        () => f.mode("wrong-scope"),
        () => f.chain([...chain, outgoing]),
        () => f.chain([...chain, { ...outgoing, phase: "received", receivedEntryId: "received-U" }]),
      ];
      const { priority: _priority, ...ordinaryCommand } = f.baseCommand;
      for (const [index, mutate] of attempts.entries()) {
        f.mode("received"); f.chain(chain); mutate();
        expect(() => f.directory.resolveNativeMessageTarget(f.T, "steer")).toThrow();
        await expect(direct.routeMessage(f.T, "must hold", undefined, "steer")).rejects.toThrow();
        expect((await f.receiveRouter.acceptControl({ ...ordinaryCommand, targetId: f.T,
          commandId: `direct-held:${index}` }, f.identity(f.A), undefined, "mesh")).accepted).toBe(false);
      }
      expect(f.sent).toHaveLength(0); expect(f.delivered).toHaveLength(0);
      f.mode("received"); f.chain(chain);
      expect(resolveNativeMainSuccessor(f.mesh.root, f.T, f.reader)).toBeUndefined();
    } finally { await f.close(); }
  });

  it("direct T retains its own interrupt, halt, drain, session identity and activation gates, distinct from old S", async () => {
    const f = await setup();
    try {
      const direct = f.taskRouter(f.T, f.A, f.sender); f.install(direct, "direct-sender", [f.T]);
      f.install(f.receiveRouter, "receiver", [f.S, f.T]);
      f.receiver.start((...args) => f.receiveRouter.acceptControl(...args)); f.sender.start(() => ({ accepted: false }));
      const send = (id: string, options = {}) => direct.routeMessage(f.T, id, undefined, "steer", undefined, { idempotencyKey: id, ...options });
      await expect(send("S-supervisor-not-T", { from: f.identity("supervisor-of-S"), priority: "interrupt" })).rejects.toThrow(/policy denied/);
      const supervisor = (id: string, recipient: string) => {
        // The wire's sender is the actual control-plane identity, not options.from.
        const control = f.plane(id, { captureOwnerLease: f.directory.captureControlOwnerLease.bind(f.directory) });
        control.start(() => ({ accepted: false }));
        const router = f.taskRouter(recipient, id, control); f.install(router, "supervisor-sender", [recipient]); return router;
      };
      expect((await supervisor("supervisor-of-T-only", f.T).routeMessage(f.T, "T-supervisor", undefined, "steer", undefined,
        { priority: "interrupt" })).acknowledged).toBe(true);
      expect(f.supervisorChecks).toContain(f.T);
      expect((await f.receiveRouter.acceptControl({ ...f.baseCommand, targetId: f.T, commandId: "T-own-root" },
        f.identity(f.T), undefined, "mesh")).accepted).toBe(true);
      expect((await f.receiveRouter.acceptControl({ ...f.baseCommand, targetId: f.T, commandId: "S-not-T-supervisor" },
        f.identity("supervisor-of-S"), undefined, "mesh")).accepted).toBe(false);
      expect((await supervisor("supervisor-of-S", f.S).routeMessage(f.S, "S supervisor unchanged", undefined, "steer", undefined,
        { priority: "interrupt" })).acknowledged).toBe(true);
      const before = f.sent.length;
      closeMainInbox(f.T);
      await expect(send("T-draining")).rejects.toThrow(/FABRIC_MAIN_DRAINING|draining/i);
      reopenMainInbox(f.T); f.active(false);
      await expect(send("T-inactive")).rejects.toThrow(/rotated/);
      f.active(true);
      const session = vi.spyOn(f.targetManager, "getSessionId").mockReturnValue(f.source.sessionId);
      try { await expect(send("T-session-changed")).rejects.toThrow(/identity\/path changed/); }
      finally { session.mockRestore(); }
      expect(f.sent).toHaveLength(before);
      f.main.halt();
      const halted = await send("T-halted");
      expect(halted).toMatchObject({ acknowledged: true, halted: true, triggered: false, reason: "operator-halt" });
      expect(f.main.drainSnapshot().halted).toBe(true); // Admission remains truthful; halt prevents provider triggering.
      expect(f.delivered).toHaveLength(1); // Only old S uses the native old-address Main adapter.
    } finally { await f.close(); }
  });

  it("authorizes exact original S on receive before lookup, preserves S interrupt authority, and applies current T gates", async () => {
    const f = await setup();
    try {
      const before = f.reads();
      for (const denied of ["bad", "supervisor-of-T-only"]) {
        expect((await f.receiveRouter.acceptControl(f.baseCommand, f.identity(denied), undefined, "mesh")).accepted).toBe(false);
        expect(f.reads()).toBe(before);
      }
      f.order.length = 0;
      expect((await f.receiveRouter.acceptControl(f.baseCommand, f.identity("supervisor-of-S"), undefined, "mesh")).accepted).toBe(true);
      expect(f.order[0]).toBe("receiver:authorize"); expect(f.order[1]).toBe("read");
      expect(f.supervisorChecks).toEqual([f.S]); expect(f.delivered[0]!.request.priority).toBe("interrupt");
      const request = (from: string, deliveryId: string): FabricMainAgentDeliveryRequest => ({ from: f.identity(from),
        message: "original interrupt", delivery: "steer", priority: "interrupt", verification: "mesh", deliveryId });
      // Bypass the fixture policy to prove the real Main adapter independently authorizes S.
      expect(() => f.main.deliverNativeRecoveryAgent(f.S, request("supervisor-of-T-only", "T-supervisor"))).toThrow(/authorized/i);
      expect(f.main.deliverNativeRecoveryAgent(f.S, request(f.S, "S-own-root"))).toMatchObject({ queued: true });
      expect(() => f.main.deliverNativeRecoveryAgent(f.S, request(f.T, "T-is-not-S"))).toThrow(/authorized/i);
      const count = f.sent.length;
      closeMainInbox(f.T);
      expect(() => f.main.deliverNativeRecoveryAgent(f.S, request("supervisor-of-S", "T-gate"))).toThrow(/FABRIC_MAIN_DRAINING/);
      expect(f.sent).toHaveLength(count);
      reopenMainInbox(f.T); closeMainInbox(f.S);
      expect(f.main.deliverNativeRecoveryAgent(f.S, { from: f.identity(f.A), message: "current T is open", delivery: "steer", deliveryId: "S-gate-not-T" })).toMatchObject({ queued: true });
      f.active(false);
      expect(() => f.main.deliverNativeRecoveryAgent(f.S, { from: f.identity(f.A), message: "T rotated", delivery: "steer", deliveryId: "T-inactive" })).toThrow(/rotated/);
      f.active(true);
      const noAlias = f.delivered.length, noAliasReads = f.reads();
      for (const operation of ["stop", "ask", "cancel", "setModel", "setThinking"] as const) {
        expect((await f.receiveRouter.acceptControl({ ...f.baseCommand, operation, priority: undefined } as any,
          f.identity(f.A), undefined, "mesh")).accepted).toBe(false);
      }
      expect(f.delivered).toHaveLength(noAlias); expect(f.reads()).toBe(noAliasReads);
      expect(f.main.matches(f.S)).toBe(false); // Dashboard/user delivery gets no old-address identity.
    } finally { await f.close(); }
  });

  it("selects only committed exact FILE message commands at T's current owner/epoch; uncertainty never falls back", async () => {
    const f = await setup();
    try {
      expect(f.directory.get(f.S)).toBeUndefined(); expect(f.directory.resolveNativeMessageTarget(f.S, "followUp")?.id).toBe(f.T);
      expect(f.directory.resolveNativeMessageTarget("same-display-name", "followUp")).toBeUndefined();
      expect(f.directory.resolveNativeMessageTarget(f.S, "stop" as any)).toBeUndefined();
      const prior = f.mesh.latestSequence();
      const publish = (kind: string, epoch = f.receiver.incarnation, topic = "fabric.control.command", to = f.S, targetId = f.S) =>
        f.mesh.publish({ topic, kind, to, from: f.identity(f.A), data: { ...f.baseCommand, operation: kind, priority: undefined,
          commandId: `filter:${kind}:${epoch}:${targetId}`, ownerIncarnation: epoch, targetId } });
      const eligible = await publish("followUp");
      await publish("steer", "old-S-epoch"); await publish("accepted", f.receiver.incarnation, "fabric.control.ack");
      for (const kind of ["stop", "ask", "cancel", "setModel", "setThinking"]) await publish(kind);
      await publish("followUp", f.receiver.incarnation, "fabric.control.command", f.S, "session:forged-target");
      const filter = new FileControlReader(f.mesh, f.T);
      filter.setNativeCommandRoute({ readNativeCustody: f.reader, ownerIdentityId: f.T, ownerIncarnation: f.receiver.incarnation });
      const selected: string[] = [];
      await filter.drain(event => { if (event.sequence > prior) selected.push(event.id); }, () => false);
      expect(selected).toEqual([eligible.id]);
      for (const owner of ["session:unrelated-owner", f.T]) {
        const exact = new FileControlReader(f.mesh, owner);
        if (owner !== f.T) exact.setNativeCommandRoute({ readNativeCustody: f.reader, ownerIdentityId: owner, ownerIncarnation: f.receiver.incarnation });
        const events: string[] = []; await exact.drain(event => { events.push(event.id); }, () => false);
        expect(events).toEqual([]);
      }
      for (const mode of ["prepared", "hold", "throw", "none"]) {
        f.mode(mode);
        await expect(f.sendRouter.routeMessage(f.S, `native ${mode}`, undefined, "followUp")).rejects.toThrow();
        expect(() => f.main.deliverNativeRecoveryAgent(f.S, { from: f.identity(f.A), message: "no fallback", delivery: "steer", deliveryId: mode })).toThrow();
        if (mode !== "none") {
          expect(() => f.directory.resolveNativeMessageTarget(f.S, "followUp")).toThrow(/HOLD|received pair unavailable/);
          const held = new FileControlReader(f.mesh, f.T);
          held.setNativeCommandRoute({ readNativeCustody: f.reader, ownerIdentityId: f.T, ownerIncarnation: f.receiver.incarnation });
          await expect(held.drain(() => {}, () => false)).rejects.toThrow(/HOLD|received pair unavailable/);
        }
      }
      expect(f.delivered).toEqual([]); expect(f.sent).toEqual([]);
      f.mode("received");
      const O = "session:33333333-3333-4333-8333-333333333333";
      registerMainInbox(f.mesh.root, f.identity(O), O.slice(8), path.join(f.root, "ordinary.jsonl"));
      await recordMainSuccessor(f.mesh, O, O.slice(8), f.T);
      expect(resolveNativeMainSuccessor(f.mesh.root, O, f.reader)).toBeUndefined();
      expect(f.directory.resolveNativeMessageTarget(O, "followUp")).toBeUndefined();
      f.directory.setNativeRecoveryReader(undefined);
      expect(f.directory.resolveNativeMessageTarget(f.S, "followUp")).toBeUndefined();
    } finally { await f.close(); }
  });

  it("routes through every committed predecessor but refuses an uncommitted edge or a terminal outgoing PREPARED", async () => {
    const f = await setup(true);
    try {
      expect(resolveNativeMainSuccessor(f.mesh.root, f.S, f.reader)?.bindings).toHaveLength(2);
      expect(f.directory.resolveNativeMessageTarget(f.S, "followUp")?.id).toBe(f.T);
      f.receiver.start((...args) => f.receiveRouter.acceptControl(...args)); f.sender.start(() => ({ accepted: false }));
      expect((await f.sendRouter.routeMessage(f.S, "two committed hops", undefined, "followUp")).acknowledged).toBe(true);
      expect(f.delivered).toHaveLength(1); expect(f.delivered[0]!.originalTargetId).toBe(f.S);
      const chain = f.transfers(), middle = `session:${chain[1]!.source.sessionId}`;
      const request: FabricMainAgentDeliveryRequest = { from: f.identity(f.A), message: "middle original", delivery: "steer", deliveryId: "middle-original" };
      expect(f.main.deliverNativeRecoveryAgent(middle, request)).toMatchObject({ queued: true });
      expect(() => f.main.deliverNativeRecoveryAgent("session:unrelated", { ...request, deliveryId: "unrelated" })).toThrow(/chain/);
      const { receivedEntryId: _received, ...uncommitted } = chain[1]!;
      f.chain([chain[0]!, { ...uncommitted, phase: "prepared" }]);
      expect(() => f.directory.resolveNativeMessageTarget(f.S, "followUp")).toThrow(/HOLD|received pair unavailable/);
      expect(() => f.main.deliverNativeRecoveryAgent(f.S, { ...request, deliveryId: "partial-chain" })).toThrow();
      const sealed: NativeRecoveryTransfer = { ...uncommitted, phase: "prepared", operationId: "outgoing-prepared",
        preparedDigest: "c".repeat(64), preparedEntryId: "outgoing", source: f.target,
        target: { sessionId: "44444444-4444-4444-8444-444444444444", sessionFile: path.join(f.root, "future.jsonl") } };
      f.chain([...chain, sealed]);
      expect(() => f.directory.resolveNativeMessageTarget(f.S, "followUp")).toThrow(/HOLD|terminal is sealed/);
      expect(() => f.main.deliverNativeRecoveryAgent(f.S, { ...request, deliveryId: "sealed-T" })).toThrow();
      expect(f.delivered).toHaveLength(1);
    } finally { await f.close(); }
  });

  it("holds a torn native successor locator instead of treating it as absent native recovery", async () => {
    const f = await setup();
    const locator = path.join(f.mesh.root, "main-followups", "successors", `${createHash("sha256").update(f.S).digest("hex")}.json`);
    const original = fs.readFileSync(locator);
    try {
      fs.writeFileSync(locator, "{torn native locator");
      expect(() => f.directory.resolveNativeMessageTarget(f.S, "followUp")).toThrow(/HOLD|JSON|Unexpected/i);
    } finally { fs.writeFileSync(locator, original); await f.close(); }
  });

  it("preserves old-address NATS command/ACK correlation with real control admission over a local in-memory carrier", async () => {
    const f = await setup();
    class Queue {
      values: any[] = []; waiter: ((value: any) => void) | undefined;
      push(value: any) { if (this.waiter) { const done = this.waiter; this.waiter = undefined; done(value); } else this.values.push(value); }
      take(signal: AbortSignal): Promise<any> {
        if (signal.aborted) return Promise.resolve(undefined);
        if (this.values.length) return Promise.resolve(this.values.shift());
        return new Promise(resolve => {
          const finish = (value: any) => { signal.removeEventListener("abort", abort); this.waiter = undefined; resolve(value); };
          const abort = () => finish(undefined); this.waiter = finish; signal.addEventListener("abort", abort, { once: true });
        });
      }
    }
    const endpoints = new Map<string, Endpoint>(), publications: any[] = [], heldAcks: any[] = [];
    let sequence = 0, ackReady!: () => void;
    const ackPublished = new Promise<void>(resolve => { ackReady = resolve; });
    class Endpoint {
      kind = "nats" as const; queue = new Queue(); self: { machine: string; hostId: string };
      constructor(machine: string, hostId: string) { this.self = { machine, hostId }; endpoints.set(hostId, this); }
      offer(payload: any, sourceMachine: string) {
        let completed!: () => void; const disposition = new Promise<void>(resolve => { completed = resolve; });
        this.queue.push({ payload, sourceMachine, verification: "bridge", sequence: ++sequence, attempt: 1,
          working() {}, async ack() { completed(); }, nak() { completed(); }, async term() { completed(); } });
        return disposition;
      }
      async send(envelope: any) {
        const createdAt = Date.now(), body = typeof envelope.body === "function" ? envelope.body(createdAt) : envelope.body;
        const resolved = JSON.parse(JSON.stringify({ ...envelope, body, createdAt })); publications.push(resolved);
        const destination = endpoints.get(resolved.to.hostId)!;
        assert.ok(destination); assert.equal(destination.self.machine, resolved.to.machine);
        if (resolved.kind === "ack") { heldAcks.push({ resolved, destination, sourceMachine: this.self.machine }); ackReady(); }
        else void destination.offer(resolved, this.self.machine);
        return { id: resolved.id, sequence: ++sequence, duplicate: false, envelope: resolved };
      }
      async *inbox(signal: AbortSignal) { while (!signal.aborted) { const value = await this.queue.take(signal); if (!value) return; yield value; } }
      kv(): never { throw new Error("in-memory carrier has no KV"); }
      async drainState() { return { pending: 0, ackPending: 0 }; }
      async close() {}
    }
    try {
      const routes = new Map([
        [f.A, { machine: "caller", ownerHostId: f.A, ownerIdentityId: f.A, ownerIncarnation: "caller-epoch", live: true, capabilities: ["followUp", "steer"] }],
        [f.T, { machine: "owner", ownerHostId: f.T, ownerIdentityId: f.T, ownerIncarnation: f.directory.ownerIncarnation, live: true, capabilities: ["followUp", "steer"] }],
      ]);
      const view: any = { resolve: (id: string) => routes.get(id), leaseLive: () => true };
      expect(routes.has(f.S)).toBe(false);
      const ns = f.plane(f.A, { transport: new Endpoint("caller", f.A) as any, participants: view,
        relayOperatorMachine: "fabric-ops", ownerIncarnation: "caller-epoch" });
      const nr = f.plane(f.T, { transport: new Endpoint("owner", f.T) as any, participants: view,
        relayOperatorMachine: "fabric-ops", ownerIncarnation: f.directory.ownerIncarnation });
      const receive = f.taskRouter("session:unrelated-spawner", f.T, nr, f.main); f.install(receive, "receiver");
      const send = f.taskRouter(f.S, f.A, ns); f.install(send, "sender");
      nr.start((...args) => receive.acceptControl(...args)); ns.start(() => ({ accepted: false }));
      let settled = false;
      const request = send.routeMessage(f.S, "NATS original address", { exact: true }, "followUp", undefined,
        { idempotencyKey: "nats-stable" }).finally(() => { settled = true; });
      await Promise.race([ackPublished, request.then(() => { throw new Error("NATS request settled before held ACK"); })]);
      const command = publications.find(p => p.kind === "command");
      expect(command.to.hostId).toBe(f.T); expect(command.targetId).toBe(f.S); expect(command.body.targetId).toBe(f.S);
      const held = heldAcks[0];
      expect(held.resolved.body.targetId).toBe(f.S); expect(held.resolved.from.id).toBe(f.T);
      await held.destination.offer(held.resolved, "rogue-machine");
      expect(settled).toBe(false);
      await held.destination.offer({ ...held.resolved, from: f.identity(f.S) }, held.sourceMachine);
      expect(settled).toBe(false);
      await held.destination.offer({ ...held.resolved, body: { ...held.resolved.body, targetId: f.T } }, held.sourceMachine);
      expect(settled).toBe(false);
      await held.destination.offer(held.resolved, held.sourceMachine);
      expect((await request).acknowledged).toBe(true);
      expect(f.delivered).toHaveLength(1); expect(f.sent).toHaveLength(1);
      expect(f.delivered[0]!.request).toMatchObject({ deliveryId: command.body.commandId, outcomeSend: { to: f.S } });
      expect(f.sent[0]!.details.deliveryId).toBe(command.body.commandId);
    } finally { await f.close(); }
  });
});

describe("#180 S4 persisted session namespace durability", () => {
  it.skipIf(process.platform === "win32").each(cases)("retains the journal payload on persistent $namespace $operation $barrier barrier failure, then retires once", ({ operation, barrier, symlink }) => {
    const root = rootDirectory();
    const { manager, directory, intermediate } = session(root, symlink);
    const state = controller(root, manager);
    state.main.deliverAgent({ from: actor, message: "reply", delivery: "steer", deliveryId: "retire-once" });
    const details = state.sent[0]!.details;
    persist(manager, details);
    if (operation === "replacement") replace(root, manager, details);
    const probe = barrierProbe(manager.getSessionFile()!, barrier === "leaf" ? directory : path.dirname(directory));
    const retire = fs.rmSync.bind(fs);
    const retired = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) === state.journal) probe.events.push("payload-retirement");
      retire(target, options);
    });
    try {
      for (let attempt = 0; attempt < 3; attempt++) {
        state.settle();
        expect(fs.readFileSync(state.journal, "utf8")).toContain("retire-once");
        expect(fs.existsSync(`${state.journal}.delivered`)).toBe(false);
      }
      expect(probe.events.filter((event) => event === "session-file")).toHaveLength(3);
      expect(probe.events).not.toContain("payload-retirement");
      probe.recover();
      state.settle();
      const chain = receiptChain(manager, intermediate);
      expect(probe.events.slice(0, chain.length + 1)).toEqual(["session-file", ...chain]);
      expect(probe.events.indexOf("payload-retirement")).toBeGreaterThan(chain.length);
      expect(fs.existsSync(state.journal)).toBe(false);
      expect(JSON.parse(fs.readFileSync(`${state.journal}.delivered`, "utf8")).ids).toEqual(["retire-once"]);
      state.settle();
      expect(probe.events.filter((event) => event === "payload-retirement")).toHaveLength(1);
      expect(state.sent).toHaveLength(1);
    } finally { retired.mockRestore(); probe.restore(); state.main.closeFollowUpDrain(); }
  });

  it.skipIf(process.platform === "win32").each(cases)("retains the resident source and acknowledges nothing on persistent session-only $namespace $operation $barrier failure, then acknowledges once", async ({ operation, barrier, symlink }) => {
    const root = rootDirectory();
    const { manager, directory, intermediate } = session(root, symlink);
    const id = "reply";
    const deliveryId = `resident:session:root:${id}`;
    const details = { id: "session-message", deliveryId };
    persist(manager, details);
    const state = controller(root, manager);
    const request = { from: actor, message: "reply", delivery: "steer" as const, deliveryId };
    if (operation === "replacement") {
      // Prime cached receipts, then replace at the SAME SIZE. Size-only caches must not
      // bypass namespace barriers or retain the old inode's receipt after replacement.
      expect(state.main.deliverAgent(request)).toMatchObject({ duplicate: true });
      const before = fs.statSync(manager.getSessionFile()!);
      replace(root, manager, details);
      const after = fs.statSync(manager.getSessionFile()!);
      expect(after.size).toBe(before.size);
      expect(after.ino).not.toBe(before.ino);
    }
    const mesh = new MeshStore(path.join(root, "mesh"), DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, DEFAULT_FABRIC_CONFIG.mesh.maxReadEvents);
    const key = `${residentDeliveryPrefix("session:root")}${id}`;
    await mesh.put({ key, identity: { id: residentHostId("session:root"), name: "resident", kind: "main" }, value: {
      format: RESIDENT_HOST_FORMAT, id, rootId: "session:root", from: actor,
      message: "reply", delivery: "steer", triggerTurn: true, createdAt: 1,
    } });
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:root", sessionId: "root", cwd: root, projectRoot: root,
      meshRoot: mesh.root, actorRoot: path.join(root, "mesh", "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
    };
    const probe = barrierProbe(manager.getSessionFile()!, barrier === "leaf" ? directory : path.dirname(directory));
    const deliver = state.main.deliverAgent.bind(state.main);
    const delivered = vi.spyOn(state.main, "deliverAgent").mockImplementation((value) => {
      const result = deliver(value); probe.events.push("acknowledgment"); return result;
    });
    const remove = mesh.delete.bind(mesh);
    const removed = vi.spyOn(mesh, "delete").mockImplementation(async (value) => {
      if (value.key === key) probe.events.push("source-delete");
      return remove(value);
    });
    const client = new ResidencyClient({ config, mesh, participants: {} as never, mainAgent: state.main });
    // This checks repeated barrier failure, not real-time retry backoff. Exercise the
    // actual client retry callbacks without spending the 5s observation on 1+2+4s waits.
    const timeout = globalThis.setTimeout;
    const retryClock = vi.spyOn(globalThis, "setTimeout").mockImplementation((callback, ms, ...args) =>
      timeout(callback, ms === 1000 || ms === 2000 || ms === 4000 ? 10 : ms, ...args));
    try {
      client.start();
      await waitFor(() => delivered.mock.results.length >= 3 || delivered.mock.results.some((result) => result.type === "return"));
      expect(delivered.mock.results.every((result) => result.type === "throw")).toBe(true);
      expect(mesh.get(key)).toBeDefined();
      expect(probe.events).not.toContain("acknowledgment");
      expect(probe.events).not.toContain("source-delete");
      expect(state.sent).toHaveLength(0);
      expect(fs.existsSync(state.journal)).toBe(false);
      probe.recover();
      await waitFor(() => mesh.get(key) === undefined);
      const chain = receiptChain(manager, intermediate);
      expect(probe.events.slice(0, chain.length + 1)).toEqual(["session-file", ...chain]);
      expect(probe.events.slice(chain.length + 1)).toEqual(["acknowledgment", "source-delete"]);
      expect(delivered.mock.results.filter((result) => result.type === "return")).toHaveLength(1);
      expect(delivered.mock.results.at(-1)).toMatchObject({ value: { duplicate: true } });
      expect(removed.mock.calls.filter(([value]) => value.key === key)).toHaveLength(1);
      expect(state.sent).toHaveLength(0);
    } finally {
      await client.close(); retryClock.mockRestore(); removed.mockRestore(); delivered.mockRestore(); probe.restore(); state.main.closeFollowUpDrain();
    }
  });

  it.skipIf(process.platform === "win32").each(["resolution", "opened-inode", "walk-replaced", "barrier-retarget", "barrier-replaced"])("fails closed on a symlink receipt %s mismatch and recovers without redelivery", (failure) => {
    const root = rootDirectory();
    const { manager } = session(root, true);
    const state = controller(root, manager);
    const file = manager.getSessionFile()!;
    const physical = fs.realpathSync(file);
    const other = path.join(root, "other.jsonl");
    fs.writeFileSync(other, fs.readFileSync(physical));
    const request = { from: actor, message: "reply", delivery: "steer" as const, deliveryId: "bind-once" };
    state.main.deliverAgent(request);
    persist(manager, state.sent[0]!.details);
    const readlink = fs.readlinkSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    let fail = true;
    const resolved = vi.spyOn(fs, "readlinkSync").mockImplementation((target, options) => {
      if (fail && String(target) === file) {
        if (failure === "resolution") throw new Error("receipt resolution unavailable");
        if (failure === "opened-inode") return other;
        if (failure === "walk-replaced") { fs.unlinkSync(file); fs.symlinkSync(physical, file); }
      }
      return readlink(target, options);
    });
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      sync(fd);
      if (fail && (failure === "barrier-retarget" || failure === "barrier-replaced") && fs.fstatSync(fd).isDirectory()) {
        fs.unlinkSync(file); fs.symlinkSync(failure === "barrier-retarget" ? other : physical, file);
      }
    });
    try {
      state.settle();
      expect(fs.readFileSync(state.journal, "utf8")).toContain("bind-once");
      expect(fs.existsSync(`${state.journal}.delivered`)).toBe(false);
      fail = false;
      if (failure === "barrier-retarget") { fs.unlinkSync(file); fs.symlinkSync(physical, file); }
      state.settle();
      expect(fs.existsSync(state.journal)).toBe(false);
      expect(JSON.parse(fs.readFileSync(`${state.journal}.delivered`, "utf8")).ids).toEqual(["bind-once"]);
      expect(state.sent).toHaveLength(1);
    } finally { synced.mockRestore(); resolved.mockRestore(); state.main.closeFollowUpDrain(); }
  });

  it("keeps Windows receipt handles writable but noncreating/nontruncating, and skips unsupported directory fsync", () => {
    const root = rootDirectory();
    const manager = SessionManager.create(root, path.join(root, "separate", "sessions"));
    persist(manager, { id: "entry", deliveryId: "windows-receipt" });
    const state = controller(root, manager);
    const file = manager.getSessionFile()!;
    const before = fs.readFileSync(file, "utf8");
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    const opened = vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) => {
      expect(String(target)).toBe(file);
      expect(flags).toBe("r+");
      return open(target, flags, mode);
    });
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      expect(fs.fstatSync(fd).isDirectory()).toBe(false);
      sync(fd);
    });
    try {
      Object.defineProperty(process, "platform", { value: "win32" });
      expect(state.main.deliverAgent({ from: actor, message: "reply", delivery: "steer", deliveryId: "windows-receipt" }))
        .toMatchObject({ duplicate: true });
      expect(opened).toHaveBeenCalledTimes(1);
      expect(synced).toHaveBeenCalledTimes(1);
    } finally {
      Object.defineProperty(process, "platform", platform);
      synced.mockRestore(); opened.mockRestore(); state.main.closeFollowUpDrain();
    }
    expect(fs.readFileSync(file, "utf8")).toBe(before);
  });

  it("invalidates cached receipts when the persisted session file disappears", () => {
    const root = rootDirectory();
    const manager = SessionManager.create(root, path.join(root, "separate", "sessions"));
    persist(manager, { id: "entry", deliveryId: "removed-receipt" });
    const state = controller(root, manager);
    const request = { from: actor, message: "reply", delivery: "steer" as const, deliveryId: "removed-receipt" };
    try {
      expect(state.main.deliverAgent(request)).toMatchObject({ duplicate: true });
      fs.rmSync(manager.getSessionFile()!);
      expect(state.main.deliverAgent(request).duplicate).toBeUndefined();
      expect(state.sent).toHaveLength(1);
      expect(fs.readFileSync(state.journal, "utf8")).toContain("removed-receipt");
    } finally { state.main.closeFollowUpDrain(); }
  });

  it("rescans a same-size replacement instead of trusting the previous inode's cached receipts", () => {
    const root = rootDirectory();
    const manager = SessionManager.create(root, path.join(root, "separate", "sessions"));
    persist(manager, { id: "entry-a", deliveryId: "receipt-a" });
    const state = controller(root, manager);
    const request = (deliveryId: string) => ({ from: actor, message: "reply", delivery: "steer" as const, deliveryId });
    try {
      expect(state.main.deliverAgent(request("receipt-a"))).toMatchObject({ duplicate: true });
      const before = fs.statSync(manager.getSessionFile()!).size;
      replace(root, manager, { id: "entry-b", deliveryId: "receipt-b" });
      expect(fs.statSync(manager.getSessionFile()!).size).toBe(before);
      expect(state.main.deliverAgent(request("receipt-b"))).toMatchObject({ duplicate: true });
      expect(state.main.deliverAgent(request("receipt-a")).duplicate).toBeUndefined();
      expect(state.sent).toHaveLength(1);
      expect(fs.readFileSync(state.journal, "utf8")).toContain("receipt-a");
    } finally { state.main.closeFollowUpDrain(); }
  });
});
