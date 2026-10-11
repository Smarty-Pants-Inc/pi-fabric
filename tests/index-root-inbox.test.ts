import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox } from "../src/topology/root-inbox.js";

const me: MeshIdentity = { id: "session:busy-main", name: "Main", kind: "main" };
const peer: MeshIdentity = { id: "session:peer", name: "Peer", kind: "main" };
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.restoreAllMocks();
});

// Exercise the registered index hooks with a real durable cursor and canonical session receipts.
// FollowUps stay queued while steers keep the run alive, as in Pi's native agent loop.
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-index-root-inbox-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  let now = Date.now();
  const inbox = new RootInbox(mesh, me, () => [me.id], { now: () => now, steerGraceMs: 0 });
  cleanups.push(async () => { await inbox.close(); mesh.closeState(); });
  inbox.start();
  const entries: any[] = [];
  const queued: any[] = [];
  const sessionFile = path.join(root, "session.jsonl");
  fs.writeFileSync(sessionFile, JSON.stringify({ type: "session", id: "busy-main" }) + "\n");
  const append = (message: any) => {
    const entry = { type: "custom_message", ...message };
    entries.push(entry);
    fs.appendFileSync(sessionFile, JSON.stringify(entry) + "\n");
  };
  const handlers = new Map<string, Array<(event: any, context: ExtensionContext) => unknown>>();
  const fake = {
    hostCapabilities: { turnProvenance: 1 },
    sendMessage: vi.fn((message: any, _options: any) => queued.push(message)),
    sendUserMessage: vi.fn(),
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []),
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(), setActiveTools: vi.fn(),
    on: (name: string, handler: (event: any, context: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(h => h !== handler));
    },
  };
  const context = {
    cwd: root, hasUI: false, isIdle: () => false, hasPendingMessages: () => queued.length > 0,
    getContextUsage: () => undefined,
    sessionManager: { getSessionId: () => "busy-main", getSessionFile: () => sessionFile,
      getEntries: () => entries, getBranch: () => entries },
    ui: { notify: vi.fn(), setStatus: vi.fn() },
  } as unknown as ExtensionContext;
  vi.spyOn(FabricState.prototype, "initialized", "get").mockReturnValue(true);
  vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "provisionalConfig").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  const maybeCommit = vi.fn(async () => {});
  vi.spyOn(FabricState.prototype, "compact", "get").mockReturnValue({ maybeCommit } as any);
  const lifecycle = vi.spyOn(FabricState.prototype, "publishHostLifecycle").mockResolvedValue(undefined);
  const reconcile = vi.spyOn(FabricState.prototype, "nextRootInbox").mockImplementation((session, _idle, options) => inbox.next(session, options));
  vi.spyOn(FabricState.prototype, "nextRecordsInboxMessage").mockResolvedValue(undefined);
  await piFabric(fake as unknown as ExtensionAPI);
  const emit = async (name: string, event: any = {}) => {
    if (name === "before_agent_start") {
      event.systemPromptOptions ??= { sections: {} };
    }
    for (const handler of handlers.get(name) ?? []) await handler(event, context);
  };
  const consume = () => { for (const message of queued.splice(0)) append(message); };
  const work = async (text: string) => {
    const event = await mesh.publish({ from: peer, to: me.id, topic: "fleet.work.task", kind: "handoff", text });
    now = Math.max(now, event.createdAt + 1);
    return event;
  };
  const cursor = () => mesh.get(inbox.key, { fresh: true })!.value as any;
  const ids = () => entries.flatMap(entry => entry.details?.ids ?? []);
  const turn = (stopReason = "stop", preview = true) => emit("turn_end", {
    message: { role: "assistant", stopReason },
    ...(preview ? { context: { pendingMessages: queued.map(message => ({ role: "custom", ...message })) } } : {}),
  });
  return { mesh, inbox, context, entries, fake, queued, emit, consume, work, cursor, ids, turn, reconcile,
    maybeCommit, lifecycle, advance: (ms: number) => { now += ms; } };
};

describe("index root inbox turn/settle reconciliation (#4313)", () => {
  it("commits held work and receives later events over 50 steer-driven turns without a completed settle", async () => {
    const h = await fixture();
    const first = await h.work("initial batch");
    await h.emit("before_agent_start"); h.consume();
    const later = await h.work("arrived inside the run");
    h.reconcile.mockClear(); h.fake.sendMessage.mockClear();
    for (let turn = 0; turn < 50; turn++) {
      await h.turn();
      expect(h.cursor().after).toBeGreaterThanOrEqual(first.sequence);
      // Steering delays followUp consumption: reconciliation must not queue it 50 times.
      if (turn < 49) expect(h.ids()).toEqual([first.id]);
    }
    expect(h.reconcile).toHaveBeenCalledTimes(50);
    expect(h.fake.sendMessage).toHaveBeenCalledTimes(1);
    expect(h.fake.sendMessage.mock.calls[0]?.[1]).toMatchObject({ deliverAs: "followUp", triggerTurn: true });
    h.consume();
    await h.turn();
    expect(h.cursor().pending).toBeUndefined();
    expect(h.ids()).toEqual([first.id, later.id]);
    expect(new Set(h.ids()).size).toBe(h.ids().length);
  });

  it.each(["turn", "settle", "late-signal", "failed-read"])("redelivers a followUp cancelled before its receipt at an aborted %s", async boundary => {
    const h = await fixture();
    const first = await h.work("held");
    await h.emit("before_agent_start"); h.consume();
    const pending = await h.work("cancelled queued followUp");
    const abort = new AbortController();
    if (boundary === "late-signal") Object.assign(h.context, { signal: abort.signal });
    await h.turn();
    expect(h.queued.flatMap(message => message.details.ids)).toEqual([pending.id]);
    h.queued.splice(0); // Host cancellation is not a canonical session receipt.
    h.fake.sendMessage.mockClear();
    if (boundary === "failed-read") {
      h.reconcile.mockRejectedValueOnce(new Error("cursor unavailable during cancellation"));
      await h.emit("agent_settled", { outcome: "aborted" });
    } else if (boundary === "turn") await h.turn("aborted");
    else if (boundary === "settle") await h.emit("agent_settled", { outcome: "aborted" });
    else {
      abort.abort(); Object.assign(h.context, { signal: undefined });
      // Legacy Pi hides both the outcome and the finished run signal at settle.
      await h.emit("agent_settled");
    }
    expect(h.fake.sendMessage).not.toHaveBeenCalled();
    expect(h.cursor().after).toBe(first.sequence);
    expect(h.cursor().pending.ids).toEqual([pending.id]);
    const newer = await h.work("behind the cancelled batch");
    await h.emit("before_agent_start"); h.consume();
    expect(h.ids()).toEqual([first.id, pending.id]);
    await h.turn(); h.consume(); await h.turn();
    expect(h.ids()).toEqual([first.id, pending.id, newer.id]);
    expect(h.cursor().pending).toBeUndefined();
    expect(new Set(h.ids()).size).toBe(3);
  });

  it.each([true, false])("expires a queued ID after more than one turn without a receipt or native queue evidence (preview=%s)", async preview => {
    const h = await fixture();
    await h.work("held"); await h.emit("before_agent_start"); h.consume();
    const pending = await h.work("lost queued followUp");
    await h.turn(); h.queued.splice(0);
    h.fake.sendMessage.mockClear();
    await h.turn("stop", preview);
    expect(h.fake.sendMessage).not.toHaveBeenCalled();
    await h.turn("stop", preview);
    expect(h.fake.sendMessage).toHaveBeenCalledTimes(1);
    expect(h.queued.flatMap(message => message.details.ids)).toEqual([pending.id]);
    h.consume(); await h.turn();
    expect(h.cursor().pending).toBeUndefined();
  });

  it.each(["completed", "failed", "error", "aborted", "provider-blocked"])("reconciles a %s settle, committing held work without retrying unsuccessful runs", async outcome => {
    const h = await fixture();
    const first = await h.work("held before settle");
    await h.emit("before_agent_start"); h.consume();
    const later = await h.work("newer than the held batch");
    h.fake.sendMessage.mockClear();
    await h.emit("agent_settled", { outcome });
    expect(h.cursor().after).toBeGreaterThanOrEqual(first.sequence);
    if (outcome === "completed") {
      expect(h.queued.flatMap(message => message.details.ids)).toEqual([later.id]);
    } else {
      expect(h.cursor().pending).toBeUndefined();
      expect(h.fake.sendMessage).not.toHaveBeenCalled();
      // The commit-only settle did not admit or advance past the newer work.
      await h.emit("before_agent_start"); h.consume();
      expect(h.ids()).toEqual([first.id, later.id]);
    }
  });

  it("commits in finally even when another settle operation throws", async () => {
    const h = await fixture();
    const first = await h.work("held");
    await h.emit("before_agent_start"); h.consume();
    await h.work("must not retry a failed settle hook");
    h.fake.sendMessage.mockClear();
    h.maybeCommit.mockRejectedValueOnce(new Error("compact failed"));
    await expect(h.emit("agent_settled", { outcome: "completed" })).rejects.toThrow("compact failed");
    expect(h.cursor().pending).toBeUndefined();
    expect(h.cursor().after).toBe(first.sequence);
    expect(h.fake.sendMessage).not.toHaveBeenCalled();
  });

  it.each(["error", "aborted"])("commits a held batch at an %s turn boundary without triggering a turn", async reason => {
    const h = await fixture();
    await h.work("held"); await h.emit("before_agent_start"); h.consume();
    await h.work("not an automatic retry"); h.fake.sendMessage.mockClear();
    await h.turn(reason);
    expect(h.cursor().pending).toBeUndefined();
    expect(h.fake.sendMessage).not.toHaveBeenCalled();
  });

  it("reconciles even when turn lifecycle publication throws", async () => {
    const h = await fixture();
    await h.work("held"); await h.emit("before_agent_start"); h.consume();
    h.lifecycle.mockRejectedValueOnce(new Error("lifecycle failed"));
    await expect(h.turn()).rejects.toThrow("lifecycle failed");
    expect(h.cursor().pending).toBeUndefined();
  });

  it("publishes one wedge alarm for an old pending range while the Main takes turns", async () => {
    const h = await fixture();
    const event = await h.work("followUp delayed by steering");
    await h.emit("before_agent_start"); // Deliberately leave the batch unrecorded.
    h.advance(15 * 60_000 + 1);
    for (let turn = 0; turn < 4; turn++) await h.turn();
    const alarms = h.mesh.read({ after: 0, limit: 100, topic: "fleet.alarm.root-inbox" });
    expect(alarms).toHaveLength(1);
    expect(alarms[0]?.data).toMatchObject({ rootId: me.id, pending: { after: 0, through: event.sequence, ids: [event.id] } });
    expect(h.fake.sendMessage).toHaveBeenCalledTimes(1);
  });
});
