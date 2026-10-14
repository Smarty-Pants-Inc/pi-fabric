import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { FabricState } from "../src/fabric-state.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";
import { RootInbox, rootInboxMessage, rootInboxSession, type RootInboxSession } from "../src/topology/root-inbox.js";
import { filterRootInboxContext } from "../src/topology/root-inbox-context.js";

// smarty-dev#8962: code-voice got one <fabric-inbox> event (e2d5cfb1, mesh seq 1132029) at least
// six times after it had consumed it. An event injected into a turn is acknowledged by id and never
// injected again: not by a read that raced the ack, not after a restart, not by a stale writer's
// re-scan from an older cursor, and never past the stale horizon on the pending path.
const peer: MeshIdentity = { id: "session:dev-lead", name: "dev-lead", kind: "main" };
// The canonical session file does not (yet) hold the injected message: the case consume() exists for.
const notHeld: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
const TOPIC = "fleet.work.voice";
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

/** A real Fabric Main runtime (mesh, root inbox, ack path) on a temp mesh root; Pi itself is faked. */
const mainRuntime = (root: string, sessionId: string) => {
  const sent: Array<{ details: { ids: string[]; receipts: string[] } }> = [];
  const pi = { on: vi.fn(() => () => {}), events: { emit: vi.fn() }, getThinkingLevel: () => "off",
    getSessionName: () => "code-voice", sendMessage: vi.fn((message: never) => sent.push(message)), appendEntry: vi.fn(),
  } as unknown as ExtensionAPI;
  const context = { cwd: root, mode: "rpc", hasUI: false, isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(root, "unused-extension.mjs"), worker: path.join(root, "unused-worker.mjs"),
    residentHost: path.join(root, "unused-resident.mjs"), skills: root,
  } });
  return { runtime, context, pi, sent };
};

/** A temp root and a mesh-only Fabric config for a real Main runtime. */
const runtimeRoot = (prefix: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: path.join(root, "mesh"), actorPollMs: 20 },
    agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false },
  });
  return { root, config };
};

describe("root inbox acknowledges injected events by id (smarty-dev#8962)", () => {
  it("never injects an acknowledged event again: not to a read that raced the ack, not after a restart", async () => {
    const { root, config } = runtimeRoot("fabric-inbox-ack-runtime-");
    const sessionId = "c0dec0de-0000-4000-8000-000000008962";
    const mainId = `session:${sessionId}`;
    const first = mainRuntime(root, sessionId);
    let firstUp = true;
    cleanups.push(async () => { if (firstUp) await first.runtime.shutdown(); });
    await first.runtime.initialize(first.context, config);
    const event = await first.runtime.mesh.publish({ topic: TOPIC, kind: "report", from: peer, to: mainId, text: "voice lane ready" });
    // Only bypass the inbox's existing 60-second steer grace, not publication or routing.
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      const batch = await first.runtime.nextRootInbox(notHeld);
      expect(batch?.events.map(value => value.id)).toEqual([event.id]);
      deliverRootInbox(first.pi, batch!.events);
      const { details } = first.sent[0]!;
      expect(details.ids).toEqual([event.id]);
      // The message enters a turn (message_end acks it) while another inbox read is in flight
      // (a turn-boundary reconcile or the idle wake). The ack must not be dropped by that read.
      const raced = first.runtime.nextRootInbox(notHeld);
      await first.runtime.consumeRootInbox(details.ids, details.receipts);
      expect((await raced)?.events ?? []).toEqual([]);
      for (let turn = 0; turn < 3; turn++) expect((await first.runtime.nextRootInbox(notHeld))?.events).toEqual([]);
    } finally { clock.mockRestore(); }
    // Restart: a new runtime for the same session on the same mesh re-reads the durable cursor.
    await first.runtime.shutdown();
    firstUp = false;
    const second = mainRuntime(root, sessionId);
    cleanups.push(() => second.runtime.shutdown());
    await second.runtime.initialize(second.context, config);
    const later = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      expect((await second.runtime.nextRootInbox(notHeld))?.events).toEqual([]);
      const fresh = await second.runtime.mesh.publish({ topic: TOPIC, kind: "report", from: peer, to: mainId, text: "new work" });
      later.mockReturnValue(Date.now() + 2 * 61_000);
      expect((await second.runtime.nextRootInbox(notHeld))?.events.map(value => value.id)).toEqual([fresh.id]);
    } finally { later.mockRestore(); }
  }, 30_000);
});

/**
 * The registered index hooks on a fake Pi. FabricState forwards its inbox surface to the runtime it
 * activated; here that is the real runtime given, so the real RootInbox state decides.
 */
const indexHooks = async (runtime: FabricRuntimeState, cwd: string, sessionId: string) => {
  const handlers = new Map<string, Array<(event: any, context: ExtensionContext) => unknown>>();
  const queued: any[] = [];
  const entries: any[] = [];
  const fake = {
    hostCapabilities: { turnProvenance: 1 },
    sendMessage: vi.fn((message: any) => queued.push(message)), sendUserMessage: vi.fn(),
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []),
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(), setActiveTools: vi.fn(),
    on: (name: string, handler: (event: any, context: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(h => h !== handler));
    },
  };
  const context = {
    cwd, hasUI: false, isIdle: () => false, hasPendingMessages: () => queued.length > 0, getContextUsage: () => undefined,
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getLeafId: () => null,
      getEntries: () => entries, getBranch: () => entries },
    ui: { notify: vi.fn(), setStatus: vi.fn() },
  } as unknown as ExtensionContext;
  vi.spyOn(FabricState.prototype, "initialized", "get").mockReturnValue(true);
  vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "provisionalConfig").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "compact", "get").mockReturnValue({ maybeCommit: vi.fn(async () => {}) } as any);
  vi.spyOn(FabricState.prototype, "publishHostLifecycle").mockResolvedValue(undefined);
  vi.spyOn(FabricState.prototype, "nextRecordsInboxMessage").mockResolvedValue(undefined);
  for (const name of ["nextRootInbox", "consumeRootInbox", "rootInboxAcked"]) if (name in FabricState.prototype) {
    vi.spyOn(FabricState.prototype as any, name).mockImplementation((...args: unknown[]) => (runtime as any)[name](...args));
  }
  await piFabric(fake as unknown as ExtensionAPI);
  const emit = async (name: string, event: any = {}) => {
    if (name === "before_agent_start") event.systemPromptOptions ??= { sections: {} };
    for (const handler of handlers.get(name) ?? []) await handler(event, context);
  };
  // Pi injects each queued message into a turn: message_end, then it is in the session.
  const inject = async () => {
    const injected = queued.splice(0).map(message => ({ role: "custom", ...message, timestamp: Date.now() }));
    for (const message of injected) {
      await emit("message_end", { message });
      entries.push({ type: "custom_message", id: `entry-${entries.length}`, ...message });
    }
    return injected;
  };
  // Pi's emitContext: each context handler sees the previous one's list; the last one goes to the model.
  const modelContext = async (messages: any[]) => {
    let current = messages;
    for (const handler of handlers.get("context") ?? []) {
      current = ((await handler({ type: "context", messages: current }, context)) as { messages?: any[] } | undefined)?.messages ?? current;
    }
    return current;
  };
  return { fake, queued, entries, emit, inject, modelContext };
};
const text = (messages: readonly any[]) => messages.map(message => typeof message.content === "string" ? message.content
  : (message.content ?? []).map((part: any) => part.text ?? "").join("")).join("\n");
/** How many times the model sees this inbox event's body. */
const shown = (messages: readonly any[], id: string) => text(messages).split(`<event id="${id}"`).length - 1;
const reply = (words: string) => ({ role: "assistant", content: [{ type: "text", text: words }], stopReason: "stop", timestamp: 0 });

describe("the context hook shows each inbox event to the model once (smarty-dev#8962)", () => {
  it("drops copies queued before the ack and a copy after it, and keeps a new event of a mixed batch", async () => {
    const { root, config } = runtimeRoot("fabric-inbox-ack-context-");
    const sessionId = "c0dec0de-0000-4000-8000-000000008963";
    const mainId = `session:${sessionId}`;
    const main = mainRuntime(root, sessionId);
    cleanups.push(() => main.runtime.shutdown());
    await main.runtime.initialize(main.context, config);
    const hooks = await indexHooks(main.runtime, root, sessionId);
    const event = await main.runtime.mesh.publish({ topic: TOPIC, kind: "report", from: peer, to: mainId, text: "voice lane ready" });
    // Only bypass the inbox's existing 60-second steer grace, not publication or routing.
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      // Copy 1 at turn start (nextTurn). The followUp lease expires after a further turn without
      // queue evidence, so turn_end queues copy 2: both are queued before either enters a turn.
      await hooks.emit("before_agent_start");
      await hooks.emit("turn_end", { message: { role: "assistant", stopReason: "stop" } });
      await hooks.emit("turn_end", { message: { role: "assistant", stopReason: "stop" } });
      expect(hooks.queued.map(message => message.details.ids)).toEqual([[event.id], [event.id]]);
      // Copy 1 enters a turn and its message_end acks the event; copy 2 is already queued and follows.
      const [first, second] = await hooks.inject();
      // A copy after the ack (a stale writer or an older build), alone and mixed with new work.
      deliverRootInbox(hooks.fake as unknown as ExtensionAPI, [event]);
      const fresh = await main.runtime.mesh.publish({ topic: TOPIC, kind: "report", from: peer, to: mainId, text: "new work" });
      deliverRootInbox(hooks.fake as unknown as ExtensionAPI, [event, fresh]);
      const [third, mixed] = await hooks.inject();
      expect(mixed!.details.ids).toEqual([event.id, fresh.id]);

      const sent = await hooks.modelContext([{ role: "user", content: "start", timestamp: 0 }, first, reply("on it"),
        second, reply("already done"), third, reply("already done"), mixed]);
      expect(shown(sent, event.id)).toBe(1);
      expect(shown(sent, fresh.id)).toBe(1);
      // The model keeps the copy where it first saw the event; the mixed batch keeps only the new event.
      expect(text([sent[1]])).toContain(`<event id="${event.id}"`);
      expect(sent.at(-1)).toMatchObject({ details: { ids: [fresh.id] } });
      expect(text([sent.at(-1)])).toContain(`<fabric-inbox count="1">`);

      // A request that ends on a repeated copy still ends on the user's side, without the event.
      const tail = await hooks.modelContext([{ role: "user", content: "start", timestamp: 0 }, first, reply("on it"), second]);
      expect(shown(tail, event.id)).toBe(1);
      expect(tail.at(-1)).toMatchObject({ role: "custom", details: { ids: [] } });

      // Compaction cut the copy that consumed the event: an acked event is not shown again at all.
      hooks.entries.push({ type: "compaction", id: "compaction", firstKeptEntryId: hooks.entries[2].id, summary: "earlier work" });
      const compacted = await hooks.modelContext([{ role: "compactionSummary", summary: "earlier work", tokensBefore: 1, timestamp: 0 },
        third, reply("already done"), mixed]);
      expect(shown(compacted, event.id)).toBe(0);
      expect(shown(compacted, fresh.id)).toBe(1);
    } finally { clock.mockRestore(); }
  }, 30_000);
});

describe("filterRootInboxContext (smarty-dev#8962)", () => {
  it("cuts repeated shown events while keeping withheld overflow eligible in a later copy", () => {
    const event = (n: number) => ({ id: `ev-${n}`, sequence: 100 + n, topic: TOPIC, kind: "report", from: peer,
      text: `body ${n}`, createdAt: 1_700_000_000_000 + n, data: {} }) as unknown as MeshEvent;
    // Ten events: eight shown and two withheld for a later bounded injection; only shown ids deduplicate.
    const first = { role: "custom", ...rootInboxMessage(Array.from({ length: 10 }, (_, n) => event(n))), timestamp: 1 };
    const later = { role: "custom", ...rootInboxMessage([event(3), event(9), event(20)]), timestamp: 2 };
    const result = filterRootInboxContext([first, reply("ok"), later], { acked: () => false, compactedCopies: () => new Map() });
    expect(first.details.ids).toEqual(Array.from({ length: 8 }, (_, n) => `ev-${n}`));
    expect(result.messages[0]).toBe(first);
    expect(result.messages[2]).toMatchObject({ details: { ids: ["ev-9", "ev-20"] } });
    expect(text([result.messages[2]])).toMatch(/^<fabric-inbox count="2">/);
    expect(shown(result.messages, "ev-3")).toBe(1);
    expect(shown(result.messages, "ev-9")).toBe(1);
    expect(shown(result.messages, "ev-20")).toBe(1);
  });
});

const me: MeshIdentity = { id: "session:code-voice", name: "code-voice", kind: "main" };
const setup = (options: { horizonMs?: number } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-ack-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  cleanups.push(() => { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const box = () => {
    const inbox = new RootInbox(mesh, me, () => [me.id], { now: () => now, steerGraceMs: 0, ...options });
    cleanups.push(() => inbox.close());
    return inbox;
  };
  const publish = (text = "shadow", data?: unknown) => mesh.publish({ topic: TOPIC, kind: "report", from: peer, to: me.id, text, data });
  return { mesh, box, publish, advance: (ms: number) => { now += ms; } };
};
const ids = (events: readonly MeshEvent[] | undefined) => (events ?? []).map(event => event.id);

describe("root inbox high-water dedup and stale re-delivery (smarty-dev#8962)", () => {
  it("does not re-inject an acknowledged event when a stale writer saves an older cursor and a restart re-scans", async () => {
    const h = setup();
    const before = h.box(); before.start();
    const event = await h.publish("voice lane ready"); h.advance(1);
    const batch = await before.next(notHeld);
    expect(ids(batch.events)).toEqual([event.id]);
    // A reload: the successor loads the pending batch and the injected message acks it there.
    const after = h.box();
    await after.consume([event.id]);
    expect((await after.next(notHeld)).events).toEqual([]);
    // The predecessor's in-flight turn-boundary reconcile lands late and saves its cached view
    // (pending batch, no receipt), because a follow-up receipt changed its state.
    const followUp = rootInboxSession([{ type: "custom_message", customType: "pi-fabric-agent-message",
      timestamp: new Date(Date.now()).toISOString(), details: { id: "fu-1", from: { id: peer.id } } }]);
    await before.next(followUp, { commitOnly: true });
    // A restart re-scans from whatever cursor is durable now: nothing at or below the mark again.
    for (let restart = 0; restart < 3; restart++) expect((await h.box().next(notHeld)).events).toEqual([]);
    const fresh = await h.publish("new work"); h.advance(1);
    expect(ids((await h.box().next(notHeld)).events)).toEqual([fresh.id]);
  });

  it("expires a pending event past the stale horizon on re-delivery, including one the log no longer returns", async () => {
    const h = setup({ horizonMs: 60_000 });
    const inbox = h.box(); inbox.start();
    const kept = await h.publish("kept"); const gone = await h.publish("gone"); h.advance(1);
    expect(ids((await inbox.next(notHeld)).events)).toEqual([kept.id, gone.id]);
    // The live log stops returning one pending event (retention/rotation): it is neither delivered nor expired yet.
    const read = h.mesh.read.bind(h.mesh);
    vi.spyOn(h.mesh, "read").mockImplementation(input => read(input).filter(event => event.id !== gone.id));
    h.advance(61_000);
    // Never acked, not held, past the horizon: re-delivery injects nothing and reports it stale once.
    const redelivery = await h.box().next(notHeld);
    expect(redelivery.events).toEqual([]);
    expect(redelivery.skippedStale).toBe(2);
    // The pending batch no longer wedges the cursor: new work arrives.
    const fresh = await h.publish("new work"); h.advance(1);
    expect(ids((await h.box().next(notHeld)).events)).toEqual([fresh.id]);
    expect(h.mesh.get(inbox.key, { fresh: true })?.value).not.toHaveProperty("pending.ids", [gone.id]);
  });
});
