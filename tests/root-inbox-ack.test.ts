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
import { archiveFileName, MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { hasDeliveryOutcome } from "../src/mesh/delivery-outcomes.js";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";
import { eventReceipt, eventReceipts } from "../src/topology/root-inbox-shared.js";
import { RootInbox, rootInboxMessage, rootInboxSession, type RootInboxSession } from "../src/topology/root-inbox.js";
import { filterRootInboxContext } from "../src/topology/root-inbox-context.js";
import { rootInboxMessage as issuedInboxMessage, verifyIssuedInbox } from "./root-inbox-issued-fixture.js";

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

describe("shown-only receipts joined with main's canonical extraction", () => {
  it("rejects unsigned inbox receipts but preserves admitted native carrier receipts", () => {
    const event = { id: "canonical-join", sequence: 1, topic: TOPIC, kind: "blocked", from: peer,
      text: "shown action", createdAt: Date.now() } as MeshEvent;
    const message = rootInboxMessage([event]);
    const timestamp = new Date().toISOString();
    const unsigned = rootInboxSession([{ type: "custom_message", timestamp, ...message }]);
    expect(unsigned.holdsBatch([event.id])).toBe(false);
    expect([...unsigned.delivered!]).toEqual([]);
    // Main #1226 records admitted native identity receipts independently of inbox issuance.
    const native = rootInboxSession([{ type: "custom_message", timestamp,
      customType: "pi-fabric-agent-message", details: { receipts: message.details.receipts } }]);
    for (const receipt of message.details.receipts) {
      expect(native.delivered!.has(receipt)).toBe(true);
      expect(native.deliveredAt!.get(receipt)).toBe(Date.parse(timestamp));
    }
  });

  it.each(["invalid", "2026-01-01", 0])("never renews an issued receipt with invalid canonical timestamp %s", timestamp => {
    const event = { id: `invalid-time-${timestamp}`, sequence: 1, topic: TOPIC, kind: "blocked", from: peer,
      text: "shown action", createdAt: Date.now() } as MeshEvent;
    const message = issuedInboxMessage([event]);
    const session = rootInboxSession([{ type: "custom_message", timestamp, ...message }], verifyIssuedInbox);
    expect(session.holdsBatch([event.id])).toBe(true); // Exact issued identity remains replay evidence.
    for (const receipt of message.details.receipts) {
      expect(session.delivered!.has(receipt)).toBe(true);
      expect(session.deliveredAt!.has(receipt)).toBe(false); // No scan-time legacy fallback authority.
    }
    const validAt = new Date().toISOString();
    const valid = rootInboxSession([{ type: "custom_message", timestamp: validAt, ...message }], verifyIssuedInbox);
    for (const receipt of message.details.receipts) expect(valid.deliveredAt!.get(receipt)).toBe(Date.parse(validAt));
  });
});

describe("root inbox acknowledges injected events by id (smarty-dev#8962)", () => {
  it("automatically retries a generic EIO on the idle watch without another publication (pi-fabric#2010 F4)", async () => {
    const { root, config } = runtimeRoot("fabric-inbox-eio-watch-");
    const sessionId = "c0dec0de-0000-4000-8000-000000002010";
    const main = mainRuntime(root, sessionId);
    cleanups.push(() => main.runtime.shutdown());
    await main.runtime.initialize(main.context, config);
    const event = await main.runtime.mesh.publish({ topic: TOPIC, kind: "blocked", from: peer,
      to: `session:${sessionId}`, text: "retry without another publication" });
    const sequence = main.runtime.mesh.latestSequence();
    // Keep the notification lane quiescent: the watch still owns a real one-shot timer,
    // but observes an unchanged directory so incidental file notifications cannot rescue EIO.
    const quiet = path.join(root, "quiet-watch"); fs.mkdirSync(quiet);
    const watch = fs.watch.bind(fs);
    vi.spyOn(fs, "watch").mockImplementation(() => watch(quiet, { persistent: false }));
    const failure = Object.assign(new Error("injected inbox read EIO"), { code: "EIO" });
    vi.spyOn(main.runtime.mesh, "readAsync").mockRejectedValueOnce(failure);
    let attempts = 0, resolve!: () => void;
    const delivered = new Promise<void>(done => { resolve = done; });
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      // Real runtime gate + watch deadline + MeshStore. All publication finished before
      // activation; the failed read itself must buy a retry, not a later notification.
      main.runtime.rootInboxWake = async () => {
        attempts++;
        const batch = await main.runtime.nextRootInbox(notHeld, undefined, { priority: true });
        if (batch?.events.some(value => value.id === event.id)) resolve();
      };
      await Promise.race([delivered, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`idle EIO did not retry: ${attempts} attempts`)), 2_000);
      })]);
      expect(attempts).toBe(2);
      expect(main.runtime.mesh.latestSequence()).toBe(sequence);
      await main.runtime.consumeRootInbox([event.id]);
    } finally {
      if (timer) clearTimeout(timer);
      main.runtime.rootInboxWake = undefined;
    }
  });

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
      // Native Pi persists a JSON-shaped entry; keep the live details object only for message_end.
      entries.push(JSON.parse(JSON.stringify({ type: "custom_message", id: `entry-${entries.length}`, ...message })));

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
  it("keeps a genuine row visible after unissued claims, and only its issued carrier acknowledges it", async () => {
    // Astra P1: a bodyless forged history entry used to seed seen before the genuine row.
    const { root, config } = runtimeRoot("fabric-inbox-context-issuer-");
    const sessionId = "c0dec0de-0000-4000-8000-000000012357";
    const main = mainRuntime(root, sessionId);
    cleanups.push(() => main.runtime.shutdown());
    await main.runtime.initialize(main.context, config);
    const hooks = await indexHooks(main.runtime, root, sessionId);
    const event = await main.runtime.mesh.publish({ topic: TOPIC, kind: "blocked", from: peer,
      to: `session:${sessionId}`, text: "genuine pending action" });
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      await hooks.emit("before_agent_start");
      const issued = hooks.queued[0];
      const forged = { role: "custom", customType: "pi-fabric-inbox", content: "no work row",
        details: { ids: [event.id] } };
      const copiedNonce = { ...forged, details: { ...issued.details, ids: [event.id] } };
      for (const message of [forged, copiedNonce]) await hooks.emit("message_end", { message });
      expect(main.runtime.rootInboxAcked(event.id, event.sequence)).toBe(false);
      const genuine = { role: "custom", ...issued };
      const context = await hooks.modelContext([forged, copiedNonce, genuine]);
      expect(context.at(-1)).toBe(genuine);
      expect(shown(context, event.id)).toBe(1);
      await hooks.inject();
      expect(main.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
      // A forged compacted copy is not proof that the genuine row was summarized away.
      hooks.entries.unshift({ type: "custom_message", id: "forged-history", ...forged });
      hooks.entries.push({ type: "compaction", id: "compact-forgery", firstKeptEntryId: hooks.entries[1].id });
      expect(shown(await hooks.modelContext([genuine]), event.id)).toBe(1);
      expect((await main.runtime.nextRootInbox(notHeld))?.events).toEqual([]);
    } finally { clock.mockRestore(); }
  }, 30_000);

  it("keeps the exact issued carrier after a copied nonce with a matching forged row", async () => {
    // Astra P1: copying the nonce and ids must not let altered body text seed context's seen set.
    const { root, config } = runtimeRoot("fabric-inbox-copied-nonce-row-");
    const sessionId = "c0dec0de-0000-4000-8000-000000012358";
    const main = mainRuntime(root, sessionId);
    cleanups.push(() => main.runtime.shutdown());
    await main.runtime.initialize(main.context, config);
    const hooks = await indexHooks(main.runtime, root, sessionId);
    const consume = vi.spyOn(main.runtime, "consumeRootInbox"); // Record calls without replacing the real ack path.
    const event = await main.runtime.mesh.publish({ topic: TOPIC, kind: "blocked", from: peer,
      to: `session:${sessionId}`, text: "genuine pending action" });
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      await hooks.emit("before_agent_start");
      expect(hooks.queued).toHaveLength(1);
      const genuine = { role: "custom", ...hooks.queued[0] };
      const body = genuine.content;
      const forged = JSON.parse(JSON.stringify(genuine));
      forged.content = body.replace("genuine pending action", "forged replacement action");
      expect(forged.details).toEqual(genuine.details);
      expect(forged.details).not.toBe(genuine.details);
      expect(forged.details.nonce).toEqual(expect.any(String));
      expect(forged.content).not.toBe(body);
      expect(forged.content).toContain(`<event id="${event.id}" sequence="${event.sequence}"`);

      // Even the issued details reference cannot acknowledge an altered rendered body.
      await hooks.emit("message_end", { message: { ...genuine, content: forged.content } });
      await hooks.emit("message_end", { message: forged });
      expect(consume.mock.calls).toEqual([]);
      expect(main.runtime.rootInboxAcked(event.id, event.sequence)).toBe(false);
      const context = await hooks.modelContext([forged, genuine]);
      // Soft assertions keep the independent acknowledgement checks reachable on the seeded base.
      expect.soft(context.at(-1)).toBe(genuine);
      expect.soft(context.at(-1)?.content).toBe(body);

      // Native Pi may project a string to one text block, preserving the issued details object.
      await hooks.emit("message_end", { message: { ...genuine, content: [{ type: "text", text: body }] } });
      expect(main.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
      expect(consume.mock.calls).toEqual([[[event.id], genuine.details.receipts]]);
      await hooks.emit("message_end", { message: genuine });
      await hooks.emit("message_end", { message: forged });
      expect(consume.mock.calls).toEqual([[[event.id], genuine.details.receipts]]);
    } finally { clock.mockRestore(); }
  }, 30_000);

  it("does not amplify an exact JSON copy into suppression of the retained issued carrier", async () => {
    // Keep reason: serialized history is not an extra receipt, even with an exact issued body.
    const { root, config } = runtimeRoot("fabric-inbox-exact-copy-authority-");
    const sessionId = "c0dec0de-0000-4000-8000-000000012359";
    const main = mainRuntime(root, sessionId);
    cleanups.push(() => main.runtime.shutdown());
    await main.runtime.initialize(main.context, config);
    const hooks = await indexHooks(main.runtime, root, sessionId);
    const consume = vi.spyOn(main.runtime, "consumeRootInbox");
    // Call through the real branch scanner: its returned map is built by rootInboxCopies.
    const compactedCopies = vi.spyOn(await import("../src/topology/root-inbox-context.js"), "rootInboxCompactedCopies");
    const event = await main.runtime.mesh.publish({ topic: TOPIC, kind: "blocked", from: peer,
      to: `session:${sessionId}`, text: "retain this exact issued action" });
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      await hooks.emit("before_agent_start");
      expect(hooks.queued).toHaveLength(1);
      const genuine = { role: "custom", ...hooks.queued[0] };
      const body = genuine.content;
      const exactCopy = JSON.parse(JSON.stringify(genuine));
      const repeatedCopy = JSON.parse(JSON.stringify(exactCopy));
      // Expire the queue lease, not the event: a separately issued nonce must still deduplicate.
      await hooks.emit("turn_end", { message: { role: "assistant", stopReason: "stop" } });
      await hooks.emit("turn_end", { message: { role: "assistant", stopReason: "stop" } });
      expect(hooks.queued).toHaveLength(2);
      const independent = { role: "custom", ...hooks.queued[1] };
      expect(independent.details.ids).toEqual([event.id]);
      expect(independent.details.nonce).not.toBe(genuine.details.nonce);
      expect(exactCopy).toEqual(genuine);
      expect(exactCopy.details).not.toBe(genuine.details);
      // Current native Pi isolates each message_end handler draft. Its authentic detached draft
      // is indistinguishable from this exact JSON copy: nonce AND issued body authorize the same
      // private shown-only snapshot once, never extra work or extra suppression authority.
      for (const message of [exactCopy, repeatedCopy]) await hooks.emit("message_end", { message });
      expect(consume.mock.calls).toEqual([[[event.id], genuine.details.receipts]]);
      expect(main.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
      // One nonce cannot gain extra suppression authority by appearing twice, nor exhaust a later scan.
      for (let scan = 0; scan < 2; scan++) {
        const context = await hooks.modelContext([exactCopy, repeatedCopy, genuine]);
        expect.soft(context.at(-1)).toBe(genuine);
        expect.soft(context.at(-1)?.content).toBe(body);
        expect.soft(shown(context, event.id)).toBe(1);
        const issuedOnly = await hooks.modelContext([genuine, independent]);
        expect.soft(issuedOnly[0]).toBe(genuine);
        expect.soft(shown(issuedOnly, event.id)).toBe(1);
      }

      await hooks.emit("message_end", { message: genuine });
      expect(main.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
      for (const [id, message] of [["exact-copy-1", exactCopy], ["exact-copy-2", repeatedCopy],
        ["genuine-kept", genuine]] as const) {
        hooks.entries.push(JSON.parse(JSON.stringify({ type: "custom_message", id,
          timestamp: new Date().toISOString(), ...message })));
      }
      hooks.entries.push({ type: "compaction", id: "compact-exact-copies", firstKeptEntryId: "genuine-kept" });
      // The two earlier occurrences of that same nonce cannot manufacture a compacted consuming copy
      // while the actual issued carrier remains in context. Projection keeps only that last carrier.
      for (let scan = 0; scan < 2; scan++) {
        const context = await hooks.modelContext([
          { role: "compactionSummary", summary: "earlier history", tokensBefore: 1, timestamp: 0 }, genuine,
        ]);
        expect.soft(context.at(-1)).toBe(genuine);
        expect.soft(context.at(-1)?.content).toBe(body);
      }
      // Now genuinely compact away the first issued carrier, leaving the independent nonce.
      // Three occurrences of the first nonce (two JSON copies and its original) prove ONE copy,
      // and that proof still suppresses the independently issued duplicate on each fresh scan.
      hooks.entries.pop();
      hooks.entries.push(JSON.parse(JSON.stringify({ type: "custom_message", id: "independent-kept",
        timestamp: new Date().toISOString(), ...independent })));
      hooks.entries.push({ type: "compaction", id: "compact-consuming-carrier", firstKeptEntryId: "independent-kept" });
      for (let scan = 0; scan < 2; scan++) {
        compactedCopies.mockClear();
        const context = await hooks.modelContext([
          { role: "compactionSummary", summary: "consumed action", tokensBefore: 1, timestamp: 0 }, independent,
        ]);
        expect.soft(shown(context, event.id)).toBe(0);
        expect.soft(compactedCopies.mock.results.at(-1)?.value?.get(event.id)).toBe(1);
      }
      for (const message of [exactCopy, repeatedCopy, genuine]) await hooks.emit("message_end", { message });
      expect(consume.mock.calls).toEqual([[[event.id], genuine.details.receipts]]);
    } finally { clock.mockRestore(); }
  }, 30_000);

  it("starts a new extension instance without authority from historic issued carriers", async () => {
    // Keep reason: generation-local issuer authority must not leak through serialized session history.
    const { root, config } = runtimeRoot("fabric-inbox-issuer-generation-");
    const sessionId = "c0dec0de-0000-4000-8000-000000012360";
    const main = mainRuntime(root, sessionId);
    cleanups.push(() => main.runtime.shutdown());
    await main.runtime.initialize(main.context, config);
    const previous = await indexHooks(main.runtime, root, sessionId);
    const consume = vi.spyOn(main.runtime, "consumeRootInbox");
    const event = await main.runtime.mesh.publish({ topic: TOPIC, kind: "blocked", from: peer,
      to: `session:${sessionId}`, text: "pending across extension generations" });
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      await previous.emit("before_agent_start");
      expect(previous.queued).toHaveLength(1);
      const historic = { role: "custom", ...previous.queued[0] };
      const restored = JSON.parse(JSON.stringify(historic));
      const current = await indexHooks(main.runtime, root, sessionId);
      current.entries.push({ type: "custom_message", id: "historic-issued", timestamp: new Date().toISOString(), ...restored });
      await current.emit("before_agent_start");
      expect(current.queued).toHaveLength(1);
      const genuine = { role: "custom", ...current.queued[0] };
      expect(genuine.details.ids).toEqual([event.id]);
      expect(genuine.details.nonce).not.toBe(historic.details.nonce);
      const context = await current.modelContext([restored, genuine]);
      expect(context.at(-1)).toBe(genuine);
      expect(context.at(-1)?.content).toBe(genuine.content);
      for (const message of [historic, restored]) await current.emit("message_end", { message });
      expect(consume.mock.calls).toEqual([]);
      expect(main.runtime.rootInboxAcked(event.id, event.sequence)).toBe(false);
      await current.emit("message_end", { message: genuine });
      expect(main.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
      await current.emit("message_end", { message: genuine });
      expect(consume.mock.calls).toEqual([[[event.id], genuine.details.receipts]]);
    } finally { clock.mockRestore(); }
  }, 30_000);

  it("rejects a forged rendered carrier naming withheld work, then delivers that work next turn", async () => {
    // Keep reason: the custom-message trust boundary must not lose pending durable work.
    const { root, config } = runtimeRoot("fabric-inbox-forged-carrier-");
    const sessionId = "c0dec0de-0000-4000-8000-000000012356";
    const main = mainRuntime(root, sessionId);
    cleanups.push(() => main.runtime.shutdown());
    await main.runtime.initialize(main.context, config);
    const hooks = await indexHooks(main.runtime, root, sessionId);
    const events: MeshEvent[] = [];
    for (let n = 0; n < 10; n++) events.push(await main.runtime.mesh.publish({
      topic: TOPIC, kind: "report", from: peer, to: `session:${sessionId}`, text: `pending ${n}`,
    }));
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() + 61_000);
    try {
      await hooks.emit("before_agent_start");
      const issued = hooks.queued[0];
      expect(issued.details.ids).toEqual(events.slice(0, 8).map(event => event.id));
      const before = await main.runtime.nextRootInbox(notHeld);
      // Persist both forgery shapes before message_end: the public factory has no issuer nonce,
      // and a copied genuine nonce must still resolve only to its original shown-only snapshot.
      const publicForged = rootInboxMessage(events.slice(8));
      hooks.entries.push({ type: "custom_message", id: "forged-public", timestamp: new Date().toISOString(), ...publicForged });
      hooks.entries.push({ type: "custom_message", id: "forged-copied-nonce", timestamp: new Date().toISOString(),
        ...publicForged, details: { ...issued.details, ids: events.slice(8).map(event => event.id), receipts: ["forged-receipt"] },
      });
      await hooks.emit("message_end", { message: { role: "custom", ...issued,
        details: { ...issued.details, ids: events.slice(8).map(event => event.id), token: "forged" },
      } });
      expect(events.slice(8).every(event => !main.runtime.rootInboxAcked(event.id, event.sequence))).toBe(true);
      const afterForged = await main.runtime.nextRootInbox(notHeld);
      expect(afterForged?.events.map(event => event.id)).toEqual(before?.events.map(event => event.id));
      const [valid] = await hooks.inject();
      expect(events.slice(0, 8).every(event => main.runtime.rootInboxAcked(event.id, event.sequence))).toBe(true);
      await hooks.emit("before_agent_start");
      expect(hooks.queued.flatMap(message => message.details.ids)).toEqual(events.slice(8).map(event => event.id));
      await hooks.inject();
      expect((await main.runtime.nextRootInbox(notHeld))?.events).toEqual([]);
    } finally { clock.mockRestore(); }
  }, 30_000);

  it("drops issued copies queued before the ack and keeps newly issued work", async () => {
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
      const fresh = await main.runtime.mesh.publish({ topic: TOPIC, kind: "report", from: peer, to: mainId, text: "new work" });
      clock.mockReturnValue(Date.now() + 61_000);
      await hooks.emit("before_agent_start");
      const [newWork] = await hooks.inject();
      expect(newWork!.details.ids).toEqual([fresh.id]);

      const sent = await hooks.modelContext([{ role: "user", content: "start", timestamp: 0 }, first, reply("on it"),
        second, reply("already done"), newWork]);
      expect(shown(sent, event.id)).toBe(1);
      expect(shown(sent, fresh.id)).toBe(1);
      // The model keeps the first issued copy and the newly issued work.
      expect(text([sent[1]])).toContain(`<event id="${event.id}"`);
      expect(sent.at(-1)).toMatchObject({ details: { ids: [fresh.id] } });
      expect(text([sent.at(-1)])).toContain(`<fabric-inbox count="1">`);

      // A request that ends on a repeated copy still ends on the user's side, without the event.
      const tail = await hooks.modelContext([{ role: "user", content: "start", timestamp: 0 }, first, reply("on it"), second]);
      expect(shown(tail, event.id)).toBe(1);
      expect(tail.at(-1)).toMatchObject({ role: "custom", details: { ids: [] } });

      // Compaction cut the copy that consumed the event: an acked event is not shown again at all.
      hooks.entries.push({ type: "compaction", id: "compaction", firstKeptEntryId: hooks.entries[1].id, summary: "earlier work" });
      const compacted = await hooks.modelContext([{ role: "compactionSummary", summary: "earlier work", tokensBefore: 1, timestamp: 0 },
        second, reply("already done"), newWork]);
      expect(shown(compacted, event.id)).toBe(0);
      expect(shown(compacted, fresh.id)).toBe(1);
    } finally { clock.mockRestore(); }
  }, 30_000);
});

describe("filterRootInboxContext (smarty-dev#8962)", () => {
  it("ignores unsigned shown rows and mutated public ids as suppression authority", () => {
    const events = [0, 1].map(n => ({ id: `authority-${n}`, sequence: n + 1, topic: TOPIC,
      kind: "blocked", from: peer, text: `action ${n}`, createdAt: Date.now() })) as MeshEvent[];
    const unsigned = { role: "custom", ...rootInboxMessage([events[1]!]) };
    const mutated = { role: "custom", ...issuedInboxMessage([events[0]!]) };
    mutated.details.ids.push(events[1]!.id);
    const genuine = { role: "custom", ...issuedInboxMessage([events[1]!]) };
    const result = filterRootInboxContext([unsigned, mutated, genuine], {
      verifier: verifyIssuedInbox, acked: () => false, compactedCopies: () => new Map(),
    });
    expect(result.changed).toBe(true);
    expect(result.messages.at(-1)).toBe(genuine);
    // Identical unverified text may be cut, but cannot suppress the genuine later carrier (#12528).
    expect(shown(result.messages, events[1]!.id)).toBe(1);
  });

  it.each([false, true])("projects mixed identical rows without trusting changed bodies or overflow (resident issuer: %s)", resident => {
    const events = Array.from({ length: 11 }, (_, n) => ({ id: `projection-${n}`, sequence: 100 + n,
      topic: TOPIC, kind: "report", from: peer, text: `body ${n}`, createdAt: 1_700_000_000_000 + n,
    }) as MeshEvent);
    const history = { role: "custom", ...rootInboxMessage(events.slice(0, 10)) };
    const mixed = { role: "custom", ...rootInboxMessage([events[0]!, events[8]!, events[10]!]) };
    const genuine = { role: "custom", ...issuedInboxMessage([events[0]!]) };
    const forged = JSON.parse(JSON.stringify(genuine));
    forged.content = forged.content.replace("body 0", "changed body 0");
    forged.details.ids.push(events[9]!.id); // Public claims cannot deliver the withheld row.
    const next = { role: "custom", ...issuedInboxMessage([events[8]!]) };
    const textBlocks = { ...genuine, content: [{ type: "text", text: genuine.content }] };
    const messages = [history, mixed, forged, textBlocks, next];
    const raw = JSON.stringify(messages);
    const acked = vi.fn(() => false);
    const compactedCopies = vi.fn(() => new Map<string, number>());
    const options = { verifier: resident ? verifyIssuedInbox : () => undefined, acked, compactedCopies };
    const result = filterRootInboxContext(messages, options);
    expect(result.messages).toContain(textBlocks); // Prefer the real carrier over all copied claims.
    expect(result.messages.at(-1)).toBe(next);
    expect(result.messages).toContain(forged); // A changed body is not an identical rendered row.
    expect(shown(result.messages, events[0]!.id)).toBe(2); // Changed claim plus genuine work, never loss.
    for (const event of [...events.slice(1, 9), events[10]!]) expect(shown(result.messages, event.id)).toBe(1);
    expect(text([result.messages[0]])).toContain('<fabric-inbox count="9" shown="7">');
    expect(text([result.messages[0]])).toContain(history.content.match(/<overflow[\s\S]*?<\/overflow>/)![0]);
    expect(text([result.messages[1]])).toContain('<fabric-inbox count="1">');
    expect(shown(result.messages, events[9]!.id)).toBe(0);
    expect(acked.mock.calls).not.toContainEqual([events[9]!.id, events[9]!.sequence]);
    expect(acked.mock.calls).not.toContainEqual([events[10]!.id, events[10]!.sequence]);
    if (!resident) expect(acked).not.toHaveBeenCalled();
    expect(compactedCopies).not.toHaveBeenCalled();
    const withheld = { role: "custom", ...issuedInboxMessage([events[9]!]) };
    const later = filterRootInboxContext([...messages, withheld], options);
    expect(later.messages.at(-1)).toBe(withheld);
    expect(shown(later.messages, events[9]!.id)).toBe(1);
    expect(JSON.stringify(messages)).toBe(raw);
  });

  it("preserves the pending summary when an earlier carrier duplicates a shown row", () => {
    const events = Array.from({ length: 10 }, (_, n) => ({ id: `ev-${n}`, sequence: 100 + n,
      topic: TOPIC, kind: "report", from: peer, text: `body ${n}`, createdAt: 1_700_000_000_000 + n,
    }) as MeshEvent);
    const earlier = { role: "custom", ...issuedInboxMessage([events[0]!]) };
    const later = { role: "custom", ...issuedInboxMessage(events) };
    const pending = { role: "custom", ...issuedInboxMessage(events.slice(8)) };
    const acked = vi.fn(() => false);
    const result = filterRootInboxContext([earlier, later, pending], { verifier: verifyIssuedInbox, acked, compactedCopies: () => new Map() });
    expect(text([result.messages[1]])).toContain('<fabric-inbox count="9" shown="7">');
    expect(text([result.messages[1]])).toContain(later.content.match(/<overflow[\s\S]*?<\/overflow>/)![0]);
    expect(text([result.messages[1]])).toContain('<overflow count="2">+2 more');
    expect(result.messages[1]).toMatchObject({ details: { ids: events.slice(1, 8).map(event => event.id) } });
    for (const event of events) expect(shown(result.messages, event.id)).toBe(1);
    expect(result.messages[2]).toBe(pending);
    expect(later.details.ids).toEqual(events.slice(0, 8).map(event => event.id));
    expect(acked.mock.calls).not.toContainEqual([events[8]!.id, events[9]!.sequence]);
  });

  it("cuts repeated shown events while keeping withheld overflow eligible in a later copy", () => {
    const event = (n: number) => ({ id: `ev-${n}`, sequence: 100 + n, topic: TOPIC, kind: "report", from: peer,
      text: `body ${n}`, createdAt: 1_700_000_000_000 + n, data: {} }) as unknown as MeshEvent;
    // Ten events: eight shown and two withheld for a later bounded injection; only shown ids deduplicate.
    const first = { role: "custom", ...issuedInboxMessage(Array.from({ length: 10 }, (_, n) => event(n))), timestamp: 1 };
    const later = { role: "custom", ...issuedInboxMessage([event(3), event(9), event(20)]), timestamp: 2 };
    const result = filterRootInboxContext([first, reply("ok"), later], { verifier: verifyIssuedInbox, acked: () => false, compactedCopies: () => new Map() });
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
const setup = (options: { horizonMs?: number; pageSize?: number } = {}) => {
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

describe("root inbox cold archive consumption (pi-fabric#2010 RFL F1)", () => {
  // catch: https://github.com/Smarty-Pants-Inc/pi-fabric/pull/2010#issuecomment-6999716694
  // RFL's real-archive probe: notification admission is not a durable ack obligation.
  it.each(["transient read", "persistent read", "transient put", "persistent put"] as const)(
    "close retries the cold ack obligation after %s EIO, or rejects truthfully", async failure => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-eio-close-"));
      const meshRoot = path.join(root, "mesh"), archive = path.join(root, "archive");
      fs.mkdirSync(meshRoot);
      const now = Date.now();
      const events: MeshEvent[] = Array.from({ length: 30 }, (_, i) => ({
        id: `eio-close-${i + 1}`, sequence: i + 1, topic: TOPIC, kind: i === 21 ? "blocked" : "report",
        from: peer, to: me.id, createdAt: now, text: `row ${i} ${"x".repeat(2048)}`,
        data: { deliveryId: `work-${i + 1}` },
      }));
      const file = path.join(archive, "2026/01/01", archiveFileName(TOPIC));
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, events.map(event => JSON.stringify(event) + "\n").join(""));
      fs.writeFileSync(path.join(meshRoot, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
      fs.writeFileSync(path.join(archive, "MESH.json"), JSON.stringify({ version: 1, firstSequence: 1, meshRoot }));
      fs.writeFileSync(path.join(meshRoot, "events.jsonl"), events.slice(-8).map(event => JSON.stringify(event) + "\n").join(""));
      fs.writeFileSync(path.join(meshRoot, "sequence"), "30");
      const mesh = new MeshStore(meshRoot, 64 * 1024, 500, { stateBackend: "file" });
      const box = () => new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 });
      const first = box(), cold = box();
      cleanups.push(() => { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
      cleanups.push(async () => { await cold.close(); await first.close(); });
      await mesh.put({ key: first.key, identity: me, value: { after: 0 } });
      const event = events[21]!;
      expect(ids((await first.next(notHeld)).events)).toContain(event.id);
      cold.start();
      const eio = Object.assign(new Error(`injected cold-ack ${failure} EIO`), { code: "EIO" });
      const persistent = failure.startsWith("persistent");
      const fault = failure.endsWith("read") ? vi.spyOn(mesh, "readAsync") : vi.spyOn(mesh, "put");
      if (persistent) fault.mockRejectedValue(eio); else fault.mockRejectedValueOnce(eio);
      // The historical queue API may fulfil after the failed first attempt. Close must
      // independently retry the still-owed durable state, not merely its recovery read.
      await cold.consume([event.id], eventReceipts(event));
      try {
        if (persistent) await expect(cold.close()).rejects.toThrow(eio.message);
        else {
          await cold.close();
          const persisted = mesh.get(cold.key, { fresh: true })!.value as { acked: Array<[string, number]> };
          expect(persisted.acked).toContainEqual([event.id, event.sequence]);
          mesh.closeState();
          const reopenedMesh = new MeshStore(meshRoot, 64 * 1024, 500, { stateBackend: "file" });
          const reopened = new RootInbox(reopenedMesh, me, () => [me.id], { steerGraceMs: 0 });
          try {
            expect(ids((await reopened.next(notHeld)).events)).not.toContain(event.id);
          } finally { await reopened.close(); reopenedMesh.closeState(); }
        }
      } finally { fault.mockRestore(); }
    });

  // catch: https://github.com/Smarty-Pants-Inc/pi-fabric/pull/2010
  // Real storage, not delayed/mock pages: append invalidates the warmed archive range identity.
  it.each(["offered", "reloaded"] as const)("%s ack does not synchronously rescan a 50 MB archive after an append", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-cold-ack-"));
    const meshRoot = path.join(root, "mesh"), archive = path.join(root, "archive");
    const at = Date.UTC(2026, 0, 1);
    const event = (sequence: number): MeshEvent => ({
      id: `cold-ack-${sequence}`, sequence, topic: TOPIC, kind: "report", from: peer, to: me.id,
      createdAt: at, text: "x".repeat(12 * 1024), data: { deliveryId: `cold-work-${sequence}` },
    });
    fs.mkdirSync(meshRoot);
    const file = path.join(archive, "2026/01/01", archiveFileName(TOPIC));
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const events = Array.from({ length: 4096 }, (_, index) => event(index + 1));
    fs.writeFileSync(file, events.map(value => JSON.stringify(value) + "\n").join(""));
    const archiveBytes = fs.statSync(file).size;
    expect(archiveBytes).toBeGreaterThan(50_000_000);
    fs.writeFileSync(path.join(meshRoot, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir: archive }));
    fs.writeFileSync(path.join(archive, "MESH.json"), JSON.stringify({ version: 1, firstSequence: 1, meshRoot }));
    const live = path.join(meshRoot, "events.jsonl"), sequenceFile = path.join(meshRoot, "sequence");
    fs.writeFileSync(live, events.slice(-8).map(value => JSON.stringify(value) + "\n").join(""));
    fs.writeFileSync(sequenceFile, "4096");
    const mesh = new MeshStore(meshRoot, 64 * 1024, 500, { stateBackend: "file" });
    cleanups.push(() => { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
    const box = () => {
      const inbox = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0, now: () => at + 1 });
      cleanups.push(() => inbox.close());
      return inbox;
    };
    const first = box();
    await mesh.put({ key: first.key, identity: me, value: { after: 0 } });
    const batch = await first.next(notHeld);
    expect(batch.events.length).toBeGreaterThan(0);
    const message = rootInboxMessage(batch.events);
    const inbox = mode === "offered" ? first : box();
    // A complete committed append, including the live suffix and publication high-water.
    const appended = JSON.stringify(event(4097)) + "\n";
    fs.appendFileSync(file, appended);
    fs.appendFileSync(live, appended);
    fs.writeFileSync(sequenceFile, "4097");
    const start = performance.now();
    inbox.ack(message.details.ids, message.details.receipts);
    // ack is synchronous: this entire call is exactly one main-thread execution section,
    // excluding all fixture setup and subsequent awaited recovery/durability work.
    const synchronousMs = performance.now() - start;
    console.info("cold-archive-ack", JSON.stringify({ mode, archiveBytes, synchronousMs, ids: message.details.ids }));
    for (const id of message.details.ids) expect(inbox.acked(id)).toBe(true);
    // 50 ms allows >6x the reader's 8 ms slice target but rejects RFL's ~250 ms scan.
    expect(synchronousMs, `${mode} ack synchronous section: ${synchronousMs.toFixed(1)} ms`).toBeLessThan(50);
    await inbox.consume(message.details.ids, message.details.receipts);
    const reloaded = box(); reloaded.start();
    for (const id of message.details.ids) {
      expect(reloaded.acked(id, events.find(value => value.id === id)!.sequence)).toBe(true);
      expect(hasDeliveryOutcome(meshRoot, { eventId: id, to: me.id, from: peer.id, mode: "publish" }, "delivered")).toBe(true);
    }
    expect(ids((await reloaded.next(notHeld)).events).some(id => message.details.ids.includes(id))).toBe(false);
  }, 30_000);
});

describe("root inbox high-water dedup and stale re-delivery (smarty-dev#8962)", () => {
  // Keep: a canonical-receipt callback may land after a page was collected but before cursor commit.
  // Gate real async reads; do not make receipt visibility depend on machine speed or a timer.
  it("rechecks a timestamped canonical receipt learned by another reader across a page await", async () => {
    const h = setup({ pageSize: 1 });
    const inbox = h.box();
    inbox.start();
    const event = await h.publish();
    const deferred = () => {
      let resolve!: () => void;
      const promise = new Promise<void>(done => { resolve = done; });
      return { promise, resolve };
    };
    const aPaused = deferred(), bPaused = deferred(), releaseA = deferred(), releaseB = deferred();
    const read = h.mesh.readAsync.bind(h.mesh);
    let starts = 0, pausedA = false;
    vi.spyOn(h.mesh, "readAsync").mockImplementation(async input => {
      const page = await read(input);
      if (input?.after === 0 && ++starts === 2) { bPaused.resolve(); await releaseB.promise; }
      if (input?.after === event.sequence && !pausedA) { pausedA = true; aPaused.resolve(); await releaseA.promise; }
      return page;
    });
    let a: Promise<unknown> | undefined, b: Promise<unknown> | undefined;
    try {
      const first = inbox.next(notHeld);
      a = first;
      await aPaused.promise;
      const receipt = eventReceipt(event.id);
      b = inbox.next({ ...notHeld, delivered: new Set([receipt]), deliveredAt: new Map([[receipt, Date.now()]]) });
      await bPaused.promise;
      releaseA.resolve();
      expect(ids((await first).events)).not.toContain(event.id);
    } finally {
      releaseA.resolve(); releaseB.resolve();
      await Promise.allSettled([a, b].filter((p): p is Promise<unknown> => p !== undefined));
    }
    const reloaded = h.box();
    reloaded.start();
    expect(ids((await reloaded.next(notHeld)).events)).not.toContain(event.id);
  });
  it.each(["next", "wake"] as const)("%s rechecks an ack and its high-water mark across archive-read awaits", async mode => {
    const h = setup();
    const inbox = h.box(); inbox.start();
    const event = await h.publish("ack during asynchronous recovery"); h.advance(1);
    expect(ids((await inbox.next(notHeld)).events)).toEqual([event.id]);
    const read = h.mesh.readAsync.bind(h.mesh);
    let entered!: () => void, resume!: () => void;
    const reading = new Promise<void>(resolve => { entered = resolve; });
    const gate = new Promise<void>(resolve => { resume = resolve; });
    vi.spyOn(h.mesh, "readAsync").mockImplementationOnce(async input => {
      const page = await read(input);
      entered(); await gate;
      return page;
    });
    const pending = mode === "next" ? inbox.next(notHeld) : inbox.wake(notHeld, () => true);
    try {
      await reading;
      await inbox.consume([event.id]);
      resume();
      expect(ids((await pending)?.events)).toEqual([]);
      expect((h.mesh.get(inbox.key, { fresh: true })!.value as { after: number }).after).toBeGreaterThanOrEqual(event.sequence);
      expect((await h.box().next(notHeld)).events).toEqual([]);
    } finally { resume(); await pending; }
  });

  it("never persists a cold ack's fallback bound when it lands at a save microtask boundary", async () => {
    const h = setup(), first = h.box(); first.start();
    const events: MeshEvent[] = [];
    for (let index = 0; index < 24; index++) events.push(await h.publish(`work ${index}`));
    await first.next(notHeld);
    const cold = h.box(); cold.start();
    const consumed = events[21]!;
    const marker = "new-canonical-receipt";
    const saving = cold.next({ ...notHeld, delivered: new Set([marker]),
      deliveredAt: new Map([[marker, Date.now()]]) }, { commitOnly: true });
    // The save is already queued. An unconditional await of an empty recovery pass would
    // let this ack enter its snapshot without resolving the original sequence/outcome.
    queueMicrotask(() => cold.ack([consumed.id]));
    await saving;
    const saved = () => (h.mesh.get(cold.key, { fresh: true })!.value as { acked?: Array<[string, number]> }).acked ?? [];
    const during = saved();
    await cold.next(notHeld, { commitOnly: true });
    expect([...during, ...saved()]).not.toContainEqual([consumed.id, events.at(-1)!.sequence]);
    expect(saved()).toContainEqual([consumed.id, consumed.sequence]);
    expect(hasDeliveryOutcome(h.mesh.root, { eventId: consumed.id, to: me.id, from: peer.id, mode: "publish" }, "delivered")).toBe(true);
  });

  it("retains cold ack recovery after a read failure and persists the actual lookahead sequence", async () => {
    const h = setup();
    const first = h.box(); first.start();
    const events: MeshEvent[] = [];
    for (let index = 0; index < 24; index++) events.push(await h.mesh.publish({
      topic: TOPIC, kind: index === 21 ? "blocked" : "report", from: peer, to: me.id, text: `work ${index}`,
    }));
    const offered = await first.next(notHeld);
    const lookahead = events[21]!;
    expect(ids(offered.events)).toContain(lookahead.id);
    // This successor has only durable pending/ack state, not the predecessor's offer cache.
    const cold = h.box(); cold.ack([lookahead.id]);
    expect(cold.acked(lookahead.id)).toBe(true);
    const error = Object.assign(new Error("cold receipt read EIO"), { code: "EIO" });
    vi.spyOn(h.mesh, "readAsync").mockRejectedValueOnce(error);
    await expect(cold.next(notHeld, { commitOnly: true })).rejects.toThrow("cold receipt read EIO");
    // Failed recovery leaves the exact id suppressed in memory and keeps its pre-ack bounds.
    expect(cold.acked(lookahead.id)).toBe(true);
    await cold.next(notHeld, { commitOnly: true });
    const state = h.mesh.get(cold.key, { fresh: true })!.value as { acked: Array<[string, number]> };
    expect(state.acked).toContainEqual([lookahead.id, lookahead.sequence]);
    expect(state.acked).not.toContainEqual([lookahead.id, events.at(-1)!.sequence]);
    const reloaded = h.box(); reloaded.start();
    expect(reloaded.acked(lookahead.id, lookahead.sequence)).toBe(true);
    expect(ids((await reloaded.next(notHeld)).events)).not.toContain(lookahead.id);
    expect(hasDeliveryOutcome(h.mesh.root, { eventId: lookahead.id, to: me.id, from: peer.id, mode: "publish" }, "delivered")).toBe(true);
  });

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
    const read = h.mesh.readAsync.bind(h.mesh);
    vi.spyOn(h.mesh, "readAsync").mockImplementation(async input => (await read(input)).filter(event => event.id !== gone.id));
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
