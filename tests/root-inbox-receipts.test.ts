import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import * as diagnostics from "../src/core/diagnostics.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { FabricState } from "../src/fabric-state.js";
import {
  confirmedRootInboxSession, rootInboxCarrierDigest,
  rootInboxReceiptIssuer, registerRootInboxReceipt, verifyRootInboxReceipt,
} from "../src/topology/root-inbox-shared.js";

// Keep reasons: the recorded native-handler isolation regression, and the receipt trust
// boundary. Use the registered extension hooks and real durable inbox, not an issuer test double.
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  await diagnostics.flushFabricDiagnostics?.();
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});
const notHeld = { holdsBatch: () => false, holdsSteer: () => false };
const sessionId = "11111111-1111-4111-8111-111111111111";
const identity = { id: `session:${sessionId}`, name: "main", kind: "main" as const, sessionId };
const peer = { id: "session:peer", name: "peer", kind: "main" as const };
type Carrier = { role: "custom"; customType: string; content: string; details: { nonce: string; ids: string[]; receipts: string[] } };
const clone = <T>(value: T): T => JSON.parse(JSON.stringify(value));
const shown = (messages: readonly any[], id: string) => JSON.stringify(messages).split(`<event id=\\"${id}\\"`).length - 1;

const setup = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-native-receipts-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }));
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  diagnostics.configureFabricDiagnostics(sessionId);
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: path.join(root, "mesh"), actorPollMs: 20 },
    agents: { enabled: false }, residency: { enabled: false }, records: { enabled: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false },
  });
  vi.spyOn(FabricState.prototype, "initialized", "get").mockReturnValue(true);
  vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "provisionalConfig").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "compact", "get").mockReturnValue({ maybeCommit: vi.fn(async () => {}) } as any);
  vi.spyOn(FabricState.prototype, "publishHostLifecycle").mockResolvedValue(undefined);
  vi.spyOn(FabricState.prototype, "nextRecordsInboxMessage").mockResolvedValue(undefined);
  // Every generation gets a new real runtime and private extension issuer, on the same mesh.
  const start = async (manager?: ExtensionContext["sessionManager"]) => {
    const handlers = new Map<string, Array<(event: any, context: ExtensionContext) => unknown>>();
    const queued: Carrier[] = [];
    const entries: any[] = [];
    const fake = {
      hostCapabilities: { turnProvenance: 1 },
      sendMessage: vi.fn((message: any) => queued.push({ role: "custom", ...message })), sendUserMessage: vi.fn(),
      events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
      getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []), getThinkingLevel: () => "off",
      getSessionName: () => "main", appendEntry: vi.fn(),
      registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(), setActiveTools: vi.fn(),
      on: (name: string, handler: (event: any, context: ExtensionContext) => unknown) => {
        handlers.set(name, [...(handlers.get(name) ?? []), handler]);
        return () => {};
      },
    } as unknown as ExtensionAPI;
    const context = {
      cwd: root, mode: "rpc", hasUI: false, isProjectTrusted: () => true,
      isIdle: () => false, hasPendingMessages: () => queued.length > 0, getContextUsage: () => undefined,
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: manager ?? { getSessionId: () => sessionId, getSessionFile: () => path.join(root, "deferred-session.jsonl"),
        isPersisted: () => true, getLeafId: () => null, getEntries: () => entries, getBranch: () => entries },
      ui: { notify: vi.fn(), setStatus: vi.fn() },
    } as unknown as ExtensionContext;
    const runtime = new FabricRuntimeState(fake, new CapturedToolCatalog(), { paths: {
      extension: path.join(root, "unused-extension.mjs"), worker: path.join(root, "unused-worker.mjs"),
      residentHost: path.join(root, "unused-resident.mjs"), skills: root,
    } });
    let stopped = false;
    const stop = async () => { if (!stopped) { stopped = true; await runtime.shutdown(); } };
    cleanups.push(stop);
    await runtime.initialize(context, config);
    for (const name of ["nextRootInbox", "consumeRootInbox", "rootInboxAcked"] as const) {
      vi.spyOn(FabricState.prototype, name).mockImplementation((...args: any[]) => (runtime[name] as Function)(...args));
    }
    await piFabric(fake);
    const emit = async (name: string, event: any = {}) => {
      if (name === "before_agent_start") event.systemPromptOptions ??= { sections: {} };
      for (const handler of handlers.get(name) ?? []) await handler(event, context);
    };
    const modelContext = async (messages: any[]) => {
      let current = clone(messages); // Native emitContext isolates its messages too.
      for (const handler of handlers.get("context") ?? []) {
        current = ((await handler({ type: "context", messages: current }, context)) as { messages?: any[] } | undefined)?.messages ?? current;
      }
      return current;
    };
    const issue = async () => { await emit("before_agent_start"); return queued.splice(0); };
    const oneIssued = async (): Promise<Carrier> => {
      const messages = await issue();
      expect(messages).toHaveLength(1);
      const message = messages[0];
      if (!message) throw new Error("expected one issued inbox carrier");
      return message;
    };
    const publish = (text: string) => runtime.mesh.publish({ topic: "fleet.work.receipts", kind: "blocked", from: peer, to: identity.id, text });
    const callback = async (message: unknown) => {
      // Installed Pi's emitMessageEnd calls isolateValue for EACH handler, not just each event.
      for (const handler of handlers.get("message_end") ?? []) await handler({ type: "message_end", message: clone(message) }, context);
    };
    const bind = async (reason: "startup" | "reload") => {
      // Exercise the registered session_start handler's actual custody bind/teardown. Stop at the
      // independent bootstrap boundary: this fixture already owns a real initialized runtime.
      const boundary = new Error("receipt test reached bootstrap boundary");
      const bootstrap = vi.spyOn(FabricState.prototype, "bootstrap").mockRejectedValueOnce(boundary);
      try { await expect(emit("session_start", { reason })).rejects.toBe(boundary); }
      finally { bootstrap.mockRestore(); }
    };
    return { runtime, context, entries, emit, modelContext, issue, oneIssued, publish, callback, stop, bind };
  };
  const first = await start();
  // Exact-session priority admission intentionally bypasses the ordinary native-steer grace.
  return { first, start };
};

describe("resident root inbox issuer custody", () => {
  it("retains bounded private snapshots, consumption and leases only for the same-manager reload", () => {
    const manager = {};
    const original = rootInboxReceiptIssuer(manager, sessionId);
    const carrier = { content: "authentic shown body", details: { ids: ["shown"], receipts: ["shown-receipt"], nonce: "" } };
    registerRootInboxReceipt(original, carrier.details, carrier.content);
    const detached = clone(carrier);
    const snapshot = verifyRootInboxReceipt(original, detached.details, detached, new Set())!;
    original.consumed.add(snapshot);
    original.queued.set("shown", 7);
    original.turn = 7;
    const reloaded = rootInboxReceiptIssuer(manager, sessionId, true);
    expect(reloaded).toBe(original);
    expect(reloaded.queued.get("shown")).toBe(7);
    expect(reloaded.turn).toBe(7); // Lease age must not restart at zero or become negative.
    expect(reloaded.consumed.has(snapshot)).toBe(true);
    // Lifetime consumption never burns canonical/context scan authority.
    for (let scan = 0; scan < 2; scan++) {
      const accepted = new Set<string>();
      expect(verifyRootInboxReceipt(reloaded, detached.details, detached, accepted)).toBe(snapshot);
      expect(verifyRootInboxReceipt(reloaded, detached.details, detached, accepted)).toBeUndefined();
    }
    // A restarted host/process with the same public session id has no resident custody.
    const otherManager = {};
    const restarted = rootInboxReceiptIssuer(otherManager, sessionId, true);
    expect(verifyRootInboxReceipt(restarted, detached.details, detached, new Set())).toBeUndefined();
    const changedSession = rootInboxReceiptIssuer(manager, "another-session", true);
    expect(verifyRootInboxReceipt(changedSession, detached.details, detached, new Set())).toBeUndefined();
    const notReload = rootInboxReceiptIssuer(otherManager, sessionId);
    expect(notReload).not.toBe(restarted);
    rootInboxReceiptIssuer(manager); // Release custody without a receiving session identity.
    const afterQuit = rootInboxReceiptIssuer(manager, sessionId, true);
    expect(afterQuit).not.toBe(original);
    expect(verifyRootInboxReceipt(afterQuit, detached.details, detached, new Set())).toBeUndefined();
    rootInboxReceiptIssuer(manager);
    rootInboxReceiptIssuer(otherManager);
  });

  it("does not burn a nonce on tampering and evicts old snapshots without trusting public digests", () => {
    const issuer = rootInboxReceiptIssuer();
    const first = { content: "first shown body", details: { ids: ["first"], receipts: ["receipt"], nonce: "" } };
    registerRootInboxReceipt(issuer, first.details, first.content);
    const accepted = new Set<string>();
    const forged = { ...clone(first), content: "changed", digest: rootInboxCarrierDigest(first) };
    expect(verifyRootInboxReceipt(issuer, forged.details, forged, accepted)).toBeUndefined();
    expect(accepted.has(first.details.nonce)).toBe(false);
    expect(verifyRootInboxReceipt(issuer, first.details, first, accepted)?.ids).toEqual(["first"]);
    let latest = first;
    for (let index = 0; index < 2_048; index++) {
      latest = { content: `body ${index}`, details: { ids: [`id-${index}`], receipts: [], nonce: "" } };
      registerRootInboxReceipt(issuer, latest.details, latest.content);
    }
    expect(issuer.receipts.size).toBe(2_048);
    expect(verifyRootInboxReceipt(issuer, first.details, first, new Set())).toBeUndefined();
    expect(verifyRootInboxReceipt(issuer, latest.details, latest, new Set())?.ids).toEqual(["id-2047"]);
  });
});

describe("native root inbox receipt isolation", () => {
  it("restores a queued carrier through the registered same-manager session_start reload handler", async () => {
    const { first, start } = await setup();
    await first.bind("startup");
    const event = await first.publish("native-held queue survives idle reload");
    const queued = await first.oneIssued();
    expect(first.runtime.rootInboxAcked(event.id, event.sequence)).toBe(false);
    const managerView = first.context.sessionManager;
    // This is an actual teardown handler, not a direct registry clear/restore helper call.
    await first.emit("session_shutdown", { reason: "reload" });
    await first.stop();
    const reloaded = await start(managerView);
    expect(reloaded.context.sessionManager).toBe(managerView);
    await reloaded.bind("reload");
    // The native queue still holds the original issuance. Its retained lease prevents issuing
    // another copy before it enters a turn; its detached callback must authenticate after reload.
    expect(await reloaded.issue()).toEqual([]);
    const consume = vi.spyOn(reloaded.runtime, "consumeRootInbox");
    await reloaded.callback(clone(queued));
    expect(consume.mock.calls).toEqual([[[event.id], queued.details.receipts]]);
    expect(reloaded.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
    await reloaded.callback(clone(queued));
    expect(consume).toHaveBeenCalledTimes(1);
    expect(shown(await reloaded.modelContext([queued]), event.id)).toBe(1);
    expect(await reloaded.issue()).toEqual([]);
  }, 30_000);

  it("commits a confirmed canonical receipt even if immediate consumption never ran", async () => {
    const { first } = await setup();
    const consume = vi.spyOn(first.runtime, "consumeRootInbox");
    const event = await first.publish("canonical fallback for the isolated callback");
    const issued = await first.oneIssued();
    // Isolate the fallback that masked the native-handler isolation bug in ordinary successful turns. No message_end ack.
    expect(first.runtime.rootInboxAcked(event.id, event.sequence)).toBe(false);
    fs.writeFileSync(first.context.sessionManager.getSessionFile()!, `${JSON.stringify({
      type: "custom_message", id: "canonical", timestamp: new Date().toISOString(), ...clone(issued),
    })}\n`);
    await first.emit("turn_end", { message: { role: "assistant", stopReason: "stop" } });
    expect(consume).not.toHaveBeenCalled();
    expect(first.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
    expect(await first.issue()).toEqual([]);
    expect((await first.runtime.nextRootInbox(notHeld))?.events).toEqual([]);
  }, 30_000);

  it("consumes a native detached carrier once through turns, compaction and a fresh issuer", async () => {
    const { first, start } = await setup();
    const consume = vi.spyOn(first.runtime, "consumeRootInbox");
    const event = await first.publish("native isolated followUp");
    const issued = await first.oneIssued();
    expect(issued.details.ids).toEqual([event.id]);
    const detached = clone(issued);
    expect(detached.details).not.toBe(issued.details);
    expect(detached.details.nonce).toBe(issued.details.nonce);
    expect(rootInboxCarrierDigest(detached)).toBe(rootInboxCarrierDigest(issued));
    await first.callback(detached);
    // Native message_end is the same delivery boundary as on the legacy reference-passing host.
    // No canonical receipt exists to mask an immediate-consumer failure.
    expect(consume.mock.calls).toEqual([[[event.id], issued.details.receipts]]);
    expect(first.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
    first.entries.push({ type: "custom_message", id: "original", timestamp: new Date().toISOString(), ...clone(issued) });
    expect(confirmedRootInboxSession(first.context.sessionManager).holdsBatch([event.id])).toBe(false);
    const nativeTextBlocks = { ...detached, content: [{ type: "text", text: detached.content }] };
    expect(shown(await first.modelContext([nativeTextBlocks]), event.id)).toBe(1);
    expect(first.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
    expect(consume.mock.calls).toEqual([[[event.id], issued.details.receipts]]);
    for (let turn = 0; turn < 3; turn++) {
      await first.callback(detached);
      expect(shown(await first.modelContext([nativeTextBlocks]), event.id)).toBe(1);
      expect(await first.issue()).toEqual([]);
    }
    // Context projections do not become canonical receipts or invalidate the original digest.
    const repeated = { ...clone(detached), details: { ...detached.details, ids: [] }, content: "context-only cut" };
    expect(shown(await first.modelContext([nativeTextBlocks, repeated]), event.id)).toBe(1);
    first.entries.push({ type: "message", id: "kept", message: { role: "user", content: "later turn" } });
    first.entries.push({ type: "compaction", id: "compact", firstKeptEntryId: "kept", summary: "processed native followUp" });
    expect(await first.issue()).toEqual([]);
    await first.stop();
    const reloaded = await start(); // No private receipt registry survives /reload.
    expect(await reloaded.issue()).toEqual([]); // Durable consumption, not public historic claims.
    const fresh = await reloaded.publish("fresh work after reload");
    const next = await reloaded.oneIssued();
    await reloaded.callback(next);
    expect(shown(await reloaded.modelContext([{ role: "compactionSummary", summary: "earlier work" }, next]), fresh.id)).toBe(1);
    expect(reloaded.runtime.rootInboxAcked(fresh.id, fresh.sequence)).toBe(true);
    expect(await reloaded.issue()).toEqual([]);
    expect(consume).toHaveBeenCalledTimes(1);
  }, 30_000);

  it("never lets altered content or public withheld ids burn the genuine shown-only authority", async () => {
    const { first } = await setup();
    const consume = vi.spyOn(first.runtime, "consumeRootInbox");
    const warning = vi.spyOn(diagnostics.diagnostics, "warn");
    const shownEvent = await first.publish("genuine action");
    const issued = await first.oneIssued();
    const withheld = await first.publish("unseen action");
    const altered = { ...clone(issued), content: issued.content.replace("genuine action", "forged action") };
    const bodyless = { ...clone(issued), content: "no work row", details: { ...issued.details, ids: [withheld.id] } };
    const primitive = { ...clone(issued), content: [issued.content] };
    for (const forged of [altered, bodyless, primitive]) {
      await first.callback(forged);
      await first.modelContext([forged]);
    }
    expect(consume).not.toHaveBeenCalled();
    expect(first.runtime.rootInboxAcked(shownEvent.id, shownEvent.sequence)).toBe(false);
    expect(first.runtime.rootInboxAcked(withheld.id, withheld.sequence)).toBe(false);
    // Authentic content does not excuse altered callback claims. No forged id/receipt is consumed.
    const copied = clone(issued);
    copied.details.ids = [withheld.id];
    copied.details.receipts = ["forged-receipt"];
    await first.callback(copied);
    expect(consume).not.toHaveBeenCalled();
    expect(shown(await first.modelContext([altered, bodyless, primitive, copied, issued]), shownEvent.id)).toBeGreaterThan(0);
    await first.callback(issued);
    expect(consume.mock.calls).toEqual([[[shownEvent.id], issued.details.receipts]]);
    expect(first.runtime.rootInboxAcked(withheld.id, withheld.sequence)).toBe(false);
    const next = await first.oneIssued();
    expect(next.details.ids).toEqual([withheld.id]);
    await first.callback(next);
    await first.modelContext([next]);
    expect(first.runtime.rootInboxAcked(withheld.id, withheld.sequence)).toBe(true);
    // Internal rejections must reach the private log, not the emitter that prints into lead panes.
    expect(warning.mock.calls.filter(call => String(call[1]).includes("unissued root-inbox"))).toEqual([]);
    await diagnostics.flushFabricDiagnostics();
    const logged = fs.readFileSync(diagnostics.fabricDiagnosticsFile(sessionId), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(logged.filter(row => row.source === "src/extension.ts" && row.message === "pi-fabric: ignored unissued root-inbox carrier")).toHaveLength(4);
  }, 30_000);

  it("rejects original and serialized historic receipts in a fresh issuer, then recovers once", async () => {
    const { first, start } = await setup();
    const event = await first.publish("not yet received by a model");
    const historic = await first.oneIssued();
    // Cancelled while still queued: there was no native message_end or confirmed canonical append.
    expect(first.runtime.rootInboxAcked(event.id, event.sequence)).toBe(false);
    await first.stop();
    const reloaded = await start();
    const consume = vi.spyOn(reloaded.runtime, "consumeRootInbox");
    // Neither original in-process objects nor canonical JSON copies import issuer authority.
    reloaded.entries.push({ type: "custom_message", id: "historic", ...clone(historic) });
    for (const previous of [historic, clone(historic)]) await reloaded.callback(previous);
    await reloaded.modelContext([historic]);
    expect(consume).not.toHaveBeenCalled();
    expect(reloaded.runtime.rootInboxAcked(event.id, event.sequence)).toBe(false);
    const current = await reloaded.oneIssued();
    expect(current.details.ids).toEqual([event.id]);
    expect(current.details.nonce).not.toBe(historic.details.nonce);
    // A forged historic copy cannot hide the newly issued genuine action, even after compaction.
    reloaded.entries.push({ type: "compaction", id: "compact-old", firstKeptEntryId: "missing", summary: "untrusted claims" });
    const projected = await reloaded.modelContext([historic, current]);
    expect(projected.at(-1)?.content).toBe(current.content);
    expect(consume).not.toHaveBeenCalled();
    await reloaded.callback(current);
    await reloaded.modelContext([current]);
    await reloaded.callback(current);
    await reloaded.modelContext([current]);
    expect(consume.mock.calls).toEqual([[[event.id], current.details.receipts]]);
    expect(await reloaded.issue()).toEqual([]);
  }, 30_000);

  it("retries a failed durable consumption rather than burning the detached receipt", async () => {
    const { first } = await setup();
    const event = await first.publish("retry durable ack");
    const issued = await first.oneIssued();
    const consume = vi.spyOn(first.runtime, "consumeRootInbox").mockRejectedValueOnce(new Error("temporary durable-write failure"));
    await first.callback(issued);
    await first.modelContext([issued]);
    expect(first.runtime.rootInboxAcked(event.id, event.sequence)).toBe(false);
    await first.callback(issued);
    await first.modelContext([issued]);
    expect(first.runtime.rootInboxAcked(event.id, event.sequence)).toBe(true);
    await first.modelContext([issued]);
    expect(consume).toHaveBeenCalledTimes(2);
    expect(await first.issue()).toEqual([]);
  }, 30_000);
});
