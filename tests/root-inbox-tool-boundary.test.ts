import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager,
  type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import piFabric from "../src/index.js";
import { FabricState } from "../src/fabric-state.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox } from "../src/topology/root-inbox.js";
import { deliverRootInbox, pendingRootInboxCarriers, type RootInboxCarrier, type RootInboxCarrierOptions } from "../src/topology/root-inbox-delivery.js";
import { eventReceipt, registerRootInboxReceipt, rootInboxReceiptIssuer, verifyRootInboxReceipt } from "../src/topology/root-inbox-shared.js";

// Keep reason: #18131/#1495, a real starvation failure and an authenticated ACK trust boundary.
const me: MeshIdentity = { id: "session:boundary-main", name: "Main", kind: "main" };
const peer: MeshIdentity = { id: "session:worker", name: "Worker", kind: "main" };
const cleanup: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  vi.restoreAllMocks();
});
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
type Handler = (event: any, context: ExtensionContext) => any;

// Actual extension inbox hooks, actual durable RootInbox and actual consumeInjectedInbox.
// UI/startup/other product lifecycle hooks are not part of this focused session adapter.
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-boundary-"));
  cleanup.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const inbox = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 });
  inbox.start();
  cleanup.push(async () => { await inbox.close(); mesh.closeState(); });
  const handlers = new Map<string, Handler[]>();
  const entries: any[] = [];
  const sent: Array<{ message: RootInboxCarrier; options: RootInboxCarrierOptions }> = [];
  let nativeSend: ((message: RootInboxCarrier, options: RootInboxCarrierOptions) => void) | undefined;
  let installing = true;
  const pi = {
    hostCapabilities: { turnProvenance: 1 },
    sendMessage: vi.fn((message: RootInboxCarrier, options: RootInboxCarrierOptions) => {
      sent.push({ message, options });
      nativeSend?.(message, options);
    }),
    sendUserMessage: vi.fn(),
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: vi.fn(() => []), getAllTools: vi.fn(() => []), getThinkingLevel: () => "off",
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(), setActiveTools: vi.fn(),
    on: (name: string, handler: Handler) => {
      if (installing && !["before_agent_start", "turn_end", "message_end"].includes(name)) return () => {};
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(other => other !== handler));
    },
  } as unknown as ExtensionAPI;
  vi.spyOn(FabricState.prototype, "initialized", "get").mockReturnValue(true);
  vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "provisionalConfig").mockReturnValue(DEFAULT_FABRIC_CONFIG);
  vi.spyOn(FabricState.prototype, "publishHostLifecycle").mockResolvedValue(undefined);
  vi.spyOn(FabricState.prototype, "resetSpeculation").mockImplementation(() => {});
  vi.spyOn(FabricState.prototype, "nextRecordsInboxMessage").mockResolvedValue(undefined);
  vi.spyOn(FabricState.prototype, "nextRootInbox").mockImplementation((session, _idle, options) => inbox.next(session, options));
  const consumed = vi.spyOn(FabricState.prototype, "consumeRootInbox").mockImplementation((ids, receipts) => inbox.consume(ids, receipts));
  await piFabric(pi);
  installing = false;
  const context = {
    cwd: root, hasUI: false, isIdle: () => false, hasPendingMessages: () => false,
    getContextUsage: () => undefined,
    sessionManager: { getSessionId: () => "boundary-main", isPersisted: () => false,
      getSessionFile: () => undefined, getEntries: () => entries, getBranch: () => entries },
    ui: { notify: vi.fn(), setStatus: vi.fn() },
  } as unknown as ExtensionContext;
  const emit = async (name: string, event: any = {}, ctx = context) => {
    if (name === "before_agent_start") event.systemPromptOptions ??= { sections: {} };
    for (const handler of [...(handlers.get(name) ?? [])]) await handler(event, ctx);
  };
  const turn = (stopReason = "toolUse") => emit("turn_end", {
    message: { role: "assistant", stopReason }, context: { pendingMessages: [] },
  });
  const work = (text: string) => mesh.publish({ from: peer, to: me.id, topic: "fleet.work.task", kind: "completion", text });
  return { root, mesh, inbox, pi, entries, sent, consumed, context, emit, turn, work,
    connect: (send: NonNullable<typeof nativeSend>) => { nativeSend = send; } };
};

describe("authenticated root carriers in the existing Main followUp drain", () => {
  it("retains one carrier for 50 not-yet-eligible boundaries, then ACKs only native consumption once", async () => {
    const h = await fixture();
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    let main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    const journal = path.join(h.root, "followups.json");
    main.attachFollowUpDrain(h.context, 120_000, journal);
    cleanup.push(() => main.closeFollowUpDrain());
    const event = await h.work("completion arrived while Main was chaining tools");
    await h.turn();
    const held = pendingRootInboxCarriers(h.pi)[0]!;
    const nonce = (held.details as any).nonce;
    expect(typeof nonce).toBe("string");
    expect(h.sent).toEqual([]);
    expect(h.consumed).not.toHaveBeenCalled();
    expect(fs.existsSync(journal)).toBe(false); // RootInbox is the only durable queue owner.
    for (let turn = 0; turn < 50; turn++) await h.turn();
    expect(pendingRootInboxCarriers(h.pi)).toEqual([held]);
    expect(h.sent).toEqual([]); // A missing native preview must not expire a drain-held lease.
    now += 120_001;
    await h.turn();
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.message).toBe(held); // No wrapper, re-render, nonce replacement or wake restamp.
    expect(h.sent[0]!.options).toMatchObject({ deliverAs: "steer", triggerTurn: true });
    expect(h.sent[0]!.message.customType).toBe("pi-fabric-inbox");
    expect(h.sent[0]!.options).toMatchObject({ provenance: { sender: { id: peer.id }, via: "followUp" } });
    expect(h.consumed).not.toHaveBeenCalled(); // Handover is NOT an ACK.
    const detached = JSON.parse(JSON.stringify({ role: "custom", ...held }));
    await h.emit("message_end", { message: { ...detached, content: "forged content" } });
    await h.emit("message_end", { message: { ...detached, details: { ...detached.details, ids: ["forged"] } } });
    expect(h.consumed).not.toHaveBeenCalled();
    main.prepareReload(); main.closeFollowUpDrain();
    main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 120_000, journal);
    await h.emit("message_end", { message: detached });
    await h.emit("message_end", { message: JSON.parse(JSON.stringify(detached)) });
    expect(h.consumed).toHaveBeenCalledExactlyOnceWith([event.id], expect.any(Array));
    h.entries.push({ type: "custom_message", ...detached });
    for (let turn = 0; turn < 5; turn++) await h.turn();
    expect(h.sent).toHaveLength(1);
    expect((h.mesh.get(h.inbox.key, { fresh: true })!.value as any).pending).toBeUndefined();
  });

  it("keeps ordinary prefixes and the indivisible root carrier in FIFO order", async () => {
    const h = await fixture();
    const main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 120_000);
    cleanup.push(() => main.closeFollowUpDrain());
    main.deliverAgent({ from: peer, message: "ordinary before", delivery: "followUp" });
    await h.work("root in the middle"); await h.turn();
    const carrier = pendingRootInboxCarriers(h.pi)[0]!;
    main.deliverAgent({ from: peer, message: "ordinary after", delivery: "followUp" });
    main.flushHeldAtNextBoundary(); await h.turn();
    expect(h.sent.map(item => item.message.customType)).toEqual([
      "pi-fabric-agent-message", "pi-fabric-inbox", "pi-fabric-agent-message",
    ]);
    expect(h.sent[0]!.message.content).toContain("ordinary before");
    expect(h.sent[1]!.message).toBe(carrier);
    expect(h.sent[2]!.message.content).toContain("ordinary after");
    expect(h.consumed).not.toHaveBeenCalled();
  });

  it("holds the original carrier through an error and a cleared lease without waking or duplicating it", async () => {
    const h = await fixture();
    const main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 120_000);
    cleanup.push(() => main.closeFollowUpDrain());
    await h.work("held through provider failure"); await h.turn();
    const carrier = pendingRootInboxCarriers(h.pi)[0]!;
    await h.turn("error");
    await h.emit("agent_before_settle", { outcome: "error", context: { pendingMessages: [] } });
    await h.emit("agent_settled", { outcome: "error" });
    expect(h.sent).toEqual([]);
    expect(h.consumed).not.toHaveBeenCalled();
    // A later healthy boundary must see the live drain custody even though commitOnly
    // cleared its original lease. There must not be a new nonce/carrier for the same event.
    await h.turn();
    // Recovery may hand it to the native queue immediately. Either custody location
    // must contain only the original exact carrier, never a reissued duplicate.
    expect([...h.sent.map(item => item.message), ...pendingRootInboxCarriers(h.pi)]).toEqual([carrier]);
    expect(h.consumed).not.toHaveBeenCalled();
  });

  it.each(["user", "custom"] as const)("joins the exact root FIFO prefix to an authorized %s prompt without requesting a turn", async messageType => {
    const h = await fixture();
    const main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 120_000);
    cleanup.push(() => main.closeFollowUpDrain());
    main.deliverAgent({ from: peer, message: "ordinary before", delivery: "followUp" });
    const event = await h.work("root joins this prompt"); await h.turn();
    const carrier = pendingRootInboxCarriers(h.pi)[0]!;
    main.deliverAgent({ from: peer, message: "ordinary after", delivery: "followUp" });
    await h.emit("before_agent_start", { messageType });
    expect(h.sent.map(item => item.message.customType)).toEqual(["pi-fabric-agent-message", "pi-fabric-inbox"]);
    expect(h.sent[0]!.message.content).toContain("ordinary before");
    expect(h.sent[1]!.message).toBe(carrier);
    for (const sent of h.sent) expect(sent.options).toMatchObject({
      deliverAs: messageType === "user" ? "nextTurn" : "followUp", triggerTurn: false,
    });
    expect(pendingRootInboxCarriers(h.pi)).toEqual([]);
    expect(main.queueDepth().pendingFollowUps).toBe(1); // The later ordinary item does not hitchhike.
    expect(h.consumed).not.toHaveBeenCalled();
    expect((h.mesh.get(h.inbox.key, { fresh: true })!.value as any).pending.ids).toEqual([event.id]);
    await h.emit("agent_start");
    expect(h.sent).toHaveLength(2); // No fallback duplicate or wake.
    const native = JSON.parse(JSON.stringify({ role: "custom", ...carrier }));
    await h.emit("message_end", { message: native });
    await h.emit("message_end", { message: JSON.parse(JSON.stringify(native)) });
    expect(h.consumed).toHaveBeenCalledExactlyOnceWith([event.id], expect.any(Array));
  });

  it("joins a held root to a legacy custom recovery's initial poll, retaining the provider hold for later peers", async () => {
    const h = await fixture();
    const main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 120_000);
    cleanup.push(() => main.closeFollowUpDrain());
    await h.work("root on the already-authorized retry"); await h.turn();
    const carrier = pendingRootInboxCarriers(h.pi)[0]!;
    await h.turn("error");
    await h.emit("agent_before_settle", { outcome: "error", context: { pendingMessages: [] } });
    await h.emit("agent_settled", { outcome: "error" });
    expect(h.sent).toEqual([]);
    await h.emit("agent_start"); // Old SDK custom triggers omit before_agent_start.
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.message).toBe(carrier);
    expect(h.sent[0]!.options).toMatchObject({ deliverAs: "steer", triggerTurn: true });
    expect(h.consumed).not.toHaveBeenCalled();
    const later = main.deliverAgent({ from: peer, message: "wait for this attempt's outcome", delivery: "followUp" });
    expect(later).toMatchObject({ triggered: false, pendingFollowUps: 1 });
    expect(h.sent).toHaveLength(1);
    await h.turn("error");
    await h.emit("agent_settled", { outcome: "error" });
    expect(h.sent).toHaveLength(1); // No second attempt bought by the join.
    expect(h.consumed).not.toHaveBeenCalled();
  });

  it.each(["halted", "aborted", "reload", "retry-veto"] as const)("does not join a held root across a %s gate", async mode => {
    const h = await fixture();
    const main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 120_000);
    cleanup.push(() => main.closeFollowUpDrain());
    await h.work("held behind a gate"); await h.turn();
    const carrier = pendingRootInboxCarriers(h.pi)[0]!;
    if (mode === "halted") main.halt();
    else if (mode === "reload") main.prepareReload();
    else if (mode === "retry-veto") await h.emit("turn_end", {
      message: { role: "assistant", stopReason: "error", recovery: { retryable: false } }, context: { pendingMessages: [] },
    });
    else {
      const abort = new AbortController(); abort.abort();
      Object.assign(h.context, { signal: abort.signal });
    }
    await h.emit("before_agent_start", { messageType: "custom" });
    await h.emit("agent_start");
    expect(h.sent).toEqual([]);
    expect(pendingRootInboxCarriers(h.pi)).toEqual([carrier]);
    expect(h.consumed).not.toHaveBeenCalled();
  });

  it("leaves ordinary held messages alone when a new prompt has no held root", async () => {
    const h = await fixture();
    const main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 120_000);
    cleanup.push(() => main.closeFollowUpDrain());
    main.deliverAgent({ from: peer, message: "ordinary only", delivery: "followUp" });
    await h.emit("before_agent_start", { messageType: "user" });
    await h.emit("agent_start");
    expect(h.sent).toEqual([]);
    expect(main.queueDepth().pendingFollowUps).toBe(1);
  });

  it("returns an unhanded carrier to durable RootInbox custody on controller reload, without a journal ACK", async () => {
    const h = await fixture();
    const journal = path.join(h.root, "followups.json");
    let main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 120_000, journal);
    cleanup.push(() => main.closeFollowUpDrain());
    await h.work("reload before native handover"); await h.turn();
    const abandoned = pendingRootInboxCarriers(h.pi)[0]!;
    main.prepareReload(); main.closeFollowUpDrain();
    expect(h.sent).toEqual([]); expect(h.consumed).not.toHaveBeenCalled();
    expect(fs.existsSync(journal)).toBe(false);
    main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 120_000, journal);
    await h.turn(); await h.turn(); // Existing one-further-turn lease expires without queue evidence.
    const recovered = pendingRootInboxCarriers(h.pi)[0]!;
    expect(recovered).toBeDefined(); expect(recovered).not.toBe(abandoned);
    main.flushHeldAtNextBoundary(); await h.turn();
    expect(h.sent).toHaveLength(1); expect(h.sent[0]!.message).toBe(recovered);
    await h.emit("message_end", { message: { role: "custom", ...recovered } });
    expect(h.consumed).toHaveBeenCalledTimes(1);
  });

  it("retains nonce/content authority and one-consume identity across a same-manager issuer reload", () => {
    const manager = {};
    const issuer = rootInboxReceiptIssuer(manager, "boundary-main");
    const carrier = { customType: "pi-fabric-inbox", content: "exact original content", display: true,
      details: { ids: ["one"], receipts: ["receipt"] } };
    registerRootInboxReceipt(issuer, carrier.details, carrier.content);
    const snapshot = verifyRootInboxReceipt(issuer, carrier.details, carrier, new Set())!;
    issuer.consumed.add(snapshot);
    const reloaded = rootInboxReceiptIssuer(manager, "boundary-main", true);
    const detached = JSON.parse(JSON.stringify(carrier));
    expect(reloaded).toBe(issuer);
    expect(verifyRootInboxReceipt(reloaded, detached.details, detached, new Set())).toBe(snapshot);
    expect(reloaded.consumed.has(snapshot)).toBe(true);
    expect(verifyRootInboxReceipt(reloaded, detached.details, { ...detached, content: "edited" }, new Set())).toBeUndefined();
    expect(verifyRootInboxReceipt(rootInboxReceiptIssuer({}, "boundary-main", true), detached.details, detached, new Set())).toBeUndefined();
  });

  it.each(["passive", "late", "halted", "aborted"] as const)("does not wake Main for %s work", async mode => {
    const h = await fixture();
    const main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
    main.attachFollowUpDrain(h.context, 1);
    cleanup.push(() => main.closeFollowUpDrain());
    const event = await h.work("do not revive cancelled work");
    if (mode === "passive" || mode === "late") {
      deliverRootInbox(h.pi, [{ ...event, ...(mode === "late" ? { deliveredLate: true } : {}) }],
        { deliverAs: "followUp", triggerTurn: mode === "late" });
    } else {
      await h.turn();
      expect(h.sent).toEqual([]);
      if (mode === "halted") { main.halt(); await h.turn(); }
      else {
        const abort = new AbortController(); abort.abort();
        Object.assign(h.context, { signal: abort.signal });
        await h.turn("aborted");
        await h.emit("agent_settled", { outcome: "aborted" });
      }
    }
    expect(h.sent).toHaveLength(1);
    expect(h.sent[0]!.message.customType).toBe("pi-fabric-inbox");
    expect(h.sent[0]!.options?.triggerTurn).toBe(false);
    expect(h.consumed).not.toHaveBeenCalled();
  });
});

// Parent owns the fresh build. No build/installation is started by this bounded task.
// These are source-adapter SDK controls; the separate integration below loads the actual bundle.
describe.skipIf(!fs.existsSync(path.join(import.meta.dirname, "../dist/index.js")))("real SDK Main tool chaining (#18131)", () => {
  it.each([false, true])("negative control/fixed aged drain (drain=%s)", async drain => {
    const h = await fixture();
    const faux = fauxProvider();
    const runtime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false,
      authPath: path.join(h.root, "auth.json") });
    runtime.registerNativeProvider(faux.provider);
    const turns = 24;
    const started = Array.from({ length: turns }, deferred);
    const gates = Array.from({ length: turns }, deferred);
    let tools = 0;
    let settles = 0;
    let main!: MainAgentController;
    const loader = new DefaultResourceLoader({
      cwd: h.root, agentDir: path.join(h.root, "agent"), noSkills: true, noPromptTemplates: true,
      noThemes: true, noContextFiles: true,
      extensionFactories: [{ name: "inbox-boundary", factory: pi => {
        h.connect((message, options) => pi.sendMessage(message, options));
        pi.registerTool({ name: "work", label: "work", description: "held real tool boundary", parameters: Type.Object({}),
          execute: async () => {
            const index = tools++;
            started[index]!.resolve();
            await gates[index]!.promise;
            return { content: [{ type: "text", text: "done" }], details: {} };
          } });
        pi.on("session_start", (_event, ctx) => {
          main = new MainAgentController(h.pi, me.id, true, h.root, "boundary-main");
          main.attachFollowUpDrain(ctx, drain ? 10 : 0, path.join(h.root, "followups.json"));
        });
        pi.on("before_agent_start", (event, ctx) => h.emit("before_agent_start", event, ctx));
        pi.on("turn_end", (event, ctx) => h.emit("turn_end", event, ctx));
        pi.on("message_end", (event, ctx) => h.emit("message_end", event, ctx));
        pi.on("agent_start", (event, ctx) => h.emit("agent_start", event, ctx));
        pi.on("agent_before_settle", (event, ctx) => h.emit("agent_before_settle", event, ctx));
        pi.on("agent_settled", (event, ctx) => h.emit("agent_settled", event, ctx));
        pi.on("tool_execution_start", (event, ctx) => h.emit("tool_execution_start", event, ctx));
        pi.on("tool_execution_end", (event, ctx) => h.emit("tool_execution_end", event, ctx));
        pi.on("agent_settled", () => { settles++; });
      } }],
    });
    await loader.reload();
    const { session } = await createAgentSession({ cwd: h.root, agentDir: path.join(h.root, "agent"),
      modelRuntime: runtime, model: faux.getModel(), resourceLoader: loader,
      sessionManager: SessionManager.inMemory(h.root), tools: ["work"] });
    cleanup.push(async () => { main?.closeFollowUpDrain(); await session.abort(); session.clearQueue(); session.dispose(); });
    await session.bindExtensions({});
    faux.setResponses([...Array.from({ length: turns }, () => fauxAssistantMessage(fauxToolCall("work", {}), { stopReason: "toolUse" })),
      fauxAssistantMessage("chain completed"), fauxAssistantMessage("read native followUp at last")]);
    const run = session.prompt("keep doing tool work");
    const visible = () => session.messages.filter(message => message.role === "custom" &&
      (message as { customType?: string }).customType === "pi-fabric-inbox");
    try {
      for (let index = 0; index < turns; index++) {
        await started[index]!.promise;
        if (index === 4) await h.work("COMPLETION_DURING_BUSY_MAIN");
        if (index === 5) {
          expect(visible()).toEqual([]); // Never inject inside an executing tool.
          expect(h.consumed).not.toHaveBeenCalled();
          await new Promise(resolve => setTimeout(resolve, 20)); // One bounded wait: make the held carrier eligible.
        }
        if (index >= 6) {
          expect(settles).toBe(0);
          expect(visible()).toHaveLength(drain ? 1 : 0);
          expect(h.consumed).toHaveBeenCalledTimes(drain ? 1 : 0);
        }
        gates[index]!.resolve();
      }
      await run;
      expect(visible()).toHaveLength(1);
      expect(h.consumed).toHaveBeenCalledTimes(1);
      const delivered = visible()[0] as unknown as { content: string; details: { nonce: string } };
      expect(delivered.content).toContain("COMPLETION_DURING_BUSY_MAIN");
      expect(delivered.content).not.toContain("<fabric-agent-message");
      expect(typeof delivered.details.nonce).toBe("string");
      const carrier = h.sent.find(item => item.message.customType === "pi-fabric-inbox")!;
      expect(carrier.options?.deliverAs).toBe(drain ? "steer" : "followUp");
      // An exact duplicate native callback remains one ACK after all those model/tool turns.
      await h.emit("message_end", { message: JSON.parse(JSON.stringify(visible()[0])) });
      expect(h.consumed).toHaveBeenCalledTimes(1);
    } finally {
      for (const gate of gates) gate.resolve();
      await session.abort();
      await run.catch(() => undefined);
    }
  }, 20_000);
});

// No fixture(), FabricState spies, manually attached controller, or source extension route.
// This catches identity/activation mismatches between the bundle's delivery bridge and Main.
const fabricEntry = path.resolve("dist/index.js");
describe.skipIf(!fs.existsSync(fabricEntry))("built Fabric root inbox during SDK tool chaining (#18131)", () => {
  it("receives a mature MeshStore completion before settle and commits its actual delivered receipt once", async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-built-inbox-boundary-")));
    cleanup.push(() => fs.rmSync(root, { recursive: true, force: true }));
    const keys = ["PI_FABRIC_MESH_ROOT", "PI_CODING_AGENT_DIR", "PI_FABRIC_INBOX_WAKE_MS", "PI_FABRIC_INBOX_WAKE_COOLDOWN_MS"] as const;
    const saved = keys.map(key => [key, process.env[key]] as const);
    cleanup.push(() => {
      for (const [key, value] of saved) {
        if (value === undefined) delete process.env[key]; else process.env[key] = value;
      }
    });
    const agentDir = path.join(root, "agent");
    const meshRoot = path.join(root, "mesh");
    fs.mkdirSync(agentDir, { recursive: true });
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
      fullCodeMode: false, capture: { enabled: false }, mesh: { announce: true, followUpFlushMs: 10 },
    }));
    process.env.PI_CODING_AGENT_DIR = agentDir;
    process.env.PI_FABRIC_MESH_ROOT = meshRoot;
    delete process.env.PI_FABRIC_INBOX_WAKE_MS;
    delete process.env.PI_FABRIC_INBOX_WAKE_COOLDOWN_MS;
    const faux = fauxProvider({ tokensPerSecond: 100_000 });
    const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false,
      authPath: path.join(root, "auth.json") });
    modelRuntime.registerNativeProvider(faux.provider);
    const mesh = new MeshStore(meshRoot, 64 * 1024, 500);
    cleanup.push(() => mesh.closeState());
    let tools = 0;
    let completion: Awaited<ReturnType<MeshStore["publish"]>> | undefined;
    let rootId = "";
    const marker = "BUILT_COMPLETION_DURING_BUSY_MAIN";
    const loader = new DefaultResourceLoader({
      cwd: root, agentDir, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      additionalExtensionPaths: [fabricEntry],
      extensionFactories: [{ name: "busy-work-producer", factory: pi => {
        pi.registerTool({ name: "busy_work", label: "busy work", description: "One bounded step of a busy tool chain",
          parameters: Type.Object({}), execute: async () => {
            tools++;
            if (tools === 5) {
              // MeshStore owns the real publication. Age only its commit clock so the
              // event is past RootInbox's 60s steer grace without a minute-long test.
              const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now() - 120_000);
              try {
                completion = await mesh.publish({ from: peer, to: rootId, topic: "fleet.work.built-boundary",
                  kind: "completion", text: marker });
              } finally { clock.mockRestore(); }
            }
            await new Promise(resolve => setTimeout(resolve, 25));
            return { content: [{ type: "text", text: "step complete" }], details: {} };
          } });
      } }],
    });
    await loader.reload();
    const { session } = await createAgentSession({ cwd: root, agentDir, modelRuntime, model: faux.getModel(),
      resourceLoader: loader, sessionManager: SessionManager.inMemory(root) });
    cleanup.push(async () => {
      await session.abort();
      await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
      session.dispose();
    });
    await session.bindExtensions({ shutdownHandler: async () => {} });
    rootId = `session:${session.sessionManager.getSessionId()}`;
    const key = "topology/inbox/" + createHash("sha256").update(rootId).digest("hex").slice(0, 32);
    const cursor = () => mesh.get(key, { fresh: true })?.value as { delivered?: string[]; pending?: unknown; after: number } | undefined;
    const inboxMessages = () => session.messages.filter(message => message.role === "custom" &&
      (message as { customType?: string }).customType === "pi-fabric-inbox");
    // Real Fabric activation/bootstrap, just like root-inbox-session.test.ts start().
    faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return 1" })), fauxAssistantMessage("ready")]);
    await session.prompt("activate Fabric");
    expect(inboxMessages()).toEqual([]);
    let settles = 0;
    const unsubscribe = session.subscribe(event => { if (event.type === "agent_settled") settles++; });
    cleanup.push(unsubscribe);
    let firstSeenAt = -1;
    faux.setResponses([...Array.from({ length: 24 }, () => (context: { messages: unknown[] }) => {
      if (JSON.stringify(context.messages).includes(marker) && firstSeenAt < 0) {
        firstSeenAt = tools;
        expect(settles).toBe(0); // Actual model request, before the chain's first settle.
        expect(firstSeenAt).toBeLessThan(24);
        expect(cursor()?.delivered).toContain(eventReceipt(completion!.id));
        expect(cursor()?.after).toBeGreaterThanOrEqual(completion!.sequence);
      }
      return fauxAssistantMessage(fauxToolCall("busy_work", {}));
    }), fauxAssistantMessage("chain completed"), fauxAssistantMessage("unexpected late completion")]);
    await session.prompt("keep working through 24 tool calls");
    expect(tools).toBe(24);
    expect(completion).toBeDefined();
    expect(completion!.createdAt).toBeLessThan(Date.now() - 60_000);
    expect(firstSeenAt).toBeGreaterThanOrEqual(5);
    expect(firstSeenAt).toBeLessThan(24);
    expect(settles).toBe(1);
    const issuedIds = () => inboxMessages().flatMap(message =>
      (message as { details?: { ids?: string[] } }).details?.ids ?? []);
    expect(issuedIds()).toEqual([completion!.id]);
    expect(cursor()?.delivered?.filter(receipt => receipt === eventReceipt(completion!.id))).toHaveLength(1);
    expect(cursor()?.pending).toBeUndefined();
    // Another real SDK prompt must not reissue the completion or append another receipt.
    faux.setResponses([fauxAssistantMessage("nothing repeated")]);
    await session.prompt("check the durable inbox again");
    expect(issuedIds()).toEqual([completion!.id]);
    expect(cursor()?.delivered?.filter(receipt => receipt === eventReceipt(completion!.id))).toHaveLength(1);
  }, 60_000);
});
