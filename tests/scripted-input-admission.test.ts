import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController, type FabricMainAgentDeliveryRequest } from "../src/main-agent.js";
import {
  acknowledgeScriptedInput, admitBoundScriptedInput, admitContinuation, admitScriptedInput,
  bindInputAdmission, findScriptedInputAdmission, listPendingScriptedInputs, markScriptedInputPublished,
  prepareScriptedInput, readScriptedInput,
} from "../src/recovery/input-admission.js";

// Keep reasons: data safety, a trust boundary, and commit-before-response crash windows.
// The native Main controller and Pi session serializer are real. The ExtensionAPI seam
// appends native carriers synchronously, without a model or a second delivery implementation.
// These are admission/processing-carrier assertions, NOT consequent-effect completion claims.
const roots: string[] = [];
const controllers: MainAgentController[] = [];
const bindings: Array<() => void> = [];
const from = { id: "script-driver", name: "Script driver", kind: "main" as const };
const temp = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-scripted-input-"));
  roots.push(root);
  return root;
};
const jsonFiles = (root: string): string[] => fs.existsSync(root) ? fs.readdirSync(root, { withFileTypes: true }).flatMap(entry =>
  entry.isDirectory() ? jsonFiles(path.join(root, entry.name)) : entry.name.endsWith(".json") ? [path.join(root, entry.name)] : []) : [];
const receiver = (root = temp(), file?: string, persist = true) => {
  const manager = file ? SessionManager.open(file) : SessionManager.create(root, path.join(root, "sessions"));
  if (!file) {
    manager.appendMessage({ role: "user", content: "bootstrap", timestamp: Date.now() });
    manager.appendMessage(fauxAssistantMessage("ready"));
  }
  const sessionId = manager.getSessionId();
  const handlers = new Map<string, Array<(event: unknown, ctx: ExtensionContext) => unknown>>();
  const sent: Array<{ content: string; details: Record<string, unknown> }> = [];
  const pi = {
    on: (name: string, handler: (event: unknown, ctx: ExtensionContext) => unknown) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(value => value !== handler));
    },
    sendMessage: (message: { content: string; details: Record<string, unknown> }) => {
      sent.push(message);
      if (persist) manager.appendCustomMessageEntry("pi-fabric-agent-message", message.content, true, message.details);
    },
  } as unknown as ExtensionAPI;
  const context = { isIdle: () => true, hasPendingMessages: () => false, sessionManager: manager } as unknown as ExtensionContext;
  const mainAgent = new MainAgentController(pi, `session:${sessionId}`, true, root, sessionId);
  const journal = path.join(root, "main-followups", `${sessionId}.json`);
  mainAgent.attachFollowUpDrain(context, 0, journal);
  controllers.push(mainAgent);
  return { root, sessionId, mainAgent, manager, sent, journal,
    file: manager.getSessionFile()!,
    carriers: () => manager.getEntries().filter(entry => entry.type === "custom_message" && entry.customType === "pi-fabric-agent-message"),
    boundary: () => { for (const handler of handlers.get("agent_before_settle") ?? []) handler({ outcome: "completed", context: { pendingMessages: [] } }, context); },
  };
};
const requestFor = (sessionId: string, inputId = "round-1", deadlineAt = Date.now() + 60_000): FabricMainAgentDeliveryRequest => ({
  from: { ...from }, message: "immutable prompt", delivery: "followUp", verification: "mesh",
  data: { scriptedInput: { version: 1, inputId, sessionId, deadlineAt }, manifest: { slot: 1 } },
});
const admit = (state: ReturnType<typeof receiver>, request = requestFor(state.sessionId)) =>
  admitScriptedInput({ root: state.root, sessionId: state.sessionId, mainAgent: state.mainAgent, request });
const prepare = (root: string, request: FabricMainAgentDeliveryRequest, targetId = `session:${readScriptedInput(request.data)!.sessionId}`) =>
  prepareScriptedInput({ root, from: request.from, targetId, message: request.message, data: request.data,
    delivery: request.delivery as "steer" | "followUp",
    ...(request.priority === undefined ? {} : { priority: request.priority }),
    ...(request.triggerTurn === undefined ? {} : { triggerTurn: request.triggerTurn }) });
const identityOf = (request: FabricMainAgentDeliveryRequest) => ({ from: request.from, ...readScriptedInput(request.data)! });
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const cleanup of bindings.splice(0)) cleanup();
  for (const controller of controllers.splice(0)) controller.closeFollowUpDrain();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("scripted input metadata", () => {
  it("leaves ordinary traffic alone and returns an isolated metadata snapshot", () => {
    for (const data of [undefined, null, "text", {}, { other: 1 }]) expect(readScriptedInput(data)).toBeUndefined();
    const request = requestFor("session-1");
    const metadata = readScriptedInput(request.data)!;
    expect(metadata).toEqual({ version: 1, inputId: "round-1", sessionId: "session-1", deadlineAt: expect.any(Number) });
    (request.data as { scriptedInput: { inputId: string } }).scriptedInput.inputId = "mutated";
    expect(metadata.inputId).toBe("round-1");
  });

  it.each([undefined, null, {}, { version: 2, inputId: "a", sessionId: "s", deadlineAt: 1 },
    { version: 1, inputId: "", sessionId: "s", deadlineAt: 1 },
    { version: 1, inputId: "a", sessionId: "", deadlineAt: 1 },
    ...[NaN, Infinity, -1, 1.5, "123"].map(deadlineAt => ({ version: 1, inputId: "a", sessionId: "s", deadlineAt })),
  ])("refuses malformed opted-in metadata (%j)", scriptedInput => {
    expect(() => readScriptedInput({ scriptedInput })).toThrow(/scriptedInput/i);
  });

});

// The native directory-fsync durability profile is unavailable on Windows; the
// portable metadata and explicit unsupported-profile tests still run there.
describe.skipIf(process.platform === "win32")("immutable scripted sender outbox", () => {
  it("persists intent before publish and recovers it after a sender module restart without driver input", async () => {
    const root = temp(); const request = requestFor("recipient");
    const prepared = prepare(root, request);
    expect(jsonFiles(root).length).toBeGreaterThan(0);
    (request.data as { manifest: { slot: number } }).manifest.slot = 99;
    vi.resetModules();
    const { listPendingScriptedInputs: listAfterRestart } = await import("../src/recovery/input-admission.js");
    expect(listAfterRestart({ root, from })).toEqual([{ ...prepared, from, targetId: "session:recipient", published: false }]);
    const identity = identityOf(request);
    markScriptedInputPublished({ root, ...identity });
    expect(listPendingScriptedInputs({ root, from })[0]).toMatchObject({ ...prepared, published: true });
    expect(() => prepare(root, request)).toThrow(/conflict/i);
    const replay = prepare(root, requestFor("recipient", "round-1", prepared.deadlineAt));
    expect(replay).toEqual(prepared);
    acknowledgeScriptedInput({ root, ...identity, result: { queued: true, messageId: "native-receipt", routed: "main" } });
    expect(listPendingScriptedInputs({ root, from })).toEqual([]);
    const acknowledged = prepare(root, requestFor("recipient", "round-1", prepared.deadlineAt));
    expect(acknowledged).toEqual({ ...prepared, result: { queued: true, messageId: "native-receipt", routed: "main",
      deadlineAt: prepared.deadlineAt, duplicate: true, triggered: false } });
    vi.spyOn(Date, "now").mockReturnValue(prepared.deadlineAt + 1);
    expect(prepare(root, requestFor("recipient", "round-1", prepared.deadlineAt))).toEqual(acknowledged);
    acknowledged.result!.messageId = "caller mutation";
    expect(prepare(root, requestFor("recipient", "round-1", prepared.deadlineAt)).result!.messageId).toBe("native-receipt");
    expect(() => acknowledgeScriptedInput({ root, ...identity, result: { queued: true, messageId: "different", routed: "main" } })).toThrow(/conflict/i);
  });

  it("binds exact text, sender, recipient and all metadata, but not object key order", () => {
    const root = temp(); const request = requestFor("recipient");
    const first = prepare(root, request);
    const original = request.data as { scriptedInput: unknown; manifest: unknown };
    expect(prepare(root, { ...request, data: { manifest: original.manifest, scriptedInput: original.scriptedInput } })).toEqual(first);
    for (const changed of [
      { ...request, message: `${request.message} ` },
      { ...request, delivery: "steer" as const },
      { ...request, triggerTurn: false },
      { ...request, from: { ...from, name: "different" } },
      { ...request, data: { ...original, extra: true } },
      { ...request, data: { ...original, scriptedInput: { ...readScriptedInput(request.data), deadlineAt: first.deadlineAt + 1 } } },
    ]) expect(() => prepare(root, changed)).toThrow(/conflict/i);
    expect(() => prepare(root, request, "session:elsewhere")).toThrow(/session|recipient|target/i);
    expect(prepare(root, requestFor("recipient", "round-2")).idempotencyKey).not.toBe(first.idempotencyKey);
    expect(prepare(root, { ...request, from: { ...from, id: "second-driver" } }).idempotencyKey).not.toBe(first.idempotencyKey);
  });

  it("canonicalizes omitted triggerTurn to true before persistence and survives local-to-remote policy materialization", () => {
    const state = receiver(); const root = temp(); const request = requestFor(state.sessionId);
    const prepared = prepare(root, request);
    expect(prepared.triggerTurn).toBe(true);
    const remoteRequest = { ...request, triggerTurn: true, deliveryId: "remote-command-different-host" };
    expect(prepare(root, remoteRequest)).toEqual(prepared);
    admit(state, request); // Original local path omitted the native default.
    const moved = receiver(state.root, state.file);
    expect(admit(moved, remoteRequest)).toMatchObject({ duplicate: true, triggered: false });
    expect(moved.carriers()).toHaveLength(1);
    expect(moved.sent).toHaveLength(0);
    expect(() => prepare(root, { ...request, triggerTurn: false })).toThrow(/conflict/i);
    expect(() => admit(moved, { ...remoteRequest, triggerTurn: false })).toThrow(/conflict/i);
  });

  it("does not mistake broker publication or a changed-deadline result for native admission", () => {
    const root = temp(); const request = requestFor("recipient"); prepare(root, request);
    const identity = { root, ...identityOf(request) };
    expect(() => acknowledgeScriptedInput({ ...identity, result: { queued: true, messageId: "broker", routed: "mesh" } })).toThrow(/native|ACK/i);
    expect(() => acknowledgeScriptedInput({ ...identity, result: { queued: true, messageId: "native", routed: "main", deadlineAt: Date.now() + 120_000 } })).toThrow(/native|deadline/i);
    expect(listPendingScriptedInputs({ root, from })).toHaveLength(1);
  });

  it("retains expired unpublished intents in the finite outbox and refuses lossy JSON payloads", () => {
    const root = temp(); const request = requestFor("recipient", "expired", Date.now() - 1);
    prepare(root, request);
    expect(listPendingScriptedInputs({ root, from })).toHaveLength(1);
    expect(() => prepare(root, { ...requestFor("recipient", "lossy"), data: { scriptedInput: readScriptedInput(request.data), lost: undefined } })).toThrow(/JSON|serializ/i);
    expect(() => markScriptedInputPublished({ root, from, sessionId: "recipient", inputId: "missing" })).toThrow(/missing|history|intent/i);
  });
});

describe.skipIf(process.platform === "win32")("native receiver admission and restart reconciliation", () => {
  it("returns a native result synchronously, enforces session identity, and keeps distinct rounds", () => {
    const state = receiver(); const request = requestFor(state.sessionId);
    expect(() => admit(state, requestFor("wrong-session"))).toThrow(/session/i);
    const first = admit(state, request);
    expect(first).toMatchObject({ queued: true, routed: "main", messageId: expect.any(String) });
    expect(first).not.toBeInstanceOf(Promise);
    expect(admit(state, { ...request, deliveryId: "new-command-on-a-new-host" })).toMatchObject({ duplicate: true, triggered: false });
    admit(state, requestFor(state.sessionId, "round-2"));
    expect(state.carriers()).toHaveLength(2);
    expect(state.sent.map(message => message.details.deliveryId)[0]).not.toBe("new-command-on-a-new-host");
    expect(() => admit(state, { ...request, message: "changed prompt" })).toThrow(/conflict/i);
    expect(state.carriers()).toHaveLength(2);
  });

  it("receiver dies after native append before response: a fresh controller on another machine processes only one carrier", async () => {
    const state = receiver(); const request = requestFor(state.sessionId, "round-1", Date.now() + 1000);
    const senderRoot = temp(); const prepared = prepare(senderRoot, request);
    const deliver = state.mainAgent.deliverAgent.bind(state.mainAgent);
    vi.spyOn(state.mainAgent, "deliverAgent").mockImplementation(value => { deliver(value); throw new Error("killed before response"); });
    expect(() => admit(state, request)).toThrow("killed before response");
    expect(state.carriers()).toHaveLength(1);
    // No close/shutdown of the old controller runs before opening the replacement.
    const readFile = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === "/etc/machine-id") return "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n";
      if (String(file) === "/proc/sys/kernel/random/boot_id") return "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb\n";
      return (readFile as (...values: unknown[]) => unknown)(file, ...args);
    }) as typeof fs.readFileSync);
    vi.spyOn(Date, "now").mockReturnValue(prepared.deadlineAt + 1);
    const next = receiver(state.root, state.file);
    vi.resetModules();
    const restarted = await import("../src/recovery/input-admission.js");
    const pending = restarted.listPendingScriptedInputs({ root: senderRoot, from });
    expect(pending).toHaveLength(1); // No driver re-supplied the immutable round.
    const recovered = restarted.admitScriptedInput({ root: next.root, sessionId: next.sessionId, mainAgent: next.mainAgent,
      request: { from: pending[0]!.from, message: pending[0]!.message, data: pending[0]!.data, delivery: "followUp", deliveryId: "changed-command" } });
    expect(recovered).toMatchObject({ queued: true, duplicate: true, triggered: false });
    expect(next.sent).toHaveLength(0);
    expect(next.carriers()).toHaveLength(1);
    expect(fs.readFileSync(next.file, "utf8")).toContain(prepared.idempotencyKey);
    restarted.acknowledgeScriptedInput({ root: senderRoot, ...identityOf(request), result: recovered });
    expect(restarted.listPendingScriptedInputs({ root: senderRoot, from })).toEqual([]);
  });

  it("journaled but not session-appended input survives the lost response and native replay", () => {
    const state = receiver(undefined, undefined, false); const request = requestFor(state.sessionId);
    const deliver = state.mainAgent.deliverAgent.bind(state.mainAgent);
    vi.spyOn(state.mainAgent, "deliverAgent").mockImplementation(value => { deliver(value); throw new Error("lost ACK"); });
    expect(() => admit(state, request)).toThrow("lost ACK");
    expect(state.carriers()).toHaveLength(0);
    expect(fs.readFileSync(state.journal, "utf8")).toContain("immutable prompt");
    const next = receiver(state.root, state.file);
    expect(admit(next, request)).toMatchObject({ duplicate: true });
    next.boundary(); next.boundary();
    expect(next.carriers()).toHaveLength(1);
    expect(next.sent).toHaveLength(1);
  });

  it("intent-before-native failure is not an admission; expired retries never enter the native session", () => {
    const state = receiver(); const request = requestFor(state.sessionId, "not-admitted", Date.now() + 1000);
    const fail = vi.spyOn(state.mainAgent, "deliverAgent").mockImplementation(() => { throw new Error("before native append"); });
    expect(() => admit(state, request)).toThrow("before native append");
    fail.mockRestore();
    vi.spyOn(Date, "now").mockReturnValue(readScriptedInput(request.data)!.deadlineAt + 1);
    expect(() => admit(state, request)).toThrow(/deadline|expired/i);
    expect(() => admit(state, requestFor(state.sessionId, "new-expired", Date.now() - 1))).toThrow(/deadline|expired/i);
    expect(state.carriers()).toHaveLength(0);
    bindings.push(bindInputAdmission(state));
    return expect(admitContinuation({ sessionId: state.sessionId, originalInputId: "not-admitted", recoveryGeneration: "g1", text: "resume" })).rejects.toThrow(/admitted|history/i);
  });

  it("a deadline crossed during durable intent persistence still refuses before native append", () => {
    const state = receiver(); const request = requestFor(state.sessionId, "slow-store", Date.now() + 1000);
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      rename(source, target);
      if (String(target).includes("input-admission")) vi.spyOn(Date, "now").mockReturnValue(readScriptedInput(request.data)!.deadlineAt);
    });
    expect(() => admit(state, request)).toThrow(/deadline|expired/i);
    expect(state.carriers()).toHaveLength(0);
    expect(state.sent).toHaveLength(0);
  });

  it("conflicts take precedence over expiry, while admitted duplicate receipts remain available", () => {
    const state = receiver(); const request = requestFor(state.sessionId, "round-1", Date.now() + 1000);
    admit(state, request);
    vi.spyOn(Date, "now").mockReturnValue(readScriptedInput(request.data)!.deadlineAt + 1);
    expect(() => admit(state, { ...request, message: "changed" })).toThrow(/conflict/i);
    expect(() => admit(state, { ...request, data: { scriptedInput: { ...readScriptedInput(request.data), deadlineAt: Date.now() + 1000 } } })).toThrow(/conflict/i);
    expect(admit(state, request)).toMatchObject({ duplicate: true, triggered: false });
    expect(state.carriers()).toHaveLength(1);
  });
});

describe.skipIf(process.platform === "win32")("bound B3 continuation and control orphan admission", () => {
  it("current validated control reentry admits a kill-before-B5-record round, but historical lookup never creates", () => {
    const state = receiver(); const request = requestFor(state.sessionId);
    bindings.push(bindInputAdmission(state));
    const before = jsonFiles(state.root);
    expect(findScriptedInputAdmission({ sessionId: state.sessionId, request })).toBeUndefined();
    expect(jsonFiles(state.root)).toEqual(before);
    expect(state.carriers()).toHaveLength(0);
    // The caller has revalidated CURRENT epoch/session/source/body; no generic handler is re-run.
    expect(admitBoundScriptedInput({ sessionId: state.sessionId, request })).toMatchObject({ queued: true });
    expect(findScriptedInputAdmission({ sessionId: state.sessionId, request })).toMatchObject({ duplicate: true });
    expect(() => findScriptedInputAdmission({ sessionId: state.sessionId, request: { ...request, triggerTurn: false } })).toThrow(/conflict/i);
    expect(state.carriers()).toHaveLength(1);
  });

  it("existing-only lookup cannot turn a pending pre-native record into new input, but current reentry can", () => {
    const state = receiver(); const request = requestFor(state.sessionId);
    bindings.push(bindInputAdmission(state));
    const fail = vi.spyOn(state.mainAgent, "deliverAgent").mockImplementation(() => { throw new Error("before native"); });
    expect(() => admitBoundScriptedInput({ sessionId: state.sessionId, request })).toThrow("before native");
    fail.mockRestore();
    const filesBefore = jsonFiles(state.root).map(file => fs.readFileSync(file, "utf8"));
    expect(findScriptedInputAdmission({ sessionId: state.sessionId, request })).toBeUndefined();
    expect(jsonFiles(state.root).map(file => fs.readFileSync(file, "utf8"))).toEqual(filesBefore);
    expect(state.carriers()).toHaveLength(0);
    expect(admitBoundScriptedInput({ sessionId: state.sessionId, request })).toMatchObject({ queued: true });
    expect(state.carriers()).toHaveLength(1);
  });

  it.each(["lookup", "current-reentry"] as const)("kill-after-native-before-control-outcome reconciles once via %s, even after deadline", mode => {
    const state = receiver(); const request = requestFor(state.sessionId, "orphan", Date.now() + 1000);
    const cleanup = bindInputAdmission(state); bindings.push(cleanup);
    const deliver = state.mainAgent.deliverAgent.bind(state.mainAgent);
    vi.spyOn(state.mainAgent, "deliverAgent").mockImplementation(value => { deliver(value); throw new Error("control outcome lost"); });
    expect(() => admitBoundScriptedInput({ sessionId: state.sessionId, request })).toThrow("control outcome lost");
    cleanup();
    const next = receiver(state.root, state.file); bindings.push(bindInputAdmission(next));
    vi.spyOn(Date, "now").mockReturnValue(readScriptedInput(request.data)!.deadlineAt + 1);
    const result = mode === "lookup" ? findScriptedInputAdmission({ sessionId: next.sessionId, request }) :
      admitBoundScriptedInput({ sessionId: next.sessionId, request });
    expect(result).toMatchObject({ queued: true, duplicate: true });
    expect(next.carriers()).toHaveLength(1);
    expect(next.sent).toHaveLength(0);
  });

  it("replays exact delivery, priority and trigger policy from the sender outbox", () => {
    const state = receiver(); const root = temp();
    const request = { ...requestFor(state.sessionId), from: { id: state.mainAgent.id, name: "Main", kind: "main" as const },
      delivery: "steer" as const, priority: "interrupt" as const, triggerTurn: true };
    const prepared = prepare(root, request);
    const pending = listPendingScriptedInputs({ root, from: request.from })[0]!;
    expect(pending).toMatchObject({ delivery: "steer", priority: "interrupt", triggerTurn: true });
    const replay: FabricMainAgentDeliveryRequest = { from: pending.from, message: pending.message, data: pending.data,
      delivery: pending.delivery, priority: pending.priority!, triggerTurn: pending.triggerTurn!, verification: "mesh" };
    admit(state, replay);
    expect(state.sent[0]!.details).toMatchObject({ deliveryId: prepared.idempotencyKey, delivery: "steer", triggerTurn: true });
    const { priority: _replayPriority, ...withoutReplayPriority } = replay;
    const { priority: _requestPriority, ...withoutRequestPriority } = request;
    expect(() => admit(state, withoutReplayPriority)).toThrow(/conflict/i);
    expect(() => prepare(root, withoutRequestPriority)).toThrow(/conflict/i);
  });

  it("binds only one live owner, delegates native requests, and cleanup removes that owner", () => {
    const state = receiver(); const request = requestFor(state.sessionId);
    expect(() => admitBoundScriptedInput({ sessionId: state.sessionId, request })).toThrow(/bound|binding/i);
    const cleanup = bindInputAdmission(state); bindings.push(cleanup);
    expect(() => bindInputAdmission(state)).toThrow(/bound|binding/i);
    expect(admitBoundScriptedInput({ sessionId: state.sessionId, request })).toMatchObject({ queued: true });
    cleanup(); cleanup();
    expect(() => admitBoundScriptedInput({ sessionId: state.sessionId, request })).toThrow(/bound|binding/i);
    bindings.push(bindInputAdmission(state));
    cleanup(); // Stale cleanup must not remove the replacement binding.
    expect(admitBoundScriptedInput({ sessionId: state.sessionId, request })).toMatchObject({ duplicate: true });
  });

  it("carries the exact original deadline and dedupes by original input plus recovery generation", async () => {
    const state = receiver(); const request = requestFor(state.sessionId);
    admit(state, request); bindings.push(bindInputAdmission(state));
    const options = { sessionId: state.sessionId, originalInputId: "round-1", recoveryGeneration: "g1", text: "Continue unfinished work." };
    const first = await admitContinuation(options);
    expect(first).toMatchObject({ admissionId: expect.any(String), existing: false });
    expect(await admitContinuation(options)).toEqual({ ...first, existing: true });
    await expect(admitContinuation({ ...options, text: "Different continuation." })).rejects.toThrow(/conflict/i);
    const second = await admitContinuation({ ...options, recoveryGeneration: "g2" });
    expect(second.admissionId).not.toBe(first.admissionId);
    const continued = state.sent.slice(1).map(message => readScriptedInput(message.details.data));
    expect(continued).toHaveLength(2);
    expect(continued.every(metadata => metadata?.deadlineAt === readScriptedInput(request.data)!.deadlineAt)).toBe(true);
    expect(state.carriers()).toHaveLength(3);
  });

  it("a second distinct admitted original gets its own continuation even in the same recovery generation", async () => {
    const state = receiver(); bindings.push(bindInputAdmission(state));
    const deadlineAt = Date.now() + 60_000;
    admit(state, requestFor(state.sessionId, "round-1", deadlineAt));
    admit(state, requestFor(state.sessionId, "round-2", deadlineAt));
    const options = { sessionId: state.sessionId, recoveryGeneration: "g1", text: "resume" };
    const first = await admitContinuation({ ...options, originalInputId: "round-1" });
    const second = await admitContinuation({ ...options, originalInputId: "round-2" });
    expect(first.admissionId).not.toBe(second.admissionId);
    expect(first.existing).toBe(false); expect(second.existing).toBe(false);
    expect(state.carriers()).toHaveLength(4);
  });

  it("refuses missing or ambiguous originals and expired new continuations without fabricating history", async () => {
    const state = receiver(); bindings.push(bindInputAdmission(state));
    const options = { sessionId: state.sessionId, originalInputId: "missing", recoveryGeneration: "g1", text: "resume" };
    await expect(admitContinuation(options)).rejects.toThrow(/history|original|admitted/i);
    const request = requestFor(state.sessionId, "original", Date.now() + 1000);
    admit(state, request);
    vi.spyOn(Date, "now").mockReturnValue(readScriptedInput(request.data)!.deadlineAt + 1);
    await expect(admitContinuation({ ...options, originalInputId: "original" })).rejects.toThrow(/deadline|expired/i);
    expect(state.carriers()).toHaveLength(1);
    vi.restoreAllMocks();
    admit(state, { ...request, from: { ...from, id: "another-sender" } });
    await expect(admitContinuation({ ...options, originalInputId: "original" })).rejects.toThrow(/ambiguous/i);
    expect(state.carriers()).toHaveLength(2);
  });

  it("reconciles an original's lost result and a continuation's lost result across restart, including expiry", async () => {
    const state = receiver(); const request = requestFor(state.sessionId, "original", Date.now() + 1000);
    const deliver = state.mainAgent.deliverAgent.bind(state.mainAgent);
    const fail = vi.spyOn(state.mainAgent, "deliverAgent").mockImplementation(value => { deliver(value); throw new Error("lost result"); });
    expect(() => admit(state, request)).toThrow("lost result");
    fail.mockRestore();
    const cleanup = bindInputAdmission(state); bindings.push(cleanup);
    const options = { sessionId: state.sessionId, originalInputId: "original", recoveryGeneration: "g1", text: "resume" };
    const failContinuation = vi.spyOn(state.mainAgent, "deliverAgent").mockImplementation(value => {
      const result = deliver(value);
      if (value.message === "resume") throw new Error("continuation response lost");
      return result;
    });
    await expect(admitContinuation(options)).rejects.toThrow("continuation response lost");
    failContinuation.mockRestore(); cleanup();
    vi.spyOn(Date, "now").mockReturnValue(readScriptedInput(request.data)!.deadlineAt + 1);
    const next = receiver(state.root, state.file); bindings.push(bindInputAdmission(next));
    expect(await admitContinuation(options)).toMatchObject({ existing: true });
    await expect(admitContinuation({ ...options, text: "changed after expiry" })).rejects.toThrow(/conflict/i);
    expect(next.carriers()).toHaveLength(2);
    expect(next.sent).toHaveLength(0);
  });
});

describe.skipIf(process.platform === "win32")("durability and fail-closed history", () => {
  it("validates immutable sender and receiver envelope corruption on every read", async () => {
    const root = temp(); const request = requestFor("recipient"); prepare(root, request);
    const file = jsonFiles(root)[0]!;
    fs.writeFileSync(file, fs.readFileSync(file, "utf8").replace("immutable prompt", "corrupted prompt"));
    expect(() => listPendingScriptedInputs({ root, from })).toThrow(/corrupt|checksum|digest/i);
    expect(() => prepare(root, request)).toThrow(/corrupt|checksum|digest/i);
    const state = receiver(); const nativeRequest = requestFor(state.sessionId);
    admit(state, nativeRequest); bindings.push(bindInputAdmission(state));
    const record = jsonFiles(state.root).find(candidate => fs.readFileSync(candidate, "utf8").includes('"sha256"'))!;
    fs.writeFileSync(record, fs.readFileSync(record, "utf8").replace("immutable prompt", "corrupted prompt"));
    expect(() => admit(state, nativeRequest)).toThrow(/corrupt|checksum|digest/i);
    await expect(admitContinuation({ sessionId: state.sessionId, originalInputId: "round-1", recoveryGeneration: "g1", text: "resume" })).rejects.toThrow(/corrupt|checksum|digest/i);
    expect(state.carriers()).toHaveLength(1);
  });

  it("a receiver-result write failure is reconciled against native delivery, never a mark-before-effect loss", () => {
    const state = receiver(); const request = requestFor(state.sessionId);
    const rename = fs.renameSync.bind(fs);
    let writes = 0;
    const fail = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(target).includes("input-admission") && ++writes === 2) throw new Error("result persistence failed");
      return rename(source, target);
    });
    expect(() => admit(state, request)).toThrow("result persistence failed");
    expect(state.carriers()).toHaveLength(1);
    fail.mockRestore();
    expect(admit(state, request)).toMatchObject({ duplicate: true });
    expect(state.carriers()).toHaveLength(1);
  });

  it("failed intent barriers prevent native delivery, and visible-but-unconfirmed records owe fresh barriers", () => {
    const state = receiver(); const request = requestFor(state.sessionId);
    const sync = fs.fsyncSync.bind(fs); const open = fs.openSync.bind(fs);
    const opened = new Map<number, string>();
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); opened.set(fd, String(file)); return fd; });
    let fail = true;
    const paths: string[] = [];
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const file = opened.get(fd)!; paths.push(file);
      if (fail && file === state.root) throw new Error("ancestor namespace barrier failed");
      sync(fd);
    });
    expect(() => admit(state, request)).toThrow(/barrier failed/);
    expect(state.carriers()).toHaveLength(0);
    expect(() => admit(state, request)).toThrow(/barrier failed/);
    // The second attempt installed a visible record but failed its post-rename barrier.
    // Loading that record cannot turn visibility into an admission receipt.
    expect(() => admit(state, request)).toThrow(/barrier failed/);
    fail = false; paths.length = 0;
    expect(admit(state, request)).toMatchObject({ queued: true });
    expect(paths).toContain(state.root);
    expect(state.carriers()).toHaveLength(1);
    const senderRoot = temp(); fail = true;
    // A sender file barrier failure likewise cannot produce a prepared publish request.
    const broken = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("sender fsync failed"); });
    expect(() => prepare(senderRoot, request)).toThrow("sender fsync failed");
    broken.mockRestore();
  });

});

describe("scripted input without a namespace durability profile", () => {
  it("refuses unsupported namespace durability before publishing or native admission", () => {
    const root = temp(); const descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
    try {
      Object.defineProperty(process, "platform", { value: "win32" });
      expect(() => prepare(root, requestFor("recipient"))).toThrow(/durab|namespace/i);
      expect(jsonFiles(root)).toEqual([]);
    } finally { Object.defineProperty(process, "platform", descriptor); }
  });
});
