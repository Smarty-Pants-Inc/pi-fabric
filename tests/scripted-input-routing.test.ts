import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { prepareScriptedInput, listPendingScriptedInputs } from "../src/recovery/input-admission.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";
import { FabricControlPlane, type FabricControlCommand } from "../src/topology/control-plane.js";
import { MeshStore } from "../src/mesh/store.js";

// Data safety/trust boundary: real router -> native Main journal, not a call-count-only
// admission fake. No model, child process, live NATS broker or physical fencing claim.
const SESSION = "11111111-1111-4111-8111-111111111111";
const TARGET = `session:${SESSION}`;
const FROM = { id: "session:22222222-2222-4222-8222-222222222222", name: "Sender", kind: "main" as const };
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const roots: string[] = [];
const drains: Array<() => void> = [];
const planes: FabricControlPlane[] = [];
const stores: MeshStore[] = [];
afterEach(async () => {
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  for (const store of stores.splice(0)) store.closeState();
  for (const close of drains.splice(0)) close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const root = () => { const value = fs.mkdtempSync(path.join(os.tmpdir(), "scripted-routing-")); roots.push(value); return value; };
const data = (deadlineAt = Date.now() + 60_000) => ({ scriptedInput: { version: 1, inputId: "round-1", sessionId: SESSION, deadlineAt }, payload: "immutable" });
const unknown = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0];
function router(meshRoot: string, main: Ports[2], control?: Ports[4], participant?: FabricParticipantInfo) {
  const actors = { identity: FROM, mesh: { root: meshRoot }, status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
    validateDirectMessage: vi.fn(), tell: vi.fn(), ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn(), resolveBinding: vi.fn(), resolveActivationBinding: vi.fn() } as unknown as Ports[1];
  const participants = { get: (id: string) => participant?.id === id ? participant : undefined, scheduleRefresh() {}, lastKnown: () => undefined };
  return new AgentMessageRouter(unknown, actors, main, participants, control, b => b);
}
function native(meshRoot: string, entries: unknown[] = []) {
  const sent: Array<{ customType: string; content: string; details: Record<string, any> }> = [];
  const handlers = new Map<string, Array<(event: unknown, ctx: ExtensionContext) => unknown>>();
  const pi = { on: (name: string, fn: (event: unknown, ctx: ExtensionContext) => unknown) => {
    handlers.set(name, [...(handlers.get(name) ?? []), fn]); return () => {};
  }, sendMessage: (message: typeof sent[number]) => sent.push(message), sendUserMessage() {}, getThinkingLevel: () => "off" } as unknown as ExtensionAPI;
  const ctx = { isIdle: () => true, hasPendingMessages: () => false, signal: { aborted: false },
    sessionManager: { getEntries: () => entries, getSessionId: () => SESSION } } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, TARGET, true, process.cwd(), SESSION);
  const journal = path.join(meshRoot, "main-followups", "receiver.json");
  main.attachFollowUpDrain(ctx, 60_000, journal);
  drains.push(() => main.closeFollowUpDrain());
  return { main, sent, journal, entries, confirm() {
    for (const message of sent) entries.push({ type: "custom_message", customType: message.customType, details: message.details });
    for (const fn of handlers.get("agent_settled") ?? []) fn({ outcome: "completed" }, ctx);
    main.confirmInbox();
  } };
}
function files(directory: string): string[] {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => entry.isDirectory()
    ? files(path.join(directory, entry.name)) : [fs.readFileSync(path.join(directory, entry.name), "utf8")]);
}
const remote = (epoch = "owner:old") => ({ id: TARGET, rootId: TARGET, kind: "root", status: "idle", local: false,
  ownerHostId: "runtime:receiver", ownerIdentityId: TARGET, ownerIncarnation: epoch, controlProtocol: "v1",
  capabilities: ["followUp", "steer"] }) as FabricParticipantInfo;
const noMain = { id: FROM.id, local: true, matches: (id: string) => id === FROM.id, deliverAgent: vi.fn() } as unknown as Ports[2];

describe("opt-in scripted input on the existing agents sender/receiver path", () => {
  it.each(["main", SESSION, "a-published-name", "actor:one", "spawner"])("refuses non-exact target %s before delivery", async target => {
    const r = root(), n = native(r);
    await expect(router(r, n.main).routeMessage(target, "original", data(), "followUp")).rejects.toThrow(/exact.*session|session.*exact/i);
    expect(n.sent).toEqual([]);
  });

  it.skipIf(process.platform === "win32")("durably freezes original bytes before control publication and rejects a conflicting caller key", async () => {
    const r = root(), offered = data();
    let published = 0, key: string | undefined;
    const control = { request: async (...args: any[]) => {
      published++; key = args[5].idempotencyKey;
      expect(files(r).some(bytes => bytes.includes("original bytes") && bytes.includes("round-1"))).toBe(true);
      expect(args[3].data).toEqual(offered);
      return { queued: true, messageId: "receipt", routed: "mesh", acknowledged: true };
    } } as unknown as Ports[4];
    const sender = router(r, noMain, control, remote());
    await sender.routeMessage(TARGET, "original bytes", offered, "followUp");
    expect(key).toBeTruthy();
    await expect(sender.routeMessage(TARGET, "original bytes", offered, "followUp", undefined, { idempotencyKey: "different-caller-key" }))
      .rejects.toThrow(/idempotency|key/i);
    expect(published).toBe(1);
    await expect(router(r, noMain, control, remote("owner:new")).routeMessage(TARGET, "changed bytes", offered, "followUp"))
      .rejects.toThrow(/conflict|different|immutable/i);
    expect(published).toBe(1);
  });

  it.skipIf(process.platform === "win32")("reconciles receiver commit-before-ACK and sender restart with one native input, preserving exact bytes and attribution", async () => {
    const r = root(), n = native(r), offered = data();
    const text = "  original bytes\n";
    const first = await router(r, n.main).routeMessage(TARGET, text, offered, "steer");
    expect(n.sent).toHaveLength(1);
    expect(n.sent[0]!.details.deliveryId).toBeTruthy();
    expect(fs.readFileSync(n.journal, "utf8")).toContain("round-1");
    n.confirm();
    n.main.closeFollowUpDrain();
    // No sender/result memory survives. Native consumption evidence and immutable input do.
    const successor = native(r, n.entries);
    const replay = await router(r, successor.main).routeMessage(TARGET, text, offered, "steer");
    expect(replay.messageId).toBeTruthy();
    expect(successor.sent).toHaveLength(0);
    const command: FabricControlCommand = { version: 1, commandId: "new-physical-command", targetId: TARGET,
      operation: "steer", replyTo: "new-machine-sender", requestedAt: Date.now(), deadlineAt: offered.scriptedInput.deadlineAt,
      message: text, data: offered };
    expect(await router(r, successor.main).acceptControl(command, FROM, undefined, "mesh")).toMatchObject({ accepted: true });
    expect(successor.sent).toHaveLength(0);
    // Main trims presentation text; the durable scripted intent keeps exact offered bytes.
    expect(files(r).some(bytes => bytes.includes(JSON.stringify(text)))).toBe(true);
    expect(n.sent[0]!.content).toContain(text.trim());
    expect(n.sent[0]!.content).toContain(FROM.name);
    expect(first.messageId).toBeTruthy();
    await expect(router(r, successor.main).routeMessage(TARGET, "changed bytes", offered, "steer"))
      .rejects.toThrow(/conflict|different|immutable/i);
  });

  it.skipIf(process.platform === "win32")("preserves omitted triggerTurn across local admission then real remote control delivery after restart", async () => {
    const receiverRoot = root(), senderRoot = root(), n = native(receiverRoot), offered = data();
    const first = await router(receiverRoot, n.main).routeMessage(TARGET, "unchanged", offered, "followUp");
    n.confirm(); n.main.closeFollowUpDrain();
    const successor = native(receiverRoot, n.entries);
    const mesh = () => { const value = new MeshStore(receiverRoot, 65_536, 1_000); stores.push(value); return value; };
    const owner = new FabricControlPlane(mesh(), { id: TARGET, name: "Receiver", kind: "main" }, {
      enabled: true, hostId: "runtime:receiver", ownerIncarnation: "new-epoch", pollMs: 20 });
    const sender = new FabricControlPlane(mesh(), FROM, { enabled: true, hostId: "runtime:sender", pollMs: 20 });
    planes.push(owner, sender);
    owner.start((...args) => router(receiverRoot, successor.main).acceptControl(...args)); sender.start(() => ({ accepted: false }));
    const replay = await router(senderRoot, noMain, sender, remote("new-epoch")).routeMessage(TARGET, "unchanged", offered, "followUp");
    expect(replay).toMatchObject({ acknowledged: true, routed: "mesh", messageId: first.messageId });
    expect(n.sent).toHaveLength(1); expect(successor.sent).toHaveLength(0);
    // Same key but a genuinely different policy is NOT an equivalent default.
    await expect(router(senderRoot, noMain, sender, remote("new-epoch")).routeMessage(TARGET, "unchanged", offered, "followUp", undefined, { triggerTurn: false }))
      .rejects.toThrow(/conflict/i);
  });

  it.skipIf(process.platform === "win32")("returns a saved sender native ACK after restart and deadline without publication", async () => {
    const r = root(), n = native(r), offered = data(), sender = router(r, n.main);
    const first = await sender.routeMessage(TARGET, "saved receipt", offered, "steer");
    const clock = vi.spyOn(Date, "now").mockReturnValue(offered.scriptedInput.deadlineAt + 1);
    try {
      const failPublish = { request: vi.fn(() => { throw new Error("must not publish saved ACK"); }) } as unknown as Ports[4];
      const replay = await router(r, noMain, failPublish, remote("new-epoch")).routeMessage(TARGET, "saved receipt", offered, "steer");
      expect(replay).toMatchObject({ messageId: first.messageId, duplicate: true });
      expect(failPublish!.request).not.toHaveBeenCalled(); expect(n.sent).toHaveLength(1);
    } finally { clock.mockRestore(); }
  });

  it.skipIf(process.platform === "win32")("resumes original sender intents in a finite pass and leaves expired offers visible", async () => {
    const r = root(), live = data(), expired = { ...data(Date.now() - 10), scriptedInput: { ...data(Date.now() - 10).scriptedInput, inputId: "expired" } };
    for (const item of [live, expired]) prepareScriptedInput({ root: r, from: FROM, targetId: TARGET, message: "saved original", data: item,
      delivery: "followUp", triggerTurn: false });
    let sends = 0;
    const control = { request: async (...args: any[]) => {
      sends++; expect(args[3].message).toBe("saved original"); expect(args[3].triggerTurn).toBe(false);
      expect(args[3].data.scriptedInput.deadlineAt).toBe(live.scriptedInput.deadlineAt);
      return { queued: true, messageId: "native-receipt", acknowledged: true, routed: "mesh" };
    } } as unknown as Ports[4];
    const resumed = await router(r, noMain, control, remote("current-epoch")).resumeScriptedInputs();
    expect(resumed).toEqual(expect.arrayContaining([{ inputId: "round-1", status: "acknowledged" }, { inputId: "expired", status: "expired" }]));
    expect(sends).toBe(1);
    expect(listPendingScriptedInputs({ root: r, from: FROM }).map(item => (item.data as ReturnType<typeof data>).scriptedInput.inputId)).toEqual(["expired"]);
  });

  it("exposes the native provider boot hook without discarding its failure summary", async () => {
    const summary = [{ inputId: "still-missing", status: "expired" as const }];
    const resume = vi.spyOn(AgentMessageRouter.prototype, "resumeScriptedInputs").mockResolvedValue(summary);
    try {
      const provider = new AgentsProvider({ cwd: process.cwd() } as never, { identity: FROM } as never, {} as never,
        noMain as never, {} as never, undefined, {} as never);
      expect(await provider.resumeScriptedInputs(FROM)).toBe(summary);
      expect(resume).toHaveBeenCalledWith(FROM, undefined);
    } finally { resume.mockRestore(); }
  });

  it("does not turn a tagged control into a task/actor delivery or normalize its target", async () => {
    const r = root(), n = native(r), receiver = router(r, n.main);
    for (const targetId of ["main", SESSION, "actor:one"]) {
      const response = await receiver.acceptControl({ version: 1, commandId: targetId, targetId, operation: "steer",
        replyTo: FROM.id, requestedAt: Date.now(), message: "input", data: data() }, FROM, undefined, "mesh");
      expect(response.accepted).toBe(false);
    }
    expect(n.sent).toHaveLength(0);
  });

  it("rejects expired and malformed opted-in input while ordinary Main delivery is unchanged", async () => {
    const r = root(), n = native(r), sender = router(r, n.main);
    const expired = sender.routeMessage(TARGET, "expired", data(Date.now() - 1), "steer");
    if (process.platform === "win32") await expect(expired).rejects.toMatchObject({ code: "SCRIPTED_INPUT_NOT_DURABLE" });
    else await expect(expired).rejects.toThrow(/expir|deadline/i);
    await expect(sender.routeMessage(TARGET, "malformed", { scriptedInput: { version: 9 } }, "steer")).rejects.toThrow();
    expect(n.sent).toHaveLength(0);
    await expect(sender.routeMessage("main", "ordinary", undefined, "steer")).resolves.toMatchObject({ queued: true });
    expect(n.sent).toHaveLength(1);
  });
});
