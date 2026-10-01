import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

const roots: string[] = [];
const cleanup: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const sessionId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const identity: MeshIdentity = { id: `session:${sessionId}`, sessionId, name: "Main", kind: "main" };
const sender: MeshIdentity = { id: "session:sender", name: "sender", kind: "main", sessionId: "sender" };
const record = (who: MeshIdentity): FabricParticipantRecord => ({
  format: 1, id: who.id, rootId: who.id, ownerHostId: who.id, ownerIdentityId: who.id,
  kind: "root", name: who.name, status: "idle", capabilities: ["steer", "followUp", "fabric"],
  runner: "pi", transport: "host", controlProtocol: "v1", ...(who.sessionId ? { sessionId: who.sessionId } : {}),
  cwd: process.cwd(), startedAt: 1, updatedAt: Date.now(),
});
const fixture = async (filesOnly = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-main-reload-"));
  roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const mesh = () => new MeshStore(meshRoot, 64 * 1024, 1_000);
  if (filesOnly) await mesh().put({ key: LIVENESS_POLICY_KEY, value: { version: 1, participants: "files", hostLeases: "files" }, identity });
  const directory = (who: MeshIdentity) => {
    const result = new ParticipantDirectory(mesh(), { enabled: true, hostId: who.id, rootId: who.id, identity: who });
    result.registerSource(() => [record(who)]);
    cleanup.push(() => result.close());
    return result;
  };
  const owner = directory(identity);
  const observer = directory(sender);
  await owner.start();
  await observer.start();
  const entries: any[] = [];
  const sent: Array<{ content: string; details: any; options: any }> = [];
  const main = (idle = true, flushMs = 60_000) => {
    const handlers = new Map<string, Array<(event: any, ctx: ExtensionContext) => void>>();
    const pi = {
      on(name: string, handler: (event: any, ctx: ExtensionContext) => void) {
        handlers.set(name, [...(handlers.get(name) ?? []), handler]);
        return () => handlers.set(name, (handlers.get(name) ?? []).filter((fn) => fn !== handler));
      },
      sendMessage(message: any, options: any) {
        sent.push({ content: message.content, details: message.details, options });
        entries.push({ type: "custom_message", customType: message.customType, details: message.details });
      },
      getThinkingLevel: () => "off",
    } as unknown as ExtensionAPI;
    const context = { isIdle: () => idle, hasPendingMessages: () => false, sessionManager: { getEntries: () => entries } } as unknown as ExtensionContext;
    const controller = new MainAgentController(pi, identity.id, true, root, sessionId);
    controller.attachFollowUpDrain(context, flushMs, path.join(meshRoot, "main-followups", `${encodeURIComponent(sessionId)}.json`));
    cleanup.push(() => controller.closeFollowUpDrain());
    return { controller, pi, context, setIdle: (value: boolean) => { idle = value; }, emit: (name: string, event: any = {}) => { for (const fn of handlers.get(name) ?? []) fn(event, context); } };
  };
  const plane = (who: MeshIdentity) => {
    const result = new FabricControlPlane(mesh(), who, { enabled: true, hostId: who.id, pollMs: 20 });
    cleanup.push(() => result.close());
    return result;
  };
  const router = (target: MainAgentController, participants: ParticipantDirectory, control: FabricControlPlane) => new AgentMessageRouter(
    { status: () => { throw new Error("Unknown Fabric agent"); } } as never,
    { identity: sender, validateDirectMessage: () => {}, status: () => { throw new Error("Unknown Fabric actor"); } } as never,
    target, participants, control, (binding) => binding,
  );
  const senderControl = plane(sender);
  senderControl.start(() => ({ accepted: false }));
  const senderMain = new MainAgentController({} as ExtensionAPI, sender.id, true, root, "sender");
  const sendRouter = router(senderMain, observer, senderControl);
  return { owner, observer, entries, sent, main, plane, router, sendRouter, directory, mesh };
};

// Exercise the router's read-port contract even when native participant files invalidate by stat.
const cacheRootSnapshot = (directory: ParticipantDirectory) => {
  const get = directory.get.bind(directory);
  const cached = get(identity.id);
  vi.spyOn(directory, "get").mockImplementation((id, now, options) =>
    id === identity.id && !options?.fresh ? cached : get(id, now, options),
  );
};

describe("Main reload sender admission (smarty-dev#2160 item 4)", () => {
  it.each([false, true])("delivers followUp and steer once at shutdown, without a runtime, and just after session_start (files=%s)", async (filesOnly) => {
    const f = await fixture(filesOnly);
    const old = f.main();
    const oldControl = f.plane(identity);
    const oldRouter = f.router(old.controller, f.owner, oldControl);
    oldControl.start((command, from, signal) => oldRouter.acceptControl(command, from, signal));
    // Optional calls also exercise the same sender failure on the pre-fix source.
    old.controller.prepareReload?.();
    oldControl.pause?.();
    await f.owner.quiesce("reload");
    const outcomes: Array<Promise<unknown>> = [];
    const errors: string[] = [];
    const send = (phase: string) => {
      for (const kind of ["followUp", "steer"] as const) {
        const id = phase === "absent" ? sessionId : identity.id;
        outcomes.push(f.sendRouter.routeMessage(id, `${phase}-${kind}`, undefined, kind).catch((error) => { errors.push(error.message); return error; }));
      }
    };
    send("shutdown");
    await vi.waitFor(() => expect(f.mesh().read({ topic: "fabric.control.command", limit: 100 }).length + errors.length).toBe(2));
    expect(errors).toEqual([]);
    expect(f.sent).toHaveLength(0);
    await oldControl.close();
    old.controller.closeFollowUpDrain();
    await f.owner.close();
    // No old directory/host is running now, but its address survives the bounded reload.
    expect(f.observer.get(identity.id, Date.now(), { fresh: true })).toMatchObject({ status: "reloading", capabilities: ["steer", "followUp", "fabric"] });
    send("absent");
    await vi.waitFor(() => expect(f.mesh().read({ topic: "fabric.control.command", limit: 100 })).toHaveLength(4));
    expect(f.sent).toHaveLength(0);
    const fresh = f.main();
    const newDirectory = f.directory(identity);
    await newDirectory.start();
    const newControl = f.plane(identity);
    const newRouter = f.router(fresh.controller, newDirectory, newControl);
    newControl.start((command, from, signal) => newRouter.acceptControl(command, from, signal));
    send("after");
    for (const result of await Promise.all(outcomes)) expect(result).toMatchObject({ queued: true });
    await vi.waitFor(() => expect(f.sent).toHaveLength(6));
    for (const phase of ["shutdown", "absent", "after"]) for (const kind of ["followUp", "steer"]) {
      const deliveries = f.sent.filter((item) => item.content.includes(`${phase}-${kind}`));
      expect(deliveries).toHaveLength(1);
      expect(deliveries[0]!.options.deliverAs).toBe(kind);
    }
    await newControl.close();
    fresh.emit("turn_end", { context: { pendingMessages: [] } });
    fresh.controller.closeFollowUpDrain();
    const replay = f.main();
    replay.emit("turn_end", { context: { pendingMessages: [] } });
    expect(f.sent).toHaveLength(6);
  }, 15_000);

  it("real FabricRuntimeState shutdown/reinitialize retains all six sender messages, then a real exit rejects", async () => {
    const { FabricRuntimeState } = await import("../src/fabric-runtime-state.js");
    const { CapturedToolCatalog } = await import("../src/capture/catalog.js");
    const { normalizeFabricConfig } = await import("../src/config.js");
    const f = await fixture();
    await f.owner.close();
    const root = f.mesh().root;
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", path.dirname(root));
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(path.dirname(root), "agent"));
    const host = f.main();
    host.controller.closeFollowUpDrain();
    Object.assign(host.pi, { events: { emit: vi.fn() } });
    const context = Object.assign(host.context, {
      cwd: path.dirname(root), hasUI: false, mode: "rpc", isProjectTrusted: () => true,
      modelRegistry: { getAvailable: () => [], find: () => undefined },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [],
        getLeafId: () => null, getEntries: () => f.entries },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    });
    const config = normalizeFabricConfig({ fullCodeMode: false,
      mesh: { enabled: true, root, actorPollMs: 20 }, agents: { enabled: false }, residency: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false } });
    const create = () => new FabricRuntimeState(host.pi, new CapturedToolCatalog(), { paths: {
      extension: path.resolve("dist/index.js"), worker: path.resolve("tests/fixtures/fake-worker.mjs"),
      residentHost: path.join(path.dirname(root), "unused.mjs"), skills: path.dirname(root),
    } });
    const old = create();
    const fresh = create();
    let release: (() => void) | undefined;
    let entered = false;
    let shutdown: Promise<void> | undefined;
    const outcomes: Array<Promise<unknown>> = [];
    const send = (phase: string) => {
      for (const kind of ["followUp", "steer"] as const) {
        outcomes.push(f.sendRouter.routeMessage(identity.id, `runtime-${phase}-${kind}`, undefined, kind).catch((error) => error));
      }
    };
    try {
      await old.initialize(context, config);
      const close = old.agents.close.bind(old.agents);
      vi.spyOn(old.agents, "close").mockImplementation(async () => {
        entered = true;
        await new Promise<void>((resolve) => { release = resolve; });
        await close();
      });
      shutdown = old.shutdown("reload");
      send("shutdown");
      await vi.waitFor(() => expect(entered).toBe(true));
      expect(f.sent).toHaveLength(0);
      expect(f.observer.get(identity.id, Date.now(), { fresh: true })).toMatchObject({ status: "reloading" });
      release!();
      await shutdown;
      send("gap");
      await vi.waitFor(() => expect(f.mesh().read({ topic: "fabric.control.command", limit: 100 })).toHaveLength(4));
      expect(f.sent).toHaveLength(0);
      await fresh.initialize(context, config);
      send("after");
      for (const outcome of await Promise.all(outcomes)) expect(outcome).toMatchObject({ queued: true });
      expect(f.sent).toHaveLength(6);
      for (const phase of ["shutdown", "gap", "after"]) for (const kind of ["followUp", "steer"]) {
        expect(f.sent.filter((message) => message.content.includes(`runtime-${phase}-${kind}`))).toHaveLength(1);
      }
      host.emit("turn_end", { context: { pendingMessages: [] } });
      await fresh.shutdown("exit");
      for (const kind of ["followUp", "steer"] as const) {
        await expect(f.sendRouter.routeMessage(identity.id, "gone", undefined, kind)).rejects.toThrow("Unknown Fabric participant");
      }
    } finally {
      release?.();
      await shutdown;
      await old.shutdown();
      await fresh.shutdown();
      await Promise.all(outcomes);
      vi.restoreAllMocks();
      vi.unstubAllEnvs();
    }
  }, 30_000);

  it.each(["followUp", "steer"] as const)("accepts %s while the old runtime is absent, before the replacement joins", async (kind) => {
    const f = await fixture();
    await f.owner.quiesce("reload");
    await f.owner.close();
    let failure: Error | undefined;
    const pending = f.sendRouter.routeMessage(identity.id, `gap-${kind}`, undefined, kind).catch((error) => { failure = error; return error; });
    await vi.waitFor(() => expect(f.mesh().read({ topic: "fabric.control.command", limit: 100 }).length + (failure ? 1 : 0)).toBe(1));
    expect(failure).toBeUndefined();
    expect(f.sent).toHaveLength(0);
    const fresh = f.main();
    const nextDirectory = f.directory(identity);
    await nextDirectory.start();
    const control = f.plane(identity);
    const owner = f.router(fresh.controller, nextDirectory, control);
    control.start((command, from, signal) => owner.acceptControl(command, from, signal));
    expect(await pending).toMatchObject({ queued: true });
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]!.options.deliverAs).toBe(kind);
  });

  it.each(["followUp", "steer"] as const)("journals an in-flight %s admitted as reload starts, without handing it to the old Pi", async (kind) => {
    const f = await fixture();
    const old = f.main();
    const control = f.plane(identity);
    const owner = f.router(old.controller, f.owner, control);
    old.controller.prepareReload?.();
    const command = { version: 1 as const, commandId: `inflight-${kind}`, targetId: identity.id, replyTo: sender.id,
      operation: kind, message: `inflight-${kind}`, triggerTurn: false, requestedAt: Date.now() };
    expect(await owner.acceptControl(command, sender)).toMatchObject({ accepted: true });
    // Hooks that were already dispatched cannot flush into the old runner either.
    old.emit("turn_end", { context: { pendingMessages: [] } });
    old.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
    expect(f.sent).toHaveLength(0);
    old.controller.closeFollowUpDrain();
    const fresh = f.main();
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]!.options).toMatchObject({ deliverAs: kind, triggerTurn: false });
    fresh.emit("turn_end", { context: { pendingMessages: [] } });
    const next = f.router(fresh.controller, f.owner, control);
    expect(await next.acceptControl(command, sender)).toMatchObject({ accepted: true });
    expect(f.sent).toHaveLength(1);
    fresh.controller.closeFollowUpDrain();
    f.main();
    expect(f.sent).toHaveLength(1);
  });

  it.each(["followUp", "steer"] as const)("a busy replacement preserves the journalled %s delivery mode and trigger policy", async (kind) => {
    const f = await fixture();
    const old = f.main();
    old.controller.prepareReload?.();
    old.controller.deliverAgent({ from: sender, message: "busy replay", delivery: kind, triggerTurn: false, deliveryId: `busy-${kind}` });
    old.controller.closeFollowUpDrain();
    const fresh = f.main(false);
    expect(f.sent).toHaveLength(0);
    fresh.emit("turn_end", { context: { pendingMessages: [] } });
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]!.options).toMatchObject({ deliverAs: kind, triggerTurn: false });
  });

  it.each(["aborted", "error", undefined] as const)("a busy replay cannot wake Main after a %s settle", async (outcome) => {
    const f = await fixture();
    const old = f.main();
    old.controller.prepareReload();
    const requests = (["steer", "followUp", "nextTurn"] as const).map((delivery) => ({
      from: sender, message: `halted-${delivery}`, delivery, triggerTurn: true, deliveryId: `halted-${delivery}`,
    }));
    const ids = requests.map((request) => old.controller.deliverAgent(request).messageId);
    old.controller.closeFollowUpDrain();
    const fresh = f.main(false);
    if (outcome !== undefined) fresh.emit("agent_before_settle", { outcome, context: { pendingMessages: [] } });
    fresh.emit("agent_settled", { outcome });
    expect(f.sent.map((item) => item.options)).toEqual(requests.map(({ delivery }) => ({ deliverAs: delivery, triggerTurn: false })));
    expect(f.sent.map((item) => item.details.id)).toEqual(ids);
    for (const request of requests) expect(fresh.controller.deliverAgent(request)).toMatchObject({ duplicate: true });
    fresh.controller.closeFollowUpDrain();
    f.main();
    expect(f.sent).toHaveLength(requests.length);
  });

  it.each([false, true])("a failed compaction suppresses triggering replay (aborted=%s)", async (aborted) => {
    const f = await fixture();
    const old = f.main();
    old.controller.prepareReload();
    for (const delivery of ["steer", "followUp"] as const) old.controller.deliverAgent({
      from: sender, message: `compact-${delivery}`, delivery, deliveryId: `compact-${delivery}`,
    });
    old.controller.closeFollowUpDrain();
    const fresh = f.main(false);
    fresh.setIdle(true);
    const operation = new AbortController();
    fresh.emit("session_before_compact", { reason: "manual", signal: operation.signal });
    if (aborted) operation.abort();
    fresh.emit("session_compact_failed", { reason: "manual", aborted, errorMessage: "failed" });
    fresh.emit("agent_settled", { outcome: "completed" }); // an unrelated completion cannot lift the owner's stop
    expect(f.sent.map((item) => item.options)).toEqual(["steer", "followUp"].map((deliverAs) => ({ deliverAs, triggerTurn: false })));
  });

  it.each([0, 60_000].flatMap(flushMs => [
    { errorMessage: "Already compacted", willRetry: false, failed: false },
    { errorMessage: "Compaction failed: Already compacted", willRetry: false, failed: false },
    { errorMessage: "Nothing to compact (session too small)", willRetry: false, failed: false },
    { errorMessage: "Compaction failed: Nothing to compact (session too small)", willRetry: false, failed: false },
    { errorMessage: "Compaction cancelled", willRetry: false, failed: false },
    { errorMessage: "Compaction failed: provider quoted Already compacted", willRetry: false, failed: true },
    { errorMessage: "Compaction failed: unavailable", willRetry: false, failed: true },
    { errorMessage: "Compaction failed: unavailable", willRetry: true, failed: false },
  ].map(test => ({ flushMs, ...test }))))(
    "compaction failure keeps boundary replay passive without inventing an owner stop ($errorMessage, retry=$willRetry, flushMs=$flushMs)",
    async ({ flushMs, errorMessage, willRetry, failed }) => {
      const f = await fixture();
      const old = f.main(true, flushMs);
      old.controller.prepareReload();
      const request = { from: sender, message: "boundary replay", delivery: "steer" as const, deliveryId: "compact-replay" };
      old.controller.deliverAgent(request);
      old.controller.closeFollowUpDrain();
      const fresh = f.main(false, flushMs);
      fresh.setIdle(true);
      fresh.emit("session_compact_failed", { reason: "manual", aborted: false, errorMessage, willRetry });
      expect(f.sent[0]!.options.triggerTurn).toBe(false);
      fresh.controller.deliverAgent({ from: sender, message: "later peer", delivery: "followUp" });
      expect(f.sent.at(-1)!.options.triggerTurn).toBe(!failed);
      fresh.controller.closeFollowUpDrain();
      const reloaded = f.main(true, flushMs);
      reloaded.controller.deliverAgent({ from: sender, message: "peer after reload", delivery: "steer" });
      expect(f.sent.at(-1)!.options.triggerTurn).toBe(true); // Failure is not durable owner intent.
      reloaded.emit("session_compact_failed", { reason: "manual", aborted: false, errorMessage, willRetry });
      reloaded.emit("session_compact", { reason: "manual" }); // Recovery, with no user input.
      reloaded.controller.deliverAgent({ from: sender, message: "peer after recovery", delivery: "steer" });
      expect(f.sent.at(-1)!.options.triggerTurn).toBe(true);
    },
  );

  it.each([0, 60_000])("a lost direct Pi handoff is reconciled passively at a failed boundary (flushMs=%s)", async (flushMs) => {
    const f = await fixture();
    const old = f.main(false, flushMs);
    const request = { from: sender, message: "lost handoff", delivery: "steer" as const, deliveryId: "lost-handoff" };
    const first = old.controller.deliverAgent(request);
    expect(f.sent[0]!.options.triggerTurn).toBe(true);
    old.controller.closeFollowUpDrain();
    f.entries.splice(0); // no durable receipt and no surviving Pi queue at the replacement boundary
    const fresh = f.main(false, flushMs);
    expect(f.sent).toHaveLength(1);
    fresh.emit("agent_before_settle", { outcome: "error", context: { pendingMessages: [] } });
    fresh.emit("agent_settled", { outcome: "error" });
    expect(f.sent).toHaveLength(2);
    expect(f.sent[1]!.details.id).toBe(first.messageId);
    expect(f.sent[1]!.options).toEqual({ deliverAs: "steer", triggerTurn: false });
    expect(fresh.controller.deliverAgent(request)).toMatchObject({ duplicate: true });
    fresh.controller.closeFollowUpDrain();
    f.main(true, flushMs);
    expect(f.sent).toHaveLength(2);
  });

  it.each([0, 60_000].flatMap((flushMs) => [false, true].map((ownerHalt) => ({ flushMs, ownerHalt }))))(
    "a terminal provider failure stays passive until a successful turn, without lifting an owner halt (flushMs=$flushMs, ownerHalt=$ownerHalt)",
    async ({ flushMs, ownerHalt }) => {
      const f = await fixture();
      const host = f.main(false, flushMs);
      host.emit("turn_end", { message: { stopReason: "error" } });
      host.emit("agent_before_settle", { outcome: "error", context: { pendingMessages: [] } });
      host.emit("agent_settled", { outcome: "error" });
      host.setIdle(true);
      if (ownerHalt) host.controller.halt();
      host.controller.deliverAgent({ from: sender, message: "failed provider", delivery: "followUp" });
      expect(f.sent.at(-1)!.options.triggerTurn).toBe(false);
      host.emit("agent_start");
      host.emit("input", { source: "extension" });
      host.controller.deliverAgent({ from: sender, message: "not recovered yet", delivery: "steer" });
      expect(f.sent.at(-1)!.options.triggerTurn).toBe(false);
      host.emit("turn_end", { message: { stopReason: "stop" }, context: { pendingMessages: [] } });
      host.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
      host.emit("agent_settled", { outcome: "completed" });
      host.controller.deliverAgent({ from: sender, message: "provider recovered", delivery: "followUp" });
      expect(f.sent.at(-1)!.options.triggerTurn).toBe(!ownerHalt);
    },
  );

  it.each([0, 60_000])("cancel then reload keeps replay passive until user input (flushMs=%s)", async (flushMs) => {
    const f = await fixture();
    const old = f.main(false, flushMs);
    old.emit("agent_settled", { outcome: "aborted" });
    old.controller.prepareReload();
    const request = { from: sender, message: "after cancel", delivery: "steer" as const, deliveryId: "cancel-gap" };
    const admitted = old.controller.deliverAgent(request);
    old.controller.closeFollowUpDrain();
    const fresh = f.main(true, flushMs);
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]!.details.id).toBe(admitted.messageId);
    expect(f.sent[0]!.options).toEqual({ deliverAs: "steer", triggerTurn: false });
    fresh.emit("agent_start");
    fresh.emit("input", { source: "extension" });
    fresh.controller.deliverAgent({ from: sender, message: "live while stopped", delivery: "followUp" });
    expect(f.sent.at(-1)!.options.triggerTurn).toBe(false);
    fresh.emit("input", { source: "interactive" });
    fresh.controller.deliverAgent({ from: sender, message: "user resumed", delivery: "steer" });
    expect(f.sent.at(-1)!.options.triggerTurn).toBe(true);
    expect(fresh.controller.deliverAgent(request)).toMatchObject({ duplicate: true });
  });

  it.each([0, 60_000].flatMap((flushMs) => ["EACCES", "EIO", "parse", "shape"].map((failure) => ({ flushMs, failure }))))(
    "an unreadable halt index keeps replay and gap control passive until user input ($failure, flushMs=$flushMs)", async ({ flushMs, failure }) => {
      const f = await fixture();
      const old = f.main(false, flushMs);
      old.controller.prepareReload();
      old.controller.deliverAgent({ from: sender, message: "unverified replay", delivery: "steer", deliveryId: "unverified-replay" });
      old.controller.halt();
      old.controller.closeFollowUpDrain();
      const index = path.join(f.mesh().root, "main-followups", `${encodeURIComponent(sessionId)}.json.delivered`);
      expect(JSON.parse(fs.readFileSync(index, "utf8")).halted).toBe(true);
      if (failure === "parse") fs.writeFileSync(index, "{");
      if (failure === "shape") fs.writeFileSync(index, JSON.stringify({ version: 1, ids: [], halted: "unknown" }));
      const read = fs.readFileSync;
      let attempts = 0;
      const fault = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
        if (String(file) === index && ["EACCES", "EIO"].includes(failure)) {
          attempts++;
          throw Object.assign(new Error("halt index unreadable"), { code: failure });
        }
        return (read as (...args: unknown[]) => unknown)(file, ...args);
      }) as typeof fs.readFileSync);
      const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
      try {
        const fresh = f.main(true, flushMs);
        expect(f.sent).toHaveLength(1);
        expect(f.sent[0]!.options).toEqual({ deliverAs: "steer", triggerTurn: false });
        if (failure === "EACCES") expect(attempts).toBe(5);
        if (failure === "EIO") expect(attempts).toBe(1);
        const owner = f.router(fresh.controller, f.owner, f.plane(identity));
        const command = { version: 1 as const, commandId: "unverified-gap", targetId: identity.id, replyTo: sender.id,
          operation: "followUp" as const, message: "unverified gap", requestedAt: Date.now() };
        expect(await owner.acceptControl(command, sender)).toMatchObject({ accepted: true });
        fresh.emit("agent_settled", { outcome: "completed" });
        fresh.emit("input", { source: "extension" });
        expect(f.sent.at(-1)!.options.triggerTurn).toBe(false);
        expect(warning.mock.calls.filter(([message]) => String(message).includes("halt index"))).toHaveLength(1);
        fault.mockRestore();
        // Delivery must never overwrite unknown owner authority with a running state.
        if (["EACCES", "EIO"].includes(failure)) expect(JSON.parse(read(index, "utf8")).halted).toBe(true);
        fresh.emit("input", { source: "interactive" });
        fresh.controller.deliverAgent({ from: sender, message: "user recovered", delivery: "steer" });
        expect(f.sent.at(-1)!.options.triggerTurn).toBe(true);
        expect(JSON.parse(read(index, "utf8")).halted).toBeUndefined();
      } finally {
        fault.mockRestore();
        warning.mockRestore();
      }
    });

  it.each([0, 60_000])("an unreadable halt index keeps gap control passive without a payload journal (flushMs=%s)", async (flushMs) => {
    const f = await fixture();
    const old = f.main(true, flushMs);
    old.controller.halt();
    old.controller.closeFollowUpDrain();
    const journal = path.join(f.mesh().root, "main-followups", `${encodeURIComponent(sessionId)}.json`);
    expect(fs.existsSync(journal)).toBe(false);
    const read = fs.readFileSync;
    const fault = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === `${journal}.delivered`) throw Object.assign(new Error("halt index unreadable"), { code: "EIO" });
      return (read as (...args: unknown[]) => unknown)(file, ...args);
    }) as typeof fs.readFileSync);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const fresh = f.main(true, flushMs);
      const owner = f.router(fresh.controller, f.owner, f.plane(identity));
      expect(await owner.acceptControl({ version: 1, commandId: "gap-only", targetId: identity.id, replyTo: sender.id,
        operation: "steer", message: "gap without journal", requestedAt: Date.now() }, sender)).toMatchObject({ accepted: true });
      expect(f.sent).toHaveLength(1);
      expect(f.sent[0]!.options.triggerTurn).toBe(false);
      expect(warning).toHaveBeenCalledTimes(1);
    } finally {
      fault.mockRestore();
      warning.mockRestore();
    }
  });

  it.each([0, 60_000])("a transient halt-index read failure retries and retains readable running permission (flushMs=%s)", async (flushMs) => {
    const f = await fixture();
    const journal = path.join(f.mesh().root, "main-followups", `${encodeURIComponent(sessionId)}.json`);
    fs.mkdirSync(path.dirname(journal), { recursive: true });
    fs.writeFileSync(`${journal}.delivered`, JSON.stringify({ version: 1, ids: [] }));
    const read = fs.readFileSync;
    let attempts = 0;
    const fault = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === `${journal}.delivered` && ++attempts <= 2) throw Object.assign(new Error("transient"), { code: "EBUSY" });
      return (read as (...args: unknown[]) => unknown)(file, ...args);
    }) as typeof fs.readFileSync);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const fresh = f.main(true, flushMs);
      expect(attempts).toBe(3);
      fresh.controller.deliverAgent({ from: sender, message: "read recovered", delivery: "steer" });
      expect(f.sent[0]!.options.triggerTurn).toBe(true);
      expect(warning).not.toHaveBeenCalled();
    } finally {
      fault.mockRestore();
      warning.mockRestore();
    }
  });

  it.each([0, 60_000])("a readable not-halted index permits replay and gap control to wake (flushMs=%s)", async (flushMs) => {
    const f = await fixture();
    const old = f.main(false, flushMs);
    old.controller.prepareReload();
    old.controller.deliverAgent({ from: sender, message: "running replay", delivery: "steer", deliveryId: "running-replay" });
    old.controller.closeFollowUpDrain();
    const index = path.join(f.mesh().root, "main-followups", `${encodeURIComponent(sessionId)}.json.delivered`);
    fs.writeFileSync(index, JSON.stringify({ version: 1, ids: [], halted: false }));
    const fresh = f.main(true, flushMs);
    expect(f.sent[0]!.options.triggerTurn).toBe(true);
    const owner = f.router(fresh.controller, f.owner, f.plane(identity));
    expect(await owner.acceptControl({ version: 1, commandId: "running-gap", targetId: identity.id, replyTo: sender.id,
      operation: "steer", message: "running gap", requestedAt: Date.now() }, sender)).toMatchObject({ accepted: true });
    expect(f.sent.at(-1)!.options.triggerTurn).toBe(true);
  });

  it("an Escape halt latch survives reload even without an aborted turn or settle", async () => {
    const f = await fixture();
    const old = f.main();
    // Escape while idle has no aborted run event. Optional invocation lets this fail at the boundary on the old head.
    (old.controller as MainAgentController & { halt?: () => void }).halt?.();
    old.controller.prepareReload();
    old.controller.deliverAgent({ from: sender, message: "escape-gap", delivery: "followUp", deliveryId: "escape-gap" });
    old.controller.closeFollowUpDrain();
    f.main();
    expect(f.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: false });
  });

  it("the sender class is part of the coalescing policy", async () => {
    const f = await fixture();
    const main = f.main(false);
    const request = { from: sender, message: "main sender", delivery: "followUp" as const, data: { coalesceKey: "same" } };
    const first = main.controller.deliverAgent(request);
    const second = main.controller.deliverAgent({ ...request, from: { ...sender, kind: "agent" }, message: "agent sender" });
    expect(second.coalesced).toBeUndefined();
    main.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
    expect(f.sent.flatMap((item) => item.details.items ?? [item.details]).map((item) => item.id)).toEqual([first.messageId, second.messageId]);
  });

  it("a mixed-policy reload queue survives later same-key followUps exactly once in order", async () => {
    const f = await fixture();
    const old = f.main();
    old.controller.prepareReload();
    const policies = [
      { delivery: "steer", triggerTurn: true },
      { delivery: "steer", triggerTurn: false },
      { delivery: "followUp", triggerTurn: false },
      { delivery: "nextTurn", triggerTurn: true },
      { delivery: "nextTurn", triggerTurn: false },
    ] as const;
    const originals = policies.map((policy, index) => ({
      ...policy, from: sender, message: `journal-${index}`, deliveryId: `journal-${index}`,
      data: { coalesceKey: `policy-${index}` },
    }));
    const originalIds = originals.map((request) => old.controller.deliverAgent(request).messageId);
    old.controller.closeFollowUpDrain();
    const fresh = f.main(false);
    expect(f.sent).toHaveLength(0);
    const later = originals.map((request, index) => ({
      ...request, delivery: "followUp" as const, triggerTurn: true,
      message: `later-${index}`, deliveryId: `later-${index}`,
    }));
    const results = later.map((request) => fresh.controller.deliverAgent(request));
    expect(results.map((result) => result.coalesced)).toEqual(policies.map(() => undefined));
    expect(f.sent).toHaveLength(0);
    fresh.emit("turn_end", { context: { pendingMessages: [] } });
    fresh.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
    const expectedIds = [...originalIds, ...results.map((result) => result.messageId)];
    expect(f.sent.flatMap((item) => item.details.items ?? [item.details]).map((item) => item.id)).toEqual(expectedIds);
    expect(f.sent.slice(0, policies.length).map((item) => item.options)).toEqual(
      policies.map(({ delivery, triggerTurn }) => ({ deliverAs: delivery, triggerTurn })),
    );
    expect(f.sent.at(-1)!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    fresh.emit("turn_end", { context: { pendingMessages: [] } });
    for (const request of [...originals, ...later]) {
      expect(fresh.controller.deliverAgent(request)).toMatchObject({ duplicate: true });
    }
    fresh.controller.closeFollowUpDrain();
    const replay = f.main(false);
    replay.emit("turn_end", { context: { pendingMessages: [] } });
    replay.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
    expect(f.sent.flatMap((item) => item.details.items ?? [item.details]).map((item) => item.id)).toEqual(expectedIds);
  });

  it("a matching reload delivery policy coalesces without losing its explicit replay policy", async () => {
    const f = await fixture();
    const old = f.main();
    old.controller.prepareReload();
    const request = { from: sender, message: "original", delivery: "followUp" as const,
      triggerTurn: true, deliveryId: "matching-original", data: { coalesceKey: "matching" } };
    const first = old.controller.deliverAgent(request);
    old.controller.closeFollowUpDrain();
    const fresh = f.main(false);
    const replacement = fresh.controller.deliverAgent({ ...request, message: "replacement", deliveryId: "matching-replacement" });
    expect(replacement).toMatchObject({ coalesced: true, replacedMessageId: first.messageId });
    fresh.emit("turn_end", { context: { pendingMessages: [] } });
    expect(f.sent).toHaveLength(1);
    expect(f.sent[0]!.details.id).toBe(replacement.messageId);
    expect(f.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: true });
    expect(fresh.controller.deliverAgent(request)).toMatchObject({ duplicate: true });
    fresh.emit("turn_end", { context: { pendingMessages: [] } });
    fresh.controller.closeFollowUpDrain();
    f.main();
    expect(f.sent).toHaveLength(1);
  });

  it("keeps a fixed reload expiry through heartbeats and keeps the Main discoverable as a peer", async () => {
    const f = await fixture(true);
    await f.owner.quiesce("reload");
    const until = f.observer.get(identity.id, Date.now(), { fresh: true })!.reloadUntil!;
    await f.owner.refresh();
    expect(f.observer.get(identity.id, Date.now(), { fresh: true })!.reloadUntil).toBe(until);
    expect(f.observer.peers().map((peer) => peer.id)).toContain(identity.id);
    await f.owner.close();
    expect(f.observer.get(identity.id, until + 1, { fresh: true })).toBeUndefined();
  });

  it.each(["followUp", "steer"] as const)("freshens a cached native idle root before %s during shutdown", async (kind) => {
    const f = await fixture();
    cacheRootSnapshot(f.observer);
    expect(f.observer.get(identity.id)).toMatchObject({ status: "idle" });
    await f.owner.quiesce();
    expect(f.observer.get(identity.id)).toMatchObject({ status: "idle" }); // cached, not admission authority
    await expect(f.sendRouter.routeMessage(identity.id, "gone", undefined, kind)).rejects.toThrow("is shutting down");
    expect(f.mesh().read({ topic: "fabric.control.command", limit: 100 })).toEqual([]);
  });

  it.each(["followUp", "steer"] as const)("freshens a cached native idle root to bound %s by the reload lease", async (kind) => {
    const f = await fixture();
    cacheRootSnapshot(f.observer);
    expect(f.observer.get(identity.id)).toMatchObject({ status: "idle" });
    await f.owner.quiesce("reload");
    expect(f.observer.get(identity.id)).toMatchObject({ status: "idle" });
    const until = f.owner.get(identity.id, Date.now(), { fresh: true })!.reloadUntil!;
    const request = vi.spyOn(f.sendRouter.control!, "request").mockResolvedValue({
      queued: true, messageId: "reload-gap", routed: "mesh", acknowledged: true,
    });
    const before = Date.now();
    await expect(f.sendRouter.routeMessage(identity.id, "gap", undefined, kind)).resolves.toMatchObject({ queued: true });
    const options = request.mock.calls[0]![5]!;
    expect(options.routedRemoteHost).toBeNull();
    expect(options.timeoutMs).toBeGreaterThan(0);
    expect(options.timeoutMs).toBeLessThanOrEqual(until - before);
    expect(options.timeoutMs).toBeGreaterThanOrEqual(until - Date.now());
  });

  it.each(["followUp", "steer"] as const)("a real exit still rejects %s at shutdown and after removal", async (kind) => {
    const f = await fixture();
    await f.owner.quiesce();
    await expect(f.sendRouter.routeMessage(identity.id, "gone", undefined, kind)).rejects.toThrow("is shutting down");
    await f.owner.close();
    await expect(f.sendRouter.routeMessage(identity.id, "gone", undefined, kind)).rejects.toThrow("Unknown Fabric participant");
  });

  it.each(["followUp", "steer"] as const)("an expired reload lease rejects %s rather than using the recently-lapsed reply grace", async (kind) => {
    const f = await fixture();
    await f.owner.quiesce("reload");
    await f.owner.close();
    const now = Date.now() + 120_000;
    const get = f.observer.get.bind(f.observer);
    const known = f.observer.lastKnown.bind(f.observer);
    const peers = f.observer.peers.bind(f.observer);
    vi.spyOn(f.observer, "get").mockImplementation((id, _now, options) => get(id, now, options));
    vi.spyOn(f.observer, "lastKnown").mockImplementation((id) => known(id, now));
    // Discovery and admission must observe the same expired lease, including #201's peer hint.
    vi.spyOn(f.observer, "peers").mockImplementation(() => peers(now));
    await expect(f.sendRouter.routeMessage(identity.id, "too late", undefined, kind)).rejects.toThrow("Unknown Fabric participant");
    expect(f.mesh().read({ topic: "fabric.control.command", limit: 100 })).toEqual([]);
  });
});
