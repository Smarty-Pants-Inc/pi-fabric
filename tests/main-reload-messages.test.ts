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
  const main = (idle = true) => {
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
    controller.attachFollowUpDrain(context, 60_000, path.join(meshRoot, "main-followups", `${encodeURIComponent(sessionId)}.json`));
    cleanup.push(() => controller.closeFollowUpDrain());
    return { controller, pi, context, emit: (name: string, event: any = {}) => { for (const fn of handlers.get(name) ?? []) fn(event, context); } };
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
    vi.spyOn(f.observer, "get").mockImplementation((id, _now, options) => get(id, now, options));
    vi.spyOn(f.observer, "lastKnown").mockImplementation((id) => known(id, now));
    await expect(f.sendRouter.routeMessage(identity.id, "too late", undefined, kind)).rejects.toThrow("Unknown Fabric participant");
  });
});
