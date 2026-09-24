import { afterEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import type { AgentRunResult } from "../src/agents/types.js";
import type { FabricActorMessage } from "../src/actors/types.js";
import type { FabricControlCommand } from "../src/topology/control-plane.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { collectAgentToolPreviewNodes, waitWithProgress, waitWithActorProgress } from "../src/providers/agents-progress.js";
import { collectAgentToolPreviewNodes as publicPreview, type AgentToolPreviewTreeOptions } from "../src/providers/agents-provider.js";

const record = (id = "run"): AgentRunResult => ({
  id, name: id, task: "task", status: "completed", runner: "pi", transport: "process",
  cwd: "/project", startedAt: 1, updatedAt: 2, turns: 1, toolCalls: 2, text: "done",
  usage: { input: 3, output: 4, cacheRead: 0, cacheWrite: 0, cost: 0 },
});

const message: FabricActorMessage = {
  id: "reply", actorId: "actor", actorName: "Actor", direction: "out", source: "actor", createdAt: 1,
};

afterEach(() => vi.useRealTimers());

describe("agents provider progress service boundaries", () => {
  it("preserves the public preview export identity", () => {
    expect(publicPreview).toBe(collectAgentToolPreviewNodes);
    expectTypeOf<AgentToolPreviewTreeOptions>().toEqualTypeOf<Parameters<typeof collectAgentToolPreviewNodes>[1]>();
  });

  it("attaches final metrics and preview even before the first poll", async () => {
    vi.useFakeTimers();
    const result = record();
    const sink = { update: vi.fn(), activity: vi.fn(), attachPreview: vi.fn() };
    await expect(waitWithProgress(
      { wait: async () => result, status: () => result }, { read: vi.fn() }, "run", sink, () => true,
    )).resolves.toBe(result);
    expect(sink.activity).toHaveBeenCalledWith({ type: "metrics", tokens: 7, toolCalls: 2, cost: 0 });
    expect(sink.attachPreview).toHaveBeenCalledWith(expect.objectContaining({ id: "run", status: "completed" }));
    expect(sink.update).toHaveBeenCalledWith("Agent run: completed");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("clears the poll and preserves rejection when cancellation removes the run", async () => {
    vi.useFakeTimers();
    const failure = new Error("cancelled");
    await expect(waitWithProgress({
      wait: () => Promise.reject(failure),
      status: () => { throw new Error("Unknown Fabric agent"); },
    }, { read: vi.fn() }, "run", { update: vi.fn() }, () => true)).rejects.toBe(failure);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rejects polling errors and stops polling even if the worker is still pending", async () => {
    vi.useFakeTimers();
    const failure = new Error("status unavailable");
    const result = waitWithProgress({
      wait: () => new Promise<AgentRunResult>(() => {}),
      status: () => { throw failure; },
    }, { read: vi.fn() }, "run", { update: vi.fn() }, () => true);
    const assertion = expect(result).rejects.toBe(failure);
    await vi.advanceTimersByTimeAsync(1000);
    await assertion;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("uses the latest terminal actor worker and tolerates transcript cleanup", async () => {
    vi.useFakeTimers();
    const sink = { update: vi.fn(), attachPreview: vi.fn() };
    const read = vi.fn(() => { throw new Error("log removed"); });
    await waitWithActorProgress({ list: () => [
      { ...record("old"), actorId: "actor" },
      { ...record("new"), actorId: "actor", logFile: "/removed" },
    ] }, { read }, "actor", "Actor", Promise.resolve(message), sink, () => true);
    expect(sink.attachPreview).toHaveBeenCalledWith(expect.objectContaining({ id: "new", tools: [] }));
    expect(read).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });
});

type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const routing = () => {
  const agents = {
    status: vi.fn<Ports[0]["status"]>(() => { throw new Error("Unknown Fabric agent"); }),
    steer: vi.fn<Ports[0]["steer"]>(), followUp: vi.fn<Ports[0]["followUp"]>(), stop: vi.fn<Ports[0]["stop"]>(),
  };
  const actors = {
    identity: { id: "main", name: "Main", kind: "main" as const },
    status: vi.fn<Ports[1]["status"]>(() => { throw new Error("Unknown Fabric actor"); }),
    validateDirectMessage: vi.fn<Ports[1]["validateDirectMessage"]>(),
    tell: vi.fn<Ports[1]["tell"]>(), ask: vi.fn<Ports[1]["ask"]>(), stop: vi.fn<Ports[1]["stop"]>(),
    steerRemote: vi.fn<Ports[1]["steerRemote"]>(), resolveBinding: vi.fn<Ports[1]["resolveBinding"]>(),
  };
  const main = {
    id: "main", local: true, matches: (id: string) => id === "main",
    deliverAgent: vi.fn<Ports[2]["deliverAgent"]>(() => ({ queued: true, messageId: "main-msg", routed: "local" })),
  };
  const participants = { get: vi.fn<Ports[3]["get"]>(), scheduleRefresh: vi.fn() };
  const control = { request: vi.fn<NonNullable<Ports[4]>["request"]>() };
  const resolveBinding = vi.fn<Ports[5]>((binding) => binding);
  const router = new AgentMessageRouter(agents, actors, main, participants, control, resolveBinding);
  return { router, agents, actors, main, participants, control };
};
const participant = (): FabricParticipantInfo => ({
  format: 1, id: "main", name: "Main", kind: "root", rootId: "main", ownerHostId: "host",
  ownerIdentityId: "owner", status: "running", runner: "pi", transport: "host",
  capabilities: ["followUp"], startedAt: 1, updatedAt: 1, controlProtocol: "v1", local: false, stale: false,
});
const command = (operation: FabricControlCommand["operation"]): FabricControlCommand => ({
  version: 1, commandId: "cmd", targetId: "child", operation, replyTo: "caller", requestedAt: 1, message: " hello ",
});

describe("agents provider message routing service boundaries", () => {
  // smarty-dev#266: during a mesh write stall, get() finds nobody; local delivery must still work.
  it("steers a local child during a mesh write stall and names the stall for unknown targets", async () => {
    const { router, agents, participants } = routing();
    const stalled = new Error("Fabric mesh is write-stalled: Timed out waiting for the Fabric mesh lock held by pid 7 (alive, state T stopped)");
    Object.assign(participants, { writeStalled: vi.fn(() => stalled) });
    participants.get.mockReturnValue(undefined);
    agents.status.mockImplementation((id) => {
      if (id === "child") return { id: "child", name: "Child" } as ReturnType<Ports[0]["status"]>;
      throw new Error("Unknown Fabric agent");
    });
    agents.steer.mockReturnValue({ messageId: "local-msg" } as ReturnType<Ports[0]["steer"]>);

    await expect(router.routeMessage("child", "hi", undefined, "steer")).resolves.toMatchObject({ routed: "local", messageId: "local-msg" });
    await expect(router.routeMessage("gone", "hi", undefined, "steer")).rejects.toThrow(stalled.message);
  });

  // smarty-dev#447: a sender whose lease just lapsed may still be live; its owner host gets
  // the reply. A long lapse or no record at all fails with the reason.
  it("replies to a peer root whose lease lapsed moments ago through its owner host", async () => {
    const { router, participants, control } = routing();
    const peer = { ...participant(), id: "session:peer", rootId: "session:peer", stale: true };
    participants.get.mockReturnValue(undefined);
    Object.assign(participants, { lastKnown: vi.fn((id: string) => id === peer.id ? { participant: peer, lapsedMs: 20_000 } : undefined) });
    control.request.mockResolvedValue({ queued: true, messageId: "delivered", routed: "mesh", acknowledged: true });
    await expect(router.routeMessage(peer.id, "reply", undefined, "followUp")).resolves.toMatchObject({ acknowledged: true, messageId: "delivered" });
    expect(control.request).toHaveBeenCalledWith("host", peer.id, "followUp", { message: "reply", data: undefined }, "owner");
  });

  // review/astra on #44: a worker replies to its own remote Main through the same lookup.
  it.each(["main", "session:main-root"])("replies to a remote Main whose lease lapsed moments ago (%s)", async (target) => {
    const { router, participants, control, main } = routing();
    main.local = false;
    main.id = "session:main-root";
    main.matches = ((id: string) => id === "main" || id === "session:main-root") as typeof main.matches;
    const root = { ...participant(), id: "session:main-root", rootId: "session:main-root", stale: true };
    participants.get.mockReturnValue(undefined);
    Object.assign(participants, { lastKnown: vi.fn((id: string) => id === root.id ? { participant: root, lapsedMs: 15_000 } : undefined) });
    control.request.mockResolvedValue({ queued: true, messageId: "to-main", routed: "mesh", acknowledged: true });
    await expect(router.routeMessage(target, "result", undefined, "followUp")).resolves.toMatchObject({ messageId: "to-main" });
    expect(control.request).toHaveBeenCalledWith("host", root.id, "followUp", { message: "result", data: undefined }, "owner");
  });

  it("names why a remote Main cannot be resolved", async () => {
    const { router, participants, control, main } = routing();
    main.local = false;
    main.id = "session:main-root";
    const root = { ...participant(), id: "session:main-root", rootId: "session:main-root", stale: true };
    participants.get.mockReturnValue(undefined);
    const lastKnown = vi.fn<(id: string) => { participant: FabricParticipantInfo; lapsedMs: number } | undefined>();
    Object.assign(participants, { lastKnown });
    lastKnown.mockReturnValue({ participant: root, lapsedMs: 600_000 });
    await expect(router.routeMessage("main", "result", undefined, "followUp"))
      .rejects.toThrow("Unknown Fabric Main participant: session:main-root (its lease lapsed 600 s ago, so the session has probably ended)");
    lastKnown.mockReturnValue({ participant: root, lapsedMs: Number.POSITIVE_INFINITY });
    await expect(router.routeMessage("main", "result", undefined, "followUp"))
      .rejects.toThrow("Unknown Fabric Main participant: session:main-root (its host is gone or was replaced");
    lastKnown.mockReturnValue(undefined);
    await expect(router.routeMessage("main", "result", undefined, "followUp"))
      .rejects.toThrow("Unknown Fabric Main participant: session:main-root (no record on this mesh root");
    expect(control.request).not.toHaveBeenCalled();
  });

  it("reports a write-stalled mesh before trying a recently lapsed root", async () => {
    const { router, participants, control, main } = routing();
    const stalled = new Error("Fabric mesh is write-stalled: Timed out waiting for the Fabric mesh lock");
    const peer = { ...participant(), id: "session:peer", rootId: "session:peer", stale: true };
    participants.get.mockReturnValue(undefined);
    Object.assign(participants, {
      writeStalled: vi.fn(() => stalled),
      lastKnown: vi.fn(() => ({ participant: peer, lapsedMs: 10_000 })),
    });
    await expect(router.routeMessage(peer.id, "reply", undefined, "followUp")).rejects.toThrow(stalled.message);
    main.local = false;
    await expect(router.routeMessage("main", "reply", undefined, "followUp")).rejects.toThrow(stalled.message);
    expect(control.request).not.toHaveBeenCalled();
  });

  it("names why a target cannot be resolved", async () => {
    const { router, participants, control } = routing();
    const peer = { ...participant(), id: "session:gone", rootId: "session:gone", stale: true };
    participants.get.mockReturnValue(undefined);
    const lastKnown = vi.fn<(id: string) => { participant: FabricParticipantInfo; lapsedMs: number } | undefined>();
    Object.assign(participants, { lastKnown });
    lastKnown.mockReturnValue({ participant: peer, lapsedMs: 600_000 });
    await expect(router.routeMessage(peer.id, "reply", undefined, "followUp"))
      .rejects.toThrow("Unknown Fabric participant: session:gone (its lease lapsed 600 s ago, so the session has probably ended)");
    lastKnown.mockReturnValue({ participant: peer, lapsedMs: Number.POSITIVE_INFINITY });
    await expect(router.routeMessage(peer.id, "reply", undefined, "followUp")).rejects.toThrow("its host is gone or was replaced");
    lastKnown.mockReturnValue(undefined);
    await expect(router.routeMessage("session:never", "reply", undefined, "followUp"))
      .rejects.toThrow("Unknown Fabric participant: session:never (no record on this mesh root");
    expect(control.request).not.toHaveBeenCalled();
  });

  it("preserves passive Main delivery and caller identity without actor validation", async () => {
    const { router, main, actors } = routing();
    const from = { id: "source", name: "Source", kind: "main" as const };
    await router.routeMessage("main", "event", undefined, "followUp", undefined, { from, triggerTurn: false });
    expect(main.deliverAgent).toHaveBeenCalledWith({ from, message: "event", delivery: "followUp", triggerTurn: false });
    expect(actors.validateDirectMessage).not.toHaveBeenCalled();
  });

  it("rechecks remote capability withdrawal on every delivery", async () => {
    const { router, main, participants, control, actors } = routing();
    main.local = false;
    const remote = participant();
    participants.get.mockReturnValue(remote);
    await router.routeMessage("main", "first", null, "followUp");
    expect(control.request).toHaveBeenCalledWith("host", "main", "followUp", { message: "first", data: null }, "owner");
    remote.capabilities = [];
    await expect(router.routeMessage("main", "second", null, "followUp")).rejects.toThrow("does not support followUp");
    expect(control.request).toHaveBeenCalledTimes(1);
    expect(actors.steerRemote).not.toHaveBeenCalled();
  });

  it("routes listed busy peer roots through their current owner, without treating them as actors", async () => {
    const { router, participants, control, actors, main } = routing();
    const peer = { ...participant(), id: "session:peer", rootId: "session:peer" };
    participants.get.mockImplementation(id => id === peer.id ? peer : undefined);
    control.request.mockResolvedValue({ queued: true, messageId: "accepted", routed: "mesh", acknowledged: true });
    await expect(router.routeMessage(peer.id, "new authorized observation", { original: "fresh" }, "followUp",
      undefined, { triggerTurn: false })).resolves.toMatchObject({ messageId: "accepted" });
    expect(control.request).toHaveBeenCalledWith("host", peer.id, "followUp",
      { message: "new authorized observation", data: { original: "fresh" }, triggerTurn: false }, "owner");
    expect(actors.status).not.toHaveBeenCalled();
    expect(main.deliverAgent).not.toHaveBeenCalled();
    peer.ownerHostId = "replacement-host";
    peer.ownerIdentityId = "replacement-owner";
    await router.routeMessage(peer.id, "later observation", undefined, "followUp");
    expect(control.request).toHaveBeenLastCalledWith("replacement-host", peer.id, "followUp",
      { message: "later observation", data: undefined }, "replacement-owner");
    peer.capabilities = [];
    await expect(router.routeMessage(peer.id, "withdrawn", undefined, "followUp")).rejects.toThrow("does not support followUp");
    expect(control.request).toHaveBeenCalledTimes(2);
  });

  it("uses the existing legacy relay for a listed peer root without a control protocol", async () => {
    const { router, participants, actors, control } = routing();
    participants.get.mockReturnValue({ ...participant(), id: "session:legacy", controlProtocol: "legacy" });
    await router.routeMessage("session:legacy", "observation", undefined, "followUp");
    expect(actors.steerRemote).toHaveBeenCalledWith("session:legacy", "observation", "followUp", undefined);
    expect(control.request).not.toHaveBeenCalled();
  });

  it("does not hide local agent failures by falling through to actors", async () => {
    const { router, agents, actors } = routing();
    const failure = new Error("worker unavailable");
    agents.status.mockImplementation(() => { throw failure; });
    await expect(router.routeMessage("child", "hello", undefined, "steer")).rejects.toBe(failure);
    expect(actors.validateDirectMessage).not.toHaveBeenCalled();
  });

  it("translates only unknown actor targets into unknown participant errors", async () => {
    const { router, actors } = routing();
    await expect(router.routeMessage("missing", "hello", undefined, "steer")).rejects.toThrow("Unknown Fabric participant: missing");
    const failure = new Error("registry unavailable");
    actors.status.mockImplementation(() => { throw failure; });
    await expect(router.routeMessage("missing", "hello", undefined, "steer")).rejects.toBe(failure);
  });

  it("leaves cancel commands to the control plane and refreshes successful stops", async () => {
    const { router, agents, participants, actors } = routing();
    await expect(router.acceptControl(command("cancel"), actors.identity)).resolves.toEqual({
      accepted: false, error: "Cancel commands are handled by the control plane",
    });
    expect(agents.stop).not.toHaveBeenCalled();
    await expect(router.acceptControl(command("stop"), actors.identity)).resolves.toEqual({ accepted: true, messageId: "cmd" });
    expect(agents.stop).toHaveBeenCalledWith("child");
    expect(participants.scheduleRefresh).toHaveBeenCalledOnce();
  });
});
