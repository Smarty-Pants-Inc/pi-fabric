import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import { kernelFenceAvailable } from "../src/residency/file-lock.js";

vi.mock("../src/residency/file-lock.js", async (original) => ({
  ...await original<typeof import("../src/residency/file-lock.js")>(), kernelFenceAvailable: vi.fn(() => true),
}));
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

afterEach(() => { vi.useRealTimers(); vi.mocked(kernelFenceAvailable).mockReturnValue(true); });

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
  it("F4 never ensures a missing durable participant without a kernel fence", async () => {
    const ports = routing();
    ports.actors.status.mockReturnValue({ id: "actor", residency: "durable", rootId: "main" } as ReturnType<Ports[1]["status"]>);
    const ensureActor = vi.fn();
    const residency = { hostId: "resident", ensureActor, options: { config: { rootId: "main", meshRoot: "/unused" } } };
    const router = new AgentMessageRouter(ports.agents, { ...ports.actors, owns: () => false },
      ports.main, ports.participants, ports.control, (binding) => binding, residency);
    vi.mocked(kernelFenceAvailable).mockReturnValue(false);
    const result = await router.routeMessage("actor", "no unsafe restart", undefined, "followUp").catch((error: unknown) => error);
    expect(ensureActor).not.toHaveBeenCalled();
    expect(result).toBeInstanceOf(Error);
    expect((result as Error).message).toContain("owned by another host");
    expect(ports.control.request).not.toHaveBeenCalled();
  });

  it.each([
    ["ensure", "dead"], ["route", "dead"], ["ensure", "ownerless"], ["route", "ownerless"],
    ["ensure", "ownerless-submillisecond"], ["route", "ownerless-submillisecond"],
  ] as const)("F7 waits through the mesh stale window on %s failure (%s holder)", async (stage, holder) => {
    const ports = routing();
    ports.actors.status.mockReturnValue({ id: "actor", residency: "durable", rootId: "main" } as ReturnType<Ports[1]["status"]>);
    const live: FabricParticipantInfo = { ...participant(), id: "actor", kind: "actor" as const, residency: "durable" as const,
      capabilities: ["followUp"], ownerHostId: "resident" };
    ports.participants.get.mockReturnValue(live);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-router-dead-lock-"));
    const ensureActor = vi.fn(async () => {});
    const residency = { hostId: "resident", ensureActor, options: { config: { rootId: "main", meshRoot: root } } };
    const router = new AgentMessageRouter(ports.agents, { ...ports.actors, owns: () => false },
      ports.main, ports.participants, ports.control, (binding) => binding, residency);
    const failure = Object.assign(new Error("dead mesh holder"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
    vi.useFakeTimers();
    const started = Date.now();
    const lockPath = path.join(root, ".lock");
    fs.mkdirSync(lockPath);
    if (holder === "dead") fs.writeFileSync(path.join(lockPath, "owner"), `fixture\n2147483647\n${started}\n`);
    else {
      const created = started + (holder === "ownerless-submillisecond" ? 0.5 : 0);
      fs.utimesSync(lockPath, started / 1000, created / 1000);
      if (holder === "ownerless-submillisecond") {
        expect(fs.statSync(lockPath).mtimeMs).toBeGreaterThan(started);
        expect(fs.statSync(lockPath).mtimeMs).toBeLessThan(started + 1);
      }
    }
    if (stage === "ensure") ensureActor.mockRejectedValueOnce(failure);
    else ports.control.request.mockRejectedValueOnce(failure);
    ports.control.request.mockResolvedValue({ queued: true, messageId: "recovered" } as Awaited<ReturnType<NonNullable<Ports[4]>["request"]>>);
    try {
      const result = router.routeMessage("actor", "immediate", undefined, "followUp")
        .then((value) => ({ value }), (error: unknown) => ({ error }));
      await vi.advanceTimersByTimeAsync(29_999);
      expect(stage === "ensure" ? ensureActor : ports.control.request).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(2);
      expect(await result).toMatchObject({ value: { queued: true } });
      expect(Date.now() - started).toBeLessThanOrEqual(40_000);
      // The router waits/retries only; MeshStore alone owns reclamation and fencing.
      expect(fs.existsSync(lockPath)).toBe(true);
      if (holder !== "dead") expect(fs.existsSync(path.join(lockPath, "owner"))).toBe(false);
    } finally { vi.useRealTimers(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["steer", "followUp"] as const)("awaits durable recovery before routing %s with a missing or stale participant", async (kind) => {
    const ports = routing();
    const actor = { id: "actor", name: "Durable", residency: "durable", rootId: "main" } as ReturnType<Ports[1]["status"]>;
    ports.actors.status.mockReturnValue(actor);
    const live: FabricParticipantInfo = { ...participant(), id: "actor", kind: "actor" as const, residency: "durable" as const,
      capabilities: ["steer", "followUp"], ownerHostId: "resident" };
    const ensureActor = vi.fn(async () => { ports.participants.get.mockReturnValue(live); });
    const residency = { hostId: "resident", ensureActor, options: { config: { rootId: "main", meshRoot: "/unused" } } } as NonNullable<Ports[6]>;
    const router = new AgentMessageRouter(ports.agents, { ...ports.actors, owns: () => false },
      ports.main, ports.participants, ports.control, (binding) => binding, residency);
    for (const initial of [undefined, { ...live, stale: true }]) {
      ports.participants.get.mockReturnValue(initial);
      await router.routeMessage(actor.id, "restart first", undefined, kind);
    }
    expect(ensureActor).toHaveBeenCalledTimes(2);
    expect(ensureActor).toHaveBeenCalledWith(actor.id);
    expect(ports.actors.tell).not.toHaveBeenCalled();
    expect(ports.control.request).toHaveBeenCalledTimes(2);
    ensureActor.mockRejectedValueOnce(new Error("startup timeout"));
    ports.participants.get.mockReturnValue(undefined);
    await expect(router.routeMessage(actor.id, "do not deliver after failed start", undefined, kind)).rejects.toThrow("startup timeout");
    expect(ports.control.request).toHaveBeenCalledTimes(2);
  });

  it.each(["live", "missing", "malformed", "future", "expired", "wrong-root", "ownerless-future", "ownerless-expired"])("F7 does not retry an unsafe or exhausted mesh holder (%s)", async (holder) => {
    const ports = routing();
    ports.actors.status.mockReturnValue({ id: "actor", residency: "durable", rootId: holder === "wrong-root" ? "other" : "main" } as ReturnType<Ports[1]["status"]>);
    ports.participants.get.mockReturnValue({ ...participant(), id: "actor", kind: "actor", residency: "durable", capabilities: ["followUp"] });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-router-unsafe-lock-"));
    const ensureActor = vi.fn(async () => {});
    const residency = { hostId: "resident", ensureActor, options: { config: { rootId: "main", meshRoot: root } } };
    const router = new AgentMessageRouter(ports.agents, { ...ports.actors, owns: () => false },
      ports.main, ports.participants, ports.control, (binding) => binding, residency);
    const failure = Object.assign(new Error("mesh holder timeout"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
    ports.control.request.mockRejectedValue(failure);
    vi.useFakeTimers();
    // "missing" means no canonical lock at all, not an existing ownerless directory.
    if (holder !== "missing") fs.mkdirSync(path.join(root, ".lock"));
    const created = Date.now() + (holder.endsWith("future") ? 1_000 : holder.endsWith("expired") ? -40_000 : 0);
    if (holder.startsWith("ownerless")) {
      fs.utimesSync(path.join(root, ".lock"), new Date(created), new Date(created));
    } else if (holder !== "missing") fs.writeFileSync(path.join(root, ".lock", "owner"),
      holder === "malformed" ? "unknown" : `fixture\n${holder === "live" ? process.pid : 2147483647}\n${created}\n`);
    try {
      await expect(router.routeMessage("actor", "no retry", undefined, "followUp")).rejects.toBe(failure);
      expect(ports.control.request).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(0);
    } finally { vi.useRealTimers(); fs.rmSync(root, { recursive: true, force: true }); }
  });

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

  it.each(["steer", "followUp"] as const)("names a peers-listed but not yet mirrored target as retryable (%s)", async (kind) => {
    const { router, participants, control } = routing();
    const id = "session:waiting";
    Object.assign(participants, { peers: () => [{ id, host: "forge" }] });
    await expect(router.routeMessage(id, "hello", undefined, kind)).rejects.toMatchObject({
      name: "FabricParticipantNotYetMirroredError", code: "FABRIC_PARTICIPANT_NOT_YET_MIRRORED", retryable: true,
      message: expect.stringContaining("not yet mirrored"),
    });
    expect(control.request).not.toHaveBeenCalled();
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
    expect(control.request).toHaveBeenCalledWith("host", peer.id, "followUp", { message: "reply", data: undefined, triggerTurn: true }, "owner", { routedRemoteHost: null });
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
    expect(control.request).toHaveBeenCalledWith("host", root.id, "followUp", { message: "result", data: undefined, triggerTurn: true }, "owner", { routedRemoteHost: null });
  });

  it.each(["absent", "rootId", "ownerHostId", "ownerIdentityId", "remoteHost"] as const)("refuses a cached native root when fresh authority changes (%s)", async (change) => {
    const { router, participants, control, actors } = routing();
    const native = { ...participant(), id: "session:peer", rootId: "session:peer" };
    const fresh = change === "absent" ? undefined : { ...native, [change]: "replacement" };
    participants.get.mockImplementation((_id, _now, options) => options?.fresh ? fresh : native);
    await expect(router.routeMessage(native.id, "private", { secret: true }, "followUp"))
      .rejects.toMatchObject({ name: "FabricRouteAuthorityError", code: "FABRIC_ROUTE_AUTHORITY_CHANGED" });
    expect(control.request).not.toHaveBeenCalled();
    expect(actors.steerRemote).not.toHaveBeenCalled();
  });

  it.each(["absent", "rootId", "ownerHostId", "ownerIdentityId", "remoteHost"] as const)("refuses a remote Main alias when fresh authority changes (%s)", async (change) => {
    const { router, participants, control, actors, main } = routing();
    main.local = false;
    main.id = "session:main-root";
    const native = { ...participant(), id: main.id, rootId: main.id };
    const fresh = change === "absent" ? undefined : { ...native, [change]: "replacement" };
    participants.get.mockImplementation((_id, _now, options) => options?.fresh ? fresh : native);
    await expect(router.routeMessage("main", "private", { secret: true }, "followUp"))
      .rejects.toMatchObject({ name: "FabricRouteAuthorityError", code: "FABRIC_ROUTE_AUTHORITY_CHANGED" });
    expect(control.request).not.toHaveBeenCalled();
    expect(actors.steerRemote).not.toHaveBeenCalled();
    expect(main.deliverAgent).not.toHaveBeenCalled();
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

  it.each([false, true])("refuses direct and incoming control delivery to a local non-interactive Main (fresh-only=%s)", async (freshOnly) => {
    const { router, participants, main } = routing();
    const root: FabricParticipantInfo = { ...participant(), id: main.id, interactive: false, capabilities: ["fabric"] };
    participants.get.mockImplementation((_id, _now, options) => !freshOnly || options?.fresh ? root : undefined);
    await expect(router.routeMessage(main.id, "audit must not answer", undefined, "followUp"))
      .rejects.toMatchObject({ name: "FabricParticipantNonInteractiveError" });
    await expect(router.acceptControl({ ...command("steer"), targetId: main.id }, { id: "sender", name: "Sender", kind: "main" }))
      .resolves.toMatchObject({ accepted: false, error: expect.stringContaining("non-interactive") });
    expect(main.deliverAgent).not.toHaveBeenCalled();
  });

  const failedInitialPresence = async (mode: ExtensionContext["mode"]) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-main-failed-presence-"));
    const ports = routing();
    const pi = { sendMessage: vi.fn(), sendUserMessage: vi.fn(), getThinkingLevel: () => "off" };
    const main = new MainAgentController(pi as unknown as ExtensionAPI, "session:audit", true, root, "audit",
      mode !== "print" && mode !== "json");
    const context = { mode, isIdle: () => true, hasPendingMessages: () => false } as ExtensionContext;
    main.attachFollowUpDrain(context, 0, path.join(root, "followups.json"));
    const deliverAgent = vi.spyOn(main, "deliverAgent");
    const identity = { id: main.id, name: "Main", kind: "main" as const };
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 1_000);
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: main.id, rootId: main.id, identity, heartbeatMs: 60_000, leaseMs: 120_000,
    });
    directory.registerSource(() => [directory.root(main.info(context), mode !== "print" && mode !== "json")]);
    const failure = Object.assign(new Error("Initial label write timed out"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
    const put = vi.spyOn(mesh, "put").mockRejectedValue(failure);
    const router = new AgentMessageRouter(ports.agents, { ...ports.actors, identity }, main, directory, ports.control, (binding) => binding);
    const close = async () => {
      main.closeFollowUpDrain();
      await directory.close();
      fs.rmSync(root, { recursive: true, force: true });
    };
    try {
      await expect(directory.start()).rejects.toBe(failure);
      expect(put).toHaveBeenCalledWith(expect.objectContaining({ key: "topology/peer-seq" }));
      expect(directory.get(main.id)).toBeUndefined();
      expect(directory.get(main.id, undefined, { fresh: true })).toBeUndefined();
      return { router, main, pi, deliverAgent, close };
    } catch (error) { await close(); throw error; }
  };

  it.each([
    ["print", "steer"], ["print", "followUp"], ["json", "steer"], ["json", "followUp"],
  ] as const)("rejects direct and incoming control to %s Main after failed initial presence (%s)", async (mode, kind) => {
    const state = await failedInitialPresence(mode);
    try {
      const direct = await state.router.routeMessage("main", "audit must not answer", undefined, kind)
        .catch((error: unknown) => error);
      const incoming = await state.router.acceptControl({ ...command(kind), targetId: state.main.id },
        { id: "sender", name: "Sender", kind: "main" }, undefined, "bridge");
      expect.soft(state.deliverAgent).not.toHaveBeenCalled();
      expect.soft(state.pi.sendMessage).not.toHaveBeenCalled();
      expect.soft(state.pi.sendUserMessage).not.toHaveBeenCalled();
      expect(direct).toMatchObject({ name: "FabricParticipantNonInteractiveError", code: "FABRIC_PARTICIPANT_NON_INTERACTIVE" });
      expect(incoming).toMatchObject({ accepted: false, error: expect.stringContaining("non-interactive") });
    } finally { await state.close(); }
  });

  it.each(["steer", "followUp"] as const)("delivers interactive Main direct and control messages despite failed initial presence (%s)", async (kind) => {
    const state = await failedInitialPresence("tui");
    try {
      await expect(state.router.routeMessage("main", "direct", undefined, kind)).resolves.toMatchObject({ queued: true, routed: "main" });
      await expect(state.router.acceptControl({ ...command(kind), targetId: state.main.id },
        { id: "sender", name: "Sender", kind: "main" }, undefined, "bridge")).resolves.toMatchObject({ accepted: true });
      expect(state.deliverAgent).toHaveBeenCalledTimes(2);
      expect(state.pi.sendMessage).toHaveBeenCalledTimes(2);
      expect(state.pi.sendUserMessage).not.toHaveBeenCalled();
    } finally { await state.close(); }
  });

  it("preserves passive Main delivery and caller identity without actor validation", async () => {
    const { router, main, actors } = routing();
    const from = { id: "source", name: "Source", kind: "main" as const };
    await router.routeMessage("main", "event", undefined, "followUp", undefined, { from, triggerTurn: false });
    expect(main.deliverAgent).toHaveBeenCalledWith({ from, verification: "mesh", message: "event", delivery: "followUp", triggerTurn: false });
    expect(actors.validateDirectMessage).not.toHaveBeenCalled();
  });

  it("rechecks remote capability withdrawal on every delivery", async () => {
    const { router, main, participants, control, actors } = routing();
    main.local = false;
    const remote = participant();
    participants.get.mockReturnValue(remote);
    await router.routeMessage("main", "first", null, "followUp");
    expect(control.request).toHaveBeenCalledWith("host", "main", "followUp", { message: "first", data: null, triggerTurn: true }, "owner", { routedRemoteHost: null });
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
      { message: "new authorized observation", data: { original: "fresh" }, triggerTurn: false }, "owner", { routedRemoteHost: null });
    expect(actors.status).not.toHaveBeenCalled();
    expect(main.deliverAgent).not.toHaveBeenCalled();
    peer.ownerHostId = "replacement-host";
    peer.ownerIdentityId = "replacement-owner";
    await router.routeMessage(peer.id, "later observation", undefined, "followUp");
    expect(control.request).toHaveBeenLastCalledWith("replacement-host", peer.id, "followUp",
      { message: "later observation", data: undefined, triggerTurn: true }, "replacement-owner", { routedRemoteHost: null });
    peer.capabilities = [];
    await expect(router.routeMessage(peer.id, "withdrawn", undefined, "followUp")).rejects.toThrow("does not support followUp");
    expect(control.request).toHaveBeenCalledTimes(2);
  });

  it("passes a participant's remote host separately from passive message data and rechecks capabilities", async () => {
    const { router, participants, control, actors, main } = routing();
    const peer = { ...participant(), id: "session:peer", rootId: "session:peer", remoteHost: "forge" };
    const data = { remoteHost: "business-host", routedRemoteHost: "business-route" };
    participants.get.mockImplementation(id => id === peer.id ? peer : undefined);
    control.request.mockResolvedValue({ queued: true, messageId: "delivered", routed: "mesh", acknowledged: true });
    await expect(router.routeMessage(peer.id, "observation", data, "followUp",
      undefined, { triggerTurn: false })).resolves.toMatchObject({ messageId: "delivered" });
    expect(control.request).toHaveBeenCalledWith("host", peer.id, "followUp",
      { message: "observation", data: { remoteHost: "business-host", routedRemoteHost: "business-route" }, triggerTurn: false },
      "owner", { routedRemoteHost: "forge" });
    peer.capabilities = [];
    await expect(router.routeMessage(peer.id, "withdrawn", data, "followUp",
      undefined, { triggerTurn: false })).rejects.toThrow("does not support followUp");
    expect(control.request).toHaveBeenCalledTimes(1);
    expect(actors.status).not.toHaveBeenCalled();
    expect(main.deliverAgent).not.toHaveBeenCalled();
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

  it.each(["ask", "followUp"] as const)("%s carries principal alongside raw own-root and resolved foreign bindings", async (operation) => {
    const { router, actors } = routing();
    actors.status.mockReturnValue({ id: "child", rootId: actors.identity.id } as ReturnType<Ports[1]["status"]>);
    actors.ask.mockResolvedValue({ id: "accepted" } as Awaited<ReturnType<Ports[1]["ask"]>>);
    actors.tell.mockReturnValue({ messageId: "accepted" } as ReturnType<Ports[1]["tell"]>);
    const principal = { id: "paul", binding: "voice-call" as const };
    const signal = new AbortController().signal;
    const own = { ...command(operation), principal, binding: { model: "provider/pinned" }, bindingProvenance: { kind: "owner-defaults" as const, rootId: actors.identity.id } };
    const check = (options: unknown) => {
      if (operation === "ask") expect(actors.ask).toHaveBeenLastCalledWith("child", "hello", undefined, signal, options);
      else expect(actors.tell).toHaveBeenLastCalledWith("child", "hello", undefined, options);
    };
    await expect(router.acceptControl(own, actors.identity, signal, "mesh")).resolves.toMatchObject({ accepted: true });
    check({ overrides: own.binding, provenance: expect.objectContaining({ principal }) });
    const foreign = { ...actors.identity, id: "foreign" };
    await expect(router.acceptControl(own, foreign, signal, "mesh")).resolves.toMatchObject({ accepted: false, error: "Invalid actor owner-default binding provenance" });
    for (const binding of [undefined, {}, { thinking: "high" as const }]) {
      await expect(router.acceptControl({ ...command(operation), principal, ...(binding ? { binding } : {}) }, foreign, signal, "bridge")).resolves.toMatchObject({ accepted: true });
      check({ binding: binding ?? {}, provenance: expect.objectContaining({ principal }) });
    }
    await expect(router.acceptControl({ ...command(operation), principal }, foreign, signal)).resolves.toMatchObject({ accepted: true });
    check({ binding: {}, provenance: undefined });
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

  // smarty-dev#1495: the owner of a Main reports its followUp queue with the acknowledgement.
  it("answers a remote followUp to Main with Main's queue depth", async () => {
    const { router, main, actors } = routing();
    main.deliverAgent.mockReturnValueOnce({ queued: true, messageId: "held", routed: "main", pendingFollowUps: 2, oldestAgeS: 75 });
    await expect(router.acceptControl({ ...command("followUp"), targetId: "main" }, actors.identity)).resolves.toEqual({
      accepted: true, messageId: "held", pendingFollowUps: 2, oldestAgeS: 75,
    });
    await expect(router.acceptControl({ ...command("steer"), targetId: "main" }, actors.identity)).resolves.toEqual({
      accepted: true, messageId: "main-msg",
    });
    main.deliverAgent.mockReturnValueOnce({
      queued: true, messageId: "newest", routed: "main", pendingFollowUps: 1, oldestAgeS: 5, coalesced: true, replacedMessageId: "held",
    });
    await expect(router.acceptControl({ ...command("followUp"), targetId: "main" }, actors.identity)).resolves.toEqual({
      accepted: true, messageId: "newest", pendingFollowUps: 1, oldestAgeS: 5, coalesced: true, replacedMessageId: "held",
    });
  });

  // smarty-dev#1826: the owner passes a stalled Main queue on; the sender throws, older owners never send it.
  it("passes a stalled Main queue to the sender, which throws", async () => {
    const { router, main, actors, participants, control } = routing();
    main.deliverAgent.mockReturnValueOnce({ queued: true, messageId: "held", routed: "main", pendingFollowUps: 8, oldestAgeS: 900, stalled: true });
    await expect(router.acceptControl({ ...command("followUp"), targetId: "main" }, actors.identity)).resolves.toEqual({
      accepted: true, messageId: "held", pendingFollowUps: 8, oldestAgeS: 900, stalled: true,
    });
    participants.get.mockReturnValue({ ...participant(), id: "session:peer", rootId: "session:peer" });
    control.request.mockResolvedValueOnce({ queued: true, messageId: "m", routed: "mesh", acknowledged: true, pendingFollowUps: 8, oldestAgeS: 900, stalled: true });
    await expect(router.routeMessage("session:peer", "hi", undefined, "followUp")).rejects.toThrow(
      "Fabric followUp to session:peer was accepted but is not being delivered: target idle and its held queue stalled (yours: 8 held, oldest 900 s).",
    );
    control.request.mockResolvedValueOnce({ queued: true, messageId: "m", routed: "mesh", acknowledged: true, pendingFollowUps: 8, oldestAgeS: 900 });
    await expect(router.routeMessage("session:peer", "hi", undefined, "followUp")).resolves.toMatchObject({ pendingFollowUps: 8 });
  });

  it("rejects a remote followUp to a full Main queue with the reason", async () => {
    const { router, main, actors } = routing();
    main.deliverAgent.mockImplementationOnce(() => { throw new Error("Main's followUp queue is full (50 of yours)"); });
    await expect(router.acceptControl({ ...command("followUp"), targetId: "main" }, actors.identity)).resolves.toEqual({
      accepted: false, error: "Main's followUp queue is full (50 of yours)",
    });
  });
});
