import { describe, expect, it, vi } from "vitest";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";
import type { FabricControlCommand } from "../src/topology/control-plane.js";
import type { MeshIdentity } from "../src/mesh/store.js";

// Pure routing/receiver probes: no real sessions, processes, mesh writes or deletions.
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const sender: MeshIdentity = { id: "session:sender", kind: "main", name: "Sender" };
const root = (id: string, overrides: Partial<FabricParticipantInfo> = {}): FabricParticipantInfo => ({
  format: 1, id, rootId: id, kind: "root", name: "lead-example", status: "running",
  ownerHostId: id, ownerIdentityId: id, ownerIncarnation: `epoch:${id}`, controlProtocol: "v1",
  capabilities: ["steer", "followUp"], local: false, stale: false, startedAt: 1, updatedAt: 1,
  runner: "pi", transport: "host", ...overrides,
});
const predecessor = root("session:predecessor", { stale: true });
const successor = root("session:successor");

const fixture = (entries: FabricParticipantInfo[], retained = false) => {
  const manager = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0];
  const actors = {
    identity: sender, status: vi.fn((id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); }),
    validateDirectMessage: vi.fn(), tell: vi.fn(), steerRemote: vi.fn(),
  } as unknown as Ports[1];
  const main = {
    id: sender.id, local: true, matches: (id: string) => id === "main" || id === sender.id,
    deliverAgent: vi.fn(() => ({ queued: true, messageId: "received", routed: "main" })),
  } as unknown as Ports[2];
  const participants = {
    get: vi.fn((id: string) => entries.find(p => p.id === id && !p.stale)),
    list: vi.fn(() => entries), scheduleRefresh: vi.fn(), lineageAlive: () => true,
    retainedRouteAllowed: () => retained,
    lastKnown: (id: string) => {
      const participant = entries.find(p => p.id === id && p.stale);
      return participant ? { participant, lapsedMs: 60_000 } : undefined;
    },
  } satisfies Ports[3];
  const request = vi.fn().mockResolvedValue({ queued: true, acknowledged: true, routed: "mesh", messageId: "received" });
  return { router: new AgentMessageRouter(manager, actors, main, participants, { request }, b => b),
    manager, actors, main, participants, request };
};

// A fresh published name selects the live root. Stale names remain available only when
// no live root has that name; exact IDs never retarget to a same-named successor.
describe("Main name succession (#10720)", () => {
  it.each(["followUp", "steer"] as const)("delivers by-name %s to the live successor with sender attribution", async kind => {
    const f = fixture([predecessor, successor]);
    const receiver = fixture([successor]);
    Object.assign(receiver.main, { id: successor.id, matches: (id: string) => id === successor.id });
    f.request.mockImplementation(async (hostId, targetId, operation, payload, ownerId) => {
      expect([hostId, targetId, ownerId]).toEqual([successor.id, successor.id, successor.id]);
      const command = { ...payload, commandId: "name-delivery", targetId, operation } as FabricControlCommand;
      const acceptance = await receiver.router.acceptControl(command, sender, undefined, "mesh");
      expect(acceptance).toMatchObject({ accepted: true, messageId: "received" });
      return { queued: true, acknowledged: true, routed: "mesh", messageId: acceptance.messageId };
    });
    const data = { lane: "synthetic-lane", wait: "review" };
    await expect(f.router.routeMessage("lead-example", "continue owned work", data, kind))
      .resolves.toMatchObject({ acknowledged: true, messageId: "received" });
    expect(f.request).toHaveBeenCalledOnce();
    expect(f.request.mock.calls[0]![3]).toMatchObject({ ownerIncarnation: successor.ownerIncarnation, message: "continue owned work", data });
    expect(receiver.main.deliverAgent).toHaveBeenCalledWith(expect.objectContaining({
      from: sender, verification: "mesh", message: "continue owned work", data, delivery: kind,
      outcomeSend: expect.objectContaining({ to: successor.id, from: sender.id }),
    }));
    expect(f.main.deliverAgent).not.toHaveBeenCalled();
    expect(f.actors.tell).not.toHaveBeenCalled();
  });

  it.each([
    { stale: true, status: "stopping" },
    { stale: true, status: "reloading" },
    { stale: true, remoteHost: "disconnected-host" },
  ])("does not let retained $status/$remoteHost names shadow the live publication", async old => {
    const f = fixture([root(predecessor.id, old), successor]);
    await f.router.routeMessage("lead-example", "to successor", undefined, "followUp");
    expect(f.request.mock.calls[0]![1]).toBe(successor.id);
  });

  it("prefers the live name even when the predecessor is still within native lease grace", async () => {
    const f = fixture([predecessor, successor], true);
    await f.router.routeMessage("lead-example", "live name", undefined, "followUp");
    expect(f.request.mock.calls[0]![1]).toBe(successor.id);
    // An exact predecessor address is still its own route, never an alias by name.
    await f.router.routeMessage(predecessor.id, "exact old address", undefined, "followUp");
    expect(f.request.mock.calls[1]![1]).toBe(predecessor.id);
  });

  it("keeps one native stale name routable during grace when there is no live replacement", async () => {
    const f = fixture([predecessor], true);
    await f.router.routeMessage("lead-example", "busy Main", undefined, "followUp");
    expect(f.request.mock.calls[0]![1]).toBe(predecessor.id);
  });

  it("never substitutes the successor for an ineligible exact predecessor ID", async () => {
    const f = fixture([predecessor, successor]);
    await expect(f.router.routeMessage(predecessor.id, "do not redirect", undefined, "followUp"))
      .rejects.toThrow(`Unknown Fabric participant: ${predecessor.id}`);
    expect(f.request).not.toHaveBeenCalled();
  });

  it.each([false, true])("refuses multiple live roots regardless of stale name order (reverse=%s)", async reverse => {
    const duplicate = root("session:duplicate");
    const entries = [predecessor, successor, duplicate];
    const f = fixture(reverse ? entries.reverse() : entries);
    await expect(f.router.routeMessage("lead-example", "never guess", undefined, "followUp"))
      .rejects.toThrow(`Ambiguous Fabric participant: lead-example (${[successor.id, duplicate.id].sort().join(", ")}); use an exact id`);
    expect(f.request).not.toHaveBeenCalled();
  });

  it("preserves retained-only duplicate-name ambiguity", async () => {
    const duplicate = root("session:duplicate", { stale: true });
    const f = fixture([predecessor, duplicate], true);
    await expect(f.router.routeMessage("lead-example", "never guess", undefined, "followUp"))
      .rejects.toThrow("Ambiguous Fabric participant: lead-example");
    expect(f.request).not.toHaveBeenCalled();
  });

  it("still refuses a live root name that also selects an actor", async () => {
    const f = fixture([predecessor, successor]);
    vi.mocked(f.actors.status).mockReturnValue({ id: "actor:named", name: "lead-example" } as ReturnType<Ports[1]["status"]>);
    await expect(f.router.routeMessage("lead-example", "never guess", undefined, "followUp"))
      .rejects.toThrow(`Ambiguous Fabric participant: lead-example (actor actor:named, root ${successor.id})`);
    expect(f.request).not.toHaveBeenCalled();
  });

  it.each(["non-interactive", "missing-capability"] as const)("keeps %s checks on the live successor without falling back", async guard => {
    const live = root(successor.id, guard === "non-interactive" ? { interactive: false } : { capabilities: [] });
    const f = fixture([predecessor, live], true);
    await expect(f.router.routeMessage("lead-example", "must not arrive", undefined, "followUp"))
      .rejects.toThrow(guard === "non-interactive" ? "is non-interactive" : "does not support followUp");
    expect(f.request).not.toHaveBeenCalled();
  });

  it("revalidates the live successor owner before publication", async () => {
    const f = fixture([predecessor, successor]);
    f.participants.get.mockImplementation((id: string, _now?: number, options?: { fresh?: boolean }) =>
      id === successor.id ? options?.fresh ? { ...successor, ownerIdentityId: "session:replacement-owner" } : successor : undefined);
    await expect(f.router.routeMessage("lead-example", "no changed owner", undefined, "followUp"))
      .rejects.toMatchObject({ code: "FABRIC_ROUTE_AUTHORITY_CHANGED" });
    expect(f.request).not.toHaveBeenCalled();
  });
});
