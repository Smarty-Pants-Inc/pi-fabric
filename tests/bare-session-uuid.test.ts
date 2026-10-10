import { describe, expect, it, vi } from "vitest";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

// smarty-dev#1729: agents.followUp({ id: '<bare session uuid>' }) addresses that session's Main.
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const UUID = "019a3c2e-7b41-7c3d-9e2f-0a1b2c3d4e5f";
const ACTOR = "0f1e2d3c4b5a69788796a5b4c3d2e1f0";

const setup = () => {
  const peer = {
    id: `session:${UUID}`, kind: "root", local: false, ownerHostId: `session:${UUID}`, ownerIdentityId: `session:${UUID}`,
    ownerIncarnation: "fixture:live-main", capabilities: ["steer", "followUp"], controlProtocol: "v1",
  } as unknown as FabricParticipantInfo;
  const actor = { id: ACTOR, kind: "actor", local: true, capabilities: ["steer", "followUp"] } as unknown as FabricParticipantInfo;
  const request = vi.fn(async () => ({ acknowledged: true, messageId: "m" }));
  const tell = vi.fn(() => ({ messageId: "t" }));
  const router = new AgentMessageRouter(
    { status: () => { throw new Error("Unknown Fabric agent"); }, steer: vi.fn(), followUp: vi.fn(), stop: vi.fn() } as unknown as Ports[0],
    {
      identity: { id: "session:self", name: "main", kind: "main" },
      status: (id: string) => {
        if (id === ACTOR) return { id: ACTOR, name: "worker", runner: "pi" };
        throw new Error(`Unknown Fabric actor: ${id}`);
      },
      validateDirectMessage: vi.fn(), tell, ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn(), resolveBinding: vi.fn(),
    } as unknown as Ports[1],
    { id: "session:self", local: true, matches: (id: string) => id === "session:self", deliverAgent: vi.fn() } as unknown as Ports[2],
    {
      get: (id: string) => ({ [peer.id]: peer, [ACTOR]: actor })[id],
      scheduleRefresh: vi.fn(), writeStalled: () => undefined, lastKnown: () => undefined,
    } as unknown as Ports[3],
    { request } as unknown as Ports[4],
    (binding) => binding,
  );
  return { router, request, tell };
};

describe("a bare session UUID (smarty-dev#1729)", () => {
  it("delivers steer and followUp to the live Main session:<uuid>", async () => {
    const { router, request } = setup();
    await router.routeMessage(UUID, "hi", undefined, "followUp");
    await router.routeMessage(UUID, "now", undefined, "steer");
    expect((request.mock.calls as unknown[][]).every(call =>
      (call[3] as { ownerIncarnation?: string }).ownerIncarnation === "fixture:live-main")).toBe(true);
    expect(request.mock.calls.map((call) => [(call as unknown[])[1], (call as unknown[])[2]])).toEqual([
      [`session:${UUID}`, "followUp"],
      [`session:${UUID}`, "steer"],
    ]);
  });

  it("names the fix when no session has that UUID", async () => {
    const { router } = setup();
    const other = "11111111-2222-4333-8444-555555555555";
    await expect(router.routeMessage(other, "hi", undefined, "followUp"))
      .rejects.toThrow(`Unknown Fabric participant: ${other} (no record on this mesh root: the session has ended, has not joined yet, or uses another mesh root; use 'session:${other}' for a Main session)`);
  });

  it("leaves actor ids unchanged", async () => {
    const { router, request, tell } = setup();
    await expect(router.routeMessage(ACTOR, "hi", undefined, "followUp")).resolves.toMatchObject({ routed: "local" });
    // The actor ID is unchanged; the registered local producer owns sender metadata.
    // An invocation without a stamped requester must not manufacture a principal.
    expect(tell).toHaveBeenCalledWith(ACTOR, "hi", undefined, {
      provenance: {
        v: 1, channel: "fabric", via: "followUp",
        sender: { id: "session:self", name: "main", kind: "main", verified: "mesh" },
      },
    });
    expect(request).not.toHaveBeenCalled();
    await expect(router.routeMessage("0".repeat(32), "hi", undefined, "followUp")).rejects.toThrow(/mesh root\)$/);
  });
});
