import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(closers.splice(0).map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const rootRecord = (id: string, sessionId: string): FabricParticipantRecord => ({
  format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: "main",
  status: "running", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
  cwd: "/tmp/project", sessionId, startedAt: 1, updatedAt: 2, pendingMessages: false, controlProtocol: "v1",
});

// One Pi session's Fabric host: its directory lease and its control plane on a shared mesh.
const host = (meshRoot: string, name: string, timing: { heartbeatMs: number; leaseMs: number }) => {
  // Not "main": a main also writes a legacy session entry with a fixed 15 s lease.
  const identity: MeshIdentity = { id: `session:${name}`, name: "main", kind: "actor", sessionId: name };
  const directory = new ParticipantDirectory(new MeshStore(meshRoot, 64 * 1024, 1_000), {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, ...timing,
  });
  directory.registerSource(() => [rootRecord(identity.id, name)]);
  const control = new FabricControlPlane(new MeshStore(meshRoot, 64 * 1024, 1_000), identity, {
    enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 2_000,
  });
  closers.push(async () => { await control.close(); await directory.close(); });
  return { identity, directory, control };
};

describe("a reply to a sender whose lease lapsed (smarty-dev#447)", () => {
  it("reaches a live sender during a lease stall, through its owner host", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-reply-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const replier = host(meshRoot, "replier", { heartbeatMs: 100, leaseMs: 2_000 });
    const sender = host(meshRoot, "sender", { heartbeatMs: 100, leaseMs: 400 });
    await Promise.all([replier.directory.start(), sender.directory.start()]);
    const received: string[] = [];
    sender.control.start(async (command) => {
      received.push(command.message ?? "");
      return { accepted: true, messageId: "delivered" };
    });
    replier.control.start(() => ({ accepted: false }));
    await vi.waitFor(() => expect(replier.directory.get(sender.identity.id)).toBeDefined(), { timeout: 5_000, interval: 20 });

    // The sender stays live (its control plane runs), but its heartbeat stalls: the lease lapses.
    vi.spyOn(sender.directory, "refresh").mockResolvedValue(undefined);
    await vi.waitFor(() => expect(replier.directory.get(sender.identity.id)).toBeUndefined(), { timeout: 5_000, interval: 20 });

    const unknown = () => { throw new Error("Unknown Fabric agent"); };
    const router = new AgentMessageRouter(
      { status: unknown, steer: vi.fn(), followUp: vi.fn(), stop: vi.fn() } as unknown as Ports[0],
      {
        identity: replier.identity,
        status: () => { throw new Error("Unknown Fabric actor"); },
        validateDirectMessage: vi.fn(), tell: vi.fn(), ask: vi.fn(), stop: vi.fn(), steerRemote: vi.fn(), resolveBinding: vi.fn(),
      } as unknown as Ports[1],
      { id: replier.identity.id, local: true, matches: (id: string) => id === replier.identity.id, deliverAgent: vi.fn() } as unknown as Ports[2],
      replier.directory,
      replier.control,
      (binding) => binding,
    );
    await expect(router.routeMessage(sender.identity.id, "reply to your message", undefined, "followUp"))
      .resolves.toMatchObject({ acknowledged: true, messageId: "delivered" });
    expect(received).toEqual(["reply to your message"]);
  }, 20_000);
});
