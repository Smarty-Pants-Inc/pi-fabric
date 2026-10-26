// Keep: data safety and PR #1632 failure paths; real adapter/control retry logic, fake verified carrier, no broker.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { TransportUncertainError, TransportUnavailableError, type FabricTransport, type TransportEnvelope } from "../src/transport/types.js";

const carrier = vi.hoisted(() => ({ publish: vi.fn(), verified: vi.fn(() => true) }));
vi.mock("../src/transport/nats/connection.js", () => ({
  openNatsConnection: async () => ({
    js: { publish: carrier.publish }, manager: {},
    nc: { isClosed: () => false, isDraining: () => false },
    isVerified: carrier.verified, onStatus: () => () => {},
    closed: () => new Promise(() => {}), close: async () => {},
  }),
}));
vi.mock("../src/transport/nats/kv.js", async original => ({
  ...await original<typeof import("../src/transport/nats/kv.js")>(),
  openNatsKvForMachine: async () => ({ ttlMs: 15_000 }),
}));
vi.mock("../src/transport/nats/provision.js", async original => {
  const actual = await original<typeof import("../src/transport/nats/provision.js")>();
  return { ...actual,
    ensureTransportStream: async (_manager: unknown, policy: Parameters<typeof actual.transportStreamConfig>[0]) =>
      ({ config: actual.transportStreamConfig(policy) }),
    ensureTransportConsumer: async (_manager: unknown, policy: { description: string }) => ({ config: { description: policy.description } }),
  };
});
const transports: FabricTransport[] = [];
const roots: string[] = [];
afterEach(async () => {
  for (const transport of transports.splice(0)) await transport.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.useRealTimers(); vi.restoreAllMocks(); carrier.publish.mockReset(); carrier.verified.mockReset(); carrier.verified.mockReturnValue(true);
});
async function open() {
  const transport = await openNatsTransport({ servers: "nats://fixture.invalid:1", credentials: "fixture",
    machine: "cancel-test", hostId: "caller", requestTimeoutMs: 10,
    custodyStream: "CUSTODY", livenessStream: "LIVENESS", pathBMachines: ["cancel-test"] });
  transports.push(transport); return transport;
}
const envelope: TransportEnvelope = { id: "same-envelope", kind: "command", to: { machine: "cancel-test", hostId: "owner" },
  from: { id: "caller", name: "caller", kind: "main" }, targetId: "target", operation: "followUp",
  body: { commandId: "command", message: "preserve uncertain body" } };
const nextTurn = () => new Promise<void>(resolve => setImmediate(resolve));

describe("NATS cancellation publish failure custody", () => {
  it("does not retry a closed transport cancellation", async () => {
    const transport = await open();
    carrier.publish.mockResolvedValue({ seq: 1, duplicate: false });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "closed-cancel-")); roots.push(root);
    const mesh = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
    const plane = new FabricControlPlane(mesh, envelope.from, { enabled: true, hostId: "caller", transport,
      ownerIncarnation: "caller:epoch", relayOperatorMachine: "fabric-ops", participants: { leaseLive: () => true, whenChanged: async () => {},
        resolve: () => ({ machine: "cancel-test", ownerHostId: "owner", ownerIdentityId: "owner", ownerIncarnation: "owner:epoch",
          capabilities: ["followUp"], live: true }) } });
    const send = vi.spyOn(transport, "send"), abort = new AbortController();
    const request = plane.request("owner", "target", "followUp", { message: "original" }, "owner", { signal: abort.signal }).catch(error => error);
    await nextTurn(); expect(carrier.publish).toHaveBeenCalledTimes(1);
    await transport.close();
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    abort.abort(); await request; await nextTurn();
    let closeError: unknown;
    const closing = plane.close().catch(error => { closeError = error; });
    try {
      await nextTurn();
      expect(closeError).toMatchObject({ message: "Background mesh release has unconfirmed publication obligations" });
      await vi.advanceTimersByTimeAsync(30_000); await nextTurn();
      expect(send).toHaveBeenCalledTimes(2); // Original and one final, known-no-write closed cancellation.
    } finally {
      await vi.advanceTimersByTimeAsync(300_000); await closing;
      mesh.closeState(); vi.useRealTimers();
    }
  });

  it.each(["publish", "session"] as const)("preserves earlier uncertainty when a later unavailable %s retry is aborted", async stage => {
    const transport = await open(), abort = new AbortController();
    let attempts = 0, checks = 0;
    carrier.publish.mockImplementation(async () => {
      if (++attempts === 1) throw new Error("PubAck lost");
      if (stage === "publish") abort.abort(new Error("caller abort"));
      else carrier.verified.mockImplementation(() => {
        if (++checks === 1) return true; // Waiting after the second wire failure.
        if (checks >= 3) abort.abort(new Error("caller abort during session wait"));
        return false; // Assert-open then wait-for-session.
      });
      throw new TransportUnavailableError("known no-write on later attempt");
    });
    const error = await transport.send(envelope, abort.signal).catch(error => error);
    expect(error).toBeInstanceOf(TransportUncertainError);
    expect(error).toMatchObject({ id: envelope.id, resolvedBody: envelope.body });
    expect(carrier.publish).toHaveBeenCalledTimes(2);
    const calls = carrier.publish.mock.calls;
    expect(calls[1]![1]).toBe(calls[0]![1]); // Exact wire bytes, not a fresh body stamp.
  });
});
