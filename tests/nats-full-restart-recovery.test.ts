// Keep: a full broker restart crosses JetStream leadership, participant lease expiry,
// the KV watch snapshots, and each routed control request/ACK on the real transport.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { PATH_B_OPERATOR_MACHINE } from "../src/mesh/nats-auth/permissions.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { ensureTransportStream } from "../src/transport/nats/provision.js";
import { transportInboxStream, transportInboxStreamSubject } from "../src/transport/nats/subjects.js";
import type { FabricTransport } from "../src/transport/types.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary() ?? "";
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const brokerSuite = describe.skipIf(!binary || process.platform !== "linux");
const MACHINE = "machine-a";
const PARTICIPANTS = 22;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

interface Owner {
  readonly id: string;
  readonly identity: MeshIdentity;
  readonly transport: FabricTransport;
  readonly mesh: MeshStore;
  readonly directory: ParticipantDirectory;
  control: FabricControlPlane | undefined;
}

let harness: PathBHarness | undefined;
const owners: Owner[] = [];
const received = new Map<string, number>();

const waitUntil = async (predicate: () => boolean, label: string, deadline: number): Promise<void> => {
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(25);
  }
  throw new Error(`timed out: ${label}`);
};

const allViewsReady = (): boolean => owners.length === PARTICIPANTS && owners.every((owner) => {
  if (!owner.directory.canConsumeMesh()) return false;
  const view = owner.directory.participantView;
  if (!view) return false;
  return owners.every((peer) => view.resolve(peer.id)?.live === true);
});

const sendRing = async (prefix: string, deadline: number): Promise<void> => {
  const before = new Map(received);
  const acknowledgements = await Promise.all(owners.map((sender, index) => {
    const target = owners[(index + 1) % owners.length]!;
    const timeoutMs = Math.max(1, Math.min(10_000, deadline - Date.now()));
    const control = sender.control;
    if (!control) throw new Error(`control plane is unavailable for ${sender.id}`);
    return control.request(target.id, target.id, "followUp", {
      message: `${prefix}:${index}`,
      ownerIncarnation: target.directory.ownerIncarnation,
    }, target.identity.id, { idempotencyKey: `${prefix}-${index}`, timeoutMs });
  }));
  expect(acknowledgements).toHaveLength(PARTICIPANTS);
  expect(acknowledgements.every((result) => result.queued && result.acknowledged === true && result.routed === "mesh")).toBe(true);
  expect(owners.every((owner) => (received.get(owner.id) ?? 0) > (before.get(owner.id) ?? 0))).toBe(true);
};

brokerSuite("real NATS full-stop recovery across participant views and routed ACKs", () => {
  beforeAll(async () => {
    harness = await createPathBHarness({ binary, tls: {} });
    expect(harness.serverVersion).toBe("2.14.7");
    const bootstrap = await harness.connect(harness.users.bootstrap);
    const manager = await jetstreamManager(bootstrap);
    const js = jetstream(bootstrap);
    await openNatsKvForMachine(js, "leases", MACHINE);
    await openNatsKvForMachine(js, "presence", MACHINE);
    for (const machine of ["machine-a", "machine-b"] as const) {
      await ensureTransportStream(manager, {
        name: transportInboxStream(machine),
        subjects: [transportInboxStreamSubject(machine)],
        maxBytes: 1_073_741_824,
        maxAgeMs: 0,
        maxMessageBytes: 1_048_576,
        duplicateWindowMs: 600_000,
        replicas: 1,
      });
    }

    for (let index = 0; index < PARTICIPANTS; index++) {
      const sessionId = randomUUID();
      const id = `session:${sessionId}`;
      const identity: MeshIdentity = { id, name: `full-restart-${index}`, kind: "main", sessionId };
      const ownerRoot = path.join(harness.root, `owner-${index}`);
      const meshRoot = path.join(ownerRoot, "mesh");
      const transportStateDir = path.join(ownerRoot, "transport-state");
      fs.mkdirSync(meshRoot, { recursive: true, mode: 0o700 });
      fs.mkdirSync(transportStateDir, { recursive: true, mode: 0o700 });
      const mesh = new MeshStore(meshRoot, 64 * 1024, 1_000);
      const transport = await openNatsTransport({
        ...harness.runtimeOptions(harness.users.machineA), hostId: id, name: `full-restart-${index}`,
      });
      const directory = new ParticipantDirectory(mesh, {
        enabled: true, transport, transportStateDir, hostId: id, rootId: id, identity,
      });
      directory.registerSource((): FabricParticipantRecord[] => {
        const now = Date.now();
        return [{
          format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
          ownerIncarnation: directory.ownerIncarnation, name: identity.name, status: "running", runner: "pi",
          transport: "host", capabilities: ["followUp", "steer", "fabric"], sessionId,
          startedAt: now, updatedAt: now, controlProtocol: "v1",
        }];
      });
      owners.push({ id, identity, transport, mesh, directory, control: undefined });
      received.set(id, 0);
    }

    await Promise.all(owners.map((owner) => owner.directory.start()));
    for (const owner of owners) {
      const view = owner.directory.participantView;
      if (!view) throw new Error("participant NATS view is unavailable");
      const control = new FabricControlPlane(owner.mesh, owner.identity, {
        enabled: true,
        hostId: owner.id,
        transport: owner.transport,
        participants: view,
        ownerIncarnation: owner.directory.ownerIncarnation,
        controlIncarnationFence: "enforce",
        expectedNatsInboxStream: transportInboxStream(MACHINE),
        relayOperatorMachine: PATH_B_OPERATOR_MACHINE,
        canConsumeMesh: () => owner.directory.canConsumeMesh(),
        pollMs: 20,
        acknowledgementTimeoutMs: 10_000,
      });
      owner.control = control;
      control.start((command, from) => {
        if (command.operation !== "followUp" || command.targetId !== owner.id) {
          return { accepted: false, error: "unexpected full-restart test command" };
        }
        if (!view.resolve(from.id)?.live) return { accepted: false, error: "sender route is not live" };
        received.set(owner.id, (received.get(owner.id) ?? 0) + 1);
        return { accepted: true, messageId: command.commandId };
      });
    }
    await waitUntil(allViewsReady, "initial 22-participant views", Date.now() + 30_000);
    await sendRing("initial", Date.now() + 30_000);
  }, 60_000);

  afterAll(async () => {
    await Promise.allSettled(owners.flatMap((owner) => owner.control ? [owner.control.close()] : []));
    await Promise.allSettled(owners.map((owner) => owner.directory.close()));
    await Promise.allSettled(owners.map((owner) => owner.transport.close()));
    for (const owner of owners) owner.mesh.closeState();
    owners.length = 0;
    await harness?.stop();
    harness = undefined;
  });

  it("rebuilds every participant view and sends/receives acknowledged routed messages after a 17s full stop", async () => {
    if (!harness) throw new Error("NATS fixture unavailable");
    const server = harness.serverProcess;
    if (!server) throw new Error("NATS server process is unavailable");
    if (server.exitCode === null && server.signalCode === null) {
      await new Promise<void>((resolve, reject) => {
        const onExit = () => { clearTimeout(timer); resolve(); };
        const timer = setTimeout(() => {
          server.off("exit", onExit);
          reject(new Error("timed out waiting for full NATS stop"));
        }, 5_000);
        server.once("exit", onExit);
        server.kill("SIGTERM");
      });
    }
    await sleep(17_000);
    await harness.start();
    const recoveryDeadline = Date.now() + 30_000;
    await waitUntil(allViewsReady, "all participant views and leases ready after restart", recoveryDeadline);
    await sendRing("after-restart", recoveryDeadline);
  }, 120_000);
});
