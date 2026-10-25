// expected-skip: no NATS_BIN/PATH executable: authenticated loopback broker is required.
// Keep reason: smarty-dev#15310 A' bridge, lane L1. The raw NATS port on a REAL Path B broker with the
// standard agent grant only: KV writes under its own machine, the machine-wide inbox durable, and
// sends as itself. A second machine plays a native v2 Main (raw KV rows plus the real KV directory).
import { createHash } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { openNatsBridgeSide, type NatsBridgeDelivery, type NatsBridgePort } from "../src/mesh/bridge-nats-port.js";
import { bridgeNatsLinkIdentity } from "../src/mesh/bridge-nats-types.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { openNatsTransport } from "../src/transport/nats/index.js";
import { KvParticipantDirectory } from "../src/topology/participant-view.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import type { FabricTransport, TransportDelivery, TransportEnvelope } from "../src/transport/types.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: the bridge port needs an executable NATS_BIN");

const BRIDGE = "machine-a";
const V2 = "machine-b";
const LINK = "v2";
const LINK_ID = bridgeNatsLinkIdentity(LINK).id;
const FILE_ROOT = "session:11111111-1111-4111-8111-111111111111";
const NATS_ROOT = "session:22222222-2222-4222-8222-222222222222";
const key = (machine: string, kind: "p" | "h", id: string) => `${machine}.${kind}.${createHash("sha256").update(id).digest("hex").slice(0, 32)}`;

let broker: PathBHarness;
let port: NatsBridgePort | undefined;
let v2: FabricTransport | undefined;
let receiver: KvParticipantDirectory | undefined;

beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {} });
  for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
  try {
    expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally { vi.unstubAllEnvs(); }
}, 30_000);

afterAll(async () => {
  await Promise.allSettled([port?.close(), receiver?.close(), v2?.close()]);
  await broker?.stop();
});

const natsRoot = (now: number): FabricParticipantRecord => ({ format: 1, id: NATS_ROOT, kind: "root", rootId: NATS_ROOT,
  ownerHostId: NATS_ROOT, ownerIdentityId: NATS_ROOT, ownerIncarnation: "nats-inc-1", name: "v2-main", status: "idle", runner: "pi",
  transport: "host", capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: now, updatedAt: now,
  sessionId: NATS_ROOT.slice(8) });
const fileRoot = (now: number): FabricParticipantRecord => ({ ...natsRoot(now), id: FILE_ROOT, rootId: FILE_ROOT, ownerHostId: FILE_ROOT,
  ownerIdentityId: FILE_ROOT, ownerIncarnation: "file-inc-1", name: "file-lead", sessionId: FILE_ROOT.slice(8) });

async function first<T>(iterable: AsyncIterable<T>, signal: AbortSignal): Promise<T> {
  for await (const item of iterable) return item;
  throw new Error(signal.aborted ? "aborted" : "iterator ended");
}

describe.skipIf(!binary)("A' bridge port on a real Path B broker", () => {
  it("mirrors, routes and receives as the bridge machine with the agent grant only", async () => {
    const runtime = broker.runtimeOptions(broker.users.machineA);
    port = await openNatsBridgeSide({
      link: LINK,
      nats: { servers: [String(runtime.servers)], machine: BRIDGE, credentials: runtime.credentials, caFile: broker.tls!.caFile,
        custodyStream: PATH_B_PERMISSION_POLICY.custodyStream, livenessStream: PATH_B_PERMISSION_POLICY.livenessStream,
        pathBMachines: [...PATH_B_PERMISSION_POLICY.pathBMachines] },
      allow: { file: [{ kind: "root", id: FILE_ROOT }], nats: [{ kind: "root", id: NATS_ROOT }] },
    }, { signal: AbortSignal.timeout(20_000) });

    // A native v2 Main on the other machine: its own lease and presence rows, under its own prefix.
    v2 = await openNatsTransport({ ...broker.runtimeOptions(broker.users.machineB), hostId: NATS_ROOT });
    const now = Date.now();
    const owner = `${NATS_ROOT}#nats-inc-1`;
    await v2.kv("leases").put(key(V2, "h", NATS_ROOT), owner, { id: NATS_ROOT, rootId: NATS_ROOT, identityId: NATS_ROOT,
      incarnation: "nats-inc-1", incarnationToken: "nats-inc-1", pid: process.pid, pidStartTime: "1",
      bootId: "12345678-1234-4234-8234-123456789abc", updatedAt: now, expiresAt: now + 15_000 }, 0);
    await v2.kv("presence").put(key(V2, "p", NATS_ROOT), owner, natsRoot(now), 0);

    // file -> NATS: the v2 directory resolves the mirrored file root and the link identity on the bridge machine.
    await port.mirror({ hosts: [{ record: { format: 1, id: FILE_ROOT, rootId: FILE_ROOT,
      identity: { id: FILE_ROOT, name: "file-lead", kind: "main" }, startedAt: now, updatedAt: now, expiresAt: now + 60_000 },
    expiresAt: now + 60_000, observedAt: now }], participants: [fileRoot(now)], linkIdentity: bridgeNatsLinkIdentity(LINK) }, now);
    receiver = new KvParticipantDirectory({ transport: v2, lease: { live: () => true, signal: new AbortController().signal },
      identityId: NATS_ROOT, ownerIncarnation: "nats-inc-1", rootId: NATS_ROOT });
    receiver.start();
    await vi.waitFor(() => {
      expect(receiver!.resolve(FILE_ROOT)).toMatchObject({ machine: BRIDGE, ownerHostId: FILE_ROOT, ownerIncarnation: "file-inc-1", live: true });
      expect(receiver!.resolve(LINK_ID)).toMatchObject({ machine: BRIDGE, live: true });
    }, { timeout: 10_000, interval: 50 });

    // NATS -> file: the port's snapshot carries the v2 root, sampled, and resolves it with its KV identity.
    await vi.waitFor(async () => expect((await port!.presence()).hosts.map(host => host.record.id)).toEqual([NATS_ROOT]),
      { timeout: 10_000, interval: 50 });
    expect(port.resolve(NATS_ROOT)).toMatchObject({ route: { machine: V2, live: true }, identity: { id: NATS_ROOT, name: "v2-main" } });

    // v2 -> bridge inbox: the machine-wide durable sees a command to the mirrored host, from machine-b.
    const inboxAbort = new AbortController();
    const inbound = first<NatsBridgeDelivery>(port.inbox(inboxAbort.signal), inboxAbort.signal);
    const body = { commandId: "c".repeat(64), targetId: FILE_ROOT, operation: "followUp", message: "hello", replyTo: NATS_ROOT,
      requestedAt: now, deadlineAt: now + 60_000, ownerIncarnation: "file-inc-1" };
    const command: TransportEnvelope = { id: "v2-command-1", kind: "command", to: { machine: BRIDGE, hostId: FILE_ROOT },
      targetId: FILE_ROOT, operation: "followUp", from: { id: NATS_ROOT, name: "v2-main", kind: "main" }, body };
    await v2.send(command);
    const delivery = await inbound;
    expect(delivery).toMatchObject({ sourceMachine: V2, toHostId: FILE_ROOT, attempt: 1,
      envelope: { id: "v2-command-1", kind: "command", from: { id: NATS_ROOT } } });
    expect(delivery.bodyDigest).toBe(controlBodyFingerprint(body, undefined));
    await delivery.ack();
    inboxAbort.abort();

    // bridge -> v2: the port sends as the mirrored file root; the v2 inbox sees machine-a as the source.
    const v2Abort = new AbortController();
    const v2Inbound = first<TransportDelivery<TransportEnvelope>>(v2.inbox(v2Abort.signal), v2Abort.signal);
    await port.send({ id: "bnf1-port-1", kind: "command", to: { machine: V2, hostId: NATS_ROOT }, targetId: NATS_ROOT,
      operation: "followUp", from: { id: FILE_ROOT, name: "file-lead", kind: "main" }, body: { ...body, targetId: NATS_ROOT } });
    const received = await v2Inbound;
    expect(received.sourceMachine).toBe(BRIDGE);
    expect(received.payload).toMatchObject({ id: "bnf1-port-1", from: { id: FILE_ROOT } });
    await received.ack();
    v2Abort.abort();

    // Never speak for an identity the link does not hold.
    await expect(port.send({ ...command, id: "bnf1-port-2", to: { machine: V2, hostId: NATS_ROOT }, targetId: NATS_ROOT,
      from: { id: "session:99999999-9999-4999-8999-999999999999", name: "x", kind: "main" } })).rejects.toMatchObject({ code: "FABRIC_SENDER_UNVERIFIED" });

    // Withdraw: the receiver loses the mirrored root at once (lease first).
    await port.withdraw();
    await vi.waitFor(() => expect(receiver!.resolve(FILE_ROOT)?.live ?? false).toBe(false), { timeout: 10_000, interval: 50 });
  }, 60_000);
});
