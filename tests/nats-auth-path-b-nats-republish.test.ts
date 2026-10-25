// expected-skip: no NATS_BIN/PATH executable: authenticated loopback broker is required.
// Keep reason: smarty-dev#15310 A' PR 2, N4 (lane L1). On a REAL Path B broker: a v2 Main's fleet.work
// event on FABRIC_EV_<ORG> lands once in the hub file mesh, authored as the link with the verified sender
// stamped; a stamped (already bridged) event never does; the bridge never publishes into FABRIC_EV; and a
// bridge credential without the consume grant refuses the start.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { openNatsBridgeSide, type NatsBridgePort } from "../src/mesh/bridge-nats-port.js";
import { natsRepublishStatePath, startNatsRepublishLink, type NatsRepublishHandle } from "../src/mesh/bridge-nats-republish.js";
import { MeshStore } from "../src/mesh/store.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { openBootstrapNatsConnection, openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { openNatsEventsPublisher } from "../src/transport/nats/events-publish.js";
import { openNatsTransport } from "../src/transport/nats/index.js";
import { ensureNatsEventsStream } from "../src/transport/nats/provision.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import type { FabricTransport } from "../src/transport/types.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: N4 republish needs an executable NATS_BIN");

const ORG = "republish-org";
const BRIDGE = "machine-a";
const V2 = "machine-b";
const LINK = "v2";
const NATS_ROOT = "session:22222222-2222-4222-8222-222222222222";
const key = (machine: string, kind: "p" | "h", id: string) => `${machine}.${kind}.${createHash("sha256").update(id).digest("hex").slice(0, 32)}`;

let broker: PathBHarness;
let plain: PathBHarness;
const closers: Array<() => Promise<unknown>> = [];
const temps: string[] = [];

const provision = async (server: PathBHarness, events: boolean): Promise<void> => {
  for (const [name, value] of Object.entries(server.adminEnv(server.users.bootstrap))) vi.stubEnv(name, value);
  try {
    expect(await runTransportAdmin(["provision", "--ca-file", server.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally { vi.unstubAllEnvs(); }
  if (!events) return;
  const bootstrap = await openBootstrapNatsConnection({ servers: `tls://localhost:${new URL(server.serverUrl).port}`,
    credentials: server.users.bootstrap.credsPath, tls: { caFile: server.tls!.caFile }, name: "republish-bootstrap" });
  // A small stream: the test broker's JetStream storage also holds the transport streams.
  try { await ensureNatsEventsStream(bootstrap.manager, { org: ORG, provision: true, maxBytes: 4 * 1024 * 1024 }); }
  finally { await bootstrap.close(); }
};

beforeAll(async () => {
  if (!binary) return;
  broker = await createPathBHarness({ binary, tls: {}, org: ORG, events: true });
  await provision(broker, true);
  plain = await createPathBHarness({ binary, tls: {} });
  await provision(plain, false);
}, 60_000);

afterAll(async () => {
  for (const close of closers.reverse()) await close().catch(() => undefined);
  await broker?.stop();
  await plain?.stop();
  for (const dir of temps) fs.rmSync(dir, { recursive: true, force: true });
});

const linkConfig = (server: PathBHarness, stateDir: string) => {
  const runtime = server.runtimeOptions(server.users.machineA);
  return { link: LINK, stateDir, nats: { servers: [String(runtime.servers)], machine: BRIDGE, credentials: runtime.credentials,
    caFile: server.tls!.caFile, custodyStream: PATH_B_PERMISSION_POLICY.custodyStream, livenessStream: PATH_B_PERMISSION_POLICY.livenessStream,
    pathBMachines: [...PATH_B_PERMISSION_POLICY.pathBMachines], org: ORG, events: true } };
};

const natsRoot = (now: number): FabricParticipantRecord => ({ format: 1, id: NATS_ROOT, kind: "root", rootId: NATS_ROOT,
  ownerHostId: NATS_ROOT, ownerIdentityId: NATS_ROOT, ownerIncarnation: "nats-inc-1", name: "v2-lead", status: "idle", runner: "pi",
  transport: "host", capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: now, updatedAt: now,
  sessionId: NATS_ROOT.slice(8) });

describe.skipIf(!binary)("A' N4 republish on a real Path B broker", () => {
  it("republishes a v2 Main's work event into the file mesh once, as the link, and never publishes into FABRIC_EV", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "nats-republish-"));
    temps.push(dir);
    const store = new MeshStore(path.join(dir, "mesh"), 256 * 1024, 1_000);
    closers.push(async () => store.closeState());
    const config = linkConfig(broker, dir);
    const port: NatsBridgePort = await openNatsBridgeSide({ link: LINK, nats: config.nats,
      allow: { file: [], nats: [{ kind: "root", id: NATS_ROOT, machine: V2 }] } }, { signal: AbortSignal.timeout(20_000) });
    closers.push(() => port.close());

    // The v2 Main on machine-b: live presence + lease in KV, and its own events publisher.
    const v2: FabricTransport = await openNatsTransport({ ...broker.runtimeOptions(broker.users.machineB), hostId: NATS_ROOT, org: ORG });
    closers.push(() => v2.close());
    const now = Date.now();
    const owner = `${NATS_ROOT}#nats-inc-1`;
    await v2.kv("leases").put(key(V2, "h", NATS_ROOT), owner, { id: NATS_ROOT, rootId: NATS_ROOT, identityId: NATS_ROOT,
      incarnation: "nats-inc-1", incarnationToken: "nats-inc-1", pid: process.pid, pidStartTime: "1",
      bootId: "12345678-1234-4234-8234-123456789abc", updatedAt: now, expiresAt: now + 15_000 }, 0);
    await v2.kv("presence").put(key(V2, "p", NATS_ROOT), owner, natsRoot(now), 0);
    await vi.waitFor(() => expect(port.resolve(NATS_ROOT)?.route.live).toBe(true), { timeout: 10_000, interval: 50 });
    const v2Connection = await openNatsConnection({ ...broker.runtimeOptions(broker.users.machineB), events: true, org: ORG, name: "v2-events" });
    closers.push(() => v2Connection.close());
    const publisher = await openNatsEventsPublisher(v2Connection, { org: ORG, provision: false, maxBytes: 4 * 1024 * 1024 });

    // Every publish the bridge's events connection attempts is recorded: none may target FABRIC_EV.
    const attempted: string[] = [];
    const connect = async (input: Parameters<typeof openNatsConnection>[0]): Promise<NatsConnectionHandle> => {
      const connection = await openNatsConnection(input);
      const publish = connection.nc.publish.bind(connection.nc);
      connection.nc.publish = ((subject: string, ...rest: unknown[]) => {
        if (!subject.startsWith("$JS.API.") && !subject.startsWith("$JS.ACK.") && !subject.startsWith("$SYS.")) attempted.push(subject);
        return (publish as (...args: unknown[]) => void)(subject, ...rest);
      }) as typeof connection.nc.publish;
      const jsPublish = connection.js.publish.bind(connection.js);
      connection.js.publish = ((subject: string, ...rest: unknown[]) => { attempted.push(`js:${subject}`);
        return (jsPublish as (...args: unknown[]) => unknown)(subject, ...rest); }) as typeof connection.js.publish;
      return connection;
    };
    const counted: string[] = [];
    const handle: NatsRepublishHandle | undefined = await startNatsRepublishLink(config, { store, port,
      allow: [{ side: "nats", kind: "root", id: NATS_ROOT, machine: V2, republishTopics: ["fleet.work.*"] }],
      signal: new AbortController().signal, log: () => undefined, connect, counters: { add: name => { counted.push(name); } } });
    expect(handle).toBeDefined();
    closers.push(() => handle!.stop());

    const base = { kind: "message", from: { id: NATS_ROOT, name: "claimed-by-body", kind: "main" as const }, createdAt: Date.now() };
    await publisher.publish({ ...base, id: "v2-task-1", topic: "fleet.work.tasks", to: "factory", text: "task done", data: { task: 1 } });
    // Already bridged somewhere: must never cross again (no loops).
    await publisher.publish({ ...base, id: "v2-loop-1", topic: "fleet.work.tasks", to: "factory", text: "loop",
      data: { bridge: { from: "elsewhere", id: "x" } } });
    // The same message id again inside the dedupe window is a broker duplicate: one stream copy.
    await publisher.publish({ ...base, id: "v2-task-1", topic: "fleet.work.tasks", to: "factory", text: "task done", data: { task: 1 } });
    const crossed = () => store.read({ after: 0, topic: "fleet.work.tasks", limit: 100 });
    await vi.waitFor(() => expect(counted).toEqual(expect.arrayContaining(["republish.accepted", "republish.refused"])),
      { timeout: 15_000, interval: 50 });
    expect(crossed()).toHaveLength(1);
    expect(crossed()[0]).toMatchObject({ topic: "fleet.work.tasks", to: "factory", text: "task done",
      from: { id: `bridge:${LINK}`, name: "mesh-bridge", kind: "main" },
      data: { task: 1, bridge: { from: LINK, id: "v2-task-1", sender: { id: NATS_ROOT, name: "v2-lead", machine: V2 } } } });
    await handle!.stop();
    expect(attempted).toEqual([]);
    expect(JSON.parse(fs.readFileSync(natsRepublishStatePath(dir), "utf8"))).toMatchObject({ state: "stopped", durable: "bev_machine-a" });
  }, 60_000);

  it("refuses the start when the bridge credential has no consume grant on FABRIC_EV, and says so for status", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "nats-republish-plain-"));
    temps.push(dir);
    const store = new MeshStore(path.join(dir, "mesh"), 256 * 1024, 1_000);
    closers.push(async () => store.closeState());
    await expect(startNatsRepublishLink(linkConfig(plain, dir), { store, port: { machine: BRIDGE, resolve: () => undefined },
      allow: [], signal: AbortSignal.timeout(20_000), log: () => undefined }))
      .rejects.toMatchObject({ code: "FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING" });
    expect(JSON.parse(fs.readFileSync(natsRepublishStatePath(dir), "utf8"))).toMatchObject({ state: "refused",
      code: "FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING" });
  }, 60_000);
});
