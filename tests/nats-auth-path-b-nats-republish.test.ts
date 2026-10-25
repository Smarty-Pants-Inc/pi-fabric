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
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { FileCarrierPresence } from "../src/mesh/bridge-nats-file.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection } from "../src/mesh/bridge-nats-identity.js";
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
const OTHER_ROOT = "session:33333333-3333-4333-8333-333333333333";
// L3's gate, as the link builds it: NATS_ROOT may republish fleet.work.*, OTHER_ROOT nothing.
const gate = createBridgeNatsIdentityGate(parseBridgeNatsAllowSection({ nats: [
  { kind: "root", id: NATS_ROOT, machine: V2, republishTopics: ["fleet.work.*"] },
  { kind: "root", id: OTHER_ROOT, machine: V2 },
] }, { link: LINK, natsMachine: BRIDGE }), { pins: { seats: new Map(), bound: new Map() } });
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

const natsRoot = (now: number, id = NATS_ROOT): FabricParticipantRecord => ({ format: 1, id, kind: "root", rootId: id,
  ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "nats-inc-1", name: "v2-lead", status: "idle", runner: "pi",
  transport: "host", capabilities: ["steer", "followUp", "fabric"], controlProtocol: "v1", startedAt: now, updatedAt: now,
  sessionId: id.slice(8) });

describe.skipIf(!binary)("A' N4 republish on a real Path B broker", () => {
  it("republishes a v2 Main's work event into the file mesh once, as the link, and never publishes into FABRIC_EV", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "nats-republish-"));
    temps.push(dir);
    const store = new MeshStore(path.join(dir, "mesh"), 256 * 1024, 1_000);
    closers.push(async () => store.closeState());
    const config = linkConfig(broker, dir);
    const port: NatsBridgePort = await openNatsBridgeSide({ link: LINK, nats: config.nats,
      allow: { file: [], nats: [{ kind: "root", id: NATS_ROOT, machine: V2 }, { kind: "root", id: OTHER_ROOT, machine: V2 }] } },
      { signal: AbortSignal.timeout(20_000) });
    closers.push(() => port.close());
    // The hub's file presence, as the link's carrier reads it.
    const filePresence = new FileCarrierPresence(new StoreBridgeSide(store, LINK));

    // The v2 Main on machine-b: live presence + lease in KV, and its own events publisher.
    const v2: FabricTransport = await openNatsTransport({ ...broker.runtimeOptions(broker.users.machineB), hostId: NATS_ROOT, org: ORG });
    closers.push(() => v2.close());
    const now = Date.now();
    for (const id of [NATS_ROOT, OTHER_ROOT]) {
      const owner = `${id}#nats-inc-1`;
      await v2.kv("leases").put(key(V2, "h", id), owner, { id, rootId: id, identityId: id,
        incarnation: "nats-inc-1", incarnationToken: "nats-inc-1", pid: process.pid, pidStartTime: "1",
        bootId: "12345678-1234-4234-8234-123456789abc", updatedAt: now, expiresAt: now + 15_000 }, 0);
      await v2.kv("presence").put(key(V2, "p", id), owner, natsRoot(now, id), 0);
    }
    await vi.waitFor(() => expect(port.resolve(OTHER_ROOT)?.route.live).toBe(true), { timeout: 10_000, interval: 50 });
    expect(port.resolve(NATS_ROOT)?.route.live).toBe(true);
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
    const handle: NatsRepublishHandle | undefined = await startNatsRepublishLink(config, { store, port, gate,
      views: { refresh: () => filePresence.refresh(), nats: () => port.presenceView(), file: () => filePresence.view() },
      signal: new AbortController().signal, log: () => undefined, connect, counters: { add: name => { counted.push(name); } } });
    expect(handle).toBeDefined();
    closers.push(() => handle!.stop());

    const base = { kind: "message", from: { id: NATS_ROOT, name: "claimed-by-body", kind: "main" as const }, createdAt: Date.now() };
    // A work topic the hub factory takes when addressed to it (fleet.work.<repo>.<n>).
    const topic = "fleet.work.pi-fabric.1593";
    await publisher.publish({ ...base, id: "v2-task-1", topic, to: "factory", text: "task done", data: { task: 1 } });
    // Already bridged somewhere: must never cross again (no loops).
    await publisher.publish({ ...base, id: "v2-loop-1", topic, to: "factory", text: "loop",
      data: { bridge: { from: "elsewhere", id: "x" } } });
    // The same message id again inside the dedupe window is a broker duplicate: one stream copy.
    await publisher.publish({ ...base, id: "v2-task-1", topic, to: "factory", text: "task done", data: { task: 1 } });
    // A live allow-listed root without fleet.work.* in its republishTopics: L3's gate refuses it.
    await publisher.publish({ ...base, from: { id: OTHER_ROOT, name: "other", kind: "main" }, id: "v2-other-1",
      topic, to: "factory", text: "not mine to republish" });
    const crossed = () => store.read({ after: 0, topic, limit: 100 });
    await vi.waitFor(() => expect(counted.filter(name => name === "republish.refused")).toHaveLength(2),
      { timeout: 15_000, interval: 50 });
    await vi.waitFor(() => expect(counted).toContain("republish.accepted"), { timeout: 15_000, interval: 50 });
    expect(crossed()).toHaveLength(1);
    expect(crossed()[0]).toMatchObject({ topic, to: "factory", text: "task done",
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
    const config = linkConfig(plain, dir);
    // The link's port opens on this credential even though the config names an org (N4's): it never asks for
    // org or event grants, so the refusal below is N4's own, by name, not a USER.INFO mismatch.
    expect(config.nats.org).toBe(ORG);
    const port = await openNatsBridgeSide({ link: LINK, nats: config.nats, allow: { file: [], nats: [] } },
      { signal: AbortSignal.timeout(20_000) });
    closers.push(() => port.close());
    const filePresence = new FileCarrierPresence(new StoreBridgeSide(store, LINK));
    const refusal = startNatsRepublishLink(config, { store, port, gate,
      views: { refresh: () => filePresence.refresh(), nats: () => port.presenceView(), file: () => filePresence.view() },
      signal: AbortSignal.timeout(20_000), log: () => undefined });
    await expect(refusal).rejects.toMatchObject({ code: "FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING" });
    await expect(refusal).rejects.toThrow(
      /^FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING: .* lacks the FABRIC_EV_\S+ consume grant \(durable bev_machine-a: /);
    expect(JSON.parse(fs.readFileSync(natsRepublishStatePath(dir), "utf8"))).toMatchObject({ state: "refused",
      code: "FABRIC_BRIDGE_NATS_EVENTS_GRANT_MISSING" });
  }, 60_000);
});
