// Keep reason: the recorded Ready-but-absent incident is only caught across the actual
// extension session lifecycle, native NATS retry, authenticated broker, and broker-visible root.
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { normalizeFabricConfig } from "../src/config.js";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import type { NatsKvStore } from "../src/transport/nats/kv.js";
import { PATH_B_MACHINES, resolveNatsBinary, createPathBHarness, type PathBHarness } from "./helpers/nats-cluster.js";
import { ReconnectRelay } from "./helpers/nats-reconnect-server.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");

async function bounded<T>(promise: Promise<T>, label: string, timeoutMs = 10_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    })]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function waitFor<T>(read: () => Promise<T>, accept: (value: T) => boolean, label: string, timeoutMs = 12_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let value: T;
  do {
    value = await read();
    if (accept(value)) return value;
    if (Date.now() >= deadline) break;
    await delay(25);
  } while (true);
  throw new Error(`${label} timed out after ${timeoutMs}ms`);
}

async function provision(broker: PathBHarness): Promise<void> {
  const env = broker.adminEnv(broker.users.bootstrap);
  const previous = new Map(Object.keys(env).map(name => [name, process.env[name]]));
  for (const [name, value] of Object.entries(env)) process.env[name] = value;
  try {
    expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally {
    for (const [name, value] of previous) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

type Handler = (event: unknown, context: ExtensionContext) => unknown;

function sessionFixture(broker: PathBHarness, relay: ReconnectRelay) {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "nats-first-connect-session-"));
  const agentDir = path.join(cwd, "agent");
  const sessionId = randomUUID();
  const rootId = `session:${sessionId}`;
  fs.mkdirSync(agentDir, { recursive: true, mode: 0o700 });
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_FABRIC_MESH_HOST_TRANSPORT", "nats");

  const config = normalizeFabricConfig({
    fullCodeMode: false,
    components: [],
    prewalk: { enabled: false, alwaysRearm: false },
    agents: { enabled: false },
    residency: { enabled: false },
    mcp: { enabled: false },
    memory: { enabled: false },
    jev: { enabled: false },
    mesh: {
      enabled: true,
      announce: true,
      transport: "nats",
      hostTransport: "nats",
      root: path.join(cwd, "mesh"),
      nats: {
        ...broker.runtimeOptions(broker.users.machineA),
        servers: relay.url,
        operatorMachine: "fabric-ops",
      },
    },
  });
  fs.mkdirSync(path.join(cwd, ".pi"), { mode: 0o700 });
  fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify(config), { mode: 0o600 });

  const handlers = new Map<string, Handler[]>();
  const setStatus = vi.fn();
  const pi = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    on(name: string, handler: Handler) { handlers.set(name, [...(handlers.get(name) ?? []), handler]); },
    getActiveTools: () => [], getAllTools: () => [], getThinkingLevel: () => "off",
    getSessionName: () => "first-connect-session", appendEntry: vi.fn(),
    registerCommand: vi.fn(), registerShortcut: vi.fn(), registerMessageRenderer: vi.fn(), registerTool: vi.fn(),
    setActiveTools: vi.fn(), sendMessage: vi.fn(),
  } as unknown as ExtensionAPI;
  let disposed = false;
  const context = new Proxy({
    cwd, hasUI: false, mode: "rpc", isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined },
    sessionManager: {
      getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getEntries: () => [],
      getLeafId: () => null, getLeafEntry: () => undefined, getEntry: () => undefined,
    },
    ui: { setStatus, notify: vi.fn() },
  }, { get(object, key, receiver) {
    if (disposed) throw new Error("This extension ctx is stale after session replacement or reload.");
    return Reflect.get(object, key, receiver);
  } }) as unknown as ExtensionContext;
  const emit = async (name: string, event: unknown = {}, owner = context) => {
    for (const handler of handlers.get(name) ?? []) await handler(event, owner);
  };
  return { cwd, agentDir, rootId, sessionId, context, pi, setStatus, emit,
    dispose() { disposed = true; }, restoreForCleanup() { disposed = false; } };
}

async function inspector(broker: PathBHarness): Promise<{
  connection: NatsConnectionHandle;
  presence: NatsKvStore;
  leases: NatsKvStore;
}> {
  const connection = await openNatsConnection({
    ...broker.runtimeOptions(broker.users.machineB),
    name: `first-connect-inspector-${randomUUID()}`,
  });
  try {
    const presence = await openNatsKvForMachine(connection.js, "presence", "machine-b", false, connection.onStatus);
    const leases = await openNatsKvForMachine(connection.js, "leases", "machine-b", false, connection.onStatus);
    return { connection, presence, leases };
  } catch (error) {
    await connection.close();
    throw error;
  }
}

const keyFor = (machine: string, kind: "p" | "h", id: string): string =>
  `${machine}.${kind}.${createHash("sha256").update(id).digest("hex").slice(0, 32)}`;

async function registerActualExtension(pi: ExtensionAPI): Promise<void> {
  if (process.env.PI_TEST_COMPILED === "1") {
    const entry = new URL("../dist/index.js", import.meta.url).href;
    const extension = await import(entry);
    await extension.default(pi);
    return;
  }
  const { createFabricExtension } = await import("../src/extension.js");
  await createFabricExtension(import.meta.url)(pi);
}

let broker: PathBHarness | undefined;

describe.skipIf(!binary || process.platform === "win32")("actual NATS session_start first-connect flow", () => {
  beforeAll(async () => {
    broker = await createPathBHarness({ binary: binary!, tls: { dnsNames: ["localhost"], ipAddresses: [] } });
    await provision(broker);
  }, 30_000);

  afterAll(async () => {
    await broker?.stop();
    broker = undefined;
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it("returns NOT CONNECTED during a real outage, then joins the authenticated broker without restart or re-ensure", async () => {
    if (!broker) throw new Error("real NATS broker fixture is unavailable");
    const relay = await ReconnectRelay.create(broker);
    relay.enabled = false;
    const f = sessionFixture(broker, relay);
    const readyFile = path.join(f.agentDir, "fabric", `nats-ready-${process.pid}`);
    const participantKey = keyFor("machine-a", "p", f.rootId);
    const leaseKey = keyFor("machine-a", "h", f.rootId);
    let observer: Awaited<ReturnType<typeof inspector>> | undefined;
    let shutdownInvoked = false;
    try {
      observer = await inspector(broker);
      expect(await observer.presence.get(participantKey)).toBeUndefined();
      expect(await observer.leases.get(leaseKey)).toBeUndefined();

      await registerActualExtension(f.pi);
      // This is the actual registered extension handler. It must not await native first-connect.
      await bounded(f.emit("session_start"), "ordinary session_start return", 8_000);
      expect(relay.enabled).toBe(false);
      expect(f.setStatus).toHaveBeenCalledWith(
        "fabric-nats", expect.stringContaining("NOT CONNECTED"),
      );
      expect(path.basename(readyFile)).toBe(`nats-ready-${process.pid}`);
      expect(fs.existsSync(readyFile)).toBe(false);

      // Cold source activation loads the real lazy runtime before dialing. Allow that work
      // here, separately from the unchanged nonblocking session_start bound above.
      await waitFor(async () => relay.connectionCount, count => count > 0, "initial unavailable endpoint attempt", 20_000);
      await bounded(relay.whenIdle(), "failed initial endpoint socket close", 2_000);
      expect(relay.subjects).not.toContain("$SYS.REQ.USER.INFO");
      expect(await observer.presence.get(participantKey)).toBeUndefined();
      expect(await observer.leases.get(leaseKey)).toBeUndefined();

      // Keep a full three-second outage AFTER the initial failed socket, even on cold activation.
      await delay(3_000);
      expect(fs.existsSync(readyFile)).toBe(false);
      expect(await observer.presence.get(participantKey)).toBeUndefined();
      expect(await observer.leases.get(leaseKey)).toBeUndefined();

      const statusCallsBeforeRecovery = f.setStatus.mock.calls.length;
      relay.holdVerification = true;
      relay.enabled = true;
      await bounded(relay.verification.promise, "real authenticated USER.INFO request", 10_000);
      expect(fs.existsSync(readyFile), "readiness must wait for verified USER.INFO").toBe(false);
      expect(await observer.presence.get(participantKey)).toBeUndefined();
      relay.releaseVerification();

      await waitFor(async () => fs.existsSync(readyFile), exists => exists, "PID-scoped verified readiness", 12_000);
      const participant = await waitFor(
        () => observer!.presence.get<Record<string, unknown>>(participantKey),
        value => value !== undefined,
        "broker-visible root participant publication",
        12_000,
      );
      const lease = await waitFor(
        () => observer!.leases.get<Record<string, unknown>>(leaseKey),
        value => value !== undefined,
        "broker-visible host lease publication",
        12_000,
      );
      expect(participant!.value).toMatchObject({
        id: f.rootId, rootId: f.rootId, sessionId: f.sessionId, kind: "root",
        ownerHostId: f.rootId, ownerIdentityId: f.rootId,
      });
      expect(lease!.value).toMatchObject({ id: f.rootId, rootId: f.rootId, identityId: f.rootId, pid: process.pid });
      expect(relay.subjects).toContain("$SYS.REQ.USER.INFO");
      await waitFor(
        async () => f.setStatus.mock.calls.slice(statusCallsBeforeRecovery).some(([key, value]) => key === "fabric-nats" && value === undefined),
        cleared => cleared,
        "connected NATS status clear",
        5_000,
      );

      // Observe custody at native carrier closure, not merely after shutdown has returned:
      // withdrawal must finish while the established authenticated connection is still usable.
      expect(relay.activeConnections).toBeGreaterThan(0);
      const custodyAtCarrierClose = relay.whenIdle().then(async () => ({
        participant: await observer!.presence.get(participantKey), lease: await observer!.leases.get(leaseKey),
      }));
      // No second session_start or direct ensure/initialize call: recovery happened in this live session.
      shutdownInvoked = true;
      await bounded(f.emit("session_shutdown", { reason: "exit" }), "connected session_shutdown", 15_000);
      expect(fs.existsSync(readyFile), "shutdown must clear this process's readiness marker").toBe(false);
      await bounded(relay.whenIdle(), "shutdown closes the recovered client sockets", 3_000);
      expect(await bounded(custodyAtCarrierClose, "custody withdrawn before established carrier closes", 3_000))
        .toEqual({ participant: undefined, lease: undefined });
    } finally {
      if (!shutdownInvoked) {
        shutdownInvoked = true;
        await bounded(f.emit("session_shutdown", { reason: "exit" }), "session fixture shutdown", 15_000).catch(() => undefined);
      }
      await observer?.connection.close().catch(() => undefined);
      await relay.stop();
      fs.rmSync(f.cwd, { recursive: true, force: true });
    }
  }, 60_000);

  it.each(["outage", "verification"] as const)("cancels a disposed %s-pending session without shutdown or a successor", async (phase) => {
    if (!broker) throw new Error("real NATS broker fixture is unavailable");
    const relay = await ReconnectRelay.create(broker);
    relay.enabled = phase !== "outage";
    relay.holdVerification = phase === "verification";
    const f = sessionFixture(broker, relay);
    const readyFile = path.join(f.agentDir, "fabric", `nats-ready-${process.pid}`);
    const participantKey = keyFor("machine-a", "p", f.rootId);
    const leaseKey = keyFor("machine-a", "h", f.rootId);
    let observer: Awaited<ReturnType<typeof inspector>> | undefined;
    try {
      observer = await inspector(broker);
      await registerActualExtension(f.pi);
      await bounded(f.emit("session_start"), "dispose fixture session_start return", 8_000);
      if (phase === "verification") {
        await bounded(relay.verification.promise, "real authenticated USER.INFO before disposal");
      } else {
        await waitFor(async () => relay.connectionCount, count => count > 0, "outage attempt before disposal", 5_000);
        await bounded(relay.whenIdle(), "failed outage socket before disposal", 2_000);
      }
      expect(fs.existsSync(readyFile)).toBe(false);
      expect(await observer.presence.get(participantKey)).toBeUndefined();
      expect(await observer.leases.get(leaseKey)).toBeUndefined();
      const statusesBeforeDisposal = [...f.setStatus.mock.calls];

      // AgentSession.dispose() invalidates every ctx getter but emits no session_shutdown.
      // No native connection/runtime/protocol mocks: the owner must stop the actual client.
      f.dispose();
      if (phase === "verification") {
        await bounded(relay.verificationClosed.promise, "disposed pending USER.INFO socket closes", 1_500);
      }
      // 250ms liveness safety net plus scheduling allowance; not a startup deadline.
      await delay(750);
      await bounded(relay.whenIdle(), "disposed pending activation releases all sockets", 1_500);
      const attemptsAfterDisposal = relay.connectionCount;
      const commandsAfterDisposal = [...relay.commands];
      relay.enabled = true;
      relay.releaseVerification();
      // Exceeds the native backoff cap (5s + <250ms jitter), so an orphan cannot hide in a gap.
      await delay(6_000);
      expect(relay.connectionCount).toBe(attemptsAfterDisposal);
      expect(relay.commands).toEqual(commandsAfterDisposal);
      expect(fs.existsSync(readyFile)).toBe(false);
      expect(await observer.presence.get(participantKey)).toBeUndefined();
      expect(await observer.leases.get(leaseKey)).toBeUndefined();
      expect(f.setStatus.mock.calls).toEqual(statusesBeforeDisposal);
      expect(relay.errors).toEqual([]);
    } finally {
      // Restore the fixture only after proving disposal alone cancelled acquisition.
      f.restoreForCleanup();
      await bounded(f.emit("session_shutdown", { reason: "exit" }), "dispose fixture final cleanup", 8_000).catch(() => undefined);
      await observer?.connection.close().catch(() => undefined);
      await relay.stop();
      fs.rmSync(f.cwd, { recursive: true, force: true });
    }
  }, 30_000);

  it("withdraws an established predecessor before closing its carrier, then cancels only its disposed pending successor", async () => {
    if (!broker) throw new Error("real NATS broker fixture is unavailable");
    const relay = await ReconnectRelay.create(broker);
    const successorRelay = await ReconnectRelay.create(broker);
    const f = sessionFixture(broker, relay);
    const readyFile = path.join(f.agentDir, "fabric", `nats-ready-${process.pid}`);
    const participantKey = keyFor("machine-a", "p", f.rootId);
    const leaseKey = keyFor("machine-a", "h", f.rootId);
    const successorId = randomUUID();
    const successorKey = keyFor("machine-a", "p", `session:${successorId}`);
    const successorStatus = vi.fn();
    let successorDisposed = false;
    const successor = new Proxy({ ...f.context,
      sessionManager: { ...f.context.sessionManager, getSessionId: () => successorId },
      ui: { ...f.context.ui, setStatus: successorStatus },
    }, { get(object, key, receiver) {
      if (successorDisposed) throw new Error("This extension ctx is stale after session replacement or reload.");
      return Reflect.get(object, key, receiver);
    } }) as unknown as ExtensionContext;
    let observer: Awaited<ReturnType<typeof inspector>> | undefined;
    try {
      observer = await inspector(broker);
      await registerActualExtension(f.pi);
      await bounded(f.emit("session_start"), "predecessor session_start", 8_000);
      await waitFor(() => observer!.presence.get(participantKey), value => value !== undefined, "established predecessor");
      await waitFor(() => observer!.leases.get(leaseKey), value => value !== undefined, "predecessor custody");
      await waitFor(async () => f.setStatus.mock.calls.filter(([key]) => key === "fabric-nats").at(-1)?.[1] === undefined,
        cleared => cleared, "predecessor activation has fully committed");
      expect(fs.existsSync(readyFile)).toBe(true);

      // A completed activation has no new dispose watchdog. Its carrier is needed to withdraw
      // custody on replacement; disposal itself must not prematurely abort that established owner.
      f.dispose();
      await delay(600);
      expect(relay.activeConnections).toBeGreaterThan(0);
      expect(fs.existsSync(readyFile)).toBe(true);
      const predecessorCustodyAtClose = relay.whenIdle().then(async () => ({
        participant: await observer!.presence.get(participantKey), lease: await observer!.leases.get(leaseKey),
      }));
      // Separate real relays make the predecessor's carrier-close event unambiguous even
      // if a new socket opens before all old TLS close callbacks have finished.
      const configFile = path.join(f.cwd, ".pi", "fabric.json");
      const successorConfig = JSON.parse(fs.readFileSync(configFile, "utf8"));
      successorConfig.mesh.nats.servers = successorRelay.url;
      fs.writeFileSync(configFile, JSON.stringify(successorConfig), { mode: 0o600 });
      successorRelay.holdVerification = true;
      await bounded(f.emit("session_start", {}, successor), "successor nonblocking session_start", 8_000);
      await bounded(successorRelay.verification.promise, "successor authenticated USER.INFO");
      expect(await bounded(predecessorCustodyAtClose, "predecessor custody at carrier close", 3_000))
        .toEqual({ participant: undefined, lease: undefined });
      expect(fs.existsSync(readyFile)).toBe(false);
      expect(await observer.presence.get(successorKey)).toBeUndefined();
      expect(successorStatus.mock.calls.some(([, value]) => /disposed|superseded/.test(String(value)))).toBe(false);

      const statusesBeforeDisposal = [...successorStatus.mock.calls];
      successorDisposed = true;
      await bounded(successorRelay.verificationClosed.promise, "disposed successor acquisition socket closes", 1_500);
      await bounded(successorRelay.whenIdle(), "disposed successor sockets drain", 1_500);
      expect(successorStatus.mock.calls).toEqual(statusesBeforeDisposal);
      expect(fs.existsSync(readyFile)).toBe(false);
      expect(await observer.presence.get(successorKey)).toBeUndefined();
      expect(relay.errors).toEqual([]);
      expect(successorRelay.errors).toEqual([]);
    } finally {
      successorDisposed = false;
      await bounded(f.emit("session_shutdown", { reason: "exit" }, successor), "predecessor/successor fixture cleanup", 8_000).catch(() => undefined);
      await observer?.connection.close().catch(() => undefined);
      await relay.stop();
      await successorRelay.stop();
      fs.rmSync(f.cwd, { recursive: true, force: true });
    }
  }, 30_000);

  it("cancels an outage-pending session on shutdown and opens no socket after the endpoint returns", async () => {
    if (!broker) throw new Error("real NATS broker fixture is unavailable");
    const relay = await ReconnectRelay.create(broker);
    relay.enabled = false;
    const f = sessionFixture(broker, relay);
    const readyFile = path.join(f.agentDir, "fabric", `nats-ready-${process.pid}`);
    const participantKey = keyFor("machine-a", "p", f.rootId);
    let observer: Awaited<ReturnType<typeof inspector>> | undefined;
    let shutdownInvoked = false;
    try {
      observer = await inspector(broker);
      await registerActualExtension(f.pi);
      await bounded(f.emit("session_start"), "outage-pending session_start return", 8_000);
      await waitFor(async () => relay.connectionCount, count => count > 0, "initial unavailable endpoint attempt", 5_000);
      await bounded(relay.whenIdle(), "unavailable endpoint socket close", 2_000);
      expect(fs.existsSync(readyFile)).toBe(false);
      expect(await observer.presence.get(participantKey)).toBeUndefined();

      shutdownInvoked = true;
      await bounded(f.emit("session_shutdown", { reason: "exit" }), "shutdown cancels pending first connect", 8_000);
      expect(fs.existsSync(readyFile)).toBe(false);
      await bounded(relay.whenIdle(), "cancelled first-connect socket close", 2_000);
      const attemptsAtShutdown = relay.connectionCount;
      const commandsAtShutdown = [...relay.commands];
      relay.enabled = true;
      // Bounded safety interval spans multiple native reconnect backoffs; close events above are primary.
      await delay(3_000);
      expect(relay.connectionCount).toBe(attemptsAtShutdown);
      expect(relay.commands).toEqual(commandsAtShutdown);
      expect(fs.existsSync(readyFile)).toBe(false);
      expect(await observer.presence.get(participantKey)).toBeUndefined();
    } finally {
      if (!shutdownInvoked) {
        shutdownInvoked = true;
        await bounded(f.emit("session_shutdown", { reason: "exit" }), "session fixture shutdown", 8_000).catch(() => undefined);
      }
      await observer?.connection.close().catch(() => undefined);
      await relay.stop();
      fs.rmSync(f.cwd, { recursive: true, force: true });
    }
  }, 30_000);
});
