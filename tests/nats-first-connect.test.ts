// Keep reason: this covers the recorded session_start outage path (W1-critical) on the real authenticated TLS broker.
// ReconnectRelay only drops/routes bytes; it never synthesizes the NATS handshake or USER.INFO response.
import fs from "node:fs";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { ReconnectRelay } from "./helpers/nats-reconnect-server.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");

async function bounded<T>(promise: Promise<T>, label: string, ms = 8_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    })]);
  } finally { if (timer) clearTimeout(timer); }
}

async function waitFor(predicate: () => boolean, label: string, ms = 8_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`${label} timed out after ${ms}ms`);
    await delay(20);
  }
}

function outcome<T>(promise: Promise<T>): Promise<{ readonly status: "fulfilled"; readonly value: T } | { readonly status: "rejected"; readonly error: unknown }> {
  return promise.then(
    (value) => ({ status: "fulfilled" as const, value }),
    (error: unknown) => ({ status: "rejected" as const, error }),
  );
}

type OpenOutcome = Awaited<ReturnType<typeof outcome<NatsConnectionHandle>>>;

let broker: PathBHarness | undefined;
let markerSequence = 0;

function readyFile(): string {
  if (!broker) throw new Error("real NATS broker fixture is not ready");
  return path.join(broker.root, `nats-ready-${++markerSequence}`);
}

function openWithReadyFile(relay: ReconnectRelay, file: string, signal?: AbortSignal): Promise<NatsConnectionHandle> {
  if (!broker) throw new Error("real NATS broker fixture is not ready");
  const options = {
    ...broker.runtimeOptions(broker.users.machineA),
    servers: relay.url,
    retryFirstConnect: true,
    readyFile: file,
    ...(signal === undefined ? {} : { signal }),
  };
  // readyFile is the planned API; keep this test RED until the production option is added.
  return openNatsConnection(options as unknown as Parameters<typeof openNatsConnection>[0]);
}

describe.skipIf(!binary)("openNatsConnection first-connect readiness", () => {
  beforeAll(async () => {
    broker = await createPathBHarness({ binary: binary!, tls: { dnsNames: ["localhost"], ipAddresses: [] } });
  }, 30_000);

  afterAll(async () => {
    await broker?.stop();
    broker = undefined;
  });

  it("recovers when the NATS endpoint returns 3 seconds after startup without restarting the caller", async () => {
    const relay = await ReconnectRelay.create(broker!);
    const file = readyFile();
    relay.enabled = false; // The client-facing NATS endpoint is unavailable; the real TLS/auth broker stays behind the relay.
    const started = performance.now();
    let connection: NatsConnectionHandle | undefined;
    const settled = outcome(openWithReadyFile(relay, file)); // Observe an early first-connect rejection immediately.
    try {
      await waitFor(() => relay.connectionCount > 0, "initial connection attempt", 3_000);
      await delay(Math.max(0, 3_000 - (performance.now() - started)));
      expect(fs.existsSync(file)).toBe(false);

      relay.holdVerification = true;
      relay.enabled = true;
      const progress = await bounded(Promise.race([
        relay.verification.promise.then(() => "USER.INFO" as const),
        settled.then(() => "settled" as const),
      ]), "recovered connection or first-connect failure");
      if (progress === "USER.INFO") {
        // A real USER.INFO request reached the broker, but its response is held at the relay.
        expect(fs.existsSync(file)).toBe(false);
        relay.releaseVerification();
      }

      const result: OpenOutcome = await bounded(settled, "first connection after endpoint recovery");
      expect(result.status).toBe("fulfilled");
      if (result.status !== "fulfilled") return;
      connection = result.value;
      expect(fs.existsSync(file)).toBe(true);
    } finally {
      await connection?.close().catch(() => undefined);
      await relay.stop();
    }
  }, 25_000);

  it("aborts a waiting first connect promptly and makes no later socket after the endpoint returns", async () => {
    const relay = await ReconnectRelay.create(broker!);
    const file = readyFile();
    const controller = new AbortController();
    relay.enabled = false;
    const settled = outcome(openWithReadyFile(relay, file, controller.signal));
    try {
      await waitFor(() => relay.connectionCount > 0, "initial connection attempt", 3_000);
      const wasPending = await Promise.race([settled.then(() => false), delay(150).then(() => true)]);
      expect.soft(wasPending, "initial connection must still be waiting before cancellation").toBe(true);

      const abortedAt = performance.now();
      controller.abort(new Error("first connect cancelled by test"));
      const result: OpenOutcome = await bounded(settled, "aborted first-connect settlement", 1_500);
      expect(result.status).toBe("rejected");
      expect(performance.now() - abortedAt).toBeLessThan(1_500);
      expect(fs.existsSync(file)).toBe(false);

      await bounded(relay.whenIdle(), "aborted first-connect sockets close", 1_500);
      expect(relay.activeConnections).toBe(0);
      const attemptsAtReturn = relay.connectionCount;
      const commandsAtReturn = [...relay.commands];
      relay.enabled = true; // Let a real broker accept any stale retry after cancellation.
      await delay(1_500);
      expect(relay.connectionCount).toBe(attemptsAtReturn);
      expect(relay.activeConnections).toBe(0);
      expect(relay.commands).toEqual(commandsAtReturn);
      expect(fs.existsSync(file)).toBe(false);
    } finally {
      controller.abort();
      await relay.stop();
    }
  }, 12_000);

  it("creates readiness only after verification, removes it on reconnect, and removes it on close", async () => {
    const relay = await ReconnectRelay.create(broker!);
    const file = readyFile();
    let connection: NatsConnectionHandle | undefined;
    let unsubscribe: (() => void) | undefined;
    try {
      const result: OpenOutcome = await bounded(outcome(openWithReadyFile(relay, file)), "initial verified connection");
      expect(result.status).toBe("fulfilled");
      if (result.status !== "fulfilled") return;
      connection = result.value;
      expect.soft(fs.existsSync(file), "ready marker after verified first connect").toBe(true);

      let disconnected!: () => void;
      const disconnect = new Promise<void>((resolve) => { disconnected = resolve; });
      unsubscribe = connection.onStatus((status) => { if (status.type === "disconnect") disconnected(); });
      relay.holdVerification = true;
      relay.disconnect();
      await bounded(disconnect, "native disconnect status");
      await waitFor(() => !fs.existsSync(file), "ready marker removal during reconnect");
      expect(fs.existsSync(file)).toBe(false);

      relay.enabled = true;
      await waitFor(() => relay.subjects.filter((subject) => subject === "$SYS.REQ.USER.INFO").length >= 2, "reconnect USER.INFO request");
      expect(fs.existsSync(file)).toBe(false);
      relay.releaseVerification();
      await waitFor(() => fs.existsSync(file) && connection!.isVerified(), "verified reconnect readiness");
      expect(fs.existsSync(file)).toBe(true);

      unsubscribe();
      unsubscribe = undefined;
      await connection.close();
      connection = undefined;
      expect(fs.existsSync(file)).toBe(false);
      await bounded(relay.whenIdle(), "closed connection has no relay sockets");
    } finally {
      unsubscribe?.();
      await connection?.close().catch(() => undefined);
      await relay.stop();
    }
  }, 25_000);

  it("keeps the readiness marker absent while the real USER.INFO response is held", async () => {
    const relay = await ReconnectRelay.create(broker!);
    const file = readyFile();
    relay.holdVerification = true;
    let connection: NatsConnectionHandle | undefined;
    try {
      const settled = outcome(openWithReadyFile(relay, file));
      await bounded(relay.verification.promise, "real USER.INFO request held by relay");
      expect(relay.subjects).toContain("$SYS.REQ.USER.INFO");
      expect(fs.existsSync(file)).toBe(false);

      const stillWaiting = await Promise.race([settled.then(() => false), delay(150).then(() => true)]);
      expect(stillWaiting).toBe(true);
      relay.releaseVerification();
      const result: OpenOutcome = await bounded(settled, "connection after USER.INFO response");
      expect(result.status).toBe("fulfilled");
      if (result.status !== "fulfilled") return;
      connection = result.value;
      expect(fs.existsSync(file)).toBe(true);
    } finally {
      await connection?.close().catch(() => undefined);
      await relay.stop();
    }
  }, 15_000);
});
