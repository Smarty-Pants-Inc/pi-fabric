// Keep reason: pending first-dial sockets and late DNS continuations cross the owner-cancellation
// boundary; the session_start outage flow cannot cheaply exercise these stalled native phases.
import dns from "node:dns";
import fs from "node:fs";
import path from "node:path";
import { createServer, type AddressInfo, type Socket } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { openBootstrapNatsConnection, openNatsConnection as openConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { deferred, ReconnectRelay } from "./helpers/nats-reconnect-server.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");

async function bounded<T>(promise: Promise<T>, label: string, ms = 5_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out: ${label}`)), ms);
    })]);
  } finally { clearTimeout(timer); }
}
const outcome = <T>(promise: Promise<T>): Promise<T | Error> => promise.catch((cause: unknown) => {
  if (!(cause instanceof Error)) throw new Error("expected Error rejection", { cause });
  return cause;
});

/** A real TCP peer that deliberately never completes INFO or the TLS handshake. */
async function stalledPeer(info: boolean) {
  const accepted = deferred<void>();
  const bytesReceived = deferred<void>();
  const idle = deferred<void>();
  const sockets = new Set<Socket>();
  let attempts = 0;
  const server = createServer(socket => {
    attempts += 1;
    sockets.add(socket);
    socket.on("error", () => undefined);
    socket.on("data", () => bytesReceived.resolve());
    socket.once("close", () => {
      sockets.delete(socket);
      if (sockets.size === 0) idle.resolve();
    });
    if (info) socket.write(`INFO ${JSON.stringify({ server_id: "native-abort-fixture", version: "2.10.24", proto: 1, max_payload: 1_048_576, tls_required: true })}\r\n`);
    accepted.resolve();
  });
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const port = (server.address() as AddressInfo).port;
  return {
    url: `tls://localhost:${port}`,
    port,
    accepted: accepted.promise,
    bytesReceived: bytesReceived.promise,
    idle: idle.promise,
    get attempts() { return attempts; },
    get active() { return sockets.size; },
    close: async () => {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    },
  };
}

let broker: PathBHarness;
let sequence = 0;
const marker = () => path.join(broker.root, `native-ready-${++sequence}`);
// These are deliberate long-lived runtime-style owners. Default one-shot behavior is checked separately.
const openNatsConnection = (options: Parameters<typeof openConnection>[0]) => openConnection({ retryFirstConnect: true, ...options });

describe.skipIf(!binary || process.platform === "win32")("native first-connect socket ownership", () => {
  beforeAll(async () => { broker = await createPathBHarness({ binary: binary!, tls: { dnsNames: ["localhost"], ipAddresses: [] } }); }, 30_000);
  afterAll(async () => { await broker?.stop(); });

  it.each(["INFO", "TLS upgrade", "TLS first"] as const)("aborts a pending %s socket, rejects promptly, and never retries after abort", async phase => {
    const peer = await stalledPeer(phase === "TLS upgrade");
    const owner = new AbortController();
    const file = marker();
    fs.writeFileSync(file, "stale process marker\n");
    const settled = outcome(openNatsConnection({
      ...broker.runtimeOptions(broker.users.machineA), servers: peer.url, signal: owner.signal,
      tls: { caFile: broker.tls!.caFile, ...(phase === "TLS first" ? { handshakeFirst: true } : {}) },
      timeoutMs: 10_000, readyFile: file,
    }));
    try {
      await bounded(peer.accepted, `${phase} TCP accept`);
      if (phase !== "INFO") await bounded(peer.bytesReceived, `${phase} ClientHello`);
      expect(peer.active).toBe(1);
      expect(fs.existsSync(file)).toBe(false);
      expect(await Promise.race([settled.then(() => "settled"), delay(30).then(() => "pending")])).toBe("pending");
      const reason = new Error(`cancel pending ${phase}`);
      owner.abort(reason);
      expect(await bounded(settled, `${phase} abort settlement`, 1_000)).toBe(reason);
      await bounded(peer.idle, `${phase} peer sees socket close`, 1_000);
      expect(peer.active).toBe(0);
      const attemptsAtAbort = peer.attempts;
      // Safety window only; normal completion above is driven by native close events.
      // This spans the first reconnect backoff, with a still-listening real endpoint.
      await delay(1_000);
      expect(peer.attempts).toBe(attemptsAtAbort);
      expect(peer.active).toBe(0);
      expect(fs.existsSync(file)).toBe(false);
    } finally { owner.abort(); await peer.close(); await settled; }
  }, 10_000);

  it("does not let a late DNS result open a socket, while a concurrent independent owner connects", async () => {
    const peer = await stalledPeer(false);
    const owner = new AbortController();
    const enteredDns = deferred<void>();
    let releaseDns!: () => void;
    // Hold exactly this lookup at the OS resolver boundary, not the transport or dial loop.
    // The second owner below uses the unmodified real resolver and authenticated TLS broker.
    const lookup = vi.spyOn(dns, "lookup").mockImplementationOnce(((host: string, _options: unknown,
      callback: (error: NodeJS.ErrnoException | null, addresses: dns.LookupAddress[]) => void) => {
      expect(host).toBe("held-firstconnect.invalid");
      releaseDns = () => callback(null, [{ address: "127.0.0.1", family: 4 }]);
      enteredDns.resolve();
    }) as typeof dns.lookup);
    const file = marker();
    const settled = outcome(openNatsConnection({ ...broker.runtimeOptions(broker.users.machineA),
      servers: `tls://held-firstconnect.invalid:${peer.port}`, signal: owner.signal, readyFile: file }));
    let other: NatsConnectionHandle | undefined;
    try {
      await bounded(enteredDns.promise, "held DNS lookup");
      lookup.mockRestore();
      other = await bounded(openNatsConnection({ ...broker.runtimeOptions(broker.users.machineB), readyFile: marker() }), "independent owner native connect");
      expect(other.isVerified()).toBe(true);
      owner.abort();
      expect(await bounded(settled, "abort during DNS", 1_000)).toBeInstanceOf(Error);
      expect(peer.attempts).toBe(0);
      releaseDns();
      // Let the real native _doDial continuation resume; a closed owner must stop at dial().
      await delay(1_000);
      expect(peer.attempts).toBe(0);
      expect(peer.active).toBe(0);
      expect(fs.existsSync(file)).toBe(false);
      expect(other.isVerified()).toBe(true);
      await other.nc.flush();
    } finally {
      lookup.mockRestore(); owner.abort(); releaseDns?.();
      await other?.close(); await peer.close(); await settled;
    }
  }, 12_000);

  it("does not create a TLS-first socket when a certificate read completes after abort", async () => {
    const peer = await stalledPeer(false);
    const owner = new AbortController();
    const enteredFile = deferred<void>();
    let releaseFile!: () => void;
    const readFile = fs.readFile;
    const read = vi.spyOn(fs, "readFile").mockImplementationOnce(((file: string,
      callback: (error: NodeJS.ErrnoException | null, bytes: Buffer) => void) => {
      expect(file).toBe(broker.tls!.caFile);
      let released = false;
      releaseFile = () => {
        if (released) return;
        released = true;
        readFile(file, callback);
      };
      enteredFile.resolve();
    }) as typeof fs.readFile);
    const file = marker();
    const settled = outcome(openNatsConnection({ ...broker.runtimeOptions(broker.users.machineA),
      servers: peer.url, tls: { caFile: broker.tls!.caFile, handshakeFirst: true }, signal: owner.signal, readyFile: file }));
    try {
      await bounded(enteredFile.promise, "held CA file read");
      read.mockRestore();
      owner.abort();
      expect(await bounded(settled, "abort during CA file read", 1_000)).toBeInstanceOf(Error);
      releaseFile();
      await delay(1_000); // Late native certificate continuation must not create a TLS socket.
      expect(peer.attempts).toBe(0);
      expect(peer.active).toBe(0);
      expect(fs.existsSync(file)).toBe(false);
    } finally { read.mockRestore(); owner.abort(); releaseFile?.(); await peer.close(); await settled; }
  }, 10_000);

  it("reports the actual TCP errno once per failed server attempt without exposing credentials", async () => {
    const peer = await stalledPeer(false);
    await peer.close(); // A closed real loopback listener produces ECONNREFUSED, not a simulated transport error.
    const owner = new AbortController();
    const failed = deferred<void>();
    const lines: string[] = [];
    const warning = vi.spyOn(console, "warn").mockImplementation((line: unknown) => {
      lines.push(String(line)); failed.resolve();
    });
    const file = marker();
    const settled = outcome(openNatsConnection({ ...broker.runtimeOptions(broker.users.machineA),
      servers: peer.url, name: "private-auth-name-must-not-be-logged", signal: owner.signal, readyFile: file }));
    try {
      await bounded(failed.promise, "real TCP refusal diagnostic");
      expect(lines).toHaveLength(1);
      expect(lines[0]).toMatch(/^NATS connection attempt failed phase=TCP code=ECONNREFUSED errno=-?\d+$/);
      expect(lines[0]).not.toContain("private-auth-name");
      expect(lines[0]).not.toContain(broker.users.machineA.credsPath);
      expect(fs.existsSync(file)).toBe(false);
      owner.abort();
      expect(await bounded(settled, "cancel after TCP refusal", 1_000)).toBeInstanceOf(Error);
    } finally { owner.abort(); warning.mockRestore(); await settled; }
  }, 10_000);

  it("keeps simultaneous pending sockets isolated when only one owner aborts", async () => {
    const peers = await Promise.all([stalledPeer(false), stalledPeer(true)]);
    const owners = [new AbortController(), new AbortController()];
    const openings = peers.map((peer, i) => outcome(openNatsConnection({ ...broker.runtimeOptions(broker.users.machineA),
      servers: peer.url, timeoutMs: 10_000, signal: owners[i]!.signal, readyFile: marker() })));
    try {
      await Promise.all(peers.map(peer => bounded(peer.accepted, "simultaneous TCP accept")));
      await bounded(peers[1]!.bytesReceived, "second owner's pending TLS");
      owners[0]!.abort();
      expect(await bounded(openings[0]!, "first owner's abort", 1_000)).toBeInstanceOf(Error);
      await bounded(peers[0]!.idle, "first owner's socket close", 1_000);
      expect(peers[0]!.active).toBe(0);
      expect(peers[1]!.active).toBe(1);
      expect(await Promise.race([openings[1]!.then(() => "settled"), delay(50).then(() => "pending")])).toBe("pending");
      owners[1]!.abort();
      expect(await bounded(openings[1]!, "second owner's abort", 1_000)).toBeInstanceOf(Error);
      await bounded(peers[1]!.idle, "second owner's socket close", 1_000);
    } finally {
      for (const owner of owners) owner.abort();
      await Promise.all(peers.map(peer => peer.close())); await Promise.all(openings);
    }
  }, 10_000);

  it("times out pending raw INFO sockets inside native attempts and reports the actual failure without readiness", async () => {
    const peer = await stalledPeer(false);
    const owner = new AbortController();
    const failed = deferred<void>();
    const lines: string[] = [];
    const warning = vi.spyOn(console, "warn").mockImplementation((line: unknown) => {
      lines.push(String(line)); if (String(line).includes("code=TimeoutError")) failed.resolve();
    });
    const file = marker();
    const settled = outcome(openNatsConnection({ ...broker.runtimeOptions(broker.users.machineA),
      servers: peer.url, timeoutMs: 100, signal: owner.signal, readyFile: file }));
    try {
      await bounded(peer.accepted, "deadline TCP accept");
      await bounded(failed.promise, "native timeout diagnostic");
      await bounded(peer.idle, "native timeout destroys pre-INFO socket");
      expect(lines).toContain("NATS connection attempt failed phase=INFO code=TimeoutError");
      expect(peer.active).toBe(0);
      expect(fs.existsSync(file)).toBe(false);
      expect(await Promise.race([settled.then(() => "settled"), delay(30).then(() => "pending")])).toBe("pending");
      owner.abort();
      expect(await bounded(settled, "cancel native timeout retry", 1_000)).toBeInstanceOf(Error);
    } finally { warning.mockRestore(); owner.abort(); await peer.close(); await settled; }
  }, 10_000);

  it("does not let old re-verification or close overwrite or delete a successor's readiness token", async () => {
    const relay = await ReconnectRelay.create(broker);
    const file = marker();
    let previous: NatsConnectionHandle | undefined;
    let successor: NatsConnectionHandle | undefined;
    try {
      previous = await openNatsConnection({ ...broker.runtimeOptions(broker.users.machineA), servers: relay.url, readyFile: file });
      const oldToken = fs.readFileSync(file, "utf8");
      relay.holdVerification = true;
      relay.disconnect(); relay.enabled = true;
      await bounded(relay.verification.promise, "old owner's held re-verification", 10_000);
      expect(fs.existsSync(file)).toBe(false);
      successor = await openNatsConnection({ ...broker.runtimeOptions(broker.users.machineA), readyFile: file });
      const newToken = fs.readFileSync(file, "utf8");
      expect(newToken).not.toBe(oldToken);
      const reverified = deferred<void>();
      previous.onStatus(status => { if (status.type === "reconnect") reverified.resolve(); });
      relay.releaseVerification();
      await bounded(reverified.promise, "old owner's re-verification finishes");
      expect(fs.readFileSync(file, "utf8")).toBe(newToken);
      await previous.close(); previous = undefined;
      expect(fs.readFileSync(file, "utf8")).toBe(newToken);
      await successor.close(); successor = undefined;
      expect(fs.existsSync(file)).toBe(false);
    } finally { await previous?.close(); await successor?.close(); await relay.stop(); }
  }, 20_000);

  it("requires a fresh re-verification after another disconnect supersedes a held USER.INFO", async () => {
    const relay = await ReconnectRelay.create(broker);
    const file = marker();
    let connection: NatsConnectionHandle | undefined;
    try {
      connection = await openNatsConnection({ ...broker.runtimeOptions(broker.users.machineA), servers: relay.url, readyFile: file, timeoutMs: 500 });
      const reverified = deferred<void>();
      connection.onStatus(status => { if (status.type === "reconnect") reverified.resolve(); });
      relay.holdVerification = true;
      relay.disconnect(); relay.enabled = true;
      await bounded(relay.verification.promise, "held first re-verification", 10_000);
      expect(fs.existsSync(file)).toBe(false);
      relay.disconnect(); relay.enabled = true;
      relay.releaseVerification();
      expect(fs.existsSync(file)).toBe(false);
      await bounded(reverified.promise, "fresh native connection re-verifies after superseded request", 10_000);
      expect(relay.subjects.filter(subject => subject === "$SYS.REQ.USER.INFO")).toHaveLength(3);
      expect(connection.isVerified()).toBe(true);
      expect(fs.existsSync(file)).toBe(true);
    } finally { await connection?.close(); await relay.stop(); }
  }, 25_000);

  it.each(["bootstrap", "verified"] as const)("fails fast by default for an admin-style %s open without the private CA", async kind => {
    const file = marker();
    const owner = new AbortController();
    const opening = kind === "bootstrap"
      ? openBootstrapNatsConnection({ ...broker.runtimeOptions(broker.users.bootstrap), tls: {}, readyFile: file, timeoutMs: 5_000, signal: owner.signal })
      : openConnection({ ...broker.runtimeOptions(broker.users.machineA), tls: {}, readyFile: file, timeoutMs: 5_000, signal: owner.signal });
    const settled = outcome(opening);
    try {
      const result = await bounded(settled, "default admin-style certificate refusal", 1_500);
      expect(result).toBeInstanceOf(Error);
      expect((result as Error).cause).toMatchObject({ code: expect.stringMatching(/^(UNABLE_TO_VERIFY_LEAF_SIGNATURE|SELF_SIGNED_CERT_IN_CHAIN|UNABLE_TO_GET_ISSUER_CERT_LOCALLY)$/) });
      expect(fs.existsSync(file)).toBe(false);
    } finally { owner.abort(); await settled; }
  });

  it("never writes or clears a runtime marker for an auxiliary bootstrap connection", async () => {
    const file = marker();
    fs.writeFileSync(file, "canonical-runtime-owner\n");
    const auxiliary = await openBootstrapNatsConnection({ ...broker.runtimeOptions(broker.users.bootstrap), readyFile: file });
    try { expect(fs.readFileSync(file, "utf8")).toBe("canonical-runtime-owner\n"); }
    finally { await auxiliary.close(); }
    expect(fs.readFileSync(file, "utf8")).toBe("canonical-runtime-owner\n");
  });
});
