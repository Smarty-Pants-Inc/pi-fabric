// Keep reason: reconnect crosses the server-expanded account-scope trust boundary.
// Real nats-server + private-CA DNS TLS only. No mocked USER.INFO, transport, KV, or broker.
import fs from "node:fs";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { connect, credsAuthenticator, nkeys, type NatsConnection } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { encodeAccountJwt, encodeOperatorJwt, encodeUserJwt, formatUserCreds } from "../src/mesh/nats-auth/jwt.js";
import { PATH_B_OPERATOR_MACHINE, roleTemplateWithPathB } from "../src/mesh/nats-auth/permissions.js";
import * as connectionModule from "../src/transport/nats/connection.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { ensureTransportConsumer, ensureTransportStream } from "../src/transport/nats/provision.js";
import { TransportUnavailableError, type FabricTransport, type FabricTransportOptions, type TransportEnvelope } from "../src/transport/types.js";
import { ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportDurableName, transportInboxFilter, transportInboxStream, transportInboxStreamSubject, transportInboxSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, PATH_B_TEST_MESH, resolveNatsBinary, type PathBHarness, type PathBIdentity, type PathBUser } from "./helpers/nats-cluster.js";
import { ReconnectRelay } from "./helpers/nats-reconnect-server.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const HOST = "reconnect-security-runtime";
const REVOKED_GRANT = "fabric.in.*.*.*.*.{{tag(machine)}}";
const policy = { maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576, duplicateWindowMs: 600_000, replicas: 1 as const };
const dnsUrl = (h: PathBHarness) => `tls://localhost:${new URL(h.serverUrl).port}`;

async function bounded<T>(promise: Promise<T>, label: string, ms = 10_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`timed out: ${label}`)), ms); })]);
  } finally { if (timer) clearTimeout(timer); }
}

/** Same operator/account/signing keys, byte-identical user JWTs; only account scope differs. */
function identityFactories() {
  const operator = nkeys.createOperator();
  const operatorSigning = nkeys.createOperator();
  const system = nkeys.createAccount();
  const account = nkeys.createAccount();
  const agent = nkeys.createAccount();
  const admin = nkeys.createAccount();
  const pairs = [operator, operatorSigning, system, account, agent, admin];
  const clock = { nowSeconds: () => Math.floor(Date.now() / 1_000) };
  const signer = (key: typeof operator) => ({ publicKey: key.getPublicKey(), sign: (bytes: Uint8Array) => key.sign(bytes) });
  const operatorJwt = encodeOperatorJwt({ name: "reconnect-test", signingKeys: [operatorSigning.getPublicKey()], systemAccount: system.getPublicKey() }, signer(operator), clock);
  const systemAccountJwt = encodeAccountJwt({ name: "SYS", subject: system.getPublicKey(), signingKeys: [], limits: { mem_storage: 0, disk_storage: 0 } }, signer(operatorSigning), clock);
  const template = roleTemplateWithPathB("agent", { custodyStream: PATH_B_PERMISSION_POLICY.custodyStream, livenessStream: PATH_B_PERMISSION_POLICY.livenessStream }, PATH_B_MACHINES);
  const accountJwt = (revokeMachineAInbox = false) => encodeAccountJwt({
    name: "reconnect-app", subject: account.getPublicKey(),
    limits: { conn: -1, leaf: -1, subs: -1, data: -1, payload: -1, mem_storage: 0, disk_storage: 4_294_967_296, streams: 64, consumer: 256 },
    signingKeys: [
      { key: agent.getPublicKey(), role: "agent", template: { ...template, pub: { ...template.pub,
        allow: template.pub.allow.filter(subject => !(revokeMachineAInbox && subject === REVOKED_GRANT)) } } },
      { key: admin.getPublicKey(), role: "maintainer", template: { pub: { allow: [">"], deny: [] }, sub: { allow: ["_INBOX.{{subject()}}.>"] } } },
    ],
  }, signer(operatorSigning), clock);
  const exact = accountJwt();
  const revoked = accountJwt(true);
  let original: PathBIdentity | undefined;
  const first = (root: string): PathBIdentity => {
    const user = (machine: PathBUser["machine"], role: "agent" | "maintainer", name: string): PathBUser => {
      const key = nkeys.createUser();
      try {
        const jwt = encodeUserJwt({ name, subject: key.getPublicKey(), issuerAccount: account.getPublicKey(), expiresAt: clock.nowSeconds() + 3_600,
          tags: [`role:${role}`, `machine:${machine}`, ...(role === "agent" ? [`mesh:${PATH_B_TEST_MESH}`] : [])] }, signer(role === "agent" ? agent : admin), clock);
        const credsPath = path.join(root, `${name}.creds`);
        fs.writeFileSync(credsPath, formatUserCreds(jwt, key.getSeed()), { flag: "wx", mode: 0o600 });
        return { machine, publicKey: key.getPublicKey(), credsPath };
      } finally { key.clear(); }
    };
    const bootstrap = user(PATH_B_OPERATOR_MACHINE, "maintainer", "bootstrap");
    original = { operatorJwt, systemAccount: system.getPublicKey(), systemAccountJwt, appAccount: account.getPublicKey(), appAccountJwt: exact,
      users: { machineA: user("machine-a", "agent", "machine-a"), machineB: user("machine-b", "agent", "machine-b"), bootstrap, maintainer: bootstrap } };
    return original;
  };
  return {
    first,
    current: (): PathBIdentity => { if (!original) throw new Error("first identity was not created"); return original; },
    second: (): PathBIdentity => { if (!original) throw new Error("first identity was not created"); return { ...original, appAccountJwt: revoked }; },
    revoked: (): PathBIdentity => { if (!original) throw new Error("first identity was not created"); return { ...original, appAccountJwt: revoked }; },
    clear: () => { for (const key of pairs) key.clear(); },
  };
}

async function prepareFixture(withSecond: boolean) {
  const identities = identityFactories();
  const brokers: PathBHarness[] = [];
  const relays: ReconnectRelay[] = [];
  const connections: NatsConnection[] = [];
  const runtimes: FabricTransport[] = [];
  const openings = new Set<Promise<FabricTransport>>();
  let closing = false;
  const abort = new AbortController();
  const close = async () => {
    if (closing) return;
    closing = true;
    abort.abort();
    try {
      await Promise.all(connections.map((nc) => nc.close()));
      await Promise.all(runtimes.map((runtime) => runtime.close()));
      for (const relay of relays) relay.disconnect();
      // Break held verification first, then join opens; seeded RED must not leak late runtimes.
      await Promise.allSettled([...openings]);
      for (const relay of [...relays].reverse()) await relay.stop();
      for (const broker of [...brokers].reverse()) await broker.stop();
    } finally { identities.clear(); }
  };
  try {
    const first = await createPathBHarness({ binary: binary!, identityFactory: identities.first, tls: { dnsNames: ["localhost"], ipAddresses: [] } });
    brokers.push(first);
    const initialIdentity = identities.current();
    if (withSecond) brokers.push(await createPathBHarness({ binary: binary!, identityFactory: identities.second, tls: { dnsNames: ["localhost"], ipAddresses: [] } }));
    for (const broker of brokers) relays.push(await ReconnectRelay.create(broker));
    // Both configured URLs initially reach the exact scope, regardless of client randomization.
    // Only fresh connections switch to the changed broker after the verified runtime is open.
    relays[1]?.routeTo(first);
    const raw = async (broker: PathBHarness, user: PathBUser) => {
      const nc = await connect({ servers: dnsUrl(broker), tls: { caFile: broker.tls!.caFile }, reconnect: false,
        authenticator: credsAuthenticator(Uint8Array.from(fs.readFileSync(user.credsPath))), inboxPrefix: `_INBOX.${user.publicKey}`, timeout: 2_000 });
      connections.push(nc);
      return nc;
    };
    for (const broker of brokers) {
      const nc = await raw(broker, broker.users.bootstrap);
      const manager = await jetstreamManager(nc);
      await ensureTransportStream(manager, { ...policy, name: ADVISORY_CAPTURE_STREAM, subjects: [transportAdvisoryStreamSubject], maxAgeMs: 30 * 86_400_000, maxMessageBytes: 65_536 });
      for (const machine of PATH_B_MACHINES) await ensureTransportStream(manager, { ...policy, name: transportInboxStream(machine), subjects: [transportInboxStreamSubject(machine)] });
      for (const bucket of ["leases", "presence"] as const) await openNatsKvForMachine(jetstream(nc), bucket, "machine-a", true);
      await ensureTransportConsumer(manager.consumers, { stream: transportInboxStream("machine-a"), durable: transportDurableName(HOST),
        filterSubject: transportInboxFilter("machine-a", HOST), description: JSON.stringify({ fabricHostId: HOST, machine: "machine-a" }), maxAckPending: 128 });
    }
    const trackRuntime = (opening: Promise<FabricTransport>): Promise<FabricTransport> => {
      const tracked = opening.then(async (runtime) => {
        if (closing) await runtime.close();
        else runtimes.push(runtime);
        return runtime;
      });
      openings.add(tracked);
      void tracked.then(() => openings.delete(tracked), () => openings.delete(tracked));
      return tracked;
    };
    return { brokers, relays, raw, abort, close, trackRuntime, identities, initialIdentity };
  } catch (error) { await close(); throw error; }
}

async function fixture(withSecond: boolean, ownerSignal?: AbortSignal) {
  const f = await prepareFixture(withSecond);
  try {
    const first = f.brokers[0]!;
    const options = { ...first.runtimeOptions(first.users.machineA), servers: f.relays.map((relay) => relay.url),
      tls: { ca: f.brokers.map((broker) => fs.readFileSync(broker.tls!.caFile, "utf8")).join("\n") }, hostId: HOST,
      provision: false, requestTimeoutMs: 2_000, ...(ownerSignal === undefined ? {} : { signal: ownerSignal }) };
    const runtime = await f.trackRuntime(openNatsTransport(options));
    if (f.brokers[1]) f.relays[1]!.routeTo(f.brokers[1]);
    return { ...f, runtime };
  } catch (error) { await f.close(); throw error; }
}

// Exercise the shared factory port: nats is a sibling config property, not mesh.nats.
// Only the unused MeshStore dependency is substituted; transport/auth/TLS/broker are real.
function ownerOptions(f: Awaited<ReturnType<typeof prepareFixture>>, signal: AbortSignal, hostId = HOST): FabricTransportOptions {
  const broker = f.brokers[0]!;
  const nats = { ...broker.runtimeOptions(broker.users.machineA), servers: f.relays[0]!.url, replicas: 1 };
  return { config: { ...DEFAULT_FABRIC_CONFIG.mesh, nats }, hostId, mesh: {} as FabricTransportOptions["mesh"], signal };
}

const command = (id: string, machine = "machine-a"): TransportEnvelope => ({ id, kind: "command", to: { machine, hostId: HOST }, targetId: "reconnect-target", operation: "followUp", from: { id: "sender", name: "sender", kind: "main" }, body: { id } });
const outcome = async (fn: () => unknown): Promise<unknown> => { try { await fn(); return undefined; } catch (error) { return error; } };

function expectTypedVerificationError(error: unknown): void {
  expect(error).toBeInstanceOf(connectionModule.NatsAuthorizationError);
}

// Caller retry, not a status poll: exercise the public send path with a finite retry budget.
// Broker verification/reopen is event-driven; backoff is only the caller's missed-ready safety net.
async function sendAfterReopen(runtime: FabricTransport, envelope: TransportEnvelope): Promise<void> {
  for (let attempt = 0; attempt < 30; attempt++) {
    try { await runtime.send(envelope); return; }
    catch (error) { if (!(error instanceof TransportUnavailableError)) throw error; }
    await delay(Math.min(20 * 2 ** attempt, 250));
  }
  throw new Error("transport did not reopen within caller retry budget");
}

describe.skipIf(!binary || process.platform === "win32")("verified transport reopen against real TLS brokers", () => {
  it.each(["explicit", "owner"] as const)("suppresses buffered KV and inbox deliveries after %s close", async (kind) => {
    const owner = new AbortController();
    const f = await fixture(false, owner.signal);
    const watchAbort = new AbortController();
    let watcher: AsyncIterator<unknown> | undefined;
    let inbox: AsyncIterator<unknown> | undefined;
    try {
      const kv = f.runtime.kv("presence");
      await kv.put("machine-a.h.buffer-a", HOST, { n: 1 }, 0);
      await kv.put("machine-a.h.buffer-b", HOST, { n: 2 }, 0);
      watcher = kv.watch("machine-a.h.*", watchAbort.signal)[Symbol.asyncIterator]();
      expect((await bounded(watcher.next(), "live buffered snapshot")).value).toMatchObject({ type: "put", entry: { value: { n: 1 } } });
      await f.runtime.send(command("buffered-inbox-a"));
      await f.runtime.send(command("buffered-inbox-b"));
      const typedInbox = f.runtime.inbox(f.abort.signal)[Symbol.asyncIterator]();
      inbox = typedInbox;
      const first = (await bounded(typedInbox.next(), "live buffered inbox")).value!;
      expect(first.payload.id).toBe("buffered-inbox-a");
      await first.ack();
      if (kind === "owner") owner.abort();
      else await f.runtime.close();
      await bounded(f.relays[0]!.whenIdle(), "closed buffered transport socket");
      expect((await bounded(watcher.next(), "closed buffered KV")).done).toBe(true);
      expect((await bounded(typedInbox.next(), "closed buffered inbox")).done).toBe(true);
      expect(watchAbort.signal.aborted).toBe(false);
      expect(f.abort.signal.aborted).toBe(false);
      const fresh = kv.watch("machine-a.h.*", watchAbort.signal)[Symbol.asyncIterator]();
      expect((await bounded(fresh.next(), "fresh watch after close")).done).toBe(true);
    } finally {
      watchAbort.abort(); f.abort.abort();
      await watcher?.return?.(); await inbox?.return?.();
      await f.close();
    }
  }, 30_000);

  it("refuses an already-aborted factory owner before any socket or client protocol, while a live owner works", async () => {
    const f = await prepareFixture(false);
    try {
      const relay = f.relays[0]!;
      const owner = new AbortController();
      owner.abort();
      const before = relay.connectionCount;
      expect(before).toBe(0);
      expect(relay.activeConnections).toBe(0);
      const refused = await bounded(outcome(() => f.trackRuntime(openNatsTransport(ownerOptions(f, owner.signal)))), "pre-aborted owner refusal", 1_500);
      expect(refused).toBeInstanceOf(Error);
      expect(relay.connectionCount).toBe(before);
      expect(relay.activeConnections).toBe(0);
      expect(relay.subjects).toEqual([]);
      expect(relay.commands).toEqual([]);

      // A counterexample on the identical real endpoint rules out a dead relay/TLS fixture.
      const liveOwner = new AbortController();
      const runtime = await f.trackRuntime(openNatsTransport(ownerOptions(f, liveOwner.signal)));
      const iterator = runtime.inbox(f.abort.signal)[Symbol.asyncIterator]();
      const next = iterator.next();
      await runtime.send(command("live-factory-owner"));
      const received = (await bounded(next, "live owner delivery")).value!;
      expect(received.payload.id).toBe("live-factory-owner");
      await received.ack();
      expect(await runtime.drainState()).toEqual({ pending: 0, ackPending: 0 });
      expect(relay.connectionCount).toBe(before + 1);
      expect(relay.subjects.filter((subject) => subject === "$SYS.REQ.USER.INFO")).toHaveLength(1);
      await iterator.return?.();
      await runtime.close();
      await bounded(relay.whenIdle(), "live owner explicit close", 1_500);
      expect(relay.errors).toEqual([]);
    } finally { await f.close(); }
  }, 90_000);

  it("cancels a factory owner's opening while real USER.INFO is held, promptly closing without provisioning its durable", async () => {
    const f = await prepareFixture(false);
    try {
      const broker = f.brokers[0]!;
      const relay = f.relays[0]!;
      const owner = new AbortController();
      const hostId = `${HOST}-cancelled-opening`;
      relay.holdVerification = true;
      const opening = f.trackRuntime(openNatsTransport(ownerOptions(f, owner.signal, hostId)));
      const settled = outcome(() => opening); // Observe rejection immediately, including before abort.
      await bounded(relay.verification.promise, "opening real USER.INFO request");
      expect(relay.activeConnections).toBe(1);
      expect(relay.subjects).toEqual(["$SYS.REQ.USER.INFO"]);
      const started = performance.now();
      owner.abort();
      const error = await bounded(settled, "owner cancels opening promptly", 1_500);
      expect(error).toBeInstanceOf(Error);
      await bounded(relay.verificationClosed.promise, "cancelled opening socket close", 1_500);
      await bounded(relay.whenIdle(), "cancelled opening has no sockets", 1_500);
      expect(performance.now() - started).toBeLessThan(2_000);
      expect(relay.activeConnections).toBe(0);
      const commandsAtClose = [...relay.commands];
      relay.releaseVerification(); // A late real response cannot publish a runtime or create a consumer.
      const observer = await f.raw(broker, broker.users.bootstrap);
      const manager = await jetstreamManager(observer);
      const consumers = await manager.consumers.list(transportInboxStream("machine-a")).next();
      expect(consumers.some((consumer) => consumer.name === transportDurableName(hostId))).toBe(false);
      expect(relay.connectionCount).toBe(1);
      expect(relay.subjects).toEqual(["$SYS.REQ.USER.INFO"]);
      expect(relay.commands).toEqual(commandsAtClose);
      expect(relay.errors).toEqual([]);
    } finally { await f.close(); }
  }, 90_000);

  it.each([false, true])("owner abort closes an established transport (held reopen: %s), ends a waiting inbox, and stays closed after broker restart", async (heldReopen) => {
    const owner = new AbortController();
    const f = await fixture(false, owner.signal);
    try {
      const broker = f.brokers[0]!;
      const relay = f.relays[0]!;
      const iterator = f.runtime.inbox(f.abort.signal)[Symbol.asyncIterator]();
      const first = iterator.next();
      await f.runtime.send(command("live-direct-owner"));
      const initial = (await bounded(first, "live direct owner delivery")).value!;
      expect(initial.payload.id).toBe("live-direct-owner");
      await initial.ack();
      const kv = f.runtime.kv("presence");
      const pending = iterator.next(); // Its independent inbox signal is deliberately NOT aborted.
      void pending.catch(() => undefined); // Keep an early failure observed until the bounded assertion below.
      if (heldReopen) {
        relay.holdVerification = true;
        relay.disconnect();
        relay.enabled = true;
        await bounded(relay.verification.promise, "owner's held reopen real USER.INFO");
      }
      owner.abort();
      expect((await bounded(pending, "owner abort ends waiting inbox", 1_500)).done).toBe(true);
      expect(f.abort.signal.aborted).toBe(false);
      if (heldReopen) await bounded(relay.verificationClosed.promise, "owner cancels held reopen socket", 1_500);
      await bounded(relay.whenIdle(), "owner abort closes all relay sockets", 1_500);
      expect(relay.activeConnections).toBe(0);
      expect(await outcome(() => f.runtime.send(command("aborted-owner-send")))).toBeInstanceOf(TransportUnavailableError);
      expect(await outcome(() => kv.get("machine-a.h.reconnect"))).toBeInstanceOf(TransportUnavailableError);
      const connectionsAtClose = relay.connectionCount;
      const subjectsAtClose = [...relay.subjects];
      const commandsAtClose = [...relay.commands];
      relay.releaseVerification();
      await broker.restart(); // Actual child, same store; the relay remains reachable throughout.
      const observer = await f.raw(broker, broker.users.bootstrap);
      const incoming = { ...command("after-owner-abort"), createdAt: Date.now() };
      await jetstream(observer).publish(transportInboxSubject("machine-a", HOST, "cmd", incoming.targetId, "machine-b"),
        new TextEncoder().encode(JSON.stringify({ format: 1, sourceMachine: "machine-b", envelope: incoming })));
      await observer.flush();
      // Safety-window assertion, not readiness polling: closure above is native-event driven.
      // 750ms spans the first reopen backoff (100–200ms); any post-close dial/protocol is a failure.
      await delay(750);
      expect(relay.connectionCount).toBe(connectionsAtClose);
      expect(relay.activeConnections).toBe(0);
      expect(relay.subjects).toEqual(subjectsAtClose);
      expect(relay.commands).toEqual(commandsAtClose);
      expect((await bounded(iterator.next(), "closed inbox cannot receive post-abort command", 1_500)).done).toBe(true);
      expect(await outcome(() => f.runtime.send(command("still-aborted-after-restart")))).toBeInstanceOf(TransportUnavailableError);
      expect(await outcome(() => kv.get("machine-a.h.reconnect"))).toBeInstanceOf(TransportUnavailableError);
      const manager = await jetstreamManager(observer);
      const consumer = await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(HOST));
      expect(consumer.delivered.consumer_seq).toBe(1);
      expect(consumer.ack_floor.consumer_seq).toBe(1);
      expect((await manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(2);
      expect(relay.errors).toEqual([]);
    } finally { await f.close(); }
  }, 90_000);

  it("retries a transient USER.INFO timeout on native reconnect and resumes the inbox", async () => {
    const f = await fixture(false);
    const relay = f.relays[0]!;
    let inbox: AsyncIterator<import("../src/transport/types.js").TransportDelivery<TransportEnvelope>> | undefined;
    try {
      inbox = f.runtime.inbox(f.abort.signal)[Symbol.asyncIterator]();
      const initial = inbox.next();
      await f.runtime.send(command("before-transient-verification-timeout"));
      const delivered = (await bounded(initial, "pre-timeout inbox delivery")).value!;
      await delivered.ack();
      const resumedInbox = inbox.next();
      void resumedInbox.catch(() => undefined);

      relay.subjects.length = 0;
      relay.commands.length = 0;
      relay.dropNextVerification = true;
      relay.disconnect();
      relay.enabled = true;
      await bounded(relay.verification.promise, "first reconnect USER.INFO request is dropped", 10_000);
      expect(relay.subjects).toEqual(["$SYS.REQ.USER.INFO"]);
      expect(await outcome(() => f.runtime.send(command("blocked-during-transient-verification"))))
        .toBeInstanceOf(TransportUnavailableError);
      expect(await outcome(() => f.runtime.kv("presence").get("machine-a.h.reconnect-timeout")))
        .toBeInstanceOf(TransportUnavailableError);

      const retry = command("after-transient-verification-timeout");
      await sendAfterReopen(f.runtime, retry);
      const resumed = (await bounded(resumedInbox, "existing inbox resumes after retry verification")).value!;
      expect(resumed.payload.id).toBe(retry.id);
      await resumed.ack();
      expect(relay.subjects.filter((subject) => subject === "$SYS.REQ.USER.INFO")).toHaveLength(2);
      expect(await f.runtime.drainState()).toEqual({ pending: 0, ackPending: 0 });
      expect(relay.errors).toEqual([]);
    } finally {
      f.abort.abort();
      await inbox?.return?.();
      await f.close();
    }
  }, 90_000);

  it("re-verifies changed USER.INFO permissions before native reconnect sends or inbox delivery resume", async () => {
    const f = await fixture(false);
    const broker = f.brokers[0]!;
    const relay = f.relays[0]!;
    let inbox: AsyncIterator<import("../src/transport/types.js").TransportDelivery<TransportEnvelope>> | undefined;
    try {
      inbox = f.runtime.inbox(f.abort.signal)[Symbol.asyncIterator]();
      const first = inbox.next();
      await f.runtime.send(command("before-permission-restart"));
      const delivered = (await bounded(first, "pre-restart authenticated inbox delivery")).value!;
      await delivered.ack();
      const pendingAck = inbox.next();
      await f.runtime.send(command("ack-must-wait-for-reverification"));
      const heldDelivery = (await bounded(pendingAck, "pre-restart delivery retained for gated ACK")).value!;
      let inboxError: unknown;
      let earlyDelivery: unknown;
      const waiting = inbox.next().then((result) => {
        if (!result.done) earlyDelivery = result.value;
      }, (error) => { inboxError = error; });

      relay.holdVerification = true;
      relay.subjects.length = 0;
      relay.commands.length = 0;
      relay.disconnect();
      // Keep the runtime on the disabled relay while the persisted account ACL changes during its outage.
      const updatedIdentity = f.identities.revoked();
      expect(broker.configText.includes(f.initialIdentity.appAccountJwt)).toBe(true);
      const updatedConfig = broker.configText.replace(f.initialIdentity.appAccountJwt, updatedIdentity.appAccountJwt);
      expect(updatedConfig).not.toBe(broker.configText);
      const configPath = path.join(broker.root, "nats.conf");
      fs.writeFileSync(configPath, updatedConfig, { mode: 0o600 });
      fs.chmodSync(configPath, 0o600);
      const resolverJwt = path.join(broker.root, "resolver", `${f.initialIdentity.appAccount}.jwt`);
      fs.writeFileSync(resolverJwt, updatedIdentity.appAccountJwt, { mode: 0o600 });
      fs.chmodSync(resolverJwt, 0o600);
      await bounded(broker.restart(), "same-store broker restart with reduced account grants", 25_000);
      relay.enabled = true;
      await bounded(relay.verification.promise, "reconnect USER.INFO on the real broker", 10_000);

      const outbound = command("must-not-publish-before-reverification");
      expect(await outcome(() => heldDelivery.ack())).toBeInstanceOf(TransportUnavailableError);
      expect(await outcome(() => f.runtime.send(outbound))).toBeInstanceOf(TransportUnavailableError);
      expect(await outcome(() => f.runtime.kv("presence").put("machine-a.h.reconnect-auth", HOST, { forbidden: true }, 0)))
        .toBeInstanceOf(TransportUnavailableError);
      expect(relay.subjects).toEqual(["$SYS.REQ.USER.INFO"]);

      const permissionProbe = await f.raw(broker, broker.users.machineA);
      const livePermissions = (await permissionProbe.request("$SYS.REQ.USER.INFO", new Uint8Array(), { timeout: 2_000 }))
        .json<{ data: { permissions: { publish: { allow: string[] } } } }>().data.permissions.publish.allow;
      expect(livePermissions).not.toContain("fabric.in.*.*.*.*.machine-a");
      await permissionProbe.close();
      const observer = await f.raw(broker, broker.users.bootstrap);
      const manager = await jetstreamManager(observer);
      const incoming = { ...command("unverified-inbound"), createdAt: Date.now() };
      await jetstream(observer).publish(transportInboxSubject("machine-a", HOST, "cmd", incoming.targetId, "machine-b"),
        new TextEncoder().encode(JSON.stringify({ format: 1, sourceMachine: "machine-b", envelope: incoming })));
      await observer.flush();
      expect((await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(HOST))).delivered.stream_seq)
        .toBe(heldDelivery.sequence);
      await delay(100);
      expect(earlyDelivery).toBeUndefined();
      expect(relay.subjects).toEqual(["$SYS.REQ.USER.INFO"]);

      relay.releaseVerification();
      await bounded(relay.verificationClosed.promise, "mismatched USER.INFO connection close", 5_000);
      await bounded(waiting, "terminal inbox authorization failure", 5_000);
      expectTypedVerificationError(inboxError);
      expectTypedVerificationError(await outcome(() => f.runtime.send(outbound)));
      expectTypedVerificationError(await outcome(() => f.runtime.kv("presence").get("machine-a.h.reconnect-auth")));
      expect(relay.subjects).toEqual(["$SYS.REQ.USER.INFO"]);
      expect(relay.errors).toEqual([]);
      const finalConsumer = await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(HOST));
      expect(finalConsumer.delivered.stream_seq).toBe(heldDelivery.sequence);
      expect(finalConsumer.ack_floor.stream_seq).toBe(delivered.sequence);
      await observer.close();
    } finally {
      f.abort.abort();
      await inbox?.return?.();
      await f.close();
    }
  }, 90_000);

  it("refuses a second configured server's changed account scope with no publication or inbound delivery", async () => {
    const f = await fixture(true);
    try {
      const [first, second] = f.brokers as [PathBHarness, PathBHarness];
      const [firstRelay, secondRelay] = f.relays as [ReconnectRelay, ReconnectRelay];
      // Independently prove both brokers authenticate exactly the same credential and expand different ACLs.
      const infos = [];
      for (const broker of [first, second]) {
        const nc = await f.raw(broker, first.users.machineA);
        infos.push((await nc.request("$SYS.REQ.USER.INFO", new Uint8Array(), { timeout: 2_000 })).json<{ data: { user: string; permissions: { publish: { allow: string[] } } } }>());
      }
      expect(infos[0]!.data.user).toBe(first.users.machineA.publicKey);
      expect(infos[1]!.data.user).toBe(infos[0]!.data.user);
      const removedGrant = "fabric.in.*.*.*.*.machine-a";
      expect(infos[1]!.data.permissions.publish.allow).not.toContain(removedGrant);
      expect([...infos[1]!.data.permissions.publish.allow, removedGrant].sort()).toEqual([...infos[0]!.data.permissions.publish.allow].sort());
      const received: unknown[] = [];
      let inboxError: unknown;
      const iterator = f.runtime.inbox(f.abort.signal)[Symbol.asyncIterator]();
      const pending = iterator.next().then((result) => { if (!result.done) received.push(result.value); }, (error) => { inboxError = error; });
      // Every configured endpoint now reaches the second real broker. This removes
      // random server ordering from the security assertion without mocking the client.
      for (const relay of [firstRelay, secondRelay]) {
        relay.holdVerification = true;
        relay.routeTo(second);
        relay.disconnect();
        relay.subjects.length = 0;
        relay.enabled = true;
      }
      const changedRelay = await bounded(Promise.race([firstRelay, secondRelay].map(async (relay) => {
        await relay.verification.promise;
        return relay;
      })), "second broker real USER.INFO request");
      const outbound = command("must-not-escape", "machine-b");
      expect(await outcome(() => f.runtime.send(outbound))).toBeInstanceOf(TransportUnavailableError);
      expect(await outcome(() => f.runtime.kv("presence").put("machine-a.h.reconnect", HOST, { forbidden: true }, 0))).toBeInstanceOf(TransportUnavailableError);
      const observer = await f.raw(second, second.users.bootstrap);
      const manager = await jetstreamManager(observer);
      const incoming = { ...command("unverified-inbound"), createdAt: Date.now() };
      await jetstream(observer).publish(transportInboxSubject("machine-a", HOST, "cmd", incoming.targetId, "machine-b"),
        new TextEncoder().encode(JSON.stringify({ format: 1, sourceMachine: "machine-b", envelope: incoming })));
      await observer.flush();
      expect(received).toEqual([]);
      expect(changedRelay.subjects).toEqual(["$SYS.REQ.USER.INFO"]);
      expect((await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(HOST))).delivered.stream_seq).toBe(0);
      changedRelay.releaseVerification();
      await bounded(changedRelay.verificationClosed.promise, "changed-scope connection closes");
      // The same exact verified-open path must reject with the public typed verification error.
      expectTypedVerificationError(await outcome(() => connectionModule.openNatsConnection({ ...second.runtimeOptions(first.users.machineA), servers: dnsUrl(second) })));
      await bounded(pending, "typed permanent inbox refusal");
      expectTypedVerificationError(inboxError);
      expectTypedVerificationError(await outcome(() => f.runtime.send(outbound)));
      expect((await manager.streams.info(transportInboxStream("machine-b"))).state.messages).toBe(0);
      expect((await manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(1);
      expect((await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(HOST))).delivered.stream_seq).toBe(0);
      expect(received).toEqual([]);
      expect(changedRelay.subjects.every((subject) => subject === "$SYS.REQ.USER.INFO")).toBe(true);
      expect(changedRelay.errors).toEqual([]);
      f.abort.abort();
      await bounded(pending, "inbox abort");
    } finally { await f.close(); }
  }, 90_000);

  it("automatically reopens after a real broker restart, preserving host/durable and exactly-once caller retry plus KV", async () => {
    const f = await fixture(false);
    try {
      const broker = f.brokers[0]!;
      const relay = f.relays[0]!;
      const iterator = f.runtime.inbox(f.abort.signal)[Symbol.asyncIterator]();
      const before = iterator.next();
      await f.runtime.send(command("before-restart"));
      const initial = (await bounded(before, "pre-restart command")).value!;
      expect(initial.payload.id).toBe("before-restart");
      await initial.ack();
      const kv = f.runtime.kv("presence"); // A retained public handle must not keep a dead connection.
      const revision = await kv.put("machine-a.h.reconnect", HOST, { generation: 1 }, 0);
      relay.holdVerification = true;
      relay.disconnect();
      await broker.restart(); // Same resolver, JetStream store, host and durable; actual child restart.
      relay.enabled = true;
      await bounded(relay.verification.promise, "restarted broker real USER.INFO request");
      const retried = command("after-restart-retried");
      expect(await outcome(() => f.runtime.send(retried))).toBeInstanceOf(TransportUnavailableError);
      expect(await outcome(() => kv.get("machine-a.h.reconnect"))).toBeInstanceOf(TransportUnavailableError);
      const after = iterator.next();
      let delivered = false;
      void after.then((result) => { delivered = !result.done; }, () => undefined);
      expect(delivered).toBe(false);
      relay.releaseVerification();
      await sendAfterReopen(f.runtime, retried);
      const resumed = (await bounded(after, "post-restart command on existing iterator")).value!;
      expect(resumed.payload.id).toBe(retried.id);
      await resumed.ack();
      expect(f.runtime.self).toEqual({ machine: "machine-a", hostId: HOST });
      expect((await kv.get<{ generation: number }>("machine-a.h.reconnect"))?.value).toEqual({ generation: 1 });
      const nextRevision = await kv.put("machine-a.h.reconnect", HOST, { generation: 2 }, revision);
      expect(nextRevision).toBeGreaterThan(revision);
      expect((await kv.get<{ generation: number }>("machine-a.h.reconnect"))?.value).toEqual({ generation: 2 });
      expect(await f.runtime.drainState()).toEqual({ pending: 0, ackPending: 0 });
      const observer = await f.raw(broker, broker.users.bootstrap);
      const manager = await jetstreamManager(observer);
      const consumer = await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(HOST));
      expect(consumer.config.durable_name).toBe(transportDurableName(HOST));
      expect(consumer.delivered.consumer_seq).toBe(2); // Detect redelivery as well as duplicate publication.
      expect(consumer.ack_floor.consumer_seq).toBe(2);
      expect((await manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(2);
      expect(relay.subjects.filter((subject) => subject === "$SYS.REQ.USER.INFO")).toHaveLength(2);
      expect(relay.errors).toEqual([]);
      f.abort.abort();
      await iterator.return?.();
    } finally { await f.close(); }
  }, 90_000);
});
