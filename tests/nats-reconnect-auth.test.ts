// Keep reason: reconnect crosses the server-expanded account-scope trust boundary.
// Real nats-server + private-CA DNS TLS only. No mocked USER.INFO, transport, KV, or broker.
import fs from "node:fs";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { connect, credsAuthenticator, nkeys, type NatsConnection } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { describe, expect, it } from "vitest";
import { encodeAccountJwt, encodeOperatorJwt, encodeUserJwt, formatUserCreds } from "../src/mesh/nats-auth/jwt.js";
import { PATH_B_OPERATOR_MACHINE, roleTemplateWithPathB } from "../src/mesh/nats-auth/permissions.js";
import * as connectionModule from "../src/transport/nats/connection.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { ensureTransportConsumer, ensureTransportStream } from "../src/transport/nats/provision.js";
import { TransportUnavailableError, type FabricTransport, type TransportEnvelope } from "../src/transport/types.js";
import { ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportDurableName, transportInboxFilter, transportInboxStream, transportInboxStreamSubject, transportInboxSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, PATH_B_TEST_MESH, resolveNatsBinary, type PathBHarness, type PathBIdentity, type PathBUser } from "./helpers/nats-cluster.js";
import { ReconnectRelay } from "./helpers/nats-reconnect-server.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const HOST = "reconnect-security-runtime";
const EXTRA_GRANT = "reconnect.unexpected.publish";
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
  const accountJwt = (extra: boolean) => encodeAccountJwt({
    name: "reconnect-app", subject: account.getPublicKey(),
    limits: { conn: -1, leaf: -1, subs: -1, data: -1, payload: -1, mem_storage: 0, disk_storage: 4_294_967_296, streams: 64, consumer: 256 },
    signingKeys: [
      { key: agent.getPublicKey(), role: "agent", template: { ...template, pub: { ...template.pub, allow: [...template.pub.allow, ...(extra ? [EXTRA_GRANT] : [])] } } },
      { key: admin.getPublicKey(), role: "maintainer", template: { pub: { allow: [">"], deny: [] }, sub: { allow: ["_INBOX.{{subject()}}.>"] } } },
    ],
  }, signer(operatorSigning), clock);
  const exact = accountJwt(false);
  const changed = accountJwt(true);
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
    second: (): PathBIdentity => { if (!original) throw new Error("first identity was not created"); return { ...original, appAccountJwt: changed }; },
    clear: () => { for (const key of pairs) key.clear(); },
  };
}

async function fixture(withSecond: boolean) {
  const identities = identityFactories();
  const brokers: PathBHarness[] = [];
  const relays: ReconnectRelay[] = [];
  const connections: NatsConnection[] = [];
  const runtimes: FabricTransport[] = [];
  const abort = new AbortController();
  const close = async () => {
    abort.abort();
    await Promise.all(connections.map((nc) => nc.close()));
    await Promise.all(runtimes.map((runtime) => runtime.close()));
    for (const relay of relays) relay.disconnect();
    for (const relay of [...relays].reverse()) await relay.stop();
    for (const broker of [...brokers].reverse()) await broker.stop();
  };
  try {
    const first = await createPathBHarness({ binary: binary!, identityFactory: identities.first, tls: { dnsNames: ["localhost"], ipAddresses: [] } });
    brokers.push(first);
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
    const runtime = await openNatsTransport({ ...first.runtimeOptions(first.users.machineA), servers: relays.map((relay) => relay.url),
      tls: { ca: brokers.map((broker) => fs.readFileSync(broker.tls!.caFile, "utf8")).join("\n") }, hostId: HOST, provision: false, requestTimeoutMs: 2_000 });
    runtimes.push(runtime);
    if (brokers[1]) relays[1]!.routeTo(brokers[1]);
    return { brokers, relays, runtime, raw, abort, close };
  } catch (error) { await close(); throw error; }
  finally { identities.clear(); }
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
      expect(infos[1]!.data.permissions.publish.allow).toEqual(expect.arrayContaining([EXTRA_GRANT]));
      expect(infos[1]!.data.permissions.publish.allow.filter((s) => s !== EXTRA_GRANT).sort()).toEqual([...infos[0]!.data.permissions.publish.allow].sort());
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
