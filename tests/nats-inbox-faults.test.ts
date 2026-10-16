// Real Path B data-safety/trust-boundary regressions. No mocked broker, shortened AckWait,
// consumer deletion, product credentials, or destructive operator operation.
import { Buffer } from "node:buffer";
import fs from "node:fs";
import { createConnection, createServer, type AddressInfo, type Socket } from "node:net";
import { connect as tlsConnect, createSecureContext, TLSSocket } from "node:tls";
import { setTimeout as delay } from "node:timers/promises";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { openNatsTransport, type NatsTransportOptions } from "../src/transport/nats/inbox.js";
import { ensureTransportStream } from "../src/transport/nats/provision.js";
import { openNatsKvForMachine } from "../src/transport/nats/kv.js";
import { transportDurableName, transportInboxStream, transportInboxStreamSubject } from "../src/transport/nats/subjects.js";
import {
  TransportCapacityError, TransportInvalidError, TransportUncertainError,
  type FabricTransport, type TransportDelivery, type TransportEnvelope,
} from "../src/transport/types.js";
import { createPathBHarness, resolveNatsBinary, type PathBHarness, type PathBTlsFiles, type PathBUser } from "./helpers/nats-cluster.js";

const MAX_BYTES = 1_073_741_824;
const MAX_MESSAGE_BYTES = 1_048_576;
const ACK_WAIT_MS = 30_000;
const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");

// Every delivery/termination wait has a deadline; timers are only safety nets. Normal
// evidence comes from actual NATS frames, delivered messages and consumer/stream info.
async function bounded<T>(promise: Promise<T>, label: string, ms = 7_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    })]);
  } finally { clearTimeout(timer); }
}

interface Frame { readonly line: string; readonly subject: string; readonly reply?: string; readonly body: Buffer }

/** Transparent loopback fault injector: frame boundaries, not chunk boundaries. Authentication
 * and all stream/consumer behavior remain the real server's. It never logs credential frames. */
class WireFaults {
  readonly sockets = new Set<Socket>();
  readonly pullBatches: number[] = [];
  ackDrops = 0;
  pubAckDrops = 0;
  publishes = 0;
  progress = 0;
  private dropAck = false;
  private losePubAck = false;
  private readonly lostReplies = new Set<string>();
  private readonly listener = createServer((client) => this.accept(client));
  private constructor(private readonly upstream: URL, private readonly tls: PathBTlsFiles) {}
  url = "";

  static async open(serverUrl: string, tls: PathBTlsFiles): Promise<WireFaults> {
    const proxy = new WireFaults(new URL(serverUrl), tls);
    await new Promise<void>((resolve, reject) => {
      proxy.listener.once("error", reject);
      proxy.listener.listen(0, "127.0.0.1", resolve);
    });
    proxy.url = `tls://localhost:${(proxy.listener.address() as AddressInfo).port}`;
    return proxy;
  }

  private track<T extends Socket>(socket: T): T {
    this.sockets.add(socket);
    socket.on("error", () => undefined);
    socket.once("close", () => this.sockets.delete(socket));
    return socket;
  }

  private accept(client: Socket): void {
    this.track(client);
    const upstream = this.track(createConnection({ host: this.upstream.hostname, port: Number(this.upstream.port) }));
    client.once("close", () => upstream.destroy());
    upstream.once("close", () => client.destroy());
    let info = Buffer.alloc(0);
    const readInfo = (chunk: Buffer): void => {
      info = Buffer.concat([info, chunk]);
      const end = info.indexOf("\r\n");
      if (end < 0) return;
      upstream.removeListener("data", readInfo);
      if (!info.subarray(0, end).toString().startsWith("INFO ") || info.length !== end + 2) {
        client.destroy(); upstream.destroy(); return;
      }
      client.write(info);
      const secureClient = this.track(new TLSSocket(client, { isServer: true, secureContext: createSecureContext({
        cert: fs.readFileSync(this.tls.certFile), key: fs.readFileSync(this.tls.keyFile),
      }) }));
      const secureUpstream = this.track(tlsConnect({ socket: upstream, servername: "localhost", ca: fs.readFileSync(this.tls.caFile), rejectUnauthorized: true }));
      secureClient.once("close", () => secureUpstream.destroy());
      secureUpstream.once("close", () => secureClient.destroy());
      this.pipe(secureClient, secureUpstream, true);
      this.pipe(secureUpstream, secureClient, false);
    };
    upstream.on("data", readInfo);
  }

  loseNextAck(): void { this.dropAck = true; }
  loseNextPubAck(): void { this.losePubAck = true; }

  private forward(frame: Frame, outbound: boolean): boolean {
    if (outbound) {
      if (frame.subject.startsWith("$JS.API.CONSUMER.MSG.NEXT.")) {
        this.pullBatches.push((JSON.parse(frame.body.toString()) as { batch: number }).batch);
      }
      if (frame.subject.startsWith("$JS.ACK.")) {
        if (frame.body.toString() === "+WPI") this.progress += 1;
        if (frame.body.toString() === "+ACK" && this.dropAck) {
          this.dropAck = false; this.ackDrops += 1; return false;
        }
      }
      if (frame.subject.startsWith("fabric.in.")) {
        this.publishes += 1;
        if (this.losePubAck && frame.reply) {
          this.losePubAck = false; this.lostReplies.add(frame.reply);
        }
      }
    } else if (this.lostReplies.delete(frame.subject)) {
      this.pubAckDrops += 1; return false;
    }
    return true;
  }

  private pipe(source: Socket, destination: Socket, outbound: boolean): void {
    let pending = Buffer.alloc(0);
    source.on("data", (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length) {
        const end = pending.indexOf("\r\n");
        if (end < 0) return;
        const line = pending.subarray(0, end).toString();
        const tokens = line.split(/ +/);
        const command = tokens[0];
        const payload = ["PUB", "HPUB", "MSG", "HMSG"].includes(command ?? "");
        const length = payload ? Number(tokens.at(-1)) : 0;
        const frameLength = end + 2 + (payload ? length + 2 : 0);
        if (pending.length < frameLength) return;
        const bytes = pending.subarray(0, frameLength);
        pending = pending.subarray(frameLength);
        const headers = command === "HPUB" || command === "HMSG" ? Number(tokens.at(-2)) : 0;
        const replyIndex = outbound ? 2 : 3;
        const hasReply = tokens.length === (outbound ? (headers ? 5 : 4) : (headers ? 6 : 5));
        const frame: Frame = { line, subject: tokens[1] ?? "", ...(hasReply ? { reply: tokens[replyIndex] } : {}),
          body: bytes.subarray(end + 2 + headers, end + 2 + length) };
        if (this.forward(frame, outbound)) destination.write(bytes);
      }
    });
  }

  async close(): Promise<void> {
    this.sockets.forEach((socket) => socket.destroy());
    await new Promise<void>((resolve, reject) => this.listener.close((error) => error ? reject(error) : resolve()));
  }
}

let fixture: PathBHarness;
const transports: FabricTransport[] = [];
const proxies: WireFaults[] = [];
let inspector: NatsConnectionHandle;

async function open(user: PathBUser, hostId: string, overrides: Partial<NatsTransportOptions> = {}): Promise<FabricTransport> {
  const transport = await openNatsTransport({ ...fixture.runtimeOptions(user), hostId,
    requestTimeoutMs: 1_500, ...overrides });
  transports.push(transport);
  return transport;
}

function envelope(sender: FabricTransport, receiver: FabricTransport, id: string, body: unknown = { text: id }): TransportEnvelope {
  return { id, kind: "command", to: receiver.self, targetId: `target:${id}`, operation: "followUp",
    from: { id: sender.self.hostId, name: "fault-test", kind: "main" }, body };
}

type InboxIterator = AsyncIterator<TransportDelivery<TransportEnvelope>>;
async function next(iterator: InboxIterator, ms = 7_000): Promise<TransportDelivery<TransportEnvelope>> {
  const result = await bounded(iterator.next(), "real inbox delivery", ms);
  expect(result.done).toBe(false);
  if (result.done || !result.value) throw new Error("inbox ended before delivery");
  return result.value;
}

async function consumerInfo(receiver: FabricTransport) {
  return inspector.manager.consumers.info(transportInboxStream(receiver.self.machine), transportDurableName(receiver.self.hostId));
}

async function pendingIs(receiver: FabricTransport, ackPending: number, pending = 0): Promise<void> {
  expect(await receiver.drainState()).toEqual({ pending, ackPending });
  const info = await consumerInfo(receiver);
  expect({ pending: info.num_pending, ackPending: info.num_ack_pending }).toEqual({ pending, ackPending });
}

describe.skipIf(!binary)("real machine-authenticated inbox faults", () => {
  beforeAll(async () => {
    fixture = await createPathBHarness({ binary: binary!, tls: {} });
    expect(fixture.configText).toContain("sync_interval: always");
    const bootstrap = await fixture.connect(fixture.users.bootstrap);
    const { jetstream, jetstreamManager } = await import("@nats-io/jetstream");
    const manager = await jetstreamManager(bootstrap);
    const js = jetstream(bootstrap);
    // Bootstrap only seeds resources. All product send/pull/ACK/drain calls below use
    // the unmodified, real machine credentials, exact policy and strict agent verification.
    await ensureTransportStream(manager, { name: "FABRIC_ADV", subjects: ["$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"],
      maxBytes: 64 * 1024 * 1024, maxAgeMs: 30 * 24 * 60 * 60_000, maxMessageBytes: 64 * 1024, duplicateWindowMs: 600_000 });
    await openNatsKvForMachine(js, "leases", "machine-a");
    await openNatsKvForMachine(js, "presence", "machine-a");
    await Promise.all([fixture.users.machineA, fixture.users.machineB].map((user) => ensureTransportStream(manager, {
      name: transportInboxStream(user.machine), subjects: [transportInboxStreamSubject(user.machine)],
      maxBytes: MAX_BYTES, maxAgeMs: 0, maxMessageBytes: MAX_MESSAGE_BYTES, duplicateWindowMs: 600_000, replicas: 1,
    })));
    inspector = await openNatsConnection(fixture.operatorOptions());
  }, 60_000);

  afterEach(async () => {
    await bounded(Promise.all(transports.splice(0).map((transport) => transport.close())), "transport cleanup");
    await Promise.all(proxies.splice(0).map((proxy) => proxy.close()));
  }, 15_000);

  afterAll(async () => {
    await inspector?.close();
    await fixture?.stop();
  }, 15_000);

  it("close finishes with two suspended unacked deliveries after abort; exact durable reopens and really redelivers after 30s", async () => {
    const sender = await open(fixture.users.machineA, "close-sender");
    const receiver = await open(fixture.users.machineB, "close-receiver");
    const signal = new AbortController();
    const iterator = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
    const sent = await sender.send(envelope(sender, receiver, "unacked-across-close"));
    const sentOther = await sender.send(envelope(sender, receiver, "another-unacked-across-close"));
    const first = await next(iterator);
    const firstAt = Date.now();
    const heldOther = await next(iterator);
    expect(first).toMatchObject({ attempt: 1, sequence: sent.sequence, sourceMachine: "machine-a" });
    expect(heldOther).toMatchObject({ attempt: 1, sequence: sentOther.sequence });
    await pendingIs(receiver, 2);
    const before = await consumerInfo(receiver);
    expect(before.config).toMatchObject({ ack_policy: "explicit", ack_wait: 30_000_000_000, max_deliver: 5,
      max_ack_pending: 128, max_waiting: 1, deliver_policy: "all" });
    signal.abort();
    await bounded(receiver.close(), "close with two in-hand deliveries", 2_000);
    expect((await bounded(iterator.next(), "closed iterator")).done).toBe(true);
    const reopened = await open(fixture.users.machineB, receiver.self.hostId);
    const after = await consumerInfo(reopened);
    expect(after.name).toBe(before.name);
    expect(after.created).toBe(before.created);
    await pendingIs(reopened, 2);
    const resumedSignal = new AbortController();
    const resumed = reopened.inbox(resumedSignal.signal)[Symbol.asyncIterator]();
    const second = await next(resumed, ACK_WAIT_MS + 10_000);
    expect(Date.now() - firstAt).toBeGreaterThanOrEqual(ACK_WAIT_MS - 1_000);
    expect(second).toMatchObject({ sequence: first.sequence, attempt: 2, payload: first.payload });
    await second.ack();
    const redeliveredOther = await next(resumed);
    expect(redeliveredOther).toMatchObject({ sequence: heldOther.sequence, attempt: 2, payload: heldOther.payload });
    await redeliveredOther.ack();
    await pendingIs(reopened, 0);
    resumedSignal.abort();
    expect((await bounded(resumed.next(), "abort resumed iterator")).done).toBe(true);
  }, 50_000);

  it("drops an actual outgoing broker ACK (not a NAK or omitted ACK), times out and redelivers", async () => {
    const proxy = await WireFaults.open(fixture.serverUrl, fixture.tls!); proxies.push(proxy);
    const sender = await open(fixture.users.machineA, "lost-ack-sender");
    const receiver = await open(fixture.users.machineB, "lost-ack-receiver", { servers: proxy.url });
    const signal = new AbortController();
    const iterator = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
    await sender.send(envelope(sender, receiver, "actually-lost-ack"));
    const first = await next(iterator);
    const firstAt = Date.now();
    proxy.loseNextAck();
    await expect(bounded(first.ack(), "lost ACK timeout")).rejects.toThrow();
    expect(proxy.ackDrops).toBe(1);
    await pendingIs(receiver, 1);
    const second = await next(iterator, ACK_WAIT_MS + 10_000);
    expect(Date.now() - firstAt).toBeGreaterThanOrEqual(ACK_WAIT_MS - 1_000);
    expect(second).toMatchObject({ sequence: first.sequence, attempt: 2, payload: { id: "actually-lost-ack" } });
    await second.ack();
    await pendingIs(receiver, 0);
    expect(proxy.pullBatches.length).toBeGreaterThan(0);
    expect(proxy.pullBatches.every((batch) => batch === 16)).toBe(true);
    signal.abort();
    expect((await bounded(iterator.next(), "lost ACK iterator end")).done).toBe(true);
  }, 50_000);

  it("restarts the owned server with durable backlog, then consumes and confirms ACKs without losing Limits records", async () => {
    const sender = await open(fixture.users.machineA, "restart-sender");
    const receiver = await open(fixture.users.machineB, "restart-receiver");
    const a = await sender.send(envelope(sender, receiver, "restart-backlog-a"));
    const b = await sender.send(envelope(sender, receiver, "restart-backlog-b"));
    await pendingIs(receiver, 0, 2);
    const before = await consumerInfo(receiver);
    await Promise.all([sender.close(), receiver.close(), inspector.close()]);
    await fixture.restart();
    inspector = await openNatsConnection(fixture.operatorOptions());
    const reopened = await open(fixture.users.machineB, "restart-receiver");
    const rebound = await consumerInfo(reopened);
    expect(rebound.name).toBe(before.name);
    expect(rebound.config).toEqual(before.config);
    // nats-server 2.11's sub-ms restore rounding can cross a JS millisecond boundary.
    // Allow only that 1ms precision loss; name/config/backlog/stream sequences still persist.
    expect(Math.abs(Date.parse(rebound.created) - Date.parse(before.created))).toBeLessThanOrEqual(1);
    await pendingIs(reopened, 0, 2);
    const signal = new AbortController();
    const iterator = reopened.inbox(signal.signal)[Symbol.asyncIterator]();
    const first = await next(iterator); await first.ack();
    const second = await next(iterator); await second.ack();
    expect([first.sequence, second.sequence]).toEqual([a.sequence, b.sequence]);
    await pendingIs(reopened, 0);
    const stream = await inspector.manager.streams.info(transportInboxStream("machine-b"));
    expect(stream.config).toMatchObject({ retention: "limits", storage: "file", discard: "new", num_replicas: 1,
      max_age: 0, max_bytes: MAX_BYTES, max_msg_size: MAX_MESSAGE_BYTES, allow_direct: false });
    // E19's report-only inspector cannot read private bodies. Bootstrap verifies
    // retained Limits records independently; no product operation uses this authority.
    const { jetstreamManager } = await import("@nats-io/jetstream");
    const bootstrap = await jetstreamManager(await fixture.connect(fixture.users.bootstrap));
    expect((await bootstrap.streams.getMessage(stream.config.name, { seq: a.sequence }))?.seq).toBe(a.sequence);
    expect((await bootstrap.streams.getMessage(stream.config.name, { seq: b.sequence }))?.seq).toBe(b.sequence);
    signal.abort();
    expect((await bounded(iterator.next(), "restart iterator end")).done).toBe(true);
  }, 40_000);

  it("working() extends real AckWait; no delivery at the original timeout, then redelivery when progress stops", async () => {
    const proxy = await WireFaults.open(fixture.serverUrl, fixture.tls!); proxies.push(proxy);
    const sender = await open(fixture.users.machineA, "working-sender");
    const receiver = await open(fixture.users.machineB, "working-receiver", { servers: proxy.url });
    const signal = new AbortController();
    const iterator = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
    await sender.send(envelope(sender, receiver, "working-long-admission"));
    const first = await next(iterator);
    const startedAt = Date.now();
    let premature = false;
    const redelivery = iterator.next().then((result) => { premature = true; return result; });
    // Normal production progress is every 10s. Here the held admission is the event source;
    // real elapsed deadlines intentionally cannot be replaced by fake timers or NAKs.
    await delay(10_000); first.working();
    await delay(10_000); first.working();
    await delay(10_000); first.working();
    const lastProgressAt = Date.now();
    await delay(4_000);
    expect(Date.now() - startedAt).toBeGreaterThan(ACK_WAIT_MS);
    expect(premature).toBe(false);
    expect(proxy.progress).toBe(3);
    const current = await consumerInfo(receiver);
    expect(current.delivered.consumer_seq).toBe(1);
    expect(current.num_ack_pending).toBe(1);
    const result = await bounded(redelivery, "post-working actual redelivery", ACK_WAIT_MS + 8_000);
    expect(Date.now() - lastProgressAt).toBeGreaterThanOrEqual(ACK_WAIT_MS - 1_000);
    expect(result.value).toMatchObject({ attempt: 2, sequence: first.sequence });
    await result.value!.ack();
    await pendingIs(receiver, 0);
    signal.abort();
    expect((await bounded(iterator.next(), "working iterator end")).done).toBe(true);
  }, 80_000);

  it("close ends an outstanding idle next(), and a second live iterator is rejected without stealing the durable", async () => {
    const receiver = await open(fixture.users.machineB, "idle-receiver");
    const signal = new AbortController();
    const iterator = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
    const waiting = iterator.next();
    const other = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
    await expect(other.next()).rejects.toBeInstanceOf(TransportInvalidError);
    await expect.poll(async () => (await consumerInfo(receiver)).num_waiting, { timeout: 3_000, interval: 25 }).toBe(1);
    await pendingIs(receiver, 0);
    await bounded(receiver.close(), "close live idle inbox");
    expect((await bounded(waiting, "idle next ends on close")).done).toBe(true);
    expect((await consumerInfo(receiver)).num_ack_pending).toBe(0);
  }, 15_000);

  it("enforces E14/E16's operator USER.INFO grant boundary (legacy broad fixture is explicitly refused)", async () => {
    const checking = openNatsConnection(fixture.operatorOptions());
    if (!/^fabric-ops(?:-[a-z0-9]+)?$/.test(fixture.users.maintainer.machine)) {
      // This worktree predates the parent-owned E14 helper change. It must not turn the
      // legacy broad ACK grant into an exception to the new verification boundary.
      await expect(checking).rejects.toThrow("machine tag mismatch");
    } else {
      const operator = await checking;
      expect(operator.userInfo).toBeDefined();
      await operator.close();
    }
  }, 10_000);

  it("refuses an existing durable with another owner description on open and on first pull, without rewriting it", async () => {
    const receiver = await open(fixture.users.machineB, "description-drift");
    const sender = await open(fixture.users.machineA, "description-drift-sender");
    await sender.send(envelope(sender, receiver, "description-drift-backlog"));
    const before = await consumerInfo(receiver);
    const bootstrap = await fixture.connect(fixture.users.bootstrap);
    const { jetstreamManager } = await import("@nats-io/jetstream");
    const admin = await jetstreamManager(bootstrap);
    const wrongDescription = JSON.stringify({ fabricHostId: "another-runtime", machine: "machine-b" });
    await admin.consumers.update(transportInboxStream("machine-b"), before.name, { description: wrongDescription });
    const iterator = receiver.inbox(new AbortController().signal)[Symbol.asyncIterator]();
    await expect(iterator.next()).rejects.toThrow(/description|owner/);
    await receiver.close();
    await expect(open(fixture.users.machineB, "description-drift")).rejects.toThrow(/description|owner/);
    const after = await inspector.manager.consumers.info(transportInboxStream("machine-b"), before.name);
    expect(after.config.description).toBe(wrongDescription);
    expect(after.config.filter_subject).toBe(before.config.filter_subject);
    expect(after.created).toBe(before.created);
    expect(after.num_pending).toBe(1);
    expect(after.num_ack_pending).toBe(0);
  }, 15_000);

  it("lost PubAck is uncertain with the exact frozen body; no blind retry, same-id retry dedupes", async () => {
    const proxy = await WireFaults.open(fixture.serverUrl, fixture.tls!); proxies.push(proxy);
    const sender = await open(fixture.users.machineA, "uncertain-sender", { servers: proxy.url });
    const receiver = await open(fixture.users.machineB, "uncertain-receiver");
    const body = { text: "frozen-before-wire" };
    let stamps = 0;
    const input = envelope(sender, receiver, "lost-puback", () => { stamps += 1; return body; });
    proxy.loseNextPubAck();
    const sending = sender.send(input).catch((error: unknown) => error);
    body.text = "mutated-after-publish";
    const error = await bounded(sending, "lost PubAck uncertainty");
    expect(error).toBeInstanceOf(TransportUncertainError);
    expect(error).toMatchObject({ id: input.id, resolvedBody: { text: "frozen-before-wire" } });
    expect(stamps).toBe(1);
    expect(proxy.pubAckDrops).toBe(1);
    expect(proxy.publishes).toBe(1);
    await pendingIs(receiver, 0, 1);
    const retry = await sender.send({ ...input, body: (error as TransportUncertainError).resolvedBody });
    expect(retry.duplicate).toBe(true);
    expect(proxy.publishes).toBe(2);
    const signal = new AbortController();
    const iterator = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
    const delivered = await next(iterator);
    expect(delivered).toMatchObject({ sequence: retry.sequence, payload: { body: { text: "frozen-before-wire" } } });
    await delivered.ack();
    await pendingIs(receiver, 0);
    signal.abort();
    expect((await bounded(iterator.next(), "uncertain iterator end")).done).toBe(true);
  }, 20_000);

  it("rejects header-unstable IDs and unserializable bodies before storage while preserving original stamp throws", async () => {
    const sender = await open(fixture.users.machineA, "validation-sender");
    const receiver = await open(fixture.users.machineB, "validation-receiver");
    const initial = (await inspector.manager.streams.info(transportInboxStream("machine-b"))).state.last_seq;
    const ids = ["", " leading", "trailing ", "\ttrim", "newline\n", "carriage\rreturn", "nul\0id", "del\x7f", "\u00a0trim"];
    await Promise.all(ids.map((id) => expect(sender.send(envelope(sender, receiver, id))).rejects.toBeInstanceOf(TransportInvalidError)));
    const cyclic: { self?: unknown } = {}; cyclic.self = cyclic;
    await expect(sender.send(envelope(sender, receiver, "json-cycle", cyclic))).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID", persisted: false });
    await expect(sender.send(envelope(sender, receiver, "json-bigint", 1n))).rejects.toBeInstanceOf(TransportInvalidError);
    const fenceError = new Error("original synchronous incarnation fence refusal");
    await expect(sender.send(envelope(sender, receiver, "fence-throw", () => { throw fenceError; }))).rejects.toBe(fenceError);
    expect((await inspector.manager.streams.info(transportInboxStream("machine-b"))).state.last_seq).toBe(initial);
    const opaque = "completion:run/opaque id.with spaces:☃";
    const sent = await sender.send(envelope(sender, receiver, opaque));
    expect((await sender.send(envelope(sender, receiver, opaque))).duplicate).toBe(true);
    const signal = new AbortController();
    const iterator = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
    const delivered = await next(iterator);
    expect(delivered).toMatchObject({ sequence: sent.sequence, payload: { id: opaque } });
    await delivered.ack();
    await pendingIs(receiver, 0);
    signal.abort();
    expect((await bounded(iterator.next(), "validation iterator end")).done).toBe(true);
  }, 20_000);

  it("returns typed oversize/capacity refusals and refuses a report-only operator as a runtime", async () => {
    const sender = await open(fixture.users.machineA, "refusal-sender");
    const receiver = await open(fixture.users.machineB, "refusal-receiver");
    const initial = (await inspector.manager.streams.info(transportInboxStream("machine-b"))).state.last_seq;
    await expect(sender.send(envelope(sender, receiver, "oversize-local", "x".repeat(MAX_MESSAGE_BYTES))))
      .rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID", persisted: false });
    // E19 removed even KV read authority from the operator. An unknown verifyMachine:false
    // property cannot turn that report-only credential into a runtime: the strict agent
    // check refuses it before any private command publish (no runtime bypass remains).
    const prepared = await open(fixture.users.machineA, "denied-publisher");
    await prepared.close();
    await expect(open(fixture.users.maintainer, "denied-publisher", { machine: "machine-a", verifyMachine: false } as Partial<NatsTransportOptions>))
      .rejects.toThrow();
    expect((await inspector.manager.streams.info(transportInboxStream("machine-b"))).state.last_seq).toBe(initial);

    // Keep every frozen stream property. Capacity is exhausted by real file-backed bytes,
    // with a smaller finite max_bytes on this test-only stream (not a max_msgs shortcut).
    const bootstrap = await fixture.connect(fixture.users.bootstrap);
    const { jetstreamManager } = await import("@nats-io/jetstream");
    const admin = await jetstreamManager(bootstrap);
    const initialBytes = (await admin.streams.info(transportInboxStream("machine-b"))).state.bytes;
    await admin.streams.update(transportInboxStream("machine-b"), { max_bytes: initialBytes + 1_200, max_msg_size: 1_024 });
    try {
      await expect(sender.send(envelope(sender, receiver, "oversize-broker", "x".repeat(1_100))))
        .rejects.toBeInstanceOf(TransportInvalidError);
      const accepted = await sender.send(envelope(sender, receiver, "capacity-first", "x".repeat(300)));
      await expect(sender.send(envelope(sender, receiver, "capacity-second", "x".repeat(300))))
        .rejects.toBeInstanceOf(TransportCapacityError);
      const stream = await inspector.manager.streams.info(transportInboxStream("machine-b"));
      expect(stream.state.last_seq).toBe(accepted.sequence);
      expect(stream.config).toMatchObject({ retention: "limits", discard: "new", num_replicas: 1, max_age: 0 });
    } finally {
      await admin.streams.update(transportInboxStream("machine-b"), { max_bytes: MAX_BYTES, max_msg_size: MAX_MESSAGE_BYTES });
    }
  }, 25_000);
});
