// Transparent loopback NATS fault relay for one client population (smarty-dev#15310, A' L4).
// Generalized from the private WireFaults class in tests/nats-inbox-faults.test.ts: frame boundaries,
// not chunk boundaries. Authentication, permissions, streams and consumers stay the real server's.
// It never logs or retains credential frames: only subjects, reply tokens and the bodies of
// `$JS.ACK.*` frames are inspected. Every fault is explicit, counted and confined to the clients
// that dial `url` (the bridge link in the mixed harness); the two Mains dial the broker directly.
import { Buffer } from "node:buffer";
import fs from "node:fs";
import { createConnection, createServer, type AddressInfo, type Socket } from "node:net";
import { connect as tlsConnect, createSecureContext, TLSSocket } from "node:tls";
import type { PathBTlsFiles } from "./nats-cluster.js";

export interface WireFrame {
  readonly line: string;
  readonly command: string;
  readonly subject: string;
  readonly reply?: string;
  readonly body: Buffer;
}

export type FrameMatch = (frame: WireFrame) => boolean;

/** Parsed JetStream ACK reply subject (v1 9-token or v2 12-token form). */
export interface JsAckSubject { readonly stream: string; readonly consumer: string; readonly delivered: number; readonly streamSequence: number }

export const parseJsAck = (subject: string): JsAckSubject | undefined => {
  const tokens = subject.split(".");
  if (tokens[0] !== "$JS" || tokens[1] !== "ACK") return undefined;
  const at = tokens.length === 9 ? 2 : tokens.length >= 11 ? 4 : -1;
  if (at < 0) return undefined;
  const delivered = Number(tokens[at + 2]), streamSequence = Number(tokens[at + 3]);
  if (!Number.isSafeInteger(delivered) || !Number.isSafeInteger(streamSequence)) return undefined;
  return { stream: tokens[at]!, consumer: tokens[at + 1]!, delivered, streamSequence };
};

/** One JetStream message delivered to a relay client (inbound MSG with a `$JS.ACK` reply). */
export interface JsDelivery extends JsAckSubject { readonly subject: string }

interface Rule { readonly match: FrameMatch; remaining: number }

const PAYLOAD_COMMANDS = new Set(["PUB", "HPUB", "MSG", "HMSG"]);

export class NatsWireFaults {
  /** Every accepted client socket (a NATS client reconnect is a new accept). */
  accepts = 0;
  /** Client sockets refused while blocked. */
  refused = 0;
  /** Number of sever() calls that destroyed at least one live client. */
  severed = 0;
  ackDrops = 0;
  /** The `+ACK`s this relay dropped, parsed. */
  readonly droppedAcks: JsAckSubject[] = [];
  pubAckDrops = 0;
  /** Outbound `+ACK` frames that reached the broker. */
  readonly acks: JsAckSubject[] = [];
  /** JetStream deliveries to relay clients; `delivered > 1` is a real broker redelivery. */
  readonly deliveries: JsDelivery[] = [];
  /** Outbound publishes to `fabric.in.*` (commands and protocol ACKs crossing to an inbox). */
  readonly inboxPublishes: string[] = [];
  /** The same publishes with their envelope JSON (test data only; credential frames are never PUBs to fabric.in). */
  readonly inboxFrames: Array<{ readonly subject: string; readonly body: string }> = [];

  /** Envelope bodies this relay's clients published to an inbox since index `since`, containing `token`. */
  publishedWith(token: string, since = 0): Array<{ readonly subject: string; readonly body: string }> {
    return this.inboxFrames.slice(since).filter((frame) => frame.body.includes(token));
  }
  url = "";
  private blocked = false;
  private readonly sockets = new Set<Socket>();
  private readonly clients = new Set<Socket>();
  private readonly ackRules: Rule[] = [];
  private readonly pubAckRules: Rule[] = [];
  private readonly lostReplies = new Set<string>();
  private readonly listener = createServer((client) => this.accept(client));
  private constructor(private readonly upstream: URL, private readonly tls: PathBTlsFiles) {}

  static async open(serverUrl: string, tls: PathBTlsFiles): Promise<NatsWireFaults> {
    const relay = new NatsWireFaults(new URL(serverUrl), tls);
    await new Promise<void>((resolve, reject) => {
      relay.listener.once("error", reject);
      relay.listener.listen(0, "127.0.0.1", resolve);
    });
    // The fixture certificate names localhost, so clients dial by name and verify the CA.
    relay.url = `tls://localhost:${(relay.listener.address() as AddressInfo).port}`;
    return relay;
  }

  /** Live client connections through this relay (one per open NATS connection). */
  get liveClients(): number { return this.clients.size; }

  /** Drop the next `count` outbound `+ACK` frames matching `match`: the broker never sees them. */
  loseAcks(match: FrameMatch = () => true, count = 1): void {
    if (!Number.isSafeInteger(count) || count <= 0) throw new Error("ACK drop count must be a positive safe integer");
    this.ackRules.push({ match, remaining: count });
  }

  /** Drop the broker `+ACK` of the next delivery (to a relay client) that matches, e.g. by its original subject. */
  loseAckOf(match: (delivery: JsDelivery) => boolean, count = 1): void {
    this.loseAcks((frame) => {
      const ack = parseJsAck(frame.subject);
      const delivery = ack && this.deliveryOf(ack.stream, ack.streamSequence);
      return delivery !== undefined && match(delivery);
    }, count);
  }

  /** The latest delivery of one stored message to a relay client. */
  deliveryOf(stream: string, streamSequence: number): JsDelivery | undefined {
    return [...this.deliveries].reverse().find((delivery) => delivery.stream === stream && delivery.streamSequence === streamSequence);
  }

  /** Let the next `count` matching publishes reach the broker, then drop the broker's PubAck reply. */
  losePubAcks(match: FrameMatch = (frame) => frame.subject.startsWith("fabric.in."), count = 1): void {
    if (!Number.isSafeInteger(count) || count <= 0) throw new Error("PubAck drop count must be a positive safe integer");
    this.pubAckRules.push({ match, remaining: count });
  }

  /** A forced disconnect: destroy every live client connection now. Reconnects are accepted. */
  sever(): number {
    const live = [...this.clients];
    this.clients.clear(); // synchronous: a later whenClients() waits for genuine reconnects
    for (const socket of this.sockets) socket.destroy();
    if (live.length) this.severed += 1;
    return live.length;
  }

  /** Sever every live client and refuse new ones until unblock(): a broker outage seen by this population. */
  block(): void { this.blocked = true; this.sever(); }
  unblock(): void { this.blocked = false; }

  /** Native socket-close observation: resolves when no client socket remains. */
  async whenNoClients(timeoutMs = 5_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.clients.size) {
      if (Date.now() > deadline) throw new Error(`relay still has ${this.clients.size} client(s) after ${timeoutMs}ms`);
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  /** Resolves once at least `count` clients are connected (reconnect evidence). */
  async whenClients(count = 1, timeoutMs = 15_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.clients.size < count) {
      if (Date.now() > deadline) throw new Error(`relay has ${this.clients.size}/${count} client(s) after ${timeoutMs}ms`);
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  private track<T extends Socket>(socket: T): T {
    this.sockets.add(socket);
    socket.on("error", () => undefined); // A deliberate disconnect is part of the fixture.
    socket.once("close", () => this.sockets.delete(socket));
    return socket;
  }

  private accept(client: Socket): void {
    if (this.blocked) { this.refused += 1; client.destroy(); return; }
    this.accepts += 1;
    this.clients.add(client);
    client.once("close", () => this.clients.delete(client));
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

  private take(rules: Rule[], frame: WireFrame): boolean {
    const index = rules.findIndex((rule) => rule.match(frame));
    if (index < 0) return false;
    const rule = rules[index]!;
    rule.remaining -= 1;
    if (rule.remaining <= 0) rules.splice(index, 1);
    return true;
  }

  private forward(frame: WireFrame, outbound: boolean): boolean {
    if (outbound) {
      if (frame.subject.startsWith("$JS.ACK.") && frame.body.toString() === "+ACK") {
        const ack = parseJsAck(frame.subject);
        if (this.take(this.ackRules, frame)) {
          this.ackDrops += 1;
          if (ack) this.droppedAcks.push(ack);
          return false;
        }
        if (ack) this.acks.push(ack);
      }
      if (frame.subject.startsWith("fabric.in.")) {
        this.inboxPublishes.push(frame.subject);
        this.inboxFrames.push({ subject: frame.subject, body: frame.body.toString("utf8") });
        if (frame.reply && this.take(this.pubAckRules, frame)) this.lostReplies.add(frame.reply);
      }
    } else {
      if (this.lostReplies.delete(frame.subject)) { this.pubAckDrops += 1; return false; }
      const delivery = frame.reply ? parseJsAck(frame.reply) : undefined;
      if (delivery) this.deliveries.push({ ...delivery, subject: frame.subject });
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
        const command = tokens[0] ?? "";
        const payload = PAYLOAD_COMMANDS.has(command);
        const length = payload ? Number(tokens.at(-1)) : 0;
        const frameLength = end + 2 + (payload ? length + 2 : 0);
        if (pending.length < frameLength) return;
        const bytes = pending.subarray(0, frameLength);
        pending = pending.subarray(frameLength);
        const headers = command === "HPUB" || command === "HMSG" ? Number(tokens.at(-2)) : 0;
        const replyIndex = outbound ? 2 : 3;
        const hasReply = tokens.length === (outbound ? (headers ? 5 : 4) : (headers ? 6 : 5));
        const frame: WireFrame = { line, command, subject: payload ? tokens[1] ?? "" : "",
          ...(payload && hasReply ? { reply: tokens[replyIndex] } : {}),
          body: payload ? bytes.subarray(end + 2 + headers, end + 2 + length) : Buffer.alloc(0) };
        if (!payload || this.forward(frame, outbound)) destination.write(bytes);
      }
    });
  }

  async close(): Promise<void> {
    this.blocked = true;
    this.sockets.forEach((socket) => socket.destroy());
    await new Promise<void>((resolve, reject) => this.listener.close((error) => error ? reject(error) : resolve()));
  }
}

/** A broker `+ACK` that settles a message of `stream`. */
export const ackOnStream = (stream: string): FrameMatch => (frame) => parseJsAck(frame.subject)?.stream === stream;
/** `fabric.in.<destinationMachine>.<host hash>.<cmd|ack>.<target hash>.<sourceMachine>` (src/transport/nats/subjects.ts). */
export const parseInboxSubject = (subject: string) => {
  const tokens = subject.split(".");
  if (tokens.length !== 7 || tokens[0] !== "fabric" || tokens[1] !== "in" || (tokens[4] !== "cmd" && tokens[4] !== "ack")) return undefined;
  return { machine: tokens[2]!, hostHash: tokens[3]!, kind: tokens[4] as "cmd" | "ack", targetHash: tokens[5]!, sourceMachine: tokens[6]! };
};

/** A publish into `fabric.in.<machine>.<host>.<kind>...` (command or protocol ACK). */
export const publishTo = (machine: string, kind?: "cmd" | "ack"): FrameMatch => (frame) => {
  const tokens = frame.subject.split(".");
  return tokens[0] === "fabric" && tokens[1] === "in" && tokens[2] === machine && (kind === undefined || tokens[4] === kind);
};
