// Real-broker TLS control relay. It never synthesizes INFO/USER.INFO or permissions.
// Hold only an authentication-verification request; all other client/server traffic flows unchanged.
import fs from "node:fs";
import { createServer, connect as tcpConnect, type AddressInfo, type Socket } from "node:net";
import { connect as tlsConnect, createSecureContext, TLSSocket } from "node:tls";
import type { PathBHarness } from "./nats-cluster.js";

export function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

export class ReconnectRelay {
  readonly verification = deferred<void>();
  readonly verificationClosed = deferred<void>();
  readonly subjects: string[] = [];
  readonly commands: string[] = [];
  readonly errors: Error[] = [];
  readonly url: string;
  enabled = true;
  holdVerification = false;
  private held: Array<() => void> = [];
  private readonly sockets = new Set<Socket>();
  private routeTarget: PathBHarness;
  private constructor(private readonly broker: PathBHarness, private readonly server: ReturnType<typeof createServer>, port: number) {
    this.url = `tls://localhost:${port}`;
    this.routeTarget = broker;
  }

  routeTo(broker: PathBHarness): void { this.routeTarget = broker; }

  static async create(broker: PathBHarness): Promise<ReconnectRelay> {
    let relay!: ReconnectRelay;
    const server = createServer((socket) => relay.accept(socket));
    await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
    relay = new ReconnectRelay(broker, server, (server.address() as AddressInfo).port);
    return relay;
  }

  private track<T extends Socket>(socket: T): T {
    this.sockets.add(socket);
    socket.on("error", () => undefined); // A deliberate disconnect is part of the fixture.
    socket.once("close", () => this.sockets.delete(socket));
    return socket;
  }

  private accept(client: Socket): void {
    this.track(client);
    if (!this.enabled) { client.destroy(); return; }
    const target = this.routeTarget;
    const upstream = this.track(tcpConnect({ host: "127.0.0.1", port: Number(new URL(target.serverUrl).port) }));
    client.once("close", () => upstream.destroy());
    upstream.once("close", () => client.destroy());
    let info = Buffer.alloc(0);
    const readInfo = (chunk: Buffer): void => {
      info = Buffer.concat([info, chunk]);
      const end = info.indexOf("\r\n");
      if (end < 0) return;
      upstream.removeListener("data", readInfo);
      if (!info.subarray(0, end).toString().startsWith("INFO ") || info.length !== end + 2) {
        this.errors.push(new Error("unexpected real broker TLS preamble"));
        client.destroy(); upstream.destroy(); return;
      }
      // NATS sends plaintext INFO before the TLS handshake. Preserve it byte-for-byte.
      client.write(info);
      const secureClient = this.track(new TLSSocket(client, { isServer: true, secureContext: createSecureContext({
        cert: fs.readFileSync(`${this.broker.root}/tls/server.pem`), key: fs.readFileSync(`${this.broker.root}/tls/server.key`),
      }) }));
      const secureUpstream = this.track(tlsConnect({ socket: upstream, servername: "localhost", ca: fs.readFileSync(target.tls!.caFile), rejectUnauthorized: true }));
      secureClient.once("close", () => secureUpstream.destroy());
      secureUpstream.once("close", () => secureClient.destroy());
      // Never hold inbound traffic: an early application delivery must remain observable.
      secureUpstream.on("data", (bytes: Buffer) => secureClient.write(bytes));
      let pending = Buffer.alloc(0);
      let heldOnThisConnection = false;
      secureClient.once("close", () => { if (heldOnThisConnection) this.verificationClosed.resolve(); });
      secureClient.on("data", (bytes: Buffer) => {
        pending = Buffer.concat([pending, bytes]);
        for (;;) {
          const lineEnd = pending.indexOf("\r\n");
          if (lineEnd < 0) return;
          const words = pending.subarray(0, lineEnd).toString().split(/\s+/);
          const publish = words[0] === "PUB" || words[0] === "HPUB";
          const size = publish ? Number(words.at(-1)) : 0;
          if (!Number.isSafeInteger(size) || size < 0 || size > 2 ** 22) {
            this.errors.push(new Error("invalid relay protocol frame size")); secureClient.destroy(); return;
          }
          const frameEnd = lineEnd + 2 + (publish ? size + 2 : 0);
          if (pending.length < frameEnd) return;
          const frame = Buffer.from(pending.subarray(0, frameEnd));
          pending = pending.subarray(frameEnd);
          // Record only protocol verbs and subjects, never CONNECT/JWT/seed/payload bytes.
          this.commands.push(words[0]!);
          if (publish) this.subjects.push(words[1]!);
          if (publish && words[1] === "$SYS.REQ.USER.INFO" && this.holdVerification) {
            heldOnThisConnection = true;
            this.held.push(() => { if (!secureUpstream.destroyed) secureUpstream.write(frame); });
            this.verification.resolve();
          } else secureUpstream.write(frame);
        }
      });
    };
    upstream.on("data", readInfo);
  }

  releaseVerification(): void {
    this.holdVerification = false;
    for (const send of this.held.splice(0)) send();
  }

  disconnect(): void {
    this.enabled = false;
    for (const socket of this.sockets) socket.destroy();
  }

  async stop(): Promise<void> {
    this.disconnect();
    this.held = [];
    await new Promise<void>((resolve, reject) => this.server.close((error) => error ? reject(error) : resolve()));
  }
}
