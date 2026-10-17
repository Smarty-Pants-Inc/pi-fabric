// Real official NATS 2.14.7 R3 proof for the lost-first-pull regression. The relay drops one native TCP PUB
// $JS.API.CONSUMER.MSG.NEXT after observing the real empty open and its real consumer placement. This is a forced
// equivalent of the measured lost-first-pull fault, not a reproduction of the route-ordering cause; it does not mock
// the SDK, synthesize protocol frames, or call product delete/purge APIs.
import { createServer, connect as tcpConnect, type AddressInfo, type Socket } from "node:net";
import fs from "node:fs";
import path from "node:path";
import { connect } from "@nats-io/transport-node";
import { jetstreamManager } from "@nats-io/jetstream";
import { afterEach, expect, it } from "vitest";
import { closeNatsStateBackend, natsStreamName, openNatsStateBackend } from "../src/mesh/state-backend-nats.js";
import type { MeshIdentity } from "../src/mesh/event-log.js";
import { startNatsElectionCluster, stopNatsElectionCluster, type NatsElectionCluster } from "./helpers/nats-election-cluster.js";

const binary = process.env.NATS_BIN?.trim();
const identity: MeshIdentity = { id: "pull-recovery-test", name: "pull-recovery-test", kind: "agent" };
const clusters: NatsElectionCluster[] = [];

afterEach(async () => {
  for (const cluster of clusters.splice(0)) await stopNatsElectionCluster(cluster);
});

const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};

const bounded = async <T>(promise: Promise<T>, milliseconds: number, label: string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} exceeded ${milliseconds}ms`)), milliseconds);
    })]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
};

const nextSubject = "$JS.API.CONSUMER.MSG.NEXT.";
const destructiveSubject = (subject: string): boolean => subject.startsWith("$JS.") &&
  /(?:^|\.)(?:DELETE|PURGE)(?:\.|$)/.test(subject);

/**
 * A byte-preserving native NATS relay. It holds the first pull until the test can inspect the real CREATE result;
 * the selected held frame is either forwarded unchanged (healthy placement) or censored exactly once (fault case).
 */
class PullFaultRelay {
  readonly url: string;
  readonly errors: Error[] = [];
  readonly destructiveAttempts: string[] = [];
  readonly forwardedSubjects: string[] = [];
  readonly droppedNextSubjects: string[] = [];
  readonly firstNext = deferred<string>();
  private readonly sockets = new Set<Socket>();
  private readonly seenNext = new Set<string>();
  private server: ReturnType<typeof createServer>;
  private held: { subject: string; frame: Buffer; upstream: Socket } | undefined;
  private enabled = true;
  private constructor(private readonly target: string, server: ReturnType<typeof createServer>, port: number,
    private readonly holdFirstNext: boolean) {
    this.server = server;
    this.url = `nats://127.0.0.1:${port}`;
  }

  static async create(target: string, holdFirstNext: boolean): Promise<PullFaultRelay> {
    let relay!: PullFaultRelay;
    const server = createServer(client => relay.accept(client));
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address() as AddressInfo;
    relay = new PullFaultRelay(target, server, address.port, holdFirstNext);
    return relay;
  }

  private track<T extends Socket>(socket: T): T {
    this.sockets.add(socket);
    socket.on("error", () => undefined);
    socket.once("close", () => this.sockets.delete(socket));
    return socket;
  }

  private accept(client: Socket): void {
    if (!this.enabled) { client.destroy(); return; }
    const upstream = this.track(tcpConnect({ host: "127.0.0.1", port: Number(new URL(this.target).port) }));
    this.track(client);
    client.once("close", () => upstream.destroy());
    upstream.once("close", () => client.destroy());
    upstream.on("data", bytes => { if (!client.destroyed) client.write(bytes); });

    let pending = Buffer.alloc(0);
    client.on("data", (bytes: Buffer) => {
      pending = Buffer.concat([pending, bytes]);
      for (;;) {
        const lineEnd = pending.indexOf("\r\n");
        if (lineEnd < 0) return;
        const words = pending.subarray(0, lineEnd).toString().split(/\s+/);
        const verb = words[0];
        const publish = verb === "PUB" || verb === "HPUB";
        const size = publish ? Number(words.at(-1)) : 0;
        if (publish && (!Number.isSafeInteger(size) || size < 0 || size > 2 ** 22)) {
          this.errors.push(new Error("invalid client publish frame"));
          client.destroy();
          upstream.destroy();
          return;
        }
        const frameEnd = lineEnd + 2 + (publish ? size + 2 : 0);
        if (pending.length < frameEnd) return;
        const frame = Buffer.from(pending.subarray(0, frameEnd));
        pending = pending.subarray(frameEnd);
        const subject = publish ? words[1] : undefined;
        if (publish && subject !== undefined) {
          this.forwardedSubjects.push(subject);
          if (destructiveSubject(subject)) {
            // A destructive API request is censored at the relay boundary: never forward it and never execute a
            // local manager delete/purge as a cleanup substitute. The test fails if product traffic attempts one.
            this.destructiveAttempts.push(subject);
            continue;
          }
          if (subject.startsWith(nextSubject)) {
            if (this.holdFirstNext && !this.seenNext.has(subject)) {
              this.seenNext.add(subject);
              this.held = { subject, frame, upstream };
              this.firstNext.resolve(subject);
              continue;
            }
            this.seenNext.add(subject);
          }
        }
        upstream.write(frame);
      }
    });
  }

  dropHeldNext(subject: string): void {
    if (!this.held || this.held.subject !== subject) throw new Error(`no held first NEXT for ${subject}`);
    this.droppedNextSubjects.push(subject);
    this.held = undefined;
  }

  releaseHeldNext(): void {
    const held = this.held;
    this.held = undefined;
    if (held && !held.upstream.destroyed) held.upstream.write(held.frame);
  }

  async stop(): Promise<void> {
    this.enabled = false;
    this.held = undefined;
    for (const socket of this.sockets) socket.destroy();
    await new Promise<void>((resolve, reject) => {
      if (!this.server.listening) { resolve(); return; }
      this.server.close(error => error ? reject(error) : resolve());
    });
  }
}

const streamInfo = async (seed: string, stream: string) => {
  const nc = await connect({ servers: [seed], ignoreClusterUpdates: true, reconnect: false, timeout: 1_000 });
  try { return await (await jetstreamManager(nc, { timeout: 1_000 })).streams.info(stream); }
  finally { await nc.drain(); }
};

const nodeFor = (cluster: NatsElectionCluster, id: string | undefined) =>
  cluster.nodes.find(node => node.name === id || node.metaId === id);

it.skipIf(!binary)("recovers a real R3 backend after its first native pull is lost", async () => {
  const cluster = await startNatsElectionCluster(binary!);
  clusters.push(cluster);
  const seedNode = cluster.nodes.find(node => node.clientUrl === cluster.seedUrl)!;
  const observer = await connect({ servers: [cluster.seedUrl], ignoreClusterUpdates: true, reconnect: false, timeout: 1_000 });
  const manager = await jetstreamManager(observer, { timeout: 1_000 });
  const roots: string[] = [];
  const relays: PullFaultRelay[] = [];

  const closeRoot = (root: string): void => {
    closeNatsStateBackend(root);
    roots.splice(roots.indexOf(root), 1);
  };
  const closeRelay = async (relay: PullFaultRelay): Promise<void> => {
    relays.splice(relays.indexOf(relay), 1);
    await relay.stop();
  };

  try {
    // Counterexample: the same real client path, relay byte forwarding unchanged, and a normal first pull.
    const normalRoot = path.join(cluster.root, "normal-first-pull");
    fs.mkdirSync(normalRoot);
    roots.push(normalRoot);
    const normalRelay = await PullFaultRelay.create(cluster.seedUrl, false);
    relays.push(normalRelay);
    const normal = await openNatsStateBackend({ root: normalRoot, servers: [normalRelay.url], monitoringUrl: cluster.seedMonitorUrl,
      replicas: 3, snapshotEvery: 0 });
    expect(normal.diagnostics().nats).toMatchObject({ appliedSeq: 0, knownSeq: 0 });
    const normalEntry = await normal.put({ key: "normal/one", value: 1, identity });
    expect(normalEntry).toMatchObject({ key: "normal/one", value: 1, version: 1 });
    expect(normal.get("normal/one")).toMatchObject({ value: 1, version: 1 });
    const normalStream = await streamInfo(cluster.seedUrl, natsStreamName(normalRoot));
    expect(normalStream.config.num_replicas).toBe(3);
    expect(normalStream.state).toMatchObject({ last_seq: 1, messages: 1 });
    expect(normal.diagnostics().nats).toMatchObject({ appliedSeq: 1, knownSeq: 1 });
    expect(normalRelay.errors).toEqual([]);
    expect(normalRelay.destructiveAttempts).toEqual([]);
    closeRoot(normalRoot);
    await closeRelay(normalRelay);

    let selected = false;
    const placements: { setup: number; consumer: string; consumerHost: string; connectedSeed: string }[] = [];
    for (let setup = 1; setup <= 32; setup++) {
      const root = path.join(cluster.root, `lost-first-next-${setup}`);
      fs.mkdirSync(root);
      roots.push(root);
      const relay = await PullFaultRelay.create(cluster.seedUrl, true);
      relays.push(relay);
      const backend = await openNatsStateBackend({ root, servers: [relay.url], monitoringUrl: cluster.seedMonitorUrl,
        replicas: 3, snapshotEvery: 0 });
      // The empty open has no history to catch up; the first native NEXT is held only so its actual placement can be
      // inspected. No failed operation is retried: every open here completed successfully with stream head zero.
      expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 0, knownSeq: 0 });
      const heldSubject = await bounded(relay.firstNext.promise, 2_000, "first native NEXT observation");
      expect(heldSubject.startsWith(nextSubject)).toBe(true);
      const suffix = heldSubject.slice(nextSubject.length).split(".");
      expect(suffix).toHaveLength(2);
      const [stream, consumer] = suffix;
      expect(stream).toBe(natsStreamName(root));
      const before = await manager.consumers.info(stream!, consumer!);
      expect(before).toMatchObject({ stream_name: stream, name: consumer });
      const host = nodeFor(cluster, before.cluster?.leader);
      if (!host) throw new Error(`consumer host not in fixture: ${JSON.stringify(before.cluster)}`);
      const placement = { setup, consumer: consumer!, consumerHost: host.name, connectedSeed: seedNode.name };
      placements.push(placement);
      console.log(`NATS_PULL_PLACEMENT ${JSON.stringify(placement)}`);

      if (host === seedNode) {
        // This is a healthy placement control. Forward the held first pull unchanged and close only our owned
        // backend/relay/cluster resources; no product delete or purge is used.
        relay.releaseHeldNext();
        closeRoot(root);
        await closeRelay(relay);
        continue;
      }

      selected = true;
      relay.dropHeldNext(heldSubject);
      expect(relay.droppedNextSubjects).toEqual([heldSubject]);
      const emptyStream = await streamInfo(cluster.seedUrl, stream!);
      expect(emptyStream.config.num_replicas).toBe(3);
      expect(emptyStream.state).toMatchObject({ last_seq: 0, messages: 0 });

      // The backend's normal five-second lock budget is intentionally untouched: on the unfixed product this is the
      // measured BUSY-at-zero RED; the repair must make this real put ACK, store, and project without a second write.
      const written = await backend.put({ key: "lost/first", value: { recovered: true }, identity });
      expect(written).toMatchObject({ key: "lost/first", value: { recovered: true }, version: 1 });
      expect(backend.get("lost/first")).toMatchObject({ value: { recovered: true }, version: 1 });
      const after = await manager.consumers.info(stream!, consumer!);
      expect(after.name).toBe(before.name);
      expect(after.created).toBe(before.created);
      expect((await manager.consumers.list(stream!).next()).map(info => info.name)).toEqual([before.name]);
      const recoveredStream = await streamInfo(cluster.seedUrl, stream!);
      expect(recoveredStream.state).toMatchObject({ last_seq: 1, messages: 1 });
      expect(backend.diagnostics().nats).toMatchObject({ appliedSeq: 1, knownSeq: 1 });
      expect(backend.diagnostics().nats?.failure).toBeUndefined();
      expect(relay.errors).toEqual([]);
      expect(relay.droppedNextSubjects).toHaveLength(1);
      expect(relay.destructiveAttempts).toEqual([]);
      console.log(`NATS_PULL_RECOVERY ${JSON.stringify({ fault: "forced-lost-first-NEXT-equivalent", routeOrderingReproduced: false,
        setup, consumer: before.name, created: before.created, consumerHost: host.name, connectedSeed: seedNode.name,
        droppedNext: relay.droppedNextSubjects.length, streamMessages: 1, diagnostics: backend.diagnostics().nats })}`);
      closeRoot(root);
      await closeRelay(relay);
      break;
    }
    expect(selected, `no consumer host distinct from connected seed within 32 healthy empty opens: ${JSON.stringify(placements)}`).toBe(true);
  } finally {
    for (const root of [...roots]) closeRoot(root);
    for (const relay of [...relays]) await closeRelay(relay);
    await observer.drain();
  }
}, 90_000);
