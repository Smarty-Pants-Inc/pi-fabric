import { spawn, type ChildProcess } from "node:child_process";
import { createWriteStream, existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync, type WriteStream } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import net from "node:net";
import { once } from "node:events";
import { finished } from "node:stream/promises";
import { connect, type NatsConnection } from "@nats-io/transport-node";
import { jetstream, jetstreamManager, JetStreamApiError, type JetStreamManager } from "@nats-io/jetstream";
import { NatsKvLeaseStore } from "../../src/topology/nats-kv-leases.js";

export const artifactDirectory = (): string => {
  const dir = process.env.NATS_LEASE_ARTIFACT_DIR ?? join(tmpdir(), "nats-lease-evidence");
  mkdirSync(dir, { recursive: true });
  return dir;
};
export const sleep = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
export const deferred = <T = void>(): { promise: Promise<T>; resolve: (value: T) => void } => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
};

export const natsLeaseServerBinary = (): string => process.env.NATS_SERVER_BIN ??
  resolve(".local/nats/nats-server-v2.14.7-linux-amd64/nats-server");
/** No server is installed by normal tests. An explicit binary opts in and MUST exist. */
export const hasNatsLeaseServer = (): boolean => Boolean(process.env.NATS_SERVER_BIN) ||
  existsSync(natsLeaseServerBinary());

interface NodeProcess {
  name: string; child: ChildProcess; exited: Promise<unknown>; output: WriteStream;
  clientPort: number; monitorPort: number;
}

/** Test-only topology readiness polling, never used for lease acquisition or renewal. */
export class NatsLeaseCluster {
  readonly root = mkdtempSync(join(tmpdir(), "u2-nats-cluster-"));
  readonly nodes: NodeProcess[] = [];
  readonly connections: NatsConnection[] = [];
  nc!: NatsConnection;
  jsm!: JetStreamManager;
  store!: NatsKvLeaseStore;
  readonly bucket: string;
  constructor(readonly label: string, readonly maxLeaseMs = 5_000) {
    this.bucket = `U2_${label.replace(/[^a-zA-Z0-9_]/g, "_")}_${process.pid}`;
  }
  get servers(): string[] { return this.nodes.map(n => `nats://127.0.0.1:${n.clientPort}`); }
  async connection(): Promise<NatsConnection> {
    const nc = await connect({ servers: this.servers, timeout: 2_000,
      reconnectTimeWait: 50, maxReconnectAttempts: 100, noRandomize: true });
    this.connections.push(nc); return nc;
  }
  async start(): Promise<this> {
    const binary = natsLeaseServerBinary();
    if (!existsSync(binary)) throw new Error(`Missing verified NATS binary: ${binary}`);
    // Reserve all ports together, release immediately before spawning this isolated cluster.
    const reservations = await Promise.all(Array.from({ length: 9 }, async () => {
      const server = net.createServer(); server.listen(0, "127.0.0.1"); await once(server, "listening");
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("Cannot reserve local port");
      return { server, port: address.port };
    }));
    const ports = reservations.map(r => r.port);
    await Promise.all(reservations.map(r => new Promise<void>(resolve => r.server.close(() => resolve()))));
    const meta = deferred();
    let metaSeen = false;
    const ready: Promise<void>[] = [];
    try {
      for (let i = 0; i < 3; i++) {
        const name = `u2-${this.label}-${i}`;
        const clientPort = ports[i * 3]!, routePort = ports[i * 3 + 1]!, monitorPort = ports[i * 3 + 2]!;
        const config = `server_name: ${name}\nhost: 127.0.0.1\nport: ${clientPort}\nhttp: 127.0.0.1:${monitorPort}\njetstream {\n store_dir: "${join(this.root, name)}"\n sync_interval: always\n}\ncluster {\n name: u2-${this.label}\n host: 127.0.0.1\n port: ${routePort}\n routes: [${ports.filter((_, j) => j % 3 === 1 && j !== i * 3 + 1).map(p => `"nats://127.0.0.1:${p}"`).join(",")} ]\n}\n`;
        const configPath = join(this.root, `${name}.conf`);
        writeFileSync(configPath, config);
        writeFileSync(join(artifactDirectory(), `${name}.conf`), config);
        const output = createWriteStream(join(artifactDirectory(), `${name}.log`));
        const child = spawn(binary, ["-c", configPath], { stdio: ["ignore", "pipe", "pipe"] });
        const exited = once(child, "exit");
        // Always observe exit, including startup failures.
        exited.catch(() => undefined);
        this.nodes.push({ name, child, exited, output, clientPort, monitorPort });
        const nodeReady = deferred();
        let tail = "", readySeen = false;
        const data = (chunk: Buffer): void => {
          output.write(chunk); tail = (tail + chunk.toString()).slice(-32_000);
          if (!readySeen && tail.includes("Server is ready")) { readySeen = true; nodeReady.resolve(); }
          if (!metaSeen && /JetStream cluster new metadata leader/.test(tail)) { metaSeen = true; meta.resolve(); }
        };
        child.stdout!.on("data", data); child.stderr!.on("data", data);
        ready.push(this.#bounded(nodeReady.promise, 10_000, `server ${name} readiness`));
      }
      await Promise.all(ready);
      await this.#bounded(meta.promise, 15_000, "metadata leader election");
      this.nc = await this.connection();
      // Election logging precedes all peers becoming eligible for placement. Retry ONLY
      // startup placement/timeouts, never a lease operation. Bound the entire readiness phase.
      const admissionDeadline = Date.now() + 20_000;
      for (;;) {
        try {
          this.jsm = await jetstreamManager(this.nc, { timeout: 5_000 });
          this.store = await NatsKvLeaseStore.open(this.nc, { bucket: this.bucket,
            maxLeaseMs: this.maxLeaseMs, timeoutMs: 5_000 });
          break;
        } catch (error) {
          if (Date.now() >= admissionDeadline ||
            !((error instanceof JetStreamApiError && error.code === 10005) ||
              (error instanceof Error && error.name === "TimeoutError"))) throw error;
          await sleep(50);
        }
      }
      const deadline = Date.now() + 10_000;
      for (;;) {
        const info = await this.jsm.streams.info(`KV_${this.bucket}`);
        if (info.cluster?.leader && info.cluster.replicas?.length === 2 &&
          info.cluster.replicas.every(r => r.current)) {
          writeFileSync(join(artifactDirectory(), `${this.label}-cluster.json`), JSON.stringify(info, null, 2));
          break;
        }
        if (Date.now() >= deadline) throw new Error("R3 replicas did not become current");
        await sleep(50);
      }
      // A current replica set does not prove publish-subject interest has propagated
      // to this client server. Probe the DATA plane before any lease operation. Retry
      // only this disposable startup record, never acquire/renew or an ownership grant.
      const subject = `$KV.${this.bucket}.__readiness`;
      const js = jetstream(this.nc, { timeout: 500 });
      const publishDeadline = Date.now() + 10_000;
      let attempts = 0;
      for (;;) {
        try {
          attempts++;
          await js.publish(subject, new Uint8Array());
          await this.jsm.streams.purge(`KV_${this.bucket}`, { filter: subject });
          writeFileSync(join(artifactDirectory(), `${this.label}-readiness.json`),
            JSON.stringify({ phase: "startup-only data-plane publish/purge", attempts }, null, 2) + "\n");
          break;
        } catch (error) {
          if (Date.now() >= publishDeadline || !(error instanceof Error &&
            (error.name === "JetStreamNotEnabled" || error.name === "TimeoutError"))) throw error;
          await sleep(50);
        }
      }
      return this;
    } catch (error) { await this.close(); throw error; }
  }
  async #bounded<T>(promise: Promise<T>, ms: number, name: string): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([promise, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Timeout: ${name}`)), ms);
      })]);
    } finally { if (timer) clearTimeout(timer); }
  }
  async killLeader(): Promise<string> {
    const info = await this.jsm.streams.info(`KV_${this.bucket}`);
    const leader = info.cluster?.leader;
    const node = this.nodes.find(n => n.name === leader);
    if (!node) throw new Error(`No matching stream leader: ${leader}`);
    node.child.kill("SIGKILL"); await node.exited;
    return node.name;
  }
  async close(): Promise<void> {
    await Promise.all(this.connections.map(nc => nc.close()));
    const stopped = await Promise.allSettled(this.nodes.map(async node => {
      if (node.child.exitCode === null && node.child.signalCode === null) node.child.kill("SIGTERM");
      try { await this.#bounded(node.exited, 5_000, `reap ${node.name}`); }
      catch (error) {
        // A failed test must not leave an owned server alive. Never kill by process name.
        if (node.child.exitCode === null && node.child.signalCode === null) {
          node.child.kill("SIGKILL");
          await this.#bounded(node.exited, 5_000, `force reap ${node.name}`);
        } else throw error;
      } finally { node.output.end(); await finished(node.output); }
    }));
    rmSync(this.root, { recursive: true, force: true });
    const failure = stopped.find(result => result.status === "rejected");
    if (failure?.status === "rejected") throw failure.reason;
  }
}
