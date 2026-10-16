// Path B hosted-test harness. NATS_BIN is preferred and must be an absolute executable path when set;
// otherwise this resolves nats-server from PATH, then the fleet pinned-tool location. The harness owns one
// child process, two loopback ports, a private resolver/store tree, and credential files. The rendered config
// sets JetStream sync_interval: always. restart() reuses the same config/store; stop() is terminal cleanup.
// nats-state-server.ts was inspected: it exports external-server selectors, not readiness/version functions.
// No credential bytes, seeds, JWTs, or identity values are included in errors or public diagnostics.
import { spawn, type ChildProcess } from "node:child_process";
import { get as httpGet } from "node:http";
import { createServer, type AddressInfo } from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { jetstream, jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import { connect, credsAuthenticator, nkeys, type NatsConnection } from "@nats-io/transport-node";
import { encodeAccountJwt, encodeOperatorJwt, encodeUserJwt, formatUserCreds, type ScopedSigningKey } from "../../src/mesh/nats-auth/jwt.js";
import { PATH_B_OPERATOR_MACHINE, pathBRoleTemplate } from "../../src/mesh/nats-auth/permissions.js";
import { renderNatsServerConfig } from "../../src/mesh/nats-auth/server-config.js";

export const PATH_B_MACHINES = ["machine-a", "machine-b"] as const;
export type PathBMachine = (typeof PATH_B_MACHINES)[number];

export interface PathBUser {
  readonly machine: PathBMachine | typeof PATH_B_OPERATOR_MACHINE;
  readonly publicKey: string;
  readonly credsPath: string;
}

export interface PathBUsers {
  readonly machineA: PathBUser;
  readonly machineB: PathBUser;
  /** Historical Module C alias: this is the distinct report-only role=operator credential. */
  readonly maintainer: PathBUser;
  readonly bootstrap: PathBUser;
}

/** Test-only identity injection: broker/process/config ownership stays with this harness. */
export interface PathBIdentity {
  readonly operatorJwt: string;
  readonly systemAccount: string;
  readonly systemAccountJwt: string;
  readonly appAccount: string;
  readonly appAccountJwt: string;
  readonly users: PathBUsers;
}

export interface PathBHarnessOptions {
  readonly binary?: string;
  /** Issue real CLI artifacts without changing the default hand-built fault-injection fixture. */
  readonly identityFactory?: (root: string) => PathBIdentity | Promise<PathBIdentity>;
  /** Deliberately misissued grants for fail-closed identity-check tests. */
  readonly additionalAgentPublish?: readonly string[];
}

interface KeyPair {
  getPublicKey(): string;
  getSeed(): Uint8Array;
  sign(bytes: Uint8Array): Uint8Array;
  clear(): void;
}

const API_TIMEOUT_MS = 2_500;
const START_TIMEOUT_MS = 20_000;
const EXPECTED_VERSION = process.env.NATS_EXPECTED_SERVER_VERSION?.trim();
const ACCOUNT_LIMITS = {
  conn: -1,
  leaf: -1,
  subs: -1,
  data: -1,
  payload: -1,
  mem_storage: 0,
  disk_storage: 4_294_967_296,
  streams: 64,
  consumer: 256,
};
const SYSTEM_LIMITS = {
  conn: -1,
  leaf: -1,
  subs: -1,
  data: -1,
  payload: -1,
  mem_storage: 0,
  disk_storage: 0,
  streams: 0,
  consumer: 0,
};

function pairOf(value: unknown): KeyPair {
  return value as KeyPair;
}

function signerOf(pair: KeyPair) {
  return { publicKey: pair.getPublicKey(), sign: (bytes: Uint8Array) => pairOf(pair).sign(bytes) };
}

async function freeLoopbackPort(used: Set<number>): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", resolve);
  });
  const address = probe.address() as AddressInfo;
  await new Promise<void>((resolve, reject) => probe.close((error) => error ? reject(error) : resolve()));
  if (used.has(address.port)) return freeLoopbackPort(used);
  used.add(address.port);
  return address.port;
}

function executable(file: string): boolean {
  try {
    fs.accessSync(file, fs.constants.X_OK);
    return fs.statSync(file).isFile();
  } catch {
    return false;
  }
}

/** Resolve only an explicitly supplied absolute path or an executable found without a shell. */
export function resolveNatsBinary(explicit = process.env.NATS_BIN?.trim()): string | undefined {
  if (explicit) {
    if (!path.isAbsolute(explicit) || !executable(explicit)) {
      throw new Error("NATS_BIN must be an absolute executable nats-server path");
    }
    return explicit;
  }
  const candidates = [
    ...(process.env.PATH ?? "").split(path.delimiter).filter(Boolean).map((dir) => path.join(dir, "nats-server")),
    "/home/paul/.local/bin/nats-server",
  ];
  return candidates.find(executable);
}

function httpJson(url: string): Promise<{ status: number; value?: Record<string, unknown> }> {
  return new Promise((resolve) => {
    const request = httpGet(url, (response) => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk: string) => { body += chunk; });
      response.on("end", () => {
        try {
          const value = JSON.parse(body) as unknown;
          const object = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
          resolve(object === undefined ? { status: response.statusCode ?? 0 } : { status: response.statusCode ?? 0, value: object });
        } catch {
          resolve({ status: response.statusCode ?? 0 });
        }
      });
    });
    request.setTimeout(1_000, () => request.destroy());
    request.once("error", () => resolve({ status: 0 }));
  });
}

async function childExited(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (child.exitCode !== null || child.signalCode !== null) return true;
  return Promise.race([
    new Promise<boolean>((resolve) => child.once("exit", () => resolve(true))),
    sleep(timeoutMs).then(() => false),
  ]);
}

async function stopChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM");
  if (await childExited(child, 2_000)) return;
  child.kill("SIGKILL");
  if (!(await childExited(child, 2_000))) throw new Error("owned nats-server child did not stop");
}

function publicShape(value: unknown, prefix = "", depth = 0): string[] {
  if (depth > 3 || value === null || typeof value !== "object") return prefix ? [prefix] : [];
  if (Array.isArray(value)) return prefix ? [`${prefix}[]`] : [];
  const record = value as Record<string, unknown>;
  return Object.keys(record).sort().flatMap((key) => {
    const current = prefix ? `${prefix}.${key}` : key;
    return depth >= 3 ? [current] : publicShape(record[key], current, depth + 1);
  });
}

export class PathBHarness {
  readonly root: string;
  readonly serverUrl: string;
  readonly monitorUrl: string;
  readonly users: PathBUsers;
  readonly configText: string;
  readonly binary: string;
  serverVersion = "";
  private child: ChildProcess | undefined;
  private readonly connections = new Set<NatsConnection>();
  private constructor(input: {
    root: string;
    serverUrl: string;
    monitorUrl: string;
    users: PathBUsers;
    configText: string;
    binary: string;
  }) {
    this.root = input.root;
    this.serverUrl = input.serverUrl;
    this.monitorUrl = input.monitorUrl;
    this.users = input.users;
    this.configText = input.configText;
    this.binary = input.binary;
  }

  static async create(options: PathBHarnessOptions = {}): Promise<PathBHarness> {
    const binary = resolveNatsBinary(options.binary);
    if (!binary) throw new Error("nats auth Path B test skipped: nats-server executable was not found");
    const parent = fs.realpathSync.native(process.env.TMPDIR?.trim() || os.tmpdir());
    const root = fs.realpathSync.native(fs.mkdtempSync(path.join(parent, "pi-fabric-nats-path-b-")));
    fs.chmodSync(root, 0o700);
    const used = new Set<number>();
    const pairs: KeyPair[] = [];
    let harness: PathBHarness | undefined;
    try {
      const listenPort = await freeLoopbackPort(used);
      const monitorPort = await freeLoopbackPort(used);
      const createIdentity = async (): Promise<PathBIdentity> => {
        if (options.identityFactory) return options.identityFactory(root);
        const operatorIdentity = pairOf(nkeys.createOperator());
        const operatorSigning = pairOf(nkeys.createOperator());
        const systemAccountPair = pairOf(nkeys.createAccount());
        const appAccountPair = pairOf(nkeys.createAccount());
        const agentSigning = pairOf(nkeys.createAccount());
        const maintainerSigning = pairOf(nkeys.createAccount());
        const bootstrapSigning = pairOf(nkeys.createAccount());
        const machineA = pairOf(nkeys.createUser());
        const machineB = pairOf(nkeys.createUser());
        const maintainer = pairOf(nkeys.createUser());
        const bootstrap = pairOf(nkeys.createUser());
        pairs.push(operatorIdentity, operatorSigning, systemAccountPair, appAccountPair, agentSigning, maintainerSigning, bootstrapSigning, machineA, machineB, maintainer, bootstrap);
        const now = Math.floor(Date.now() / 1_000);
        const clock = { nowSeconds: () => Math.floor(Date.now() / 1_000) };
        const systemAccount = systemAccountPair.getPublicKey();
        const appAccount = appAccountPair.getPublicKey();
        const operatorJwt = encodeOperatorJwt(
          { name: "path-b-fixture", signingKeys: [operatorSigning.getPublicKey()], systemAccount },
          signerOf(operatorIdentity),
          clock,
        );
        const systemAccountJwt = encodeAccountJwt(
          { name: "SYS", subject: systemAccount, signingKeys: [], limits: SYSTEM_LIMITS },
          signerOf(operatorSigning),
          clock,
        );
        const agentTemplate = pathBRoleTemplate("agent", "{{tag(machine)}}", PATH_B_MACHINES);
        const scopedKeys: ScopedSigningKey[] = [
          { key: agentSigning.getPublicKey(), role: "agent", template: { ...agentTemplate, pub: { ...agentTemplate.pub, allow: [...agentTemplate.pub.allow, ...(options.additionalAgentPublish ?? [])] } } },
          {
            key: maintainerSigning.getPublicKey(),
            role: "operator",
            template: pathBRoleTemplate("operator", PATH_B_OPERATOR_MACHINE, PATH_B_MACHINES),
          },
          {
            key: bootstrapSigning.getPublicKey(),
            role: "maintainer",
            template: { pub: { allow: [">"], deny: [] }, sub: { allow: ["_INBOX.{{subject()}}.>"] } },
          },
        ];
        const appAccountJwt = encodeAccountJwt(
          { name: "path-b-app", subject: appAccount, signingKeys: scopedKeys, limits: ACCOUNT_LIMITS },
          signerOf(operatorSigning),
          clock,
        );
        const user = (pair: KeyPair, machine: PathBUser["machine"], signer: KeyPair, role: "agent" | "maintainer" | "operator", name: string, fileName: string): PathBUser => {
          const jwt = encodeUserJwt(
            {
              name,
              subject: pair.getPublicKey(),
              issuerAccount: appAccount,
              tags: [`role:${role}`, `machine:${machine}`],
              expiresAt: now + 3_600,
            },
            signerOf(signer),
            clock,
          );
          const credsPath = path.join(root, fileName);
          fs.writeFileSync(credsPath, formatUserCreds(jwt, pair.getSeed()), { flag: "wx", mode: 0o600 });
          fs.chmodSync(credsPath, 0o600);
          return { machine, publicKey: pair.getPublicKey(), credsPath };
        };
        const machineAUser = user(machineA, "machine-a", agentSigning, "agent", "agent/path-b/machine-a", "machine-a.creds");
        const machineBUser = user(machineB, "machine-b", agentSigning, "agent", "agent/path-b/machine-b", "machine-b.creds");
        const maintainerUser = user(maintainer, PATH_B_OPERATOR_MACHINE, maintainerSigning, "operator", "operator/path-b", "maintainer.creds");
        const bootstrapUser = user(bootstrap, PATH_B_OPERATOR_MACHINE, bootstrapSigning, "maintainer", "bootstrap/path-b", "bootstrap.creds");
        return {
          operatorJwt, systemAccount, systemAccountJwt, appAccount, appAccountJwt,
          users: { machineA: machineAUser, machineB: machineBUser, maintainer: maintainerUser, bootstrap: bootstrapUser },
        };
      };
      const { operatorJwt, systemAccount, systemAccountJwt, appAccount, appAccountJwt, users } = await createIdentity();
      const storeDir = path.join(root, "store");
      const resolverDir = path.join(root, "resolver");
      fs.mkdirSync(storeDir, { mode: 0o700 });
      fs.mkdirSync(resolverDir, { mode: 0o700 });
      const configText = renderNatsServerConfig({
        bus: { nodes: 1, replicas: 1 },
        serverName: "path-b-fixture",
        listen: { host: "127.0.0.1", port: listenPort },
        monitor: { host: "127.0.0.1", port: monitorPort },
        jetstream: { storeDir },
        operatorJwt,
        systemAccount,
        resolver: { dir: resolverDir },
        preload: { [systemAccount]: systemAccountJwt, [appAccount]: appAccountJwt },
      });
      if (!configText.includes("sync_interval: always")) throw new Error("Path B harness config is not synchronous");
      const configPath = path.join(root, "nats.conf");
      fs.writeFileSync(configPath, configText, { flag: "wx", mode: 0o600 });
      fs.chmodSync(configPath, 0o600);
      harness = new PathBHarness({
        root,
        serverUrl: `nats://127.0.0.1:${listenPort}`,
        monitorUrl: `http://127.0.0.1:${monitorPort}`,
        users,
        configText,
        binary,
      });
      await harness.start();
      return harness;
    } catch (error) {
      if (harness) await harness.stop();
      else fs.rmSync(root, { recursive: true, force: true });
      if (error instanceof Error && error.message.includes("skipped")) throw error;
      throw new Error(`Path B NATS fixture setup failed: ${error instanceof Error ? error.message : "unknown error"}`);
    } finally {
      for (const pair of pairs) pair.clear();
    }
  }

  async start(): Promise<void> {
    if (this.child && this.child.exitCode === null && this.child.signalCode === null) return;
    const configPath = path.join(this.root, "nats.conf");
    this.child = spawn(this.binary, ["-c", configPath], {
      cwd: this.root,
      env: { PATH: process.env.PATH ?? "", HOME: this.root, TMPDIR: this.root, LANG: "C" },
      stdio: ["ignore", "ignore", "ignore"],
    });
    let spawnFailed = false;
    this.child.once("error", () => { spawnFailed = true; });
    const deadline = Date.now() + START_TIMEOUT_MS;
    while (Date.now() < deadline) {
      if (spawnFailed) throw new Error("nats-server spawn failed");
      if (this.child.exitCode !== null || this.child.signalCode !== null) throw new Error("nats-server exited before readiness");
      const health = await httpJson(`${this.monitorUrl}/healthz?js-enabled-only=true`);
      if (health.status === 200) {
        const varz = await httpJson(`${this.monitorUrl}/varz`);
        const version = varz.value?.version;
        this.serverVersion = typeof version === "string" ? version : "unknown";
        if (EXPECTED_VERSION && this.serverVersion !== EXPECTED_VERSION) {
          throw new Error(`nats-server version mismatch: expected ${EXPECTED_VERSION}, got ${this.serverVersion}`);
        }
        return;
      }
      await sleep(100);
    }
    throw new Error("nats-server did not become JetStream-ready");
  }

  async restart(): Promise<void> {
    await this.closeConnections();
    if (this.child) await stopChild(this.child);
    this.child = undefined;
    await this.start();
  }

  async connect(user: Pick<PathBUser, "publicKey" | "credsPath">): Promise<NatsConnection> {
    const nc = await connect({
      servers: this.serverUrl,
      authenticator: credsAuthenticator(Uint8Array.from(fs.readFileSync(user.credsPath))),
      inboxPrefix: `_INBOX.${user.publicKey}`,
      reconnect: false,
      timeout: API_TIMEOUT_MS,
    });
    this.connections.add(nc);
    return nc;
  }

  async userInfoFieldPaths(user: PathBUser): Promise<string[]> {
    const nc = await this.connect(user);
    try {
      const response = await nc.request("$SYS.REQ.USER.INFO", new Uint8Array(), { timeout: API_TIMEOUT_MS });
      return publicShape(response.json<unknown>());
    } finally {
      this.connections.delete(nc);
      await nc.close();
    }
  }

  async closeConnections(): Promise<void> {
    await Promise.allSettled([...this.connections].map((nc) => nc.close()));
    this.connections.clear();
  }

  async stop(): Promise<void> {
    await this.closeConnections();
    if (this.child) await stopChild(this.child);
    this.child = undefined;
    fs.rmSync(this.root, { recursive: true, force: true });
  }

}

export async function createPathBHarness(options: PathBHarnessOptions = {}): Promise<PathBHarness> {
  return PathBHarness.create(options);
}

export async function openJetStream(nc: NatsConnection): Promise<{ readonly manager: JetStreamManager; readonly js: ReturnType<typeof jetstream> }> {
  return { manager: await jetstreamManager(nc), js: jetstream(nc) };
}