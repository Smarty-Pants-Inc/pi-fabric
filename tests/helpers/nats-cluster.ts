// Path B hosted-test harness. NATS_BIN is preferred and must be an absolute executable path when set;
// otherwise this resolves nats-server from PATH, then the fleet pinned-tool location. The harness owns one
// child process, two loopback ports, a private resolver/store tree, and credential files. The rendered config
// sets JetStream sync_interval: always. restart() reuses the same config/store; stop() is terminal cleanup.
// nats-state-server.ts was inspected: it exports external-server selectors, not readiness/version functions.
// No credential bytes, seeds, JWTs, or identity values are included in errors or public diagnostics.
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { get as httpGet } from "node:http";
import { createServer, type AddressInfo } from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { jetstream, jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import { connect, credsAuthenticator, nkeys, type NatsConnection } from "@nats-io/transport-node";
import { encodeAccountJwt, encodeOperatorJwt, encodeUserJwt, formatUserCreds, type ScopedSigningKey } from "../../src/mesh/nats-auth/jwt.js";
import { PATH_B_OPERATOR_MACHINE, roleTemplateWithPathB, type RoleTemplate } from "../../src/mesh/nats-auth/permissions.js";
import { renderNatsServerConfig } from "../../src/mesh/nats-auth/server-config.js";
import { startTime } from "./owned-processes.js";

export const PATH_B_MACHINES = ["machine-a", "machine-b"] as const;
export type PathBMachine = (typeof PATH_B_MACHINES)[number];
/** Fixed custody/liveness streams and machine inventory used by both the signed templates and runtime verification. */
export const PATH_B_PERMISSION_POLICY = {
  custodyStream: "FABRIC_CUSTODY",
  livenessStream: "FABRIC_LIVENESS",
  pathBMachines: [...PATH_B_MACHINES] as string[],
} as const;
/** Fixed test mesh bound by the signed agent `mesh:` tag (32 lowercase hex). */
export const PATH_B_TEST_MESH = "0123456789abcdef0123456789abcdef";
/** Admin CLI policy environment matching PATH_B_PERMISSION_POLICY (admin may read expected policy from env). */
export const PATH_B_POLICY_ENV: Readonly<Record<string, string>> = {
  PI_FABRIC_NATS_CUSTODY_STREAM: PATH_B_PERMISSION_POLICY.custodyStream,
  PI_FABRIC_NATS_LIVENESS_STREAM: PATH_B_PERMISSION_POLICY.livenessStream,
  PI_FABRIC_NATS_MACHINES: PATH_B_MACHINES.join(","),
};

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
  /** Hand-built principal identity (roleTemplateWithPathB("principal")) for runtime-refusal negatives only. */
  readonly principal?: PathBUser;
  /** Actual system-account credential; never a runtime authority. */
  readonly system?: PathBUser;
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
  /** Additional signed mesh bindings for multi-tag template expansion tests. */
  readonly additionalAgentMeshes?: readonly string[];
  /** Deliberately misissued extra subscribe allows for fail-closed identity-check tests. */
  readonly additionalAgentSubscribe?: readonly string[];
  /** Deliberately widened operator subscriptions; production operator remains metadata-only. */
  readonly additionalOperatorSubscribe?: readonly string[];
  /** Deliberately removed agent publish denies for fail-closed identity-check tests. */
  readonly removeAgentDeny?: readonly string[];
  /** Serve client TLS from an owned ephemeral test CA (openssl subprocess). Server cert SANs default to localhost/127.0.0.1. */
  readonly tls?: { readonly dnsNames?: readonly string[]; readonly ipAddresses?: readonly string[] };
}

export interface PathBTlsFiles {
  readonly caFile: string;
  /** A second, unrelated CA that never signed the server certificate. */
  readonly untrustedCaFile: string;
  /** Fixture server keypair, used only by the test fault relay. */
  readonly certFile: string;
  readonly keyFile: string;
}

function openssl(cwd: string, args: readonly string[]): void {
  execFileSync("openssl", [...args], { cwd, stdio: ["ignore", "ignore", "ignore"], env: { PATH: process.env.PATH ?? "", LANG: "C" } });
}

/** Create a private test CA, an unrelated CA, and a CA-signed server certificate. Keys never leave root. */
function createTestTls(root: string, dnsNames: readonly string[], ipAddresses: readonly string[]): PathBTlsFiles & { certFile: string; keyFile: string } {
  const dir = path.join(root, "tls");
  fs.mkdirSync(dir, { mode: 0o700 });
  const file = (name: string) => path.join(dir, name);
  const ca = (name: string) => openssl(dir, ["req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-days", "1",
    "-subj", `/CN=${name}`, "-keyout", file(`${name}.key`), "-out", file(`${name}.pem`),
    "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign"]);
  ca("path-b-test-ca");
  ca("path-b-untrusted-ca");
  const san = [...dnsNames.map((name) => `DNS:${name}`), ...ipAddresses.map((ip) => `IP:${ip}`)].join(",");
  fs.writeFileSync(file("server.ext"), `basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=${san}\n`, { mode: 0o600 });
  openssl(dir, ["req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-subj", "/CN=path-b-server",
    "-keyout", file("server.key"), "-out", file("server.csr")]);
  openssl(dir, ["x509", "-req", "-in", file("server.csr"), "-CA", file("path-b-test-ca.pem"), "-CAkey", file("path-b-test-ca.key"),
    "-CAcreateserial", "-days", "1", "-extfile", file("server.ext"), "-out", file("server.pem")]);
  return { caFile: file("path-b-test-ca.pem"), untrustedCaFile: file("path-b-untrusted-ca.pem"), certFile: file("server.pem"), keyFile: file("server.key") };
}

function faultedAgentTemplate(options: PathBHarnessOptions): RoleTemplate {
  const base = roleTemplateWithPathB("agent", { custodyStream: PATH_B_PERMISSION_POLICY.custodyStream, livenessStream: PATH_B_PERMISSION_POLICY.livenessStream }, PATH_B_MACHINES);
  const removed = new Set(options.removeAgentDeny ?? []);
  return {
    pub: { allow: [...base.pub.allow, ...(options.additionalAgentPublish ?? [])], deny: base.pub.deny.filter((subject) => !removed.has(subject)) },
    sub: { allow: [...base.sub.allow, ...(options.additionalAgentSubscribe ?? [])] },
  };
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

/** Deadlines observe rejection immediately and never leave their timer behind.
 * A timeout is a failure, not cancellation: callers still owe native cleanup in finally.
 */
export async function teardownWithin<T>(label: string, operation: () => Promise<T>, timeoutMs = 1_000): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([Promise.resolve().then(operation), new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}

async function childExited(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (!child.pid || child.exitCode !== null || child.signalCode !== null) return true;
  return new Promise(resolve => {
    const finish = (exited: boolean) => { clearTimeout(timer); child.off("exit", exitedListener); resolve(exited); };
    const exitedListener = () => finish(true);
    const timer = setTimeout(() => finish(false), timeoutMs);
    child.once("exit", exitedListener);
    if (child.exitCode !== null || child.signalCode !== null) finish(true);
  });
}

const childBirths = new WeakMap<ChildProcess, string>();
async function stopChild(child: ChildProcess): Promise<void> {
  if (!child.pid || child.exitCode !== null || child.signalCode !== null) return;
  const signal = (value: NodeJS.Signals) => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    if (process.platform === "linux" && (!childBirths.get(child) || startTime(child.pid!) !== childBirths.get(child))) {
      throw new Error("owned nats-server birth is unconfirmed; refusing signal");
    }
    child.kill(value); // Direct captured child only, never a host-wide/group/name kill.
  };
  signal("SIGTERM");
  if (await childExited(child, 2_000)) return;
  signal("SIGKILL");
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
  readonly tls: PathBTlsFiles | undefined;
  serverVersion = "";
  private child: ChildProcess | undefined;
  get processId(): number | undefined { return this.child?.pid; }
  private readonly connections = new Set<NatsConnection>();
  private constructor(input: {
    root: string;
    serverUrl: string;
    monitorUrl: string;
    users: PathBUsers;
    configText: string;
    binary: string;
    tls?: PathBTlsFiles;
  }) {
    this.tls = input.tls;
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
        const systemSigning = pairOf(nkeys.createAccount());
        const appAccountPair = pairOf(nkeys.createAccount());
        const agentSigning = pairOf(nkeys.createAccount());
        const maintainerSigning = pairOf(nkeys.createAccount());
        const bootstrapSigning = pairOf(nkeys.createAccount());
        const machineA = pairOf(nkeys.createUser());
        const machineB = pairOf(nkeys.createUser());
        const maintainer = pairOf(nkeys.createUser());
        const bootstrap = pairOf(nkeys.createUser());
        const principalSigning = pairOf(nkeys.createAccount());
        const principal = pairOf(nkeys.createUser());
        const systemUser = pairOf(nkeys.createUser());
        pairs.push(systemUser, systemSigning, operatorIdentity, operatorSigning, systemAccountPair, appAccountPair, agentSigning, maintainerSigning, bootstrapSigning, machineA, machineB, maintainer, bootstrap, principalSigning, principal);
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
          { name: "SYS", subject: systemAccount, signingKeys: [systemSigning.getPublicKey()], limits: SYSTEM_LIMITS },
          signerOf(operatorSigning),
          clock,
        );
        // Fully merged #1276 templates (no copied grants); only the explicit fault options perturb the agent template.
        const streams = { custodyStream: PATH_B_PERMISSION_POLICY.custodyStream, livenessStream: PATH_B_PERMISSION_POLICY.livenessStream };
        const operatorTemplate = roleTemplateWithPathB("operator", streams, PATH_B_MACHINES);
        const scopedKeys: ScopedSigningKey[] = [
          { key: agentSigning.getPublicKey(), role: "agent", template: faultedAgentTemplate(options) },
          {
            key: maintainerSigning.getPublicKey(),
            role: "operator",
            template: { ...operatorTemplate, sub: { allow: [...operatorTemplate.sub.allow, ...(options.additionalOperatorSubscribe ?? [])] } },
          },
          { key: principalSigning.getPublicKey(), role: "principal", template: roleTemplateWithPathB("principal", streams, PATH_B_MACHINES) },
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
        const user = (pair: KeyPair, machine: PathBUser["machine"], signer: KeyPair, role: "agent" | "maintainer" | "operator" | "principal", name: string, fileName: string): PathBUser => {
          const tags = role === "agent"
            ? [`role:${role}`, `machine:${machine}`, ...[PATH_B_TEST_MESH, ...(options.additionalAgentMeshes ?? [])].map((mesh) => `mesh:${mesh}`)]
            : [`role:${role}`, `machine:${machine}`];
          const jwt = encodeUserJwt(
            {
              name,
              subject: pair.getPublicKey(),
              issuerAccount: appAccount,
              tags,
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
        // Principal carries a machine tag for a runtime machine so a refusal proves role, not tag, checking.
        const principalUser = user(principal, "machine-a", principalSigning, "principal", "principal/path-b", "principal.creds");
        const systemJwt = encodeUserJwt({ name: "sys/path-b", subject: systemUser.getPublicKey(), issuerAccount: systemAccount,
          tags: ["role:sys", "machine:machine-a"], expiresAt: now + 3_600 }, signerOf(systemSigning), clock);
        const systemCreds = path.join(root, "system.creds");
        fs.writeFileSync(systemCreds, formatUserCreds(systemJwt, systemUser.getSeed()), { flag: "wx", mode: 0o600 });
        const system: PathBUser = { machine: "machine-a", publicKey: systemUser.getPublicKey(), credsPath: systemCreds };
        return {
          operatorJwt, systemAccount, systemAccountJwt, appAccount, appAccountJwt,
          users: { machineA: machineAUser, machineB: machineBUser, maintainer: maintainerUser, bootstrap: bootstrapUser, principal: principalUser, system },
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
      const tlsFiles = options.tls
        ? createTestTls(root, options.tls.dnsNames ?? ["localhost"], options.tls.ipAddresses ?? ["127.0.0.1"])
        : undefined;
      // Client TLS is fixture-owned config only; the product renderer has no client TLS block.
      const fullConfig = tlsFiles
        ? `${configText}\ntls {\n  cert_file: ${JSON.stringify(tlsFiles.certFile)}\n  key_file: ${JSON.stringify(tlsFiles.keyFile)}\n  timeout: 2\n}\n`
        : configText;
      const configPath = path.join(root, "nats.conf");
      fs.writeFileSync(configPath, fullConfig, { flag: "wx", mode: 0o600 });
      fs.chmodSync(configPath, 0o600);
      harness = new PathBHarness({
        root,
        serverUrl: `${tlsFiles ? "tls" : "nats"}://127.0.0.1:${listenPort}`,
        monitorUrl: `http://127.0.0.1:${monitorPort}`,
        users,
        configText: fullConfig,
        binary,
        ...(tlsFiles ? { tls: {
          caFile: tlsFiles.caFile, untrustedCaFile: tlsFiles.untrustedCaFile,
          certFile: tlsFiles.certFile, keyFile: tlsFiles.keyFile,
        } } : {}),
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
    if (process.platform === "linux" && this.child.pid) childBirths.set(this.child, startTime(this.child.pid));
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

  /** Captured native child handle for teardown exit evidence (not just logical idle). */
  get serverProcess(): ChildProcess | undefined { return this.child; }

  async restart(): Promise<void> {
    try { await this.closeConnections(); }
    finally {
      if (this.child) await stopChild(this.child);
      this.child = undefined;
    }
    await this.start();
  }

  async connect(user: Pick<PathBUser, "publicKey" | "credsPath">): Promise<NatsConnection> {
    const nc = await connect({
      servers: this.serverUrl,
      authenticator: credsAuthenticator(Uint8Array.from(fs.readFileSync(user.credsPath))),
      inboxPrefix: `_INBOX.${user.publicKey}`,
      reconnect: false,
      timeout: API_TIMEOUT_MS,
      ...(this.tls ? { tls: { caFile: this.tls.caFile } } : {}),
    });
    this.connections.add(nc);
    return nc;
  }

  /** Runtime (agent) options: TLS fixtures use the certificate DNS identity; default raw auth fixtures stay plaintext. */
  runtimeOptions(user: Pick<PathBUser, "machine" | "credsPath">) {
    const port = new URL(this.serverUrl).port;
    return {
      servers: this.tls ? `tls://localhost:${port}` : this.serverUrl,
      credentials: user.credsPath,
      machine: user.machine,
      ...PATH_B_PERMISSION_POLICY,
      ...(this.tls ? { tls: { caFile: this.tls.caFile } } : {}),
    };
  }

  /** Report-only admin connection options: explicit role:'operator' with the reserved operator machine. */
  operatorOptions(user: Pick<PathBUser, "machine" | "credsPath"> = this.users.maintainer) {
    return { ...this.runtimeOptions(user), role: "operator" as const };
  }

  /** Admin CLI environment; TLS callers supply the CA path explicitly on the command line. */
  adminEnv(user: Pick<PathBUser, "machine" | "credsPath">): Record<string, string> {
    const port = new URL(this.serverUrl).port;
    return { ...PATH_B_POLICY_ENV, PI_FABRIC_NATS_SERVERS: this.tls ? `tls://localhost:${port}` : this.serverUrl, PI_FABRIC_NATS_CREDENTIALS: user.credsPath, PI_FABRIC_MACHINE: user.machine };
  }

  async userInfoFieldPaths(user: PathBUser): Promise<string[]> {
    const nc = await this.connect(user);
    try {
      const response = await nc.request("$SYS.REQ.USER.INFO", new Uint8Array(), { timeout: API_TIMEOUT_MS });
      return publicShape(response.json<unknown>());
    } finally {
      this.connections.delete(nc);
      await teardownWithin("NATS user-info connection close", () => nc.close());
    }
  }

  async closeConnections(): Promise<void> {
    const connections = [...this.connections];
    this.connections.clear();
    const results = await Promise.allSettled(connections.map(nc => teardownWithin("NATS connection close", () => nc.close())));
    const failures = results.flatMap(result => result.status === "rejected" ? [result.reason] : []);
    if (failures.length) throw new AggregateError(failures, failures.map(String).join("; "));
  }

  async stop(): Promise<void> {
    const failures: unknown[] = [];
    try { await this.closeConnections(); } catch (error) { failures.push(error); }
    finally {
      try { if (this.child) await stopChild(this.child); } catch (error) { failures.push(error); }
      finally {
        if (!this.child || this.child.exitCode !== null || this.child.signalCode !== null) this.child = undefined;
        // Even a failed native/connection join cannot bypass private-root cleanup.
        try { fs.rmSync(this.root, { recursive: true, force: true }); } catch (error) { failures.push(error); }
      }
    }
    if (failures.length) throw new AggregateError(failures, failures.map(String).join("; "));
  }

}

export async function createPathBHarness(options: PathBHarnessOptions = {}): Promise<PathBHarness> {
  return PathBHarness.create(options);
}

export async function openJetStream(nc: NatsConnection): Promise<{ readonly manager: JetStreamManager; readonly js: ReturnType<typeof jetstream> }> {
  return { manager: await jetstreamManager(nc), js: jetstream(nc) };
}