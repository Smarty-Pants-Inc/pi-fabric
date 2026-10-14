import { spawn, type ChildProcess } from "node:child_process";
import { get as httpGet } from "node:http";
import { createServer, type AddressInfo } from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  ReplayPolicy,
  StorageType,
} from "@nats-io/jetstream";
import {
  AuthorizationError,
  connect,
  credsAuthenticator,
  nkeys,
  PermissionViolationError,
  type NatsConnection,
} from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

// Hosted-only #7504 E1: the test owns one operator-mode nats-server child and a private TMPDIR parent.
// This covers the encoder -> account role templates -> rendered 1/1 full-resolver config -> real server path.
// It is not init --local, the state adapter, a cluster, nor the remaining T1/T4/T5/T6/T14/T15/T16 plan.
// No NATS server is started by the test unless NATS_BIN is explicitly supplied. CI requires its pinned 2.14.7 binary.

const NATS_BIN = process.env.NATS_BIN?.trim() || undefined;
if (process.env.NATS_AUTH_REQUIRED === "1" && (!NATS_BIN || !path.isAbsolute(NATS_BIN) || !fs.existsSync(NATS_BIN))) {
  throw new Error("NATS_AUTH_REQUIRED=1 but NATS_BIN is missing, non-absolute, or does not exist");
}

const repoRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const JWT_SOURCE = path.join(repoRoot, "src/mesh/nats-auth/jwt.ts");
const PERMISSIONS_SOURCE = path.join(repoRoot, "src/mesh/nats-auth/permissions.ts");
const CONFIG_SOURCE = path.join(repoRoot, "src/mesh/nats-auth/server-config.ts");
const CUS = "FIXTURE_CUSTODY";
const LIVE = "KV_FIXTURE_LIVENESS";
const STREAMS = { custodyStream: CUS, livenessStream: LIVE };
const MESH_A = "0123456789abcdef0123456789abcdef";
const MESH_B = "fedcba9876543210fedcba9876543210";
const MESH_OTHER = "00112233445566778899aabbccddeeff";
const ROOT_A = `FABRIC_MESH_${MESH_A}`;
const ROOT_B = `FABRIC_MESH_${MESH_B}`;
const ROOT_OTHER = `FABRIC_MESH_${MESH_OTHER}`;
const ROLE_NAMES = ["agent", "principal", "maintainer", "break-glass"] as const;
const REQUIRED_SERVER_VERSION = "2.14.7";
const API_TIMEOUT_MS = 2_500;
const DENIAL_TIMEOUT_MS = 2_000;
const SEED_PATTERN = /\bS[OAU][A-Z2-7]{50,}\b/g;
const JWT_PATTERN = /\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g;
const utf8 = new TextEncoder();

type Role = (typeof ROLE_NAMES)[number];
interface Signer {
  publicKey: string;
  sign(bytes: Uint8Array): Uint8Array;
}
interface Clock {
  nowSeconds(): number;
}
interface RoleTemplate {
  pub: { allow: string[]; deny: string[] };
  sub: { allow: string[]; deny?: string[] };
}
interface ScopedSigningKey {
  key: string;
  role: Role;
  template: RoleTemplate;
  description?: string;
}
interface JwtModule {
  encodeOperatorJwt(input: { name: string; signingKeys: string[]; systemAccount: string }, signer: Signer, clock: Clock): string;
  encodeAccountJwt(
    input: { name: string; subject: string; signingKeys: ScopedSigningKey[] | string[]; limits: Record<string, unknown> },
    signer: Signer,
    clock: Clock,
  ): string;
  encodeUserJwt(
    input: { name: string; subject: string; issuerAccount: string; tags: string[]; expiresAt: number },
    signer: Signer,
    clock: Clock,
  ): string;
  encodeSystemUserJwt(input: { name: string; subject: string; expiresAt: number }, signer: Signer, clock: Clock): string;
  formatUserCreds(jwt: string, seed: Uint8Array): Uint8Array;
}
interface PermissionsModule {
  roleTemplate(role: Role, streams: { custodyStream: string; livenessStream: string }): RoleTemplate;
}
interface ConfigModule {
  renderNatsServerConfig(input: {
    bus: { nodes: number; replicas: number };
    serverName: string;
    listen: { host: string; port: number };
    monitor: { host: string; port: number };
    jetstream: { storeDir: string };
    operatorJwt: string;
    systemAccount: string;
    resolver: { dir: string };
    preload: Record<string, string>;
  }): string;
}
interface AuthModules {
  jwt: JwtModule;
  permissions: PermissionsModule;
  config: ConfigModule;
}
interface KeyPair {
  getPublicKey(): string;
  getSeed(): Uint8Array;
  sign(bytes: Uint8Array): Uint8Array;
  clear(): void;
}
interface UserCreds {
  role: Role;
  publicKey: string;
  jwt: string;
  creds: Uint8Array;
}
interface NatsFixture {
  tempBase: string;
  root: string;
  configText: string;
  serverUrl: string;
  child: ChildProcess;
  operatorJwt: string;
  systemAccount: string;
  systemAccountJwt: string;
  orgAccount: string;
  orgAccountJwt: string;
  users: Record<Role, UserCreds>;
  rolePairs: Record<Role, KeyPair>;
  systemUser: { publicKey: string; creds: Uint8Array };
  decoyInboxKey: KeyPair;
}
interface ClientContext {
  nc: NatsConnection;
  permissions: PermissionOracle;
  manager: Awaited<ReturnType<typeof jetstreamManager>>;
  js: ReturnType<typeof jetstream>;
}

const roleLifetime: Record<Role, number> = {
  agent: 7 * 24 * 60 * 60,
  principal: 90 * 24 * 60 * 60,
  maintainer: 60 * 60,
  "break-glass": 60 * 60,
};
const ACCOUNT_LIMITS = {
  conn: -1,
  leaf: -1,
  subs: -1,
  data: -1,
  payload: -1,
  mem_storage: 0,
  disk_storage: 1_073_741_824,
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

let modules: AuthModules | undefined;
let fixture: NatsFixture | undefined;
const liveConnections = new Set<NatsConnection>();
const ownedPairs: KeyPair[] = [];

function signerOf(pair: KeyPair): Signer {
  return { publicKey: pair.getPublicKey(), sign: (bytes) => pair.sign(bytes) };
}

async function loadSource<T>(source: string): Promise<T> {
  if (!fs.existsSync(source)) {
    throw new Error(`RED (#7504 hosted auth): required core source is absent: ${path.relative(repoRoot, source)}`);
  }
  return (await import(/* @vite-ignore */ source)) as T;
}

function redact(text: string): string {
  return text.replace(SEED_PATTERN, "[seed redacted]").replace(JWT_PATTERN, "[JWT redacted]").slice(-4_000);
}

function assertNoAuthBypass(configText: string): void {
  expect(/^listen:\s+"127\.0\.0\.1:\d+"$/m.test(configText)).toBe(true);
  expect(/^http:\s+"127\.0\.0\.1:\d+"$/m.test(configText)).toBe(true);
  expect(/^  type: full$/m.test(configText)).toBe(true);
  expect(/^resolver_preload \{$/m.test(configText)).toBe(true);
  expect(/^\s*(?:no_auth_user|authorization)\s*:/m.test(configText)).toBe(false);
  expect(/^cluster\s*\{/m.test(configText)).toBe(false);
}

async function freeLoopbackPort(excluded: Set<number>): Promise<number> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const probe = createServer();
    await new Promise<void>((resolve, reject) => {
      probe.once("error", reject);
      probe.listen(0, "127.0.0.1", () => resolve());
    });
    const address = probe.address() as AddressInfo;
    await new Promise<void>((resolve, reject) => probe.close((error) => (error ? reject(error) : resolve())));
    if (!excluded.has(address.port)) {
      excluded.add(address.port);
      return address.port;
    }
  }
  throw new Error("could not reserve distinct loopback ports for the NATS fixture");
}

function httpStatus(url: string): Promise<number> {
  return new Promise((resolve) => {
    const request = httpGet(url, (response) => {
      response.resume();
      resolve(response.statusCode ?? 0);
    });
    request.setTimeout(1_000, () => request.destroy());
    request.once("error", () => resolve(0));
  });
}

function childExited(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve(true);
  return Promise.race([
    new Promise<boolean>((resolve) => child.once("exit", () => resolve(true))),
    sleep(timeoutMs).then(() => false),
  ]);
}

async function stopOwnedChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM"); // This fixture owns exactly this child PID; never signal a process group or search by name.
  if (await childExited(child, 2_000)) return;
  child.kill("SIGKILL");
  if (!(await childExited(child, 2_000))) throw new Error("owned nats-server child did not exit after SIGKILL");
}

function removeOwnedTempParent(root: string, tempBase: string): void {
  const resolvedRoot = fs.realpathSync.native(root);
  const childPrefix = tempBase.endsWith(path.sep) ? tempBase : `${tempBase}${path.sep}`;
  if (resolvedRoot === tempBase || !resolvedRoot.startsWith(childPrefix)) {
    throw new Error("refusing to remove a path outside this fixture's private TMPDIR parent");
  }
  fs.rmSync(resolvedRoot, { recursive: true, force: true });
}

async function createFixture(auth: AuthModules): Promise<NatsFixture> {
  const inheritedTemp = process.env.TMPDIR?.trim() || os.tmpdir();
  const tempBase = fs.realpathSync.native(inheritedTemp);
  const root = fs.realpathSync.native(fs.mkdtempSync(path.join(tempBase, "pi-fabric-nats-auth-")));
  fs.chmodSync(root, 0o700);
  const rootStat = fs.statSync(root);
  if ((rootStat.mode & 0o777) !== 0o700 || (typeof process.getuid === "function" && rootStat.uid !== process.getuid())) {
    removeOwnedTempParent(root, tempBase);
    throw new Error("NATS fixture parent is not private and owned by the current uid");
  }
  if (!/^[\x20-\x7e]+$/.test(root) || root.includes("$")) {
    removeOwnedTempParent(root, tempBase);
    throw new Error("inherited TMPDIR must resolve to a printable-ASCII absolute path for the server config");
  }

  const pairs: KeyPair[] = [];
  let child: ChildProcess | undefined;
  let output = "";
  const appendOutput = (chunk: Buffer): void => {
    if (output.length < 24_000) output += chunk.toString("utf8").slice(0, 24_000 - output.length);
  };
  try {
    const operatorIdentity = nkeys.createOperator() as KeyPair;
    const operatorSigning = nkeys.createOperator() as KeyPair;
    const systemAccountPair = nkeys.createAccount() as KeyPair;
    const orgAccountPair = nkeys.createAccount() as KeyPair;
    const rolePairs: Record<Role, KeyPair> = {
      agent: nkeys.createAccount() as KeyPair,
      principal: nkeys.createAccount() as KeyPair,
      maintainer: nkeys.createAccount() as KeyPair,
      "break-glass": nkeys.createAccount() as KeyPair,
    };
    pairs.push(operatorIdentity, operatorSigning, systemAccountPair, orgAccountPair, ...Object.values(rolePairs));
    ownedPairs.push(...pairs);

    const nowSeconds = Math.floor(Date.now() / 1_000);
    const clock: Clock = { nowSeconds: () => Math.floor(Date.now() / 1_000) };
    const systemAccount = systemAccountPair.getPublicKey();
    const orgAccount = orgAccountPair.getPublicKey();
    const operatorJwt = auth.jwt.encodeOperatorJwt(
      { name: "fixture-operator", signingKeys: [operatorSigning.getPublicKey()], systemAccount },
      signerOf(operatorIdentity),
      clock,
    );
    const systemAccountJwt = auth.jwt.encodeAccountJwt(
      { name: "SYS", subject: systemAccount, signingKeys: [], limits: SYSTEM_LIMITS },
      signerOf(operatorSigning),
      clock,
    );
    const roleSigningKeys: ScopedSigningKey[] = ROLE_NAMES.map((role) => ({
      key: rolePairs[role].getPublicKey(),
      role,
      template: auth.permissions.roleTemplate(role, STREAMS),
    }));
    const orgAccountJwt = auth.jwt.encodeAccountJwt(
      { name: "fixture-org", subject: orgAccount, signingKeys: roleSigningKeys, limits: ACCOUNT_LIMITS },
      signerOf(operatorSigning),
      clock,
    );

    const users = {} as Record<Role, UserCreds>;
    const roleTags: Record<Role, string[]> = {
      agent: ["host:hosted-test", "lane:fixture", "session:t2-agent", "release:fixture", "role:agent", `mesh:${MESH_A}`, `mesh:${MESH_B}`],
      principal: ["role:principal"],
      maintainer: ["role:maintainer"],
      "break-glass": ["role:break-glass"],
    };
    for (const role of ROLE_NAMES) {
      const userPair = nkeys.createUser() as KeyPair;
      ownedPairs.push(userPair);
      const name = role === "agent" ? "agent/hosted-test/fixture/t2-agent" : `${role}/fixture/hosted-auth`;
      const jwt = auth.jwt.encodeUserJwt(
        {
          name,
          subject: userPair.getPublicKey(),
          issuerAccount: orgAccount,
          tags: roleTags[role],
          expiresAt: nowSeconds + roleLifetime[role],
        },
        signerOf(rolePairs[role]),
        clock,
      );
      users[role] = {
        role,
        publicKey: userPair.getPublicKey(),
        jwt,
        creds: auth.jwt.formatUserCreds(jwt, userPair.getSeed()),
      };
    }

    const systemUserPair = nkeys.createUser() as KeyPair;
    const decoyInboxKey = nkeys.createUser() as KeyPair;
    ownedPairs.push(systemUserPair, decoyInboxKey);
    const systemUserJwt = auth.jwt.encodeSystemUserJwt(
      {
        name: "fixture-sys-revoker",
        subject: systemUserPair.getPublicKey(),
        expiresAt: nowSeconds + 60 * 60,
      },
      signerOf(systemAccountPair),
      clock,
    );
    const systemUser = {
      publicKey: systemUserPair.getPublicKey(),
      creds: auth.jwt.formatUserCreds(systemUserJwt, systemUserPair.getSeed()),
    };

    const excluded = new Set<number>();
    const natsPort = await freeLoopbackPort(excluded);
    const monitorPort = await freeLoopbackPort(excluded);
    const storeDir = path.join(root, "store");
    const resolverDir = path.join(root, "resolver");
    fs.mkdirSync(storeDir, { mode: 0o700 });
    fs.mkdirSync(resolverDir, { mode: 0o700 });
    const configText = auth.config.renderNatsServerConfig({
      bus: { nodes: 1, replicas: 1 },
      serverName: "hosted-auth-fixture",
      listen: { host: "127.0.0.1", port: natsPort },
      monitor: { host: "127.0.0.1", port: monitorPort },
      jetstream: { storeDir },
      operatorJwt,
      systemAccount,
      resolver: { dir: resolverDir },
      preload: { [systemAccount]: systemAccountJwt, [orgAccount]: orgAccountJwt },
    });
    assertNoAuthBypass(configText);
    const configPath = path.join(root, "nats.conf");
    fs.writeFileSync(configPath, configText, { flag: "wx", mode: 0o600 });
    fs.chmodSync(configPath, 0o600);

    child = spawn(NATS_BIN as string, ["-c", configPath], {
      cwd: root,
      env: { PATH: process.env.PATH ?? "", HOME: root, TMPDIR: root, LANG: "C" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stdout?.on("data", appendOutput);
    child.stderr?.on("data", appendOutput);

    const monitorUrl = `http://127.0.0.1:${monitorPort}`;
    const deadline = Date.now() + 20_000;
    let ready = false;
    while (Date.now() < deadline) {
      if (child.exitCode !== null || child.signalCode !== null) break;
      if ((await httpStatus(`${monitorUrl}/healthz?js-enabled-only=true`)) === 200) {
        ready = true;
        break;
      }
      await sleep(100);
    }
    if (!ready) {
      throw new Error(`pinned nats-server did not become JetStream-ready: ${redact(output)}`);
    }
    return {
      tempBase,
      root,
      configText,
      serverUrl: `nats://127.0.0.1:${natsPort}`,
      child,
      operatorJwt,
      systemAccount,
      systemAccountJwt,
      orgAccount,
      orgAccountJwt,
      users,
      rolePairs,
      systemUser,
      decoyInboxKey,
    };
  } catch (error) {
    if (child) await stopOwnedChild(child);
    removeOwnedTempParent(root, tempBase);
    const message = error instanceof Error ? `${error.name}: ${redact(error.message)}` : "unknown fixture setup error";
    throw new Error(`hosted NATS auth fixture setup failed: ${message}${output ? `; server output: ${redact(output)}` : ""}`);
  }
}

/** Watches server ERROR frames. A denied publish only passes on the typed, exact server op/subject error. */
class PermissionOracle {
  private readonly waiters: Array<(error: Error) => void> = [];

  constructor(private readonly nc: NatsConnection) {
    void (async () => {
      for await (const status of nc.status()) {
        if (status.type !== "error") continue;
        this.waiters.shift()?.(status.error);
      }
    })().catch(() => undefined);
  }

  async expectViolation(
    operation: "publish" | "subscription",
    subject: string,
    action: () => void,
  ): Promise<void> {
    let deliver!: (error: Error) => void;
    const received = new Promise<Error>((resolve) => {
      deliver = resolve;
      this.waiters.push(resolve);
    });
    try {
      action();
      await this.nc.flush();
      const error = await Promise.race([
        received,
        sleep(DENIAL_TIMEOUT_MS).then(() => {
          throw new Error(`no PermissionViolationError for ${operation} ${subject}; timeout is not a denial`);
        }),
      ]);
      expect(error).toBeInstanceOf(PermissionViolationError);
      if (!(error instanceof PermissionViolationError)) return;
      expect({ operation: error.operation, subject: error.subject }).toEqual({ operation, subject });
    } finally {
      const index = this.waiters.indexOf(deliver);
      if (index >= 0) this.waiters.splice(index, 1);
    }
  }
}

async function openRoleClient(user: UserCreds): Promise<ClientContext> {
  const currentFixture = fixture;
  if (!currentFixture) throw new Error("NATS fixture is not initialized");
  const nc = await connect({
    servers: currentFixture.serverUrl,
    authenticator: credsAuthenticator(user.creds),
    inboxPrefix: `_INBOX.${user.publicKey}`,
    reconnect: false,
    timeout: API_TIMEOUT_MS,
  });
  liveConnections.add(nc);
  const permissions = new PermissionOracle(nc);
  const manager = await jetstreamManager(nc);
  return { nc, permissions, manager, js: jetstream(nc) };
}

async function openSystemClient(): Promise<{ nc: NatsConnection; permissions: PermissionOracle }> {
  const system = fixture?.systemUser;
  if (!system || !fixture) throw new Error("system user fixture is not initialized");
  const nc = await connect({
    servers: fixture.serverUrl,
    authenticator: credsAuthenticator(system.creds),
    inboxPrefix: `_INBOX.${system.publicKey}`,
    reconnect: false,
    timeout: API_TIMEOUT_MS,
  });
  liveConnections.add(nc);
  const permissions = new PermissionOracle(nc);
  return { nc, permissions };
}

async function expectDeniedPublish(client: ClientContext | { nc: NatsConnection; permissions: PermissionOracle }, subject: string, payload = "{}"): Promise<void> {
  await client.permissions.expectViolation("publish", subject, () => client.nc.publish(subject, payload));
}

async function expectDeniedSubscription(
  client: ClientContext | { nc: NatsConnection; permissions: PermissionOracle },
  subject: string,
): Promise<void> {
  await client.permissions.expectViolation("subscription", subject, () => {
    client.nc.subscribe(subject);
  });
}

async function requestObject(nc: NatsConnection, subject: string, payload = "{}"): Promise<Record<string, unknown>> {
  const response = await nc.request(subject, payload, { timeout: API_TIMEOUT_MS });
  const value = response.json<unknown>();
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`expected a JSON object response from ${subject}`);
  }
  return value as Record<string, unknown>;
}

async function assertUserInfo(client: ClientContext): Promise<void> {
  const context = await requestObject(client.nc, "$SYS.REQ.USER.INFO");
  expect(context.server).toBeDefined();
  expect(context.data).toBeDefined();
}

function meshStreamConfig(name: string) {
  return {
    name,
    subjects: [`${name}.state`, `${name}.snapshot`],
    storage: StorageType.File,
    allow_direct: false,
    max_age: 0,
  };
}

function singletonStreamConfig(name: string, subject: string) {
  return { name, subjects: [subject], storage: StorageType.File, allow_direct: false, max_age: 0 };
}

async function seedMeshStream(client: ClientContext, name: string): Promise<void> {
  await client.manager.streams.add(meshStreamConfig(name));
  const ack = await client.js.publish(`${name}.state`, `fixture-seed:${name}`);
  expect(ack.stream).toBe(name);
}

async function assertAgentReadAndConsumer(client: ClientContext, stream: string, durable: string): Promise<void> {
  const info = await client.manager.streams.info(stream);
  expect(info.config.name).toBe(stream);
  const message = await client.manager.streams.getMessage(stream, { seq: 1 });
  expect(message?.string()).toBe(`fixture-seed:${stream}`);

  const created = await client.manager.consumers.add(stream, {
    name: durable,
    durable_name: durable,
    ack_policy: AckPolicy.None,
    deliver_policy: DeliverPolicy.All,
    replay_policy: ReplayPolicy.Instant,
    filter_subject: `${stream}.state`,
  });
  expect(created.name).toBe(durable);
  const consumer = await client.js.consumers.get(stream, durable);
  const next = await consumer.next({ expires: API_TIMEOUT_MS });
  expect(next?.data).toBeDefined();
  expect(next?.string()).toBe(`fixture-seed:${stream}`);
  expect((await client.manager.consumers.info(stream, durable)).name).toBe(durable);
  expect(await client.manager.consumers.delete(stream, durable)).toBe(true);
}

function resignWithScopedPermission(jwt: string, signer: KeyPair): string {
  const [header, payload] = jwt.split(".") as [string, string, string];
  const claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as {
    nats: Record<string, unknown>;
  };
  claims.nats.pub = { allow: [">"] };
  const nextPayload = Buffer.from(JSON.stringify(claims)).toString("base64url");
  const signingInput = `${header}.${nextPayload}`;
  const signature = signer.sign(utf8.encode(signingInput));
  return `${signingInput}.${Buffer.from(signature).toString("base64url")}`;
}

async function refusedConnect(creds: Uint8Array | undefined): Promise<unknown> {
  try {
    const currentFixture = fixture;
    if (!currentFixture) throw new Error("NATS fixture is not initialized");
    const options = {
      servers: currentFixture.serverUrl,
      reconnect: false,
      maxReconnectAttempts: 0,
      timeout: API_TIMEOUT_MS,
      ...(creds ? { authenticator: credsAuthenticator(creds) } : {}),
    };
    const nc = await connect(options);
    liveConnections.add(nc);
    await nc.close();
    liveConnections.delete(nc);
    return undefined;
  } catch (error) {
    return error;
  }
}

async function cleanupFixture(): Promise<void> {
  await Promise.allSettled([...liveConnections].map((nc) => nc.close()));
  liveConnections.clear();
  if (fixture) {
    await stopOwnedChild(fixture.child);
    removeOwnedTempParent(fixture.root, fixture.tempBase);
    fixture = undefined;
  }
}

describe("#7504 hosted T2/T8 NATS auth slice (pinned real server only)", () => {
  if (!NATS_BIN) {
    it.skip("nats auth skipped locally: set NATS_BIN to a real nats-server; NATS_AUTH_REQUIRED=1 makes absence fatal", () => undefined);
    return;
  }

  beforeAll(async () => {
    const loaded: AuthModules = {
      jwt: await loadSource<JwtModule>(JWT_SOURCE),
      permissions: await loadSource<PermissionsModule>(PERMISSIONS_SOURCE),
      config: await loadSource<ConfigModule>(CONFIG_SOURCE),
    };
    modules = loaded;
    fixture = await createFixture(loaded);
  }, 60_000);

  it("T2: real role matrix, private inboxes, and explicit deny controls", async () => {
    if (!fixture || !modules) throw new Error("hosted NATS auth fixture was not initialized");
    const clients = {} as Record<Role, ClientContext>;
    for (const role of ROLE_NAMES) clients[role] = await openRoleClient(fixture.users[role]);

    // Every role authenticates through the encoder and can use its own scoped request/reply inbox.
    for (const role of ROLE_NAMES) {
      const client = clients[role];
      expect(client.nc.info?.auth_required).toBe(true);
      expect(client.nc.info?.version).toBe(REQUIRED_SERVER_VERSION);
      const jsInfo = await client.manager.getAccountInfo();
      expect(jsInfo.limits.max_storage).toBeGreaterThan(0);
      await assertUserInfo(client);
      await requestObject(client.nc, "$JS.API.INFO");
      await expectDeniedSubscription(client, `_INBOX.${fixture.decoyInboxKey.getPublicKey()}.>`);
    }

    const maintainer = clients.maintainer;
    await seedMeshStream(maintainer, ROOT_A);
    await seedMeshStream(maintainer, ROOT_B);
    await seedMeshStream(maintainer, ROOT_OTHER);
    const breakGlass = clients["break-glass"];
    await breakGlass.manager.streams.add(singletonStreamConfig(CUS, `${CUS}.owner`));
    await breakGlass.manager.streams.add(singletonStreamConfig(LIVE, `${LIVE}.signal`));

    // Agent's two independently expanded mesh tags both permit state/snapshot and the full named-consumer read path.
    for (const [index, root] of [ROOT_A, ROOT_B].entries()) {
      const agent = clients.agent;
      await assertAgentReadAndConsumer(agent, root, `agent-proof-${index + 1}`);
      const stateAck = await agent.js.publish(`${root}.state`, `agent-state-${index + 1}`);
      const snapshotAck = await agent.js.publish(`${root}.snapshot`, `agent-snapshot-${index + 1}`);
      expect(stateAck.stream).toBe(root);
      expect(snapshotAck.stream).toBe(root);
    }
    await expectDeniedPublish(clients.agent, `$JS.API.STREAM.INFO.${ROOT_OTHER}`);
    await expectDeniedPublish(clients.agent, `${ROOT_OTHER}.state`, "not-allowed");
    await expectDeniedPublish(clients.agent, `$JS.API.STREAM.CREATE.${ROOT_A}`, JSON.stringify(meshStreamConfig(ROOT_A)));
    await expectDeniedPublish(clients.agent, `$JS.API.STREAM.UPDATE.${ROOT_A}`, JSON.stringify({ description: "agent-update-refused" }));
    await expectDeniedPublish(clients.agent, `$JS.API.STREAM.PURGE.${ROOT_A}`);
    await expectDeniedPublish(clients.agent, `$JS.API.STREAM.DELETE.${ROOT_A}`);

    // Principal is a real reader/listing identity, with state and stream lifecycle writes refused.
    const principal = clients.principal;
    expect((await principal.manager.streams.info(ROOT_A)).config.name).toBe(ROOT_A);
    expect((await principal.manager.streams.getMessage(ROOT_A, { seq: 1 }))?.string()).toBe(`fixture-seed:${ROOT_A}`);
    const names = await principal.manager.streams.names().next();
    expect(names).toContain(ROOT_A);
    await expectDeniedPublish(principal, `${ROOT_A}.state`, "principal-write-refused");
    await expectDeniedPublish(principal, `${ROOT_A}.snapshot`, "principal-snapshot-refused");
    await expectDeniedPublish(principal, `$JS.API.STREAM.CREATE.${ROOT_A}`, JSON.stringify(meshStreamConfig(ROOT_A)));
    await expectDeniedPublish(principal, `$JS.API.STREAM.UPDATE.${ROOT_A}`, JSON.stringify({ description: "must-not-update" }));
    await expectDeniedPublish(principal, `$JS.API.STREAM.PURGE.${ROOT_A}`);
    await expectDeniedPublish(principal, `$JS.API.STREAM.DELETE.${ROOT_A}`);

    // Break-glass has only the narrow CUS/LIVE info/create/update surface; INFO is positive before every refusal.
    for (const stream of [CUS, LIVE]) {
      const before = await breakGlass.manager.streams.info(stream);
      expect(before.config.name).toBe(stream);
      expect(before.config.allow_direct).toBe(false);
      expect(before.config.max_age).toBe(0);
      const updated = await breakGlass.manager.streams.update(stream, { description: "break-glass hosted fixture update" });
      expect(updated.config.name).toBe(stream);
      expect(updated.config.description).toBe("break-glass hosted fixture update");
    }
    await expectDeniedPublish(breakGlass, `$JS.API.STREAM.MSG.GET.${CUS}`, JSON.stringify({ seq: 1 }));
    await expectDeniedPublish(breakGlass, `$JS.API.STREAM.MSG.GET.${LIVE}`, JSON.stringify({ seq: 1 }));
    await expectDeniedPublish(breakGlass, `$JS.API.STREAM.INFO.${ROOT_A}`);
    await expectDeniedPublish(breakGlass, `$JS.API.STREAM.CREATE.${ROOT_A}`, JSON.stringify(meshStreamConfig(ROOT_A)));
    await expectDeniedPublish(breakGlass, `${ROOT_A}.state`, "break-glass-state-refused");
    await expectDeniedPublish(breakGlass, `${ROOT_A}.snapshot`, "break-glass-snapshot-refused");

    // An unknown synthetic debug/attach subject receives no allow-list grant; no attach transport subject is invented.
    const attachProbe = "FABRIC.DEBUG.ATTACH.PROBE";
    for (const role of ROLE_NAMES) {
      const template = modules.permissions.roleTemplate(role, STREAMS);
      const allSubjects = [...template.pub.allow, ...template.pub.deny, ...template.sub.allow, ...(template.sub.deny ?? [])];
      expect(allSubjects.some((subject) => /attach|debug|pi-server|watch|drive/i.test(subject))).toBe(false);
      await expectDeniedPublish(clients[role], attachProbe, "no-attach-grant");
    }

    // Every org role denies CUS DIRECT.GET (the exact and concrete forms), and CUS/LIVE purge/delete.
    // Streams were positively created and inspected above, so a missing-resource response cannot count as denial.
    for (const role of ROLE_NAMES) {
      const client = clients[role];
      await expectDeniedPublish(client, `$JS.API.DIRECT.GET.${CUS}`, JSON.stringify({ seq: 1 }));
      await expectDeniedPublish(client, `$JS.API.DIRECT.GET.${CUS}.1`, JSON.stringify({ seq: 1 }));
      for (const stream of [CUS, LIVE]) {
        await expectDeniedPublish(client, `$JS.API.STREAM.PURGE.${stream}`);
        await expectDeniedPublish(client, `$JS.API.STREAM.DELETE.${stream}`);
      }
      await expectDeniedPublish(client, "$SYS.REQ.CLAIMS.UPDATE", fixture.orgAccountJwt);
      await expectDeniedPublish(client, "$SYS.REQ.CLAIMS.LOOKUP", fixture.orgAccount);
    }

    // The normal three roles cannot create or update either protected stream, including maintainer's broad wildcard allow.
    for (const role of ["agent", "principal", "maintainer"] as const) {
      for (const stream of [CUS, LIVE]) {
        await expectDeniedPublish(clients[role], `$JS.API.STREAM.CREATE.${stream}`, JSON.stringify(singletonStreamConfig(stream, `${stream}.replacement`)));
        await expectDeniedPublish(clients[role], `$JS.API.STREAM.UPDATE.${stream}`, JSON.stringify({ description: "normal-role-update-refused" }));
      }
    }

    // Maintainer alone has actual mesh-root create/update/purge/delete control. These destructive operations target only
    // the temporary ROOT_OTHER stream in this fixture; no local server or non-fixture path is touched.
    const changed = await maintainer.manager.streams.update(ROOT_OTHER, { description: "maintainer lifecycle control" });
    expect(changed.config.description).toBe("maintainer lifecycle control");
    const purge = await maintainer.manager.streams.purge(ROOT_OTHER);
    expect(purge.purged).toBeGreaterThan(0);
    expect(await maintainer.manager.streams.delete(ROOT_OTHER)).toBe(true);
  }, 180_000);

  it("T8 configured single-node operator auth: creds connect, no creds/unknown signer/scoped permissions refuse", async () => {
    if (!fixture || !modules) throw new Error("hosted NATS auth fixture was not initialized");
    assertNoAuthBypass(fixture.configText);
    const client = await openRoleClient(fixture.users.agent);
    expect(client.nc.info?.version).toBe(REQUIRED_SERVER_VERSION);
    expect((await requestObject(client.nc, "$JS.API.INFO")).type).toBeDefined();

    const noCredentials = await refusedConnect(undefined);
    expect(noCredentials).toBeInstanceOf(AuthorizationError);

    const unknownSigner = nkeys.createAccount() as KeyPair;
    const unknownUser = nkeys.createUser() as KeyPair;
    ownedPairs.push(unknownSigner, unknownUser);
    const unknownJwt = modules.jwt.encodeUserJwt(
      {
        name: "agent/hosted-test/fixture/unknown-signer",
        subject: unknownUser.getPublicKey(),
        issuerAccount: fixture.orgAccount,
        tags: ["role:agent", `mesh:${MESH_A}`],
        expiresAt: Math.floor(Date.now() / 1_000) + roleLifetime.agent,
      },
      signerOf(unknownSigner),
      { nowSeconds: () => Math.floor(Date.now() / 1_000) },
    );
    const unknownCreds = modules.jwt.formatUserCreds(unknownJwt, unknownUser.getSeed());
    expect(await refusedConnect(unknownCreds)).toBeInstanceOf(AuthorizationError);

    const scopedProbe = nkeys.createUser() as KeyPair;
    ownedPairs.push(scopedProbe);
    // Re-sign valid user claims with the listed agent scope, adding forbidden user-level permissions.
    const agentScope = fixture.rolePairs.agent;
    const correctlySignedProbe = modules.jwt.encodeUserJwt(
      {
        name: "agent/hosted-test/fixture/scoped-permission-probe",
        subject: scopedProbe.getPublicKey(),
        issuerAccount: fixture.orgAccount,
        tags: ["role:agent", `mesh:${MESH_A}`],
        expiresAt: Math.floor(Date.now() / 1_000) + roleLifetime.agent,
      },
      signerOf(agentScope),
      { nowSeconds: () => Math.floor(Date.now() / 1_000) },
    );
    const invalidScopedJwt = resignWithScopedPermission(correctlySignedProbe, agentScope);
    const invalidScopedCreds = modules.jwt.formatUserCreds(invalidScopedJwt, scopedProbe.getSeed());
    expect(await refusedConnect(invalidScopedCreds)).toBeInstanceOf(AuthorizationError);
  }, 60_000);

  it("T8 SYS-only resolver update permission is real; lookup and unrelated SYS requests are refused", async () => {
    if (!fixture) throw new Error("hosted NATS auth fixture was not initialized");
    const sys = await openSystemClient();
    // The server's resolver update service returns an acknowledgement; the SYS JWT has no lookup or wildcard grant.
    const acknowledgement = await sys.nc.request("$SYS.REQ.CLAIMS.UPDATE", fixture.orgAccountJwt, { timeout: API_TIMEOUT_MS });
    expect(acknowledgement.data.byteLength).toBeGreaterThan(0);
    await expectDeniedPublish(sys, "$SYS.REQ.CLAIMS.LOOKUP", fixture.orgAccount);
    await expectDeniedPublish(sys, "$SYS.REQ.USER.INFO");
    await expectDeniedSubscription(sys, `_INBOX.${fixture.decoyInboxKey.getPublicKey()}.>`);
  }, 30_000);
});

afterAll(async () => {
  await cleanupFixture();
  for (const pair of ownedPairs.splice(0)) pair.clear();
});
