// expected-skip: no nats-server or Windows: the TLS/credential-file Path B acceptance needs a POSIX broker job.
// Keep reasons: #16868 rollback data safety and the runtime-agent USER.INFO/ownership trust boundary.
// The parent runs checks under smarty-test-env/CPUQuota/nice; this file never starts another test runner.
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { transformSync } from "esbuild";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import * as jetstreamSdk from "@nats-io/jetstream";
import type { ConsumerAPI, ConsumerConfig, ConsumerInfo, JetStreamManager, JsMsg } from "@nats-io/jetstream";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { ensureTransportConsumer, ensureTransportStream, INBOX_DUPLICATE_WINDOW_MS, type TransportConsumerPolicy } from "../src/transport/nats/provision.js";
import { hashToken, transportDurableName, transportInboxFilter, transportInboxStream, transportInboxStreamSubject, transportInboxSubject } from "../src/transport/nats/subjects.js";
import { downgradeNatsInbox } from "../tools/nats-inbox-downgrade.js";
import { createPathBHarness, openJetStream, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, PATH_B_POLICY_ENV, resolveNatsBinary, type PathBHarness, type PathBUser } from "./helpers/nats-cluster.js";
import { ReconnectRelay } from "./helpers/nats-reconnect-server.js";

const { AckPolicy, DeliverPolicy, ReplayPolicy, JetStreamApiError } = jetstreamSdk;
const WEEK_NS = 7 * 24 * 60 * 60 * 1_000_000_000;
const ACK_WAIT_NS = 30_000_000_000;
const OLD_REVISION = "ca4b1482119748394df5125c2a8d2853676f7e04";
const OLD_SOURCE_SHA256 = "f7a1afa432d988b96cd25babc74febcddd85e87f0c2316164004508c9379e702";

// Exact complete src/transport/nats/provision.ts from OLD_REVISION, including its real helper,
// error-code handling, constants and types. Retrieved with git show during authoring, not at test time.
// Source: https://github.com/fabric-runtime/pi-fabric/blob/ca4b1482119748394df5125c2a8d2853676f7e04/src/transport/nats/provision.ts
// The decoded UTF-8 string (including its final LF) is byte-pinned above. Do not replace this with
// a locally reimplemented strict check: the actual old executable must reject the migrated durable.
const FROZEN_OLD_SOURCE = `import {
  AckPolicy,
  DeliverPolicy,
  DiscardPolicy,
  JetStreamApiError,
  ReplayPolicy,
  RetentionPolicy,
  StorageType,
  type ConsumerInfo,
  type ConsumerAPI,
  type JetStreamManager,
  type StreamConfig,
  type StreamInfo,
} from "@nats-io/jetstream";

const STREAM_NOT_FOUND = 10059;
const STREAM_EXISTS = 10058;
const CONSUMER_NOT_FOUND = 10014;
const nanos = (ms: number): number => ms * 1_000_000;
/** Preserve the existing inbox deduplication window; longer retries require a separate migration. */
export const INBOX_DUPLICATE_WINDOW_MS = 10 * 60_000;
const code = (error: unknown): number | undefined => {
  if (!(error instanceof JetStreamApiError)) return undefined;
  try { return error.apiError().err_code; } catch { return error.code; }
};

export interface TransportStreamPolicy {
  readonly name: string;
  readonly subjects: readonly string[];
  readonly maxBytes: number;
  readonly maxAgeMs: number;
  readonly maxMessageBytes: number;
  readonly duplicateWindowMs: number;
  readonly replicas?: number;
  readonly discard?: DiscardPolicy;
  /** Existing Fabric event streams may be server-unbounded; publishers still enforce the application cap. */
  readonly allowUnboundedMaxMessageBytes?: boolean;
  /** Explicit legacy compatibility for an existing stream with max_age: 0; never updates it. */
  readonly allowUnboundedMaxAge?: boolean;
}

export const transportStreamConfig = (policy: TransportStreamPolicy): Partial<StreamConfig> => ({
  name: policy.name,
  subjects: [...policy.subjects],
  storage: StorageType.File,
  retention: RetentionPolicy.Limits,
  discard: policy.discard ?? DiscardPolicy.New,
  max_bytes: policy.maxBytes,
  max_age: nanos(policy.maxAgeMs),
  max_msg_size: policy.maxMessageBytes,
  max_msgs: -1,
  max_msgs_per_subject: -1,
  duplicate_window: nanos(policy.duplicateWindowMs),
  num_replicas: policy.replicas ?? 1,
  no_ack: false,
  allow_direct: false,
});

export const assertTransportStream = (info: StreamInfo, wanted: Partial<StreamConfig>, allowUnboundedMaxMessageBytes = false,
  allowUnboundedMaxAge = false): void => {
  const maxMessageBytesMatch = info.config.max_msg_size === wanted.max_msg_size ||
    (allowUnboundedMaxMessageBytes && info.config.max_msg_size === -1);
  const maxAgeMatch = info.config.max_age === wanted.max_age ||
    (allowUnboundedMaxAge && info.config.max_age === 0);
  const same = info.config.storage === wanted.storage &&
    info.config.retention === wanted.retention &&
    info.config.discard === wanted.discard &&
    info.config.max_bytes === wanted.max_bytes &&
    maxAgeMatch &&
    maxMessageBytesMatch &&
    info.config.max_msgs === wanted.max_msgs &&
    info.config.max_msgs_per_subject === wanted.max_msgs_per_subject &&
    info.config.duplicate_window === wanted.duplicate_window &&
    info.config.num_replicas === wanted.num_replicas &&
    (info.config.no_ack ?? false) === (wanted.no_ack ?? false) &&
    (info.config.allow_direct ?? false) === (wanted.allow_direct ?? false) &&
    info.config.subjects.join("\\n") === (wanted.subjects as string[]).join("\\n");
  if (!same) throw new Error(\`NATS stream configuration mismatch: \${info.config.name}\`);
};

export const ensureTransportStream = async (manager: JetStreamManager, policy: TransportStreamPolicy, provision = true): Promise<StreamInfo> => {
  const wanted = transportStreamConfig(policy);
  let info: StreamInfo;
  try { info = await manager.streams.info(policy.name); }
  catch (error) {
    if (code(error) !== STREAM_NOT_FOUND) throw error;
    if (!provision) throw new Error(\`required NATS stream \${policy.name} is not provisioned\`);
    try { info = await manager.streams.add(wanted as StreamConfig & { name: string }); }
    catch (createError) {
      if (code(createError) !== STREAM_EXISTS) throw createError;
      info = await manager.streams.info(policy.name);
    }
  }
  assertTransportStream(info, wanted, policy.allowUnboundedMaxMessageBytes, policy.allowUnboundedMaxAge);
  return info;
};

/** Org-wide Fabric event history. Fabric-store provisions this; runtime opens only verify it. */
export interface NatsEventsStreamOptions {
  readonly org: string;
  /** Pass mesh.nats.provision. Only literal true permits creation; existing streams are never updated. */
  readonly provision?: boolean;
  readonly maxBytes?: number;
  readonly maxMessageBytes?: number;
  readonly duplicateWindowMs?: number;
  readonly replicas?: number;
}

const normalizedNatsOrg = (org: string): string => {
  if (typeof org !== "string" || /^[a-z0-9][a-z0-9-]{0,62}$/.exec(org)?.[0] !== org || org === "sys") {
    throw new Error("NATS events org must be a GitHub-login slug");
  }
  return org.toUpperCase().replaceAll("-", "_");
};

export const natsEventsStreamName = (org: string): string => \`FABRIC_EV_\${normalizedNatsOrg(org)}\`;
export const natsGithubStreamName = (org: string): string => \`GITHUB_\${normalizedNatsOrg(org)}\`;

export const ensureNatsEventsStream = async (manager: JetStreamManager, options: NatsEventsStreamOptions): Promise<StreamInfo> => {
  const name = natsEventsStreamName(options.org);
  const maxBytes = options.maxBytes ?? 2_147_483_648;
  const maxMessageBytes = options.maxMessageBytes ?? 1_048_576;
  const duplicateWindowMs = options.duplicateWindowMs ?? 2 * 60 * 1_000;
  const replicas = options.replicas ?? 1;
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) throw new Error("NATS events maxBytes must be a positive finite safe integer");
  if (!Number.isSafeInteger(maxMessageBytes) || maxMessageBytes <= 0 || maxMessageBytes > maxBytes) {
    throw new Error("NATS events maxMessageBytes must be a positive safe integer no greater than maxBytes");
  }
  if (!Number.isSafeInteger(duplicateWindowMs) || duplicateWindowMs <= 0 || !Number.isSafeInteger(nanos(duplicateWindowMs))) {
    throw new Error("NATS events duplicateWindowMs must be positive and representable in nanoseconds");
  }
  if (!Number.isSafeInteger(replicas) || replicas <= 0) throw new Error("NATS events replicas must be a positive safe integer");
  return ensureTransportStream(manager, {
    name,
    subjects: [\`fabric.ev.\${options.org}.>\`],
    maxBytes,
    maxAgeMs: 0,
    maxMessageBytes,
    duplicateWindowMs,
    replicas,
    discard: DiscardPolicy.New,
    allowUnboundedMaxMessageBytes: true,
  }, options.provision === true);
};

/** BUILD NOW provisioning entry: existing compatible streams are accepted; only an explicit admin path may create. */
export const ensureFabricEventsStream = (manager: JetStreamManager, org: string, provision: boolean): Promise<StreamInfo> =>
  ensureNatsEventsStream(manager, { org, provision });

export interface TransportConsumerPolicy {
  readonly stream: string;
  readonly durable: string;
  readonly filterSubject: string;
  readonly description: string;
  readonly maxAckPending: number;
  /** JetStream nanoseconds; omitted preserves the normal-host inactive_threshold: 0 policy. */
  readonly inactiveThresholdNs?: number;
}

export const ensureTransportConsumer = async (api: ConsumerAPI, policy: TransportConsumerPolicy): Promise<ConsumerInfo> => {
  const backoff = Array.from({ length: 5 }, () => nanos(30_000));
  let info: ConsumerInfo;
  try { info = await api.info(policy.stream, policy.durable); }
  catch (error) {
    if (code(error) !== CONSUMER_NOT_FOUND) throw error;
    try {
      info = await api.add(policy.stream, {
        durable_name: policy.durable,
        description: policy.description,
        filter_subject: policy.filterSubject,
        ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        replay_policy: ReplayPolicy.Instant,
        ack_wait: nanos(30_000),
        max_deliver: 5,
        backoff,
        max_ack_pending: policy.maxAckPending,
        max_waiting: 1,
        inactive_threshold: policy.inactiveThresholdNs ?? 0,
      });
    } catch (createError) {
      // A concurrent opener may have won the create. Read it back only if the winner exists;
      // preserve permission and validation failures instead of masking them as a race.
      try { info = await api.info(policy.stream, policy.durable); }
      catch { throw createError; }
    }
  }
  const actual = info.config;
  const inactiveThresholdMismatch = policy.inactiveThresholdNs === undefined
    ? actual.inactive_threshold !== 0 && actual.inactive_threshold !== undefined
    : actual.inactive_threshold !== policy.inactiveThresholdNs;
  if (actual.durable_name !== policy.durable || actual.filter_subject !== policy.filterSubject || actual.filter_subjects?.length ||
    actual.deliver_subject || actual.headers_only || actual.ack_policy !== AckPolicy.Explicit || actual.replay_policy !== ReplayPolicy.Instant ||
    actual.max_waiting !== 1 || actual.deliver_policy !== DeliverPolicy.All || actual.ack_wait !== nanos(30_000) || actual.max_deliver !== 5 ||
    actual.max_ack_pending !== policy.maxAckPending || inactiveThresholdMismatch ||
    JSON.stringify(actual.backoff) !== JSON.stringify(backoff)) {
    throw new Error(\`NATS consumer configuration mismatch: \${policy.durable}\`);
  }
  return info;
};
`;

const digest = (value: string | Uint8Array): string => createHash("sha256").update(value).digest("hex");
let oldEnsure: typeof ensureTransportConsumer | undefined;
function frozenOldEnsure(): typeof ensureTransportConsumer {
  expect(digest(FROZEN_OLD_SOURCE), "frozen old source must be byte-for-byte, not a strict-check imitation").toBe(OLD_SOURCE_SHA256);
  if (!oldEnsure) {
    const compiled = transformSync(FROZEN_OLD_SOURCE, {
      loader: "ts", format: "cjs", target: "es2022", sourcefile: `${OLD_REVISION}/src/transport/nats/provision.ts`,
    });
    const module = { exports: {} as { ensureTransportConsumer?: typeof ensureTransportConsumer } };
    // Only the actual installed SDK is injectable. No source rewriting or live provision.ts import
    // can change the old function's policy; the CJS transform only erases types/lowers exports.
    const sdkRequire = (id: string): typeof jetstreamSdk => {
      if (id !== "@nats-io/jetstream") throw new Error("unexpected frozen fixture dependency");
      return jetstreamSdk;
    };
    new Function("require", "module", "exports", compiled.code)(sdkRequire, module, module.exports);
    if (typeof module.exports.ensureTransportConsumer !== "function") throw new Error("frozen helper export missing");
    oldEnsure = module.exports.ensureTransportConsumer;
  }
  return oldEnsure;
}

interface Target {
  readonly hostId: string;
  readonly machine: string;
  readonly kind: "rt" | "br";
  readonly policy: TransportConsumerPolicy;
}
function target(kind: "rt" | "br", label: string, machine = "machine-a"): Target {
  const hostId = kind === "br" ? `bridge:${label}` : `session:${label}`;
  return { hostId, machine, kind, policy: {
    stream: transportInboxStream(machine),
    durable: kind === "br" ? `br_${hashToken(hostId)}` : transportDurableName(hostId),
    filterSubject: kind === "br" ? `fabric.in.${machine}.>` : transportInboxFilter(machine, hostId),
    description: kind === "br" ? JSON.stringify({ fabricBridgeLink: label, machine }) : JSON.stringify({ fabricHostId: hostId, machine }),
    maxAckPending: 128,
  } };
}

// Only selected stable broker fields: ConsumerInfo.ts and num_waiting are observation timestamps/
// transient pulls, not cursor state. Do not normalize away config differences except lifetime when
// explicitly comparing across the migration. Zero may be omitted in a real server JSON response.
function snapshot(info: ConsumerInfo, omitLifetime = false) {
  const config = { ...info.config };
  if (omitLifetime) delete config.inactive_threshold;
  return { stream: info.stream_name, name: info.name, created: info.created, config,
    ack_floor: info.ack_floor, delivered: info.delivered,
    num_ack_pending: info.num_ack_pending, num_pending: info.num_pending, num_redelivered: info.num_redelivered };
}
const inactive = (info: ConsumerInfo): number => info.config.inactive_threshold ?? 0;

async function bounded<T>(promise: Promise<T>, label: string, timeoutMs = 5_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}

// Resolve once to an absolute canonical executable, never a shell/PATH lookup per CLI invocation.
// BUN_BIN lets the parent explicitly select its installed pin; no download/install/version probe.
function resolveBunBinary(): string | undefined {
  const explicit = process.versions.bun ? process.execPath : process.env.BUN_BIN;
  const candidates = explicit ? [explicit] : (process.env.PATH ?? "").split(path.delimiter).filter(Boolean).map(dir => path.resolve(dir, "bun"));
  for (const candidate of candidates) {
    if (!path.isAbsolute(candidate)) throw new Error("BUN_BIN must be an absolute executable path");
    try {
      fs.accessSync(candidate, fs.constants.X_OK);
      if (fs.statSync(candidate).isFile()) return fs.realpathSync(candidate);
    } catch { /* Try the next installed candidate, never install one. */ }
  }
  return undefined;
}
const bunBinary = resolveBunBinary();
const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const repoRoot = fileURLToPath(new URL("..", import.meta.url));
const cliEntry = fileURLToPath(new URL("../tools/nats-inbox-downgrade.ts", import.meta.url));

interface CliResult { readonly code: number; readonly stdout: string; readonly stderr: string }
let broker: PathBHarness;
let inspector: JetStreamManager;
let relay: ReconnectRelay;
const runtimeConnections: NatsConnectionHandle[] = [];
async function runtime(user: PathBUser = broker.users.machineA): Promise<NatsConnectionHandle> {
  const connection = await openNatsConnection(broker.runtimeOptions(user));
  runtimeConnections.push(connection);
  expect(connection.isVerified()).toBe(true);
  return connection;
}
function cliArgs(t: Target, apply = false, user = t.machine === "machine-b" ? broker.users.machineB : broker.users.machineA): string[] {
  return ["--host-id", t.hostId, "--machine", t.machine, "--creds", user.credsPath,
    "--ca", broker.tls!.caFile, "--servers", relay.url,
    ...(t.kind === "br" ? ["--kind", "br"] : []), ...(apply ? ["--apply"] : [])];
}
function replaceFlag(argv: readonly string[], flag: string, value: string): string[] {
  const result = [...argv], index = result.indexOf(flag);
  if (index < 0) return [...result, flag, value];
  result[index + 1] = value;
  return result;
}
function noCredentialOutput(result: CliResult): void {
  const output = `${result.stdout}\n${result.stderr}`;
  // Assert booleans so a failing test does not echo a leaked JWT or seed into its diagnostics.
  const secrets = [broker.users.machineA, broker.users.machineB, broker.users.bootstrap, broker.users.maintainer]
    .flatMap(user => fs.readFileSync(user.credsPath, "utf8").match(/[A-Za-z0-9_-]{40,}(?:\.[A-Za-z0-9_-]+)*/g) ?? []);
  expect(secrets.some(secret => output.includes(secret)), "CLI must not disclose credentials").toBe(false);
  expect(/BEGIN NATS USER JWT|BEGIN USER NKEY SEED/.test(output), "CLI must not disclose credential blocks").toBe(false);
}
async function runCli(argv: readonly string[], overrides: NodeJS.ProcessEnv = {}): Promise<CliResult> {
  if (!bunBinary) throw new Error("real downgrade CLI acceptance requires installed Bun (set absolute BUN_BIN)");
  const env: NodeJS.ProcessEnv = { ...process.env, ...PATH_B_POLICY_ENV, HOME: broker.root };
  for (const key of ["PI_FABRIC_MESH_NATS", "PI_FABRIC_NATS_CREDENTIALS", "PI_FABRIC_NATS_SERVERS", "PI_FABRIC_MACHINE",
    "NODE_EXTRA_CA_CERTS", "NODE_TLS_REJECT_UNAUTHORIZED", "SSL_CERT_FILE", "SSL_CERT_DIR"]) delete env[key];
  Object.assign(env, overrides);
  const result = await new Promise<CliResult>((resolve, reject) => {
    execFile(bunBinary, [cliEntry, ...argv], {
      cwd: repoRoot, env, encoding: "utf8", timeout: 15_000, maxBuffer: 64 * 1024, killSignal: "SIGKILL", windowsHide: true,
    }, (error, stdout, stderr) => {
      const failure = error as (Error & { code?: number | string; signal?: string; killed?: boolean }) | null;
      // Never propagate execFile's raw error (it can embed child output/argv). A timeout, signal or
      // output overflow is infrastructure failure, not an expected nonzero refusal.
      if (failure && (typeof failure.code !== "number" || failure.signal || failure.killed)) {
        reject(new Error("downgrade CLI failed to exit normally (diagnostics withheld)"));
      } else resolve({ code: (failure?.code as number | undefined) ?? 0, stdout, stderr });
    });
  });
  noCredentialOutput(result);
  return result;
}
function receipt(result: CliResult, t: Target, mode: "dry-run" | "applied" | "unchanged", before: number): void {
  expect(result.code).toBe(0);
  expect(result.stderr).toBe("");
  expect(JSON.parse(result.stdout)).toMatchObject({ format: 1, mode, stream: t.policy.stream, durable: t.policy.durable,
    beforeInactiveThresholdNs: before, inactiveThresholdNs: mode === "dry-run" ? before : 0, targetInactiveThresholdNs: 0 });
}
function mutationSubjects(subjects: readonly string[]): string[] {
  // Strict read allow-list, not just JS mutation verbs: an accidental fabric/KV publish,
  // ACK or pull must also fail the no-write assertion. CONNECT credentials are never captured.
  return subjects.filter(subject => subject !== "$SYS.REQ.USER.INFO" && subject !== "$JS.API.INFO" &&
    !subject.startsWith("$JS.API.CONSUMER.INFO.") && !subject.startsWith("$JS.API.STREAM.INFO."));
}
async function inspect(t: Target): Promise<ConsumerInfo> {
  return inspector.consumers.info(t.policy.stream, t.policy.durable);
}
async function inventory(stream: string) {
  const result: ReturnType<typeof snapshot>[] = [];
  for await (const info of inspector.consumers.list(stream)) result.push(snapshot(info));
  return result.sort((a, b) => a.name.localeCompare(b.name));
}
async function identity(connection: NatsConnectionHandle) {
  const response = await connection.nc.request("$SYS.REQ.USER.INFO", new Uint8Array(), { timeout: 2_500 });
  const data = response.json<{ data: { user: string; account: string; account_name: string; permissions: {
    publish: { allow: string[]; deny: string[] }; subscribe: { allow: string[]; deny?: string[] }; responses?: unknown;
  } } }>().data;
  // USER.INFO's expires is a remaining-duration countdown, not a changed credential lifetime;
  // permission-list order is broker map iteration order. Preserve every permission field and
  // compare sorted copies, plus signed user/account identity. Credential bytes are pinned below.
  return { user: data.user, account: data.account, account_name: data.account_name, permissions: {
    ...data.permissions,
    publish: { ...data.permissions.publish, allow: [...data.permissions.publish.allow].sort(), deny: [...data.permissions.publish.deny].sort() },
    subscribe: { ...data.permissions.subscribe, allow: [...data.permissions.subscribe.allow].sort(),
      ...(data.permissions.subscribe.deny === undefined ? {} : { deny: [...data.permissions.subscribe.deny].sort() }) },
  } };
}

// Explicit corruption fixtures only. The old-binary round trips below NEVER create their legacy
// consumer with this helper: they execute the hash-pinned old module using real runtime credentials.
function corruptionConfig(t: Target, overrides: Partial<ConsumerConfig> = {}): ConsumerConfig {
  return { durable_name: t.policy.durable, description: t.policy.description, filter_subject: t.policy.filterSubject,
    ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant,
    ack_wait: ACK_WAIT_NS, max_deliver: 5, backoff: Array.from({ length: 5 }, () => ACK_WAIT_NS),
    max_ack_pending: 128, max_waiting: 1, inactive_threshold: WEEK_NS, ...overrides };
}

it("executes the byte-pinned ca4b148 old module without needing Git or another fixture file", () => {
  expect(frozenOldEnsure()).toBeTypeOf("function");
});

describe.skipIf(!binary || process.platform === "win32")("real TLS Path B runtime-agent inbox downgrade", () => {
  beforeAll(async () => {
    broker = await createPathBHarness({ binary: binary!, tls: {} });
    inspector = (await openJetStream(await broker.connect(broker.users.bootstrap))).manager;
    // Bootstrap seeds only infrastructure; round-trip creates, upgrades, publishes, pulls, ACKs
    // and the CLI update all use the harness's unchanged signed runtime-agent credentials.
    for (const machine of PATH_B_MACHINES) await ensureTransportStream(inspector, {
      name: transportInboxStream(machine), subjects: [transportInboxStreamSubject(machine)],
      maxBytes: 1_073_741_824, maxAgeMs: 0, maxMessageBytes: 1_048_576,
      duplicateWindowMs: INBOX_DUPLICATE_WINDOW_MS, replicas: 1,
    });
    relay = await ReconnectRelay.create(broker); // Transparent: no hold/drop/identity substitutions.
  }, 45_000);
  afterEach(async () => {
    try { await Promise.all(runtimeConnections.splice(0).map(connection => connection.close())); }
    finally { if (relay) await bounded(relay.whenIdle(), "CLI TLS sockets close"); }
  }, 15_000);
  afterAll(async () => {
    try { await relay?.stop(); }
    finally { await broker?.stop(); }
  }, 15_000);

  it.each([
    { kind: "rt" as const, machine: "machine-a", inFlight: false },
    { kind: "br" as const, machine: "machine-b", inFlight: true },
  ])("$kind: old creates zero -> new finite -> old honestly rejects -> real CLI rollback -> old drains", async ({ kind, machine, inFlight }) => {
    const t = target(kind, `frozen-rollback-${kind}`, machine);
    const user = machine === "machine-b" ? broker.users.machineB : broker.users.machineA;
    const credentialHash = digest(fs.readFileSync(user.credsPath));
    const connection = await runtime(user);
    const beforeIdentity = await identity(connection);
    expect(beforeIdentity.user).toBe(user.publicKey);
    expect(beforeIdentity.permissions.subscribe.allow).toEqual([`_INBOX.${user.publicKey}.>`]);
    expect(beforeIdentity.permissions.publish.allow.includes(">")).toBe(false);
    expect(beforeIdentity.permissions.publish.allow.includes("$JS.API.>")).toBe(false);
    expect(beforeIdentity.permissions.publish.deny).toContain("$JS.API.STREAM.CREATE.>");

    const old = frozenOldEnsure();
    const created = await old(connection.manager.consumers, t.policy);
    expect(inactive(created)).toBe(0);
    expect(snapshot(await old(connection.manager.consumers, t.policy))).toEqual(snapshot(created));
    // Publish exactly two retained records through the runtime user's source-machine boundary.
    const sent = [];
    for (const label of ["acked", "remaining"]) sent.push(await connection.js.publish(
      transportInboxSubject(machine, t.hostId, "cmd", `${kind}:${label}`, machine),
      new TextEncoder().encode(`${kind}:${label}`), { msgID: `${t.hostId}:${label}` },
    ));
    const consumer = await connection.js.consumers.get(t.policy.stream, t.policy.durable);
    const messages = await consumer.fetch({ max_messages: inFlight ? 2 : 1, expires: 2_000 });
    const iterator = messages[Symbol.asyncIterator]();
    try {
      const first = await bounded(iterator.next(), "old consumer first delivery");
      expect(first.done).toBe(false);
      expect(first.value?.info.streamSequence).toBe(sent[0]!.seq);
      expect(await (first.value as JsMsg).ackAck()).toBe(true);
      if (inFlight) {
        const held = await bounded(iterator.next(), "old consumer unacked delivery");
        expect(held.done).toBe(false);
        expect(held.value?.info.streamSequence).toBe(sent[1]!.seq);
        // Intentionally no ACK/NAK: br proves actual in-flight state survives too.
      }
    } finally {
      messages.stop();
      // stop() queues its finalizer in the SDK's iterator; advance once to execute it
      // before joining closed(). The exact-size fetch cannot prefetch another record.
      expect((await bounded(iterator.next(), "old pull iterator finishes")).done).toBe(true);
      await bounded(messages.closed(), "old pull iterator stops");
      await iterator.return?.();
      await connection.nc.flush();
    }
    const legacy = await inspect(t);
    expect(legacy.ack_floor).toMatchObject({ consumer_seq: 1, stream_seq: sent[0]!.seq });
    expect([legacy.num_ack_pending, legacy.num_pending]).toEqual(inFlight ? [1, 0] : [0, 1]);

    // The real new helper migrates the old cursor, without changing any other broker field.
    await ensureTransportConsumer(connection.manager.consumers, t.policy);
    const migrated = await inspect(t);
    expect(inactive(migrated)).toBe(WEEK_NS);
    expect(snapshot(migrated, true)).toEqual(snapshot(legacy, true));
    await expect(old(connection.manager.consumers, t.policy)).rejects.toThrow(`NATS consumer configuration mismatch: ${t.policy.durable}`);
    expect(snapshot(await inspect(t))).toEqual(snapshot(migrated));

    // Standalone Bun entry, not its exported parser or an in-process fake. TLS is verified on
    // both relay legs; every request and permission check reaches the actual Path B broker.
    const dryStart = relay.subjects.length;
    receipt(await runCli(cliArgs(t)), t, "dry-run", WEEK_NS);
    const drySubjects = relay.subjects.slice(dryStart);
    expect(drySubjects).toContain("$SYS.REQ.USER.INFO");
    expect(drySubjects).toContain(`$JS.API.CONSUMER.INFO.${t.policy.stream}.${t.policy.durable}`);
    expect(mutationSubjects(drySubjects)).toEqual([]);
    expect(snapshot(await inspect(t))).toEqual(snapshot(migrated));

    const beforeInventory = await inventory(t.policy.stream);
    const applyStart = relay.subjects.length;
    // The second positive path also exercises the JSON policy source instead of only env CSV.
    const policyEnv = kind === "br" ? { PI_FABRIC_MESH_NATS: JSON.stringify(PATH_B_PERMISSION_POLICY) } : {};
    receipt(await runCli(cliArgs(t, true), policyEnv), t, "applied", WEEK_NS);
    const applySubjects = relay.subjects.slice(applyStart);
    expect(applySubjects).toContain("$SYS.REQ.USER.INFO");
    const mutations = mutationSubjects(applySubjects);
    expect(mutations).toHaveLength(1);
    expect(mutations[0]!.startsWith(`$JS.API.CONSUMER.CREATE.${t.policy.stream}.${t.policy.durable}`)).toBe(true);
    // SDK update uses CONSUMER.CREATE(action=update), NOT delete/recreate or a new grant.
    expect(applySubjects.some(subject => /CONSUMER\.(DELETE|RESET|PAUSE)|STREAM\.(CREATE|UPDATE|DELETE|PURGE)/.test(subject))).toBe(false);
    const downgraded = await inspect(t); // Independent bootstrap inspection, not the CLI receipt.
    expect(inactive(downgraded)).toBe(0);
    expect(snapshot(downgraded, true)).toEqual(snapshot(legacy, true));
    const afterInventory = await inventory(t.policy.stream);
    const withoutTargetLifetime = (rows: typeof beforeInventory) => rows.map(row => {
      if (row.name !== t.policy.durable) return row;
      const config = { ...row.config }; delete config.inactive_threshold;
      return { ...row, config };
    });
    expect(withoutTargetLifetime(afterInventory)).toEqual(withoutTargetLifetime(beforeInventory));
    expect(snapshot(await old(connection.manager.consumers, t.policy), true)).toEqual(snapshot(legacy, true));
    expect(snapshot(await inspect(t))).toEqual(snapshot(downgraded));

    const unchangedStart = relay.subjects.length;
    receipt(await runCli(cliArgs(t, true)), t, "unchanged", 0);
    expect(mutationSubjects(relay.subjects.slice(unchangedStart))).toEqual([]);
    expect(snapshot(await inspect(t))).toEqual(snapshot(downgraded));
    expect(await identity(connection)).toEqual(beforeIdentity);
    const fresh = await runtime(user); // Also re-run the actual signed-identity/USER.INFO verifier.
    expect(await identity(fresh)).toEqual(beforeIdentity);
    expect(digest(fs.readFileSync(user.credsPath))).toBe(credentialHash);

    // A new pull from the old-compatible durable, not an ACK on a stale in-memory handle.
    // br waits for the real 30-second backoff; no shortened policy, NAK or consumer reset.
    const reopened = await connection.js.consumers.get(t.policy.stream, t.policy.durable);
    const remaining = await bounded(reopened.next({ expires: inFlight ? 40_000 : 2_000 }), "rollback remaining message", 45_000);
    expect(remaining?.info.streamSequence).toBe(sent[1]!.seq);
    expect(new TextDecoder().decode(remaining!.data)).toBe(`${kind}:remaining`);
    if (inFlight) expect(remaining!.info.deliveryCount).toBeGreaterThan(1);
    expect(await remaining!.ackAck()).toBe(true);
    const drained = await inspect(t);
    expect([drained.num_ack_pending, drained.num_pending]).toEqual([0, 0]);
    expect(drained.ack_floor.stream_seq).toBe(sent[1]!.seq);
    expect(drained.created).toBe(legacy.created);
  }, 90_000);

  it("keeps explicit-positive ephemeral policy compatible without reset, and proves zero-reset would break old reopen", async () => {
    // ca4b148 inbox.ts:193-200 already supplies ephemeralInactiveSeconds * 1e9 when
    // ephemeralHost=true. Those explicit-positive policies are NOT newly bounded hosts.
    const t = target("rt", "explicit-positive-ephemeral"), connection = await runtime();
    const explicitPolicy = { ...t.policy, inactiveThresholdNs: 300 * 1_000_000_000 };
    const old = frozenOldEnsure();
    await old(connection.manager.consumers, explicitPolicy);
    const before = await inspect(t);
    expect(inactive(before)).toBe(explicitPolicy.inactiveThresholdNs);
    await ensureTransportConsumer(connection.manager.consumers, explicitPolicy);
    expect(snapshot(await inspect(t))).toEqual(snapshot(before));
    expect(snapshot(await old(connection.manager.consumers, explicitPolicy))).toEqual(snapshot(before));
    expect(snapshot(await inspect(t))).toEqual(snapshot(before));
    // Negative control on this private fixture only: wrongly applying zero-reset breaks the
    // frozen caller's explicit-positive policy. Production must leave these hosts unchanged.
    receipt(await runCli(cliArgs(t, true)), t, "applied", explicitPolicy.inactiveThresholdNs);
    const wronglyReset = await inspect(t);
    expect(inactive(wronglyReset)).toBe(0);
    expect(snapshot(wronglyReset, true)).toEqual(snapshot(before, true));
    await expect(old(connection.manager.consumers, explicitPolicy)).rejects.toThrow(
      `NATS consumer configuration mismatch: ${t.policy.durable}`,
    );
    expect(snapshot(await inspect(t))).toEqual(snapshot(wronglyReset));
  });

  const incompatible: Array<[string, Partial<ConsumerConfig>]> = [
    ["owner", { description: "foreign owner" }],
    ["filter", { filter_subject: "fabric.in.machine-a.>" }],
    ["headers-only", { headers_only: true }],
    ["ack policy", { ack_policy: AckPolicy.All }],
    ["replay policy", { replay_policy: ReplayPolicy.Original }],
    ["max waiting", { max_waiting: 2 }],
    ["deliver policy", { deliver_policy: DeliverPolicy.New }],
    ["ack wait", { ack_wait: 10_000_000_000, backoff: Array.from({ length: 5 }, () => 10_000_000_000) }],
    ["max deliveries", { max_deliver: 6 }],
    ["max ACK pending", { max_ack_pending: 129 }],
    ["backoff", { backoff: [ACK_WAIT_NS] }],
  ];
  it.each(incompatible)("refuses a real foreign/incompatible %s before threshold mutation", async (label, overrides) => {
    const t = target("rt", `bad-${label.replaceAll(" ", "-").toLowerCase()}`);
    await inspector.consumers.add(t.policy.stream, corruptionConfig(t, overrides));
    const before = snapshot(await inspect(t));
    await expect(downgradeNatsInbox(await runtime(), { ...t, apply: true })).rejects.toThrow();
    expect(snapshot(await inspect(t))).toEqual(before);
  });

  it("refuses real multiple-filter and push consumers without touching their lifetimes", async () => {
    const connection = await runtime();
    for (const kind of ["multiple-filters", "push"]) {
      const t = target("rt", kind), config = corruptionConfig(t);
      if (kind === "multiple-filters") {
        delete config.filter_subject;
        config.filter_subjects = [t.policy.filterSubject, transportInboxFilter(t.machine, "another-host")];
      } else {
        delete config.max_waiting;
        config.deliver_subject = `_INBOX.${broker.users.machineA.publicKey}.push`;
        config.deliver_group = "foreign-push-group";
      }
      await inspector.consumers.add(t.policy.stream, config);
      const before = snapshot(await inspect(t));
      await expect(downgradeNatsInbox(connection, { ...t, apply: true })).rejects.toThrow();
      expect(snapshot(await inspect(t))).toEqual(before);
    }
  });

  it.each(["owner", "filter"])("CLI refuses a real bridge %s mismatch without writes", async fault => {
    const t = target("br", `bad-bridge-${fault}`, "machine-b");
    await inspector.consumers.add(t.policy.stream, corruptionConfig(t, fault === "owner"
      ? { description: JSON.stringify({ fabricBridgeLink: "another-link", machine: t.machine }) }
      : { filter_subject: transportInboxFilter(t.machine, t.hostId) }));
    const before = snapshot(await inspect(t)), start = relay.subjects.length;
    expect((await runCli(cliArgs(t, true))).code).not.toBe(0);
    expect(mutationSubjects(relay.subjects.slice(start))).toEqual([]);
    expect(snapshot(await inspect(t))).toEqual(before);
  });

  it.each(["missing", "wrong-name"])("refuses a %s durable, never creating a replacement", async fault => {
    const t = target("rt", `refuse-${fault}`);
    if (fault === "wrong-name") await inspector.consumers.add(t.policy.stream, corruptionConfig(t, {
      durable_name: `rt_${hashToken("not-the-requested-host")}`,
    }));
    const before = await inventory(t.policy.stream), start = relay.subjects.length;
    expect((await runCli(cliArgs(t, true))).code).not.toBe(0);
    expect(mutationSubjects(relay.subjects.slice(start))).toEqual([]);
    expect(await inventory(t.policy.stream)).toEqual(before);
    const missing = await inspect(t).then(() => undefined, (error: unknown) => error);
    expect(missing).toBeInstanceOf(JetStreamApiError);
    if (!(missing instanceof JetStreamApiError)) throw new Error("expected broker consumer-not-found API error");
    expect(missing.apiError()).toMatchObject({ code: 404, err_code: 10014 });
  });

  it("rejects wrong-machine, operator and bootstrap credentials, without borrowing their authority", async () => {
    const t = target("rt", "identity-refusals");
    const connection = await runtime();
    await frozenOldEnsure()(connection.manager.consumers, t.policy);
    await ensureTransportConsumer(connection.manager.consumers, t.policy);
    const before = snapshot(await inspect(t));
    for (const user of [broker.users.machineB, broker.users.maintainer, broker.users.bootstrap]) {
      const start = relay.subjects.length;
      expect((await runCli(cliArgs(t, true, user))).code).not.toBe(0);
      expect(mutationSubjects(relay.subjects.slice(start))).toEqual([]);
      expect(snapshot(await inspect(t))).toEqual(before);
    }
  });

  const invalidArguments: Array<[string, (argv: string[]) => string[]]> = [
    ["unknown flag", argv => [...argv, "--threshold", "0"]],
    ["positional target", argv => [...argv, "another-host"]],
    ["duplicate host", argv => [...argv, "--host-id", "session:other"]],
    ["duplicate kind", argv => [...argv, "--kind", "rt", "--kind", "br"]],
    ["duplicate apply", argv => [...argv, "--apply"]],
    ["missing value", argv => [...argv, "--kind"]],
    ["invalid kind", argv => [...argv, "--kind", "ev"]],
    ["empty host", argv => replaceFlag(argv, "--host-id", "")],
    ["whitespace host", argv => replaceFlag(argv, "--host-id", " session:other")],
    ["br without bridge prefix", argv => [...argv, "--kind", "br"]],
    ["empty bridge link", argv => [...replaceFlag(argv, "--host-id", "bridge:"), "--kind", "br"]],
    ["control-bearing bridge link", argv => [...replaceFlag(argv, "--host-id", "bridge:bad\nlink"), "--kind", "br"]],
    ["duplicate machine", argv => [...argv, "--machine", "machine-b"]],
    ["duplicate server", argv => [...argv, "--servers", relay.url]],
    ["arbitrary durable target", argv => [...argv, "--durable", "another-durable"]],
    ["arbitrary stream target", argv => [...argv, "--stream", "FABRIC_IN_machine-b"]],
    ["invalid machine", argv => replaceFlag(argv, "--machine", "machine-a.>")],
    ["relative creds", argv => replaceFlag(argv, "--creds", "agent.creds")],
    ["relative CA", argv => replaceFlag(argv, "--ca", "ca.pem")],
    ["plaintext server", argv => replaceFlag(argv, "--servers", "nats://localhost:4222")],
    ["empty server item", argv => replaceFlag(argv, "--servers", `${relay.url},`)],
    ["server userinfo", argv => replaceFlag(argv, "--servers", "tls://user:password@localhost:4222")],
    ["system org", argv => [...argv, "--org", "sys"]],
  ];
  it.each(invalidArguments)("CLI rejects %s and leaves an existing finite target untouched", async (_label, corrupt) => {
    const t = target("rt", "invalid-cli-sentinel");
    const connection = await runtime();
    await ensureTransportConsumer(connection.manager.consumers, t.policy);
    const before = await inventory(t.policy.stream), start = relay.subjects.length;
    expect((await runCli(corrupt(cliArgs(t, true)))).code).not.toBe(0);
    expect(relay.subjects.slice(start)).toEqual([]);
    expect(await inventory(t.policy.stream)).toEqual(before);
  });
});

// Narrow boundary tests only: corrupt/stale broker readbacks and verification loss cannot be
// induced by a conforming real server. They do not stand in for either rt/br rollback above.
const boundaryTarget = target("rt", "boundary");
function boundaryInfo(overrides: Partial<ConsumerConfig> = {}): ConsumerInfo {
  return { name: boundaryTarget.policy.durable, stream_name: boundaryTarget.policy.stream,
    created: "2026-01-01T00:00:00.000Z", config: corruptionConfig(boundaryTarget, overrides),
    delivered: { consumer_seq: 2, stream_seq: 2 }, ack_floor: { consumer_seq: 1, stream_seq: 1 },
    num_ack_pending: 1, num_pending: 1, num_redelivered: 0, num_waiting: 0,
  } as ConsumerInfo;
}
function boundary() {
  const initial = boundaryInfo(), after = boundaryInfo({ inactive_threshold: 0 });
  const info = vi.fn(async () => after).mockResolvedValueOnce(initial);
  const update = vi.fn(async () => after), add = vi.fn(async () => after), remove = vi.fn(async () => true);
  const consumers = { info, update, add, delete: remove } as unknown as ConsumerAPI;
  let verified = true;
  const connection = { manager: { consumers } as JetStreamManager, isVerified: () => verified };
  return { initial, after, info, update, add, remove, connection, unverify: () => { verified = false; } };
}
function noCreateDelete(f: ReturnType<typeof boundary>): void {
  expect(f.add).not.toHaveBeenCalled();
  expect(f.remove).not.toHaveBeenCalled();
}

describe("downgrade verification/readback boundary", () => {
  it("updates only inactive_threshold:0, reading independently rather than trusting the update response", async () => {
    const f = boundary();
    f.update.mockResolvedValueOnce(f.initial); // Deliberately stale successful update response.
    await expect(downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true })).resolves.toMatchObject({
      format: 1, mode: "applied", beforeInactiveThresholdNs: WEEK_NS, inactiveThresholdNs: 0, targetInactiveThresholdNs: 0,
    });
    expect(f.update).toHaveBeenCalledExactlyOnceWith(boundaryTarget.policy.stream, boundaryTarget.policy.durable, { inactive_threshold: 0 });
    expect(f.info).toHaveBeenCalledTimes(2);
    noCreateDelete(f);
  });

  it("refuses an unverifiable connection before any broker operation", async () => {
    const f = boundary(); f.unverify();
    await expect(downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true })).rejects.toThrow();
    expect(f.info).not.toHaveBeenCalled(); expect(f.update).not.toHaveBeenCalled(); noCreateDelete(f);
  });
  it("rechecks verification after inspection and before mutation", async () => {
    const f = boundary();
    f.info.mockReset().mockImplementationOnce(async () => { f.unverify(); return f.initial; });
    await expect(downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true })).rejects.toThrow();
    expect(f.update).not.toHaveBeenCalled(); noCreateDelete(f);
  });
  it("does not report applied if verification is lost during the update", async () => {
    const f = boundary();
    f.update.mockImplementationOnce(async () => { f.unverify(); return f.after; });
    await expect(downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true })).rejects.toThrow();
    expect(f.update).toHaveBeenCalledTimes(1); noCreateDelete(f);
  });

  it("does not report applied if verification is lost while reading back", async () => {
    const f = boundary();
    f.info.mockReset().mockResolvedValueOnce(f.initial).mockImplementationOnce(async () => {
      f.unverify(); return f.after;
    });
    await expect(downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true })).rejects.toThrow();
    expect(f.update).toHaveBeenCalledTimes(1); noCreateDelete(f);
  });

  const badReadbacks: Array<[string, Partial<ConsumerConfig>]> = [
    ["threshold", { inactive_threshold: WEEK_NS }],
    ["durable name", { durable_name: "foreign" }],
    ["owner", { description: "foreign owner" }],
    ["filter", { filter_subject: "fabric.in.machine-a.>" }],
    ["multiple filters", { filter_subjects: [boundaryTarget.policy.filterSubject] }],
    ["push delivery", { deliver_subject: "push.inbox" }],
    ["push group", { deliver_group: "foreign-push-group" }],
    ["headers only", { headers_only: true }],
    ["ack policy", { ack_policy: AckPolicy.None }],
    ["replay policy", { replay_policy: ReplayPolicy.Original }],
    ["max waiting", { max_waiting: 2 }],
    ["deliver policy", { deliver_policy: DeliverPolicy.New }],
    ["ack wait", { ack_wait: ACK_WAIT_NS - 1 }],
    ["max deliveries", { max_deliver: 6 }],
    ["max ACK pending", { max_ack_pending: 129 }],
    ["backoff", { backoff: [ACK_WAIT_NS] }],
  ];
  it.each(badReadbacks)("rejects a corrupt post-update %s instead of accepting the successful update response", async (_label, overrides) => {
    const f = boundary();
    f.info.mockReset().mockResolvedValueOnce(f.initial).mockResolvedValueOnce(boundaryInfo({ inactive_threshold: 0, ...overrides }));
    await expect(downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true })).rejects.toThrow();
    expect(f.update).toHaveBeenCalledTimes(1); noCreateDelete(f);
  });
  it.each(["initial info", "update", "readback"])("refuses/redacts %s errors, never falling back to create/delete", async phase => {
    const f = boundary(), privateMarker = "PRIVATE_BROKER_CAUSE_DO_NOT_DISCLOSE";
    const unavailable = new Error(privateMarker);
    if (phase === "initial info") f.info.mockReset().mockRejectedValueOnce(unavailable);
    else if (phase === "update") f.update.mockRejectedValueOnce(unavailable);
    else f.info.mockReset().mockResolvedValueOnce(f.initial).mockRejectedValueOnce(unavailable);
    const failure = await downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true }).then(() => undefined, (error: unknown) => error);
    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message.includes(privateMarker)).toBe(false);
    expect(f.update).toHaveBeenCalledTimes(phase === "initial info" ? 0 : 1); noCreateDelete(f);
  });
  it("snapshots dry-run intent and target before the first await", async () => {
    const f = boundary();
    const options = { hostId: boundaryTarget.hostId, machine: boundaryTarget.machine, kind: "rt" as const, apply: false };
    f.info.mockReset().mockImplementationOnce(async () => {
      options.apply = true; options.hostId = "session:changed-caller"; options.machine = "machine-b";
      return f.initial;
    });
    await expect(downgradeNatsInbox(f.connection, options)).resolves.toMatchObject({
      mode: "dry-run", stream: boundaryTarget.policy.stream, durable: boundaryTarget.policy.durable,
      beforeInactiveThresholdNs: WEEK_NS, inactiveThresholdNs: WEEK_NS,
    });
    expect(f.info).toHaveBeenCalledExactlyOnceWith(boundaryTarget.policy.stream, boundaryTarget.policy.durable);
    expect(f.update).not.toHaveBeenCalled(); noCreateDelete(f);
  });
  it.each([undefined, 0])("accepts server-omitted/zero lifetime %s as an unchanged apply without writes", async threshold => {
    const f = boundary(), existing = boundaryInfo({ inactive_threshold: 0 });
    if (threshold === undefined) delete existing.config.inactive_threshold;
    f.info.mockReset().mockResolvedValueOnce(existing);
    await expect(downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true })).resolves.toMatchObject({
      mode: "unchanged", beforeInactiveThresholdNs: 0, inactiveThresholdNs: 0, targetInactiveThresholdNs: 0,
    });
    expect(f.update).not.toHaveBeenCalled(); noCreateDelete(f);
  });
  it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])("refuses corrupt lifetime %s before any update", async threshold => {
    const f = boundary();
    f.info.mockReset().mockResolvedValueOnce(boundaryInfo({ inactive_threshold: threshold }));
    await expect(downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true })).rejects.toThrow();
    expect(f.update).not.toHaveBeenCalled(); noCreateDelete(f);
  });
  it("does not provision when CONSUMER.INFO says missing", async () => {
    const f = boundary(), missing = new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" });
    f.info.mockReset().mockRejectedValueOnce(missing);
    await expect(downgradeNatsInbox(f.connection, { ...boundaryTarget, apply: true })).rejects.toThrow();
    expect(f.update).not.toHaveBeenCalled(); noCreateDelete(f);
  });
});
