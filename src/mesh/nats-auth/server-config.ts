import { nkeys } from "@nats-io/transport-node";

// smarty-dev#7504 phase 1 slice 1: pure nats-server configuration renderer (design c4d26466 sections 3 and 9.1, Q5).
// Operator mode with the full account resolver, the SYS account preloaded, JetStream with sync_interval always.
// Topology comes only from the configured `bus`; nothing here probes hosts. Supported: bus 1/1 (one loopback node,
// no cluster) and bus 3/3 (configured routes with mutual TLS and known-URL checking, no route password). Stream
// replicas are not part of the server config: the adapter checks them against the configuration (smarty-dev#11373).
// The input carries public material only (public keys, public operator/account JWTs). Errors name the field and never
// echo a JWT or any other value. No file system access, no clock, no process spawn.

export interface NatsHostPort {
  host: string;
  port: number;
}

export interface NatsBusTopology {
  /** Configured node count: 1 (one machine) or 3 (fleet). Never derived from a runtime host count. */
  nodes: number;
  /** Configured stream replicas: equal to nodes (1/1 or 3/3); validated, never lowered, not rendered. */
  replicas: number;
}

export interface NatsResolverConfig {
  /** Absolute resolver directory, caller-chosen (no HOME default). */
  dir: string;
  /** Go duration such as "2m"; omitted means the server default. */
  interval?: string;
  /** Go duration such as "1900ms"; omitted means the server default. */
  timeout?: string;
}

export interface NatsRouteTls {
  certFile: string;
  keyFile: string;
  caFile: string;
}

export interface NatsClusterConfig {
  /** Cluster name, identical on every node. */
  name: string;
  listen: NatsHostPort;
  /** Every configured node including this one, as nats-route://<dns-name>:<port>; the same list on every node. */
  routes: string[];
  tls: NatsRouteTls;
}

export interface NatsServerConfigInput {
  bus: NatsBusTopology;
  serverName: string;
  listen: NatsHostPort;
  monitor?: NatsHostPort;
  jetstream: { storeDir: string };
  /** The PUBLIC operator JWT, inlined (never a file path). */
  operatorJwt: string;
  /** The SYS account public key (A…); must equal the operator JWT's system_account. */
  systemAccount: string;
  resolver: NatsResolverConfig;
  /** Account public key (A…) -> public account JWT. Must contain systemAccount. */
  preload: Record<string, string>;
  /** Required for bus 3/3, refused for bus 1/1. */
  cluster?: NatsClusterConfig;
}

export class NatsServerConfigError extends Error {
  readonly field: string;
  constructor(field: string, reason: string) {
    super(`invalid NATS server config field ${field}: ${reason}`);
    this.name = "NatsServerConfigError";
    this.field = field;
  }
}

const SUPPORTED_BUS = new Set(["1/1", "3/3"]);
const NAME = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
const DURATION = /^[1-9][0-9]{0,8}(ms|s|m|h)$/;
const PUBLIC_KEY = /^[A-Z2-7]{56}$/;
const JWT_SEGMENT = /^[A-Za-z0-9_-]+$/;
const DNS_LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const ROUTE = /^nats-route:\/\/([^:/@[\]]+):([0-9]{1,5})$/;
const MAX_JWT_BYTES = 1 << 20;

function fail(field: string, reason: string): never {
  throw new NatsServerConfigError(field, reason);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function record(value: unknown, field: string, required: readonly string[], optional: readonly string[] = []) {
  if (!isRecord(value)) return fail(field, "must be an object");
  const allowed = new Set([...required, ...optional]);
  const prefix = field === "" ? "" : `${field}.`;
  for (const key of Object.keys(value)) if (!allowed.has(key)) fail(`${prefix}<unsupported>`, "is not a supported field");
  for (const key of required) if (value[key] === undefined) fail(`${prefix}${key}`, "is required");
  return value;
}

function text(value: unknown, field: string): string {
  return typeof value === "string" ? value : fail(field, "must be a string");
}

function name(value: unknown, field: string): string {
  const s = text(value, field);
  return NAME.test(s) ? s : fail(field, "must be 1-64 characters of [A-Za-z0-9_-], starting with a letter or digit");
}

/** Absolute, normalized, printable-ASCII path without variables; rendered JSON-quoted. */
function absolutePath(value: unknown, field: string): string {
  const s = text(value, field);
  if (!/^[\x20-\x7e]+$/.test(s) || s.includes("$")) fail(field, "must be printable ASCII without '$'");
  if (!s.startsWith("/")) fail(field, "must be an absolute path (no HOME or relative default)");
  const segments = s.slice(1).split("/");
  if (segments.some((segment) => segment === "" || segment === "." || segment === "..")) {
    fail(field, "must be a normalized path without empty, '.' or '..' segments");
  }
  return s;
}

function port(value: unknown, field: string): number {
  return typeof value === "number" && Number.isInteger(value) && value >= 1 && value <= 65_535
    ? value
    : fail(field, "must be an integer port 1-65535");
}

function ipv4(host: string): number[] | undefined {
  const parts = host.split(".");
  if (parts.length !== 4 || !parts.every((part) => /^(0|[1-9][0-9]{0,2})$/.test(part))) return undefined;
  const octets = parts.map(Number);
  return octets.every((octet) => octet <= 255) ? octets : undefined;
}

function dnsName(host: string): boolean {
  if (host.length === 0 || host.length > 253) return false;
  const labels = host.split(".");
  return labels.every((label) => DNS_LABEL.test(label)) && !/^[0-9]+$/.test(labels[labels.length - 1] as string);
}

function hostPort(value: unknown, field: string, loopbackOnly: boolean): NatsHostPort {
  const v = record(value, field, ["host", "port"]);
  const host = text(v.host, `${field}.host`);
  const octets = ipv4(host);
  if (loopbackOnly) {
    if (octets?.[0] !== 127) fail(`${field}.host`, "must be an IPv4 loopback literal on bus 1/1");
  } else if (octets ? octets.every((octet) => octet === 0) || octets[0] === 255 : !dnsName(host)) {
    fail(`${field}.host`, "must be a non-wildcard IPv4 literal or a lowercase DNS name");
  }
  return { host, port: port(v.port, `${field}.port`) };
}

function duration(value: unknown, field: string): string {
  const s = text(value, field);
  return DURATION.test(s) ? s : fail(field, "must be a Go duration such as 2m or 1900ms");
}

function publicKey(value: unknown, prefix: "O" | "A"): boolean {
  if (typeof value !== "string" || !PUBLIC_KEY.test(value) || value[0] !== prefix) return false;
  try {
    nkeys.fromPublic(value);
    return true;
  } catch {
    return false;
  }
}

interface VerifiedJwt {
  iss: string;
  sub: string;
  nats: Record<string, unknown>;
}

/** Decode and verify a public NATS v2 JWT with the installed nkeys; never echoes the token. */
function verifiedJwt(value: unknown, field: string, type: "operator" | "account"): VerifiedJwt {
  const token = text(value, field);
  if (token.length > MAX_JWT_BYTES || !token.startsWith("eyJ")) fail(field, "must be an inline JWT, not a file path");
  const segments = token.split(".");
  if (segments.length !== 3 || !segments.every((segment) => JWT_SEGMENT.test(segment))) {
    fail(field, "must be three unpadded base64url segments");
  }
  const [h, p] = segments as [string, string, string];
  const bytes = segments.map((segment) => Buffer.from(segment, "base64url"));
  if (bytes.some((decoded, index) => decoded.toString("base64url") !== segments[index])) {
    fail(field, "must use canonical base64url");
  }
  let header: unknown;
  let payload: unknown;
  try {
    header = JSON.parse((bytes[0] as Buffer).toString("utf8"));
    payload = JSON.parse((bytes[1] as Buffer).toString("utf8"));
  } catch {
    return fail(field, "header or payload is not JSON");
  }
  if (!isRecord(header) || String(header.typ).toLowerCase() !== "jwt" || header.alg !== "ed25519-nkey") {
    fail(field, "header must be typ JWT, alg ed25519-nkey");
  }
  if (!isRecord(payload) || !isRecord(payload.nats)) return fail(field, "payload has no nats claims");
  const nats = payload.nats;
  if (nats.version !== 2 || nats.type !== type) fail(field, `must be a version 2 ${type} JWT`);
  const { iss, sub } = payload;
  if (!publicKey(iss, "O")) fail(field, "issuer must be an operator public key");
  if (!publicKey(sub, type === "operator" ? "O" : "A")) fail(field, `subject must be an ${type} public key`);
  const signature = bytes[2] as Buffer;
  let verified = false;
  try {
    verified =
      signature.length === 64 &&
      nkeys.fromPublic(iss as string).verify(new TextEncoder().encode(`${h}.${p}`), new Uint8Array(signature));
  } catch {
    verified = false;
  }
  if (!verified) fail(field, "signature does not verify against the issuer");
  return { iss: iss as string, sub: sub as string, nats };
}

/**
 * Validate the configuration and return a normalized copy. Throws NatsServerConfigError naming the first bad field.
 * The checks are static against the configuration; none depends on the running host.
 */
export function validateNatsServerConfigInput(input: unknown): NatsServerConfigInput {
  const v = record(
    input,
    "",
    ["bus", "serverName", "listen", "jetstream", "operatorJwt", "systemAccount", "resolver", "preload"],
    ["monitor", "cluster"],
  );

  const busRecord = record(v.bus, "bus", ["nodes", "replicas"]);
  const nodes = busRecord.nodes;
  const replicas = busRecord.replicas;
  if (typeof nodes !== "number" || !Number.isInteger(nodes)) fail("bus.nodes", "must be an integer");
  if (typeof replicas !== "number" || !Number.isInteger(replicas)) fail("bus.replicas", "must be an integer");
  if (!SUPPORTED_BUS.has(`${nodes}/${replicas}`)) {
    fail("bus", "supported configurations are nodes 1 replicas 1, and nodes 3 replicas 3");
  }
  const bus: NatsBusTopology = { nodes: nodes as number, replicas: replicas as number };
  const single = bus.nodes === 1;

  const serverName = name(v.serverName, "serverName");
  const listen = hostPort(v.listen, "listen", single);
  const monitor = v.monitor === undefined ? undefined : hostPort(v.monitor, "monitor", single);
  if (monitor && monitor.port === listen.port) fail("monitor.port", "must differ from listen.port");

  const jetstreamRecord = record(v.jetstream, "jetstream", ["storeDir"]);
  const storeDir = absolutePath(jetstreamRecord.storeDir, "jetstream.storeDir");

  const operator = verifiedJwt(v.operatorJwt, "operatorJwt", "operator");
  if (operator.iss !== operator.sub) fail("operatorJwt", "must be self-signed by the operator identity key");
  const operatorSigningKeys = operator.nats.signing_keys ?? [];
  if (!Array.isArray(operatorSigningKeys) || !operatorSigningKeys.every((key) => publicKey(key, "O"))) {
    fail("operatorJwt", "signing_keys must be operator public keys");
  }
  const trustedIssuers = new Set<string>([operator.sub, ...(operatorSigningKeys as string[])]);

  const systemAccount = text(v.systemAccount, "systemAccount");
  if (!publicKey(systemAccount, "A")) fail("systemAccount", "must be an account public key");
  if (operator.nats.system_account !== systemAccount) {
    fail("systemAccount", "must equal the operator JWT system_account");
  }

  const resolverRecord = record(v.resolver, "resolver", ["dir"], ["interval", "timeout"]);
  const resolver: NatsResolverConfig = { dir: absolutePath(resolverRecord.dir, "resolver.dir") };
  if (resolver.dir === storeDir) fail("resolver.dir", "must differ from jetstream.storeDir");
  if (resolverRecord.interval !== undefined) resolver.interval = duration(resolverRecord.interval, "resolver.interval");
  if (resolverRecord.timeout !== undefined) resolver.timeout = duration(resolverRecord.timeout, "resolver.timeout");

  if (!isRecord(v.preload)) fail("preload", "must be an object of account public key to account JWT");
  const preload: Record<string, string> = {};
  for (const key of Object.keys(v.preload as Record<string, unknown>).sort()) {
    const field = `preload.${PUBLIC_KEY.test(key) ? key : "<invalid key>"}`;
    if (!publicKey(key, "A")) fail(field, "key must be an account public key");
    const account = verifiedJwt((v.preload as Record<string, unknown>)[key], field, "account");
    if (account.sub !== key) fail(field, "account JWT subject must equal its key");
    if (!trustedIssuers.has(account.iss)) fail(field, "account JWT issuer is not the operator or its signing key");
    preload[key] = (v.preload as Record<string, string>)[key] as string;
  }
  if (preload[systemAccount] === undefined) fail("preload", "must contain the system account JWT");

  const config: NatsServerConfigInput = {
    bus,
    serverName,
    listen,
    ...(monitor ? { monitor } : {}),
    jetstream: { storeDir },
    operatorJwt: v.operatorJwt as string,
    systemAccount,
    resolver,
    preload,
  };

  if (single) {
    if (v.cluster !== undefined) fail("cluster", "must be absent on bus 1/1");
    return config;
  }
  if (v.cluster === undefined) fail("cluster", "is required on bus 3/3");
  const c = record(v.cluster, "cluster", ["name", "listen", "routes", "tls"]);
  const clusterName = name(c.name, "cluster.name");
  const clusterListen = hostPort(c.listen, "cluster.listen", false);
  if (clusterListen.port === listen.port || clusterListen.port === monitor?.port) {
    fail("cluster.listen.port", "must differ from listen.port and monitor.port");
  }
  if (!Array.isArray(c.routes) || c.routes.length !== bus.nodes) {
    fail("cluster.routes", "must list every configured node, including this one");
  }
  const routes = (c.routes as unknown[]).map((route) => {
    const match = typeof route === "string" ? ROUTE.exec(route) : null;
    if (!match || !dnsName(match[1] as string)) {
      fail("cluster.routes", "each route must be nats-route://<dns-name>:<port> matching the TLS certificate SANs");
    }
    port(Number((match as RegExpExecArray)[2]), "cluster.routes");
    return route as string;
  });
  if (new Set(routes).size !== routes.length) fail("cluster.routes", "must not repeat a route");
  const tlsRecord = record(c.tls, "cluster.tls", ["certFile", "keyFile", "caFile"]);
  const tls: NatsRouteTls = {
    certFile: absolutePath(tlsRecord.certFile, "cluster.tls.certFile"),
    keyFile: absolutePath(tlsRecord.keyFile, "cluster.tls.keyFile"),
    caFile: absolutePath(tlsRecord.caFile, "cluster.tls.caFile"),
  };
  if (new Set([tls.certFile, tls.keyFile, tls.caFile]).size !== 3) fail("cluster.tls", "files must be distinct");
  config.cluster = { name: clusterName, listen: clusterListen, routes, tls };
  return config;
}

const quote = (value: string) => JSON.stringify(value);

/** Render deterministic nats-server configuration text. Validates first and never returns partial text. */
export function renderNatsServerConfig(input: NatsServerConfigInput): string {
  const config = validateNatsServerConfigInput(input);
  const lines = [
    "# Generated by pi-fabric (smarty-dev#7504): NATS operator mode, full account resolver.",
    `# bus: nodes=${config.bus.nodes} replicas=${config.bus.replicas}. Stream replicas are not set here; the adapter checks them (smarty-dev#11373).`,
    `server_name: ${quote(config.serverName)}`,
    `listen: ${quote(`${config.listen.host}:${config.listen.port}`)}`,
  ];
  if (config.monitor) lines.push(`http: ${quote(`${config.monitor.host}:${config.monitor.port}`)}`);
  lines.push(
    "jetstream {",
    `  store_dir: ${quote(config.jetstream.storeDir)}`,
    "  sync_interval: always",
    "}",
    `operator: ${quote(config.operatorJwt)}`,
    `system_account: ${quote(config.systemAccount)}`,
    "resolver {",
    "  type: full",
    `  dir: ${quote(config.resolver.dir)}`,
    "  allow_delete: false",
  );
  if (config.resolver.interval) lines.push(`  interval: ${quote(config.resolver.interval)}`);
  if (config.resolver.timeout) lines.push(`  timeout: ${quote(config.resolver.timeout)}`);
  lines.push("}", "resolver_preload {");
  for (const key of Object.keys(config.preload).sort()) lines.push(`  ${key}: ${quote(config.preload[key] as string)}`);
  lines.push("}");
  if (config.cluster) {
    const { name: clusterName, listen, routes, tls } = config.cluster;
    lines.push(
      "cluster {",
      `  name: ${quote(clusterName)}`,
      `  listen: ${quote(`${listen.host}:${listen.port}`)}`,
      "  routes: [",
      ...routes.map((route) => `    ${quote(route)}`),
      "  ]",
      "  tls {",
      `    cert_file: ${quote(tls.certFile)}`,
      `    key_file: ${quote(tls.keyFile)}`,
      `    ca_file: ${quote(tls.caFile)}`,
      "    verify_cert_and_check_known_urls: true",
      "  }",
      "}",
    );
  }
  return `${lines.join("\n")}\n`;
}
