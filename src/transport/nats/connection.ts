import { closeSync, constants as fsConstants, fstatSync, openSync, readFileSync } from "node:fs";
import { Buffer } from "node:buffer";
import { isIP } from "node:net";
import { credsAuthenticator, type NatsConnection } from "@nats-io/nats-core";
import { jetstream, jetstreamManager, type JetStreamClient, type JetStreamManager } from "@nats-io/jetstream";
import { AuthorizationError, PermissionViolationError, connect, type NodeConnectionOptions, type Status, type TlsOptions } from "@nats-io/transport-node";
import { pathBMachines } from "../../mesh/nats-auth/mint.js";
import { PATH_B_OPERATOR_MACHINE_PATTERN, roleTemplateWithPathB } from "../../mesh/nats-auth/permissions.js";
import { validateMachine } from "./subjects.js";
import { TransportUnavailableError } from "../types.js";

/** Persistent identity/scope verification failure. The connection is closed, not retried. */
export class NatsAuthorizationError extends Error {
  readonly code = "FABRIC_NATS_AUTHORIZATION";
  readonly persisted = false;
  constructor(message: string, options?: ErrorOptions) { super(message, options); this.name = "NatsAuthorizationError"; }
}

export type NatsStatusListener = (status: Status) => void;
export type SubscribeNatsStatus = (listener: NatsStatusListener) => () => void;

/** Expected #1276 Path B policy inputs. The same values the account scope was minted from. */
export interface NatsPermissionPolicy {
  readonly custodyStream: string;
  readonly livenessStream: string;
  readonly pathBMachines: readonly string[];
}

interface NatsTransportSecurityOptions {
  readonly servers: string | readonly string[];
  /** Official transport-node TLS options. Only the documented fields are forwarded; verification cannot be disabled. */
  readonly tls?: TlsOptions;
  readonly name?: string;
  readonly timeoutMs?: number;
}

export interface NatsConnectionOptions extends NatsPermissionPolicy, NatsTransportSecurityOptions {
  readonly credentials: string;
  /** The server-verified machine identity; must equal the single signed `machine:` tag. */
  readonly machine: string;
  /** Defaults to the scoped runtime agent. `operator` is for explicit admin report connections only. */
  readonly role?: "agent" | "operator";
}

/** Bootstrap authority for admin provisioning only. Deliberately not exported from the package entry. */
export interface NatsBootstrapConnectionOptions extends NatsTransportSecurityOptions {
  readonly credentials: string;
}

export interface NatsConnectionHandle {
  readonly nc: NatsConnection;
  readonly js: JetStreamClient;
  readonly manager: JetStreamManager;
  readonly userInfo?: unknown;
  /** Native reconnect is disabled. Resolves a typed retryable error on loss; explicit close resolves void. */
  closed(): Promise<void | Error>;
  onStatus: SubscribeNatsStatus;
  close(): Promise<void>;
}

const connectionFailure = (cause: unknown, message: string): Error => {
  let error = cause;
  for (let depth = 0; error && depth < 6; depth += 1) {
    if (error instanceof NatsAuthorizationError) return error;
    if (error instanceof AuthorizationError || error instanceof PermissionViolationError) {
      return new NatsAuthorizationError(message, { cause });
    }
    error = error instanceof Error ? error.cause : undefined;
  }
  return new TransportUnavailableError(message, { cause });
};

const USER_NKEY = /^U[A-Z2-7]{55}$/;
const MESH = /^[0-9a-f]{32}$/;
const MAX_MESHES = 64;
const MAX_CREDENTIALS_BYTES = 64 * 1024;
const MISMATCH = "NATS USER.INFO machine tag mismatch";

/**
 * Read a creds file through one descriptor: open (no symlink follow, nonblocking for FIFOs), fstat the same fd,
 * then read it. POSIX only: a regular file owned by the current uid with no group/other mode bits. On win32 the
 * POSIX owner/mode model does not describe the ACL, so this fails closed instead of guessing.
 */
const readCredentialFile = (file: string): Uint8Array => {
  if (typeof file !== "string" || file.length === 0) throw new Error("NATS credentials are required");
  if (process.platform === "win32" || typeof process.getuid !== "function") {
    throw new Error("NATS credentials file permissions cannot be verified on this platform");
  }
  const fd = openSync(file, fsConstants.O_RDONLY | (fsConstants.O_NOFOLLOW ?? 0) | (fsConstants.O_NONBLOCK ?? 0));
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile()) throw new Error("NATS credentials must be a regular file");
    if (stat.uid !== process.getuid()) throw new Error("NATS credentials must be owned by the current user");
    if ((stat.mode & 0o077) !== 0) throw new Error("NATS credentials must not be accessible to group or others");
    if (stat.size > MAX_CREDENTIALS_BYTES) throw new Error("NATS credentials file is too large");
    const bytes = Uint8Array.from(readFileSync(fd));
    if (bytes.byteLength > MAX_CREDENTIALS_BYTES) throw new Error("NATS credentials file is too large");
    return bytes;
  } finally {
    closeSync(fd);
  }
};

/** Read only the public user identity from a NATS creds file; the full bytes stay in memory for auth. */
export const readNatsCredentials = (file: string): { readonly bytes: Uint8Array; readonly subject: string; readonly tags: readonly string[] } => {
  const bytes = readCredentialFile(file);
  const text = Buffer.from(bytes).toString("utf8");
  const match = text.match(/-{3,}BEGIN NATS USER JWT-{3,}\s*([A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){2})\s*-{3,}END NATS USER JWT-{3,}/);
  if (!match?.[1]) throw new Error("NATS credentials do not contain a user JWT");
  const segments = match[1].split(".");
  let payload: unknown;
  try {
    payload = JSON.parse(Buffer.from(segments[1]!, "base64url").toString("utf8"));
  } catch {
    throw new Error("NATS user JWT is invalid");
  }
  const subject = payload && typeof payload === "object" && typeof (payload as Record<string, unknown>).sub === "string"
    ? (payload as Record<string, unknown>).sub as string
    : "";
  if (!USER_NKEY.test(subject)) throw new Error("NATS user JWT subject is invalid");
  const nats = (payload as Record<string, unknown>).nats as Record<string, unknown> | undefined;
  const tags = Array.isArray(nats?.tags) && nats.tags.every((tag) => typeof tag === "string") ? nats.tags as string[] : [];
  return { bytes, subject, tags };
};

const isPlainObject = (value: unknown): value is Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
};

const TLS_STRING_FIELDS = ["certFile", "cert", "caFile", "ca", "keyFile", "key"] as const;
const TLS_FIELDS = new Set<string | symbol>([...TLS_STRING_FIELDS, "handshakeFirst", "rejectUnauthorized"]);

/** Fresh copy of the documented TLS fields only. Unknown keys (servername, checkServerIdentity, ...) are refused. */
const tlsOptions = (value: unknown): NonNullable<NodeConnectionOptions["tls"]> => {
  if (value === undefined) return {};
  if (!isPlainObject(value)) throw new Error("NATS tls options must be a plain object");
  for (const key of Reflect.ownKeys(value)) if (!TLS_FIELDS.has(key)) throw new Error("unsupported NATS tls option");
  const out: Record<string, string | boolean> = {};
  for (const key of TLS_STRING_FIELDS) {
    const field = value[key];
    if (field === undefined) continue;
    if (typeof field !== "string" || field.length === 0) throw new Error(`NATS tls.${key} must be a nonempty string`);
    out[key] = field;
  }
  if (value.handshakeFirst !== undefined) {
    if (typeof value.handshakeFirst !== "boolean") throw new Error("NATS tls.handshakeFirst must be a boolean");
    out.handshakeFirst = value.handshakeFirst;
  }
  if (value.rejectUnauthorized !== undefined && value.rejectUnauthorized !== true) throw new Error("NATS tls verification cannot be disabled");
  return out as NonNullable<NodeConnectionOptions["tls"]>;
};

const parseServer = (server: unknown): { readonly tls: boolean; readonly numeric: boolean } => {
  // Strict charset so WHATWG URL and the nats-core host:port parser cannot disagree (no userinfo, whitespace, %, \\).
  if (typeof server !== "string" || !/^[A-Za-z0-9.:[\]/_-]+$/.test(server)) throw new Error("invalid NATS server URL");
  const scheme = /^([a-z][a-z0-9+.-]*):\/\//i.exec(server)?.[1]?.toLowerCase();
  if (scheme !== undefined && scheme !== "nats" && scheme !== "tls") throw new Error("NATS server URL scheme must be nats:// or tls://");
  let url: URL;
  try { url = new URL(scheme === undefined ? `nats://${server}` : server); } catch { throw new Error("invalid NATS server URL"); }
  if (url.username || url.password || (url.pathname !== "" && url.pathname !== "/") || url.search || url.hash || !url.hostname) {
    throw new Error("invalid NATS server URL");
  }
  // NATS URLs are non-special: use WHATWG's HTTP host parser to normalize legacy hex/octal/short IPv4 first.
  const host = new URL(`http://${url.hostname}`).hostname.replace(/^\[(.*)\]$/, "$1");
  return { tls: scheme === "tls", numeric: isIP(host) !== 0 };
};

/** Production and tests use the same mandatory DNS-verified TLS path. */
const transportSecurity = (options: NatsTransportSecurityOptions): Pick<NodeConnectionOptions, "servers" | "tls" | "ignoreClusterUpdates"> => {
  for (const key of ["NODE_EXTRA_CA_CERTS", "NODE_TLS_REJECT_UNAUTHORIZED", "SSL_CERT_FILE", "SSL_CERT_DIR"]) {
    if (process.env[key] !== undefined) throw new Error(`NATS refuses ${key}; configure trust explicitly with tls.caFile`);
  }
  const servers = typeof options.servers === "string" ? [options.servers] : Array.isArray(options.servers) ? [...options.servers] as string[] : [];
  if (servers.length === 0) throw new Error("NATS servers are required");
  const parsed = servers.map(parseServer);
  if (options.tls === undefined && parsed.some((server) => !server.tls)) throw new Error("NATS requires tls:// servers or explicit tls options");
  // ponytail: transport-node 3.4 startTLS leaves tlsName empty for literal IPs, so Node checks 'localhost' instead.
  // Use the certificate DNS name rather than bespoke TLS identity plumbing; pin all configured cluster endpoints.
  if (parsed.some((server) => server.numeric)) throw new Error("NATS TLS URL must use the certificate DNS name, not an IP address");
  return { servers, tls: tlsOptions(options.tls), ignoreClusterUpdates: true };
};

const tagValues = (tags: readonly string[], key: string): string[] =>
  tags.filter((tag) => tag.startsWith(`${key}:`)).map((tag) => tag.slice(key.length + 1));

interface ExpectedPermissions {
  readonly pubAllow: ReadonlySet<string>;
  readonly pubDeny: ReadonlySet<string>;
  readonly subAllow: ReadonlySet<string>;
  readonly subDeny: ReadonlySet<string>;
}

/** Server-expanded #1276 scope for this signed user, built by the same roleTemplateWithPathB and validators as mint. */
const expectedPermissions = (role: "agent" | "operator", machine: string, policy: NatsPermissionPolicy, auth: ReturnType<typeof readNatsCredentials>): ExpectedPermissions => {
  const machines = pathBMachines(policy.pathBMachines);
  const roles = tagValues(auth.tags, "role");
  const signedMachines = tagValues(auth.tags, "machine");
  const meshes = tagValues(auth.tags, "mesh");
  if (roles.length !== 1 || roles[0] !== role || signedMachines.length !== 1 || signedMachines[0] !== machine) throw new Error(MISMATCH);
  if (role === "agent") {
    if (PATH_B_OPERATOR_MACHINE_PATTERN.test(machine) || !machines.includes(machine)) throw new Error(MISMATCH);
    if (meshes.length === 0 || meshes.length > MAX_MESHES || new Set(meshes).size !== meshes.length || !meshes.every((mesh) => MESH.test(mesh))) {
      throw new Error(MISMATCH);
    }
  } else if (!PATH_B_OPERATOR_MACHINE_PATTERN.test(machine) || meshes.length !== 0) {
    throw new Error(MISMATCH);
  }
  const template = roleTemplateWithPathB(role, { custodyStream: policy.custodyStream, livenessStream: policy.livenessStream }, machines);
  const expand = (subjects: readonly string[]): Set<string> => new Set(subjects.flatMap((subject) => {
    const fixed = subject.replaceAll("{{subject()}}", auth.subject).replaceAll("{{tag(machine)}}", machine);
    const out = fixed.includes("{{tag(mesh)}}") ? meshes.map((mesh) => fixed.replaceAll("{{tag(mesh)}}", mesh)) : [fixed];
    if (out.some((value) => value.includes("{{"))) throw new Error("unexpected NATS permission template");
    return out;
  }));
  return { pubAllow: expand(template.pub.allow), pubDeny: expand(template.pub.deny), subAllow: expand(template.sub.allow), subDeny: new Set() };
};

/** Absent list = empty; present list must be strings. Unknown keys refuse. */
const permissionLists = (value: unknown): { allow: Set<string>; deny: Set<string> } | undefined => {
  if (value === undefined) return { allow: new Set(), deny: new Set() };
  if (!isPlainObject(value) || Object.keys(value).some((key) => key !== "allow" && key !== "deny")) return undefined;
  const list = (entry: unknown): Set<string> | undefined => entry === undefined ? new Set()
    : Array.isArray(entry) && entry.every((subject) => typeof subject === "string") ? new Set(entry as string[]) : undefined;
  const allow = list(value.allow);
  const deny = list(value.deny);
  return allow && deny ? { allow, deny } : undefined;
};

const sameSet = (actual: ReadonlySet<string>, expected: ReadonlySet<string>): boolean =>
  actual.size === expected.size && [...expected].every((subject) => actual.has(subject));

const verifyIdentity = async (nc: NatsConnection, timeoutMs: number, auth: ReturnType<typeof readNatsCredentials>, expected: ExpectedPermissions): Promise<unknown> => {
  const response = await nc.request("$SYS.REQ.USER.INFO", new Uint8Array(), { timeout: timeoutMs }).catch((cause: unknown) => {
    throw connectionFailure(cause, "NATS USER.INFO verification is unavailable");
  });
  let value: unknown;
  try { value = JSON.parse(new TextDecoder().decode(response.data)); } catch (cause) {
    throw new NatsAuthorizationError("NATS USER.INFO response is invalid", { cause });
  }
  // USER.INFO returns the authenticated user and expanded permissions, not JWT tags. Tie the locally decoded tags to
  // the server-authenticated subject, then require the exact expanded scope: no missing, extra or response rights.
  const data = isPlainObject(value) && isPlainObject(value.data) ? value.data : undefined;
  const permissions = data?.permissions;
  if (!data || data.user !== auth.subject || !isPlainObject(permissions) ||
    Object.keys(permissions).some((key) => key !== "publish" && key !== "subscribe")) throw new NatsAuthorizationError(MISMATCH);
  const publish = permissionLists(permissions.publish);
  const subscribe = permissionLists(permissions.subscribe);
  if (!publish || !subscribe || !sameSet(publish.allow, expected.pubAllow) || !sameSet(publish.deny, expected.pubDeny) ||
    !sameSet(subscribe.allow, expected.subAllow) || !sameSet(subscribe.deny, expected.subDeny)) throw new NatsAuthorizationError(MISMATCH);
  return value;
};

const refuseBypass = (options: object): void => {
  for (const key of ["verifyMachine", "inboxPrefix", "allowInsecureLoopback"]) if (key in options) throw new Error(`NATS connection option ${key} is not supported`);
};

const open = async (
  options: NatsTransportSecurityOptions & { readonly credentials: string },
  verify: ((nc: NatsConnection, timeoutMs: number, auth: ReturnType<typeof readNatsCredentials>) => Promise<unknown>) | undefined,
  prepare: (auth: ReturnType<typeof readNatsCredentials>) => void = () => undefined,
): Promise<NatsConnectionHandle> => {
  const timeoutMs = options.timeoutMs ?? 5_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) throw new Error("timeoutMs must be positive");
  const security = transportSecurity(options);
  const auth = readNatsCredentials(options.credentials);
  prepare(auth);
  const nc = await connect({
    ...security,
    ...(options.name ? { name: options.name } : {}),
    timeout: timeoutMs,
    // A new physical connection must pass USER.INFO before any JS/KV client is exposed.
    // Native reconnect replays subscriptions/outbound frames before asynchronous status observers run.
    // The transport owner must reopen through openNatsConnection and recreate its clients instead.
    reconnect: false,
    maxReconnectAttempts: 0,
    authenticator: credsAuthenticator(auth.bytes),
    inboxPrefix: `_INBOX.${auth.subject}`,
  }).catch((cause: unknown) => { throw connectionFailure(cause, "NATS connection failed"); });
  try {
    const userInfo = verify ? await verify(nc, timeoutMs, auth) : undefined;
    const manager = await jetstreamManager(nc, { timeout: timeoutMs });
    const js = jetstream(nc, { timeout: timeoutMs });
    const listeners = new Set<NatsStatusListener>();
    let closing = false;
    const closed = nc.closed().then((cause) => closing ? undefined
      : new TransportUnavailableError("NATS connection lost; reopen and reverify before use", { cause }));
    const statusTask = (async () => {
      for await (const status of nc.status()) {
        for (const listener of listeners) {
          try { listener(status); } catch { /* One observer must not prevent shutdown. */ }
        }
      }
    })();
    return {
      nc,
      js,
      manager,
      ...(userInfo === undefined ? {} : { userInfo }),
      closed: () => closed,
      onStatus: (listener) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
      close: async () => {
        closing = true;
        await nc.close();
        await statusTask;
        listeners.clear();
      },
    };
  } catch (error) {
    await nc.close();
    throw error;
  }
};

/** Verified connection: always checks the signed identity and the exact USER.INFO scope. No bypass exists. */
export const openNatsConnection = async (options: NatsConnectionOptions): Promise<NatsConnectionHandle> => {
  refuseBypass(options);
  const role = options.role ?? "agent";
  if (role !== "agent" && role !== "operator") throw new Error("NATS connection role must be agent or operator");
  if (typeof options.machine !== "string") throw new Error("NATS machine is required");
  const machine = validateMachine(options.machine);
  const policy: NatsPermissionPolicy = { custodyStream: options.custodyStream, livenessStream: options.livenessStream, pathBMachines: options.pathBMachines };
  let expected: ExpectedPermissions | undefined;
  return open(options, (nc, timeoutMs, auth) => verifyIdentity(nc, timeoutMs, auth, expected!), (auth) => {
    // Fail closed before dialing: missing/invalid policy or a signed identity that cannot match.
    try { expected = expectedPermissions(role, machine, policy, auth); }
    catch (cause) { throw new NatsAuthorizationError("NATS signed identity / permission policy mismatch", { cause }); }
  });
};

/** Separate bootstrap authority for `fabric-transport-admin provision` only; never used by the runtime. */
export const openBootstrapNatsConnection = async (options: NatsBootstrapConnectionOptions): Promise<NatsConnectionHandle> => {
  refuseBypass(options);
  return open(options, undefined);
};
