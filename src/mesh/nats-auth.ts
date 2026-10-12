// NATS decentralized authentication for the mesh (smarty-dev#7504 / #8031 R1; design: docs/designs/nats-auth-7504.md).
// One operator signs one account per org; each launch, principal device or maintainer job gets its own user, signed by
// the account's scoped signing key for its role, so the server applies that role's permission template. The same code
// serves the fleet (keys in 1Password, minted in memory on Ryzen 1) and a self-hosted single machine (a local operator
// under ~/.config/fabric/nats, `initLocalNats`): only key custody and configuration differ.
//
// ponytail: the JWT encoder is ~60 lines over @nats-io/nkeys (already a transitive dependency of the client) instead of
// @nats-io/jwt, which is pre-1.0. The design's gate G-7 (decision D4) keeps it only while a test shows nsc decodes our
// tokens to the same claims and nats-server accepts them (tests/nats-auth.test.ts, tests/nats-auth-server.test.ts).
//
// Loaded only by the `pi-fabric nats` CLI and by a connection that carries creds: never in the startup graph.
import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { createAccount, createOperator, createUser, fromPublic, fromSeed, type KeyPair } from "@nats-io/nkeys";

// ------------------------------------------------------------------ JWT encoding (NATS JWT v2)

export type NatsRole = "agent" | "principal" | "maintainer";
export const NATS_ROLES: readonly NatsRole[] = ["agent", "principal", "maintainer"];

/** The permission block of a user or of a scoped signing key's template (NATS JWT v2 `UserPermissionLimits`). */
export interface NatsPermissionTemplate {
  pub: { allow: string[] };
  sub: { allow: string[] };
  subs: number;
  data: number;
  payload: number;
}

type Claims = { jti?: string; iat: number; iss: string; name: string; sub: string; exp?: number; nats: Record<string, unknown> };

const HEADER = { typ: "JWT", alg: "ed25519-nkey" };
const b64url = (bytes: Uint8Array | string): string => Buffer.from(bytes).toString("base64url");
const base32 = (bytes: Uint8Array): string => {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  let bits = 0, value = 0, out = "";
  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) { out += alphabet[(value >>> (bits - 5)) & 31]; bits -= 5; }
  }
  if (bits > 0) out += alphabet[(value << (5 - bits)) & 31];
  return out;
};

/**
 * Signs `claims` with `signer` (an nkey pair), byte for byte as nats-io/jwt v2 (nsc) does: the standard claims in Go
 * field order (exp, jti, iat, iss, name, sub, then `nats`), no `aud`, `exp` omitted when unset, and the jti =
 * base32(no padding)(sha512/256(the compact JSON of the standard claims only, without jti and without `nats`)).
 * The jti does not bind permissions or tags (nor does nsc's); nothing here uses it as a content hash.
 */
export const encodeNatsJwt = (claims: Claims, signer: KeyPair): string => {
  const standard = { ...(claims.exp === undefined ? {} : { exp: claims.exp }), iat: claims.iat, iss: claims.iss, name: claims.name, sub: claims.sub };
  const jti = base32(createHash("sha512-256").update(JSON.stringify(standard)).digest());
  const body = { ...(claims.exp === undefined ? {} : { exp: claims.exp }), jti, iat: claims.iat, iss: claims.iss, name: claims.name,
    sub: claims.sub, nats: claims.nats };
  const unsigned = `${b64url(JSON.stringify(HEADER))}.${b64url(JSON.stringify(body))}`;
  return `${unsigned}.${b64url(signer.sign(Buffer.from(unsigned, "ascii")))}`;
};

/** Decodes a NATS JWT and checks its header and its signature by `iss`. Throws on any mismatch. */
export const decodeNatsJwt = (jwt: string): Claims => {
  const parts = jwt.split(".");
  if (parts.length !== 3) throw new Error("not a JWT");
  const header = JSON.parse(Buffer.from(parts[0]!, "base64url").toString("utf8")) as { typ?: unknown; alg?: unknown };
  if (header.alg !== HEADER.alg || String(header.typ).toUpperCase() !== "JWT") throw new Error("not a NATS JWT");
  const claims = JSON.parse(Buffer.from(parts[1]!, "base64url").toString("utf8")) as Claims;
  if (typeof claims.iss !== "string" || !fromPublic(claims.iss).verify(Buffer.from(`${parts[0]}.${parts[1]}`, "ascii"),
    Buffer.from(parts[2]!, "base64url"))) throw new Error("bad NATS JWT signature");
  return claims;
};

const nowSeconds = (now = Date.now()): number => Math.floor(now / 1000);
const pub = (pair: KeyPair): string => pair.getPublicKey();
const seed = (pair: KeyPair): string => new TextDecoder().decode(pair.getSeed());

// ------------------------------------------------------------------ the role templates (design 1.4)

const AGENT_STREAM = "FABRIC_MESH_{{tag(mesh)}}";
const CONSUMER_VERBS = ["CREATE", "MSG.NEXT", "DELETE", "INFO"];
const INBOX = "_INBOX.{{subject()}}.>";
const UNLIMITED = { subs: -1, data: -1, payload: -1 };

/**
 * The least each role needs (design 1.3-1.4). An agent reaches only the streams named by its `mesh:` tags; it never
 * creates, updates, purges or deletes a stream. A principal reads every root of its org. A maintainer administers them.
 * Every role subscribes only to its own inbox (`_INBOX.<its user key>`: the client sets that inbox prefix).
 */
export const natsRoleTemplate = (role: NatsRole): NatsPermissionTemplate => {
  const common = ["$JS.API.INFO", "$SYS.REQ.USER.INFO"];
  if (role === "agent") {
    return { pub: { allow: [`${AGENT_STREAM}.state`, `${AGENT_STREAM}.snapshot`, ...common, `$JS.API.STREAM.INFO.${AGENT_STREAM}`,
      `$JS.API.STREAM.MSG.GET.${AGENT_STREAM}`, ...CONSUMER_VERBS.map(verb => `$JS.API.CONSUMER.${verb}.${AGENT_STREAM}.>`)] },
    sub: { allow: [INBOX] }, ...UNLIMITED };
  }
  const reads = [...common, "$JS.API.STREAM.NAMES", "$JS.API.STREAM.INFO.*", "$JS.API.STREAM.MSG.GET.*",
    ...CONSUMER_VERBS.map(verb => `$JS.API.CONSUMER.${verb}.*.>`)];
  if (role === "principal") return { pub: { allow: reads }, sub: { allow: [INBOX] }, ...UNLIMITED };
  return { pub: { allow: ["*.state", "*.snapshot", ...reads, ...["CREATE", "UPDATE", "DELETE", "PURGE"].map(verb => `$JS.API.STREAM.${verb}.*`)] },
    sub: { allow: [INBOX] }, ...UNLIMITED };
};

// ------------------------------------------------------------------ operator, accounts, users

export interface NatsAccountKeys { name: string; identity: KeyPair; scopes: Record<NatsRole, KeyPair> }

/** The operator JWT, self-signed by its identity key; it names its signing key and the system account. */
export const natsOperatorJwt = (identity: KeyPair, input: { name: string; signingKeys: string[]; systemAccount: string; now?: number }): string =>
  encodeNatsJwt({ iat: nowSeconds(input.now), iss: pub(identity), name: input.name, sub: pub(identity),
    nats: { signing_keys: input.signingKeys, system_account: input.systemAccount, type: "operator", version: 2 } }, identity);

/** The system account's JWT (no JetStream), signed by the operator signing key. */
export const natsSystemAccountJwt = (operatorSigning: KeyPair, account: KeyPair, input: { name?: string; now?: number } = {}): string =>
  encodeNatsJwt({ iat: nowSeconds(input.now), iss: pub(operatorSigning), name: input.name ?? "SYS", sub: pub(account),
    nats: { limits: { subs: -1, data: -1, payload: -1, imports: -1, exports: -1, wildcards: true, conn: -1, leaf: -1 },
      type: "account", version: 2 } }, operatorSigning);

/**
 * An org account's JWT, signed by the operator signing key: JetStream enabled, no exports or imports (the org boundary),
 * one scoped signing key per role carrying that role's template, and the revoked user keys (`revocations`, each with
 * the time before which its JWTs are refused).
 */
export const natsOrgAccountJwt = (operatorSigning: KeyPair, account: NatsAccountKeys,
  input: { revocations?: Record<string, number>; now?: number } = {}): string =>
  encodeNatsJwt({ iat: nowSeconds(input.now), iss: pub(operatorSigning), name: account.name, sub: pub(account.identity),
    nats: {
      limits: { subs: -1, data: -1, payload: -1, imports: -1, exports: -1, wildcards: true, conn: -1, leaf: -1,
        mem_storage: -1, disk_storage: -1, streams: -1, consumer: -1, max_ack_pending: -1, mem_max_stream_bytes: -1, disk_max_stream_bytes: -1 },
      signing_keys: NATS_ROLES.map(role => ({ kind: "user_scope", key: pub(account.scopes[role]), role, template: natsRoleTemplate(role),
        description: `Fabric mesh ${role} users (smarty-dev#7504)` })),
      ...(input.revocations && Object.keys(input.revocations).length > 0 ? { revocations: input.revocations } : {}),
      default_permissions: { pub: {}, sub: {} }, authorization: {},
      type: "account", version: 2,
    } }, operatorSigning);

/** A user with no scope (the system account's user, for the resolver and the tools), signed by the account key itself. */
export const natsPlainUserJwt = (accountSigner: KeyPair, user: KeyPair, input: { name: string; now?: number; exp?: number }): string =>
  encodeNatsJwt({ iat: nowSeconds(input.now), iss: pub(accountSigner), name: input.name, sub: pub(user),
    ...(input.exp === undefined ? {} : { exp: input.exp }),
    nats: { pub: {}, sub: {}, ...UNLIMITED, type: "user", version: 2 } }, accountSigner);

/** The `.creds` text a NATS client reads (credsAuthenticator): the user JWT and its seed. Secret: never log or store it. */
export const formatNatsCreds = (jwt: string, userSeed: string): string => [
  "-----BEGIN NATS USER JWT-----", jwt, "------END NATS USER JWT------", "",
  "************************* IMPORTANT *************************",
  "NKEY Seed printed below can be used to sign and prove identity.",
  "NKEYs are sensitive and should be treated as secrets.", "",
  "-----BEGIN USER NKEY SEED-----", userSeed, "------END USER NKEY SEED------", "",
  "*************************************************************", "",
].join("\n");

/** The user JWT inside a `.creds` text (public), or undefined. */
export const natsCredsJwt = (creds: string): string | undefined => /-----BEGIN NATS USER JWT-----\s*([^\s-][^\s]*)\s*-+END NATS USER JWT/.exec(creds)?.[1];

export interface NatsMintInput {
  role: NatsRole;
  /** The org account's public key (`A…`): the user's `issuer_account`. */
  account: string;
  /** The seed of that account's scoped signing key for `role` (fleet: from 1Password, in memory only). */
  scopeSeed: string;
  /** The user's name: `agent/<host>/<lane>/<session>`, `principal/<person>/<device>`, `maintainer/<org>/<job>/<run>`. */
  name: string;
  /** The mesh streams an agent may open, as the 32 lowercase hex of `natsStreamName` (`FABRIC_MESH_<hex>`). */
  mesh?: string[];
  /** Other `key:value` tags (host, lane, session, release, parent). */
  tags?: Record<string, string>;
  /** Lifetime in seconds. Defaults: agent 7 days, principal 90 days, maintainer 2 hours (design 1.4). */
  ttlSeconds?: number;
  now?: number;
}

export interface NatsMintedUser { creds: string; publicKey: string; jwt: string; exp: number }

const DEFAULT_TTL: Record<NatsRole, number> = { agent: 7 * 86_400, principal: 90 * 86_400, maintainer: 2 * 3_600 };
const TAG = /^[a-z0-9._-]{1,64}$/;

/**
 * Mints one user in memory: a fresh user nkey pair and a JWT signed by the role's scoped signing key (so the server
 * applies the role template; the user JWT itself carries no permissions). `iat` is 60 s in the past (clock skew), no
 * `nbf`. Returns the creds text, which the caller passes on a pipe and never writes to disk.
 */
export const mintNatsUser = (input: NatsMintInput): NatsMintedUser => {
  const scope = fromSeed(new TextEncoder().encode(input.scopeSeed));
  if (!pub(scope).startsWith("A")) throw new Error("the scoped signing key must be an account key (A…)");
  if (!/^A[A-Z2-7]{55}$/.test(input.account)) throw new Error("the account must be an account public key (A…)");
  const mesh = input.mesh ?? [];
  if (input.role === "agent" && mesh.length === 0) throw new Error("an agent user needs at least one --mesh stream (32 hex)");
  if (input.role !== "agent" && mesh.length > 0) throw new Error("only an agent user is bound to mesh streams");
  for (const hex of mesh) if (!/^[0-9a-f]{32}$/.test(hex)) throw new Error(`bad mesh stream hex ${JSON.stringify(hex)} (32 lowercase hex)`);
  const tags = [`role:${input.role}`, ...mesh.map(hex => `mesh:${hex}`)];
  for (const [key, value] of Object.entries(input.tags ?? {})) {
    if (!TAG.test(key) || !TAG.test(value) || key === "role" || key === "mesh") throw new Error(`bad tag ${key}:${value} (lowercase [a-z0-9._-])`);
    tags.push(`${key}:${value}`);
  }
  const user = createUser();
  const issued = nowSeconds(input.now) - 60;
  const exp = nowSeconds(input.now) + (input.ttlSeconds ?? DEFAULT_TTL[input.role]);
  const jwt = encodeNatsJwt({ iat: issued, iss: pub(scope), name: input.name, sub: pub(user), exp,
    nats: { issuer_account: input.account, pub: {}, sub: {}, tags, type: "user", version: 2 } }, scope);
  return { creds: formatNatsCreds(jwt, seed(user)), publicKey: pub(user), jwt, exp };
};

// ------------------------------------------------------------------ server configuration (operator mode, full resolver)

export interface NatsServerConfigInput {
  serverName: string;
  /** The client listen address, `host:port` (default `127.0.0.1:4222`). */
  listen?: string;
  /** The monitoring listen address (`host:port`), read-only HTTP; omit to disable. */
  http?: string;
  operatorJwt: string;
  systemAccount: string;
  resolverDir: string;
  /** Account JWTs the resolver starts with (at least the system account's), by account public key. */
  preload: Record<string, string>;
  jetstreamDir: string;
  /** Client TLS (non-loopback listeners). */
  tls?: { certFile: string; keyFile: string; caFile?: string };
  /**
   * A cluster: routes use mutual TLS with known-URL checking (design Q5: `verify_cert_and_check_known_urls`, no route
   * password). Routes are `tls://host:6222` URLs whose host names are in the peers' certificates.
   */
  cluster?: { name: string; listen: string; routes: string[]; tls: { certFile: string; keyFile: string; caFile: string } };
}

const quote = (value: string): string => JSON.stringify(value);

/** The nats-server configuration text for one node (single node or one of a cluster): the same shape either way. */
export const natsServerConfig = (input: NatsServerConfigInput): string => {
  const lines = [
    `server_name: ${quote(input.serverName)}`,
    `listen: ${quote(input.listen ?? "127.0.0.1:4222")}`,
    ...(input.http ? [`http: ${quote(input.http)}`] : []),
    `operator: ${quote(input.operatorJwt)}`,
    `system_account: ${quote(input.systemAccount)}`,
    "resolver: {",
    "  type: full",
    `  dir: ${quote(input.resolverDir)}`,
    "  allow_delete: false",
    "  interval: \"2m\"",
    "  timeout: \"5s\"",
    "}",
    "resolver_preload: {",
    ...Object.entries(input.preload).map(([account, jwt]) => `  ${account}: ${quote(jwt)}`),
    "}",
    "jetstream {",
    `  store_dir: ${quote(input.jetstreamDir)}`,
    "  sync_interval: always",
    "}",
  ];
  if (input.tls) {
    lines.push("tls {", `  cert_file: ${quote(input.tls.certFile)}`, `  key_file: ${quote(input.tls.keyFile)}`,
      ...(input.tls.caFile ? [`  ca_file: ${quote(input.tls.caFile)}`] : []), "}");
  }
  if (input.cluster) {
    const { cluster } = input;
    for (const route of cluster.routes) if (!/^tls:\/\/[^/\s]+:\d+$/.test(route)) throw new Error(`a route must be tls://host:port (got ${route})`);
    lines.push("cluster {", `  name: ${quote(cluster.name)}`, `  listen: ${quote(cluster.listen)}`, "  routes: [",
      ...cluster.routes.map(route => `    ${quote(route)}`), "  ]", "  tls {", `    cert_file: ${quote(cluster.tls.certFile)}`,
      `    key_file: ${quote(cluster.tls.keyFile)}`, `    ca_file: ${quote(cluster.tls.caFile)}`, "    verify_cert_and_check_known_urls: true",
      "    timeout: 5", "  }", "}");
  }
  return `${lines.join("\n")}\n`;
};

// ------------------------------------------------------------------ the self-hosted single machine (design 3, S1)

/** The self-hosted key store (`~/.config/fabric/nats`): dir 0700, files 0600, the user's own trust. */
export const defaultLocalNatsDir = (): string => path.join(os.homedir(), ".config", "fabric", "nats");

export interface LocalNatsInit {
  dir: string;
  org: string;
  operator: string;
  systemAccount: string;
  account: string;
  configFile: string;
  listen: string;
}

const writePrivate = (file: string, text: string): void => {
  const descriptor = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
  try { fs.writeSync(descriptor, text); fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
};
const ensurePrivateDir = (dir: string): void => {
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const stat = fs.lstatSync(dir);
  const uid = typeof process.getuid === "function" ? process.getuid() : undefined;
  if (!stat.isDirectory() || (uid !== undefined && stat.uid !== uid)) throw new Error(`${dir} is not a directory of this user`);
  fs.chmodSync(dir, 0o700);
};

/** Refuses on a fleet host: there the keys stay in 1Password and only Ryzen 1 mints (design 3, `nats.custody`). */
export const assertSelfHostedCustody = (env: NodeJS.ProcessEnv = process.env): void => {
  if ((env.PI_FABRIC_NATS_CUSTODY ?? "").trim().toLowerCase() === "fleet") {
    throw new Error("this host has NATS custody `fleet`: keys stay in 1Password and only the fleet issuer mints (pi-fabric nats init --local refused)");
  }
};

const ORG = /^[a-z][a-z0-9-]{0,62}$/;

/**
 * `pi-fabric nats init --local`: a local operator, its signing key, the system account and its user, one org account
 * with the 3 role signing keys, and the single-node server config (operator mode, full resolver, loopback listener).
 * Refuses an existing store (no overwrite) and a fleet host.
 */
export const initLocalNats = (input: { dir?: string; org?: string; port?: number; httpPort?: number; env?: NodeJS.ProcessEnv } = {}): LocalNatsInit => {
  assertSelfHostedCustody(input.env);
  const dir = path.resolve(input.dir ?? defaultLocalNatsDir());
  const org = input.org ?? "local";
  if (!ORG.test(org)) throw new Error(`bad org name ${JSON.stringify(org)}`);
  if (fs.existsSync(path.join(dir, "operator.jwt"))) throw new Error(`${dir} already holds a NATS operator (rotate it instead)`);
  ensurePrivateDir(dir);
  ensurePrivateDir(path.join(dir, "keys"));
  const operator = createOperator(), operatorSigning = createOperator(), sys = createAccount(), sysUser = createUser();
  const account: NatsAccountKeys = { name: org, identity: createAccount(),
    scopes: { agent: createAccount(), principal: createAccount(), maintainer: createAccount() } };
  const operatorJwt = natsOperatorJwt(operator, { name: "local", signingKeys: [pub(operatorSigning)], systemAccount: pub(sys) });
  const sysJwt = natsSystemAccountJwt(operatorSigning, sys);
  const orgJwt = natsOrgAccountJwt(operatorSigning, account);
  const keys: Record<string, KeyPair> = { operator, "operator-signing": operatorSigning, "sys-account": sys,
    [`account-${org}`]: account.identity, ...Object.fromEntries(NATS_ROLES.map(role => [`account-${org}-${role}`, account.scopes[role]])) };
  for (const [name, pair] of Object.entries(keys)) writePrivate(path.join(dir, "keys", `${name}.nk`), `${seed(pair)}\n`);
  writePrivate(path.join(dir, "sys-user.creds"), formatNatsCreds(natsPlainUserJwt(sys, sysUser, { name: "sys" }), seed(sysUser)));
  writePrivate(path.join(dir, "operator.jwt"), `${operatorJwt}\n`);
  writePrivate(path.join(dir, `account-${org}.jwt`), `${orgJwt}\n`);
  writePrivate(path.join(dir, "org"), `${org}\n`);
  ensurePrivateDir(path.join(dir, "jwt"));
  ensurePrivateDir(path.join(dir, "jetstream"));
  const listen = `127.0.0.1:${input.port ?? 4222}`;
  const configFile = path.join(dir, "nats-server.conf");
  writePrivate(configFile, natsServerConfig({ serverName: "fabric-local", listen, http: `127.0.0.1:${input.httpPort ?? 8222}`, operatorJwt,
    systemAccount: pub(sys), resolverDir: path.join(dir, "jwt"), preload: { [pub(sys)]: sysJwt, [pub(account.identity)]: orgJwt },
    jetstreamDir: path.join(dir, "jetstream") }));
  return { dir, org, operator: pub(operator), systemAccount: pub(sys), account: pub(account.identity), configFile, listen };
};

const readSeed = (dir: string, name: string): string => fs.readFileSync(path.join(dir, "keys", `${name}.nk`), "utf8").trim();

/** Mints a user from the local store (self-hosted): the same `mintNatsUser`, with the local org's scoped key. */
export const mintLocalNatsUser = (input: Omit<NatsMintInput, "account" | "scopeSeed"> & { dir?: string; env?: NodeJS.ProcessEnv }): NatsMintedUser => {
  assertSelfHostedCustody(input.env);
  const dir = path.resolve(input.dir ?? defaultLocalNatsDir());
  const org = fs.readFileSync(path.join(dir, "org"), "utf8").trim();
  const account = pub(fromSeed(new TextEncoder().encode(readSeed(dir, `account-${org}`))));
  const { dir: _dir, env: _env, ...mint } = input;
  return mintNatsUser({ ...mint, account, scopeSeed: readSeed(dir, `account-${org}-${input.role}`) });
};

/** The local store's system account creds (the resolver and the tools: claims pushes, revocations). */
export const localSystemCreds = (dir = defaultLocalNatsDir()): string => fs.readFileSync(path.join(path.resolve(dir), "sys-user.creds"), "utf8");

/**
 * Revokes users in the local org: re-signs the account JWT with their keys revoked from now on and returns it, to be
 * pushed with `pushNatsAccountJwt`. The local store keeps the revocations so a later revoke keeps them.
 */
export const revokeLocalNatsUsers = (userKeys: string[], input: { dir?: string; now?: number; env?: NodeJS.ProcessEnv } = {}): string => {
  assertSelfHostedCustody(input.env);
  const dir = path.resolve(input.dir ?? defaultLocalNatsDir());
  const org = fs.readFileSync(path.join(dir, "org"), "utf8").trim();
  for (const key of userKeys) if (!/^U[A-Z2-7]{55}$/.test(key)) throw new Error(`not a user public key: ${key}`);
  const file = path.join(dir, `revocations-${org}.json`);
  const revocations = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, number> : {};
  const at = nowSeconds(input.now);
  for (const key of userKeys) revocations[key] = at;
  const account: NatsAccountKeys = { name: org, identity: fromSeed(new TextEncoder().encode(readSeed(dir, `account-${org}`))),
    scopes: Object.fromEntries(NATS_ROLES.map(role => [role, fromSeed(new TextEncoder().encode(readSeed(dir, `account-${org}-${role}`)))])) as Record<NatsRole, KeyPair> };
  const jwt = natsOrgAccountJwt(fromSeed(new TextEncoder().encode(readSeed(dir, "operator-signing"))), account,
    { revocations, ...(input.now === undefined ? {} : { now: input.now }) });
  const temporary = `${file}.${process.pid}.tmp`;
  writePrivate(temporary, `${JSON.stringify(revocations)}\n`);
  fs.renameSync(temporary, file);
  return jwt;
};

// ------------------------------------------------------------------ revocation push and the creds cutoff

/**
 * Pushes an account JWT to the cluster on `$SYS.REQ.CLAIMS.UPDATE` with the system account's creds, and returns what the
 * answering server said. A revocation takes effect when every server holds the new JWT (design 2.4).
 */
export const pushNatsAccountJwt = async (servers: string[], systemCreds: string, jwt: string): Promise<{ ok: boolean; message: string }> => {
  const { connect, credsAuthenticator } = await import("@nats-io/transport-node");
  const nc = await connect({ servers, authenticator: credsAuthenticator(new TextEncoder().encode(systemCreds)) });
  try {
    const reply = await nc.request("$SYS.REQ.CLAIMS.UPDATE", new TextEncoder().encode(jwt), { timeout: 5_000 });
    const body = JSON.parse(new TextDecoder().decode(reply.data)) as { data?: { code?: number; message?: string }; error?: { code?: number; description?: string } };
    if (body.error) return { ok: false, message: body.error.description ?? JSON.stringify(body.error) };
    return { ok: (body.data?.code ?? 200) < 300, message: body.data?.message ?? "" };
  } finally { await nc.close(); }
};

/** Fabric's own cutoff before a user JWT's `exp` (design 1.4: exp - 1 h), so a session never runs into the hard expiry. */
export const NATS_CREDS_CUTOFF_SECONDS = 3_600;

/** The user key and the expiry of a creds text; refuses creds past Fabric's cutoff (exp - margin) or without a user JWT. */
export const natsCredsIdentity = (creds: string, input: { now?: number; cutoffSeconds?: number } = {}): { userKey: string; exp: number | undefined } => {
  const jwt = natsCredsJwt(creds);
  if (!jwt) throw new Error("FABRIC_MESH_STATE_NATS_NO_CREDS: the NATS creds hold no user JWT");
  const claims = decodeNatsJwt(jwt);
  if (!/^U[A-Z2-7]{55}$/.test(claims.sub)) throw new Error("FABRIC_MESH_STATE_NATS_NO_CREDS: the NATS creds are not a user's");
  if (claims.exp !== undefined && nowSeconds(input.now) >= claims.exp - (input.cutoffSeconds ?? NATS_CREDS_CUTOFF_SECONDS)) {
    throw new Error(`FABRIC_MESH_STATE_NATS_CREDS_EXPIRED: the NATS user ${claims.sub} is past Fabric's cutoff (exp ${claims.exp}); relaunch through the issuer`);
  }
  return { userKey: claims.sub, exp: claims.exp };
};

// ------------------------------------------------------------------ the embedded node

export interface LocalNatsServer { url: string; httpUrl: string; process: ChildProcess; stop(): Promise<void> }

/** A free loopback TCP port (bound and released). */
export const freeLoopbackPort = (): Promise<number> => new Promise((resolve, reject) => {
  const server = net.createServer();
  server.once("error", reject);
  server.listen(0, "127.0.0.1", () => {
    const address = server.address();
    server.close(() => (typeof address === "object" && address ? resolve(address.port) : reject(new Error("no port"))));
  });
});

/**
 * Starts `nats-server -c <config>` as a child (the embedded single node) and waits until its monitoring endpoint
 * answers. The config's listeners decide the bind address: `initLocalNats` writes 127.0.0.1 only. `stop()` ends it.
 */
export const startLocalNatsServer = async (input: { binary: string; configFile: string; url: string; httpUrl: string; timeoutMs?: number;
  /** "server": up and answering, before JetStream has a cluster quorum (a cluster node starting alone). Default: healthy. */
  ready?: "healthy" | "server" }): Promise<LocalNatsServer> => {
  const child = spawn(input.binary, ["-c", input.configFile], { stdio: ["ignore", "ignore", "pipe"] });
  let stderr = "";
  child.stderr?.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString("utf8")).slice(-4_000); });
  const exited = new Promise<void>(resolve => child.once("exit", () => resolve()));
  const stop = async (): Promise<void> => { if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM"); await exited; };
  const deadline = Date.now() + (input.timeoutMs ?? 10_000);
  for (;;) {
    if (child.exitCode !== null) throw new Error(`nats-server exited ${child.exitCode}: ${stderr}`);
    try {
      const response = await fetch(new URL(input.ready === "server" ? "/varz" : "/healthz", input.httpUrl), { signal: AbortSignal.timeout(500) });
      if (response.ok) break;
    } catch { /* not up yet */ }
    if (Date.now() > deadline) { await stop(); throw new Error(`nats-server did not become healthy: ${stderr}`); }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  return { url: input.url, httpUrl: input.httpUrl, process: child, stop };
};
