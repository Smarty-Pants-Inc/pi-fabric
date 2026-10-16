import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { nkeys } from "@nats-io/transport-node";
import type { NatsRole, RoleTemplate } from "./permissions.js";

/**
 * smarty-dev#7504 slice 1 (design 1.1, 1.4, 2.1, 10.5, 10.6): minimal NATS JWT v2 encoder, public-claims decoder and
 * user `.creds` formatter over the installed nkeys (reached through the direct `@nats-io/transport-node` export).
 *
 * ponytail: D4 (decided, Amendment B 10.6) — an own small encoder that follows the Go reference
 * `github.com/nats-io/jwt/v2` (claims.go header/segments/jti, user_claims.go, account_claims.go, operator_claims.go,
 * signingkeys.go, creds_utils.go), instead of `@nats-io/jwt`: that package is not installed, is pre-1.0, hashes the
 * whole claim (standard base64 SHA-512) for `jti` instead of Go's base32 SHA-512/256 over the base claims, and forces
 * `iat = now`, so it cannot back-date `iat` (design 1.4). No new dependency, no bare transitive import, no jose.
 * The oracle for server acceptance is the hosted nats-server 2.14.7 (E1 T2/T8, owner fabric-store); an `nsc`
 * cross-check is unconfirmed. These encoders prove self-consistency only.
 *
 * Secrets: callers pass a `Signer` (public key + sign callback); seeds never cross the encoder API. Errors carry a
 * fixed field name only, never an input value, a public key, a JWT or a seed. `decodeAndVerifyNatsJwt` checks the
 * issuer signature and claim shape; it is NOT an operator-membership proof: callers (the minter) bind the
 * operator -> account -> signing-key chain and the SYS identity themselves.
 */

export interface Signer {
  publicKey: string;
  sign(bytes: Uint8Array): Uint8Array;
}

export interface EncodeClock {
  nowSeconds(): number;
}

export type Clock = EncodeClock;

export interface ScopedSigningKey {
  key: string;
  role: NatsRole | "operator";
  template: RoleTemplate;
  description?: string;
}

/** Go `OperatorLimits` subset this slice encodes; integers, -1 = unlimited where Go allows it. */
export interface AccountLimitsInput {
  subs?: number;
  data?: number;
  payload?: number;
  imports?: number;
  exports?: number;
  conn?: number;
  leaf?: number;
  mem_storage?: number;
  disk_storage?: number;
  streams?: number;
  consumer?: number;
}

export interface OperatorJwtInput {
  name: string;
  signingKeys: string[];
  systemAccount: string;
}

export interface AccountJwtInput {
  name: string;
  subject: string;
  signingKeys: ScopedSigningKey[] | string[];
  limits: AccountLimitsInput;
}

export interface UserJwtInput {
  name: string;
  subject: string;
  issuerAccount: string;
  tags: string[];
  expiresAt: number;
  iatBackdateSeconds?: number;
}

export interface SystemUserJwtInput {
  name: string;
  subject: string;
  expiresAt: number;
  iatBackdateSeconds?: number;
}

export interface DecodedNatsJwt {
  header: { typ: "JWT"; alg: "ed25519-nkey" };
  claims: Record<string, unknown> & {
    iss: string;
    sub: string;
    iat: number;
    jti: string;
    exp?: number;
    nats: Record<string, unknown> & { type: "operator" | "account" | "user"; version: 2 };
  };
}

export class NatsJwtError extends Error {
  readonly field: string;
  constructor(field: string) {
    super(`invalid ${field}`);
    this.name = "NatsJwtError";
    this.field = field;
  }
}

const HEADER_TEXT = '{"typ":"JWT","alg":"ed25519-nkey"}';
const MAX_TOKEN_BYTES = 1024 * 1024; // Go decoder.go limit
const DEFAULT_IAT_BACKDATE = 60;
const MAX_IAT_BACKDATE = 300;
const MAX_USER_LIFETIME = 90 * 86_400; // longest role lifetime (principal, design 1.4)
const MAX_SYSTEM_USER_LIFETIME = 3_600; // SYS-only user, design 10.5
const MAX_TAGS = 128;
const NO_LIMIT = -1;

const PUBLIC_KEY = /^[OAU][A-Z2-7]{55}$/;
const SEGMENT = /^[A-Za-z0-9_-]+$/;
const JTI = /^[A-Z2-7]{52}$/;
// Printable ASCII without JSON/HTML-sensitive characters; JS regexes without the m flag anchor at the whole input, so a
// trailing LF/CR/U+2028/U+2029 never matches.
const NAME = /^[A-Za-z0-9][A-Za-z0-9._:@/+-]{0,254}$/;
const SECRET_SHAPE = /S[OAU][A-Z2-7]{20,}|eyJ/;
const TAG = /^[a-z][a-z0-9_-]{0,31}:[a-z0-9][a-z0-9._-]{0,127}$/;
const MESH_TAG_VALUE = /^[0-9a-f]{32}$/;
// One NATS subject, optionally with `{{subject()}}` / `{{tag(x)}}` / `{{name()}}` templates; no whitespace.
const SUBJECT = /^[A-Za-z0-9_$*>{}().:-]+$/;
const ROLES: readonly string[] = ["agent", "principal", "maintainer", "break-glass", "operator"];

const utf8 = new TextEncoder();
const strictUtf8 = new TextDecoder("utf-8", { fatal: true });
const systemClock: EncodeClock = { nowSeconds: () => Math.floor(Date.now() / 1_000) };

function fail(field: string): never {
  throw new NatsJwtError(field);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === null || Object.getPrototypeOf(proto) === null;
}

function isBytes(value: unknown): value is Uint8Array {
  return Object.prototype.toString.call(value) === "[object Uint8Array]";
}

/** Plain object with only the allowed own keys; an unknown key is refused without naming it. */
function record(value: unknown, field: string, required: readonly string[], optional: readonly string[] = []): Record<string, unknown> {
  if (!isPlainObject(value)) fail(field);
  const allowed = new Set([...required, ...optional]);
  for (const key of Object.keys(value)) if (!allowed.has(key)) fail(`${field} (unsupported field)`);
  for (const key of required) if (value[key] === undefined) fail(`${field}.${key}`);
  return value;
}

/** NKey public key of one class, CRC and prefix checked by the installed nkeys. */
function publicKey(value: unknown, prefix: "O" | "A" | "U", field: string): string {
  if (typeof value !== "string" || !PUBLIC_KEY.test(value) || value[0] !== prefix) fail(field);
  try {
    nkeys.fromPublic(value);
  } catch {
    return fail(field);
  }
  return value;
}

function name(value: unknown, field: string): string {
  if (typeof value !== "string" || !NAME.test(value) || SECRET_SHAPE.test(value)) fail(field);
  return value;
}

function nowOf(clock: EncodeClock): number {
  let now: unknown;
  try {
    now = clock.nowSeconds();
  } catch {
    return fail("clock");
  }
  if (typeof now !== "number" || !Number.isSafeInteger(now) || now <= 0) fail("clock");
  return now;
}

function backdate(value: unknown): number {
  if (value === undefined) return DEFAULT_IAT_BACKDATE;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > MAX_IAT_BACKDATE) fail("iatBackdateSeconds");
  return value;
}

/** Expiry strictly after now (so after the back-dated iat) and within the lifetime ceiling. */
function expiry(value: unknown, now: number, maxLifetime: number): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= now || value - now > maxLifetime) fail("expiresAt");
  return value;
}

function tags(value: unknown): string[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > MAX_TAGS) fail("tags");
  const out: string[] = [];
  for (const tag of value) {
    // Strict lowercase k:v, refused rather than lowercased (preflight Q-g).
    if (typeof tag !== "string" || !TAG.test(tag)) fail("tags");
    const separator = tag.indexOf(":");
    // `{{tag(mesh)}}` is substring-expanded into stream names: exactly 32 lowercase hex, never a wildcard or template.
    if (tag.slice(0, separator) === "mesh" && !MESH_TAG_VALUE.test(tag.slice(separator + 1))) fail("tags");
    out.push(tag);
  }
  if (new Set(out).size !== out.length) fail("tags");
  return out;
}

function subjects(value: unknown, field: string, literal: boolean): string[] {
  if (!Array.isArray(value) || value.length > 4_096) fail(field);
  const out: string[] = [];
  for (const subject of value) {
    if (typeof subject !== "string" || subject.length > 1_024 || !SUBJECT.test(subject)) fail(field);
    // A missing tag in a deny entry fails authentication, so denies are literal.
    if (literal && subject.includes("{")) fail(field);
    out.push(subject);
  }
  if (new Set(out).size !== out.length) fail(field);
  return out;
}

// Go encoding/json escapes <, >, & and U+2028/U+2029; the jti hash and the payload use the same bytes.
function goJson(value: unknown): string {
  return JSON.stringify(value)
    .replace(/</g, "\\u003c")
    .replace(/>/g, "\\u003e")
    .replace(/&/g, "\\u0026")
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029");
}

function base32NoPad(bytes: Uint8Array): string {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  let out = "";
  let value = 0;
  let bits = 0;
  for (const byte of bytes) {
    value = ((value << 8) | byte) & 0xffff;
    bits += 8;
    while (bits >= 5) {
      out += alphabet[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) out += alphabet[(value << (5 - bits)) & 31];
  return out;
}

const b64url = (bytes: Uint8Array | string) => Buffer.from(bytes).toString("base64url");

/** Validates the signer's class before it signs anything; then requires a 64-byte signature verifying under its key. */
function checkedSigner(signer: unknown, prefix: "O" | "A"): { publicKey: string; sign(bytes: Uint8Array): Uint8Array } {
  if (typeof signer !== "object" || signer === null) fail("signer");
  const candidate = signer as Partial<Signer>;
  const key = publicKey(candidate.publicKey, prefix, "signer");
  const sign = candidate.sign;
  if (typeof sign !== "function") fail("signer");
  return {
    publicKey: key,
    sign(bytes) {
      let signature: unknown;
      try {
        signature = sign.call(signer, bytes);
      } catch {
        return fail("signer");
      }
      if (!isBytes(signature) || signature.length !== 64) fail("signer");
      let verified = false;
      try {
        verified = nkeys.fromPublic(key).verify(bytes, signature);
      } catch {
        verified = false;
      }
      if (!verified) fail("signer");
      return Uint8Array.from(signature);
    },
  };
}

interface BaseClaims {
  exp?: number;
  iat: number;
  iss: string;
  name: string;
  sub: string;
}

/** Go ClaimsData order (aud, exp, jti, iat, iss, name, nbf, sub) then `nats`; jti over the base claims without jti. */
function encode(base: BaseClaims, nats: Record<string, unknown>, signer: ReturnType<typeof checkedSigner>): string {
  const hashed: Record<string, unknown> = {};
  if (base.exp !== undefined) hashed.exp = base.exp;
  hashed.iat = base.iat;
  hashed.iss = base.iss;
  hashed.name = base.name;
  hashed.sub = base.sub;
  const jti = base32NoPad(createHash("sha512-256").update(goJson(hashed)).digest());
  const claims: Record<string, unknown> = {};
  if (base.exp !== undefined) claims.exp = base.exp;
  claims.jti = jti;
  claims.iat = base.iat;
  claims.iss = base.iss;
  claims.name = base.name;
  claims.sub = base.sub;
  claims.nats = nats;
  const signingInput = `${b64url(HEADER_TEXT)}.${b64url(goJson(claims))}`;
  const signature = signer.sign(utf8.encode(signingInput));
  return `${signingInput}.${b64url(signature)}`;
}

/** Operator JWT, self-signed by the operator identity key (iss = sub = O). */
export function encodeOperatorJwt(input: OperatorJwtInput, signer: Signer, clock: EncodeClock): string {
  const value = record(input, "input", ["name", "signingKeys", "systemAccount"]);
  const operatorName = name(value.name, "name");
  const identity = checkedSigner(signer, "O");
  if (!Array.isArray(value.signingKeys) || value.signingKeys.length === 0 || value.signingKeys.length > 64) fail("signingKeys");
  const signingKeys = value.signingKeys.map((key) => publicKey(key, "O", "signingKeys"));
  if (new Set(signingKeys).size !== signingKeys.length || signingKeys.includes(identity.publicKey)) fail("signingKeys");
  const systemAccount = publicKey(value.systemAccount, "A", "systemAccount");
  const now = nowOf(clock);
  return encode(
    { iat: now, iss: identity.publicKey, name: operatorName, sub: identity.publicKey },
    { signing_keys: signingKeys, system_account: systemAccount, type: "operator", version: 2 },
    identity,
  );
}

// Go OperatorLimits field order: NatsLimits, AccountLimits, JetStreamLimits.
const UNLIMITED_LIMIT_KEYS = ["subs", "data", "payload", "imports", "exports", "conn", "leaf"] as const;
const JETSTREAM_LIMIT_KEYS = ["mem_storage", "disk_storage", "streams", "consumer"] as const;
const LIMIT_ORDER = ["subs", "data", "payload", "imports", "exports", "conn", "leaf", ...JETSTREAM_LIMIT_KEYS] as const;
// Go NewAccountClaims defaults: a v2 claim decoded without them gets zero, and `conn: 0` refuses every connection.
const FILLED_DEFAULTS = ["subs", "data", "payload", "conn", "leaf"] as const;

function limits(value: unknown): Record<string, number> {
  const input = record(value, "limits", [], LIMIT_ORDER);
  for (const key of UNLIMITED_LIMIT_KEYS) {
    const limit = input[key];
    if (limit !== undefined && (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < NO_LIMIT)) fail(`limits.${key}`);
  }
  // JetStream quotas are finite here (no -1); zero is kept as given (SYS: JetStream disabled). The minter requires them.
  for (const key of JETSTREAM_LIMIT_KEYS) {
    const limit = input[key];
    if (limit !== undefined && (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 0)) fail(`limits.${key}`);
  }
  const out: Record<string, number> = {};
  for (const key of LIMIT_ORDER) {
    const limit = input[key] as number | undefined;
    if (limit !== undefined) out[key] = limit;
    else if ((FILLED_DEFAULTS as readonly string[]).includes(key)) out[key] = NO_LIMIT;
  }
  return out;
}

function template(value: unknown): Record<string, unknown> {
  const input = record(value, "signingKeys.template", ["pub", "sub"]);
  const pub = record(input.pub, "signingKeys.template", ["allow", "deny"]);
  const sub = record(input.sub, "signingKeys.template", ["allow"], ["deny"]);
  const subOut: Record<string, unknown> = { allow: subjects(sub.allow, "signingKeys.template", false) };
  if (sub.deny !== undefined) subOut.deny = subjects(sub.deny, "signingKeys.template", true);
  // Go NewUserScope defaults: subs/data/payload -1 (user limits explicit on the template, absent on scoped users).
  return {
    pub: { allow: subjects(pub.allow, "signingKeys.template", false), deny: subjects(pub.deny, "signingKeys.template", true) },
    sub: subOut,
    subs: NO_LIMIT,
    data: NO_LIMIT,
    payload: NO_LIMIT,
  };
}

function accountSigningKeys(value: unknown, subject: string): Array<string | Record<string, unknown>> {
  if (!Array.isArray(value) || value.length > 64) fail("signingKeys");
  const plain = value.every((entry) => typeof entry === "string");
  const out: Array<{ key: string; claim: string | Record<string, unknown> }> = [];
  for (const entry of value) {
    if (plain) {
      const key = publicKey(entry, "A", "signingKeys");
      out.push({ key, claim: key });
      continue;
    }
    const scoped = record(entry, "signingKeys", ["key", "role", "template"], ["description"]);
    const key = publicKey(scoped.key, "A", "signingKeys");
    if (typeof scoped.role !== "string" || !ROLES.includes(scoped.role)) fail("signingKeys.role");
    let description = "";
    if (scoped.description !== undefined) {
      if (typeof scoped.description !== "string" || !/^[A-Za-z0-9 ._:/-]{0,256}$/.test(scoped.description)) fail("signingKeys.description");
      description = scoped.description;
    }
    // Go UserScope field order: kind, key, role, template, description.
    out.push({ key, claim: { kind: "user_scope", key, role: scoped.role, template: template(scoped.template), description } });
  }
  const keys = out.map((entry) => entry.key);
  if (new Set(keys).size !== keys.length || keys.includes(subject)) fail("signingKeys");
  // Go SigningKeys.MarshalJSON writes the entries sorted by key.
  return out.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0)).map((entry) => entry.claim);
}

/**
 * Account JWT signed by an operator key: scoped `user_scope` entries (or plain A keys, e.g. SYS), explicit Go
 * connection defaults, a deny-all default permission so an unscoped user never inherits a grant, and no
 * imports/exports. Whether the signer is listed in the operator JWT is the caller's (minter's) check.
 */
export function encodeAccountJwt(input: AccountJwtInput, operatorSigner: Signer, clock: EncodeClock): string {
  const value = record(input, "input", ["name", "subject", "signingKeys", "limits"]);
  const accountName = name(value.name, "name");
  const signer = checkedSigner(operatorSigner, "O");
  const subject = publicKey(value.subject, "A", "subject");
  const signingKeys = accountSigningKeys(value.signingKeys, subject);
  const accountLimits = limits(value.limits);
  const now = nowOf(clock);
  // Go Account field order: imports, exports, limits, signing_keys, revocations, default_permissions, ...; then
  // GenericFields. Empty signing_keys is omitted as Go's omitempty does.
  const nats: Record<string, unknown> = { limits: accountLimits };
  if (signingKeys.length > 0) nats.signing_keys = signingKeys;
  nats.default_permissions = { pub: { deny: [">"] }, sub: { deny: [">"] } };
  nats.type = "account";
  nats.version = 2;
  return encode({ iat: now, iss: signer.publicKey, name: accountName, sub: subject }, nats, signer);
}

/**
 * Scoped org user: signed by the role's scoped signing key (A, distinct from `issuer_account`). The `nats` block is
 * exactly issuer_account/tags/type/version: Go ValidateScopedSigner refuses any user permission or limit, the
 * account's scope template supplies them.
 */
export function encodeUserJwt(input: UserJwtInput, scopedSigner: Signer, clock: EncodeClock): string {
  const value = record(input, "input", ["name", "subject", "issuerAccount", "tags", "expiresAt"], ["iatBackdateSeconds"]);
  const userName = name(value.name, "name");
  const signer = checkedSigner(scopedSigner, "A");
  const subject = publicKey(value.subject, "U", "subject");
  const issuerAccount = publicKey(value.issuerAccount, "A", "issuerAccount");
  if (issuerAccount === signer.publicKey) fail("issuerAccount");
  const userTags = tags(value.tags);
  const now = nowOf(clock);
  const exp = expiry(value.expiresAt, now, MAX_USER_LIFETIME);
  const iat = now - backdate(value.iatBackdateSeconds);
  if (iat <= 0) fail("clock");
  return encode(
    { exp, iat, iss: signer.publicKey, name: userName, sub: subject },
    { issuer_account: issuerAccount, tags: userTags, type: "user", version: 2 },
    signer,
  );
}

/**
 * SYS-only user (10.5): signed by the SYS account identity key, no issuer_account, publish ONLY
 * `$SYS.REQ.CLAIMS.UPDATE`, subscribe ONLY its own literal inbox, explicit Go user limits -1. No caller permission
 * field is accepted. Binding the signer to the operator's `system_account` is the minter's check.
 */
export function encodeSystemUserJwt(input: SystemUserJwtInput, sysIdentitySigner: Signer, clock: EncodeClock): string {
  const value = record(input, "input", ["name", "subject", "expiresAt"], ["iatBackdateSeconds"]);
  const userName = name(value.name, "name");
  const signer = checkedSigner(sysIdentitySigner, "A");
  const subject = publicKey(value.subject, "U", "subject");
  const now = nowOf(clock);
  const exp = expiry(value.expiresAt, now, MAX_SYSTEM_USER_LIFETIME);
  const iat = now - backdate(value.iatBackdateSeconds);
  if (iat <= 0) fail("clock");
  return encode(
    { exp, iat, iss: signer.publicKey, name: userName, sub: subject },
    {
      pub: { allow: ["$SYS.REQ.CLAIMS.UPDATE"] },
      sub: { allow: [`_INBOX.${subject}.>`] },
      subs: NO_LIMIT,
      data: NO_LIMIT,
      payload: NO_LIMIT,
      type: "user",
      version: 2,
    },
    signer,
  );
}

function decodeSegment(segment: string, field: string): Uint8Array {
  if (!SEGMENT.test(segment)) fail(field);
  const bytes = Buffer.from(segment, "base64url");
  // Canonical raw base64url only: re-encoding must give the same text (no padding, no stray trailing bits).
  if (bytes.toString("base64url") !== segment) fail(field);
  return new Uint8Array(bytes);
}

function parseJson(bytes: Uint8Array, field: string): unknown {
  try {
    return JSON.parse(strictUtf8.decode(bytes));
  } catch {
    return fail(field);
  }
}

function optionalTime(value: unknown, field: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) fail(field);
  return value;
}

const CLAIM_KEYS = ["aud", "exp", "jti", "iat", "iss", "name", "nbf", "sub", "nats"];
const ISSUER_CLASS = { operator: "O", account: "O", user: "A" } as const;
const SUBJECT_CLASS = { operator: "O", account: "A", user: "U" } as const;

/**
 * Parses and verifies a public NATS v2 JWT: canonical raw base64url segments, the exact v2 header, version 2, a known
 * type, issuer/subject NKey classes for that type, the issuer's ed25519 signature, a required iat, and exp/nbf against
 * the clock (expired at exp). It does NOT prove operator membership, the SYS identity or a scoped-key binding:
 * callers bind those. Errors never echo the token, a key or a claim value.
 */
export function decodeAndVerifyNatsJwt(token: string, clock: EncodeClock = systemClock): DecodedNatsJwt {
  if (typeof token !== "string" || token.length === 0 || token.length > MAX_TOKEN_BYTES) fail("jwt");
  const segments = token.split(".");
  if (segments.length !== 3) fail("jwt (segments)");
  const [headerSegment, payloadSegment, signatureSegment] = segments as [string, string, string];
  const header = parseJson(decodeSegment(headerSegment, "jwt (header)"), "jwt (header)");
  if (!isPlainObject(header) || Object.keys(header).length !== 2 || header.typ !== "JWT" || header.alg !== "ed25519-nkey") fail("jwt (header)");
  const claims = parseJson(decodeSegment(payloadSegment, "jwt (payload)"), "jwt (payload)");
  const signature = decodeSegment(signatureSegment, "jwt (signature)");
  if (!isPlainObject(claims) || !isPlainObject(claims.nats)) fail("jwt (claims)");
  for (const key of Object.keys(claims)) if (!CLAIM_KEYS.includes(key)) fail("jwt (claims)");
  const nats = claims.nats;
  const type = nats.type;
  if (type !== "operator" && type !== "account" && type !== "user") fail("jwt (type)");
  if (nats.version !== 2) fail("jwt (version)");
  const iss = publicKey(claims.iss, ISSUER_CLASS[type], "jwt (issuer)");
  publicKey(claims.sub, SUBJECT_CLASS[type], "jwt (subject)");
  // The issuer's signature over the exact "<header>.<payload>" ASCII bytes (Go decoder.go).
  let verified = false;
  try {
    verified = signature.length === 64 && nkeys.fromPublic(iss).verify(utf8.encode(`${headerSegment}.${payloadSegment}`), signature);
  } catch {
    verified = false;
  }
  if (!verified) fail("jwt (signature)");
  if (typeof claims.jti !== "string" || !JTI.test(claims.jti)) fail("jwt (jti)");
  if (claims.name !== undefined && typeof claims.name !== "string") fail("jwt (name)");
  if (claims.aud !== undefined && typeof claims.aud !== "string") fail("jwt (aud)");
  if (type === "operator" && claims.iss !== claims.sub) fail("jwt (issuer)");
  if (type === "user" && nats.issuer_account !== undefined) publicKey(nats.issuer_account, "A", "jwt (issuer_account)");
  const iat = optionalTime(claims.iat, "jwt (iat)");
  if (iat === undefined) fail("jwt (iat)");
  const exp = optionalTime(claims.exp, "jwt (exp)");
  const nbf = optionalTime(claims.nbf, "jwt (nbf)");
  const now = nowOf(clock);
  if (exp !== undefined && (exp <= iat || now >= exp)) fail("jwt (expired)");
  if (nbf !== undefined && nbf > now) fail("jwt (not yet valid)");
  return { header: { typ: "JWT", alg: "ed25519-nkey" }, claims: claims as DecodedNatsJwt["claims"] };
}

const SEED_BYTES = 58;
const BASE32_BYTE = (byte: number) => (byte >= 0x41 && byte <= 0x5a) || (byte >= 0x32 && byte <= 0x37);

/**
 * User `.creds` in the Go creds_utils.go layout (byte-exact), read by nats-core `credsAuthenticator`. The JWT must
 * verify (decodeAndVerifyNatsJwt, so an expired user JWT is refused; a hosted expired-user negative fixture builds
 * its own raw creds) and the seed must be the `SU` seed of the JWT subject. The seed is never turned into a string;
 * the temporary key pair is built over a private copy and cleared, never the caller's buffer.
 */
export function formatUserCreds(jwt: string, userSeed: Uint8Array): Uint8Array {
  let decoded: DecodedNatsJwt;
  try {
    decoded = decodeAndVerifyNatsJwt(jwt);
  } catch {
    return fail("jwt");
  }
  if (decoded.claims.nats.type !== "user") fail("jwt");
  if (!isBytes(userSeed) || userSeed.length !== SEED_BYTES || userSeed[0] !== 0x53 || userSeed[1] !== 0x55) fail("seed");
  for (const byte of userSeed) if (!BASE32_BYTE(byte)) fail("seed");
  const seed = Uint8Array.from(userSeed);
  let owner: string | undefined;
  try {
    const pair = nkeys.fromSeed(seed);
    try {
      owner = pair.getPublicKey();
    } finally {
      pair.clear();
    }
  } catch {
    owner = undefined;
  }
  try {
    if (owner === undefined || owner[0] !== "U" || owner !== decoded.claims.sub) fail("seed");
    const head = utf8.encode(
      `-----BEGIN NATS USER JWT-----\n${jwt}\n------END NATS USER JWT------\n\n` +
        "************************* IMPORTANT *************************\n" +
        "NKEY Seed printed below can be used to sign and prove identity.\n" +
        "NKEYs are sensitive and should be treated as secrets.\n\n" +
        "-----BEGIN USER NKEY SEED-----\n",
    );
    const tail = utf8.encode("\n------END USER NKEY SEED------\n\n*************************************************************\n");
    const out = new Uint8Array(head.length + userSeed.length + tail.length);
    out.set(head, 0);
    out.set(userSeed, head.length);
    out.set(tail, head.length + userSeed.length);
    return out;
  } finally {
    seed.fill(0);
  }
}
