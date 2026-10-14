import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { credsAuthenticator, nkeys } from "@nats-io/transport-node";
import { beforeAll, describe, expect, it } from "vitest";

// smarty-dev#7504 phase 1 slice 1: NATS decentralized-auth claims (frozen Amendment B, sections 1.1, 1.4, 2.1, 9.2, 10.5).
// RED-first: original 15 cases plus Amendment B SYS-JWT, public decoder and retirement-guard cases. Each case loads its
// module after asserting presence, so absent implementations fail as labelled RED (not a collection error or regression).
// The GREEN run uses these same cases and code paths.
//
// Secret hygiene: every key is generated at runtime by the installed nkeys (via @nats-io/transport-node). Assertions that
// touch seeds, .creds bytes or whole JWT strings compare booleans or lengths only, so a failure never prints them.
// Decoded claims (public keys, names, tags) are not secrets and may appear in a diff.
//
// Decision C (why unit, not E2E): these check self-consistency with the installed NATS primitives (nkeys signature
// verification, nats-core credsAuthenticator) and with the Go nats-io/jwt v2 reference layout. They cannot prove that
// nats-server accepts the JWTs; that is E1 T2/T8 on the pinned real nats-server 2.14.7 in hosted CI (owner fabric-store).

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const JWT_SOURCE = join(repoRoot, "src/mesh/nats-auth/jwt.ts");
const PERMISSIONS_SOURCE = join(repoRoot, "src/mesh/nats-auth/permissions.ts");
const RETIREMENT_SOURCE = join(repoRoot, "src/mesh/nats-auth/retirement.ts");

type NatsRole = "agent" | "principal" | "maintainer";
interface Signer {
  publicKey: string;
  sign(bytes: Uint8Array): Uint8Array;
}
interface Clock {
  nowSeconds(): number;
}
type EncodeClock = Clock;
interface RoleTemplate {
  pub: { allow: string[]; deny: string[] };
  sub: { allow: string[] };
}
interface ScopedSigningKey {
  key: string;
  role: NatsRole;
  template: RoleTemplate;
  description?: string;
}
interface JwtModule {
  encodeOperatorJwt(input: { name: string; signingKeys: string[]; systemAccount: string }, signer: Signer, clock: Clock): string;
  encodeAccountJwt(
    input: { name: string; subject: string; signingKeys: ScopedSigningKey[] | string[]; limits: Record<string, unknown> },
    operatorSigner: Signer,
    clock: Clock,
  ): string;
  encodeUserJwt(
    input: {
      name: string;
      subject: string;
      issuerAccount: string;
      tags: string[];
      expiresAt: number;
      iatBackdateSeconds?: number;
    },
    scopedSigner: Signer,
    clock: Clock,
  ): string;
  encodeSystemUserJwt(
    input: { name: string; subject: string; expiresAt: number; iatBackdateSeconds?: number },
    sysIdentitySigner: Signer,
    clock: Clock,
  ): string;
  decodeAndVerifyNatsJwt(jwt: string): {
    header: Record<string, unknown>;
    claims: Record<string, unknown> & { nats: Record<string, unknown> };
  };
  formatUserCreds(jwt: string, userSeed: Uint8Array): Uint8Array;
}
interface IssuedCredential {
  publicKey: string;
  role: "agent" | "principal" | "maintainer" | "break-glass";
  signer: string;
  expiresAt: number;
}
interface RetirementModule {
  assertSigningKeyRetirable(signingKey: string, credentials: readonly IssuedCredential[], clock: EncodeClock): void;
}
interface PermissionsModule {
  roleTemplate(role: NatsRole, input: { custodyStream: string; livenessStream: string }): RoleTemplate;
}

async function loadSource<T>(path: string): Promise<T> {
  // Absence is labelled as RED for a contract not yet written, not as a regression of earlier behavior.
  expect(existsSync(path), `RED (#7504, implementation absent): ${relative(repoRoot, path)} does not exist yet`).toBe(true);
  return (await import(/* @vite-ignore */ path)) as T;
}
const loadJwt = () => loadSource<JwtModule>(JWT_SOURCE);
const loadPermissions = () => loadSource<PermissionsModule>(PERMISSIONS_SOURCE);
async function loadRetirement(): Promise<RetirementModule> {
  expect(
    existsSync(RETIREMENT_SOURCE),
    `RED (#7504, implementation absent): ${relative(repoRoot, RETIREMENT_SOURCE)} does not exist yet`,
  ).toBe(true);
  const module = (await import(/* @vite-ignore */ RETIREMENT_SOURCE)) as Partial<RetirementModule>;
  expect(
    typeof module.assertSigningKeyRetirable === "function",
    "RED (#7504, retirement export absent): assertSigningKeyRetirable is not exported yet",
  ).toBe(true);
  return module as RetirementModule;
}

type KeyPair = ReturnType<typeof nkeys.createUser>;
const signerOf = (kp: KeyPair): Signer => ({ publicKey: kp.getPublicKey(), sign: (bytes) => kp.sign(bytes) });
const NOW = 1_800_000_000;
const clock: Clock = { nowSeconds: () => NOW };
const clockAt = (seconds: number): EncodeClock => ({ nowSeconds: () => seconds });
const DAY = 86_400;
const HOUR = 3_600;
const NINETY_DAYS = 90 * DAY;
const MESH_HEX = "0123456789abcdef0123456789abcdef";
// Fixture names only. The real <CUS>/<LIVE> names are open item O-2 and are not invented here.
const STREAMS = { custodyStream: "FIXTURE_CUSTODY", livenessStream: "KV_FIXTURE_LIVENESS" };
const LIMITS = { conn: -1, subs: -1, data: -1, payload: -1, mem_storage: 0, disk_storage: 1_073_741_824, streams: 64, consumer: 256 };

const utf8 = new TextEncoder();
const SEGMENT = /^[A-Za-z0-9_-]+$/;
const SEED_SHAPE = /S[OAU][A-Z2-7]{56}/;
const GO_BASE_ORDER = ["aud", "exp", "iat", "iss", "name", "nbf", "sub"] as const;
const GO_CLAIM_ORDER = ["aud", "exp", "jti", "iat", "iss", "name", "nbf", "sub", "nats"] as const;

interface Decoded {
  headerText: string;
  payloadText: string;
  claims: Record<string, unknown> & { nats: Record<string, unknown> };
  segments: string[];
  signature: Uint8Array;
}

function decode(jwt: string): Decoded {
  const segments = jwt.split(".");
  expect(segments.length).toBe(3);
  for (const segment of segments) expect(SEGMENT.test(segment), "segments are unpadded base64url").toBe(true);
  const [h, p, s] = segments as [string, string, string];
  const headerText = Buffer.from(h, "base64url").toString("utf8");
  const payloadText = Buffer.from(p, "base64url").toString("utf8");
  return { headerText, payloadText, claims: JSON.parse(payloadText), segments, signature: new Uint8Array(Buffer.from(s, "base64url")) };
}

function signClaims(claims: Record<string, unknown>, signer: Signer, headerText = '{"typ":"JWT","alg":"ed25519-nkey"}'): string {
  const header = Buffer.from(headerText, "utf8").toString("base64url");
  const payload = Buffer.from(JSON.stringify(claims), "utf8").toString("base64url");
  const signingInput = `${header}.${payload}`;
  const signature = signer.sign(utf8.encode(signingInput));
  return `${signingInput}.${Buffer.from(signature).toString("base64url")}`;
}

function verifiesWith(jwt: string, publicKey: string): boolean {
  const [h, p] = jwt.split(".");
  const { signature } = decode(jwt);
  return nkeys.fromPublic(publicKey).verify(utf8.encode(`${h}.${p}`), signature);
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

// Go nats-io/jwt/v2 claims.go: jti = base32(no pad) of SHA-512/256 over json.Marshal(ClaimsData) with jti empty, so only
// the 8 base claims in struct order, omitempty, with Go json.Marshal's HTML-safe escaping of <, >, &, U+2028, U+2029.
function goJson(value: unknown): string {
  return JSON.stringify(value)
    .replace(/</g, "\\u003c")
    .replace(/>/g, "\\u003e")
    .replace(/&/g, "\\u0026")
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029");
}

function referenceJti(claims: Record<string, unknown>): string {
  const base: Record<string, unknown> = {};
  for (const key of GO_BASE_ORDER) {
    const value = claims[key];
    if (value !== undefined && value !== "" && value !== 0) base[key] = value;
  }
  return base32NoPad(createHash("sha512-256").update(goJson(base)).digest());
}

function expectGoShape(decoded: Decoded) {
  expect(decoded.headerText).toBe('{"typ":"JWT","alg":"ed25519-nkey"}');
  expect(decoded.signature.length).toBe(64);
  const keys = Object.keys(decoded.claims);
  expect(keys).toEqual(GO_CLAIM_ORDER.filter((key) => keys.includes(key)));
  expect(typeof decoded.claims.jti).toBe("string");
  expect(/^[A-Z2-7]{52}$/.test(decoded.claims.jti as string)).toBe(true);
  expect(decoded.claims.jti).toBe(referenceJti(decoded.claims));
  expect(Number.isInteger(decoded.claims.iat)).toBe(true);
  expect(decoded.claims.nats.version).toBe(2);
}

function expectNoSeed(text: string, seeds: Uint8Array[]) {
  for (const seed of seeds) expect(text.includes(new TextDecoder().decode(seed))).toBe(false);
  expect(SEED_SHAPE.test(text)).toBe(false);
}

function thrownText(fn: () => unknown): string | undefined {
  try {
    fn();
  } catch (error) {
    return error instanceof Error ? `${error.name} ${error.message} ${String(error.stack)}` : String(error);
  }
  return undefined;
}

let operatorIdentity: KeyPair;
let operatorSigning: KeyPair;
let systemAccount: KeyPair;
let orgAccount: KeyPair;
let agentScope: KeyPair;
let principalScope: KeyPair;
let user: KeyPair;
let otherUser: KeyPair;
let allSeeds: Uint8Array[];

beforeAll(() => {
  operatorIdentity = nkeys.createOperator();
  operatorSigning = nkeys.createOperator();
  systemAccount = nkeys.createAccount();
  orgAccount = nkeys.createAccount();
  agentScope = nkeys.createAccount();
  principalScope = nkeys.createAccount();
  user = nkeys.createUser();
  otherUser = nkeys.createUser();
  allSeeds = [operatorIdentity, operatorSigning, systemAccount, orgAccount, agentScope, principalScope, user, otherUser].map((kp) =>
    kp.getSeed(),
  );
});

function agentUserInput(subject = user.getPublicKey()) {
  return {
    name: "agent/fixture-host/fixture-lane/session-0001",
    subject,
    issuerAccount: orgAccount.getPublicKey(),
    tags: ["host:fixture-host", "lane:fixture-lane", "session:session-0001", "role:agent", `mesh:${MESH_HEX}`],
    expiresAt: NOW + 7 * DAY,
  };
}

function systemUserInput(subject = user.getPublicKey()) {
  return { name: "fixture-sys-resolver", subject, expiresAt: NOW + HOUR };
}

function currentSystemUserJwt(encodeSystemUserJwt: JwtModule["encodeSystemUserJwt"]): { jwt: string; now: number } {
  const now = Math.floor(Date.now() / 1_000);
  return {
    now,
    jwt: encodeSystemUserJwt({ ...systemUserInput(), expiresAt: now + HOUR }, signerOf(systemAccount), clockAt(now)),
  };
}

function issuedCredential(
  publicKey: string,
  role: IssuedCredential["role"],
  signer: string,
  expiresAt: number,
): IssuedCredential {
  return { publicKey, role, signer, expiresAt };
}

describe("#7504 operator JWT", () => {
  it("is self-signed by the operator identity with Go v2 header, segments, jti and claims", async () => {
    const { encodeOperatorJwt } = await loadJwt();
    const jwt = encodeOperatorJwt(
      { name: "fixture-operator", signingKeys: [operatorSigning.getPublicKey()], systemAccount: systemAccount.getPublicKey() },
      signerOf(operatorIdentity),
      clock,
    );
    const decoded = decode(jwt);
    expectGoShape(decoded);
    expect(decoded.claims.iss).toBe(operatorIdentity.getPublicKey());
    expect(decoded.claims.sub).toBe(operatorIdentity.getPublicKey());
    expect(decoded.claims.name).toBe("fixture-operator");
    expect(decoded.claims.exp).toBeUndefined();
    expect(decoded.claims.nbf).toBeUndefined();
    expect(decoded.claims.nats).toMatchObject({
      signing_keys: [operatorSigning.getPublicKey()],
      system_account: systemAccount.getPublicKey(),
      type: "operator",
      version: 2,
    });
    expect(verifiesWith(jwt, operatorIdentity.getPublicKey())).toBe(true);
    expect(verifiesWith(jwt, operatorSigning.getPublicKey())).toBe(false);
    expectNoSeed(decoded.payloadText, allSeeds);
    expectNoSeed(jwt, allSeeds);
  });

  it("refuses a non-operator signer, a non-O signing key and a non-A system account", async () => {
    const { encodeOperatorJwt } = await loadJwt();
    const good = { name: "fixture-operator", signingKeys: [operatorSigning.getPublicKey()], systemAccount: systemAccount.getPublicKey() };
    expect(() => encodeOperatorJwt(good, signerOf(orgAccount), clock)).toThrow();
    expect(() => encodeOperatorJwt(good, signerOf(user), clock)).toThrow();
    expect(() => encodeOperatorJwt({ ...good, signingKeys: [orgAccount.getPublicKey()] }, signerOf(operatorIdentity), clock)).toThrow();
    expect(() => encodeOperatorJwt({ ...good, systemAccount: operatorSigning.getPublicKey() }, signerOf(operatorIdentity), clock)).toThrow();
  });
});

describe("#7504 account JWT", () => {
  it("is signed by the operator signing key, carries scoped keys with the role template and no imports/exports", async () => {
    const { encodeAccountJwt } = await loadJwt();
    const { roleTemplate } = await loadPermissions();
    const agentTemplate = roleTemplate("agent", STREAMS);
    const principalTemplate = roleTemplate("principal", STREAMS);
    const scoped: ScopedSigningKey[] = [
      { key: agentScope.getPublicKey(), role: "agent", template: agentTemplate, description: "agent launches" },
      { key: principalScope.getPublicKey(), role: "principal", template: principalTemplate },
    ];
    const jwt = encodeAccountJwt(
      { name: "fixture-org", subject: orgAccount.getPublicKey(), signingKeys: scoped, limits: LIMITS },
      signerOf(operatorSigning),
      clock,
    );
    const decoded = decode(jwt);
    expectGoShape(decoded);
    expect(decoded.claims.iss).toBe(operatorSigning.getPublicKey());
    expect(decoded.claims.sub).toBe(orgAccount.getPublicKey());
    expect(decoded.claims.name).toBe("fixture-org");
    expect(decoded.claims.nats.type).toBe("account");
    expect(decoded.claims.nats.limits).toMatchObject(LIMITS);
    expect(decoded.claims.nats.imports ?? []).toEqual([]);
    expect(decoded.claims.nats.exports ?? []).toEqual([]);
    // Non-scoped users must never inherit a grant from the account default.
    const defaults = decoded.claims.nats.default_permissions as RoleTemplate | undefined;
    expect(defaults?.pub?.allow ?? []).toEqual([]);
    expect((defaults?.sub as { allow?: string[] } | undefined)?.allow ?? []).toEqual([]);

    const keys = decoded.claims.nats.signing_keys as Array<Record<string, unknown>>;
    expect(keys.map((entry) => entry.key)).toEqual([...scoped.map((entry) => entry.key)].sort());
    const byKey = new Map(keys.map((entry) => [entry.key, entry]));
    for (const entry of scoped) {
      const claim = byKey.get(entry.key) as Record<string, unknown>;
      expect(claim.kind).toBe("user_scope");
      expect(claim.role).toBe(entry.role);
      // Go NewUserScope defaults: subs/data/payload -1; the permissions are exactly the role template.
      expect(claim.template).toEqual({ ...entry.template, subs: -1, data: -1, payload: -1 });
      if (entry.description === undefined) expect(claim.description ?? "").toBe("");
      else expect(claim.description).toBe(entry.description);
    }
    expect(verifiesWith(jwt, operatorSigning.getPublicKey())).toBe(true);
    expect(verifiesWith(jwt, operatorIdentity.getPublicKey())).toBe(false);
    expectNoSeed(decoded.payloadText, allSeeds);
  });

  it("accepts plain A-key signing keys (sorted strings) for an unscoped account such as SYS", async () => {
    const { encodeAccountJwt } = await loadJwt();
    const plain = [agentScope.getPublicKey(), principalScope.getPublicKey()];
    const jwt = encodeAccountJwt(
      { name: "SYS", subject: systemAccount.getPublicKey(), signingKeys: plain, limits: {} },
      signerOf(operatorSigning),
      clock,
    );
    const decoded = decode(jwt);
    expectGoShape(decoded);
    expect(decoded.claims.nats.signing_keys).toEqual([...plain].sort());
    expect(verifiesWith(jwt, operatorSigning.getPublicKey())).toBe(true);
  });

  it("refuses a non-operator signer, a non-A subject and a non-A scoped key", async () => {
    const { encodeAccountJwt } = await loadJwt();
    const { roleTemplate } = await loadPermissions();
    const template = roleTemplate("agent", STREAMS);
    const good = {
      name: "fixture-org",
      subject: orgAccount.getPublicKey(),
      signingKeys: [{ key: agentScope.getPublicKey(), role: "agent" as const, template }],
      limits: LIMITS,
    };
    expect(() => encodeAccountJwt(good, signerOf(orgAccount), clock)).toThrow();
    expect(() => encodeAccountJwt(good, signerOf(user), clock)).toThrow();
    expect(() => encodeAccountJwt({ ...good, subject: user.getPublicKey() }, signerOf(operatorSigning), clock)).toThrow();
    expect(() =>
      encodeAccountJwt(
        { ...good, signingKeys: [{ key: operatorSigning.getPublicKey(), role: "agent" as const, template }] },
        signerOf(operatorSigning),
        clock,
      ),
    ).toThrow();
    expect(() =>
      encodeAccountJwt({ ...good, signingKeys: [{ key: user.getPublicKey(), role: "agent" as const, template }] }, signerOf(operatorSigning), clock),
    ).toThrow();
  });
});

describe("#7504 scoped user JWT", () => {
  it("is signed by the scoped key with exactly issuer_account/tags/type/version 2 and no permissions or limits", async () => {
    const { encodeUserJwt } = await loadJwt();
    const input = agentUserInput();
    const jwt = encodeUserJwt(input, signerOf(agentScope), clock);
    const decoded = decode(jwt);
    expectGoShape(decoded);
    expect(decoded.claims.iss).toBe(agentScope.getPublicKey());
    expect(decoded.claims.sub).toBe(user.getPublicKey());
    expect(decoded.claims.name).toBe(input.name);
    expect(Object.keys(decoded.claims.nats)).toEqual(["issuer_account", "tags", "type", "version"]);
    expect(decoded.claims.nats).toEqual({ issuer_account: orgAccount.getPublicKey(), tags: input.tags, type: "user", version: 2 });
    expect(verifiesWith(jwt, agentScope.getPublicKey())).toBe(true);
    expect(verifiesWith(jwt, orgAccount.getPublicKey())).toBe(false);
    expectNoSeed(decoded.payloadText, allSeeds);
    expectNoSeed(jwt, allSeeds);
  });

  it("back-dates iat by 60 s by default, sets the explicit 7-day agent exp, and no nbf", async () => {
    const { encodeUserJwt } = await loadJwt();
    const decoded = decode(encodeUserJwt(agentUserInput(), signerOf(agentScope), clock));
    expect(decoded.claims.iat).toBe(NOW - 60);
    expect(decoded.claims.exp).toBe(NOW + 7 * DAY);
    expect(decoded.claims.nbf).toBeUndefined();
    const explicit = decode(encodeUserJwt({ ...agentUserInput(), iatBackdateSeconds: 0 }, signerOf(agentScope), clock));
    expect(explicit.claims.iat).toBe(NOW);
    expect(explicit.claims.jti).toBe(referenceJti(explicit.claims));
    expect(explicit.claims.jti).not.toBe(decoded.claims.jti);
  });

  it("gives each fresh U key its own subject and jti, both verifying, with jti over the base claims only", async () => {
    const { encodeUserJwt } = await loadJwt();
    const first = decode(encodeUserJwt(agentUserInput(user.getPublicKey()), signerOf(agentScope), clock));
    const second = decode(encodeUserJwt(agentUserInput(otherUser.getPublicKey()), signerOf(agentScope), clock));
    expect(first.claims.sub).not.toBe(second.claims.sub);
    expect(first.claims.jti).not.toBe(second.claims.jti);
    // Not jwt.js's whole-claim hash: the nats block must not feed the jti.
    const { jti: _jti, ...withoutJti } = first.claims;
    expect(first.claims.jti).not.toBe(base32NoPad(createHash("sha512-256").update(JSON.stringify(withoutJti)).digest()));
    for (const decoded of [first, second]) {
      const jwt = decoded.segments.join(".");
      expect(verifiesWith(jwt, agentScope.getPublicKey())).toBe(true);
    }
  });

  it("refuses wrong signer types, wrong subject/issuer prefixes, exp not after iat and malformed tags", async () => {
    const { encodeUserJwt } = await loadJwt();
    const good = agentUserInput();
    const scoped = signerOf(agentScope);
    expect(() => encodeUserJwt(good, signerOf(operatorSigning), clock)).toThrow();
    expect(() => encodeUserJwt(good, signerOf(otherUser), clock)).toThrow();
    expect(() => encodeUserJwt({ ...good, subject: orgAccount.getPublicKey() }, scoped, clock)).toThrow();
    expect(() => encodeUserJwt({ ...good, issuerAccount: user.getPublicKey() }, scoped, clock)).toThrow();
    expect(() => encodeUserJwt({ ...good, issuerAccount: operatorSigning.getPublicKey() }, scoped, clock)).toThrow();
    expect(() => encodeUserJwt({ ...good, expiresAt: NOW - 60 }, scoped, clock)).toThrow();
    expect(() => encodeUserJwt({ ...good, expiresAt: NOW - 61 }, scoped, clock)).toThrow();
    expect(() => encodeUserJwt({ ...good, expiresAt: Number.NaN }, scoped, clock)).toThrow();
    expect(() => encodeUserJwt({ ...good, expiresAt: NOW + 7 * DAY + 0.5 }, scoped, clock)).toThrow();
    expect(() => encodeUserJwt({ ...good, iatBackdateSeconds: -1 }, scoped, clock)).toThrow();
    for (const tag of ["mesh", "mesh: abc", "mesh:a b", "", ":x", "mesh:\n"]) {
      expect(() => encodeUserJwt({ ...good, tags: [...good.tags, tag] }, scoped, clock), JSON.stringify(tag)).toThrow();
    }
  });

  it("refuses a mesh tag that is not exactly 32 lowercase hex, so {{tag(mesh)}} cannot inject a wildcard or another name", async () => {
    const { encodeUserJwt } = await loadJwt();
    const good = agentUserInput();
    const bad = ["mesh:*", "mesh:>", `mesh:${MESH_HEX}.>`, `mesh:${MESH_HEX}*`, `mesh:${MESH_HEX.slice(1)}`, `mesh:${MESH_HEX}0`];
    bad.push(`mesh:${MESH_HEX.toUpperCase()}`, "mesh:0123456789abcdef0123456789abcdeg", `mesh:${MESH_HEX} `, "mesh:{{subject()}}");
    for (const tag of bad) {
      expect(() => encodeUserJwt({ ...good, tags: [...good.tags, tag] }, signerOf(agentScope), clock), JSON.stringify(tag)).toThrow();
    }
  });

  it("refuses uppercase tags instead of silently lowercasing them (preflight Q-g)", async () => {
    const { encodeUserJwt } = await loadJwt();
    const good = agentUserInput();
    expect(() => encodeUserJwt({ ...good, tags: [...good.tags, `MESH:${MESH_HEX}`] }, signerOf(agentScope), clock)).toThrow();
  });

  it("refuses a signer whose signature does not verify under its claimed public key", async () => {
    const { encodeUserJwt } = await loadJwt();
    const lying: Signer = { publicKey: agentScope.getPublicKey(), sign: (bytes) => principalScope.sign(bytes) };
    expect(() => encodeUserJwt(agentUserInput(), lying, clock)).toThrow();
  });

  it("chains user -> scoped key in account -> operator signing key in operator JWT", async () => {
    const { encodeOperatorJwt, encodeAccountJwt, encodeUserJwt } = await loadJwt();
    const { roleTemplate } = await loadPermissions();
    const operator = decode(
      encodeOperatorJwt(
        { name: "fixture-operator", signingKeys: [operatorSigning.getPublicKey()], systemAccount: systemAccount.getPublicKey() },
        signerOf(operatorIdentity),
        clock,
      ),
    );
    const accountJwt = encodeAccountJwt(
      {
        name: "fixture-org",
        subject: orgAccount.getPublicKey(),
        signingKeys: [{ key: agentScope.getPublicKey(), role: "agent", template: roleTemplate("agent", STREAMS) }],
        limits: LIMITS,
      },
      signerOf(operatorSigning),
      clock,
    );
    const account = decode(accountJwt);
    const userJwt = encodeUserJwt(agentUserInput(), signerOf(agentScope), clock);
    const userClaims = decode(userJwt).claims;

    expect((operator.claims.nats.signing_keys as string[]).includes(account.claims.iss as string)).toBe(true);
    expect(verifiesWith(accountJwt, account.claims.iss as string)).toBe(true);
    expect(userClaims.nats.issuer_account).toBe(account.claims.sub);
    const scopedKeys = (account.claims.nats.signing_keys as Array<{ key: string }>).map((entry) => entry.key);
    expect(scopedKeys.includes(userClaims.iss as string)).toBe(true);
    expect(verifiesWith(userJwt, userClaims.iss as string)).toBe(true);
  });
});

describe("#7504 SYS-only user JWT", () => {
  it("is a v2 SYS-identity-signed U JWT with only claims-update and its own inbox", async () => {
    const { encodeSystemUserJwt } = await loadJwt();
    const input = systemUserInput();
    const jwt = encodeSystemUserJwt(input, signerOf(systemAccount), clock);
    const decoded = decode(jwt);
    expectGoShape(decoded);
    expect(decoded.claims.iss).toBe(systemAccount.getPublicKey());
    expect(decoded.claims.iss).not.toBe(operatorSigning.getPublicKey());
    expect(decoded.claims.sub).toBe(user.getPublicKey());
    expect(decoded.claims.name).toBe(input.name);
    expect(decoded.claims.iat).toBe(NOW - 60);
    expect(decoded.claims.exp).toBe(NOW + HOUR);
    expect(decoded.claims.nbf).toBeUndefined();
    expect(Object.keys(decoded.claims.nats)).toEqual(["pub", "sub", "subs", "data", "payload", "type", "version"]);
    expect(decoded.claims.nats).toMatchObject({
      pub: { allow: ["$SYS.REQ.CLAIMS.UPDATE"] },
      sub: { allow: [`_INBOX.${user.getPublicKey()}.>`] },
      subs: -1,
      data: -1,
      payload: -1,
      type: "user",
      version: 2,
    });
    expect(decoded.claims.nats.issuer_account).toBeUndefined();
    expect(verifiesWith(jwt, systemAccount.getPublicKey())).toBe(true);
    expect(verifiesWith(jwt, operatorIdentity.getPublicKey())).toBe(false);
    expectNoSeed(decoded.payloadText, allSeeds);
    expectNoSeed(jwt, allSeeds);
  });

  it("does not grant or echo caller-supplied permission fields", async () => {
    const { encodeSystemUserJwt } = await loadJwt();
    const marker = "CALLER_PERMISSION_MARKER_7504";
    const input = {
      ...systemUserInput(),
      pub: { allow: ["$SYS.REQ.>", marker] },
      sub: { allow: ["_INBOX.>", marker] },
      permissions: { pub: [marker], sub: [marker] },
      issuer_account: marker,
      role: "maintainer",
    };
    let jwt: string | undefined;
    const errorText = thrownText(() => {
      jwt = encodeSystemUserJwt(input, signerOf(systemAccount), clock);
    });
    if (errorText !== undefined) {
      expect(errorText.includes(marker)).toBe(false);
      return;
    }
    const claims = decode(jwt as string).claims;
    expect(claims.nats.pub).toEqual({ allow: ["$SYS.REQ.CLAIMS.UPDATE"] });
    expect(claims.nats.sub).toEqual({ allow: [`_INBOX.${user.getPublicKey()}.>`] });
    expect(claims.nats.issuer_account).toBeUndefined();
    expect(JSON.stringify(claims).includes(marker)).toBe(false);
    expect(JSON.stringify(claims).includes("$SYS.REQ.>")).toBe(false);
  });

  it("refuses non-A signers and non-U subjects without echoing invalid public-key values", async () => {
    const { encodeSystemUserJwt } = await loadJwt();
    const input = systemUserInput();
    const cases: Array<[string, Signer, string]> = [
      [input.subject, signerOf(operatorSigning), operatorSigning.getPublicKey()],
      [input.subject, signerOf(otherUser), otherUser.getPublicKey()],
      [orgAccount.getPublicKey(), signerOf(systemAccount), orgAccount.getPublicKey()],
      [operatorIdentity.getPublicKey(), signerOf(systemAccount), operatorIdentity.getPublicKey()],
    ];
    for (const [subject, signer, invalidValue] of cases) {
      const errorText = thrownText(() => encodeSystemUserJwt({ ...input, subject }, signer, clock));
      expect(errorText !== undefined).toBe(true);
      expect((errorText ?? "").includes(invalidValue)).toBe(false);
      expectNoSeed(errorText ?? "", allSeeds);
    }
  });
});

describe("#7504 public NATS JWT verification", () => {
  it("verifies a valid SYS user JWT and returns its public header and claims", async () => {
    const { encodeSystemUserJwt, decodeAndVerifyNatsJwt } = await loadJwt();
    const { jwt } = currentSystemUserJwt(encodeSystemUserJwt);
    const decoded = decodeAndVerifyNatsJwt(jwt);
    expect(decoded.header).toEqual({ typ: "JWT", alg: "ed25519-nkey" });
    expect(decoded.claims.iss).toBe(systemAccount.getPublicKey());
    expect(decoded.claims.sub).toBe(user.getPublicKey());
    expect(decoded.claims.nats.type).toBe("user");
    expect(verifiesWith(jwt, decoded.claims.iss as string)).toBe(true);
    expect(JSON.stringify(decoded).includes(jwt)).toBe(false);
  });

  it.each(["segment-count", "padding", "base64url-alphabet"] as const)("refuses malformed token encoding %s without echoing the token", async (kind) => {
    const { encodeSystemUserJwt, decodeAndVerifyNatsJwt } = await loadJwt();
    const { jwt } = currentSystemUserJwt(encodeSystemUserJwt);
    const parts = jwt.split(".");
    const token = kind === "segment-count" ? "not-a-jwt" : kind === "padding" ? `${jwt}=` : `!.${parts[1]}.${parts[2]}`;
    const errorText = thrownText(() => decodeAndVerifyNatsJwt(token));
    expect(errorText !== undefined).toBe(true);
    expect((errorText ?? "").includes(token)).toBe(false);
    expectNoSeed(errorText ?? "", allSeeds);
  });

  it("refuses an invalid issuer signature without echoing the token", async () => {
    const { encodeSystemUserJwt, decodeAndVerifyNatsJwt } = await loadJwt();
    const { jwt } = currentSystemUserJwt(encodeSystemUserJwt);
    const [header, payload, signature] = jwt.split(".") as [string, string, string];
    const index = Math.floor(signature.length / 2);
    const alteredSignature = `${signature.slice(0, index)}${signature[index] === "A" ? "B" : "A"}${signature.slice(index + 1)}`;
    const altered = `${header}.${payload}.${alteredSignature}`;
    const errorText = thrownText(() => decodeAndVerifyNatsJwt(altered));
    expect(errorText !== undefined).toBe(true);
    expect((errorText ?? "").includes(altered)).toBe(false);
    expectNoSeed(errorText ?? "", allSeeds);
  });

  it.each(["unknown-type", "wrong-version", "wrong-header", "account-subject", "operator-issuer", "missing-iat", "expired"] as const)(
    "refuses malformed signed claim shape %s without echoing the token",
    async (kind) => {
      const { encodeSystemUserJwt, decodeAndVerifyNatsJwt } = await loadJwt();
      const { jwt: valid, now } = currentSystemUserJwt(encodeSystemUserJwt);
      const source = decode(valid).claims;
      const claims: Record<string, unknown> = { ...source, nats: { ...source.nats } };
      const nats = claims.nats as Record<string, unknown>;
      let signer = signerOf(systemAccount);
      let header = '{"typ":"JWT","alg":"ed25519-nkey"}';
      if (kind === "unknown-type") nats.type = "not-a-nats-claim-type";
      if (kind === "wrong-version") nats.version = 1;
      if (kind === "wrong-header") header = '{"typ":"JWT","alg":"ed25519"}';
      if (kind === "account-subject") claims.sub = orgAccount.getPublicKey();
      if (kind === "operator-issuer") {
        claims.iss = operatorSigning.getPublicKey();
        signer = signerOf(operatorSigning);
      }
      if (kind === "missing-iat") delete claims.iat;
      if (kind === "expired") claims.exp = now - 1;
      claims.jti = referenceJti(claims);
      const malformed = signClaims(claims, signer, header);
      const errorText = thrownText(() => decodeAndVerifyNatsJwt(malformed));
      expect(errorText !== undefined).toBe(true);
      expect((errorText ?? "").includes(malformed)).toBe(false);
      expectNoSeed(errorText ?? "", allSeeds);
    },
  );
});

describe("#7504 signing-key retirement guard", () => {
  // The caller supplies the complete actual issued-record inventory; this pure guard neither discovers nor persists it.
  it("refuses an absent inventory", async () => {
    const { assertSigningKeyRetirable } = await loadRetirement();
    expect(() =>
      assertSigningKeyRetirable(agentScope.getPublicKey(), undefined as unknown as readonly IssuedCredential[], clock),
    ).toThrow();
  });

  it("refuses malformed inventory records, key classes, roles and expiries", async () => {
    const { assertSigningKeyRetirable } = await loadRetirement();
    const valid = issuedCredential(user.getPublicKey(), "agent", agentScope.getPublicKey(), NOW + DAY);
    const malformed: unknown[] = [
      null,
      {},
      [{ ...valid, publicKey: orgAccount.getPublicKey() }],
      [{ ...valid, signer: user.getPublicKey() }],
      [{ ...valid, role: "sys" }],
      [{ ...valid, expiresAt: 0 }],
      [{ ...valid, expiresAt: NOW + 0.5 }],
      [{ ...valid, expiresAt: Number.NaN }],
    ];
    for (const inventory of malformed) {
      expect(
        () => assertSigningKeyRetirable(agentScope.getPublicKey(), inventory as readonly IssuedCredential[], clock),
        "malformed retirement inventory must be refused",
      ).toThrow();
    }
  });

  it("waits through the longest actual expiry for the selected key and accepts at that expiry", async () => {
    const { assertSigningKeyRetirable } = await loadRetirement();
    const credentials = [
      issuedCredential(user.getPublicKey(), "agent", agentScope.getPublicKey(), NOW + DAY),
      issuedCredential(otherUser.getPublicKey(), "agent", agentScope.getPublicKey(), NOW + 7 * DAY),
    ];
    expect(() => assertSigningKeyRetirable(agentScope.getPublicKey(), credentials, clockAt(NOW + DAY))).toThrow();
    assertSigningKeyRetirable(agentScope.getPublicKey(), credentials, clockAt(NOW + 7 * DAY));
  });

  it("uses an issued principal's actual 90-day expiry, not a global 7-day bound", async () => {
    const { assertSigningKeyRetirable } = await loadRetirement();
    const credentials = [issuedCredential(user.getPublicKey(), "principal", principalScope.getPublicKey(), NOW + NINETY_DAYS)];
    expect(() => assertSigningKeyRetirable(principalScope.getPublicKey(), credentials, clockAt(NOW + 7 * DAY))).toThrow();
    assertSigningKeyRetirable(principalScope.getPublicKey(), credentials, clockAt(NOW + NINETY_DAYS));
  });

  it("does not let another signing key's later expiry block this key", async () => {
    const { assertSigningKeyRetirable } = await loadRetirement();
    const credentials = [
      issuedCredential(user.getPublicKey(), "agent", agentScope.getPublicKey(), NOW + DAY),
      issuedCredential(otherUser.getPublicKey(), "principal", principalScope.getPublicKey(), NOW + NINETY_DAYS),
    ];
    assertSigningKeyRetirable(agentScope.getPublicKey(), credentials, clockAt(NOW + 2 * DAY));
  });

  it("does not bypass an unexpired issued credential marked reissued or revoked", async () => {
    const { assertSigningKeyRetirable } = await loadRetirement();
    const credential = {
      ...issuedCredential(user.getPublicKey(), "principal", principalScope.getPublicKey(), NOW + NINETY_DAYS),
      reissued: true,
      revoked: true,
    };
    expect(() => assertSigningKeyRetirable(principalScope.getPublicKey(), [credential], clockAt(NOW + 7 * DAY))).toThrow();
  });
});

describe("#7504 user .creds", () => {
  it("lays out the Go creds_utils blocks and authenticates a nonce through nats-core credsAuthenticator", async () => {
    const { encodeUserJwt, formatUserCreds } = await loadJwt();
    const jwt = encodeUserJwt(agentUserInput(), signerOf(agentScope), clock);
    const seed = user.getSeed();
    const creds = formatUserCreds(jwt, seed);
    expect(creds instanceof Uint8Array).toBe(true);
    const text = new TextDecoder().decode(creds);
    const seedText = new TextDecoder().decode(seed);
    expect(text.startsWith(`-----BEGIN NATS USER JWT-----\n${jwt}\n------END NATS USER JWT------\n\n`)).toBe(true);
    expect(text.includes(`-----BEGIN USER NKEY SEED-----\n${seedText}\n------END USER NKEY SEED------\n`)).toBe(true);
    expect(text.split(seedText).length - 1).toBe(1);
    expect(text.split(jwt).length - 1).toBe(1);

    const nonce = `fixture-nonce-${NOW}`;
    const auth = credsAuthenticator(creds)(nonce) as { jwt?: string; nkey?: string; sig?: string };
    expect(auth.jwt === jwt).toBe(true);
    expect(auth.nkey).toBe(user.getPublicKey());
    const sig = nkeys.decode(auth.sig ?? "");
    expect(sig.length).toBe(64);
    expect(nkeys.fromPublic(user.getPublicKey()).verify(utf8.encode(nonce), sig)).toBe(true);
    expect(nkeys.fromPublic(otherUser.getPublicKey()).verify(utf8.encode(nonce), sig)).toBe(false);
  });

  it("refuses a multi-line JWT, a non-user seed and a seed that does not own the JWT subject, without echoing the seed", async () => {
    const { encodeUserJwt, formatUserCreds } = await loadJwt();
    const jwt = encodeUserJwt(agentUserInput(), signerOf(agentScope), clock);
    const cases: Array<[string, () => unknown, Uint8Array]> = [
      ["multi-line jwt", () => formatUserCreds(`${jwt}\n`, user.getSeed()), user.getSeed()],
      ["account seed", () => formatUserCreds(jwt, orgAccount.getSeed()), orgAccount.getSeed()],
      ["other user's seed", () => formatUserCreds(jwt, otherUser.getSeed()), otherUser.getSeed()],
      ["not a jwt", () => formatUserCreds("not-a-jwt", user.getSeed()), user.getSeed()],
    ];
    for (const [label, run, seed] of cases) {
      const text = thrownText(run);
      expect(text !== undefined, `${label} must throw`).toBe(true);
      expect((text ?? "").includes(new TextDecoder().decode(seed)), `${label} error must not echo the seed`).toBe(false);
    }
  });
});
