import { createHash } from "node:crypto";
import { nkeys } from "@nats-io/transport-node";
import {
  decodeAndVerifyNatsJwt,
  encodeAccountJwt,
  encodeOperatorJwt,
  encodeSystemUserJwt,
  encodeUserJwt,
  formatUserCreds,
} from "./jwt.js";
import { PATH_B_OPERATOR_MACHINE, roleTemplate, roleTemplateWithPathB } from "./permissions.js";

/**
 * smarty-dev#7504 slice 1 (design Amendment B): high-level NATS minter for the `pi-fabric nats` CLI.
 *
 * Pure: no files, environment, argv or credential store. Inputs are parsed JSON (`unknown`), validated in full before
 * any key is generated. Outputs keep public `metadata` apart from `artifacts` (file bytes: seeds, creds, public JWTs),
 * so a caller can print metadata without ever serializing a secret. Errors carry a fixed field name only, never a
 * value, an input key, a JWT or a seed.
 *
 * Not here (rest of phase 1): rotate, durable issuance inventory, reissue, revoke, `init --local`. Break-glass
 * approval and the stop-first engine runbook are the caller's duty; the approval reference is audit metadata only.
 */

export type NatsOrgRole = "agent" | "principal" | "maintainer" | "break-glass";

export interface NatsMintClock {
  nowSeconds(): number;
}

export interface NatsMintArtifact {
  readonly name: string;
  readonly bytes: Uint8Array;
}

export interface NatsMintOutput<M> {
  readonly metadata: M;
  readonly artifacts: readonly NatsMintArtifact[];
}

export interface NatsOperatorMetadata {
  kind: "operator";
  operator: { name: string; publicKey: string };
  operatorSigningKey: { publicKey: string };
  systemAccount: { name: "SYS"; publicKey: string };
}

export interface NatsAccountMetadata {
  kind: "account";
  account: { name: string; publicKey: string };
  issuer: string;
  signingKeys: Record<NatsOrgRole, string>;
}

export interface NatsUserMetadata {
  kind: "user";
  role: NatsOrgRole | "sys";
  name: string;
  publicKey: string;
  account: string;
  org?: string;
  /** The verified A key that signed the user JWT. */
  signer: string;
  /** Compatibility alias of `signer`. */
  issuer: string;
  issuedAt: number;
  expiresAt: number;
  jwtSha256: string;
  audit?: { person: string; runId: string; approvalRef: string };
}

export class NatsMintError extends Error {
  readonly field: string;
  constructor(field: string) {
    super(`invalid ${field}`);
    this.name = "NatsMintError";
    this.field = field;
  }
}

const HOUR = 3_600;
const DAY = 24 * HOUR;
const LIFETIME = {
  agent: 7 * DAY,
  principal: 90 * DAY,
  compaction: HOUR,
  migration: 2 * HOUR,
  "break-glass": HOUR,
  sys: HOUR,
} as const;
const ORG_ROLES: readonly NatsOrgRole[] = ["agent", "principal", "maintainer", "break-glass"];

// JS regexes without the m flag anchor at the whole input: a final LF/CR/U+2028/U+2029 never matches.
const ORG_NAME = /^[a-z0-9][a-z0-9-]{0,62}$/;
const LABEL = /^[a-z0-9][a-z0-9._-]{0,63}$/;
const MESH = /^[0-9a-f]{32}$/;
const APPROVAL_REF = /^[A-Za-z0-9][A-Za-z0-9._:/#?=&+@-]{0,255}$/;
const SECRET_SHAPE = /S[A-Z][A-Z2-7]{40,}|eyJ/;
const SEED = /^S[A-Z][A-Z2-7]{40,120}$/;
const MAX_MESHES = 64;

const systemClock: NatsMintClock = { nowSeconds: () => Math.floor(Date.now() / 1000) };
const utf8 = new TextEncoder();
type KeyPair = ReturnType<typeof nkeys.createUser>;
type Claims = { iss: string; sub: string; name?: string; iat: number; exp?: number; nats: Record<string, unknown> };
type Input = Record<string, unknown>;

function fail(field: string): never {
  throw new NatsMintError(field);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** A plain JSON object with exactly the allowed keys; an unknown key is refused without naming it. */
function object(value: unknown, field: string, required: readonly string[], optional: readonly string[] = []): Input {
  if (!isRecord(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail(field);
  const input = value as Input;
  const allowed = new Set([...required, ...optional]);
  for (const key of Object.keys(input)) if (!allowed.has(key)) fail(`${field} (unsupported field)`);
  for (const key of required) if (input[key] === undefined) fail(key);
  return input;
}

function matching(value: unknown, pattern: RegExp, field: string): string {
  return typeof value === "string" && pattern.test(value) ? value : fail(field);
}

function orgName(value: unknown, field: string): string {
  const name = matching(value, ORG_NAME, field);
  return name === "sys" ? fail(field) : name;
}

function quota(value: unknown, field: string): number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : fail(field);
}

function now(clock: NatsMintClock): number {
  const value = clock.nowSeconds();
  return Number.isSafeInteger(value) && value > 0 ? value : fail("clock");
}

/** Seed string to a key pair of the given public-key class; the caller clears it. Never echoes the seed. */
function keyFromSeed(value: unknown, field: string, prefix: "O" | "A"): KeyPair {
  if (typeof value !== "string" || !SEED.test(value)) fail(field);
  let pair: KeyPair;
  try {
    pair = nkeys.fromSeed(utf8.encode(value));
  } catch {
    return fail(field);
  }
  if (pair.getPublicKey()[0] !== prefix) {
    pair.clear();
    fail(field);
  }
  return pair;
}

const signerOf = (pair: KeyPair) => ({ publicKey: pair.getPublicKey(), sign: (bytes: Uint8Array) => pair.sign(bytes) });
const seedArtifact = (name: string, pair: KeyPair): NatsMintArtifact => ({ name, bytes: Uint8Array.from(pair.getSeed()) });
const jwtArtifact = (name: string, jwt: string): NatsMintArtifact => ({ name, bytes: utf8.encode(`${jwt}\n`) });
const clearAll = (pairs: readonly (KeyPair | undefined)[]) => {
  for (const pair of pairs) pair?.clear();
};

/** Verified public claims of the given type. Core may return `{header, claims}` or the claims; both are accepted. */
function verified(jwt: unknown, field: string, type: "operator" | "account" | "user"): Claims {
  if (typeof jwt !== "string") fail(field);
  let decoded: unknown;
  try {
    decoded = decodeAndVerifyNatsJwt(jwt);
  } catch {
    return fail(field);
  }
  const claims = isRecord(decoded) && isRecord(decoded.claims) ? decoded.claims : decoded;
  if (!isRecord(claims) || !isRecord(claims.nats) || claims.nats.type !== type) fail(field);
  const { iss, sub, iat } = claims as Record<string, unknown>;
  if (typeof iss !== "string" || typeof sub !== "string" || !Number.isSafeInteger(iat)) fail(field);
  return claims as Claims;
}

interface VerifiedOperator {
  identity: string;
  signingKeys: string[];
  systemAccount: string;
}

function verifyOperator(jwt: unknown): VerifiedOperator {
  const claims = verified(jwt, "operatorJwt", "operator");
  const { signing_keys: signingKeys, system_account: systemAccount } = claims.nats;
  const keys = Array.isArray(signingKeys) && signingKeys.every((key) => typeof key === "string") ? (signingKeys as string[]) : [];
  if (claims.iss !== claims.sub || claims.sub[0] !== "O" || typeof systemAccount !== "string") fail("operatorJwt");
  return { identity: claims.sub, signingKeys: keys, systemAccount: systemAccount as string };
}

/** Account claims verified and issued by this operator (identity or a listed signing key). */
function verifyAccount(jwt: unknown, field: string, operator: VerifiedOperator): Claims {
  const claims = verified(jwt, field, "account");
  if (claims.sub[0] !== "A" || ![operator.identity, ...operator.signingKeys].includes(claims.iss)) fail(field);
  return claims;
}

/** Order-insensitive canonical form: object keys sorted, string arrays sorted. */
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).sort().join(",")}]`;
  if (isRecord(value)) return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}

/**
 * Path B mint schema (opt-in; all original #7504 fields stay unchanged):
 * - account input adds pathBMachines: a nonempty, unique array of 1..64 machine literals
 *   /^[a-z0-9][a-z0-9-]{0,31}$/; canary value ["ryzen2", "ryzen4"].
 * - every user input for that account repeats the same pathBMachines to verify the exact scope.
 * - agent input uses its existing `host` field as the machine; it must be in pathBMachines.
 * - maintainer input adds `machine`, which MUST be the reserved operator literal "fabric-ops".
 *   This is a role principal, not a host; it is forbidden in pathBMachines and agent credentials.
 *   Operator ACK publication enumerates the destination machines; there is no wildcard destination.
 *   Operator credentials are read/relay only; stream provisioning uses a separate bootstrap authority.
 * Neither input contains secrets in argv/environment: existing CLI stdin JSON and artifact rules apply.
 * A fleet-policy JSON can retain its existing `hosts` map and add pathBMachines, then mint Path B
 * agent users only for those host keys. Other hosts keep their existing #7504 account/credentials.
 */
function pathBMachines(value: unknown): string[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > 64) fail("pathBMachines");
  const machines = value.map((machine) => matching(machine, /^[a-z0-9][a-z0-9-]{0,31}$/, "pathBMachines"));
  if (new Set(machines).size !== machines.length || machines.includes(PATH_B_OPERATOR_MACHINE)) fail("pathBMachines");
  return machines;
}

function pathBScopeDescription(value: unknown): string {
  return `path-b/${createHash("sha256").update(JSON.stringify(pathBMachines(value).sort())).digest("hex")}`;
}

function templates(input: Input): Record<NatsOrgRole, ReturnType<typeof roleTemplate>> {
  const streams = { custodyStream: input.custodyStream as string, livenessStream: input.livenessStream as string };
  const machines = input.pathBMachines === undefined ? [] : pathBMachines(input.pathBMachines);
  try {
    const withPathB = input.pathBMachines !== undefined;
    return Object.fromEntries(ORG_ROLES.map((role) => [role, withPathB ? roleTemplateWithPathB(role, streams, machines) : roleTemplate(role, streams)])) as Record<NatsOrgRole, ReturnType<typeof roleTemplate>>;
  } catch {
    return fail("custodyStream/livenessStream");
  }
}

function withMint<T>(run: () => T): T {
  try {
    return run();
  } catch (error) {
    if (error instanceof NatsMintError) throw error;
    // Core encoder/keys errors are not echoed: they may describe input values.
    throw new NatsMintError("input");
  }
}

export function createNatsOperator(input: unknown, clock: NatsMintClock = systemClock): NatsMintOutput<NatsOperatorMetadata> {
  return withMint(() => {
    const name = orgName(object(input, "input", ["operatorName"]).operatorName, "operatorName");
    const at = now(clock);
    const fixed = { nowSeconds: () => at };
    const identity = nkeys.createOperator();
    const signing = nkeys.createOperator();
    const sys = nkeys.createAccount();
    try {
      const operatorJwt = encodeOperatorJwt(
        { name, signingKeys: [signing.getPublicKey()], systemAccount: sys.getPublicKey() },
        signerOf(identity),
        fixed,
      );
      // SYS: JetStream disabled; the core constructor fills the Go connection defaults. No SYS user is minted here.
      const sysJwt = encodeAccountJwt(
        { name: "SYS", subject: sys.getPublicKey(), signingKeys: [], limits: { mem_storage: 0, disk_storage: 0 } },
        signerOf(signing),
        fixed,
      );
      return {
        metadata: {
          kind: "operator",
          operator: { name, publicKey: identity.getPublicKey() },
          operatorSigningKey: { publicKey: signing.getPublicKey() },
          systemAccount: { name: "SYS", publicKey: sys.getPublicKey() },
        },
        artifacts: [
          jwtArtifact("operator.jwt", operatorJwt),
          seedArtifact("operator.nk", identity),
          seedArtifact("operator-signing.nk", signing),
          jwtArtifact("sys-account.jwt", sysJwt),
          seedArtifact("sys-account.nk", sys),
        ],
      };
    } finally {
      clearAll([identity, signing, sys]);
    }
  });
}

export function createNatsAccount(input: unknown, clock: NatsMintClock = systemClock): NatsMintOutput<NatsAccountMetadata> {
  return withMint(() => {
    const value = object(input, "input", ["name", "operatorJwt", "operatorSigningSeed", "jetstream", "custodyStream", "livenessStream"], ["pathBMachines"]);
    const name = orgName(value.name, "name");
    const operator = verifyOperator(value.operatorJwt);
    const js = object(value.jetstream, "jetstream", ["memStorage", "diskStorage", "streams", "consumers"]);
    const limits = {
      mem_storage: quota(js.memStorage, "jetstream.memStorage"),
      disk_storage: quota(js.diskStorage, "jetstream.diskStorage"),
      streams: quota(js.streams, "jetstream.streams"),
      consumer: quota(js.consumers, "jetstream.consumers"),
    };
    const byRole = templates(value);
    const at = now(clock);
    const fixed = { nowSeconds: () => at };
    const signer = keyFromSeed(value.operatorSigningSeed, "operatorSigningSeed", "O");
    const account = nkeys.createAccount();
    const scoped = ORG_ROLES.map(() => nkeys.createAccount());
    try {
      // The identity key is offline custody: only a listed operator signing key issues accounts here.
      if (!operator.signingKeys.includes(signer.getPublicKey())) fail("operatorSigningSeed");
      const jwt = encodeAccountJwt(
        {
          name,
          subject: account.getPublicKey(),
          signingKeys: ORG_ROLES.map((role, index) => ({
            key: scoped[index]!.getPublicKey(), role, template: byRole[role],
            ...(value.pathBMachines === undefined ? {} : { description: pathBScopeDescription(value.pathBMachines) }),
          })),
          limits,
        },
        signerOf(signer),
        fixed,
      );
      return {
        metadata: {
          kind: "account",
          account: { name, publicKey: account.getPublicKey() },
          issuer: signer.getPublicKey(),
          signingKeys: Object.fromEntries(ORG_ROLES.map((role, index) => [role, scoped[index]!.getPublicKey()])) as Record<NatsOrgRole, string>,
        },
        artifacts: [
          jwtArtifact("account.jwt", jwt),
          seedArtifact("account.nk", account),
          ...ORG_ROLES.map((role, index) => seedArtifact(`signing-${role}.nk`, scoped[index]!)),
        ],
      };
    } finally {
      clearAll([signer, account, ...scoped]);
    }
  });
}

const ORG_COMMON = ["operatorJwt", "accountPublicKey", "accountJwt", "signingSeed", "custodyStream", "livenessStream"] as const;

interface UserSpec {
  role: NatsOrgRole | "sys";
  name: string;
  tags: string[];
  lifetime: number;
  account: string;
  org?: string;
  audit?: NatsUserMetadata["audit"];
}

/** Verifies the operator -> org account -> role key chain and the unchanged scope template; returns the signer. */
function orgSigner(value: Input, role: NatsOrgRole): { signer: KeyPair; account: string; org: string } {
  const operator = verifyOperator(value.operatorJwt);
  const account = verifyAccount(value.accountJwt, "accountJwt", operator);
  if (account.sub === operator.systemAccount) fail("accountJwt");
  if (value.accountPublicKey !== account.sub) fail("accountPublicKey");
  const org = orgName(account.name, "accountJwt");
  const expected = templates(value)[role];
  const keys = Array.isArray(account.nats.signing_keys) ? account.nats.signing_keys : [];
  const signer = keyFromSeed(value.signingSeed, "signingSeed", "A");
  const entry = keys.find((key) => isRecord(key) && key.key === signer.getPublicKey());
  if (!isRecord(entry) || entry.kind !== "user_scope" || entry.role !== role) {
    signer.clear();
    fail("signingSeed");
  }
  if (typeof entry.description === "string" && entry.description.startsWith("path-b/") && value.pathBMachines === undefined) {
    signer.clear();
    fail("pathBMachines");
  }
  if (value.pathBMachines !== undefined && role !== "agent" && role !== "maintainer") {
    signer.clear();
    fail("role");
  }
  if (value.pathBMachines !== undefined && entry.description !== pathBScopeDescription(value.pathBMachines)) {
    signer.clear();
    fail("pathBMachines (scope differs)");
  }
  if (canonical((entry as Input).template) !== canonical({ ...expected, subs: -1, data: -1, payload: -1 })) {
    signer.clear();
    fail("custodyStream/livenessStream (scope template differs)");
  }
  return { signer, account: account.sub, org };
}

function issueUser(spec: UserSpec, signer: KeyPair, clock: NatsMintClock): NatsMintOutput<NatsUserMetadata> {
  const user = nkeys.createUser();
  try {
    const at = now(clock);
    const fixed = { nowSeconds: () => at };
    const subject = user.getPublicKey();
    const expiresAt = at + spec.lifetime;
    const jwt =
      spec.role === "sys"
        ? encodeSystemUserJwt({ name: spec.name, subject, expiresAt }, signerOf(signer), fixed)
        : encodeUserJwt({ name: spec.name, subject, issuerAccount: spec.account, tags: spec.tags, expiresAt }, signerOf(signer), fixed);
    const claims = verified(jwt, "user", "user");
    if (claims.sub !== subject || claims.iss !== signer.getPublicKey() || claims.exp !== expiresAt) fail("user");
    const seed = Uint8Array.from(user.getSeed());
    let creds: Uint8Array;
    try {
      creds = formatUserCreds(jwt, seed);
    } finally {
      seed.fill(0);
    }
    const metadata: NatsUserMetadata = {
      kind: "user",
      role: spec.role,
      name: spec.name,
      publicKey: subject,
      account: spec.account,
      ...(spec.org === undefined ? {} : { org: spec.org }),
      signer: claims.iss,
      issuer: claims.iss,
      issuedAt: claims.iat,
      expiresAt,
      jwtSha256: createHash("sha256").update(jwt).digest("hex"),
      ...(spec.audit === undefined ? {} : { audit: spec.audit }),
    };
    return { metadata, artifacts: [{ name: "user.creds", bytes: creds }] };
  } finally {
    clearAll([user, signer]);
  }
}

/** Ordinary org user (agent, principal, maintainer): one fresh U key per call, fixed role lifetime. */
export function mintNatsUser(input: unknown, clock: NatsMintClock = systemClock): NatsMintOutput<NatsUserMetadata> {
  return withMint(() => {
    const role = isRecord(input) ? input.role : undefined;
    if (role !== "agent" && role !== "principal" && role !== "maintainer") return fail("role");
    const fields = {
      agent: { required: ["host", "lane", "session", "release", "meshes"], optional: ["parent", "pathBMachines"] },
      principal: { required: ["person", "device"], optional: ["pathBMachines"] },
      maintainer: { required: ["job", "runId"], optional: ["machine", "pathBMachines"] },
    }[role];
    const value = object(input, "input", ["role", ...ORG_COMMON, ...fields.required], fields.optional);
    const label = (key: string) => matching(value[key], LABEL, key);
    let spec: Omit<UserSpec, "name" | "account" | "org"> & { nameIn(org: string): string };
    if (role === "agent") {
      const meshes = Array.isArray(value.meshes) ? value.meshes.map((mesh) => matching(mesh, MESH, "meshes")) : fail("meshes");
      if (meshes.length === 0 || meshes.length > MAX_MESHES || new Set(meshes).size !== meshes.length) fail("meshes");
      const [host, lane, session, release] = ["host", "lane", "session", "release"].map(label) as [string, string, string, string];
      const parent = value.parent === undefined ? [] : [`parent:${label("parent")}`];
      const pathMachines = value.pathBMachines === undefined ? undefined : pathBMachines(value.pathBMachines);
      if (pathMachines && (host === PATH_B_OPERATOR_MACHINE || !pathMachines.includes(host))) fail("host");
      const machineTag = pathMachines === undefined ? [] : [`machine:${host}`];
      const tags = [`host:${host}`, `lane:${lane}`, `session:${session}`, `release:${release}`, "role:agent", ...machineTag, ...meshes.map((mesh) => `mesh:${mesh}`), ...parent];
      spec = { role, nameIn: () => `agent/${host}/${lane}/${session}`, tags, lifetime: LIFETIME.agent };
    } else if (role === "principal") {
      // Deliberately unbound: a principal reads the whole org (design 1.4), so it carries no mesh tag.
      const [person, device] = [label("person"), label("device")];
      spec = { role, nameIn: () => `principal/${person}/${device}`, tags: ["role:principal", `person:${person}`, `device:${device}`], lifetime: LIFETIME.principal };
    } else {
      const job = value.job === "compaction" || value.job === "migration" ? value.job : fail("job");
      const runId = label("runId");
      const machine = value.machine === undefined ? undefined : matching(value.machine, /^[a-z0-9][a-z0-9-]{0,31}$/, "machine");
      if (value.pathBMachines !== undefined && machine !== PATH_B_OPERATOR_MACHINE) fail("machine");
      if (value.pathBMachines === undefined && machine !== undefined) fail("machine");
      const machineTag = machine === undefined ? [] : [`machine:${machine}`];
      spec = { role, nameIn: (org) => `maintainer/${org}/${job}/${runId}`, tags: ["role:maintainer", `job:${job}`, `run:${runId}`, ...machineTag], lifetime: LIFETIME[job] };
    }
    const { nameIn, ...issued } = spec;
    const { signer, account, org } = orgSigner(value, role);
    return issueUser({ ...issued, name: nameIn(org), account, org }, signer, clock);
  });
}

/** Break-glass user: only through this explicit call (`mint user --break-glass`), 1 h, audit metadata only. */
export function mintNatsBreakGlassUser(input: unknown, clock: NatsMintClock = systemClock): NatsMintOutput<NatsUserMetadata> {
  return withMint(() => {
    const value = object(input, "input", [...ORG_COMMON, "person", "runId", "approvalRef"]);
    const person = matching(value.person, LABEL, "person");
    const runId = matching(value.runId, LABEL, "runId");
    const approvalRef = matching(value.approvalRef, APPROVAL_REF, "approvalRef");
    if (SECRET_SHAPE.test(approvalRef)) fail("approvalRef");
    const { signer, account, org } = orgSigner(value, "break-glass");
    const tags = ["role:break-glass", `person:${person}`, `run:${runId}`];
    const spec = { role: "break-glass" as const, name: `break-glass/${org}/${person}/${runId}`, tags, lifetime: LIFETIME["break-glass"] };
    return issueUser({ ...spec, account, org, audit: { person, runId, approvalRef } }, signer, clock);
  });
}

/** SYS-only user: signed by the operator-bound SYS identity, never an org key; 1 h; CLAIMS.UPDATE and own inbox. */
export function mintNatsSystemUser(input: unknown, clock: NatsMintClock = systemClock): NatsMintOutput<NatsUserMetadata> {
  return withMint(() => {
    const value = object(input, "input", ["operatorJwt", "systemAccountJwt", "systemAccountSeed", "person", "runId"]);
    const person = matching(value.person, LABEL, "person");
    const runId = matching(value.runId, LABEL, "runId");
    const operator = verifyOperator(value.operatorJwt);
    const sys = verifyAccount(value.systemAccountJwt, "systemAccountJwt", operator);
    if (sys.sub !== operator.systemAccount) fail("systemAccountJwt");
    const signer = keyFromSeed(value.systemAccountSeed, "systemAccountSeed", "A");
    if (signer.getPublicKey() !== sys.sub) {
      signer.clear();
      fail("systemAccountSeed");
    }
    return issueUser({ role: "sys", name: `sys/${person}/${runId}`, tags: [], lifetime: LIFETIME.sys, account: sys.sub }, signer, clock);
  });
}
