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
import { PATH_B_OPERATOR_MACHINE, PATH_B_OPERATOR_MACHINE_PATTERN, roleTemplate, roleTemplateWithPathB, type NatsRole, type PathBEventsOptions } from "./permissions.js";

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

export type NatsOrgRole = NatsRole | "operator";

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
  signingKeys: Record<NatsRole, string> & Partial<Record<"operator", string>>;
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
const ORG_ROLES: readonly NatsRole[] = ["agent", "principal", "maintainer", "break-glass"];

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
 * - Only opted-in accounts add a fifth scoped signing key, role="operator".
 * - Optional events/org repeat mesh.nats policy on account and agent user inputs; only events=true
 *   adds shared-org event grants. The signed account template must match issuance exactly.
 * - Path B agent/operator user inputs repeat the same pathBMachines to verify the exact scope.
 *   Principal, normal maintainer and break-glass issuance need no inventory: signed account
 *   operator-key metadata determines whether the Path B advisory reservation applies.
 * - agent input uses its existing `host` field as the machine; it must be in pathBMachines.
 * - operator input requires job ("compaction" | "migration"), runId and pathBMachines;
 *   optional machine defaults to "fabric-ops" and must match /^fabric-ops(-[a-z0-9]+)?$/.
 *   This is a reserved role namespace, not a host; it is forbidden in pathBMachines and agent credentials.
 *   The operator is metadata/report-only (E19): no ACK, inbox bodies, relay or lifecycle rights.
 *   It binds one bootstrap-owned advisory durable; provisioning uses a separate bootstrap authority.
 * - Normal maintainer remains role="maintainer" with job/runId and NO machine identity.
 *   All four legacy roles retain their non-opted-in output and break-glass scope is unchanged.
 * Neither input contains secrets in argv/environment: existing CLI stdin JSON and artifact rules apply.
 * A fleet-policy JSON can retain its existing `hosts` map and add pathBMachines, then mint Path B
 * agent users only for those host keys. Other hosts keep their existing #7504 account/credentials.
 */
export function pathBMachines(value: unknown): string[] {
  if (!Array.isArray(value) || value.length === 0 || value.length > 64) fail("pathBMachines");
  const machines = value.map((machine) => matching(machine, /^[a-z0-9][a-z0-9-]{0,31}$/, "pathBMachines"));
  if (new Set(machines).size !== machines.length || machines.some((machine) => PATH_B_OPERATOR_MACHINE_PATTERN.test(machine))) fail("pathBMachines");
  return machines;
}

function pathBScopeDescription(value: unknown): string {
  return `path-b/${createHash("sha256").update(JSON.stringify(pathBMachines(value).sort())).digest("hex")}`;
}

function pathBEvents(input: Input): PathBEventsOptions {
  if (input.events !== undefined && typeof input.events !== "boolean") fail("events");
  if (input.org !== undefined && typeof input.org !== "string") fail("org");
  return {
    ...(input.events === undefined ? {} : { events: input.events }),
    ...(input.org === undefined ? {} : { org: input.org }),
  };
}

function mintTemplate(role: NatsOrgRole, input: Input, withPathB: boolean, machines: readonly string[], events: PathBEventsOptions = {}): ReturnType<typeof roleTemplate> {
  const streams = { custodyStream: input.custodyStream as string, livenessStream: input.livenessStream as string };
  if (role === "operator" && !withPathB) fail("role");
  try {
    const org = input.org === undefined ? undefined : orgName(input.org, "org");
    const options = { ...events, ...(org === undefined ? {} : { org }) };
    return withPathB ? roleTemplateWithPathB(role, streams, machines, options) : roleTemplate(role as NatsRole, streams);
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
    const value = object(input, "input", ["name", "operatorJwt", "operatorSigningSeed", "jetstream", "custodyStream", "livenessStream"], ["pathBMachines", "events", "org"]);
    const name = orgName(value.name, "name");
    const operator = verifyOperator(value.operatorJwt);
    const js = object(value.jetstream, "jetstream", ["memStorage", "diskStorage", "streams", "consumers"]);
    const limits = {
      mem_storage: quota(js.memStorage, "jetstream.memStorage"),
      disk_storage: quota(js.diskStorage, "jetstream.diskStorage"),
      streams: quota(js.streams, "jetstream.streams"),
      consumer: quota(js.consumers, "jetstream.consumers"),
    };
    const withPathB = value.pathBMachines !== undefined;
    const machines = withPathB ? pathBMachines(value.pathBMachines) : [];
    const events = pathBEvents(value);
    if (events.events === true && !withPathB) fail("pathBMachines");
    const serviceOrg = value.org === undefined ? undefined : orgName(value.org, "org");
    if (serviceOrg !== undefined && !withPathB) fail("pathBMachines");
    const roles: readonly NatsOrgRole[] = withPathB ? [...ORG_ROLES, "operator"] : ORG_ROLES;
    const byRole = Object.fromEntries(roles.map((role) => [role, mintTemplate(role, value, withPathB, machines, events)])) as Record<NatsOrgRole, ReturnType<typeof roleTemplate>>;
    const at = now(clock);
    const fixed = { nowSeconds: () => at };
    const signer = keyFromSeed(value.operatorSigningSeed, "operatorSigningSeed", "O");
    const account = nkeys.createAccount();
    const scoped = roles.map(() => nkeys.createAccount());
    try {
      // The identity key is offline custody: only a listed operator signing key issues accounts here.
      if (!operator.signingKeys.includes(signer.getPublicKey())) fail("operatorSigningSeed");
      const jwt = encodeAccountJwt(
        {
          name,
          subject: account.getPublicKey(),
          signingKeys: roles.map((role, index) => ({
            key: scoped[index]!.getPublicKey(), role, template: byRole[role],
            ...(value.pathBMachines === undefined || (role !== "agent" && role !== "operator")
              ? {} : { description: pathBScopeDescription(value.pathBMachines) }),
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
          signingKeys: Object.fromEntries(roles.map((role, index) => [role, scoped[index]!.getPublicKey()])) as NatsAccountMetadata["signingKeys"],
        },
        artifacts: [
          jwtArtifact("account.jwt", jwt),
          seedArtifact("account.nk", account),
          ...roles.map((role, index) => seedArtifact(`signing-${role}.nk`, scoped[index]!)),
        ],
      };
    } finally {
      clearAll([signer, account, ...scoped]);
    }
  });
}

const ORG_COMMON = ["operatorJwt", "accountPublicKey", "accountJwt", "signingSeed", "custodyStream", "livenessStream"] as const;
const ORG_OPTIONAL = ["org"] as const;

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
  const keys = Array.isArray(account.nats.signing_keys) ? account.nats.signing_keys : [];
  // The account is signature-verified above. Reservation is an account property, not a
  // caller inventory toggle: principal/maintainer/break-glass never need that inventory.
  const operatorScope = keys.find((key) => isRecord(key) && key.kind === "user_scope" && key.role === "operator");
  const withPathB = isRecord(operatorScope) && typeof operatorScope.description === "string" && /^path-b\/[a-f0-9]{64}$/.test(operatorScope.description);
  if (operatorScope !== undefined && !withPathB) fail("accountJwt");
  const events = pathBEvents(value);
  if (events.events === true && !withPathB) fail("pathBMachines");
  const signer = keyFromSeed(value.signingSeed, "signingSeed", "A");
  try {
    const entry = keys.find((key) => isRecord(key) && key.key === signer.getPublicKey());
    if (!isRecord(entry) || entry.kind !== "user_scope" || entry.role !== role) fail("signingSeed");
    const inventoryRole = role === "agent" || role === "operator";
    let machines: string[] = [];
    if (withPathB && inventoryRole) {
      machines = pathBMachines(value.pathBMachines);
      const description = pathBScopeDescription(machines);
      if (entry.description !== description || operatorScope.description !== description) fail("pathBMachines (scope differs)");
    } else if (value.pathBMachines !== undefined) {
      fail("role");
    }
    const expected = mintTemplate(role, value, withPathB, machines, events);
    const expectedScope = {
      pub: expected.pub,
      sub: expected.sub,
      ...(expected.allow_responses ? { resp: expected.allow_responses } : {}),
      subs: -1,
      data: -1,
      payload: -1,
    };
    if (canonical(entry.template) !== canonical(expectedScope)) {
      fail("custodyStream/livenessStream (scope template differs)");
    }
    return { signer, account: account.sub, org };
  } catch (error) {
    signer.clear();
    throw error;
  }
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

/** Ordinary org user (plus opt-in operator): one fresh U key per call, fixed role/job lifetime. */
export function mintNatsUser(input: unknown, clock: NatsMintClock = systemClock): NatsMintOutput<NatsUserMetadata> {
  return withMint(() => {
    const role = isRecord(input) ? input.role : undefined;
    if (role !== "agent" && role !== "principal" && role !== "maintainer" && role !== "operator") return fail("role");
    const fields = {
      agent: { required: ["host", "lane", "session", "release", "meshes"], optional: ["parent", "pathBMachines", "events", "org"] },
      principal: { required: ["person", "device"], optional: ["pathBMachines"] },
      maintainer: { required: ["job", "runId"], optional: [] },
      operator: { required: ["job", "runId", "pathBMachines"], optional: ["machine", "events", "org"] },
    }[role];
    const value = object(input, "input", ["role", ...ORG_COMMON, ...fields.required], [...fields.optional, ...ORG_OPTIONAL]);
    const label = (key: string) => matching(value[key], LABEL, key);
    let spec: Omit<UserSpec, "name" | "account" | "org"> & { nameIn(org: string): string };
    if (role === "agent") {
      const meshes = Array.isArray(value.meshes) ? value.meshes.map((mesh) => matching(mesh, MESH, "meshes")) : fail("meshes");
      if (meshes.length === 0 || meshes.length > MAX_MESHES || new Set(meshes).size !== meshes.length) fail("meshes");
      const [host, lane, session, release] = ["host", "lane", "session", "release"].map(label) as [string, string, string, string];
      const parent = value.parent === undefined ? [] : [`parent:${label("parent")}`];
      const pathMachines = value.pathBMachines === undefined ? undefined : pathBMachines(value.pathBMachines);
      if (pathMachines && (PATH_B_OPERATOR_MACHINE_PATTERN.test(host) || !pathMachines.includes(host))) fail("host");
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
      const machineTag = role === "operator"
        ? [`machine:${matching(value.machine === undefined ? PATH_B_OPERATOR_MACHINE : value.machine, PATH_B_OPERATOR_MACHINE_PATTERN, "machine")}`] : [];
      spec = { role, nameIn: (org) => `${role}/${org}/${job}/${runId}`, tags: [`role:${role}`, `job:${job}`, `run:${runId}`, ...machineTag], lifetime: LIFETIME[job] };
    }
    const { nameIn, ...issued } = spec;
    const { signer, account, org } = orgSigner(value, role);
    return issueUser({ ...issued, name: nameIn(org), account, org }, signer, clock);
  });
}

/** Break-glass user: only through this explicit call (`mint user --break-glass`), 1 h, audit metadata only. */
export function mintNatsBreakGlassUser(input: unknown, clock: NatsMintClock = systemClock): NatsMintOutput<NatsUserMetadata> {
  return withMint(() => {
    const value = object(input, "input", [...ORG_COMMON, "person", "runId", "approvalRef"], ORG_OPTIONAL);
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
