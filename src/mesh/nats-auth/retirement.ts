import { nkeys } from "@nats-io/transport-node";
import type { EncodeClock } from "./jwt.js";

/**
 * smarty-dev#7504 slice 1 (design 2.1, 10.4; lead decision on resume): per-signing-key retirement guard.
 *
 * Pure: it reads only its arguments, mutates nothing and persists nothing. ASSUMPTION, owned by the caller: `credentials`
 * is the COMPLETE actual issued-record inventory (at least every record signed by `signingKey`). The guard cannot
 * discover or prove completeness; a missing or malformed inventory is unknown and refused, never replaced by an
 * inferred global bound (no 7-day default).
 *
 * The key may be removed from the account JWT only at or after the longest actual `expiresAt` among records it
 * signed; another key's records never extend it. There is no early path: fields such as `reissued` or `revoked` are
 * not consulted. Reissue, revoke, a durable inventory and `rotate` are rest-of-phase-1 and are not stubbed here.
 *
 * Decision C: the hosted proof is T15 (owner fabric-store); a server cannot check the caller's inventory.
 */

export type IssuedCredentialRole = "agent" | "principal" | "maintainer" | "break-glass";

/** Public metadata recorded for every issued org user (the minter's user metadata carries these fields). */
export interface IssuedCredential {
  publicKey: string;
  role: IssuedCredentialRole;
  signer: string;
  expiresAt: number;
}

export class NatsRetirementError extends Error {
  readonly field: string;
  /** Present when the inventory is valid but a selected-key credential is still unexpired: the earliest retire time. */
  readonly retirableAt?: number;
  constructor(field: string, retirableAt?: number) {
    super(retirableAt === undefined ? `invalid ${field}` : `signing key is not retirable before ${retirableAt}`);
    this.name = "NatsRetirementError";
    this.field = field;
    if (retirableAt !== undefined) this.retirableAt = retirableAt;
  }
}

const ROLES: readonly string[] = ["agent", "principal", "maintainer", "break-glass"];
const PUBLIC_KEY = /^[AU][A-Z2-7]{55}$/;

function fail(field: string): never {
  throw new NatsRetirementError(field);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === null || Object.getPrototypeOf(proto) === null;
}

function publicKey(value: unknown, prefix: "A" | "U", field: string): string {
  if (typeof value !== "string" || !PUBLIC_KEY.test(value) || value[0] !== prefix) fail(field);
  try {
    nkeys.fromPublic(value);
  } catch {
    return fail(field);
  }
  return value;
}

/**
 * Throws unless `signingKey` (an account signing key, A) can be retired now: every credential it signed in the
 * complete caller-supplied inventory has reached its `expiresAt`. Accepts at the exact expiry.
 */
export function assertSigningKeyRetirable(signingKey: string, credentials: readonly IssuedCredential[], clock: EncodeClock): void {
  const key = publicKey(signingKey, "A", "signingKey");
  if (!Array.isArray(credentials)) fail("credentials");
  const seen = new Set<string>();
  let latest = 0;
  for (const entry of credentials as readonly unknown[]) {
    if (!isPlainObject(entry)) fail("credentials");
    const user = publicKey(entry.publicKey, "U", "credentials.publicKey");
    const signer = publicKey(entry.signer, "A", "credentials.signer");
    if (typeof entry.role !== "string" || !ROLES.includes(entry.role)) fail("credentials.role");
    const expiresAt = entry.expiresAt;
    if (typeof expiresAt !== "number" || !Number.isSafeInteger(expiresAt) || expiresAt <= 0) fail("credentials.expiresAt");
    // One record per issued user key; a duplicate makes the inventory ambiguous, so it is unknown.
    if (seen.has(user)) fail("credentials (duplicate)");
    seen.add(user);
    if (signer === key && expiresAt > latest) latest = expiresAt;
  }
  let now: unknown;
  try {
    now = clock.nowSeconds();
  } catch {
    return fail("clock");
  }
  if (typeof now !== "number" || !Number.isSafeInteger(now) || now <= 0) fail("clock");
  if (now < latest) throw new NatsRetirementError("signingKey", latest);
}
