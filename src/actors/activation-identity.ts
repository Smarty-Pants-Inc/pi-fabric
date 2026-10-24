export const FABRIC_ACTOR_SELF_ENV = "PI_FABRIC_ACTOR_SELF";

export interface FabricActorActivationIdentity {
  actorId: string;
  name: string;
  rootId: string;
  ownershipToken: string;
  instructionsDigest: string;
  roleStamp?: string;
}

const identityString = (value: unknown, maximumLength = 512): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= maximumLength && value.trim() === value;

/** Keep only the activation identity fields; this is a read-only worker handoff, not actor authority. */
export function normalizeFabricActorActivationIdentity(value: unknown): FabricActorActivationIdentity | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const source = value as Record<string, unknown>;
  if (!identityString(source.actorId) || !identityString(source.name) || !identityString(source.rootId) ||
      !identityString(source.ownershipToken, 15_000) || typeof source.instructionsDigest !== "string" ||
      !/^[a-f0-9]{64}$/.test(source.instructionsDigest)) return undefined;
  if (source.roleStamp !== undefined && !identityString(source.roleStamp)) return undefined;
  return Object.freeze({
    actorId: source.actorId,
    name: source.name,
    rootId: source.rootId,
    ownershipToken: source.ownershipToken,
    instructionsDigest: source.instructionsDigest,
    ...(source.roleStamp !== undefined ? { roleStamp: source.roleStamp } : {}),
  });
}

/** A role stamp is descriptive metadata embedded in rendered actor instructions, never an authority check. */
export function actorRoleStamp(instructions: string, explicit?: unknown): string | undefined {
  if (identityString(explicit)) return explicit;
  return instructions.match(/\(role\s+[^\s()]{1,200}\s+[a-f0-9]{12,40}\)/i)?.[0];
}

export function actorActivationIdentityFromEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): FabricActorActivationIdentity | undefined {
  const serialized = environment[FABRIC_ACTOR_SELF_ENV];
  if (typeof serialized !== "string" || serialized.length > 16_384) return undefined;
  try {
    return normalizeFabricActorActivationIdentity(JSON.parse(serialized));
  } catch {
    return undefined;
  }
}
