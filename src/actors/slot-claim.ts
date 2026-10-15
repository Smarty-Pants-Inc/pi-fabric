import type { FabricActorSlotClaim } from "./types.js";

// First-use provider validation, not part of the shared eager actor-types graph.
const invalidActorSlotClaim = (message: string): never => {
  const error = new Error(message);
  error.name = "ActorSlotClaimRefusedError";
  throw Object.assign(error, { code: "FABRIC_ACTOR_SLOT_CLAIM_REFUSED", reason: "invalid" });
};

export function normalizeActorSlotClaim(value: unknown): FabricActorSlotClaim | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null || Array.isArray(value) ||
      Object.keys(value).some(key => key !== "key" && key !== "ifVersion")) {
    return invalidActorSlotClaim("Invalid actor slotClaim: use { key, ifVersion }");
  }
  const { key, ifVersion } = value as Record<string, unknown>;
  if (typeof key !== "string" || !key.length || typeof ifVersion !== "number" ||
      !Number.isSafeInteger(ifVersion) || ifVersion <= 0) {
    return invalidActorSlotClaim("Invalid actor slotClaim: key must be nonempty and ifVersion a positive safe integer");
  }
  return { key, ifVersion };
}
