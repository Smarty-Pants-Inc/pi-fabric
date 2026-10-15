import { createHash } from "node:crypto";

export const FABRIC_IDEMPOTENCY_KEY_REUSED = "FABRIC_IDEMPOTENCY_KEY_REUSED";

/** A caller key cannot acknowledge a different semantic body. Never include either body. */
export class FabricIdempotencyKeyReusedError extends Error {
  readonly code = FABRIC_IDEMPOTENCY_KEY_REUSED;
  readonly retryable = false;
  readonly idempotencyKey?: string;

  constructor(idempotencyKey?: string) {
    super("Fabric idempotency key reused with different input");
    this.name = "FabricIdempotencyKeyReusedError";
    if (idempotencyKey !== undefined) this.idempotencyKey = idempotencyKey.slice(0, 200);
  }
}

const BODY_FIELDS = [
  "targetId", "operation", "message", "data", "wakeCause", "triggerTurn", "priority",
  "binding", "bindingProvenance",
] as const;

/** Input is already wire JSON: serialize object keys lexically, including numeric-looking keys. */
const canonicalWireJson = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonicalWireJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object).sort().map(key => `${JSON.stringify(key)}:${canonicalWireJson(object[key])}`).join(",")}}`;
  }
  return JSON.stringify(value)!;
};

/**
 * Bind only semantic input and the captured principal. Owner epoch/route are authenticated
 * separately; per-attempt IDs, timestamps, reply addresses and bridge stamps are not body.
 * Normalize through JSON first to retain wire semantics (toJSON, omitted undefined object
 * fields, null array holes/non-finite numbers). Cycles and BigInt fail just as wire encoding does.
 */
export const controlBodyFingerprint = (command: unknown, principal?: unknown): string => {
  const input = command !== null && typeof command === "object" ? command as Record<string, unknown> : {};
  const body: Record<string, unknown> = Object.create(null);
  for (const field of BODY_FIELDS) body[field] = input[field];
  body.principal = principal;
  const wire: unknown = JSON.parse(JSON.stringify(body));
  return createHash("sha256").update(canonicalWireJson(wire), "utf8").digest("hex");
};
