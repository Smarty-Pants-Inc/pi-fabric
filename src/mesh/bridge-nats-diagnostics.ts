import { createHash } from "node:crypto";

const MAX_FAILURES = 5;
// Fixed labels only: syntactically valid identifiers can still be credentials or UUIDs.
const labels = (known: readonly string[]): ReadonlyMap<string, string> => new Map(known.map(label => [label, label]));
const ERROR_NAMES = labels([
  "Error", "AggregateError", "TypeError", "RangeError", "SyntaxError", "AbortError", "TimeoutError", "NatsError",
  "NatsAuthorizationError", "AuthorizationError", "PermissionViolationError", "UserAuthenticationExpiredError",
  "ClosedConnectionError", "DrainingConnectionError", "ConnectionError", "ProtocolError", "RequestError", "NoResponders",
  "InvalidSubjectError", "InvalidArgumentError", "InvalidOperationError", "InvalidNameError",
  "JetStreamNotEnabled", "JetStreamError", "JetStreamStatusError", "JetStreamApiError",
  "ConsumerNotFoundError", "StreamNotFoundError", "NatsMirrorRefusedError", "NatsMirrorFencedError",
]);
const ERROR_CODES = labels([
  "ETIMEDOUT", "ECONNRESET", "ECONNREFUSED", "ENOTFOUND", "EAI_AGAIN", "EPIPE", "ENETUNREACH", "EHOSTUNREACH",
  "400", "401", "403", "404", "408", "409", "500", "503", "504",
  "FABRIC_TRANSPORT_UNAVAILABLE", "FABRIC_TRANSPORT_UNCERTAIN", "FABRIC_TRANSPORT_CAPACITY", "FABRIC_TRANSPORT_DENIED",
  "FABRIC_TRANSPORT_INVALID", "FABRIC_TRANSPORT_UNSUPPORTED", "FABRIC_KV_CONFLICT", "FABRIC_DIRECTORY_NOT_READY",
  "FABRIC_NATS_AUTHORIZATION", "FABRIC_BRIDGE_NATS_FENCED", "FABRIC_BRIDGE_NATS_RECORD_INVALID",
  "FABRIC_BRIDGE_NATS_DIRECTORY_NOT_READY", "FABRIC_BRIDGE_NATS_ID_ON_ANOTHER_MACHINE", "FABRIC_BRIDGE_NATS_LEASE_CONFLICT",
  "FABRIC_BRIDGE_NATS_PRESENCE_CONFLICT", "FABRIC_BRIDGE_NATS_STALE_INCARNATION", "FABRIC_BRIDGE_NATS_EXPIRED",
  "FABRIC_BRIDGE_NATS_DUPLICATE",
]);
const label = (value: unknown, known: ReadonlyMap<string, string>): string => {
  const text = typeof value === "string" || typeof value === "number" && Number.isFinite(value) ? String(value) : "";
  return known.get(text) ?? "OTHER";
};
const field = (value: unknown, key: string): unknown => {
  try { return typeof value === "object" && value !== null ? (value as Record<string, unknown>)[key] : undefined; }
  catch { return undefined; }
};

/** Fixed error labels only: never Error.message, cause payloads, records or credentials. */
const errorTokens = (error: unknown): Array<{ name: string; code: string }> => {
  const result: Array<{ name: string; code: string }> = [];
  const queue = [error], seen = new Set<unknown>();
  for (let scanned = 0; queue.length > 0 && scanned < MAX_FAILURES * 2 && result.length < MAX_FAILURES; scanned++) {
    const current = queue.shift();
    if (seen.has(current)) continue;
    seen.add(current);
    const children = field(current, "errors");
    if (Array.isArray(children) && children.length > 0) queue.push(...children.slice(0, MAX_FAILURES));
    else result.push({ name: label(field(current, "name"), ERROR_NAMES), code: label(field(current, "code"), ERROR_CODES) });
  }
  return result;
};

export interface NatsRenewalFailure {
  readonly operation: "lease-renewal" | "lease-drop" | "presence-refresh";
  readonly elapsedMs: number;
  readonly keyHash: string;
  readonly errors: ReturnType<typeof errorTokens>;
}
const renewals = new WeakMap<object, readonly NatsRenewalFailure[]>();

export const natsRenewalFailure = (error: unknown, operation: NatsRenewalFailure["operation"], key: string,
  elapsedMs: number): NatsRenewalFailure => ({ operation,
  elapsedMs: Number.isFinite(elapsedMs) ? Math.max(0, Math.round(elapsedMs)) : 0,
  keyHash: createHash("sha256").update(key).digest("hex").slice(0, 16), errors: errorTokens(error) });

/** Side-channel attribution preserves the exact thrown errors, including singleton codes/identity. */
export const noteNatsRenewalFailures = (error: unknown, failures: readonly NatsRenewalFailure[]): void => {
  if (typeof error === "object" && error !== null) renewals.set(error, failures.slice(0, MAX_FAILURES));
};

/** Fixed schema and labels, safe for the port's INFO callback. */
export const natsBridgeErrorSummary = (error: unknown): string => {
  const failures = typeof error === "object" && error !== null ? renewals.get(error) : undefined;
  return JSON.stringify(failures ? { operation: "mirror-renewal", failures }
    : { operation: "port", errors: errorTokens(error) });
};
