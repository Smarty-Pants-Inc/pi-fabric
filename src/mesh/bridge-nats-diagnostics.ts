import { createHash } from "node:crypto";

const MAX_FAILURES = 5;
const TOKEN = /^[A-Za-z0-9_-]{1,64}$/;
const token = (value: unknown): string => {
  const text = typeof value === "string" || typeof value === "number" && Number.isFinite(value) ? String(value) : "";
  return TOKEN.test(text) ? text : "unknown";
};
const field = (value: unknown, key: string): unknown => {
  try { return typeof value === "object" && value !== null ? (value as Record<string, unknown>)[key] : undefined; }
  catch { return undefined; }
};

/** Identifier tokens only: never Error.message, cause payloads, records or credentials. */
const errorTokens = (error: unknown): Array<{ name: string; code: string }> => {
  const result: Array<{ name: string; code: string }> = [];
  const queue = [error], seen = new Set<unknown>();
  for (let scanned = 0; queue.length > 0 && scanned < MAX_FAILURES * 2 && result.length < MAX_FAILURES; scanned++) {
    const current = queue.shift();
    if (seen.has(current)) continue;
    seen.add(current);
    const children = field(current, "errors");
    if (Array.isArray(children) && children.length > 0) queue.push(...children.slice(0, MAX_FAILURES));
    else result.push({ name: token(field(current, "name")), code: token(field(current, "code")) });
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

/** Fixed schema and bounded tokens, safe for the port's INFO callback. */
export const natsBridgeErrorSummary = (error: unknown): string => {
  const failures = typeof error === "object" && error !== null ? renewals.get(error) : undefined;
  return JSON.stringify(failures ? { operation: "mirror-renewal", failures }
    : { operation: "port", errors: errorTokens(error) });
};
