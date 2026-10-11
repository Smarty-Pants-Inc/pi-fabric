/**
 * smarty-dev#8742: tell a provider error a retry cannot fix from one that backoff can.
 *
 * Pi exposes only the failed assistant message on an error turn: `stopReason`, the formatted
 * `errorMessage`, and `diagnostics`. Its `after_provider_response` hook fires only for a
 * successful response, and the SDK error object (status, headers, parsed body) is reduced to its
 * message before any extension sees it. The most structured evidence left is therefore the HTTP
 * status and the JSON body that every pi-ai provider formats into `errorMessage`:
 *
 * - Anthropic SDK: `409 {"type":"error","error":{"type":"invalid_request_error","message":"..."}}`
 * - OpenAI SDK via formatProviderError: `<provider> API error (409): {"type":"...",...}` or `409: {...}`
 *
 * ponytail: `X-Should-Retry: false` is invisible here (Pi drops response headers on errors), so a
 * 409 counts as non-retryable only with a non-retryable body type; a bare 409 stays a conflict
 * throttle and keeps today's backoff.
 */

/** How a failed turn's provider error is handled. */
export interface ProviderErrorClass {
  retryable: boolean;
  /** The HTTP status Pi's formatted message carries, when it carries one. */
  status?: number;
  /** One short line for wake status reasons, notices, and alarms. */
  short: string;
  /** The evidence that decided: a Pi diagnostic, the status, the parsed body, or raw text. */
  basis: "diagnostic" | "status" | "body" | "text" | "default";
}

type FailedTurn = { stopReason?: unknown; errorMessage?: unknown; diagnostics?: unknown };

// Anthropic/OpenAI error types that describe the request or the credentials, not load.
const NON_RETRYABLE_TYPES = new Set([
  "invalid_request_error", "authentication_error", "permission_error", "not_found_error",
  "request_too_large", "invalid_api_key", "model_not_found",
]);
// Error types that describe load, whatever 4xx status a gateway wraps them in.
const RETRYABLE_TYPES = new Set(["rate_limit_error", "overloaded_error", "api_error", "timeout_error", "rate_limit_exceeded"]);
const AFFINITY = "compaction_affinity_missing";
const SHORT_MAX = 160;

const oneLine = (text: string): string => {
  const line = text.replace(/\s+/g, " ").trim();
  return line.length > SHORT_MAX ? `${line.slice(0, SHORT_MAX - 1)}…` : line;
};

/** The HTTP status at the head of a pi-ai formatted provider error, if any. */
const statusOf = (message: string): number | undefined => {
  const head = message.slice(0, 200);
  const match = /^\s*(\d{3})(?=[\s:]|$)/.exec(head) ?? /\((\d{3})\):/.exec(head);
  const status = match ? Number(match[1]) : undefined;
  return status !== undefined && status >= 400 && status <= 599 ? status : undefined;
};

interface ErrorBody { type?: string | undefined; code?: string | undefined; message?: string | undefined }

const text = (value: unknown): string | undefined => typeof value === "string" && value ? value : undefined;

/** The provider's JSON error body, parsed; a truncated body falls back to its first "type" field. */
const bodyOf = (message: string): ErrorBody | undefined => {
  const start = message.indexOf("{");
  if (start < 0) return undefined;
  try {
    const parsed = JSON.parse(message.slice(start)) as Record<string, unknown>;
    const inner = (parsed?.error && typeof parsed.error === "object" ? parsed.error : parsed) as Record<string, unknown>;
    const type = text(inner.type) ?? (text(parsed.type) !== "error" ? text(parsed.type) : undefined);
    return { type, code: text(inner.code) ?? text(parsed.code), message: text(inner.message) ?? text(parsed.message) };
  } catch {
    // pi-ai truncates long bodies (MAX_PROVIDER_ERROR_BODY_CHARS), which breaks JSON.parse.
    const type = /"type"\s*:\s*"([a-z_]+_error)"/.exec(message.slice(start))?.[1];
    return type ? { type } : undefined;
  }
};

/**
 * Classify a failed turn's assistant message; undefined when the turn did not fail. Non-retryable:
 * an HTTP 4xx other than 408, 429 and a throttling 409 (a 409 needs a non-retryable body type),
 * or the gateway's `compaction_affinity_missing`. Everything else, including 5xx, 429, network
 * failures and errors with no status, stays retryable.
 */
export const classifyProviderError = (turn: FailedTurn | undefined): ProviderErrorClass | undefined => {
  if (turn?.stopReason !== "error") return undefined;
  const message = typeof turn.errorMessage === "string" ? turn.errorMessage : "";
  const diagnostics = Array.isArray(turn.diagnostics) ? turn.diagnostics as Array<{ type?: unknown }> : [];
  // A provider plan/usage limit (Pi's provider_limit, e.g. smarty_limit) is a 429 window: backoff fits.
  if (diagnostics.some(diagnostic => diagnostic?.type === "provider_limit")) {
    return { retryable: true, short: oneLine(message || "provider limit"), basis: "diagnostic" };
  }
  const status = statusOf(message);
  const body = bodyOf(message);
  const describe = (): string => oneLine([
    status === undefined ? undefined : String(status),
    body?.type ?? body?.code,
  ].filter(Boolean).join(" ") + (body?.message ? `: ${body.message}` : status === undefined ? message : ""))
    || "unknown provider error";
  const verdict = (retryable: boolean, basis: ProviderErrorClass["basis"]): ProviderErrorClass =>
    ({ retryable, ...(status === undefined ? {} : { status }), short: describe(), basis });
  if (body?.code === AFFINITY || body?.type === AFFINITY || body?.message?.includes(AFFINITY)) return verdict(false, "body");
  if (status !== undefined) {
    if (status === 408 || status === 429 || status >= 500) return verdict(true, "status");
    const type = body?.type ?? body?.code;
    if (status === 409) return type !== undefined && NON_RETRYABLE_TYPES.has(type) ? verdict(false, "body") : verdict(true, "status");
    if (type !== undefined && RETRYABLE_TYPES.has(type)) return verdict(true, "body");
    // ponytail: CLIProxyAPI answers 400 "unknown provider for model" while it reloads after a
    // restart (smarty-dev#1856); Pi retries it too. A real unknown model then keeps backing off.
    if (/unknown provider for model/i.test(message)) return verdict(true, "text");
    return verdict(false, type === undefined ? "status" : "body");
  }
  // ponytail: last resort, raw text. A message with no status or body (a gateway that drops both,
  // a provider that rewrites the error) is matched on the gateway's exact error token only; any
  // other unstructured error keeps today's backoff rather than guessing from prose.
  if (message.includes(AFFINITY)) return verdict(false, "text");
  return verdict(true, "default");
};
