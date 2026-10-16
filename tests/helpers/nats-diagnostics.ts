/** Test-only failure output boundary. Never serialize arbitrary runtime/config/error objects.
 * Keep only diagnostic metadata, scrub strings BEFORE truncation, and bound every field,
 * collection and nesting level. In particular no environment/config/credential bag survives.
 */
const FIELDS = new Set([
  "pid", "closed", "stderr", "extensionErrors", "bootstrap", "failure", "sessionId", "stage",
  "error", "name", "message", "stack", "type", "extensionPath", "event", "cause", "inherited",
  "PI_FABRIC_SESSION_ID", "PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_RUN_ROOT",
  "child", "exitCode", "signalCode", "health", "varz", "jsz", "status", "value",
  "server_id", "server_name", "version", "jetstream", "config", "sync_interval", "sync_always", "strict",
  "streams", "consumers", "messages", "broker", "resets", "bucket", "prefix", "active", "created",
  "filter_subject", "idle_heartbeat", "deliver_policy", "num_pending", "delivered", "consumer_seq",
  "stream_seq", "last_active", "consumer", "deleted", "at", "ready", "presence", "leases", "errors",
  "key", "revision", "machine", "reason",
]);
export const NATS_DIAGNOSTIC_FIELD_LIMIT = 1024;

function redact(value: string): string {
  return value
    // A credentials block may contain arbitrary prose or even malformed JWT/seed bytes.
    .replace(/-+BEGIN[^\r\n]*(?:NATS|JWT|SEED|CREDENTIAL)[^\r\n]*-+[\s\S]*?(?:-+END[^\r\n]*-+|$)/gi, "[REDACTED credentials]")
    .replace(/\bS[A-Z2-7]{55,}\b/g, "[REDACTED seed]")
    .replace(/\beyJ[A-Za-z0-9_-]*(?:\.[A-Za-z0-9_-]+){1,2}/g, "[REDACTED JWT]")
    .replace(/\b(?:Bearer|Basic)\s+[^\s,;]+/gi, "[REDACTED authorization]")
    .replace(/\b(?:[\w-]*(?:token|password|secret|credential|api[_-]?key|seed|jwt)[\w-]*)["']?\s*[:=]\s*(?:"(?:\\.|[^"\\])*"|'[^']*'|[^\s,;}]+)/gi, "[REDACTED credential]")
    // Conservatively hide ALL paths, including credential paths with spaces. Retain
    // stack function names/line numbers elsewhere, not a runner's private namespace.
    .replace(/"[^"\r\n]*(?:[\\/]|\.creds\b)[^"\r\n]*"|'[^'\r\n]*(?:[\\/]|\.creds\b)[^'\r\n]*'/gi, "[REDACTED path]")
    .replace(/(?:[A-Za-z]:[\\/]|~?[\\/])[^\s"'<>;,)}\]]+/g, "[REDACTED path]")
    .replace(/\b[^\s"'<>;,)}\]]*\.(?:creds|jwt|seed|pem|key)\b/gi, "[REDACTED path]")
    .replace(/\b(?:auth|credentials|secrets?)\.json\b/gi, "[REDACTED path]")
    // Opaque token-shaped runs are unsafe even without a recognizable label.
    .replace(/[A-Za-z0-9_+\/=\-]{32,}/g, "[REDACTED opaque]")
    .replace(/[\u0000-\u0008\u000b-\u001f\u007f]/g, "");
}

function scrubString(value: string): string {
  const safe = redact(value);
  return safe.length > NATS_DIAGNOSTIC_FIELD_LIMIT
    ? `${safe.slice(0, NATS_DIAGNOSTIC_FIELD_LIMIT - 12)}[truncated]` : safe;
}

/** Safe to throw: no original error, cause, prototype or arbitrary properties survive. */
export function sanitizeNatsError(value: unknown): Error {
  const safe = new Error(value instanceof Error ? scrubString(value.message) : formatNatsDiagnostic(value));
  if (value instanceof Error) safe.name = scrubString(value.name);
  safe.stack = scrubString(value instanceof Error && typeof value.stack === "string"
    ? value.stack : safe.stack ?? String(safe));
  return safe;
}

export function formatNatsDiagnostic(value: unknown): string {
  let remaining = 128;
  const select = (input: unknown, depth: number): unknown => {
    if (--remaining < 0 || depth > 6) return "[bounded]";
    if (typeof input === "string") return scrubString(input);
    if (typeof input === "number") return Number.isFinite(input) ? input : null;
    if (input === null || typeof input === "boolean") return input;
    if (input instanceof Error) return select({ name: input.name, message: input.message, stack: input.stack, cause: input.cause }, depth);
    if (Array.isArray(input)) return input.slice(0, 16).map(child => select(child, depth + 1));
    if (typeof input !== "object" || input === null) return undefined;
    return Object.fromEntries(Object.entries(input).filter(([key]) => FIELDS.has(key))
      .map(([key, child]) => [key, select(child, depth + 1)]));
  };
  return JSON.stringify(select(value, 0)) ?? "null";
}
