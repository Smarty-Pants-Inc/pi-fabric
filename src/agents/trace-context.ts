// Dependency-free: the standalone worker validates its argument with this module.

/** W3C Trace Context headers, exported to a child as TRACEPARENT and TRACESTATE. */
export interface FabricTraceContextV1 {
  traceparent: string;
  tracestate?: string;
}

/** The launch a trace context is requested for. */
export interface FabricChildTraceContextLaunch {
  /** The outer tool call (normally `fabric_exec`) that started the child; absent for actor turns. */
  parentToolCallId?: string;
  /** The nested `agents.*` call, as carried by its audit and proxied `tool_result`. */
  nestedToolCallId?: string;
  /** Absent when a durable resident host mints the id after the request leaves the session. */
  runId?: string;
  runner: string;
  name: string;
}

/**
 * Host-local synchronous request emitted before each child launch and
 * relaunch. The first valid `reply` made before the listener's first await
 * wins; with none, the child keeps the environment it would inherit.
 */
export interface FabricChildTraceContextRequestV1 extends FabricChildTraceContextLaunch {
  version: 1;
  reply: (context: FabricTraceContextV1) => void;
}

const MAX_TRACESTATE_CHARS = 512;
// version-traceid-parentid-flags, lowercase hex; version ff and all-zero ids are invalid.
const TRACEPARENT = /^(?!ff)[0-9a-f]{2}-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}$/;
const TRACESTATE = /^[\x20-\x7e]+$/;
const TRACESTATE_KEY = /^(?:[a-z][a-z0-9_*\/\-]{0,255}|[a-z0-9][a-z0-9_*\/\-]{0,240}@[a-z][a-z0-9_*\/\-]{0,13})$/;
const TRACESTATE_VALUE = /^[\x20-\x2b\x2d-\x3c\x3e-\x7e]{0,255}[\x21-\x2b\x2d-\x3c\x3e-\x7e]$/;

const validTracestate = (value: unknown): value is string => {
  if (typeof value !== "string" || value.length > MAX_TRACESTATE_CHARS || !TRACESTATE.test(value)) return false;
  const members = value.split(",");
  if (members.length > 32) return false;
  const keys = new Set<string>();
  for (const member of members) {
    // ASCII-space OWS only: the environment boundary still forbids tabs and all other controls.
    const entry = member.replace(/^ +| +$/g, "");
    const separator = entry.indexOf("=");
    const key = entry.slice(0, separator);
    if (separator < 0 || !TRACESTATE_KEY.test(key) || keys.has(key) || !TRACESTATE_VALUE.test(entry.slice(separator + 1))) return false;
    keys.add(key);
  }
  return true;
};

/**
 * Only the two headers, in their wire grammar: a reply reaches a child's
 * environment, so it can never name another variable or span a line.
 */
export const readTraceContext = (value: unknown): FabricTraceContextV1 | undefined => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const { traceparent, tracestate } = value as Record<string, unknown>;
  if (typeof traceparent !== "string" || !TRACEPARENT.test(traceparent)) return undefined;
  // Invalid vendor state must not invalidate a valid parent.
  return validTracestate(tracestate) ? { traceparent, tracestate } : { traceparent };
};
